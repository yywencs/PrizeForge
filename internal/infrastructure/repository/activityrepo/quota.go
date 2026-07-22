package activityrepo

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"prizeforge/internal/domain/activity"
	"prizeforge/internal/infrastructure/adapter"
	"prizeforge/internal/infrastructure/repository/po"
	"time"

	"gorm.io/gorm"
)

const reserveActivityQuotaScript = `
	local total_key = KEYS[1]
	local month_key = KEYS[2]
	local day_key = KEYS[3]
	local pending_key = KEYS[4]
	local request_id = ARGV[1]
	local order_id = ARGV[2]

	local pending = redis.call('GET', pending_key)
	if pending then
		local decoded_ok, reservation = pcall(cjson.decode, pending)
		if not decoded_ok or not reservation.request_id or not reservation.order_id then
			return {-5, pending}
		end
		if reservation.request_id == request_id then
			return {1, pending}
		end
		return {2, pending}
	end

	if redis.call('EXISTS', total_key) == 0 or
		redis.call('EXISTS', month_key) == 0 or
		redis.call('EXISTS', day_key) == 0 then
		return {-1, ''}
	end

	local total = tonumber(redis.call('GET', total_key))
	local month = tonumber(redis.call('GET', month_key))
	local day = tonumber(redis.call('GET', day_key))
	if not total or not month or not day then
		return {-5, ''}
	end
	if total <= 0 then
		return {-2, ''}
	end
	if month <= 0 then
		return {-3, ''}
	end
	if day <= 0 then
		return {-4, ''}
	end

	local reservation = cjson.encode({request_id = request_id, order_id = order_id})
	redis.call('DECR', total_key)
	redis.call('DECR', month_key)
	redis.call('DECR', day_key)
	redis.call('PERSIST', total_key)
	redis.call('PERSIST', month_key)
	redis.call('PERSIST', day_key)
	redis.call('SET', pending_key, reservation)
	return {0, reservation}
`

const initializeActivityQuotaScript = `
	local existing = redis.call('EXISTS', KEYS[1], KEYS[2], KEYS[3])
	if existing == 3 then
		redis.call('PERSIST', KEYS[1])
		redis.call('PERSIST', KEYS[2])
		redis.call('PERSIST', KEYS[3])
		return 0
	end
	if existing ~= 0 then
		return -1
	end

	redis.call('MSET', KEYS[1], ARGV[1], KEYS[2], ARGV[2], KEYS[3], ARGV[3])
	return 1
`

type activityQuotaReservation struct {
	RequestID string `json:"request_id"`
	OrderID   string `json:"order_id"`
}

// reserveActivityQuota 使用一个 Lua 脚本原子完成总、月、日额度扣减和进行中订单占位。
// 相同 request_id 会复用第一次生成的 order_id，不同 request_id 在旧订单完成前会被拒绝。
func (r *Repository) reserveActivityQuota(ctx context.Context, order *activity.UserRaffleOrder) (string, bool, error) {
	keys := activityQuotaKeys(order.UserID, order.ActivityID, order.OrderTime)

	for attempt := 0; attempt < 2; attempt++ {
		result, err := r.redis.Eval(
			ctx,
			reserveActivityQuotaScript,
			keys,
			order.RequestID,
			order.OrderID,
		)
		if err != nil {
			return "", false, err
		}

		status, payload, err := parseActivityQuotaResult(result)
		if err != nil {
			return "", false, err
		}
		switch status {
		case 0, 1:
			var reservation activityQuotaReservation
			if err := json.Unmarshal([]byte(payload), &reservation); err != nil {
				return "", false, fmt.Errorf("decode activity quota reservation: %w", err)
			}
			if reservation.OrderID == "" || reservation.RequestID != order.RequestID {
				return "", false, errors.New("invalid activity quota reservation")
			}
			return reservation.OrderID, status == 1, nil
		case 2:
			return "", false, activity.ErrDrawInProgress
		case -1:
			if attempt == 1 {
				return "", false, errors.New("activity quota cache is not initialized")
			}
			if err := r.initializeActivityQuota(ctx, order.UserID, order.ActivityID, order.OrderTime); err != nil {
				return "", false, err
			}
		case -2:
			return "", false, activity.ErrActivityQuotaError
		case -3:
			return "", false, activity.ErrActivityAccountMonthCountSurplusNotEnough
		case -4:
			return "", false, activity.ErrActivityAccountDayCountSurplusNotEnough
		case -5:
			return "", false, errors.New("activity quota cache contains invalid data")
		default:
			return "", false, fmt.Errorf("unexpected activity quota status %d", status)
		}
	}

	return "", false, errors.New("activity quota reservation exhausted retries")
}

func activityQuotaKeys(userID string, activityID int64, orderTime time.Time) []string {
	return []string{
		adapter.GetActivityAccountTotalSurplusKey(activityID, userID),
		adapter.GetActivityAccountMonthSurplusKey(activityID, userID, orderTime.Format("2006-01")),
		adapter.GetActivityAccountDaySurplusKey(activityID, userID, orderTime.Format("2006-01-02")),
		adapter.GetPendingRaffleOrderKey(activityID, userID),
	}
}

func parseActivityQuotaResult(result interface{}) (int64, string, error) {
	values, ok := result.([]interface{})
	if !ok || len(values) != 2 {
		return 0, "", fmt.Errorf("unexpected activity quota result %#v", result)
	}
	status, ok := values[0].(int64)
	if !ok {
		return 0, "", fmt.Errorf("unexpected activity quota status %#v", values[0])
	}
	payload, ok := values[1].(string)
	if !ok {
		return 0, "", fmt.Errorf("unexpected activity quota payload %#v", values[1])
	}
	return status, payload, nil
}

// initializeActivityQuota 仅在三类额度键都不存在时，使用同一份 MySQL 快照原子初始化 Redis。
// 只缺少部分键意味着缓存状态不完整，此时拒绝重建，避免用滞后的数据库值覆盖已发生的预占。
func (r *Repository) initializeActivityQuota(ctx context.Context, userID string, activityID int64, orderTime time.Time) error {
	db, _ := r.routerDB.DBStrategy(userID)
	if db == nil {
		return activity.ErrDBRouterError
	}

	var accountPO po.RaffleActivityAccount
	if err := db.WithContext(ctx).
		Table("raffle_activity_account").
		Where("user_id = ? AND activity_id = ?", userID, activityID).
		First(&accountPO).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return activity.ErrActivityQuotaError
		}
		return err
	}

	monthSurplus := accountPO.MonthCountSurplus
	var monthPO po.RaffleActivityAccountMonth
	monthErr := db.WithContext(ctx).
		Table("raffle_activity_account_month").
		Where("user_id = ? AND activity_id = ? AND month = ?", userID, activityID, orderTime.Format("2006-01")).
		First(&monthPO).Error
	if monthErr == nil {
		monthSurplus = monthPO.MonthCountSurplus
	} else if !errors.Is(monthErr, gorm.ErrRecordNotFound) {
		return monthErr
	}

	daySurplus := accountPO.DayCountSurplus
	var dayPO po.RaffleActivityAccountDay
	dayErr := db.WithContext(ctx).
		Table("raffle_activity_account_day").
		Where("user_id = ? AND activity_id = ? AND day = ?", userID, activityID, orderTime.Format("2006-01-02")).
		First(&dayPO).Error
	if dayErr == nil {
		daySurplus = dayPO.DayCountSurplus
	} else if !errors.Is(dayErr, gorm.ErrRecordNotFound) {
		return dayErr
	}

	return r.initializeActivityQuotaValues(
		ctx,
		userID,
		activityID,
		orderTime,
		accountPO.TotalCountSurplus,
		monthSurplus,
		daySurplus,
	)
}

func (r *Repository) initializeActivityQuotaValues(
	ctx context.Context,
	userID string,
	activityID int64,
	currentTime time.Time,
	totalSurplus, monthSurplus, daySurplus int,
) error {
	keys := activityQuotaKeys(userID, activityID, currentTime)
	result, err := r.redis.Eval(
		ctx,
		initializeActivityQuotaScript,
		keys[:3],
		totalSurplus,
		monthSurplus,
		daySurplus,
	)
	if err != nil {
		return err
	}
	status, ok := result.(int64)
	if !ok {
		return fmt.Errorf("unexpected activity quota initialization result %#v", result)
	}
	if status == -1 {
		return errors.New("activity quota cache is partially initialized")
	}
	return nil
}
