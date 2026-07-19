package strategyrepo

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"prizeforge/internal/domain/strategy"
	"prizeforge/internal/domain/task"
	"prizeforge/internal/infrastructure/adapter"
	"prizeforge/internal/infrastructure/repository/po"
	"prizeforge/pkg/cache"
	"prizeforge/pkg/common"
	"strconv"
	"strings"
	"time"

	"github.com/hibiken/asynq"
	"gorm.io/gorm"
)

// 获取strategyID的全部StrategyAward
func (sr *strategyRepository) QueryStrategyAwardList(ctx context.Context, strategyID int64) ([]*strategy.StrategyAward, error) {
	var strategyAwards []*strategy.StrategyAward

	cacheKey := adapter.GetStrategyAwardKey(strategyID)

	err := sr.redis.Once(&cache.Item{
		Ctx:   ctx,
		Key:   cacheKey,
		Value: &strategyAwards,
		TTL:   10 * 24 * time.Hour,

		Do: func(*cache.Item) (interface{}, error) {
			var dbResult []po.StrategyAward
			err := sr.db.WithContext(ctx).
				Where("strategy_id = ?", strategyID).
				Find(&dbResult).Error

			if err != nil {
				return nil, err
			}

			var entities []*strategy.StrategyAward
			for _, po := range dbResult {
				entities = append(entities, po.ToEntity())
			}

			return entities, nil
		},
	})

	if err != nil {
		return nil, err
	}

	return strategyAwards, nil
}

func (d *strategyRepository) QueryStrategyAward(ctx context.Context, strategyID int64, awardID int64) (*strategy.StrategyAward, error) {
	var strategyAward strategy.StrategyAward

	cacheKey := fmt.Sprintf("%s:%d", adapter.GetStrategyAwardKey(strategyID), awardID)

	err := d.redis.Once(&cache.Item{
		Ctx:   ctx,
		Key:   cacheKey,
		Value: &strategyAward,
		TTL:   10 * 24 * time.Hour,

		Do: func(*cache.Item) (interface{}, error) {
			var dbResult po.StrategyAward
			err := d.db.WithContext(ctx).
				Where("strategy_id = ? AND award_id = ?", strategyID, awardID).
				First(&dbResult).Error

			if err != nil {
				if errors.Is(err, gorm.ErrRecordNotFound) {
					return nil, nil
				}
				return nil, err
			}

			return dbResult.ToEntity(), nil
		},
	})

	if err != nil {
		return nil, err
	}

	return &strategyAward, nil
}

// UpdateStrategyAwardStock 根据队列消费结果持久化库存；正式抽奖用 orderID 保证重复任务只扣一次。
func (d *strategyRepository) UpdateStrategyAwardStock(ctx context.Context, userID string, orderID string, strategyID int64, awardID int64) error {
	if orderID == "" {
		tx := d.db.WithContext(ctx).
			Model(&po.StrategyAward{}).
			Where("strategy_id = ? AND award_id = ? AND award_count_surplus > 0", strategyID, awardID).
			Update("award_count_surplus", gorm.Expr("award_count_surplus - 1"))
		if tx.Error != nil {
			return tx.Error
		}
		if tx.RowsAffected == 0 {
			return errors.New("no stock to consume or record not found")
		}
		return nil
	}

	return d.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		reservation := &po.StrategyAwardStockReservation{
			UserID:     userID,
			OrderID:    orderID,
			StrategyID: strategyID,
			AwardID:    awardID,
		}
		if err := tx.Create(reservation).Error; err != nil {
			if errors.Is(err, gorm.ErrDuplicatedKey) || strings.Contains(err.Error(), "Duplicate entry") {
				return nil
			}
			return err
		}

		res := tx.Model(&po.StrategyAward{}).
			Where("strategy_id = ? AND award_id = ? AND award_count_surplus > 0", strategyID, awardID).
			Update("award_count_surplus", gorm.Expr("award_count_surplus - 1"))
		if res.Error != nil {
			return res.Error
		}
		if res.RowsAffected == 0 {
			return errors.New("no stock to consume or record not found")
		}
		return nil
	})
}

// NewAwardStockConsumeHandler 返回一个可直接用于 ConsumeAwardStockQueue 的处理函数
func NewAwardStockConsumeHandler(ctx context.Context, repo *strategyRepository) func(msgs []task.AwardStockConsumeMessage) error {
	return func(msgs []task.AwardStockConsumeMessage) error {
		for _, m := range msgs {
			if err := repo.UpdateStrategyAwardStock(ctx, m.UserID, m.OrderID, m.StrategyID, m.AwardID); err != nil {
				return err
			}
		}
		return nil
	}
}

// QueryAwardRuleWeight 查询策略规则权重配置
func (d *strategyRepository) QueryAwardRuleWeight(ctx context.Context, strategyID int64) ([]*strategy.WeightBucket, error) {
	var ruleWeightVOList []*strategy.WeightBucket

	cacheKey := adapter.GetStrategyRuleWeightKey(strategyID)
	err := d.redis.Once(&cache.Item{
		Ctx:   ctx,
		Key:   cacheKey,
		Value: &ruleWeightVOList,
		TTL:   10 * 24 * time.Hour,
		Do: func(*cache.Item) (interface{}, error) {
			var rulePO po.StrategyRule
			err := d.db.WithContext(ctx).
				Where("strategy_id = ? AND rule_model = ?", strategyID, "rule_weight").
				First(&rulePO).Error

			if err != nil {
				if errors.Is(err, gorm.ErrRecordNotFound) {
					return nil, nil
				}
				return nil, err
			}

			ruleValueGroups := strings.Split(rulePO.RuleValue, common.SPACE)

			uniqueAwardIDs := make(map[int]struct{})

			type parsedRule struct {
				weightStr string
				weight    int
				awardIDs  []int
			}
			var parsedRules []parsedRule

			for _, group := range ruleValueGroups {
				if group == "" {
					continue
				}
				parts := strings.Split(group, common.COLON)
				if len(parts) != 2 {
					continue
				}

				weightStr := parts[0]
				awardIDsStr := parts[1]

				weight, err := strconv.Atoi(weightStr)
				if err != nil {
					return nil, err
				}

				var awardIDs []int
				idStrs := strings.Split(awardIDsStr, common.SPLIT)
				for _, idStr := range idStrs {
					id, err := strconv.Atoi(idStr)
					if err != nil {
						continue
					}
					awardIDs = append(awardIDs, id)
					uniqueAwardIDs[id] = struct{}{}
				}

				parsedRules = append(parsedRules, parsedRule{
					weightStr: group,
					weight:    weight,
					awardIDs:  awardIDs,
				})
			}

			awardIDList := make([]int64, 0, len(uniqueAwardIDs))
			for id := range uniqueAwardIDs {
				awardIDList = append(awardIDList, int64(id))
			}

			awardTitleMap := make(map[int]string)
			if len(awardIDList) > 0 {
				var awardPOs []po.StrategyAward
				err = d.db.WithContext(ctx).
					Where("strategy_id = ? AND award_id IN ?", strategyID, awardIDList).
					Find(&awardPOs).Error
				if err != nil {
					return nil, err
				}
				for _, po := range awardPOs {
					awardTitleMap[int(po.AwardID)] = po.AwardTitle
				}
			}

			var voList []*strategy.WeightBucket
			for _, rule := range parsedRules {
				var awardList []strategy.Award
				for _, id := range rule.awardIDs {
					title := awardTitleMap[id]
					awardList = append(awardList, strategy.Award{
						AwardId:    id,
						AwardTitle: title,
					})
				}

				voList = append(voList, &strategy.WeightBucket{
					RuleValue: rule.weightStr,
					Weight:    rule.weight,
					AwardIds:  rule.awardIDs,
					AwardList: awardList,
				})
			}

			return voList, nil
		},
	})

	if err != nil {
		return nil, err
	}

	return ruleWeightVOList, nil
}

func (d *strategyRepository) SubtractionAwardStock(ctx context.Context, strategyID int64, awardID int64) (bool, error) {
	cacheKey := adapter.GetStrategyAwardCountKey(strategyID, awardID)

	surplus, err := d.redis.Decr(ctx, cacheKey)
	if err != nil {
		return false, err
	}

	if surplus < 0 {
		return false, nil
	}

	lockKey := fmt.Sprintf("%s_%d", cacheKey, surplus)
	ok, err := d.redis.SetNX(ctx, lockKey, "locked", time.Hour*24)
	if err != nil {
		return false, err
	}

	return ok, nil
}

func (d *strategyRepository) ReserveAwardStock(ctx context.Context, userID string, orderID string, strategyID int64, awardID int64) (int64, bool, error) {
	if orderID == "" {
		ok, err := d.SubtractionAwardStock(ctx, strategyID, awardID)
		return awardID, ok, err
	}

	reservationKey := adapter.GetStrategyAwardReservationKey(userID, orderID)
	stockKey := adapter.GetStrategyAwardCountKey(strategyID, awardID)
	const reservationTTL = 30 * 24 * time.Hour
	script := `
		local existing = redis.call("GET", KEYS[1])
		if existing then
			return {2, existing}
		end

		local stock = redis.call("GET", KEYS[2])
		if not stock or tonumber(stock) <= 0 then
			return {0, ARGV[1]}
		end

		redis.call("DECR", KEYS[2])
		redis.call("SET", KEYS[1], ARGV[1], "EX", ARGV[2])
		return {1, ARGV[1]}
	`
	result, err := d.redis.Eval(ctx, script, []string{reservationKey, stockKey},
		strconv.FormatInt(awardID, 10),
		strconv.FormatInt(int64(reservationTTL/time.Second), 10),
	)
	if err != nil {
		return 0, false, err
	}
	values, ok := result.([]interface{})
	if !ok || len(values) < 2 {
		return 0, false, fmt.Errorf("unexpected stock reservation result: %T", result)
	}
	status, ok := values[0].(int64)
	if !ok {
		return 0, false, fmt.Errorf("unexpected stock reservation status: %T", values[0])
	}
	reservedAwardID, err := strconv.ParseInt(fmt.Sprint(values[1]), 10, 64)
	if err != nil {
		return 0, false, err
	}
	return reservedAwardID, status == 1 || status == 2, nil
}

func (d *strategyRepository) AwardStockConsumeSendQueue(ctx context.Context, userID string, orderID string, strategyID int64, awardID int64) error {
	msg := task.AwardStockConsumeMessage{
		StrategyID: strategyID,
		AwardID:    awardID,
		OrderID:    orderID,
		UserID:     userID,
	}

	payload, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	t := asynq.NewTask(task.TaskTypeStrategyAwardStockConsume, payload)
	options := []asynq.Option{asynq.Queue("critical"), asynq.ProcessIn(1 * time.Second)}
	if orderID != "" {
		options = append(options, asynq.TaskID("strategy-award-stock:"+userID+":"+orderID))
	}
	_, err = d.queue.Enqueue(t, options...)
	if errors.Is(err, asynq.ErrTaskIDConflict) {
		return nil
	}
	return err
}
