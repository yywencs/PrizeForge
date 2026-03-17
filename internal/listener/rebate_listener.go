package listener

import (
	"big-market-kratos/internal/biz/activity"
	"big-market-kratos/internal/biz/rebate"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"time"
)

type RebateListener struct {
	activityService *activity.ActivityQuotaUsecase
}

func NewRebateListener(activityService *activity.ActivityQuotaUsecase) *RebateListener {
	return &RebateListener{
		activityService: activityService,
	}
}

func (l *RebateListener) Handle(ctx context.Context, body []byte) (retry bool, err error) {
	// 1. Unmarshal message
	// Use a local struct to decode Data directly into RebateMessage
	var event struct {
		ID        string
		Timestamp time.Time
		Data      rebate.RebateMessage
	}
	if err := json.Unmarshal(body, &event); err != nil {
		slog.Error("Unmarshal rebate message failed", "err", err, "body", string(body))
		return false, fmt.Errorf("unmarshal failed: %w", err)
	}

	rebateMsg := event.Data
	slog.Info("Received rebate message", "userId", rebateMsg.UserID, "rebateType", rebateMsg.RebateType, "config", rebateMsg.RebateConfig)

	// 2. Process based on RebateType
	switch rebateMsg.RebateType {
	case string(rebate.Sku):
		// Parse SKU from config
		sku, err := strconv.ParseInt(rebateMsg.RebateConfig, 10, 64)
		if err != nil {
			slog.Error("Invalid sku config", "config", rebateMsg.RebateConfig, "err", err)
			return false, nil // Invalid config, no retry
		}

		// Construct SkuRecharge
		skuRecharge := &activity.SkuRecharge{
			UserID:        rebateMsg.UserID,
			Sku:           sku,
			OutBusinessNo: rebateMsg.BizID,
		}

		// 3. Create Raffle Activity Order (Award Distribution)
		orderID, err := l.activityService.CreateRaffleActivityOrder(ctx, skuRecharge)
		if err != nil {
			// Check for duplicate key error (idempotency)
			if errors.Is(err, activity.ErrDBIndexDuplicate) {
				slog.Warn("Duplicate rebate order, considered success", "userId", rebateMsg.UserID, "bizId", rebateMsg.BizID)
				return false, nil
			}

			slog.Error("CreateRaffleActivityOrder failed", "userId", rebateMsg.UserID, "sku", sku, "err", err)
			return true, err
		}
		slog.Info("Rebate award distributed successfully", "userId", rebateMsg.UserID, "orderId", orderID)

	case string(rebate.Integral):
		// TODO: Handle integral rebate
		slog.Info("Integral rebate not implemented yet", "userId", rebateMsg.UserID)
		return false, nil

	default:
		slog.Warn("Unknown rebate type", "type", rebateMsg.RebateType)
		return false, nil
	}

	return false, nil
}
