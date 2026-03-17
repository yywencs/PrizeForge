package data

import (
	"big-market-kratos/internal/biz/rebate"
	"big-market-kratos/internal/data/po"
	"big-market-kratos/pkg/rabbitmq"
	"context"
	"errors"
	"fmt"
	"time"

	"gorm.io/gorm"
)

type rebateRepository struct {
	db        *gorm.DB
	routerDB  *DBRouter
	publisher *Publisher
}

func NewRebateRepository(db *gorm.DB, routerDB *DBRouter, publisher *Publisher) rebate.Repo {
	return &rebateRepository{
		db:        db,
		routerDB:  routerDB,
		publisher: publisher,
	}
}

func (r *rebateRepository) QueryDailyBehaviorRebateConfig(ctx context.Context, behaviorType rebate.BehaviorType) ([]*rebate.DailyBehaviorRebate, error) {
	var list []*po.DailyBehaviorRebate
	err := r.db.WithContext(ctx).
		Where("behavior_type = ? AND state = ?", behaviorType, "open").
		Find(&list).Error
	if err != nil {
		return nil, err
	}

	res := make([]*rebate.DailyBehaviorRebate, 0, len(list))
	for _, v := range list {
		res = append(res, v.ToEntity())
	}
	return res, nil
}

// SaveUserRebateOrder saves orders in a transaction and publishes messages
func (r *rebateRepository) SaveUserRebateOrder(ctx context.Context, userId string, aggregate *rebate.BehaviorRebate) error {
	// 1. Get DB connection for sharding
	db, tableSuffix := r.routerDB.DBStrategy(userId)
	if db == nil {
		return rebate.ErrDBRouterNotFound
	}

	// 2. Transaction
	err := db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		for _, order := range aggregate.BehaviorRebateOrders {
			po := &po.UserBehaviorRebateOrder{
				UserID:        order.UserID,
				OrderID:       order.OrderID,
				BehaviorType:  order.BehaviorType,
				OutBusinessNo: order.OutBusinessNo,
				RebateDesc:    order.RebateDesc,
				RebateType:    order.RebateType,
				RebateConfig:  order.RebateConfig,
				BizID:         order.BizID,
				CreateTime:    time.Now(),
				UpdateTime:    time.Now(),
			}

			// Specify table name with suffix
			tableName := fmt.Sprintf("user_behavior_rebate_order_%s", tableSuffix)
			if err := tx.Table(tableName).Create(&po).Error; err != nil {
				if errors.Is(err, gorm.ErrDuplicatedKey) {
					return nil
				}
				return err
			}
		}
		return nil
	})

	if err != nil {
		return err
	}

	// 3. Publish messages (Best effort)
	for _, order := range aggregate.BehaviorRebateOrders {
		msg := rebate.RebateMessage{
			UserID:       order.UserID,
			RebateDesc:   order.RebateDesc,
			RebateType:   order.RebateType,
			RebateConfig: order.RebateConfig,
			BizID:        order.BizID,
		}

		event := rabbitmq.NewBaseEvent(msg)

		_ = r.publisher.PublishSendRebate(ctx, event)
	}

	return nil
}

func (r *rebateRepository) QueryUserRebateOrder(ctx context.Context, userId string, outBusinessNo string) ([]*rebate.BehaviorRebateOrder, error) {
	db, tableSuffix := r.routerDB.DBStrategy(userId)
	var list []*po.UserBehaviorRebateOrder
	err := db.WithContext(ctx).Table("user_behavior_rebate_order_"+tableSuffix).
		Where("user_id = ? AND biz_id LIKE ?", userId, outBusinessNo+"%").
		Find(&list).Error
	if err != nil {
		return nil, err
	}

	res := make([]*rebate.BehaviorRebateOrder, 0, len(list))
	for _, v := range list {
		res = append(res, v.ToEntity())
	}
	return res, nil
}
