package rebate

import (
	rebatebiz "prizeforge/internal/domain/rebate"
	"prizeforge/internal/infrastructure/adapter"
	"prizeforge/internal/infrastructure/repository/po"
	"prizeforge/pkg/rabbitmq"
	"context"
	"errors"
	"fmt"
	"time"

	"gorm.io/gorm"
)

type rebateRepository struct {
	db        *gorm.DB
	routerDB  *adapter.DBRouter
	publisher *adapter.Publisher
}

func NewRebateRepository(db *gorm.DB, routerDB *adapter.DBRouter, publisher *adapter.Publisher) rebatebiz.Repo {
	return &rebateRepository{
		db:        db,
		routerDB:  routerDB,
		publisher: publisher,
	}
}

func (r *rebateRepository) QueryDailyBehaviorRebateConfig(ctx context.Context, behaviorType rebatebiz.BehaviorType) ([]*rebatebiz.DailyBehaviorRebate, error) {
	var list []*po.DailyBehaviorRebate
	err := r.db.WithContext(ctx).
		Where("behavior_type = ? AND state = ?", behaviorType, "open").
		Find(&list).Error
	if err != nil {
		return nil, err
	}

	res := make([]*rebatebiz.DailyBehaviorRebate, 0, len(list))
	for _, v := range list {
		res = append(res, v.ToEntity())
	}
	return res, nil
}

// SaveUserRebateOrder saves orders in a transaction and publishes messages
func (r *rebateRepository) SaveUserRebateOrder(ctx context.Context, userId string, aggregate *rebatebiz.BehaviorRebate) error {
	db, tableSuffix := r.routerDB.DBStrategy(userId)
	if db == nil {
		return rebatebiz.ErrDBRouterNotFound
	}

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

	for _, order := range aggregate.BehaviorRebateOrders {
		msg := rebatebiz.RebateMessage{
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

func (r *rebateRepository) QueryUserRebateOrder(ctx context.Context, userId string, outBusinessNo string) ([]*rebatebiz.BehaviorRebateOrder, error) {
	db, tableSuffix := r.routerDB.DBStrategy(userId)
	var list []*po.UserBehaviorRebateOrder
	err := db.WithContext(ctx).Table("user_behavior_rebate_order_"+tableSuffix).
		Where("user_id = ? AND biz_id LIKE ?", userId, outBusinessNo+"%").
		Find(&list).Error
	if err != nil {
		return nil, err
	}

	res := make([]*rebatebiz.BehaviorRebateOrder, 0, len(list))
	for _, v := range list {
		res = append(res, v.ToEntity())
	}
	return res, nil
}
