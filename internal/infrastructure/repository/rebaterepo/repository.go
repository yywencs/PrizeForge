package rebaterepo

import (
	"context"
	"errors"
	"fmt"
	"prizeforge/internal/domain/rebate"
	"prizeforge/internal/infrastructure/repository/po"
	"prizeforge/pkg/rabbitmq"
	"time"

	mysqlDriver "github.com/go-sql-driver/mysql"
	"gorm.io/gorm"
)

type rebateRepository struct {
	db        *gorm.DB
	routerDB  databaseRouter
	publisher rebatePublisher
}

// databaseRouter 定义返利仓储选择用户分片所需的最小路由能力。
type databaseRouter interface {
	DBStrategy(string) (*gorm.DB, string)
}

// rebatePublisher 定义返利订单提交后发布领域消息所需的最小能力。
type rebatePublisher interface {
	PublishSendRebate(context.Context, *rabbitmq.BaseEvent) error
}

func NewRebateRepository(db *gorm.DB, routerDB databaseRouter, publisher rebatePublisher) rebate.Repo {
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
	db, tableSuffix := r.routerDB.DBStrategy(userId)
	if db == nil {
		return rebate.ErrDBRouterNotFound
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
				if isDuplicateKeyError(err) {
					continue
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
		msg := rebate.RebateMessage{
			UserID:       order.UserID,
			RebateDesc:   order.RebateDesc,
			RebateType:   order.RebateType,
			RebateConfig: order.RebateConfig,
			BizID:        order.BizID,
		}

		event := rabbitmq.NewBaseEvent(msg)

		if err := r.publisher.PublishSendRebate(ctx, event); err != nil {
			return err
		}
	}

	return nil
}

func isDuplicateKeyError(err error) bool {
	if errors.Is(err, gorm.ErrDuplicatedKey) {
		return true
	}
	var mysqlErr *mysqlDriver.MySQLError
	return errors.As(err, &mysqlErr) && mysqlErr.Number == 1062
}

func (r *rebateRepository) QueryUserRebateOrder(ctx context.Context, userId string, outBusinessNo string) ([]*rebate.BehaviorRebateOrder, error) {
	db, tableSuffix := r.routerDB.DBStrategy(userId)
	if db == nil {
		return nil, rebate.ErrDBRouterNotFound
	}
	var list []*po.UserBehaviorRebateOrder
	err := db.WithContext(ctx).Table("user_behavior_rebate_order_"+tableSuffix).
		Where("user_id = ? AND out_business_no = ?", userId, outBusinessNo).
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
