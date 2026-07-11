package po

import (
	"prizeforge/internal/domain/rebate"
	"time"
)

type DailyBehaviorRebate struct {
	ID           int64     `gorm:"column:id;primaryKey;autoIncrement"`
	BehaviorType string    `gorm:"column:behavior_type"`
	RebateDesc   string    `gorm:"column:rebate_desc"`
	RebateType   string    `gorm:"column:rebate_type"`
	RebateConfig string    `gorm:"column:rebate_config"`
	State        string    `gorm:"column:state"`
	CreateTime   time.Time `gorm:"column:create_time"`
	UpdateTime   time.Time `gorm:"column:update_time"`
}

func (DailyBehaviorRebate) TableName() string {
	return "daily_behavior_rebate"
}

func (p *DailyBehaviorRebate) ToEntity() *rebate.DailyBehaviorRebate {
	return &rebate.DailyBehaviorRebate{
		BehaviorType: p.BehaviorType,
		RebateDesc:   p.RebateDesc,
		RebateType:   p.RebateType,
		RebateConfig: p.RebateConfig,
	}
}

type UserBehaviorRebateOrder struct {
	ID            int64     `gorm:"column:id;primaryKey;autoIncrement"`
	UserID        string    `gorm:"column:user_id"`
	OrderID       string    `gorm:"column:order_id"`
	BehaviorType  string    `gorm:"column:behavior_type"`
	OutBusinessNo string    `gorm:"column:out_business_no"`
	RebateDesc    string    `gorm:"column:rebate_desc"`
	RebateType    string    `gorm:"column:rebate_type"`
	RebateConfig  string    `gorm:"column:rebate_config"`
	BizID         string    `gorm:"column:biz_id"`
	CreateTime    time.Time `gorm:"column:create_time"`
	UpdateTime    time.Time `gorm:"column:update_time"`
}

func (UserBehaviorRebateOrder) TableName() string {
	return "user_behavior_rebate_order"
}

func (p *UserBehaviorRebateOrder) ToEntity() *rebate.BehaviorRebateOrder {
	return &rebate.BehaviorRebateOrder{
		UserID:        p.UserID,
		OrderID:       p.OrderID,
		BehaviorType:  p.BehaviorType,
		RebateDesc:    p.RebateDesc,
		RebateType:    p.RebateType,
		RebateConfig:  p.RebateConfig,
		OutBusinessNo: p.OutBusinessNo,
		BizID:         p.BizID,
	}
}
