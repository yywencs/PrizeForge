package po

import (
	"big-market-kratos/internal/biz/award"
	"time"
)

// UserAwardRecord 用户中奖记录表
type UserAwardRecord struct {
	// ID 自增ID
	ID uint64 `gorm:"column:id;primaryKey;autoIncrement;comment:自增ID"`

	// UserID 用户ID
	UserID string `gorm:"column:user_id;type:varchar(32);not null;index;comment:用户ID"`

	// ActivityID 活动ID
	ActivityID int64 `gorm:"column:activity_id;not null;index;comment:活动ID"`

	// StrategyID 抽奖策略ID
	StrategyID int64 `gorm:"column:strategy_id;not null;index;comment:抽奖策略ID"`

	// OrderID 抽奖订单ID【作为幂等使用】
	OrderID string `gorm:"column:order_id;type:varchar(12);not null;unique;comment:抽奖订单ID【作为幂等使用】"`

	// AwardID 奖品ID
	AwardID int64 `gorm:"column:award_id;not null;comment:奖品ID"`

	// AwardTitle 奖品标题（名称）
	AwardTitle string `gorm:"column:award_title;type:varchar(128);not null;comment:奖品标题（名称）"`

	// AwardTime 中奖时间
	AwardTime time.Time `gorm:"column:award_time;type:datetime;not null;comment:中奖时间"`

	// AwardState 奖品状态；create-创建、completed-发奖完成
	AwardState string `gorm:"column:award_state;type:varchar(16);not null;default:create;comment:奖品状态；create-创建、completed-发奖完成"`

	// CreateTime 创建时间
	CreateTime time.Time `gorm:"column:create_time;type:datetime;not null;autoCreateTime;comment:创建时间"`

	// UpdateTime 更新时间
	UpdateTime time.Time `gorm:"column:update_time;type:datetime;not null;autoUpdateTime;comment:更新时间"`
}

// TableName 指定表名
func (UserAwardRecord) TableName() string {
	return "user_award_record"
}

func (p *UserAwardRecord) ToEntity() *award.UserAwardRecord {
	return &award.UserAwardRecord{
		UserID:     p.UserID,
		ActivityID: p.ActivityID,
		StrategyID: p.StrategyID,
		OrderID:    p.OrderID,
		AwardID:    int(p.AwardID),
		AwardTitle: p.AwardTitle,
		AwardTime:  p.AwardTime,
		AwardState: award.AwardState(p.AwardState),
	}
}
