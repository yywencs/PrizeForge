package po

import (
	"big-market-kratos/internal/biz/activity"
	"time"
)

type RaffleActivity struct {
	ID            uint64    `gorm:"column:id;primaryKey;autoIncrement;comment:自增ID"`
	ActivityID    int64     `gorm:"column:activity_id;not null;comment:活动ID"`
	ActivityName  string    `gorm:"column:activity_name;type:varchar(64);not null;comment:活动名称"`
	ActivityDesc  string    `gorm:"column:activity_desc;type:varchar(128);not null;comment:活动描述"`
	BeginDateTime time.Time `gorm:"column:begin_date_time;type:datetime;not null;comment:开始时间"`
	EndDateTime   time.Time `gorm:"column:end_date_time;type:datetime;not null;comment:结束时间"`
	StrategyID    int64     `gorm:"column:strategy_id;not null;comment:抽奖策略ID"`
	State         string    `gorm:"column:state;type:varchar(8);not null;default:create;comment:活动状态"`
	CreateTime    time.Time `gorm:"column:create_time;type:datetime;not null;autoCreateTime;comment:创建时间"`
	UpdateTime    time.Time `gorm:"column:update_time;type:datetime;not null;autoUpdateTime;comment:更新时间"`
}

type RaffleActivityCount struct {
	ID              uint64    `gorm:"column:id;primaryKey;autoIncrement;comment:自增ID"`
	ActivityCountID int64     `gorm:"column:activity_count_id;not null;comment:活动次数编号"`
	TotalCount      int       `gorm:"column:total_count;not null;comment:总次数"`
	DayCount        int       `gorm:"column:day_count;not null;comment:日次数"`
	MonthCount      int       `gorm:"column:month_count;not null;comment:月次数"`
	CreateTime      time.Time `gorm:"column:create_time;type:datetime;not null;autoCreateTime;comment:创建时间"`
	UpdateTime      time.Time `gorm:"column:update_time;type:datetime;not null;autoUpdateTime;comment:更新时间"`
}

type RaffleActivitySku struct {
	ID                uint64    `gorm:"column:id;primaryKey;autoIncrement;comment:自增ID"`
	Sku               int64     `gorm:"column:sku;not null;comment:商品sku"`
	ActivityID        int64     `gorm:"column:activity_id;not null;comment:活动ID"`
	ActivityCountID   int64     `gorm:"column:activity_count_id;not null;comment:活动个人参与次数ID"`
	StockCount        int       `gorm:"column:stock_count;not null;comment:商品库存"`
	StockCountSurplus int       `gorm:"column:stock_count_surplus;not null;comment:剩余库存"`
	CreateTime        time.Time `gorm:"column:create_time;type:datetime;not null;autoCreateTime;comment:创建时间"`
	UpdateTime        time.Time `gorm:"column:update_time;type:datetime;not null;autoUpdateTime;comment:更新时间"`
}

type RaffleActivityAccount struct {
	ID                uint64    `gorm:"column:id;primaryKey;autoIncrement;comment:自增ID"`
	UserID            string    `gorm:"column:user_id;type:varchar(32);not null;comment:用户ID"`
	ActivityID        int64     `gorm:"column:activity_id;not null;comment:活动ID"`
	TotalCount        int       `gorm:"column:total_count;not null;comment:总次数"`
	TotalCountSurplus int       `gorm:"column:total_count_surplus;not null;comment:总次数-剩余"`
	DayCount          int       `gorm:"column:day_count;not null;comment:日次数"`
	DayCountSurplus   int       `gorm:"column:day_count_surplus;not null;comment:日次数-剩余"`
	MonthCount        int       `gorm:"column:month_count;not null;comment:月次数"`
	MonthCountSurplus int       `gorm:"column:month_count_surplus;not null;comment:月次数-剩余"`
	CreateTime        time.Time `gorm:"column:create_time;type:datetime;not null;autoCreateTime;comment:创建时间"`
	UpdateTime        time.Time `gorm:"column:update_time;type:datetime;not null;autoUpdateTime;comment:更新时间"`
}

type RaffleActivityOrder struct {
	ID            uint64    `gorm:"column:id;primaryKey;autoIncrement;comment:自增ID"`
	UserID        string    `gorm:"column:user_id;type:varchar(32);not null;comment:用户ID"`
	Sku           int64     `gorm:"column:sku;not null;comment:商品sku"`
	ActivityID    int64     `gorm:"column:activity_id;not null;comment:活动ID"`
	ActivityName  string    `gorm:"column:activity_name;type:varchar(64);not null;comment:活动名称"`
	StrategyID    int64     `gorm:"column:strategy_id;not null;comment:抽奖策略ID"`
	OrderID       string    `gorm:"column:order_id;type:varchar(12);not null;comment:订单ID"`
	OrderTime     time.Time `gorm:"column:order_time;type:datetime;not null;comment:下单时间"`
	TotalCount    int       `gorm:"column:total_count;not null;comment:总次数"`
	DayCount      int       `gorm:"column:day_count;not null;comment:日次数"`
	MonthCount    int       `gorm:"column:month_count;not null;comment:月次数"`
	State         string    `gorm:"column:state;type:varchar(8);not null;default:complete;comment:订单状态（complete）"`
	OutBusinessNo string    `gorm:"column:out_business_no;type:varchar(64);not null;comment:业务仿重ID - 外部透传的，确保幂等"`
	CreateTime    time.Time `gorm:"column:create_time;type:datetime;not null;autoCreateTime;comment:创建时间"`
	UpdateTime    time.Time `gorm:"column:update_time;type:datetime;not null;autoUpdateTime;comment:更新时间"`
}

type RaffleActivityAccountDay struct {
	ID              uint64    `gorm:"column:id;primaryKey;autoIncrement;comment:自增ID"`
	UserID          string    `gorm:"column:user_id;type:varchar(32);not null;comment:用户ID"`
	ActivityID      int64     `gorm:"column:activity_id;not null;comment:活动ID"`
	Day             string    `gorm:"column:day;type:varchar(10);not null;comment:日期（yyyy-mm-dd）"`
	DayCount        int       `gorm:"column:day_count;not null;comment:日次数"`
	DayCountSurplus int       `gorm:"column:day_count_surplus;not null;comment:日次数-剩余"`
	CreateTime      time.Time `gorm:"column:create_time;type:datetime;not null;autoCreateTime;comment:创建时间"`
	UpdateTime      time.Time `gorm:"column:update_time;type:datetime;not null;autoUpdateTime;comment:更新时间"`
}

type RaffleActivityAccountMonth struct {
	ID                uint64    `gorm:"column:id;primaryKey;autoIncrement;comment:自增ID"`
	UserID            string    `gorm:"column:user_id;type:varchar(32);not null;comment:用户ID"`
	ActivityID        int64     `gorm:"column:activity_id;not null;comment:活动ID"`
	Month             string    `gorm:"column:month;type:varchar(7);not null;comment:月（yyyy-mm）"`
	MonthCount        int       `gorm:"column:month_count;not null;comment:月次数"`
	MonthCountSurplus int       `gorm:"column:month_count_surplus;not null;comment:月次数-剩余"`
	CreateTime        time.Time `gorm:"column:create_time;type:datetime;not null;autoCreateTime;comment:创建时间"`
	UpdateTime        time.Time `gorm:"column:update_time;type:datetime;not null;autoUpdateTime;comment:更新时间"`
}

func (RaffleActivity) TableName() string {
	return "raffle_activity"
}

func (RaffleActivityCount) TableName() string {
	return "raffle_activity_count"
}

func (RaffleActivitySku) TableName() string {
	return "raffle_activity_sku"
}

func (RaffleActivityAccount) TableName() string {
	return "raffle_activity_account"
}

func (RaffleActivityOrder) TableName() string {
	return "raffle_activity_order"
}

func (RaffleActivityAccountDay) TableName() string {
	return "raffle_activity_account_day"
}

func (RaffleActivityAccountMonth) TableName() string {
	return "raffle_activity_account_month"
}

type UserRaffleOrder struct {
	ID           uint64    `gorm:"column:id;primaryKey;autoIncrement;comment:自增ID"`
	UserID       string    `gorm:"column:user_id;type:varchar(32);not null;comment:用户ID"`
	ActivityID   int64     `gorm:"column:activity_id;not null;comment:活动ID"`
	ActivityName string    `gorm:"column:activity_name;type:varchar(64);not null;comment:活动名称"`
	StrategyID   int64     `gorm:"column:strategy_id;not null;comment:抽奖策略ID"`
	OrderID      string    `gorm:"column:order_id;type:varchar(12);not null;comment:订单ID"`
	OrderTime    time.Time `gorm:"column:order_time;type:datetime;not null;comment:下单时间"`
	OrderState   string    `gorm:"column:order_state;type:varchar(16);not null;default:create;comment:订单状态；create-创建、used-已使用、cancel-已作废"`
	CreateTime   time.Time `gorm:"column:create_time;type:datetime;not null;autoCreateTime;comment:创建时间"`
	UpdateTime   time.Time `gorm:"column:update_time;type:datetime;not null;autoUpdateTime;comment:更新时间"`
}

func (UserRaffleOrder) TableName() string {
	return "user_raffle_order"
}

func (p *RaffleActivity) ToEntity() *activity.Activity {
	return &activity.Activity{
		ActivityID:    p.ActivityID,
		ActivityName:  p.ActivityName,
		ActivityDesc:  p.ActivityDesc,
		BeginDateTime: p.BeginDateTime,
		EndDateTime:   p.EndDateTime,
		StrategyID:    p.StrategyID,
		State:         activity.ActivityState(p.State),
	}
}

func (p *RaffleActivityCount) ToEntity() *activity.ActivityCount {
	return &activity.ActivityCount{
		ActivityCountID: p.ActivityCountID,
		TotalCount:      p.TotalCount,
		DayCount:        p.DayCount,
		MonthCount:      p.MonthCount,
	}
}

func (p *RaffleActivitySku) ToEntity() *activity.ActivitySku {
	return &activity.ActivitySku{
		Sku:               p.Sku,
		ActivityID:        p.ActivityID,
		ActivityCountID:   p.ActivityCountID,
		StockCount:        p.StockCount,
		StockCountSurplus: p.StockCountSurplus,
	}
}

func (p *RaffleActivityAccount) ToEntity() *activity.ActivityAccount {
	return &activity.ActivityAccount{
		UserID:            p.UserID,
		ActivityID:        p.ActivityID,
		TotalCount:        p.TotalCount,
		TotalCountSurplus: p.TotalCountSurplus,
		DayCount:          p.DayCount,
		DayCountSurplus:   p.DayCountSurplus,
		MonthCount:        p.MonthCount,
		MonthCountSurplus: p.MonthCountSurplus,
	}
}

func (p *RaffleActivityOrder) ToEntity() *activity.ActivityOrder {
	return &activity.ActivityOrder{
		UserID:        p.UserID,
		Sku:           p.Sku,
		ActivityID:    p.ActivityID,
		ActivityName:  p.ActivityName,
		StrategyID:    p.StrategyID,
		OrderID:       p.OrderID,
		OrderTime:     p.OrderTime,
		TotalCount:    p.TotalCount,
		DayCount:      p.DayCount,
		MonthCount:    p.MonthCount,
		State:         p.State,
		OutBusinessNo: p.OutBusinessNo,
	}
}

func (p *RaffleActivityAccountDay) ToEntity() *activity.ActivityAccountDay {
	return &activity.ActivityAccountDay{
		UserID:          p.UserID,
		ActivityID:      p.ActivityID,
		Day:             p.Day,
		DayCount:        p.DayCount,
		DayCountSurplus: p.DayCountSurplus,
	}
}

func (p *RaffleActivityAccountMonth) ToEntity() *activity.ActivityAccountMonth {
	return &activity.ActivityAccountMonth{
		UserID:            p.UserID,
		ActivityID:        p.ActivityID,
		Month:             p.Month,
		MonthCount:        p.MonthCount,
		MonthCountSurplus: p.MonthCountSurplus,
	}
}

func (p *UserRaffleOrder) ToEntity() *activity.UserRaffleOrder {
	return &activity.UserRaffleOrder{
		UserID:       p.UserID,
		ActivityID:   p.ActivityID,
		ActivityName: p.ActivityName,
		StrategyID:   p.StrategyID,
		OrderID:      p.OrderID,
		OrderTime:    p.OrderTime,
		OrderState:   activity.UserRaffleOrderState(p.OrderState),
	}
}
