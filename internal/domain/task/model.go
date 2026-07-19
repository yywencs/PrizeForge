package task

import "time"

// TaskTypeStrategyAwardStockConsume 定义策略奖品库存消费任务类型
const TaskTypeStrategyAwardStockConsume = "strategy:award_stock_consume"

type Task struct {
	ID         uint64
	UserID     string
	Topic      string
	MessageID  string
	Message    string
	State      string
	CreateTime time.Time
	UpdateTime time.Time
}

type AwardStockConsumeMessage struct {
	StrategyID int64  `json:"strategy_id"`
	AwardID    int64  `json:"award_id"`
	OrderID    string `json:"order_id,omitempty"`
	UserID     string `json:"user_id,omitempty"`
}
