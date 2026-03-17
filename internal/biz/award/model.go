package award

import "time"

// SendAwardMessage 奖品消息载体
type SendAwardMessage struct {
	// UserID 用户ID
	UserID string `json:"user_id"`
	// AwardID 奖品ID
	AwardID int `json:"award_id"`
	// AwardTitle 奖品标题（名称）
	AwardTitle string `json:"award_title"`
}

// 用户中奖记录实体
type UserAwardRecord struct {
	// 用户ID
	UserID string
	// 活动ID
	ActivityID int64
	// 抽奖策略ID
	StrategyID int64
	// 抽奖订单ID【作为幂等使用】
	OrderID string
	// 奖品ID
	AwardID int
	// 奖品标题（名称）
	AwardTitle string
	// 中奖时间
	AwardTime time.Time
	// 奖品状态；create-创建、completed-发奖完成
	AwardState AwardState
}

// 任务实体
type Task struct {
	// 用户ID
	UserID string
	// 消息主题
	Topic string
	// 消息编号
	MessageID string
	// 消息主体
	Message SendAwardMessage
	// 任务状态；create-创建、completed-完成、fail-失败
	State TaskState
}

type UserAwardTaskInfo struct {
	UserAwardRecord *UserAwardRecord
	Task            *Task
}

// AwardState 奖项状态值对象
type AwardState string

const (
	AwardStateCreate   AwardState = "create"
	AwardStateComplete AwardState = "complete"
	AwardStateFail     AwardState = "fail"
)

func (s AwardState) Desc() string {
	switch s {
	case AwardStateCreate:
		return "创建"
	case AwardStateComplete:
		return "发奖完成"
	case AwardStateFail:
		return "发奖失败"
	default:
		return "未知"
	}
}

// TaskState 任务状态值对象
type TaskState string

const (
	TaskStateCreate   TaskState = "create"
	TaskStateComplete TaskState = "complete"
	TaskStateFail     TaskState = "fail"
)

func (s TaskState) Desc() string {
	switch s {
	case TaskStateCreate:
		return "创建"
	case TaskStateComplete:
		return "发送完成"
	case TaskStateFail:
		return "发送失败"
	default:
		return "未知"
	}
}
