package po

import (
	"big-market-kratos/internal/biz/task"
	"time"
)

// Task 任务表
type Task struct {
	// ID 自增ID
	ID uint64 `gorm:"column:id;primaryKey;autoIncrement;comment:自增ID"`

	// UserID 用户ID
	UserID string `gorm:"column:user_id;type:varchar(32);not null;comment:用户ID"`

	// Topic 消息主题
	Topic string `gorm:"column:topic;type:varchar(32);not null;comment:消息主题"`

	// MessageID 消息编号
	MessageID string `gorm:"column:message_id;type:varchar(64);not null;comment:消息编号" json:"message_id"`

	// Message 消息主体
	Message string `gorm:"column:message;type:varchar(512);not null;comment:消息主体"`

	// State 任务状态；create-创建、completed-完成、fail-失败
	State string `gorm:"column:state;type:varchar(16);not null;default:create;comment:任务状态；create-创建、completed-完成、fail-失败"`

	// CreateTime 创建时间
	CreateTime time.Time `gorm:"column:create_time;type:datetime;not null;autoCreateTime;comment:创建时间"`

	// UpdateTime 更新时间
	UpdateTime time.Time `gorm:"column:update_time;type:datetime;not null;autoUpdateTime;comment:更新时间"`
}

// TableName 指定表名
func (Task) TableName() string {
	return "task"
}

func (p *Task) ToEntity() *task.Task {
	return &task.Task{
		ID:         p.ID,
		UserID:     p.UserID,
		Topic:      p.Topic,
		MessageID:  p.MessageID,
		Message:    p.Message,
		State:      p.State,
		CreateTime: p.CreateTime,
		UpdateTime: p.UpdateTime,
	}
}
