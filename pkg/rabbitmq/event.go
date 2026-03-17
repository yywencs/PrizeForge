package rabbitmq

import (
	"time"

	"github.com/google/uuid"
)

// BaseEvent 标准的 MQ 信封
type BaseEvent struct {
	ID        string      `json:"id"`
	Timestamp time.Time   `json:"timestamp"`
	Data      interface{} `json:"data"`
}

func NewBaseEvent(data interface{}) *BaseEvent {
	return &BaseEvent{
		ID:        uuid.New().String(), // 自动生成防重 ID
		Timestamp: time.Now(),
		Data:      data,
	}
}
