package rabbitmq

import (
	"crypto/rand"
	"encoding/hex"
	"time"
)

// BaseEvent 标准的 MQ 信封
type BaseEvent struct {
	ID        string      `json:"id"`
	Timestamp time.Time   `json:"timestamp"`
	Data      interface{} `json:"data"`
}

func generateShortID() string {
	bytes := make([]byte, 5) // 5 bytes = 10 hex characters
	if _, err := rand.Read(bytes); err != nil {
		return time.Now().Format("0601021504") // 兜底策略，使用时间格式（长度10）
	}
	return hex.EncodeToString(bytes)
}

func NewBaseEvent(data interface{}) *BaseEvent {
	return &BaseEvent{
		ID:        generateShortID(), // 自动生成较短的防重 ID（长度10）
		Timestamp: time.Now(),
		Data:      data,
	}
}
