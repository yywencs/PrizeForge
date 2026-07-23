package idgen

import (
	"encoding/hex"
	"fmt"

	"github.com/google/uuid"
)

const OrderIDLength = 32

// NewOrderID 生成不带连字符的 UUIDv7 订单号。
//
// UUIDv7 的高位包含时间信息，既避免原 12 位随机数在历史数据增长后发生碰撞，
// 也比完全随机的 UUID 更有利于数据库索引的局部性。
func NewOrderID() (string, error) {
	id, err := uuid.NewV7()
	if err != nil {
		return "", fmt.Errorf("生成 UUIDv7 订单号: %w", err)
	}
	return hex.EncodeToString(id[:]), nil
}
