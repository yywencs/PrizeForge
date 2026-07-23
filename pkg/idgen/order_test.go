package idgen

import (
	"encoding/hex"
	"testing"

	"github.com/google/uuid"
)

// TestNewOrderIDGeneratesUniqueUUIDv7 验证订单号是 32 位 UUIDv7，
// 并且连续生成的订单号不会重复。
func TestNewOrderIDGeneratesUniqueUUIDv7(t *testing.T) {
	const count = 10000
	seen := make(map[string]struct{}, count)

	for range count {
		orderID, err := NewOrderID()
		if err != nil {
			t.Fatalf("NewOrderID() error = %v, want nil", err)
		}
		if len(orderID) != OrderIDLength {
			t.Fatalf("NewOrderID() length = %d, want %d: %q", len(orderID), OrderIDLength, orderID)
		}
		raw, err := hex.DecodeString(orderID)
		if err != nil {
			t.Fatalf("NewOrderID() = %q, want hexadecimal UUID: %v", orderID, err)
		}
		parsed, err := uuid.FromBytes(raw)
		if err != nil {
			t.Fatalf("uuid.FromBytes() error = %v", err)
		}
		if parsed.Version() != 7 {
			t.Fatalf("NewOrderID() version = %d, want 7", parsed.Version())
		}
		if _, exists := seen[orderID]; exists {
			t.Fatalf("NewOrderID() generated duplicate %q", orderID)
		}
		seen[orderID] = struct{}{}
	}
}
