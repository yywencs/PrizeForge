package xerr

import (
	"errors"
	"testing"
)

// TestWithCauseDoesNotMutateSentinel 验证为全局哨兵错误追加不同 cause 时，
// 每次都会生成独立错误，原始消息不会跨请求累积。
func TestWithCauseDoesNotMutateSentinel(t *testing.T) {
	sentinel := New("TEST_ERROR", "操作失败")
	firstCause := errors.New("first cause")
	secondCause := errors.New("second cause")

	first := sentinel.WithCause(firstCause)
	second := sentinel.WithCause(secondCause)

	if got := sentinel.GetErrMsg(); got != "操作失败" {
		t.Fatalf("sentinel message = %q, want %q", got, "操作失败")
	}
	if got := first.GetErrMsg(); got != "操作失败: first cause" {
		t.Fatalf("first message = %q, want %q", got, "操作失败: first cause")
	}
	if got := second.GetErrMsg(); got != "操作失败: second cause" {
		t.Fatalf("second message = %q, want %q", got, "操作失败: second cause")
	}
	if first == sentinel || second == sentinel || first == second {
		t.Fatal("WithCause() reused a mutable CodeError instance")
	}
	if !errors.Is(first, sentinel) {
		t.Fatal("derived error does not match its sentinel")
	}
	if !errors.Is(first, firstCause) {
		t.Fatal("derived error does not unwrap to its cause")
	}
}

// TestWithMetadataDoesNotMutateSentinel 验证 metadata 按稳定顺序写入错误副本，
// 且不会污染后续从同一个哨兵派生的错误。
func TestWithMetadataDoesNotMutateSentinel(t *testing.T) {
	sentinel := New("INVALID_CONFIG", "配置无效")

	first := sentinel.WithMetadata(map[string]string{
		"user_id":     "user-1",
		"activity_id": "100301",
	})
	second := sentinel.WithMetadata(map[string]string{"strategy_id": "100006"})

	if got := sentinel.GetErrMsg(); got != "配置无效" {
		t.Fatalf("sentinel message = %q, want %q", got, "配置无效")
	}
	if got := first.GetErrMsg(); got != "配置无效 [activity_id=100301] [user_id=user-1]" {
		t.Fatalf("first message = %q", got)
	}
	if got := second.GetErrMsg(); got != "配置无效 [strategy_id=100006]" {
		t.Fatalf("second message = %q", got)
	}
	if !errors.Is(first, sentinel) || !errors.Is(second, sentinel) {
		t.Fatal("metadata-derived error does not match its sentinel")
	}
}

// TestCodeErrorDerivationKeepsCause 验证追加 metadata 后仍保留底层 cause，
// 支持调用方通过 errors.Is 判断原始错误。
func TestCodeErrorDerivationKeepsCause(t *testing.T) {
	sentinel := New("QUERY_FAILED", "查询失败")
	cause := errors.New("database unavailable")

	derived := sentinel.WithCause(cause).WithMetadata(map[string]string{"shard": "02"})

	if got := derived.GetErrMsg(); got != "查询失败: database unavailable [shard=02]" {
		t.Fatalf("derived message = %q", got)
	}
	if !errors.Is(derived, sentinel) {
		t.Fatal("derived error does not match its sentinel")
	}
	if !errors.Is(derived, cause) {
		t.Fatal("derived error does not preserve its cause")
	}
}
