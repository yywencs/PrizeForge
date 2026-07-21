package xerr

import (
	"fmt"
	"sort"
)

// CodeError 实现了 error 接口，但带了 Code
type CodeError struct {
	errCode ErrCode
	errMsg  string
	cause   error
}

// 必须实现 Error() string 方法
func (e *CodeError) Error() string {
	return fmt.Sprintf("ErrCode:%s, ErrMsg:%s", e.errCode, e.errMsg)
}

func (e *CodeError) GetErrCode() ErrCode {
	return e.errCode
}

func (e *CodeError) GetErrMsg() string {
	return e.errMsg
}

// Is 根据错误码判断两个 CodeError 是否属于同一类错误，
// 使派生错误仍能通过 errors.Is 匹配原始哨兵错误。
func (e *CodeError) Is(target error) bool {
	targetError, ok := target.(*CodeError)
	return ok && e != nil && targetError != nil && e.errCode == targetError.errCode
}

// Unwrap 返回底层原始错误，使 errors.Is 和 errors.As 可以继续沿错误链判断。
func (e *CodeError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.cause
}

// 工厂方法：创建一个新的 CodeError
func New(errCode ErrCode, errMsg string) *CodeError {
	return &CodeError{errCode: errCode, errMsg: errMsg}
}

func NewErrCodeMsg(errCode ErrCode, errMsg string) *CodeError {
	return &CodeError{errCode: errCode, errMsg: errMsg}
}

func NewErrCode(errCode ErrCode) *CodeError {
	return &CodeError{errCode: errCode, errMsg: MapErrMsg(errCode)}
}

// WithCause 返回包含原始错误的新副本，不修改当前 CodeError。
func (e *CodeError) WithCause(cause error) *CodeError {
	derived := *e
	if cause != nil {
		derived.errMsg = fmt.Sprintf("%s: %v", e.errMsg, cause)
		derived.cause = cause
	}
	return &derived
}

// WithMetadata 返回追加元数据后的新副本，不修改当前 CodeError。
func (e *CodeError) WithMetadata(md map[string]string) *CodeError {
	derived := *e
	keys := make([]string, 0, len(md))
	for key := range md {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		derived.errMsg = fmt.Sprintf("%s [%s=%s]", derived.errMsg, key, md[key])
	}
	return &derived
}
