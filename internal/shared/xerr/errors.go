package xerr

import "fmt"

// CodeError 实现了 error 接口，但带了 Code
type CodeError struct {
	errCode ErrCode
	errMsg  string
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

// WithCause wraps the original error as cause.
func (e *CodeError) WithCause(cause error) *CodeError {
	if cause != nil {
		e.errMsg = fmt.Sprintf("%s: %v", e.errMsg, cause)
	}
	return e
}

// WithMetadata appends metadata to the error message.
func (e *CodeError) WithMetadata(md map[string]string) *CodeError {
	for k, v := range md {
		e.errMsg = fmt.Sprintf("%s [%s=%s]", e.errMsg, k, v)
	}
	return e
}
