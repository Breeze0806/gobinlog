package gobinlog

import (
	"bytes"
	"errors"
	"fmt"
)

//信息流到达EOF错误信息用于标识binlog流结束
var (
	errStreamEOF = errors.New("stream reached EOF") //信息流到达EOF
)

//Error gobinlog的错误
type Error struct {
	ori error
	msg string
}

func newError(ori error) *Error {
	return &Error{
		ori: ori,
	}
}

func (e *Error) msgf(format string, args ...interface{}) *Error {
	buf := bytes.NewBuffer(nil)
	buf.WriteString(fmt.Sprintf(format, args...))
	buf.WriteString(e.msg)
	e.msg = buf.String()
	return e
}

//Original 获取不带msg的原本的错误
func (e *Error) Original() error {
	return e.ori
}

//Error 获取详细错误信息
func (e *Error) Error() string {
	return fmt.Sprintf("%v oriErr: %v", e.msg, e.ori)
}
