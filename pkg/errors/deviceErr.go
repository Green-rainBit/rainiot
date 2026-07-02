package errors

import "fmt"

type MyError struct {
	Code int
	Msg  string
}

func (e MyError) Error() string {
	return fmt.Sprintf("{\"code\":%d, \"msg\":\"%s\"}", e.Code, e.Msg)
}
func NewMyError(code int, msg string) error {
	return MyError{Code: code, Msg: msg}
}
