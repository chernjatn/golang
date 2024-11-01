package errs

import "errors"

var (
    DefaultError = errors.New("default error")
    NotFountError = errors.New("not found error")
    WrongPrice = errors.New("wrong price")
)
