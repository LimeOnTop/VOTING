// Package apperr defines transport-independent application error categories.
package apperr

import "errors"

var (
	ErrNotFound        = errors.New("опрос не найден")
	ErrInvalidIdentity = errors.New("сначала разрешите cookie и откройте страницу голосования")
	ErrOverloaded      = errors.New("сервис занят; повторите запрос позже")
	ErrUnavailable     = errors.New("сервис временно недоступен")
)

type Validation struct{ Cause error }

func (e *Validation) Error() string { return e.Cause.Error() }
func (e *Validation) Unwrap() error { return e.Cause }
