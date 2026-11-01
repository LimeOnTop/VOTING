// Package apperr defines transport-independent application error categories.
package apperr

import "errors"

var (
	ErrAlreadyVoted    = errors.New("вы уже проголосовали в этом опросе")
	ErrVotePending     = errors.New("предыдущая отправка ещё выполняется; повторите запрос позже")
	ErrNotFound        = errors.New("опрос не найден")
	ErrInvalidIdentity = errors.New("сначала разрешите cookie и откройте страницу голосования")
	ErrOverloaded      = errors.New("сервис занят; повторите запрос позже")
	ErrUnavailable     = errors.New("сервис временно недоступен")
)

type Validation struct{ Cause error }

func (e *Validation) Error() string { return e.Cause.Error() }
func (e *Validation) Unwrap() error { return e.Cause }
