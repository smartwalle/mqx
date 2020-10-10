package mx

import (
	"errors"
)

var (
	ErrClosedQueue = errors.New("mx: closed queue")
)

type Handler func(m Message) bool

type Queue interface {
	Enqueue(data []byte) error

	Dequeue(group string, handler Handler) error

	Close() error
}
