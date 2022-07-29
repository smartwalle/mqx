package mx

import (
	"errors"
)

var (
	ErrClosedQueue = errors.New("closed queue")
)

type Handler func(m Message) bool

type Queue interface {
	Enqueue(data []byte) error

	MultiEnqueue(data ...[]byte) error

	Dequeue(group string, handler Handler) error

	Close() error
}

type Producer interface {
	Enqueue(data []byte) error

	MultiEnqueue(data ...[]byte) error

	Close() error
}

type Consumer interface {
	Dequeue(handler Handler) error

	Close() error
}
