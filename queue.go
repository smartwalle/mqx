package mx

import (
	"errors"
)

var (
	ErrClosedQueue = errors.New("qx: closed queue")
)

type Handler func(m Message) bool

type Queue interface {
	Enqueue([]byte) error

	Dequeue(Handler) error

	Close() error
}
