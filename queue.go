package mx

import (
	"errors"
)

var (
	ErrClosedQueue = errors.New("qx: closed queue")
)

type Queue interface {
	Enqueue(value []byte) error

	Dequeue() (Message, error)

	Close() error
}
