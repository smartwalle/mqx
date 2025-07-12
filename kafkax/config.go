package kafkax

import (
	"errors"
	"github.com/segmentio/kafka-go"
	"net"
)

type State int32

const (
	StateIdle     State = 0
	StateRunning  State = 1
	StateShutdown State = 2
)

var (
	ErrQueueRunning    = errors.New("queue running")
	ErrQueueClosed     = errors.New("queue closed")
	ErrBadQueue        = errors.New("bad queue")
	ErrHandlerNotFound = errors.New("message handler not found")
)

type Config struct {
	Reader kafka.ReaderConfig
	Writer kafka.Writer
}

func TCP(address ...string) net.Addr {
	return kafka.TCP(address...)
}

func NewConfig() *Config {
	return &Config{}
}
