package kafkax

import (
	"errors"
	"github.com/segmentio/kafka-go"
	"net"
)

const (
	kStateIdle     = 0
	kStateRunning  = 1
	kStateFinished = 2
)

var (
	ErrQueueRunning = errors.New("queue running")
	ErrQueueClosed  = errors.New("queue closed")
	ErrBadQueue     = errors.New("bad queue")
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
