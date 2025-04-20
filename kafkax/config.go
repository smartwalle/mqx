package kafkax

import (
	"errors"
	"github.com/segmentio/kafka-go"
	"net"
)

var (
	ErrQueueClosed = errors.New("queue closed")
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
