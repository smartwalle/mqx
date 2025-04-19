package kafkax

import (
	"errors"
	"github.com/segmentio/kafka-go"
	"net"
)

var (
	ErrClosedQueue = errors.New("closed queue")
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
