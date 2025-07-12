package nsqx

import (
	"errors"
	"github.com/nsqio/go-nsq"
	"time"
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
	*nsq.Config
	NSQAddr        string
	NSQLookupAddrs []string
}

func NewConfig() *Config {
	var c = &Config{}
	c.Config = nsq.NewConfig()
	c.NSQAddr = "127.0.0.1:4150"
	c.NSQLookupAddrs = []string{"127.0.0.1:4161"}
	c.LookupdPollInterval = time.Second * 10
	return c
}
