package nsq

import (
	"errors"
	"github.com/nsqio/go-nsq"
	"github.com/smartwalle/mx"
	"sync"
)

type Consumer struct {
	closed   bool
	mu       *sync.Mutex
	topic    string
	group    string
	config   *Config
	consumer *nsq.Consumer
	logger   Logger
	logLevel nsq.LogLevel
}

func NewConsumer(topic, group string, config *Config) (*Consumer, error) {
	var c = &Consumer{}
	c.closed = false
	c.mu = &sync.Mutex{}
	c.topic = topic
	c.group = group
	c.config = config
	return c, nil
}

func (c *Consumer) SetLogger(l Logger, lv nsq.LogLevel) {
	c.logger = l
	c.logLevel = lv
}

func (c *Consumer) Dequeue(handler mx.Handler) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return mx.ErrClosedQueue
	}

	if c.consumer != nil {
		c.consumer.Stop()
		<-c.consumer.StopChan
		c.consumer = nil
	}

	if c.consumer == nil {
		consumer, err := nsq.NewConsumer(c.topic, c.group, c.config.Config)
		if err != nil {
			return err
		}
		c.consumer = consumer
		c.consumer.SetLogger(c.logger, c.logLevel)
	}

	c.consumer.AddHandler(nsq.HandlerFunc(func(message *nsq.Message) error {
		var m = &Message{}
		m.m = message
		m.topic = c.topic
		if handler(m) {
			return nil
		}
		return errors.New("consume message failed")
	}))

	if err := c.consumer.ConnectToNSQLookupds(c.config.NSQLookupAddrs); err != nil {
		return err
	}
	return nil
}

func (c *Consumer) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return mx.ErrClosedQueue
	}

	c.closed = true

	if c.consumer != nil {
		c.consumer.Stop()
		<-c.consumer.StopChan
		c.consumer = nil
	}
	return nil
}
