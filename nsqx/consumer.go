package nsqx

import (
	"context"
	"github.com/nsqio/go-nsq"
	"sync/atomic"
)

type Message = nsq.Message

type Handler func(ctx context.Context, message *Message) error

type Consumer struct {
	topic    string
	channel  string
	config   *Config
	handler  Handler
	consumer *nsq.Consumer
	logger   Logger
	logLevel nsq.LogLevel
	state    atomic.Int32
}

func NewConsumer(topic, channel string, config *Config, handler Handler) *Consumer {
	var c = &Consumer{}
	c.topic = topic
	c.channel = channel
	c.config = config
	c.handler = handler
	return c
}

func (c *Consumer) SetLogger(l Logger, lv nsq.LogLevel) {
	c.logger = l
	c.logLevel = lv
}

func (c *Consumer) Start(ctx context.Context) error {
	if !c.state.CompareAndSwap(int32(StateIdle), int32(StateRunning)) {
		switch State(c.state.Load()) {
		case StateRunning:
			return ErrQueueRunning
		case StateShutdown:
			return ErrQueueClosed
		default:
			return ErrBadQueue
		}
	}

	consumer, err := nsq.NewConsumer(c.topic, c.channel, c.config.Config)
	if err != nil {
		return err
	}
	c.consumer = consumer
	c.consumer.SetLogger(c.logger, c.logLevel)

	c.consumer.AddHandler(nsq.HandlerFunc(func(message *nsq.Message) error {
		return c.handler(ctx, message)
	}))

	if err = c.consumer.ConnectToNSQLookupds(c.config.NSQLookupAddrs); err != nil {
		return err
	}
	return nil
}

func (c *Consumer) State() State {
	return State(c.state.Load())
}

func (c *Consumer) Stop(ctx context.Context) error {
	if !c.state.CompareAndSwap(int32(StateRunning), int32(StateShutdown)) {
		return nil
	}

	if c.consumer != nil {
		c.consumer.Stop()
		<-c.consumer.StopChan
	}
	return nil
}
