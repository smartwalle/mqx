package kafkax

import (
	"context"
	"github.com/segmentio/kafka-go"
	"sync/atomic"
)

type Message = kafka.Message

type MessageHandler func(ctx context.Context, message Message) bool

type Consumer struct {
	topic   string
	group   string
	config  *Config
	handler MessageHandler
	reader  *kafka.Reader
	state   atomic.Int32
}

func NewConsumer(topic, group string, config *Config) *Consumer {
	var c = &Consumer{}
	c.topic = topic
	c.group = group
	c.config = config
	return c
}

func (c *Consumer) OnMessage(handler MessageHandler) {
	c.handler = handler
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

	if c.handler == nil {
		return ErrHandlerNotFound
	}

	c.config.Reader.Topic = c.topic
	c.config.Reader.GroupID = c.group

	var reader = kafka.NewReader(c.config.Reader)
	c.reader = reader

	for {
		message, err := reader.FetchMessage(ctx)
		if err != nil {
			return err
		}

		if c.handler(ctx, message) {
			if err = reader.CommitMessages(ctx, message); err != nil {
				return err
			}
		}
	}
}

func (c *Consumer) State() State {
	return State(c.state.Load())
}

func (c *Consumer) Stop(ctx context.Context) error {
	if !c.state.CompareAndSwap(int32(StateRunning), int32(StateShutdown)) {
		return nil
	}

	if c.reader != nil {
		if err := c.reader.Close(); err != nil {
			return err
		}
	}

	return nil
}
