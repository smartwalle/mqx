package kafkax

import (
	"context"
	"github.com/segmentio/kafka-go"
	"sync/atomic"
)

type Message = kafka.Message

type Handler func(ctx context.Context, message Message) bool

type Consumer struct {
	topic   string
	group   string
	config  *Config
	handler Handler
	reader  *kafka.Reader
	state   atomic.Int32
}

func NewConsumer(topic, group string, config *Config, handler Handler) *Consumer {
	var c = &Consumer{}
	c.topic = topic
	c.group = group
	c.config = config
	c.handler = handler
	return c
}

func (c *Consumer) Start(ctx context.Context) error {
	if !c.state.CompareAndSwap(kStateIdle, kStateRunning) {
		switch c.state.Load() {
		case kStateRunning:
			return ErrQueueRunning
		case kStateFinished:
			return ErrQueueClosed
		default:
			return ErrBadQueue
		}
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

func (c *Consumer) Stop(ctx context.Context) error {
	//if !c.state.CompareAndSwap(kStateRunning, kStateFinished) {
	//	return nil
	//}
	c.state.Store(kStateFinished)

	if c.reader != nil {
		if err := c.reader.Close(); err != nil {
			return err
		}
	}

	return nil
}
