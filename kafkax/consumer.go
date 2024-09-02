package kafkax

import (
	"context"
	"github.com/segmentio/kafka-go"
	"github.com/smartwalle/mx"
	"sync"
)

type Consumer struct {
	closed bool
	mu     *sync.Mutex
	topic  string
	group  string
	config *Config
	reader *kafka.Reader
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

func (c *Consumer) Dequeue(handler mx.Handler) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return mx.ErrClosedQueue
	}

	if c.reader != nil {
		c.reader.Close()
		c.reader = nil
	}

	c.config.Reader.Topic = c.topic
	c.config.Reader.GroupID = c.group

	var reader = kafka.NewReader(c.config.Reader)
	c.reader = reader

	go func() {
		for {
			message, err := reader.FetchMessage(context.Background())
			if err != nil {
				return
			}

			var m = &Message{}
			m.m = message
			if handler(m) {
				reader.CommitMessages(context.Background(), message)
			}
		}
	}()

	return nil
}

func (c *Consumer) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return mx.ErrClosedQueue
	}

	c.closed = true

	if c.reader != nil {
		if err := c.reader.Close(); err != nil {
			return err
		}
		c.reader = nil
	}

	return nil
}
