package pulsar

import (
	"context"
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/smartwalle/mx"
	"sync"
)

type Consumer struct {
	closed   bool
	mu       *sync.Mutex
	topic    string
	group    string
	config   *Config
	client   pulsar.Client
	consumer pulsar.Consumer
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

	if c.consumer != nil {
		c.consumer.Close()
		c.consumer = nil
	}

	if c.client == nil {
		client, err := pulsar.NewClient(c.config.ClientOptions)
		if err != nil {
			return err
		}
		c.client = client
	}

	c.config.ConsumerOptions.Topic = c.topic
	c.config.ConsumerOptions.SubscriptionName = c.group

	consumer, err := c.client.Subscribe(c.config.ConsumerOptions)
	if err != nil {
		return err
	}
	c.consumer = consumer

	go func() {
		for {
			message, err := consumer.Receive(context.Background())
			if err != nil {
				return
			}
			var m = &Message{}
			m.m = message
			if handler(m) {
				consumer.Ack(message)
			} else {
				consumer.Nack(message)
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

	if c.consumer != nil {
		c.consumer.Close()
		c.consumer = nil
	}

	if c.client != nil {
		c.client.Close()
		c.client = nil
	}

	return nil
}
