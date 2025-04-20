package pulsarx

import (
	"context"
	"github.com/apache/pulsar-client-go/pulsar"
	"sync/atomic"
)

type Message = pulsar.Message

//pulsar.ProducerMessage

type Handler func(ctx context.Context, message Message) bool

type Consumer struct {
	topic            string
	subscriptionName string
	config           *Config
	handler          Handler
	client           pulsar.Client
	consumer         pulsar.Consumer
	inShutdown       atomic.Bool
}

func NewConsumer(topic, subscriptionName string, config *Config, handler Handler) *Consumer {
	var c = &Consumer{}
	c.topic = topic
	c.subscriptionName = subscriptionName
	c.config = config
	c.handler = handler
	return c
}

func (c *Consumer) Start(ctx context.Context) error {
	if c.inShutdown.Load() {
		return ErrClosedQueue
	}

	client, err := pulsar.NewClient(c.config.ClientOptions)
	if err != nil {
		return err
	}
	c.client = client

	c.config.ConsumerOptions.Topic = c.topic
	c.config.ConsumerOptions.SubscriptionName = c.subscriptionName

	consumer, err := c.client.Subscribe(c.config.ConsumerOptions)
	if err != nil {
		return err
	}
	c.consumer = consumer

	for {
		message, err := consumer.Receive(ctx)
		if err != nil {
			return err
		}

		if c.handler(ctx, message) {
			consumer.Ack(message)
		} else {
			consumer.Nack(message)
		}
	}
}

func (c *Consumer) Stop(ctx context.Context) error {
	if !c.inShutdown.CompareAndSwap(false, true) {
		return nil
	}

	if c.consumer != nil {
		c.consumer.Close()
	}

	if c.client != nil {
		c.client.Close()
	}
	return nil
}
