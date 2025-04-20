package pulsarx

import (
	"context"
	"github.com/apache/pulsar-client-go/pulsar"
	"sync/atomic"
)

type Message = pulsar.Message

type Handler func(ctx context.Context, message Message) bool

type Consumer struct {
	topic            string
	subscriptionName string
	config           *Config
	handler          Handler
	client           pulsar.Client
	consumer         pulsar.Consumer
	state            atomic.Int32
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
	//if !c.state.CompareAndSwap(kStateRunning, kStateFinished) {
	//	return nil
	//}
	c.state.Store(kStateFinished)

	if c.consumer != nil {
		c.consumer.Close()
	}

	if c.client != nil {
		c.client.Close()
	}
	return nil
}
