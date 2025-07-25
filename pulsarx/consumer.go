package pulsarx

import (
	"context"
	"github.com/apache/pulsar-client-go/pulsar"
	"sync/atomic"
)

type Message = pulsar.Message

type MessageHandler func(ctx context.Context, message Message) bool

type Consumer struct {
	topic            string
	subscriptionName string
	config           *Config
	handler          MessageHandler
	client           pulsar.Client
	consumer         pulsar.Consumer
	state            atomic.Int32
}

func NewConsumer(topic, subscriptionName string, config *Config) *Consumer {
	var c = &Consumer{}
	c.topic = topic
	c.subscriptionName = subscriptionName
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

func (c *Consumer) State() State {
	return State(c.state.Load())
}

func (c *Consumer) Stop(ctx context.Context) error {
	if !c.state.CompareAndSwap(int32(StateRunning), int32(StateShutdown)) {
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
