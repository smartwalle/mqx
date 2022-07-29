package pulsar

import (
	"context"
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/smartwalle/mx"
	"sync"
)

type Consumer struct {
	mu       *sync.Mutex
	closed   bool
	topic    string
	group    string
	config   *Config
	client   pulsar.Client
	consumer pulsar.Consumer
}

func NewConsumer(topic, group string, config *Config) (*Consumer, error) {
	var c = &Consumer{}
	c.mu = &sync.Mutex{}
	c.closed = false
	c.topic = topic
	c.group = group
	c.config = config
	return c, nil
}

func (this *Consumer) Dequeue(handler mx.Handler) error {
	this.mu.Lock()
	defer this.mu.Unlock()
	if this.closed {
		return mx.ErrClosedQueue
	}

	if this.consumer != nil {
		this.consumer.Close()
		this.consumer = nil
	}

	if this.client == nil {
		client, err := pulsar.NewClient(this.config.ClientOptions)
		if err != nil {
			return err
		}
		this.client = client
	}

	this.config.ConsumerOptions.Topic = this.topic
	this.config.ConsumerOptions.SubscriptionName = this.group

	consumer, err := this.client.Subscribe(this.config.ConsumerOptions)
	if err != nil {
		return err
	}
	this.consumer = consumer

	go func() {
		for {
			message, err := this.consumer.Receive(context.Background())
			if err != nil {
				return
			}
			var m = &Message{}
			m.m = message
			if handler(m) {
				this.consumer.Ack(message)
			} else {
				this.consumer.Nack(message)
			}
		}
	}()
	return nil
}

func (this *Consumer) Close() error {
	this.mu.Lock()
	defer this.mu.Unlock()

	if this.closed {
		return nil
	}
	this.closed = true

	if this.consumer != nil {
		this.consumer.Close()
		this.consumer = nil
	}

	if this.client != nil {
		this.client.Close()
		this.client = nil
	}

	return nil
}
