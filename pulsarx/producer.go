package pulsarx

import (
	"context"
	"github.com/apache/pulsar-client-go/pulsar"
	"sync/atomic"
	"time"
)

type Producer struct {
	closed     int32
	topic      string
	config     *Config
	client     pulsar.Client
	producer   pulsar.Producer
	inShutdown atomic.Bool
}

func NewProducer(topic string, config *Config) (*Producer, error) {
	client, err := pulsar.NewClient(config.ClientOptions)
	if err != nil {
		return nil, err
	}
	config.ProducerOptions.Topic = topic
	producer, err := client.CreateProducer(config.ProducerOptions)
	if err != nil {
		return nil, err
	}

	var p = &Producer{}
	p.closed = 0
	p.topic = topic
	p.config = config
	p.producer = producer
	return p, nil
}

func (p *Producer) Enqueue(ctx context.Context, data []byte) error {
	var message = &pulsar.ProducerMessage{}
	message.Payload = data
	return p.EnqueueMessage(ctx, message)
}

func (p *Producer) EnqueueMessage(ctx context.Context, message *pulsar.ProducerMessage) error {
	if p.inShutdown.Load() {
		return ErrQueueClosed
	}
	_, err := p.producer.Send(ctx, message)
	return err
}

func (p *Producer) DeferredEnqueue(ctx context.Context, delay time.Duration, data []byte) error {
	if p.inShutdown.Load() {
		return ErrQueueClosed
	}

	var message = &pulsar.ProducerMessage{}
	message.Payload = data
	message.DeliverAfter = delay
	return p.EnqueueMessage(ctx, message)
}

func (p *Producer) Close() error {
	if !atomic.CompareAndSwapInt32(&p.closed, 0, 1) {
		return nil
	}

	if p.producer != nil {
		p.producer.Flush()
		p.producer.Close()
	}

	if p.client != nil {
		p.client.Close()
	}

	return nil
}
