package pulsar

import (
	"context"
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/smartwalle/mx"
	"sync/atomic"
	"time"
)

type Producer struct {
	closed   int32
	topic    string
	config   *Config
	client   pulsar.Client
	producer pulsar.Producer
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

func (p *Producer) Enqueue(data []byte) error {
	var m = NewProducerMessage()
	m.Payload = data
	return p.EnqueueMessage(m)
}

func (p *Producer) EnqueueMessage(m *pulsar.ProducerMessage) error {
	if m == nil {
		return nil
	}

	if atomic.LoadInt32(&p.closed) == 1 {
		return mx.ErrClosedQueue
	}

	_, err := p.producer.Send(context.Background(), m)
	return err
}

func (p *Producer) DeferredEnqueue(delay time.Duration, data []byte) error {
	var m = NewProducerMessage()
	m.Payload = data
	m.DeliverAfter = delay
	return p.EnqueueMessage(m)
}

func (p *Producer) MultiEnqueue(data ...[]byte) error {
	if len(data) == 0 {
		return nil
	}

	if atomic.LoadInt32(&p.closed) == 1 {
		return mx.ErrClosedQueue
	}

	for _, d := range data {
		var m = NewProducerMessage()
		m.Payload = d
		if _, err := p.producer.Send(context.Background(), m); err != nil {
			return err
		}
	}
	return nil
}

func (p *Producer) Close() error {
	if !atomic.CompareAndSwapInt32(&p.closed, 0, 1) {
		return nil
	}

	if p.producer != nil {
		p.producer.Close()
		p.producer = nil
	}

	if p.client != nil {
		p.client.Close()
		p.client = nil
	}

	return nil
}
