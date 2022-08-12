package pulsar

import (
	"context"
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/smartwalle/mx"
	"sync"
	"time"
)

type Producer struct {
	mu       *sync.Mutex
	closed   bool
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
	p.mu = &sync.Mutex{}
	p.closed = false
	p.topic = topic
	p.config = config
	p.producer = producer
	return p, nil
}

func (this *Producer) Enqueue(data []byte) error {
	var m = NewProducerMessage()
	m.Payload = data
	return this.EnqueueMessage(m)
}

func (this *Producer) EnqueueMessage(m *pulsar.ProducerMessage) error {
	if m == nil {
		return nil
	}

	this.mu.Lock()
	defer this.mu.Unlock()

	if this.closed {
		return mx.ErrClosedQueue
	}

	_, err := this.producer.Send(context.Background(), m)
	return err
}

func (this *Producer) DeferredEnqueue(delay time.Duration, data []byte) error {
	var m = NewProducerMessage()
	m.Payload = data
	m.DeliverAfter = delay
	return this.EnqueueMessage(m)
}

func (this *Producer) MultiEnqueue(data ...[]byte) error {
	if len(data) == 0 {
		return nil
	}

	this.mu.Lock()
	defer this.mu.Unlock()

	if this.closed {
		return mx.ErrClosedQueue
	}

	for _, d := range data {
		var m = NewProducerMessage()
		m.Payload = d
		if _, err := this.producer.Send(context.Background(), m); err != nil {
			return err
		}
	}
	return nil
}

func (this *Producer) Close() error {
	this.mu.Lock()
	defer this.mu.Unlock()

	if this.closed {
		return nil
	}
	this.closed = true

	if this.producer != nil {
		this.producer.Close()
		this.producer = nil
	}

	if this.client != nil {
		this.client.Close()
		this.client = nil
	}

	return nil
}
