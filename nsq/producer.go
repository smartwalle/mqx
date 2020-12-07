package nsq

import (
	"github.com/nsqio/go-nsq"
	"github.com/smartwalle/mx"
	"sync"
)

type Producer struct {
	mu       *sync.Mutex
	closed   bool
	config   *Config
	producer *nsq.Producer
}

func NewProducer(config *Config) (*Producer, error) {
	producer, err := nsq.NewProducer(config.NSQAddr, config.Config)
	if err != nil {
		return nil, err
	}

	var p = &Producer{}
	p.mu = &sync.Mutex{}
	p.closed = false
	p.config = config
	p.producer = producer
	return p, nil
}

func (this *Producer) Enqueue(topic string, data []byte) error {
	this.mu.Lock()
	defer this.mu.Unlock()

	if this.closed {
		return mx.ErrClosedQueue
	}
	return this.producer.Publish(topic, data)
}

func (this *Producer) Close() error {
	this.mu.Lock()
	defer this.mu.Unlock()

	if this.closed {
		return nil
	}
	this.closed = true

	if this.producer != nil {
		this.producer.Stop()
		this.producer = nil
	}

	return nil
}
