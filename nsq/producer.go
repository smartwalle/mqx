package nsq

import (
	"github.com/nsqio/go-nsq"
	"github.com/smartwalle/mx"
	"sync/atomic"
	"time"
)

type Producer struct {
	closed   int32
	topic    string
	config   *Config
	producer *nsq.Producer
}

func NewProducer(topic string, config *Config) (*Producer, error) {
	producer, err := nsq.NewProducer(config.NSQAddr, config.Config)
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

func (p *Producer) SetLogger(l Logger, lv nsq.LogLevel) {
	p.producer.SetLogger(l, lv)
}

func (p *Producer) Enqueue(data []byte) error {
	if atomic.LoadInt32(&p.closed) == 1 {
		return mx.ErrClosedQueue
	}
	return p.producer.Publish(p.topic, data)
}

func (p *Producer) DeferredEnqueue(delay time.Duration, data []byte) error {
	if atomic.LoadInt32(&p.closed) == 1 {
		return mx.ErrClosedQueue
	}
	return p.producer.DeferredPublish(p.topic, delay, data)
}

func (p *Producer) MultiEnqueue(data ...[]byte) error {
	if len(data) == 0 {
		return nil
	}

	if atomic.LoadInt32(&p.closed) == 1 {
		return mx.ErrClosedQueue
	}
	return p.producer.MultiPublish(p.topic, data)
}

func (p *Producer) Close() error {
	if !atomic.CompareAndSwapInt32(&p.closed, 0, 1) {
		return nil
	}

	if p.producer != nil {
		p.producer.Stop()
		p.producer = nil
	}

	return nil
}
