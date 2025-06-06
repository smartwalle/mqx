package nsqx

import (
	"context"
	"github.com/nsqio/go-nsq"
	"sync/atomic"
	"time"
)

type Producer struct {
	topic    string
	config   *Config
	producer *nsq.Producer
	closed   atomic.Bool
}

func NewProducer(topic string, config *Config) (*Producer, error) {
	producer, err := nsq.NewProducer(config.NSQAddr, config.Config)
	if err != nil {
		return nil, err
	}

	var p = &Producer{}
	p.topic = topic
	p.config = config
	p.producer = producer
	return p, nil
}

func (p *Producer) SetLogger(l Logger, lv nsq.LogLevel) {
	p.producer.SetLogger(l, lv)
}

func (p *Producer) Enqueue(ctx context.Context, data []byte) error {
	if p.closed.Load() {
		return ErrQueueClosed
	}
	return p.producer.Publish(p.topic, data)
}

func (p *Producer) DeferredEnqueue(ctx context.Context, delay time.Duration, data []byte) error {
	if p.closed.Load() {
		return ErrQueueClosed
	}
	return p.producer.DeferredPublish(p.topic, delay, data)
}

func (p *Producer) MultiEnqueue(ctx context.Context, data ...[]byte) error {
	if len(data) == 0 {
		return nil
	}

	if p.closed.Load() {
		return ErrQueueClosed
	}
	return p.producer.MultiPublish(p.topic, data)
}

func (p *Producer) Close() error {
	//if !p.closed.CompareAndSwap(false, true) {
	//	return nil
	//}
	p.closed.Store(true)

	if p.producer != nil {
		p.producer.Stop()
	}

	return nil
}
