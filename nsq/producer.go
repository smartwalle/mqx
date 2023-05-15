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

func (this *Producer) SetLogger(l Logger, lv nsq.LogLevel) {
	this.producer.SetLogger(l, lv)
}

func (this *Producer) Enqueue(data []byte) error {
	if atomic.LoadInt32(&this.closed) == 1 {
		return mx.ErrClosedQueue
	}
	return this.producer.Publish(this.topic, data)
}

func (this *Producer) DeferredEnqueue(delay time.Duration, data []byte) error {
	if atomic.LoadInt32(&this.closed) == 1 {
		return mx.ErrClosedQueue
	}
	return this.producer.DeferredPublish(this.topic, delay, data)
}

func (this *Producer) MultiEnqueue(data ...[]byte) error {
	if len(data) == 0 {
		return nil
	}

	if atomic.LoadInt32(&this.closed) == 1 {
		return mx.ErrClosedQueue
	}
	return this.producer.MultiPublish(this.topic, data)
}

func (this *Producer) Close() error {
	if !atomic.CompareAndSwapInt32(&this.closed, 0, 1) {
		return nil
	}

	if this.producer != nil {
		this.producer.Stop()
		this.producer = nil
	}

	return nil
}
