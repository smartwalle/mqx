package nsq

import (
	"errors"
	"github.com/nsqio/go-nsq"
	"github.com/smartwalle/mx"
	"sync/atomic"
)

type Consumer struct {
	closed   int32
	topic    string
	group    string
	config   *Config
	consumer *nsq.Consumer
	logger   Logger
	logLevel nsq.LogLevel
}

func NewConsumer(topic, group string, config *Config) (*Consumer, error) {
	var c = &Consumer{}
	c.closed = 0
	c.topic = topic
	c.group = group
	c.config = config
	return c, nil
}

func (this *Consumer) SetLogger(l Logger, lv nsq.LogLevel) {
	this.logger = l
	this.logLevel = lv
}

func (this *Consumer) Dequeue(handler mx.Handler) error {
	if atomic.LoadInt32(&this.closed) == 1 {
		return mx.ErrClosedQueue
	}

	if this.consumer != nil {
		this.consumer.Stop()
		<-this.consumer.StopChan
		this.consumer = nil
	}

	if this.consumer == nil {
		consumer, err := nsq.NewConsumer(this.topic, this.group, this.config.Config)
		if err != nil {
			return err
		}
		this.consumer = consumer
		this.consumer.SetLogger(this.logger, this.logLevel)
	}

	this.consumer.AddHandler(nsq.HandlerFunc(func(message *nsq.Message) error {
		var m = &Message{}
		m.m = message
		m.topic = this.topic
		if handler(m) {
			return nil
		}
		return errors.New("consume message failed")
	}))

	if err := this.consumer.ConnectToNSQLookupds(this.config.NSQLookupAddrs); err != nil {
		return err
	}
	return nil
}

func (this *Consumer) Close() error {
	if !atomic.CompareAndSwapInt32(&this.closed, 0, 1) {
		return nil
	}

	if this.consumer != nil {
		this.consumer.Stop()
		<-this.consumer.StopChan
		this.consumer = nil
	}
	return nil
}
