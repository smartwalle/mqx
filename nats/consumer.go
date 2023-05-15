package nats

import (
	n "github.com/nats-io/nats.go"
	"github.com/smartwalle/mx"
	"sync/atomic"
)

type Consumer struct {
	closed int32
	topic  string
	group  string
	config *Config
	conn   *n.Conn
	sub    *n.Subscription
}

func NewConsumer(topic, group string, config *Config) (*Consumer, error) {
	conn, err := config.Connect()
	if err != nil {
		return nil, err
	}
	if err = conn.Flush(); err != nil {
		return nil, err
	}
	if err = conn.LastError(); err != nil {
		return nil, err
	}

	var c = &Consumer{}
	c.closed = 0
	c.topic = topic
	c.group = group
	c.config = config
	c.conn = conn
	return c, nil
}

func (this *Consumer) Dequeue(handler mx.Handler) error {
	if atomic.LoadInt32(&this.closed) == 1 {
		return mx.ErrClosedQueue
	}

	if this.sub != nil {
		this.sub.Unsubscribe()
	}

	sub, err := this.conn.QueueSubscribe(this.topic, this.group, func(msg *n.Msg) {
		var m = &Message{}
		m.m = msg
		handler(m)
	})
	if err != nil {
		return err
	}
	this.sub = sub

	return nil
}

func (this *Consumer) Close() error {
	if !atomic.CompareAndSwapInt32(&this.closed, 0, 1) {
		return nil
	}

	if this.sub != nil {
		this.sub.Unsubscribe()
		this.sub = nil
	}

	if this.conn != nil {
		this.conn.Close()
		this.conn = nil
	}
	return nil
}
