package nats

import (
	n "github.com/nats-io/nats.go"
	"github.com/smartwalle/mx"
	"sync"
)

type Consumer struct {
	mu     *sync.Mutex
	closed bool
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
	c.mu = &sync.Mutex{}
	c.closed = false
	c.topic = topic
	c.group = group
	c.config = config
	c.conn = conn
	return c, nil
}

func (this *Consumer) Dequeue(handler mx.Handler) error {
	this.mu.Lock()
	defer this.mu.Unlock()
	if this.closed {
		return mx.ErrClosedQueue
	}

	if this.sub != nil {
		this.sub.Unsubscribe()
		this.sub.Drain()
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
	this.mu.Lock()
	defer this.mu.Unlock()

	if this.closed {
		return nil
	}
	this.closed = true

	if this.sub != nil {
		this.sub.Unsubscribe()
		this.sub.Drain()
		this.sub = nil
	}

	if this.conn != nil {
		this.conn.Close()
		this.conn = nil
	}
	return nil
}
