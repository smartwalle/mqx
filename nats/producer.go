package nats

import (
	n "github.com/nats-io/nats.go"
	"github.com/smartwalle/mx"
	"sync/atomic"
)

type Producer struct {
	closed int32
	topic  string
	config *Config
	conn   *n.Conn
}

func NewProducer(topic string, config *Config) (*Producer, error) {
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

	var p = &Producer{}
	p.closed = 0
	p.topic = topic
	p.config = config
	p.conn = conn
	return p, nil
}

func (this *Producer) Enqueue(data []byte) error {
	var m = &n.Msg{}
	m.Subject = this.topic
	m.Data = data
	return this.EnqueueMessage(m)
}

func (this *Producer) EnqueueMessage(m *n.Msg) error {
	if m == nil {
		return nil
	}

	if atomic.LoadInt32(&this.closed) == 1 {
		return mx.ErrClosedQueue
	}

	m.Subject = this.topic

	err := this.conn.PublishMsg(m)
	return err
}

func (this *Producer) MultiEnqueue(data ...[]byte) error {
	if len(data) == 0 {
		return nil
	}

	if atomic.LoadInt32(&this.closed) == 1 {
		return mx.ErrClosedQueue
	}

	for _, d := range data {
		var m = &n.Msg{}
		m.Subject = this.topic
		m.Data = d
		if err := this.conn.PublishMsg(m); err != nil {
			return err
		}
	}

	return nil
}

func (this *Producer) Close() error {
	if !atomic.CompareAndSwapInt32(&this.closed, 0, 1) {
		return nil
	}

	if this.conn != nil {
		this.conn.Close()
		this.conn = nil
	}
	return nil
}
