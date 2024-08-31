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

func (p *Producer) Enqueue(data []byte) error {
	var m = &n.Msg{}
	m.Subject = p.topic
	m.Data = data
	return p.EnqueueMessage(m)
}

func (p *Producer) EnqueueMessage(m *n.Msg) error {
	if m == nil {
		return nil
	}

	if atomic.LoadInt32(&p.closed) == 1 {
		return mx.ErrClosedQueue
	}

	m.Subject = p.topic

	err := p.conn.PublishMsg(m)
	return err
}

func (p *Producer) MultiEnqueue(data ...[]byte) error {
	if len(data) == 0 {
		return nil
	}

	if atomic.LoadInt32(&p.closed) == 1 {
		return mx.ErrClosedQueue
	}

	for _, d := range data {
		var m = &n.Msg{}
		m.Subject = p.topic
		m.Data = d
		if err := p.conn.PublishMsg(m); err != nil {
			return err
		}
	}

	return nil
}

func (p *Producer) Close() error {
	if !atomic.CompareAndSwapInt32(&p.closed, 0, 1) {
		return nil
	}

	if p.conn != nil {
		p.conn.Close()
		p.conn = nil
	}
	return nil
}
