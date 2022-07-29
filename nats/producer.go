package nats

import (
	"errors"
	n "github.com/nats-io/nats.go"
	"github.com/smartwalle/mx"
	"sync"
)

type Producer struct {
	mu     *sync.Mutex
	closed bool
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
	p.mu = &sync.Mutex{}
	p.closed = false
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

	this.mu.Lock()
	defer this.mu.Unlock()

	if this.closed {
		return mx.ErrClosedQueue
	}
	m.Subject = this.topic

	err := this.conn.PublishMsg(m)
	return err
}

func (this *Producer) MultiEnqueue(data ...[]byte) error {
	return errors.New("method MultiEnqueue not implemented")
}

func (this *Producer) Close() error {
	this.mu.Lock()
	defer this.mu.Unlock()

	if this.closed {
		return nil
	}
	this.closed = true

	if this.conn != nil {
		this.conn.Close()
		this.conn = nil
	}
	return nil
}
