package kafkax

import (
	"context"
	"github.com/segmentio/kafka-go"
	"github.com/smartwalle/mx"
	"sync/atomic"
)

type Producer struct {
	closed int32
	topic  string
	writer *kafka.Writer
}

func NewProducer(topic string, config *Config) (*Producer, error) {
	var writer = config.Writer
	var p = &Producer{}
	p.closed = 0
	p.topic = topic
	p.writer = &writer
	p.writer.Topic = ""
	return p, nil
}

func (p *Producer) Enqueue(data []byte) error {
	var m = kafka.Message{}
	m.Topic = p.topic
	m.Value = data
	return p.EnqueueMessages(m)
}

func (p *Producer) EnqueueMessage(m kafka.Message) error {
	m.Topic = p.topic
	return p.EnqueueMessages(m)
}

func (p *Producer) MultiEnqueue(data ...[]byte) error {
	if len(data) == 0 {
		return nil
	}

	var ms = make([]kafka.Message, 0, len(data))
	for _, d := range data {
		var m = kafka.Message{}
		m.Topic = p.topic
		m.Value = d
		ms = append(ms, m)
	}
	return p.EnqueueMessages(ms...)
}

func (p *Producer) EnqueueMessages(m ...kafka.Message) error {
	if atomic.LoadInt32(&p.closed) == 1 {
		return mx.ErrClosedQueue
	}
	return p.writer.WriteMessages(context.Background(), m...)
}

func (p *Producer) Close() error {
	if !atomic.CompareAndSwapInt32(&p.closed, 0, 1) {
		return nil
	}

	if p.writer != nil {
		if err := p.writer.Close(); err != nil {
			return err
		}
		p.writer = nil
	}

	return nil
}
