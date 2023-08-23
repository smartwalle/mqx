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

func (this *Producer) Enqueue(data []byte) error {
	var m = kafka.Message{}
	m.Topic = this.topic
	m.Value = data
	return this.EnqueueMessages(m)
}

func (this *Producer) EnqueueMessage(m kafka.Message) error {
	m.Topic = this.topic
	return this.EnqueueMessages(m)
}

func (this *Producer) MultiEnqueue(data ...[]byte) error {
	if len(data) == 0 {
		return nil
	}

	var ms = make([]kafka.Message, 0, len(data))
	for _, d := range data {
		var m = kafka.Message{}
		m.Topic = this.topic
		m.Value = d
		ms = append(ms, m)
	}
	return this.EnqueueMessages(ms...)
}

func (this *Producer) EnqueueMessages(m ...kafka.Message) error {
	if atomic.LoadInt32(&this.closed) == 1 {
		return mx.ErrClosedQueue
	}
	return this.writer.WriteMessages(context.Background(), m...)
}

func (this *Producer) Close() error {
	if !atomic.CompareAndSwapInt32(&this.closed, 0, 1) {
		return nil
	}

	if this.writer != nil {
		if err := this.writer.Close(); err != nil {
			return err
		}
		this.writer = nil
	}

	return nil
}
