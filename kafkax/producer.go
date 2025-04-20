package kafkax

import (
	"context"
	"github.com/segmentio/kafka-go"
	"sync/atomic"
)

type Producer struct {
	topic  string
	writer *kafka.Writer
	closed atomic.Bool
}

func NewProducer(topic string, config *Config) *Producer {
	var writer = config.Writer
	var p = &Producer{}
	p.topic = topic
	p.writer = &writer
	p.writer.Topic = topic
	return p
}

func (p *Producer) Enqueue(ctx context.Context, data []byte) error {
	var message = kafka.Message{}
	message.Value = data
	return p.EnqueueMessages(ctx, message)
}

func (p *Producer) EnqueueMessage(ctx context.Context, message kafka.Message) error {
	message.Topic = p.topic
	return p.EnqueueMessages(ctx, message)
}

func (p *Producer) MultiEnqueue(ctx context.Context, data ...[]byte) error {
	if len(data) == 0 {
		return nil
	}

	var messages = make([]kafka.Message, 0, len(data))
	for _, d := range data {
		var m = kafka.Message{}
		m.Value = d
		messages = append(messages, m)
	}
	return p.EnqueueMessages(ctx, messages...)
}

func (p *Producer) EnqueueMessages(ctx context.Context, messages ...kafka.Message) error {
	if p.closed.Load() {
		return ErrQueueClosed
	}

	return p.writer.WriteMessages(ctx, messages...)
}

func (p *Producer) Close() error {
	//if !p.closed.CompareAndSwap(false, true) {
	//	return nil
	//}
	p.closed.Store(true)

	if p.writer != nil {
		if err := p.writer.Close(); err != nil {
			return err
		}
	}

	return nil
}
