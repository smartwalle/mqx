package kafkax

import (
	"context"
	"github.com/segmentio/kafka-go"
	"github.com/smartwalle/mx"
	"sync/atomic"
)

type Consumer struct {
	closed int32
	topic  string
	group  string
	config *Config
	reader *kafka.Reader
}

func NewConsumer(topic, group string, config *Config) (*Consumer, error) {
	var c = &Consumer{}
	c.closed = 0
	c.topic = topic
	c.group = group
	c.config = config
	return c, nil
}

func (this *Consumer) Dequeue(handler mx.Handler) error {
	if atomic.LoadInt32(&this.closed) == 1 {
		return mx.ErrClosedQueue
	}

	if this.reader != nil {
		this.reader.Close()
		this.reader = nil
	}

	this.config.Reader.Topic = this.topic
	this.config.Reader.GroupID = this.group

	var reader = kafka.NewReader(this.config.Reader)
	this.reader = reader

	go func() {
		for {
			message, err := this.reader.FetchMessage(context.Background())
			if err != nil {
				return
			}

			var m = &Message{}
			m.m = message
			if handler(m) {
				this.reader.CommitMessages(context.Background(), message)
			}
		}
	}()

	return nil
}

func (this *Consumer) Close() error {
	if !atomic.CompareAndSwapInt32(&this.closed, 0, 1) {
		return nil
	}

	if this.reader != nil {
		if err := this.reader.Close(); err != nil {
			return err
		}
		this.reader = nil
	}

	return nil
}
