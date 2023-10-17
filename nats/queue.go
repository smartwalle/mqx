package nats

import (
	n "github.com/nats-io/nats.go"
	"github.com/smartwalle/mx"
)

type Config struct {
	n.Options
}

func NewConfig() *Config {
	var c = &Config{}
	c.Options = n.GetDefaultOptions()
	c.Servers = []string{"127.0.0.1:4222"}
	return c
}

type Queue struct {
	producer *Producer
	consumer *Consumer
	config   *Config
	topic    string
}

func NewQueue(topic string, config *Config) (*Queue, error) {
	producer, err := NewProducer(topic, config)
	if err != nil {
		return nil, err
	}

	var q = &Queue{}
	q.producer = producer
	q.config = config
	q.topic = topic
	return q, nil
}

func (q *Queue) Enqueue(data []byte) error {
	return q.producer.Enqueue(data)
}

func (q *Queue) MultiEnqueue(data ...[]byte) error {
	return q.producer.MultiEnqueue(data...)
}

func (q *Queue) Dequeue(group string, handler mx.Handler) error {
	if q.consumer != nil {
		q.consumer.Close()
	}

	var err error
	q.consumer, err = NewConsumer(q.topic, group, q.config)
	if err != nil {
		return err
	}
	return q.consumer.Dequeue(handler)
}

func (q *Queue) Close() error {
	if q.consumer != nil {
		if err := q.consumer.Close(); err != nil {
			return err
		}
	}

	if q.producer != nil {
		if err := q.producer.Close(); err != nil {
			return err
		}
	}
	return nil
}
