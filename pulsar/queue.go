package pulsar

import (
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/smartwalle/mx"
	"time"
)

type Config struct {
	ClientOptions   pulsar.ClientOptions
	ConsumerOptions pulsar.ConsumerOptions
	ProducerOptions pulsar.ProducerOptions
}

func NewConfig() *Config {
	var c = &Config{}
	c.ClientOptions.URL = "pulsar://localhost:6650"
	c.ConsumerOptions.Type = pulsar.Shared
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

func (q *Queue) DeferredEnqueue(delay time.Duration, data []byte) error {
	return q.producer.DeferredEnqueue(delay, data)
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
