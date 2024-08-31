package nsq

import (
	"github.com/nsqio/go-nsq"
	"github.com/smartwalle/mx"
	"time"
)

type Config struct {
	*nsq.Config
	NSQAddr        string
	NSQLookupAddrs []string
}

func NewConfig() *Config {
	var c = &Config{}
	c.Config = nsq.NewConfig()
	c.NSQAddr = "127.0.0.1:4150"
	c.NSQLookupAddrs = []string{"127.0.0.1:4161"}
	c.LookupdPollInterval = time.Second * 10
	return c
}

type Queue struct {
	producer *Producer
	consumer *Consumer
	config   *Config
	topic    string
	logger   Logger
	logLevel nsq.LogLevel
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

func (q *Queue) SetLogger(l Logger, lv nsq.LogLevel) {
	q.logger = l
	q.logLevel = lv
	q.producer.SetLogger(q.logger, q.logLevel)
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
	q.consumer.SetLogger(q.logger, q.logLevel)
	return q.consumer.Dequeue(handler)
}

func (q *Queue) Close() error {
	if q.producer != nil {
		if err := q.producer.Close(); err != nil {
			return err
		}
	}

	if q.consumer != nil {
		if err := q.consumer.Close(); err != nil {
			return err
		}
	}
	return nil
}
