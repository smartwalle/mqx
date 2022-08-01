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

func (this *Queue) SetLogger(l Logger, lv nsq.LogLevel) {
	this.logger = l
	this.logLevel = lv
	this.producer.SetLogger(this.logger, this.logLevel)
}

func (this *Queue) Enqueue(data []byte) error {
	return this.producer.Enqueue(data)
}

func (this *Queue) DeferredEnqueue(delay time.Duration, data []byte) error {
	return this.producer.DeferredEnqueue(delay, data)
}

func (this *Queue) MultiEnqueue(data ...[]byte) error {
	return this.producer.MultiEnqueue(data...)
}

func (this *Queue) Dequeue(group string, handler mx.Handler) error {
	if this.consumer != nil {
		this.consumer.Close()
	}

	var err error
	this.consumer, err = NewConsumer(this.topic, group, this.config)
	if err != nil {
		return err
	}
	this.consumer.SetLogger(this.logger, this.logLevel)
	return this.consumer.Dequeue(handler)
}

func (this *Queue) Close() error {
	if this.producer != nil {
		if err := this.producer.Close(); err != nil {
			return err
		}
	}

	if this.consumer != nil {
		if err := this.consumer.Close(); err != nil {
			return err
		}
	}
	return nil
}
