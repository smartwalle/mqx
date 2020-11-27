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
	producer mx.Producer
	consumer mx.Consumer
	config   *Config
	topic    string
}

func NewQueue(topic string, config *Config) (*Queue, error) {
	producer, err := NewProducer(config)
	if err != nil {
		return nil, err
	}

	var q = &Queue{}
	q.config = config
	q.producer = producer
	q.topic = topic
	return q, nil
}

func (this *Queue) Enqueue(data []byte) error {
	return this.producer.Enqueue(this.topic, data)
}

func (this *Queue) EnqueueTopic(topic string, data []byte) error {
	return this.producer.Enqueue(topic, data)
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
	this.consumer.Dequeue(handler)

	return nil
}

func (this *Queue) Close() error {
	if this.producer != nil {
		this.producer.Close()
	}

	if this.consumer != nil {
		this.consumer.Close()
	}
	return nil
}
