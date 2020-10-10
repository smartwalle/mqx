package nsq

import (
	"errors"
	"github.com/nsqio/go-nsq"
	"github.com/smartwalle/mx"
	"sync"
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
	mu       *sync.Mutex
	closed   bool
	topic    string
	channel  string
	config   *Config
	producer *nsq.Producer
	consumer *nsq.Consumer
}

func New(topic, channel string, config *Config) (*Queue, error) {
	producer, err := nsq.NewProducer(config.NSQAddr, config.Config)
	if err != nil {
		return nil, err
	}

	var q = &Queue{}
	q.mu = &sync.Mutex{}
	q.closed = false
	q.topic = topic
	q.channel = channel
	q.config = config
	q.producer = producer
	return q, nil
}

func (this *Queue) Enqueue(value []byte) error {
	if this.closed {
		return mx.ErrClosedQueue
	}
	return this.producer.Publish(this.topic, value)
}

func (this *Queue) Dequeue(h mx.Handler) error {
	this.mu.Lock()
	defer this.mu.Unlock()
	if this.closed {
		return mx.ErrClosedQueue
	}
	if this.consumer != nil {
		this.consumer.Stop()
		<-this.consumer.StopChan
		this.consumer = nil
	}

	if this.consumer == nil {
		consumer, err := nsq.NewConsumer(this.topic, this.channel, this.config.Config)
		if err != nil {
			return err
		}
		this.consumer = consumer
	}

	this.consumer.AddHandler(nsq.HandlerFunc(func(message *nsq.Message) error {
		var m = &Message{}
		m.m = message
		m.topic = this.topic
		if h(m) {
			return nil
		}
		return errors.New("qx: consume message failed")
	}))

	if err := this.consumer.ConnectToNSQLookupds(this.config.NSQLookupAddrs); err != nil {
		return err
	}
	return nil
}

func (this *Queue) Close() error {
	this.mu.Lock()
	defer this.mu.Unlock()

	if this.closed {
		return nil
	}
	this.closed = true

	if this.consumer != nil {
		this.consumer.Stop()
		<-this.consumer.StopChan
	}

	if this.producer != nil {
		this.producer.Stop()
	}

	return nil
}
