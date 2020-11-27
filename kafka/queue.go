package kafka

import (
	"github.com/Shopify/sarama"
	"github.com/smartwalle/mx"
)

type Config struct {
	*sarama.Config
	Addrs []string
}

func NewConfig() *Config {
	var c = &Config{}
	c.Config = sarama.NewConfig()
	c.Addrs = []string{"127.0.0.1:9092"}
	c.Version = sarama.V2_1_0_0

	// 等待服务器所有副本都保存成功后的响应
	c.Producer.RequiredAcks = sarama.WaitForAll
	// 随机的分区类型：返回一个分区器，该分区器每次选择一个随机分区
	c.Producer.Partitioner = sarama.NewRandomPartitioner
	// 是否等待成功和失败后的响应
	c.Config.Producer.Return.Successes = true
	c.Config.Producer.Return.Errors = true

	c.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin
	c.Consumer.Offsets.Initial = sarama.OffsetOldest
	return c
}

type Queue struct {
	producer *Producer
	consumer *Consumer
	config   *Config
	topic    string
}

func NewQueue(topic string, config *Config) (*Queue, error) {
	producer, err := NewProducer(config)
	if err != nil {
		return nil, err
	}

	var q = &Queue{}
	q.producer = producer
	q.config = config
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
	if this.consumer != nil {
		if err := this.consumer.Close(); err != nil {
			return err
		}
		this.consumer = nil
	}

	if this.producer != nil {
		if err := this.producer.Close(); err != nil {
			return err
		}
		this.producer = nil
	}
	return nil
}
