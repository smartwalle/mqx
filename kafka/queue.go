package kafka

import (
	"github.com/IBM/sarama"
	"github.com/smartwalle/mx"
)

type Config struct {
	*sarama.Config
	Addrs      []string
	Completion func(message *sarama.ProducerMessage, err error)
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
	c.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.NewBalanceStrategyRange()}
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
