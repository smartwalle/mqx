package rocketmq

import (
	"github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/apache/rocketmq-client-go/v2/producer"
	"github.com/smartwalle/mx"
	"time"
)

type Config struct {
	NameServerAddrs  []string
	NameServerDomain string
	//ClientIP          string
	InstanceName string
	//UnitMode          bool
	//UnitName          string
	VIPChannelEnabled bool
	RetryTimes        int
	Credentials       primitive.Credentials
	Namespace         string
	Producer          struct {
		Group                 string
		Selector              producer.QueueSelector
		SendMsgTimeout        time.Duration
		DefaultTopicQueueNums int
		CreateTopicKey        string
		Interceptors          []primitive.Interceptor
	}
	Consumer struct {
		//ConsumeTimestamp              string
		//ConsumerPullTimeout           time.Duration
		ConsumeConcurrentlyMaxSpan int
		//PullThresholdForQueue         int64
		//PullThresholdSizeForQueue     int
		//PullThresholdForTopic         int
		//PullThresholdSizeForTopic     int
		//PullInterval                  time.Duration
		ConsumeMessageBatchMaxSize int
		PullBatchSize              int32
		//PostSubscriptionWhenPull      bool
		MaxReconsumeTimes             int32
		SuspendCurrentQueueTimeMillis time.Duration
		//ConsumeTimeout                time.Duration
		ConsumerModel              consumer.MessageModel
		Strategy                   consumer.AllocateStrategy
		ConsumeOrderly             bool
		FromWhere                  consumer.ConsumeFromWhere
		Interceptors               []primitive.Interceptor
		MaxTimeConsumeContinuously time.Duration
		AutoCommit                 bool
		RebalanceLockInterval      time.Duration
	}
}

func NewConfig() *Config {
	var c = &Config{}
	c.NameServerAddrs = []string{"127.0.0.1:9876"}
	c.InstanceName = "DEFAULT"
	c.RetryTimes = 3

	c.Producer.Group = "DEFAULT_PRODUCER"
	c.Producer.Selector = producer.NewRoundRobinQueueSelector()
	c.Producer.SendMsgTimeout = 3 * time.Second
	c.Producer.DefaultTopicQueueNums = 4
	c.Producer.CreateTopicKey = "TBW102"

	c.Consumer.Strategy = consumer.AllocateByAveragely
	c.Consumer.MaxTimeConsumeContinuously = 60 * time.Second
	c.Consumer.RebalanceLockInterval = 20 * time.Second
	c.Consumer.MaxReconsumeTimes = -1
	c.Consumer.ConsumerModel = consumer.Clustering
	c.Consumer.AutoCommit = true
	//c.Consumer.PullBatchSize = 1
	c.Consumer.ConsumeMessageBatchMaxSize = 1
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
