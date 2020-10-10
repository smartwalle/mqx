package rocketmq

import (
	"context"
	"github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/apache/rocketmq-client-go/v2/producer"
	"github.com/smartwalle/mx"
	"sync"
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
	mu       *sync.Mutex
	closed   bool
	topic    string
	group    string
	config   *Config
	producer rocketmq.Producer
	consumer rocketmq.PushConsumer
}

func New(topic, group string, config *Config) (*Queue, error) {
	var opts []producer.Option
	opts = append(opts, producer.WithGroupName(config.Producer.Group))
	opts = append(opts, producer.WithInstanceName(config.InstanceName))
	opts = append(opts, producer.WithNameServer(config.NameServerAddrs))
	opts = append(opts, producer.WithNameServerDomain(config.NameServerDomain))
	opts = append(opts, producer.WithNamespace(config.Namespace))
	opts = append(opts, producer.WithVIPChannel(config.VIPChannelEnabled))
	opts = append(opts, producer.WithRetry(config.RetryTimes))
	opts = append(opts, producer.WithCredentials(config.Credentials))

	opts = append(opts, producer.WithInterceptor(config.Producer.Interceptors...))
	opts = append(opts, producer.WithSendMsgTimeout(config.Producer.SendMsgTimeout))
	opts = append(opts, producer.WithQueueSelector(config.Producer.Selector))
	opts = append(opts, producer.WithDefaultTopicQueueNums(config.Producer.DefaultTopicQueueNums))
	opts = append(opts, producer.WithCreateTopicKey(config.Producer.CreateTopicKey))

	var producer, err = rocketmq.NewProducer(opts...)
	if err != nil {
		return nil, err
	}

	if err = producer.Start(); err != nil {
		return nil, err
	}

	var q = &Queue{}
	q.mu = &sync.Mutex{}
	q.closed = false
	q.topic = topic
	q.group = group
	q.config = config
	q.producer = producer
	return q, nil
}

func (this *Queue) Enqueue(value []byte) error {
	var m = primitive.NewMessage(this.topic, value)
	return this.EnqueueMessage(m)
}

func (this *Queue) EnqueueMessage(m *primitive.Message) error {
	if m == nil {
		return nil
	}

	if this.closed {
		return mx.ErrClosedQueue
	}

	_, err := this.producer.SendSync(context.Background(), m)
	return err
}

func (this *Queue) Dequeue(h mx.Handler) error {
	this.mu.Lock()
	defer this.mu.Unlock()
	if this.closed {
		return mx.ErrClosedQueue
	}

	if this.consumer != nil {
		this.consumer.Unsubscribe(this.topic)
		this.consumer.Shutdown()
		this.consumer = nil
	}

	if this.consumer == nil {
		var opts []consumer.Option
		opts = append(opts, consumer.WithGroupName(this.group))
		opts = append(opts, consumer.WithInstance(this.config.InstanceName))
		opts = append(opts, consumer.WithNameServer(this.config.NameServerAddrs))
		opts = append(opts, consumer.WithNameServerDomain(this.config.NameServerDomain))
		opts = append(opts, consumer.WithNamespace(this.config.Namespace))
		opts = append(opts, consumer.WithVIPChannel(this.config.VIPChannelEnabled))
		opts = append(opts, consumer.WithRetry(this.config.RetryTimes))
		opts = append(opts, consumer.WithCredentials(this.config.Credentials))

		opts = append(opts, consumer.WithConsumerModel(this.config.Consumer.ConsumerModel))
		opts = append(opts, consumer.WithConsumeFromWhere(this.config.Consumer.FromWhere))
		opts = append(opts, consumer.WithConsumerOrder(this.config.Consumer.ConsumeOrderly))
		opts = append(opts, consumer.WithConsumeMessageBatchMaxSize(this.config.Consumer.ConsumeMessageBatchMaxSize))
		opts = append(opts, consumer.WithInterceptor(this.config.Consumer.Interceptors...))
		opts = append(opts, consumer.WithMaxReconsumeTimes(this.config.Consumer.MaxReconsumeTimes))
		opts = append(opts, consumer.WithStrategy(this.config.Consumer.Strategy))
		opts = append(opts, consumer.WithPullBatchSize(this.config.Consumer.PullBatchSize))
		opts = append(opts, consumer.WithRebalanceLockInterval(this.config.Consumer.RebalanceLockInterval))
		opts = append(opts, consumer.WithAutoCommit(this.config.Consumer.AutoCommit))
		opts = append(opts, consumer.WithSuspendCurrentQueueTimeMillis(this.config.Consumer.SuspendCurrentQueueTimeMillis))

		consumer, err := rocketmq.NewPushConsumer(opts...)
		if err != nil {
			return err
		}
		this.consumer = consumer
	}

	this.consumer.Subscribe(this.topic, consumer.MessageSelector{}, func(ctx context.Context, messages ...*primitive.MessageExt) (consumer.ConsumeResult, error) {
		for _, msg := range messages {
			var m = &Message{}
			m.m = msg
			if h(m) {
				return consumer.ConsumeSuccess, nil
			}
		}
		return consumer.ConsumeRetryLater, nil
	})
	this.consumer.Start()
	return nil
}

func (this *Queue) Close() error {
	this.mu.Lock()
	defer this.mu.Unlock()

	if this.closed {
		return nil
	}
	this.closed = true

	if this.producer != nil {
		if err := this.producer.Shutdown(); err != nil {
			return err
		}
	}

	if this.consumer != nil {
		this.consumer.Unsubscribe(this.topic)
		if err := this.consumer.Shutdown(); err != nil {
			return err
		}
	}

	return nil
}
