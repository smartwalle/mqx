package pulsarx

import (
	"errors"
	"github.com/apache/pulsar-client-go/pulsar"
)

var (
	ErrClosedQueue = errors.New("closed queue")
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
