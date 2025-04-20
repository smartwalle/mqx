package pulsarx

import (
	"errors"
	"github.com/apache/pulsar-client-go/pulsar"
)

const (
	kStateIdle     = 0
	kStateRunning  = 1
	kStateFinished = 2
)

var (
	ErrQueueRunning = errors.New("queue running")
	ErrQueueClosed  = errors.New("queue closed")
	ErrBadQueue     = errors.New("bad queue")
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
