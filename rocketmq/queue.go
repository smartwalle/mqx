package rocketmq

import (
	"github.com/smartwalle/mx"
)

type Queue struct {
}

func New() (mx.Queue, error) {
	var q = &Queue{}
	return q, nil
}

func (this *Queue) Enqueue(value []byte) error {
	return nil
}

func (this *Queue) AsyncEnqueue(value []byte) error {
	return nil
}

func (this *Queue) Dequeue() (mx.Message, error) {
	return nil, nil
}

func (this *Queue) Close() error {
	return nil
}
