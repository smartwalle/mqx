package main

import (
	"fmt"
	"github.com/smartwalle/mx"
	"github.com/smartwalle/mx/nsq"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	var config = nsq.NewConfig()
	q, err := nsq.NewQueue("topic-1", config)
	if err != nil {
		fmt.Println(err)
		return
	}

	q.Dequeue("channel-2", func(m mx.Message) bool {
		fmt.Println("Dequeue 1", time.Now(), string(m.Value()))
		return true
	})

	fmt.Println("begin...")
	for i := 0; i < 1000; i++ {
		if err := q.Enqueue([]byte(fmt.Sprintf("hello %d", i))); err != nil {
			fmt.Println("Enqueue", err)
			break
		}
	}
	fmt.Println("end...")

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	select {
	case <-sig:
	}
	fmt.Println("Close", q.Close())
}
