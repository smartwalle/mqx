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
	q, err := nsq.New("topic-1", config)
	if err != nil {
		fmt.Println(err)
		return
	}

	q.Dequeue("channel-4", func(m mx.Message) bool {
		fmt.Println("Dequeue", time.Now(), string(m.Value()))
		return true
	})

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	select {
	case <-sig:
	}
	fmt.Println("Close", q.Close())
}
