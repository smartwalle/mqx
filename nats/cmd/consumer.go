package main

import (
	"fmt"
	"github.com/smartwalle/mx"
	"github.com/smartwalle/mx/nats"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	var config = nats.NewConfig()
	config.Servers = []string{"192.168.1.77:4222"}
	c, err := nats.NewConsumer("topic-1", "channel-1", config)
	if err != nil {
		fmt.Println(err)
		return
	}

	err = c.Dequeue(func(m mx.Message) bool {
		fmt.Println("Dequeue 1", time.Now(), string(m.Value()))
		return true
	})
	fmt.Println(err)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	select {
	case <-sig:
	}
	fmt.Println("Close", c.Close())
}
