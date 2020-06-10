package main

import (
	"fmt"
	"github.com/smartwalle/mx"
	"github.com/smartwalle/mx/nsq"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	var config = nsq.NewConfig()
	config.MaxInFlight = 2
	config.NSQLookupdAddrs = []string{"localhost:4161"}

	var q, err = nsq.New("topic-1", "channel-1", config)
	if err != nil {
		fmt.Println(err)
		return
	}

	q.Dequeue(func(m mx.Message, err error) bool {
		fmt.Println("Dequeue", string(m.Value()))
		return true
	})

	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)
	select {
	case <-sigterm:
	}
	fmt.Println("Close", q.Close())
}
