package main

import (
	"fmt"
	"github.com/smartwalle/mx/nats"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	var config = nats.NewConfig()
	config.Servers = []string{"192.168.1.77:4222"}
	p, err := nats.NewProducer(config)
	if err != nil {
		fmt.Println(err)
		return
	}

	fmt.Println("begin...")
	for i := 0; i < 1000; i++ {
		if err := p.Enqueue("topic-1", []byte(fmt.Sprintf("hello %d", i))); err != nil {
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
	fmt.Println("Close", p.Close())
}
