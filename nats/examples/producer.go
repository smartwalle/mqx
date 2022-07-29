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
	config.Servers = []string{"192.168.1.99:4222"}
	p, err := nats.NewProducer("topic-1", config)
	if err != nil {
		fmt.Println(err)
		return
	}

	fmt.Println("begin...")
	for i := 0; i < 1; i++ {
		if err := p.Enqueue([]byte(fmt.Sprintf("hello %d", i))); err != nil {
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
