package main

import (
	"fmt"
	"github.com/smartwalle/mx/nsq"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	var config = nsq.NewConfig()
	q, err := nsq.New("topic-1", config)
	if err != nil {
		fmt.Println(err)
		return
	}

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
