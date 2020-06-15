package main

import (
	"fmt"
	"github.com/smartwalle/mx/rocketmq"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	var config = rocketmq.NewConfig()
	q, err := rocketmq.New("topic-1", "group-1", config)
	if err != nil {
		fmt.Println(err)
		return
	}

	fmt.Println("begin...")
	for i := 0; i < 10; i++ {
		if err := q.Enqueue([]byte(fmt.Sprintf("hello  %s %d", time.Now(), i))); err != nil {
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
