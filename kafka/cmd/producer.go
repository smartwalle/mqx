package main

import (
	"fmt"
	"github.com/smartwalle/mx/kafka"
	"os"
	"os/signal"
	"syscall"
)

// 查看 Topic 信息
// ./bin/kafka-topics.sh --describe --zookeeper 127.0.0.1 --topic topic_name
//
// 调整 Topic 分区数量
// ./bin/kafka-topics.sh --alter --zookeeper 127.0.0.1 --topic topic_name --partitions partition_count
//
// 删除 Topic
// ./bin/kafka-topics.sh --delete --zookeeper 127.0.0.1 --topic topic_name

func main() {
	var config = kafka.NewConfig()
	q, err := kafka.New("topic-1", config)
	if err != nil {
		fmt.Println(err)
		return
	}

	fmt.Println("begin...")
	for i := 0; i < 2; i++ {
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
