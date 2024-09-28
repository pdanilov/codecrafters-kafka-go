package main

import (
	"fmt"
	"os"

	"github.com/codecrafters-io/kafka-starter-go/internal/pkg/kafka"
)

func main() {
	srv, err := kafka.NewKafkaServer("9092")
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	defer srv.Shutdown()

	if err := srv.Run(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
