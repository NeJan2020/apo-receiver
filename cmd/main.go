package main

import (
	"context"
	"log"
	"os"

	"github.com/CloudDetail/apo-receiver/pkg/receiver"
)

func main() {
	err := receiver.Run(context.Background())
	if err != nil {
		log.Fatalf("Failed to run application: %v", err)
		os.Exit(1)
	}
}
