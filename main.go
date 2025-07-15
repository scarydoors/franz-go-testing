package main

import (
	"context"
	"fmt"
	"os"

	"github.com/twmb/franz-go/pkg/kgo"
)

func run(ctx context.Context) error {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers("localhost:9094"),
		kgo.ConsumerGroup("franz-go-group"),
		kgo.ConsumeTopics("test-topic"),
	)

	if err != nil {
		return err
	}

	if err := cl.Ping(ctx); err != nil {
		return err
	}

	return nil
}

func main() {
	ctx := context.Background()

	if err := run(ctx); err != nil {
		fmt.Fprintln(os.Stderr, err);
	}
}
