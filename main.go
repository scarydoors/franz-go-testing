package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

func run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
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

	go consume(ctx, cl)

	time.Sleep(5 * time.Second)
	cancel()
	cl.Close()
	fmt.Println("should have closed client")
	time.Sleep(5 * time.Second)

	return nil
}

func consume(ctx context.Context, cl *kgo.Client) error {
	outer: for {
		fetches := cl.PollFetches(ctx)
		if fetches.IsClientClosed() {
			break
		}

		select {
		case <-ctx.Done():
			fmt.Printf("%v", fetches.Errors())
			break outer
		default:
		}

		time.Sleep(500 * time.Millisecond)
		fmt.Println("still inside loop")
	}

	fmt.Println("escaped loop")
	return nil
}

func main() {
	ctx := context.Background()

	if err := run(ctx); err != nil {
		fmt.Fprintln(os.Stderr, err);
	}
}
