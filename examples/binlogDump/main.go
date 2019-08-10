package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"

	"github.com/Breeze0806/gbinlog"
)

var filename = flag.String("c", "", "config")

func main() {
	flag.Parse()
	if *filename == "" {
		log.Fatalf("config is empty")
	}

	e := newEnvironment(*filename)
	defer e.close()
	if err := e.build(); err != nil {
		log.Fatalf("build fail. err: %v", err)
	}

	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	processWait := make(chan os.Signal, 1)
	signal.Notify(processWait, os.Kill, os.Interrupt)

	go func() {
		select {
		case <-processWait:
			cancel()
		}
	}()

	err := e.streamer.Stream(ctx, func(t *gbinlog.Transaction) error {
		showTransaction(t, e.out)
		return nil
	})

	if err != nil {
		log.Fatalf("Stream fail. err: %v", err)
	}
}
