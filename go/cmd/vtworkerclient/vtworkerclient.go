package main

import (
	"flag"
	"os"
	"os/signal"
	"syscall"

	"context"

	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/worker/vtworkerclient"

	logutilpb "vitess.io/vitess/go/vt/proto/logutil"
)

var (
	server = flag.String("server", "", "server to use for connection")
)

func main() {
	flag.Parse()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		s := <-sigChan
		log.Errorf("Trying to cancel current command after receiving signal: %v", s)
		cancel()
	}()

	logger := logutil.NewConsoleLogger()

	err := vtworkerclient.RunCommandAndWait(
		ctx, *server, flag.Args(),
		func(e *logutilpb.Event) {
			logutil.LogEvent(logger, e)
		})
	if err != nil {
		log.Error(err)
		os.Exit(1)
	}
}
