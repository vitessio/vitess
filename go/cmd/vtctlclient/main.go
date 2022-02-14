package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"context"

	"vitess.io/vitess/go/exit"
	"vitess.io/vitess/go/trace"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/vtctl/vtctlclient"

	logutilpb "vitess.io/vitess/go/vt/proto/logutil"
)

// The default values used by these flags cannot be taken from wrangler and
// actionnode modules, as we don't want to depend on them at all.
var (
	actionTimeout = flag.Duration("action_timeout", time.Hour, "timeout for the total command")
	server        = flag.String("server", "", "server to use for connection")
)

func main() {
	defer exit.Recover()

	flag.Parse()

	closer := trace.StartTracing("vtctlclient")
	defer trace.LogErrorsWhenClosing(closer)

	logger := logutil.NewConsoleLogger()

	// We can't do much without a -server flag
	if *server == "" {
		log.Error(errors.New("please specify -server <vtctld_host:vtctld_port> to specify the vtctld server to connect to"))
		os.Exit(1)
	}

	ctx, cancel := context.WithTimeout(context.Background(), *actionTimeout)
	defer cancel()

	err := vtctlclient.RunCommandAndWait(
		ctx, *server, flag.Args(),
		func(e *logutilpb.Event) {
			logutil.LogEvent(logger, e)
		})
	if err != nil {
		if strings.Contains(err.Error(), "flag: help requested") {
			return
		}

		errStr := strings.Replace(err.Error(), "remote error: ", "", -1)
		fmt.Printf("%s Error: %s\n", flag.Arg(0), errStr)
		log.Error(err)
		os.Exit(1)
	}
}
