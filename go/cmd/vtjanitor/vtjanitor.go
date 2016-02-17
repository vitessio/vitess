package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/exit"
	"github.com/youtube/vitess/go/flagutil"
	"github.com/youtube/vitess/go/vt/janitor"
	"github.com/youtube/vitess/go/vt/logutil"
	"github.com/youtube/vitess/go/vt/servenv"
	"github.com/youtube/vitess/go/vt/tabletmanager/tmclient"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/wrangler"
)

var (
	sleepTime     = flag.Duration("sleep_time", 3*time.Minute, "how long to sleep between janitor runs")
	keyspace      = flag.String("keyspace", "", "keyspace to manage")
	shard         = flag.String("shard", "", "shard to manage")
	dryRunModules flagutil.StringListValue
	activeModules flagutil.StringListValue
)

func init() {
	servenv.RegisterDefaultFlags()
	flag.Var(&dryRunModules, "dry_run_modules", "modules to run in dry run mode")
	flag.Var(&activeModules, "active_modules", "modules to run in active mode")

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "Available modules:\n")
		for _, m := range janitor.AvailableModules() {
			fmt.Fprintf(os.Stderr, "  %v\n", m)
		}
		fmt.Fprint(os.Stderr, "\n")

		flag.PrintDefaults()

	}
}

func main() {
	defer exit.Recover()

	flag.Parse()
	servenv.Init()

	ts := topo.GetServer()

	scheduler, err := janitor.New(*keyspace, *shard, ts, wrangler.New(logutil.NewConsoleLogger(), ts, tmclient.NewTabletManagerClient()), *sleepTime)
	if err != nil {
		log.Errorf("janitor.New: %v", err)
		exit.Return(1)
	}

	if len(activeModules)+len(dryRunModules) < 1 {
		log.Error("no modules to run specified")
		exit.Return(1)
	}

	scheduler.Enable(activeModules)
	scheduler.EnableDryRun(dryRunModules)
	go scheduler.Run()
	servenv.RunDefault()
}
