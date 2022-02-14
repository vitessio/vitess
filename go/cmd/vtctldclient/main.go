package main

import (
	"flag"
	"os"

	"vitess.io/vitess/go/cmd/vtctldclient/command"
	"vitess.io/vitess/go/exit"
	"vitess.io/vitess/go/vt/log"
)

func main() {
	defer exit.Recover()

	// Grab all those global flags across the codebase and shove 'em on in.
	command.Root.PersistentFlags().AddGoFlagSet(flag.CommandLine)

	// hack to get rid of an "ERROR: logging before flag.Parse"
	args := os.Args[:]
	os.Args = os.Args[:1]
	flag.Parse()
	os.Args = args

	// back to your regularly scheduled cobra programming
	if err := command.Root.Execute(); err != nil {
		log.Error(err)
		exit.Return(1)
	}
}
