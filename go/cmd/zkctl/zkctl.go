/*
Copyright 2019 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// zkctl initializes and controls ZooKeeper with Vitess-specific configuration.
package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/spf13/pflag"

	"vitess.io/vitess/go/exit"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/zkctl"

	// Include deprecation warnings for soon-to-be-unsupported flag invocations.
	_flag "vitess.io/vitess/go/internal/flag"
)

var usage = `
Commands:

	init | start | shutdown | teardown
`

var (
	zkCfg = flag.String("zk.cfg", "6@<hostname>:3801:3802:3803",
		"zkid@server1:leaderPort1:electionPort1:clientPort1,...)")
	myID = flag.Uint("zk.myid", 0,
		"which server do you want to be? only needed when running multiple instance on one box, otherwise myid is implied by hostname")

	// Reason for nolint : Used in line 54 (stdin = bufio.NewReader(os.Stdin)) in the init function
	stdin *bufio.Reader //nolint
)

func init() {
	_flag.SetUsage(flag.CommandLine, _flag.UsageOptions{
		Epilogue: func(w io.Writer) { fmt.Fprint(w, usage) },
	})
	stdin = bufio.NewReader(os.Stdin)
}

func main() {
	defer exit.Recover()
	defer logutil.Flush()

	fs := pflag.NewFlagSet("zkctl", pflag.ExitOnError)
	log.RegisterFlags(fs)
	logutil.RegisterFlags(fs)
	_flag.Parse(fs)
	args := _flag.Args()

	if len(args) == 0 {
		flag.Usage()
		exit.Return(1)
	}

	zkConfig := zkctl.MakeZkConfigFromString(*zkCfg, uint32(*myID))
	zkd := zkctl.NewZkd(zkConfig)

	action := _flag.Arg(0)
	var err error
	switch action {
	case "init":
		err = zkd.Init()
	case "shutdown":
		err = zkd.Shutdown()
	case "start":
		err = zkd.Start()
	case "teardown":
		err = zkd.Teardown()
	default:
		log.Errorf("invalid action: %v", action)
		exit.Return(1)
	}
	if err != nil {
		log.Errorf("failed %v: %v", action, err)
		exit.Return(1)
	}
}
