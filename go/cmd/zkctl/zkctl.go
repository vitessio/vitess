// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// zkctl initializes and controls ZooKeeper with Vitess-specific configuration.
package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/exit"
	"github.com/youtube/vitess/go/vt/logutil"
	"github.com/youtube/vitess/go/zk/zkctl"
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

	stdin *bufio.Reader
)

func init() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
		flag.PrintDefaults()
		fmt.Fprintf(os.Stderr, usage)
	}
	stdin = bufio.NewReader(os.Stdin)
}

func main() {
	defer exit.Recover()
	defer logutil.Flush()

	flag.Parse()
	args := flag.Args()

	if len(args) == 0 {
		flag.Usage()
		exit.Return(1)
	}

	zkConfig := zkctl.MakeZkConfigFromString(*zkCfg, uint32(*myID))
	zkd := zkctl.NewZkd(zkConfig)

	action := flag.Arg(0)
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
