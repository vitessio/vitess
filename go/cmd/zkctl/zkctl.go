// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"

	"code.google.com/p/vitess/go/relog"
	"code.google.com/p/vitess/go/zk/zkctl"
)

var usage = `
Commands:

	init | start | shutdown | teardown
`

var zkCfg = flag.String("zk.cfg", "6@<hostname>:3801:3802:3803",
	"zkid@server1:leaderPort1:electionPort1:clientPort1,...)")
var myId = flag.Uint("zk.myid", 0,
	"which server do you want to be? only needed when running multiple instance on one box, otherwise myid is implied by hostname")
var force = flag.Bool("force", false, "force action, no promptin")
var logLevel = flag.String("log.level", "WARNING", "set log level")
var stdin *bufio.Reader

func init() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
		flag.PrintDefaults()
		fmt.Fprintf(os.Stderr, usage)
	}
	stdin = bufio.NewReader(os.Stdin)
}

func confirm(prompt string) bool {
	if *force {
		return true
	}
	fmt.Fprintf(os.Stderr, prompt+" [NO/yes] ")

	line, _ := stdin.ReadString('\n')
	return strings.ToLower(strings.TrimSpace(line)) == "yes"
}

func main() {
	flag.Parse()
	args := flag.Args()

	if len(args) == 0 {
		flag.Usage()
		os.Exit(1)
	}

	relog.SetPrefix("zkctl ")
	if err := relog.SetLevelByName(*logLevel); err != nil {
		log.Fatal(err)
	}
	zkConfig := zkctl.MakeZkConfigFromString(*zkCfg, uint32(*myId))
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
		log.Fatalf("invalid action: %v", action)
	}
	if err != nil {
		log.Fatalf("failed %v: %v", action, err)
	}
}
