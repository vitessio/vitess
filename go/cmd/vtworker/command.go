// Copyright 2013, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.
package main

import (
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/vt/worker"
	"github.com/youtube/vitess/go/vt/wrangler"
)

type command struct {
	name   string
	method func(wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) worker.Worker
	params string
	help   string // if help is empty, won't list the command
}

type commandGroup struct {
	name     string
	commands []command
}

var commands = []commandGroup{
	commandGroup{
		"Diffs", []command{
			command{"SplitDiff", commandSplitDiff,
				"<keyspace/shard|zk shard path>",
				"Diffs a rdonly destination shard against its SourceShards"},
		},
	},
}

func shardParamToKeyspaceShard(param string) (string, string) {
	if param[0] == '/' {
		// old zookeeper path, convert to new-style
		zkPathParts := strings.Split(param, "/")
		if len(zkPathParts) != 8 || zkPathParts[0] != "" || zkPathParts[1] != "zk" || zkPathParts[2] != "global" || zkPathParts[3] != "vt" || zkPathParts[4] != "keyspaces" || zkPathParts[6] != "shards" {
			log.Fatalf("Invalid shard path: %v", param)
		}
		return zkPathParts[5], zkPathParts[7]
	}
	zkPathParts := strings.Split(param, "/")
	if len(zkPathParts) != 2 {
		log.Fatalf("Invalid shard path: %v", param)
	}
	return zkPathParts[0], zkPathParts[1]
}

func commandSplitDiff(wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) worker.Worker {
	subFlags.Parse(args)
	if subFlags.NArg() != 1 {
		log.Fatalf("command SplitDiff requires <keyspace/shard|zk shard path>")
	}
	keyspace, shard := shardParamToKeyspaceShard(subFlags.Arg(0))
	return worker.NewSplitDiffWorker(wr, *cell, keyspace, shard)
}

func commandWorker(wr *wrangler.Wrangler, args []string) worker.Worker {
	action := args[0]

	actionLowerCase := strings.ToLower(action)
	for _, group := range commands {
		for _, cmd := range group.commands {
			if strings.ToLower(cmd.name) == actionLowerCase {
				subFlags := flag.NewFlagSet(action, flag.ExitOnError)
				subFlags.Usage = func() {
					fmt.Fprintf(os.Stderr, "Usage: %s %s %s\n\n", os.Args[0], cmd.name, cmd.params)
					fmt.Fprintf(os.Stderr, "%s\n\n", cmd.help)
					subFlags.PrintDefaults()
				}
				return cmd.method(wr, subFlags, args[1:])
			}
		}
	}
	flag.Usage()
	log.Fatalf("Unknown command %#v\n\n", action)
	return nil
}

func runCommand(wr *wrangler.Wrangler, args []string) {
	wrk := commandWorker(wr, args)
	done, err := setAndStartWorker(wrk)
	if err != nil {
		log.Fatalf("Cannot set worker: %v", err)
	}

	// a go routine displays the status every second
	go func() {
		timer := time.Tick(time.Second)
		for {
			select {
			case <-done:
				log.Infof("Command is done:")
				log.Info(wrk.StatusAsText())
				os.Exit(0)
			case <-timer:
				log.Info(wrk.StatusAsText())
			}
		}
	}()
}
