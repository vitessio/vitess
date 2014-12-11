// Copyright 2013, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.
package main

import (
	"flag"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	log "github.com/golang/glog"
	"github.com/henryanand/vitess/go/vt/worker"
	"github.com/henryanand/vitess/go/vt/wrangler"
)

var (
	commandDisplayInterval = flag.Duration("command_display_interval", time.Second, "Interval between each status update when vtworker is executing a single command from the command line")
)

type command struct {
	Name        string
	method      func(wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) worker.Worker
	interactive func(wr *wrangler.Wrangler, w http.ResponseWriter, r *http.Request)
	params      string
	Help        string // if help is empty, won't list the command
}

type commandGroup struct {
	Name        string
	Description string
	Commands    []command
}

var commands = []commandGroup{
	commandGroup{
		"Diffs",
		"Workers comparing and validating data",
		[]command{},
	},
	commandGroup{
		"Clones",
		"Workers copying data for backups and clones",
		[]command{},
	},
}

func init() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s [global parameters] command [command parameters]\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "\nThe global optional parameters are:\n")
		flag.PrintDefaults()
		fmt.Fprintf(os.Stderr, "\nThe commands are listed below, sorted by group. Use '%s <command> -h' for more help.\n\n", os.Args[0])
		for _, group := range commands {
			fmt.Fprintf(os.Stderr, "%v: %v\n", group.Name, group.Description)
			for _, cmd := range group.Commands {
				fmt.Fprintf(os.Stderr, "  %v %v\n", cmd.Name, cmd.params)
			}
			fmt.Fprintf(os.Stderr, "\n")
		}
	}
}

func addCommand(groupName string, c command) {
	for i, group := range commands {
		if group.Name == groupName {
			commands[i].Commands = append(commands[i].Commands, c)
			return
		}
	}
	panic(fmt.Errorf("Trying to add to missing group %v", groupName))
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

func commandWorker(wr *wrangler.Wrangler, args []string) worker.Worker {
	action := args[0]

	actionLowerCase := strings.ToLower(action)
	for _, group := range commands {
		for _, cmd := range group.Commands {
			if strings.ToLower(cmd.Name) == actionLowerCase {
				subFlags := flag.NewFlagSet(action, flag.ExitOnError)
				subFlags.Usage = func() {
					fmt.Fprintf(os.Stderr, "Usage: %s %s %s\n\n", os.Args[0], cmd.Name, cmd.params)
					fmt.Fprintf(os.Stderr, "%s\n\n", cmd.Help)
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

func runCommand(args []string) {
	wrk := commandWorker(wr, args)
	done, err := setAndStartWorker(wrk)
	if err != nil {
		log.Fatalf("Cannot set worker: %v", err)
	}

	// a go routine displays the status every second
	go func() {
		timer := time.Tick(*commandDisplayInterval)
		for {
			select {
			case <-done:
				log.Infof("Command is done:")
				log.Info(wrk.StatusAsText())
				if wrk.Error() != nil {
					os.Exit(1)
				}
				os.Exit(0)
			case <-timer:
				log.Info(wrk.StatusAsText())
			}
		}
	}()
}
