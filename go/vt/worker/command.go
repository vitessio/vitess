// Copyright 2013, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package worker

import (
	"flag"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	log "github.com/golang/glog"
	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/vt/wrangler"
)

type command struct {
	Name        string
	method      func(wi *Instance, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) (Worker, error)
	Interactive func(wi *Instance, ctx context.Context, wr *wrangler.Wrangler, w http.ResponseWriter, r *http.Request)
	Params      string
	Help        string // if help is empty, won't list the command
}

type commandGroup struct {
	Name        string
	Description string
	Commands    []command
}

// Commands is the list of available command groups.
var Commands = []commandGroup{
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
	commandGroup{
		"Debugging",
		"Internal commands to test the general worker functionality",
		[]command{},
	},
}

func addCommand(groupName string, c command) {
	for i, group := range Commands {
		if group.Name == groupName {
			Commands[i].Commands = append(Commands[i].Commands, c)
			return
		}
	}
	panic(fmt.Errorf("Trying to add to missing group %v", groupName))
}

func commandWorker(wi *Instance, wr *wrangler.Wrangler, args []string, cell string) (Worker, error) {
	action := args[0]

	actionLowerCase := strings.ToLower(action)
	for _, group := range Commands {
		for _, cmd := range group.Commands {
			if strings.ToLower(cmd.Name) == actionLowerCase {
				subFlags := flag.NewFlagSet(action, flag.ExitOnError)
				subFlags.Usage = func() {
					fmt.Fprintf(os.Stderr, "Usage: %s %s %s\n\n", os.Args[0], cmd.Name, cmd.Params)
					fmt.Fprintf(os.Stderr, "%s\n\n", cmd.Help)
					subFlags.PrintDefaults()
				}
				return cmd.method(wi, wr, subFlags, args[1:])
			}
		}
	}
	flag.Usage()
	return nil, fmt.Errorf("unknown command: %v", action)
}

// RunCommand executes the vtworker command specified by "args" and blocks until the command has finished.
func (wi *Instance) RunCommand(args []string, wr *wrangler.Wrangler) error {
	wrk, err := commandWorker(wi, wr, args, wi.cell)
	if err != nil {
		return err
	}
	done, err := wi.setAndStartWorker(wrk)
	if err != nil {
		return fmt.Errorf("cannot set worker: %v", err)
	}

	// display the status every second
	timer := time.Tick(wi.commandDisplayInterval)
	for {
		select {
		case <-done:
			log.Info(wrk.StatusAsText())
			wi.currentWorkerMutex.Lock()
			err := wi.lastRunError
			wi.currentWorkerMutex.Unlock()
			if err != nil {
				return err
			}
			return nil
		case <-timer:
			log.Info(wrk.StatusAsText())
		}
	}
}
