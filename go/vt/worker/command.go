// Copyright 2013, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package worker

import (
	"flag"
	"fmt"
	"html/template"
	"net/http"
	"os"
	"strings"
	"time"

	log "github.com/golang/glog"
	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/vt/logutil"
	"github.com/youtube/vitess/go/vt/wrangler"
)

type command struct {
	Name        string
	method      func(wi *Instance, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) (Worker, error)
	Interactive func(wi *Instance, ctx context.Context, wr *wrangler.Wrangler, w http.ResponseWriter, r *http.Request) (Worker, *template.Template, map[string]interface{}, error)
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

func commandWorker(wi *Instance, wr *wrangler.Wrangler, args []string, cell string, runFromCli bool) (Worker, error) {
	action := args[0]

	actionLowerCase := strings.ToLower(action)
	for _, group := range Commands {
		for _, cmd := range group.Commands {
			if strings.ToLower(cmd.Name) == actionLowerCase {
				var subFlags *flag.FlagSet
				if runFromCli {
					subFlags = flag.NewFlagSet(action, flag.ExitOnError)
				} else {
					subFlags = flag.NewFlagSet(action, flag.ContinueOnError)
				}
				// The command may be run from an RPC and may not log to the console.
				// The Wrangler logger defines where the output has to go.
				subFlags.SetOutput(logutil.NewLoggerWriter(wr.Logger()))
				subFlags.Usage = func() {
					wr.Logger().Printf("Usage: %s %s %s\n\n", os.Args[0], cmd.Name, cmd.Params)
					wr.Logger().Printf("%s\n\n", cmd.Help)
					subFlags.PrintDefaults()
				}
				return cmd.method(wi, wr, subFlags, args[1:])
			}
		}
	}
	if runFromCli {
		flag.Usage()
	} else {
		PrintAllCommands(wr.Logger())
	}
	return nil, fmt.Errorf("unknown command: %v", action)
}

// RunCommand executes the vtworker command specified by "args". Use WaitForCommand() to block on the returned done channel.
// If wr is nil, the default wrangler will be used.
// If you pass a wr wrangler, note that a MemoryLogger will be added to its current logger.
func (wi *Instance) RunCommand(args []string, wr *wrangler.Wrangler, runFromCli bool) (Worker, chan struct{}, error) {
	if wr == nil {
		wr = wi.wr
	}
	wrk, err := commandWorker(wi, wr, args, wi.cell, runFromCli)
	if err != nil {
		return nil, nil, err
	}
	done, err := wi.setAndStartWorker(wrk, wr)
	if err != nil {
		return nil, nil, fmt.Errorf("cannot set worker: %v", err)
	}
	return wrk, done, nil
}

// WaitForCommand blocks until "done" is closed. In the meantime, it logs the status of "wrk".
func (wi *Instance) WaitForCommand(wrk Worker, done chan struct{}) error {
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

// PrintAllCommands prints a help text for all registered commands to the given Logger.
func PrintAllCommands(logger logutil.Logger) {
	for _, group := range Commands {
		if group.Name == "Debugging" {
			continue
		}
		logger.Printf("%v: %v\n", group.Name, group.Description)
		for _, cmd := range group.Commands {
			logger.Printf("  %v %v\n", cmd.Name, cmd.Params)
		}
		logger.Printf("\n")
	}
}
