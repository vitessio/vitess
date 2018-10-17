/*
Copyright 2017 Google Inc.

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

package worker

import (
	"flag"
	"html/template"
	"net/http"
	"os"
	"strings"
	"time"

	"golang.org/x/net/context"
	"vitess.io/vitess/go/vt/proto/vtrpc"

	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/wrangler"
)

// Command contains the detail of a command which can be run in vtworker.
// While "Method" is run from the command line or RPC, "Interactive" may contain
// special logic to parse a web form and return templated HTML output.
type Command struct {
	Name        string
	Method      func(wi *Instance, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) (Worker, error)
	Interactive func(ctx context.Context, wi *Instance, wr *wrangler.Wrangler, w http.ResponseWriter, r *http.Request) (Worker, *template.Template, map[string]interface{}, error)
	Params      string
	Help        string // if help is empty, won't list the command
}

type commandGroup struct {
	Name        string
	Description string
	Commands    []Command
}

// commands is the list of available command groups.
var commands = []commandGroup{
	{
		"Diffs",
		"Workers comparing and validating data",
		[]Command{},
	},
	{
		"Clones",
		"Workers copying data for backups and clones",
		[]Command{},
	},
	{
		"Backfills",
		"Workers to backfill data like lookup tables",
		[]Command{},
	},
	{
		"Square",
		"Square specific commands",
		[]Command{},
	},
	{
		"Debugging",
		"Internal commands to test the general worker functionality",
		[]Command{},
	},
}

// AddCommand registers a command and makes it available.
func AddCommand(groupName string, c Command) {
	for i, group := range commands {
		if group.Name == groupName {
			commands[i].Commands = append(commands[i].Commands, c)
			return
		}
	}
	panic(vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "trying to add to missing group %v", groupName))
}

func commandWorker(wi *Instance, wr *wrangler.Wrangler, args []string, cell string, runFromCli bool) (Worker, error) {
	action := args[0]

	actionLowerCase := strings.ToLower(action)
	for _, group := range commands {
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
				return cmd.Method(wi, wr, subFlags, args[1:])
			}
		}
	}
	if runFromCli {
		flag.Usage()
	} else {
		PrintAllCommands(wr.Logger())
	}
	return nil, vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "unknown command: %v", action)
}

// RunCommand executes the vtworker command specified by "args". Use WaitForCommand() to block on the returned done channel.
// If wr is nil, the default wrangler will be used.
// If you pass a wr wrangler, note that a MemoryLogger will be added to its current logger.
// The returned worker and done channel may be nil if no worker was started e.g. in case of a "Reset".
func (wi *Instance) RunCommand(ctx context.Context, args []string, wr *wrangler.Wrangler, runFromCli bool) (Worker, chan struct{}, error) {
	if len(args) >= 1 {
		switch args[0] {
		case "Reset":
			return nil, nil, wi.Reset()
		case "Cancel":
			wi.Cancel()
			return nil, nil, nil
		}
	}

	if wr == nil {
		wr = wi.wr
	}
	wrk, err := commandWorker(wi, wr, args, wi.cell, runFromCli)
	if err != nil {
		return nil, nil, err
	}
	done, err := wi.setAndStartWorker(ctx, wrk, wr)
	if err != nil {
		return nil, nil, vterrors.Wrap(err, "cannot set worker")
	}
	return wrk, done, nil
}

// WaitForCommand blocks until "done" is closed. In the meantime, it logs the status of "wrk".
func (wi *Instance) WaitForCommand(wrk Worker, done chan struct{}) error {
	// display the status every second
	timer := time.NewTicker(wi.commandDisplayInterval)
	defer timer.Stop()
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
		case <-timer.C:
			log.Info(wrk.StatusAsText())
		}
	}
}

// PrintAllCommands prints a help text for all registered commands to the given Logger.
func PrintAllCommands(logger logutil.Logger) {
	for _, group := range commands {
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
