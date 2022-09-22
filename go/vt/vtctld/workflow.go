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

package vtctld

import (
	"context"
	"time"

	"github.com/spf13/pflag"

	"vitess.io/vitess/go/trace"

	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vtctl"
	"vitess.io/vitess/go/vt/workflow"
	"vitess.io/vitess/go/vt/workflow/topovalidator"
)

var (
	workflowManagerInit        bool
	workflowManagerUseElection bool

	workflowManagerDisable []string
)

func registerVtctldWorkflowFlags(fs *pflag.FlagSet) {
	fs.BoolVar(&workflowManagerInit, "workflow_manager_init", workflowManagerInit, "Initialize the workflow manager in this vtctld instance.")
	fs.BoolVar(&workflowManagerUseElection, "workflow_manager_use_election", workflowManagerUseElection, "if specified, will use a topology server-based master election to ensure only one workflow manager is active at a time.")
	fs.StringSliceVar(&workflowManagerDisable, "workflow_manager_disable", workflowManagerDisable, "comma separated list of workflow types to disable")
}

func init() {
	for _, cmd := range []string{"vtcombo", "vtctld"} {
		servenv.OnParseFor(cmd, registerVtctldWorkflowFlags)
	}

}

func initWorkflowManager(ts *topo.Server) {
	if workflowManagerInit {
		// Uncomment this line to register the UI test validator.
		// topovalidator.RegisterUITestValidator()

		// Register the Topo Validators, and the workflow.
		topovalidator.RegisterKeyspaceValidator()
		topovalidator.RegisterShardValidator()
		topovalidator.Register()

		// Unregister the disabled workflows.
		for _, name := range workflowManagerDisable {
			workflow.Unregister(name)
		}

		// Create the WorkflowManager.
		vtctl.WorkflowManager = workflow.NewManager(ts)
		vtctl.WorkflowManager.SetSanitizeHTTPHeaders(sanitizeLogMessages)

		// Register the long polling and websocket handlers.
		vtctl.WorkflowManager.HandleHTTPLongPolling(apiPrefix + "workflow")
		vtctl.WorkflowManager.HandleHTTPWebSocket(apiPrefix + "workflow")

		if workflowManagerUseElection {
			runWorkflowManagerElection(ts)
		} else {
			runWorkflowManagerAlone()
		}
	}
}

func runWorkflowManagerAlone() {
	ctx, cancel := context.WithCancel(context.Background())
	go vtctl.WorkflowManager.Run(ctx)

	// Running cancel on OnTermSync will cancel the context of any
	// running workflow inside vtctld. They may still checkpoint
	// if they want to.
	servenv.OnTermSync(cancel)
}

func runWorkflowManagerElection(ts *topo.Server) {
	var mp topo.LeaderParticipation

	// We use servenv.ListeningURL which is only populated during Run,
	// so we have to start this with OnRun.
	servenv.OnRun(func() {
		span, ctx := trace.NewSpan(context.Background(), "WorkflowManagerElection")
		defer span.Finish()

		conn, err := ts.ConnForCell(ctx, topo.GlobalCell)
		if err != nil {
			log.Errorf("Cannot get global cell topo connection, disabling workflow manager: %v", err)
			return
		}

		mp, err = conn.NewLeaderParticipation("vtctld", servenv.ListeningURL.Host)
		if err != nil {
			log.Errorf("Cannot start LeaderParticipation, disabling workflow manager: %v", err)
			return
		}

		// Set up a redirect host so when we are not the
		// primary, we can redirect traffic properly.
		vtctl.WorkflowManager.SetRedirectFunc(func() (string, error) {
			ctx := context.Background()
			return mp.GetCurrentLeaderID(ctx)
		})

		go func() {
			for {
				ctx, err := mp.WaitForLeadership()
				switch {
				case err == nil:
					vtctl.WorkflowManager.Run(ctx)
				case topo.IsErrType(err, topo.Interrupted):
					return
				default:
					log.Errorf("Got error while waiting for master, will retry in 5s: %v", err)
					time.Sleep(5 * time.Second)
				}
			}
		}()
	})

	// When we get killed, clean up.
	servenv.OnTermSync(func() {
		mp.Stop()
	})
}
