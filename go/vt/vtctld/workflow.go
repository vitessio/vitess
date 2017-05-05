/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreedto in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package vtctld

import (
	"flag"
	"time"

	log "github.com/golang/glog"
	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/flagutil"
	"github.com/youtube/vitess/go/vt/schemamanager/schemaswap"
	"github.com/youtube/vitess/go/vt/servenv"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/vtctl"
	"github.com/youtube/vitess/go/vt/workflow"
	"github.com/youtube/vitess/go/vt/workflow/resharding"
	"github.com/youtube/vitess/go/vt/workflow/topovalidator"
)

var (
	workflowManagerInit        = flag.Bool("workflow_manager_init", false, "Initialize the workflow manager in this vtctld instance.")
	workflowManagerUseElection = flag.Bool("workflow_manager_use_election", false, "if specified, will use a topology server-based master election to ensure only one workflow manager is active at a time.")
	workflowManagerDisable     flagutil.StringListValue
)

func init() {
	flag.Var(&workflowManagerDisable, "workflow_manager_disable", "comma separated list of workflow types to disable")
}

func initWorkflowManager(ts topo.Server) {
	if *workflowManagerInit {
		// Uncomment this line to register the UI test validator.
		// topovalidator.RegisterUITestValidator()

		// Register the Topo Validators, and the workflow.
		topovalidator.RegisterKeyspaceValidator()
		topovalidator.RegisterShardValidator()
		topovalidator.Register()

		// Register the Schema Swap workflow.
		schemaswap.RegisterWorkflowFactory()

		// Register the Horizontal Resharding workflow.
		resharding.Register()

		// Unregister the blacklisted workflows.
		for _, name := range workflowManagerDisable {
			workflow.Unregister(name)
		}

		// Create the WorkflowManager.
		vtctl.WorkflowManager = workflow.NewManager(ts)

		// Register the long polling and websocket handlers.
		vtctl.WorkflowManager.HandleHTTPLongPolling(apiPrefix + "workflow")
		vtctl.WorkflowManager.HandleHTTPWebSocket(apiPrefix + "workflow")

		if *workflowManagerUseElection {
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

func runWorkflowManagerElection(ts topo.Server) {
	var mp topo.MasterParticipation

	// We use servenv.ListeningURL which is only populated during Run,
	// so we have to start this with OnRun.
	servenv.OnRun(func() {
		var err error
		mp, err = ts.NewMasterParticipation("vtctld", servenv.ListeningURL.Host)
		if err != nil {
			log.Errorf("Cannot start MasterParticipation, disabling workflow manager: %v", err)
			return
		}

		// Set up a redirect host so when we are not the
		// master, we can redirect traffic properly.
		vtctl.WorkflowManager.SetRedirectFunc(func() (string, error) {
			ctx := context.Background()
			return mp.GetCurrentMasterID(ctx)
		})

		go func() {
			for {
				ctx, err := mp.WaitForMastership()
				switch err {
				case nil:
					vtctl.WorkflowManager.Run(ctx)
				case topo.ErrInterrupted:
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
