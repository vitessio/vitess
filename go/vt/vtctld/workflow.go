package vtctld

import (
	"flag"
	"time"

	"context"

	"vitess.io/vitess/go/trace"

	"vitess.io/vitess/go/flagutil"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vtctl"
	"vitess.io/vitess/go/vt/workflow"
	"vitess.io/vitess/go/vt/workflow/resharding"
	"vitess.io/vitess/go/vt/workflow/reshardingworkflowgen"
	"vitess.io/vitess/go/vt/workflow/topovalidator"
)

var (
	workflowManagerInit        = flag.Bool("workflow_manager_init", false, "Initialize the workflow manager in this vtctld instance.")
	workflowManagerUseElection = flag.Bool("workflow_manager_use_election", false, "if specified, will use a topology server-based master election to ensure only one workflow manager is active at a time.")
	workflowManagerDisable     flagutil.StringListValue
)

func init() {
	flag.Var(&workflowManagerDisable, "workflow_manager_disable", "comma separated list of workflow types to disable")
}

func initWorkflowManager(ts *topo.Server) {
	if *workflowManagerInit {
		// Uncomment this line to register the UI test validator.
		// topovalidator.RegisterUITestValidator()

		// Register the Topo Validators, and the workflow.
		topovalidator.RegisterKeyspaceValidator()
		topovalidator.RegisterShardValidator()
		topovalidator.Register()

		// Register the Horizontal Resharding workflow.
		resharding.Register()

		// Register workflow that generates Horizontal Resharding workflows.
		reshardingworkflowgen.Register()

		// Unregister the disabled workflows.
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
