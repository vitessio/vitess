package vtctld

import (
	"flag"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/vt/servenv"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/vtctl"
	"github.com/youtube/vitess/go/vt/workflow"
)

var (
	workflowManagerInit = flag.Bool("workflow_manager_init", false, "Initialize the workflow manager in this vtctld instance.")
)

func initWorkflowManager(ts topo.Server) {
	if *workflowManagerInit {
		vtctl.WorkflowManager = workflow.NewManager(ts)

		// Register the long polling and websocket handlers.
		vtctl.WorkflowManager.HandleHTTPLongPolling(apiPrefix + "workflow")
		vtctl.WorkflowManager.HandleHTTPWebSocket(apiPrefix + "workflow")

		// FIXME(alainjobart) look at a flag to use master
		// election here.
		ctx, cancel := context.WithCancel(context.Background())
		go vtctl.WorkflowManager.Run(ctx)

		// Running cancel on Close will cancel the context of
		// any running workflow inside vtctld. They may still
		// checkpoint if they want to. We will wait for them
		// all to exit properly before returning from Close().
		servenv.OnClose(cancel)
	}
}
