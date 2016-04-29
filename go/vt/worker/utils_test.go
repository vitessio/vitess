package worker

import (
	"fmt"
	"testing"

	"github.com/youtube/vitess/go/vt/wrangler"
)

// This file contains common test helper.

func runCommand(t *testing.T, wi *Instance, wr *wrangler.Wrangler, args []string) error {
	worker, done, err := wi.RunCommand(args, wr, false /* runFromCli */)
	if err != nil {
		return fmt.Errorf("Worker creation failed: %v", err)
	}
	if err := wi.WaitForCommand(worker, done); err != nil {
		return fmt.Errorf("Worker failed: %v", err)
	}

	t.Logf("Got status: %v", worker.StatusAsText())
	if worker.State() != WorkerStateDone {
		return fmt.Errorf("Worker finished but not successfully: %v", err)
	}
	return nil
}
