/*
Copyright 2026 The Vitess Authors.

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

package vreplication

import (
	"fmt"
	"os"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/test/endtoend/cluster"
)

// parallelApplyEnabledOnTablets returns true if the vttablets are started with
// a default of more than one parallel replication worker.
func parallelApplyEnabledOnTablets() bool {
	return slices.Contains(extraVTTabletArgs, parallelReplicationWorkers)
}

// requireParallelApplierStarted waits for the vttablet's log to show that the
// parallel applier started with the given number of workers. This confirms that
// the parallel path ran, rather than silently falling back to the serial applier.
// If workflow is not empty then only a stream of that workflow counts.
func requireParallelApplierStarted(t *testing.T, tablet *cluster.VttabletProcess, workflow string, workers int) {
	t.Helper()
	workersRE := regexp.MustCompile(fmt.Sprintf(`\bworkers=%d\b`, workers))
	require.Eventually(t, func() bool {
		logBytes, err := os.ReadFile(tablet.ErrorLog)
		if err != nil {
			return false
		}
		for line := range strings.SplitSeq(string(logBytes), "\n") {
			if !strings.Contains(line, "Parallel applier started") || !workersRE.MatchString(line) {
				continue
			}
			if workflow == "" || strings.Contains(line, "workflow="+workflow+" ") {
				return true
			}
		}
		return false
	}, workflowStateTimeout, 500*time.Millisecond,
		"tablet %s did not log that the parallel applier started with %d workers (workflow %q)", tablet.TabletPath, workers, workflow)
}
