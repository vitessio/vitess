/*
Copyright 2025 The Vitess Authors.

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

package engine

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/servenv"
)

// TestPlanSwitcherMetrics tests that the PlanSwitcher increments the correct metric
func TestPlanSwitcherMetrics(t *testing.T) {
	p := &PlanSwitcher{
		Optimized: &TransactionStatus{},
	}

	vc := &loggingVCursor{
		metrics: InitMetrics(servenv.NewExporter("PlanTest", "")),
	}
	initial := vc.metrics.optimizedQueryExec.Counts()
	_, err := p.TryExecute(context.Background(), vc, nil, false)
	require.NoError(t, err)
	after := vc.metrics.optimizedQueryExec.Counts()
	require.EqualValues(t, 1, after["MultiShard"]-initial["MultiShard"])
}
