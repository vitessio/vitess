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

package tabletmanager

import (
	"context"
	"testing"
)

func TestTabletExternallyReparentedAlwaysUpdatesTimestamp(t *testing.T) {
	ctx := context.Background()
	agent := createTestAgent(ctx, t, nil)

	// Initial call sets the timestamp.
	if err := agent.TabletExternallyReparented(ctx, "unused_id"); err != nil {
		t.Fatal(err)
	}
	if agent._masterTermStartTime.IsZero() {
		t.Fatalf("master_term_start_time should have been updated")
	}

	// Run RPC again and verify that the timestamp was updated.
	ter1 := agent._masterTermStartTime
	if err := agent.TabletExternallyReparented(ctx, "unused_id"); err != nil {
		t.Fatal(err)
	}
	ter2 := agent._masterTermStartTime
	if ter1 == ter2 {
		t.Fatalf("subsequent TER call did not update the master_term_start_time: %v = %v", ter1, ter2)
	}
}
