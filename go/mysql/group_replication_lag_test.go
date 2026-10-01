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

package mysql

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestGroupReplicationApplierLag(t *testing.T) {
	tests := []struct {
		name      string
		status    GroupReplicationApplierStatus
		wantLag   time.Duration
		wantKnown bool
	}{
		{name: "nothing queued", wantKnown: true},
		{name: "oldest transaction being applied", status: GroupReplicationApplierStatus{QueuedTransactions: 9, Applying: true, OldestApplying: 4 * time.Second}, wantLag: 4 * time.Second, wantKnown: true},
		{name: "primary clock behind", status: GroupReplicationApplierStatus{QueuedTransactions: 1, Applying: true, OldestApplying: -time.Second}, wantKnown: true},
		{name: "queued, not yet scheduled", status: GroupReplicationApplierStatus{QueuedTransactions: 2}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lag, known := tt.status.ApplierLag()
			assert.Equal(t, tt.wantLag, lag)
			assert.Equal(t, tt.wantKnown, known)
		})
	}
}

func TestGroupReplicationApplierStatusHasQuorum(t *testing.T) {
	assert.True(t, (&GroupReplicationApplierStatus{Members: 3, ReachableMembers: 3}).HasQuorum())
	assert.True(t, (&GroupReplicationApplierStatus{Members: 3, ReachableMembers: 2}).HasQuorum())
	assert.False(t, (&GroupReplicationApplierStatus{Members: 3, ReachableMembers: 1}).HasQuorum())
	assert.False(t, (&GroupReplicationApplierStatus{Members: 2, ReachableMembers: 1}).HasQuorum())
	assert.True(t, (&GroupReplicationApplierStatus{Members: 1, ReachableMembers: 1}).HasQuorum())
	assert.False(t, (&GroupReplicationApplierStatus{}).HasQuorum())
	assert.False(t, (*GroupReplicationApplierStatus)(nil).HasQuorum())
}
