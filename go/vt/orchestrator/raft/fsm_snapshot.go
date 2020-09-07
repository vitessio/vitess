/*
   Copyright 2017 Shlomi Noach, GitHub Inc.

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

package orcraft

import (
	"vitess.io/vitess/go/vt/orchestrator/external/raft"
)

// fsmSnapshot handles raft persisting of snapshots
type fsmSnapshot struct {
	snapshotCreatorApplier SnapshotCreatorApplier
}

func newFsmSnapshot(snapshotCreatorApplier SnapshotCreatorApplier) *fsmSnapshot {
	return &fsmSnapshot{
		snapshotCreatorApplier: snapshotCreatorApplier,
	}
}

// Persist
func (f *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	data, err := f.snapshotCreatorApplier.GetData()
	if err != nil {
		return err
	}
	if _, err := sink.Write(data); err != nil {
		return err
	}
	return sink.Close()
}

// Release
func (f *fsmSnapshot) Release() {
}
