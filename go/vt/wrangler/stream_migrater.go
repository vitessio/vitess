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

package wrangler

import (
	"context"

	"vitess.io/vitess/go/vt/vtctl/workflow"
)

type streamMigrater struct {
	sm *workflow.StreamMigrator
}

func buildStreamMigrater(ctx context.Context, ts *trafficSwitcher, cancelMigrate bool) (*streamMigrater, error) {
	sm, err := workflow.BuildStreamMigrator(ctx, ts, cancelMigrate)
	if err != nil {
		return nil, err
	}

	return &streamMigrater{sm: sm}, nil
}

func (sm *streamMigrater) stopStreams(ctx context.Context) ([]string, error) {
	return sm.sm.StopStreams(ctx)
}

func (sm *streamMigrater) migrateStreams(ctx context.Context) error {
	return sm.sm.MigrateStreams(ctx)
}

func (sm *streamMigrater) cancelMigration(ctx context.Context) {
	sm.sm.CancelMigration(ctx)
}

// streamMigraterFinalize finalizes the stream migration.
// It's a standalone function because it does not use the streamMigrater state.
func streamMigraterfinalize(ctx context.Context, ts *trafficSwitcher, workflows []string) error {
	return workflow.StreamMigratorFinalize(ctx, ts, workflows)
}

func copyTabletStreams(in []*workflow.VReplicationStream) []*workflow.VReplicationStream {
	return workflow.VReplicationStreams(in).Copy().ToSlice()
}
