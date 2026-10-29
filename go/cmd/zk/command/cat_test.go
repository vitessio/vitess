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

package command

import (
	"context"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/cmd/zk/internal/zkfs"
	"vitess.io/vitess/go/vt/log/logtest"
	"vitess.io/vitess/go/vt/topo/zk2topo"
)

// TestCatLogsPathAndError checks that a node that can't be read is logged
// with its path and the error as attributes. The context is canceled, so no
// ZooKeeper server is contacted.
func TestCatLogsPathAndError(t *testing.T) {
	oldFs := fs
	t.Cleanup(func() { fs = oldFs })
	fs = &zkfs.FS{Conn: zk2topo.Connect("localhost:0")}

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	Cat.SetContext(ctx)
	require.NoError(t, Cat.Flags().Parse([]string{"/zk/test/node"}))

	logs := logtest.Capture(t, slog.LevelInfo)
	require.EqualError(t, commandCat(Cat, nil), "cat: some paths had errors")

	record := logs.Record(t, "cat: cannot access")
	assert.Equal(t, "WARN", record["level"])
	assert.Equal(t, "/zk/test/node", record["path"])
	assert.Equal(t, context.Canceled.Error(), record["error"])
}
