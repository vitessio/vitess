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

package cli

import (
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/log/logtest"
	"vitess.io/vitess/go/vt/mysqlctl"
)

// TestRunLogsExistingMycnf checks that the my.cnf path is an attribute of the
// record logged when mysqlctld starts from an existing my.cnf. The my.cnf is
// empty, so run fails right after.
func TestRunLogsExistingMycnf(t *testing.T) {
	t.Setenv("VTDATAROOT", t.TempDir())
	oldTabletUID := tabletUID
	t.Cleanup(func() { tabletUID = oldTabletUID })
	tabletUID = 4242
	mycnfFile := mysqlctl.MycnfFile(tabletUID)
	require.NoError(t, os.MkdirAll(filepath.Dir(mycnfFile), 0o755))
	require.NoError(t, os.WriteFile(mycnfFile, nil, 0o644))

	logs := logtest.Capture(t, slog.LevelInfo)
	Main.SetContext(t.Context())
	require.Error(t, run(Main, nil))

	record := logs.Record(t, "mycnf file already exists, starting without init")
	assert.Equal(t, "INFO", record["level"])
	assert.Equal(t, mycnfFile, record["path"])
}
