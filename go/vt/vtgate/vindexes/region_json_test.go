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

package vindexes

import (
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/log/logtest"
)

func TestRegionJSONLogsRegionMapPath(t *testing.T) {
	logs := logtest.Capture(t, slog.LevelInfo)
	path := filepath.Join(t.TempDir(), "region_map.json")
	require.NoError(t, os.WriteFile(path, []byte(`{"US": 1}`), 0o644))

	_, err := newRegionJSON("region_json", map[string]string{"region_map": path, "region_bytes": "1"})
	require.NoError(t, err)

	record := logs.Record(t, "Loaded Region map")
	assert.Equal(t, "INFO", record["level"])
	assert.Equal(t, path, record["path"])
}
