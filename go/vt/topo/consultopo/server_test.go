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

package consultopo

import (
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/log/logtest"
)

func TestNewServerLogsCellWithoutClientAuth(t *testing.T) {
	credsFile := filepath.Join(t.TempDir(), "creds.json")
	require.NoError(t, os.WriteFile(credsFile, []byte(`{"other_cell": {"acl_token": "123456"}}`), 0o600))
	oldCredsFile := consulAuthClientStaticFile
	consulAuthClientStaticFile = credsFile
	t.Cleanup(func() { consulAuthClientStaticFile = oldCredsFile })

	logs := logtest.Capture(t, slog.LevelInfo)
	s, err := NewServer("test_cell", "127.0.0.1:1", "/vitess")
	require.NoError(t, err)
	defer s.Close()

	record := logs.Record(t, "Client auth not configured for cell")
	assert.Equal(t, "WARN", record["level"])
	assert.Equal(t, "test_cell", record["cell"])
}
