/*
Copyright 2024 The Vitess Authors.

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

package opentsdb

import (
	"log/slog"
	"sync"
	"testing"

	"github.com/spf13/pflag"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/log/logtest"
	"vitess.io/vitess/go/vt/servenv"
)

func TestRegisterFlags(t *testing.T) {
	oldOpenTSDBURI := openTSDBURI
	defer func() {
		openTSDBURI = oldOpenTSDBURI
	}()

	fs := pflag.NewFlagSet("test", pflag.ExitOnError)

	registerFlags(fs)

	err := fs.Set("opentsdb-uri", "testURI")
	require.NoError(t, err)
	assert.Equal(t, "testURI", openTSDBURI)
}

// initOnce keeps Init from adding another run hook when the test is repeated.
var initOnce sync.Once

func TestInitFailureLogs(t *testing.T) {
	oldOpenTSDBURI := openTSDBURI
	t.Cleanup(func() { openTSDBURI = oldOpenTSDBURI })
	openTSDBURI = ""

	logs := logtest.Capture(t, slog.LevelInfo)
	initOnce.Do(func() { Init("") })
	servenv.FireRunHooks()
	record := logs.Record(t, "Failed to initialize singleton opentsdb backend")
	assert.Equal(t, "INFO", record["level"])
	assert.Equal(t, "cannot create opentsdb PushBackend with empty --opentsdb-uri", record["error"])
}
