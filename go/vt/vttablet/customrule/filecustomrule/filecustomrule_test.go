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

package filecustomrule

import (
	"log/slog"
	"os"
	"path"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/log/logtest"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/rules"
	"vitess.io/vitess/go/vt/vttablet/tabletservermock"
)

var customRule1 = `[
				{
					"Name": "r1",
					"Description": "disallow bindvar 'asdfg'",
					"BindVarConds":[{
						"Name": "asdfg",
						"OnAbsent": false,
						"Operator": ""
					}]
				}
			]`

func TestFileCustomRule(t *testing.T) {
	tqsc := tabletservermock.NewController()

	var qrs *rules.Rules
	rulepath := path.Join(os.TempDir(), ".customrule.json")
	// Set r1 and try to get it back
	err := os.WriteFile(rulepath, []byte(customRule1), os.FileMode(0o644))
	require.NoErrorf(t, err, "Cannot write r1 to rule file %s, err=%v", rulepath, err)

	fcr := NewFileCustomRule()
	// Let FileCustomRule to build rule from the local file
	err = fcr.Open(tqsc, rulepath)
	require.NoError(t, err)
	// Fetch query rules we built to verify correctness
	qrs, _, err = fcr.GetRules()
	require.NoError(t, err)
	qr := qrs.Find("r1")
	require.NotNilf(t, qr, "Expect custom rule r1 to be found, but got nothing, qrs=%v", qrs)
}

func TestParseRulesLogs(t *testing.T) {
	logs := logtest.Capture(t, slog.LevelInfo)
	dir := t.TempDir()

	missing := path.Join(dir, "missing.json")
	_, err := ParseRules(missing)
	require.Error(t, err)
	record := logs.Record(t, "Error reading file")
	assert.Equal(t, missing, record["path"])
	assert.Equal(t, err.Error(), record["error"])

	invalid := path.Join(dir, "invalid.json")
	require.NoError(t, os.WriteFile(invalid, []byte("not json"), 0o644))
	_, err = ParseRules(invalid)
	require.Error(t, err)
	record = logs.Record(t, "Error unmarshaling query rules")
	assert.Equal(t, invalid, record["path"])
	assert.Equal(t, err.Error(), record["error"])
	assert.Equal(t, "INVALID_ARGUMENT", record["error_code"])
}
