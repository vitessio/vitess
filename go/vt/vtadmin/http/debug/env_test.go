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

package debug

import (
	"testing"

	"github.com/spf13/pflag"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/flagutil"
	vtadmindebug "vitess.io/vitess/go/vt/vtadmin/debug"
)

func TestReadEnvHonorsASinglyPassedFlag(t *testing.T) {
	t.Setenv("VTADMIN_TEST_OMIT", "omitted-value")
	t.Setenv("VTADMIN_TEST_SANITIZE", "sanitized-value")

	OmitEnv, SanitizeEnv = flagutil.StringSetFlag{}, flagutil.StringSetFlag{}
	t.Cleanup(func() {
		OmitEnv, SanitizeEnv = flagutil.StringSetFlag{}, flagutil.StringSetFlag{}
	})

	fs := pflag.NewFlagSet("vtadmin", pflag.ContinueOnError)
	fs.Var(&OmitEnv, "http-debug-omit-env", "")
	fs.Var(&SanitizeEnv, "http-debug-sanitize-env", "")
	require.NoError(t, fs.Parse([]string{
		"--http-debug-omit-env", "VTADMIN_TEST_OMIT",
		"--http-debug-sanitize-env", "VTADMIN_TEST_SANITIZE",
	}))

	env := make(map[string]string)
	for _, kv := range readEnv() {
		env[kv[0]] = kv[1]
	}

	// Assert per key: failing on the whole map would print the entire environment.
	_, omitted := env["VTADMIN_TEST_OMIT"]
	require.False(t, omitted, "VTADMIN_TEST_OMIT should have been omitted from the env listing")
	require.Equal(t, vtadmindebug.SanitizeString("sanitized-value"), env["VTADMIN_TEST_SANITIZE"])
}
