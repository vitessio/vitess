/*
Copyright 2022 The Vitess Authors.

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

package flags

// These tests ensure that we are changing flags intentionally and do not accidentally make
// changes such as removing a flag. Since there's no way to test the command-line
// flag handling portion explicitly in the unit tests we do so here.

import (
	"bytes"
	"context"
	_ "embed"
	"os"
	"os/exec"
	"testing"
	"text/template"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	//go:embed mysqlctl.txt
	mysqlctlTxt string

	//go:embed mysqlctld.txt
	mysqlctldTxt string

	//go:embed topo2topo.txt
	topo2topoTxt string

	//go:embed vtaclcheck.txt
	vtaclcheckTxt string

	//go:embed vtcombo.txt
	vtcomboTxt string

	//go:embed vtexplain.txt
	vtexplainTxt string

	//go:embed vtgate.txt
	vtgateTxt string

	//go:embed vttablet.txt
	vttabletTxt string

	//go:embed vttlstest.txt
	vttlstestTxt string

	//go:embed vtctld.txt
	vtctldTxt string

	//go:embed vtorc.txt
	vtorcTxt string

	//go:embed vtctlclient.txt
	vtctlclientTxt string

	//go:embed vtctldclient.txt
	vtctldclientTxt string

	//go:embed vtgateclienttest.txt
	vtgateclienttestTxt string

	//go:embed vttestserver.txt
	vttestserverTxt string

	//go:embed zkctld.txt
	zkctldTxt string

	//go:embed vtbackup.txt
	vtbackupTxt string

	//go:embed zkctl.txt
	zkctlTxt string

	//go:embed zk.txt
	zkTxt string

	helpOutput = map[string]string{
		"mysqlctl":         mysqlctlTxt,
		"mysqlctld":        mysqlctldTxt,
		"topo2topo":        topo2topoTxt,
		"vtaclcheck":       vtaclcheckTxt,
		"vtbackup":         vtbackupTxt,
		"vtcombo":          vtcomboTxt,
		"vtctlclient":      vtctlclientTxt,
		"vtctld":           vtctldTxt,
		"vtctldclient":     vtctldclientTxt,
		"vtexplain":        vtexplainTxt,
		"vtgate":           vtgateTxt,
		"vtgateclienttest": vtgateclienttestTxt,
		"vtorc":            vtorcTxt,
		"vttablet":         vttabletTxt,
		"vttestserver":     vttestserverTxt,
		"vttlstest":        vttlstestTxt,
		"zk":               zkTxt,
		"zkctl":            zkctlTxt,
		"zkctld":           zkctldTxt,
	}
)

func TestHelpOutput(t *testing.T) {
	wd, err := os.Getwd()
	require.NoError(t, err)
	t.Parallel()
	args := []string{"--help"}
	for binary, helptext := range helpOutput {
		t.Run(binary, func(t *testing.T) {
			tmpl, err := template.New(binary).Parse(helptext)
			require.NoError(t, err)

			var buf bytes.Buffer
			err = tmpl.Execute(&buf, struct {
				Workdir string
			}{
				Workdir: wd,
			})
			require.NoError(t, err)

			cmd := exec.Command(binary, args...)
			output := bytes.Buffer{}
			cmd.Stderr = &output
			cmd.Stdout = &output
			err = cmd.Run()
			require.NoError(t, err)
			require.Equal(t, buf.String(), output.String())
		})
	}
}

// TestVtctldHTTPAPIFlagsRemoved checks argument parsing, since these flags were
// already hidden from help while they were accepted as deprecated no-ops.
func TestVtctldHTTPAPIFlagsRemoved(t *testing.T) {
	flags := []struct {
		name  string
		value string
	}{
		{"cell", "zone1"},
		{"proxy-tablets", "true"},
		{"proxy_tablets", "true"},
		{"action-timeout", "1s"},
		{"action_timeout", "1s"},
		{"tablet-health-keep-alive", "1s"},
		{"tablet_health_keep_alive", "1s"},
	}

	for _, binary := range []string{"vtctld", "vtcombo"} {
		t.Run(binary, func(t *testing.T) {
			// Help exits after argument parsing, without starting server services.
			// The remaining flag from the shared vtctld registration must work.
			output, err := runFlagCommand(t, binary, "--vtctld-sanitize-log-messages=true", "--help")
			require.NoError(t, err, "%s", output)

			for _, flag := range flags {
				t.Run(flag.name, func(t *testing.T) {
					output, err := runFlagCommand(t, binary, "--"+flag.name+"="+flag.value, "--help")
					var exitErr *exec.ExitError
					require.ErrorAs(t, err, &exitErr, "expected rejection of --%s", flag.name)
					assert.Equal(t, 1, exitErr.ExitCode(), "%s", output)
					assert.Contains(t, output, "unknown flag: --"+flag.name)
				})
			}
		})
	}
}

func runFlagCommand(t *testing.T, binary string, args ...string) (string, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	t.Cleanup(cancel)
	output, err := exec.CommandContext(ctx, binary, args...).CombinedOutput()
	require.NoError(t, ctx.Err(), "%s timed out: %s", binary, output)
	return string(output), err
}
