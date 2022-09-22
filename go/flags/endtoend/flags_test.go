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
	_ "embed"
	"os/exec"
	"testing"

	"vitess.io/vitess/go/test/utils"

	"github.com/stretchr/testify/require"
)

var (
	//go:embed vtexplain.txt
	vtexplainTxt string

	//go:embed vtgate.txt
	vtgateTxt string

	//go:embed vtgr.txt
	vtgrTxt string

	//go:embed vttablet.txt
	vttabletTxt string

	//go:embed vtctld.txt
	vtctldTxt string

	//go:embed vtorc.txt
	vtorcTxt string

	helpOutput = map[string]string{
		"vtexplain": vtexplainTxt,
		"vtgate":    vtgateTxt,
		"vtgr":      vtgrTxt,
		"vttablet":  vttabletTxt,
		"vtctld":    vtctldTxt,
		"vtorc":     vtorcTxt,
	}
)

func TestHelpOutput(t *testing.T) {
	args := []string{"--help"}
	for binary, helptext := range helpOutput {
		t.Run(binary, func(t *testing.T) {
			cmd := exec.Command(binary, args...)
			output := bytes.Buffer{}
			cmd.Stderr = &output
			err := cmd.Run()
			require.NoError(t, err)
			utils.MustMatch(t, helptext, output.String())
		})
	}
}
