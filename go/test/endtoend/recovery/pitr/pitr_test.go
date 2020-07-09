package pitr

import (
	"flag"
	"os/exec"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"vitess.io/vitess/go/test/endtoend/cluster"
)

const (
	binlogServer = "rippled"
)

func TestPointInTimeRecovery(t *testing.T) {
	defer cluster.PanicHandler(nil)
	flag.Parse()

	tmpProcess := exec.Command(
		binlogServer,
		"--version",
	)
	output, err := tmpProcess.CombinedOutput()
	require.NoError(t, err)
	assert.True(t, strings.Contains(string(output), "Debug build"))
}
