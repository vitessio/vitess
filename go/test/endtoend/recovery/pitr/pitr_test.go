package pitr

import (
	"os/exec"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	binlogServer = "rippled"
)

func TestPointInTimeRecovery(t *testing.T) {
	tmpProcess := exec.Command(
		binlogServer,
		"--version",
	)
	output, err := tmpProcess.CombinedOutput()
	require.NoError(t, err)
	assert.True(t, strings.Contains(string(output), "Debug build"))
}
