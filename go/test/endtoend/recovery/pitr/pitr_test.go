package pitr

import (
	"flag"
	"os"
	"os/exec"
	"path"
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

	exePath := path.Join(os.Getenv("EXTRA_BIN"), binlogServer)
	tmpProcess := exec.Command(
		exePath,
		"--version",
	)
	output, err := tmpProcess.CombinedOutput()
	require.NoError(t, err)
	assert.True(t, strings.Contains(string(output), "Debug build"))
}
