package vttest

import (
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestVtcomboArguments(t *testing.T) {
	env := &LocalTestEnv{}
	args := env.VtcomboArguments()

	t.Run("service_map flag", func(t *testing.T) {
		require.Contains(t, args, "-service_map", "vttest.LocalTestEnv must provide `-service_map` flag to vtcombo")

		x := sort.SearchStrings(args, "-service_map")
		require.Less(t, x+1, len(args), "-service_map vtcombo flag (idx = %d) must take an argument. full arg list: %v", x, args)

		expectedServiceList := []string{
			"grpc-vtgateservice",
			"grpc-vtctl",
			"grpc-vtctld",
		}
		serviceMapList := strings.Split(args[x+1], ",")
		assert.ElementsMatch(t, expectedServiceList, serviceMapList, "-service_map list does not contain expected vtcombo services")
	})
}
