/*
Copyright 2021 The Vitess Authors.

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

package vttest

import (
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/utils"
)

func TestVtcomboArguments(t *testing.T) {
	env := &LocalTestEnv{}
	args := env.VtcomboArguments()

	serviceMapFlag := utils.GetFlagVariantForTests("--service-map")
	t.Run("service-map flag", func(t *testing.T) {
		require.Contains(t, args, serviceMapFlag, fmt.Sprintf("vttest.LocalTestEnv must provide `%s` flag to vtcombo", serviceMapFlag))

		x := sort.SearchStrings(args, serviceMapFlag)
		require.Less(t, x+1, len(args), "%s vtcombo flag (idx = %d) must take an argument. full arg list: %v", serviceMapFlag, x, args)

		expectedServiceList := []string{
			"grpc-vtgateservice",
			"grpc-vtctl",
			"grpc-vtctld",
		}
		serviceMapList := strings.Split(args[x+1], ",")
		assert.ElementsMatch(t, expectedServiceList, serviceMapList, fmt.Sprintf("%s list does not contain expected vtcombo services", serviceMapFlag))
	})
}
