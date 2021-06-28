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

package tabletmanager

import (
	"strings"
	"testing"

	"vitess.io/vitess/go/test/endtoend/cluster"

	"github.com/stretchr/testify/require"
)

func TestShutdown(t *testing.T) {
	defer cluster.PanicHandler(t)
	// initially check that all the tablets exist in the topology
	checkTabletExistenceInTopo(t, []string{masterTablet.Alias, replicaTablet.Alias, rdonlyTablet.Alias}, nil)

	// shutdown the vttablet process of the replica tablet
	err := replicaTablet.VttabletProcess.TearDown()
	require.NoError(t, err)
	defer func() {
		_ = replicaTablet.VttabletProcess.Setup()
	}()

	// check that its record in the topology has deleted
	checkTabletExistenceInTopo(t, []string{masterTablet.Alias, rdonlyTablet.Alias}, []string{replicaTablet.Alias})

	// now shutdown the read only tablet
	err = rdonlyTablet.VttabletProcess.TearDown()
	require.NoError(t, err)
	defer func() {
		_ = rdonlyTablet.VttabletProcess.Setup()
	}()

	// check that its record is also deleted
	checkTabletExistenceInTopo(t, []string{masterTablet.Alias}, []string{replicaTablet.Alias, rdonlyTablet.Alias})

	// now shutdown the master tablet
	err = masterTablet.VttabletProcess.TearDown()
	require.NoError(t, err)
	defer func() {
		_ = masterTablet.VttabletProcess.Setup()
	}()

	// check that its record is not deleted
	// we do not delete the record of the master tablet
	checkTabletExistenceInTopo(t, []string{masterTablet.Alias}, []string{replicaTablet.Alias, rdonlyTablet.Alias})
}

// getAllTabletsInTopo gets all the tablets in the topology for the cell
func getAllTabletsInTopo(t *testing.T) string {
	out, err := clusterInstance.VtctlProcess.ExecuteCommandWithOutput("ListAllTablets", cell)
	require.NoError(t, err)
	return out
}

// checkTabletExistenceInTopo checks that the tablets exist in the topo server
// it takes 2 lists of tablet aliases to check for, one that should be present and the other which should not be
func checkTabletExistenceInTopo(t *testing.T, tabletAliasesPresent []string, tabletAliasesAbsent []string) {
	res := getAllTabletsInTopo(t)
	for _, alias := range tabletAliasesPresent {
		isPresent := strings.Contains(res, alias)
		require.True(t, isPresent, "tablet alias %s should be present but is not", alias)
	}

	for _, alias := range tabletAliasesAbsent {
		isPresent := strings.Contains(res, alias)
		require.False(t, isPresent, "tablet alias %s should not be present but is", alias)
	}
}
