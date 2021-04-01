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

package reparent

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/test/endtoend/cluster"
)

var (
	masterTablet  *cluster.Vttablet
	replicaTablet *cluster.Vttablet
)

func TestReparentGracefulRangeBased(t *testing.T) {
	defer cluster.PanicHandler(t)
	ctx := context.Background()

	shardName = "0000000000000000-ffffffffffffffff"
	defer func() { shardName = "0" }()

	setupRangeBasedCluster(ctx, t)
	defer teardownCluster()

	// Perform a graceful reparent operation
	_, err := prs(t, replicaTablet)
	require.NoError(t, err)
	validateTopology(t, false)
	checkMasterTablet(t, replicaTablet)
	confirmReplication(t, replicaTablet, []*cluster.Vttablet{masterTablet})
}
