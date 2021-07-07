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

package reparent

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/test/stress"
	"vitess.io/vitess/go/vt/log"
)

func TestStressReparentDownMaster(t *testing.T) {
	defer cluster.PanicHandler(t)
	setupReparentCluster(t)
	defer teardownCluster()

	err := clusterInstance.StartVtgate()
	require.NoError(t, err)

	// connects to vtgate
	params := mysql.ConnParams{Port: clusterInstance.VtgateMySQLPort, Host: "localhost", DbName: "ks"}
	s := stress.New(t, &params, 60*time.Second, false).Start()

	// Make the current master agent and database unavailable.
	stopTablet(t, tab1, true)

	// Perform a planned reparent operation, will try to contact
	// the current master and fail somewhat quickly
	_, err = prsWithTimeout(t, tab2, false, "1s", "5s")
	require.Error(t, err)

	validateTopology(t, false)

	// Run forced reparent operation, this should now proceed unimpeded.
	out, err := ers(t, tab2, "30s")
	log.Infof("EmergencyReparentShard Output: %v", out)
	require.NoError(t, err)

	// Now we'll manually remove it, simulating a human cleaning up a dead master.
	deleteTablet(t, tab1)

	// Now validate topo is correct.
	validateTopology(t, false)
	checkMasterTablet(t, tab2)
	confirmReplication(t, tab2, []*cluster.Vttablet{tab3, tab4})

	s.Wait(10 * time.Second)
}
