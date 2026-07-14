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

package api

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"vitess.io/vitess/go/test/vitesst"
)

const (
	keyspaceName = "ks"
	shardName    = "0"
	cell1        = "zone1"
	cell2        = "zone2"
)

var clusterInstance *vitesst.Cluster

func TestMain(m *testing.M) {
	exitcode, err := func() (int, error) {
		ctx := context.Background()

		cluster, err := vitesst.NewCluster(
			vitesst.WithCells(cell1, cell2),
			vitesst.WithoutVTGate(),
			vitesst.WithVTOrc(),
			vitesst.WithKeyspace(keyspaceName).
				WithShardNames(shardName).
				WithReplicas(1).
				WithRDOnly(1).
				WithoutPrimaryElection().
				WithTabletSpec(func(spec *vitesst.TabletSpec) {
					spec.Cell = cell1
				}),
		)
		if err != nil {
			return 1, err
		}
		cleanup, err := cluster.Start(ctx)
		if err != nil {
			return 1, err
		}
		defer func() {
			if err := cleanup(ctx); err != nil {
				fmt.Fprintln(os.Stderr, "cluster teardown:", err)
			}
		}()

		// Clear super_read_only on every tablet so that writes issued directly
		// against a replica's mysqld succeed, then let VTOrc elect the primary.
		for _, tablet := range cluster.Tablets() {
			if _, err := tablet.QueryTabletWithDB(ctx, "SET GLOBAL super_read_only = OFF", ""); err != nil {
				return 1, err
			}
		}
		if err := cluster.WaitForHealthyShard(ctx, keyspaceName, shardName, 60*time.Second); err != nil {
			return 1, err
		}

		clusterInstance = cluster

		return m.Run(), nil
	}()

	if err != nil {
		fmt.Printf("%v\n", err)
		os.Exit(1)
	} else {
		os.Exit(exitcode)
	}
}
