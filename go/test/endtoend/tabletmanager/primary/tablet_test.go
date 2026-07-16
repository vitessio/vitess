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
package primary

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"

	"vitess.io/vitess/go/vitesst"
	"vitess.io/vitess/go/vt/grpcclient"
	"vitess.io/vitess/go/vt/vttablet/tabletconn"

	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"

	// Register the grpc queryservice dialer so StreamHealth works.
	_ "vitess.io/vitess/go/vt/vttablet/grpctabletconn"
)

var (
	clusterInstance *vitesst.Cluster
	primaryTablet   *vitesst.Tablet
	replicaTablet   *vitesst.Tablet
	keyspaceName    = "ks"
	shardName       = "0"
	tabletArgs      = []string{
		"--lock-tables-timeout", "5s",
		"--enable-replication-reporter",
	}
	tabletStatusTimeout = 60 * time.Second

	sqlSchema = `
	create table t1(
		id bigint,
		value varchar(16),
		primary key(id)
	) Engine=InnoDB;
`

	vSchema = `
	{
    "sharded": true,
    "vindexes": {
      "hash": {
        "type": "hash"
      }
    },
    "tables": {
      "t1": {
        "column_vindexes": [
          {
            "column": "id",
            "name": "hash"
          }
        ]
      }
    }
  }`
)

func setup(t *testing.T) {
	t.Helper()
	ctx := t.Context()

	cluster, err := vitesst.NewCluster(t,
		vitesst.WithVTTabletArgs(tabletArgs...),
		vitesst.WithKeyspace(keyspaceName).
			WithShardNames(shardName).
			WithReplicas(1).
			WithSchema(sqlSchema).
			WithVSchema(vSchema),
	)
	require.NoError(t, err)

	cleanup, err := cluster.Start(t, ctx)
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Minute)
		defer cancel()
		if cleanupErr := cleanup(cleanupCtx); cleanupErr != nil {
			t.Logf("cluster teardown: %v", cleanupErr)
		}
	})
	require.NoError(t, err)

	clusterInstance = cluster
	shard := cluster.Keyspace(keyspaceName).Shard(shardName)
	primaryTablet = shard.Primary()
	replicaTablet = shard.Replicas()[0]
}

func TestRepeatedInitShardPrimary(t *testing.T) {
	setup(t)

	// Test that using InitShardPrimary can go back and forth between 2 hosts.
	ctx := t.Context()

	// Make replica tablet as primary
	err := initShardPrimary(ctx, replicaTablet)
	require.NoError(t, err)

	// Run health check on both, make sure they are both healthy.
	// Also make sure the types are correct.
	err = clusterInstance.Vtctld().ExecuteCommand(ctx, "RunHealthCheck", primaryTablet.Alias())
	require.NoError(t, err)
	checkHealth(t, primaryTablet, false)

	err = clusterInstance.Vtctld().ExecuteCommand(ctx, "RunHealthCheck", replicaTablet.Alias())
	require.NoError(t, err)
	checkHealth(t, replicaTablet, false)

	checkTabletType(t, primaryTablet.Alias(), "REPLICA")
	checkTabletType(t, replicaTablet.Alias(), "PRIMARY")

	// Come back to the original tablet.
	err = initShardPrimary(ctx, primaryTablet)
	require.NoError(t, err)

	// Run health check on both, make sure they are both healthy.
	// Also make sure the types are correct.
	err = clusterInstance.Vtctld().ExecuteCommand(ctx, "RunHealthCheck", primaryTablet.Alias())
	require.NoError(t, err)
	checkHealth(t, primaryTablet, false)

	err = clusterInstance.Vtctld().ExecuteCommand(ctx, "RunHealthCheck", replicaTablet.Alias())
	require.NoError(t, err)
	checkHealth(t, replicaTablet, false)

	checkTabletType(t, primaryTablet.Alias(), "PRIMARY")
	checkTabletType(t, replicaTablet.Alias(), "REPLICA")
}

func TestPrimaryRestartSetsPTSTimestamp(t *testing.T) {
	setup(t)

	// Test that PTS timestamp is set when we restart the PRIMARY vttablet.
	// PTS = PrimaryTermStart.
	// See StreamHealthResponse.primary_term_start_timestamp for details.
	ctx := t.Context()

	// Make replica as primary
	err := initShardPrimary(ctx, replicaTablet)
	require.NoError(t, err)

	err = replicaTablet.WaitForTabletStatus(ctx, tabletStatusTimeout, "SERVING")
	require.NoError(t, err)

	// Capture the current PTS.
	shrs, err := streamTabletHealth(ctx, replicaTablet, 1)
	require.NoError(t, err)

	streamHealthRes1 := shrs[0]
	actualType := streamHealthRes1.GetTarget().GetTabletType()
	tabletType := topodatapb.TabletType_value["PRIMARY"]
	got := fmt.Sprintf("%d", actualType)
	want := strconv.Itoa(int(tabletType))
	assert.Equal(t, want, got)
	assert.NotNil(t, streamHealthRes1.GetPrimaryTermStartTimestamp())
	assert.True(t, streamHealthRes1.GetPrimaryTermStartTimestamp() > 0,
		"PTS on PRIMARY must be set after InitShardPrimary")

	// Restart the PRIMARY vttablet and test again

	// kill the newly promoted primary tablet
	err = replicaTablet.StopVttablet(ctx)
	require.NoError(t, err)

	// Start Vttablet
	err = replicaTablet.StartVttablet(ctx, tabletArgs...)
	require.NoError(t, err)

	// Make sure that the PTS did not change
	shrs, err = streamTabletHealth(ctx, replicaTablet, 1)
	require.NoError(t, err)

	streamHealthRes2 := shrs[0]

	actualType = streamHealthRes2.GetTarget().GetTabletType()
	tabletType = topodatapb.TabletType_value["PRIMARY"]
	got = fmt.Sprintf("%d", actualType)
	want = strconv.Itoa(int(tabletType))
	assert.Equal(t, want, got)

	assert.NotNil(t, streamHealthRes2.GetPrimaryTermStartTimestamp())
	assert.True(t, streamHealthRes2.GetPrimaryTermStartTimestamp() == streamHealthRes1.GetPrimaryTermStartTimestamp(),
		fmt.Sprintf("When the PRIMARY vttablet was restarted, "+
			"the PTS timestamp must be set by reading the old value from the tablet record. Old: %d, New: %d",
			streamHealthRes1.GetPrimaryTermStartTimestamp(),
			streamHealthRes2.GetPrimaryTermStartTimestamp()))

	// Reset primary
	err = initShardPrimary(ctx, primaryTablet)
	require.NoError(t, err)
	err = primaryTablet.WaitForTabletStatus(ctx, tabletStatusTimeout, "SERVING")
	require.NoError(t, err)
}

// initShardPrimary promotes the given tablet to primary of its shard.
func initShardPrimary(ctx context.Context, tablet *vitesst.Tablet) error {
	return clusterInstance.Vtctld().ExecuteCommand(ctx,
		"InitShardPrimary",
		"--force", "--wait-replicas-timeout", "31s",
		fmt.Sprintf("%s/%s", keyspaceName, shardName),
		tablet.Alias())
}

// streamTabletHealth invokes a HealthStream on a tablet and returns the
// responses. It returns an error if the stream ends with fewer than count
// responses.
func streamTabletHealth(ctx context.Context, tablet *vitesst.Tablet, count int) (responses []*querypb.StreamHealthResponse, err error) {
	proto, err := tablet.TabletProto(ctx)
	if err != nil {
		return nil, err
	}

	conn, err := tabletconn.GetDialer()(ctx, proto, grpcclient.FailFast(false))
	if err != nil {
		return nil, err
	}

	i := 0
	err = conn.StreamHealth(ctx, func(shr *querypb.StreamHealthResponse) error {
		responses = append(responses, shr)
		i++
		if i >= count {
			return io.EOF
		}
		return nil
	})

	switch {
	case err != nil:
		return nil, err
	case len(responses) < count:
		return nil, errors.New("stream ended early")
	}
	return responses, nil
}

func checkHealth(t *testing.T, tablet *vitesst.Tablet, shouldError bool) {
	status, _, err := tablet.MakeAPICall(t.Context(), "/healthz")
	require.NoError(t, err)
	if shouldError {
		assert.True(t, status > 400)
	} else {
		assert.Equal(t, 200, status)
	}
}

func checkTabletType(t *testing.T, tabletAlias string, typeWant string) {
	out, err := clusterInstance.Vtctld().ExecuteCommandWithOutput(t.Context(), "GetTablet", tabletAlias)
	require.NoError(t, err)
	tablet := &topodatapb.Tablet{}
	err = protojson.Unmarshal([]byte(out), tablet)
	require.NoError(t, err)

	actualType := tablet.GetType()
	got := fmt.Sprintf("%d", actualType)

	tabletType := topodatapb.TabletType_value[typeWant]
	want := strconv.Itoa(int(tabletType))

	assert.Equal(t, want, got)
}
