package resharding

import (
	"context"
	"flag"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/youtube/vitess/go/vt/logutil"
	"github.com/youtube/vitess/go/vt/worker/fakevtworkerclient"
	"github.com/youtube/vitess/go/vt/worker/vtworkerclient"
	"github.com/youtube/vitess/go/vt/wrangler"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

func TestHorizontalResharding(t *testing.T) {
	// Create fake wrangler using mock interface, which is used for the unit test in steps CopySchema and MigratedServedType.
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockWranglerInterface := NewMockWrangler(ctrl)
	mockWranglerInterface.EXPECT().CopySchemaShardFromShard(
		context.TODO(),
		nil,  /* tableArray*/
		nil,  /* excludeTableArray */
		true, /*includeViews*/
		"test_keyspace",
		"0",
		"test_keyspace",
		"-80",
		wrangler.DefaultWaitSlaveTimeout).Return(nil)

	mockWranglerInterface.EXPECT().CopySchemaShardFromShard(
		context.TODO(),
		nil,  /* tableArray*/
		nil,  /* excludeTableArray */
		true, /*includeViews*/
		"test_keyspace",
		"0",
		"test_keyspace",
		"80-",
		wrangler.DefaultWaitSlaveTimeout).Return(nil)

	mockWranglerInterface.EXPECT().WaitForFilteredReplication(context.TODO(), "test_keyspace", "-80").Return(nil)
	mockWranglerInterface.EXPECT().WaitForFilteredReplication(context.TODO(), "test_keyspace", "80-").Return(nil)

	servedTypeParams := []topodatapb.TabletType{topodatapb.TabletType_RDONLY,
		topodatapb.TabletType_REPLICA,
		topodatapb.TabletType_MASTER}
	for _, servedType := range servedTypeParams {
		mockWranglerInterface.EXPECT().MigrateServedTypes(
			context.TODO(),
			"test_keyspace",
			"0",
			nil, /* cells */
			servedType,
			false, /* reverse */
			false, /* skipReFreshState */
			wrangler.DefaultFilteredReplicationWaitTime).Return(nil)
	}

	// Create fakeworkerclient, which is used for the unit test in steps SplitClone and SplitDiff.
	fakeVtworkerClient := fakevtworkerclient.NewFakeVtworkerClient()
	vtworkerclient.RegisterFactory("fake", fakeVtworkerClient.FakeVtworkerClientFactory)
	defer vtworkerclient.UnregisterFactoryForTest("fake")
	flag.Set("vtworker_client_protocol", "fake")
	fakeVtworkerClient.RegisterResultForAddr("localhost:15032", []string{"SplitClone", "--min_healthy_rdonly_tablets=1", "test_keyspace/0"}, "", nil)
	fakeVtworkerClient.RegisterResultForAddr("localhost:15032", []string{"SplitDiff", "--min_healthy_rdonly_tablets=1", "test_keyspace/-80"}, "", nil)
	fakeVtworkerClient.RegisterResultForAddr("localhost:15032", []string{"SplitDiff", "--min_healthy_rdonly_tablets=1", "test_keyspace/80-"}, "", nil)

	// Create the workflow (ignore the node construction since we don't test the front-end part in this unit test).
	hw := &HorizontalReshardingWorkflow{
		keyspace:  "test_keyspace",
		vtworkers: []string{"localhost:15032"},
		wr:        mockWranglerInterface,
		logger:    logutil.NewMemoryLogger(),
	}

	perShard := &PerShardHorizontalResharding{
		data: &PerShardHorizontalReshardingData{
			Keyspace:          "test_keyspace",
			SourceShard:       "0",
			DestinationShards: []string{"-80", "80-"},
			Vtworker:          "localhost:15032",
		},
	}
	perShard.parent = hw
	hw.subWorkflows = append(hw.subWorkflows, perShard)

	if err := hw.executeWorkflow(); err != nil {
		t.Errorf("%s: Horizontal resharding workflow should not fail", err)
	}
}
