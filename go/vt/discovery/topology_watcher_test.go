/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreedto in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package discovery

import (
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
	"github.com/youtube/vitess/go/vt/topo/memorytopo"
	"golang.org/x/net/context"
)

func TestCellTabletsWatcher(t *testing.T) {
	checkWatcher(t, true)
}

func TestShardReplicationWatcher(t *testing.T) {
	checkWatcher(t, false)
}

func checkWatcher(t *testing.T, cellTablets bool) {
	ts := memorytopo.NewServer("aa")
	fhc := NewFakeHealthCheck()
	var tw *TopologyWatcher
	if cellTablets {
		tw = NewCellTabletsWatcher(ts, fhc, "aa", 10*time.Minute, 5)
	} else {
		tw = NewShardReplicationWatcher(ts, fhc, "aa", "keyspace", "shard", 10*time.Minute, 5)
	}

	// Wait for the initial topology load to finish. Otherwise we
	// have a background loadTablets() that's running, and it can
	// interact with our tests in weird ways.
	if err := tw.WaitForInitialTopology(); err != nil {
		t.Fatalf("initial WaitForInitialTopology failed")
	}

	// Add a tablet to the topology.
	tablet := &topodatapb.Tablet{
		Alias: &topodatapb.TabletAlias{
			Cell: "aa",
			Uid:  0,
		},
		Hostname: "host1",
		PortMap: map[string]int32{
			"vt": 123,
		},
		Keyspace: "keyspace",
		Shard:    "shard",
	}
	if err := ts.CreateTablet(context.Background(), tablet); err != nil {
		t.Fatalf("CreateTablet failed: %v", err)
	}
	tw.loadTablets()

	// Check the tablet is returned by GetAllTablets().
	allTablets := fhc.GetAllTablets()
	key := TabletToMapKey(tablet)
	if _, ok := allTablets[key]; !ok || len(allTablets) != 1 || !proto.Equal(allTablets[key], tablet) {
		t.Errorf("fhc.GetAllTablets() = %+v; want %+v", allTablets, tablet)
	}

	// same tablet, different port, should update (previous
	// one should go away, new one be added).
	tablet.PortMap["vt"] = 456
	if _, err := ts.UpdateTabletFields(context.Background(), tablet.Alias, func(t *topodatapb.Tablet) error {
		t.PortMap["vt"] = 456
		return nil
	}); err != nil {
		t.Fatalf("UpdateTabletFields failed: %v", err)
	}
	tw.loadTablets()
	allTablets = fhc.GetAllTablets()
	key = TabletToMapKey(tablet)
	if _, ok := allTablets[key]; !ok || len(allTablets) != 1 || !proto.Equal(allTablets[key], tablet) {
		t.Errorf("fhc.GetAllTablets() = %+v; want %+v", allTablets, tablet)
	}

	// Remove and re-add with a new uid. This should trigger a ReplaceTablet in loadTablets,
	// because the uid does not match.
	if err := ts.DeleteTablet(context.Background(), tablet.Alias); err != nil {
		t.Fatalf("DeleteTablet failed: %v", err)
	}
	tablet.Alias.Uid = 1
	if err := ts.CreateTablet(context.Background(), tablet); err != nil {
		t.Fatalf("CreateTablet failed: %v", err)
	}
	tw.loadTablets()

	allTablets = fhc.GetAllTablets()
	key = TabletToMapKey(tablet)
	if _, ok := allTablets[key]; !ok || len(allTablets) != 1 || !proto.Equal(allTablets[key], tablet) {
		t.Errorf("fhc.GetAllTablets() = %+v; want %+v", allTablets, tablet)
	}

	tw.Stop()
}

func TestFilterByShard(t *testing.T) {
	testcases := []struct {
		filters  []string
		keyspace string
		shard    string
		included bool
	}{
		// un-sharded keyspaces
		{
			filters:  []string{"ks1|0"},
			keyspace: "ks1",
			shard:    "0",
			included: true,
		},
		{
			filters:  []string{"ks1|0"},
			keyspace: "ks2",
			shard:    "0",
			included: false,
		},
		// custom sharding, different shard
		{
			filters:  []string{"ks1|0"},
			keyspace: "ks1",
			shard:    "1",
			included: false,
		},
		// keyrange based sharding
		{
			filters:  []string{"ks1|-80"},
			keyspace: "ks1",
			shard:    "0",
			included: false,
		},
		{
			filters:  []string{"ks1|-80"},
			keyspace: "ks1",
			shard:    "-40",
			included: true,
		},
		{
			filters:  []string{"ks1|-80"},
			keyspace: "ks1",
			shard:    "-80",
			included: true,
		},
		{
			filters:  []string{"ks1|-80"},
			keyspace: "ks1",
			shard:    "80-",
			included: false,
		},
		{
			filters:  []string{"ks1|-80"},
			keyspace: "ks1",
			shard:    "c0-",
			included: false,
		},
	}

	for _, tc := range testcases {
		fbs, err := NewFilterByShard(nil, tc.filters)
		if err != nil {
			t.Errorf("cannot create FilterByShard for filters %v: %v", tc.filters, err)
		}

		tablet := &topodatapb.Tablet{
			Keyspace: tc.keyspace,
			Shard:    tc.shard,
		}

		got := fbs.isIncluded(tablet)
		if got != tc.included {
			t.Errorf("isIncluded(%v,%v) for filters %v returned %v but expected %v", tc.keyspace, tc.shard, tc.filters, got, tc.included)
		}
	}
}
