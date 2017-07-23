/*
Copyright 2017 Google Inc.

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

package vtgate

import (
	"encoding/json"
	"flag"
	"fmt"
	"sync"
	"time"

	"github.com/youtube/vitess/go/vt/key"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/vterrors"
	"github.com/youtube/vitess/go/vt/vttablet/queryservice"
	"github.com/youtube/vitess/go/vt/vttablet/sandboxconn"
	"github.com/youtube/vitess/go/vt/vttablet/tabletconn"
	"golang.org/x/net/context"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
	vschemapb "github.com/youtube/vitess/go/vt/proto/vschema"
	vtrpcpb "github.com/youtube/vitess/go/vt/proto/vtrpc"
)

// sandbox_test.go provides a sandbox for unit testing VTGate.

const (
	KsTestSharded             = "TestSharded"
	KsTestUnsharded           = "TestUnsharded"
	KsTestUnshardedServedFrom = "TestUnshardedServedFrom"
)

func init() {
	ksToSandbox = make(map[string]*sandbox)
	createSandbox(KsTestSharded)
	createSandbox(KsTestUnsharded)
	tabletconn.RegisterDialer("sandbox", sandboxDialer)
	flag.Set("tablet_protocol", "sandbox")
}

var sandboxMu sync.Mutex
var ksToSandbox map[string]*sandbox

func createSandbox(keyspace string) *sandbox {
	sandboxMu.Lock()
	defer sandboxMu.Unlock()
	s := &sandbox{VSchema: "{}"}
	s.Reset()
	ksToSandbox[keyspace] = s
	return s
}

func getSandbox(keyspace string) *sandbox {
	sandboxMu.Lock()
	defer sandboxMu.Unlock()
	return ksToSandbox[keyspace]
}

func getSandboxSrvVSchema() *vschemapb.SrvVSchema {
	result := &vschemapb.SrvVSchema{
		Keyspaces: map[string]*vschemapb.Keyspace{},
	}
	sandboxMu.Lock()
	defer sandboxMu.Unlock()
	for keyspace, sandbox := range ksToSandbox {
		var vs vschemapb.Keyspace
		_ = json.Unmarshal([]byte(sandbox.VSchema), &vs)
		result.Keyspaces[keyspace] = &vs
	}
	return result
}

func addSandboxServedFrom(keyspace, servedFrom string) {
	sandboxMu.Lock()
	defer sandboxMu.Unlock()
	ksToSandbox[keyspace].KeyspaceServedFrom = servedFrom
	ksToSandbox[servedFrom] = ksToSandbox[keyspace]
}

type sandbox struct {
	// Use sandmu to access the variables below
	sandmu sync.Mutex

	// SrvKeyspaceCounter tracks how often GetSrvKeyspace was called
	SrvKeyspaceCounter int

	// SrvKeyspaceMustFail specifies how often GetSrvKeyspace must fail before succeeding
	SrvKeyspaceMustFail int

	// DialCounter tracks how often sandboxDialer was called
	DialCounter int

	// DialMustFail specifies how often sandboxDialer must fail before succeeding
	DialMustFail int

	// DialMustTimeout specifies how often sandboxDialer must time out
	DialMustTimeout int

	// KeyspaceServedFrom specifies the served-from keyspace for vertical resharding
	KeyspaceServedFrom string

	// ShardSpec specifies the sharded keyranges
	ShardSpec string

	// SrvKeyspaceCallback specifies the callback function in GetSrvKeyspace
	SrvKeyspaceCallback func()

	// VSchema specifies the vschema in JSON format.
	VSchema string
}

// Reset cleans up sandbox internal state.
func (s *sandbox) Reset() {
	s.sandmu.Lock()
	defer s.sandmu.Unlock()
	s.SrvKeyspaceCounter = 0
	s.SrvKeyspaceMustFail = 0
	s.DialCounter = 0
	s.DialMustFail = 0
	s.DialMustTimeout = 0
	s.KeyspaceServedFrom = ""
	s.ShardSpec = DefaultShardSpec
	s.SrvKeyspaceCallback = nil
}

// DefaultShardSpec is the default sharding scheme for testing.
var DefaultShardSpec = "-20-40-60-80-a0-c0-e0-"

func getAllShards(shardSpec string) ([]*topodatapb.KeyRange, error) {
	shardedKrArray, err := key.ParseShardingSpec(shardSpec)
	if err != nil {
		return nil, err
	}
	return shardedKrArray, nil
}

func createShardedSrvKeyspace(shardSpec, servedFromKeyspace string) (*topodatapb.SrvKeyspace, error) {
	shardKrArray, err := getAllShards(shardSpec)
	if err != nil {
		return nil, err
	}
	shards := make([]*topodatapb.ShardReference, 0, len(shardKrArray))
	for i := 0; i < len(shardKrArray); i++ {
		shard := &topodatapb.ShardReference{
			Name:     key.KeyRangeString(shardKrArray[i]),
			KeyRange: shardKrArray[i],
		}
		shards = append(shards, shard)
	}
	shardedSrvKeyspace := &topodatapb.SrvKeyspace{
		ShardingColumnName: "user_id", // exact value is ignored
		ShardingColumnType: topodatapb.KeyspaceIdType_UINT64,
		Partitions: []*topodatapb.SrvKeyspace_KeyspacePartition{
			{
				ServedType:      topodatapb.TabletType_MASTER,
				ShardReferences: shards,
			},
			{
				ServedType:      topodatapb.TabletType_REPLICA,
				ShardReferences: shards,
			},
			{
				ServedType:      topodatapb.TabletType_RDONLY,
				ShardReferences: shards,
			},
		},
	}
	if servedFromKeyspace != "" {
		shardedSrvKeyspace.ServedFrom = []*topodatapb.SrvKeyspace_ServedFrom{
			{
				TabletType: topodatapb.TabletType_RDONLY,
				Keyspace:   servedFromKeyspace,
			},
			{
				TabletType: topodatapb.TabletType_MASTER,
				Keyspace:   servedFromKeyspace,
			},
		}
	}
	return shardedSrvKeyspace, nil
}

func createUnshardedKeyspace() (*topodatapb.SrvKeyspace, error) {
	shard := &topodatapb.ShardReference{
		Name: "0",
	}

	unshardedSrvKeyspace := &topodatapb.SrvKeyspace{
		Partitions: []*topodatapb.SrvKeyspace_KeyspacePartition{
			{
				ServedType:      topodatapb.TabletType_MASTER,
				ShardReferences: []*topodatapb.ShardReference{shard},
			},
			{
				ServedType:      topodatapb.TabletType_REPLICA,
				ShardReferences: []*topodatapb.ShardReference{shard},
			},
			{
				ServedType:      topodatapb.TabletType_RDONLY,
				ShardReferences: []*topodatapb.ShardReference{shard},
			},
		},
	}
	return unshardedSrvKeyspace, nil
}

// sandboxTopo satisfies the SrvTopoServer interface
type sandboxTopo struct {
}

// GetSrvKeyspaceNames is part of SrvTopoServer.
func (sct *sandboxTopo) GetSrvKeyspaceNames(ctx context.Context, cell string) ([]string, error) {
	sandboxMu.Lock()
	defer sandboxMu.Unlock()
	keyspaces := make([]string, 0, 1)
	for k := range ksToSandbox {
		keyspaces = append(keyspaces, k)
	}
	return keyspaces, nil
}

// GetSrvKeyspace is part of SrvTopoServer.
func (sct *sandboxTopo) GetSrvKeyspace(ctx context.Context, cell, keyspace string) (*topodatapb.SrvKeyspace, error) {
	sand := getSandbox(keyspace)
	sand.sandmu.Lock()
	defer sand.sandmu.Unlock()
	if sand.SrvKeyspaceCallback != nil {
		sand.SrvKeyspaceCallback()
	}
	sand.SrvKeyspaceCounter++
	if sand.SrvKeyspaceMustFail > 0 {
		sand.SrvKeyspaceMustFail--
		return nil, fmt.Errorf("topo error GetSrvKeyspace")
	}
	switch keyspace {
	case KsTestUnshardedServedFrom:
		servedFromKeyspace, err := createUnshardedKeyspace()
		if err != nil {
			return nil, err
		}
		servedFromKeyspace.ServedFrom = []*topodatapb.SrvKeyspace_ServedFrom{
			{
				TabletType: topodatapb.TabletType_RDONLY,
				Keyspace:   KsTestUnsharded,
			},
			{
				TabletType: topodatapb.TabletType_MASTER,
				Keyspace:   KsTestUnsharded,
			},
		}
		return servedFromKeyspace, nil
	case KsTestUnsharded:
		return createUnshardedKeyspace()
	}

	return createShardedSrvKeyspace(sand.ShardSpec, sand.KeyspaceServedFrom)
}

// WatchSrvVSchema is part of SrvTopoServer.
func (sct *sandboxTopo) WatchSrvVSchema(ctx context.Context, cell string) (*topo.WatchSrvVSchemaData, <-chan *topo.WatchSrvVSchemaData, topo.CancelFunc) {
	return &topo.WatchSrvVSchemaData{
		Value: getSandboxSrvVSchema(),
	}, make(chan *topo.WatchSrvVSchemaData), func() {}
}

func sandboxDialer(tablet *topodatapb.Tablet, timeout time.Duration) (queryservice.QueryService, error) {
	sand := getSandbox(tablet.Keyspace)
	sand.sandmu.Lock()
	defer sand.sandmu.Unlock()
	sand.DialCounter++
	if sand.DialMustFail > 0 {
		sand.DialMustFail--
		return nil, vterrors.New(vtrpcpb.Code_UNAVAILABLE, "conn error")
	}
	if sand.DialMustTimeout > 0 {
		time.Sleep(timeout)
		sand.DialMustTimeout--
		return nil, vterrors.New(vtrpcpb.Code_UNAVAILABLE, "conn unreachable")
	}
	sbc := sandboxconn.NewSandboxConn(tablet)
	return sbc, nil
}
