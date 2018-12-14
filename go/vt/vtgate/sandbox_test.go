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
	"flag"
	"fmt"
	"sync"

	"golang.org/x/net/context"
	"vitess.io/vitess/go/json2"
	"vitess.io/vitess/go/vt/grpcclient"
	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/queryservice"
	"vitess.io/vitess/go/vt/vttablet/sandboxconn"
	"vitess.io/vitess/go/vt/vttablet/tabletconn"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

// sandbox_test.go provides a sandbox for unit testing VTGate.

const (
	KsTestSharded             = "TestSharded"
	KsTestUnsharded           = "TestUnsharded"
	KsTestUnshardedServedFrom = "TestUnshardedServedFrom"
	KsTestBadVSchema          = "TestXBadVSchema"
)

func init() {
	ksToSandbox = make(map[string]*sandbox)
	createSandbox(KsTestSharded)
	createSandbox(KsTestUnsharded)
	createSandbox(KsTestBadVSchema)
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
		if err := json2.Unmarshal([]byte(sandbox.VSchema), &vs); err != nil {
			panic(err)
		}
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

// sandboxTopo satisfies the srvtopo.Server interface
type sandboxTopo struct {
	topoServer *topo.Server
}

// newSandboxForCells creates a new topo with a backing memory topo for
// the given cells.
//
// when this version is used, WatchSrvVSchema can properly simulate watches
func newSandboxForCells(cells []string) *sandboxTopo {
	return &sandboxTopo{
		topoServer: memorytopo.NewServer(cells...),
	}
}

// GetTopoServer is part of the srvtopo.Server interface
func (sct *sandboxTopo) GetTopoServer() (*topo.Server, error) {
	return sct.topoServer, nil
}

// GetSrvKeyspaceNames is part of the srvtopo.Server interface.
func (sct *sandboxTopo) GetSrvKeyspaceNames(ctx context.Context, cell string) ([]string, error) {
	sandboxMu.Lock()
	defer sandboxMu.Unlock()
	keyspaces := make([]string, 0, 1)
	for k := range ksToSandbox {
		keyspaces = append(keyspaces, k)
	}
	return keyspaces, nil
}

// GetSrvKeyspace is part of the srvtopo.Server interface.
func (sct *sandboxTopo) GetSrvKeyspace(ctx context.Context, cell, keyspace string) (*topodatapb.SrvKeyspace, error) {
	sand := getSandbox(keyspace)
	if sand == nil {
		return nil, fmt.Errorf("topo error GetSrvKeyspace")
	}
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

// WatchSrvVSchema is part of the srvtopo.Server interface.
//
// If the sandbox was created with a backing topo service, piggy back on it
// to properly simulate watches, otherwise just immediately call back the
// caller.
func (sct *sandboxTopo) WatchSrvVSchema(ctx context.Context, cell string, callback func(*vschemapb.SrvVSchema, error)) {
	srvVSchema := getSandboxSrvVSchema()

	if sct.topoServer == nil {
		callback(srvVSchema, nil)
		return
	}

	sct.topoServer.UpdateSrvVSchema(ctx, cell, srvVSchema)
	current, updateChan, _ := sct.topoServer.WatchSrvVSchema(ctx, cell)
	callback(current.Value, nil)
	go func() {
		for {
			update := <-updateChan
			callback(update.Value, update.Err)
		}
	}()
}

func sandboxDialer(tablet *topodatapb.Tablet, failFast grpcclient.FailFast) (queryservice.QueryService, error) {
	sand := getSandbox(tablet.Keyspace)
	sand.sandmu.Lock()
	defer sand.sandmu.Unlock()
	sand.DialCounter++
	if sand.DialMustFail > 0 {
		sand.DialMustFail--
		return nil, vterrors.New(vtrpcpb.Code_UNAVAILABLE, "conn error")
	}
	sbc := sandboxconn.NewSandboxConn(tablet)
	return sbc, nil
}
