//go:build gofuzz
// +build gofuzz

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

package fuzzing

import (
	"context"
	"fmt"
	"net"
	"testing"

	"google.golang.org/grpc"

	"vitess.io/vitess/go/vt/vttablet/grpctmclient"
	"vitess.io/vitess/go/vt/vttablet/grpctmserver"
	"vitess.io/vitess/go/vt/vttablet/tmrpctest"

	fuzz "github.com/AdaLogics/go-fuzz-headers"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

func onceInit() {
	testing.Init()
}

// createTablets creates a slice of Tablets that can
// be used by the fuzz targets.
func (fs *fuzzStore) createTablets(f *fuzz.ConsumeFuzzer) error {
	tabletCount, err := f.GetInt()
	if err != nil {
		return err
	}
	tabletCount = tabletCount % 40
	if tabletCount == 0 {
		return fmt.Errorf("We don't need a nil-len list")
	}
	tablets := make([]*topodatapb.Tablet, 0)
	for i := 0; i < tabletCount; i++ {
		tablet := &topodatapb.Tablet{}
		err = f.GenerateStruct(tablet)
		if err != nil {
			return err
		}
		tablets = append(tablets, tablet)
	}
	fs.tablets = tablets
	return nil
}

// createTabletAliases creates a slice of TabletAliases that can
// be used by the fuzz targets.
func (fs *fuzzStore) createTabletAliases(f *fuzz.ConsumeFuzzer) error {
	tabletAliasCount, err := f.GetInt()
	if err != nil {
		return err
	}
	tabletAliasCount = tabletAliasCount % 40
	if tabletAliasCount == 0 {
		return fmt.Errorf("We don't need a nil-len list")
	}
	tabletAliases := make([]*topodatapb.TabletAlias, 0)
	for i := 0; i < tabletAliasCount; i++ {
		tabletAlias := &topodatapb.TabletAlias{}
		err = f.GenerateStruct(tabletAlias)
		if err != nil {
			return err
		}
		tabletAliases = append(tabletAliases, tabletAlias)
	}
	fs.tabletAliases = tabletAliases
	return nil
}

// createStrings creates a slice of strings that can be used
// by the fuzz tagets.
func (fs *fuzzStore) createStrings(f *fuzz.ConsumeFuzzer) error {
	stringCount, err := f.GetInt()
	if err != nil {
		return err
	}
	stringCount = stringCount % 100
	if stringCount == 0 {
		return fmt.Errorf("We don't need a nil-len list")
	}
	stringSlice := make([]string, 0)
	for i := 0; i < stringCount; i++ {
		newString, err := f.GetString()
		if err != nil {
			return err
		}
		stringSlice = append(stringSlice, newString)
	}
	fs.miscStrings = stringSlice
	return nil
}

// createBytes creates a slice of byte slices that can be used
// by the fuzz targets.
func (fs *fuzzStore) createBytes(f *fuzz.ConsumeFuzzer) error {
	bytesCount, err := f.GetInt()
	if err != nil {
		return err
	}
	bytesCount = bytesCount % 40
	if bytesCount == 0 {
		return fmt.Errorf("We don't need a nil-len list")
	}
	byteSlice := make([][]byte, 0)
	for i := 0; i < bytesCount; i++ {
		newBytes, err := f.GetBytes()
		if err != nil {
			return err
		}
		byteSlice = append(byteSlice, newBytes)
	}
	fs.miscBytes = byteSlice
	return nil
}

// createInts creates a list of ints that can be used
// by the fuzz targets.
func (fs *fuzzStore) createInts(f *fuzz.ConsumeFuzzer) error {
	intCount, err := f.GetInt()
	if err != nil {
		return err
	}
	intCount = intCount % 40
	if intCount == 0 {
		return fmt.Errorf("We don't need a nil-len list")
	}
	intSlice := make([]int, 0)
	for i := 0; i < intCount; i++ {
		newInt, err := f.GetInt()
		if err != nil {
			return err
		}
		intSlice = append(intSlice, newInt)
	}
	fs.miscInts = intSlice
	return nil
}

// createExecutionOrder creates an array of ints that will later
// be used to determine the order in which we call our fuzz targets.
func (fs *fuzzStore) createExecutionOrder(f *fuzz.ConsumeFuzzer) error {
	intCount, err := f.GetInt()
	if err != nil {
		return err
	}
	intCount = intCount % 60
	if intCount == 0 {
		return fmt.Errorf("We don't need a nil-len list")
	}
	executionOrder := make([]int, 0)
	for i := 0; i < intCount; i++ {
		newInt, err := f.GetInt()
		if err != nil {
			return err
		}
		executionOrder = append(executionOrder, newInt)
	}
	fs.executionOrder = executionOrder
	return nil
}

type fuzzStore struct {
	tablets        []*topodatapb.Tablet
	tabletAliases  []*topodatapb.TabletAlias
	miscStrings    []string
	miscBytes      [][]byte
	miscInts       []int
	client         *grpctmclient.Client
	executionOrder []int
}

// newFuzzStore creates a store of the data that is needed
// for the fuzz targets in FuzzGRPCTMServer. The reason
// a store is created is because the set up of the server
// comes with a big expense, and having data ready to pass
// to the targets improves efficiency.
func newFuzzStore(f *fuzz.ConsumeFuzzer) (*fuzzStore, error) {
	fs := &fuzzStore{}

	err := fs.createTablets(f)
	if err != nil {
		return nil, err
	}

	err = fs.createTabletAliases(f)
	if err != nil {
		return nil, err
	}

	err = fs.createStrings(f)
	if err != nil {
		return nil, err
	}

	err = fs.createBytes(f)
	if err != nil {
		return nil, err
	}

	err = fs.createInts(f)
	if err != nil {
		return nil, err
	}

	err = fs.createExecutionOrder(f)
	if err != nil {
		return nil, err
	}
	return fs, nil
}

func (fs *fuzzStore) getTablet() (*topodatapb.Tablet, error) {
	if len(fs.tablets) == 0 {
		return nil, fmt.Errorf("Not enough tablets")
	}
	tablet := fs.tablets[0]
	fs.tablets = fs.tablets[1:]
	return tablet, nil
}

func (fs *fuzzStore) getString() (string, error) {
	if len(fs.miscStrings) == 0 {
		return "", fmt.Errorf("Not enough tablets")
	}
	returnString := fs.miscStrings[0]
	fs.miscStrings = fs.miscStrings[1:]
	return returnString, nil
}

func (fs *fuzzStore) getInt() (int, error) {
	if len(fs.miscInts) == 0 {
		return 0, fmt.Errorf("Not enough tablets")
	}
	returnInt := fs.miscInts[0]
	fs.miscInts = fs.miscInts[1:]
	return returnInt, nil
}

func (fs *fuzzStore) getBytes() ([]byte, error) {
	if len(fs.miscBytes) == 0 {
		return nil, fmt.Errorf("Not enough tablets")
	}
	returnBytes := fs.miscBytes[0]
	fs.miscBytes = fs.miscBytes[1:]
	return returnBytes, nil
}

func (fs *fuzzStore) getTabletAlias() (*topodatapb.TabletAlias, error) {
	if len(fs.tabletAliases) == 0 {
		return nil, fmt.Errorf("Not enough tablets")
	}
	tabletAlias := fs.tabletAliases[0]
	fs.tabletAliases = fs.tabletAliases[1:]
	return tabletAlias, nil
}

// callExecuteFetchAsApp implements a wrapper
// for fuzzing ExecuteFetchAsApp
func (fs *fuzzStore) callExecuteFetchAsApp() error {
	tablet, err := fs.getTablet()
	if err != nil {
		return err
	}
	byteQuery, err := fs.getBytes()
	if err != nil {
		return err
	}
	maxRows, err := fs.getInt()
	if err != nil {
		return err
	}
	_, _ = fs.client.ExecuteFetchAsApp(context.Background(), tablet, false, byteQuery, maxRows)
	return nil
}

// callInitPrimary implements a wrapper
// for fuzzing InitPrimary
func (fs *fuzzStore) callInitPrimary() error {
	tablet, err := fs.getTablet()
	if err != nil {
		return err
	}
	_, _ = fs.client.InitPrimary(context.Background(), tablet, false)
	return nil
}

// callResetReplication implements a wrapper
// for fuzzing ResetReplication
func (fs *fuzzStore) callResetReplication() error {
	tablet, err := fs.getTablet()
	if err != nil {
		return err
	}
	_ = fs.client.ResetReplication(context.Background(), tablet)
	return nil
}

// callGetReplicas implements a wrapper
// for fuzzing GetReplicas
func (fs *fuzzStore) callGetReplicas() error {
	tablet, err := fs.getTablet()
	if err != nil {
		return err
	}
	_, _ = fs.client.GetReplicas(context.Background(), tablet)
	return nil
}

// callStartReplication implements a wrapper
// for fuzzing StartReplication
func (fs *fuzzStore) callStartReplication() error {
	tablet, err := fs.getTablet()
	if err != nil {
		return err
	}
	_ = fs.client.StartReplication(context.Background(), tablet, false)
	return nil
}

// callStopReplication implements a wrapper
// for fuzzing StopReplication
func (fs *fuzzStore) callStopReplication() error {
	tablet, err := fs.getTablet()
	if err != nil {
		return err
	}
	_ = fs.client.StopReplication(context.Background(), tablet)
	return nil
}

// callPrimaryPosition implements a wrapper
// for fuzzing PrimaryPosition
func (fs *fuzzStore) callPrimaryPosition() error {
	tablet, err := fs.getTablet()
	if err != nil {
		return err
	}
	_, _ = fs.client.PrimaryPosition(context.Background(), tablet)
	return nil
}

// callReplicationStatus implements a wrapper
// for fuzzing ReplicationStatus
func (fs *fuzzStore) callReplicationStatus() error {
	tablet, err := fs.getTablet()
	if err != nil {
		return err
	}
	_, _ = fs.client.ReplicationStatus(context.Background(), tablet)
	return nil
}

// callFullStatus implements a wrapper
// for fuzzing FullStatus
func (fs *fuzzStore) callFullStatus() error {
	tablet, err := fs.getTablet()
	if err != nil {
		return err
	}
	_, _ = fs.client.FullStatus(context.Background(), tablet)
	return nil
}

// callPrimaryStatus implements a wrapper
// for fuzzing PrimaryStatus
func (fs *fuzzStore) callPrimaryStatus() error {
	tablet, err := fs.getTablet()
	if err != nil {
		return err
	}
	_, _ = fs.client.PrimaryStatus(context.Background(), tablet)
	return nil
}

// callDemotePrimary implements a wrapper
// for fuzzing DemotePrimary
func (fs *fuzzStore) callDemotePrimary() error {
	tablet, err := fs.getTablet()
	if err != nil {
		return err
	}
	_, _ = fs.client.DemotePrimary(context.Background(), tablet)
	return nil
}

// callUndoDemotePrimary implements a wrapper
// for fuzzing UndoDemotePrimary
func (fs *fuzzStore) callUndoDemotePrimary() error {
	tablet, err := fs.getTablet()
	if err != nil {
		return err
	}
	_ = fs.client.UndoDemotePrimary(context.Background(), tablet, false)
	return nil
}

// callReplicaWasPromoted implements a wrapper
// for fuzzing ReplicaWasPromoted
func (fs *fuzzStore) callReplicaWasPromoted() error {
	tablet, err := fs.getTablet()
	if err != nil {
		return err
	}
	_ = fs.client.ReplicaWasPromoted(context.Background(), tablet)
	return nil
}

// callResetReplicationParameters implements a wrapper
// for fuzzing ResetReplicationParameters
func (fs *fuzzStore) callResetReplicationParameters() error {
	tablet, err := fs.getTablet()
	if err != nil {
		return err
	}
	_ = fs.client.ResetReplicationParameters(context.Background(), tablet)
	return nil
}

// callPromoteReplica implements a wrapper
// for fuzzing PromoteReplica
func (fs *fuzzStore) callPromoteReplica() error {
	tablet, err := fs.getTablet()
	if err != nil {
		return err
	}
	_, _ = fs.client.PromoteReplica(context.Background(), tablet, false)
	return nil
}

// callStopReplicationAndGetStatus implements a wrapper
// for fuzzing StopReplicationAndGetStatus
func (fs *fuzzStore) callStopReplicationAndGetStatus() error {
	tablet, err := fs.getTablet()
	if err != nil {
		return err
	}
	_, _, _ = fs.client.StopReplicationAndGetStatus(context.Background(), tablet, 0)
	return nil
}

// callReplicaWasRestarted implements a wrapper
// for fuzzing ReplicaWasRestarted
func (fs *fuzzStore) callReplicaWasRestarted() error {
	tablet, err := fs.getTablet()
	if err != nil {
		return err
	}
	parent, err := fs.getTabletAlias()
	if err != nil {
		return err
	}
	_ = fs.client.ReplicaWasRestarted(context.Background(), tablet, parent)
	return nil
}

// callWaitForPosition implements a wrapper
// for fuzzing WaitForPosition
func (fs *fuzzStore) callWaitForPosition() error {
	tablet, err := fs.getTablet()
	if err != nil {
		return err
	}
	pos, err := fs.getString()
	if err != nil {
		return err
	}
	_ = fs.client.WaitForPosition(context.Background(), tablet, pos)
	return nil
}

// callVReplicationExec implements a wrapper
// for fuzzing VReplicationExec
func (fs *fuzzStore) callVReplicationExec() error {
	tablet, err := fs.getTablet()
	if err != nil {
		return err
	}
	query, err := fs.getString()
	if err != nil {
		return err
	}
	_, _ = fs.client.VReplicationExec(context.Background(), tablet, query)
	return nil
}

// callVExec implements a wrapper
// for fuzzing VExec
func (fs *fuzzStore) callVExec() error {
	tablet, err := fs.getTablet()
	if err != nil {
		return err
	}
	query, err := fs.getString()
	if err != nil {
		return err
	}
	workflow, err := fs.getString()
	if err != nil {
		return err
	}
	keyspace, err := fs.getString()
	if err != nil {
		return err
	}
	_, _ = fs.client.VExec(context.Background(), tablet, query, workflow, keyspace)
	return nil
}

// callVReplicationWaitForPos implements a wrapper
// for fuzzing VReplicationWaitForPos
func (fs *fuzzStore) callVReplicationWaitForPos() error {
	tablet, err := fs.getTablet()
	if err != nil {
		return err
	}
	pos, err := fs.getString()
	if err != nil {
		return err
	}
	timeCreatedNS, err := fs.getInt()
	if err != nil {
		return err
	}
	_ = fs.client.VReplicationWaitForPos(context.Background(), tablet, timeCreatedNS, pos)
	return nil
}

// callSetReplicationSource implements a wrapper
// for fuzzing SetReplicationSource
func (fs *fuzzStore) callSetReplicationSource() error {
	tablet, err := fs.getTablet()
	if err != nil {
		return err
	}
	pos, err := fs.getString()
	if err != nil {
		return err
	}
	timeCreatedNS, err := fs.getInt()
	if err != nil {
		return err
	}
	parent, err := fs.getTabletAlias()
	if err != nil {
		return err
	}
	_ = fs.client.SetReplicationSource(context.Background(), tablet, parent, int64(timeCreatedNS), pos, false, false)
	return nil
}

// callInitReplica implements a wrapper
// for fuzzing InitReplica
func (fs *fuzzStore) callInitReplica() error {
	tablet, err := fs.getTablet()
	if err != nil {
		return err
	}
	timeCreatedNS, err := fs.getInt()
	if err != nil {
		return err
	}
	parent, err := fs.getTabletAlias()
	if err != nil {
		return err
	}
	replicationPosition, err := fs.getString()
	if err != nil {
		return err
	}
	_ = fs.client.InitReplica(context.Background(), tablet, parent, replicationPosition, int64(timeCreatedNS), false)
	return nil
}

// callPopulateReparentJournal implements a wrapper
// for fuzzing PopulateReparentJournal
func (fs *fuzzStore) callPopulateReparentJournal() error {
	tablet, err := fs.getTablet()
	if err != nil {
		return err
	}
	timeCreatedNS, err := fs.getInt()
	if err != nil {
		return err
	}
	tabletAlias, err := fs.getTabletAlias()
	if err != nil {
		return err
	}
	actionName, err := fs.getString()
	if err != nil {
		return err
	}
	pos, err := fs.getString()
	if err != nil {
		return err
	}
	_ = fs.client.PopulateReparentJournal(context.Background(), tablet, int64(timeCreatedNS), actionName, tabletAlias, pos)
	return nil
}

// executeInRandomOrder calls the fuzz targets in
// the order specified by f.executionOrder
func (fs *fuzzStore) executeInRandomOrder() {
	maxTargets := 24
	for _, execInt := range fs.executionOrder {
		var err error
		switch execInt % maxTargets {
		case 0:
			err = fs.callInitPrimary()
		case 1:
			err = fs.callResetReplication()
		case 2:
			err = fs.callGetReplicas()
		case 3:
			err = fs.callStartReplication()
		case 4:
			err = fs.callStopReplication()
		case 5:
			err = fs.callPrimaryPosition()
		case 7:
			err = fs.callReplicationStatus()
		case 8:
			err = fs.callPrimaryStatus()
		case 9:
			err = fs.callDemotePrimary()
		case 11:
			err = fs.callUndoDemotePrimary()
		case 12:
			err = fs.callReplicaWasPromoted()
		case 13:
			err = fs.callPromoteReplica()
		case 14:
			err = fs.callReplicaWasRestarted()
		case 15:
			err = fs.callWaitForPosition()
		case 16:
			err = fs.callVReplicationExec()
		case 17:
			err = fs.callVExec()
		case 18:
			err = fs.callStopReplicationAndGetStatus()
		case 19:
			err = fs.callExecuteFetchAsApp()
		case 20:
			err = fs.callVReplicationWaitForPos()
		case 21:
			err = fs.callSetReplicationSource()
		case 22:
			err = fs.callInitReplica()
		case 23:
			err = fs.callPopulateReparentJournal()
		case 24:
			err = fs.callResetReplicationParameters()
		case 25:
			err = fs.callFullStatus()
		}

		// err means that fuzzStore doesn't have any data
		// to pass to the target so we return here
		if err != nil {
			return
		}
	}
}

// FuzzGRPCTMServer implements the fuzzer.
func FuzzGRPCTMServer(data []byte) int {
	initter.Do(onceInit)
	f := fuzz.NewConsumer(data)
	fs, err := newFuzzStore(f)
	if err != nil {
		return 0
	}
	t := &testing.T{}

	// Listen on a random port
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0
	}
	defer listener.Close()

	host := listener.Addr().(*net.TCPAddr).IP.String()
	port := int32(listener.Addr().(*net.TCPAddr).Port)
	_, _ = host, port
	for _, tablet := range fs.tablets {
		tablet.Hostname = host
		tablet.PortMap = map[string]int32{
			"grpc": port,
		}
	}
	s := grpc.NewServer()
	fakeTM := tmrpctest.NewFakeRPCTM(t)
	grpctmserver.RegisterForTest(s, fakeTM)
	go s.Serve(listener)
	defer s.Stop()

	// Create a gRPC client to talk to the fake tablet.
	client := grpctmclient.NewClient()
	defer client.Close()
	fs.client = client

	// Call the targets in random order.
	fs.executeInRandomOrder()

	return 1
}
