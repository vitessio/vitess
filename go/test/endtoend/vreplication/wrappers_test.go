/*
Copyright 2023 The Vitess Authors.

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

package vreplication

import (
	"math/rand"
	"strconv"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/log"
)

type moveTablesFlavor int

const (
	moveTablesFlavorRandom moveTablesFlavor = iota
	moveTablesFlavorVtctl
	moveTablesFlavorVtctld
)

var moveTablesFlavors = []moveTablesFlavor{
	moveTablesFlavorVtctl,
	moveTablesFlavorVtctld,
}

type moveTables struct {
	vc             *VitessCluster
	workflowName   string
	targetKeyspace string
	sourceKeyspace string
	tables         string
	atomicCopy     bool
	sourceShards   string
}

type iMoveTables interface {
	Create()
	Show()
	SwitchReads()
	SwitchWrites()
	SwitchReadsAndWrites()
	ReverseReadsAndWrites()
	Cancel()
	Complete()
	Flavor() string
}

func newMoveTables(vc *VitessCluster, mt *moveTables, flavor moveTablesFlavor) iMoveTables {
	mt.vc = vc
	var mt2 iMoveTables
	if flavor == moveTablesFlavorRandom {
		flavor = moveTablesFlavors[rand.Intn(len(moveTablesFlavors))]
	}
	switch flavor {
	case moveTablesFlavorVtctl:
		mt2 = newVtctlMoveTables(mt)
	case moveTablesFlavorVtctld:
		mt2 = newVtctldMoveTables(mt)
	default:
		panic("unreachable")
	}
	log.Infof("Using moveTables flavor: %s", mt2.Flavor())
	return mt2
}

type VtctlMoveTables struct {
	*moveTables
}

func (vmt *VtctlMoveTables) Flavor() string {
	return "vtctl"
}

func newVtctlMoveTables(mt *moveTables) *VtctlMoveTables {
	return &VtctlMoveTables{mt}
}

func (vmt *VtctlMoveTables) Create() {
	log.Infof("vmt is %+v", vmt.vc, vmt.tables)
	err := tstWorkflowExec(vmt.vc.t, "", vmt.workflowName, vmt.sourceKeyspace, vmt.targetKeyspace,
		vmt.tables, workflowActionCreate, "", vmt.sourceShards, "", vmt.atomicCopy)
	require.NoError(vmt.vc.t, err)
}

func (vmt *VtctlMoveTables) SwitchReadsAndWrites() {
	err := tstWorkflowExec(vmt.vc.t, "", vmt.workflowName, vmt.sourceKeyspace, vmt.targetKeyspace,
		vmt.tables, workflowActionSwitchTraffic, "", "", "", vmt.atomicCopy)
	require.NoError(vmt.vc.t, err)
}

func (vmt *VtctlMoveTables) ReverseReadsAndWrites() {
	err := tstWorkflowExec(vmt.vc.t, "", vmt.workflowName, vmt.sourceKeyspace, vmt.targetKeyspace,
		vmt.tables, workflowActionReverseTraffic, "", "", "", vmt.atomicCopy)
	require.NoError(vmt.vc.t, err)
}

func (vmt *VtctlMoveTables) Show() {
	//TODO implement me
	panic("implement me")
}

func (vmt *VtctlMoveTables) SwitchReads() {
	//TODO implement me
	panic("implement me")
}

func (vmt *VtctlMoveTables) SwitchWrites() {
	//TODO implement me
	panic("implement me")
}

func (vmt *VtctlMoveTables) Cancel() {
	err := tstWorkflowExec(vmt.vc.t, "", vmt.workflowName, vmt.sourceKeyspace, vmt.targetKeyspace,
		vmt.tables, workflowActionCancel, "", "", "", vmt.atomicCopy)
	require.NoError(vmt.vc.t, err)
}

func (vmt *VtctlMoveTables) Complete() {
	//TODO implement me
	panic("implement me")
}

var _ iMoveTables = (*VtctldMoveTables)(nil)

type VtctldMoveTables struct {
	*moveTables
}

func newVtctldMoveTables(mt *moveTables) *VtctldMoveTables {
	return &VtctldMoveTables{mt}
}

func (v VtctldMoveTables) Flavor() string {
	return "vtctld"
}

func (v VtctldMoveTables) exec(args ...string) {
	args2 := []string{"MoveTables", "--workflow=" + v.workflowName, "--target-keyspace=" + v.targetKeyspace}
	args2 = append(args2, args...)
	if err := vc.VtctldClient.ExecuteCommand(args2...); err != nil {
		v.vc.t.Fatalf("failed to create MoveTables workflow: %v", err)
	}
}

func (v VtctldMoveTables) Create() {
	args := []string{"Create", "--source-keyspace=" + v.sourceKeyspace}
	if v.tables != "" {
		args = append(args, "--tables="+v.tables)
	} else {
		args = append(args, "--all-tables")
	}
	if v.atomicCopy {
		args = append(args, "--atomic-copy="+strconv.FormatBool(v.atomicCopy))
	}
	if v.sourceShards != "" {
		args = append(args, "--source-shards="+v.sourceShards)
	}
	v.exec(args...)
}

func (v VtctldMoveTables) SwitchReadsAndWrites() {
	v.exec("SwitchTraffic")
}

func (v VtctldMoveTables) ReverseReadsAndWrites() {
	v.exec("ReverseTraffic")
}

func (v VtctldMoveTables) Show() {
	//TODO implement me
	panic("implement me")
}

func (v VtctldMoveTables) SwitchReads() {
	//TODO implement me
	panic("implement me")
}

func (v VtctldMoveTables) SwitchWrites() {
	//TODO implement me
	panic("implement me")
}

func (v VtctldMoveTables) Cancel() {
	v.exec("Cancel")
}

func (v VtctldMoveTables) Complete() {
	//TODO implement me
	panic("implement me")
}
