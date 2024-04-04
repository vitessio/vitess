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
<<<<<<< HEAD
	err := tstWorkflowExec(vmt.vc.t, "", vmt.workflowName, vmt.sourceKeyspace, vmt.targetKeyspace,
		vmt.tables, workflowActionSwitchTraffic, "", "", "", vmt.atomicCopy)
=======
	err := tstWorkflowExecVtctl(vmt.vc.t, "", vmt.workflowName, vmt.sourceKeyspace, vmt.targetKeyspace,
		vmt.tables, workflowActionSwitchTraffic, "", "", "", defaultWorkflowExecOptions)
>>>>>>> 4a1870ad59 (VReplication: Get workflowFlavorVtctl endtoend testing working properly again (#15636))
	require.NoError(vmt.vc.t, err)
}

func (vmt *VtctlMoveTables) ReverseReadsAndWrites() {
<<<<<<< HEAD
	err := tstWorkflowExec(vmt.vc.t, "", vmt.workflowName, vmt.sourceKeyspace, vmt.targetKeyspace,
		vmt.tables, workflowActionReverseTraffic, "", "", "", vmt.atomicCopy)
=======
	err := tstWorkflowExecVtctl(vmt.vc.t, "", vmt.workflowName, vmt.sourceKeyspace, vmt.targetKeyspace,
		vmt.tables, workflowActionReverseTraffic, "", "", "", defaultWorkflowExecOptions)
>>>>>>> 4a1870ad59 (VReplication: Get workflowFlavorVtctl endtoend testing working properly again (#15636))
	require.NoError(vmt.vc.t, err)
}

func (vmt *VtctlMoveTables) Show() {
	//TODO implement me
	panic("implement me")
}

<<<<<<< HEAD
=======
func (vmt *VtctlMoveTables) exec(action string) {
	options := &workflowExecOptions{
		deferSecondaryKeys: false,
		atomicCopy:         vmt.atomicCopy,
	}
	err := tstWorkflowExecVtctl(vmt.vc.t, "", vmt.workflowName, vmt.sourceKeyspace, vmt.targetKeyspace,
		vmt.tables, action, vmt.tabletTypes, vmt.sourceShards, "", options)
	require.NoError(vmt.vc.t, err)
}
>>>>>>> 4a1870ad59 (VReplication: Get workflowFlavorVtctl endtoend testing working properly again (#15636))
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
<<<<<<< HEAD
=======

func (vrs *VtctlReshard) exec(action string) {
	options := &workflowExecOptions{}
	err := tstWorkflowExecVtctl(vrs.vc.t, "", vrs.workflowName, "", vrs.targetKeyspace,
		"", action, vrs.tabletTypes, vrs.sourceShards, vrs.targetShards, options)
	require.NoError(vrs.vc.t, err)
}

func (vrs *VtctlReshard) SwitchReads() {
	//TODO implement me
	panic("implement me")
}

func (vrs *VtctlReshard) SwitchWrites() {
	//TODO implement me
	panic("implement me")
}

func (vrs *VtctlReshard) Cancel() {
	vrs.exec(workflowActionCancel)
}

func (vrs *VtctlReshard) Complete() {
	vrs.exec(workflowActionComplete)
}

func (vrs *VtctlReshard) GetLastOutput() string {
	return vrs.lastOutput
}

func (vrs *VtctlReshard) Start() {
	panic("implement me")
}

func (vrs *VtctlReshard) Stop() {
	panic("implement me")
}

var _ iReshard = (*VtctldReshard)(nil)

type VtctldReshard struct {
	*reshardWorkflow
}

func newVtctldReshard(rs *reshardWorkflow) *VtctldReshard {
	return &VtctldReshard{rs}
}

func (v VtctldReshard) Flavor() string {
	return "vtctld"
}

func (v VtctldReshard) exec(args ...string) {
	args2 := []string{"Reshard", "--workflow=" + v.workflowName, "--target-keyspace=" + v.targetKeyspace}
	args2 = append(args2, args...)
	var err error
	if v.lastOutput, err = vc.VtctldClient.ExecuteCommandWithOutput(args2...); err != nil {
		v.vc.t.Fatalf("failed to create Reshard workflow: %v: %s", err, v.lastOutput)
	}
}

func (v VtctldReshard) Create() {
	args := []string{"Create"}
	if v.sourceShards != "" {
		args = append(args, "--source-shards="+v.sourceShards)
	}
	if v.targetShards != "" {
		args = append(args, "--target-shards="+v.targetShards)
	}
	if v.skipSchemaCopy {
		args = append(args, "--skip-schema-copy="+strconv.FormatBool(v.skipSchemaCopy))
	}
	args = append(args, v.createFlags...)
	v.exec(args...)
}

func (v VtctldReshard) SwitchReadsAndWrites() {
	args := []string{"SwitchTraffic"}
	args = append(args, v.switchFlags...)
	v.exec(args...)
}

func (v VtctldReshard) ReverseReadsAndWrites() {
	v.exec("ReverseTraffic")
}

func (v VtctldReshard) Show() {
	v.exec("Show")
}

func (v VtctldReshard) SwitchReads() {
	//TODO implement me
	panic("implement me")
}

func (v VtctldReshard) SwitchWrites() {
	//TODO implement me
	panic("implement me")
}

func (v VtctldReshard) Cancel() {
	args := []string{"Cancel"}
	args = append(args, v.cancelFlags...)
	v.exec(args...)
}

func (v VtctldReshard) Complete() {
	args := []string{"Complete"}
	args = append(args, v.completeFlags...)
	v.exec(args...)
}

func (v VtctldReshard) GetLastOutput() string {
	return v.lastOutput
}

func (vrs *VtctldReshard) Start() {
	vrs.exec("Start")
}

func (vrs *VtctldReshard) Stop() {
	vrs.exec("Stop")
}
>>>>>>> 4a1870ad59 (VReplication: Get workflowFlavorVtctl endtoend testing working properly again (#15636))
