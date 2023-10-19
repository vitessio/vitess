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

	"vitess.io/vitess/go/vt/wrangler"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/log"
)

type iWorkflow interface {
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

type workflowFlavor int

const (
	workflowFlavorRandom workflowFlavor = iota
	workflowFlavorVtctl
	workflowFlavorVtctld
)

var workflowFlavors = []workflowFlavor{
	workflowFlavorVtctl,
	workflowFlavorVtctld,
}

type workflowInfo struct {
	vc             *VitessCluster
	workflowName   string
	targetKeyspace string
	tabletTypes    string
}

// MoveTables wrappers

type moveTablesWorkflow struct {
	*workflowInfo
	sourceKeyspace string
	tables         string
	atomicCopy     bool
	sourceShards   string
}

type iMoveTables interface {
	iWorkflow
}

func newMoveTables(vc *VitessCluster, mt *moveTablesWorkflow, flavor workflowFlavor) iMoveTables {
	mt.vc = vc
	var mt2 iMoveTables
	if flavor == workflowFlavorRandom {
		flavor = workflowFlavors[rand.Intn(len(workflowFlavors))]
	}
	switch flavor {
	case workflowFlavorVtctl:
		mt2 = newVtctlMoveTables(mt)
	case workflowFlavorVtctld:
		mt2 = newVtctldMoveTables(mt)
	default:
		panic("unreachable")
	}
	log.Infof("Using moveTables flavor: %s", mt2.Flavor())
	return mt2
}

type VtctlMoveTables struct {
	*moveTablesWorkflow
}

func (vmt *VtctlMoveTables) Flavor() string {
	return "vtctl"
}

func newVtctlMoveTables(mt *moveTablesWorkflow) *VtctlMoveTables {
	return &VtctlMoveTables{mt}
}

func (vmt *VtctlMoveTables) Create() {
	currentWorkflowType = wrangler.MoveTablesWorkflow
	vmt.exec(workflowActionCreate)
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

func (vmt *VtctlMoveTables) exec(action string) {
	err := tstWorkflowExec(vmt.vc.t, "", vmt.workflowName, vmt.sourceKeyspace, vmt.targetKeyspace,
		vmt.tables, action, vmt.tabletTypes, vmt.sourceShards, "", vmt.atomicCopy)
	require.NoError(vmt.vc.t, err)
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
	vmt.exec(workflowActionCancel)
}

func (vmt *VtctlMoveTables) Complete() {
	vmt.exec(workflowActionComplete)
}

var _ iMoveTables = (*VtctldMoveTables)(nil)

type VtctldMoveTables struct {
	*moveTablesWorkflow
}

func newVtctldMoveTables(mt *moveTablesWorkflow) *VtctldMoveTables {
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
	v.exec("Complete")
}

// Reshard wrappers

type reshardWorkflow struct {
	*workflowInfo
	sourceShards   string
	targetShards   string
	skipSchemaCopy bool
}

type iReshard interface {
	iWorkflow
}

func newReshard(vc *VitessCluster, rs *reshardWorkflow, flavor workflowFlavor) iReshard {
	rs.vc = vc
	var rs2 iReshard
	if flavor == workflowFlavorRandom {
		flavor = workflowFlavors[rand.Intn(len(workflowFlavors))]
	}
	switch flavor {
	case workflowFlavorVtctl:
		rs2 = newVtctlReshard(rs)
	case workflowFlavorVtctld:
		rs2 = newVtctldReshard(rs)
	default:
		panic("unreachable")
	}
	log.Infof("Using reshard flavor: %s", rs2.Flavor())
	return rs2
}

type VtctlReshard struct {
	*reshardWorkflow
}

func (vrs *VtctlReshard) Flavor() string {
	return "vtctl"
}

func newVtctlReshard(rs *reshardWorkflow) *VtctlReshard {
	return &VtctlReshard{rs}
}

func (vrs *VtctlReshard) Create() {
	currentWorkflowType = wrangler.ReshardWorkflow
	vrs.exec(workflowActionCreate)
}

func (vrs *VtctlReshard) SwitchReadsAndWrites() {
	vrs.exec(workflowActionSwitchTraffic)
}

func (vrs *VtctlReshard) ReverseReadsAndWrites() {
	vrs.exec(workflowActionReverseTraffic)
}

func (vrs *VtctlReshard) Show() {
	//TODO implement me
	panic("implement me")
}

func (vrs *VtctlReshard) exec(action string) {
	err := tstWorkflowExec(vrs.vc.t, "", vrs.workflowName, "", vrs.targetKeyspace,
		"", action, vrs.tabletTypes, vrs.sourceShards, vrs.targetShards, false)
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
	if err := vc.VtctldClient.ExecuteCommand(args2...); err != nil {
		v.vc.t.Fatalf("failed to create Reshard workflow: %v", err)
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
	v.exec(args...)
}

func (v VtctldReshard) SwitchReadsAndWrites() {
	v.exec("SwitchTraffic")
}

func (v VtctldReshard) ReverseReadsAndWrites() {
	v.exec("ReverseTraffic")
}

func (v VtctldReshard) Show() {
	//TODO implement me
	panic("implement me")
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
	v.exec("Cancel")
}

func (v VtctldReshard) Complete() {
	v.exec("Complete")
}
