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
	"math/rand/v2"
	"strconv"
	"strings"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/log"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
)

type iWorkflow interface {
	Create()
	Show()
	MirrorTraffic()
	SwitchReads()
	SwitchWrites()
	SwitchReadsAndWrites()
	ReverseReads()
	ReverseWrites()
	ReverseReadsAndWrites()
	Cancel()
	Complete()
	Flavor() string
	GetLastOutput() string
	Start()
	Status()
	Stop()
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

var workflowFlavorNames = map[workflowFlavor]string{
	workflowFlavorVtctl:  "vtctl",
	workflowFlavorVtctld: "vtctld",
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

	// currently only used by vtctld
	lastOutput    string
	createFlags   []string
	completeFlags []string
	mirrorFlags   []string
	switchFlags   []string
	showFlags     []string
}

type iMoveTables interface {
	iWorkflow
}

func newMoveTables(vc *VitessCluster, mt *moveTablesWorkflow, flavor workflowFlavor) iMoveTables {
	mt.vc = vc
	var mt2 iMoveTables
	if flavor == workflowFlavorRandom {
		flavor = workflowFlavors[rand.IntN(len(workflowFlavors))]
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
	currentWorkflowType = binlogdatapb.VReplicationWorkflowType_MoveTables
	vmt.exec(workflowActionCreate)
}

func (vmt *VtctlMoveTables) MirrorTraffic() {
	// TODO implement me
	panic("implement me")
}

func (vmt *VtctlMoveTables) SwitchReadsAndWrites() {
	err := tstWorkflowExecVtctl(vmt.vc.t, "", vmt.workflowName, vmt.sourceKeyspace, vmt.targetKeyspace,
		vmt.tables, workflowActionSwitchTraffic, "", "", "", defaultWorkflowExecOptions)
	require.NoError(vmt.vc.t, err)
}

func (vmt *VtctlMoveTables) ReverseReadsAndWrites() {
	err := tstWorkflowExecVtctl(vmt.vc.t, "", vmt.workflowName, vmt.sourceKeyspace, vmt.targetKeyspace,
		vmt.tables, workflowActionReverseTraffic, "", "", "", defaultWorkflowExecOptions)
	require.NoError(vmt.vc.t, err)
}

func (vmt *VtctlMoveTables) Show() {
	// TODO implement me
	panic("implement me")
}

func (vmt *VtctlMoveTables) Status() {
	currentWorkflowType = binlogdatapb.VReplicationWorkflowType_MoveTables
	vmt.exec("Status")
}

func (vmt *VtctlMoveTables) exec(action string) {
	options := &workflowExecOptions{
		deferSecondaryKeys: false,
		atomicCopy:         vmt.atomicCopy,
	}
	err := tstWorkflowExecVtctl(vmt.vc.t, "", vmt.workflowName, vmt.sourceKeyspace, vmt.targetKeyspace,
		vmt.tables, action, vmt.tabletTypes, vmt.sourceShards, "", options)
	require.NoError(vmt.vc.t, err)
}
func (vmt *VtctlMoveTables) SwitchReads() {
	err := tstWorkflowExecVtctl(vmt.vc.t, "", vmt.workflowName, vmt.sourceKeyspace, vmt.targetKeyspace,
		vmt.tables, workflowActionSwitchTraffic, "replica,rdonly", "", "", defaultWorkflowExecOptions)
	require.NoError(vmt.vc.t, err)
}

func (vmt *VtctlMoveTables) SwitchWrites() {
	err := tstWorkflowExecVtctl(vmt.vc.t, "", vmt.workflowName, vmt.sourceKeyspace, vmt.targetKeyspace,
		vmt.tables, workflowActionSwitchTraffic, "primary", "", "", defaultWorkflowExecOptions)
	require.NoError(vmt.vc.t, err)
}
func (vmt *VtctlMoveTables) ReverseReads() {
	err := tstWorkflowExecVtctl(vmt.vc.t, "", vmt.workflowName, vmt.sourceKeyspace, vmt.targetKeyspace,
		vmt.tables, workflowActionReverseTraffic, "replica,rdonly", "", "", defaultWorkflowExecOptions)
	require.NoError(vmt.vc.t, err)
}

func (vmt *VtctlMoveTables) ReverseWrites() {
	err := tstWorkflowExecVtctl(vmt.vc.t, "", vmt.workflowName, vmt.sourceKeyspace, vmt.targetKeyspace,
		vmt.tables, workflowActionReverseTraffic, "primary", "", "", defaultWorkflowExecOptions)
	require.NoError(vmt.vc.t, err)
}

func (vmt *VtctlMoveTables) Cancel() {
	vmt.exec(workflowActionCancel)
}

func (vmt *VtctlMoveTables) Complete() {
	vmt.exec(workflowActionComplete)
}

func (vmt *VtctlMoveTables) GetLastOutput() string {
	return vmt.lastOutput
}

func (vmt *VtctlMoveTables) Start() {
	panic("implement me")
}

func (vmt *VtctlMoveTables) Stop() {
	panic("implement me")
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
	var err error
	v.vc.t.Logf("Executing workflow command: vtctldclient %s", strings.Join(args2, " "))
	v.lastOutput, err = vc.VtctldClient.ExecuteCommandWithOutput(args2...)
	require.NoError(v.vc.t, err, "failed MoveTables action, error: %v: output: %s", err, v.lastOutput)
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
	args = append(args, v.createFlags...)
	v.exec(args...)
}

func (v VtctldMoveTables) MirrorTraffic() {
	args := []string{"MirrorTraffic"}
	args = append(args, v.mirrorFlags...)
	v.exec(args...)
}

func (v VtctldMoveTables) SwitchReadsAndWrites() {
	args := []string{"SwitchTraffic"}
	args = append(args, v.switchFlags...)
	v.exec(args...)
}

func (v VtctldMoveTables) ReverseReadsAndWrites() {
	v.exec("ReverseTraffic")
}

func (v VtctldMoveTables) Show() {
	args := []string{"Show"}
	args = append(args, v.showFlags...)
	v.exec(args...)
}

func (v VtctldMoveTables) Status() {
	v.exec("Status")
}

func (v VtctldMoveTables) SwitchReads() {
	args := []string{"SwitchTraffic", "--tablet-types=rdonly,replica"}
	args = append(args, v.switchFlags...)
	v.exec(args...)
}

func (v VtctldMoveTables) SwitchWrites() {
	args := []string{"SwitchTraffic", "--tablet-types=primary"}
	args = append(args, v.switchFlags...)
	v.exec(args...)
}

func (v VtctldMoveTables) ReverseReads() {
	args := []string{"ReverseTraffic", "--tablet-types=rdonly,replica"}
	args = append(args, v.switchFlags...)
	v.exec(args...)
}

func (v VtctldMoveTables) ReverseWrites() {
	args := []string{"ReverseTraffic", "--tablet-types=primary"}
	args = append(args, v.switchFlags...)
	v.exec(args...)
}

func (v VtctldMoveTables) Cancel() {
	args := []string{"Cancel", "--delete-batch-size=500"}
	v.exec(args...)
}

func (v VtctldMoveTables) Complete() {
	args := []string{"Complete"}
	args = append(args, v.completeFlags...)
	v.exec(args...)
}

func (v VtctldMoveTables) GetLastOutput() string {
	return v.lastOutput
}

func (v VtctldMoveTables) Start() {
	v.exec("Start")
}

func (v VtctldMoveTables) Stop() {
	v.exec("Stop")
}

// Reshard wrappers

type reshardWorkflow struct {
	*workflowInfo
	sourceShards   string
	targetShards   string
	skipSchemaCopy bool

	// currently only used by vtctld
	lastOutput    string
	createFlags   []string
	completeFlags []string
	cancelFlags   []string
	switchFlags   []string
}

type iReshard interface {
	iWorkflow
}

func newReshard(vc *VitessCluster, rs *reshardWorkflow, flavor workflowFlavor) iReshard {
	rs.vc = vc
	var rs2 iReshard
	if flavor == workflowFlavorRandom {
		flavor = workflowFlavors[rand.IntN(len(workflowFlavors))]
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

func (vrs *VtctlReshard) ReverseReads() {
	//TODO implement me
	panic("implement me")
}

func (vrs *VtctlReshard) ReverseWrites() {
	//TODO implement me
	panic("implement me")
}

func (vrs *VtctlReshard) Flavor() string {
	return "vtctl"
}

func newVtctlReshard(rs *reshardWorkflow) *VtctlReshard {
	return &VtctlReshard{rs}
}

func (vrs *VtctlReshard) Create() {
	currentWorkflowType = binlogdatapb.VReplicationWorkflowType_Reshard
	vrs.exec(workflowActionCreate)
}

func (vrs *VtctlReshard) MirrorTraffic() {
	// TODO implement me
	panic("implement me")
}

func (vrs *VtctlReshard) Status() {
	currentWorkflowType = binlogdatapb.VReplicationWorkflowType_Reshard
	vrs.exec("Status")
}

func (vrs *VtctlReshard) SwitchReadsAndWrites() {
	vrs.exec(workflowActionSwitchTraffic)
}

func (vrs *VtctlReshard) ReverseReadsAndWrites() {
	vrs.exec(workflowActionReverseTraffic)
}

func (vrs *VtctlReshard) Show() {
	// TODO implement me
	panic("implement me")
}

func (vrs *VtctlReshard) exec(action string) {
	options := &workflowExecOptions{}
	err := tstWorkflowExecVtctl(vrs.vc.t, "", vrs.workflowName, "", vrs.targetKeyspace,
		"", action, vrs.tabletTypes, vrs.sourceShards, vrs.targetShards, options)
	require.NoError(vrs.vc.t, err)
}

func (vrs *VtctlReshard) SwitchReads() {
	// TODO implement me
	panic("implement me")
}

func (vrs *VtctlReshard) SwitchWrites() {
	// TODO implement me
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
	v.vc.t.Logf("Executing command: vtctldclient %s", strings.Join(args2, " "))
	v.lastOutput, err = vc.VtctldClient.ExecuteCommandWithOutput(args2...)
	require.NoError(v.vc.t, err, "failed Reshard action, error: %v: output: %s", err, v.lastOutput)
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

func (v VtctldReshard) MirrorTraffic() {
	// TODO implement me
	panic("implement me")
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

func (v *VtctldReshard) Status() {
	v.exec("Status")
}

func (v VtctldReshard) SwitchReads() {
	args := []string{"SwitchTraffic"}
	args = append(args, v.switchFlags...)
	args = append(args, "--tablet-types=rdonly,replica")
	v.exec(args...)
}

func (v VtctldReshard) SwitchWrites() {
	args := []string{"SwitchTraffic"}
	args = append(args, v.switchFlags...)
	args = append(args, "--tablet-types=primary")
	v.exec(args...)
}

func (v VtctldReshard) ReverseReads() {
	args := []string{"ReverseTraffic"}
	args = append(args, v.switchFlags...)
	args = append(args, "--tablet-types=rdonly,replica")
	v.exec(args...)
}

func (v VtctldReshard) ReverseWrites() {
	args := []string{"ReverseTraffic"}
	args = append(args, v.switchFlags...)
	args = append(args, "--tablet-types=primary")
	v.exec(args...)
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
