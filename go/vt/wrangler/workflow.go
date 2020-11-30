package wrangler

import (
	"context"
	"fmt"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"

	"github.com/looplab/fsm"
	"vitess.io/vitess/go/vt/log"
)

const (
	WorkflowStateNotStarted                    = "Not Started"
	WorkflowStateCopying                       = "Copying"
	WorkflowStateReplicating                   = "Replicating"
	WorkflowStateReplicaReadsSwitched          = "Replica Reads Switched"
	WorkflowStateRdonlyReadsSwitched           = "Rdonly Reads Switched"
	WorkflowStateReadsSwitched                 = "Reads Switched"
	WorkflowStateWritesSwitched                = "Writes Switched"
	WorkflowStateReplicaReadsAndWritesSwitched = "Replica Reads and Writes Switched"
	WorkflowStateRdonlyReadsAndWritesSwitched  = "Rdonly Reads and Writes Switched"
	WorkflowStateReadsAndWritesSwitched        = "Both Reads and Writes Switched"
	WorkflowStateCompleted                     = "Completed"
	WorkflowStateAborted                       = "Aborted"
	WorkflowStateError                         = "Error"
)

const (
	WorkflowEventStart              = "Start"
	WorkflowEventCopyCompleted      = "CopyCompleted"
	WorkflowEventSwitchReads        = "SwitchReads"
	WorkflowEventSwitchReplicaReads = "SwitchReplicaReads"
	WorkflowEventSwitchRdonlyReads  = "SwitchRdonlyReads"
	WorkflowEventSwitchWrites       = "SwitchWrites"
	WorkflowEventComplete           = "Complete"
	WorkflowEventAbort              = "Abort"
	WorkflowEventReverseReads       = "ReverseReads"
	WorkflowEventReverseWrites      = "ReverseWrites"
)

type Workflow struct {
	name          string
	wsm           *fsm.FSM
	typ           string
	isReplicating bool
	isRunning     bool
	hasErrors     bool
}

func (wr *Wrangler) IsUserFacingEvent(ev string) bool {
	allUserFacingEvents := []string{WorkflowEventStart, WorkflowEventSwitchReads, WorkflowEventSwitchWrites,
		WorkflowEventComplete, WorkflowEventAbort}
	for _, ev2 := range allUserFacingEvents {
		if ev2 == ev {
			return true
		}
	}
	return false
}

func getWorkflowTransitions() []fsm.EventDesc {
	return []fsm.EventDesc{
		{Name: WorkflowEventStart, Src: []string{WorkflowStateNotStarted}, Dst: WorkflowStateCopying},
		{Name: WorkflowEventCopyCompleted, Src: []string{WorkflowStateCopying}, Dst: WorkflowStateReplicating},

		{Name: WorkflowEventSwitchReplicaReads, Src: []string{WorkflowStateReplicating}, Dst: WorkflowStateReplicaReadsSwitched},
		{Name: WorkflowEventSwitchReplicaReads, Src: []string{WorkflowStateRdonlyReadsSwitched}, Dst: WorkflowStateReadsSwitched},
		{Name: WorkflowEventSwitchReplicaReads, Src: []string{WorkflowStateWritesSwitched}, Dst: WorkflowStateReplicaReadsAndWritesSwitched},

		{Name: WorkflowEventSwitchRdonlyReads, Src: []string{WorkflowStateReplicating}, Dst: WorkflowStateRdonlyReadsSwitched},
		{Name: WorkflowEventSwitchRdonlyReads, Src: []string{WorkflowStateReplicaReadsSwitched}, Dst: WorkflowStateReadsSwitched},
		{Name: WorkflowEventSwitchRdonlyReads, Src: []string{WorkflowStateWritesSwitched}, Dst: WorkflowStateRdonlyReadsAndWritesSwitched},

		{Name: WorkflowEventSwitchReads, Src: []string{WorkflowStateReplicating}, Dst: WorkflowStateReadsSwitched},
		{Name: WorkflowEventSwitchReads, Src: []string{WorkflowStateWritesSwitched}, Dst: WorkflowStateReadsAndWritesSwitched},

		{Name: WorkflowEventSwitchWrites, Src: []string{WorkflowStateReplicating}, Dst: WorkflowStateWritesSwitched},
		{Name: WorkflowEventSwitchWrites, Src: []string{WorkflowStateReadsSwitched}, Dst: WorkflowStateReadsAndWritesSwitched},

		{Name: WorkflowEventComplete, Src: []string{WorkflowStateReadsAndWritesSwitched}, Dst: WorkflowStateCompleted},
		{Name: WorkflowEventAbort, Src: []string{WorkflowStateNotStarted, WorkflowStateCopying,
			WorkflowStateReplicating, WorkflowStateReadsSwitched, WorkflowStateWritesSwitched}, Dst: WorkflowStateAborted},
		{Name: WorkflowEventReverseReads, Src: []string{WorkflowStateReadsSwitched}, Dst: WorkflowStateReplicating},
		{Name: WorkflowEventReverseReads, Src: []string{WorkflowStateReadsAndWritesSwitched}, Dst: WorkflowStateWritesSwitched},
		{Name: WorkflowEventReverseWrites, Src: []string{WorkflowStateWritesSwitched}, Dst: WorkflowStateReplicating},
		{Name: WorkflowEventReverseWrites, Src: []string{WorkflowStateReadsAndWritesSwitched}, Dst: WorkflowStateReadsSwitched},
	}
}

func NewWorkflow(name, typ string, callbacks map[string]fsm.Callback) (*Workflow, error) {
	wf := &Workflow{
		name: name, typ: typ,
	}

	wf.wsm = fsm.NewFSM(WorkflowStateNotStarted, getWorkflowTransitions(), callbacks)
	return wf, nil
}

type MoveTablesWorkflow struct {
	ctx       context.Context
	wf        *Workflow
	allTables bool
	wr        *Wrangler

	sourceKeyspace, targetKeyspace, tableSpecs, cell, tabletTypes, excludeTables string
}

func (mtwf *MoveTablesWorkflow) String() string {
	s := fmt.Sprintf("%s Workflow %s from keyspace %s to keyspace %s. Current State: %s\n",
		mtwf.wf.typ, mtwf.wf.name, mtwf.targetKeyspace, mtwf.sourceKeyspace, mtwf.wf.wsm.Current())
	return s
}

func (wr *Wrangler) NewMoveTablesWorkflow(ctx context.Context, workflow, sourceKeyspace, targetKeyspace, tableSpecs,
	cell, tabletTypes string, allTables bool, excludeTables string) (*MoveTablesWorkflow, error) {
	callbacks := make(map[string]fsm.Callback)
	mtwf := &MoveTablesWorkflow{wr: wr, ctx: ctx, sourceKeyspace: sourceKeyspace, targetKeyspace: targetKeyspace,
		tabletTypes: tabletTypes, tableSpecs: tableSpecs, cell: cell,
		allTables: allTables, excludeTables: excludeTables}

	callbacks["before_"+WorkflowEventStart] = func(e *fsm.Event) { mtwf.initMoveTables() }
	callbacks["before_"+WorkflowEventSwitchReads] = func(e *fsm.Event) { mtwf.switchReads() }
	callbacks["before_"+WorkflowEventSwitchWrites] = func(e *fsm.Event) { mtwf.switchWrites() }

	ts, ws, err := wr.getWorkflowState(ctx, targetKeyspace, workflow)
	if err != nil {
		return nil, err
	}
	wf, err := NewWorkflow(workflow, "MoveTables", callbacks)
	if err != nil {
		return nil, err
	}
	mtwf.sourceKeyspace = ts.sourceKeyspace

	state := ""
	if ts == nil {
		state = WorkflowStateNotStarted
	} else if ws.RdonlyReadsSwitched && ws.ReplicaReadsSwitched {
		state = WorkflowStateReadsSwitched
	} else if ws.WritesSwitched {
		state = WorkflowStateWritesSwitched
	} else {
		state = WorkflowStateReplicating //FIXME: copying, error, ...
	}
	if state == "" {
		return nil, fmt.Errorf("workflow is in an inconsistent state: %+v", mtwf)
	}
	wf.wsm.SetState(state)
	mtwf.wf = wf
	return mtwf, nil
}

func (mtwf *MoveTablesWorkflow) FireEvent(ev string) error {
	return mtwf.wf.wsm.Event(ev)

}

func (mtwf *MoveTablesWorkflow) Start() error {
	log.Infof("In MoveTablesWorkflow.Start() for %+v", mtwf)
	err := mtwf.wf.wsm.Event(WorkflowEventStart)
	if err != nil {
		return err
	}
	return nil
}

func (mtwf *MoveTablesWorkflow) initMoveTables() error {
	log.Infof("In MoveTablesWorkflow.initMoveTables() for %+v", mtwf)
	return mtwf.wr.MoveTables(mtwf.ctx, mtwf.wf.name, mtwf.sourceKeyspace, mtwf.targetKeyspace, mtwf.tableSpecs,
		mtwf.cell, mtwf.tabletTypes, mtwf.allTables, mtwf.excludeTables)
}

func (mtwf *MoveTablesWorkflow) switchReads() error {
	log.Infof("In MoveTablesWorkflow.switchReads() for %+v", mtwf)
	_, err := mtwf.wr.SwitchReads(mtwf.ctx, mtwf.targetKeyspace, mtwf.wf.name,
		[]topodatapb.TabletType{topodatapb.TabletType_REPLICA, topodatapb.TabletType_RDONLY}, nil, DirectionForward, false)
	if err != nil {
		return err
	}
	return nil
}

func (mtwf *MoveTablesWorkflow) switchWrites() error {
	log.Infof("In MoveTablesWorkflow.switchWrites() for %+v", mtwf)
	journalId, _, err := mtwf.wr.SwitchWrites(mtwf.ctx, mtwf.targetKeyspace, mtwf.wf.name, DefaultFilteredReplicationWaitTime,
		false, false, true, false)
	if err != nil {
		return err
	}
	log.Infof("switchWrites succeeded with journal id %s", journalId)
	return nil
}

/*

New
GetState

Start
Pause
Restart

SwitchReads
ResetReads
SwitchWrites
ResetWrites

GetProgress
Abort
Finalize

*/
