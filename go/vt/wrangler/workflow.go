package wrangler

import (
	"context"

	"github.com/looplab/fsm"
	"vitess.io/vitess/go/vt/log"
)

const (
	WorkflowStateNotStarted             = "Not Started"
	WorkflowStateCopying                = "Copying"
	WorkflowStateReplicating            = "Replicating"
	WorkflowStateOnlyReadsSwitched      = "Reads Switched"
	WorkflowStateOnlyWritesSwitched     = "Writes Switched"
	WorkflowStateReadsAndWritesSwitched = "Both Reads and Writes Switched"
	WorkflowStateCompleted              = "Completed"
	WorkflowStateAborted                = "Aborted"
)

const (
	WorkflowEventStart         = "Start"
	WorkflowEventCopyCompleted = "CopyCompleted"
	WorkflowEventSwitchReads   = "SwitchReads"
	WorkflowEventSwitchWrites  = "SwitchWrites"
	WorkflowEventComplete      = "Complete"
	WorkflowEventAbort         = "Abort"
	WorkflowEventReverseReads  = "ReverseReads"
	WorkflowEventReverseWrites = "ReverseWrites"
)

type Workflow struct {
	name          string
	wsm           *fsm.FSM
	typ           string
	isReplicating bool
	isRunning     bool
	hasErrors     bool
}

func init() {
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
		{Name: WorkflowEventSwitchReads, Src: []string{WorkflowStateReplicating}, Dst: WorkflowStateOnlyReadsSwitched},
		{Name: WorkflowEventSwitchReads, Src: []string{WorkflowStateOnlyWritesSwitched}, Dst: WorkflowStateReadsAndWritesSwitched},
		{Name: WorkflowEventSwitchWrites, Src: []string{WorkflowStateReplicating}, Dst: WorkflowStateOnlyWritesSwitched},
		{Name: WorkflowEventSwitchWrites, Src: []string{WorkflowStateOnlyReadsSwitched}, Dst: WorkflowStateReadsAndWritesSwitched},
		{Name: WorkflowEventComplete, Src: []string{WorkflowStateReadsAndWritesSwitched}, Dst: WorkflowStateCompleted},
		{Name: WorkflowEventAbort, Src: []string{WorkflowStateNotStarted, WorkflowStateCopying,
			WorkflowStateReplicating, WorkflowStateOnlyReadsSwitched, WorkflowStateOnlyWritesSwitched}, Dst: WorkflowStateAborted},
		{Name: WorkflowEventReverseReads, Src: []string{WorkflowStateOnlyReadsSwitched}, Dst: WorkflowStateReplicating},
		{Name: WorkflowEventReverseReads, Src: []string{WorkflowStateReadsAndWritesSwitched}, Dst: WorkflowStateOnlyWritesSwitched},
		{Name: WorkflowEventReverseWrites, Src: []string{WorkflowStateOnlyWritesSwitched}, Dst: WorkflowStateReplicating},
		{Name: WorkflowEventReverseWrites, Src: []string{WorkflowStateReadsAndWritesSwitched}, Dst: WorkflowStateOnlyReadsSwitched},
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

func (wr *Wrangler) NewMoveTablesWorkflow(ctx context.Context, workflow, sourceKeyspace, targetKeyspace, tableSpecs,
	cell, tabletTypes string, allTables bool, excludeTables string) (*MoveTablesWorkflow, error) {
	callbacks := make(map[string]fsm.Callback)
	mtwf := &MoveTablesWorkflow{wr: wr, ctx: ctx, sourceKeyspace: sourceKeyspace, targetKeyspace: targetKeyspace,
		tabletTypes: tabletTypes, tableSpecs: tableSpecs, cell: cell,
		allTables: allTables, excludeTables: excludeTables}
	callbacks["before_Start"] = func(e *fsm.Event) { mtwf.initMoveTables() }
	wf, err := NewWorkflow(workflow, "MoveTables", callbacks)
	if err != nil {
		return nil, err
	}
	mtwf.wf = wf
	return mtwf, nil
}

func (mtwf *MoveTablesWorkflow) Start() error {
	log.Infof("In MoveTablesWorkflow.Start() for %+v", mtwf)
	mtwf.wf.wsm.Event(WorkflowEventStart)
	return nil
}

func (mtwf *MoveTablesWorkflow) initMoveTables() error {
	log.Infof("In MoveTablesWorkflow.initMoveTables() for %+v", mtwf)
	return mtwf.wr.MoveTables(mtwf.ctx, mtwf.wf.name, mtwf.sourceKeyspace, mtwf.targetKeyspace, mtwf.tableSpecs,
		mtwf.cell, mtwf.tabletTypes, mtwf.allTables, mtwf.excludeTables)
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
