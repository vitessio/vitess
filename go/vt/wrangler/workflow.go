package wrangler

import (
	"context"
	"fmt"
	"strings"
	"time"

	"vitess.io/vitess/go/sqltypes"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vtgate/evalengine"

	"github.com/looplab/fsm"
	"vitess.io/vitess/go/vt/log"
)

/*
	TODO
	* use SwitchTraffic and ReverseTraffic
	* Actions: Abort, Complete
	* Unit Tests (lots of!)
    * expand e2e for testing all possible transitions

	* implement Reshard same as MoveTables!

*/

// Possible workflow states
const (
	WorkflowStateNotStarted                    = "Not Started"
	WorkflowStateStarted                       = "Replicating, Reads and Writes Not Switched"
	WorkflowStateReplicaReadsSwitched          = "Replica Reads Switched"
	WorkflowStateRdonlyReadsSwitched           = "Rdonly Reads Switched"
	WorkflowStateReadsSwitched                 = "Reads Switched"
	WorkflowStateWritesSwitched                = "Writes Switched"
	WorkflowStateReplicaReadsAndWritesSwitched = "Writes and Replica Reads Switched"
	WorkflowStateRdonlyReadsAndWritesSwitched  = "Writes and Rdonly Reads Switched"
	WorkflowStateReadsAndWritesSwitched        = "Both Reads and Writes Switched"
	WorkflowStateCompleted                     = "Completed"
	WorkflowStateAborted                       = "Aborted"
)

// Possible events that cause workflow state transitions
const (
	WorkflowEventStart              = "Start"
	WorkflowEventSwitchReads        = "SwitchReads"
	WorkflowEventSwitchReplicaReads = "SwitchReplicaReads"
	WorkflowEventSwitchRdonlyReads  = "SwitchRdonlyReads"
	WorkflowEventSwitchWrites       = "SwitchWrites"
	WorkflowEventComplete           = "Complete"
	WorkflowEventAbort              = "Abort"
	WorkflowEventReverseReads       = "ReverseReads"
	WorkflowEventReverseWrites      = "ReverseWrites"
)

type reshardingWorkflowInfo struct {
	name string
	wsm  *fsm.FSM
	typ  string
}

var eventNameMap map[string]string

func init() {
	eventNameMap = make(map[string]string)
	transitions := getWorkflowTransitions()
	for _, transition := range transitions {
		eventNameMap[strings.ToLower(transition.Name)] = transition.Name
	}
}

// region FSM setup

func getWorkflowTransitions() []fsm.EventDesc {
	return []fsm.EventDesc{
		{Name: WorkflowEventStart, Src: []string{WorkflowStateNotStarted}, Dst: WorkflowStateStarted},

		{Name: WorkflowEventSwitchReplicaReads, Src: []string{WorkflowStateStarted}, Dst: WorkflowStateReplicaReadsSwitched},
		{Name: WorkflowEventSwitchReplicaReads, Src: []string{WorkflowStateRdonlyReadsSwitched}, Dst: WorkflowStateReadsSwitched},
		{Name: WorkflowEventSwitchReplicaReads, Src: []string{WorkflowStateWritesSwitched}, Dst: WorkflowStateReplicaReadsAndWritesSwitched},

		{Name: WorkflowEventSwitchRdonlyReads, Src: []string{WorkflowStateStarted}, Dst: WorkflowStateRdonlyReadsSwitched},
		{Name: WorkflowEventSwitchRdonlyReads, Src: []string{WorkflowStateReplicaReadsSwitched}, Dst: WorkflowStateReadsSwitched},
		{Name: WorkflowEventSwitchRdonlyReads, Src: []string{WorkflowStateWritesSwitched}, Dst: WorkflowStateRdonlyReadsAndWritesSwitched},

		{Name: WorkflowEventSwitchReads, Src: []string{WorkflowStateStarted}, Dst: WorkflowStateReadsSwitched},
		{Name: WorkflowEventSwitchReads, Src: []string{WorkflowStateWritesSwitched}, Dst: WorkflowStateReadsAndWritesSwitched},

		{Name: WorkflowEventSwitchWrites, Src: []string{WorkflowStateStarted}, Dst: WorkflowStateWritesSwitched},
		{Name: WorkflowEventSwitchWrites, Src: []string{WorkflowStateReadsSwitched}, Dst: WorkflowStateReadsAndWritesSwitched},
		{Name: WorkflowEventSwitchWrites, Src: []string{WorkflowStateReplicaReadsSwitched}, Dst: WorkflowStateReplicaReadsAndWritesSwitched},
		{Name: WorkflowEventSwitchWrites, Src: []string{WorkflowStateRdonlyReadsSwitched}, Dst: WorkflowStateRdonlyReadsAndWritesSwitched},

		{Name: WorkflowEventComplete, Src: []string{WorkflowStateReadsAndWritesSwitched}, Dst: WorkflowStateCompleted},
		{Name: WorkflowEventAbort, Src: []string{WorkflowStateNotStarted,
			WorkflowStateStarted, WorkflowStateReadsSwitched, WorkflowStateWritesSwitched}, Dst: WorkflowStateAborted},

		{Name: WorkflowEventReverseReads, Src: []string{WorkflowStateReadsSwitched,
			WorkflowStateReplicaReadsSwitched, WorkflowStateRdonlyReadsSwitched}, Dst: WorkflowStateStarted},
		{Name: WorkflowEventReverseReads, Src: []string{WorkflowStateReadsAndWritesSwitched,
			WorkflowStateReplicaReadsAndWritesSwitched, WorkflowStateRdonlyReadsAndWritesSwitched}, Dst: WorkflowStateWritesSwitched},

		{Name: WorkflowEventReverseWrites, Src: []string{WorkflowStateWritesSwitched}, Dst: WorkflowStateStarted},
		{Name: WorkflowEventReverseWrites, Src: []string{WorkflowStateReadsAndWritesSwitched}, Dst: WorkflowStateReadsSwitched},
	}
}

func (mtwf *MoveTablesWorkflow) getCallbacks() map[string]fsm.Callback {
	callbacks := make(map[string]fsm.Callback)
	callbacks["before_"+WorkflowEventStart] = func(e *fsm.Event) {
		if err := mtwf.initMoveTables(); err != nil {
			e.Cancel(err)
		}
	}
	callbacks["before_"+WorkflowEventSwitchReads] = func(e *fsm.Event) {
		mtwf.params.TabletTypes = "replica,rdonly"
		if err := mtwf.switchReads(); err != nil {
			e.Cancel(err)
		}
	}
	callbacks["before_"+WorkflowEventSwitchReplicaReads] = func(e *fsm.Event) {
		mtwf.params.TabletTypes = "replica"
		if err := mtwf.switchReads(); err != nil {
			e.Cancel(err)
		}
	}
	callbacks["before_"+WorkflowEventSwitchRdonlyReads] = func(e *fsm.Event) {
		mtwf.params.TabletTypes = "rdonly"
		if err := mtwf.switchReads(); err != nil {
			e.Cancel(err)
		}
	}
	callbacks["before_"+WorkflowEventSwitchWrites] = func(e *fsm.Event) {
		if err := mtwf.switchWrites(); err != nil {
			e.Cancel(err)
		}
	}
	callbacks["before_"+WorkflowEventReverseReads] = func(e *fsm.Event) {
		var tabletTypes []string
		if mtwf.ws.ReplicaReadsSwitched && mtwf.ws.RdonlyReadsSwitched {
			tabletTypes = append(tabletTypes, "replica", "rdonly")
		} else if mtwf.ws.ReplicaReadsSwitched {
			tabletTypes = append(tabletTypes, "replica")
		} else if mtwf.ws.RdonlyReadsSwitched {
			tabletTypes = append(tabletTypes, "rdonly")

		} else {
			e.Cancel(fmt.Errorf("reads have not been switched for %s.%s", mtwf.params.TargetKeyspace, mtwf.params.Workflow))
			return
		}
		mtwf.params.TabletTypes = strings.Join(tabletTypes, ",")
		mtwf.params.Direction = DirectionBackward
		if err := mtwf.switchReads(); err != nil {
			e.Cancel(err)
		}
	}
	callbacks["before_"+WorkflowEventReverseWrites] = func(e *fsm.Event) {
		mtwf.params.Direction = DirectionBackward
		if err := mtwf.switchWrites(); err != nil {
			e.Cancel(err)
		}
	}

	return callbacks
}

func newWorkflow(name, typ string, callbacks map[string]fsm.Callback) (*reshardingWorkflowInfo, error) {
	wf := &reshardingWorkflowInfo{
		name: name, typ: typ,
	}

	wf.wsm = fsm.NewFSM(WorkflowStateNotStarted, getWorkflowTransitions(), callbacks)
	return wf, nil
}

// endregion

// region Move Tables Public API

// MoveTablesWorkflow stores various internal objects for a workflow
type MoveTablesWorkflow struct {
	ctx    context.Context
	wf     *reshardingWorkflowInfo
	wr     *Wrangler
	params *MoveTablesParams
	ts     *trafficSwitcher
	ws     *workflowState
}

func (mtwf *MoveTablesWorkflow) String() string {
	s := fmt.Sprintf("%s workflow %s from keyspace %s to keyspace %s.\nCurrent State: %s",
		mtwf.wf.typ, mtwf.wf.name, mtwf.params.SourceKeyspace, mtwf.params.TargetKeyspace, mtwf.wf.wsm.Current())
	return s
}

// AvailableActions returns all available actions for a workflow, for display purposes
func (mtwf *MoveTablesWorkflow) AvailableActions() string {
	return strings.Join(mtwf.wf.wsm.AvailableTransitions(), ",")
}

// MoveTablesParams stores args and options passed to a MoveTables command
type MoveTablesParams struct {
	Workflow, SourceKeyspace, TargetKeyspace, Tables string
	Cells, TabletTypes, ExcludeTables                string
	EnableReverseReplication, DryRun, AllTables      bool

	Timeout   time.Duration
	Direction TrafficSwitchDirection
}

// NewMoveTablesWorkflow sets up a MoveTables workflow object based on options provided, deduces the state of the
// workflow from the persistent state stored in the vreplication table and the topo
func (wr *Wrangler) NewMoveTablesWorkflow(ctx context.Context, params *MoveTablesParams) (*MoveTablesWorkflow, error) {
	log.Infof("NewMoveTablesWorkflow with params %+v", params)
	mtwf := &MoveTablesWorkflow{wr: wr, ctx: ctx, params: params}
	ts, ws, err := wr.getWorkflowState(ctx, params.TargetKeyspace, params.Workflow)
	if err != nil {
		return nil, err
	}
	log.Infof("Workflow state is %+v", ws)
	wf, err := newWorkflow(params.Workflow, "MoveTables", mtwf.getCallbacks())
	if err != nil {
		return nil, err
	}
	if ts != nil { //Other than on Start we need to get SourceKeyspace from the workflow
		mtwf.params.SourceKeyspace = ts.sourceKeyspace
		mtwf.ts = ts
	}
	mtwf.ws = ws
	state := ""
	if ts == nil {
		state = WorkflowStateNotStarted
	} else if ws.WritesSwitched {
		if ws.ReplicaReadsSwitched && ws.RdonlyReadsSwitched {
			state = WorkflowStateReadsAndWritesSwitched
		} else if ws.RdonlyReadsSwitched {
			state = WorkflowStateRdonlyReadsAndWritesSwitched
		} else if ws.ReplicaReadsSwitched {
			state = WorkflowStateReplicaReadsAndWritesSwitched
		} else {
			state = WorkflowStateWritesSwitched
		}
	} else if ws.RdonlyReadsSwitched && ws.ReplicaReadsSwitched {
		state = WorkflowStateReadsSwitched
	} else if ws.RdonlyReadsSwitched {
		state = WorkflowStateRdonlyReadsSwitched
	} else if ws.ReplicaReadsSwitched {
		state = WorkflowStateReplicaReadsSwitched
	} else {
		state = WorkflowStateStarted
	}
	if state == "" {
		return nil, fmt.Errorf("workflow is in an inconsistent state: %+v", mtwf)
	}
	log.Infof("Setting workflow state to %s", state)
	wf.wsm.SetState(state)
	mtwf.wf = wf
	return mtwf, nil
}

// FireEvent causes the transition of the workflow by applying a valid action specified in MoveTables
func (mtwf *MoveTablesWorkflow) FireEvent(ev string) error {
	ev = eventNameMap[strings.ToLower(ev)]
	return mtwf.wf.wsm.Event(ev)
}

// IsActionValid checks if a MoveTables subcommand is a valid event for the current state of the workflow
func (mtwf *MoveTablesWorkflow) IsActionValid(ev string) bool {
	ev = eventNameMap[strings.ToLower(ev)]
	return mtwf.wf.wsm.Can(ev)
}

// CurrentState returns the current state of the workflow's finite state machine
func (mtwf *MoveTablesWorkflow) CurrentState() string {
	return mtwf.wf.wsm.Current()
}

// Visualize returns a graphViz script for the (static) resharding workflow state machine
func (mtwf *MoveTablesWorkflow) Visualize() string {
	return fsm.Visualize(mtwf.wf.wsm)
}

// endregion

// region Helpers

func (mtwf *MoveTablesWorkflow) getCellsAsArray() []string {
	if mtwf.params.Cells != "" {
		return strings.Split(mtwf.params.Cells, ",")
	}
	return nil
}

func (mtwf *MoveTablesWorkflow) getTabletTypes() []topodatapb.TabletType {
	tabletTypesArr := strings.Split(mtwf.params.TabletTypes, ",")
	var tabletTypes []topodatapb.TabletType
	for _, tabletType := range tabletTypesArr {
		servedType, _ := topoproto.ParseTabletType(tabletType)
		tabletTypes = append(tabletTypes, servedType)
	}
	return tabletTypes
}

// endregion

// region Core Actions

func (mtwf *MoveTablesWorkflow) initMoveTables() error {
	log.Infof("In MoveTablesWorkflow.initMoveTables() for %+v", mtwf)
	return mtwf.wr.MoveTables(mtwf.ctx, mtwf.wf.name, mtwf.params.SourceKeyspace, mtwf.params.TargetKeyspace, mtwf.params.Tables,
		mtwf.params.Cells, mtwf.params.TabletTypes, mtwf.params.AllTables, mtwf.params.ExcludeTables)
}

func (mtwf *MoveTablesWorkflow) switchReads() error {
	log.Infof("In MoveTablesWorkflow.switchReads() for %+v", mtwf)
	_, err := mtwf.wr.SwitchReads(mtwf.ctx, mtwf.params.TargetKeyspace, mtwf.wf.name, mtwf.getTabletTypes(),
		mtwf.getCellsAsArray(), mtwf.params.Direction, false)
	if err != nil {
		return err
	}
	return nil
}

func (mtwf *MoveTablesWorkflow) switchWrites() error {
	log.Infof("In MoveTablesWorkflow.switchWrites() for %+v", mtwf)
	if mtwf.params.Direction == DirectionBackward {
		keyspace := mtwf.params.SourceKeyspace
		mtwf.params.SourceKeyspace = mtwf.params.TargetKeyspace
		mtwf.params.TargetKeyspace = keyspace
		mtwf.params.Workflow = reverseName(mtwf.params.Workflow)
		log.Infof("In MoveTablesWorkflow.switchWrites(reverse) for %+v", mtwf)
	}
	journalID, _, err := mtwf.wr.SwitchWrites(mtwf.ctx, mtwf.params.TargetKeyspace, mtwf.params.Workflow, mtwf.params.Timeout,
		false, mtwf.params.Direction == DirectionBackward, mtwf.params.EnableReverseReplication, false)
	if err != nil {
		return err
	}
	log.Infof("switchWrites succeeded with journal id %s", journalID)
	return nil
}

// endregion

// region Copy Progress

// TableCopyProgress stores the row counts and disk sizes of the source and target tables
type TableCopyProgress struct {
	TargetRowCount, TargetTableSize int64
	SourceRowCount, SourceTableSize int64
}

// CopyProgress stores the TableCopyProgress for all tables still being copied
type CopyProgress map[string]*TableCopyProgress

// GetCopyProgress returns the progress of all tables being copied in the workflow
func (mtwf *MoveTablesWorkflow) GetCopyProgress() (*CopyProgress, error) {
	ctx := context.Background()
	getTablesQuery := "select table_name from _vt.copy_state cs, _vt.vreplication vr where vr.id = cs.vrepl_id and vr.id = %d"
	getRowCountQuery := "select table_name, table_rows, data_length from information_schema.tables where table_schema = %s and table_name in (%s)"
	tables := make(map[string]bool)
	const MaxRows = 1000
	sourceMasters := make(map[*topodatapb.TabletAlias]bool)
	for _, target := range mtwf.ts.targets {
		for id, bls := range target.sources {
			query := fmt.Sprintf(getTablesQuery, id)
			p3qr, err := mtwf.wr.tmc.ExecuteFetchAsDba(ctx, target.master.Tablet, true, []byte(query), MaxRows, false, false)
			if err != nil {
				return nil, err
			}
			if len(p3qr.Rows) < 1 {
				continue
			}
			qr := sqltypes.Proto3ToResult(p3qr)
			for i := 0; i < len(p3qr.Rows); i++ {
				tables[qr.Rows[0][0].ToString()] = true
			}
			sourcesi, err := mtwf.wr.ts.GetShard(ctx, bls.Keyspace, bls.Shard)
			if err != nil {
				return nil, err
			}
			sourceMasters[sourcesi.MasterAlias] = true
		}
	}
	if len(tables) == 0 {
		return nil, nil
	}
	tableList := ""
	targetRowCounts := make(map[string]int64)
	sourceRowCounts := make(map[string]int64)
	targetTableSizes := make(map[string]int64)
	sourceTableSizes := make(map[string]int64)

	for table := range tables {
		if tableList != "" {
			tableList += ","
		}
		tableList += encodeString(table)
		targetRowCounts[table] = 0
		sourceRowCounts[table] = 0
		targetTableSizes[table] = 0
		sourceTableSizes[table] = 0
	}

	var getTableMetrics = func(tablet *topodatapb.Tablet, query string, rowCounts *map[string]int64, tableSizes *map[string]int64) error {
		p3qr, err := mtwf.wr.tmc.ExecuteFetchAsDba(ctx, tablet, true, []byte(query), len(tables), false, false)
		if err != nil {
			return err
		}
		qr := sqltypes.Proto3ToResult(p3qr)
		for i := 0; i < len(qr.Rows); i++ {
			table := qr.Rows[0][0].ToString()
			rowCount, err := evalengine.ToInt64(qr.Rows[0][1])
			if err != nil {
				return err
			}
			tableSize, err := evalengine.ToInt64(qr.Rows[0][2])
			if err != nil {
				return err
			}
			(*rowCounts)[table] += rowCount
			(*tableSizes)[table] += tableSize
		}
		return nil
	}
	sourceDbName := ""
	for _, tsSource := range mtwf.ts.sources {
		sourceDbName = tsSource.master.DbName()
		break
	}
	if sourceDbName == "" {
		return nil, fmt.Errorf("no sources found for workflow %s.%s", mtwf.ws.TargetKeyspace, mtwf.ws.Workflow)
	}
	targetDbName := ""
	for _, tsTarget := range mtwf.ts.targets {
		targetDbName = tsTarget.master.DbName()
		break
	}
	if sourceDbName == "" || targetDbName == "" {
		return nil, fmt.Errorf("workflow %s.%s is incorrectly configured", mtwf.ws.TargetKeyspace, mtwf.ws.Workflow)
	}

	query := fmt.Sprintf(getRowCountQuery, encodeString(targetDbName), tableList)
	log.Infof("query is %s", query)
	for _, target := range mtwf.ts.targets {
		tablet := target.master.Tablet
		if err := getTableMetrics(tablet, query, &targetRowCounts, &targetTableSizes); err != nil {
			return nil, err
		}
	}

	query = fmt.Sprintf(getRowCountQuery, encodeString(sourceDbName), tableList)
	log.Infof("query is %s", query)
	for source := range sourceMasters {
		ti, err := mtwf.wr.ts.GetTablet(ctx, source)
		tablet := ti.Tablet
		if err != nil {
			return nil, err
		}
		if err := getTableMetrics(tablet, query, &sourceRowCounts, &sourceTableSizes); err != nil {
			return nil, err
		}
	}

	copyProgress := CopyProgress{}
	for table, rowCount := range targetRowCounts {
		copyProgress[table] = &TableCopyProgress{
			TargetRowCount:  rowCount,
			TargetTableSize: targetTableSizes[table],
			SourceRowCount:  sourceRowCounts[table],
			SourceTableSize: sourceTableSizes[table],
		}
	}
	return &copyProgress, nil
}

// endregion
