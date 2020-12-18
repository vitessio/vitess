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

	"vitess.io/vitess/go/vt/log"
)

/*
	TODO
    * expand e2e for testing all possible transitions

	* Unit Tests (run coverage first and identify)

	* implement/test Reshard same as MoveTables!
*/

const (
	WorkflowActionStart          = "Start"
	WorkflowActionSwitchTraffic  = "SwitchTraffic"
	WorkflowActionReverseTraffic = "ReverseTraffic"
	WorkflowActionComplete       = "Complete"
	WorkflowActionAbort          = "Abort"
)

type reshardingWorkflowInfo struct {
	name string
	typ  string
}

func newWorkflow(name, typ string) (*reshardingWorkflowInfo, error) {
	wf := &reshardingWorkflowInfo{
		name: name, typ: typ,
	}
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
	s := ""
	return s
}

// MoveTablesParams stores args and options passed to a MoveTables command
type MoveTablesParams struct {
	Workflow, SourceKeyspace, TargetKeyspace, Tables string
	Cells, TabletTypes, ExcludeTables                string
	EnableReverseReplication, DryRun, AllTables      bool
	RenameTables, KeepData                           bool
	Timeout                                          time.Duration
	Direction                                        TrafficSwitchDirection
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
	wf, err := newWorkflow(params.Workflow, "MoveTables")
	if err != nil {
		return nil, err
	}
	if ts != nil { //Other than on Start we need to get SourceKeyspace from the workflow
		mtwf.params.TargetKeyspace = ts.targetKeyspace
		mtwf.params.Workflow = ts.workflow
		mtwf.params.SourceKeyspace = ts.sourceKeyspace
	}
	mtwf.ts = ts
	mtwf.ws = ws
	mtwf.wf = wf
	return mtwf, nil
}

// Exists checks if the workflow has already been initiated
func (mtwf *MoveTablesWorkflow) Exists() bool {
	log.Infof("mtwf %v", mtwf)

	return mtwf.ws != nil
}

// CurrentState returns the current state of the workflow's finite state machine
func (mtwf *MoveTablesWorkflow) CurrentState() string {
	log.Infof("mtwf %v", mtwf)
	var stateInfo []string
	ws := mtwf.ws
	s := ""
	if !mtwf.Exists() {
		stateInfo = append(stateInfo, "Not Started")
	} else {
		if len(ws.RdonlyCellsNotSwitched) == 0 && len(ws.ReplicaCellsNotSwitched) == 0 {
			s = "All Reads Switched"
		} else if len(ws.RdonlyCellsSwitched) == 0 && len(ws.ReplicaCellsSwitched) == 0 {
			s = "Reads Not Switched"
		} else {
			s = "Reads Partially Switched: "
			if len(ws.ReplicaCellsNotSwitched) == 0 {
				s += "All Replica Reads Switched"
			} else {
				s += "Replicas switched in cells: " + strings.Join(ws.ReplicaCellsSwitched, ",")
			}
			if len(ws.RdonlyCellsNotSwitched) == 0 {
				s += "All Rdonly Reads Switched"
			} else {
				s += "Rdonly switched in cells: " + strings.Join(ws.RdonlyCellsSwitched, ",")
			}
		}
		stateInfo = append(stateInfo, s)
		if ws.WritesSwitched {
			stateInfo = append(stateInfo, "Writes Switched")
		} else {
			stateInfo = append(stateInfo, "Writes Not Switched")
		}
	}
	return strings.Join(stateInfo, ". ")
}

// Start initiates a workflow
func (mtwf *MoveTablesWorkflow) Start() error {
	if mtwf.Exists() {
		return fmt.Errorf("workflow has already been started")
	}
	return mtwf.initMoveTables()
}

// SwitchTraffic switches traffic forward for tablet_types passed
func (mtwf *MoveTablesWorkflow) SwitchTraffic(direction TrafficSwitchDirection) error {
	if !mtwf.Exists() {
		return fmt.Errorf("workflow has not yet been started")
	}
	mtwf.params.Direction = direction
	hasReplica, hasRdonly, hasMaster, err := mtwf.parseTabletTypes()
	if err != nil {
		return err
	}
	if hasReplica || hasRdonly {
		if err := mtwf.switchReads(); err != nil {
			return err
		}
	}
	if hasMaster {
		if err := mtwf.switchWrites(); err != nil {
			return err
		}
	}
	return nil
}

// ReverseTraffic switches traffic backwards for tablet_types passed
func (mtwf *MoveTablesWorkflow) ReverseTraffic() error {
	if !mtwf.Exists() {
		return fmt.Errorf("workflow has not yet been started")
	}
	return mtwf.SwitchTraffic(DirectionBackward)
}

// Complete cleans up a successful workflow
func (mtwf *MoveTablesWorkflow) Complete() error {
	ws := mtwf.ws
	if !ws.WritesSwitched || len(ws.ReplicaCellsNotSwitched) > 0 || len(ws.RdonlyCellsNotSwitched) > 0 {
		return fmt.Errorf("cannot complete workflow because you have not yet switched all read and write traffic")
	}
	var renameTable TableRemovalType
	if mtwf.params.RenameTables {
		renameTable = RenameTable
	} else {
		renameTable = DropTable
	}
	if _, err := mtwf.wr.DropSources(mtwf.ctx, mtwf.ws.TargetKeyspace, mtwf.ws.Workflow, renameTable, mtwf.params.KeepData, false, false); err != nil {
		return err
	}
	return nil
}

// Abort deletes all artifacts from a workflow which has not yet been switched
func (mtwf *MoveTablesWorkflow) Abort() error {
	ws := mtwf.ws
	if ws.WritesSwitched || len(ws.ReplicaCellsSwitched) > 0 || len(ws.RdonlyCellsSwitched) > 0 {
		return fmt.Errorf("cannot abort workflow because you have already switched some or all read and write traffic")
	}
	if _, err := mtwf.wr.DropTargets(mtwf.ctx, mtwf.ws.TargetKeyspace, mtwf.ws.Workflow, mtwf.params.KeepData, false); err != nil {
		return err
	}
	return nil
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

func (mtwf *MoveTablesWorkflow) parseTabletTypes() (hasReplica, hasRdonly, hasMaster bool, err error) {
	tabletTypesArr := strings.Split(mtwf.params.TabletTypes, ",")
	for _, tabletType := range tabletTypesArr {
		switch tabletType {
		case "replica":
			hasReplica = true
		case "rdonly":
			hasRdonly = true
		case "master":
			hasMaster = true
		default:
			return false, false, false, fmt.Errorf("invalid tablet type passed %s", tabletType)
		}
	}
	return hasReplica, hasRdonly, hasMaster, nil
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
	var tabletTypes []topodatapb.TabletType
	for _, tt := range mtwf.getTabletTypes() {
		if tt != topodatapb.TabletType_MASTER {
			tabletTypes = append(tabletTypes, tt)
		}
	}

	_, err := mtwf.wr.SwitchReads(mtwf.ctx, mtwf.params.TargetKeyspace, mtwf.wf.name, tabletTypes,
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
