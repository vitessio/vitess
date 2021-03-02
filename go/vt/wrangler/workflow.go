package wrangler

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"vitess.io/vitess/go/sqltypes"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vtgate/evalengine"

	"vitess.io/vitess/go/vt/log"
)

// VReplicationWorkflowType specifies whether workflow is MoveTables or Reshard
type VReplicationWorkflowType int

// VReplicationWorkflowType enums
const (
	MoveTablesWorkflow = VReplicationWorkflowType(iota)
	ReshardWorkflow
	MigrateWorkflow
)

// Workflow state display strings
const (
	WorkflowStateNotCreated     = "Not Created"
	WorkflowStateNotSwitched    = "Reads Not Switched. Writes Not Switched"
	WorkflowStateReadsSwitched  = "All Reads Switched. Writes Not Switched"
	WorkflowStateWritesSwitched = "Reads Not Switched. Writes Switched"
	WorkflowStateAllSwitched    = "All Reads Switched. Writes Switched"
)

// region Move Tables Public API

// VReplicationWorkflow stores various internal objects for a workflow
type VReplicationWorkflow struct {
	workflowType VReplicationWorkflowType
	ctx          context.Context
	wr           *Wrangler
	params       *VReplicationWorkflowParams
	ts           *trafficSwitcher
	ws           *workflowState
}

func (vrw *VReplicationWorkflow) String() string {
	s := ""
	s += fmt.Sprintf("Parameters: %+v\n", vrw.params)
	s += fmt.Sprintf("State: %+v", vrw.CachedState())
	return s
}

// VReplicationWorkflowParams stores args and options passed to a VReplicationWorkflow command
type VReplicationWorkflowParams struct {
	WorkflowType                      VReplicationWorkflowType
	Workflow, TargetKeyspace          string
	Cells, TabletTypes, ExcludeTables string
	EnableReverseReplication, DryRun  bool
	KeepData                          bool
	Timeout                           time.Duration
	Direction                         TrafficSwitchDirection

	// MoveTables specific
	SourceKeyspace, Tables  string
	AllTables, RenameTables bool

	// Reshard specific
	SourceShards, TargetShards []string
	SkipSchemaCopy             bool
	AutoStart, StopAfterCopy   bool

	// Migrate specific
	ExternalCluster string
}

// NewVReplicationWorkflow sets up a MoveTables or Reshard workflow based on options provided, deduces the state of the
// workflow from the persistent state stored in the vreplication table and the topo
func (wr *Wrangler) NewVReplicationWorkflow(ctx context.Context, workflowType VReplicationWorkflowType,
	params *VReplicationWorkflowParams) (*VReplicationWorkflow, error) {

	log.Infof("NewVReplicationWorkflow with params %+v", params)
	vrw := &VReplicationWorkflow{wr: wr, ctx: ctx, params: params, workflowType: workflowType}
	ts, ws, err := wr.getWorkflowState(ctx, params.TargetKeyspace, params.Workflow)
	if err != nil {
		return nil, err
	}
	log.Infof("Workflow state is %+v", ws)
	if ts != nil { //Other than on create we need to get SourceKeyspace from the workflow
		vrw.params.TargetKeyspace = ts.targetKeyspace
		vrw.params.Workflow = ts.workflow
		vrw.params.SourceKeyspace = ts.sourceKeyspace
	}
	vrw.ts = ts
	vrw.ws = ws
	return vrw, nil
}

// CurrentState reloads and returns a human readable workflow state
func (vrw *VReplicationWorkflow) CurrentState() string {
	var err error
	vrw.ts, vrw.ws, err = vrw.wr.getWorkflowState(vrw.ctx, vrw.params.TargetKeyspace, vrw.params.Workflow)
	if err != nil {
		return err.Error()
	}
	if vrw.ws == nil {
		return "Workflow Not Found"
	}
	return vrw.stateAsString(vrw.ws)
}

// CachedState returns a human readable workflow state at the time the workflow was created
func (vrw *VReplicationWorkflow) CachedState() string {
	return vrw.stateAsString(vrw.ws)
}

// Exists checks if the workflow has already been initiated
func (vrw *VReplicationWorkflow) Exists() bool {
	return vrw.ws != nil
}

func (vrw *VReplicationWorkflow) stateAsString(ws *workflowState) string {
	log.Infof("Workflow state is %+v", ws)
	var stateInfo []string
	s := ""
	if !vrw.Exists() {
		stateInfo = append(stateInfo, WorkflowStateNotCreated)
	} else {
		if len(ws.RdonlyCellsNotSwitched) == 0 && len(ws.ReplicaCellsNotSwitched) == 0 && len(ws.ReplicaCellsSwitched) > 0 {
			s = "All Reads Switched"
		} else if len(ws.RdonlyCellsSwitched) == 0 && len(ws.ReplicaCellsSwitched) == 0 {
			s = "Reads Not Switched"
		} else {
			stateInfo = append(stateInfo, "Reads partially switched")
			if len(ws.ReplicaCellsNotSwitched) == 0 {
				s += "All Replica Reads Switched"
			} else if len(ws.ReplicaCellsSwitched) == 0 {
				s += "Replica not switched"
			} else {
				s += "Replica switched in cells: " + strings.Join(ws.ReplicaCellsSwitched, ",")
			}
			stateInfo = append(stateInfo, s)
			s = ""
			if len(ws.RdonlyCellsNotSwitched) == 0 {
				s += "All Rdonly Reads Switched"
			} else if len(ws.RdonlyCellsSwitched) == 0 {
				s += "Rdonly not switched"
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

// Create initiates a workflow
func (vrw *VReplicationWorkflow) Create() error {
	var err error
	if vrw.Exists() {
		return fmt.Errorf("workflow already exists")
	}
	if vrw.CachedState() != WorkflowStateNotCreated {
		return fmt.Errorf("workflow has already been created, state is %s", vrw.CachedState())
	}
	switch vrw.workflowType {
	case MoveTablesWorkflow, MigrateWorkflow:
		err = vrw.initMoveTables()
	case ReshardWorkflow:
		err = vrw.initReshard()
	default:
		return fmt.Errorf("unknown workflow type %d", vrw.workflowType)
	}
	if err != nil {
		return err
	}
	return nil
}

// WorkflowError has per stream errors if present in a workflow
type WorkflowError struct {
	Tablet      string
	ID          int64
	Description string
}

// NewWorkflowError returns a new WorkflowError object
func NewWorkflowError(tablet string, id int64, description string) *WorkflowError {
	wfErr := &WorkflowError{
		Tablet:      tablet,
		ID:          id,
		Description: description,
	}
	return wfErr
}

// GetStreamCount returns a count of total and running streams and any stream errors
func (vrw *VReplicationWorkflow) GetStreamCount() (int64, int64, []*WorkflowError, error) {
	var err error
	var workflowErrors []*WorkflowError
	var totalStreams, runningStreams int64
	res, err := vrw.wr.ShowWorkflow(vrw.ctx, vrw.params.Workflow, vrw.params.TargetKeyspace)
	if err != nil {
		return 0, 0, nil, err
	}
	for ksShard := range res.ShardStatuses {
		statuses := res.ShardStatuses[ksShard].MasterReplicationStatuses
		for _, st := range statuses {
			totalStreams++
			if strings.HasPrefix(st.Message, "Error:") {
				workflowErrors = append(workflowErrors, NewWorkflowError(st.Tablet, st.ID, st.Message))
				continue
			}
			if st.Pos == "" {
				continue
			}
			if st.State == "Running" {
				runningStreams++
			}
		}
	}

	return totalStreams, runningStreams, workflowErrors, nil
}

// SwitchTraffic switches traffic in the direction passed for specified tablet_types
func (vrw *VReplicationWorkflow) SwitchTraffic(direction TrafficSwitchDirection) (*[]string, error) {
	var dryRunResults []string
	var rdDryRunResults, wrDryRunResults *[]string
	var isCopyInProgress bool
	var err error
	var hasReplica, hasRdonly, hasMaster bool

	if !vrw.Exists() {
		return nil, fmt.Errorf("workflow has not yet been started")
	}
	if vrw.workflowType == MigrateWorkflow {
		return nil, fmt.Errorf("invalid action for Migrate workflow: SwitchTraffic")
	}

	isCopyInProgress, err = vrw.IsCopyInProgress()
	if err != nil {
		return nil, err
	}
	if isCopyInProgress {
		return nil, fmt.Errorf("cannot switch traffic at this time, copy is still in progress for this workflow")
	}

	vrw.params.Direction = direction
	hasReplica, hasRdonly, hasMaster, err = vrw.parseTabletTypes()
	if err != nil {
		return nil, err
	}
	if hasReplica || hasRdonly {
		if rdDryRunResults, err = vrw.switchReads(); err != nil {
			return nil, err
		}
	}
	if rdDryRunResults != nil {
		dryRunResults = append(dryRunResults, *rdDryRunResults...)
	}
	if hasMaster {
		if wrDryRunResults, err = vrw.switchWrites(); err != nil {
			return nil, err
		}
	}
	if wrDryRunResults != nil {
		dryRunResults = append(dryRunResults, *wrDryRunResults...)
	}
	return &dryRunResults, nil
}

// ReverseTraffic switches traffic backwards for tablet_types passed
func (vrw *VReplicationWorkflow) ReverseTraffic() (*[]string, error) {
	if !vrw.Exists() {
		return nil, fmt.Errorf("workflow has not yet been started")
	}
	if vrw.workflowType == MigrateWorkflow {
		return nil, fmt.Errorf("invalid action for Migrate workflow: ReverseTraffic")
	}
	return vrw.SwitchTraffic(DirectionBackward)
}

// Workflow errors
const (
	ErrWorkflowNotFullySwitched  = "cannot complete workflow because you have not yet switched all read and write traffic"
	ErrWorkflowPartiallySwitched = "cannot cancel workflow because you have already switched some or all read and write traffic"
)

// Complete cleans up a successful workflow
func (vrw *VReplicationWorkflow) Complete() (*[]string, error) {
	var dryRunResults *[]string
	var err error
	ws := vrw.ws

	if vrw.workflowType == MigrateWorkflow {
		return vrw.wr.finalizeMigrateWorkflow(vrw.ctx, ws.TargetKeyspace, ws.Workflow, vrw.params.Tables,
			false, vrw.params.KeepData, vrw.params.DryRun)
	}

	if !ws.WritesSwitched || len(ws.ReplicaCellsNotSwitched) > 0 || len(ws.RdonlyCellsNotSwitched) > 0 {
		return nil, fmt.Errorf(ErrWorkflowNotFullySwitched)
	}
	var renameTable TableRemovalType
	if vrw.params.RenameTables {
		renameTable = RenameTable
	} else {
		renameTable = DropTable
	}
	if dryRunResults, err = vrw.wr.DropSources(vrw.ctx, vrw.ws.TargetKeyspace, vrw.ws.Workflow, renameTable,
		false, vrw.params.KeepData, vrw.params.DryRun); err != nil {
		return nil, err
	}
	return dryRunResults, nil
}

// Cancel deletes all artifacts from a workflow which has not yet been switched
func (vrw *VReplicationWorkflow) Cancel() error {
	ws := vrw.ws
	if vrw.workflowType == MigrateWorkflow {
		_, err := vrw.wr.finalizeMigrateWorkflow(vrw.ctx, ws.TargetKeyspace, ws.Workflow, "",
			true, vrw.params.KeepData, vrw.params.DryRun)
		return err
	}

	if ws.WritesSwitched || len(ws.ReplicaCellsSwitched) > 0 || len(ws.RdonlyCellsSwitched) > 0 {
		return fmt.Errorf(ErrWorkflowPartiallySwitched)
	}
	if _, err := vrw.wr.DropTargets(vrw.ctx, vrw.ws.TargetKeyspace, vrw.ws.Workflow, vrw.params.KeepData, false); err != nil {
		return err
	}
	vrw.ts = nil
	return nil
}

// endregion

// region Helpers

func (vrw *VReplicationWorkflow) getCellsAsArray() []string {
	if vrw.params.Cells != "" {
		return strings.Split(vrw.params.Cells, ",")
	}
	return nil
}

func (vrw *VReplicationWorkflow) getTabletTypes() []topodatapb.TabletType {
	tabletTypesArr := strings.Split(vrw.params.TabletTypes, ",")
	var tabletTypes []topodatapb.TabletType
	for _, tabletType := range tabletTypesArr {
		servedType, _ := topoproto.ParseTabletType(tabletType)
		tabletTypes = append(tabletTypes, servedType)
	}
	return tabletTypes
}

func (vrw *VReplicationWorkflow) parseTabletTypes() (hasReplica, hasRdonly, hasMaster bool, err error) {
	tabletTypesArr := strings.Split(vrw.params.TabletTypes, ",")
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

func (vrw *VReplicationWorkflow) initMoveTables() error {
	log.Infof("In VReplicationWorkflow.initMoveTables() for %+v", vrw)
	return vrw.wr.MoveTables(vrw.ctx, vrw.params.Workflow, vrw.params.SourceKeyspace, vrw.params.TargetKeyspace,
		vrw.params.Tables, vrw.params.Cells, vrw.params.TabletTypes, vrw.params.AllTables, vrw.params.ExcludeTables,
		vrw.params.AutoStart, vrw.params.StopAfterCopy, vrw.params.ExternalCluster)
}

func (vrw *VReplicationWorkflow) initReshard() error {
	log.Infof("In VReplicationWorkflow.initReshard() for %+v", vrw)
	return vrw.wr.Reshard(vrw.ctx, vrw.params.TargetKeyspace, vrw.params.Workflow, vrw.params.SourceShards,
		vrw.params.TargetShards, vrw.params.SkipSchemaCopy, vrw.params.Cells, vrw.params.TabletTypes, vrw.params.AutoStart, vrw.params.StopAfterCopy)
}

func (vrw *VReplicationWorkflow) switchReads() (*[]string, error) {
	log.Infof("In VReplicationWorkflow.switchReads() for %+v", vrw)
	var tabletTypes []topodatapb.TabletType
	for _, tt := range vrw.getTabletTypes() {
		if tt != topodatapb.TabletType_MASTER {
			tabletTypes = append(tabletTypes, tt)
		}
	}
	var dryRunResults *[]string
	var err error
	dryRunResults, err = vrw.wr.SwitchReads(vrw.ctx, vrw.params.TargetKeyspace, vrw.params.Workflow, tabletTypes,
		vrw.getCellsAsArray(), vrw.params.Direction, vrw.params.DryRun)
	if err != nil {
		return nil, err
	}
	return dryRunResults, nil
}

func (vrw *VReplicationWorkflow) switchWrites() (*[]string, error) {
	var journalID int64
	var dryRunResults *[]string
	var err error
	log.Infof("In VReplicationWorkflow.switchWrites() for %+v", vrw)
	if vrw.params.Direction == DirectionBackward {
		keyspace := vrw.params.SourceKeyspace
		vrw.params.SourceKeyspace = vrw.params.TargetKeyspace
		vrw.params.TargetKeyspace = keyspace
		vrw.params.Workflow = reverseName(vrw.params.Workflow)
		log.Infof("In VReplicationWorkflow.switchWrites(reverse) for %+v", vrw)
	}
	journalID, dryRunResults, err = vrw.wr.SwitchWrites(vrw.ctx, vrw.params.TargetKeyspace, vrw.params.Workflow, vrw.params.Timeout,
		false, vrw.params.Direction == DirectionBackward, vrw.params.EnableReverseReplication, vrw.params.DryRun)
	if err != nil {
		return nil, err
	}
	log.Infof("switchWrites succeeded with journal id %s", journalID)
	return dryRunResults, nil
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

// IsCopyInProgress returns true if any table remains to be copied
func (vrw *VReplicationWorkflow) IsCopyInProgress() (bool, error) {
	ctx := context.Background()
	getTablesQuery := "select 1 from _vt.copy_state cs, _vt.vreplication vr where vr.id = cs.vrepl_id and vr.id = %d"
	for _, target := range vrw.ts.targets {
		for id := range target.sources {
			query := fmt.Sprintf(getTablesQuery, id)
			p3qr, err := vrw.wr.tmc.ExecuteFetchAsDba(ctx, target.master.Tablet, true, []byte(query), 1, false, false)
			if err != nil {
				return false, err
			}
			if len(p3qr.Rows) > 0 {
				return true, nil
			}
		}
	}
	return false, nil
}

// GetCopyProgress returns the progress of all tables being copied in the workflow
func (vrw *VReplicationWorkflow) GetCopyProgress() (*CopyProgress, error) {
	ctx := context.Background()
	getTablesQuery := "select table_name from _vt.copy_state cs, _vt.vreplication vr where vr.id = cs.vrepl_id and vr.id = %d"
	getRowCountQuery := "select table_name, table_rows, data_length from information_schema.tables where table_schema = %s and table_name in (%s)"
	tables := make(map[string]bool)
	const MaxRows = 1000
	sourceMasters := make(map[*topodatapb.TabletAlias]bool)
	for _, target := range vrw.ts.targets {
		for id, bls := range target.sources {
			query := fmt.Sprintf(getTablesQuery, id)
			p3qr, err := vrw.wr.tmc.ExecuteFetchAsDba(ctx, target.master.Tablet, true, []byte(query), MaxRows, false, false)
			if err != nil {
				return nil, err
			}
			if len(p3qr.Rows) < 1 {
				continue
			}
			qr := sqltypes.Proto3ToResult(p3qr)
			for i := 0; i < len(p3qr.Rows); i++ {
				tables[qr.Rows[i][0].ToString()] = true
			}
			sourcesi, err := vrw.wr.ts.GetShard(ctx, bls.Keyspace, bls.Shard)
			if err != nil {
				return nil, err
			}
			found := false
			for existingSource := range sourceMasters {
				if existingSource.Uid == sourcesi.MasterAlias.Uid {
					found = true
				}
			}
			if !found {
				sourceMasters[sourcesi.MasterAlias] = true
			}
		}
	}
	if len(tables) == 0 {
		return nil, nil
	}
	var tableList []string
	targetRowCounts := make(map[string]int64)
	sourceRowCounts := make(map[string]int64)
	targetTableSizes := make(map[string]int64)
	sourceTableSizes := make(map[string]int64)

	for table := range tables {
		tableList = append(tableList, encodeString(table))
		targetRowCounts[table] = 0
		sourceRowCounts[table] = 0
		targetTableSizes[table] = 0
		sourceTableSizes[table] = 0
	}

	var getTableMetrics = func(tablet *topodatapb.Tablet, query string, rowCounts *map[string]int64, tableSizes *map[string]int64) error {
		p3qr, err := vrw.wr.tmc.ExecuteFetchAsDba(ctx, tablet, true, []byte(query), len(tables), false, false)
		if err != nil {
			return err
		}
		qr := sqltypes.Proto3ToResult(p3qr)
		for i := 0; i < len(qr.Rows); i++ {
			table := qr.Rows[i][0].ToString()
			rowCount, err := evalengine.ToInt64(qr.Rows[i][1])
			if err != nil {
				return err
			}
			tableSize, err := evalengine.ToInt64(qr.Rows[i][2])
			if err != nil {
				return err
			}
			(*rowCounts)[table] += rowCount
			(*tableSizes)[table] += tableSize
		}
		return nil
	}
	sourceDbName := ""
	for _, tsSource := range vrw.ts.sources {
		sourceDbName = tsSource.master.DbName()
		break
	}
	if sourceDbName == "" {
		return nil, fmt.Errorf("no sources found for workflow %s.%s", vrw.ws.TargetKeyspace, vrw.ws.Workflow)
	}
	targetDbName := ""
	for _, tsTarget := range vrw.ts.targets {
		targetDbName = tsTarget.master.DbName()
		break
	}
	if sourceDbName == "" || targetDbName == "" {
		return nil, fmt.Errorf("workflow %s.%s is incorrectly configured", vrw.ws.TargetKeyspace, vrw.ws.Workflow)
	}
	sort.Strings(tableList) // sort list for repeatability for mocking in tests
	tablesStr := strings.Join(tableList, ",")
	query := fmt.Sprintf(getRowCountQuery, encodeString(targetDbName), tablesStr)
	for _, target := range vrw.ts.targets {
		tablet := target.master.Tablet
		if err := getTableMetrics(tablet, query, &targetRowCounts, &targetTableSizes); err != nil {
			return nil, err
		}
	}

	query = fmt.Sprintf(getRowCountQuery, encodeString(sourceDbName), tablesStr)
	for source := range sourceMasters {
		ti, err := vrw.wr.ts.GetTablet(ctx, source)
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
