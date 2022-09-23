package wrangler

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/discovery"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topotools"
	"vitess.io/vitess/go/vt/vtctl/workflow"
	"vitess.io/vitess/go/vt/vtgate/evalengine"

	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
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

// VReplicationWorkflowParams stores args and options passed to a VReplicationWorkflow command
type VReplicationWorkflowParams struct {
	WorkflowType                      VReplicationWorkflowType
	Workflow, TargetKeyspace          string
	Cells, TabletTypes, ExcludeTables string
	EnableReverseReplication, DryRun  bool
	KeepData                          bool
	KeepRoutingRules                  bool
	Timeout                           time.Duration
	Direction                         workflow.TrafficSwitchDirection
	MaxAllowedTransactionLagSeconds   int64

	// MoveTables/Migrate specific
	SourceKeyspace, Tables  string
	AllTables, RenameTables bool
	SourceTimeZone          string
	DropForeignKeys         bool

	// Reshard specific
	SourceShards, TargetShards []string
	SkipSchemaCopy             bool
	AutoStart, StopAfterCopy   bool

	// Migrate specific
	ExternalCluster string
}

// VReplicationWorkflow stores various internal objects for a workflow
type VReplicationWorkflow struct {
	workflowType VReplicationWorkflowType
	ctx          context.Context
	wr           *Wrangler
	params       *VReplicationWorkflowParams
	ts           *trafficSwitcher
	ws           *workflow.State
}

func (vrw *VReplicationWorkflow) String() string {
	s := ""
	s += fmt.Sprintf("Parameters: %+v\n", vrw.params)
	s += fmt.Sprintf("State: %+v", vrw.CachedState())
	return s
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

func (vrw *VReplicationWorkflow) reloadState() (*workflow.State, error) {
	var err error
	vrw.ts, vrw.ws, err = vrw.wr.getWorkflowState(vrw.ctx, vrw.params.TargetKeyspace, vrw.params.Workflow)
	return vrw.ws, err
}

// CurrentState reloads and returns a human readable workflow state
func (vrw *VReplicationWorkflow) CurrentState() string {
	var err error
	vrw.ws, err = vrw.reloadState()
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

func (vrw *VReplicationWorkflow) stateAsString(ws *workflow.State) string {
	log.Infof("Workflow state is %+v", ws)
	var stateInfo []string
	s := ""
	if !vrw.Exists() {
		stateInfo = append(stateInfo, WorkflowStateNotCreated)
	} else {
		if !vrw.ts.isPartialMigration { // shard level traffic switching is all or nothing
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
		}
		if ws.WritesSwitched {
			stateInfo = append(stateInfo, "Writes Switched")
		} else if vrw.ts.isPartialMigration {
			if ws.WritesPartiallySwitched {
				// For partial migrations, the traffic switching is all or nothing
				// at the shard level, so reads are effectively switched on the
				// shard when writes are switched.
				sourceShards := vrw.ts.SourceShards()
				switchedShards := make([]string, len(sourceShards))
				for i, sourceShard := range sourceShards {
					switchedShards[i] = sourceShard.ShardName()
				}
				stateInfo = append(stateInfo, fmt.Sprintf("Reads partially switched, for shards: %s", strings.Join(switchedShards, ",")))
				stateInfo = append(stateInfo, fmt.Sprintf("Writes partially switched, for shards: %s", strings.Join(switchedShards, ",")))
			} else {
				stateInfo = append(stateInfo, "Reads Not Switched")
				stateInfo = append(stateInfo, "Writes Not Switched")
			}
		} else {
			stateInfo = append(stateInfo, "Writes Not Switched")
		}
	}
	return strings.Join(stateInfo, ". ")
}

// Create initiates a workflow
func (vrw *VReplicationWorkflow) Create(ctx context.Context) error {
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
		excludeTables := strings.Split(vrw.params.ExcludeTables, ",")
		keyspace := vrw.params.SourceKeyspace

		vschmErr := vrw.wr.ValidateVSchema(ctx, keyspace, vrw.params.SourceShards, excludeTables, true /*includeViews*/)
		if vschmErr != nil {
			return fmt.Errorf("Create ReshardWorkflow failed: %v", vschmErr)
		}

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

// GetStreamCount returns a count of total streams and of streams that have started processing
func (vrw *VReplicationWorkflow) GetStreamCount() (int64, int64, []*WorkflowError, error) {
	var err error
	var workflowErrors []*WorkflowError
	var total, started int64
	res, err := vrw.wr.ShowWorkflow(vrw.ctx, vrw.params.Workflow, vrw.params.TargetKeyspace)
	if err != nil {
		return 0, 0, nil, err
	}
	for ksShard := range res.ShardStatuses {
		statuses := res.ShardStatuses[ksShard].PrimaryReplicationStatuses
		for _, st := range statuses {
			total++
			if strings.HasPrefix(st.Message, "Error:") {
				workflowErrors = append(workflowErrors, NewWorkflowError(st.Tablet, st.ID, st.Message))
				continue
			}
			if st.Pos == "" {
				continue
			}
			if st.State == "Running" || st.State == "Copying" {
				started++
			}
		}
	}

	return total, started, workflowErrors, nil
}

// SwitchTraffic switches traffic in the direction passed for specified tablet_types
func (vrw *VReplicationWorkflow) SwitchTraffic(direction workflow.TrafficSwitchDirection) (*[]string, error) {
	var dryRunResults []string
	var rdDryRunResults, wrDryRunResults *[]string
	var err error
	var hasReplica, hasRdonly, hasPrimary bool

	if !vrw.Exists() {
		return nil, fmt.Errorf("workflow has not yet been started")
	}
	if vrw.workflowType == MigrateWorkflow {
		return nil, fmt.Errorf("invalid action for Migrate workflow: SwitchTraffic")
	}

	vrw.params.Direction = direction

	workflowName := vrw.params.Workflow
	keyspace := vrw.params.TargetKeyspace
	if vrw.params.Direction == workflow.DirectionBackward {
		workflowName = workflow.ReverseWorkflowName(workflowName)
		keyspace = vrw.params.SourceKeyspace
	}

	reason, err := vrw.canSwitch(keyspace, workflowName)
	if err != nil {
		return nil, err
	}
	if reason != "" {
		return nil, fmt.Errorf("cannot switch traffic for workflow %s at this time: %s", workflowName, reason)
	}

	hasReplica, hasRdonly, hasPrimary, err = vrw.parseTabletTypes()
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
	if hasPrimary {
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
	return vrw.SwitchTraffic(workflow.DirectionBackward)
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
			false, vrw.params.KeepData, vrw.params.KeepRoutingRules, vrw.params.DryRun)
	}

	if !ws.WritesSwitched || len(ws.ReplicaCellsNotSwitched) > 0 || len(ws.RdonlyCellsNotSwitched) > 0 {
		return nil, fmt.Errorf(ErrWorkflowNotFullySwitched)
	}
	var renameTable workflow.TableRemovalType
	if vrw.params.RenameTables {
		renameTable = workflow.RenameTable
	} else {
		renameTable = workflow.DropTable
	}
	if dryRunResults, err = vrw.wr.DropSources(vrw.ctx, vrw.ws.TargetKeyspace, vrw.ws.Workflow, renameTable,
		vrw.params.KeepData, vrw.params.KeepRoutingRules, false /* force */, vrw.params.DryRun); err != nil {
		return nil, err
	}
	return dryRunResults, nil
}

// Cancel deletes all artifacts from a workflow which has not yet been switched
func (vrw *VReplicationWorkflow) Cancel() error {
	ws := vrw.ws
	if vrw.workflowType == MigrateWorkflow {
		_, err := vrw.wr.finalizeMigrateWorkflow(vrw.ctx, ws.TargetKeyspace, ws.Workflow, "",
			true, vrw.params.KeepData, vrw.params.KeepRoutingRules, vrw.params.DryRun)
		return err
	}

	if ws.WritesSwitched || len(ws.ReplicaCellsSwitched) > 0 || len(ws.RdonlyCellsSwitched) > 0 {
		return fmt.Errorf(ErrWorkflowPartiallySwitched)
	}
	if _, err := vrw.wr.DropTargets(vrw.ctx, vrw.ws.TargetKeyspace, vrw.ws.Workflow, vrw.params.KeepData, vrw.params.KeepRoutingRules, false); err != nil {
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

func (vrw *VReplicationWorkflow) parseTabletTypes() (hasReplica, hasRdonly, hasPrimary bool, err error) {
	tabletTypes, _, err := discovery.ParseTabletTypesAndOrder(vrw.params.TabletTypes)
	if err != nil {
		return false, false, false, err
	}
	for _, tabletType := range tabletTypes {
		switch tabletType {
		case topodatapb.TabletType_REPLICA:
			hasReplica = true
		case topodatapb.TabletType_RDONLY:
			hasRdonly = true
		case topodatapb.TabletType_PRIMARY:
			hasPrimary = true
		default:
			return false, false, false, fmt.Errorf("invalid tablet type passed %s", tabletType)
		}
	}
	return hasReplica, hasRdonly, hasPrimary, nil
}

// endregion

// region Core Actions

func (vrw *VReplicationWorkflow) initMoveTables() error {
	log.Infof("In VReplicationWorkflow.initMoveTables() for %+v", vrw)
	return vrw.wr.MoveTables(vrw.ctx, vrw.params.Workflow, vrw.params.SourceKeyspace, vrw.params.TargetKeyspace,
		vrw.params.Tables, vrw.params.Cells, vrw.params.TabletTypes, vrw.params.AllTables, vrw.params.ExcludeTables,
		vrw.params.AutoStart, vrw.params.StopAfterCopy, vrw.params.ExternalCluster, vrw.params.DropForeignKeys,
		vrw.params.SourceTimeZone, vrw.params.SourceShards)
}

func (vrw *VReplicationWorkflow) initReshard() error {
	log.Infof("In VReplicationWorkflow.initReshard() for %+v", vrw)
	return vrw.wr.Reshard(vrw.ctx, vrw.params.TargetKeyspace, vrw.params.Workflow, vrw.params.SourceShards,
		vrw.params.TargetShards, vrw.params.SkipSchemaCopy, vrw.params.Cells, vrw.params.TabletTypes, vrw.params.AutoStart, vrw.params.StopAfterCopy)
}

func (vrw *VReplicationWorkflow) switchReads() (*[]string, error) {
	log.Infof("In VReplicationWorkflow.switchReads() for %+v", vrw)
	fullTabletTypes, _, err := discovery.ParseTabletTypesAndOrder(vrw.params.TabletTypes)
	if err != nil {
		return nil, err
	}
	var nonPrimaryTabletTypes []topodatapb.TabletType
	for _, tt := range fullTabletTypes {
		if tt != topodatapb.TabletType_PRIMARY {
			nonPrimaryTabletTypes = append(nonPrimaryTabletTypes, tt)
		}
	}
	var dryRunResults *[]string
	dryRunResults, err = vrw.wr.SwitchReads(vrw.ctx, vrw.params.TargetKeyspace, vrw.params.Workflow, nonPrimaryTabletTypes,
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
	if vrw.params.Direction == workflow.DirectionBackward {
		keyspace := vrw.params.SourceKeyspace
		vrw.params.SourceKeyspace = vrw.params.TargetKeyspace
		vrw.params.TargetKeyspace = keyspace
		vrw.params.Workflow = workflow.ReverseWorkflowName(vrw.params.Workflow)
		log.Infof("In VReplicationWorkflow.switchWrites(reverse) for %+v", vrw)
	}
	journalID, dryRunResults, err = vrw.wr.SwitchWrites(vrw.ctx, vrw.params.TargetKeyspace, vrw.params.Workflow, vrw.params.Timeout,
		false, vrw.params.Direction == workflow.DirectionBackward, vrw.params.EnableReverseReplication, vrw.params.DryRun)
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

const (
	cannotSwitchError               = "workflow has errors"
	cannotSwitchCopyIncomplete      = "copy is still in progress"
	cannotSwitchHighLag             = "replication lag %ds is higher than allowed lag %ds"
	cannotSwitchFailedTabletRefresh = "could not refresh all of the tablets involved in the operation:\n%s"
	cannotSwitchFrozen              = "workflow is frozen"
)

func (vrw *VReplicationWorkflow) canSwitch(keyspace, workflowName string) (reason string, err error) {
	ws, err := vrw.reloadState()
	if err != nil {
		return "", err
	}
	if vrw.params.Direction == workflow.DirectionForward && ws.WritesSwitched ||
		vrw.params.Direction == workflow.DirectionBackward && !ws.WritesSwitched {
		log.Infof("writes already switched no need to check lag")
		return "", nil
	}
	log.Infof("state:%s, direction %d, switched %t", vrw.CachedState(), vrw.params.Direction, ws.WritesSwitched)
	result, err := vrw.wr.getStreams(vrw.ctx, workflowName, keyspace)
	if err != nil {
		return "", err
	}
	for ksShard := range result.ShardStatuses {
		statuses := result.ShardStatuses[ksShard].PrimaryReplicationStatuses
		for _, st := range statuses {
			switch st.State {
			case "Copying":
				return cannotSwitchCopyIncomplete, nil
			case "Error":
				return cannotSwitchError, nil
			}
		}
	}
	if result.Frozen {
		return cannotSwitchFrozen, nil
	}
	if result.MaxVReplicationTransactionLag > vrw.params.MaxAllowedTransactionLagSeconds {
		return fmt.Sprintf(cannotSwitchHighLag, result.MaxVReplicationTransactionLag, vrw.params.MaxAllowedTransactionLagSeconds), nil
	}

	// Ensure that the tablets on both sides are in good shape as we make this same call in the process
	// and an error will cause us to backout
	refreshErrors := strings.Builder{}
	var m sync.Mutex
	var wg sync.WaitGroup
	rtbsCtx, cancel := context.WithTimeout(vrw.ctx, shardTabletRefreshTimeout)
	defer cancel()
	refreshTablets := func(shards []*topo.ShardInfo, stype string) {
		defer wg.Done()
		for _, si := range shards {
			if partial, partialDetails, err := topotools.RefreshTabletsByShard(rtbsCtx, vrw.wr.ts, vrw.wr.tmc, si, nil, vrw.wr.Logger()); err != nil || partial {
				m.Lock()
				refreshErrors.WriteString(fmt.Sprintf("failed to successfully refresh all tablets in the %s/%s %s shard (%v):\n  %v\n",
					si.Keyspace(), si.ShardName(), stype, err, partialDetails))
				m.Unlock()
			}
		}
	}
	wg.Add(1)
	go refreshTablets(vrw.ts.SourceShards(), "source")
	wg.Add(1)
	go refreshTablets(vrw.ts.TargetShards(), "target")
	wg.Wait()
	if refreshErrors.Len() > 0 {
		return fmt.Sprintf(cannotSwitchFailedTabletRefresh, refreshErrors.String()), nil
	}
	return "", nil
}

// GetCopyProgress returns the progress of all tables being copied in the workflow
func (vrw *VReplicationWorkflow) GetCopyProgress() (*CopyProgress, error) {
	ctx := context.Background()
	getTablesQuery := "select table_name from _vt.copy_state cs, _vt.vreplication vr where vr.id = cs.vrepl_id and vr.id = %d"
	getRowCountQuery := "select table_name, table_rows, data_length from information_schema.tables where table_schema = %s and table_name in (%s)"
	tables := make(map[string]bool)
	const MaxRows = 1000
	sourcePrimaries := make(map[*topodatapb.TabletAlias]bool)
	for _, target := range vrw.ts.targets {
		for id, bls := range target.Sources {
			query := fmt.Sprintf(getTablesQuery, id)
			p3qr, err := vrw.wr.tmc.ExecuteFetchAsDba(ctx, target.GetPrimary().Tablet, true, &tabletmanagerdatapb.ExecuteFetchAsDbaRequest{
				Query:   []byte(query),
				MaxRows: MaxRows,
			})
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
			for existingSource := range sourcePrimaries {
				if existingSource.Uid == sourcesi.PrimaryAlias.Uid {
					found = true
				}
			}
			if !found {
				sourcePrimaries[sourcesi.PrimaryAlias] = true
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
		p3qr, err := vrw.wr.tmc.ExecuteFetchAsDba(ctx, tablet, true, &tabletmanagerdatapb.ExecuteFetchAsDbaRequest{
			Query:   []byte(query),
			MaxRows: uint64(len(tables)),
		})
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
		sourceDbName = tsSource.GetPrimary().DbName()
		break
	}
	if sourceDbName == "" {
		return nil, fmt.Errorf("no sources found for workflow %s.%s", vrw.ws.TargetKeyspace, vrw.ws.Workflow)
	}
	targetDbName := ""
	for _, tsTarget := range vrw.ts.targets {
		targetDbName = tsTarget.GetPrimary().DbName()
		break
	}
	if sourceDbName == "" || targetDbName == "" {
		return nil, fmt.Errorf("workflow %s.%s is incorrectly configured", vrw.ws.TargetKeyspace, vrw.ws.Workflow)
	}
	sort.Strings(tableList) // sort list for repeatability for mocking in tests
	tablesStr := strings.Join(tableList, ",")
	query := fmt.Sprintf(getRowCountQuery, encodeString(targetDbName), tablesStr)
	for _, target := range vrw.ts.targets {
		tablet := target.GetPrimary().Tablet
		if err := getTableMetrics(tablet, query, &targetRowCounts, &targetTableSizes); err != nil {
			return nil, err
		}
	}

	query = fmt.Sprintf(getRowCountQuery, encodeString(sourceDbName), tablesStr)
	for source := range sourcePrimaries {
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

// region Workflow related utility functions

// deleteWorkflowVDiffData cleans up any potential VDiff related data associated with the workflow on the given tablet
func (wr *Wrangler) deleteWorkflowVDiffData(ctx context.Context, tablet *topodatapb.Tablet, workflow string) {
	sqlDeleteVDiffs := `delete from vd, vdt, vdl using _vt.vdiff as vd inner join _vt.vdiff_table as vdt on (vd.id = vdt.vdiff_id)
						inner join _vt.vdiff_log as vdl on (vd.id = vdl.vdiff_id)
						where vd.keyspace = %s and vd.workflow = %s`
	query := fmt.Sprintf(sqlDeleteVDiffs, encodeString(tablet.Keyspace), encodeString(workflow))
	rows := -1
	if _, err := wr.tmc.ExecuteFetchAsDba(ctx, tablet, false, &tabletmanagerdatapb.ExecuteFetchAsDbaRequest{
		Query:   []byte(query),
		MaxRows: uint64(rows),
	}); err != nil {
		if sqlErr, ok := err.(*mysql.SQLError); ok && sqlErr.Num != mysql.ERNoSuchTable { // the tables may not exist if no vdiffs have been run
			wr.Logger().Errorf("Error deleting vdiff data for %s.%s workflow: %v", tablet.Keyspace, workflow, err)
		}
	}
}

// endregion
