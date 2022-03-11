package vreplication

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"vitess.io/vitess/go/vt/log"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/tmclient"
)

const (
	onlineDDLMigrationTableName = "_vt.vreplication_onlineddl_migration"
	//TODO: add one to many from vreplication_onlineddl_migration to vrepl_id
	createOnlineDDLMigrationTable = `create table if not exists _vt.vreplication_onlineddl_migration(
		id BIGINT(20) AUTO_INCREMENT,
		uuid VARBINARY(256) NOT NULL,
		vrepl_id BIGINT(20) NOT NULL,  
		state VARBINARY(100) NOT NULL,
		materialized_table VARBINARY(256) NOT NULL,
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
		updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
		PRIMARY KEY (id))`
	addOnlineDDLMigrationQuery           = "insert ignore into %s (uuid, state, vrepl_id, materialized_table) values ('%s', '%s', %d, '%s')"
	getOnlineDDLMigrationQuery           = "select id, state, materialized_table from %s where uuid = '%s' and vrepl_id = %d"
	markOnlineDDLMigrationCompletedQuery = "update %s set state = '%s' where uuid = '%s' and vrepl_id = %d"
)

var onlineDDLMu sync.Mutex

// ErrReloadCopyState is sent when a new table is inserted into copy state to switch the current player from Replicating to Copying
// is not an error, but linter expects an Err prefix since we return this as an error.
// todo: better way would be to explicitly signal the request to restart, but since that involves changing a lot of function signatures
// and impacts a lot of code we take this shortcut now?
var ErrReloadCopyState = fmt.Errorf("new copy state")

type OnlineDDLMigration struct {
	uuid              string
	state             string
	id                int64
	vreplID           int64
	materializedTable string

	ctx context.Context
	vp  *vplayer
}

type OnlineDDLMigrationState string

const (
	OnlineDDLMigrationStateUnknown    OnlineDDLMigrationState = ""
	OnlineDDLMigrationStateInProgress OnlineDDLMigrationState = "inprogress"
	OnlineDDLMigrationStateComplete   OnlineDDLMigrationState = "complete"
)

func newOnlineDDLMigration(ctx context.Context, vp *vplayer, uuid, materializedTable string) (*OnlineDDLMigration, error) {
	odm := &OnlineDDLMigration{
		uuid:              uuid,
		ctx:               ctx,
		vp:                vp,
		vreplID:           int64(vp.vr.id),
		materializedTable: materializedTable,
	}
	if err := odm.get(); err != nil {
		return nil, err
	}
	return odm, nil
}

func (odm *OnlineDDLMigration) exists() bool {
	return odm.id != 0
}

func (odm *OnlineDDLMigration) completed() bool {
	return odm.state == string(OnlineDDLMigrationStateComplete)
}

func (odm *OnlineDDLMigration) get() error {
	query := fmt.Sprintf(getOnlineDDLMigrationQuery, onlineDDLMigrationTableName, odm.uuid, odm.vp.vr.id)
	qr, err := odm.vp.vr.dbClient.Execute(query)
	if err != nil {
		return err
	}
	numRows := len(qr.Rows)
	switch numRows {
	case 0:
		return nil
	case 1:
		row := qr.Named().Row()
		odm.state = row["state"].ToString()
		odm.id, _ = row["id"].ToInt64()
		odm.materializedTable = row["materialized_table"].ToString()
		log.Infof("odm.get: uuid %s, id %d, state %s, materialized_table %s", odm.uuid,
			odm.id, odm.state, odm.materializedTable)
		return nil
	default:
		return fmt.Errorf("too many rows %d found for uuid %s", len(qr.Rows), odm.uuid)
	}
}

func (odm *OnlineDDLMigration) register(ddl string) (bool, error) {
	if odm.exists() {
		return false, nil
	}
	log.Infof("uuid %s, registering ddl %s", odm.uuid, ddl)
	query := fmt.Sprintf(addOnlineDDLMigrationQuery, onlineDDLMigrationTableName, odm.uuid,
		OnlineDDLMigrationStateInProgress, odm.vp.vr.id, odm.materializedTable)
	qr, err := odm.vp.vr.dbClient.Execute(query)
	if err != nil {
		log.Infof("%s", err)
		return false, err
	}
	// if this is a new migration, create the materialized table
	if qr.InsertID != 0 {
		log.Infof("did not insert %s into %s", odm.uuid, onlineDDLMigrationTableName)
		odm.id = int64(qr.InsertID)
		log.Infof("uuid %s, id %d, exec %s", odm.uuid, odm.id, ddl)
		if _, err := odm.vp.vr.dbClient.ExecuteWithRetry(odm.ctx, ddl); err != nil {
			log.Infof("%s", err)
			return false, err
		}
	}
	//todo: for existing migration and new one add to map table
	log.Infof("uuid %s, id %d, execed %s, updating plan", odm.uuid, odm.id, ddl)
	if err := odm.addToCopyState(odm.materializedTable); err != nil {
		return false, err
	}
	// both for new and existing migrations we need to update the plan
	if err := odm.vp.updatePlan(odm.ctx); err != nil {
		log.Infof("%s", err)
		return false, err
	}
	log.Infof("uuid %s, updated plan", odm.uuid)

	return true, nil
}

func (odm *OnlineDDLMigration) addToCopyState(table string) error {
	var buf strings.Builder
	buf.WriteString("insert into _vt.copy_state(vrepl_id, table_name) values ")
	fmt.Fprintf(&buf, " (%d, %s)", odm.vp.vr.id, encodeString(table))
	if _, err := odm.vp.vr.dbClient.ExecuteWithRetry(odm.ctx, buf.String()); err != nil {
		log.Infof("%s", err)
		return err
	}
	log.Infof("added %s to copy_state for %d", table, odm.vp.vr.id)
	return nil
}

func (odm *OnlineDDLMigration) complete(ddl string) error {
	if !odm.exists() {
		return fmt.Errorf("migration does not exist: %s", odm.uuid)
	}
	log.Infof("uuid %s, completing ddl %s", odm.uuid, ddl)
	if odm.completed() {
		log.Infof("uuid %s, already completed ddl %s", odm.uuid, ddl)
		return nil
	}
	log.Infof("uuid %s, execing %s", odm.uuid, ddl)

	if _, err := odm.vp.vr.dbClient.ExecuteWithRetry(odm.ctx, ddl); err != nil {
		log.Infof("%s", err)
		return err
	}
	query := fmt.Sprintf(markOnlineDDLMigrationCompletedQuery, onlineDDLMigrationTableName,
		OnlineDDLMigrationStateComplete, odm.uuid, odm.vp.vr.id)
	log.Infof("execing update %s", query)
	if _, err := odm.vp.vr.dbClient.Execute(query); err != nil {
		log.Infof("%s", err)
		return err
	}
	log.Infof("reloading schema after uuid %s", odm.uuid)
	if err := odm.vp.reloadSchema(odm.ctx); err != nil {
		log.Infof("%s", err)
		return err
	}
	return nil
}

func (vp *vplayer) reloadSchema(ctx context.Context) error {
	log.Infof(">>>>>> reloading schema on %s", vp.vr.vre.tabletAlias)
	tmClient := tmclient.NewTabletManagerClient()
	tablet, err := vp.vr.vre.ts.GetTablet(ctx, vp.vr.vre.tabletAlias)
	if err != nil {
		log.Infof(">>>>> err is %s", err)
		return err
	}
	if err := tmClient.ReloadSchema(ctx, tablet.Tablet, ""); err != nil {
		log.Infof("ReloadSchema error %s", err)
		vterrors.Errorf(vtrpcpb.Code_UNKNOWN, "Error in reloadSchema")
		return err
	}
	return nil
}

func (vp *vplayer) updatePlan(ctx context.Context) error {
	// Applying the ddl would change the schema and the field events that we get will reflect the new schema.
	// So reload the extended column information from the information_schema and rebuild the replicator plan.
	// Otherwise, the field events will not match the current plan, resulting in errors.
	colInfo, err := vp.vr.buildColInfoMap(ctx)
	if err != nil {
		return err
	}
	vp.vr.colInfoMap = colInfo

	plan, err := buildReplicatorPlan(vp.vr.source.Filter, vp.vr.colInfoMap, vp.copyState, vp.vr.stats)
	if err != nil {
		vp.vr.stats.ErrorCounts.Add([]string{"Plan"}, 1)
		return err
	}
	vp.replicatorPlan = plan
	log.Infof("after updating plans: %+v", vp.replicatorPlan.TablePlans)
	return nil
}

func (vp *vplayer) handleOnlineDDLEvent(ctx context.Context, event *binlogdatapb.VEvent) error {

	log.Infof(">>>>> Got VEventType_ONLINEDDLEVENT for %+v", event)
	onlineDDLMu.Lock()
	defer onlineDDLMu.Unlock()

	odEvent := event.OnlineDdlEvent
	odEventType := event.OnlineDdlEvent.EventType

	odm, err := newOnlineDDLMigration(ctx, vp, odEvent.Uuid, odEvent.MaterializedTableName)
	if err != nil {
		log.Errorf("%s", err)
		return err
	}

	switch odEventType {
	case binlogdatapb.OnlineDDLEventType_MATERIALIZED_TABLE:
		var registered bool
		var err error
		if registered, err = odm.register(odEvent.Ddl); err != nil {
			log.Errorf("%s", err)
			return err
		}
		if registered {
			log.Infof("Registered table %s, returning ErrReloadCopyState", odEvent.MaterializedTableName)
			return ErrReloadCopyState
		}
	case binlogdatapb.OnlineDDLEventType_RENAME_TABLE:
		// todo: need to synchronize all source streams that merge into target, so how do we know how many streams
		// are there. Can one shard start and complete before other streams even start?
		if err := odm.complete(odEvent.Ddl); err != nil {
			log.Errorf("%s", err)
			return err
		}
	case binlogdatapb.OnlineDDLEventType_CREATE_TABLE:
		panic("tbd")
	}

	return nil
}
