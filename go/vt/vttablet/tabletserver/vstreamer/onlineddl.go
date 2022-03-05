package vstreamer

import (
	"strings"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/log"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	vtschema "vitess.io/vitess/go/vt/schema"
	"vitess.io/vitess/go/vt/sqlparser"
)

// onlineDDLMigrationsTable gets the attributes of a migration needed for vstreamer to stream materialized tables
type onlineDDLMigrationsTable struct {
	uuid              string
	mysqlTable        string
	materializedTable string
}

// mustProcessMaterializedTable returns true for materialized online ddl tables which are part of a reshard
func (vs *vstreamer) mustProcessMaterializedTable(tableName string) bool {
	if vtschema.IsInternalOperationTableName(tableName) &&
		vtschema.IsOnlineDDLMaterializedTableName(tableName) &&
		vs.filter.WorkflowType == int64(binlogdatapb.VReplicationWorkflowType_RESHARD) {

		return true
	}
	return false
}

// getMaterializedTableOnlineDDLEvent is sent to the target when a materialized table is seen for the first time in
// vstreamer iteration. It sends the required info to the target for planning this table.
func (vs *vstreamer) getMaterializedTableOnlineDDLEvent(table *onlineDDLMigrationsTable) (*binlogdatapb.VEvent, error) {
	materializedTableCreateStatement, err := vs.se.GetCreateTableStatement(vs.ctx, table.materializedTable)
	if err != nil {
		return nil, err
	}
	materializedTableEvent := &binlogdatapb.VEvent{
		Type: binlogdatapb.VEventType_ONLINEDDLEVENT,
		OnlineDdlEvent: &binlogdatapb.OnlineDDLEvent{
			EventType:             binlogdatapb.OnlineDDLEventType_MATERIALIZED_TABLE,
			Uuid:                  table.uuid,
			MaterializedTableName: table.materializedTable,
			MysqlTableName:        table.mysqlTable,
			Ddl:                   materializedTableCreateStatement,
		},
	}
	return materializedTableEvent, nil
}

// materializedTable is of the form _04f532ce_9265_11ec_893f_04ed332e05c2_20220220165152_vrepl
func getUuidFromMaterializedTableName(materializedTableName string) string {
	const (
		uuidStartIndex = 1
		uuidLength     = 35
	)
	if len(materializedTableName) < uuidStartIndex+uuidLength+1 {
		return ""
	}
	return materializedTableName[uuidStartIndex : uuidStartIndex+uuidLength+1]
}

// sendVEventsForOnlineDDLMigrations sends MATERIALIZED_TABLE events to the target.
// When vstreamer first starts it calls this function with an empty string so that events are sent for all active migrations.
// When a materialized table is encountered for the first time this function is called just for that table.
// The reason we need to call this when vstreamer first starts, is to handle this (very low probability!) race:
//   1. Reshard is initiated and current tables are populated in _vt.copy_state
//	 2. A new OnlineDDL is created and completed before the position is recorded in start_pos for the Reshard
//   3. Now the Online DDL events in the binlog were *before* this start_pos. So the Reshard never processes
//		those binlogs. Hence we send all active migrations once.
func (vs *vstreamer) sendVEventsForOnlineDDLMigrations(materializedTable string) error {
	log.Infof("sendVEventsForOnlineDDLMigrations %s", materializedTable)
	var vevents []*binlogdatapb.VEvent
	tables, _ := vs.getActiveOnlineDDLMigrations(materializedTable)
	for _, table := range tables {
		vevent, err := vs.getMaterializedTableOnlineDDLEvent(table)
		if err != nil {
			return err
		}
		vs.vschema.aliasTable(table.materializedTable, table.mysqlTable)
		vevents = append(vevents, vevent)
	}
	log.Infof("sendVEventsForOnlineDDLMigrations sending events %+v", vevents)
	if len(vevents) > 0 {
		if err := vs.send(vevents); err != nil {
			return err
		}
	}
	return nil
}

func (vs *vstreamer) getActiveOnlineDDLMigrations(materializedTable string) ([]*onlineDDLMigrationsTable, error) {
	log.Infof("getActiveOnlineDDLMigrations for %s", materializedTable)
	var tables []*onlineDDLMigrationsTable
	query := "select migration_uuid, mysql_table, materialized_table from _vt.schema_migrations where migration_status IN ('queued', 'ready', 'running') and materialized_table != ''"
	conn, err := vs.cp.Connect(vs.ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	qr, err := conn.ExecuteFetch(query, 10000, true)
	if err != nil {
		return nil, err
	}

	result := qr.Named()
	for _, row := range result.Rows {
		if materializedTable != "" && materializedTable != row["materialized_table"].ToString() {
			continue
		}
		table := &onlineDDLMigrationsTable{
			uuid:              row["migration_uuid"].ToString(),
			mysqlTable:        row["mysql_table"].ToString(),
			materializedTable: row["materialized_table"].ToString(),
		}
		tables = append(tables, table)
	}
	log.Infof("getActiveOnlineDDLMigrations returns %+v", tables)
	return tables, nil
}

// getRenameOnlineDDLEvent looks for the "rename table" query that is called when a migration is completed
// and if it identifies one of the targets to be a materialized table creates a vevent to send to the target
func (vs *vstreamer) getRenameOnlineDDLEvent(query mysql.Query, dbname string) (*binlogdatapb.VEvent, error) {
	if query.Database != "" && query.Database != dbname {
		return nil, nil
	}
	ast, err := sqlparser.Parse(query.SQL)
	if err != nil {
		return nil, err
	}
	switch stmt := ast.(type) {
	case sqlparser.DBDDLStatement:
		return nil, nil
	case sqlparser.DDLStatement:
		// TODO: need to pass a CREATE TABLE as EventType: binlogdatapb.OnlineDDLEventType_CREATE?

		// rename query looks like
		// "RENAME TABLE `customer` TO `_swap_07f1423b926511ecbbb504ed332e05c2`, `_04f532ce_9265_11ec_893f_04ed332e05c2_20220220165152_vrepl` TO `customer`, `_swap_07f1423b926511ecbbb504ed332e05c2` TO `_04f532ce_9265_11ec_893f_04ed332e05c2_20220220165152_vrepl`
		if strings.Contains(strings.ToUpper(query.SQL), "RENAME TABLE") && len(stmt.GetToTables()) == 3 {
			materializedTable := stmt.GetToTables()[2].Name.String()
			uuid := getUuidFromMaterializedTableName(materializedTable)
			if vtschema.IsOnlineDDLMaterializedTableName(materializedTable) {
				ev := &binlogdatapb.VEvent{
					Type: binlogdatapb.VEventType_ONLINEDDLEVENT,
					OnlineDdlEvent: &binlogdatapb.OnlineDDLEvent{
						Uuid:      uuid,
						EventType: binlogdatapb.OnlineDDLEventType_RENAME_TABLE,
						Ddl:       query.SQL,
					},
				}
				log.Infof("grod: Sending rename event %+v", ev)
				return ev, nil
			}
		}
	}
	return nil, nil
}
