package sidecardb

import (
	"context"
	"embed"
	"fmt"
	"strings"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/schemadiff"
)

//go:embed vtschema/*
var schemaLocation embed.FS

type VTTable struct {
	module string
	path   string
	name   string
}

type Exec func(ctx context.Context, query string, maxRows int, wantFields bool) (*sqltypes.Result, error)

func (t *VTTable) String() string {
	return fmt.Sprintf("_vt table: %s (%s)", t.name, t.module)
}

type VTSchemaInit struct {
	ctx            context.Context
	exec           Exec
	existingTables map[string]bool
}

var vtTables []*VTTable

func init() {
	// todo: twopc tables?
	vtTables = []*VTTable{
		{"Misc", "metadata/reparent_journal.sql", "reparent_journal"},
		{"Online DDL", "onlineddl/schema_migrations.sql", "schema_migrations"},
		{"VTGate Schema Tracker", "schematracker/schemacopy.sql", "schemacopy"},
		{"VReplication", "vreplication/vreplication.sql", "vreplication"},
		{"VReplication", "vreplication/vreplication_log.sql", "vreplication_log"},
		{"VReplication", "vreplication/copy_state.sql", "copy_state"},
		{"VReplication", "vreplication/resharding_journal.sql", "resharding_journal"},
		{"VReplication", "vreplication/schema_version.sql", "schema_version"},
		{"VDiff", "vdiff/vdiff.sql", "vdiff"},
		{"VDiff", "vdiff/vdiff_log.sql", "vdiff_log"},
		{"VDiff", "vdiff/vdiff_table.sql", "vdiff_table"},
		{"Misc", "misc/heartbeat.sql", "heartbeat"},
	}
}

func (si *VTSchemaInit) setCurrentDatabase(dbName string) (string, error) {
	rs, err := si.exec(si.ctx, "select database()", 1, false)
	if err != nil {
		return "", err
	}
	currentDB := rs.Rows[0][0].ToString()
	rs, err = si.exec(si.ctx, fmt.Sprintf("use %s", dbName), 1000, false)
	if err != nil {
		return "", err
	}
	return currentDB, nil
}

func (si *VTSchemaInit) CreateVTDatabase() error {
	rs, err := si.exec(si.ctx, "SHOW DATABASES LIKE '_vt'", 2, false)
	if err != nil {
		return err
	}

	switch len(rs.Rows) {
	case 0:
		_, err := si.exec(si.ctx, "CREATE DATABASE IF NOT EXISTS _vt", 1, false)
		if err != nil {
			return err
		}
		log.Infof("Created _vt database")
		break
	case 1:
		log.Infof("_vt database already exists, not an error")
		break
	default:
		return fmt.Errorf("found too many rows for _vt: %d", len(rs.Rows))
	}
	return nil
}

func (si *VTSchemaInit) getCurrentSchema(tableName string) (string, error) {
	var currentTableSchema string
	showCreateTableSQL := "show create table _vt.%s"
	rs, err := si.exec(si.ctx, fmt.Sprintf(showCreateTableSQL, tableName), 1, false)
	if err != nil {
		log.Errorf("Error showing _vt table %s: %+v", tableName, err)
		return "", err
	}
	if len(rs.Rows) > 0 {
		currentTableSchema = rs.Rows[0][1].ToString()
		log.Infof("current schema %s", currentTableSchema)
	}
	return currentTableSchema, nil
}

func stripCharset(schema string) string {
	ind := strings.Index(schema, "DEFAULT CHARSET")
	if ind <= 0 {
		return schema
	}
	return schema[:ind]
}

func (si *VTSchemaInit) findTableSchemaDiff(current, desired string) (string, error) {

	current = stripCharset(current)
	hints := &schemadiff.DiffHints{}
	diff, err := schemadiff.DiffCreateTablesQueries(current, desired, hints)
	if err != nil {
		return "", err
	}

	tableAlterSQL := diff.CanonicalStatementString()
	return tableAlterSQL, nil
}

// expects that we are already in the _vt database
func (si *VTSchemaInit) createOrUpgradeTable(table *VTTable) error {

	var desiredTableSchema string
	ctx := si.ctx
	bytes, err := schemaLocation.ReadFile(fmt.Sprintf("vtschema/%s", table.path))
	if err != nil {
		return err
	}
	desiredTableSchema = string(bytes)

	var tableAlterSQL string
	if si.tableExists(table.name) {
		currentTableSchema, err := si.getCurrentSchema(table.name)
		if err != nil {
			return err
		}

		tableAlterSQL, err = si.findTableSchemaDiff(currentTableSchema, desiredTableSchema)
		if err != nil {
			return err
		}

	} else {
		tableAlterSQL = desiredTableSchema
	}

	if strings.TrimSpace(tableAlterSQL) != "" {
		//log.Infof("tableAlterSQL is %s", tableAlterSQL)
		_, err = si.exec(ctx, tableAlterSQL, 1, false)
		if err != nil {
			log.Errorf("Error altering _vt table %s: %+v", table, err)
			return err
		}
		newTableSchema, err := si.getCurrentSchema(table.name)
		if err != nil {
			return err
		}
		tableAlterSQL, err = si.findTableSchemaDiff(newTableSchema, desiredTableSchema)
		if err != nil {
			return err
		}
		if tableAlterSQL != "" {
			err = fmt.Errorf("table alter did not work, desired schema is %s but current schema is %s: %s",
				desiredTableSchema, newTableSchema, tableAlterSQL)
			//log.Error(err)
		}
		log.Infof("Created or upgraded _vt table %s", table)
		return nil
	}
	log.Infof("Table %s was already correct", table.name)
	return nil
}

func (si *VTSchemaInit) tableExists(tableName string) bool {
	_, ok := si.existingTables[tableName]
	return ok
}

func (si *VTSchemaInit) setExistingTables() error {
	si.existingTables = make(map[string]bool)
	rs, err := si.exec(si.ctx, "show tables from _vt", 1000, false)
	if err != nil {
		return err
	}
	for _, row := range rs.Rows {
		si.existingTables[row[0].ToString()] = true
	}
	return nil
}

// Init creates or upgrades the _vt schema based on declarative schema for all _vt tables
func Init(ctx context.Context, exec Exec) error {
	if !InitVTSchemaOnTabletInit {
		log.Infof("init-vt-schema-on-tablet-init NOT set, not updating _vt schema on tablet init")
		return nil
	}
	si := &VTSchemaInit{
		ctx:  ctx,
		exec: exec,
	}

	if err := si.CreateVTDatabase(); err != nil {
		return err
	}

	currentDatabase, err := si.setCurrentDatabase("_vt")
	if err != nil {
		return err
	}
	// nolint
	defer si.setCurrentDatabase(currentDatabase)

	if err = si.setExistingTables(); err != nil {
		return err
	}

	for _, table := range vtTables {
		if err := si.createOrUpgradeTable(table); err != nil {
			return err
		}
	}
	log.Flush()
	return nil
}
