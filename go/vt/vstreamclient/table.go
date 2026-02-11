package vstreamclient

import (
	"errors"
	"fmt"
	"maps"
	"reflect"
	"slices"
	"time"

	"vitess.io/vitess/go/sqlescape"
	"vitess.io/vitess/go/sqltypes"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

// TableConfig is the configuration for a table, which is used to configure filtering and scanning of the results
type TableConfig struct {
	Keyspace string
	Table    string

	// if no configured, this is set to "select * from keyspace.table", streaming all the fields. This can be
	// overridden to select only the fields that are needed, which can reduce memory usage and improve performance,
	// and also alias the fields to match the struct fields.
	Query string

	// MaxRowsPerFlush serves two purposes:
	//  1. it limits the number of rows that are flushed at once, to avoid large transactions. If more than
	//     this number of rows are processed, they will be flushed in chunks of this size.
	//  2. if this number is exceeded before reaching the minFlushDuration, it will trigger a flush to avoid
	//     holding too much memory in the rows slice.
	MaxRowsPerFlush int

	// if true, will reuse the same slice for each batch, which can reduce memory allocations. It does mean
	// that the caller must copy the data if they want to keep it, because the slice will be reused.
	ReuseBatchSlice bool

	// if true, will error and block the stream if there are fields in the result that don't match the struct
	ErrorOnUnknownFields bool

	// this is the function that will be called to flush the rows to the handler. This is called when the
	// minFlushDuration has passed, or the maxRowsPerFlush has been exceeded.
	FlushFn FlushFunc

	// TODO: translate this to *sql.Tx so we can pass it to the flush function
	FlushInTx bool

	// purposefully not exported, so we don't have to use a mutex to access it. The idea is that the consumer only
	// needs to access this when they are flushing the rows, so they can copy the data if they want to keep it. If
	// there is a use case for accessing this outside of the flush function, we can add a getter method.
	stats TableStats

	// this is the data type for this table, which is used to scan the results. Regardless whether a pointer
	// or value is supplied, the return value will always be []*DataType.
	DataType          any
	underlyingType    reflect.Type
	implementsScanner bool

	// this stores the current batch of rows for this table, which caches the results until the next flush.
	currentBatch []Row

	// this is the mapping of fields to the query results, which is used to scan the results. This is done
	// on a per-shard basis, because while unlikely, it's possible that the same table in different shards
	// could have different schemas.
	shards map[string]shardConfig
}

// TableStats keeps track of the number of rows processed and flushed for a single table
type TableStats struct {
	// how many rows have been processed for this table. These are incremented as each row
	// is processed, regardless of whether it is flushed.
	RowInsertCount int
	RowUpdateCount int
	RowDeleteCount int

	FlushedRowCount int // sum of rows flushed, including all insert/update/delete events

	FlushCount    int       // how many times the flush function was executed for this table. Not incremented for no-ops
	LastFlushedAt time.Time // only set after a flush successfully completes
}

// shardConfig is the per-shard configuration for a table, which is used to scan the results
type shardConfig struct {
	fieldMap map[string]fieldMapping
	fields   []*querypb.Field
}

// fieldMapping caches the mapping of table fields to struct fields, to reduce reflection overhead. This is
// configured once per shard, and used for all rows in that shard, unless a DDL event changes the schema,
// in which case the mapping is updated.
type fieldMapping struct {
	rowIndex    int
	structIndex []int
	structType  any
	kind        reflect.Kind
	isPointer   bool
}

// TODO: there's a consolidation issue to deal with here. Someone could easily add a new table in
// code after the copy phase completes and redeploy, expecting it to catch up, but it wouldn't
// by default. To support that, I think we'd have to store a list of tables in state, and if a
// new table is added, catch it up to the same vgtid, then do a cutover to be in the same stream.
// That'd be a much better user experience, but a decent amount of added complexity.
func (v *VStreamClient) initTables(tables []TableConfig) error {
	if len(v.tables) > 0 {
		return fmt.Errorf("vstreamclient: %d tables already configured", len(v.tables))
	}

	if len(tables) == 0 {
		return errors.New("vstreamclient: no tables provided")
	}

	for _, table := range tables {
		// basic validation
		if table.DataType == nil {
			return fmt.Errorf("vstreamclient: table %s.%s has no data type", table.Keyspace, table.Table)
		}

		if table.Keyspace == "" {
			return fmt.Errorf("vstreamclient: table %v has no keyspace", table)
		}

		if table.Table == "" {
			return fmt.Errorf("vstreamclient: table %v has no table name", table)
		}

		// make sure the keyspace exists in the cluster
		_, ok := v.shardsByKeyspace[table.Keyspace]
		if !ok {
			return fmt.Errorf("vstreamclient: keyspace %s not found in the cluster", table.Keyspace)
		}

		// the key is the keyspace and table name, separated by a period. We use this because vstream events
		// use this as the table name, so it's easier for lookup, and it's unique.
		k := fmt.Sprintf("%s.%s", table.Keyspace, table.Table)

		// if the same table is referenced multiple times in the same stream, only one table will actually
		// receive events. This prevents users from unknowingly missing events for the second table reference.
		if _, ok = v.tables[k]; ok {
			return fmt.Errorf("duplicate table %s in keyspace %s", table.Table, table.Keyspace)
		}

		// set defaults if not provided
		if table.Query == "" {
			table.Query = "select * from " + sqlescape.EscapeID(table.Table)
		}

		if table.MaxRowsPerFlush == 0 {
			table.MaxRowsPerFlush = DefaultMaxRowsPerFlush
		}

		// if the data type implements VStreamScanner, we will use that to scan the results
		_, table.implementsScanner = table.DataType.(VStreamScanner)

		// regardless whether the user provided a pointer to a struct or a struct, we want to store the
		// underlying type of the struct, so we can create new instances of it later
		table.underlyingType = reflect.Indirect(reflect.ValueOf(table.DataType)).Type()

		if table.underlyingType.Kind() != reflect.Struct {
			return fmt.Errorf("vstreamclient: data type for table %s.%s must be a struct", table.Keyspace, table.Table)
		}

		table.shards = make(map[string]shardConfig)

		// initialize the slice containing the batch of rows for this table
		table.resetBatch()

		// store the table in the map
		v.tables[k] = &table
	}

	return nil
}

func validateTableConfig(providedTables, dbTables map[string]*TableConfig) error {
	providedTablesMap := tablesToDBTableConfig(providedTables)
	dbTablesMap := tablesToDBTableConfig(dbTables)

	if !maps.Equal(providedTablesMap, dbTablesMap) {
		// TODO: this could be more user-friendly and show the differences
		return errors.New("vstreamclient: provided tables do not match stored tables")
	}

	return nil
}

func (table *TableConfig) resetBatch() {
	if table.ReuseBatchSlice && table.currentBatch != nil {
		table.currentBatch = slices.Delete(table.currentBatch, 0, len(table.currentBatch))
	} else {
		table.currentBatch = make([]Row, 0, table.MaxRowsPerFlush)
	}
}

func (table *TableConfig) handleFieldEvent(ev *binlogdatapb.FieldEvent) error {
	var fieldMap map[string]fieldMapping
	var err error

	if !table.implementsScanner {
		fieldMap, err = table.reflectMapFields(ev.Fields)
		if err != nil {
			return err
		}
	}

	table.shards[ev.Shard] = shardConfig{
		fieldMap: fieldMap,
		fields:   ev.Fields,
	}

	return nil
}

func (table *TableConfig) reflectMapFields(fields []*querypb.Field) (map[string]fieldMapping, error) {
	fieldMap := make(map[string]fieldMapping, len(fields))

	for i := 0; i < table.underlyingType.NumField(); i++ {
		structField := table.underlyingType.Field(i)
		if !structField.IsExported() {
			continue
		}

		// get the field name from the vstream, db, json tag, or the field name, in that order
		mappedFieldName := structField.Tag.Get("vstream")
		if mappedFieldName == "-" {
			continue
		}
		if mappedFieldName == "" {
			mappedFieldName = structField.Tag.Get("db")
		}
		if mappedFieldName == "" {
			mappedFieldName = structField.Tag.Get("json")
		}
		if mappedFieldName == "" {
			mappedFieldName = structField.Name
		}

		var found bool
		for j, tableField := range fields {
			if tableField.Name != mappedFieldName {
				continue
			}

			found = true
			fieldMap[mappedFieldName] = fieldMapping{
				rowIndex:    j,
				structIndex: structField.Index,
				kind:        structField.Type.Kind(),
				isPointer:   structField.Type.Kind() == reflect.Ptr,
			}
		}
		if !found && table.ErrorOnUnknownFields {
			return nil, fmt.Errorf("vstreamclient: field %s not found in provided data type", mappedFieldName)
		}
	}

	// sanity check that we found at least one field
	if len(fieldMap) == 0 {
		return nil, fmt.Errorf("vstreamclient: no matching fields found for table %s", table.Table)
	}

	return fieldMap, nil
}

func (table *TableConfig) handleRowEvent(ev *binlogdatapb.RowEvent, vstreamStats *VStreamStats) error {
	shard, ok := table.shards[ev.Shard]
	if !ok {
		return fmt.Errorf("unexpected shard: %s", ev.Shard)
	}

	table.currentBatch = slices.Grow(table.currentBatch, len(ev.RowChanges))

	for _, rc := range ev.RowChanges {
		var row []sqltypes.Value

		switch {
		case rc.After == nil: // delete event
			// even though a delete event might be represented as a nil row, the consumer will still need to know
			// which row was deleted, so we'll pass the before row to the consumer, which should contain the primary
			// key fields, so they can be used however necessary to handle the delete in a downstream system.
			row = sqltypes.MakeRowTrusted(shard.fields, rc.Before)
			vstreamStats.RowDeleteCount++
			table.stats.RowDeleteCount++

		case rc.Before == nil: // insert event
			row = sqltypes.MakeRowTrusted(shard.fields, rc.After)
			vstreamStats.RowInsertCount++
			table.stats.RowInsertCount++

		case rc.Before != nil: // update event
			row = sqltypes.MakeRowTrusted(shard.fields, rc.After)
			vstreamStats.RowUpdateCount++
			table.stats.RowUpdateCount++
		}

		// create a new struct for the row
		v := reflect.New(table.underlyingType)
		table.currentBatch = append(table.currentBatch, Row{
			RowEvent:  ev,
			RowChange: rc,
			Data:      v.Interface(),
		})

		// use the custom scanner if available
		if table.implementsScanner {
			returnVals := v.MethodByName("VStreamScan").Call([]reflect.Value{
				reflect.ValueOf(shard.fields),
				reflect.ValueOf(row),
				reflect.ValueOf(ev),
				reflect.ValueOf(rc),
			})
			if !returnVals[0].IsNil() {
				return fmt.Errorf("vstreamclient: client scan failed: %w", returnVals[0].Interface().(error))
			}
		} else {
			err := copyRowToStruct(shard, row, v)
			if err != nil {
				return err
			}
		}
	}

	return nil
}
