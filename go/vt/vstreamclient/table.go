package vstreamclient

import (
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
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

	// if not configured, this is set to "select * from keyspace.table", streaming all the fields. This can be
	// overridden to select only the fields that are needed, which can reduce memory usage and improve performance,
	// and also alias the fields to match the struct fields.
	Query string

	// MaxRowsPerFlush serves two purposes:
	//  1. it limits the number of rows that are flushed at once, to avoid large transactions. If more than
	//     this number of rows are processed, they will be flushed in chunks of this size.
	//  2. if this number is exceeded before reaching the minFlushDuration, it will trigger a flush to avoid
	//     holding too much memory in the rows slice.
	MaxRowsPerFlush int

	// ReuseBatchSlice will reuse the backing array of the `rows []Row` slice passed to FlushFn
	// across successive batches, which can reduce memory allocations.
	//
	// DANGER: If true, the caller MUST NOT retain references to the `rows` slice or any of its
	// elements after FlushFn returns. The slice and its contents will be aggressively mutated
	// in place by the internal processing loop. Retaining pointers will lead to data corruption.
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
	timeLocation      *time.Location

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
	structType  reflect.Type
	kind        reflect.Kind
	isPointer   bool
	jsonDecode  bool
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

	queriesByTableName := make(map[string]string)

	for _, table := range tables {
		// basic validation
		if table.DataType == nil {
			return fmt.Errorf("vstreamclient: table %s.%s has no data type", table.Keyspace, table.Table)
		}

		dataTypeValue := reflect.ValueOf(table.DataType)
		switch dataTypeValue.Kind() {
		case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
			if dataTypeValue.IsNil() {
				return fmt.Errorf("vstreamclient: table %s.%s has no data type", table.Keyspace, table.Table)
			}
		}

		if table.Keyspace == "" {
			return fmt.Errorf("vstreamclient: table %v has no keyspace", table)
		}

		if table.Table == "" {
			return fmt.Errorf("vstreamclient: table %v has no table name", table)
		}
		if table.FlushFn == nil {
			return fmt.Errorf("vstreamclient: table %s.%s has no flush function", table.Keyspace, table.Table)
		}

		// make sure the keyspace exists in the cluster
		_, ok := v.shardsByKeyspace[table.Keyspace]
		if !ok {
			return fmt.Errorf("vstreamclient: keyspace %s not found in the cluster", table.Keyspace)
		}

		k := qualifiedTableName(table.Keyspace, table.Table)

		if _, ok = v.tables[k]; ok {
			return fmt.Errorf("vstreamclient: duplicate table %s", k)
		}

		// set defaults if not provided
		if table.Query == "" {
			table.Query = "select * from " + sqlescape.EscapeID(table.Table)
		}

		// VStream rules match by bare table name, so same-named tables across keyspaces can
		// end up sharing whichever rule is matched first. Requiring identical queries makes
		// that overlap harmless instead of silently applying the wrong filter/projection.
		if existingQuery, ok := queriesByTableName[table.Table]; ok && existingQuery != table.Query {
			return fmt.Errorf("vstreamclient: same table name across keyspaces must use identical queries: %s", table.Table)
		}
		queriesByTableName[table.Table] = table.Query

		if table.MaxRowsPerFlush == 0 {
			table.MaxRowsPerFlush = DefaultMaxRowsPerFlush
		} else if table.MaxRowsPerFlush < 0 {
			return fmt.Errorf("vstreamclient: max rows per flush must be positive for table %s.%s, got %d", table.Keyspace, table.Table, table.MaxRowsPerFlush)
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

func qualifiedTableName(keyspace, table string) string {
	return keyspace + "." + table
}

func (v *VStreamClient) lookupTable(tableName string) (*TableConfig, error) {
	if table, ok := v.tables[tableName]; ok {
		return table, nil
	}

	var matched *TableConfig
	for _, table := range v.tables {
		if table.Table != tableName {
			continue
		}
		if matched != nil {
			return nil, fmt.Errorf("vstreamclient: ambiguous table name: %s", tableName)
		}
		matched = table
	}

	if matched == nil {
		return nil, fmt.Errorf("vstreamclient: unexpected table name: %s", tableName)
	}

	return matched, nil
}

func validateTableConfig(providedTables, dbTables map[string]*TableConfig) error {
	providedTablesMap := tablesToDBTableConfig(providedTables)
	dbTablesMap := tablesToDBTableConfig(dbTables)

	keys := make([]string, 0, len(providedTablesMap)+len(dbTablesMap))
	seen := make(map[string]struct{}, len(providedTablesMap)+len(dbTablesMap))
	for key := range providedTablesMap {
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		keys = append(keys, key)
	}
	for key := range dbTablesMap {
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		keys = append(keys, key)
	}
	slices.Sort(keys)

	diffs := make([]string, 0)
	for _, key := range keys {
		provided, providedOK := providedTablesMap[key]
		stored, storedOK := dbTablesMap[key]

		switch {
		case providedOK && !storedOK:
			diffs = append(diffs, fmt.Sprintf("table %s is new in provided config", formatTableConfigName(key, provided)))

		case !providedOK && storedOK:
			diffs = append(diffs, fmt.Sprintf("table %s is missing from provided config", formatTableConfigName(key, stored)))

		case provided != stored:
			tableName := formatTableConfigName(key, provided)
			if storedName := formatTableConfigName(key, stored); storedName != tableName {
				diffs = append(diffs, fmt.Sprintf("table %s identity changed: provided %q, stored %q", key, tableName, storedName))
				continue
			}

			if provided.Query != stored.Query {
				diffs = append(diffs, fmt.Sprintf("table %s query changed: provided %q, stored %q", tableName, provided.Query, stored.Query))
			}
		}
	}

	if len(diffs) > 0 {
		return fmt.Errorf("vstreamclient: provided tables do not match stored tables: %s", strings.Join(diffs, "; "))
	}

	return nil
}

func formatTableConfigName(key string, table dbTableConfig) string {
	if table.Keyspace != "" && table.Table != "" {
		return qualifiedTableName(table.Keyspace, table.Table)
	}

	return key
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
	table.reflectMapStructFields(table.underlyingType, nil, fields, fieldMap)

	if table.ErrorOnUnknownFields {
		for _, f := range fields {
			if _, ok := fieldMap[f.Name]; !ok {
				return nil, fmt.Errorf("vstreamclient: field %s not found in provided data type", f.Name)
			}
		}
	}

	// sanity check that we found at least one field
	if len(fieldMap) == 0 {
		return nil, fmt.Errorf("vstreamclient: no matching fields found for table %s", table.Table)
	}

	return fieldMap, nil
}

func (table *TableConfig) reflectMapStructFields(structType reflect.Type, indexPrefix []int, fields []*querypb.Field, fieldMap map[string]fieldMapping) {
	for i := range structType.NumField() {
		structField := structType.Field(i)
		if !structField.IsExported() {
			continue
		}

		// get the field name from the vstream, db, json tag, or the field name, in that order
		mappedFieldName, jsonDecode := parseVStreamTag(structField.Tag.Get("vstream"))
		if mappedFieldName == "-" {
			continue
		}
		if mappedFieldName == "" {
			mappedFieldName = parseTagName(structField.Tag.Get("db"))
		}
		if mappedFieldName == "" {
			mappedFieldName = parseTagName(structField.Tag.Get("json"))
		}

		fullIndex := append(append([]int(nil), indexPrefix...), structField.Index...)
		if table.shouldRecurseStructField(structField, mappedFieldName, jsonDecode, fields) {
			table.reflectMapStructFields(structField.Type, fullIndex, fields, fieldMap)
			continue
		}

		if mappedFieldName == "" {
			mappedFieldName = structField.Name
		}

		for j, tableField := range fields {
			if tableField.Name != mappedFieldName {
				continue
			}

			fieldType := structField.Type
			isPointer := fieldType.Kind() == reflect.Ptr
			if isPointer {
				fieldType = fieldType.Elem()
			}

			fieldMap[mappedFieldName] = fieldMapping{
				rowIndex:    j,
				structIndex: fullIndex,
				structType:  fieldType,
				kind:        fieldType.Kind(),
				isPointer:   isPointer,
				jsonDecode:  jsonDecode,
			}
		}
	}
}

func (table *TableConfig) shouldRecurseStructField(structField reflect.StructField, mappedFieldName string, jsonDecode bool, fields []*querypb.Field) bool {
	if mappedFieldName != "" || jsonDecode {
		return false
	}
	if structField.Type.Kind() != reflect.Struct || structField.Type == reflect.TypeFor[time.Time]() {
		return false
	}
	for _, field := range fields {
		if field.Name == structField.Name {
			return false
		}
	}
	return true
}

func parseVStreamTag(tag string) (name string, jsonDecode bool) {
	if tag == "" {
		return "", false
	}

	parts := strings.Split(tag, ",")
	name = strings.TrimSpace(parts[0])
	for _, opt := range parts[1:] {
		if strings.EqualFold(strings.TrimSpace(opt), "json") {
			jsonDecode = true
		}
	}

	return name, jsonDecode
}

func parseTagName(tag string) string {
	if tag == "" {
		return ""
	}

	name, _, _ := strings.Cut(tag, ",")
	return strings.TrimSpace(name)
}

func (table *TableConfig) validateRowChangeForDefaultDecoding(rc *binlogdatapb.RowChange) error {
	if table.implementsScanner || rc == nil || rc.After == nil {
		return nil
	}

	qualifiedName := qualifiedTableName(table.Keyspace, table.Table)

	if rc.JsonPartialValues != nil {
		return fmt.Errorf("vstreamclient: partial JSON updates are unsupported for default decoding on table %s; implement VStreamScanner to handle RowChange bitmaps explicitly", qualifiedName)
	}

	if rc.DataColumns != nil {
		return fmt.Errorf("vstreamclient: partial row images are unsupported for default decoding on table %s; implement VStreamScanner to handle RowChange bitmaps explicitly", qualifiedName)
	}

	return nil
}

func (table *TableConfig) handleRowEvent(ev *binlogdatapb.RowEvent, vstreamStats *VStreamStats) error {
	shard, ok := table.shards[ev.Shard]
	if !ok {
		return fmt.Errorf("vstreamclient: unexpected shard: %s", ev.Shard)
	}

	table.currentBatch = slices.Grow(table.currentBatch, len(ev.RowChanges))

	for _, rc := range ev.RowChanges {
		if err := table.validateRowChangeForDefaultDecoding(rc); err != nil {
			return err
		}

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

		var data any

		// create a new struct for the row
		var v reflect.Value
		if row != nil {
			v = reflect.New(table.underlyingType)

			if table.implementsScanner {
				scanner := v.Interface().(VStreamScanner)
				if err := scanner.VStreamScan(shard.fields, row, ev, rc); err != nil {
					return fmt.Errorf("vstreamclient: client scan failed: %w", err)
				}
			} else {
				err := copyRowToStructInLocation(shard, row, v, table.timeLocation)
				if err != nil {
					return err
				}
			}

			data = v.Interface()
		}

		table.currentBatch = append(table.currentBatch, Row{
			RowEvent:  ev,
			RowChange: rc,
			Data:      data,
		})
	}

	return nil
}
