/*
Copyright 2022 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package schemadiff

import (
	"strconv"
	"strings"

	"vitess.io/vitess/go/vt/sqlparser"
)

//
type AlterTableEntityDiff struct {
	sqlparser.AlterTable
}

func (d *AlterTableEntityDiff) IsEmpty() bool {
	return d.Statement() == nil
}

func (d *AlterTableEntityDiff) Statement() sqlparser.Statement {
	if d == nil {
		return nil
	}
	return &d.AlterTable
}

//
type CreateTableEntityDiff struct {
	sqlparser.CreateTable
}

func (d *CreateTableEntityDiff) IsEmpty() bool {
	return d.Statement() == nil
}

func (d *CreateTableEntityDiff) Statement() sqlparser.Statement {
	if d == nil {
		return nil
	}
	return &d.CreateTable
}

//
type DropTableEntityDiff struct {
	sqlparser.DropTable
}

func (d *DropTableEntityDiff) IsEmpty() bool {
	return d.Statement() == nil
}

func (d *DropTableEntityDiff) Statement() sqlparser.Statement {
	if d == nil {
		return nil
	}
	return &d.DropTable
}

//
type CreateTableEntity struct {
	sqlparser.CreateTable
}

func NewCreateTableEntity(c *sqlparser.CreateTable) *CreateTableEntity {
	return &CreateTableEntity{CreateTable: *c}
}

func (c *CreateTableEntity) Format() string {
	return sqlparser.String(&c.CreateTable)
}

// Clause implements Entity interface function
func (c *CreateTableEntity) Clause() string {
	return sqlparser.String(&c.CreateTable)
}

// Diff implements Entity interface function
func (c *CreateTableEntity) Diff(other Entity, hints *DiffHints) (EntityDiff, error) {
	otherCreateTable, ok := other.(*CreateTableEntity)
	if !ok {
		return nil, ErrEntityTypeMismatch
	}
	if hints.StrictIndexOrdering {
		return nil, ErrStrictIndexOrderingUnsupported
	}
	if c.CreateTable.TableSpec == nil {
		return nil, ErrUnexpectedTableSpec
	}
	if len(c.CreateTable.TableSpec.Constraints) > 0 {
		return nil, ErrFKConstraintsUnsupported
	}
	if len(otherCreateTable.TableSpec.Constraints) > 0 {
		return nil, ErrFKConstraintsUnsupported
	}

	d, err := c.TableDiff(otherCreateTable, hints)
	if err != nil {
		return nil, err
	}
	return d, nil
}

// Diff compares this table statement with another table statement, and sees what it takes to
// change this table to look like the other table.
// It returns an AlterTable statement if changes are found, or nil if not.
// the other table may be of different name; its name is ignored.
func (c *CreateTableEntity) TableDiff(other *CreateTableEntity, hints *DiffHints) (*AlterTableEntityDiff, error) {
	otherStmt := other.CreateTable
	otherStmt.Table = c.CreateTable.Table

	if !c.CreateTable.IsFullyParsed() {
		return nil, ErrNotFullyParsed
	}
	if !otherStmt.IsFullyParsed() {
		return nil, ErrNotFullyParsed
	}

	format := sqlparser.String(&c.CreateTable)
	otherFormat := sqlparser.String(&otherStmt)
	if format == otherFormat {
		return nil, nil
	}

	alterTable := &sqlparser.AlterTable{
		Table: otherStmt.Table,
	}
	{
		// diff columns
		// ordered columns for both tables:
		t1Columns := c.CreateTable.TableSpec.Columns
		t2Columns := other.CreateTable.TableSpec.Columns
		c.diffColumns(alterTable, t1Columns, t2Columns, hints)
	}
	{
		// diff keys
		// ordered keys for both tables:
		t1Keys := c.CreateTable.TableSpec.Indexes
		t2Keys := other.CreateTable.TableSpec.Indexes
		c.diffKeys(alterTable, t1Keys, t2Keys, hints)
	}
	{
		// diff partitions
		// ordered keys for both tables:
		t1Partitions := c.CreateTable.TableSpec.PartitionOption
		t2Partitions := other.CreateTable.TableSpec.PartitionOption
		if err := c.diffPartitions(alterTable, t1Partitions, t2Partitions, hints); err != nil {
			return nil, err
		}
	}
	{
		// diff table options
		// ordered keys for both tables:
		t1Options := c.CreateTable.TableSpec.Options
		t2Options := other.CreateTable.TableSpec.Options
		if err := c.diffOptions(alterTable, t1Options, t2Options, hints); err != nil {
			return nil, err
		}
	}
	if len(alterTable.AlterOptions) == 0 && alterTable.PartitionSpec == nil {
		// it's possible that the table definitions are different, and still there's no
		// "real" difference. Reasons could be:
		// - reordered keys -- we treat that as non-diff
		return nil, nil
	}
	return &AlterTableEntityDiff{AlterTable: *alterTable}, nil
}

func (c *CreateTableEntity) diffOptions(alterTable *sqlparser.AlterTable,
	t1Options sqlparser.TableOptions,
	t2Options sqlparser.TableOptions,
	hints *DiffHints,
) error {
	t1OptionsMap := map[string]*sqlparser.TableOption{}
	t2OptionsMap := map[string]*sqlparser.TableOption{}
	for _, option := range t1Options {
		t1OptionsMap[option.Name] = option
	}
	for _, option := range t2Options {
		t2OptionsMap[option.Name] = option
	}
	alterTableOptions := sqlparser.TableOptions{}
	// dropped options
	for _, t1Option := range t1Options {
		if _, ok := t2OptionsMap[t1Option.Name]; !ok {
			// option exists in t1 but not in t2, hence it is dropped
			var tableOption *sqlparser.TableOption
			switch strings.ToUpper(t1Option.Name) {
			case "AUTO_INCREMENT":
				// skip
			case "AVG_ROW_LENGTH":
				// skip. MyISAM only, not interesting
			case "CHECKSUM":
				tableOption = &sqlparser.TableOption{Value: sqlparser.NewIntLiteral("0")}
			case "COLLATE":
				// skip. the default collation is applied per CHARSET
			case "COMMENT":
				tableOption = &sqlparser.TableOption{String: ""}
			case "COMPRESSION":
				tableOption = &sqlparser.TableOption{Value: sqlparser.NewStrLiteral("")}
			case "CONNECTION":
				tableOption = &sqlparser.TableOption{Value: sqlparser.NewStrLiteral("")}
			case "DATA DIRECTORY":
				tableOption = &sqlparser.TableOption{Value: sqlparser.NewStrLiteral("")}
			case "DELAY_KEY_WRITE":
				tableOption = &sqlparser.TableOption{Value: sqlparser.NewIntLiteral("0")}
			case "ENCRYPTION":
				tableOption = &sqlparser.TableOption{Value: sqlparser.NewStrLiteral("N")}
			case "ENGINE":
				// skip
			case "INDEX DIRECTORY":
				tableOption = &sqlparser.TableOption{Value: sqlparser.NewStrLiteral("")}
			case "INSERT_METHOD":
				// MyISAM only. skip
			case "KEY_BLOCK_SIZE":
				tableOption = &sqlparser.TableOption{Value: sqlparser.NewIntLiteral("0")}
			case "MAX_ROWS":
				tableOption = &sqlparser.TableOption{Value: sqlparser.NewIntLiteral("0")}
			case "MIN_ROWS":
				tableOption = &sqlparser.TableOption{Value: sqlparser.NewIntLiteral("0")}
			case "PACK_KEYS":
				tableOption = &sqlparser.TableOption{String: "DEFAULT"}
			case "PASSWORD":
				// unused option. skip
			case "ROW_FORMAT":
				tableOption = &sqlparser.TableOption{String: "DEFAULT"}
			case "STATS_AUTO_RECALC":
				tableOption = &sqlparser.TableOption{String: "DEFAULT"}
			case "STATS_PERSISTENT":
				tableOption = &sqlparser.TableOption{String: "DEFAULT"}
			case "STATS_SAMPLE_PAGES":
				tableOption = &sqlparser.TableOption{String: "DEFAULT"}
			case "TABLESPACE":
				// not supporting the change, skip
			case "UNION":
				// MyISAM/MERGE only. Skip
			default:
				return ErrUnsupportedTableOption
			}
			if tableOption != nil {
				tableOption.Name = t1Option.Name
				alterTableOptions = append(alterTableOptions, tableOption)
			}
		}

	}
	// changed options
	for _, t2Option := range t2Options {
		if t1Option, ok := t1OptionsMap[t2Option.Name]; ok {
			options1 := sqlparser.TableOptions{t1Option}
			options2 := sqlparser.TableOptions{t2Option}
			if sqlparser.String(options1) != sqlparser.String(options2) {
				// options are different.
				// However, we don't automatically apply these changes. It depends on the option!
				switch strings.ToUpper(t1Option.Name) {
				case "AUTO_INCREMENT":
					switch hints.AutoIncrementStrategy {
					case AutoIncrementApplyAlways:
						alterTableOptions = append(alterTableOptions, t2Option)
					case AutoIncrementApplyHigher:
						option1AutoIncrement, err := strconv.ParseInt(t1Option.Value.Val, 10, 64)
						if err != nil {
							return err
						}
						option2AutoIncrement, err := strconv.ParseInt(t2Option.Value.Val, 10, 64)
						if err != nil {
							return err
						}
						if option2AutoIncrement > option1AutoIncrement {
							// never decrease AUTO_INCREMENT. Only increase
							alterTableOptions = append(alterTableOptions, t2Option)
						}
					case AutoIncrementIgnore:
						// do not apply
					}
				default:
					// Apply the new options
					alterTableOptions = append(alterTableOptions, t2Option)
				}
			}
		}
	}
	// added options
	for _, t2Option := range t2Options {
		if _, ok := t1OptionsMap[t2Option.Name]; !ok {
			switch strings.ToUpper(t2Option.Name) {
			case "AUTO_INCREMENT":
				switch hints.AutoIncrementStrategy {
				case AutoIncrementApplyAlways, AutoIncrementApplyHigher:
					alterTableOptions = append(alterTableOptions, t2Option)
				case AutoIncrementIgnore:
					// do not apply
				}
			default:
				alterTableOptions = append(alterTableOptions, t2Option)
			}
		}
	}

	if len(alterTableOptions) > 0 {
		alterTable.AlterOptions = append(alterTable.AlterOptions, alterTableOptions)
	}
	return nil
}

func (c *CreateTableEntity) diffPartitions(alterTable *sqlparser.AlterTable,
	t1Partitions *sqlparser.PartitionOption,
	t2Partitions *sqlparser.PartitionOption,
	hints *DiffHints,
) error {
	switch {
	case t1Partitions == nil && t2Partitions == nil:
		return nil
	case t2Partitions == nil:
		// remove partitioning
		partitionSpec := &sqlparser.PartitionSpec{
			Action: sqlparser.RemoveAction,
			IsAll:  true,
		}
		alterTable.PartitionSpec = partitionSpec
	case sqlparser.String(t1Partitions) == sqlparser.String(t2Partitions):
		// identical partitioning
		return nil
	default:
		// either partitioning was added or it was changed
		// return ErrPartitioningUnsupported
		return ErrPartitioningUnsupported
		// TODO(shlomi): paritions will be supported when sqlparser support for partitions is complete.
		// missing right now: parsing PARTITION BY LIST/RANGE, ALTER TABLE... PARTITION (PartitionOption)
		//		alterTable.PartitionOption = t2Partitions
	}
	return nil
}

func (c *CreateTableEntity) diffKeys(alterTable *sqlparser.AlterTable,
	t1Keys []*sqlparser.IndexDefinition,
	t2Keys []*sqlparser.IndexDefinition,
	hints *DiffHints,
) {
	t1KeysMap := map[string]*sqlparser.IndexDefinition{}
	t2KeysMap := map[string]*sqlparser.IndexDefinition{}
	for _, key := range t1Keys {
		t1KeysMap[key.Info.Name.String()] = key
	}
	for _, key := range t2Keys {
		t2KeysMap[key.Info.Name.String()] = key
	}

	dropKeyStatement := func(name sqlparser.ColIdent) *sqlparser.DropKey {
		dropKey := &sqlparser.DropKey{}
		if dropKey.Name.String() == "PRIMARY" {
			dropKey.Type = sqlparser.PrimaryKeyType
		} else {
			dropKey.Type = sqlparser.NormalKeyType
			dropKey.Name = name
		}
		return dropKey
	}

	// evaluate dropped columns
	//
	for _, t1Key := range t1Keys {
		if _, ok := t2KeysMap[t1Key.Info.Name.String()]; !ok {
			// column exists in t1 but not in t2, hence it is dropped
			dropKey := dropKeyStatement(t1Key.Info.Name)
			alterTable.AlterOptions = append(alterTable.AlterOptions, dropKey)
		}
	}

	for _, t2Key := range t2Keys {
		t2KeyName := t2Key.Info.Name.String()
		// evaluate modified & added keys:
		//
		if t1Key, ok := t1KeysMap[t2KeyName]; ok {
			// key exists in both tables
			// check diff between before/after columns:
			if sqlparser.String(t2Key) != sqlparser.String(t1Key) {
				// keys with same name have different definition. There is no ALTER INDEX statement,
				// we're gonna drop and create.
				dropKey := dropKeyStatement(t1Key.Info.Name)
				addKey := &sqlparser.AddIndexDefinition{
					IndexDefinition: t2Key,
				}
				alterTable.AlterOptions = append(alterTable.AlterOptions, dropKey)
				alterTable.AlterOptions = append(alterTable.AlterOptions, addKey)
			}
		} else {
			// key exists in t2 but not in t1, hence it is added
			addKey := &sqlparser.AddIndexDefinition{
				IndexDefinition: t2Key,
			}
			alterTable.AlterOptions = append(alterTable.AlterOptions, addKey)
		}
	}
}

// evaluateColumnReordering produces a minimal uni-directional reordering set of columns. To elaborate:
// The function receives two sets of columns. the two must be permutations of one another. Specifically,
// these are the columns shared between the from&to tables.
// The function evaluates the minimal number of steps such that:
// - each steps reorders a column, specificly moving it backwards (never forward)
// - the final set of steps permutate the "from" columns order in to the "to" columns order
func evaluateColumnReordering(t1SharedColumns, t2SharedColumns []*sqlparser.ColumnDefinition) map[string]int {
	minimalColumnReordering := map[string]int{}
	// buf is an ever changing space. It begins with the "from" set of columns.
	// In neach step of th ealgorithm we will remove one element from the buffer. So each
	// step the buffer will become smaller, with less work to be done
	buf := t1SharedColumns[:]
	colIndexInBuf := func(name string) int {
		for i := range buf {
			if buf[i].Name.String() == name {
				return i
			}
		}
		return -1
	}
	// t2Col is our desired state. It's the "to" table's ordered list of columns
	for i, t2Col := range t2SharedColumns {
		// for each position in the target list of columns, we check whether we need to
		// reorder a column.
		// t2ColName is the desired column name at this position:
		t2ColName := t2Col.Name.String()
		if t2ColName == buf[0].Name.String() {
			// no need to reorder this column. continue
			buf = buf[1:]
			continue
		}
		// we already know where we want this column: in position i, and we know that
		// because we iterate t2SharedColumns
		minimalColumnReordering[t2ColName] = i
		// Remove the column from buf. This makes buf one element shorter with less work for next steps
		colIndex := colIndexInBuf(t2ColName)
		buf = append(buf[0:colIndex], buf[colIndex+1:]...)
	}
	return minimalColumnReordering
}

// Diff compares this table statement with another table statement, and sees what it takes to
// change this table to look like the other table.
// It returns an AlterTable statement if changes are found, or nil if not.
// the other table may be of different name; its name is ignored.
func (c *CreateTableEntity) diffColumns(alterTable *sqlparser.AlterTable,
	t1Columns []*sqlparser.ColumnDefinition,
	t2Columns []*sqlparser.ColumnDefinition,
	hints *DiffHints,
) {
	// map columns by names for easy access
	t1ColumnsMap := map[string]*sqlparser.ColumnDefinition{}
	t2ColumnsMap := map[string]*sqlparser.ColumnDefinition{}
	for _, col := range t1Columns {
		t1ColumnsMap[col.Name.String()] = col
	}
	for _, col := range t2Columns {
		t2ColumnsMap[col.Name.String()] = col
	}

	// For purpose of column reordering detection, we maintain a list of
	// shared columns, by order of appearance in t1
	t1SharedColumns := []*sqlparser.ColumnDefinition{}

	// evaluate dropped columns
	//
	for _, t1Col := range t1Columns {
		if _, ok := t2ColumnsMap[t1Col.Name.String()]; ok {
			t1SharedColumns = append(t1SharedColumns, t1Col)
		} else {
			// column exists in t1 but not in t2, hence it is dropped
			dropColumn := &sqlparser.DropColumn{
				Name: getColName(&t1Col.Name),
			}
			alterTable.AlterOptions = append(alterTable.AlterOptions, dropColumn)
		}
	}

	// For purpose of column reordering detection, we maintain a list of
	// shared columns, by order of appearance in t2
	t2SharedColumns := []*sqlparser.ColumnDefinition{}
	for _, t2Col := range t2Columns {
		t2ColName := t2Col.Name.String()
		if _, ok := t1ColumnsMap[t2ColName]; ok {
			// column exists in both tables
			t2SharedColumns = append(t2SharedColumns, t2Col)
		}
	}

	// evaluate modified columns
	//
	columnReordering := evaluateColumnReordering(t1SharedColumns, t2SharedColumns)
	for _, t2Col := range t2SharedColumns {
		t2ColName := t2Col.Name.String()
		// we know that column exists in both tables
		t1Col := t1ColumnsMap[t2ColName]
		t1ColEntity := NewColumnDefinitionEntity(t1Col)
		t2ColEntity := NewColumnDefinitionEntity(t2Col)

		// check diff between before/after columns:
		modifyColumn := t1ColEntity.ColumnDiff(t2ColEntity, hints)
		// It is also possible that a column is reordered. Whether the column definition has
		// or hasn't changed, if a column is reordered then that's a change of its own!
		if columnReorderIndex, ok := columnReordering[t2ColName]; ok {
			// seems like we previously evaluated that this column should be reordered
			if modifyColumn == nil {
				// create column change
				modifyColumn = &ModifyColumnDiff{
					ModifyColumn: sqlparser.ModifyColumn{
						NewColDefinition: t2Col,
					},
				}
			}
			if columnReorderIndex == 0 {
				modifyColumn.ModifyColumn.First = true
			} else {
				modifyColumn.ModifyColumn.After = getColName(&t2SharedColumns[columnReorderIndex-1].Name)
			}
		}
		if modifyColumn != nil {
			// column definition or ordering has changed
			alterTable.AlterOptions = append(alterTable.AlterOptions, &modifyColumn.ModifyColumn)
		}
	}
	// Evaluate added columns
	//
	// Every added column is obviously a diff. But on top of that, we are also interested to know
	// if the column is added somewhere in between existing columns rather than appended to the
	// end of existing columns list.
	expectAppendIndex := len(t2SharedColumns)
	for t2ColIndex, t2Col := range t2Columns {
		t2ColName := t2Col.Name.String()
		if _, ok := t1ColumnsMap[t2ColName]; !ok {
			// column exists in t2 but not in t1, hence it is added
			addColumn := &sqlparser.AddColumns{
				Columns: []*sqlparser.ColumnDefinition{t2Col},
			}
			if t2ColIndex < expectAppendIndex {
				// This column is added somewhere in between existing columns, not appended at end of column list
				if t2ColIndex == 0 {
					addColumn.First = true
				} else {
					addColumn.After = getColName(&t2Columns[t2ColIndex-1].Name)
				}
			}
			expectAppendIndex++
			alterTable.AlterOptions = append(alterTable.AlterOptions, addColumn)
		}
	}
}
