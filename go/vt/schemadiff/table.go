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
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"

	golcs "github.com/yudai/golcs"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/vt/sqlparser"
)

type AlterTableEntityDiff struct {
	from       *CreateTableEntity
	to         *CreateTableEntity
	alterTable *sqlparser.AlterTable

	subsequentDiff *AlterTableEntityDiff
}

// IsEmpty implements EntityDiff
func (d *AlterTableEntityDiff) IsEmpty() bool {
	return d.Statement() == nil
}

// Entities implements EntityDiff
func (d *AlterTableEntityDiff) Entities() (from Entity, to Entity) {
	return d.from, d.to
}

// Statement implements EntityDiff
func (d *AlterTableEntityDiff) Statement() sqlparser.Statement {
	if d == nil {
		return nil
	}
	return d.alterTable
}

// AlterTable returns the underlying sqlparser.AlterTable that was generated for the diff.
func (d *AlterTableEntityDiff) AlterTable() *sqlparser.AlterTable {
	if d == nil {
		return nil
	}
	return d.alterTable
}

// StatementString implements EntityDiff
func (d *AlterTableEntityDiff) StatementString() (s string) {
	if stmt := d.Statement(); stmt != nil {
		s = sqlparser.String(stmt)
	}
	return s
}

// CanonicalStatementString implements EntityDiff
func (d *AlterTableEntityDiff) CanonicalStatementString() (s string) {
	if stmt := d.Statement(); stmt != nil {
		s = sqlparser.CanonicalString(stmt)
	}
	return s
}

// SubsequentDiff implements EntityDiff
func (d *AlterTableEntityDiff) SubsequentDiff() EntityDiff {
	if d == nil {
		return nil
	}
	return d.subsequentDiff
}

// SetSubsequentDiff implements EntityDiff
func (d *AlterTableEntityDiff) SetSubsequentDiff(subDiff EntityDiff) {
	if d == nil {
		return
	}
	if subTableDiff, ok := subDiff.(*AlterTableEntityDiff); ok {
		d.subsequentDiff = subTableDiff
	} else {
		d.subsequentDiff = nil
	}
}

// addSubsequentDiff adds a subsequent diff to the tail of the diff sequence
func (d *AlterTableEntityDiff) addSubsequentDiff(diff *AlterTableEntityDiff) {
	if d.subsequentDiff == nil {
		d.subsequentDiff = diff
	} else {
		d.subsequentDiff.addSubsequentDiff(diff)
	}
}

type CreateTableEntityDiff struct {
	to          *CreateTableEntity
	createTable *sqlparser.CreateTable
}

// IsEmpty implements EntityDiff
func (d *CreateTableEntityDiff) IsEmpty() bool {
	return d.Statement() == nil
}

// Entities implements EntityDiff
func (d *CreateTableEntityDiff) Entities() (from Entity, to Entity) {
	return nil, &CreateTableEntity{CreateTable: d.createTable}
}

// Statement implements EntityDiff
func (d *CreateTableEntityDiff) Statement() sqlparser.Statement {
	if d == nil {
		return nil
	}
	return d.createTable
}

// CreateTable returns the underlying sqlparser.CreateTable that was generated for the diff.
func (d *CreateTableEntityDiff) CreateTable() *sqlparser.CreateTable {
	if d == nil {
		return nil
	}
	return d.createTable
}

// StatementString implements EntityDiff
func (d *CreateTableEntityDiff) StatementString() (s string) {
	if stmt := d.Statement(); stmt != nil {
		s = sqlparser.String(stmt)
	}
	return s
}

// CanonicalStatementString implements EntityDiff
func (d *CreateTableEntityDiff) CanonicalStatementString() (s string) {
	if stmt := d.Statement(); stmt != nil {
		s = sqlparser.CanonicalString(stmt)
	}
	return s
}

// SubsequentDiff implements EntityDiff
func (d *CreateTableEntityDiff) SubsequentDiff() EntityDiff {
	return nil
}

// SetSubsequentDiff implements EntityDiff
func (d *CreateTableEntityDiff) SetSubsequentDiff(EntityDiff) {
}

type DropTableEntityDiff struct {
	from      *CreateTableEntity
	dropTable *sqlparser.DropTable
}

// IsEmpty implements EntityDiff
func (d *DropTableEntityDiff) IsEmpty() bool {
	return d.Statement() == nil
}

// Entities implements EntityDiff
func (d *DropTableEntityDiff) Entities() (from Entity, to Entity) {
	return d.from, nil
}

// Statement implements EntityDiff
func (d *DropTableEntityDiff) Statement() sqlparser.Statement {
	if d == nil {
		return nil
	}
	return d.dropTable
}

// DropTable returns the underlying sqlparser.DropTable that was generated for the diff.
func (d *DropTableEntityDiff) DropTable() *sqlparser.DropTable {
	if d == nil {
		return nil
	}
	return d.dropTable
}

// StatementString implements EntityDiff
func (d *DropTableEntityDiff) StatementString() (s string) {
	if stmt := d.Statement(); stmt != nil {
		s = sqlparser.String(stmt)
	}
	return s
}

// CanonicalStatementString implements EntityDiff
func (d *DropTableEntityDiff) CanonicalStatementString() (s string) {
	if stmt := d.Statement(); stmt != nil {
		s = sqlparser.CanonicalString(stmt)
	}
	return s
}

// SubsequentDiff implements EntityDiff
func (d *DropTableEntityDiff) SubsequentDiff() EntityDiff {
	return nil
}

// SetSubsequentDiff implements EntityDiff
func (d *DropTableEntityDiff) SetSubsequentDiff(EntityDiff) {
}

type RenameTableEntityDiff struct {
	from        *CreateTableEntity
	to          *CreateTableEntity
	renameTable *sqlparser.RenameTable
}

// IsEmpty implements EntityDiff
func (d *RenameTableEntityDiff) IsEmpty() bool {
	return d.Statement() == nil
}

// Entities implements EntityDiff
func (d *RenameTableEntityDiff) Entities() (from Entity, to Entity) {
	return d.from, d.to
}

// Statement implements EntityDiff
func (d *RenameTableEntityDiff) Statement() sqlparser.Statement {
	if d == nil {
		return nil
	}
	return d.renameTable
}

// RenameTable returns the underlying sqlparser.RenameTable that was generated for the diff.
func (d *RenameTableEntityDiff) RenameTable() *sqlparser.RenameTable {
	if d == nil {
		return nil
	}
	return d.renameTable
}

// StatementString implements EntityDiff
func (d *RenameTableEntityDiff) StatementString() (s string) {
	if stmt := d.Statement(); stmt != nil {
		s = sqlparser.String(stmt)
	}
	return s
}

// CanonicalStatementString implements EntityDiff
func (d *RenameTableEntityDiff) CanonicalStatementString() (s string) {
	if stmt := d.Statement(); stmt != nil {
		s = sqlparser.CanonicalString(stmt)
	}
	return s
}

// SubsequentDiff implements EntityDiff
func (d *RenameTableEntityDiff) SubsequentDiff() EntityDiff {
	return nil
}

// SetSubsequentDiff implements EntityDiff
func (d *RenameTableEntityDiff) SetSubsequentDiff(EntityDiff) {
}

// CreateTableEntity stands for a TABLE construct. It contains the table's CREATE statement.
type CreateTableEntity struct {
	*sqlparser.CreateTable
}

func NewCreateTableEntity(c *sqlparser.CreateTable) (*CreateTableEntity, error) {
	if !c.IsFullyParsed() {
		return nil, &NotFullyParsedError{Entity: c.Table.Name.String(), Statement: sqlparser.CanonicalString(c)}
	}
	entity := &CreateTableEntity{CreateTable: c}
	entity.normalize()
	return entity, nil
}

// normalize cleans up the table definition:
// - setting names to all keys
// - table option case (upper/lower/special)
// The function returns this receiver as courtesy
func (c *CreateTableEntity) normalize() *CreateTableEntity {
	c.normalizeKeys()
	c.normalizeUnnamedConstraints()
	c.normalizeTableOptions()
	c.normalizeColumnOptions()
	c.normalizeIndexOptions()
	c.normalizePartitionOptions()
	return c
}

func (c *CreateTableEntity) normalizeTableOptions() {
	for _, opt := range c.CreateTable.TableSpec.Options {
		opt.Name = strings.ToLower(opt.Name)
		switch opt.Name {
		case "charset":
			opt.String = strings.ToLower(opt.String)
			if charset, ok := collationEnv.CharsetAlias(opt.String); ok {
				opt.String = charset
			}
		case "collate":
			opt.String = strings.ToLower(opt.String)
			if collation, ok := collationEnv.CollationAlias(opt.String); ok {
				opt.String = collation
			}
		case "engine":
			opt.String = strings.ToUpper(opt.String)
			if engineName, ok := engineCasing[opt.String]; ok {
				opt.String = engineName
			}
		case "row_format":
			opt.String = strings.ToUpper(opt.String)
		}
	}
}

func (c *CreateTableEntity) Clone() Entity {
	return &CreateTableEntity{CreateTable: sqlparser.CloneRefOfCreateTable(c.CreateTable)}
}

// Right now we assume MySQL 8.0 for the collation normalization handling.
const mysqlCollationVersion = "8.0.0"

var collationEnv = collations.NewEnvironment(mysqlCollationVersion)

func defaultCharset() string {
	collation := collationEnv.LookupByID(collations.ID(collationEnv.DefaultConnectionCharset()))
	if collation == nil {
		return ""
	}
	return collation.Charset().Name()
}

func defaultCharsetCollation(charset string) string {
	collation := collationEnv.DefaultCollationForCharset(charset)
	if collation == nil {
		return ""
	}
	return collation.Name()
}

func (c *CreateTableEntity) normalizeColumnOptions() {
	tableCharset := defaultCharset()
	tableCollation := ""
	for _, option := range c.CreateTable.TableSpec.Options {
		switch strings.ToUpper(option.Name) {
		case "CHARSET":
			tableCharset = option.String
		case "COLLATE":
			tableCollation = option.String
		}
	}
	defaultCollation := defaultCharsetCollation(tableCharset)
	if tableCollation == "" {
		tableCollation = defaultCollation
	}

	for _, col := range c.CreateTable.TableSpec.Columns {
		if col.Type.Options == nil {
			col.Type.Options = &sqlparser.ColumnTypeOptions{}
		}

		// Map known lowercase fields to always be lowercase
		col.Type.Type = strings.ToLower(col.Type.Type)
		col.Type.Charset.Name = strings.ToLower(col.Type.Charset.Name)
		col.Type.Options.Collate = strings.ToLower(col.Type.Options.Collate)

		// See https://dev.mysql.com/doc/refman/8.0/en/create-table.html
		// If neither NULL nor NOT NULL is specified, the column is treated as though NULL had been specified.
		// That documentation though is not 100% true. There's an exception, and that is
		// the `explicit_defaults_for_timestamp` flag. When that is disabled (the default on 5.7),
		// a timestamp defaults to `NOT NULL`.
		//
		// We opt here to instead remove that difference and always then add `NULL` and treat
		// `explicit_defaults_for_timestamp` as always enabled in the context of DDL for diffing.
		if col.Type.Type == "timestamp" {
			if col.Type.Options.Null == nil || *col.Type.Options.Null {
				timestampNull := true
				col.Type.Options.Null = &timestampNull
			}
		} else {
			if col.Type.Options.Null != nil && *col.Type.Options.Null {
				col.Type.Options.Null = nil
			}
		}
		if col.Type.Options.Null == nil || *col.Type.Options.Null {
			// If `DEFAULT NULL` is specified and the column allows NULL,
			// we drop that in the normalized form since that is equivalent to the default value.
			// See also https://dev.mysql.com/doc/refman/8.0/en/data-type-defaults.html
			if _, ok := col.Type.Options.Default.(*sqlparser.NullVal); ok {
				col.Type.Options.Default = nil
			}
		}

		if col.Type.Options.Invisible != nil && !*col.Type.Options.Invisible {
			// If a column is marked `VISIBLE`, that's the same as the default.
			col.Type.Options.Invisible = nil
		}

		// Map any charset aliases to the real charset. This applies mainly right
		// now to utf8 being an alias for utf8mb3.
		if charset, ok := collationEnv.CharsetAlias(col.Type.Charset.Name); ok {
			col.Type.Charset.Name = charset
		}

		// Map any collation aliases to the real collation. This applies mainly right
		// now to utf8 being an alias for utf8mb3 collations.
		if collation, ok := collationEnv.CollationAlias(col.Type.Options.Collate); ok {
			col.Type.Options.Collate = collation
		}

		// Remove any lengths for integral types since it is deprecated there and
		// doesn't mean anything anymore.
		if _, ok := integralTypes[col.Type.Type]; ok {
			// We can remove the length except when we have a boolean, which is
			// stored as a tinyint(1) and treated special.
			if !isBool(col.Type) {
				col.Type.Length = nil
			}
		}

		if _, ok := charsetTypes[col.Type.Type]; ok {
			// If the charset is explicitly configured and it mismatches, we don't normalize
			// anything for charsets or collations and move on.
			if col.Type.Charset.Name != "" && col.Type.Charset.Name != tableCharset {
				continue
			}

			// Alright, first check if both charset and collation are the same as
			// the table level options, in that case we can remove both since that's equivalent.
			if col.Type.Charset.Name == tableCharset && col.Type.Options.Collate == tableCollation {
				col.Type.Charset.Name = ""
				col.Type.Options.Collate = ""
			}
			// If we have no charset or collation defined, we inherit the table defaults
			// and don't need to do anything here and can continue to the next column.
			// It doesn't matter if that's because it's not defined, or if it was because
			// it was explicitly set to the same values.
			if col.Type.Charset.Name == "" && col.Type.Options.Collate == "" {
				continue
			}

			// We have a matching charset as the default, but it is explicitly set. In that
			// case we still want to clear it, but set the default collation for the given charset
			// if no collation is defined yet. We set then the collation to the default collation.
			if col.Type.Charset.Name != "" {
				col.Type.Charset.Name = ""
				if col.Type.Options.Collate == "" {
					col.Type.Options.Collate = defaultCollation
				}
			}

			// We now have one case left, which is when we have set a collation but it's the same
			// as the table level. In that case, we can clear it since that is equivalent.
			if col.Type.Options.Collate == tableCollation {
				col.Type.Options.Collate = ""
			}
		}
	}
}

func (c *CreateTableEntity) normalizeIndexOptions() {
	for _, idx := range c.CreateTable.TableSpec.Indexes {
		// This name is taking straight from the input string
		// so we want to normalize this to always lowercase.
		idx.Info.Type = strings.ToLower(idx.Info.Type)
		for _, opt := range idx.Options {
			opt.Name = strings.ToLower(opt.Name)
		}
	}
}

func isBool(colType sqlparser.ColumnType) bool {
	return colType.Type == sqlparser.KeywordString(sqlparser.TINYINT) && colType.Length != nil && sqlparser.CanonicalString(colType.Length) == "1"
}

func (c *CreateTableEntity) normalizePartitionOptions() {
	if c.CreateTable.TableSpec.PartitionOption == nil {
		return
	}

	for _, def := range c.CreateTable.TableSpec.PartitionOption.Definitions {
		if def.Options == nil || def.Options.Engine == nil {
			continue
		}

		def.Options.Engine.Name = strings.ToUpper(def.Options.Engine.Name)
		if engineName, ok := engineCasing[def.Options.Engine.Name]; ok {
			def.Options.Engine.Name = engineName
		}
	}
}

func (c *CreateTableEntity) normalizeKeys() {
	// let's ensure all keys have names
	keyNameExists := map[string]bool{}
	// first, we iterate and take note for all keys that do already have names
	for _, key := range c.CreateTable.TableSpec.Indexes {
		if name := key.Info.Name.Lowered(); name != "" {
			keyNameExists[name] = true
		}
	}
	for _, key := range c.CreateTable.TableSpec.Indexes {
		// Normalize to KEY which matches MySQL behavior for the type.
		if key.Info.Type == sqlparser.KeywordString(sqlparser.INDEX) {
			key.Info.Type = sqlparser.KeywordString(sqlparser.KEY)
		}
		// now, let's look at keys that do not have names, and assign them new names
		if name := key.Info.Name.String(); name == "" {
			// we know there must be at least one column covered by this key
			var colName string
			if len(key.Columns) > 0 {
				expressionFound := false
				for _, col := range key.Columns {
					if col.Expression != nil {
						expressionFound = true
					}
				}
				if expressionFound {
					// that's the name MySQL picks for an unnamed key when there's at least one functional index expression
					colName = "functional_index"
				} else {
					// like MySQL, we first try to call our index by the name of the first column:
					colName = key.Columns[0].Column.String()
				}
			}
			suggestedKeyName := colName
			// now let's see if that name is taken; if it is, enumerate new news until we find a free name
			for enumerate := 2; keyNameExists[strings.ToLower(suggestedKeyName)]; enumerate++ {
				suggestedKeyName = fmt.Sprintf("%s_%d", colName, enumerate)
			}
			// OK we found a free slot!
			key.Info.Name = sqlparser.NewIdentifierCI(suggestedKeyName)
			keyNameExists[strings.ToLower(suggestedKeyName)] = true
		}

		// Drop options that are the same as the default.
		keptOptions := make([]*sqlparser.IndexOption, 0, len(key.Options))
		for _, option := range key.Options {
			switch strings.ToUpper(option.Name) {
			case "USING":
				if strings.EqualFold(option.String, "BTREE") {
					continue
				}
			case "VISIBLE":
				continue
			}
			keptOptions = append(keptOptions, option)
		}
		key.Options = keptOptions
	}
}

func (c *CreateTableEntity) normalizeUnnamedConstraints() {
	// let's ensure all keys have names
	constraintNameExists := map[string]bool{}
	// first, we iterate and take note for all keys that do already have names
	for _, constraint := range c.CreateTable.TableSpec.Constraints {
		if name := constraint.Name.Lowered(); name != "" {
			constraintNameExists[name] = true
		}
	}

	// now, let's look at keys that do not have names, and assign them new names
	for _, constraint := range c.CreateTable.TableSpec.Constraints {
		if name := constraint.Name.String(); name == "" {
			nameFormat := "%s_chk_%d"
			if _, fk := constraint.Details.(*sqlparser.ForeignKeyDefinition); fk {
				nameFormat = "%s_ibfk_%d"
			}
			suggestedCheckName := fmt.Sprintf(nameFormat, c.CreateTable.Table.Name.String(), 1)
			// now let's see if that name is taken; if it is, enumerate new news until we find a free name
			for enumerate := 2; constraintNameExists[strings.ToLower(suggestedCheckName)]; enumerate++ {
				suggestedCheckName = fmt.Sprintf(nameFormat, c.CreateTable.Table.Name.String(), enumerate)
			}
			// OK we found a free slot!
			constraint.Name = sqlparser.NewIdentifierCI(suggestedCheckName)
			constraintNameExists[strings.ToLower(suggestedCheckName)] = true
		}
	}
}

// Name implements Entity interface
func (c *CreateTableEntity) Name() string {
	return c.CreateTable.GetTable().Name.String()
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

	d, err := c.TableDiff(otherCreateTable, hints)
	if err != nil {
		return nil, err
	}
	return d, nil
}

// TableDiff compares this table statement with another table statement, and sees what it takes to
// change this table to look like the other table.
// It returns an AlterTable statement if changes are found, or nil if not.
// the other table may be of different name; its name is ignored.
func (c *CreateTableEntity) TableDiff(other *CreateTableEntity, hints *DiffHints) (*AlterTableEntityDiff, error) {
	if !c.CreateTable.IsFullyParsed() {
		return nil, &NotFullyParsedError{Entity: c.Name(), Statement: sqlparser.CanonicalString(c.CreateTable)}
	}
	if !other.CreateTable.IsFullyParsed() {
		return nil, &NotFullyParsedError{Entity: other.Name(), Statement: sqlparser.CanonicalString(other.CreateTable)}
	}

	if c.identicalOtherThanName(other) {
		return nil, nil
	}

	alterTable := &sqlparser.AlterTable{
		Table: c.CreateTable.Table,
	}
	diffedTableCharset := ""
	var parentAlterTableEntityDiff *AlterTableEntityDiff
	var partitionSpecs []*sqlparser.PartitionSpec
	var superfluousFulltextKeys []*sqlparser.AddIndexDefinition
	{
		t1Options := c.CreateTable.TableSpec.Options
		t2Options := other.CreateTable.TableSpec.Options
		diffedTableCharset = c.diffTableCharset(t1Options, t2Options)
	}
	{
		// diff columns
		// ordered columns for both tables:
		t1Columns := c.CreateTable.TableSpec.Columns
		t2Columns := other.CreateTable.TableSpec.Columns
		c.diffColumns(alterTable, t1Columns, t2Columns, hints, diffedTableCharset != "")
	}
	{
		// diff keys
		// ordered keys for both tables:
		t1Keys := c.CreateTable.TableSpec.Indexes
		t2Keys := other.CreateTable.TableSpec.Indexes
		superfluousFulltextKeys = c.diffKeys(alterTable, t1Keys, t2Keys, hints)
	}
	{
		// diff constraints
		// ordered constraints for both tables:
		t1Constraints := c.CreateTable.TableSpec.Constraints
		t2Constraints := other.CreateTable.TableSpec.Constraints
		c.diffConstraints(alterTable, t1Constraints, t2Constraints, hints)
	}
	{
		// diff partitions
		// ordered keys for both tables:
		t1Partitions := c.CreateTable.TableSpec.PartitionOption
		t2Partitions := other.CreateTable.TableSpec.PartitionOption
		var err error
		partitionSpecs, err = c.diffPartitions(alterTable, t1Partitions, t2Partitions, hints)
		if err != nil {
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
	tableSpecHasChanged := len(alterTable.AlterOptions) > 0 || alterTable.PartitionOption != nil || alterTable.PartitionSpec != nil
	if tableSpecHasChanged {
		parentAlterTableEntityDiff = &AlterTableEntityDiff{alterTable: alterTable, from: c, to: other}
	}
	for _, superfluousFulltextKey := range superfluousFulltextKeys {
		alterTable := &sqlparser.AlterTable{
			Table:        c.CreateTable.Table,
			AlterOptions: []sqlparser.AlterOption{superfluousFulltextKey},
		}
		diff := &AlterTableEntityDiff{alterTable: alterTable, from: c, to: other}
		// if we got superfluous fulltext keys, that means the table spec has changed, ie
		// parentAlterTableEntityDiff is not nil
		parentAlterTableEntityDiff.addSubsequentDiff(diff)
	}
	for _, partitionSpec := range partitionSpecs {
		alterTable := &sqlparser.AlterTable{
			Table:         c.CreateTable.Table,
			PartitionSpec: partitionSpec,
		}
		diff := &AlterTableEntityDiff{alterTable: alterTable, from: c, to: other}
		if parentAlterTableEntityDiff == nil {
			parentAlterTableEntityDiff = diff
		} else {
			parentAlterTableEntityDiff.addSubsequentDiff(diff)
		}
	}
	return parentAlterTableEntityDiff, nil
}

func (c *CreateTableEntity) diffTableCharset(
	t1Options sqlparser.TableOptions,
	t2Options sqlparser.TableOptions,
) string {
	getcharset := func(options sqlparser.TableOptions) string {
		for _, option := range options {
			if strings.EqualFold(option.Name, "CHARSET") {
				return option.String
			}
		}
		return ""
	}
	t1Charset := getcharset(t1Options)
	t2Charset := getcharset(t2Options)
	if t1Charset != t2Charset {
		return t2Charset
	}
	return ""
}

// isDefaultTableOptionValue sees if the value for a TableOption is also its default value
func isDefaultTableOptionValue(option *sqlparser.TableOption) bool {
	var value string
	if option.Value != nil {
		value = sqlparser.CanonicalString(option.Value)
	}
	switch strings.ToUpper(option.Name) {
	case "CHECKSUM":
		return value == "0"
	case "COMMENT":
		return option.String == ""
	case "COMPRESSION":
		return value == "" || value == "''"
	case "CONNECTION":
		return value == "" || value == "''"
	case "DATA DIRECTORY":
		return value == "" || value == "''"
	case "DELAY_KEY_WRITE":
		return value == "0"
	case "ENCRYPTION":
		return value == "N"
	case "INDEX DIRECTORY":
		return value == "" || value == "''"
	case "KEY_BLOCK_SIZE":
		return value == "0"
	case "MAX_ROWS":
		return value == "0"
	case "MIN_ROWS":
		return value == "0"
	case "PACK_KEYS":
		return strings.EqualFold(option.String, "DEFAULT")
	case "ROW_FORMAT":
		return strings.EqualFold(option.String, "DEFAULT")
	case "STATS_AUTO_RECALC":
		return strings.EqualFold(option.String, "DEFAULT")
	case "STATS_PERSISTENT":
		return strings.EqualFold(option.String, "DEFAULT")
	case "STATS_SAMPLE_PAGES":
		return strings.EqualFold(option.String, "DEFAULT")
	default:
		return false
	}
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
			case "AUTOEXTEND_SIZE":
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
			case "ENGINE_ATTRIBUTE":
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
			case "SECONDARY_ENGINE_ATTRIBUTE":
				// unused option. skip
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
				return &UnsupportedTableOptionError{Table: c.Name(), Option: strings.ToUpper(t1Option.Name)}
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
			if !sqlparser.EqualsTableOptions(options1, options2) {
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

// rangePartitionsAddedRemoved returns true when:
// - both table partitions are RANGE type
// - there is exactly one consequitive non-empty shared sequence of partitions (same names, same range values, in same order)
// - table1 may have non-empty list of partitions _preceding_ this sequence, and table2 may not
// - table2 may have non-empty list of partitions _following_ this sequence, and table1 may not
func (c *CreateTableEntity) isRangePartitionsRotation(
	t1Partitions *sqlparser.PartitionOption,
	t2Partitions *sqlparser.PartitionOption,
) (bool, []*sqlparser.PartitionSpec, error) {
	// Validate that both tables have range partitioning
	if t1Partitions.Type != t2Partitions.Type {
		return false, nil, nil
	}
	if t1Partitions.Type != sqlparser.RangeType {
		return false, nil, nil
	}
	definitions1 := t1Partitions.Definitions
	definitions2 := t2Partitions.Definitions
	// there has to be a non-empty shared list, therefore both definitions must be non-empty:
	if len(definitions1) == 0 {
		return false, nil, nil
	}
	if len(definitions2) == 0 {
		return false, nil, nil
	}
	var droppedPartitions1 []*sqlparser.PartitionDefinition
	// It's OK for prefix of t1 partitions to be nonexistent in t2 (as they may have been rotated away in t2)
	for len(definitions1) > 0 && !sqlparser.EqualsRefOfPartitionDefinition(definitions1[0], definitions2[0]) {
		droppedPartitions1 = append(droppedPartitions1, definitions1[0])
		definitions1 = definitions1[1:]
	}
	if len(definitions1) == 0 {
		// We've exhaused definition1 trying to find a shared partition with definitions2. Nothing found.
		// so there is no shared sequence between the two tables.
		return false, nil, nil
	}
	if len(definitions1) > len(definitions2) {
		return false, nil, nil
	}
	// To save computation, and because we've already shown that sqlparser.EqualsRefOfPartitionDefinition(definitions1[0], definitions2[0]),
	// we can skip one element
	definitions1 = definitions1[1:]
	definitions2 = definitions2[1:]
	// Now let's ensure that whatever is remaining in definitions1 is an exact match for a prefix of definitions2
	// It's ok if we end up with leftover elements in definition2
	for len(definitions1) > 0 {
		if !sqlparser.EqualsRefOfPartitionDefinition(definitions1[0], definitions2[0]) {
			return false, nil, nil
		}
		definitions1 = definitions1[1:]
		definitions2 = definitions2[1:]
	}
	addedPartitions2 := definitions2
	partitionSpecs := make([]*sqlparser.PartitionSpec, 0, len(droppedPartitions1)+len(addedPartitions2))
	for _, p := range droppedPartitions1 {
		partitionSpec := &sqlparser.PartitionSpec{
			Action: sqlparser.DropAction,
			Names:  []sqlparser.IdentifierCI{p.Name},
		}
		partitionSpecs = append(partitionSpecs, partitionSpec)
	}
	for _, p := range addedPartitions2 {
		partitionSpec := &sqlparser.PartitionSpec{
			Action:      sqlparser.AddAction,
			Definitions: []*sqlparser.PartitionDefinition{p},
		}
		partitionSpecs = append(partitionSpecs, partitionSpec)
	}
	return true, partitionSpecs, nil
}

func (c *CreateTableEntity) diffPartitions(alterTable *sqlparser.AlterTable,
	t1Partitions *sqlparser.PartitionOption,
	t2Partitions *sqlparser.PartitionOption,
	hints *DiffHints,
) (partitionSpecs []*sqlparser.PartitionSpec, err error) {
	switch {
	case t1Partitions == nil && t2Partitions == nil:
		return nil, nil
	case t1Partitions == nil:
		// add partitioning
		alterTable.PartitionOption = t2Partitions
	case t2Partitions == nil:
		// remove partitioning
		partitionSpec := &sqlparser.PartitionSpec{
			Action: sqlparser.RemoveAction,
			IsAll:  true,
		}
		alterTable.PartitionSpec = partitionSpec
	case sqlparser.EqualsRefOfPartitionOption(t1Partitions, t2Partitions):
		// identical partitioning
		return nil, nil
	default:
		// partitioning was changed
		// For most cases, we produce a complete re-partitioing schema: we don't try and figure out the minimal
		// needed change. For example, maybe the minimal change is to REORGANIZE a specific partition and split
		// into two, thus unaffecting the rest of the partitions. But we don't evaluate that, we just set a
		// complete new ALTER TABLE ... PARTITION BY statement.
		// The idea is that it doesn't matter: we're not looking to do optimal in-place ALTERs, we run
		// Online DDL alters, where we create a new table anyway. Thus, the optimization is meaningless.

		// Having said that, we _do_ analyze the scenario of a RANGE partitioning rotation of partitions:
		// where zero or more partitions may have been dropped from the earlier range, and zero or more
		// partitions have been added with a later range:
		isRotation, partitionSpecs, err := c.isRangePartitionsRotation(t1Partitions, t2Partitions)
		if err != nil {
			return nil, err
		}
		if isRotation {
			switch hints.RangeRotationStrategy {
			case RangeRotationIgnore:
				return nil, nil
			case RangeRotationDistinctStatements:
				return partitionSpecs, nil
			case RangeRotationFullSpec:
				// proceed to return a full rebuild
			}
		}
		alterTable.PartitionOption = t2Partitions
	}
	return nil, nil
}

func (c *CreateTableEntity) diffConstraints(alterTable *sqlparser.AlterTable,
	t1Constraints []*sqlparser.ConstraintDefinition,
	t2Constraints []*sqlparser.ConstraintDefinition,
	hints *DiffHints,
) {
	normalizeConstraintName := func(constraint *sqlparser.ConstraintDefinition) string {
		switch hints.ConstraintNamesStrategy {
		case ConstraintNamesIgnoreVitess:
			return ExtractConstraintOriginalName(constraint.Name.String())
		case ConstraintNamesIgnoreAll:
			return sqlparser.CanonicalString(constraint.Details)
		case ConstraintNamesStrict:
			return constraint.Name.String()
		default:
			// should never get here; but while here, let's assume strict.
			return constraint.Name.String()
		}
	}
	t1ConstraintsMap := map[string]*sqlparser.ConstraintDefinition{}
	t2ConstraintsMap := map[string]*sqlparser.ConstraintDefinition{}
	for _, constraint := range t1Constraints {
		t1ConstraintsMap[normalizeConstraintName(constraint)] = constraint
	}
	for _, constraint := range t2Constraints {
		t2ConstraintsMap[normalizeConstraintName(constraint)] = constraint
	}

	dropConstraintStatement := func(constraint *sqlparser.ConstraintDefinition) *sqlparser.DropKey {
		if _, fk := constraint.Details.(*sqlparser.ForeignKeyDefinition); fk {
			return &sqlparser.DropKey{Name: constraint.Name, Type: sqlparser.ForeignKeyType}
		}
		return &sqlparser.DropKey{Name: constraint.Name, Type: sqlparser.CheckKeyType}
	}

	// evaluate dropped constraints
	//
	for _, t1Constraint := range t1Constraints {
		if _, ok := t2ConstraintsMap[normalizeConstraintName(t1Constraint)]; !ok {
			// constraint exists in t1 but not in t2, hence it is dropped
			dropConstraint := dropConstraintStatement(t1Constraint)
			alterTable.AlterOptions = append(alterTable.AlterOptions, dropConstraint)
		}
	}

	for _, t2Constraint := range t2Constraints {
		normalizedT2ConstraintName := normalizeConstraintName(t2Constraint)
		// evaluate modified & added constraints:
		//
		if t1Constraint, ok := t1ConstraintsMap[normalizedT2ConstraintName]; ok {
			// constraint exists in both tables
			// check diff between before/after columns:
			if !sqlparser.EqualsConstraintInfo(t2Constraint.Details, t1Constraint.Details) {
				// constraints with same name have different definition.
				// First we check if this is only the enforced setting that changed which can
				// be directly altered.
				check1Details, ok1 := t1Constraint.Details.(*sqlparser.CheckConstraintDefinition)
				check2Details, ok2 := t2Constraint.Details.(*sqlparser.CheckConstraintDefinition)
				if ok1 && ok2 && sqlparser.EqualsExpr(check1Details.Expr, check2Details.Expr) {
					// We have the same expression, so we have a different Enforced here
					alterConstraint := &sqlparser.AlterCheck{
						Name:     t2Constraint.Name,
						Enforced: check2Details.Enforced,
					}
					alterTable.AlterOptions = append(alterTable.AlterOptions, alterConstraint)
					continue
				}

				// There's another change, so we need to drop and add.
				dropConstraint := dropConstraintStatement(t1Constraint)
				addConstraint := &sqlparser.AddConstraintDefinition{
					ConstraintDefinition: t2Constraint,
				}
				alterTable.AlterOptions = append(alterTable.AlterOptions, dropConstraint)
				alterTable.AlterOptions = append(alterTable.AlterOptions, addConstraint)
			}
		} else {
			// constraint exists in t2 but not in t1, hence it is added
			addConstraint := &sqlparser.AddConstraintDefinition{
				ConstraintDefinition: t2Constraint,
			}
			alterTable.AlterOptions = append(alterTable.AlterOptions, addConstraint)
		}
	}
}

func (c *CreateTableEntity) diffKeys(alterTable *sqlparser.AlterTable,
	t1Keys []*sqlparser.IndexDefinition,
	t2Keys []*sqlparser.IndexDefinition,
	hints *DiffHints,
) (superfluousFulltextKeys []*sqlparser.AddIndexDefinition) {
	t1KeysMap := map[string]*sqlparser.IndexDefinition{}
	t2KeysMap := map[string]*sqlparser.IndexDefinition{}
	for _, key := range t1Keys {
		t1KeysMap[key.Info.Name.String()] = key
	}
	for _, key := range t2Keys {
		t2KeysMap[key.Info.Name.String()] = key
	}

	dropKeyStatement := func(info *sqlparser.IndexInfo) *sqlparser.DropKey {
		dropKey := &sqlparser.DropKey{}
		if strings.EqualFold(info.Type, sqlparser.PrimaryKeyTypeStr) {
			dropKey.Type = sqlparser.PrimaryKeyType
		} else {
			dropKey.Type = sqlparser.NormalKeyType
			dropKey.Name = info.Name
		}
		return dropKey
	}

	// evaluate dropped keys
	//
	for _, t1Key := range t1Keys {
		if _, ok := t2KeysMap[t1Key.Info.Name.String()]; !ok {
			// column exists in t1 but not in t2, hence it is dropped
			dropKey := dropKeyStatement(t1Key.Info)
			alterTable.AlterOptions = append(alterTable.AlterOptions, dropKey)
		}
	}

	addedFulltextKeys := 0
	for _, t2Key := range t2Keys {
		t2KeyName := t2Key.Info.Name.String()
		// evaluate modified & added keys:
		//
		if t1Key, ok := t1KeysMap[t2KeyName]; ok {
			// key exists in both tables
			// check diff between before/after columns:
			if !sqlparser.EqualsRefOfIndexDefinition(t2Key, t1Key) {
				indexVisibilityChange, newVisibility := indexOnlyVisibilityChange(t1Key, t2Key)
				if indexVisibilityChange {
					alterTable.AlterOptions = append(alterTable.AlterOptions, &sqlparser.AlterIndex{
						Name:      t2Key.Info.Name,
						Invisible: newVisibility,
					})
					continue
				}

				// For other changes, we're going to drop and create.
				dropKey := dropKeyStatement(t1Key.Info)
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
			addedAsSuperfluousStatement := false
			if t2Key.Info.Fulltext {
				if addedFulltextKeys > 0 && hints.FullTextKeyStrategy == FullTextKeyDistinctStatements {
					// Special case: MySQL does not support multiple ADD FULLTEXT KEY statements in a single ALTER
					superfluousFulltextKeys = append(superfluousFulltextKeys, addKey)
					addedAsSuperfluousStatement = true
				}
				addedFulltextKeys++
			}
			if !addedAsSuperfluousStatement {
				alterTable.AlterOptions = append(alterTable.AlterOptions, addKey)
			}
		}
	}
	return superfluousFulltextKeys
}

// indexOnlyVisibilityChange checks whether the change on an index is only
// a visibility change. In that case we can use `ALTER INDEX`.
// Returns if this is a visibility only change and if true, whether
// the new visibility is invisible or not.
func indexOnlyVisibilityChange(t1Key, t2Key *sqlparser.IndexDefinition) (bool, bool) {
	t1KeyCopy := sqlparser.CloneRefOfIndexDefinition(t1Key)
	t2KeyCopy := sqlparser.CloneRefOfIndexDefinition(t2Key)
	t1KeyKeptOptions := make([]*sqlparser.IndexOption, 0, len(t1KeyCopy.Options))
	t2KeyInvisible := false
	for _, opt := range t1KeyCopy.Options {
		if strings.EqualFold(opt.Name, "invisible") {
			continue
		}
		t1KeyKeptOptions = append(t1KeyKeptOptions, opt)
	}
	t1KeyCopy.Options = t1KeyKeptOptions
	t2KeyKeptOptions := make([]*sqlparser.IndexOption, 0, len(t2KeyCopy.Options))
	for _, opt := range t2KeyCopy.Options {
		if strings.EqualFold(opt.Name, "invisible") {
			t2KeyInvisible = true
			continue
		}
		t2KeyKeptOptions = append(t2KeyKeptOptions, opt)
	}
	t2KeyCopy.Options = t2KeyKeptOptions
	if sqlparser.EqualsRefOfIndexDefinition(t2KeyCopy, t1KeyCopy) {
		return true, t2KeyInvisible
	}
	return false, false
}

// evaluateColumnReordering produces a minimal reordering set of columns. To elaborate:
// The function receives two sets of columns. the two must be permutations of one another. Specifically,
// these are the columns shared between the from&to tables.
// The function uses longest-common-subsequence (lcs) algorithm to compute which columns should not be moved.
// any column not in the lcs need to be reordered.
// The function a map of column names that need to be reordered, and the index into which they are reordered.
func evaluateColumnReordering(t1SharedColumns, t2SharedColumns []*sqlparser.ColumnDefinition) map[string]int {
	minimalColumnReordering := map[string]int{}

	t1SharedColNames := make([]interface{}, 0, len(t1SharedColumns))
	for _, col := range t1SharedColumns {
		t1SharedColNames = append(t1SharedColNames, col.Name.Lowered())
	}
	t2SharedColNames := make([]interface{}, 0, len(t2SharedColumns))
	for _, col := range t2SharedColumns {
		t2SharedColNames = append(t2SharedColNames, col.Name.Lowered())
	}

	lcs := golcs.New(t1SharedColNames, t2SharedColNames)
	lcsNames := map[string]bool{}
	for _, v := range lcs.Values() {
		lcsNames[v.(string)] = true
	}
	for i, t2Col := range t2SharedColumns {
		t2ColName := t2Col.Name.Lowered()
		// see if this column is in the longest common subsequence. If so, no need to reorder it. If not, it must be reordered.
		if _, ok := lcsNames[t2ColName]; !ok {
			minimalColumnReordering[t2ColName] = i
		}
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
	tableCharsetChanged bool,
) {
	getColumnsMap := func(cols []*sqlparser.ColumnDefinition) map[string]*columnDetails {
		var prevCol *columnDetails
		m := map[string]*columnDetails{}
		for _, col := range cols {
			colDetails := &columnDetails{
				col:     col,
				prevCol: prevCol,
			}
			if prevCol != nil {
				prevCol.nextCol = colDetails
			}
			prevCol = colDetails
			m[col.Name.Lowered()] = colDetails
		}
		return m
	}
	// map columns by names for easy access
	t1ColumnsMap := getColumnsMap(t1Columns)
	t2ColumnsMap := getColumnsMap(t2Columns)

	// For purpose of column reordering detection, we maintain a list of
	// shared columns, by order of appearance in t1
	var t1SharedColumns []*sqlparser.ColumnDefinition

	var dropColumns []*sqlparser.DropColumn
	// evaluate dropped columns
	//
	for _, t1Col := range t1Columns {
		if _, ok := t2ColumnsMap[t1Col.Name.Lowered()]; ok {
			t1SharedColumns = append(t1SharedColumns, t1Col)
		} else {
			// column exists in t1 but not in t2, hence it is dropped
			dropColumn := &sqlparser.DropColumn{
				Name: getColName(&t1Col.Name),
			}
			dropColumns = append(dropColumns, dropColumn)
		}
	}

	// For purpose of column reordering detection, we maintain a list of
	// shared columns, by order of appearance in t2
	var t2SharedColumns []*sqlparser.ColumnDefinition
	for _, t2Col := range t2Columns {
		if _, ok := t1ColumnsMap[t2Col.Name.Lowered()]; ok {
			// column exists in both tables
			t2SharedColumns = append(t2SharedColumns, t2Col)
		}
	}

	// evaluate modified columns
	//
	var modifyColumns []*sqlparser.ModifyColumn
	columnReordering := evaluateColumnReordering(t1SharedColumns, t2SharedColumns)
	for _, t2Col := range t2SharedColumns {
		t2ColName := t2Col.Name.Lowered()
		// we know that column exists in both tables
		t1Col := t1ColumnsMap[t2ColName]
		t1ColEntity := NewColumnDefinitionEntity(t1Col.col)
		t2ColEntity := NewColumnDefinitionEntity(t2Col)

		// check diff between before/after columns:
		modifyColumnDiff := t1ColEntity.ColumnDiff(t2ColEntity, hints)
		if modifyColumnDiff == nil {
			// even if there's no apparent change, there can still be implicit changes
			// it is possible that the table charset is changed. the column may be some col1 TEXT NOT NULL, possibly in both varsions 1 and 2,
			// but implicitly the column has changed its characters set. So we need to explicitly ass a MODIFY COLUMN statement, so that
			// MySQL rebuilds it.
			if tableCharsetChanged && t2ColEntity.IsTextual() && t2Col.Type.Charset.Name == "" {
				modifyColumnDiff = NewModifyColumnDiffByDefinition(t2Col)
			}
		}
		// It is also possible that a column is reordered. Whether the column definition has
		// or hasn't changed, if a column is reordered then that's a change of its own!
		if columnReorderIndex, ok := columnReordering[t2ColName]; ok {
			// seems like we previously evaluated that this column should be reordered
			if modifyColumnDiff == nil {
				// create column change
				modifyColumnDiff = NewModifyColumnDiffByDefinition(t2Col)
			}
			if columnReorderIndex == 0 {
				modifyColumnDiff.modifyColumn.First = true
			} else {
				modifyColumnDiff.modifyColumn.After = getColName(&t2SharedColumns[columnReorderIndex-1].Name)
			}
		}
		if modifyColumnDiff != nil {
			// column definition or ordering has changed
			modifyColumns = append(modifyColumns, modifyColumnDiff.modifyColumn)
		}
	}
	// Evaluate added columns
	//
	// Every added column is obviously a diff. But on top of that, we are also interested to know
	// if the column is added somewhere in between existing columns rather than appended to the
	// end of existing columns list.
	var addColumns []*sqlparser.AddColumns
	expectAppendIndex := len(t2SharedColumns)
	for t2ColIndex, t2Col := range t2Columns {
		if _, ok := t1ColumnsMap[t2Col.Name.Lowered()]; !ok {
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
			addColumns = append(addColumns, addColumn)
		}
	}
	dropColumns, addColumns, renameColumns := heuristicallyDetectColumnRenames(dropColumns, addColumns, t1ColumnsMap, t2ColumnsMap, hints)
	for _, c := range dropColumns {
		alterTable.AlterOptions = append(alterTable.AlterOptions, c)
	}
	for _, c := range modifyColumns {
		alterTable.AlterOptions = append(alterTable.AlterOptions, c)
	}
	for _, c := range renameColumns {
		alterTable.AlterOptions = append(alterTable.AlterOptions, c)
	}
	for _, c := range addColumns {
		alterTable.AlterOptions = append(alterTable.AlterOptions, c)
	}
}

func heuristicallyDetectColumnRenames(
	dropColumns []*sqlparser.DropColumn,
	addColumns []*sqlparser.AddColumns,
	t1ColumnsMap map[string]*columnDetails,
	t2ColumnsMap map[string]*columnDetails,
	hints *DiffHints,
) ([]*sqlparser.DropColumn, []*sqlparser.AddColumns, []*sqlparser.RenameColumn) {
	var renameColumns []*sqlparser.RenameColumn
	findRenamedColumn := func() bool {
		// What we're doing next is to try and identify a column RENAME.
		// We do so by cross-referencing dropped and added columns.
		// The check is heuristic, and looks like this:
		// We consider a column renamed iff:
		// - the DROP and ADD column definitions are identical other than the column name, and
		// - the DROPped and ADDded column are both FIRST, or they come AFTER the same column, and
		// - the DROPped and ADDded column are both last, or they come before the same column
		// This v1 chcek therefore cannot handle a case where two successive columns are renamed.
		// the problem is complex, and with successive renamed, or drops and adds, it can be
		// impossible to tell apart different scenarios.
		// At any case, once we heuristically decide that we found a RENAME, we cancel the DROP,
		// cancel the ADD, and inject a RENAME in place of both.

		// findRenamedColumn cross-references dropped and added columns to find a single renamed column. If such is found:
		// we remove the entry from DROPped columns, remove the entry from ADDed columns, add an entry for RENAMEd columns,
		// and return 'true'.
		// Successive calls to this function will then find the next heuristic RENAMEs.
		// the function returns 'false' if it is unable to heuristically find a RENAME.
		for iDrop, dropCol1 := range dropColumns {
			for iAdd, addCol2 := range addColumns {
				col1Details := t1ColumnsMap[dropCol1.Name.Name.Lowered()]
				if !col1Details.identicalOtherThanName(addCol2.Columns[0]) {
					continue
				}
				// columns look alike, other than their names, which we know are different.
				// are these two columns otherwise appear to be in same position?
				col2Details := t2ColumnsMap[addCol2.Columns[0].Name.Lowered()]
				if col1Details.prevColName() == col2Details.prevColName() && col1Details.nextColName() == col2Details.nextColName() {
					dropColumns = append(dropColumns[0:iDrop], dropColumns[iDrop+1:]...)
					addColumns = append(addColumns[0:iAdd], addColumns[iAdd+1:]...)
					renameColumn := &sqlparser.RenameColumn{
						OldName: dropCol1.Name,
						NewName: getColName(&addCol2.Columns[0].Name),
					}
					renameColumns = append(renameColumns, renameColumn)
					return true
				}
			}
		}
		return false
	}
	switch hints.ColumnRenameStrategy {
	case ColumnRenameAssumeDifferent:
		// do nothing
	case ColumnRenameHeuristicStatement:
		for findRenamedColumn() {
			// Iteratively detect all RENAMEs
		}
	}
	return dropColumns, addColumns, renameColumns
}

// Create implements Entity interface
func (c *CreateTableEntity) Create() EntityDiff {
	return &CreateTableEntityDiff{to: c, createTable: c.CreateTable}
}

// Drop implements Entity interface
func (c *CreateTableEntity) Drop() EntityDiff {
	dropTable := &sqlparser.DropTable{
		FromTables: []sqlparser.TableName{c.Table},
	}
	return &DropTableEntityDiff{from: c, dropTable: dropTable}
}

func sortAlterOptions(diff *AlterTableEntityDiff) {
	optionOrder := func(opt sqlparser.AlterOption) int {
		switch opt.(type) {
		case *sqlparser.DropKey:
			return 1
		case *sqlparser.DropColumn:
			return 2
		case *sqlparser.ModifyColumn:
			return 3
		case *sqlparser.RenameColumn:
			return 4
		case *sqlparser.AddColumns:
			return 5
		case *sqlparser.AddIndexDefinition:
			return 6
		case *sqlparser.AddConstraintDefinition:
			return 7
		case sqlparser.TableOptions, *sqlparser.TableOptions:
			return 8
		default:
			return math.MaxInt
		}
	}
	opts := diff.alterTable.AlterOptions
	sort.SliceStable(opts, func(i, j int) bool {
		return optionOrder(opts[i]) < optionOrder(opts[j])
	})
}

// apply attempts to apply an ALTER TABLE diff onto this entity's table definition.
// supported modifications are only those created by schemadiff's Diff() function.
func (c *CreateTableEntity) apply(diff *AlterTableEntityDiff) error {
	sortAlterOptions(diff)
	// Apply partitioning changes:
	if spec := diff.alterTable.PartitionSpec; spec != nil {
		switch {
		case spec.Action == sqlparser.RemoveAction && spec.IsAll:
			// Remove partitioning
			c.TableSpec.PartitionOption = nil
		case spec.Action == sqlparser.DropAction && len(spec.Names) > 0:
			for _, dropPartitionName := range spec.Names {
				// Drop partitions
				if c.TableSpec.PartitionOption == nil {
					return &ApplyPartitionNotFoundError{Table: c.Name(), Partition: dropPartitionName.String()}
				}
				partitionFound := false
				for i, p := range c.TableSpec.PartitionOption.Definitions {
					if strings.EqualFold(p.Name.String(), dropPartitionName.String()) {
						c.TableSpec.PartitionOption.Definitions = append(
							c.TableSpec.PartitionOption.Definitions[0:i],
							c.TableSpec.PartitionOption.Definitions[i+1:]...,
						)
						partitionFound = true
						break
					}
				}
				if !partitionFound {
					return &ApplyPartitionNotFoundError{Table: c.Name(), Partition: dropPartitionName.String()}
				}
			}
		case spec.Action == sqlparser.AddAction && len(spec.Definitions) == 1:
			// Add one partition
			if c.TableSpec.PartitionOption == nil {
				return &ApplyNoPartitionsError{Table: c.Name()}
			}
			if len(c.TableSpec.PartitionOption.Definitions) == 0 {
				return &ApplyNoPartitionsError{Table: c.Name()}
			}
			for _, p := range c.TableSpec.PartitionOption.Definitions {
				if strings.EqualFold(p.Name.String(), spec.Definitions[0].Name.String()) {
					return &ApplyDuplicatePartitionError{Table: c.Name(), Partition: spec.Definitions[0].Name.String()}
				}
			}
			c.TableSpec.PartitionOption.Definitions = append(
				c.TableSpec.PartitionOption.Definitions,
				spec.Definitions[0],
			)
		default:
			return &UnsupportedApplyOperationError{Statement: sqlparser.CanonicalString(spec)}
		}
	}
	if diff.alterTable.PartitionOption != nil {
		// Specify new spec:
		c.CreateTable.TableSpec.PartitionOption = diff.alterTable.PartitionOption
	}
	// reorderColumn attempts to reorder column that is right now in position 'colIndex',
	// based on its FIRST or AFTER specs (if any)
	reorderColumn := func(colIndex int, first bool, after *sqlparser.ColName) error {
		var newCols []*sqlparser.ColumnDefinition // nil
		col := c.TableSpec.Columns[colIndex]
		switch {
		case first:
			newCols = append(newCols, col)
			newCols = append(newCols, c.TableSpec.Columns[0:colIndex]...)
			newCols = append(newCols, c.TableSpec.Columns[colIndex+1:]...)
		case after != nil:
			afterColFound := false
			// look for the AFTER column; it has to exist!
			for a, afterCol := range c.TableSpec.Columns {
				if strings.EqualFold(afterCol.Name.String(), after.Name.String()) {
					if colIndex < a {
						// moving column i to the right
						newCols = append(newCols, c.TableSpec.Columns[0:colIndex]...)
						newCols = append(newCols, c.TableSpec.Columns[colIndex+1:a+1]...)
						newCols = append(newCols, col)
						newCols = append(newCols, c.TableSpec.Columns[a+1:]...)
					} else {
						// moving column i to the left
						newCols = append(newCols, c.TableSpec.Columns[0:a+1]...)
						newCols = append(newCols, col)
						newCols = append(newCols, c.TableSpec.Columns[a+1:colIndex]...)
						newCols = append(newCols, c.TableSpec.Columns[colIndex+1:]...)
					}
					afterColFound = true
					break
				}
			}
			if !afterColFound {
				return &ApplyColumnAfterNotFoundError{Table: c.Name(), Column: col.Name.String(), AfterColumn: after.Name.String()}
			}
		default:
			// no change in position
		}

		if newCols != nil {
			c.TableSpec.Columns = newCols
		}
		return nil
	}

	columnExists := map[string]bool{}
	for _, col := range c.CreateTable.TableSpec.Columns {
		columnExists[col.Name.Lowered()] = true
	}

	// apply a single AlterOption; only supported types are those generated by Diff()
	applyAlterOption := func(opt sqlparser.AlterOption) error {
		switch opt := opt.(type) {
		case *sqlparser.DropKey:
			// applies to either indexes or FK constraints
			// we expect the named key to be found
			found := false
			switch opt.Type {
			case sqlparser.PrimaryKeyType:
				for i, idx := range c.TableSpec.Indexes {
					if strings.EqualFold(idx.Info.Type, sqlparser.PrimaryKeyTypeStr) {
						found = true
						c.TableSpec.Indexes = append(c.TableSpec.Indexes[0:i], c.TableSpec.Indexes[i+1:]...)
						break
					}
				}
			case sqlparser.NormalKeyType:
				for i, index := range c.TableSpec.Indexes {
					if strings.EqualFold(index.Info.Name.String(), opt.Name.String()) {
						found = true
						c.TableSpec.Indexes = append(c.TableSpec.Indexes[0:i], c.TableSpec.Indexes[i+1:]...)
						break
					}
				}
			case sqlparser.ForeignKeyType, sqlparser.CheckKeyType:
				for i, constraint := range c.TableSpec.Constraints {
					if strings.EqualFold(constraint.Name.String(), opt.Name.String()) {
						found = true
						c.TableSpec.Constraints = append(c.TableSpec.Constraints[0:i], c.TableSpec.Constraints[i+1:]...)
						break
					}
				}
			default:
				return &UnsupportedApplyOperationError{Statement: sqlparser.CanonicalString(opt)}
			}
			if !found {
				return &ApplyKeyNotFoundError{Table: c.Name(), Key: opt.Name.String()}
			}
		case *sqlparser.AddIndexDefinition:
			// validate no existing key by same name
			keyName := opt.IndexDefinition.Info.Name.String()
			for _, index := range c.TableSpec.Indexes {
				if strings.EqualFold(index.Info.Name.String(), keyName) {
					return &ApplyDuplicateKeyError{Table: c.Name(), Key: keyName}
				}
			}
			for colName := range getKeyColumnNames(opt.IndexDefinition) {
				if !columnExists[colName] {
					return &InvalidColumnInKeyError{Table: c.Name(), Column: colName, Key: keyName}
				}
			}
			c.TableSpec.Indexes = append(c.TableSpec.Indexes, opt.IndexDefinition)
		case *sqlparser.AddConstraintDefinition:
			// validate no existing constraint by same name
			for _, cs := range c.TableSpec.Constraints {
				if strings.EqualFold(cs.Name.String(), opt.ConstraintDefinition.Name.String()) {
					return &ApplyDuplicateConstraintError{Table: c.Name(), Constraint: cs.Name.String()}
				}
			}
			c.TableSpec.Constraints = append(c.TableSpec.Constraints, opt.ConstraintDefinition)
		case *sqlparser.AlterCheck:
			// we expect the constraint to exist
			found := false
			for _, constraint := range c.TableSpec.Constraints {
				checkDetails, ok := constraint.Details.(*sqlparser.CheckConstraintDefinition)
				if ok && strings.EqualFold(constraint.Name.String(), opt.Name.String()) {
					found = true
					checkDetails.Enforced = opt.Enforced
					break
				}
			}
			if !found {
				return &ApplyConstraintNotFoundError{Table: c.Name(), Constraint: opt.Name.String()}
			}
		case *sqlparser.DropColumn:
			// we expect the column to exist
			found := false
			for i, col := range c.TableSpec.Columns {
				if strings.EqualFold(col.Name.String(), opt.Name.Name.String()) {
					found = true
					c.TableSpec.Columns = append(c.TableSpec.Columns[0:i], c.TableSpec.Columns[i+1:]...)
					delete(columnExists, col.Name.Lowered())
					break
				}
			}
			if !found {
				return &ApplyColumnNotFoundError{Table: c.Name(), Column: opt.Name.Name.String()}
			}
		case *sqlparser.AddColumns:
			if len(opt.Columns) != 1 {
				// our Diff only ever generates a single column per AlterOption
				return &UnsupportedApplyOperationError{Statement: sqlparser.CanonicalString(opt)}
			}
			// validate no column by same name
			addedCol := opt.Columns[0]
			for _, col := range c.TableSpec.Columns {
				if strings.EqualFold(col.Name.String(), addedCol.Name.String()) {
					return &ApplyDuplicateColumnError{Table: c.Name(), Column: addedCol.Name.String()}
				}
			}
			c.TableSpec.Columns = append(c.TableSpec.Columns, addedCol)
			// see if we need to position it anywhere other than end of table
			if err := reorderColumn(len(c.TableSpec.Columns)-1, opt.First, opt.After); err != nil {
				return err
			}
			columnExists[addedCol.Name.Lowered()] = true
		case *sqlparser.ModifyColumn:
			// we expect the column to exist
			found := false
			for i, col := range c.TableSpec.Columns {
				if strings.EqualFold(col.Name.String(), opt.NewColDefinition.Name.String()) {
					found = true
					// redefine. see if we need to position it anywhere other than end of table
					c.TableSpec.Columns[i] = opt.NewColDefinition
					if err := reorderColumn(i, opt.First, opt.After); err != nil {
						return err
					}
					break
				}
			}
			if !found {
				return &ApplyColumnNotFoundError{Table: c.Name(), Column: opt.NewColDefinition.Name.String()}
			}
		case *sqlparser.RenameColumn:
			// we expect the column to exist
			found := false
			for i, col := range c.TableSpec.Columns {
				if strings.EqualFold(col.Name.String(), opt.OldName.Name.String()) {
					found = true
					// redefine. see if we need to position it anywhere other than end of table
					c.TableSpec.Columns[i].Name = opt.NewName.Name
					delete(columnExists, opt.OldName.Name.Lowered())
					columnExists[opt.NewName.Name.Lowered()] = true
					break
				}
			}
			if !found {
				return &ApplyColumnNotFoundError{Table: c.Name(), Column: opt.OldName.Name.String()}
			}
		case *sqlparser.AlterColumn:
			// we expect the column to exist
			found := false
			for _, col := range c.TableSpec.Columns {
				if strings.EqualFold(col.Name.String(), opt.Column.Name.String()) {
					found = true
					if opt.DropDefault {
						col.Type.Options.Default = nil
					} else if opt.DefaultVal != nil {
						col.Type.Options.Default = opt.DefaultVal
					}
					col.Type.Options.Invisible = opt.Invisible
					break
				}
			}
			if !found {
				return &ApplyColumnNotFoundError{Table: c.Name(), Column: opt.Column.Name.String()}
			}
		case *sqlparser.AlterIndex:
			// we expect the index to exist
			found := false
			for _, idx := range c.TableSpec.Indexes {
				if strings.EqualFold(idx.Info.Name.String(), opt.Name.String()) {
					found = true
					if opt.Invisible {
						idx.Options = append(idx.Options, &sqlparser.IndexOption{Name: "invisible"})
					} else {
						keptOptions := make([]*sqlparser.IndexOption, 0, len(idx.Options))
						for _, idxOpt := range idx.Options {
							if strings.EqualFold(idxOpt.Name, "invisible") {
								continue
							}
							keptOptions = append(keptOptions, idxOpt)
						}
						idx.Options = keptOptions
					}
					break
				}
			}
			if !found {
				return &ApplyKeyNotFoundError{Table: c.Name(), Key: opt.Name.String()}
			}
		case sqlparser.TableOptions:
			// Apply table options. Options that have their DEFAULT value are actually removed.
			for _, option := range opt {
				func() {
					for i, existingOption := range c.TableSpec.Options {
						if strings.EqualFold(option.Name, existingOption.Name) {
							if isDefaultTableOptionValue(option) {
								// remove the option
								c.TableSpec.Options = append(c.TableSpec.Options[0:i], c.TableSpec.Options[i+1:]...)
							} else {
								c.TableSpec.Options[i] = option
							}
							// option found. No need for further iteration.
							return
						}
					}
					// option not found. We add it
					c.TableSpec.Options = append(c.TableSpec.Options, option)
				}()
			}
		default:
			return &UnsupportedApplyOperationError{Statement: sqlparser.CanonicalString(opt)}
		}
		return nil
	}
	for _, alterOption := range diff.alterTable.AlterOptions {
		if err := applyAlterOption(alterOption); err != nil {
			return err
		}
	}
	if err := c.postApplyNormalize(); err != nil {
		return err
	}
	if err := c.validate(); err != nil {
		return err
	}
	return nil
}

// Apply attempts to apply given ALTER TABLE diff onto the table defined by this entity.
// This entity is unmodified. If successful, a new CREATE TABLE entity is returned.
func (c *CreateTableEntity) Apply(diff EntityDiff) (Entity, error) {
	dup := c.Clone().(*CreateTableEntity)
	for diff != nil {
		alterDiff, ok := diff.(*AlterTableEntityDiff)
		if !ok {
			return nil, ErrEntityTypeMismatch
		}
		if !diff.IsEmpty() {
			if err := dup.apply(alterDiff); err != nil {
				return nil, err
			}
		}
		diff = diff.SubsequentDiff()
	}
	// Always normalize after an Apply to get consistent AST structures.
	dup.normalize()
	return dup, nil
}

// postApplyNormalize runs at the end of apply() and to reorganize/edit things that
// a MySQL will do implicitly:
//   - edit or remove keys if referenced columns are dropped
//   - drop check constraints for a single specific column if that column
//     is the only referenced column in that check constraint.
func (c *CreateTableEntity) postApplyNormalize() error {
	// reduce or remove keys based on existing column list
	// (a column may have been removed)postApplyNormalize
	columnExists := map[string]bool{}
	for _, col := range c.CreateTable.TableSpec.Columns {
		columnExists[col.Name.Lowered()] = true
	}
	var nonEmptyIndexes []*sqlparser.IndexDefinition

	keyHasNonExistentColumns := func(keyCol *sqlparser.IndexColumn) bool {
		if keyCol.Column.Lowered() != "" {
			if !columnExists[keyCol.Column.Lowered()] {
				return true
			}
		}
		return false
	}
	for _, key := range c.CreateTable.TableSpec.Indexes {
		var existingKeyColumns []*sqlparser.IndexColumn
		for _, keyCol := range key.Columns {
			if !keyHasNonExistentColumns(keyCol) {
				existingKeyColumns = append(existingKeyColumns, keyCol)
			}
		}
		if len(existingKeyColumns) > 0 {
			key.Columns = existingKeyColumns
			nonEmptyIndexes = append(nonEmptyIndexes, key)
		}
	}
	c.CreateTable.TableSpec.Indexes = nonEmptyIndexes

	var keptConstraints []*sqlparser.ConstraintDefinition
	for _, constraint := range c.CreateTable.TableSpec.Constraints {
		check, ok := constraint.Details.(*sqlparser.CheckConstraintDefinition)
		if !ok {
			keptConstraints = append(keptConstraints, constraint)
			continue
		}
		var referencedColumns []string
		err := sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
			switch node := node.(type) {
			case *sqlparser.ColName:
				referencedColumns = append(referencedColumns, node.Name.String())
			}
			return true, nil
		}, check.Expr)
		if err != nil {
			return err
		}
		if len(referencedColumns) != 1 {
			keptConstraints = append(keptConstraints, constraint)
			continue
		}

		referencedColumn := referencedColumns[0]
		if columnExists[strings.ToLower(referencedColumn)] {
			keptConstraints = append(keptConstraints, constraint)
		}
	}
	c.CreateTable.TableSpec.Constraints = keptConstraints

	return nil
}

func getNodeColumns(node sqlparser.SQLNode) (colNames map[string]bool) {
	colNames = map[string]bool{}
	if node != nil {
		_ = sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
			switch node := node.(type) {
			case *sqlparser.ColName:
				colNames[node.Name.Lowered()] = true
			}
			return true, nil
		}, node)
	}
	return colNames
}

func getKeyColumnNames(key *sqlparser.IndexDefinition) (colNames map[string]bool) {
	colNames = map[string]bool{}
	for _, col := range key.Columns {
		if colName := col.Column.Lowered(); colName != "" {
			colNames[colName] = true
		}
		for name := range getNodeColumns(col.Expression) {
			colNames[name] = true
		}
	}
	return colNames
}

// validate checks that the table structure is valid:
// - all columns referenced by keys exist
func (c *CreateTableEntity) validate() error {
	columnExists := map[string]bool{}
	for _, col := range c.CreateTable.TableSpec.Columns {
		colName := col.Name.Lowered()
		if columnExists[colName] {
			return &ApplyDuplicateColumnError{Table: c.Name(), Column: col.Name.String()}
		}
		columnExists[colName] = true
	}
	// validate all columns referenced by indexes do in fact exist
	for _, key := range c.CreateTable.TableSpec.Indexes {
		for colName := range getKeyColumnNames(key) {
			if !columnExists[colName] {
				return &InvalidColumnInKeyError{Table: c.Name(), Column: colName, Key: key.Info.Name.String()}
			}
		}
	}
	// validate all columns referenced by generated columns do in fact exist
	for _, col := range c.CreateTable.TableSpec.Columns {
		if col.Type.Options != nil && col.Type.Options.As != nil {
			var referencedColumns []string
			err := sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
				switch node := node.(type) {
				case *sqlparser.ColName:
					referencedColumns = append(referencedColumns, node.Name.String())
				}
				return true, nil
			}, col.Type.Options.As)
			if err != nil {
				return err
			}
			for _, referencedColName := range referencedColumns {
				if !columnExists[strings.ToLower(referencedColName)] {
					return &InvalidColumnInGeneratedColumnError{Table: c.Name(), Column: referencedColName, GeneratedColumn: col.Name.String()}
				}
			}
		}
	}
	// validate all columns referenced by functional indexes do in fact exist
	for _, idx := range c.CreateTable.TableSpec.Indexes {
		for _, idxCol := range idx.Columns {
			var referencedColumns []string
			err := sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
				switch node := node.(type) {
				case *sqlparser.ColName:
					referencedColumns = append(referencedColumns, node.Name.String())
				}
				return true, nil
			}, idxCol.Expression)
			if err != nil {
				return err
			}
			for _, referencedColName := range referencedColumns {
				if !columnExists[strings.ToLower(referencedColName)] {
					return &InvalidColumnInKeyError{Table: c.Name(), Column: referencedColName, Key: idx.Info.Name.String()}
				}
			}
		}
	}
	// validate all columns referenced by foreign key constraints do in fact exist
	for _, cs := range c.CreateTable.TableSpec.Constraints {
		check, ok := cs.Details.(*sqlparser.ForeignKeyDefinition)
		if !ok {
			continue
		}
		for _, col := range check.Source {
			if !columnExists[col.Lowered()] {
				return &InvalidColumnInForeignKeyConstraintError{Table: c.Name(), Constraint: cs.Name.String(), Column: col.String()}
			}
		}
	}
	// validate all columns referenced by constraint checks do in fact exist
	for _, cs := range c.CreateTable.TableSpec.Constraints {
		check, ok := cs.Details.(*sqlparser.CheckConstraintDefinition)
		if !ok {
			continue
		}
		var referencedColumns []string
		err := sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
			switch node := node.(type) {
			case *sqlparser.ColName:
				referencedColumns = append(referencedColumns, node.Name.String())
			}
			return true, nil
		}, check.Expr)
		if err != nil {
			return err
		}
		for _, referencedColName := range referencedColumns {
			if !columnExists[strings.ToLower(referencedColName)] {
				return &InvalidColumnInCheckConstraintError{Table: c.Name(), Constraint: cs.Name.String(), Column: referencedColName}
			}
		}
	}

	if partition := c.CreateTable.TableSpec.PartitionOption; partition != nil {
		// validate no two partitions have same name
		partitionExists := map[string]bool{}
		for _, p := range partition.Definitions {
			if partitionExists[p.Name.Lowered()] {
				return &ApplyDuplicatePartitionError{Table: c.Name(), Partition: p.Name.String()}
			}
			partitionExists[p.Name.Lowered()] = true
		}
		// validate columns referenced by partitions do in fact exist
		// also, validate that all unique keys include partitioned columns
		var partitionColNames []string
		err := sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
			switch node := node.(type) {
			case *sqlparser.ColName:
				partitionColNames = append(partitionColNames, node.Name.String())
			}
			return true, nil
		}, partition.Expr)
		if err != nil {
			return err
		}

		for _, partitionColName := range partitionColNames {
			// Validate columns exists in table:
			if !columnExists[strings.ToLower(partitionColName)] {
				return &InvalidColumnInPartitionError{Table: c.Name(), Column: partitionColName}
			}

			// Validate all unique keys include this column:
			for _, key := range c.CreateTable.TableSpec.Indexes {
				if !key.Info.Unique {
					continue
				}
				colFound := false
				for _, col := range key.Columns {
					if strings.EqualFold(col.Column.String(), partitionColName) {
						colFound = true
						break
					}
				}
				if !colFound {
					return &MissingPartitionColumnInUniqueKeyError{Table: c.Name(), Column: partitionColName, UniqueKey: key.Info.Name.String()}
				}
			}
		}
	}
	return nil
}

// identicalOtherThanName returns true when this CREATE TABLE and the given one, are identical
// other than in table's name. We assume both have been normalized.
func (c *CreateTableEntity) identicalOtherThanName(other *CreateTableEntity) bool {
	if other == nil {
		return false
	}
	return sqlparser.EqualsRefOfTableSpec(c.TableSpec, other.TableSpec) &&
		sqlparser.EqualsRefOfParsedComments(c.Comments, other.Comments)
}
