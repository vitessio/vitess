package schemadiff

import (
	"strings"

	"vitess.io/vitess/go/mysql/capabilities"
	"vitess.io/vitess/go/vt/sqlparser"
)

const (
	maxColumnsForInstantAddColumn = 1022
)

// alterOptionAvailableViaInstantDDL checks if the specific alter option is eligible to run via ALGORITHM=INSTANT
// reference: https://dev.mysql.com/doc/refman/8.0/en/innodb-online-ddl-operations.html
func alterOptionCapableOfInstantDDL(alterOption sqlparser.AlterOption, createTable *sqlparser.CreateTable, capableOf capabilities.CapableOf) (bool, error) {
	// A table with FULLTEXT index won't support adding/removing columns instantly.
	tableHasFulltextIndex := false
	for _, key := range createTable.TableSpec.Indexes {
		if key.Info.Type == sqlparser.IndexTypeFullText {
			tableHasFulltextIndex = true
			break
		}
	}
	findColumn := func(colName string) *sqlparser.ColumnDefinition {
		if createTable == nil {
			return nil
		}
		for _, col := range createTable.TableSpec.Columns {
			if strings.EqualFold(colName, col.Name.String()) {
				return col
			}
		}
		return nil
	}
	findIndexCoveringColumn := func(colName string) *sqlparser.IndexDefinition {
		for _, index := range createTable.TableSpec.Indexes {
			for _, col := range index.Columns {
				if col.Column.String() == colName {
					return index
				}
			}
		}
		return nil
	}
	findTableOption := func(optName string) *sqlparser.TableOption {
		if createTable == nil {
			return nil
		}
		for _, opt := range createTable.TableSpec.Options {
			if strings.EqualFold(optName, opt.Name) {
				return opt
			}
		}
		return nil
	}
	tableIsCompressed := false
	if opt := findTableOption("ROW_FORMAT"); opt != nil {
		if strings.EqualFold(opt.String, "COMPRESSED") {
			tableIsCompressed = true
		}
	}

	colStringStrippedDown := func(col *sqlparser.ColumnDefinition, stripEnum bool) string {
		strippedCol := sqlparser.Clone(col)
		// strip `default`
		strippedCol.Type.Options.Default = nil
		strippedCol.Type.Options.DefaultLiteral = false
		// strip `visibility`
		strippedCol.Type.Options.Invisible = nil
		if stripEnum {
			strippedCol.Type.EnumValues = nil
		}
		return sqlparser.CanonicalString(strippedCol)
	}
	hasPrefix := func(vals []string, prefix []string) bool {
		if len(vals) < len(prefix) {
			return false
		}
		for i := range prefix {
			if vals[i] != prefix[i] {
				return false
			}
		}
		return true
	}
	changeModifyColumnCapableOfInstantDDL := func(col *sqlparser.ColumnDefinition, newCol *sqlparser.ColumnDefinition) (bool, error) {
		// Check if only diff is change of default.
		// We temporarily remove the DEFAULT expression (if any) from both
		// table and ALTER statement, and compare the columns: if they're otherwise equal,
		// then the only change can be an addition/change/removal of DEFAULT, which
		// is instant-table.
		tableColDefinition := colStringStrippedDown(col, false)
		newColDefinition := colStringStrippedDown(newCol, false)
		if tableColDefinition == newColDefinition {
			return capableOf(capabilities.InstantChangeColumnDefaultFlavorCapability)
		}
		// Check if:
		// 1. this an ENUM/SET
		// 2. and the change is to append values to the end of the list
		// 3. and the number of added values does not increase the storage size for the enum/set
		// 4. while still not caring about a change in the default value
		if len(col.Type.EnumValues) > 0 && len(newCol.Type.EnumValues) > 0 {
			// both are enum or set
			if !hasPrefix(newCol.Type.EnumValues, col.Type.EnumValues) {
				return false, nil
			}
			// we know the new column definition is identical to, or extends, the old definition.
			// Now validate storage:
			if strings.EqualFold(col.Type.Type, "enum") {
				if len(col.Type.EnumValues) <= 255 && len(newCol.Type.EnumValues) > 255 {
					// this increases the SET storage size (1 byte for up to 8 values, 2 bytes beyond)
					return false, nil
				}
			}
			if strings.EqualFold(col.Type.Type, "set") {
				if (len(col.Type.EnumValues)+7)/8 != (len(newCol.Type.EnumValues)+7)/8 {
					// this increases the SET storage size (1 byte for up to 8 values, 2 bytes for 8-15, etc.)
					return false, nil
				}
			}
			// Now don't care about change of default:
			tableColDefinition := colStringStrippedDown(col, true)
			newColDefinition := colStringStrippedDown(newCol, true)
			if tableColDefinition == newColDefinition {
				return capableOf(capabilities.InstantExpandEnumCapability)
			}
		}
		return false, nil
	}

	// Up to 8.0.26 we could only ADD COLUMN as last column
	switch opt := alterOption.(type) {
	case *sqlparser.AddColumns:
		if tableHasFulltextIndex {
			// not supported if the table has a FULLTEXT index
			return false, nil
		}
		// Not supported in COMPRESSED tables
		if tableIsCompressed {
			return false, nil
		}
		for _, column := range opt.Columns {
			if isGenerated, storage := IsGeneratedColumn(column); isGenerated {
				if storage == sqlparser.StoredStorage {
					// Adding a generated "STORED" column is unsupported
					return false, nil
				}
			}
			if column.Type.Options.Default != nil && !column.Type.Options.DefaultLiteral {
				// Expression default values are not supported
				return false, nil
			}
			if strings.EqualFold(column.Type.Type, "datetime") {
				e := &ColumnDefinitionEntity{ColumnDefinition: column}
				if !e.IsNullable() && !e.HasDefault() {
					// DATETIME columns must have a default value
					return false, nil
				}
			}
		}
		if opt.First || opt.After != nil {
			// not a "last" column. Only supported as of 8.0.29
			return capableOf(capabilities.InstantAddDropColumnFlavorCapability)
		}
		// Adding a *last* column is supported in 8.0
		return capableOf(capabilities.InstantAddLastColumnFlavorCapability)
	case *sqlparser.DropColumn:
		col := findColumn(opt.Name.Name.String())
		if col == nil {
			// column not found
			return false, nil
		}
		if tableHasFulltextIndex {
			// not supported if the table has a FULLTEXT index
			return false, nil
		}
		// Not supported in COMPRESSED tables
		if tableIsCompressed {
			return false, nil
		}
		if findIndexCoveringColumn(opt.Name.Name.String()) != nil {
			// not supported if the column is part of an index
			return false, nil
		}
		if isGenerated, _ := IsGeneratedColumn(col); isGenerated {
			// supported by all 8.0 versions
			// Note: according to the docs dropping a STORED generated column is not INSTANT-able,
			// but in practice this is supported. This is why we don't test for STORED here, like
			// we did for `AddColumns`.
			return capableOf(capabilities.InstantAddDropVirtualColumnFlavorCapability)
		}
		return capableOf(capabilities.InstantAddDropColumnFlavorCapability)
	case *sqlparser.ChangeColumn:
		if opt.First || opt.After != nil {
			return false, nil
		}
		// We do not support INSTANT for renaming a column (ALTER TABLE ...CHANGE) because:
		// 1. We discourage column rename
		// 2. We do not produce CHANGE statements in declarative diff
		// 3. The success of the operation depends on whether the column is referenced by a foreign key
		//    in another table. Which is a bit too much to compute here.
		if opt.OldColumn.Name.String() != opt.NewColDefinition.Name.String() {
			return false, nil
		}
		if col := findColumn(opt.OldColumn.Name.String()); col != nil {
			return changeModifyColumnCapableOfInstantDDL(col, opt.NewColDefinition)
		}
		return false, nil
	case *sqlparser.ModifyColumn:
		if opt.First || opt.After != nil {
			return false, nil
		}
		if col := findColumn(opt.NewColDefinition.Name.String()); col != nil {
			return changeModifyColumnCapableOfInstantDDL(col, opt.NewColDefinition)
		}
		return false, nil
	case *sqlparser.AlterColumn:
		if opt.DropDefault || opt.DefaultLiteral || opt.DefaultVal != nil {
			return capableOf(capabilities.InstantChangeColumnDefaultFlavorCapability)
		}
		if opt.Invisible != nil {
			return capableOf(capabilities.InstantChangeColumnVisibilityCapability)
		}
		return false, nil
	default:
		return false, nil
	}
}

// AlterTableCapableOfInstantDDL checks if the specific ALTER TABLE is eligible to run via ALGORITHM=INSTANT, given the existing table schema and
// the MySQL server capabilities.
// The function is intentionally public, as it is intended to be used by other packages, such as onlineddl.
func AlterTableCapableOfInstantDDL(alterTable *sqlparser.AlterTable, createTable *sqlparser.CreateTable, capableOf capabilities.CapableOf) (bool, error) {
	if capableOf == nil {
		return false, nil
	}
	capable, err := capableOf(capabilities.InstantDDLFlavorCapability)
	if err != nil {
		return false, err
	}
	if !capable {
		return false, nil
	}
	if alterTable.PartitionOption != nil || alterTable.PartitionSpec != nil {
		// no INSTANT for partitions
		return false, nil
	}
	// For the ALTER statement to qualify for ALGORITHM=INSTANT, all alter options must each qualify.
	numAddedColumns := 0
	for _, alterOption := range alterTable.AlterOptions {
		instantOK, err := alterOptionCapableOfInstantDDL(alterOption, createTable, capableOf)
		if err != nil {
			return false, err
		}
		if !instantOK {
			return false, nil
		}
		switch opt := alterOption.(type) {
		case *sqlparser.AddColumns:
			numAddedColumns += len(opt.Columns)
		}
	}
	if len(createTable.TableSpec.Columns)+numAddedColumns > maxColumnsForInstantAddColumn {
		// Per MySQL docs:
		// > The maximum number of columns in the internal representation of the table cannot exceed 1022 after column addition with the INSTANT algorithm
		return false, nil
	}
	return true, nil
}
