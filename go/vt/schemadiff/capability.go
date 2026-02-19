package schemadiff

import (
	"fmt"
	"strings"

	"vitess.io/vitess/go/mysql/capabilities"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/sqlparser"
)

const (
	maxColumnsForInstantAddColumn = 1022
)

// alterOptionAvailableViaInstantDDL checks if the specific alter option is eligible to run via ALGORITHM=INSTANT
// reference: https://dev.mysql.com/doc/refman/8.0/en/innodb-online-ddl-operations.html
func alterOptionCapableOfInstantDDL(alterOption sqlparser.AlterOption, createTable *sqlparser.CreateTable, capableOf capabilities.CapableOf) (eligible bool, err error) {
	log.Info(fmt.Sprintf("Checking if ALTER %q is capable of INSTANT DDL", sqlparser.CanonicalString(alterOption)))
	defer func() {
		if eligible {
			log.Info(fmt.Sprintf("ALTER %q is eligible for INSTANT DDL", sqlparser.CanonicalString(alterOption)))
		}
	}()
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
		// Normalize explicit NULL to implicit nullable: SHOW CREATE TABLE may add the NULL
		// keyword explicitly for nullable columns, but user ALTER statements typically omit it.
		// Both mean the same thing, so we treat them as equivalent.
		if strippedCol.Type.Options.Null != nil && *strippedCol.Type.Options.Null {
			strippedCol.Type.Options.Null = nil
		}
		// Normalize the type name to lowercase. The CanonicalString formatter uses %#s for the
		// column type name which writes it as-is (case-sensitive), so "enum" and "ENUM" would
		// produce different canonical strings without this normalization.
		strippedCol.Type.Type = strings.ToLower(strippedCol.Type.Type)
		// Strip charset and collation: SHOW CREATE TABLE adds the column's collation explicitly
		// when it is inherited from a table-level COLLATE clause. User ALTER statements typically
		// omit the collation. Stripping both sides allows a semantic comparison.
		// Genuine charset/collation changes are detected separately via isCharsetOrCollationChange.
		strippedCol.Type.Charset.Name = ""
		strippedCol.Type.Options.Collate = ""
		if stripEnum {
			strippedCol.Type.EnumValues = nil
		}
		return sqlparser.CanonicalString(strippedCol)
	}
	// isCharsetOrCollationChange returns true if the new column definition explicitly specifies
	// a different charset or collation than the old one. A missing value in newCol means the
	// user did not specify it, which is treated as "no change".
	isCharsetOrCollationChange := func(col, newCol *sqlparser.ColumnDefinition) bool {
		if newCol.Type.Charset.Name != "" && !strings.EqualFold(col.Type.Charset.Name, newCol.Type.Charset.Name) {
			return true
		}
		if newCol.Type.Options.Collate != "" && !strings.EqualFold(col.Type.Options.Collate, newCol.Type.Options.Collate) {
			return true
		}
		return false
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
		if tableColDefinition == newColDefinition && !isCharsetOrCollationChange(col, newCol) {
			capable, err := capableOf(capabilities.InstantChangeColumnDefaultFlavorCapability)
			if !capable {
				log.Info(fmt.Sprintf("ALTER %q is not eligible for INSTANT DDL because the MySQL server version does not support changing a column default with INSTANT DDL", sqlparser.CanonicalString(alterOption)))
			}
			if err != nil {
				log.Error(fmt.Sprintf("Error while checking MySQL server INSTANT DDL capability for ALTER %q: %v", sqlparser.CanonicalString(alterOption), err))
			}
			return capable, err
		}
		// Check if:
		// 1. this an ENUM/SET
		// 2. and the change is to append values to the end of the list
		// 3. and the number of added values does not increase the storage size for the enum/set
		// 4. while still not caring about a change in the default value
		if len(col.Type.EnumValues) > 0 && len(newCol.Type.EnumValues) > 0 {
			// both are enum or set
			if !hasPrefix(newCol.Type.EnumValues, col.Type.EnumValues) {
				log.Info(fmt.Sprintf("ALTER %q is not eligible for INSTANT DDL because the new ENUM/SET values do not have the old values as prefix. Old columns: %q, new columns: %q",
					sqlparser.CanonicalString(alterOption), strings.Join(col.Type.EnumValues, ","), strings.Join(newCol.Type.EnumValues, ",")))
				return false, nil
			}
			// we know the new column definition is identical to, or extends, the old definition.
			// Now validate storage:
			if strings.EqualFold(col.Type.Type, "enum") {
				if len(col.Type.EnumValues) <= 255 && len(newCol.Type.EnumValues) > 255 {
					// this increases the ENUM storage size (1 byte for up to 255 values, 2 bytes beyond)
					log.Info(fmt.Sprintf("ALTER %q is not eligible for INSTANT DDL because we would be crossing the 255 enum value count, which increases the storage size from 1 to 2 bytes. Old columns: %q, new columns: %q",
						sqlparser.CanonicalString(alterOption), strings.Join(col.Type.EnumValues, ","), strings.Join(newCol.Type.EnumValues, ",")))
					return false, nil
				}
			}
			if strings.EqualFold(col.Type.Type, "set") {
				if (len(col.Type.EnumValues)+7)/8 != (len(newCol.Type.EnumValues)+7)/8 {
					// this increases the SET storage size (1 byte for up to 8 values, 2 bytes for 8-15, etc.)
					log.Info(fmt.Sprintf("ALTER %q is not eligible for INSTANT DDL because we would be crossing the 8 SET value count, which increases the storage size from 1 to 2 bytes. Old columns: %q, new columns: %q",
						sqlparser.CanonicalString(alterOption), strings.Join(col.Type.EnumValues, ","), strings.Join(newCol.Type.EnumValues, ",")))
					return false, nil
				}
			}
			// Now don't care about change of default:
			tableColDefinition := colStringStrippedDown(col, true)
			newColDefinition := colStringStrippedDown(newCol, true)
			if tableColDefinition == newColDefinition {
				if isCharsetOrCollationChange(col, newCol) {
					log.Info(fmt.Sprintf("ALTER %q is not eligible for INSTANT DDL because the charset or collation of the column is being changed. Old column: %q, new column: %q",
						sqlparser.CanonicalString(alterOption), sqlparser.CanonicalString(col), sqlparser.CanonicalString(newCol)))
					return false, nil
				}
				capable, err := capableOf(capabilities.InstantExpandEnumCapability)
				if !capable {
					log.Info(fmt.Sprintf("ALTER %q is not eligible for INSTANT DDL because the MySQL server version does not support INSTANT DDL for expanding ENUM/SET columns", sqlparser.CanonicalString(alterOption)))
				}
				if err != nil {
					log.Error(fmt.Sprintf("Error while checking MySQL server INSTANT DDL capability for ALTER %q: %v", sqlparser.CanonicalString(alterOption), err))
				}
				return capable, err
			}
			log.Info(fmt.Sprintf("ALTER %q is not eligible for INSTANT DDL because the base column definition has been changed. Old column: %q, new column: %q", sqlparser.CanonicalString(alterOption), tableColDefinition, newColDefinition))
		}
		return false, nil
	}

	// Up to 8.0.26 we could only ADD COLUMN as last column
	switch opt := alterOption.(type) {
	case *sqlparser.AddColumns:
		if tableHasFulltextIndex {
			// not supported if the table has a FULLTEXT index
			log.Info(fmt.Sprintf("ALTER %q is not eligible for INSTANT DDL because the table has a FULLTEXT index", sqlparser.CanonicalString(alterOption)))
			return false, nil
		}
		// Not supported in COMPRESSED tables
		if tableIsCompressed {
			log.Info(fmt.Sprintf("ALTER %q is not eligible for INSTANT DDL because the table is compressed", sqlparser.CanonicalString(alterOption)))
			return false, nil
		}
		for _, column := range opt.Columns {
			if isGenerated, storage := IsGeneratedColumn(column); isGenerated {
				if storage == sqlparser.StoredStorage {
					// Adding a generated "STORED" column is unsupported
					log.Info(fmt.Sprintf("ALTER %q is not eligible for INSTANT DDL because the column is a generated value", sqlparser.CanonicalString(alterOption)))
					return false, nil
				}
			}
			if column.Type.Options.Default != nil && !column.Type.Options.DefaultLiteral {
				// Expression default values are not supported
				log.Info(fmt.Sprintf("ALTER %q is not eligible for INSTANT DDL because the column has a DEFAULT function", sqlparser.CanonicalString(alterOption)))
				return false, nil
			}
			if strings.EqualFold(column.Type.Type, "datetime") {
				e := &ColumnDefinitionEntity{ColumnDefinition: column}
				if !e.IsNullable() && !e.HasDefault() {
					// DATETIME columns must have a default value
					log.Info(fmt.Sprintf("ALTER %q is not eligible for INSTANT DDL because the datetime column has no DEFAULT", sqlparser.CanonicalString(alterOption)))
					return false, nil
				}
			}
		}
		if opt.First || opt.After != nil {
			// not a "last" column. Only supported as of 8.0.29
			capable, err := capableOf(capabilities.InstantAddDropColumnFlavorCapability)
			if !capable {
				log.Info(fmt.Sprintf("ALTER %q is not eligible for INSTANT DDL because the MySQL server version does not support FIRST or AFTER clauses with INSTANT DDL", sqlparser.CanonicalString(alterOption)))
			}
			if err != nil {
				log.Error(fmt.Sprintf("Error while checking MySQL server INSTANT DDL capability for ALTER %q: %v", sqlparser.CanonicalString(alterOption), err))
			}
			return capable, err
		}
		// Adding a *last* column is supported in 8.0
		capable, err := capableOf(capabilities.InstantAddLastColumnFlavorCapability)
		if !capable {
			log.Info(fmt.Sprintf("ALTER %q is not eligible for INSTANT DDL because the MySQL server version does not support LAST clauses with INSTANT DDL", sqlparser.CanonicalString(alterOption)))
		}
		if err != nil {
			log.Error(fmt.Sprintf("Error while checking MySQL server INSTANT DDL capability for ALTER %q: %v", sqlparser.CanonicalString(alterOption), err))
		}
		return capable, err
	case *sqlparser.DropColumn:
		col := findColumn(opt.Name.Name.String())
		if col == nil {
			// column not found
			log.Info(fmt.Sprintf("ALTER %q is not eligible for INSTANT DDL because the column %q was not found", sqlparser.CanonicalString(alterOption), opt.Name.Name.String()))
			return false, nil
		}
		if tableHasFulltextIndex {
			// not supported if the table has a FULLTEXT index
			log.Info(fmt.Sprintf("ALTER %q is not eligible for INSTANT DDL because the table has a FULLTEXT index", sqlparser.CanonicalString(alterOption)))
			return false, nil
		}
		// Not supported in COMPRESSED tables
		if tableIsCompressed {
			log.Info(fmt.Sprintf("ALTER %q is not eligible for INSTANT DDL because the table is compressed", sqlparser.CanonicalString(alterOption)))
			return false, nil
		}
		if findIndexCoveringColumn(opt.Name.Name.String()) != nil {
			// not supported if the column is part of an index
			log.Info(fmt.Sprintf("ALTER %q is not eligible for INSTANT DDL because the column is part of an index", sqlparser.CanonicalString(alterOption)))
			return false, nil
		}
		if isGenerated, _ := IsGeneratedColumn(col); isGenerated {
			// supported by all 8.0 versions
			// Note: according to the docs dropping a STORED generated column is not INSTANT-able,
			// but in practice this is supported. This is why we don't test for STORED here, like
			// we did for `AddColumns`.
			capable, err := capableOf(capabilities.InstantAddDropVirtualColumnFlavorCapability)
			if !capable {
				log.Info(fmt.Sprintf("ALTER %q is not eligible for INSTANT DDL because the MySQL server version does not support dropping virtual generated columns with INSTANT DDL", sqlparser.CanonicalString(alterOption)))
			}
			if err != nil {
				log.Error(fmt.Sprintf("Error while checking MySQL server INSTANT DDL capability for ALTER %q: %v", sqlparser.CanonicalString(alterOption), err))
			}
			return capable, err
		}
		return capableOf(capabilities.InstantAddDropColumnFlavorCapability)
	case *sqlparser.ChangeColumn:
		if opt.First || opt.After != nil {
			log.Info(fmt.Sprintf("ALTER %q is not eligible for INSTANT DDL because CHANGE COLUMN does not support FIRST or AFTER clauses with INSTANT DDL", sqlparser.CanonicalString(alterOption)))
			return false, nil
		}
		// We do not support INSTANT for renaming a column (ALTER TABLE ...CHANGE) because:
		// 1. We discourage column rename
		// 2. We do not produce CHANGE statements in declarative diff
		// 3. The success of the operation depends on whether the column is referenced by a foreign key
		//    in another table. Which is a bit too much to compute here.
		if opt.OldColumn.Name.String() != opt.NewColDefinition.Name.String() {
			log.Info(fmt.Sprintf("ALTER %q is not eligible for INSTANT DDL as the column name is being changed and that is not supported with INSTANT DDL", sqlparser.CanonicalString(alterOption)))
			return false, nil
		}
		if col := findColumn(opt.OldColumn.Name.String()); col != nil {
			capable, err := changeModifyColumnCapableOfInstantDDL(col, opt.NewColDefinition)
			if err != nil {
				log.Error(fmt.Sprintf("Error while checking ALTER %q for INSTANT DDL capability: %v", sqlparser.CanonicalString(alterOption), err))
			}
			return capable, err
		}
		log.Info(fmt.Sprintf("ALTER %q is not eligible for INSTANT DDL because the column %q was not found", sqlparser.CanonicalString(alterOption), opt.OldColumn.Name.String()))
		return false, nil
	case *sqlparser.ModifyColumn:
		if opt.First || opt.After != nil {
			log.Info(fmt.Sprintf("ALTER %q is not eligible for INSTANT DDL as the MODIFY COLUMN clause does not support FIRST or AFTER clauses with INSTANT DDL", sqlparser.CanonicalString(alterOption)))
			return false, nil
		}
		if col := findColumn(opt.NewColDefinition.Name.String()); col != nil {
			capable, err := changeModifyColumnCapableOfInstantDDL(col, opt.NewColDefinition)
			if err != nil {
				log.Error(fmt.Sprintf("Error while checking ALTER %q for INSTANT DDL capability: %v", sqlparser.CanonicalString(alterOption), err))
			}
			return capable, err
		}
		log.Info(fmt.Sprintf("ALTER %q is not eligible for INSTANT DDL because the column %q was not found", sqlparser.CanonicalString(alterOption), opt.NewColDefinition.Name.String()))
		return false, nil
	case *sqlparser.AlterColumn:
		if opt.DropDefault || opt.DefaultLiteral || opt.DefaultVal != nil {
			capable, err := capableOf(capabilities.InstantChangeColumnDefaultFlavorCapability)
			if !capable {
				log.Info(fmt.Sprintf("ALTER %q is not eligible for INSTANT DDL because the MySQL server version does not support changing column defaults with INSTANT DDL", sqlparser.CanonicalString(alterOption)))
			}
			if err != nil {
				log.Error(fmt.Sprintf("Error while checking MySQL server INSTANT DDL capability for ALTER %q: %v", sqlparser.CanonicalString(alterOption), err))
			}
			return capable, err
		}
		if opt.Invisible != nil {
			capable, err := capableOf(capabilities.InstantChangeColumnVisibilityCapability)
			if !capable {
				log.Info(fmt.Sprintf("ALTER %q is not eligible for INSTANT DDL because the MySQL server version does not support changing column visibility with INSTANT DDL", sqlparser.CanonicalString(alterOption)))
			}
			if err != nil {
				log.Error(fmt.Sprintf("Error while checking MySQL server INSTANT DDL capability for ALTER %q: %v", sqlparser.CanonicalString(alterOption), err))
			}
			return capable, err
		}
		return false, nil
	case sqlparser.AlgorithmValue:
		// We accept an explicit ALGORITHM=INSTANT option.
		return strings.EqualFold(string(opt), sqlparser.InstantStr), nil
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
		log.Info(fmt.Sprintf("ALTER %q is not eligible for INSTANT DDL because the MySQL server version does not support INSTANT DDL", sqlparser.CanonicalString(alterTable)))
		return false, nil
	}
	if alterTable.PartitionOption != nil || alterTable.PartitionSpec != nil {
		// no INSTANT for partitions
		log.Info(fmt.Sprintf("ALTER %q is not eligible for INSTANT DDL because the table is partitioned", sqlparser.CanonicalString(alterTable)))
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
