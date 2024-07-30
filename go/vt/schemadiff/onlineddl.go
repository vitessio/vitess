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
	"strings"

	"vitess.io/vitess/go/vt/sqlparser"
)

// ColumnChangeExpandsDataRange sees if target column has any value set/range that is impossible in source column.
func ColumnChangeExpandsDataRange(source *ColumnDefinitionEntity, target *ColumnDefinitionEntity) (bool, string) {
	if target.IsNullable() && !source.IsNullable() {
		return true, "target is NULL-able, source is not"
	}
	if target.Length() > source.Length() {
		return true, "increased length"
	}
	if target.Scale() > source.Scale() {
		return true, "increased scale"
	}
	if source.IsUnsigned() && !target.IsUnsigned() {
		return true, "source is unsigned, target is signed"
	}
	if IntegralTypeStorage(target.Type()) > IntegralTypeStorage(source.Type()) && IntegralTypeStorage(source.Type()) != 0 {
		return true, "increased integer range"
	}
	if IntegralTypeStorage(source.Type()) <= IntegralTypeStorage(target.Type()) &&
		!source.IsUnsigned() && target.IsUnsigned() {
		// e.g. INT SIGNED => INT UNSIGNED, INT SIGNED => BIGINT UNSIGNED
		return true, "target unsigned value exceeds source unsigned value"
	}
	if FloatingPointTypeStorage(target.Type()) > FloatingPointTypeStorage(source.Type()) && FloatingPointTypeStorage(source.Type()) != 0 {
		return true, "increased floating point range"
	}
	if target.IsFloatingPointType() && !source.IsFloatingPointType() {
		return true, "target is floating point, source is not"
	}
	if target.IsDecimalType() && !source.IsDecimalType() {
		return true, "target is decimal, source is not"
	}
	if target.IsDecimalType() && source.IsDecimalType() {
		if target.Length()-target.Scale() > source.Length()-source.Scale() {
			return true, "increased decimal range"
		}
	}
	if IsExpandingDataType(source.Type(), target.Type()) {
		return true, "target is expanded data type of source"
	}
	if BlobTypeStorage(target.Type()) > BlobTypeStorage(source.Type()) && BlobTypeStorage(source.Type()) != 0 {
		return true, "increased blob range"
	}
	if source.Charset() != target.Charset() {
		if target.Charset() == "utf8mb4" {
			return true, "expand character set to utf8mb4"
		}
		if strings.HasPrefix(target.Charset(), "utf8") && !strings.HasPrefix(source.Charset(), "utf8") {
			// not utf to utf
			return true, "expand character set to utf8"
		}
	}
	for _, colType := range []string{"enum", "set"} {
		// enums and sets have very similar properties, and are practically identical in our analysis
		if source.Type() == colType {
			// this is an enum or a set
			if target.Type() != colType {
				return true, "conversion from enum/set to non-enum/set adds potential values"
			}
			// target is an enum or a set. See if all values on target exist in source
			sourceEnumTokensMap := source.EnumOrdinalValues()
			targetEnumTokensMap := target.EnumOrdinalValues()
			for k, v := range targetEnumTokensMap {
				if sourceEnumTokensMap[k] != v {
					return true, "target enum/set expands or reorders source enum/set"
				}
			}
		}
	}
	return false, ""
}

// IsValidIterationKey returns true if the key is eligible for Online DDL iteration.
func IsValidIterationKey(key *IndexDefinitionEntity) bool {
	if key == nil {
		return false
	}
	if !key.IsUnique() {
		return false
	}
	if key.HasFloat() {
		return false
	}
	if key.HasColumnPrefix() {
		return false
	}
	if key.HasNullable() {
		return false
	}
	return true
}

// PrioritizedUniqueKeys returns all unique keys on given table, ordered from "best" to "worst",
// for Online DDL purposes. The list of keys includes some that are not eligible for Online DDL
// iteration.
func PrioritizedUniqueKeys(createTableEntity *CreateTableEntity) *IndexDefinitionEntityList {
	uniqueKeys := []*IndexDefinitionEntity{}
	for _, key := range createTableEntity.IndexDefinitionEntities() {
		if !key.IsUnique() {
			continue
		}
		uniqueKeys = append(uniqueKeys, key)
	}
	sort.SliceStable(uniqueKeys, func(i, j int) bool {
		if uniqueKeys[i].IsPrimary() {
			// PRIMARY is always first
			return true
		}
		if uniqueKeys[j].IsPrimary() {
			// PRIMARY is always first
			return false
		}
		if !uniqueKeys[i].HasNullable() && uniqueKeys[j].HasNullable() {
			// Non NULLable comes first
			return true
		}
		if uniqueKeys[i].HasNullable() && !uniqueKeys[j].HasNullable() {
			// NULLable come last
			return false
		}
		if !uniqueKeys[i].HasColumnPrefix() && uniqueKeys[j].HasColumnPrefix() {
			// Non prefix comes first
			return true
		}
		if uniqueKeys[i].HasColumnPrefix() && !uniqueKeys[j].HasColumnPrefix() {
			// Prefix comes last
			return false
		}
		iFirstColEntity := uniqueKeys[i].ColumnList.Entities[0]
		jFirstColEntity := uniqueKeys[j].ColumnList.Entities[0]
		if iFirstColEntity.IsIntegralType() && !jFirstColEntity.IsIntegralType() {
			// Prioritize integers
			return true
		}
		if !iFirstColEntity.IsIntegralType() && jFirstColEntity.IsIntegralType() {
			// Prioritize integers
			return false
		}
		if !iFirstColEntity.HasBlobTypeStorage() && jFirstColEntity.HasBlobTypeStorage() {
			return true
		}
		if iFirstColEntity.HasBlobTypeStorage() && !jFirstColEntity.HasBlobTypeStorage() {
			return false
		}
		if !iFirstColEntity.IsTextual() && jFirstColEntity.IsTextual() {
			return true
		}
		if iFirstColEntity.IsTextual() && !jFirstColEntity.IsTextual() {
			return false
		}
		if storageDiff := IntegralTypeStorage(iFirstColEntity.Type()) - IntegralTypeStorage(jFirstColEntity.Type()); storageDiff != 0 {
			return storageDiff < 0
		}
		if lenDiff := len(uniqueKeys[i].ColumnList.Entities) - len(uniqueKeys[j].ColumnList.Entities); lenDiff != 0 {
			return lenDiff < 0
		}
		return false
	})
	return NewIndexDefinitionEntityList(uniqueKeys)
}

// RemovedForeignKeyNames returns the names of removed foreign keys, ignoring mere name changes
func RemovedForeignKeyNames(source *CreateTableEntity, target *CreateTableEntity) (names []string, err error) {
	if source == nil || target == nil {
		return nil, nil
	}
	diffHints := DiffHints{
		ConstraintNamesStrategy: ConstraintNamesIgnoreAll,
	}
	diff, err := source.Diff(target, &diffHints)
	if err != nil {
		return nil, err
	}
	names = []string{}
	validateWalk := func(node sqlparser.SQLNode) (kontinue bool, err error) {
		switch node := node.(type) {
		case *sqlparser.DropKey:
			if node.Type == sqlparser.ForeignKeyType {
				names = append(names, node.Name.String())
			}
		}
		return true, nil
	}
	_ = sqlparser.Walk(validateWalk, diff.Statement()) // We never return an error
	return names, nil
}

// AlterTableAnalysis contains useful Online DDL information about an AlterTable statement
type AlterTableAnalysis struct {
	ColumnRenameMap                map[string]string
	DroppedColumnsMap              map[string]bool
	IsRenameTable                  bool
	IsAutoIncrementChangeRequested bool
}

// AnalyzeAlter looks for specific changes in the AlterTable statement, that are relevant
// to OnlineDDL/VReplication
func OnlineDDLAlterTableAnalysis(alterTable *sqlparser.AlterTable) *AlterTableAnalysis {
	analysis := &AlterTableAnalysis{
		ColumnRenameMap:   make(map[string]string),
		DroppedColumnsMap: make(map[string]bool),
	}
	if alterTable == nil {
		return analysis
	}
	for _, opt := range alterTable.AlterOptions {
		switch opt := opt.(type) {
		case *sqlparser.RenameTableName:
			analysis.IsRenameTable = true
		case *sqlparser.DropColumn:
			analysis.DroppedColumnsMap[opt.Name.Name.String()] = true
		case *sqlparser.ChangeColumn:
			if opt.OldColumn != nil && opt.NewColDefinition != nil {
				oldName := opt.OldColumn.Name.String()
				newName := opt.NewColDefinition.Name.String()
				analysis.ColumnRenameMap[oldName] = newName
			}
		case sqlparser.TableOptions:
			for _, tableOption := range opt {
				if strings.ToUpper(tableOption.Name) == "AUTO_INCREMENT" {
					analysis.IsAutoIncrementChangeRequested = true
				}
			}
		}
	}
	return analysis
}

// GetExpandedColumnNames is given source and target shared columns, and returns the list of columns whose data type is expanded.
// An expanded data type is one where the target can have a value which the source does not. Examples:
// - any NOT NULL to NULLable (a NULL in the target cannot appear on source)
// - INT -> BIGINT (obvious)
// - BIGINT UNSIGNED -> INT SIGNED (negative values)
// - TIMESTAMP -> TIMESTAMP(3)
// etc.
func GetExpandedColumns(
	sourceColumns *ColumnDefinitionEntityList,
	targetColumns *ColumnDefinitionEntityList,
) (
	expandedColumns *ColumnDefinitionEntityList,
	expandedDescriptions map[string]string,
	err error,
) {
	if len(sourceColumns.Entities) != len(targetColumns.Entities) {
		return nil, nil, fmt.Errorf("source and target columns must be of same length")
	}

	expandedEntities := []*ColumnDefinitionEntity{}
	expandedDescriptions = map[string]string{}
	for i := range sourceColumns.Entities {
		// source and target columns assumed to be mapped 1:1, same length
		sourceColumn := sourceColumns.Entities[i]
		targetColumn := targetColumns.Entities[i]

		if isExpanded, description := ColumnChangeExpandsDataRange(sourceColumn, targetColumn); isExpanded {
			expandedEntities = append(expandedEntities, sourceColumn)
			expandedDescriptions[sourceColumn.Name()] = description
		}
	}
	return NewColumnDefinitionEntityList(expandedEntities), expandedDescriptions, nil
}

// AnalyzeSharedColumns returns the intersection of two lists of columns in same order as the first list
func AnalyzeSharedColumns(
	sourceColumns, targetColumns *ColumnDefinitionEntityList,
	alterTableAnalysis *AlterTableAnalysis,
) (
	sourceSharedColumns *ColumnDefinitionEntityList,
	targetSharedColumns *ColumnDefinitionEntityList,
	droppedSourceNonGeneratedColumns *ColumnDefinitionEntityList,
	sharedColumnsMap map[string]string,
) {
	sharedColumnsMap = map[string]string{}
	sourceShared := []*ColumnDefinitionEntity{}
	targetShared := []*ColumnDefinitionEntity{}
	droppedNonGenerated := []*ColumnDefinitionEntity{}

	for _, sourceColumn := range sourceColumns.Entities {
		if sourceColumn.IsGenerated() {
			continue
		}
		isDroppedFromSource := false
		// Note to a future engineer: you may be tempted to remove this loop based on the
		// assumption that the later `targetColumn := targetColumns.GetColumn(expectedTargetName)`
		// check is sufficient. It is not. It is possible that a columns was explicitly dropped
		// and added (`DROP COLUMN c, ADD COLUMN c INT`) in the same ALTER TABLE statement.
		// Without checking the ALTER TABLE statement, we would be fooled to believe that column
		// `c` is unchanged in the target, when in fact it was dropped and re-added.
		for droppedColumn := range alterTableAnalysis.DroppedColumnsMap {
			if strings.EqualFold(sourceColumn.Name(), droppedColumn) {
				isDroppedFromSource = true
				break
			}
		}
		if isDroppedFromSource {
			droppedNonGenerated = append(droppedNonGenerated, sourceColumn)
			// Column was dropped, hence cannot be a shared column
			continue
		}
		expectedTargetName := sourceColumn.NameLowered()
		if mappedName := alterTableAnalysis.ColumnRenameMap[sourceColumn.Name()]; mappedName != "" {
			expectedTargetName = mappedName
		}
		targetColumn := targetColumns.GetColumn(expectedTargetName)
		if targetColumn == nil {
			// Column not found in target
			droppedNonGenerated = append(droppedNonGenerated, sourceColumn)
			continue
		}
		if targetColumn.IsGenerated() {
			// virtual/generated columns are silently skipped.
			continue
		}
		// OK, the column is shared (possibly renamed) between source and target.
		sharedColumnsMap[sourceColumn.Name()] = targetColumn.Name()
		sourceShared = append(sourceShared, sourceColumn)
		targetShared = append(targetShared, targetColumn)
	}
	return NewColumnDefinitionEntityList(sourceShared),
		NewColumnDefinitionEntityList(targetShared),
		NewColumnDefinitionEntityList(droppedNonGenerated),
		sharedColumnsMap
}

// KeyAtLeastConstrainedAs returns 'true' when sourceUniqueKey is at least as constrained as targetUniqueKey.
// "More constrained" means the uniqueness constraint is "stronger". Thus, if sourceUniqueKey is as-or-more constrained than targetUniqueKey, then
// rows valid under sourceUniqueKey must also be valid in targetUniqueKey. The opposite is not necessarily so: rows that are valid in targetUniqueKey
// may cause a unique key violation under sourceUniqueKey
func KeyAtLeastConstrainedAs(
	sourceUniqueKey *IndexDefinitionEntity,
	targetUniqueKey *IndexDefinitionEntity,
	columnRenameMap map[string]string,
) bool {
	if !sourceUniqueKey.IsUnique() {
		return false
	}
	if !targetUniqueKey.IsUnique() {
		return true
	}
	sourceKeyLengths := map[string]int{}
	for _, col := range sourceUniqueKey.IndexDefinition.Columns {
		if col.Length == nil {
			sourceKeyLengths[col.Column.Lowered()] = math.MaxInt64
		} else {
			sourceKeyLengths[col.Column.Lowered()] = *col.Length
		}
	}
	targetKeyLengths := map[string]int{}
	for _, col := range targetUniqueKey.IndexDefinition.Columns {
		if col.Length == nil {
			targetKeyLengths[col.Column.Lowered()] = math.MaxInt64
		} else {
			targetKeyLengths[col.Column.Lowered()] = *col.Length
		}
	}
	// source is more constrained than target if every column in source is also in target, order is immaterial
	for _, sourceCol := range sourceUniqueKey.ColumnList.Entities {
		mappedColName, ok := columnRenameMap[sourceCol.Name()]
		if !ok {
			mappedColName = sourceCol.NameLowered()
		}
		targetCol := targetUniqueKey.ColumnList.GetColumn(mappedColName)
		if targetCol == nil {
			// source can't be more constrained if it covers *more* columns
			return false
		}
		// We now know that sourceCol maps to targetCol
		if sourceKeyLengths[sourceCol.NameLowered()] > targetKeyLengths[targetCol.NameLowered()] {
			// source column covers a larger prefix than target column. It is therefore less constrained.
			return false
		}
	}
	return true
}

// IntroducedUniqueConstraints returns the unique key constraints added in target.
// This does not necessarily mean that the unique key itself is new,
// rather that there's a new, stricter constraint on a set of columns, that didn't exist before. Example:
//
//	before:
//		unique key my_key (c1, c2, c3)
//	after:
//		unique key `other_key`(c1, c2)
//	Synopsis: the constraint on (c1, c2) is new; and `other_key` in target table is considered a new key
//
// Order of columns is immaterial to uniqueness of column combination.
func IntroducedUniqueConstraints(sourceUniqueKeys *IndexDefinitionEntityList, targetUniqueKeys *IndexDefinitionEntityList, columnRenameMap map[string]string) *IndexDefinitionEntityList {
	introducedUniqueConstraints := []*IndexDefinitionEntity{}
	for _, targetUniqueKey := range targetUniqueKeys.Entities {
		foundSourceKeyAtLeastAsConstrained := func() bool {
			for _, sourceUniqueKey := range sourceUniqueKeys.Entities {
				if KeyAtLeastConstrainedAs(sourceUniqueKey, targetUniqueKey, columnRenameMap) {
					// target key does not add a new constraint
					return true
				}
			}
			return false
		}
		if !foundSourceKeyAtLeastAsConstrained() {
			introducedUniqueConstraints = append(introducedUniqueConstraints, targetUniqueKey)
		}
	}
	return NewIndexDefinitionEntityList(introducedUniqueConstraints)
}

// RemovedUniqueConstraints returns the list of unique key constraints _removed_ going from source to target.
func RemovedUniqueConstraints(sourceUniqueKeys *IndexDefinitionEntityList, targetUniqueKeys *IndexDefinitionEntityList, columnRenameMap map[string]string) *IndexDefinitionEntityList {
	reverseColumnRenameMap := map[string]string{}
	for k, v := range columnRenameMap {
		reverseColumnRenameMap[v] = k
	}
	return IntroducedUniqueConstraints(targetUniqueKeys, sourceUniqueKeys, reverseColumnRenameMap)
}

// IterationKeysByColumns returns the Online DDL compliant unique keys from given list,
// whose columns are all covered by the given column list.
func IterationKeysByColumns(keys *IndexDefinitionEntityList, columns *ColumnDefinitionEntityList) *IndexDefinitionEntityList {
	subset := []*IndexDefinitionEntity{}
	for _, key := range keys.SubsetCoveredByColumns(columns).Entities {
		if IsValidIterationKey(key) {
			subset = append(subset, key)
		}
	}
	return NewIndexDefinitionEntityList(subset)
}

// MappedColumnNames
func MappedColumnNames(columnsList *ColumnDefinitionEntityList, columnNamesMap map[string]string) []string {
	names := columnsList.Names()
	for i := range names {
		if mappedName, ok := columnNamesMap[names[i]]; ok {
			names[i] = mappedName
		}
	}
	return names
}

// AlterTableAnalysis contains useful Online DDL information about an AlterTable statement
type MigrationTablesAnalysis struct {
	SourceSharedColumns     *ColumnDefinitionEntityList
	TargetSharedColumns     *ColumnDefinitionEntityList
	DroppedNoDefaultColumns *ColumnDefinitionEntityList
	ExpandedColumns         *ColumnDefinitionEntityList
	SharedColumnsMap        map[string]string
	ChosenSourceUniqueKey   *IndexDefinitionEntity
	ChosenTargetUniqueKey   *IndexDefinitionEntity
	AddedUniqueKeys         *IndexDefinitionEntityList
	RemovedUniqueKeys       *IndexDefinitionEntityList
	RemovedForeignKeyNames  []string
	IntToEnumMap            map[string]bool
	SourceAutoIncrement     uint64
	RevertibleNotes         []string
}

func OnlineDDLMigrationTablesAnalysis(
	sourceCreateTableEntity *CreateTableEntity,
	targetCreateTableEntity *CreateTableEntity,
	alterTableAnalysis *AlterTableAnalysis,
) (analysis *MigrationTablesAnalysis, err error) {
	analysis = &MigrationTablesAnalysis{
		IntToEnumMap:    make(map[string]bool),
		RevertibleNotes: []string{},
	}
	// columns:
	generatedColumns := func(columns *ColumnDefinitionEntityList) *ColumnDefinitionEntityList {
		return columns.Filter(func(col *ColumnDefinitionEntity) bool {
			return col.IsGenerated()
		})
	}
	noDefaultColumns := func(columns *ColumnDefinitionEntityList) *ColumnDefinitionEntityList {
		return columns.Filter(func(col *ColumnDefinitionEntity) bool {
			return !col.HasDefault()
		})
	}
	sourceColumns := sourceCreateTableEntity.ColumnDefinitionEntitiesList()
	targetColumns := targetCreateTableEntity.ColumnDefinitionEntitiesList()

	var droppedSourceNonGeneratedColumns *ColumnDefinitionEntityList
	analysis.SourceSharedColumns, analysis.TargetSharedColumns, droppedSourceNonGeneratedColumns, analysis.SharedColumnsMap = AnalyzeSharedColumns(sourceColumns, targetColumns, alterTableAnalysis)

	// unique keys
	sourceUniqueKeys := PrioritizedUniqueKeys(sourceCreateTableEntity)
	if sourceUniqueKeys.Len() == 0 {
		return nil, fmt.Errorf("found no possible unique key on `%s`", sourceCreateTableEntity.Name())
	}

	targetUniqueKeys := PrioritizedUniqueKeys(targetCreateTableEntity)
	if targetUniqueKeys.Len() == 0 {
		return nil, fmt.Errorf("found no possible unique key on `%s`", targetCreateTableEntity.Name())
	}
	// VReplication supports completely different unique keys on source and target, covering
	// some/completely different columns. The condition is that the key on source
	// must use columns which all exist on target table.
	eligibleSourceColumnsForUniqueKey := analysis.SourceSharedColumns.Union(generatedColumns(sourceColumns))
	analysis.ChosenSourceUniqueKey = IterationKeysByColumns(sourceUniqueKeys, eligibleSourceColumnsForUniqueKey).First()
	if analysis.ChosenSourceUniqueKey == nil {
		return nil, fmt.Errorf("found no possible unique key on `%s` whose columns are in target table `%s`", sourceCreateTableEntity.Name(), targetCreateTableEntity.Name())
	}

	eligibleTargetColumnsForUniqueKey := analysis.TargetSharedColumns.Union(generatedColumns(targetColumns))
	analysis.ChosenTargetUniqueKey = IterationKeysByColumns(targetUniqueKeys, eligibleTargetColumnsForUniqueKey).First()
	if analysis.ChosenTargetUniqueKey == nil {
		return nil, fmt.Errorf("found no possible unique key on `%s` whose columns are in source table `%s`", targetCreateTableEntity.Name(), sourceCreateTableEntity.Name())
	}

	analysis.AddedUniqueKeys = IntroducedUniqueConstraints(sourceUniqueKeys, targetUniqueKeys, alterTableAnalysis.ColumnRenameMap)
	analysis.RemovedUniqueKeys = RemovedUniqueConstraints(sourceUniqueKeys, targetUniqueKeys, alterTableAnalysis.ColumnRenameMap)
	analysis.RemovedForeignKeyNames, err = RemovedForeignKeyNames(sourceCreateTableEntity, targetCreateTableEntity)
	if err != nil {
		return nil, err
	}

	formalizeColumns := func(columnsLists ...*ColumnDefinitionEntityList) error {
		for _, colList := range columnsLists {
			for _, col := range colList.Entities {
				col.SetExplicitDefaultAndNull()
				if err := col.SetExplicitCharsetCollate(); err != nil {
					return err
				}
			}
		}
		return nil
	}

	if err := formalizeColumns(analysis.SourceSharedColumns, analysis.TargetSharedColumns, droppedSourceNonGeneratedColumns); err != nil {
		return nil, err
	}

	for i := range analysis.SourceSharedColumns.Entities {
		sourceColumn := analysis.SourceSharedColumns.Entities[i]
		mappedColumn := analysis.TargetSharedColumns.Entities[i]

		if sourceColumn.IsIntegralType() && mappedColumn.Type() == "enum" {
			analysis.IntToEnumMap[sourceColumn.Name()] = true
		}
	}

	analysis.DroppedNoDefaultColumns = noDefaultColumns(droppedSourceNonGeneratedColumns)
	var expandedDescriptions map[string]string
	analysis.ExpandedColumns, expandedDescriptions, err = GetExpandedColumns(analysis.SourceSharedColumns, analysis.TargetSharedColumns)
	if err != nil {
		return nil, err
	}

	analysis.SourceAutoIncrement, err = sourceCreateTableEntity.AutoIncrementValue()
	if err != nil {
		return nil, err
	}

	for _, uk := range analysis.RemovedUniqueKeys.Names() {
		analysis.RevertibleNotes = append(analysis.RevertibleNotes, fmt.Sprintf("unique constraint removed: %s", uk))
	}
	for _, name := range analysis.DroppedNoDefaultColumns.Names() {
		analysis.RevertibleNotes = append(analysis.RevertibleNotes, fmt.Sprintf("column %s dropped, and had no default value", name))
	}
	for _, name := range analysis.ExpandedColumns.Names() {
		analysis.RevertibleNotes = append(analysis.RevertibleNotes, fmt.Sprintf("column %s: %s", name, expandedDescriptions[name]))
	}
	for _, name := range analysis.RemovedForeignKeyNames {
		analysis.RevertibleNotes = append(analysis.RevertibleNotes, fmt.Sprintf("foreign key %s dropped", name))
	}

	return analysis, nil
}
