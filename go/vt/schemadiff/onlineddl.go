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

// KeyValidForIteration returns true if the key is eligible for Online DDL iteration.
func KeyValidForIteration(key *IndexDefinitionEntity) bool {
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
func PrioritizedUniqueKeys(createTableEntity *CreateTableEntity) []*IndexDefinitionEntity {
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
		iFirstColEntity := uniqueKeys[i].ColumnDefinitionEntities[0]
		jFirstColEntity := uniqueKeys[j].ColumnDefinitionEntities[0]
		if iFirstColEntity.IsIntegralType() && !jFirstColEntity.IsIntegralType() {
			// Prioritize integers
			return true
		}
		if !iFirstColEntity.HasBlobTypeStorage() && jFirstColEntity.HasBlobTypeStorage() {
			return true
		}
		if !iFirstColEntity.IsTextual() && jFirstColEntity.IsTextual() {
			return true
		}
		if storageDiff := IntegralTypeStorage(iFirstColEntity.Type()) - IntegralTypeStorage(jFirstColEntity.Type()); storageDiff != 0 {
			return storageDiff < 0
		}
		return false
	})
	return uniqueKeys
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
func GetAlterTableAnalysis(alterTable *sqlparser.AlterTable) *AlterTableAnalysis {
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
) {
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
	return NewColumnDefinitionEntityList(expandedEntities), expandedDescriptions
}

func GetNoDefaultColumns(columns *ColumnDefinitionEntityList) (noDefault *ColumnDefinitionEntityList) {
	subset := []*ColumnDefinitionEntity{}
	for _, col := range columns.Entities {
		if !col.HasDefault() {
			subset = append(subset, col)
		}
	}
	return NewColumnDefinitionEntityList(subset)
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
		if isGeneratedOnSource, _ := sourceColumn.IsGenerated(); isGeneratedOnSource {
			continue
		}
		isDroppedFromSource := false
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
		if isGeneratedOnTarget, _ := targetColumn.IsGenerated(); isGeneratedOnTarget {
			// virtual/generated columns are silently skipped.
			continue
		}
		sharedColumnsMap[sourceColumn.Name()] = targetColumn.Name()
		sourceShared = append(sourceShared, sourceColumn)
		targetShared = append(targetShared, targetColumn)
	}
	return NewColumnDefinitionEntityList(sourceShared), NewColumnDefinitionEntityList(targetShared), NewColumnDefinitionEntityList(droppedNonGenerated), sharedColumnsMap
}
