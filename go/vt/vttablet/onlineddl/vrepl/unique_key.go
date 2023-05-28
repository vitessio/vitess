/*
   Copyright 2016 GitHub Inc.
	 See https://github.com/github/gh-ost/blob/master/LICENSE
*/
/*
Copyright 2021 The Vitess Authors.

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

package vrepl

import (
	"strings"
)

// UniqueKeyValidForIteration returns 'false' if we should not use this unique key as the main
// iteration key in vreplication.
func UniqueKeyValidForIteration(uniqueKey *UniqueKey) bool {
	if uniqueKey.HasNullable {
		// NULLable columns in a unique key means the set of values is not really unique (two identical rows with NULLs are allowed).
		// Thus, we cannot use this unique key for iteration.
		return false
	}
	if uniqueKey.HasSubpart {
		// vreplication does not fully support indexes on column prefixes such as:
		//   UNIQUE KEY `name_idx` (`name`(15))
		// "HasSubpart" means some column covered by the index has a key length spec.
		return false
	}
	if uniqueKey.HasFloat {
		// float & double data types are imprecise and we cannot use them while iterating unique keys
		return false
	}
	return true // good to go!
}

// GetSharedUniqueKeys returns the unique keys shared between the source & target tables
func GetSharedUniqueKeys(sourceUniqueKeys, targetUniqueKeys [](*UniqueKey), columnRenameMap map[string]string) (chosenSourceUniqueKey, chosenTargetUniqueKey *UniqueKey) {
	type ukPair struct{ source, target *UniqueKey }
	var sharedUKPairs []*ukPair

	for _, sourceUniqueKey := range sourceUniqueKeys {
		if !UniqueKeyValidForIteration(sourceUniqueKey) {
			continue
		}
		for _, targetUniqueKey := range targetUniqueKeys {
			if !UniqueKeyValidForIteration(targetUniqueKey) {
				continue
			}
			uniqueKeyMatches := func() bool {
				// Compare two unique keys
				if sourceUniqueKey.Columns.Len() != targetUniqueKey.Columns.Len() {
					return false
				}
				// Expect same columns, same order, potentially column name mapping
				sourceUniqueKeyNames := sourceUniqueKey.Columns.Names()
				targetUniqueKeyNames := targetUniqueKey.Columns.Names()
				for i := range sourceUniqueKeyNames {
					sourceColumnName := sourceUniqueKeyNames[i]
					targetColumnName := targetUniqueKeyNames[i]
					mappedSourceColumnName := sourceColumnName
					if mapped, ok := columnRenameMap[sourceColumnName]; ok {
						mappedSourceColumnName = mapped
					}
					if !strings.EqualFold(mappedSourceColumnName, targetColumnName) {
						return false
					}
				}
				return true
			}
			if uniqueKeyMatches() {
				sharedUKPairs = append(sharedUKPairs, &ukPair{source: sourceUniqueKey, target: targetUniqueKey})
			}
		}
	}
	// Now that we know what the shared unique keys are, let's find the "best" shared one.
	// Source and target unique keys can have different name, even though they cover the exact same
	// columns and in same order.
	for _, pair := range sharedUKPairs {
		if pair.source.HasNullable {
			continue
		}
		if pair.target.HasNullable {
			continue
		}
		return pair.source, pair.target
	}
	return nil, nil
}

// SourceUniqueKeyAsOrMoreConstrainedThanTarget returns 'true' when sourceUniqueKey is at least as constrained as targetUniqueKey.
// "More constrained" means the uniqueness constraint is "stronger". Thus, if sourceUniqueKey is as-or-more constrained than targetUniqueKey, then
// rows valid under sourceUniqueKey must also be valid in targetUniqueKey. The opposite is not necessarily so: rows that are valid in targetUniqueKey
// may cause a unique key violation under sourceUniqueKey
func SourceUniqueKeyAsOrMoreConstrainedThanTarget(sourceUniqueKey, targetUniqueKey *UniqueKey, columnRenameMap map[string]string) bool {
	// Compare two unique keys
	if sourceUniqueKey.Columns.Len() > targetUniqueKey.Columns.Len() {
		// source can't be more constrained if it covers *more* columns
		return false
	}
	// we know that len(sourceUniqueKeyNames) <= len(targetUniqueKeyNames)
	sourceUniqueKeyNames := sourceUniqueKey.Columns.Names()
	targetUniqueKeyNames := targetUniqueKey.Columns.Names()
	// source is more constrained than target if every column in source is also in target, order is immaterial
	for i := range sourceUniqueKeyNames {
		sourceColumnName := sourceUniqueKeyNames[i]
		mappedSourceColumnName := sourceColumnName
		if mapped, ok := columnRenameMap[sourceColumnName]; ok {
			mappedSourceColumnName = mapped
		}
		columnFoundInTarget := func() bool {
			for _, targetColumnName := range targetUniqueKeyNames {
				if strings.EqualFold(mappedSourceColumnName, targetColumnName) {
					return true
				}
			}
			return false
		}
		if !columnFoundInTarget() {
			return false
		}
	}
	return true
}

// AddedUniqueKeys returns the unique key constraints added in target. This does not necessarily mean that the unique key itself is new,
// rather that there's a new, stricter constraint on a set of columns, that didn't exist before. Example:
//
//	before: unique key `my_key`(c1, c2, c3); after: unique key `my_key`(c1, c2)
//	The constraint on (c1, c2) is new; and `my_key` in target table ("after") is considered a new key
//
// Order of columns is immaterial to uniqueness of column combination.
func AddedUniqueKeys(sourceUniqueKeys, targetUniqueKeys [](*UniqueKey), columnRenameMap map[string]string) (addedUKs [](*UniqueKey)) {
	addedUKs = [](*UniqueKey){}
	for _, targetUniqueKey := range targetUniqueKeys {
		foundAsOrMoreConstrainingSourceKey := func() bool {
			for _, sourceUniqueKey := range sourceUniqueKeys {
				if SourceUniqueKeyAsOrMoreConstrainedThanTarget(sourceUniqueKey, targetUniqueKey, columnRenameMap) {
					// target key does not add a new constraint
					return true
				}
			}
			return false
		}
		if !foundAsOrMoreConstrainingSourceKey() {
			addedUKs = append(addedUKs, targetUniqueKey)
		}
	}
	return addedUKs
}

// RemovedUniqueKeys returns the list of unique key constraints _removed_ going from source to target.
func RemovedUniqueKeys(sourceUniqueKeys, targetUniqueKeys [](*UniqueKey), columnRenameMap map[string]string) (removedUKs [](*UniqueKey)) {
	reverseColumnRenameMap := map[string]string{}
	for k, v := range columnRenameMap {
		reverseColumnRenameMap[v] = k
	}
	return AddedUniqueKeys(targetUniqueKeys, sourceUniqueKeys, reverseColumnRenameMap)
}

// GetUniqueKeyCoveredByColumns returns the first unique key from given list, whose columns all appear
// in given column list.
func GetUniqueKeyCoveredByColumns(uniqueKeys [](*UniqueKey), columns *ColumnList) (chosenUniqueKey *UniqueKey) {
	for _, uniqueKey := range uniqueKeys {
		if !UniqueKeyValidForIteration(uniqueKey) {
			continue
		}
		if uniqueKey.Columns.IsSubsetOf(columns) {
			return uniqueKey
		}
	}
	return nil
}
