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
	"strings"
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

func KeyValidForIteration(key *IndexDefinitionEntity) bool {
	if key == nil {
		return false
	}
	if !key.IsUnique() {
		return false
	}
	if key.HasNullable() {
		return false
	}
	return true
}
