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

// isExpandedColumn sees if target column has any value set/range that is impossible in source column. See GetExpandedColumns comment for examples
func (c *ColumnDefinitionEntity) Expands(source *ColumnDefinitionEntity) (bool, string) {
	if c.IsNullable() && !source.IsNullable() {
		return true, "target is NULL-able, source is not"
	}
	if c.Length() > source.Length() {
		return true, "increased length"
	}
	if c.Scale() > source.Scale() {
		return true, "increased scale"
	}
	if source.IsUnsigned() && !c.IsUnsigned() {
		return true, "source is unsigned, target is signed"
	}
	if IntegralTypeStorage(c.Type()) > IntegralTypeStorage(source.Type()) && IntegralTypeStorage(source.Type()) != 0 {
		return true, "increased integer range"
	}
	if IntegralTypeStorage(source.Type()) <= IntegralTypeStorage(c.Type()) &&
		!source.IsUnsigned() && c.IsUnsigned() {
		// e.g. INT SIGNED => INT UNSIGNED, INT SIGNED => BIGINT UNSIGNED
		return true, "target unsigned value exceeds source unsigned value"
	}
	if FloatingPointTypeStorage(c.Type()) > FloatingPointTypeStorage(source.Type()) && FloatingPointTypeStorage(source.Type()) != 0 {
		return true, "increased floating point range"
	}
	if c.IsFloatingPointType() && !source.IsFloatingPointType() {
		return true, "target is floating point, source is not"
	}
	if c.IsDecimalType() && !source.IsDecimalType() {
		return true, "target is decimal, source is not"
	}
	if c.IsDecimalType() && source.IsDecimalType() {
		if c.Length()-c.Scale() > source.Length()-source.Scale() {
			return true, "increased decimal range"
		}
	}
	if IsExpandingDataType(source.Type(), c.Type()) {
		return true, "target is expanded data type of source"
	}
	if BlobTypeStorage(c.Type()) > BlobTypeStorage(source.Type()) && BlobTypeStorage(source.Type()) != 0 {
		return true, "increased blob range"
	}
	if source.Charset() != c.Charset() {
		if c.Charset() == "utf8mb4" {
			return true, "expand character set to utf8mb4"
		}
		if strings.HasPrefix(c.Charset(), "utf8") && !strings.HasPrefix(source.Charset(), "utf8") {
			// not utf to utf
			return true, "expand character set to utf8"
		}
	}
	for _, colType := range []string{"enum", "set"} {
		// enums and sets have very similar properties, and are practically identical in our analysis
		if source.Type() == colType {
			// this is an enum or a set
			if c.Type() != colType {
				return true, "conversion from enum/set to non-enum/set adds potential values"
			}
			// target is an enum or a set. See if all values on target exist in source
			sourceEnumTokensMap := source.EnumOrdinalValues()
			targetEnumTokensMap := c.EnumOrdinalValues()
			for k, v := range targetEnumTokensMap {
				if sourceEnumTokensMap[k] != v {
					return true, "target enum/set expands or reorders source enum/set"
				}
			}
		}
	}
	return false, ""
}
