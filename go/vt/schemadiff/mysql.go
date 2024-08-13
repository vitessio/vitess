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

var engineCasing = map[string]string{
	"INNODB": "InnoDB",
	"MYISAM": "MyISAM",
}

// integralTypes maps known integer types to their byte storage size
var integralTypes = map[string]int{
	"tinyint":   1,
	"smallint":  2,
	"mediumint": 3,
	"int":       4,
	"bigint":    8,
}

var floatTypes = map[string]int{
	"float":  4,
	"float4": 4,
	"float8": 8,
	"double": 8,
	"real":   8,
}

var decimalTypes = map[string]bool{
	"decimal": true,
	"numeric": true,
}

var charsetTypes = map[string]bool{
	"char":       true,
	"varchar":    true,
	"text":       true,
	"tinytext":   true,
	"mediumtext": true,
	"longtext":   true,
	"enum":       true,
	"set":        true,
}

var blobStorageExponent = map[string]int{
	"tinyblob":   8,
	"tinytext":   8,
	"blob":       16,
	"text":       16,
	"mediumblob": 24,
	"mediumtext": 24,
	"longblob":   32,
	"longtext":   32,
}

func IsFloatingPointType(columnType string) bool {
	_, ok := floatTypes[columnType]
	return ok
}

func FloatingPointTypeStorage(columnType string) int {
	return floatTypes[columnType]
}

func IsIntegralType(columnType string) bool {
	_, ok := integralTypes[columnType]
	return ok
}

func IntegralTypeStorage(columnType string) int {
	return integralTypes[columnType]
}

func IsDecimalType(columnType string) bool {
	return decimalTypes[columnType]
}

func BlobTypeStorage(columnType string) int {
	return blobStorageExponent[columnType]
}

// expandedDataTypes maps some known and difficult-to-compute by INFORMATION_SCHEMA data types which expand other data types.
// For example, in "date:datetime", datetime expands date because it has more precision. In "timestamp:date" date expands timestamp
// because it can contain years not covered by timestamp.
var expandedDataTypes = map[string]bool{
	"time:datetime":      true,
	"date:datetime":      true,
	"timestamp:datetime": true,
	"time:timestamp":     true,
	"date:timestamp":     true,
	"timestamp:date":     true,
}

func IsExpandingDataType(sourceType string, targetType string) bool {
	_, ok := expandedDataTypes[sourceType+":"+targetType]
	return ok
}
