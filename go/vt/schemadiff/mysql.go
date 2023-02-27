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

var integralTypes = map[string]bool{
	"tinyint":   true,
	"smallint":  true,
	"mediumint": true,
	"int":       true,
	"bigint":    true,
}

var floatTypes = map[string]bool{
	"float":  true,
	"float4": true,
	"float8": true,
	"double": true,
	"real":   true,
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

func IsIntegralType(columnType string) bool {
	return integralTypes[columnType]
}
