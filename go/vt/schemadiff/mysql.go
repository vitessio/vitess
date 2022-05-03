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

var charsetAliases = map[string]string{
	"utf8": "utf8mb3",
}

var integralTypes = map[string]bool{
	"tinyint":   true,
	"smallint":  true,
	"mediumint": true,
	"int":       true,
	"bigint":    true,
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

// This is the default charset for MySQL 8.0
const defaultCharset = "utf8mb4"

// This list currently is based on MySQL 8.0 and uses
// the defaults on that version.
var defaultCollations = map[string]string{
	"armscii8": "armscii8_general_ci",
	"ascii":    "ascii_general_ci",
	"big5":     "big5_chinese_ci",
	"binary":   "binary",
	"cp1250":   "cp1250_general_ci",
	"cp1251":   "cp1251_general_ci",
	"cp1256":   "cp1256_general_ci",
	"cp1257":   "cp1257_general_ci",
	"cp850":    "cp850_general_ci",
	"cp852":    "cp852_general_ci",
	"cp866":    "cp866_general_ci",
	"cp932":    "cp932_japanese_ci",
	"dec8":     "dec8_swedish_ci",
	"eucjpms":  "eucjpms_japanese_ci",
	"euckr":    "euckr_korean_ci",
	"gb18030":  "gb18030_chinese_ci",
	"gb2312":   "gb2312_chinese_ci",
	"gbk":      "gbk_chinese_ci",
	"geostd8":  "geostd8_general_ci",
	"greek":    "greek_general_ci",
	"hebrew":   "hebrew_general_ci",
	"hp8":      "hp8_english_ci",
	"keybcs2":  "keybcs2_general_ci",
	"koi8r":    "koi8r_general_ci",
	"koi8u":    "koi8u_general_ci",
	"latin1":   "latin1_swedish_ci",
	"latin2":   "latin2_general_ci",
	"latin5":   "latin5_turkish_ci",
	"latin7":   "latin7_general_ci",
	"macce":    "macce_general_ci",
	"macroman": "macroman_general_ci",
	"sjis":     "sjis_japanese_ci",
	"swe7":     "swe7_swedish_ci",
	"tis620":   "tis620_thai_ci",
	"ucs2":     "ucs2_general_ci",
	"ujis":     "ujis_japanese_ci",
	"utf16":    "utf16_general_ci",
	"utf16le":  "utf16le_general_ci",
	"utf32":    "utf32_general_ci",
	"utf8mb3":  "utf8_general_ci",
	"utf8mb4":  "utf8mb4_0900_ai_ci",
}
