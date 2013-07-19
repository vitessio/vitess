// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tablet

import (
	"strconv"
)

// These numbers should exactly match values defined in dist/mysql-5.1.52/include/mysql/mysql_com.h
const (
	VT_DECIMAL     = 0
	VT_TINY        = 1
	VT_SHORT       = 2
	VT_LONG        = 3
	VT_FLOAT       = 4
	VT_DOUBLE      = 5
	VT_NULL        = 6
	VT_TIMESTAMP   = 7
	VT_LONGLONG    = 8
	VT_INT24       = 9
	VT_DATE        = 10
	VT_TIME        = 11
	VT_DATETIME    = 12
	VT_YEAR        = 13
	VT_NEWDATE     = 14
	VT_VARCHAR     = 15
	VT_BIT         = 16
	VT_NEWDECIMAL  = 246
	VT_ENUM        = 247
	VT_SET         = 248
	VT_TINY_BLOB   = 249
	VT_MEDIUM_BLOB = 250
	VT_LONG_BLOB   = 251
	VT_BLOB        = 252
	VT_VAR_STRING  = 253
	VT_STRING      = 254
	VT_GEOMETRY    = 255
)

func convert(mysqlType int, val string) interface{} {
	switch mysqlType {
	case VT_TINY, VT_SHORT, VT_LONG, VT_LONGLONG, VT_INT24:
		return tonumber(val)
	case VT_FLOAT, VT_DOUBLE:
		fval, err := strconv.ParseFloat(val, 64)
		if err != nil { // Not expected
			panic(err)
		}
		return fval
	}
	return []byte(val)
}

func tonumber(val string) (number interface{}) {
	var err error
	if val[0] == '-' {
		number, err = strconv.ParseInt(val, 0, 64)
	} else {
		number, err = strconv.ParseUint(val, 0, 64)
	}
	if err != nil { // Not expected
		panic(err)
	}
	return number
}
