// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package vtgate provides query routing rpc services
// for vttablets.
package converter

import (
	"fmt"
)

type ResultConverter func(from interface{}, to interface{}) error

var (
	queryResultConverterMap     = make(map[string]ResultConverter)
	queryResultListConverterMap = make(map[string]ResultConverter)
)

func MakeConverterID(from, to string) string {
	if from == to {
		return ""
	}
	return fmt.Sprintf("%v|%v", from, to)
}

func ConvertQueryResult(id string, from, to interface{}) error {
	return queryResultConverterMap[id](from, to)
}

func ConvertQueryResultList(id string, from, to interface{}) error {
	return queryResultListConverterMap[id](from, to)
}
