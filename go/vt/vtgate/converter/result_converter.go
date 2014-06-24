// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package converter manages the coversion of query result.
package converter

import (
	"fmt"
)

// ResultConverter defines the type of conversion function.
type ResultConverter func(from interface{}, to interface{}) error

var (
	queryResultConverterMap     = make(map[string]ResultConverter)
	queryResultListConverterMap = make(map[string]ResultConverter)
)

// MakeConverterID generates an ID based on given protocol types.
// It ignores if both types are the same.
func MakeConverterID(from, to string) string {
	if from == to {
		return ""
	}
	return fmt.Sprintf("%v|%v", from, to)
}

// RegisterQueryResultConverter registers the converter function for QueryResult.
// The id should be the one generated from MakeConverterID().
func RegisterQueryResultConverter(id string, converter ResultConverter) {
	queryResultConverterMap[id] = converter
}

// RegisterQueryResultListConverter registers the converter function for QueryResultList.
// The id should be the one generated from MakeConverterID().
func RegisterQueryResultListConverter(id string, converter ResultConverter) {
	queryResultListConverterMap[id] = converter
}

// ConvertQueryResult calls registered conversion function for QueryResult.
// The id should be the one generated from MakeConverterID().
func ConvertQueryResult(id string, from, to interface{}) error {
	return queryResultConverterMap[id](from, to)
}

// ConvertQueryResultList calls registered conversion function for QueryResultList.
// The id should be the one generated from MakeConverterID().
func ConvertQueryResultList(id string, from, to interface{}) error {
	return queryResultListConverterMap[id](from, to)
}
