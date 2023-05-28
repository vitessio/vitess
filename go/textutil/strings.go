/*
Copyright 2020 The Vitess Authors.

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

package textutil

import (
	"net/url"
	"regexp"
	"strings"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/proto/binlogdata"
)

var (
	delimitedListRegexp      = regexp.MustCompile(`[ ,;]+`)
	SimulatedNullString      = sqltypes.NULL.String()
	SimulatedNullStringSlice = []string{sqltypes.NULL.String()}
	SimulatedNullInt         = -1
)

// SplitDelimitedList splits a given string by comma, semi-colon or space, and returns non-empty strings
func SplitDelimitedList(s string) (list []string) {
	tokens := delimitedListRegexp.Split(s, -1)
	for _, token := range tokens {
		token = strings.TrimSpace(token)
		if token == "" {
			continue
		}
		list = append(list, token)
	}
	return list
}

// EscapeJoin acts like strings.Join, except it first escapes elements via net/url
func EscapeJoin(elems []string, sep string) string {
	escapedElems := []string{}
	for i := range elems {
		escapedElems = append(escapedElems, url.QueryEscape(elems[i]))
	}
	return strings.Join(escapedElems, sep)
}

// SplitUnescape acts like strings.Split, except it then unescapes tokens via net/url
func SplitUnescape(s string, sep string) ([]string, error) {
	if s == "" {
		return nil, nil
	}
	elems := strings.Split(s, sep)
	unescapedElems := []string{}
	for i := range elems {
		d, err := url.QueryUnescape(elems[i])
		if err != nil {
			return unescapedElems, err
		}
		unescapedElems = append(unescapedElems, d)
	}
	return unescapedElems, nil
}

// SingleWordCamel takes a single word and returns is in Camel case; basically
// just capitalizing the first letter and making sure the rest are lower case.
func SingleWordCamel(w string) string {
	if w == "" {
		return w
	}
	return strings.ToUpper(w[0:1]) + strings.ToLower(w[1:])
}

// ValueIsSimulatedNull returns true if the value represents
// a NULL or unknown/unspecified value. This is used to
// distinguish between a zero value / default and a user
// provided value that is equivalent (e.g. an empty string
// or slice).
func ValueIsSimulatedNull(val any) bool {
	switch cval := val.(type) {
	case string:
		return cval == SimulatedNullString
	case []string:
		return len(cval) == 1 && cval[0] == sqltypes.NULL.String()
	case binlogdata.OnDDLAction:
		return int32(cval) == int32(SimulatedNullInt)
	case int:
		return cval == SimulatedNullInt
	case int32:
		return int32(cval) == int32(SimulatedNullInt)
	case int64:
		return int64(cval) == int64(SimulatedNullInt)
	default:
		return false
	}
}
