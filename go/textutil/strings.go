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
	"fmt"
	"net/url"
	"regexp"
	"strings"
	"unicode"

	"vitess.io/vitess/go/sqltypes"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

type TruncationLocation int

const (
	TruncationLocationMiddle TruncationLocation = iota
	TruncationLocationEnd
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
	case binlogdatapb.OnDDLAction:
		return int32(cval) == int32(SimulatedNullInt)
	case int:
		return cval == SimulatedNullInt
	case int32:
		return int32(cval) == int32(SimulatedNullInt)
	case int64:
		return int64(cval) == int64(SimulatedNullInt)
	case []topodatapb.TabletType:
		return len(cval) == 1 && cval[0] == topodatapb.TabletType(SimulatedNullInt)
	case binlogdatapb.VReplicationWorkflowState:
		return int32(cval) == int32(SimulatedNullInt)
	default:
		return false
	}
}

// Title returns a copy of the string s with all Unicode letters that begin words
// mapped to their Unicode title case.
//
// This is a simplified version of `strings.ToTitle` which is deprecated as it doesn't
// handle all Unicode characters correctly. But we don't care about those, so we can
// use this. This avoids having all of `x/text` as a dependency.
func Title(s string) string {
	// Use a closure here to remember state.
	// Hackish but effective. Depends on Map scanning in order and calling
	// the closure once per rune.
	prev := ' '
	return strings.Map(
		func(r rune) rune {
			if unicode.IsSpace(prev) {
				prev = r
				return unicode.ToTitle(r)
			}
			prev = r
			return r
		},
		s)
}

// TruncateText truncates the provided text, if needed, to the specified maximum
// length using the provided truncation indicator in place of the truncated text
// in the specified location of the original string.
func TruncateText(text string, limit int, location TruncationLocation, indicator string) (string, error) {
	ol := len(text)
	if ol <= limit {
		return text, nil
	}
	if len(indicator)+2 >= limit {
		return "", fmt.Errorf("the truncation indicator is too long for the provided text")
	}
	switch location {
	case TruncationLocationMiddle:
		prefix := (limit / 2) - len(indicator)
		suffix := (ol - (prefix + len(indicator))) + 1
		return fmt.Sprintf("%s%s%s", text[:prefix], indicator, text[suffix:]), nil
	case TruncationLocationEnd:
		return text[:limit-(len(indicator)+1)] + indicator, nil
	default:
		return "", fmt.Errorf("invalid truncation location: %d", location)
	}
}
