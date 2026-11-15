/*
Copyright 2026 The Vitess Authors.

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

package sqlparser

import (
	"strconv"
	"strings"
)

// DirectiveRequestDirectives names the marker that stands in for a
// statement's request-only comment directives while VTGate plans the
// statement.
const DirectiveRequestDirectives = "VTGATE_REQUEST_DIRECTIVES"

// requestOnlyDirectives are the comment directives that VTGate reads from
// each request before it looks up the plan, and that don't affect the plan.
var requestOnlyDirectives = map[string]bool{
	strings.ToLower(DirectiveWorkloadName):         true,
	strings.ToLower(DirectivePriority):             true,
	strings.ToLower(DirectiveConsolidator):         true,
	strings.ToLower(DirectiveIgnoreMaxMemoryRows):  true,
	strings.ToLower(DirectiveIgnoreMaxPayloadSize): true,
}

// RequestDirectives are the comment directive blocks of a statement that
// ReplaceRequestDirectives replaced with marker blocks.
type RequestDirectives struct {
	// markers are the marker blocks, and originals the blocks that they
	// replaced.
	markers   []string
	originals []string
}

// ReplaceRequestDirectives replaces each comment directive block among the
// statement-level comments of stmt that has a request-only directive with a
// marker block. The marker block keeps the block's other directives, so
// statements that differ only in their request-only directives format to the
// same text, while the directives that affect the plan stay in it. The
// planner copies a marker block wherever it copies the block, and Restore
// puts the block back.
//
// It returns nil, and leaves stmt as it is, if stmt has no such block, or if
// the text of stmt already contains the marker's name, so that Restore never
// changes text that comes from the statement itself.
func ReplaceRequestDirectives(stmt Statement) *RequestDirectives {
	commented, ok := stmt.(Commented)
	if !ok {
		return nil
	}
	comments := commented.GetParsedComments().GetComments()
	var rd *RequestDirectives
	var replaced Comments
	for i, comment := range comments {
		n := 0
		if rd != nil {
			n = len(rd.markers)
		}
		marker, ok := markerBlock(comment, n)
		if !ok {
			continue
		}
		if rd == nil {
			rd = &RequestDirectives{}
			replaced = append(Comments{}, comments...)
		}
		rd.markers = append(rd.markers, marker)
		rd.originals = append(rd.originals, comment)
		replaced[i] = marker
	}
	// The statement's text, rather than the text that VTGate received, is what
	// the planner generates queries from: an escape sequence in a string
	// literal can hide the name in the latter.
	if rd == nil || strings.Contains(String(stmt), DirectiveRequestDirectives) {
		return nil
	}
	commented.SetComments(replaced)
	return rd
}

// markerBlock returns the marker block for comment, the n-th block of its
// statement with a request-only directive, and whether comment is such a
// block. It reads the directives of comment the way Directives does.
func markerBlock(comment string, n int) (string, bool) {
	if !strings.HasPrefix(comment, commentDirectivePreamble) {
		return "", false
	}
	fields := strings.Fields(comment)
	if len(fields) < 3 {
		return "", false
	}
	var kept []string
	found := false
	for _, field := range fields[1 : len(fields)-1] {
		directive, _, _ := strings.Cut(field, "=")
		if requestOnlyDirectives[strings.ToLower(directive)] {
			found = true
			continue
		}
		kept = append(kept, field)
	}
	if !found {
		return "", false
	}
	name := DirectiveRequestDirectives
	if n > 0 {
		name += "_" + strconv.Itoa(n+1)
	}
	return commentDirectivePreamble + " " + strings.Join(append(kept, name), " ") + " */", true
}

// Restore returns sql, a query that VTGate generated from a statement whose
// request-only directives ReplaceRequestDirectives replaced, with each marker
// block replaced by the block that it stands in for.
func (rd *RequestDirectives) Restore(sql string) string {
	if rd == nil || !strings.Contains(sql, DirectiveRequestDirectives) {
		return sql
	}
	for i, marker := range rd.markers {
		sql = strings.ReplaceAll(sql, marker, rd.originals[i])
	}
	return sql
}
