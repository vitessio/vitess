// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletserver

const TRAILING_COMMENT = "_trailingComment"

type nomatch struct{}

type matchtracker struct {
	query string
	index int
}

// stripTrailing strips out the trailing comment if any and puts it in a bind variable.
// This code is a hack. Will need cleaning if it evolves beyond this.
func stripTrailing(query *Query) {
	defer func() {
		if x := recover(); x != nil {
			_ = x.(nomatch)
		}
	}()

	tracker := &matchtracker{query.Sql, len(query.Sql)}
	tracker.mustMatch('/')
	tracker.mustMatch('*')

	// find start of comment
	for {
		if !tracker.match('*') {
			continue
		}
		if tracker.match('/') {
			break
		}
	}

	// find end of query
	for tracker.match(' ') {
	}
	end_query := tracker.index + 1
	query.Sql = tracker.query[:end_query]
	query.BindVariables[TRAILING_COMMENT] = tracker.query[end_query:]
}

// restoreTrailing undoes work done by stripTrailing
func restoreTrailing(sql []byte, bindVars map[string]interface{}) []byte {
	if ytcomment, ok := bindVars[TRAILING_COMMENT]; ok {
		sql = append(sql, ytcomment.(string)...)
	}
	return sql
}

func (self *matchtracker) mustMatch(required byte) {
	if self.index == 0 {
		panic(nomatch{})
	}
	self.index--
	if self.query[self.index] != required {
		panic(nomatch{})
	}
}

func (self *matchtracker) match(required byte) bool {
	if self.index == 0 {
		panic(nomatch{})
	}
	self.index--
	if self.query[self.index] != required {
		return false
	}
	return true
}
