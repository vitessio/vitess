// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletserver

import (
	"code.google.com/p/vitess/go/vt/tabletserver/proto"
)

const TRAILING_COMMENT = "_trailingComment"

type nomatch struct{}

type matchtracker struct {
	query string
	index int
}

// stripTrailing strips out trailing comments if any and puts them in a bind variable.
// This code is a hack. Will need cleaning if it evolves beyond this.
func stripTrailing(query *proto.Query) {
	tracker := matchtracker{query.Sql, len(query.Sql)}
	pos := tracker.matchComments()
	if pos >= 0 {
		query.Sql = tracker.query[:pos]
		query.BindVariables[TRAILING_COMMENT] = tracker.query[pos:]
	}
}

// restoreTrailing undoes work done by stripTrailing
func restoreTrailing(sql []byte, bindVars map[string]interface{}) []byte {
	if ytcomment, ok := bindVars[TRAILING_COMMENT]; ok {
		sql = append(sql, ytcomment.(string)...)
	}
	return sql
}

// matchComments matches trailing comments. If no comment was found,
// it returns -1. Otherwise, it returns the position where the query ends
// before the trailing comments begin.
func (tracker *matchtracker) matchComments() (pos int) {
	// FIXME: use pos instead of lastpos after compiler bug fix
	lastpos := -1
	defer func() {
		if x := recover(); x != nil {
			_ = x.(nomatch)
			pos = lastpos
		}
	}()

	for {
		// Verify end of comment
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
		tracker.skipBlanks()
		lastpos = tracker.index
	}
	panic("unreachable")
}

func (tracker *matchtracker) mustMatch(required byte) {
	if tracker.index == 0 {
		panic(nomatch{})
	}
	tracker.index--
	if tracker.query[tracker.index] != required {
		panic(nomatch{})
	}
}

func (tracker *matchtracker) match(required byte) bool {
	if tracker.index == 0 {
		panic(nomatch{})
	}
	tracker.index--
	if tracker.query[tracker.index] != required {
		return false
	}
	return true
}

func (tracker *matchtracker) skipBlanks() {
	var ch byte
	for ; tracker.index != 0; tracker.index-- {
		ch = tracker.query[tracker.index-1]
		if ch == ' ' || ch == '\n' || ch == '\r' || ch == '\t' {
			continue
		}
		break
	}
}
