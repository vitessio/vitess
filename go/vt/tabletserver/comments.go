// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletserver

import (
	"github.com/youtube/vitess/go/vt/tabletserver/proto"
)

const TRAILING_COMMENT = "_trailingComment"

type matchtracker struct {
	query string
	index int
	eof   bool
}

// stripTrailing strips out trailing comments if any and puts them in a bind variable.
// This code is a hack. Will need cleaning if it evolves beyond this.
func stripTrailing(query *proto.Query) {
	tracker := matchtracker{
		query: query.Sql,
		index: len(query.Sql),
	}
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
	pos = -1
	for {
		// Verify end of comment
		if !tracker.match('/') {
			return pos
		}
		if !tracker.match('*') {
			return pos
		}

		// find start of comment
		for {
			if !tracker.match('*') {
				if tracker.eof {
					return pos
				}
				continue
			}
			// Skip subsequent '*'
			for tracker.match('*') {
			}
			if tracker.eof {
				return pos
			}
			// See if the last mismatch was a '/'
			if tracker.query[tracker.index] == '/' {
				break
			}
		}
		tracker.skipBlanks()
		pos = tracker.index
	}
}

// match advances to the 'previous' character and returns
// true if it's a match. If it cannot advance any more,
// it returns false and sets the eof flag. tracker.index
// points to the latest position.
func (tracker *matchtracker) match(required byte) bool {
	if tracker.index == 0 {
		tracker.eof = true
		return false
	}
	tracker.index--
	if tracker.query[tracker.index] != required {
		return false
	}
	return true
}

// skipBlanks advances till a non-blank character
// or the beginning of stream is reached. It does
// not set the eof flag. tracker.index points to
// the latest position.
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
