/*
Copyright 2012, Google Inc.
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met:

    * Redistributions of source code must retain the above copyright
notice, this list of conditions and the following disclaimer.
    * Redistributions in binary form must reproduce the above
copyright notice, this list of conditions and the following disclaimer
in the documentation and/or other materials provided with the
distribution.
    * Neither the name of Google Inc. nor the names of its
contributors may be used to endorse or promote products derived from
this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,           
DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY           
THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/

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
