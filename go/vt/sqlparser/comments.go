/*
Copyright 2017 Google Inc.

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
	"unicode"
)

type matchtracker struct {
	query string
	index int
	eof   bool
}

// SplitTrailingComments splits the query trailing comments from the query.
func SplitTrailingComments(sql string) (query, comments string) {
	trimmed := strings.TrimRightFunc(sql, unicode.IsSpace)
	tracker := matchtracker{
		query: trimmed,
		index: len(trimmed),
	}
	pos := tracker.matchComments()
	if pos >= 0 {
		return tracker.query[:pos], tracker.query[pos:]
	}
	return trimmed, ""
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

// StripLeadingComments trims the SQL string and removes any leading comments
func StripLeadingComments(sql string) string {
	sql = strings.TrimFunc(sql, unicode.IsSpace)

	for hasCommentPrefix(sql) {
		switch sql[0] {
		case '/':
			// Multi line comment
			index := strings.Index(sql, "*/")
			if index <= 1 {
				return sql
			}
			// don't strip /*! ... */ or /*!50700 ... */
			if len(sql) > 2 && sql[2] == '!' {
				return sql
			}
			sql = sql[index+2:]
		case '-':
			// Single line comment
			index := strings.Index(sql, "\n")
			if index == -1 {
				return ""
			}
			sql = sql[index+1:]
		}

		sql = strings.TrimFunc(sql, unicode.IsSpace)
	}

	return sql
}

func hasCommentPrefix(sql string) bool {
	return len(sql) > 1 && ((sql[0] == '/' && sql[1] == '*') || (sql[0] == '-' && sql[1] == '-'))
}

// ExtractMysqlComment extracts the version and SQL from a comment-only query
// such as /*!50708 sql here */
func ExtractMysqlComment(sql string) (version string, innerSQL string) {
	sql = sql[3 : len(sql)-2]

	digitCount := 0
	endOfVersionIndex := strings.IndexFunc(sql, func(c rune) bool {
		digitCount++
		return !unicode.IsDigit(c) || digitCount == 6
	})
	version = sql[0:endOfVersionIndex]
	innerSQL = strings.TrimFunc(sql[endOfVersionIndex:], unicode.IsSpace)

	return version, innerSQL
}

const commentDirectivePreamble = "/*vt!"

// CommentDirectives is the parsed representation for execution directives
// conveyed in query comments
type CommentDirectives map[string]interface{}

// ExtractCommentDirectives parses the comment list for any execution directives
// of the form:
//
//     /*vt! OPTION_ONE=1 OPTION_TWO OPTION_THREE=abcd */
//
// It returns the map of the directive values or nil if there aren't any.
func ExtractCommentDirectives(comments Comments) CommentDirectives {
	if comments == nil {
		return nil
	}

	var vals map[string]interface{}

	for _, comment := range comments {
		commentStr := string(comment)
		if commentStr[0:5] != commentDirectivePreamble {
			continue
		}

		if vals == nil {
			vals = make(map[string]interface{})
		}

		// Split on whitespace and ignore the first and last directive
		// since they contain the comment start/end
		directives := strings.Fields(commentStr)
		for i := 1; i < len(directives)-1; i++ {
			directive := directives[i]
			sep := strings.IndexByte(directive, '=')

			// No value is equivalent to a true boolean
			if sep == -1 {
				vals[directive] = true
				continue
			}

			strVal := directive[sep+1:]
			directive = directive[:sep]

			boolVal, err := strconv.ParseBool(strVal)
			if err == nil {
				vals[directive] = boolVal
				continue
			}

			intVal, err := strconv.Atoi(strVal)
			if err == nil {
				vals[directive] = intVal
				continue
			}

			vals[directive] = strVal
		}
	}
	return vals
}

// IsSet checks the directive map for the named directive and returns
// true iff the directive is set and has a true/false or 0/1 value
func (d CommentDirectives) IsSet(key string) bool {
	if d == nil {
		return false
	}

	val, ok := d[key]
	if !ok {
		return false
	}

	boolVal, ok := val.(bool)
	if ok {
		return boolVal
	}

	intVal, ok := val.(int)
	if ok {
		return intVal == 1
	}
	return false
}
