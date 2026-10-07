/*
Copyright 2019 The Vitess Authors.

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
	"fmt"
	"strconv"
	"strings"
	"unicode/utf8"

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sysvars"
	"vitess.io/vitess/go/vt/vterrors"

	querypb "vitess.io/vitess/go/vt/proto/query"
)

const (
	// DirectiveMultiShardAutocommit is the query comment directive to allow
	// single round trip autocommit with a multi-shard statement.
	DirectiveMultiShardAutocommit = "MULTI_SHARD_AUTOCOMMIT"
	// DirectiveSkipQueryPlanCache skips query plan cache when set.
	DirectiveSkipQueryPlanCache = "SKIP_QUERY_PLAN_CACHE"
	// DirectiveQueryTimeout sets a query timeout in vtgate. Only supported for SELECTS.
	DirectiveQueryTimeout = "QUERY_TIMEOUT_MS"
	// DirectiveScatterErrorsAsWarnings enables partial success scatter select queries
	DirectiveScatterErrorsAsWarnings = "SCATTER_ERRORS_AS_WARNINGS"
	// DirectiveIgnoreMaxPayloadSize skips payload size validation when set.
	DirectiveIgnoreMaxPayloadSize = "IGNORE_MAX_PAYLOAD_SIZE"
	// DirectiveIgnoreMaxMemoryRows skips memory row validation when set.
	DirectiveIgnoreMaxMemoryRows = "IGNORE_MAX_MEMORY_ROWS"
	// DirectiveAllowScatter lets scatter plans pass through even when they are turned off by `no_scatter`.
	DirectiveAllowScatter = "ALLOW_SCATTER"
	// DirectiveAllowCrossKeyspaceReads lets cross-keyspace read plans (joins and UNIONs) pass through
	// even when they are turned off by the `--prevent-cross-keyspace-reads` vtgate flag or the
	// `prevent_cross_keyspace_reads` vschema keyspace setting.
	DirectiveAllowCrossKeyspaceReads = "ALLOW_CROSS_KEYSPACE_READS"
	// DirectiveAllowHashJoin lets the planner use hash join if possible
	DirectiveAllowHashJoin = "ALLOW_HASH_JOIN"
	// DirectiveQueryPlanner lets the user specify per query which planner should be used
	DirectiveQueryPlanner = "PLANNER"
	// DirectiveVExplainRunDMLQueries tells vexplain queries/all that it is okay to also run the query.
	DirectiveVExplainRunDMLQueries = "EXECUTE_DML_QUERIES"
	// DirectiveConsolidator enables the query consolidator.
	DirectiveConsolidator = "CONSOLIDATOR"
	// DirectiveWorkloadName specifies the name of the client application workload issuing the query.
	DirectiveWorkloadName = "WORKLOAD_NAME"
	// DirectivePriority specifies the priority of a workload. It should be an integer between 0 and MaxPriorityValue,
	// where 0 is the highest priority, and MaxPriorityValue is the lowest one.
	DirectivePriority = "PRIORITY"

	// MaxPriorityValue specifies the maximum value allowed for the priority query directive. Valid priority values are
	// between zero and MaxPriorityValue.
	MaxPriorityValue = 100

	// OptimizerHintSetVar is the optimizer hint used in MySQL to set the value of a specific session variable for a query.
	OptimizerHintSetVar = "SET_VAR"
)

var ErrInvalidPriority = vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "Invalid priority value specified in query")

// isSQLSpaceRune reports whether r is a space character for SQL. A character
// outside the ASCII range is never one.
func isSQLSpaceRune(r rune) bool {
	return r < utf8.RuneSelf && IsSQLSpace(byte(r))
}

// isNonSpace reports whether r is not a space character for SQL.
func isNonSpace(r rune) bool {
	return !isSQLSpaceRune(r)
}

// leadingCommentEnd returns the first index after all leading comments, or
// 0 if there are no leading comments.
func leadingCommentEnd(text string) (end int) {
	hasComment := false
	pos := 0
	for pos < len(text) {
		// Eat up any whitespace. Trailing whitespace will be considered part of
		// the leading comments.
		nextVisibleOffset := strings.IndexFunc(text[pos:], isNonSpace)
		if nextVisibleOffset < 0 {
			break
		}
		pos += nextVisibleOffset
		remainingText := text[pos:]

		// Found visible characters. Look for '/*' at the beginning
		// and '*/' somewhere after that.
		if len(remainingText) < 4 || remainingText[:2] != "/*" || remainingText[2] == '!' {
			break
		}
		commentLength := 4 + strings.Index(remainingText[2:], "*/")
		if commentLength < 4 {
			// Missing end comment :/
			break
		}

		hasComment = true
		pos += commentLength
	}

	if hasComment {
		return pos
	}
	return 0
}

// trailingCommentStart returns the first index of trailing comments.
// If there are no trailing comments, returns the length of the input string.
//
// A '*/' at the end of the text does not always close a comment. Only the text
// before the '*/' shows if a comment is open. For example, a '/*' in a string
// literal does not start a comment. Also, a '--' or a '#' line comment can end
// with '*/' when no comment is open. For this reason, this function reads the
// text forward from the start. It does not read the text backward from the end.
//
// The callers make a plan for the query that this function returns, and they
// authorize only that query. Then they add the comments to the query again. If
// this function makes a comment out of text that is not a comment, that text
// goes to MySQL, but nothing parses it. For this reason, the scan reads the text
// the way Tokenizer.Scan does, rule for rule: string literals, quoted
// identifiers, variables, numbers, line comments, and /*!...*/ comments at the
// parser's version. FuzzSplitMarginCommentsMatchesTokenizer checks that the
// split matches the one the tokenizer gives, so a change to the tokenizer that
// this scan does not follow fails that test.
func trailingCommentStart(text, version string) (start int) {
	// contentEnd is the index that comes after the last byte of the query. Only
	// block comments and spaces come after contentEnd.
	contentEnd := 0
	inTrailingComment := false
	// contentEndsInLineComment records whether the query stops at the end of a
	// line comment. The guard at the end of this function must know this.
	contentEndsInLineComment := false
	// inVersionedComment records whether the scan is inside a /*!...*/ comment.
	// The '*/' that closes one is part of the query, and the case below has to
	// read both of its bytes together.
	inVersionedComment := false
	pos := 0

	for pos < len(text) {
		if IsSQLSpace(text[pos]) {
			// A space does not start a group of trailing comments, and it does
			// not end one.
			pos++
			continue
		}

		switch {
		case inVersionedComment && text[pos] == '*' && pos+1 < len(text) && text[pos+1] == '/':
			// The '*/' that closes a versioned comment. Read both bytes here,
			// before the line comment check: reading only the '*' would leave the
			// '/' to pair with the next comment's '/*' and look like a '//' line
			// comment, which would swallow a real trailing comment.
			//
			// This case is reached only inside a versioned comment, so a '*/'
			// elsewhere still ends an ordinary comment and a '*' elsewhere is
			// still ordinary SQL.
			pos += 2
			contentEnd = pos
			inVersionedComment = false
			inTrailingComment = false
			contentEndsInLineComment = false

		case isBlockCommentStart(text, pos) && inVersionedComment:
			// Inside a versioned comment the tokenizer reads any '/*', even a
			// '/*!', as a nested comment that ends at the first '*/'. It stays
			// in the query with the versioned comment around it.
			end := blockCommentEnd(text, pos)
			if end < 0 {
				return len(text)
			}
			pos = end

		case isBlockCommentStart(text, pos) && text[pos+2] == '!':
			// A /*!...*/ comment holds SQL that MySQL can run, so it is part of
			// the query and never a margin comment. Where it ends depends on
			// whether its version applies, the same way it does for the
			// tokenizer: see versionedCommentEnd.
			end, applies := versionedCommentEnd(text, pos, version)
			if end < 0 {
				// The comment has no end. Do not split the text.
				return len(text)
			}
			pos = end
			contentEnd = pos
			inVersionedComment = applies
			inTrailingComment = false
			contentEndsInLineComment = false

		case isBlockCommentStart(text, pos):
			end := blockCommentEnd(text, pos)
			if end < 0 {
				// The comment has no end. Do not split the text.
				return len(text)
			}
			inTrailingComment = true
			pos = end

		case isLineCommentStart(text, pos) && (!inVersionedComment || text[pos] != '/'):
			// Inside a versioned comment the tokenizer reads '//' as two '/'
			// operators, not as a line comment.
			newline := strings.IndexByte(text[pos:], '\n')
			if newline < 0 {
				// The line comment continues to the end of the text. Therefore
				// the text does not end with a block comment.
				return len(text)
			}
			// Stop before the newline character. This character ends the line
			// comment. Therefore it must stay with the comments after a split.
			contentEnd = pos + newline
			pos += newline + 1
			inTrailingComment = false
			contentEndsInLineComment = true

		case isLetter(uint16(text[pos])) || isDigit(uint16(text[pos])) || text[pos] == ':' ||
			(text[pos] == '.' && pos+1 < len(text) && isDigit(uint16(text[pos+1]))):
			// A word, a number or a bind variable. Read it whole, as the
			// tokenizer does: a number can take a '-' after its exponent, which
			// must not be read as the start of a '--' comment.
			end := wordEnd(text, pos)
			if end < 0 {
				return len(text)
			}
			pos = end
			contentEnd = pos
			inTrailingComment = false
			contentEndsInLineComment = false

		case text[pos] == '@':
			end := variableEnd(text, pos)
			if end < 0 {
				return len(text)
			}
			pos = end
			contentEnd = pos
			inTrailingComment = false
			contentEndsInLineComment = false

		case text[pos] == '\'' || text[pos] == '"' || text[pos] == '`':
			// A backslash escapes a byte in a string literal. In a quoted
			// identifier, a backslash is not an escape character.
			end := skipQuoted(text, pos, text[pos] != '`')
			if end < 0 {
				// The literal has no end. All of the text after it is in the
				// literal.
				return len(text)
			}
			pos = end
			contentEnd = pos
			inTrailingComment = false
			contentEndsInLineComment = false

		default:
			pos++
			contentEnd = pos
			inTrailingComment = false
			contentEndsInLineComment = false
		}
	}

	if !inTrailingComment {
		return len(text)
	}
	// After we remove the comments, the parser must read the text before them in
	// the same way. A '--' starts a comment only when a space or the end of the
	// text comes after it. Therefore "select 0--" is "select 0". But
	// "select 0--/**/" has two minus signs and no number to subtract. If we
	// split "select 0--/**/", the query "select 0--" makes a good plan. But the
	// full text is a syntax error after we add the comment again.
	//
	// This cannot hide a second statement. Only comments and spaces come after
	// the '--'. Therefore the full text never has a number for the two minus
	// signs, and MySQL always rejects it. We keep the text together for a
	// different reason. We must not authorize one statement and then send a
	// different statement.
	//
	// A line comment can also end with '--', as in "select 1 -- body--". Those
	// two characters are already inside the comment, and the newline that ends
	// the comment stays with the trailing comments. Therefore the query reads
	// the same way with the comments and without them, and this guard must
	// ignore that case. If it does not, the trailing comment never reaches
	// MarginComments.Trailing, and a query rule for that comment stops working.
	if !contentEndsInLineComment && strings.HasSuffix(text[:contentEnd], "--") {
		return len(text)
	}
	return contentEnd
}

// versionedCommentEnd reads the /*!...*/ comment that starts at pos the way
// Tokenizer.scanMySQLSpecificComment does. When the comment's version applies,
// the text inside is SQL: end is the index after the opening, and the caller
// reads on until the closing '*/'. When it does not apply, the whole comment is
// skipped: end is the index after its closing '*/', which is the first '*/'
// outside a comment nested in it. Quotes inside such a comment mean nothing. end
// is -1 when the comment has no end.
//
// The version is the five digits after '/*!'. With fewer digits, the comment
// has no version and always applies.
func versionedCommentEnd(text string, pos int, version string) (end int, applies bool) {
	pos += 3
	digits := 0
	for digits < 5 && pos+digits < len(text) && isDigit(uint16(text[pos+digits])) {
		digits++
	}
	commentVersion := ""
	if digits == 5 {
		commentVersion = text[pos : pos+5]
		pos += 5
	}
	if version >= commentVersion {
		return pos, true
	}
	for pos < len(text) {
		switch {
		case text[pos] == '/' && pos+1 < len(text) && text[pos+1] == '*':
			// A nested comment. Its first '*/' ends it.
			offset := strings.Index(text[pos+2:], "*/")
			if offset < 0 {
				return -1, false
			}
			pos += 2 + offset + 2
		case text[pos] == '*' && pos+1 < len(text) && text[pos+1] == '/':
			return pos + 2, false
		default:
			pos++
		}
	}
	return -1, false
}

// wordEnd returns the index after the identifier, keyword, number or bind
// variable that starts at pos, read the way Tokenizer.Scan reads one. It returns
// -1 where the tokenizer reports an error.
func wordEnd(text string, pos int) int {
	at := func(i int) uint16 {
		if i < len(text) {
			return uint16(text[i])
		}
		return eofChar
	}
	mantissa := func(i, base int) int {
		for digitVal(at(i)) < base {
			i++
		}
		return i
	}
	letters := func(i int, extra func(uint16) bool) int {
		for isLetter(at(i)) || isDigit(at(i)) || extra(at(i)) {
			i++
		}
		return i
	}
	none := func(uint16) bool { return false }

	switch ch := at(pos); {
	case ch == ':':
		// scanBindVarOrAssignmentExpression.
		pos++
		switch {
		case isDigit(at(pos)):
			return mantissa(pos, 10)
		case at(pos) == '=':
			return pos + 1
		case at(pos) == ':':
			pos++
		}
		if !isLetter(at(pos)) {
			return -1
		}
		return letters(pos, func(c uint16) bool { return c == '.' })
	case isLetter(ch):
		// scanIdentifier. A hex or bit literal (x'..', b'..') and N'..' start
		// with a letter too: the letter ends here and the quote that follows is
		// read as a quoted string, which ends where the literal does.
		return letters(pos+1, none)
	}

	// scanNumber.
	float := false
	if at(pos) == '.' {
		float = true
		pos = mantissa(pos+1, 10)
	} else {
		if at(pos) == '0' && (at(pos+1) == 'x' || at(pos+1) == 'X') {
			return numberTail(text, mantissa(pos+2, 16), false)
		}
		if at(pos) == '0' && (at(pos+1) == 'b' || at(pos+1) == 'B') {
			return numberTail(text, mantissa(pos+2, 2), false)
		}
		pos = mantissa(pos, 10)
		if at(pos) == '.' {
			float = true
			pos = mantissa(pos+1, 10)
		}
	}
	if at(pos) == 'e' || at(pos) == 'E' {
		float = true
		pos++
		if at(pos) == '+' || at(pos) == '-' {
			pos++
		}
		pos = mantissa(pos, 10)
	}
	return numberTail(text, pos, float)
}

// numberTail finishes a number the way the end of Tokenizer.scanNumber does: a
// letter right after an integer turns the whole token into an identifier, and a
// letter right after a decimal or float number is an error.
func numberTail(text string, pos int, float bool) int {
	if pos >= len(text) || !isLetter(uint16(text[pos])) {
		return pos
	}
	if float {
		return -1
	}
	for pos < len(text) && (isLetter(uint16(text[pos])) || isDigit(uint16(text[pos]))) {
		pos++
	}
	return pos
}

// variableEnd returns the index after the user or system variable that starts
// with the '@' at pos, read the way Tokenizer.Scan reads one: '@' or '@@', then
// either a backtick-quoted name, or one byte of any kind followed by letters,
// digits, '.', and quote characters. It returns -1 where the tokenizer reports
// an error.
func variableEnd(text string, pos int) int {
	pos++
	if pos < len(text) && text[pos] == '@' {
		pos++
	}
	if pos >= len(text) {
		return -1
	}
	if text[pos] == '`' {
		if pos+1 < len(text) && text[pos+1] == '`' {
			// An empty quoted name.
			return -1
		}
		return skipQuoted(text, pos, false)
	}
	pos++
	for pos < len(text) {
		ch := uint16(text[pos])
		if !isLetter(ch) && !isDigit(ch) && !isCarat(ch) {
			break
		}
		pos++
	}
	return pos
}

// isBlockCommentStart reports whether a block comment starts at pos.
func isBlockCommentStart(text string, pos int) bool {
	return pos+2 < len(text) && text[pos] == '/' && text[pos+1] == '*'
}

// isLineCommentStart reports whether a line comment starts at pos. It uses the
// same rules as the tokenizer. A '#' or a '//' always starts a line comment. A
// '--' starts a line comment only when a space or the end of the text comes
// after it.
func isLineCommentStart(text string, pos int) bool {
	if text[pos] == '#' {
		return true
	}
	if pos+1 >= len(text) {
		return false
	}
	if text[pos] == '/' && text[pos+1] == '/' {
		return true
	}
	if text[pos] != '-' || text[pos+1] != '-' {
		return false
	}
	return pos+2 >= len(text) || IsSQLSpace(text[pos+2])
}

// sqlSpaceChars holds the characters that MySQL treats as a space between two
// tokens: space, tab, newline, vertical tab, form feed and carriage return.
//
// This string is the only place that spells out the set. IsSQLSpace reads it
// through sqlSpaceTable, and the trims in SplitMarginComments take it as a
// cutset, so no second copy can drift from this one. Tokenizer.skipBlank and the
// '--' rule in Tokenizer.Scan repeat the set because they read a uint16 rather
// than a byte, and TestMarginCommentRulesMatchTokenizer checks that all of them
// stay equal.
//
// Do not use unicode.IsSpace instead. That function also accepts a no-break space
// and other characters that MySQL does not accept, and then Vitess would remove a
// character that MySQL wants to see.
const sqlSpaceChars = " \t\n\v\f\r"

// sqlSpaceTable answers IsSQLSpace with one index instead of a comparison for
// each character in the set.
var sqlSpaceTable = func() (table [256]bool) {
	for i := range len(sqlSpaceChars) {
		table[sqlSpaceChars[i]] = true
	}
	return table
}()

// IsSQLSpace reports whether c is one of the characters in sqlSpaceChars, the
// set MySQL treats as a space between two tokens.
//
// This is exported so that other packages which scan SQL a byte at a time can
// ask this one, rather than keeping a copy of the set that drifts from it.
func IsSQLSpace(c byte) bool {
	return sqlSpaceTable[c]
}

// blockCommentEnd returns the index that comes after the '*/' at the end of the
// block comment that starts at pos. It returns -1 if the comment has no end. A
// block comment cannot contain a second block comment. Therefore the first '*/'
// ends the comment.
func blockCommentEnd(text string, pos int) int {
	offset := strings.Index(text[pos+2:], "*/")
	if offset < 0 {
		return -1
	}
	return pos + 2 + offset + 2
}

// skipQuoted returns the index that comes after the delimiter at the end of the
// string literal or the quoted identifier that starts at pos. It returns -1 if
// there is no end. Two delimiters together are one delimiter in the text. When
// backslashEscapes is true, a backslash makes the next byte part of the text.
// Give true for a '..' or a ".." literal. Give false for a `..` identifier,
// because a backslash is not an escape character in an identifier.
//
// A literal can be long, for example a JSON blob in an INSERT statement.
// Therefore the code searches for the delimiter with IndexByte instead of a loop
// over each byte. A backslash can hide the delimiter, but a backslash is rare.
// Therefore the code looks for one only one time for each candidate, and then it
// calls walkQuoted.
func skipQuoted(text string, pos int, backslashEscapes bool) int {
	delim := text[pos]
	for i := pos + 1; i < len(text); {
		rel := strings.IndexByte(text[i:], delim)
		if rel < 0 {
			return -1
		}
		end := i + rel

		if backslashEscapes && strings.IndexByte(text[i:end], '\\') >= 0 {
			// This delimiter can be escaped, and so can the next one. A walk
			// from here reads each byte one time. A new search would read the
			// same bytes again for every backslash.
			return walkQuoted(text, i, delim)
		}

		if end+1 < len(text) && text[end+1] == delim {
			i = end + 2 // two delimiters together, so this is not the end
			continue
		}
		return end + 1
	}
	return -1
}

// walkQuoted completes skipQuoted for a literal that contains a backslash. It
// reads one byte at a time from pos, which is always the start of a byte that no
// backslash escapes.
func walkQuoted(text string, pos int, delim byte) int {
	for pos < len(text) {
		switch c := text[pos]; {
		case c == '\\':
			// Step over the backslash and the byte that it escapes. A backslash
			// at the end moves pos past the end, and then the loop stops.
			pos += 2
		case c != delim:
			pos++
		case pos+1 < len(text) && text[pos+1] == delim:
			pos += 2 // two delimiters together, so this is not the end
		default:
			return pos + 1
		}
	}
	return -1
}

// MarginComments holds the leading and trailing comments that surround a query.
type MarginComments struct {
	Leading  string
	Trailing string
}

// SplitMarginComments pulls out any leading or trailing comments from a raw sql
// query, reading /*!...*/ comments at the default MySQL version. It is for
// callers that only display a query. Callers that plan or authorize the query
// use Parser.SplitMarginComments with the parser that parses it.
func SplitMarginComments(sql string) (query string, comments MarginComments) {
	return defaultMarginParser.SplitMarginComments(sql)
}

// defaultMarginParser reads versioned comments for SplitMarginComments at the
// default MySQL version.
var defaultMarginParser = func() *Parser {
	parser, err := New(Options{})
	if err != nil {
		panic(err)
	}
	return parser
}()

// SplitMarginComments pulls out any leading or trailing comments from a raw sql
// query, and trims leading (if there's a comment) and trailing whitespace. It
// reads a /*!...*/ comment at the parser's MySQL version, as the parser does, so
// callers that plan or authorize the query must split it with the parser that
// parses it.
func (p *Parser) SplitMarginComments(sql string) (query string, comments MarginComments) {
	trailingStart := trailingCommentStart(sql, p.version)
	leadingEnd := leadingCommentEnd(sql[:trailingStart])
	comments = MarginComments{
		Leading:  strings.TrimLeft(sql[:leadingEnd], sqlSpaceChars),
		Trailing: strings.TrimRight(sql[trailingStart:], sqlSpaceChars),
	}
	return strings.Trim(sql[leadingEnd:trailingStart], sqlSpaceChars+";"), comments
}

// StripLeadingComments trims the SQL string and removes any leading comments
func StripLeadingComments(sql string) string {
	// Trim with the SQL space set, not unicode.IsSpace: a no-break space is part
	// of an identifier to MySQL, so removing one here would change the statement
	// this reports on.
	sql = strings.Trim(sql, sqlSpaceChars)

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

		sql = strings.Trim(sql, sqlSpaceChars)
	}

	return sql
}

func hasCommentPrefix(sql string) bool {
	return len(sql) > 1 && ((sql[0] == '/' && sql[1] == '*') || (sql[0] == '-' && sql[1] == '-'))
}

const commentDirectivePreamble = "/*vt+"

// CommentDirectives is the parsed representation for execution directives
// conveyed in query comments
type CommentDirectives struct {
	m map[string]string
}

// ResetDirectives sets the _directives member to `nil`, which means the next call to Directives()
// will re-evaluate it.
func (c *ParsedComments) ResetDirectives() {
	if c == nil {
		return
	}
	c._directives = nil
}

// Directives parses the comment list for any execution directives
// of the form:
//
//	/*vt+ OPTION_ONE=1 OPTION_TWO OPTION_THREE=abcd */
//
// It returns the map of the directive values or nil if there aren't any.
func (c *ParsedComments) Directives() *CommentDirectives {
	if c == nil {
		return nil
	}
	if c._directives == nil {
		c._directives = &CommentDirectives{m: make(map[string]string)}

		for _, commentStr := range c.comments {
			if commentStr[0:5] != commentDirectivePreamble {
				continue
			}

			// Split on whitespace and ignore the first and last directive
			// since they contain the comment start/end
			directives := strings.Fields(commentStr)
			for i := 1; i < len(directives)-1; i++ {
				directive, val, ok := strings.Cut(directives[i], "=")
				if !ok {
					val = "true"
				}
				c._directives.m[strings.ToLower(directive)] = val
			}
		}
	}
	return c._directives
}

// GetMySQLSetVarValue gets the value of the given variable if it is part of a /*+ SET_VAR() */ MySQL optimizer hint.
func (c *ParsedComments) GetMySQLSetVarValue(key string) string {
	if c == nil {
		// If we have no parsed comments, then we return an empty string.
		return ""
	}
	for _, commentStr := range c.comments {
		// Skip all the comments that don't start with the query optimizer prefix.
		if commentStr[0:3] != queryOptimizerPrefix {
			continue
		}

		pos := 4
		for pos < len(commentStr) {
			// Go over the entire comment and extract an optimizer hint.
			// We get back the final position of the cursor, along with the start and end of
			// the optimizer hint name and content.
			finalPos, ohNameStart, ohNameEnd, ohContentStart, ohContentEnd := getOptimizerHint(pos, commentStr)
			pos = finalPos + 1
			// If we didn't find an optimizer hint or if it was malformed, we skip it.
			if ohContentEnd == -1 {
				break
			}
			// Construct the name and the content from the starts and ends.
			ohName := commentStr[ohNameStart:ohNameEnd]
			ohContent := commentStr[ohContentStart:ohContentEnd]
			// Check if the optimizer hint name matches `SET_VAR`.
			if strings.EqualFold(strings.TrimSpace(ohName), OptimizerHintSetVar) {
				// If it does, then we cut the string at the first occurrence of "=".
				// That gives us the name of the variable, and the value that it is being set to.
				// If the variable matches what we are looking for, we return its value.
				setVarName, setVarValue, isValid := strings.Cut(ohContent, "=")
				if !isValid {
					continue
				}
				if strings.EqualFold(strings.TrimSpace(setVarName), key) {
					return strings.TrimSpace(setVarValue)
				}
			}
		}

		// MySQL only parses the first comment that has the optimizer hint prefix. The following ones are ignored.
		return ""
	}
	return ""
}

// GetMySQLSetVarNames gets the variable names used in /*+ SET_VAR() */ MySQL optimizer hints.
func (c *ParsedComments) GetMySQLSetVarNames() []string {
	if c == nil {
		return nil
	}
	for _, commentStr := range c.comments {
		if commentStr[0:3] != queryOptimizerPrefix {
			continue
		}

		var names []string
		pos := 4
		for pos < len(commentStr) {
			finalPos, ohNameStart, ohNameEnd, ohContentStart, ohContentEnd := getOptimizerHint(pos, commentStr)
			pos = finalPos + 1
			if ohContentEnd == -1 {
				break
			}

			ohName := commentStr[ohNameStart:ohNameEnd]
			ohContent := commentStr[ohContentStart:ohContentEnd]
			if strings.EqualFold(strings.TrimSpace(ohName), OptimizerHintSetVar) {
				setVarName, _, isValid := strings.Cut(ohContent, "=")
				if !isValid {
					continue
				}
				setVarName = strings.TrimSpace(setVarName)
				if setVarName != "" {
					names = append(names, setVarName)
				}
			}
		}

		return names
	}
	return nil
}

// SetMySQLSetVarValue updates or sets the value of the given variable as part of a /*+ SET_VAR() */ MySQL optimizer hint.
func (c *ParsedComments) SetMySQLSetVarValue(key string, value string) (newComments Comments) {
	if c == nil {
		// If we have no parsed comments, then we create a new one with the required optimizer hint and return it.
		newComments = append(newComments, fmt.Sprintf("/*+ %v(%v=%v) */", OptimizerHintSetVar, key, value))
		return
	}
	seenFirstOhComment := false
	for _, commentStr := range c.comments {
		// Skip all the comments that don't start with the query optimizer prefix.
		// Also, since MySQL only parses the first comment that has the optimizer hint prefix and ignores the following ones,
		// we skip over all the comments that come after we have seen the first comment with the optimizer hint.
		if seenFirstOhComment || commentStr[0:3] != queryOptimizerPrefix {
			newComments = append(newComments, commentStr)
			continue
		}

		seenFirstOhComment = true
		finalComment := "/*+"
		keyPresent := false
		pos := 4
		var finalCommentSb342 strings.Builder
		for pos < len(commentStr) {
			// Go over the entire comment and extract an optimizer hint.
			// We get back the final position of the cursor, along with the start and end of
			// the optimizer hint name and content.
			finalPos, ohNameStart, ohNameEnd, ohContentStart, ohContentEnd := getOptimizerHint(pos, commentStr)
			pos = finalPos + 1
			// If we didn't find an optimizer hint or if it was malformed, we skip it.
			if ohContentEnd == -1 {
				break
			}
			// Construct the name and the content from the starts and ends.
			ohName := commentStr[ohNameStart:ohNameEnd]
			ohContent := commentStr[ohContentStart:ohContentEnd]
			// Check if the optimizer hint name matches `SET_VAR`.
			if strings.EqualFold(strings.TrimSpace(ohName), OptimizerHintSetVar) {
				// If it does, then we cut the string at the first occurrence of "=".
				// That gives us the name of the variable, and the value that it is being set to.
				// If the variable matches what we are looking for, we can change its value.
				// Otherwise we add the comment as is to our final comments and move on.
				setVarName, _, isValid := strings.Cut(ohContent, "=")
				if !isValid || !strings.EqualFold(strings.TrimSpace(setVarName), key) {
					fmt.Fprintf(&finalCommentSb342, " %v(%v)", ohName, ohContent)
					continue
				}
				if strings.EqualFold(strings.TrimSpace(setVarName), key) {
					keyPresent = true
					fmt.Fprintf(&finalCommentSb342, " %v(%v=%v)", ohName, strings.TrimSpace(setVarName), value)
				}
			} else {
				// If it doesn't match, we add it to our final comment and move on.
				fmt.Fprintf(&finalCommentSb342, " %v(%v)", ohName, ohContent)
			}
		}
		finalComment += finalCommentSb342.String()
		// If we haven't found any SET_VAR optimizer hint with the matching variable,
		// then we add a new optimizer hint to introduce this variable.
		if !keyPresent {
			finalComment += fmt.Sprintf(" %v(%v=%v)", OptimizerHintSetVar, key, value)
		}

		finalComment += " */"
		newComments = append(newComments, finalComment)
	}
	// If we have not seen even a single comment that has the optimizer hint prefix,
	// then we add a new optimizer hint to introduce this variable.
	if !seenFirstOhComment {
		newComments = append(newComments, fmt.Sprintf("/*+ %v(%v=%v) */", OptimizerHintSetVar, key, value))
	}
	return newComments
}

// getOptimizerHint goes over the comment string from the given initial position.
// It returns back the final position of the cursor, along with the start and end of
// the optimizer hint name and content.
func getOptimizerHint(initialPos int, commentStr string) (pos int, ohNameStart int, ohNameEnd int, ohContentStart int, ohContentEnd int) {
	ohContentEnd = -1
	// skip spaces as they aren't interesting.
	pos = skipBlanks(initialPos, commentStr)
	ohNameStart = pos
	pos++
	// All characters until we get a space of a opening bracket are part of the optimizer hint name.
	for pos < len(commentStr) {
		if commentStr[pos] == ' ' || commentStr[pos] == '(' {
			break
		}
		pos++
	}
	// Mark the end of the optimizer hint name and skip spaces.
	ohNameEnd = pos
	pos = skipBlanks(pos, commentStr)
	// Verify that the comment is not malformed. If it doesn't contain an opening bracket
	// at the current position, then something is wrong.
	if pos >= len(commentStr) || commentStr[pos] != '(' {
		return
	}
	// Seeing the opening bracket, marks the start of the optimizer hint content.
	// We skip over the comment until we see the end of the parenthesis.
	pos++
	ohContentStart = pos
	pos = skipUntilParenthesisEnd(pos, commentStr)
	ohContentEnd = pos
	return
}

// skipUntilParenthesisEnd reads the comment string given the initial position and skips over until
// it has seen the end of opening bracket.
func skipUntilParenthesisEnd(pos int, commentStr string) int {
	for pos < len(commentStr) {
		switch commentStr[pos] {
		case ')':
			// If we see a closing bracket, we have found the ending of our parenthesis.
			return pos
		case '\'':
			// If we see a single quote character, then it signifies the start of a new string.
			// We wait until we see the end of this string.
			pos++
			pos = skipUntilCharacter(pos, commentStr, '\'')
		case '"':
			// If we see a double quote character, then it signifies the start of a new string.
			// We wait until we see the end of this string.
			pos++
			pos = skipUntilCharacter(pos, commentStr, '"')
		}
		pos++
	}

	return pos
}

// skipUntilCharacter skips until the given character has been seen in the comment string, given the starting position.
func skipUntilCharacter(pos int, commentStr string, ch byte) int {
	for pos < len(commentStr) {
		if commentStr[pos] != ch {
			pos++
			continue
		}
		break
	}
	return pos
}

// skipBlanks skips over space characters from the comment string, given the starting position.
func skipBlanks(pos int, commentStr string) int {
	for pos < len(commentStr) {
		if commentStr[pos] == ' ' {
			pos++
			continue
		}
		break
	}
	return pos
}

func (c *ParsedComments) Length() int {
	if c == nil {
		return 0
	}
	return len(c.comments)
}

func (c *ParsedComments) GetComments() Comments {
	if c != nil {
		return c.comments
	}
	return nil
}

func (c *ParsedComments) Prepend(comment string) Comments {
	if c == nil {
		return Comments{comment}
	}
	comments := make(Comments, 0, len(c.comments)+1)
	comments = append(comments, comment)
	comments = append(comments, c.comments...)
	return comments
}

// IsSet checks the directive map for the named directive and returns
// true if the directive is set and has a true/false or 0/1 value
func (d *CommentDirectives) IsSet(key string) bool {
	if d == nil {
		return false
	}
	val, found := d.m[strings.ToLower(key)]
	if !found {
		return false
	}
	// ParseBool handles "0", "1", "true", "false" and all similars
	set, _ := strconv.ParseBool(val)
	return set
}

// GetString gets a directive value as string, with default value if not found
func (d *CommentDirectives) GetString(key string, defaultVal string) (string, bool) {
	if d == nil {
		return "", false
	}
	val, ok := d.m[strings.ToLower(key)]
	if !ok {
		return defaultVal, false
	}
	if unquoted, err := strconv.Unquote(val); err == nil {
		return unquoted, true
	}
	return val, true
}

// MultiShardAutocommitDirective returns true if multishard autocommit directive is set to true in query.
func MultiShardAutocommitDirective(stmt Statement) bool {
	return checkDirective(stmt, DirectiveMultiShardAutocommit)
}

// IgnoreMaxPayloadSizeDirective returns true if the max payload size override
// directive is set to true.
func IgnoreMaxPayloadSizeDirective(stmt Statement) bool {
	switch stmt := stmt.(type) {
	// For transactional statements, they should always be passed down and
	// should not come into max payload size requirement.
	case *Begin, *Commit, *Rollback, *Savepoint, *SRollback, *Release:
		return true
	default:
		return checkDirective(stmt, DirectiveIgnoreMaxPayloadSize)
	}
}

// IgnoreMaxMaxMemoryRowsDirective returns true if the max memory rows override
// directive is set to true.
func IgnoreMaxMaxMemoryRowsDirective(stmt Statement) bool {
	return checkDirective(stmt, DirectiveIgnoreMaxMemoryRows)
}

// AllowScatterDirective returns true if the allow scatter override is set to true
func AllowScatterDirective(stmt Statement) bool {
	return checkDirective(stmt, DirectiveAllowScatter)
}

// AllowCrossKeyspaceReadsDirective returns true if the allow cross-keyspace reads override is set to true
func AllowCrossKeyspaceReadsDirective(stmt Statement) bool {
	return checkDirective(stmt, DirectiveAllowCrossKeyspaceReads)
}

func checkDirective(stmt Statement, key string) bool {
	cmt, ok := stmt.(Commented)
	if ok {
		return cmt.GetParsedComments().Directives().IsSet(key)
	}
	return false
}

type QueryHints struct {
	IgnoreMaxMemoryRows bool
	Consolidator        querypb.ExecuteOptions_Consolidator
	Workload            string
	ForeignKeyChecks    *bool
	Priority            string
	Timeout             *int
}

func BuildQueryHints(stmt Statement) (qh QueryHints, err error) {
	qh = QueryHints{}

	comment, ok := stmt.(Commented)
	if !ok {
		return qh, nil
	}

	directives := comment.GetParsedComments().Directives()

	qh.Priority, err = getPriority(directives)
	if err != nil {
		return qh, err
	}
	qh.IgnoreMaxMemoryRows = directives.IsSet(DirectiveIgnoreMaxMemoryRows)
	qh.Consolidator = getConsolidator(stmt, directives)
	qh.Workload = getWorkload(directives)
	qh.ForeignKeyChecks = getForeignKeyChecksState(comment)
	qh.Timeout = getQueryTimeout(directives)

	return qh, nil
}

// getConsolidator returns the consolidator option.
func getConsolidator(stmt Statement, directives *CommentDirectives) querypb.ExecuteOptions_Consolidator {
	if _, isSelect := stmt.(SelectStatement); !isSelect {
		return querypb.ExecuteOptions_CONSOLIDATOR_UNSPECIFIED
	}
	strv, isSet := directives.GetString(DirectiveConsolidator, "")
	if !isSet {
		return querypb.ExecuteOptions_CONSOLIDATOR_UNSPECIFIED
	}
	if i32v, ok := querypb.ExecuteOptions_Consolidator_value["CONSOLIDATOR_"+strings.ToUpper(strv)]; ok {
		return querypb.ExecuteOptions_Consolidator(i32v)
	}
	return querypb.ExecuteOptions_CONSOLIDATOR_UNSPECIFIED
}

// getWorkload gets the workload name from the provided Statement, using workloadLabel as the name of
// the query directive that specifies it.
func getWorkload(directives *CommentDirectives) string {
	workloadName, _ := directives.GetString(DirectiveWorkloadName, "")
	return workloadName
}

// getForeignKeyChecksState returns the state of foreign_key_checks variable if it is part of a SET_VAR optimizer hint in the comments.
func getForeignKeyChecksState(cmt Commented) *bool {
	fkChecksVal := cmt.GetParsedComments().GetMySQLSetVarValue(sysvars.ForeignKeyChecks)
	// If the value of the `foreign_key_checks` optimizer hint is something that doesn't make sense,
	// then MySQL just ignores it and treats it like the case, where it is unspecified. We are choosing
	// to have the same behaviour here. If the value doesn't match any of the acceptable values, we return nil,
	// that signifies that no value was specified.
	switch strings.ToLower(fkChecksVal) {
	case "on", "1":
		fkState := true
		return &fkState
	case "off", "0":
		fkState := false
		return &fkState
	}
	return nil
}

// getPriority gets the priority from the provided Statement, using DirectivePriority
func getPriority(directives *CommentDirectives) (string, error) {
	priority, ok := directives.GetString(DirectivePriority, "")
	if !ok || priority == "" {
		return "", nil
	}

	intPriority, err := strconv.Atoi(priority)
	if err != nil || intPriority < 0 || intPriority > MaxPriorityValue {
		return "", ErrInvalidPriority
	}

	return priority, nil
}

// getQueryTimeout gets the query timeout from the provided Statement, using DirectiveQueryTimeout
func getQueryTimeout(directives *CommentDirectives) *int {
	timeoutString, ok := directives.GetString(DirectiveQueryTimeout, "")
	if !ok || timeoutString == "" {
		return nil
	}

	timeout, err := strconv.Atoi(timeoutString)
	if err != nil || timeout < 0 {
		return nil
	}
	return &timeout
}
