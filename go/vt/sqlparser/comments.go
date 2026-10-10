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

// marginBounds returns the part [start, end) of sql that is not a margin
// comment. Only spaces and block comments come before start or after end.
//
// newlineFirst reports that the query ends with a line comment and a ';' after
// it. SplitMarginComments trims the ';' and, with it, the newline that ends the
// line comment, so it must put a newline before the trailing comments. Without
// it the line comment would run on into them when the callers add them again.
//
// Only text that ends with '*/' can have trailing comments, and most queries do
// not, so any other text is split at its leading comments alone. Those need no
// tokenizer: with nothing before it, a '/*' always opens a comment and the first
// '*/' after it closes the comment.
func (p *Parser) marginBounds(sql string) (start, end int, newlineFirst bool) {
	end = len(sql)
	for end > 0 && IsSQLSpace(sql[end-1]) {
		end--
	}
	if !strings.HasSuffix(sql[:end], "*/") {
		return leadingCommentEnd(sql), len(sql), false
	}
	start, end, newlineFirst, _ = p.tokenizedMarginBounds(sql)
	return start, end, newlineFirst
}

// tokenizedMarginBounds is marginBounds for any text. It reads sql with the
// tokenizer the parser uses. ok is false when the tokenizer rejects sql, and
// then nothing is split off: the parser reads all of sql and rejects it.
//
// A '*/' at the end of the text does not always close a comment. Only the text
// before it shows whether a comment is open: a '/*' in a string literal does not
// start one, and a line comment can end with '*/'. The callers make a plan for
// the query that SplitMarginComments returns, and they authorize only that
// query. Then they add the comments to the query again. If the split made a
// comment out of text that is not a comment, that text would go to MySQL with
// nothing having parsed it. Reading the text with the parser's own tokenizer
// makes the split agree with the parser on literals, quoted identifiers and
// comments.
//
// Only block comment tokens and the spaces around them are margin. Everything
// else the tokenizer reads is statement: every other token, a line comment up
// to its newline, and the bytes of a /*!...*/ comment that Scan steps over
// without returning a token, which show up between two tokens. MySQL can run
// what is inside a /*!...*/ comment, even one whose version does not apply to
// this parser, so it must never land in a margin.
func (p *Parser) tokenizedMarginBounds(sql string) (start, end int, newlineFirst, ok bool) {
	tkn := p.NewStringTokenizer(sql)
	start = -1
	keep := func(from, to int) {
		if start < 0 {
			start = from
		}
		end = to
	}
	// lastTyp and stmtEnd describe the query without the ';' tokens at its
	// end, which SplitMarginComments trims.
	var lastTyp, stmtEnd int
	for {
		before := tkn.Pos
		typ := tkn.ScanSkip()
		if typ == LEX_ERROR {
			return 0, len(sql), false, false
		}
		// Bytes other than spaces between two tokens belong to a /*!...*/
		// comment that Scan stepped over.
		from, to := before, tkn.currStart
		for from < to && IsSQLSpace(sql[from]) {
			from++
		}
		if from < to {
			for IsSQLSpace(sql[to-1]) {
				to--
			}
			keep(from, to)
			stmtEnd = end
		}
		switch {
		case typ == 0:
			if start < 0 {
				// Only comments and spaces. They all go to Trailing.
				return 0, 0, false, true
			}
			// Without a trailing comment nothing is cut off the end, and the
			// query reads the same.
			if strings.Trim(sql[end:], sqlSpaceChars) == "" {
				return start, end, false, true
			}
			// "select 0--/**/" and "select 0--;/**/": cutting off the comment
			// and trimming the ';' would leave a '--' that the end of the text
			// turns into a comment, so the callers would plan "select 0" for a
			// statement MySQL reads as two minus signs.
			//
			// The leading comments are at the start of the text, so cutting them
			// off changes nothing after them.
			if lastTyp == '-' && strings.HasSuffix(sql[:stmtEnd], "--") {
				return start, len(sql), false, true
			}
			// "select 1 -- x\n;/**/": the trim removes the newline that ends the
			// line comment.
			return start, end, lastTyp == COMMENT && stmtEnd < end, true
		case typ == COMMENT && strings.HasPrefix(sql[tkn.currStart:], "/*"):
			// A block comment: a margin, if nothing but comments follows it.
		case typ == COMMENT:
			// A line comment. Its newline stays outside, because the newline
			// ends the comment when the margin is added again.
			keep(tkn.currStart, tkn.currStart+len(strings.TrimSuffix(sql[tkn.currStart:tkn.Pos], "\n")))
			lastTyp, stmtEnd = typ, end
		case typ == ';':
			keep(tkn.currStart, tkn.Pos)
		default:
			keep(tkn.currStart, tkn.Pos)
			lastTyp, stmtEnd = typ, end
		}
	}
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
	start, end, newlineFirst := p.marginBounds(sql)
	comments = MarginComments{
		Leading:  strings.TrimLeft(sql[:start], sqlSpaceChars),
		Trailing: strings.TrimRight(sql[end:], sqlSpaceChars),
	}
	if newlineFirst {
		comments.Trailing = "\n" + comments.Trailing
	}
	return trimQuery(sql[start:end]), comments
}

// trimQuery removes the spaces and ';' characters at both ends of query. It
// does what strings.Trim with that cutset does, but SplitMarginComments runs
// for every query, and strings.Trim builds its cutset set on every call.
func trimQuery(query string) string {
	isCut := func(c byte) bool { return c == ';' || IsSQLSpace(c) }
	for len(query) > 0 && isCut(query[0]) {
		query = query[1:]
	}
	for len(query) > 0 && isCut(query[len(query)-1]) {
		query = query[:len(query)-1]
	}
	return query
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

// RewriteDoubleSlashComments replaces the "//" that starts each line comment
// in sql with "#/", and reports whether it replaced any. Vitess reads "//" as
// a comment to the end of the line, but MySQL has no such comment: it reads
// the first "/" as division and goes on to read the rest of the line. MySQL
// ends a "#" comment where Vitess ends a "//" comment, at the next '\n', so
// after the rewrite MySQL skips exactly the text that Vitess skipped. Unlike
// the "-" of "-- ", a "#" never joins the token before it (as in "1e-"), and
// the rewrite keeps every offset in sql unchanged.
//
// Statement text that Vitess forwards to MySQL as written must go through
// this rewrite first. Text that Vitess generates from a parsed statement
// carries no comment to rewrite.
func (p *Parser) RewriteDoubleSlashComments(sql string) (string, bool) {
	if !strings.Contains(sql, "//") {
		return sql, false
	}
	tokenizer := p.NewStringTokenizer(sql)
	var buf strings.Builder
	copied := 0
	for {
		pos := tokenizer.Pos
		typ, val := tokenizer.Scan()
		switch typ {
		case LEX_ERROR:
			// Some callers forward text that does not parse, and split it
			// with a tokenizer that reads on past a lexing error. Read on
			// too, so that MySQL skips every comment that tokenizer skips.
			if tokenizer.Pos > pos {
				continue
			}
			fallthrough
		case 0:
			if copied == 0 {
				return sql, false
			}
			buf.WriteString(sql[copied:])
			return buf.String(), true
		case COMMENT:
			if !strings.HasPrefix(val, "//") {
				continue
			}
			start := tokenizer.Pos - len(val)
			if copied == 0 {
				buf.Grow(len(sql))
			}
			buf.WriteString(sql[copied:start])
			buf.WriteByte('#')
			copied = start + 1
		}
	}
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
