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

// sqlSpaceChars holds the characters that MySQL treats as a space between two
// tokens: space, tab, newline, vertical tab, form feed and carriage return.
//
// This string is the only place in this file that spells out the set.
// IsSQLSpace reads it through sqlSpaceTable, and the trims in SplitMarginComments
// take it as a cutset. Tokenizer.skipBlank and the '--' rule in Tokenizer.Scan
// repeat the set because they read a uint16 rather than a byte, and
// TestIsSQLSpaceMatchesTokenizer checks that skipBlank stays equal to it.
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
// query, using a parser at the default MySQL version.
//
// Callers that plan or authorize the query must use Parser.SplitMarginComments
// with the parser that parses the query, so that both read the text the same way.
func SplitMarginComments(sql string) (query string, comments MarginComments) {
	return defaultMarginParser.SplitMarginComments(sql)
}

var defaultMarginParser = NewTestParser()

// SplitMarginComments pulls out any leading or trailing comments from a raw sql
// query. This function also trims leading (if there's a comment) and trailing
// whitespace.
//
// The callers plan and authorize only the query this returns, and then add the
// comments back before the text goes to MySQL. Nothing parses the comments, so
// they must not hold anything MySQL reads as SQL. To make sure of that, the
// split reads sql with the same tokenizer the parser uses, rather than with a
// separate scan that would have to agree with it about literals, quoted
// identifiers and comments.
func (p *Parser) SplitMarginComments(sql string) (query string, comments MarginComments) {
	start, end := p.statementBounds(sql)
	return splitAt(sql, start, end)
}

// splitAt splits sql into the margins before start and after end and the query
// between them, and trims the spaces around each part and a ';' at either end
// of the query.
func splitAt(sql string, start, end int) (query string, comments MarginComments) {
	comments = MarginComments{
		Leading:  strings.TrimLeft(sql[:start], sqlSpaceChars),
		Trailing: strings.TrimRight(sql[end:], sqlSpaceChars),
	}
	return strings.Trim(sql[start:end], sqlSpaceChars+";"), comments
}

// statementBounds returns the part [start, end) of sql that is not a margin
// comment. Only block comments and spaces come before start or after end.
//
// Only a text that ends with '*/' can have a trailing margin, and only the
// trailing margin needs the tokenizer: whether a '*/' at the end closes a
// comment depends on everything before it. Any other text is split at its
// leading comments alone, which leadingCommentsEnd finds without the tokenizer.
func (p *Parser) statementBounds(sql string) (start, end int) {
	end = len(sql)
	for end > 0 && IsSQLSpace(sql[end-1]) {
		end--
	}
	if !strings.HasSuffix(sql[:end], "*/") {
		return leadingCommentsEnd(sql[:end]), end
	}
	start, end, _ = p.scannedBounds(sql)
	return start, end
}

// leadingCommentsEnd returns the index of the first byte of text that is not a
// space or part of a block comment at the start of text. With nothing before
// it, a '/*' always opens a comment and the first '*/' after it closes the
// comment, so this needs no tokenizer. A /*!...*/ comment is part of the
// statement, and so is a comment with no end, which the parser rejects.
func leadingCommentsEnd(text string) int {
	pos := 0
	for {
		for pos < len(text) && IsSQLSpace(text[pos]) {
			pos++
		}
		rest := text[pos:]
		if !strings.HasPrefix(rest, "/*") || strings.HasPrefix(rest, "/*!") {
			return pos
		}
		closeAt := strings.Index(rest[2:], "*/")
		if closeAt < 0 {
			return pos
		}
		pos += 2 + closeAt + 2
	}
}

// scannedBounds is statementBounds for any text, read with the tokenizer. ok
// is false when the tokenizer reports an error.
//
// Everything the tokenizer reads, other than a block comment token and the
// spaces around it, counts as part of the statement:
//
//   - A line comment. A margin is re-attached as is, and a trailing line comment
//     would swallow what a caller appends after it. Its newline stays outside,
//     because the newline ends the comment when the margin is re-attached.
//   - A /*!...*/ comment, whether or not its version applies. MySQL can run
//     what is inside such a comment, and a backend can be newer than the
//     version this parser uses, so it must never land in a margin. Scan steps
//     over the comment's opening and closing, or over all of it when the version
//     does not apply, without returning a token, and records where in
//     skippedEnd.
//
// If the tokenizer reports an error, there is no split: the parser reads all of
// sql and rejects it.
func (p *Parser) scannedBounds(sql string) (start, end int, ok bool) {
	tkn := &Tokenizer{buf: sql, parser: p, scanOnly: true}
	start = -1
	// keep extends the statement over sql[from:to].
	keep := func(from, to int) {
		if start < 0 {
			start = from
		}
		end = to
	}
	// lastTyp is the type of the last token kept in the statement.
	var lastTyp int
	for {
		before, skipped := tkn.Pos, tkn.skippedEnd
		typ, val := tkn.Scan()
		if typ == LEX_ERROR {
			return 0, len(sql), false
		}
		if tkn.skippedEnd != skipped {
			// Scan stepped over versioned-comment bytes before this token. Only
			// spaces come before them, because a comment would be a token.
			from := before
			for IsSQLSpace(sql[from]) {
				from++
			}
			keep(from, tkn.skippedEnd)
		}
		switch {
		case typ == 0:
			if start < 0 {
				// Only comments and spaces. They all go to Trailing.
				return 0, 0, true
			}
			// A '--' starts a comment when the text ends after it, but not when
			// a block comment follows it, as in "select 0--/**/". The callers
			// parse the query alone, so cutting there would plan "select 0" for
			// a statement MySQL reads as two minus signs. This is the only token
			// that the end of the text turns into a comment.
			if lastTyp == '-' && strings.HasSuffix(sql[:end], "--") {
				return 0, len(sql), true
			}
			return start, end, true
		case typ == COMMENT && strings.HasPrefix(val, "/*"):
			// A block comment: a margin, if nothing but comments follows it.
		case typ == COMMENT:
			keep(tkn.currStart, tkn.currStart+len(strings.TrimSuffix(val, "\n")))
			lastTyp = typ
		default:
			keep(tkn.currStart, tkn.Pos)
			lastTyp = typ
		}
	}
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
