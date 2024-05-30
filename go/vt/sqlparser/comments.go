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
	"unicode"

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
	// DirectiveAllowScatter lets scatter plans pass through even when they are turned off by `no-scatter`.
	DirectiveAllowScatter = "ALLOW_SCATTER"
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

func isNonSpace(r rune) bool {
	return !unicode.IsSpace(r)
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
func trailingCommentStart(text string) (start int) {
	hasComment := false
	reducedLen := len(text)
	for reducedLen > 0 {
		// Eat up any whitespace. Leading whitespace will be considered part of
		// the trailing comments.
		nextReducedLen := strings.LastIndexFunc(text[:reducedLen], isNonSpace) + 1
		if nextReducedLen == 0 {
			break
		}
		reducedLen = nextReducedLen
		if reducedLen < 4 || text[reducedLen-2:reducedLen] != "*/" {
			break
		}

		// Find the beginning of the comment
		startCommentPos := strings.LastIndex(text[:reducedLen-2], "/*")
		if startCommentPos < 0 || text[startCommentPos+2] == '!' {
			// Badly formatted sql, or a special /*! comment
			break
		}

		hasComment = true
		reducedLen = startCommentPos
	}

	if hasComment {
		return reducedLen
	}
	return len(text)
}

// MarginComments holds the leading and trailing comments that surround a query.
type MarginComments struct {
	Leading  string
	Trailing string
}

// SplitMarginComments pulls out any leading or trailing comments from a raw sql query.
// This function also trims leading (if there's a comment) and trailing whitespace.
func SplitMarginComments(sql string) (query string, comments MarginComments) {
	trailingStart := trailingCommentStart(sql)
	leadingEnd := leadingCommentEnd(sql[:trailingStart])
	comments = MarginComments{
		Leading:  strings.TrimLeftFunc(sql[:leadingEnd], unicode.IsSpace),
		Trailing: strings.TrimRightFunc(sql[trailingStart:], unicode.IsSpace),
	}
	return strings.TrimFunc(sql[leadingEnd:trailingStart], func(c rune) bool {
		return unicode.IsSpace(c) || c == ';'
	}), comments
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
func ExtractMysqlComment(sql string) (string, string) {
	sql = sql[3 : len(sql)-2]

	digitCount := 0
	endOfVersionIndex := strings.IndexFunc(sql, func(c rune) bool {
		digitCount++
		return !unicode.IsDigit(c) || digitCount == 6
	})
	if endOfVersionIndex < 0 {
		return "", ""
	}
	if endOfVersionIndex < 5 {
		endOfVersionIndex = 0
	}
	version := sql[0:endOfVersionIndex]
	innerSQL := strings.TrimFunc(sql[endOfVersionIndex:], unicode.IsSpace)

	return version, innerSQL
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
					finalComment += fmt.Sprintf(" %v(%v)", ohName, ohContent)
					continue
				}
				if strings.EqualFold(strings.TrimSpace(setVarName), key) {
					keyPresent = true
					finalComment += fmt.Sprintf(" %v(%v=%v)", ohName, strings.TrimSpace(setVarName), value)
				}
			} else {
				// If it doesn't match, we add it to our final comment and move on.
				finalComment += fmt.Sprintf(" %v(%v)", ohName, ohContent)
			}
		}
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

// ForeignKeyChecksState returns the state of foreign_key_checks variable if it is part of a SET_VAR optimizer hint in the comments.
func ForeignKeyChecksState(stmt Statement) *bool {
	cmt, ok := stmt.(Commented)
	if ok {
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
	}
	return nil
}

func checkDirective(stmt Statement, key string) bool {
	cmt, ok := stmt.(Commented)
	if ok {
		return cmt.GetParsedComments().Directives().IsSet(key)
	}
	return false
}

// GetPriorityFromStatement gets the priority from the provided Statement, using DirectivePriority
func GetPriorityFromStatement(statement Statement) (string, error) {
	commentedStatement, ok := statement.(Commented)
	// This would mean that the statement lacks comments, so we can't obtain the workload from it. Hence default to
	// empty priority
	if !ok {
		return "", nil
	}

	directives := commentedStatement.GetParsedComments().Directives()
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

// Consolidator returns the consolidator option.
func Consolidator(stmt Statement) querypb.ExecuteOptions_Consolidator {
	var comments *ParsedComments
	switch stmt := stmt.(type) {
	case *Select:
		comments = stmt.Comments
	default:
		return querypb.ExecuteOptions_CONSOLIDATOR_UNSPECIFIED
	}
	if comments == nil {
		return querypb.ExecuteOptions_CONSOLIDATOR_UNSPECIFIED
	}
	directives := comments.Directives()
	strv, isSet := directives.GetString(DirectiveConsolidator, "")
	if !isSet {
		return querypb.ExecuteOptions_CONSOLIDATOR_UNSPECIFIED
	}
	if i32v, ok := querypb.ExecuteOptions_Consolidator_value["CONSOLIDATOR_"+strings.ToUpper(strv)]; ok {
		return querypb.ExecuteOptions_Consolidator(i32v)
	}
	return querypb.ExecuteOptions_CONSOLIDATOR_UNSPECIFIED
}

// GetWorkloadNameFromStatement gets the workload name from the provided Statement, using workloadLabel as the name of
// the query directive that specifies it.
func GetWorkloadNameFromStatement(statement Statement) string {
	commentedStatement, ok := statement.(Commented)
	// This would mean that the statement lacks comments, so we can't obtain the workload from it. Hence default to
	// empty workload name
	if !ok {
		return ""
	}

	directives := commentedStatement.GetParsedComments().Directives()
	workloadName, _ := directives.GetString(DirectiveWorkloadName, "")

	return workloadName
}
