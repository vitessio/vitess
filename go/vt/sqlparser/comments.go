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
	"strconv"
	"strings"
	"unicode"

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
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

func (c *ParsedComments) Length() int {
	if c == nil {
		return 0
	}
	return len(c.comments)
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
	var comments *ParsedComments
	switch stmt := stmt.(type) {
	case *Insert:
		comments = stmt.Comments
	case *Update:
		comments = stmt.Comments
	case *Delete:
		comments = stmt.Comments
	}
	return comments != nil && comments.Directives().IsSet(DirectiveMultiShardAutocommit)
}

// SkipQueryPlanCacheDirective returns true if skip query plan cache directive is set to true in query.
func SkipQueryPlanCacheDirective(stmt Statement) bool {
	var comments *ParsedComments
	switch stmt := stmt.(type) {
	case *Select:
		comments = stmt.Comments
	case *Insert:
		comments = stmt.Comments
	case *Update:
		comments = stmt.Comments
	case *Delete:
		comments = stmt.Comments
	}
	return comments != nil && comments.Directives().IsSet(DirectiveSkipQueryPlanCache)
}

// IgnoreMaxPayloadSizeDirective returns true if the max payload size override
// directive is set to true.
func IgnoreMaxPayloadSizeDirective(stmt Statement) bool {
	var comments *ParsedComments
	switch stmt := stmt.(type) {
	// For transactional statements, they should always be passed down and
	// should not come into max payload size requirement.
	case *Begin, *Commit, *Rollback, *Savepoint, *SRollback, *Release:
		return true
	case *Select:
		comments = stmt.Comments
	case *Insert:
		comments = stmt.Comments
	case *Update:
		comments = stmt.Comments
	case *Delete:
		comments = stmt.Comments
	}
	return comments != nil && comments.Directives().IsSet(DirectiveIgnoreMaxPayloadSize)
}

// IgnoreMaxMaxMemoryRowsDirective returns true if the max memory rows override
// directive is set to true.
func IgnoreMaxMaxMemoryRowsDirective(stmt Statement) bool {
	var comments *ParsedComments
	switch stmt := stmt.(type) {
	case *Select:
		comments = stmt.Comments
	case *Insert:
		comments = stmt.Comments
	case *Update:
		comments = stmt.Comments
	case *Delete:
		comments = stmt.Comments
	}
	return comments != nil && comments.Directives().IsSet(DirectiveIgnoreMaxMemoryRows)
}

// AllowScatterDirective returns true if the allow scatter override is set to true
func AllowScatterDirective(stmt Statement) bool {
	var comments *ParsedComments
	switch stmt := stmt.(type) {
	case *Select:
		comments = stmt.Comments
	case *Insert:
		comments = stmt.Comments
	case *Update:
		comments = stmt.Comments
	case *Delete:
		comments = stmt.Comments
	}
	return comments != nil && comments.Directives().IsSet(DirectiveAllowScatter)
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
