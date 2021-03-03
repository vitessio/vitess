/*
   Copyright 2016 GitHub Inc.
	 See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package vrepl

import (
	"regexp"
	"strconv"
	"strings"
)

var (
	sanitizeQuotesRegexp                 = regexp.MustCompile("('[^']*')")
	renameColumnRegexp                   = regexp.MustCompile(`(?i)\bchange\s+(column\s+|)([\S]+)\s+([\S]+)\s+`)
	dropColumnRegexp                     = regexp.MustCompile(`(?i)\bdrop\s+(column\s+|)([\S]+)$`)
	renameTableRegexp                    = regexp.MustCompile(`(?i)\brename\s+(to|as)\s+`)
	autoIncrementRegexp                  = regexp.MustCompile(`(?i)\bauto_increment[\s]*=[\s]*([0-9]+)`)
	alterTableExplicitSchemaTableRegexps = []*regexp.Regexp{
		// ALTER TABLE `scm`.`tbl` something
		regexp.MustCompile(`(?i)\balter\s+table\s+` + "`" + `([^` + "`" + `]+)` + "`" + `[.]` + "`" + `([^` + "`" + `]+)` + "`" + `\s+(.*$)`),
		// ALTER TABLE `scm`.tbl something
		regexp.MustCompile(`(?i)\balter\s+table\s+` + "`" + `([^` + "`" + `]+)` + "`" + `[.]([\S]+)\s+(.*$)`),
		// ALTER TABLE scm.`tbl` something
		regexp.MustCompile(`(?i)\balter\s+table\s+([\S]+)[.]` + "`" + `([^` + "`" + `]+)` + "`" + `\s+(.*$)`),
		// ALTER TABLE scm.tbl something
		regexp.MustCompile(`(?i)\balter\s+table\s+([\S]+)[.]([\S]+)\s+(.*$)`),
	}
	alterTableExplicitTableRegexps = []*regexp.Regexp{
		// ALTER TABLE `tbl` something
		regexp.MustCompile(`(?i)\balter\s+table\s+` + "`" + `([^` + "`" + `]+)` + "`" + `\s+(.*$)`),
		// ALTER TABLE tbl something
		regexp.MustCompile(`(?i)\balter\s+table\s+([\S]+)\s+(.*$)`),
	}
)

// AlterTableParser is a parser tool for ALTER TABLE statements
// This is imported from gh-ost. In the future, we should replace that with Vitess parsing.
type AlterTableParser struct {
	columnRenameMap        map[string]string
	droppedColumns         map[string]bool
	isRenameTable          bool
	isAutoIncrementDefined bool

	alterStatementOptions string
	alterTokens           []string

	explicitSchema string
	explicitTable  string
}

// NewAlterTableParser creates a new parser
func NewAlterTableParser() *AlterTableParser {
	return &AlterTableParser{
		columnRenameMap: make(map[string]string),
		droppedColumns:  make(map[string]bool),
	}
}

// NewParserFromAlterStatement creates a new parser with a ALTER TABLE statement
func NewParserFromAlterStatement(alterStatement string) *AlterTableParser {
	parser := NewAlterTableParser()
	parser.ParseAlterStatement(alterStatement)
	return parser
}

// tokenizeAlterStatement
func (p *AlterTableParser) tokenizeAlterStatement(alterStatement string) (tokens []string, err error) {
	terminatingQuote := rune(0)
	f := func(c rune) bool {
		switch {
		case c == terminatingQuote:
			terminatingQuote = rune(0)
			return false
		case terminatingQuote != rune(0):
			return false
		case c == '\'':
			terminatingQuote = c
			return false
		case c == '(':
			terminatingQuote = ')'
			return false
		default:
			return c == ','
		}
	}

	tokens = strings.FieldsFunc(alterStatement, f)
	for i := range tokens {
		tokens[i] = strings.TrimSpace(tokens[i])
	}
	return tokens, nil
}

func (p *AlterTableParser) sanitizeQuotesFromAlterStatement(alterStatement string) (strippedStatement string) {
	strippedStatement = alterStatement
	strippedStatement = sanitizeQuotesRegexp.ReplaceAllString(strippedStatement, "''")
	return strippedStatement
}

// parseAlterToken parses a single ALTER option (e.g. a DROP COLUMN)
func (p *AlterTableParser) parseAlterToken(alterToken string) (err error) {
	{
		// rename
		allStringSubmatch := renameColumnRegexp.FindAllStringSubmatch(alterToken, -1)
		for _, submatch := range allStringSubmatch {
			if unquoted, err := strconv.Unquote(submatch[2]); err == nil {
				submatch[2] = unquoted
			}
			if unquoted, err := strconv.Unquote(submatch[3]); err == nil {
				submatch[3] = unquoted
			}
			p.columnRenameMap[submatch[2]] = submatch[3]
		}
	}
	{
		// drop
		allStringSubmatch := dropColumnRegexp.FindAllStringSubmatch(alterToken, -1)
		for _, submatch := range allStringSubmatch {
			if unquoted, err := strconv.Unquote(submatch[2]); err == nil {
				submatch[2] = unquoted
			}
			p.droppedColumns[submatch[2]] = true
		}
	}
	{
		// rename table
		if renameTableRegexp.MatchString(alterToken) {
			p.isRenameTable = true
		}
	}
	{
		// auto_increment
		if autoIncrementRegexp.MatchString(alterToken) {
			p.isAutoIncrementDefined = true
		}
	}
	return nil
}

// ParseAlterStatement is the main function of th eparser, and parses an ALTER TABLE statement
func (p *AlterTableParser) ParseAlterStatement(alterStatement string) (err error) {
	p.alterStatementOptions = alterStatement
	for _, alterTableRegexp := range alterTableExplicitSchemaTableRegexps {
		if submatch := alterTableRegexp.FindStringSubmatch(p.alterStatementOptions); len(submatch) > 0 {
			p.explicitSchema = submatch[1]
			p.explicitTable = submatch[2]
			p.alterStatementOptions = submatch[3]
			break
		}
	}
	for _, alterTableRegexp := range alterTableExplicitTableRegexps {
		if submatch := alterTableRegexp.FindStringSubmatch(p.alterStatementOptions); len(submatch) > 0 {
			p.explicitTable = submatch[1]
			p.alterStatementOptions = submatch[2]
			break
		}
	}
	alterTokens, _ := p.tokenizeAlterStatement(p.alterStatementOptions)
	for _, alterToken := range alterTokens {
		alterToken = p.sanitizeQuotesFromAlterStatement(alterToken)
		p.parseAlterToken(alterToken)
		p.alterTokens = append(p.alterTokens, alterToken)
	}
	return nil
}

// GetNonTrivialRenames gets a list of renamed column
func (p *AlterTableParser) GetNonTrivialRenames() map[string]string {
	result := make(map[string]string)
	for column, renamed := range p.columnRenameMap {
		if column != renamed {
			result[column] = renamed
		}
	}
	return result
}

// HasNonTrivialRenames is true when columns have been renamed
func (p *AlterTableParser) HasNonTrivialRenames() bool {
	return len(p.GetNonTrivialRenames()) > 0
}

// DroppedColumnsMap returns list of dropped columns
func (p *AlterTableParser) DroppedColumnsMap() map[string]bool {
	return p.droppedColumns
}

// IsRenameTable returns true when the ALTER TABLE statement inclusdes renaming the table
func (p *AlterTableParser) IsRenameTable() bool {
	return p.isRenameTable
}

// IsAutoIncrementDefined returns true when alter options include an explicit AUTO_INCREMENT value
func (p *AlterTableParser) IsAutoIncrementDefined() bool {
	return p.isAutoIncrementDefined
}

// GetExplicitSchema returns the explciit schema, if defined
func (p *AlterTableParser) GetExplicitSchema() string {
	return p.explicitSchema
}

// HasExplicitSchema returns true when the ALTER TABLE statement includes the schema qualifier
func (p *AlterTableParser) HasExplicitSchema() bool {
	return p.GetExplicitSchema() != ""
}

// GetExplicitTable return the table name
func (p *AlterTableParser) GetExplicitTable() string {
	return p.explicitTable
}

// HasExplicitTable checks if the ALTER TABLE statement has an explicit table name
func (p *AlterTableParser) HasExplicitTable() bool {
	return p.GetExplicitTable() != ""
}

// GetAlterStatementOptions returns the options section in the ALTER TABLE statement
func (p *AlterTableParser) GetAlterStatementOptions() string {
	return p.alterStatementOptions
}

// ColumnRenameMap returns the renamed column mapping
func (p *AlterTableParser) ColumnRenameMap() map[string]string {
	return p.columnRenameMap
}
