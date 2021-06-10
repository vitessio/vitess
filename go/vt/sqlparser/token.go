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
	"bytes"
	"fmt"
	"io"

	"github.com/dolthub/vitess/go/bytes2"
	"github.com/dolthub/vitess/go/sqltypes"
	"github.com/dolthub/vitess/go/vt/vterrors"
)

const (
	defaultBufSize = 4096
	eofChar        = 0x100
)

// Tokenizer is the struct used to generate SQL
// tokens for the parser.
type Tokenizer struct {
	InStream            io.Reader
	AllowComments       bool
	SkipSpecialComments bool
	SkipToEnd           bool
	lastChar            uint16
	Position            int
	OldPosition         int
	lastToken           []byte
	lastNonNilToken     []byte
	LastError           error
	posVarIndex         int
	ParseTree           Statement
	partialDDL          *DDL
	nesting             int
	multi               bool
	specialComment      *Tokenizer

	buf     []byte
	bufPos  int
	bufSize int

	queryBuf []byte
}

// NewStringTokenizer creates a new Tokenizer for the
// sql string.
func NewStringTokenizer(sql string) *Tokenizer {
	buf := []byte(sql)
	return &Tokenizer{
		buf:     buf,
		bufSize: len(buf),
	}
}

// NewTokenizer creates a new Tokenizer reading a sql
// string from the io.Reader.
func NewTokenizer(r io.Reader) *Tokenizer {
	return &Tokenizer{
		InStream: r,
		buf:      make([]byte, defaultBufSize),
	}
}

// keywords is a map of mysql keywords that fall into two categories:
// 1) keywords considered reserved by MySQL
// 2) keywords for us to handle specially in sql.y
//
// Those marked as UNUSED are likely reserved keywords. We add them here so that
// when rewriting queries we can properly backtick quote them so they don't cause issues
//
// NOTE: If you add new keywords, add them also to the reserved_keywords or
// non_reserved_keywords grammar in sql.y -- this will allow the keyword to be used
// in identifiers. See the docs for each grammar to determine which one to put it into.
// Everything in this list will be escaped when used as identifier in printing.
var keywords = map[string]int{
	"_binary":             UNDERSCORE_BINARY,
	"_utf8mb4":            UNDERSCORE_UTF8MB4,
	"accessible":          UNUSED,
	"action":              ACTION,
	"add":                 ADD,
	"after":               AFTER,
	"against":             AGAINST,
	"all":                 ALL,
	"alter":               ALTER,
	"analyze":             ANALYZE,
	"and":                 AND,
	"as":                  AS,
	"asc":                 ASC,
	"asensitive":          UNUSED,
	"auto_increment":      AUTO_INCREMENT,
	"avg":                 AVG,
	"before":              BEFORE,
	"begin":               BEGIN,
	"between":             BETWEEN,
	"bigint":              BIGINT,
	"binary":              BINARY,
	"bit":                 BIT,
	"bit_and":             BIT_AND,
	"bit_or":              BIT_OR,
	"bit_xor":             BIT_XOR,
	"blob":                BLOB,
	"bool":                BOOL,
	"boolean":             BOOLEAN,
	"both":                UNUSED,
	"by":                  BY,
	"call":                CALL,
	"cascade":             CASCADE,
	"case":                CASE,
	"cast":                CAST,
	"catalog_name":        CATALOG_NAME,
	"change":              CHANGE,
	"char":                CHAR,
	"character":           CHARACTER,
	"charset":             CHARSET,
	"check":               CHECK,
	"class_origin":        CLASS_ORIGIN,
	"collate":             COLLATE,
	"collation":           COLLATION,
	"column":              COLUMN,
	"column_name":         COLUMN_NAME,
	"columns":             COLUMNS,
	"comment":             COMMENT_KEYWORD,
	"commit":              COMMIT,
	"committed":           COMMITTED,
	"condition":           CONDITION,
	"constraint":          CONSTRAINT,
	"constraint_catalog":  CONSTRAINT_CATALOG,
	"constraint_name":     CONSTRAINT_NAME,
	"constraint_schema":   CONSTRAINT_SCHEMA,
	"contains":            CONTAINS,
	"continue":            CONTINUE,
	"convert":             CONVERT,
	"count":               COUNT,
	"create":              CREATE,
	"cross":               CROSS,
	"cume_dist":           CUME_DIST,
	"current_date":        CURRENT_DATE,
	"current_time":        CURRENT_TIME,
	"current_timestamp":   CURRENT_TIMESTAMP,
	"current_user":        CURRENT_USER,
	"cursor":              CURSOR,
	"cursor_name":         CURSOR_NAME,
	"database":            DATABASE,
	"databases":           DATABASES,
	"data":                DATA,
	"date":                DATE,
	"datetime":            DATETIME,
	"day_hour":            UNUSED,
	"day_microsecond":     UNUSED,
	"day_minute":          UNUSED,
	"day_second":          UNUSED,
	"dec":                 DEC,
	"decimal":             DECIMAL,
	"declare":             DECLARE,
	"default":             DEFAULT,
	"definer":             DEFINER,
	"delayed":             UNUSED,
	"delete":              DELETE,
	"dense_rank":          DENSE_RANK,
	"desc":                DESC,
	"describe":            DESCRIBE,
	"deterministic":       DETERMINISTIC,
	"distinct":            DISTINCT,
	"distinctrow":         UNUSED,
	"div":                 DIV,
	"double":              DOUBLE,
	"drop":                DROP,
	"duplicate":           DUPLICATE,
	"each":                EACH,
	"else":                ELSE,
	"elseif":              ELSEIF,
	"enclosed":            ENCLOSED,
	"end":                 END,
	"enforced":            ENFORCED,
	"engines":             ENGINES,
	"enum":                ENUM,
	"escape":              ESCAPE,
	"escaped":             ESCAPED,
	"exists":              EXISTS,
	"exit":                EXIT,
	"expansion":           EXPANSION,
	"explain":             EXPLAIN,
	"false":               FALSE,
	"fetch":               UNUSED,
	"fields":              FIELDS,
	"first":               FIRST,
	"first_value":         FIRST_VALUE,
	"fixed":               FIXED,
	"float":               FLOAT_TYPE,
	"float4":              UNUSED,
	"float8":              UNUSED,
	"flush":               FLUSH,
	"follows":             FOLLOWS,
	"for":                 FOR,
	"force":               FORCE,
	"foreign":             FOREIGN,
	"format":              FORMAT,
	"found":               FOUND,
	"from":                FROM,
	"full":                FULL,
	"fulltext":            FULLTEXT,
	"function":            FUNCTION,
	"generated":           UNUSED,
	"geometry":            GEOMETRY,
	"geometrycollection":  GEOMETRYCOLLECTION,
	"get":                 UNUSED,
	"global":              GLOBAL,
	"grant":               UNUSED,
	"group":               GROUP,
	"group_concat":        GROUP_CONCAT,
	"grouping":            GROUPING,
	"groups":              GROUPS,
	"handler":             HANDLER,
	"having":              HAVING,
	"high_priority":       UNUSED,
	"hour_microsecond":    UNUSED,
	"hour_minute":         UNUSED,
	"hour_second":         UNUSED,
	"if":                  IF,
	"ignore":              IGNORE,
	"in":                  IN,
	"index":               INDEX,
	"indexes":             INDEXES,
	"infile":              INFILE,
	"inner":               INNER,
	"inout":               INOUT,
	"insensitive":         UNUSED,
	"insert":              INSERT,
	"int":                 INT,
	"int1":                UNUSED,
	"int2":                UNUSED,
	"int3":                UNUSED,
	"int4":                UNUSED,
	"int8":                UNUSED,
	"integer":             INTEGER,
	"interval":            INTERVAL,
	"into":                INTO,
	"invoker":             INVOKER,
	"io_after_gtids":      UNUSED,
	"is":                  IS,
	"isolation":           ISOLATION,
	"iterate":             UNUSED,
	"join":                JOIN,
	"json":                JSON,
	"json_arrayagg":       JSON_ARRAYAGG,
	"json_objectagg":      JSON_OBJECTAGG,
	"key":                 KEY,
	"key_block_size":      KEY_BLOCK_SIZE,
	"keys":                KEYS,
	"kill":                UNUSED,
	"lag":                 LAG,
	"language":            LANGUAGE,
	"last_insert_id":      LAST_INSERT_ID,
	"last_value":          LAST_VALUE,
	"lead":                LEAD,
	"leading":             UNUSED,
	"leave":               UNUSED,
	"left":                LEFT,
	"less":                LESS,
	"level":               LEVEL,
	"like":                LIKE,
	"limit":               LIMIT,
	"linear":              UNUSED,
	"lines":               LINES,
	"linestring":          LINESTRING,
	"load":                LOAD,
	"local":               LOCAL,
	"localtime":           LOCALTIME,
	"localtimestamp":      LOCALTIMESTAMP,
	"lock":                LOCK,
	"long":                LONG,
	"longblob":            LONGBLOB,
	"longtext":            LONGTEXT,
	"loop":                UNUSED,
	"low_priority":        LOW_PRIORITY,
	"master_bind":         UNUSED,
	"match":               MATCH,
	"max":                 MAX,
	"maxvalue":            MAXVALUE,
	"mediumblob":          MEDIUMBLOB,
	"mediumint":           MEDIUMINT,
	"mediumtext":          MEDIUMTEXT,
	"message_text":        MESSAGE_TEXT,
	"middleint":           UNUSED,
	"min":                 MIN,
	"minute_microsecond":  UNUSED,
	"minute_second":       UNUSED,
	"mod":                 MOD,
	"mode":                MODE,
	"modifies":            MODIFIES,
	"modify":              MODIFY,
	"multilinestring":     MULTILINESTRING,
	"multipoint":          MULTIPOINT,
	"multipolygon":        MULTIPOLYGON,
	"mysql_errno":         MYSQL_ERRNO,
	"names":               NAMES,
	"national":            NATIONAL,
	"natural":             NATURAL,
	"nchar":               NCHAR,
	"next":                NEXT,
	"no":                  NO,
	"no_write_to_binlog":  UNUSED,
	"not":                 NOT,
	"nth_value":           NTH_VALUE,
	"ntile":               NTILE,
	"null":                NULL,
	"numeric":             NUMERIC,
	"nvarchar":            NVARCHAR,
	"of":                  OF,
	"off":                 OFF,
	"offset":              OFFSET,
	"on":                  ON,
	"only":                ONLY,
	"optimize":            OPTIMIZE,
	"optimizer_costs":     UNUSED,
	"option":              UNUSED,
	"optionally":          OPTIONALLY,
	"or":                  OR,
	"order":               ORDER,
	"out":                 OUT,
	"outer":               OUTER,
	"outfile":             UNUSED,
	"over":                OVER,
	"partition":           PARTITION,
	"percent_rank":        PERCENT_RANK,
	"persist":             PERSIST,
	"persist_only":        PERSIST_ONLY,
	"plugins":             PLUGINS,
	"point":               POINT,
	"polygon":             POLYGON,
	"precedes":            PRECEDES,
	"precision":           PRECISION,
	"primary":             PRIMARY,
	"procedure":           PROCEDURE,
	"processlist":         PROCESSLIST,
	"query":               QUERY,
	"range":               UNUSED,
	"rank":                RANK,
	"read":                READ,
	"read_write":          UNUSED,
	"reads":               READS,
	"real":                REAL,
	"references":          REFERENCES,
	"regexp":              REGEXP,
	"release":             RELEASE,
	"rename":              RENAME,
	"reorganize":          REORGANIZE,
	"repair":              REPAIR,
	"repeat":              UNUSED,
	"repeatable":          REPEATABLE,
	"replace":             REPLACE,
	"require":             UNUSED,
	"resignal":            RESIGNAL,
	"restrict":            RESTRICT,
	"revoke":              UNUSED,
	"right":               RIGHT,
	"rlike":               REGEXP,
	"rollback":            ROLLBACK,
	"row":                 ROW,
	"row_number":          ROW_NUMBER,
	"savepoint":           SAVEPOINT,
	"schema":              SCHEMA,
	"schema_name":         SCHEMA_NAME,
	"schemas":             SCHEMAS,
	"security":            SECURITY,
	"second_microsecond":  UNUSED,
	"select":              SELECT,
	"sensitive":           UNUSED,
	"separator":           SEPARATOR,
	"sequence":            SEQUENCE,
	"serializable":        SERIALIZABLE,
	"session":             SESSION,
	"set":                 SET,
	"share":               SHARE,
	"show":                SHOW,
	"signal":              SIGNAL,
	"signed":              SIGNED,
	"smallint":            SMALLINT,
	"spatial":             SPATIAL,
	"specific":            UNUSED,
	"sql":                 SQL,
	"sql_big_result":      UNUSED,
	"sql_cache":           SQL_CACHE,
	"sql_calc_found_rows": SQL_CALC_FOUND_ROWS,
	"sql_no_cache":        SQL_NO_CACHE,
	"sql_small_result":    UNUSED,
	"sqlexception":        SQLEXCEPTION,
	"sqlstate":            SQLSTATE,
	"sqlwarning":          SQLWARNING,
	"ssl":                 UNUSED,
	"start":               START,
	"starting":            STARTING,
	"status":              STATUS,
	"std":                 STD,
	"stddev":              STDDEV,
	"stddev_pop":          STDDEV_POP,
	"stddev_samp":         STDDEV_SAMP,
	"stored":              UNUSED,
	"straight_join":       STRAIGHT_JOIN,
	"stream":              STREAM,
	"subclass_origin":     SUBCLASS_ORIGIN,
	"substr":              SUBSTR,
	"substring":           SUBSTRING,
	"sum":                 SUM,
	"table":               TABLE,
	"table_name":          TABLE_NAME,
	"tables":              TABLES,
	"terminated":          TERMINATED,
	"temporary":           TEMPORARY,
	"text":                TEXT,
	"than":                THAN,
	"then":                THEN,
	"time":                TIME,
	"timestamp":           TIMESTAMP,
	"timestampadd":        TIMESTAMPADD,
	"timestampdiff":       TIMESTAMPDIFF,
	"tinyblob":            TINYBLOB,
	"tinyint":             TINYINT,
	"tinytext":            TINYTEXT,
	"to":                  TO,
	"trailing":            UNUSED,
	"transaction":         TRANSACTION,
	"trigger":             TRIGGER,
	"triggers":            TRIGGERS,
	"true":                TRUE,
	"truncate":            TRUNCATE,
	"uncommitted":         UNCOMMITTED,
	"undo":                UNDO,
	"union":               UNION,
	"unique":              UNIQUE,
	"unlock":              UNLOCK,
	"unsigned":            UNSIGNED,
	"update":              UPDATE,
	"usage":               UNUSED,
	"use":                 USE,
	"using":               USING,
	"utc_date":            UTC_DATE,
	"utc_time":            UTC_TIME,
	"utc_timestamp":       UTC_TIMESTAMP,
	"value":               VALUE,
	"values":              VALUES,
	"var_pop":             VAR_POP,
	"varbinary":           VARBINARY,
	"varchar":             VARCHAR,
	"varcharacter":        UNUSED,
	"variables":           VARIABLES,
	"variance":            VARIANCE,
	"varying":             VARYING,
	"view":                VIEW,
	"virtual":             UNUSED,
	"warnings":            WARNINGS,
	"when":                WHEN,
	"where":               WHERE,
	"while":               UNUSED,
	"window":              WINDOW,
	"with":                WITH,
	"work":                WORK,
	"write":               WRITE,
	"xor":                 UNUSED,
	"year":                YEAR,
	"year_month":          UNUSED,
	"zerofill":            ZEROFILL,
}

// keywordStrings contains the reverse mapping of token to keyword strings
var keywordStrings = map[int]string{}

func init() {
	for str, id := range keywords {
		if id == UNUSED {
			continue
		}
		keywordStrings[id] = str
	}
}

// KeywordString returns the string corresponding to the given keyword
func KeywordString(id int) string {
	str, ok := keywordStrings[id]
	if !ok {
		return ""
	}
	return str
}

// Lex returns the next token from the Tokenizer.
// This function is used by go yacc.
func (tkn *Tokenizer) Lex(lval *yySymType) int {
	if tkn.SkipToEnd {
		return tkn.skipStatement()
	}

	typ, val := tkn.Scan()
	for typ == COMMENT {
		if tkn.AllowComments {
			break
		}
		typ, val = tkn.Scan()
	}
	if typ == 0 || typ == ';' || typ == LEX_ERROR {
		// If encounter end of statement or invalid token,
		// we should not accept partially parsed DDLs. They
		// should instead result in parser errors. See the
		// Parse function to see how this is handled.
		tkn.partialDDL = nil
	}
	lval.bytes = val
	tkn.lastToken = val
	if val != nil {
		tkn.lastNonNilToken = val
	}
	return typ
}

// Error is called by go yacc if there's a parsing error.
func (tkn *Tokenizer) Error(err string) {
	buf := &bytes2.Buffer{}
	if tkn.lastNonNilToken != nil {
		fmt.Fprintf(buf, "%s at position %v near '%s'", err, tkn.Position, tkn.lastNonNilToken)
	} else {
		fmt.Fprintf(buf, "%s at position %v", err, tkn.Position)
	}
	tkn.LastError = vterrors.SyntaxError{Message: buf.String(), Position: tkn.Position, Statement: string(tkn.buf)}

	// Try and re-sync to the next statement
	tkn.skipStatement()
}

// Scan scans the tokenizer for the next token and returns
// the token type and an optional value.
func (tkn *Tokenizer) Scan() (int, []byte) {
	if tkn.specialComment != nil {
		// Enter specialComment scan mode.
		// for scanning such kind of comment: /*! MySQL-specific code */
		specialComment := tkn.specialComment
		tok, val := specialComment.Scan()
		if tok != 0 {
			// return the specialComment scan result as the result
			return tok, val
		}
		// leave specialComment scan mode after all stream consumed.
		tkn.specialComment = nil
	}

	tkn.OldPosition = tkn.Position
	if tkn.lastChar == 0 {
		tkn.next()
	}

	tkn.skipBlank()
	switch ch := tkn.lastChar; {
	case isLetter(ch):
		tkn.next()
		if ch == 'X' || ch == 'x' {
			if tkn.lastChar == '\'' {
				tkn.next()
				return tkn.scanHex()
			}
		}
		if ch == 'B' || ch == 'b' {
			if tkn.lastChar == '\'' {
				tkn.next()
				return tkn.scanBitLiteral()
			}
		}
		isDbSystemVariable := false
		if ch == '@' && tkn.lastChar == '@' {
			isDbSystemVariable = true
		}
		return tkn.scanIdentifier(byte(ch), isDbSystemVariable)
	case isDigit(ch):
		return tkn.scanNumber(false)
	case ch == ':':
		return tkn.scanBindVar()
	case ch == ';':
		if tkn.multi {
			// In multi mode, ';' is treated as EOF. So, we don't advance.
			// Repeated calls to Scan will keep returning 0 until ParseNext
			// forces the advance.
			return 0, nil
		}
		tkn.next()
		return ';', nil
	case ch == eofChar:
		return 0, nil
	default:
		tkn.next()
		switch ch {
		case '=', ',', '(', ')', '+', '*', '%', '^', '~':
			return int(ch), nil
		case '&':
			if tkn.lastChar == '&' {
				tkn.next()
				return AND, nil
			}
			return int(ch), nil
		case '|':
			if tkn.lastChar == '|' {
				tkn.next()
				return OR, nil
			}
			return int(ch), nil
		case '?':
			tkn.posVarIndex++
			buf := new(bytes2.Buffer)
			fmt.Fprintf(buf, ":v%d", tkn.posVarIndex)
			return VALUE_ARG, buf.Bytes()
		case '.':
			if isDigit(tkn.lastChar) {
				return tkn.scanNumber(true)
			}
			return int(ch), nil
		case '/':
			switch tkn.lastChar {
			case '/':
				tkn.next()
				return tkn.scanCommentType1("//")
			case '*':
				tkn.next()
				if tkn.lastChar == '!' && !tkn.SkipSpecialComments {
					return tkn.scanMySQLSpecificComment()
				}
				return tkn.scanCommentType2()
			default:
				return int(ch), nil
			}
		case '#':
			return tkn.scanCommentType1("#")
		case '-':
			switch tkn.lastChar {
			case '-':
				tkn.next()
				return tkn.scanCommentType1("--")
			case '>':
				tkn.next()
				if tkn.lastChar == '>' {
					tkn.next()
					return JSON_UNQUOTE_EXTRACT_OP, nil
				}
				return JSON_EXTRACT_OP, nil
			}
			return int(ch), nil
		case '<':
			switch tkn.lastChar {
			case '>':
				tkn.next()
				return NE, nil
			case '<':
				tkn.next()
				return SHIFT_LEFT, nil
			case '=':
				tkn.next()
				switch tkn.lastChar {
				case '>':
					tkn.next()
					return NULL_SAFE_EQUAL, nil
				default:
					return LE, nil
				}
			default:
				return int(ch), nil
			}
		case '>':
			switch tkn.lastChar {
			case '=':
				tkn.next()
				return GE, nil
			case '>':
				tkn.next()
				return SHIFT_RIGHT, nil
			default:
				return int(ch), nil
			}
		case '!':
			if tkn.lastChar == '=' {
				tkn.next()
				return NE, nil
			}
			return int(ch), nil
		case '\'', '"':
			return tkn.scanString(ch, STRING)
		case '`':
			return tkn.scanLiteralIdentifier()
		default:
			return LEX_ERROR, []byte{byte(ch)}
		}
	}
}

// skipStatement scans until end of statement.
func (tkn *Tokenizer) skipStatement() int {
	tkn.SkipToEnd = false
	for {
		typ, _ := tkn.Scan()
		if typ == 0 || typ == ';' || typ == LEX_ERROR {
			return typ
		}
	}
}

func (tkn *Tokenizer) skipBlank() {
	ch := tkn.lastChar
	for ch == ' ' || ch == '\n' || ch == '\r' || ch == '\t' {
		tkn.next()
		ch = tkn.lastChar
	}
}

func (tkn *Tokenizer) scanIdentifier(firstByte byte, isDbSystemVariable bool) (int, []byte) {
	buffer := &bytes2.Buffer{}
	buffer.WriteByte(firstByte)
	for isLetter(tkn.lastChar) || isDigit(tkn.lastChar) || (isDbSystemVariable && isCarat(tkn.lastChar)) {
		buffer.WriteByte(byte(tkn.lastChar))
		tkn.next()
	}
	lowered := bytes.ToLower(buffer.Bytes())
	loweredStr := string(lowered)
	if keywordID, found := keywords[loweredStr]; found {
		return keywordID, buffer.Bytes()
	}
	// dual must always be case-insensitive
	if loweredStr == "dual" {
		return ID, lowered
	}
	return ID, buffer.Bytes()
}

func (tkn *Tokenizer) scanHex() (int, []byte) {
	buffer := &bytes2.Buffer{}
	tkn.scanMantissa(16, buffer)
	if tkn.lastChar != '\'' {
		return LEX_ERROR, buffer.Bytes()
	}
	tkn.next()
	if buffer.Len()%2 != 0 {
		return LEX_ERROR, buffer.Bytes()
	}
	return HEX, buffer.Bytes()
}

func (tkn *Tokenizer) scanBitLiteral() (int, []byte) {
	buffer := &bytes2.Buffer{}
	tkn.scanMantissa(2, buffer)
	if tkn.lastChar != '\'' {
		return LEX_ERROR, buffer.Bytes()
	}
	tkn.next()
	return BIT_LITERAL, buffer.Bytes()
}

func (tkn *Tokenizer) scanLiteralIdentifier() (int, []byte) {
	buffer := &bytes2.Buffer{}
	backTickSeen := false
	for {
		if backTickSeen {
			if tkn.lastChar != '`' {
				break
			}
			backTickSeen = false
			buffer.WriteByte('`')
			tkn.next()
			continue
		}
		// The previous char was not a backtick.
		switch tkn.lastChar {
		case '`':
			backTickSeen = true
		case eofChar:
			// Premature EOF.
			return LEX_ERROR, buffer.Bytes()
		default:
			buffer.WriteByte(byte(tkn.lastChar))
		}
		tkn.next()
	}
	if buffer.Len() == 0 {
		return LEX_ERROR, buffer.Bytes()
	}
	return ID, buffer.Bytes()
}

func (tkn *Tokenizer) scanBindVar() (int, []byte) {
	buffer := &bytes2.Buffer{}
	buffer.WriteByte(byte(tkn.lastChar))
	token := VALUE_ARG
	tkn.next()
	if tkn.lastChar == ':' {
		token = LIST_ARG
		buffer.WriteByte(byte(tkn.lastChar))
		tkn.next()
	}
	if !isLetter(tkn.lastChar) {
		return LEX_ERROR, buffer.Bytes()
	}
	for isLetter(tkn.lastChar) || isDigit(tkn.lastChar) || tkn.lastChar == '.' {
		buffer.WriteByte(byte(tkn.lastChar))
		tkn.next()
	}
	return token, buffer.Bytes()
}

func (tkn *Tokenizer) scanMantissa(base int, buffer *bytes2.Buffer) {
	for digitVal(tkn.lastChar) < base {
		tkn.consumeNext(buffer)
	}
}

func (tkn *Tokenizer) scanNumber(seenDecimalPoint bool) (int, []byte) {
	token := INTEGRAL
	buffer := &bytes2.Buffer{}
	if seenDecimalPoint {
		token = FLOAT
		buffer.WriteByte('.')
		tkn.scanMantissa(10, buffer)
		goto exponent
	}

	// 0x construct.
	if tkn.lastChar == '0' {
		tkn.consumeNext(buffer)
		if tkn.lastChar == 'x' || tkn.lastChar == 'X' {
			token = HEXNUM
			tkn.consumeNext(buffer)
			tkn.scanMantissa(16, buffer)
			goto exit
		}
	}

	tkn.scanMantissa(10, buffer)

	if tkn.lastChar == '.' {
		token = FLOAT
		tkn.consumeNext(buffer)
		tkn.scanMantissa(10, buffer)
	}

exponent:
	if tkn.lastChar == 'e' || tkn.lastChar == 'E' {
		token = FLOAT
		tkn.consumeNext(buffer)
		if tkn.lastChar == '+' || tkn.lastChar == '-' {
			tkn.consumeNext(buffer)
		}
		tkn.scanMantissa(10, buffer)
	}

exit:
	// A letter cannot immediately follow a number.
	if isLetter(tkn.lastChar) {
		return LEX_ERROR, buffer.Bytes()
	}

	return token, buffer.Bytes()
}

func (tkn *Tokenizer) scanString(delim uint16, typ int) (int, []byte) {
	var buffer bytes2.Buffer
	for {
		ch := tkn.lastChar
		if ch == eofChar {
			// Unterminated string.
			return LEX_ERROR, buffer.Bytes()
		}

		if ch != delim && ch != '\\' {
			buffer.WriteByte(byte(ch))

			// Scan ahead to the next interesting character.
			start := tkn.bufPos
			for ; tkn.bufPos < tkn.bufSize; tkn.bufPos++ {
				ch = uint16(tkn.buf[tkn.bufPos])
				if ch == delim || ch == '\\' {
					break
				}
			}

			buffer.Write(tkn.buf[start:tkn.bufPos])
			tkn.Position += (tkn.bufPos - start)

			if tkn.bufPos >= tkn.bufSize {
				// Reached the end of the buffer without finding a delim or
				// escape character.
				tkn.next()
				continue
			}

			tkn.bufPos++
			tkn.Position++
		}
		tkn.next() // Read one past the delim or escape character.

		if ch == '\\' {
			if tkn.lastChar == eofChar {
				// String terminates mid escape character.
				return LEX_ERROR, buffer.Bytes()
			}
			if decodedChar := sqltypes.SQLDecodeMap[byte(tkn.lastChar)]; decodedChar == sqltypes.DontEscape {
				ch = tkn.lastChar
			} else {
				ch = uint16(decodedChar)
			}

		} else if ch == delim && tkn.lastChar != delim {
			// Correctly terminated string, which is not a double delim.
			break
		}

		buffer.WriteByte(byte(ch))
		tkn.next()
	}

	return typ, buffer.Bytes()
}

func (tkn *Tokenizer) scanCommentType1(prefix string) (int, []byte) {
	buffer := &bytes2.Buffer{}
	buffer.WriteString(prefix)
	for tkn.lastChar != eofChar {
		if tkn.lastChar == '\n' {
			tkn.consumeNext(buffer)
			break
		}
		tkn.consumeNext(buffer)
	}
	return COMMENT, buffer.Bytes()
}

func (tkn *Tokenizer) scanCommentType2() (int, []byte) {
	buffer := &bytes2.Buffer{}
	buffer.WriteString("/*")
	for {
		if tkn.lastChar == '*' {
			tkn.consumeNext(buffer)
			if tkn.lastChar == '/' {
				tkn.consumeNext(buffer)
				break
			}
			continue
		}
		if tkn.lastChar == eofChar {
			return LEX_ERROR, buffer.Bytes()
		}
		tkn.consumeNext(buffer)
	}
	return COMMENT, buffer.Bytes()
}

func (tkn *Tokenizer) scanMySQLSpecificComment() (int, []byte) {
	buffer := &bytes2.Buffer{}
	buffer.WriteString("/*!")
	tkn.next()
	for {
		if tkn.lastChar == '*' {
			tkn.consumeNext(buffer)
			if tkn.lastChar == '/' {
				tkn.consumeNext(buffer)
				break
			}
			continue
		}
		if tkn.lastChar == eofChar {
			return LEX_ERROR, buffer.Bytes()
		}
		tkn.consumeNext(buffer)
	}
	_, sql := ExtractMysqlComment(buffer.String())
	tkn.specialComment = NewStringTokenizer(sql)
	return tkn.Scan()
}

func (tkn *Tokenizer) consumeNext(buffer *bytes2.Buffer) {
	if tkn.lastChar == eofChar {
		// This should never happen.
		panic("unexpected EOF")
	}
	buffer.WriteByte(byte(tkn.lastChar))
	tkn.next()
}

func (tkn *Tokenizer) next() {
	if tkn.bufPos >= tkn.bufSize && tkn.InStream != nil {
		// Try and refill the buffer
		var err error
		tkn.bufPos = 0
		if tkn.bufSize, err = tkn.InStream.Read(tkn.buf); err != io.EOF && err != nil {
			tkn.LastError = err
		}

		// In multi mode (parseNext), we need to keep track of the contents of the current statement string so that
		// lexer offsets work properly on statements that need them
		if tkn.multi {
			tkn.queryBuf = append(tkn.queryBuf, tkn.buf...)
		}
	}

	if tkn.bufPos >= tkn.bufSize {
		if tkn.lastChar != eofChar {
			tkn.Position++
			tkn.lastChar = eofChar
		}
	} else {
		tkn.Position++
		tkn.lastChar = uint16(tkn.buf[tkn.bufPos])
		tkn.bufPos++
	}
}

// reset clears any internal state.
func (tkn *Tokenizer) reset() {
	tkn.ParseTree = nil
	tkn.partialDDL = nil
	tkn.specialComment = nil
	tkn.posVarIndex = 0
	tkn.nesting = 0
	tkn.SkipToEnd = false
	bufLeft := len(tkn.buf) - tkn.bufPos
	if len(tkn.queryBuf) > bufLeft {
		tkn.queryBuf = tkn.queryBuf[len(tkn.queryBuf)-bufLeft:]
	}
	tkn.Position = 0
	tkn.OldPosition = 0
}

func isLetter(ch uint16) bool {
	return 'a' <= ch && ch <= 'z' || 'A' <= ch && ch <= 'Z' || ch == '_' || ch == '@'
}

func isCarat(ch uint16) bool {
	return ch == '.' || ch == '\'' || ch == '"' || ch == '`'
}

func digitVal(ch uint16) int {
	switch {
	case '0' <= ch && ch <= '9':
		return int(ch) - '0'
	case 'a' <= ch && ch <= 'f':
		return int(ch) - 'a' + 10
	case 'A' <= ch && ch <= 'F':
		return int(ch) - 'A' + 10
	}
	return 16 // larger than any legal digit val
}

func isDigit(ch uint16) bool {
	return '0' <= ch && ch <= '9'
}
