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
	"unicode"

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
	InStream             io.Reader
	AllowComments        bool
	SkipSpecialComments  bool
	lastChar             uint16
	Position             int
	OldPosition          int
	lastToken            []byte
	lastNonNilToken      []byte
	LastError            error
	posVarIndex          int
	ParseTree            Statement
	nesting              int
	multi                bool
	specialComment       *Tokenizer
	specialCommentEndPos int
	potentialAccountName bool

	// If true, the parser should collaborate to set `stopped` on this
	// tokenizer after a statement is parsed. From that point forward, the
	// tokenizer will return EOF, instead of new tokens. `ParseOne` uses
	// this to parse the first delimited statement and then return the
	// trailer.
	stopAfterFirstStmt bool
	stopped            bool

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
	"_armscii8":                UNDERSCORE_ARMSCII8,
	"_ascii":                   UNDERSCORE_ASCII,
	"_big5":                    UNDERSCORE_BIG5,
	"_binary":                  UNDERSCORE_BINARY,
	"_cp1250":                  UNDERSCORE_CP1250,
	"_cp1251":                  UNDERSCORE_CP1251,
	"_cp1256":                  UNDERSCORE_CP1256,
	"_cp1257":                  UNDERSCORE_CP1257,
	"_cp850":                   UNDERSCORE_CP850,
	"_cp852":                   UNDERSCORE_CP852,
	"_cp866":                   UNDERSCORE_CP866,
	"_cp932":                   UNDERSCORE_CP932,
	"_dec8":                    UNDERSCORE_DEC8,
	"_eucjpms":                 UNDERSCORE_EUCJPMS,
	"_euckr":                   UNDERSCORE_EUCKR,
	"_gb18030":                 UNDERSCORE_GB18030,
	"_gb2312":                  UNDERSCORE_GB2312,
	"_gbk":                     UNDERSCORE_GBK,
	"_geostd8":                 UNDERSCORE_GEOSTD8,
	"_greek":                   UNDERSCORE_GREEK,
	"_hebrew":                  UNDERSCORE_HEBREW,
	"_hp8":                     UNDERSCORE_HP8,
	"_keybcs2":                 UNDERSCORE_KEYBCS2,
	"_koi8r":                   UNDERSCORE_KOI8R,
	"_koi8u":                   UNDERSCORE_KOI8U,
	"_latin1":                  UNDERSCORE_LATIN1,
	"_latin2":                  UNDERSCORE_LATIN2,
	"_latin5":                  UNDERSCORE_LATIN5,
	"_latin7":                  UNDERSCORE_LATIN7,
	"_macce":                   UNDERSCORE_MACCE,
	"_macroman":                UNDERSCORE_MACROMAN,
	"_sjis":                    UNDERSCORE_SJIS,
	"_swe7":                    UNDERSCORE_SWE7,
	"_tis620":                  UNDERSCORE_TIS620,
	"_ucs2":                    UNDERSCORE_UCS2,
	"_ujis":                    UNDERSCORE_UJIS,
	"_utf16":                   UNDERSCORE_UTF16,
	"_utf16le":                 UNDERSCORE_UTF16LE,
	"_utf32":                   UNDERSCORE_UTF32,
	"_utf8":                    UNDERSCORE_UTF8,
	"_utf8mb3":                 UNDERSCORE_UTF8MB3,
	"_utf8mb4":                 UNDERSCORE_UTF8MB4,
	"accessible":               UNUSED,
	"account":                  ACCOUNT,
	"action":                   ACTION,
	"add":                      ADD,
	"admin":                    ADMIN,
	"after":                    AFTER,
	"against":                  AGAINST,
	"algorithm":                ALGORITHM,
	"all":                      ALL,
	"alter":                    ALTER,
	"always":                   ALWAYS,
	"analyze":                  ANALYZE,
	"and":                      AND,
	"as":                       AS,
	"asc":                      ASC,
	"asensitive":               UNUSED,
	"attribute":                ATTRIBUTE,
	"authentication":           AUTHENTICATION,
	"auto_increment":           AUTO_INCREMENT,
	"avg":                      AVG,
	"before":                   BEFORE,
	"begin":                    BEGIN,
	"between":                  BETWEEN,
	"serial":                   SERIAL,
	"bigint":                   BIGINT,
	"binary":                   BINARY,
	"bit":                      BIT,
	"bit_and":                  BIT_AND,
	"bit_or":                   BIT_OR,
	"bit_xor":                  BIT_XOR,
	"blob":                     BLOB,
	"bool":                     BOOL,
	"boolean":                  BOOLEAN,
	"both":                     BOTH,
	"by":                       BY,
	"call":                     CALL,
	"cascade":                  CASCADE,
	"case":                     CASE,
	"cast":                     CAST,
	"catalog_name":             CATALOG_NAME,
	"change":                   CHANGE,
	"channel":                  CHANNEL,
	"char":                     CHAR,
	"character":                CHARACTER,
	"charset":                  CHARSET,
	"check":                    CHECK,
	"cipher":                   CIPHER,
	"class_origin":             CLASS_ORIGIN,
	"client":                   CLIENT,
	"collate":                  COLLATE,
	"collation":                COLLATION,
	"column":                   COLUMN,
	"column_name":              COLUMN_NAME,
	"columns":                  COLUMNS,
	"comment":                  COMMENT_KEYWORD,
	"commit":                   COMMIT,
	"committed":                COMMITTED,
	"condition":                CONDITION,
	"connection":               CONNECTION,
	"constraint":               CONSTRAINT,
	"constraint_catalog":       CONSTRAINT_CATALOG,
	"constraint_name":          CONSTRAINT_NAME,
	"constraint_schema":        CONSTRAINT_SCHEMA,
	"contains":                 CONTAINS,
	"continue":                 CONTINUE,
	"convert":                  CONVERT,
	"count":                    COUNT,
	"create":                   CREATE,
	"cross":                    CROSS,
	"cume_dist":                CUME_DIST,
	"current":                  CURRENT,
	"current_date":             CURRENT_DATE,
	"current_time":             CURRENT_TIME,
	"current_timestamp":        CURRENT_TIMESTAMP,
	"current_user":             CURRENT_USER,
	"cursor":                   CURSOR,
	"cursor_name":              CURSOR_NAME,
	"database":                 DATABASE,
	"databases":                DATABASES,
	"data":                     DATA,
	"date":                     DATE,
	"datetime":                 DATETIME,
	"day":                      DAY,
	"day_hour":                 UNUSED,
	"day_microsecond":          UNUSED,
	"day_minute":               UNUSED,
	"day_second":               UNUSED,
	"dec":                      DEC,
	"decimal":                  DECIMAL,
	"declare":                  DECLARE,
	"default":                  DEFAULT,
	"definer":                  DEFINER,
	"delayed":                  UNUSED,
	"delete":                   DELETE,
	"dense_rank":               DENSE_RANK,
	"desc":                     DESC,
	"describe":                 DESCRIBE,
	"deterministic":            DETERMINISTIC,
	"disable":                  DISABLE,
	"distinct":                 DISTINCT,
	"distinctrow":              UNUSED,
	"div":                      DIV,
	"double":                   DOUBLE,
	"drop":                     DROP,
	"dumpfile":                 DUMPFILE,
	"duplicate":                DUPLICATE,
	"each":                     EACH,
	"else":                     ELSE,
	"elseif":                   ELSEIF,
	"enable":                   ENABLE,
	"enclosed":                 ENCLOSED,
	"encryption":               ENCRYPTION,
	"end":                      END,
	"enforced":                 ENFORCED,
	"engine":                   ENGINE,
	"engines":                  ENGINES,
	"enum":                     ENUM,
	"error":                    ERROR,
	"errors":                   ERRORS,
	"escape":                   ESCAPE,
	"escaped":                  ESCAPED,
	"event":                    EVENT,
	"except":                   EXCEPT,
	"execute":                  EXECUTE,
	"exists":                   EXISTS,
	"exit":                     EXIT,
	"expansion":                EXPANSION,
	"expire":                   EXPIRE,
	"explain":                  EXPLAIN,
	"failed_login_attempts":    FAILED_LOGIN_ATTEMPTS,
	"false":                    FALSE,
	"fetch":                    UNUSED,
	"fields":                   FIELDS,
	"file":                     FILE,
	"first":                    FIRST,
	"first_value":              FIRST_VALUE,
	"fixed":                    FIXED,
	"float":                    FLOAT_TYPE,
	"float4":                   UNUSED,
	"float8":                   UNUSED,
	"flush":                    FLUSH,
	"following":                FOLLOWING,
	"follows":                  FOLLOWS,
	"for":                      FOR,
	"force":                    FORCE,
	"foreign":                  FOREIGN,
	"format":                   FORMAT,
	"found":                    FOUND,
	"from":                     FROM,
	"full":                     FULL,
	"fulltext":                 FULLTEXT,
	"function":                 FUNCTION,
	"general":                  GENERAL,
	"generated":                GENERATED,
	"geometry":                 GEOMETRY,
	"geometrycollection":       GEOMETRYCOLLECTION,
	"get":                      UNUSED,
	"global":                   GLOBAL,
	"grant":                    GRANT,
	"grants":                   GRANTS,
	"group":                    GROUP,
	"group_concat":             GROUP_CONCAT,
	"grouping":                 GROUPING,
	"groups":                   GROUPS,
	"handler":                  HANDLER,
	"having":                   HAVING,
	"high_priority":            UNUSED,
	"history":                  HISTORY,
	"hosts":                    HOSTS,
	"hour_microsecond":         UNUSED,
	"hour_minute":              UNUSED,
	"hour_second":              UNUSED,
	"identified":               IDENTIFIED,
	"if":                       IF,
	"ignore":                   IGNORE,
	"in":                       IN,
	"index":                    INDEX,
	"indexes":                  INDEXES,
	"infile":                   INFILE,
	"initial":                  INITIAL,
	"inner":                    INNER,
	"inout":                    INOUT,
	"insensitive":              UNUSED,
	"insert":                   INSERT,
	"int":                      INT,
	"int1":                     UNUSED,
	"int2":                     UNUSED,
	"int3":                     UNUSED,
	"int4":                     UNUSED,
	"int8":                     UNUSED,
	"integer":                  INTEGER,
	"interval":                 INTERVAL,
	"into":                     INTO,
	"invoker":                  INVOKER,
	"io_after_gtids":           UNUSED,
	"is":                       IS,
	"isolation":                ISOLATION,
	"issuer":                   ISSUER,
	"iterate":                  UNUSED,
	"join":                     JOIN,
	"json":                     JSON,
	"json_arrayagg":            JSON_ARRAYAGG,
	"json_objectagg":           JSON_OBJECTAGG,
	"json_table":               JSON_TABLE,
	"key":                      KEY,
	"key_block_size":           KEY_BLOCK_SIZE,
	"keys":                     KEYS,
	"kill":                     KILL,
	"lag":                      LAG,
	"language":                 LANGUAGE,
	"last_insert_id":           LAST_INSERT_ID,
	"last_value":               LAST_VALUE,
	"lead":                     LEAD,
	"leading":                  LEADING,
	"leave":                    UNUSED,
	"left":                     LEFT,
	"less":                     LESS,
	"level":                    LEVEL,
	"like":                     LIKE,
	"limit":                    LIMIT,
	"linear":                   UNUSED,
	"lines":                    LINES,
	"linestring":               LINESTRING,
	"load":                     LOAD,
	"local":                    LOCAL,
	"localtime":                LOCALTIME,
	"localtimestamp":           LOCALTIMESTAMP,
	"lock":                     LOCK,
	"logs":                     LOGS,
	"long":                     LONG,
	"longblob":                 LONGBLOB,
	"longtext":                 LONGTEXT,
	"loop":                     UNUSED,
	"low_priority":             LOW_PRIORITY,
	"master_bind":              UNUSED,
	"match":                    MATCH,
	"max":                      MAX,
	"max_connections_per_hour": MAX_CONNECTIONS_PER_HOUR,
	"max_queries_per_hour":     MAX_QUERIES_PER_HOUR,
	"max_updates_per_hour":     MAX_UPDATES_PER_HOUR,
	"max_user_connections":     MAX_USER_CONNECTIONS,
	"maxvalue":                 MAXVALUE,
	"mediumblob":               MEDIUMBLOB,
	"mediumint":                MEDIUMINT,
	"mediumtext":               MEDIUMTEXT,
	"merge":                    MERGE,
	"message_text":             MESSAGE_TEXT,
	"middleint":                UNUSED,
	"min":                      MIN,
	"minute_microsecond":       UNUSED,
	"minute_second":            UNUSED,
	"mod":                      MOD,
	"mode":                     MODE,
	"modifies":                 MODIFIES,
	"modify":                   MODIFY,
	"multilinestring":          MULTILINESTRING,
	"multipoint":               MULTIPOINT,
	"multipolygon":             MULTIPOLYGON,
	"mysql_errno":              MYSQL_ERRNO,
	"names":                    NAMES,
	"national":                 NATIONAL,
	"natural":                  NATURAL,
	"nchar":                    NCHAR,
	"never":                    NEVER,
	"next":                     NEXT,
	"no":                       NO,
	"no_write_to_binlog":       NO_WRITE_TO_BINLOG,
	"none":                     NONE,
	"not":                      NOT,
	"nth_value":                NTH_VALUE,
	"ntile":                    NTILE,
	"null":                     NULL,
	"numeric":                  NUMERIC,
	"nvarchar":                 NVARCHAR,
	"of":                       OF,
	"off":                      OFF,
	"offset":                   OFFSET,
	"on":                       ON,
	"only":                     ONLY,
	"optimize":                 OPTIMIZE,
	"optimizer_costs":          OPTIMIZER_COSTS,
	"option":                   OPTION,
	"optional":                 OPTIONAL,
	"optionally":               OPTIONALLY,
	"or":                       OR,
	"order":                    ORDER,
	"out":                      OUT,
	"outer":                    OUTER,
	"outfile":                  OUTFILE,
	"over":                     OVER,
	"partition":                PARTITION,
	"password":                 PASSWORD,
	"password_lock_time":       PASSWORD_LOCK_TIME,
	"path":                     PATH,
	"percent_rank":             PERCENT_RANK,
	"persist":                  PERSIST,
	"persist_only":             PERSIST_ONLY,
	"plugins":                  PLUGINS,
	"point":                    POINT,
	"polygon":                  POLYGON,
	"preceding":                PRECEDING,
	"precedes":                 PRECEDES,
	"precision":                PRECISION,
	"primary":                  PRIMARY,
	"privileges":               PRIVILEGES,
	"procedure":                PROCEDURE,
	"process":                  PROCESS,
	"processlist":              PROCESSLIST,
	"proxy":                    PROXY,
	"query":                    QUERY,
	"random":                   RANDOM,
	"range":                    RANGE,
	"rank":                     RANK,
	"read":                     READ,
	"read_write":               UNUSED,
	"reads":                    READS,
	"real":                     REAL,
	"recursive":                RECURSIVE,
	"references":               REFERENCES,
	"regexp":                   REGEXP,
	"relay":                    RELAY,
	"release":                  RELEASE,
	"reload":                   RELOAD,
	"rename":                   RENAME,
	"reorganize":               REORGANIZE,
	"repair":                   REPAIR,
	"repeat":                   UNUSED,
	"repeatable":               REPEATABLE,
	"replace":                  REPLACE,
	"replication":              REPLICATION,
	"require":                  REQUIRE,
	"resignal":                 RESIGNAL,
	"restrict":                 RESTRICT,
	"reuse":                    REUSE,
	"revoke":                   REVOKE,
	"right":                    RIGHT,
	"rlike":                    REGEXP,
	"role":                     ROLE,
	"rollback":                 ROLLBACK,
	"routine":                  ROUTINE,
	"row":                      ROW,
	"rows":                     ROWS,
	"row_number":               ROW_NUMBER,
	"savepoint":                SAVEPOINT,
	"schema":                   SCHEMA,
	"schema_name":              SCHEMA_NAME,
	"schemas":                  SCHEMAS,
	"security":                 SECURITY,
	"second_microsecond":       UNUSED,
	"select":                   SELECT,
	"sensitive":                UNUSED,
	"separator":                SEPARATOR,
	"sequence":                 SEQUENCE,
	"serializable":             SERIALIZABLE,
	"session":                  SESSION,
	"set":                      SET,
	"share":                    SHARE,
	"show":                     SHOW,
	"shutdown":                 SHUTDOWN,
	"signal":                   SIGNAL,
	"signed":                   SIGNED,
	"slave":                    SLAVE,
	"slow":                     SLOW,
	"smallint":                 SMALLINT,
	"spatial":                  SPATIAL,
	"specific":                 UNUSED,
	"sql":                      SQL,
	"sql_big_result":           UNUSED,
	"sql_cache":                SQL_CACHE,
	"sql_calc_found_rows":      SQL_CALC_FOUND_ROWS,
	"sql_no_cache":             SQL_NO_CACHE,
	"sql_small_result":         UNUSED,
	"sqlexception":             SQLEXCEPTION,
	"sqlstate":                 SQLSTATE,
	"sqlwarning":               SQLWARNING,
	"srid":                     SRID,
	"ssl":                      SSL,
	"start":                    START,
	"starting":                 STARTING,
	"status":                   STATUS,
	"std":                      STD,
	"stddev":                   STDDEV,
	"stddev_pop":               STDDEV_POP,
	"stddev_samp":              STDDEV_SAMP,
	"stored":                   STORED,
	"straight_join":            STRAIGHT_JOIN,
	"stream":                   STREAM,
	"subclass_origin":          SUBCLASS_ORIGIN,
	"subject":                  SUBJECT,
	"substr":                   SUBSTR,
	"substring":                SUBSTRING,
	"sum":                      SUM,
	"super":                    SUPER,
	"table":                    TABLE,
	"table_name":               TABLE_NAME,
	"tables":                   TABLES,
	"tablespace":               TABLESPACE,
	"terminated":               TERMINATED,
	"temporary":                TEMPORARY,
	"temptable":                TEMPTABLE,
	"text":                     TEXT,
	"than":                     THAN,
	"then":                     THEN,
	"time":                     TIME,
	"timestamp":                TIMESTAMP,
	"timestampadd":             TIMESTAMPADD,
	"timestampdiff":            TIMESTAMPDIFF,
	"tinyblob":                 TINYBLOB,
	"tinyint":                  TINYINT,
	"tinytext":                 TINYTEXT,
	"to":                       TO,
	"trailing":                 TRAILING,
	"transaction":              TRANSACTION,
	"trigger":                  TRIGGER,
	"triggers":                 TRIGGERS,
	"trim":                     TRIM,
	"true":                     TRUE,
	"truncate":                 TRUNCATE,
	"unbounded":                UNBOUNDED,
	"uncommitted":              UNCOMMITTED,
	"undefined":                UNDEFINED,
	"undo":                     UNDO,
	"union":                    UNION,
	"unique":                   UNIQUE,
	"unlock":                   UNLOCK,
	"unsigned":                 UNSIGNED,
	"update":                   UPDATE,
	"usage":                    USAGE,
	"use":                      USE,
	"user":                     USER,
	"user_resources":           USER_RESOURCES,
	"using":                    USING,
	"utc_date":                 UTC_DATE,
	"utc_time":                 UTC_TIME,
	"utc_timestamp":            UTC_TIMESTAMP,
	"value":                    VALUE,
	"values":                   VALUES,
	"var_pop":                  VAR_POP,
	"varbinary":                VARBINARY,
	"varchar":                  VARCHAR,
	"varcharacter":             UNUSED,
	"variables":                VARIABLES,
	"variance":                 VARIANCE,
	"varying":                  VARYING,
	"view":                     VIEW,
	"virtual":                  VIRTUAL,
	"warnings":                 WARNINGS,
	"when":                     WHEN,
	"where":                    WHERE,
	"while":                    UNUSED,
	"window":                   WINDOW,
	"with":                     WITH,
	"work":                     WORK,
	"write":                    WRITE,
	"x509":                     X509,
	"xor":                      XOR,
	"year":                     YEAR,
	"year_month":               UNUSED,
	"zerofill":                 ZEROFILL,
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
	typ, val := tkn.Scan()
	for typ == COMMENT {
		if tkn.AllowComments {
			break
		}
		typ, val = tkn.Scan()
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
	if tkn.stopped {
		return 0, nil
	}

	tkn.OldPosition = tkn.Position

	if tkn.specialComment != nil {
		// Enter specialComment scan mode.
		// for scanning such kind of comment: /*! MySQL-specific code */
		specialComment := tkn.specialComment
		tok, val := specialComment.Scan()
		tkn.Position = specialComment.Position

		if tok != 0 {
			// return the specialComment scan result as the result
			return tok, val
		}

		// reset the position to what it was when we originally finished parsing the special comment
		tkn.Position = tkn.specialCommentEndPos

		// leave specialComment scan mode after all stream consumed.
		tkn.specialComment = nil
	}

	if tkn.potentialAccountName {
		defer func() {
			tkn.potentialAccountName = false
		}()
	}

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
		return tkn.scanIdentifier(byte(ch), false)
	case ch == '@':
		tkn.next()
		if tkn.potentialAccountName {
			return int('@'), nil
		}
		isDbSystemVariable := false
		if ch == '@' && tkn.lastChar == '@' {
			isDbSystemVariable = true
		}
		return tkn.scanIdentifier(byte(ch), isDbSystemVariable)
	case isDigit(ch):
		typ, res := tkn.scanNumber(false)
		if typ != LEX_ERROR {
			return typ, res
		}
		// LEX_ERROR is returned from scanNumber iff we see an unexpected character, so try to parse as an identifier
		// Additionally, if we saw a decimal at any point, throw the LEX_ERROR we received before
		for _, c := range res {
			if c == '.' {
				return typ, res
			}
		}
		typ1, res1 := tkn.scanIdentifier(byte(tkn.lastChar), false)
		return typ1, append(res, res1[1:]...) // Concatenate the two partial symbols
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
	if isDbSystemVariable {
		buffer.WriteByte(byte(tkn.lastChar))
		tkn.next()
	}
	for isLetter(tkn.lastChar) || isDigit(tkn.lastChar) || (isDbSystemVariable && isCarat(tkn.lastChar)) {
		buffer.WriteByte(byte(tkn.lastChar))
		tkn.next()
	}
	if tkn.lastChar == '@' {
		tkn.potentialAccountName = true
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
	if tkn.lastChar == '@' {
		tkn.potentialAccountName = true
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

	if tkn.lastChar == '@' {
		tkn.potentialAccountName = true
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

	foundStartPos := false
	startOffset := 0
	digitCount := 0

	for {
		if tkn.lastChar == '*' {
			tkn.consumeNext(buffer)
			if tkn.lastChar == '/' {
				tkn.consumeNext(buffer)
				tkn.specialCommentEndPos = tkn.Position
				break
			}
			continue
		}
		if tkn.lastChar == eofChar {
			return LEX_ERROR, buffer.Bytes()
		}
		tkn.consumeNext(buffer)

		// Already found special comment starting point
		if foundStartPos {
			continue
		}

		// Haven't reached character count
		if digitCount < 5 {
			if isDigit(tkn.lastChar) {
				// Increase digit count
				digitCount++
				continue
			} else {
				// Provided less than 5 digits, but force this to move on
				digitCount = 5
			}
		}

		// If no longer counting digits, ignore spaces until first non-space character
		if unicode.IsSpace(rune(tkn.lastChar)) {
			continue
		}

		// Found start of subexpression
		startOffset = tkn.Position - 1
		foundStartPos = true
	}
	_, sql := ExtractMysqlComment(buffer.String())

	tkn.specialComment = NewStringTokenizer(sql)
	tkn.specialComment.Position = startOffset

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
	tkn.specialComment = nil
	tkn.posVarIndex = 0
	tkn.nesting = 0
	bufLeft := len(tkn.buf) - tkn.bufPos
	if len(tkn.queryBuf) > bufLeft {
		tkn.queryBuf = tkn.queryBuf[len(tkn.queryBuf)-bufLeft:]
	}
	tkn.Position = 0
	tkn.OldPosition = 0
}

func isLetter(ch uint16) bool {
	return 'a' <= ch && ch <= 'z' || 'A' <= ch && ch <= 'Z' || ch == '_'
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
