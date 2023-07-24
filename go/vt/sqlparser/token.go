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
	"strconv"
	"strings"
	"unicode"

	"github.com/dolthub/vitess/go/bytes2"
	"github.com/dolthub/vitess/go/sqltypes"
	"github.com/dolthub/vitess/go/vt/vterrors"
)

const (
	defaultBufSize = 4096
	eofChar        = 0x100
	backtickQuote  = uint16('`')
	doubleQuote    = uint16('"')
	singleQuote    = uint16('\'')
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
	lastTyp  int
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

	// identifierQuotes holds the characters that are treated as identifier quotes. This always includes
	// the backtick char. When the ANSI_QUOTES SQL mode is enabled, it also includes the double quote char.
	identifierQuotes []uint16

	// stringLiteralQuotes holds the characters that are treated as string literal quotes. This always includes the
	// single quote char. When ANSI_QUOTES SQL mode is NOT enabled, this also contains the double quote character.
	stringLiteralQuotes []uint16

	queryBuf []byte
}

// NewStringTokenizer creates a new Tokenizer for the
// sql string.
func NewStringTokenizer(sql string) *Tokenizer {
	buf := []byte(sql)
	return &Tokenizer{
		buf:     buf,
		bufSize: len(buf),
		identifierQuotes:    []uint16{backtickQuote},
		stringLiteralQuotes: []uint16{doubleQuote, singleQuote},
	}
}

// NewStringTokenizerForAnsiQuotes creates a new Tokenizer for the specified |sql| string, configured for
// ANSI_QUOTES SQL mode, meaning that any double quotes will be interpreted as quotes around an identifier,
// not around a string literal.
func NewStringTokenizerForAnsiQuotes(sql string) *Tokenizer {
	buf := []byte(sql)
	return &Tokenizer{
		buf:                 buf,
		bufSize:             len(buf),
		identifierQuotes:    []uint16{backtickQuote, doubleQuote},
		stringLiteralQuotes: []uint16{singleQuote},
	}
}

// NewTokenizer creates a new Tokenizer reading a sql string from the io.Reader, using the
// default parser options.
func NewTokenizer(r io.Reader) *Tokenizer {
	return &Tokenizer{
		InStream: r,
		buf:      make([]byte, defaultBufSize),
		identifierQuotes:    []uint16{backtickQuote},
		stringLiteralQuotes: []uint16{doubleQuote, singleQuote},
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
	"_armscii8":                     UNDERSCORE_ARMSCII8,
	"_ascii":                        UNDERSCORE_ASCII,
	"_big5":                         UNDERSCORE_BIG5,
	"_binary":                       UNDERSCORE_BINARY,
	"_cp1250":                       UNDERSCORE_CP1250,
	"_cp1251":                       UNDERSCORE_CP1251,
	"_cp1256":                       UNDERSCORE_CP1256,
	"_cp1257":                       UNDERSCORE_CP1257,
	"_cp850":                        UNDERSCORE_CP850,
	"_cp852":                        UNDERSCORE_CP852,
	"_cp866":                        UNDERSCORE_CP866,
	"_cp932":                        UNDERSCORE_CP932,
	"_dec8":                         UNDERSCORE_DEC8,
	"_eucjpms":                      UNDERSCORE_EUCJPMS,
	"_euckr":                        UNDERSCORE_EUCKR,
	"_gb18030":                      UNDERSCORE_GB18030,
	"_gb2312":                       UNDERSCORE_GB2312,
	"_gbk":                          UNDERSCORE_GBK,
	"_geostd8":                      UNDERSCORE_GEOSTD8,
	"_greek":                        UNDERSCORE_GREEK,
	"_hebrew":                       UNDERSCORE_HEBREW,
	"_hp8":                          UNDERSCORE_HP8,
	"_keybcs2":                      UNDERSCORE_KEYBCS2,
	"_koi8r":                        UNDERSCORE_KOI8R,
	"_koi8u":                        UNDERSCORE_KOI8U,
	"_latin1":                       UNDERSCORE_LATIN1,
	"_latin2":                       UNDERSCORE_LATIN2,
	"_latin5":                       UNDERSCORE_LATIN5,
	"_latin7":                       UNDERSCORE_LATIN7,
	"_macce":                        UNDERSCORE_MACCE,
	"_macroman":                     UNDERSCORE_MACROMAN,
	"_sjis":                         UNDERSCORE_SJIS,
	"_swe7":                         UNDERSCORE_SWE7,
	"_tis620":                       UNDERSCORE_TIS620,
	"_ucs2":                         UNDERSCORE_UCS2,
	"_ujis":                         UNDERSCORE_UJIS,
	"_utf16":                        UNDERSCORE_UTF16,
	"_utf16le":                      UNDERSCORE_UTF16LE,
	"_utf32":                        UNDERSCORE_UTF32,
	"_utf8":                         UNDERSCORE_UTF8,
	"_utf8mb3":                      UNDERSCORE_UTF8MB3,
	"_utf8mb4":                      UNDERSCORE_UTF8MB4,
	"accessible":                    ACCESSIBLE,
	"account":                       ACCOUNT,
	"action":                        ACTION,
	"add":                           ADD,
	"admin":                         ADMIN,
	"after":                         AFTER,
	"against":                       AGAINST,
	"algorithm":                     ALGORITHM,
	"all":                           ALL,
	"alter":                         ALTER,
	"always":                        ALWAYS,
	"analyze":                       ANALYZE,
	"and":                           AND,
	"application_password_admin":    APPLICATION_PASSWORD_ADMIN,
	"array":                         ARRAY,
	"as":                            AS,
	"asc":                           ASC,
	"asensitive":                    ASENSITIVE,
	"at":                            AT,
	"attribute":                     ATTRIBUTE,
	"audit_abort_exempt":            AUDIT_ABORT_EXEMPT,
	"audit_admin":                   AUDIT_ADMIN,
	"authentication":                AUTHENTICATION,
	"authentication_policy_admin":   AUTHENTICATION_POLICY_ADMIN,
	"auto_increment":                AUTO_INCREMENT,
	"avg":                           AVG,
	"avg_row_length":                AVG_ROW_LENGTH,
	"backup_admin":                  BACKUP_ADMIN,
	"before":                        BEFORE,
	"begin":                         BEGIN,
	"between":                       BETWEEN,
	"bigint":                        BIGINT,
	"binary":                        BINARY,
	"binlog_admin":                  BINLOG_ADMIN,
	"binlog_encryption_admin":       BINLOG_ENCRYPTION_ADMIN,
	"bit":                           BIT,
	"bit_and":                       BIT_AND,
	"bit_or":                        BIT_OR,
	"bit_xor":                       BIT_XOR,
	"blob":                          BLOB,
	"bool":                          BOOL,
	"boolean":                       BOOLEAN,
	"both":                          BOTH,
	"by":                            BY,
	"call":                          CALL,
	"cascade":                       CASCADE,
	"case":                          CASE,
	"cast":                          CAST,
	"catalog_name":                  CATALOG_NAME,
	"chain":                         CHAIN,
	"change":                        CHANGE,
	"channel":                       CHANNEL,
	"char":                          CHAR,
	"character":                     CHARACTER,
	"charset":                       CHARSET,
	"check":                         CHECK,
	"checksum":                      CHECKSUM,
	"cipher":                        CIPHER,
	"class_origin":                  CLASS_ORIGIN,
	"client":                        CLIENT,
	"clone_admin":                   CLONE_ADMIN,
	"close":                         CLOSE,
	"collate":                       COLLATE,
	"collation":                     COLLATION,
	"column":                        COLUMN,
	"column_name":                   COLUMN_NAME,
	"columns":                       COLUMNS,
	"comment":                       COMMENT_KEYWORD,
	"commit":                        COMMIT,
	"committed":                     COMMITTED,
	"compact":                       COMPACT,
	"completion":                    COMPLETION,
	"compressed":                    COMPRESSED,
	"compression":                   COMPRESSION,
	"condition":                     CONDITION,
	"connection":                    CONNECTION,
	"connection_admin":              CONNECTION_ADMIN,
	"constraint":                    CONSTRAINT,
	"constraint_catalog":            CONSTRAINT_CATALOG,
	"constraint_name":               CONSTRAINT_NAME,
	"constraint_schema":             CONSTRAINT_SCHEMA,
	"contains":                      CONTAINS,
	"continue":                      CONTINUE,
	"convert":                       CONVERT,
	"count":                         COUNT,
	"create":                        CREATE,
	"cross":                         CROSS,
	"cube":                          CUBE,
	"cume_dist":                     CUME_DIST,
	"current":                       CURRENT,
	"current_date":                  CURRENT_DATE,
	"current_time":                  CURRENT_TIME,
	"current_timestamp":             CURRENT_TIMESTAMP,
	"current_user":                  CURRENT_USER,
	"cursor":                        CURSOR,
	"cursor_name":                   CURSOR_NAME,
	"data":                          DATA,
	"database":                      DATABASE,
	"databases":                     DATABASES,
	"date":                          DATE,
	"datetime":                      DATETIME,
	"day":                           DAY,
	"day_hour":                      DAY_HOUR,
	"day_microsecond":               DAY_MICROSECOND,
	"day_minute":                    DAY_MINUTE,
	"day_second":                    DAY_SECOND,
	"deallocate":                    DEALLOCATE,
	"dec":                           DEC,
	"decimal":                       DECIMAL,
	"declare":                       DECLARE,
	"default":                       DEFAULT,
	"definer":                       DEFINER,
	"definition":                    DEFINITION,
	"delay_key_write":               DELAY_KEY_WRITE,
	"delayed":                       DELAYED,
	"delete":                        DELETE,
	"dense_rank":                    DENSE_RANK,
	"desc":                          DESC,
	"describe":                      DESCRIBE,
	"description":                   DESCRIPTION,
	"deterministic":                 DETERMINISTIC,
	"directory":                     DIRECTORY,
	"disable":                       DISABLE,
	"disk":                          DISK,
	"distinct":                      DISTINCT,
	"distinctrow":                   DISTINCTROW,
	"div":                           DIV,
	"do":                            DO,
	"double":                        DOUBLE,
	"drop":                          DROP,
	"dual":                          DUAL,
	"dumpfile":                      DUMPFILE,
	"duplicate":                     DUPLICATE,
	"dynamic":                       DYNAMIC,
	"each":                          EACH,
	"else":                          ELSE,
	"elseif":                        ELSEIF,
	"empty":                         EMPTY,
	"enable":                        ENABLE,
	"enclosed":                      ENCLOSED,
	"encryption":                    ENCRYPTION,
	"encryption_key_admin":          ENCRYPTION_KEY_ADMIN,
	"end":                           END,
	"ends":                          ENDS,
	"enforced":                      ENFORCED,
	"engine":                        ENGINE,
	"engine_attribute":              ENGINE_ATTRIBUTE,
	"engines":                       ENGINES,
	"enum":                          ENUM,
	"error":                         ERROR,
	"errors":                        ERRORS,
	"escape":                        ESCAPE,
	"escaped":                       ESCAPED,
	"event":                         EVENT,
	"events":                        EVENTS,
	"every":                         EVERY,
	"except":                        EXCEPT,
	"execute":                       EXECUTE,
	"exists":                        EXISTS,
	"exit":                          EXIT,
	"expansion":                     EXPANSION,
	"expire":                        EXPIRE,
	"explain":                       EXPLAIN,
	"extended":                      EXTENDED,
	"extract":                       EXTRACT,
	"failed_login_attempts":         FAILED_LOGIN_ATTEMPTS,
	"false":                         FALSE,
	"fetch":                         FETCH,
	"fields":                        FIELDS,
	"file":                          FILE,
	"filter":                        FILTER,
	"firewall_admin":                FIREWALL_ADMIN,
	"firewall_exempt":               FIREWALL_EXEMPT,
	"firewall_user":                 FIREWALL_USER,
	"first":                         FIRST,
	"first_value":                   FIRST_VALUE,
	"fixed":                         FIXED,
	"float":                         FLOAT_TYPE,
	"float4":                        FLOAT4,
	"float8":                        FLOAT8,
	"flush":                         FLUSH,
	"flush_optimizer_costs":         FLUSH_OPTIMIZER_COSTS,
	"flush_status":                  FLUSH_STATUS,
	"flush_tables":                  FLUSH_TABLES,
	"flush_user_resources":          FLUSH_USER_RESOURCES,
	"following":                     FOLLOWING,
	"follows":                       FOLLOWS,
	"for":                           FOR,
	"force":                         FORCE,
	"foreign":                       FOREIGN,
	"format":                        FORMAT,
	"found":                         FOUND,
	"from":                          FROM,
	"full":                          FULL,
	"fulltext":                      FULLTEXT,
	"function":                      FUNCTION,
	"general":                       GENERAL,
	"generated":                     GENERATED,
	"geometry":                      GEOMETRY,
	"geometrycollection":            GEOMETRYCOLLECTION,
	"get":                           GET,
	"global":                        GLOBAL,
	"grant":                         GRANT,
	"grants":                        GRANTS,
	"group":                         GROUP,
	"group_concat":                  GROUP_CONCAT,
	"group_replication_admin":       GROUP_REPLICATION_ADMIN,
	"group_replication_stream":      GROUP_REPLICATION_STREAM,
	"grouping":                      GROUPING,
	"groups":                        GROUPS,
	"handler":                       HANDLER,
	"hash":                          HASH,
	"having":                        HAVING,
	"high_priority":                 HIGH_PRIORITY,
	"history":                       HISTORY,
	"hosts":                         HOSTS,
	"hour":                          HOUR,
	"hour_microsecond":              HOUR_MICROSECOND,
	"hour_minute":                   HOUR_MINUTE,
	"hour_second":                   HOUR_SECOND,
	"identified":                    IDENTIFIED,
	"if":                            IF,
	"ignore":                        IGNORE,
	"in":                            IN,
	"index":                         INDEX,
	"indexes":                       INDEXES,
	"infile":                        INFILE,
	"initial":                       INITIAL,
	"inner":                         INNER,
	"innodb_redo_log_archive":       INNODB_REDO_LOG_ARCHIVE,
	"innodb_redo_log_enable":        INNODB_REDO_LOG_ENABLE,
	"inout":                         INOUT,
	"insensitive":                   INSENSITIVE,
	"insert":                        INSERT,
	"insert_method":                 INSERT_METHOD,
	"int":                           INT,
	"int1":                          INT1,
	"int2":                          INT2,
	"int3":                          INT3,
	"int4":                          INT4,
	"int8":                          INT8,
	"integer":                       INTEGER,
	"interval":                      INTERVAL,
	"into":                          INTO,
	"invisible":                     INVISIBLE,
	"invoker":                       INVOKER,
	"io_after_gtids":                IO_AFTER_GTIDS,
	"io_before_gtids":               IO_BEFORE_GTIDS,
	"is":                            IS,
	"isolation":                     ISOLATION,
	"issuer":                        ISSUER,
	"iterate":                       ITERATE,
	"join":                          JOIN,
	"json":                          JSON,
	"json_arrayagg":                 JSON_ARRAYAGG,
	"json_objectagg":                JSON_OBJECTAGG,
	"json_table":                    JSON_TABLE,
	"key":                           KEY,
	"key_block_size":                KEY_BLOCK_SIZE,
	"keys":                          KEYS,
	"kill":                          KILL,
	"lag":                           LAG,
	"language":                      LANGUAGE,
	"last_insert_id":                LAST_INSERT_ID,
	"last_value":                    LAST_VALUE,
	"lateral":                       LATERAL,
	"lead":                          LEAD,
	"leading":                       LEADING,
	"leave":                         LEAVE,
	"left":                          LEFT,
	"less":                          LESS,
	"level":                         LEVEL,
	"like":                          LIKE,
	"limit":                         LIMIT,
	"linear":                        LINEAR,
	"lines":                         LINES,
	"linestring":                    LINESTRING,
	"list":                          LIST,
	"load":                          LOAD,
	"local":                         LOCAL,
	"localtime":                     LOCALTIME,
	"localtimestamp":                LOCALTIMESTAMP,
	"lock":                          LOCK,
	"logs":                          LOGS,
	"long":                          LONG,
	"longblob":                      LONGBLOB,
	"longtext":                      LONGTEXT,
	"loop":                          LOOP,
	"low_priority":                  LOW_PRIORITY,
	"master_bind":                   MASTER_BIND,
	"master_ssl_verify_server_cert": MASTER_SSL_VERIFY_SERVER_CERT,
	"match":                         MATCH,
	"max":                           MAX,
	"max_connections_per_hour":      MAX_CONNECTIONS_PER_HOUR,
	"max_queries_per_hour":          MAX_QUERIES_PER_HOUR,
	"max_rows":                      MAX_ROWS,
	"max_updates_per_hour":          MAX_UPDATES_PER_HOUR,
	"max_user_connections":          MAX_USER_CONNECTIONS,
	"maxvalue":                      MAXVALUE,
	"mediumblob":                    MEDIUMBLOB,
	"mediumint":                     MEDIUMINT,
	"mediumtext":                    MEDIUMTEXT,
	"merge":                         MERGE,
	"message_text":                  MESSAGE_TEXT,
	"middleint":                     MIDDLEINT,
	"microsecond":                   MICROSECOND,
	"min":                           MIN,
	"min_rows":                      MIN_ROWS,
	"minute":                        MINUTE,
	"minute_microsecond":            MINUTE_MICROSECOND,
	"minute_second":                 MINUTE_SECOND,
	"mod":                           MOD,
	"mode":                          MODE,
	"modifies":                      MODIFIES,
	"modify":                        MODIFY,
	"month":                         MONTH,
	"multilinestring":               MULTILINESTRING,
	"multipoint":                    MULTIPOINT,
	"multipolygon":                  MULTIPOLYGON,
	"mysql_errno":                   MYSQL_ERRNO,
	"name":                          NAME,
	"names":                         NAMES,
	"national":                      NATIONAL,
	"natural":                       NATURAL,
	"nested":                        NESTED,
	"nchar":                         NCHAR,
	"ndb_stored_user":               NDB_STORED_USER,
	"never":                         NEVER,
	"next":                          NEXT,
	"no":                            NO,
	"no_write_to_binlog":            NO_WRITE_TO_BINLOG,
	"none":                          NONE,
	"not":                           NOT,
	"now":                           NOW,
	"nowait":                        NOWAIT,
	"nth_value":                     NTH_VALUE,
	"ntile":                         NTILE,
	"null":                          NULL,
	"numeric":                       NUMERIC,
	"nvarchar":                      NVARCHAR,
	"of":                            OF,
	"off":                           OFF,
	"offset":                        OFFSET,
	"on":                            ON,
	"only":                          ONLY,
	"open":                          OPEN,
	"optimize":                      OPTIMIZE,
	"optimizer_costs":               OPTIMIZER_COSTS,
	"option":                        OPTION,
	"optional":                      OPTIONAL,
	"optionally":                    OPTIONALLY,
	"or":                            OR,
	"order":                         ORDER,
	"ordinality":                    ORDINALITY,
	"organization":                  ORGANIZATION,
	"out":                           OUT,
	"outer":                         OUTER,
	"outfile":                       OUTFILE,
	"over":                          OVER,
	"pack_keys":                     PACK_KEYS,
	"partition":                     PARTITION,
	"partitions":                    PARTITIONS,
	"password":                      PASSWORD,
	"password_lock_time":            PASSWORD_LOCK_TIME,
	"passwordless_user_admin":       PASSWORDLESS_USER_ADMIN,
	"path":                          PATH,
	"percent_rank":                  PERCENT_RANK,
	"persist":                       PERSIST,
	"persist_only":                  PERSIST_ONLY,
	"persist_ro_variables_admin":    PERSIST_RO_VARIABLES_ADMIN,
	"plugins":                       PLUGINS,
	"point":                         POINT,
	"polygon":                       POLYGON,
	"precedes":                      PRECEDES,
	"preceding":                     PRECEDING,
	"precision":                     PRECISION,
	"prepare":                       PREPARE,
	"preserve":                      PRESERVE,
	"primary":                       PRIMARY,
	"privileges":                    PRIVILEGES,
	"procedure":                     PROCEDURE,
	"process":                       PROCESS,
	"processlist":                   PROCESSLIST,
	"proxy":                         PROXY,
	"purge":                         PURGE,
	"quarter":                       QUARTER,
	"query":                         QUERY,
	"random":                        RANDOM,
	"range":                         RANGE,
	"rank":                          RANK,
	"read":                          READ,
	"read_write":                    READ_WRITE,
	"reads":                         READS,
	"real":                          REAL,
	"recursive":                     RECURSIVE,
	"redundant":                     REDUNDANT,
	"reference":                     REFERENCE,
	"references":                    REFERENCES,
	"regexp":                        REGEXP,
	"relay":                         RELAY,
	"release":                       RELEASE,
	"reload":                        RELOAD,
	"rename":                        RENAME,
	"reorganize":                    REORGANIZE,
	"repair":                        REPAIR,
	"repeat":                        REPEAT,
	"repeatable":                    REPEATABLE,
	"replace":                       REPLACE,
	"replica":                       REPLICA,
	"replicate_do_table":            REPLICATE_DO_TABLE,
	"replicate_ignore_table":        REPLICATE_IGNORE_TABLE,
	"replication":                   REPLICATION,
	"replication_applier":           REPLICATION_APPLIER,
	"replication_slave_admin":       REPLICATION_SLAVE_ADMIN,
	"require":                       REQUIRE,
	"reset":                         RESET,
	"resignal":                      RESIGNAL,
	"resource_group_admin":          RESOURCE_GROUP_ADMIN,
	"resource_group_user":           RESOURCE_GROUP_USER,
	"restrict":                      RESTRICT,
	"return":                        RETURN,
	"reuse":                         REUSE,
	"revoke":                        REVOKE,
	"right":                         RIGHT,
	"rlike":                         REGEXP,
	"role":                          ROLE,
	"role_admin":                    ROLE_ADMIN,
	"rollback":                      ROLLBACK,
	"routine":                       ROUTINE,
	"row":                           ROW,
	"row_format":                    ROW_FORMAT,
	"row_number":                    ROW_NUMBER,
	"rows":                          ROWS,
	"savepoint":                     SAVEPOINT,
	"schedule":                      SCHEDULE,
	"schema":                        SCHEMA,
	"schema_name":                   SCHEMA_NAME,
	"schemas":                       SCHEMAS,
	"second":                        SECOND,
	"second_microsecond":            SECOND_MICROSECOND,
	"secondary_engine_attribute":    SECONDARY_ENGINE_ATTRIBUTE,
	"security":                      SECURITY,
	"select":                        SELECT,
	"sensitive":                     SENSITIVE,
	"sensitive_variables_observer":  SENSITIVE_VARIABLES_OBSERVER,
	"separator":                     SEPARATOR,
	"sequence":                      SEQUENCE,
	"serial":                        SERIAL,
	"serializable":                  SERIALIZABLE,
	"session":                       SESSION,
	"session_variables_admin":       SESSION_VARIABLES_ADMIN,
	"set":                           SET,
	"set_user_id":                   SET_USER_ID,
	"share":                         SHARE,
	"show":                          SHOW,
	"show_routine":                  SHOW_ROUTINE,
	"shutdown":                      SHUTDOWN,
	"signal":                        SIGNAL,
	"signed":                        SIGNED,
	"skip_query_rewrite":            SKIP_QUERY_REWRITE,
	"slave":                         SLAVE,
	"slow":                          SLOW,
	"smallint":                      SMALLINT,
	"source":                        SOURCE,
	"source_connect_retry":          SOURCE_CONNECT_RETRY,
	"source_host":                   SOURCE_HOST,
	"source_password":               SOURCE_PASSWORD,
	"source_port":                   SOURCE_PORT,
	"source_retry_count":            SOURCE_RETRY_COUNT,
	"source_user":                   SOURCE_USER,
	"spatial":                       SPATIAL,
	"specific":                      SPECIFIC,
	"sql":                           SQL,
	"sql_big_result":                SQL_BIG_RESULT,
	"sql_cache":                     SQL_CACHE,
	"sql_calc_found_rows":           SQL_CALC_FOUND_ROWS,
	"sql_no_cache":                  SQL_NO_CACHE,
	"sql_small_result":              SQL_SMALL_RESULT,
	"sqlexception":                  SQLEXCEPTION,
	"sqlstate":                      SQLSTATE,
	"sqlwarning":                    SQLWARNING,
	"srid":                          SRID,
	"ssl":                           SSL,
	"start":                         START,
	"starts":                        STARTS,
	"starting":                      STARTING,
	"stats_auto_recalc":             STATS_AUTO_RECALC,
	"stats_persistent":              STATS_PERSISTENT,
	"stats_sample_pages":            STATS_SAMPLE_PAGES,
	"status":                        STATUS,
	"std":                           STD,
	"stddev":                        STDDEV,
	"stddev_pop":                    STDDEV_POP,
	"stddev_samp":                   STDDEV_SAMP,
	"stop":                          STOP,
	"storage":                       STORAGE,
	"stored":                        STORED,
	"straight_join":                 STRAIGHT_JOIN,
	"stream":                        STREAM,
	"subclass_origin":               SUBCLASS_ORIGIN,
	"subject":                       SUBJECT,
	"subpartition":                  SUBPARTITION,
	"subpartitions":                 SUBPARTITIONS,
	"substr":                        SUBSTR,
	"substring":                     SUBSTRING,
	"sum":                           SUM,
	"super":                         SUPER,
	"system":                        SYSTEM,
	"system_variables_admin":        SYSTEM_VARIABLES_ADMIN,
	"table":                         TABLE,
	"table_encryption_admin":        TABLE_ENCRYPTION_ADMIN,
	"table_name":                    TABLE_NAME,
	"tables":                        TABLES,
	"tablespace":                    TABLESPACE,
	"temporary":                     TEMPORARY,
	"temptable":                     TEMPTABLE,
	"terminated":                    TERMINATED,
	"text":                          TEXT,
	"than":                          THAN,
	"then":                          THEN,
	"time":                          TIME,
	"timestamp":                     TIMESTAMP,
	"timestampadd":                  TIMESTAMPADD,
	"timestampdiff":                 TIMESTAMPDIFF,
	"tinyblob":                      TINYBLOB,
	"tinyint":                       TINYINT,
	"tinytext":                      TINYTEXT,
	"to":                            TO,
	"tp_connection_admin":           TP_CONNECTION_ADMIN,
	"trailing":                      TRAILING,
	"transaction":                   TRANSACTION,
	"trigger":                       TRIGGER,
	"triggers":                      TRIGGERS,
	"trim":                          TRIM,
	"true":                          TRUE,
	"truncate":                      TRUNCATE,
	"unbounded":                     UNBOUNDED,
	"uncommitted":                   UNCOMMITTED,
	"undefined":                     UNDEFINED,
	"undo":                          UNDO,
	"union":                         UNION,
	"unique":                        UNIQUE,
	"unlock":                        UNLOCK,
	"unsigned":                      UNSIGNED,
	"until":                         UNTIL,
	"update":                        UPDATE,
	"usage":                         USAGE,
	"use":                           USE,
	"user":                          USER,
	"user_resources":                USER_RESOURCES,
	"using":                         USING,
	"utc_date":                      UTC_DATE,
	"utc_time":                      UTC_TIME,
	"utc_timestamp":                 UTC_TIMESTAMP,
	"value":                         VALUE,
	"values":                        VALUES,
	"var_pop":                       VAR_POP,
	"varbinary":                     VARBINARY,
	"varchar":                       VARCHAR,
	"varcharacter":                  VARCHARACTER,
	"variables":                     VARIABLES,
	"variance":                      VARIANCE,
	"varying":                       VARYING,
	"version_token_admin":           VERSION_TOKEN_ADMIN,
	"view":                          VIEW,
	"virtual":                       VIRTUAL,
	"warnings":                      WARNINGS,
	"week":                          WEEK,
	"when":                          WHEN,
	"where":                         WHERE,
	"while":                         WHILE,
	"window":                        WINDOW,
	"with":                          WITH,
	"work":                          WORK,
	"write":                         WRITE,
	"x509":                          X509,
	"xa_recover_admin":              XA_RECOVER_ADMIN,
	"xor":                           XOR,
	"year":                          YEAR,
	"year_month":                    YEAR_MONTH,
	"zerofill":                      ZEROFILL,
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
	tkn.lastTyp = typ
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
		tok, val := tkn.specialComment.Scan()
		tkn.Position = tkn.specialComment.Position

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
		case contains(tkn.stringLiteralQuotes, ch):
			return tkn.scanString(ch, STRING)
		case contains(tkn.identifierQuotes, ch):
			return tkn.scanLiteralIdentifier(ch)
		default:
			return LEX_ERROR, []byte{byte(ch)}
		}
	}
}

// contains searches the specified |slice| for the target |x|, and returns the same value of |x| if it is found. The
// target value is returned, instead of a boolean response, so that this function can be directly used inside the
// switch statement above that switches on a uint16 value.
func contains(slice []uint16, x uint16) uint16 {
	for _, element := range slice {
		if element == x {
			return element
		}
	}
	return 0
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
	if tkn.lastChar != singleQuote {
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

// scanLiteralIdentifier scans a quoted identifier. The first byte of the quoted identifier has already
// been read from the tokenizer and is passed in as the |startingChar| parameter. The type of token is
// returned as well as the actual content that was parsed.
func (tkn *Tokenizer) scanLiteralIdentifier(startingChar uint16) (int, []byte) {
	buffer := &bytes2.Buffer{}
	identifierQuoteSeen := false
	for {
		if identifierQuoteSeen {
			if tkn.lastChar != startingChar {
				break
			}
			identifierQuoteSeen = false
			buffer.WriteByte(byte(startingChar))
			tkn.next()
			continue
		}
		// The previous char was not a backtick.
		switch tkn.lastChar {
		case startingChar:
			identifierQuoteSeen = true
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
		// If there isn't a previous error, then return the colon as it may be a valid token
		if tkn.LastError == nil {
			return int(':'), buffer.Bytes()
		}
		return LEX_ERROR, buffer.Bytes()
	}
	for isLetter(tkn.lastChar) || isDigit(tkn.lastChar) || tkn.lastChar == '.' {
		buffer.WriteByte(byte(tkn.lastChar))
		tkn.next()
	}
	// Due to the way this is written to handle bindings, it includes the colon on keywords.
	// This is an issue when it comes to labels, so this is a workaround.
	if buffer.Len() >= 5 && buffer.Bytes()[0] == ':' {
		switch strings.ToLower(string(buffer.Bytes())) {
		case ":begin":
			return BEGIN, []byte("BEGIN")
		case ":loop":
			return LOOP, []byte("LOOP")
		case ":repeat":
			return REPEAT, []byte("REPEAT")
		case ":while":
			return WHILE, []byte("WHILE")
		}
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
			tkn.Position += tkn.bufPos - start

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
	
	// mysql strings get auto concatenated, so see if the next token is a string and scan it if so
	tkn.skipBlank()
	if contains(tkn.stringLiteralQuotes, tkn.lastChar) == tkn.lastChar {
		delim := tkn.lastChar
		tkn.next()
		nextTyp, nextStr := tkn.scanString(delim, STRING)
		if nextTyp == STRING {
			return nextTyp, append(buffer.Bytes(), nextStr...)
		} else {
			return LEX_ERROR, buffer.Bytes()
		}
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

// mustAtoi converts the string into an integer, by using strconv.atoi, and returns the result. If any errors are
// encountered, it registers a parsing error with |yylex|.
func mustAtoi(yylex yyLexer, s string) int {
	i, err := strconv.Atoi(s)
	if err != nil {
		yylex.Error(fmt.Sprintf("unable to parse integer from string '%s'", s))
	}
	return i
}
