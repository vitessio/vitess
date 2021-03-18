package sqlparser

import (
	"fmt"
	"sort"
	"strings"
)

type keyword struct {
	name string
	id   int
}

func (k *keyword) match(input []byte) bool {
	if len(input) != len(k.name) {
		return false
	}
	for i, c := range input {
		if 'A' <= c && c <= 'Z' {
			c += 'a' - 'A'
		}
		if k.name[i] != c {
			return false
		}
	}
	return true
}

func (k *keyword) matchStr(input string) bool {
	return keywordASCIIMatch(input, k.name)
}

func keywordASCIIMatch(input string, expected string) bool {
	if len(input) != len(expected) {
		return false
	}
	for i := 0; i < len(input); i++ {
		c := input[i]
		if 'A' <= c && c <= 'Z' {
			c += 'a' - 'A'
		}
		if expected[i] != c {
			return false
		}
	}
	return true
}

// keywords is a table of mysql keywords that fall into two categories:
// 1) keywords considered reserved by MySQL
// 2) keywords for us to handle specially in sql.y
//
// Those marked as UNUSED are likely reserved keywords. We add them here so that
// when rewriting queries we can properly backtick quote them so they don't cause issues
//
// NOTE: If you add new keywords, add them also to the reserved_keywords or
// non_reserved_keywords grammar in sql.y -- this will allow the keyword to be used
// in identifiers. See the docs for each grammar to determine which one to put it into.
var keywords = []keyword{
	{"accessible", UNUSED},
	{"action", ACTION},
	{"add", ADD},
	{"after", AFTER},
	{"against", AGAINST},
	{"algorithm", ALGORITHM},
	{"all", ALL},
	{"alter", ALTER},
	{"analyze", ANALYZE},
	{"and", AND},
	{"as", AS},
	{"asc", ASC},
	{"asensitive", UNUSED},
	{"auto_increment", AUTO_INCREMENT},
	{"avg_row_length", AVG_ROW_LENGTH},
	{"before", UNUSED},
	{"begin", BEGIN},
	{"between", BETWEEN},
	{"bigint", BIGINT},
	{"binary", BINARY},
	{"_binary", UNDERSCORE_BINARY},
	{"_utf8mb4", UNDERSCORE_UTF8MB4},
	{"_utf8", UNDERSCORE_UTF8},
	{"_latin1", UNDERSCORE_LATIN1},
	{"bit", BIT},
	{"blob", BLOB},
	{"bool", BOOL},
	{"boolean", BOOLEAN},
	{"both", UNUSED},
	{"by", BY},
	{"call", CALL},
	{"cancel", CANCEL},
	{"cascade", CASCADE},
	{"cascaded", CASCADED},
	{"case", CASE},
	{"cast", CAST},
	{"channel", CHANNEL},
	{"change", CHANGE},
	{"char", CHAR},
	{"character", CHARACTER},
	{"charset", CHARSET},
	{"check", CHECK},
	{"checksum", CHECKSUM},
	{"coalesce", COALESCE},
	{"code", CODE},
	{"collate", COLLATE},
	{"collation", COLLATION},
	{"column", COLUMN},
	{"columns", COLUMNS},
	{"comment", COMMENT_KEYWORD},
	{"committed", COMMITTED},
	{"commit", COMMIT},
	{"compact", COMPACT},
	{"complete", COMPLETE},
	{"compressed", COMPRESSED},
	{"compression", COMPRESSION},
	{"condition", UNUSED},
	{"connection", CONNECTION},
	{"constraint", CONSTRAINT},
	{"continue", UNUSED},
	{"convert", CONVERT},
	{"copy", COPY},
	{"substr", SUBSTR},
	{"substring", SUBSTRING},
	{"create", CREATE},
	{"cross", CROSS},
	{"csv", CSV},
	{"current_date", CURRENT_DATE},
	{"current_time", CURRENT_TIME},
	{"current_timestamp", CURRENT_TIMESTAMP},
	{"current_user", CURRENT_USER},
	{"cursor", UNUSED},
	{"data", DATA},
	{"database", DATABASE},
	{"databases", DATABASES},
	{"day_hour", UNUSED},
	{"day_microsecond", UNUSED},
	{"day_minute", UNUSED},
	{"day_second", UNUSED},
	{"date", DATE},
	{"datetime", DATETIME},
	{"dec", UNUSED},
	{"decimal", DECIMAL},
	{"declare", UNUSED},
	{"default", DEFAULT},
	{"definer", DEFINER},
	{"delay_key_write", DELAY_KEY_WRITE},
	{"delayed", UNUSED},
	{"delete", DELETE},
	{"desc", DESC},
	{"describe", DESCRIBE},
	{"deterministic", UNUSED},
	{"directory", DIRECTORY},
	{"disable", DISABLE},
	{"discard", DISCARD},
	{"disk", DISK},
	{"distinct", DISTINCT},
	{"distinctrow", DISTINCTROW},
	{"div", DIV},
	{"double", DOUBLE},
	{"do", DO},
	{"drop", DROP},
	{"dumpfile", DUMPFILE},
	{"duplicate", DUPLICATE},
	{"dynamic", DYNAMIC},
	{"each", UNUSED},
	{"else", ELSE},
	{"elseif", UNUSED},
	{"enable", ENABLE},
	{"enclosed", ENCLOSED},
	{"encryption", ENCRYPTION},
	{"end", END},
	{"enforced", ENFORCED},
	{"engine", ENGINE},
	{"engines", ENGINES},
	{"enum", ENUM},
	{"error", ERROR},
	{"escape", ESCAPE},
	{"escaped", ESCAPED},
	{"event", EVENT},
	{"exchange", EXCHANGE},
	{"exclusive", EXCLUSIVE},
	{"exists", EXISTS},
	{"exit", UNUSED},
	{"explain", EXPLAIN},
	{"expansion", EXPANSION},
	{"export", EXPORT},
	{"extended", EXTENDED},
	{"false", FALSE},
	{"fetch", UNUSED},
	{"fields", FIELDS},
	{"first", FIRST},
	{"fixed", FIXED},
	{"float", FLOAT_TYPE},
	{"float4", UNUSED},
	{"float8", UNUSED},
	{"flush", FLUSH},
	{"for", FOR},
	{"force", FORCE},
	{"foreign", FOREIGN},
	{"format", FORMAT},
	{"from", FROM},
	{"full", FULL},
	{"fulltext", FULLTEXT},
	{"function", FUNCTION},
	{"general", GENERAL},
	{"generated", UNUSED},
	{"geometry", GEOMETRY},
	{"geometrycollection", GEOMETRYCOLLECTION},
	{"get", UNUSED},
	{"global", GLOBAL},
	{"grant", UNUSED},
	{"group", GROUP},
	{"group_concat", GROUP_CONCAT},
	{"having", HAVING},
	{"header", HEADER},
	{"high_priority", UNUSED},
	{"hosts", HOSTS},
	{"hour_microsecond", UNUSED},
	{"hour_minute", UNUSED},
	{"hour_second", UNUSED},
	{"if", IF},
	{"ignore", IGNORE},
	{"import", IMPORT},
	{"in", IN},
	{"index", INDEX},
	{"indexes", INDEXES},
	{"infile", UNUSED},
	{"inout", UNUSED},
	{"inner", INNER},
	{"inplace", INPLACE},
	{"insensitive", UNUSED},
	{"insert", INSERT},
	{"insert_method", INSERT_METHOD},
	{"int", INT},
	{"int1", UNUSED},
	{"int2", UNUSED},
	{"int3", UNUSED},
	{"int4", UNUSED},
	{"int8", UNUSED},
	{"integer", INTEGER},
	{"interval", INTERVAL},
	{"into", INTO},
	{"io_after_gtids", UNUSED},
	{"is", IS},
	{"isolation", ISOLATION},
	{"iterate", UNUSED},
	{"invoker", INVOKER},
	{"join", JOIN},
	{"json", JSON},
	{"key", KEY},
	{"keys", KEYS},
	{"keyspaces", KEYSPACES},
	{"key_block_size", KEY_BLOCK_SIZE},
	{"kill", UNUSED},
	{"last", LAST},
	{"language", LANGUAGE},
	{"last_insert_id", LAST_INSERT_ID},
	{"leading", UNUSED},
	{"leave", UNUSED},
	{"left", LEFT},
	{"less", LESS},
	{"level", LEVEL},
	{"like", LIKE},
	{"limit", LIMIT},
	{"linear", UNUSED},
	{"lines", LINES},
	{"linestring", LINESTRING},
	{"load", LOAD},
	{"local", LOCAL},
	{"localtime", LOCALTIME},
	{"localtimestamp", LOCALTIMESTAMP},
	{"lock", LOCK},
	{"logs", LOGS},
	{"long", UNUSED},
	{"longblob", LONGBLOB},
	{"longtext", LONGTEXT},
	{"loop", UNUSED},
	{"low_priority", LOW_PRIORITY},
	{"manifest", MANIFEST},
	{"master_bind", UNUSED},
	{"match", MATCH},
	{"max_rows", MAX_ROWS},
	{"maxvalue", MAXVALUE},
	{"mediumblob", MEDIUMBLOB},
	{"mediumint", MEDIUMINT},
	{"mediumtext", MEDIUMTEXT},
	{"memory", MEMORY},
	{"merge", MERGE},
	{"middleint", UNUSED},
	{"min_rows", MIN_ROWS},
	{"minute_microsecond", UNUSED},
	{"minute_second", UNUSED},
	{"mod", MOD},
	{"mode", MODE},
	{"modify", MODIFY},
	{"modifies", UNUSED},
	{"multilinestring", MULTILINESTRING},
	{"multipoint", MULTIPOINT},
	{"multipolygon", MULTIPOLYGON},
	{"name", NAME},
	{"names", NAMES},
	{"natural", NATURAL},
	{"nchar", NCHAR},
	{"next", NEXT},
	{"no", NO},
	{"none", NONE},
	{"not", NOT},
	{"no_write_to_binlog", NO_WRITE_TO_BINLOG},
	{"null", NULL},
	{"numeric", NUMERIC},
	{"off", OFF},
	{"offset", OFFSET},
	{"on", ON},
	{"only", ONLY},
	{"open", OPEN},
	{"optimize", OPTIMIZE},
	{"optimizer_costs", OPTIMIZER_COSTS},
	{"option", OPTION},
	{"optionally", OPTIONALLY},
	{"or", OR},
	{"order", ORDER},
	{"out", UNUSED},
	{"outer", OUTER},
	{"outfile", OUTFILE},
	{"overwrite", OVERWRITE},
	{"pack_keys", PACK_KEYS},
	{"parser", PARSER},
	{"partition", PARTITION},
	{"partitioning", PARTITIONING},
	{"password", PASSWORD},
	{"plugins", PLUGINS},
	{"point", POINT},
	{"polygon", POLYGON},
	{"precision", UNUSED},
	{"primary", PRIMARY},
	{"privileges", PRIVILEGES},
	{"processlist", PROCESSLIST},
	{"procedure", PROCEDURE},
	{"query", QUERY},
	{"range", UNUSED},
	{"read", READ},
	{"reads", UNUSED},
	{"read_write", UNUSED},
	{"real", REAL},
	{"rebuild", REBUILD},
	{"redundant", REDUNDANT},
	{"references", REFERENCES},
	{"regexp", REGEXP},
	{"relay", RELAY},
	{"release", RELEASE},
	{"remove", REMOVE},
	{"rename", RENAME},
	{"reorganize", REORGANIZE},
	{"repair", REPAIR},
	{"repeat", UNUSED},
	{"repeatable", REPEATABLE},
	{"replace", REPLACE},
	{"require", UNUSED},
	{"resignal", UNUSED},
	{"restrict", RESTRICT},
	{"return", UNUSED},
	{"retry", RETRY},
	{"revert", REVERT},
	{"revoke", UNUSED},
	{"right", RIGHT},
	{"rlike", REGEXP},
	{"rollback", ROLLBACK},
	{"row_format", ROW_FORMAT},
	{"s3", S3},
	{"savepoint", SAVEPOINT},
	{"schema", SCHEMA},
	{"schemas", SCHEMAS},
	{"second_microsecond", UNUSED},
	{"security", SECURITY},
	{"select", SELECT},
	{"sensitive", UNUSED},
	{"separator", SEPARATOR},
	{"sequence", SEQUENCE},
	{"serializable", SERIALIZABLE},
	{"session", SESSION},
	{"set", SET},
	{"share", SHARE},
	{"shared", SHARED},
	{"show", SHOW},
	{"signal", UNUSED},
	{"signed", SIGNED},
	{"slow", SLOW},
	{"smallint", SMALLINT},
	{"spatial", SPATIAL},
	{"specific", UNUSED},
	{"sql", SQL},
	{"sqlexception", UNUSED},
	{"sqlstate", UNUSED},
	{"sqlwarning", UNUSED},
	{"sql_big_result", UNUSED},
	{"sql_cache", SQL_CACHE},
	{"sql_calc_found_rows", SQL_CALC_FOUND_ROWS},
	{"sql_no_cache", SQL_NO_CACHE},
	{"sql_small_result", UNUSED},
	{"ssl", UNUSED},
	{"start", START},
	{"starting", STARTING},
	{"stats_auto_recalc", STATS_AUTO_RECALC},
	{"stats_persistent", STATS_PERSISTENT},
	{"stats_sample_pages", STATS_SAMPLE_PAGES},
	{"status", STATUS},
	{"storage", STORAGE},
	{"stored", UNUSED},
	{"straight_join", STRAIGHT_JOIN},
	{"stream", STREAM},
	{"vstream", VSTREAM},
	{"table", TABLE},
	{"tables", TABLES},
	{"tablespace", TABLESPACE},
	{"temporary", TEMPORARY},
	{"temptable", TEMPTABLE},
	{"terminated", TERMINATED},
	{"text", TEXT},
	{"than", THAN},
	{"then", THEN},
	{"time", TIME},
	{"timestamp", TIMESTAMP},
	{"timestampadd", TIMESTAMPADD},
	{"timestampdiff", TIMESTAMPDIFF},
	{"tinyblob", TINYBLOB},
	{"tinyint", TINYINT},
	{"tinytext", TINYTEXT},
	{"to", TO},
	{"trailing", UNUSED},
	{"transaction", TRANSACTION},
	{"tree", TREE},
	{"traditional", TRADITIONAL},
	{"trigger", TRIGGER},
	{"triggers", TRIGGERS},
	{"true", TRUE},
	{"truncate", TRUNCATE},
	{"uncommitted", UNCOMMITTED},
	{"undefined", UNDEFINED},
	{"undo", UNUSED},
	{"union", UNION},
	{"unique", UNIQUE},
	{"unlock", UNLOCK},
	{"unsigned", UNSIGNED},
	{"update", UPDATE},
	{"upgrade", UPGRADE},
	{"usage", UNUSED},
	{"use", USE},
	{"user", USER},
	{"user_resources", USER_RESOURCES},
	{"using", USING},
	{"utc_date", UTC_DATE},
	{"utc_time", UTC_TIME},
	{"utc_timestamp", UTC_TIMESTAMP},
	{"validation", VALIDATION},
	{"values", VALUES},
	{"variables", VARIABLES},
	{"varbinary", VARBINARY},
	{"varchar", VARCHAR},
	{"varcharacter", UNUSED},
	{"varying", UNUSED},
	{"virtual", UNUSED},
	{"vindex", VINDEX},
	{"vindexes", VINDEXES},
	{"view", VIEW},
	{"vitess", VITESS},
	{"vitess_keyspaces", VITESS_KEYSPACES},
	{"vitess_metadata", VITESS_METADATA},
	{"vitess_shards", VITESS_SHARDS},
	{"vitess_tablets", VITESS_TABLETS},
	{"vitess_migration", VITESS_MIGRATION},
	{"vitess_migrations", VITESS_MIGRATIONS},
	{"vschema", VSCHEMA},
	{"warnings", WARNINGS},
	{"when", WHEN},
	{"where", WHERE},
	{"while", UNUSED},
	{"with", WITH},
	{"without", WITHOUT},
	{"work", WORK},
	{"write", WRITE},
	{"xor", XOR},
	{"year", YEAR},
	{"year_month", UNUSED},
	{"zerofill", ZEROFILL},
}

// keywordStrings contains the reverse mapping of token to keyword strings
var keywordStrings = map[int]string{}

// keywordLookupTable is a perfect hash map that maps **case insensitive** keyword names to their ids
var keywordLookupTable *perfectTable

func init() {
	for _, kw := range keywords {
		if kw.id == UNUSED {
			continue
		}
		if kw.name != strings.ToLower(kw.name) {
			panic(fmt.Sprintf("keyword %q must be lowercase in table", kw.name))
		}
		keywordStrings[kw.id] = kw.name
	}

	keywordLookupTable = buildKeywordTable(keywords)
}

// KeywordString returns the string corresponding to the given keyword
func KeywordString(id int) string {
	str, ok := keywordStrings[id]
	if !ok {
		return ""
	}
	return str
}

type perfectTable struct {
	keys       []keyword
	level0     []uint32 // power of 2 size
	level0Mask int      // len(Level0) - 1
	level1     []uint32 // power of 2 size >= len(keys)
	level1Mask int      // len(Level1) - 1
}

const offset64 = uint64(14695981039346656037)
const prime64 = uint64(1099511628211)

func fnv1aI(h uint64, s []byte) uint64 {
	for _, c := range s {
		if 'A' <= c && c <= 'Z' {
			c += 'a' - 'A'
		}
		h = (h ^ uint64(c)) * prime64
	}
	return h
}

func fnv1aIstr(h uint64, s string) uint64 {
	for i := 0; i < len(s); i++ {
		c := s[i]
		if 'A' <= c && c <= 'Z' {
			c += 'a' - 'A'
		}
		h = (h ^ uint64(c)) * prime64
	}
	return h
}

// buildKeywordTable generates a perfect hash map for all the keywords using the "Hash, displace, and compress"
// algorithm described in http://cmph.sourceforge.net/papers/esa09.pdf.
func buildKeywordTable(keywords []keyword) *perfectTable {
	type indexBucket struct {
		n    int
		vals []int
	}

	nextPow2 := func(n int) int {
		for i := 1; ; i *= 2 {
			if i >= n {
				return i
			}
		}
	}

	var (
		level0        = make([]uint32, nextPow2(len(keywords)/4))
		level0Mask    = len(level0) - 1
		level1        = make([]uint32, nextPow2(len(keywords)))
		level1Mask    = len(level1) - 1
		sparseBuckets = make([][]int, len(level0))
		zeroSeed      = offset64
	)
	for i, kw := range keywords {
		n := int(fnv1aIstr(zeroSeed, kw.name)) & level0Mask
		sparseBuckets[n] = append(sparseBuckets[n], i)
	}
	var buckets []indexBucket
	for n, vals := range sparseBuckets {
		if len(vals) > 0 {
			buckets = append(buckets, indexBucket{n, vals})
		}
	}
	sort.Slice(buckets, func(i, j int) bool {
		return len(buckets[i].vals) > len(buckets[j].vals)
	})

	occ := make([]bool, len(level1))
	var tmpOcc []int
	for _, bucket := range buckets {
		var seed uint64
	trySeed:
		tmpOcc = tmpOcc[:0]
		for _, i := range bucket.vals {
			n := int(fnv1aIstr(seed, keywords[i].name)) & level1Mask
			if occ[n] {
				for _, n := range tmpOcc {
					occ[n] = false
				}
				seed++
				goto trySeed
			}
			occ[n] = true
			tmpOcc = append(tmpOcc, n)
			level1[n] = uint32(i)
		}
		level0[bucket.n] = uint32(seed)
	}

	return &perfectTable{
		keys:       keywords,
		level0:     level0,
		level0Mask: level0Mask,
		level1:     level1,
		level1Mask: level1Mask,
	}
}

// Lookup looks up the given keyword on the perfect map for keywords.
// The provided bytes are not modified and are compared **case insensitively**
func (t *perfectTable) Lookup(keyword []byte) (int, bool) {
	i0 := int(fnv1aI(offset64, keyword)) & t.level0Mask
	seed := t.level0[i0]
	i1 := int(fnv1aI(uint64(seed), keyword)) & t.level1Mask
	cell := &t.keys[int(t.level1[i1])]
	if cell.match(keyword) {
		return cell.id, true
	}
	return 0, false
}

// LookupString looks up the given keyword on the perfect map for keywords.
// The provided string is compared **case insensitively**
func (t *perfectTable) LookupString(keyword string) (int, bool) {
	i0 := int(fnv1aIstr(offset64, keyword)) & t.level0Mask
	seed := t.level0[i0]
	i1 := int(fnv1aIstr(uint64(seed), keyword)) & t.level1Mask
	cell := &t.keys[int(t.level1[i1])]
	if cell.matchStr(keyword) {
		return cell.id, true
	}
	return 0, false
}
