//line ./go/vt/sqlparser/sql.y:6
package sqlparser

import __yyfmt__ "fmt"

//line ./go/vt/sqlparser/sql.y:6
func setParseTree(yylex interface{}, stmt Statement) {
	yylex.(*Tokenizer).ParseTree = stmt
}

func setAllowComments(yylex interface{}, allow bool) {
	yylex.(*Tokenizer).AllowComments = allow
}

func incNesting(yylex interface{}) bool {
	yylex.(*Tokenizer).nesting++
	if yylex.(*Tokenizer).nesting == 200 {
		return true
	}
	return false
}

func decNesting(yylex interface{}) {
	yylex.(*Tokenizer).nesting--
}

func forceEOF(yylex interface{}) {
	yylex.(*Tokenizer).ForceEOF = true
}

//line ./go/vt/sqlparser/sql.y:34
type yySymType struct {
	yys              int
	empty            struct{}
	statement        Statement
	selStmt          SelectStatement
	byt              byte
	bytes            []byte
	bytes2           [][]byte
	str              string
	selectExprs      SelectExprs
	selectExpr       SelectExpr
	columns          Columns
	colName          *ColName
	tableExprs       TableExprs
	tableExpr        TableExpr
	tableName        *TableName
	indexHints       *IndexHints
	expr             Expr
	exprs            Exprs
	boolVal          BoolVal
	colTuple         ColTuple
	values           Values
	valTuple         ValTuple
	subquery         *Subquery
	caseExpr         *CaseExpr
	whens            []*When
	when             *When
	orderBy          OrderBy
	order            *Order
	limit            *Limit
	insRows          InsertRows
	updateExprs      UpdateExprs
	updateExpr       *UpdateExpr
	colIdent         ColIdent
	colIdents        []ColIdent
	tableIdent       TableIdent
	convertType      *ConvertType
	aliasedTableName *AliasedTableExpr
}

const LEX_ERROR = 57346
const UNION = 57347
const SELECT = 57348
const INSERT = 57349
const UPDATE = 57350
const DELETE = 57351
const FROM = 57352
const WHERE = 57353
const GROUP = 57354
const HAVING = 57355
const ORDER = 57356
const BY = 57357
const LIMIT = 57358
const OFFSET = 57359
const FOR = 57360
const ALL = 57361
const DISTINCT = 57362
const AS = 57363
const EXISTS = 57364
const ASC = 57365
const DESC = 57366
const INTO = 57367
const DUPLICATE = 57368
const KEY = 57369
const DEFAULT = 57370
const SET = 57371
const LOCK = 57372
const VALUES = 57373
const LAST_INSERT_ID = 57374
const NEXT = 57375
const VALUE = 57376
const SQL_NO_CACHE = 57377
const SQL_CACHE = 57378
const JOIN = 57379
const STRAIGHT_JOIN = 57380
const LEFT = 57381
const RIGHT = 57382
const INNER = 57383
const OUTER = 57384
const CROSS = 57385
const NATURAL = 57386
const USE = 57387
const FORCE = 57388
const ON = 57389
const ID = 57390
const HEX = 57391
const STRING = 57392
const INTEGRAL = 57393
const FLOAT = 57394
const HEXNUM = 57395
const VALUE_ARG = 57396
const LIST_ARG = 57397
const COMMENT = 57398
const NULL = 57399
const TRUE = 57400
const FALSE = 57401
const OR = 57402
const AND = 57403
const NOT = 57404
const BETWEEN = 57405
const CASE = 57406
const WHEN = 57407
const THEN = 57408
const ELSE = 57409
const END = 57410
const LE = 57411
const GE = 57412
const NE = 57413
const NULL_SAFE_EQUAL = 57414
const IS = 57415
const LIKE = 57416
const REGEXP = 57417
const IN = 57418
const SHIFT_LEFT = 57419
const SHIFT_RIGHT = 57420
const DIV = 57421
const MOD = 57422
const UNARY = 57423
const COLLATE = 57424
const BINARY = 57425
const INTERVAL = 57426
const JSON_EXTRACT_OP = 57427
const JSON_UNQUOTE_EXTRACT_OP = 57428
const CREATE = 57429
const ALTER = 57430
const DROP = 57431
const RENAME = 57432
const ANALYZE = 57433
const TABLE = 57434
const INDEX = 57435
const VIEW = 57436
const TO = 57437
const IGNORE = 57438
const IF = 57439
const UNIQUE = 57440
const USING = 57441
const SHOW = 57442
const DESCRIBE = 57443
const EXPLAIN = 57444
const INTEGER = 57445
const CHARACTER = 57446
const CURRENT_TIMESTAMP = 57447
const DATABASE = 57448
const CURRENT_DATE = 57449
const UNIX_TIMESTAMP = 57450
const CURRENT_TIME = 57451
const LOCALTIME = 57452
const LOCALTIMESTAMP = 57453
const UTC_DATE = 57454
const UTC_TIME = 57455
const UTC_TIMESTAMP = 57456
const REPLACE = 57457
const CONVERT = 57458
const CAST = 57459
const GROUP_CONCAT = 57460
const SEPARATOR = 57461
const MATCH = 57462
const AGAINST = 57463
const BOOLEAN = 57464
const MODE = 57465
const LANGUAGE = 57466
const WITH = 57467
const QUERY = 57468
const EXPANSION = 57469
const SHARE = 57470
const UNUSED = 57471

var yyToknames = [...]string{
	"$end",
	"error",
	"$unk",
	"LEX_ERROR",
	"UNION",
	"SELECT",
	"INSERT",
	"UPDATE",
	"DELETE",
	"FROM",
	"WHERE",
	"GROUP",
	"HAVING",
	"ORDER",
	"BY",
	"LIMIT",
	"OFFSET",
	"FOR",
	"ALL",
	"DISTINCT",
	"AS",
	"EXISTS",
	"ASC",
	"DESC",
	"INTO",
	"DUPLICATE",
	"KEY",
	"DEFAULT",
	"SET",
	"LOCK",
	"VALUES",
	"LAST_INSERT_ID",
	"NEXT",
	"VALUE",
	"SQL_NO_CACHE",
	"SQL_CACHE",
	"JOIN",
	"STRAIGHT_JOIN",
	"LEFT",
	"RIGHT",
	"INNER",
	"OUTER",
	"CROSS",
	"NATURAL",
	"USE",
	"FORCE",
	"ON",
	"'('",
	"','",
	"')'",
	"ID",
	"HEX",
	"STRING",
	"INTEGRAL",
	"FLOAT",
	"HEXNUM",
	"VALUE_ARG",
	"LIST_ARG",
	"COMMENT",
	"NULL",
	"TRUE",
	"FALSE",
	"OR",
	"AND",
	"NOT",
	"'!'",
	"BETWEEN",
	"CASE",
	"WHEN",
	"THEN",
	"ELSE",
	"END",
	"'='",
	"'<'",
	"'>'",
	"LE",
	"GE",
	"NE",
	"NULL_SAFE_EQUAL",
	"IS",
	"LIKE",
	"REGEXP",
	"IN",
	"'|'",
	"'&'",
	"SHIFT_LEFT",
	"SHIFT_RIGHT",
	"'+'",
	"'-'",
	"'*'",
	"'/'",
	"DIV",
	"'%'",
	"MOD",
	"'^'",
	"'~'",
	"UNARY",
	"COLLATE",
	"BINARY",
	"INTERVAL",
	"'.'",
	"JSON_EXTRACT_OP",
	"JSON_UNQUOTE_EXTRACT_OP",
	"CREATE",
	"ALTER",
	"DROP",
	"RENAME",
	"ANALYZE",
	"TABLE",
	"INDEX",
	"VIEW",
	"TO",
	"IGNORE",
	"IF",
	"UNIQUE",
	"USING",
	"SHOW",
	"DESCRIBE",
	"EXPLAIN",
	"INTEGER",
	"CHARACTER",
	"CURRENT_TIMESTAMP",
	"DATABASE",
	"CURRENT_DATE",
	"UNIX_TIMESTAMP",
	"CURRENT_TIME",
	"LOCALTIME",
	"LOCALTIMESTAMP",
	"UTC_DATE",
	"UTC_TIME",
	"UTC_TIMESTAMP",
	"REPLACE",
	"CONVERT",
	"CAST",
	"GROUP_CONCAT",
	"SEPARATOR",
	"MATCH",
	"AGAINST",
	"BOOLEAN",
	"MODE",
	"LANGUAGE",
	"WITH",
	"QUERY",
	"EXPANSION",
	"SHARE",
	"UNUSED",
	"';'",
}
var yyStatenames = [...]string{}

const yyEofCode = 1
const yyErrCode = 2
const yyInitialStackSize = 16

//line yacctab:1
var yyExca = [...]int{
	-1, 1,
	1, -1,
	-2, 0,
	-1, 70,
	101, 295,
	-2, 294,
	-1, 153,
	48, 206,
	-2, 178,
}

const yyNprod = 299
const yyPrivate = 57344

var yyTokenNames []string
var yyStates []string

const yyLast = 1408

var yyAct = [...]int{

	299, 205, 307, 137, 453, 141, 65, 222, 273, 392,
	422, 452, 319, 371, 373, 287, 372, 202, 386, 131,
	203, 364, 84, 140, 138, 28, 49, 61, 536, 542,
	535, 541, 528, 540, 534, 539, 538, 293, 533, 456,
	527, 487, 67, 37, 168, 39, 343, 190, 344, 40,
	80, 72, 50, 51, 126, 128, 42, 143, 43, 517,
	236, 235, 245, 246, 238, 239, 240, 241, 242, 243,
	244, 237, 150, 43, 247, 282, 196, 14, 15, 16,
	17, 272, 3, 45, 46, 47, 247, 90, 221, 108,
	194, 220, 98, 407, 345, 219, 73, 67, 124, 76,
	18, 105, 134, 48, 107, 67, 44, 226, 225, 68,
	100, 52, 74, 198, 437, 77, 523, 123, 518, 257,
	258, 434, 127, 302, 227, 291, 67, 215, 277, 187,
	101, 97, 66, 237, 224, 526, 247, 530, 228, 201,
	122, 75, 70, 343, 218, 344, 227, 458, 259, 260,
	261, 262, 263, 264, 365, 112, 226, 225, 116, 103,
	408, 343, 169, 344, 68, 93, 225, 193, 195, 192,
	87, 274, 68, 227, 185, 19, 20, 22, 21, 23,
	478, 379, 227, 365, 326, 415, 276, 94, 24, 25,
	26, 345, 524, 68, 217, 507, 506, 255, 324, 325,
	323, 214, 240, 241, 242, 243, 244, 237, 197, 345,
	247, 70, 489, 492, 226, 225, 75, 63, 280, 96,
	63, 460, 394, 395, 396, 104, 303, 310, 311, 446,
	447, 227, 133, 318, 81, 292, 327, 328, 329, 330,
	331, 332, 333, 334, 335, 336, 337, 338, 339, 340,
	341, 121, 342, 298, 317, 122, 75, 99, 296, 117,
	129, 122, 118, 279, 63, 322, 349, 30, 353, 226,
	225, 356, 357, 360, 362, 320, 443, 75, 289, 290,
	301, 346, 347, 350, 361, 354, 227, 285, 321, 352,
	238, 239, 240, 241, 242, 243, 244, 237, 444, 274,
	247, 89, 256, 369, 378, 370, 385, 63, 442, 96,
	390, 393, 301, 265, 266, 267, 514, 301, 268, 269,
	270, 271, 226, 225, 388, 389, 284, 63, 398, 399,
	400, 420, 301, 313, 315, 316, 397, 348, 314, 227,
	294, 355, 99, 376, 359, 284, 301, 439, 301, 420,
	366, 402, 411, 301, 382, 405, 406, 409, 132, 410,
	122, 289, 79, 412, 185, 288, 223, 416, 320, 403,
	404, 361, 301, 383, 500, 384, 297, 387, 387, 387,
	14, 321, 304, 305, 306, 414, 294, 438, 278, 440,
	441, 433, 300, 301, 417, 75, 99, 290, 111, 226,
	225, 284, 114, 503, 450, 449, 472, 226, 225, 120,
	82, 473, 451, 470, 502, 469, 227, 459, 471, 468,
	454, 455, 122, 41, 227, 63, 375, 436, 122, 474,
	211, 428, 429, 210, 477, 376, 465, 464, 467, 466,
	531, 475, 56, 57, 482, 483, 393, 86, 377, 14,
	508, 520, 485, 488, 484, 493, 494, 59, 491, 91,
	85, 60, 496, 521, 498, 274, 69, 435, 381, 497,
	188, 110, 499, 432, 213, 53, 54, 86, 376, 376,
	376, 376, 308, 463, 457, 67, 504, 505, 162, 161,
	163, 164, 165, 166, 309, 223, 167, 209, 462, 509,
	510, 62, 511, 512, 31, 208, 419, 366, 513, 132,
	480, 78, 481, 515, 64, 83, 529, 525, 522, 479,
	33, 34, 35, 36, 14, 532, 62, 30, 95, 32,
	27, 62, 1, 431, 92, 286, 537, 189, 102, 38,
	377, 281, 191, 106, 71, 297, 109, 424, 427, 428,
	429, 425, 68, 426, 430, 207, 62, 501, 119, 216,
	358, 95, 519, 445, 130, 391, 153, 152, 186, 461,
	424, 427, 428, 429, 425, 199, 426, 430, 200, 418,
	206, 413, 275, 377, 377, 377, 377, 363, 154, 142,
	351, 295, 139, 312, 115, 212, 229, 135, 125, 88,
	423, 155, 421, 374, 283, 486, 75, 516, 55, 175,
	113, 29, 58, 13, 12, 11, 490, 10, 122, 495,
	301, 70, 162, 161, 163, 164, 165, 166, 9, 8,
	167, 159, 160, 7, 6, 136, 148, 5, 184, 236,
	235, 245, 246, 238, 239, 240, 241, 242, 243, 244,
	237, 4, 2, 247, 0, 0, 0, 0, 145, 146,
	204, 0, 0, 0, 173, 0, 147, 0, 0, 144,
	149, 0, 0, 0, 62, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 170, 0, 0, 0, 448, 0,
	0, 0, 176, 171, 177, 172, 178, 182, 183, 181,
	180, 179, 174, 156, 157, 151, 0, 158, 236, 235,
	245, 246, 238, 239, 240, 241, 242, 243, 244, 237,
	0, 0, 247, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 206, 0, 206, 245, 246, 238, 239, 240,
	241, 242, 243, 244, 237, 367, 0, 247, 368, 0,
	0, 206, 62, 0, 139, 0, 0, 0, 380, 0,
	0, 0, 0, 155, 0, 0, 0, 0, 0, 0,
	0, 175, 0, 0, 0, 0, 0, 0, 0, 0,
	122, 401, 301, 70, 162, 161, 163, 164, 165, 166,
	0, 0, 167, 159, 160, 0, 0, 136, 148, 0,
	184, 236, 235, 245, 246, 238, 239, 240, 241, 242,
	243, 244, 237, 0, 0, 247, 0, 0, 206, 206,
	145, 146, 204, 0, 0, 0, 173, 0, 147, 0,
	0, 144, 149, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 62, 0, 170, 0, 0, 0,
	0, 0, 0, 0, 176, 171, 177, 172, 178, 182,
	183, 181, 180, 179, 174, 156, 157, 151, 0, 158,
	0, 0, 0, 0, 139, 0, 0, 0, 0, 0,
	0, 0, 0, 155, 0, 0, 0, 62, 62, 62,
	62, 175, 0, 0, 0, 0, 0, 0, 476, 0,
	122, 0, 0, 70, 162, 161, 163, 164, 165, 166,
	0, 0, 167, 159, 160, 0, 0, 136, 148, 0,
	184, 236, 235, 245, 246, 238, 239, 240, 241, 242,
	243, 244, 237, 0, 0, 247, 0, 0, 0, 0,
	145, 146, 204, 0, 0, 0, 173, 0, 147, 0,
	0, 144, 149, 0, 235, 245, 246, 238, 239, 240,
	241, 242, 243, 244, 237, 0, 170, 247, 0, 0,
	0, 0, 14, 0, 176, 171, 177, 172, 178, 182,
	183, 181, 180, 179, 174, 156, 157, 151, 139, 158,
	0, 0, 0, 0, 0, 0, 0, 155, 0, 0,
	0, 0, 0, 0, 0, 175, 0, 0, 0, 0,
	0, 0, 0, 0, 122, 0, 0, 70, 162, 161,
	163, 164, 165, 166, 0, 0, 167, 159, 160, 0,
	0, 136, 148, 0, 184, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 145, 146, 0, 0, 0, 0,
	173, 0, 147, 0, 0, 144, 149, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	170, 0, 0, 0, 0, 0, 0, 0, 176, 171,
	177, 172, 178, 182, 183, 181, 180, 179, 174, 156,
	157, 151, 139, 158, 0, 0, 0, 0, 0, 0,
	0, 155, 0, 0, 0, 0, 0, 0, 0, 175,
	0, 0, 0, 0, 0, 0, 0, 0, 122, 0,
	0, 70, 162, 161, 163, 164, 165, 166, 0, 0,
	167, 159, 160, 0, 0, 136, 148, 0, 184, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 145, 146,
	0, 0, 0, 0, 173, 0, 147, 0, 0, 144,
	149, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 170, 0, 0, 0, 0, 0,
	0, 0, 176, 171, 177, 172, 178, 182, 183, 181,
	180, 179, 174, 156, 157, 151, 155, 158, 0, 0,
	0, 0, 0, 0, 175, 0, 0, 0, 0, 0,
	0, 0, 0, 122, 0, 0, 70, 162, 161, 163,
	164, 165, 166, 0, 0, 167, 159, 160, 0, 0,
	0, 148, 0, 184, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 155,
	0, 0, 0, 145, 146, 0, 0, 175, 0, 173,
	0, 147, 0, 0, 144, 149, 122, 0, 0, 70,
	162, 161, 163, 164, 165, 166, 0, 0, 167, 170,
	0, 0, 0, 0, 148, 0, 184, 176, 171, 177,
	172, 178, 182, 183, 181, 180, 179, 174, 156, 157,
	151, 0, 158, 0, 0, 0, 145, 146, 0, 0,
	0, 0, 173, 0, 147, 0, 0, 144, 149, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 170, 0, 0, 0, 0, 0, 0, 0,
	176, 171, 177, 172, 178, 182, 183, 181, 180, 179,
	174, 156, 157, 151, 231, 158, 234, 0, 0, 0,
	0, 0, 248, 249, 250, 251, 252, 253, 254, 0,
	232, 233, 230, 236, 235, 245, 246, 238, 239, 240,
	241, 242, 243, 244, 237, 0, 0, 247,
}
var yyPact = [...]int{

	71, -1000, -122, 522, -1000, -1000, -1000, -1000, -1000, -1000,
	-1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, -66,
	-55, -3, -26, -6, -1000, -1000, -1000, -1000, -1000, 518,
	456, 407, -1000, -40, 169, 504, 160, -63, -14, 165,
	-1000, -10, 165, -1000, 169, -64, 183, -64, 169, -1000,
	-1000, -1000, -1000, -1000, -1000, 427, -1000, -1000, 111, 276,
	430, 166, 30, -1000, 169, 208, -1000, 37, -1000, 29,
	-1000, 169, 94, 174, -1000, -1000, 169, -1000, -23, 169,
	449, 351, 165, -1000, 364, 205, -1000, -1000, 380, 169,
	-1000, 160, 9, -1000, 256, -1000, -1000, 169, 498, 160,
	1080, 160, -1000, 448, -69, -1000, 62, -1000, 169, -1000,
	-1000, 169, -1000, 852, -1000, 487, -1000, 402, 399, 443,
	160, 165, -1000, -1000, 347, -1000, -15, -19, -22, -1000,
	-1000, 481, 1080, -1000, 93, -1000, 1080, 1309, -1000, 312,
	-1000, 17, -1000, -1000, 1238, 1238, 1238, 1238, 1238, 1238,
	312, 312, 312, -1000, -1000, 312, 312, 312, 312, -1000,
	-1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, 966,
	-1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000,
	-1000, -1000, -1000, -1000, 1080, -1000, 27, -1000, -1000, 341,
	165, -1000, -37, -1000, -1000, -1000, -1000, -1000, -1000, -1000,
	-1000, -1000, 277, -1000, -1000, 344, 24, 169, -1000, -1000,
	-1000, -1000, 339, 312, 522, 293, 343, 22, 481, 312,
	312, 312, 466, 479, 93, 1080, 1080, 273, 66, 1185,
	207, 117, 1238, 1238, 1238, 1238, 1238, 1238, 1238, 1238,
	1238, 1238, 1238, 1238, 1238, 1238, 1238, 110, -1000, -1000,
	-1000, -1000, -1000, -1000, -1000, -1000, 518, 436, 436, -12,
	-12, -12, -12, -12, 555, 570, 457, 732, 165, 1080,
	1080, 165, 262, 322, 93, 85, 93, 165, 169, -1000,
	-1000, 169, -1000, 498, 852, 213, -1000, -1000, 226, -1000,
	-1000, 91, -1000, -1000, 442, 305, -1000, 1080, -1000, -1000,
	165, -1000, 165, 466, 165, 165, 165, -1000, 1080, 1080,
	66, 102, -1000, -1000, 162, -1000, -1000, -1000, 837, -1000,
	-1000, -1000, -1000, 207, 1238, 1238, 1238, 837, 837, 717,
	649, 869, -12, 112, 112, 38, 38, 38, 38, 38,
	202, 202, -1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000,
	296, 852, 852, -1000, 296, 230, 44, 336, 303, -1000,
	-1000, 1080, -1000, 114, -1000, 1080, -1000, -1000, -1000, 494,
	-1000, 300, 533, -1000, -1000, -1000, 452, 374, -1000, -1000,
	20, 440, 312, 13, -1000, -1000, 298, -1000, 298, 298,
	259, 249, -1000, 206, -1000, -1000, -1000, -1000, 837, 837,
	624, 1238, -1000, 296, 352, -1000, -1000, 110, 110, 110,
	-99, 165, 93, 75, -1000, 1080, 151, -1000, 485, 468,
	213, 213, 213, 213, -1000, 382, 378, -1000, 376, 369,
	392, 169, -1000, 282, 90, 511, -1000, 165, -1000, 165,
	-1000, -1000, 1080, 1080, 1080, -1000, -1000, -1000, 1238, 837,
	-1000, -95, 230, 92, 230, 230, 312, -1000, -1000, 93,
	1080, 481, 1080, 1080, 533, 327, 510, -1000, -1000, -1000,
	-1000, 377, -1000, 366, -1000, -1000, -1000, -1000, -1000, 160,
	-1000, -1000, 93, 93, -1000, 837, 230, 143, -1000, -1000,
	141, -1000, 421, -1000, -1000, 1238, 93, 466, 93, 235,
	1080, 1080, -1000, -1000, 208, -1000, -1000, 267, 110, -24,
	433, 93, 93, -5, 138, -1000, 230, -4, -111, -1000,
	508, 54, -1000, 411, 230, -1000, -102, -107, -114, -1000,
	-117, 110, -1000, -1000, -104, -1000, -105, -1000, -109, -1000,
	-112, -115, -1000,
}
var yyPgo = [...]int{

	0, 652, 81, 651, 637, 634, 633, 629, 628, 617,
	615, 614, 613, 504, 612, 611, 22, 610, 608, 607,
	605, 17, 20, 1, 604, 13, 16, 14, 603, 602,
	10, 600, 27, 599, 426, 598, 18, 19, 597, 24,
	596, 595, 23, 3, 594, 593, 12, 8, 591, 44,
	589, 57, 5, 588, 587, 21, 582, 581, 579, 569,
	567, 566, 7, 565, 9, 563, 2, 562, 560, 559,
	558, 37, 6, 132, 555, 423, 362, 544, 542, 541,
	539, 537, 72, 15, 535, 466, 165, 534, 533, 26,
	4, 11, 532, 530, 529, 162, 0,
}
var yyR1 = [...]int{

	0, 92, 93, 93, 1, 1, 1, 1, 1, 1,
	1, 1, 1, 1, 1, 2, 2, 2, 3, 3,
	4, 5, 6, 7, 7, 7, 8, 8, 8, 9,
	10, 10, 10, 11, 12, 12, 12, 94, 13, 14,
	14, 15, 15, 15, 18, 18, 18, 16, 16, 17,
	17, 21, 21, 22, 22, 22, 22, 84, 84, 84,
	83, 83, 24, 24, 25, 25, 26, 26, 27, 27,
	27, 34, 28, 28, 28, 28, 88, 88, 87, 87,
	87, 86, 86, 29, 29, 29, 29, 30, 30, 30,
	30, 31, 31, 33, 33, 32, 32, 35, 35, 35,
	35, 36, 36, 37, 37, 23, 23, 23, 23, 23,
	23, 39, 39, 38, 38, 38, 38, 38, 38, 38,
	38, 38, 38, 38, 38, 45, 45, 45, 45, 45,
	45, 40, 40, 40, 40, 40, 40, 40, 46, 46,
	46, 51, 47, 47, 90, 90, 90, 43, 43, 43,
	43, 43, 43, 43, 43, 43, 43, 43, 43, 43,
	43, 43, 43, 43, 43, 43, 43, 43, 43, 43,
	43, 43, 43, 43, 43, 43, 43, 43, 43, 43,
	43, 43, 43, 43, 43, 19, 19, 19, 19, 19,
	91, 91, 91, 91, 91, 91, 91, 91, 61, 61,
	61, 61, 61, 61, 61, 61, 60, 60, 60, 60,
	60, 60, 60, 53, 56, 56, 20, 20, 54, 54,
	55, 57, 57, 52, 52, 52, 42, 42, 42, 42,
	42, 42, 42, 44, 44, 44, 58, 58, 59, 59,
	62, 62, 63, 63, 64, 65, 65, 65, 66, 66,
	66, 66, 67, 67, 67, 68, 68, 70, 70, 69,
	69, 69, 69, 71, 71, 41, 41, 48, 48, 49,
	50, 72, 72, 73, 74, 74, 76, 76, 77, 77,
	75, 75, 78, 78, 78, 78, 78, 78, 79, 79,
	80, 80, 81, 81, 82, 85, 95, 96, 89,
}
var yyR2 = [...]int{

	0, 2, 0, 1, 1, 1, 1, 1, 1, 1,
	1, 1, 1, 1, 1, 13, 7, 3, 7, 7,
	8, 7, 3, 5, 8, 4, 6, 7, 4, 5,
	4, 5, 5, 3, 2, 2, 2, 0, 2, 0,
	2, 1, 2, 2, 0, 1, 1, 0, 1, 0,
	1, 1, 3, 1, 2, 3, 5, 0, 1, 2,
	1, 1, 0, 2, 1, 3, 1, 1, 1, 3,
	3, 3, 3, 5, 5, 3, 0, 1, 0, 1,
	2, 1, 1, 1, 2, 2, 1, 2, 3, 2,
	3, 2, 2, 2, 1, 1, 3, 0, 5, 5,
	5, 1, 3, 0, 2, 1, 3, 3, 2, 3,
	1, 1, 1, 1, 3, 3, 3, 4, 3, 4,
	3, 4, 5, 6, 2, 1, 2, 1, 2, 1,
	2, 1, 1, 1, 1, 1, 1, 1, 1, 1,
	1, 3, 1, 3, 1, 1, 1, 1, 1, 1,
	1, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 2, 2, 2, 2,
	2, 3, 3, 4, 5, 7, 3, 4, 1, 1,
	4, 6, 6, 6, 9, 0, 3, 4, 7, 3,
	1, 2, 4, 5, 7, 2, 4, 6, 1, 1,
	1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
	1, 1, 1, 5, 0, 1, 0, 2, 1, 2,
	4, 0, 2, 1, 3, 5, 1, 1, 1, 1,
	1, 1, 1, 1, 2, 2, 0, 3, 0, 2,
	0, 3, 1, 3, 2, 0, 1, 1, 0, 2,
	4, 4, 0, 2, 4, 1, 3, 0, 3, 1,
	3, 3, 5, 0, 5, 2, 1, 1, 3, 3,
	1, 1, 3, 3, 1, 1, 0, 2, 0, 3,
	0, 1, 1, 1, 1, 1, 1, 1, 0, 1,
	0, 1, 0, 2, 1, 1, 1, 1, 0,
}
var yyChk = [...]int{

	-1000, -92, -1, -2, -3, -4, -5, -6, -7, -8,
	-9, -10, -11, -12, 6, 7, 8, 9, 29, 104,
	105, 107, 106, 108, 117, 118, 119, -93, 147, -15,
	5, -13, -94, -13, -13, -13, -13, 109, -80, 111,
	115, -75, 111, 113, 109, 109, 110, 111, 109, -89,
	-89, -89, -2, 19, 20, -18, 35, 36, -14, -75,
	-34, -32, -85, 51, 10, -72, -73, -52, -82, -85,
	51, -77, 114, 110, -82, 51, 109, -82, -85, -76,
	114, 51, -76, -85, -16, 33, 20, 59, -33, 25,
	-32, 29, -87, -86, 21, -85, 53, 101, -32, 49,
	73, 101, -85, 65, 51, -89, -85, -89, 112, -85,
	22, 47, -82, -17, 38, -44, -82, 54, 57, -70,
	29, -95, 48, -32, -72, -35, 45, 113, 46, -86,
	-85, -37, 11, -73, -23, -38, 65, -43, -39, 22,
	-42, -52, -50, -51, 99, 88, 89, 96, 66, 100,
	-82, 135, -60, -61, -53, 31, 133, 134, 137, 61,
	62, 53, 52, 54, 55, 56, 57, 60, -49, -95,
	114, 123, 125, 94, 132, 39, 122, 124, 126, 131,
	130, 129, 127, 128, 68, -82, -85, -89, 22, -81,
	116, -78, 107, 105, 28, 106, 14, 146, 51, -85,
	-85, -89, -21, -22, 90, -23, -85, -74, 18, 10,
	31, 31, -41, 31, -2, -72, -69, -82, -37, 110,
	110, 110, -62, 14, -23, 64, 63, 80, -23, -40,
	83, 65, 81, 82, 67, 85, 84, 95, 88, 89,
	90, 91, 92, 93, 94, 86, 87, 98, 73, 74,
	75, 76, 77, 78, 79, -51, -95, 102, 103, -43,
	-43, -43, -43, -43, -43, -95, -95, -95, -95, -95,
	-95, -95, -2, -47, -23, -56, -23, 101, 47, -82,
	-89, -79, 112, -24, 49, 10, -84, -83, 21, -82,
	53, 101, -32, -71, 47, -48, -49, -95, -71, -96,
	49, 50, 101, -62, -95, -95, -95, -66, 16, 15,
	-23, -23, -45, 60, 65, 61, 62, -39, -43, -46,
	-49, -51, 58, 83, 81, 82, 67, -43, -43, -43,
	-43, -43, -43, -43, -43, -43, -43, -43, -43, -43,
	-43, -43, -90, 51, 53, 99, -42, -42, -82, -96,
	-21, 20, -16, -96, -21, -82, -23, -23, -68, -82,
	-96, 49, -96, -54, -55, 69, -82, -85, -85, -37,
	-22, -25, -26, -27, -28, -34, -51, -95, -83, 90,
	-85, 26, 49, -82, -82, -66, -36, -82, -36, -36,
	-23, -63, -64, -23, 60, 61, 62, -46, -43, -43,
	-43, 64, -96, -21, -21, -96, -96, 49, 116, 21,
	-96, 49, -23, -57, -55, 71, -23, -89, -58, 12,
	49, -29, -30, -31, 37, 41, 43, 38, 39, 40,
	44, -88, 21, -25, 101, 27, -49, 101, -96, 49,
	-96, -96, 49, 17, 49, -65, 23, 24, 64, -43,
	-96, -62, -91, -90, -91, -91, 138, -82, 72, -23,
	70, -59, 13, 15, -26, -27, -26, -27, 37, 37,
	37, 42, 37, 42, 37, -30, -85, -96, 90, 8,
	-82, -82, -23, -23, -64, -43, -20, 136, -96, 120,
	-95, -90, 121, -96, -96, -95, -23, -62, -23, -47,
	47, 47, 37, 37, -72, -96, 53, 54, 29, -43,
	-66, -23, -23, -96, 49, -90, -19, 83, 142, -67,
	18, 30, -90, 121, 54, -96, 139, 44, 143, 8,
	83, 29, -96, 140, 141, 144, 145, -90, 140, 140,
	142, 143, 144,
}
var yyDef = [...]int{

	0, -2, 2, 4, 5, 6, 7, 8, 9, 10,
	11, 12, 13, 14, 37, 37, 37, 37, 37, 290,
	280, 0, 0, 0, 298, 298, 298, 1, 3, 0,
	41, 44, 39, 280, 0, 0, 0, 278, 0, 0,
	291, 0, 0, 281, 0, 276, 0, 276, 0, 34,
	35, 36, 17, 42, 43, 47, 45, 46, 38, 0,
	0, 78, 95, 295, 0, 22, 271, 0, 223, 0,
	-2, 0, 0, 0, 298, 294, 0, 298, 0, 0,
	0, 0, 0, 33, 49, 0, 48, 40, 257, 0,
	94, 0, 97, 79, 0, 81, 82, 0, 103, 0,
	0, 0, 298, 0, 292, 25, 0, 28, 0, 30,
	277, 0, 298, 0, 50, 0, 233, 0, 0, 0,
	0, 0, 296, 93, 103, 71, 0, 0, 0, 80,
	96, 240, 0, 272, 273, 105, 0, 110, 113, 0,
	147, 148, 149, 150, 0, 0, 0, 0, 0, 0,
	223, 0, 0, -2, 179, 0, 0, 0, 0, 111,
	112, 226, 227, 228, 229, 230, 231, 232, 270, 0,
	207, 208, 209, 210, 211, 212, 198, 199, 200, 201,
	202, 203, 204, 205, 214, 224, 0, 23, 279, 0,
	0, 298, 288, 282, 283, 284, 285, 286, 287, 29,
	31, 32, 62, 51, 53, 57, 0, 0, 274, 275,
	234, 235, 263, 0, 266, 263, 0, 259, 240, 0,
	0, 0, 248, 0, 104, 0, 0, 0, 108, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 131, 132,
	133, 134, 135, 136, 137, 124, 0, 0, 0, 166,
	167, 168, 169, 170, 0, 0, 47, 0, 0, 0,
	0, 0, 0, 0, 142, 0, 215, 0, 0, 293,
	26, 0, 289, 103, 0, 0, 54, 58, 0, 60,
	61, 0, 16, 18, 0, 265, 267, 0, 19, 258,
	0, 297, 0, 248, 0, 0, 0, 21, 0, 0,
	106, 107, 109, 125, 0, 127, 129, 114, 115, 116,
	138, 139, 140, 0, 0, 0, 0, 118, 120, 0,
	151, 152, 153, 154, 155, 156, 157, 158, 159, 160,
	161, 162, 165, 144, 145, 146, 163, 164, 171, 172,
	0, 0, 0, 176, 0, 0, 0, 0, 0, 255,
	141, 0, 269, 221, 218, 0, 225, 298, 27, 236,
	52, 63, 64, 66, 67, 68, 76, 0, 59, 55,
	0, 0, 0, 261, 260, 20, 0, 101, 0, 0,
	249, 241, 242, 245, 126, 128, 130, 117, 119, 121,
	0, 0, 173, 0, 240, 177, 180, 0, 0, 0,
	0, 0, 143, 0, 219, 0, 0, 24, 238, 0,
	0, 0, 0, 0, 83, 0, 0, 86, 0, 0,
	0, 0, 77, 0, 0, 0, 268, 0, 98, 0,
	99, 100, 0, 0, 0, 244, 246, 247, 0, 122,
	174, 216, 0, 190, 0, 0, 0, 256, 213, 222,
	0, 240, 0, 0, 65, 72, 0, 75, 84, 85,
	87, 0, 89, 0, 91, 92, 69, 70, 56, 0,
	262, 102, 250, 251, 243, 123, 0, 0, 181, 191,
	0, 195, 0, 182, 183, 0, 220, 248, 239, 237,
	0, 0, 88, 90, 264, 175, 217, 0, 0, 185,
	252, 73, 74, 192, 0, 196, 0, 0, 0, 15,
	0, 0, 193, 0, 0, 184, 0, 0, 0, 253,
	0, 0, 197, 186, 0, 189, 0, 194, 187, 254,
	0, 0, 188,
}
var yyTok1 = [...]int{

	1, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 66, 3, 3, 3, 93, 85, 3,
	48, 50, 90, 88, 49, 89, 101, 91, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 147,
	74, 73, 75, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 95, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 84, 3, 96,
}
var yyTok2 = [...]int{

	2, 3, 4, 5, 6, 7, 8, 9, 10, 11,
	12, 13, 14, 15, 16, 17, 18, 19, 20, 21,
	22, 23, 24, 25, 26, 27, 28, 29, 30, 31,
	32, 33, 34, 35, 36, 37, 38, 39, 40, 41,
	42, 43, 44, 45, 46, 47, 51, 52, 53, 54,
	55, 56, 57, 58, 59, 60, 61, 62, 63, 64,
	65, 67, 68, 69, 70, 71, 72, 76, 77, 78,
	79, 80, 81, 82, 83, 86, 87, 92, 94, 97,
	98, 99, 100, 102, 103, 104, 105, 106, 107, 108,
	109, 110, 111, 112, 113, 114, 115, 116, 117, 118,
	119, 120, 121, 122, 123, 124, 125, 126, 127, 128,
	129, 130, 131, 132, 133, 134, 135, 136, 137, 138,
	139, 140, 141, 142, 143, 144, 145, 146,
}
var yyTok3 = [...]int{
	0,
}

var yyErrorMessages = [...]struct {
	state int
	token int
	msg   string
}{}

//line yaccpar:1

/*	parser for yacc output	*/

var (
	yyDebug        = 0
	yyErrorVerbose = false
)

type yyLexer interface {
	Lex(lval *yySymType) int
	Error(s string)
}

type yyParser interface {
	Parse(yyLexer) int
	Lookahead() int
}

type yyParserImpl struct {
	lval  yySymType
	stack [yyInitialStackSize]yySymType
	char  int
}

func (p *yyParserImpl) Lookahead() int {
	return p.char
}

func yyNewParser() yyParser {
	return &yyParserImpl{}
}

const yyFlag = -1000

func yyTokname(c int) string {
	if c >= 1 && c-1 < len(yyToknames) {
		if yyToknames[c-1] != "" {
			return yyToknames[c-1]
		}
	}
	return __yyfmt__.Sprintf("tok-%v", c)
}

func yyStatname(s int) string {
	if s >= 0 && s < len(yyStatenames) {
		if yyStatenames[s] != "" {
			return yyStatenames[s]
		}
	}
	return __yyfmt__.Sprintf("state-%v", s)
}

func yyErrorMessage(state, lookAhead int) string {
	const TOKSTART = 4

	if !yyErrorVerbose {
		return "syntax error"
	}

	for _, e := range yyErrorMessages {
		if e.state == state && e.token == lookAhead {
			return "syntax error: " + e.msg
		}
	}

	res := "syntax error: unexpected " + yyTokname(lookAhead)

	// To match Bison, suggest at most four expected tokens.
	expected := make([]int, 0, 4)

	// Look for shiftable tokens.
	base := yyPact[state]
	for tok := TOKSTART; tok-1 < len(yyToknames); tok++ {
		if n := base + tok; n >= 0 && n < yyLast && yyChk[yyAct[n]] == tok {
			if len(expected) == cap(expected) {
				return res
			}
			expected = append(expected, tok)
		}
	}

	if yyDef[state] == -2 {
		i := 0
		for yyExca[i] != -1 || yyExca[i+1] != state {
			i += 2
		}

		// Look for tokens that we accept or reduce.
		for i += 2; yyExca[i] >= 0; i += 2 {
			tok := yyExca[i]
			if tok < TOKSTART || yyExca[i+1] == 0 {
				continue
			}
			if len(expected) == cap(expected) {
				return res
			}
			expected = append(expected, tok)
		}

		// If the default action is to accept or reduce, give up.
		if yyExca[i+1] != 0 {
			return res
		}
	}

	for i, tok := range expected {
		if i == 0 {
			res += ", expecting "
		} else {
			res += " or "
		}
		res += yyTokname(tok)
	}
	return res
}

func yylex1(lex yyLexer, lval *yySymType) (char, token int) {
	token = 0
	char = lex.Lex(lval)
	if char <= 0 {
		token = yyTok1[0]
		goto out
	}
	if char < len(yyTok1) {
		token = yyTok1[char]
		goto out
	}
	if char >= yyPrivate {
		if char < yyPrivate+len(yyTok2) {
			token = yyTok2[char-yyPrivate]
			goto out
		}
	}
	for i := 0; i < len(yyTok3); i += 2 {
		token = yyTok3[i+0]
		if token == char {
			token = yyTok3[i+1]
			goto out
		}
	}

out:
	if token == 0 {
		token = yyTok2[1] /* unknown char */
	}
	if yyDebug >= 3 {
		__yyfmt__.Printf("lex %s(%d)\n", yyTokname(token), uint(char))
	}
	return char, token
}

func yyParse(yylex yyLexer) int {
	return yyNewParser().Parse(yylex)
}

func (yyrcvr *yyParserImpl) Parse(yylex yyLexer) int {
	var yyn int
	var yyVAL yySymType
	var yyDollar []yySymType
	_ = yyDollar // silence set and not used
	yyS := yyrcvr.stack[:]

	Nerrs := 0   /* number of errors */
	Errflag := 0 /* error recovery flag */
	yystate := 0
	yyrcvr.char = -1
	yytoken := -1 // yyrcvr.char translated into internal numbering
	defer func() {
		// Make sure we report no lookahead when not parsing.
		yystate = -1
		yyrcvr.char = -1
		yytoken = -1
	}()
	yyp := -1
	goto yystack

ret0:
	return 0

ret1:
	return 1

yystack:
	/* put a state and value onto the stack */
	if yyDebug >= 4 {
		__yyfmt__.Printf("char %v in %v\n", yyTokname(yytoken), yyStatname(yystate))
	}

	yyp++
	if yyp >= len(yyS) {
		nyys := make([]yySymType, len(yyS)*2)
		copy(nyys, yyS)
		yyS = nyys
	}
	yyS[yyp] = yyVAL
	yyS[yyp].yys = yystate

yynewstate:
	yyn = yyPact[yystate]
	if yyn <= yyFlag {
		goto yydefault /* simple state */
	}
	if yyrcvr.char < 0 {
		yyrcvr.char, yytoken = yylex1(yylex, &yyrcvr.lval)
	}
	yyn += yytoken
	if yyn < 0 || yyn >= yyLast {
		goto yydefault
	}
	yyn = yyAct[yyn]
	if yyChk[yyn] == yytoken { /* valid shift */
		yyrcvr.char = -1
		yytoken = -1
		yyVAL = yyrcvr.lval
		yystate = yyn
		if Errflag > 0 {
			Errflag--
		}
		goto yystack
	}

yydefault:
	/* default state action */
	yyn = yyDef[yystate]
	if yyn == -2 {
		if yyrcvr.char < 0 {
			yyrcvr.char, yytoken = yylex1(yylex, &yyrcvr.lval)
		}

		/* look through exception table */
		xi := 0
		for {
			if yyExca[xi+0] == -1 && yyExca[xi+1] == yystate {
				break
			}
			xi += 2
		}
		for xi += 2; ; xi += 2 {
			yyn = yyExca[xi+0]
			if yyn < 0 || yyn == yytoken {
				break
			}
		}
		yyn = yyExca[xi+1]
		if yyn < 0 {
			goto ret0
		}
	}
	if yyn == 0 {
		/* error ... attempt to resume parsing */
		switch Errflag {
		case 0: /* brand new error */
			yylex.Error(yyErrorMessage(yystate, yytoken))
			Nerrs++
			if yyDebug >= 1 {
				__yyfmt__.Printf("%s", yyStatname(yystate))
				__yyfmt__.Printf(" saw %s\n", yyTokname(yytoken))
			}
			fallthrough

		case 1, 2: /* incompletely recovered error ... try again */
			Errflag = 3

			/* find a state where "error" is a legal shift action */
			for yyp >= 0 {
				yyn = yyPact[yyS[yyp].yys] + yyErrCode
				if yyn >= 0 && yyn < yyLast {
					yystate = yyAct[yyn] /* simulate a shift of "error" */
					if yyChk[yystate] == yyErrCode {
						goto yystack
					}
				}

				/* the current p has no shift on "error", pop stack */
				if yyDebug >= 2 {
					__yyfmt__.Printf("error recovery pops state %d\n", yyS[yyp].yys)
				}
				yyp--
			}
			/* there is no state on the stack with an error shift ... abort */
			goto ret1

		case 3: /* no shift yet; clobber input char */
			if yyDebug >= 2 {
				__yyfmt__.Printf("error recovery discards %s\n", yyTokname(yytoken))
			}
			if yytoken == yyEofCode {
				goto ret1
			}
			yyrcvr.char = -1
			yytoken = -1
			goto yynewstate /* try again in the same state */
		}
	}

	/* reduction by production yyn */
	if yyDebug >= 2 {
		__yyfmt__.Printf("reduce %v in:\n\t%v\n", yyn, yyStatname(yystate))
	}

	yynt := yyn
	yypt := yyp
	_ = yypt // guard against "declared and not used"

	yyp -= yyR2[yyn]
	// yyp is now the index of $0. Perform the default action. Iff the
	// reduced production is ε, $1 is possibly out of range.
	if yyp+1 >= len(yyS) {
		nyys := make([]yySymType, len(yyS)*2)
		copy(nyys, yyS)
		yyS = nyys
	}
	yyVAL = yyS[yyp+1]

	/* consult goto table to find next state */
	yyn = yyR1[yyn]
	yyg := yyPgo[yyn]
	yyj := yyg + yyS[yyp].yys + 1

	if yyj >= yyLast {
		yystate = yyAct[yyg]
	} else {
		yystate = yyAct[yyj]
		if yyChk[yystate] != -yyn {
			yystate = yyAct[yyg]
		}
	}
	// dummy call; replaced with literal code
	switch yynt {

	case 1:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:202
		{
			setParseTree(yylex, yyDollar[1].statement)
		}
	case 2:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:207
		{
		}
	case 3:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:208
		{
		}
	case 4:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:212
		{
			yyVAL.statement = yyDollar[1].selStmt
		}
	case 15:
		yyDollar = yyS[yypt-13 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:228
		{
			yyVAL.selStmt = &Select{Comments: Comments(yyDollar[2].bytes2), Cache: yyDollar[3].str, Distinct: yyDollar[4].str, Hints: yyDollar[5].str, SelectExprs: yyDollar[6].selectExprs, From: yyDollar[7].tableExprs, Where: NewWhere(WhereStr, yyDollar[8].expr), GroupBy: GroupBy(yyDollar[9].exprs), Having: NewWhere(HavingStr, yyDollar[10].expr), OrderBy: yyDollar[11].orderBy, Limit: yyDollar[12].limit, Lock: yyDollar[13].str}
		}
	case 16:
		yyDollar = yyS[yypt-7 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:232
		{
			yyVAL.selStmt = &Select{Comments: Comments(yyDollar[2].bytes2), Cache: yyDollar[3].str, SelectExprs: SelectExprs{Nextval{Expr: yyDollar[5].expr}}, From: TableExprs{&AliasedTableExpr{Expr: yyDollar[7].tableName}}}
		}
	case 17:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:236
		{
			yyVAL.selStmt = &Union{Type: yyDollar[2].str, Left: yyDollar[1].selStmt, Right: yyDollar[3].selStmt}
		}
	case 18:
		yyDollar = yyS[yypt-7 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:242
		{
			yyVAL.statement = &Insert{Comments: Comments(yyDollar[2].bytes2), Ignore: yyDollar[3].str, Table: yyDollar[4].tableName, Columns: yyDollar[5].columns, Rows: yyDollar[6].insRows, OnDup: OnDup(yyDollar[7].updateExprs)}
		}
	case 19:
		yyDollar = yyS[yypt-7 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:246
		{
			cols := make(Columns, 0, len(yyDollar[6].updateExprs))
			vals := make(ValTuple, 0, len(yyDollar[7].updateExprs))
			for _, updateList := range yyDollar[6].updateExprs {
				cols = append(cols, updateList.Name.Name)
				vals = append(vals, updateList.Expr)
			}
			yyVAL.statement = &Insert{Comments: Comments(yyDollar[2].bytes2), Ignore: yyDollar[3].str, Table: yyDollar[4].tableName, Columns: cols, Rows: Values{vals}, OnDup: OnDup(yyDollar[7].updateExprs)}
		}
	case 20:
		yyDollar = yyS[yypt-8 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:258
		{
			yyVAL.statement = &Update{Comments: Comments(yyDollar[2].bytes2), Table: yyDollar[3].aliasedTableName, Exprs: yyDollar[5].updateExprs, Where: NewWhere(WhereStr, yyDollar[6].expr), OrderBy: yyDollar[7].orderBy, Limit: yyDollar[8].limit}
		}
	case 21:
		yyDollar = yyS[yypt-7 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:264
		{
			yyVAL.statement = &Delete{Comments: Comments(yyDollar[2].bytes2), Table: yyDollar[4].tableName, Where: NewWhere(WhereStr, yyDollar[5].expr), OrderBy: yyDollar[6].orderBy, Limit: yyDollar[7].limit}
		}
	case 22:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:270
		{
			yyVAL.statement = &Set{Comments: Comments(yyDollar[2].bytes2), Exprs: yyDollar[3].updateExprs}
		}
	case 23:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:276
		{
			yyVAL.statement = &DDL{Action: CreateStr, NewName: yyDollar[4].tableIdent}
		}
	case 24:
		yyDollar = yyS[yypt-8 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:280
		{
			// Change this to an alter statement
			yyVAL.statement = &DDL{Action: AlterStr, Table: yyDollar[7].tableIdent, NewName: yyDollar[7].tableIdent}
		}
	case 25:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:285
		{
			yyVAL.statement = &DDL{Action: CreateStr, NewName: NewTableIdent(yyDollar[3].colIdent.Lowered())}
		}
	case 26:
		yyDollar = yyS[yypt-6 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:291
		{
			yyVAL.statement = &DDL{Action: AlterStr, Table: yyDollar[4].tableIdent, NewName: yyDollar[4].tableIdent}
		}
	case 27:
		yyDollar = yyS[yypt-7 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:295
		{
			// Change this to a rename statement
			yyVAL.statement = &DDL{Action: RenameStr, Table: yyDollar[4].tableIdent, NewName: yyDollar[7].tableIdent}
		}
	case 28:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:300
		{
			yyVAL.statement = &DDL{Action: AlterStr, Table: NewTableIdent(yyDollar[3].colIdent.Lowered()), NewName: NewTableIdent(yyDollar[3].colIdent.Lowered())}
		}
	case 29:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:306
		{
			yyVAL.statement = &DDL{Action: RenameStr, Table: yyDollar[3].tableIdent, NewName: yyDollar[5].tableIdent}
		}
	case 30:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:312
		{
			var exists bool
			if yyDollar[3].byt != 0 {
				exists = true
			}
			yyVAL.statement = &DDL{Action: DropStr, Table: yyDollar[4].tableIdent, IfExists: exists}
		}
	case 31:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:320
		{
			// Change this to an alter statement
			yyVAL.statement = &DDL{Action: AlterStr, Table: yyDollar[5].tableIdent, NewName: yyDollar[5].tableIdent}
		}
	case 32:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:325
		{
			var exists bool
			if yyDollar[3].byt != 0 {
				exists = true
			}
			yyVAL.statement = &DDL{Action: DropStr, Table: NewTableIdent(yyDollar[4].colIdent.Lowered()), IfExists: exists}
		}
	case 33:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:335
		{
			yyVAL.statement = &DDL{Action: AlterStr, Table: yyDollar[3].tableIdent, NewName: yyDollar[3].tableIdent}
		}
	case 34:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:341
		{
			yyVAL.statement = &Other{}
		}
	case 35:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:345
		{
			yyVAL.statement = &Other{}
		}
	case 36:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:349
		{
			yyVAL.statement = &Other{}
		}
	case 37:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:354
		{
			setAllowComments(yylex, true)
		}
	case 38:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:358
		{
			yyVAL.bytes2 = yyDollar[2].bytes2
			setAllowComments(yylex, false)
		}
	case 39:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:364
		{
			yyVAL.bytes2 = nil
		}
	case 40:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:368
		{
			yyVAL.bytes2 = append(yyDollar[1].bytes2, yyDollar[2].bytes)
		}
	case 41:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:374
		{
			yyVAL.str = UnionStr
		}
	case 42:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:378
		{
			yyVAL.str = UnionAllStr
		}
	case 43:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:382
		{
			yyVAL.str = UnionDistinctStr
		}
	case 44:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:387
		{
			yyVAL.str = ""
		}
	case 45:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:391
		{
			yyVAL.str = SQLNoCacheStr
		}
	case 46:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:395
		{
			yyVAL.str = SQLCacheStr
		}
	case 47:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:400
		{
			yyVAL.str = ""
		}
	case 48:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:404
		{
			yyVAL.str = DistinctStr
		}
	case 49:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:409
		{
			yyVAL.str = ""
		}
	case 50:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:413
		{
			yyVAL.str = StraightJoinHint
		}
	case 51:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:419
		{
			yyVAL.selectExprs = SelectExprs{yyDollar[1].selectExpr}
		}
	case 52:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:423
		{
			yyVAL.selectExprs = append(yyVAL.selectExprs, yyDollar[3].selectExpr)
		}
	case 53:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:429
		{
			yyVAL.selectExpr = &StarExpr{}
		}
	case 54:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:433
		{
			yyVAL.selectExpr = &NonStarExpr{Expr: yyDollar[1].expr, As: yyDollar[2].colIdent}
		}
	case 55:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:437
		{
			yyVAL.selectExpr = &StarExpr{TableName: &TableName{Name: yyDollar[1].tableIdent}}
		}
	case 56:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:441
		{
			yyVAL.selectExpr = &StarExpr{TableName: &TableName{Qualifier: yyDollar[1].tableIdent, Name: yyDollar[3].tableIdent}}
		}
	case 57:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:446
		{
			yyVAL.colIdent = ColIdent{}
		}
	case 58:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:450
		{
			yyVAL.colIdent = yyDollar[1].colIdent
		}
	case 59:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:454
		{
			yyVAL.colIdent = yyDollar[2].colIdent
		}
	case 61:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:461
		{
			yyVAL.colIdent = NewColIdent(string(yyDollar[1].bytes))
		}
	case 62:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:466
		{
			yyVAL.tableExprs = TableExprs{&AliasedTableExpr{Expr: &TableName{Name: NewTableIdent("dual")}}}
		}
	case 63:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:470
		{
			yyVAL.tableExprs = yyDollar[2].tableExprs
		}
	case 64:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:476
		{
			yyVAL.tableExprs = TableExprs{yyDollar[1].tableExpr}
		}
	case 65:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:480
		{
			yyVAL.tableExprs = append(yyVAL.tableExprs, yyDollar[3].tableExpr)
		}
	case 68:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:490
		{
			yyVAL.tableExpr = yyDollar[1].aliasedTableName
		}
	case 69:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:494
		{
			yyVAL.tableExpr = &AliasedTableExpr{Expr: yyDollar[1].subquery, As: yyDollar[3].tableIdent}
		}
	case 70:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:498
		{
			yyVAL.tableExpr = &ParenTableExpr{Exprs: yyDollar[2].tableExprs}
		}
	case 71:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:504
		{
			yyVAL.aliasedTableName = &AliasedTableExpr{Expr: yyDollar[1].tableName, As: yyDollar[2].tableIdent, Hints: yyDollar[3].indexHints}
		}
	case 72:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:517
		{
			yyVAL.tableExpr = &JoinTableExpr{LeftExpr: yyDollar[1].tableExpr, Join: yyDollar[2].str, RightExpr: yyDollar[3].tableExpr}
		}
	case 73:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:521
		{
			yyVAL.tableExpr = &JoinTableExpr{LeftExpr: yyDollar[1].tableExpr, Join: yyDollar[2].str, RightExpr: yyDollar[3].tableExpr, On: yyDollar[5].expr}
		}
	case 74:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:525
		{
			yyVAL.tableExpr = &JoinTableExpr{LeftExpr: yyDollar[1].tableExpr, Join: yyDollar[2].str, RightExpr: yyDollar[3].tableExpr, On: yyDollar[5].expr}
		}
	case 75:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:529
		{
			yyVAL.tableExpr = &JoinTableExpr{LeftExpr: yyDollar[1].tableExpr, Join: yyDollar[2].str, RightExpr: yyDollar[3].tableExpr}
		}
	case 76:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:534
		{
			yyVAL.empty = struct{}{}
		}
	case 77:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:536
		{
			yyVAL.empty = struct{}{}
		}
	case 78:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:539
		{
			yyVAL.tableIdent = NewTableIdent("")
		}
	case 79:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:543
		{
			yyVAL.tableIdent = yyDollar[1].tableIdent
		}
	case 80:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:547
		{
			yyVAL.tableIdent = yyDollar[2].tableIdent
		}
	case 82:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:554
		{
			yyVAL.tableIdent = NewTableIdent(string(yyDollar[1].bytes))
		}
	case 83:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:560
		{
			yyVAL.str = JoinStr
		}
	case 84:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:564
		{
			yyVAL.str = JoinStr
		}
	case 85:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:568
		{
			yyVAL.str = JoinStr
		}
	case 86:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:572
		{
			yyVAL.str = StraightJoinStr
		}
	case 87:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:578
		{
			yyVAL.str = LeftJoinStr
		}
	case 88:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:582
		{
			yyVAL.str = LeftJoinStr
		}
	case 89:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:586
		{
			yyVAL.str = RightJoinStr
		}
	case 90:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:590
		{
			yyVAL.str = RightJoinStr
		}
	case 91:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:596
		{
			yyVAL.str = NaturalJoinStr
		}
	case 92:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:600
		{
			if yyDollar[2].str == LeftJoinStr {
				yyVAL.str = NaturalLeftJoinStr
			} else {
				yyVAL.str = NaturalRightJoinStr
			}
		}
	case 93:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:610
		{
			yyVAL.tableName = yyDollar[2].tableName
		}
	case 94:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:614
		{
			yyVAL.tableName = yyDollar[1].tableName
		}
	case 95:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:620
		{
			yyVAL.tableName = &TableName{Name: yyDollar[1].tableIdent}
		}
	case 96:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:624
		{
			yyVAL.tableName = &TableName{Qualifier: yyDollar[1].tableIdent, Name: yyDollar[3].tableIdent}
		}
	case 97:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:629
		{
			yyVAL.indexHints = nil
		}
	case 98:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:633
		{
			yyVAL.indexHints = &IndexHints{Type: UseStr, Indexes: yyDollar[4].colIdents}
		}
	case 99:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:637
		{
			yyVAL.indexHints = &IndexHints{Type: IgnoreStr, Indexes: yyDollar[4].colIdents}
		}
	case 100:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:641
		{
			yyVAL.indexHints = &IndexHints{Type: ForceStr, Indexes: yyDollar[4].colIdents}
		}
	case 101:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:647
		{
			yyVAL.colIdents = []ColIdent{yyDollar[1].colIdent}
		}
	case 102:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:651
		{
			yyVAL.colIdents = append(yyDollar[1].colIdents, yyDollar[3].colIdent)
		}
	case 103:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:656
		{
			yyVAL.expr = nil
		}
	case 104:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:660
		{
			yyVAL.expr = yyDollar[2].expr
		}
	case 105:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:666
		{
			yyVAL.expr = yyDollar[1].expr
		}
	case 106:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:670
		{
			yyVAL.expr = &AndExpr{Left: yyDollar[1].expr, Right: yyDollar[3].expr}
		}
	case 107:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:674
		{
			yyVAL.expr = &OrExpr{Left: yyDollar[1].expr, Right: yyDollar[3].expr}
		}
	case 108:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:678
		{
			yyVAL.expr = &NotExpr{Expr: yyDollar[2].expr}
		}
	case 109:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:682
		{
			yyVAL.expr = &IsExpr{Operator: yyDollar[3].str, Expr: yyDollar[1].expr}
		}
	case 110:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:686
		{
			yyVAL.expr = yyDollar[1].expr
		}
	case 111:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:692
		{
			yyVAL.boolVal = BoolVal(true)
		}
	case 112:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:696
		{
			yyVAL.boolVal = BoolVal(false)
		}
	case 113:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:702
		{
			yyVAL.expr = yyDollar[1].boolVal
		}
	case 114:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:706
		{
			yyVAL.expr = &ComparisonExpr{Left: yyDollar[1].expr, Operator: yyDollar[2].str, Right: yyDollar[3].boolVal}
		}
	case 115:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:710
		{
			yyVAL.expr = &ComparisonExpr{Left: yyDollar[1].expr, Operator: yyDollar[2].str, Right: yyDollar[3].expr}
		}
	case 116:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:714
		{
			yyVAL.expr = &ComparisonExpr{Left: yyDollar[1].expr, Operator: InStr, Right: yyDollar[3].colTuple}
		}
	case 117:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:718
		{
			yyVAL.expr = &ComparisonExpr{Left: yyDollar[1].expr, Operator: NotInStr, Right: yyDollar[4].colTuple}
		}
	case 118:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:722
		{
			yyVAL.expr = &ComparisonExpr{Left: yyDollar[1].expr, Operator: LikeStr, Right: yyDollar[3].expr}
		}
	case 119:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:726
		{
			yyVAL.expr = &ComparisonExpr{Left: yyDollar[1].expr, Operator: NotLikeStr, Right: yyDollar[4].expr}
		}
	case 120:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:730
		{
			yyVAL.expr = &ComparisonExpr{Left: yyDollar[1].expr, Operator: RegexpStr, Right: yyDollar[3].expr}
		}
	case 121:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:734
		{
			yyVAL.expr = &ComparisonExpr{Left: yyDollar[1].expr, Operator: NotRegexpStr, Right: yyDollar[4].expr}
		}
	case 122:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:738
		{
			yyVAL.expr = &RangeCond{Left: yyDollar[1].expr, Operator: BetweenStr, From: yyDollar[3].expr, To: yyDollar[5].expr}
		}
	case 123:
		yyDollar = yyS[yypt-6 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:742
		{
			yyVAL.expr = &RangeCond{Left: yyDollar[1].expr, Operator: NotBetweenStr, From: yyDollar[4].expr, To: yyDollar[6].expr}
		}
	case 124:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:746
		{
			yyVAL.expr = &ExistsExpr{Subquery: yyDollar[2].subquery}
		}
	case 125:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:752
		{
			yyVAL.str = IsNullStr
		}
	case 126:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:756
		{
			yyVAL.str = IsNotNullStr
		}
	case 127:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:760
		{
			yyVAL.str = IsTrueStr
		}
	case 128:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:764
		{
			yyVAL.str = IsNotTrueStr
		}
	case 129:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:768
		{
			yyVAL.str = IsFalseStr
		}
	case 130:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:772
		{
			yyVAL.str = IsNotFalseStr
		}
	case 131:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:778
		{
			yyVAL.str = EqualStr
		}
	case 132:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:782
		{
			yyVAL.str = LessThanStr
		}
	case 133:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:786
		{
			yyVAL.str = GreaterThanStr
		}
	case 134:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:790
		{
			yyVAL.str = LessEqualStr
		}
	case 135:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:794
		{
			yyVAL.str = GreaterEqualStr
		}
	case 136:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:798
		{
			yyVAL.str = NotEqualStr
		}
	case 137:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:802
		{
			yyVAL.str = NullSafeEqualStr
		}
	case 138:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:808
		{
			yyVAL.colTuple = yyDollar[1].valTuple
		}
	case 139:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:812
		{
			yyVAL.colTuple = yyDollar[1].subquery
		}
	case 140:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:816
		{
			yyVAL.colTuple = ListArg(yyDollar[1].bytes)
		}
	case 141:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:822
		{
			yyVAL.subquery = &Subquery{yyDollar[2].selStmt}
		}
	case 142:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:828
		{
			yyVAL.exprs = Exprs{yyDollar[1].expr}
		}
	case 143:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:832
		{
			yyVAL.exprs = append(yyDollar[1].exprs, yyDollar[3].expr)
		}
	case 144:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:838
		{
			yyVAL.str = string(yyDollar[1].bytes)
		}
	case 145:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:842
		{
			yyVAL.str = string(yyDollar[1].bytes)
		}
	case 146:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:846
		{
			yyVAL.str = string("binary")
		}
	case 147:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:852
		{
			yyVAL.expr = yyDollar[1].expr
		}
	case 148:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:856
		{
			yyVAL.expr = yyDollar[1].colName
		}
	case 149:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:860
		{
			yyVAL.expr = yyDollar[1].expr
		}
	case 150:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:864
		{
			yyVAL.expr = yyDollar[1].subquery
		}
	case 151:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:868
		{
			yyVAL.expr = &BinaryExpr{Left: yyDollar[1].expr, Operator: BitAndStr, Right: yyDollar[3].expr}
		}
	case 152:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:872
		{
			yyVAL.expr = &BinaryExpr{Left: yyDollar[1].expr, Operator: BitOrStr, Right: yyDollar[3].expr}
		}
	case 153:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:876
		{
			yyVAL.expr = &BinaryExpr{Left: yyDollar[1].expr, Operator: BitXorStr, Right: yyDollar[3].expr}
		}
	case 154:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:880
		{
			yyVAL.expr = &BinaryExpr{Left: yyDollar[1].expr, Operator: PlusStr, Right: yyDollar[3].expr}
		}
	case 155:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:884
		{
			yyVAL.expr = &BinaryExpr{Left: yyDollar[1].expr, Operator: MinusStr, Right: yyDollar[3].expr}
		}
	case 156:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:888
		{
			yyVAL.expr = &BinaryExpr{Left: yyDollar[1].expr, Operator: MultStr, Right: yyDollar[3].expr}
		}
	case 157:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:892
		{
			yyVAL.expr = &BinaryExpr{Left: yyDollar[1].expr, Operator: DivStr, Right: yyDollar[3].expr}
		}
	case 158:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:896
		{
			yyVAL.expr = &BinaryExpr{Left: yyDollar[1].expr, Operator: IntDivStr, Right: yyDollar[3].expr}
		}
	case 159:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:900
		{
			yyVAL.expr = &BinaryExpr{Left: yyDollar[1].expr, Operator: ModStr, Right: yyDollar[3].expr}
		}
	case 160:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:904
		{
			yyVAL.expr = &BinaryExpr{Left: yyDollar[1].expr, Operator: ModStr, Right: yyDollar[3].expr}
		}
	case 161:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:908
		{
			yyVAL.expr = &BinaryExpr{Left: yyDollar[1].expr, Operator: ShiftLeftStr, Right: yyDollar[3].expr}
		}
	case 162:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:912
		{
			yyVAL.expr = &BinaryExpr{Left: yyDollar[1].expr, Operator: ShiftRightStr, Right: yyDollar[3].expr}
		}
	case 163:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:916
		{
			yyVAL.expr = &BinaryExpr{Left: yyDollar[1].colName, Operator: JSONExtractOp, Right: yyDollar[3].expr}
		}
	case 164:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:920
		{
			yyVAL.expr = &BinaryExpr{Left: yyDollar[1].colName, Operator: JSONUnquoteExtractOp, Right: yyDollar[3].expr}
		}
	case 165:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:924
		{
			yyVAL.expr = &CollateExpr{Expr: yyDollar[1].expr, Charset: yyDollar[3].str}
		}
	case 166:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:928
		{
			yyVAL.expr = &UnaryExpr{Operator: BinaryStr, Expr: yyDollar[2].expr}
		}
	case 167:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:932
		{
			if num, ok := yyDollar[2].expr.(*SQLVal); ok && num.Type == IntVal {
				yyVAL.expr = num
			} else {
				yyVAL.expr = &UnaryExpr{Operator: UPlusStr, Expr: yyDollar[2].expr}
			}
		}
	case 168:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:940
		{
			if num, ok := yyDollar[2].expr.(*SQLVal); ok && num.Type == IntVal {
				// Handle double negative
				if num.Val[0] == '-' {
					num.Val = num.Val[1:]
					yyVAL.expr = num
				} else {
					yyVAL.expr = NewIntVal(append([]byte("-"), num.Val...))
				}
			} else {
				yyVAL.expr = &UnaryExpr{Operator: UMinusStr, Expr: yyDollar[2].expr}
			}
		}
	case 169:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:954
		{
			yyVAL.expr = &UnaryExpr{Operator: TildaStr, Expr: yyDollar[2].expr}
		}
	case 170:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:958
		{
			yyVAL.expr = &UnaryExpr{Operator: BangStr, Expr: yyDollar[2].expr}
		}
	case 171:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:962
		{
			// This rule prevents the usage of INTERVAL
			// as a function. If support is needed for that,
			// we'll need to revisit this. The solution
			// will be non-trivial because of grammar conflicts.
			yyVAL.expr = &IntervalExpr{Expr: yyDollar[2].expr, Unit: yyDollar[3].colIdent}
		}
	case 172:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:970
		{
			yyVAL.expr = &FuncExpr{Name: yyDollar[1].colIdent}
		}
	case 173:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:974
		{
			yyVAL.expr = &FuncExpr{Name: yyDollar[1].colIdent, Exprs: yyDollar[3].selectExprs}
		}
	case 174:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:978
		{
			yyVAL.expr = &FuncExpr{Name: yyDollar[1].colIdent, Distinct: true, Exprs: yyDollar[4].selectExprs}
		}
	case 175:
		yyDollar = yyS[yypt-7 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:982
		{
			yyVAL.expr = &GroupConcatExpr{Distinct: yyDollar[3].str, Exprs: yyDollar[4].selectExprs, OrderBy: yyDollar[5].orderBy, Separator: yyDollar[6].str}
		}
	case 176:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:986
		{
			yyVAL.expr = &FuncExpr{Name: yyDollar[1].colIdent}
		}
	case 177:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:990
		{
			yyVAL.expr = &FuncExpr{Name: yyDollar[1].colIdent, Exprs: yyDollar[3].selectExprs}
		}
	case 178:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:994
		{
			yyVAL.expr = &FuncExpr{Name: yyDollar[1].colIdent}
		}
	case 179:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:998
		{
			yyVAL.expr = yyDollar[1].caseExpr
		}
	case 180:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1002
		{
			yyVAL.expr = &ValuesFuncExpr{Name: yyDollar[3].colIdent}
		}
	case 181:
		yyDollar = yyS[yypt-6 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1006
		{
			yyVAL.expr = &ConvertExpr{Expr: yyDollar[3].expr, Type: yyDollar[5].convertType}
		}
	case 182:
		yyDollar = yyS[yypt-6 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1010
		{
			yyVAL.expr = &ConvertExpr{Expr: yyDollar[3].expr, Type: yyDollar[5].convertType}
		}
	case 183:
		yyDollar = yyS[yypt-6 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1014
		{
			yyVAL.expr = &ConvertExpr{Expr: yyDollar[3].expr, Type: yyDollar[5].convertType}
		}
	case 184:
		yyDollar = yyS[yypt-9 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1018
		{
			yyVAL.expr = &MatchExpr{Columns: yyDollar[3].columns, Expr: yyDollar[7].expr, Option: yyDollar[8].str}
		}
	case 185:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1024
		{
			yyVAL.str = ""
		}
	case 186:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1028
		{
			yyVAL.str = BooleanModeStr
		}
	case 187:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1032
		{
			yyVAL.str = NaturalLanguageModeStr
		}
	case 188:
		yyDollar = yyS[yypt-7 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1036
		{
			yyVAL.str = NaturalLanguageModeWithQueryExpansionStr
		}
	case 189:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1040
		{
			yyVAL.str = QueryExpansionStr
		}
	case 190:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1047
		{
			yyVAL.convertType = &ConvertType{Type: yyDollar[1].str}
		}
	case 191:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1051
		{
			yyVAL.convertType = &ConvertType{Type: yyDollar[1].str}
		}
	case 192:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1055
		{
			yyVAL.convertType = &ConvertType{Type: yyDollar[1].str, Length: NewIntVal(yyDollar[3].bytes)}
		}
	case 193:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1059
		{
			yyVAL.convertType = &ConvertType{Type: yyDollar[1].str, Length: NewIntVal(yyDollar[3].bytes), Charset: yyDollar[5].str}
		}
	case 194:
		yyDollar = yyS[yypt-7 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1063
		{
			yyVAL.convertType = &ConvertType{Type: yyDollar[1].str, Length: NewIntVal(yyDollar[3].bytes), Charset: yyDollar[7].str, Operator: CharacterSetStr}
		}
	case 195:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1067
		{
			yyVAL.convertType = &ConvertType{Type: yyDollar[1].str, Charset: yyDollar[2].str}
		}
	case 196:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1071
		{
			yyVAL.convertType = &ConvertType{Type: yyDollar[1].str, Charset: yyDollar[4].str, Operator: CharacterSetStr}
		}
	case 197:
		yyDollar = yyS[yypt-6 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1075
		{
			yyVAL.convertType = &ConvertType{Type: yyDollar[1].str, Length: NewIntVal(yyDollar[3].bytes), Scale: NewIntVal(yyDollar[5].bytes)}
		}
	case 198:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1083
		{
			yyVAL.colIdent = NewColIdent("current_timestamp")
		}
	case 199:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1087
		{
			yyVAL.colIdent = NewColIdent("current_date")
		}
	case 200:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1091
		{
			yyVAL.colIdent = NewColIdent("current_time")
		}
	case 201:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1095
		{
			yyVAL.colIdent = NewColIdent("utc_timestamp")
		}
	case 202:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1099
		{
			yyVAL.colIdent = NewColIdent("utc_time")
		}
	case 203:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1103
		{
			yyVAL.colIdent = NewColIdent("utc_date")
		}
	case 204:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1107
		{
			yyVAL.colIdent = NewColIdent("localtime")
		}
	case 205:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1111
		{
			yyVAL.colIdent = NewColIdent("localtimestamp")
		}
	case 207:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1120
		{
			yyVAL.colIdent = NewColIdent("if")
		}
	case 208:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1124
		{
			yyVAL.colIdent = NewColIdent("database")
		}
	case 209:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1128
		{
			yyVAL.colIdent = NewColIdent("unix_timestamp")
		}
	case 210:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1132
		{
			yyVAL.colIdent = NewColIdent("mod")
		}
	case 211:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1136
		{
			yyVAL.colIdent = NewColIdent("replace")
		}
	case 212:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1140
		{
			yyVAL.colIdent = NewColIdent("left")
		}
	case 213:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1146
		{
			yyVAL.caseExpr = &CaseExpr{Expr: yyDollar[2].expr, Whens: yyDollar[3].whens, Else: yyDollar[4].expr}
		}
	case 214:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1151
		{
			yyVAL.expr = nil
		}
	case 215:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1155
		{
			yyVAL.expr = yyDollar[1].expr
		}
	case 216:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1160
		{
			yyVAL.str = string("")
		}
	case 217:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1164
		{
			yyVAL.str = " separator '" + string(yyDollar[2].bytes) + "'"
		}
	case 218:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1170
		{
			yyVAL.whens = []*When{yyDollar[1].when}
		}
	case 219:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1174
		{
			yyVAL.whens = append(yyDollar[1].whens, yyDollar[2].when)
		}
	case 220:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1180
		{
			yyVAL.when = &When{Cond: yyDollar[2].expr, Val: yyDollar[4].expr}
		}
	case 221:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1185
		{
			yyVAL.expr = nil
		}
	case 222:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1189
		{
			yyVAL.expr = yyDollar[2].expr
		}
	case 223:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1195
		{
			yyVAL.colName = &ColName{Name: yyDollar[1].colIdent}
		}
	case 224:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1199
		{
			yyVAL.colName = &ColName{Qualifier: &TableName{Name: yyDollar[1].tableIdent}, Name: yyDollar[3].colIdent}
		}
	case 225:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1203
		{
			yyVAL.colName = &ColName{Qualifier: &TableName{Qualifier: yyDollar[1].tableIdent, Name: yyDollar[3].tableIdent}, Name: yyDollar[5].colIdent}
		}
	case 226:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1209
		{
			yyVAL.expr = NewStrVal(yyDollar[1].bytes)
		}
	case 227:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1213
		{
			yyVAL.expr = NewHexVal(yyDollar[1].bytes)
		}
	case 228:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1217
		{
			yyVAL.expr = NewIntVal(yyDollar[1].bytes)
		}
	case 229:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1221
		{
			yyVAL.expr = NewFloatVal(yyDollar[1].bytes)
		}
	case 230:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1225
		{
			yyVAL.expr = NewHexNum(yyDollar[1].bytes)
		}
	case 231:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1229
		{
			yyVAL.expr = NewValArg(yyDollar[1].bytes)
		}
	case 232:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1233
		{
			yyVAL.expr = &NullVal{}
		}
	case 233:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1239
		{
			// TODO(sougou): Deprecate this construct.
			if yyDollar[1].colIdent.Lowered() != "value" {
				yylex.Error("expecting value after next")
				return 1
			}
			yyVAL.expr = NewIntVal([]byte("1"))
		}
	case 234:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1248
		{
			yyVAL.expr = NewIntVal(yyDollar[1].bytes)
		}
	case 235:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1252
		{
			yyVAL.expr = NewValArg(yyDollar[1].bytes)
		}
	case 236:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1257
		{
			yyVAL.exprs = nil
		}
	case 237:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1261
		{
			yyVAL.exprs = yyDollar[3].exprs
		}
	case 238:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1266
		{
			yyVAL.expr = nil
		}
	case 239:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1270
		{
			yyVAL.expr = yyDollar[2].expr
		}
	case 240:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1275
		{
			yyVAL.orderBy = nil
		}
	case 241:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1279
		{
			yyVAL.orderBy = yyDollar[3].orderBy
		}
	case 242:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1285
		{
			yyVAL.orderBy = OrderBy{yyDollar[1].order}
		}
	case 243:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1289
		{
			yyVAL.orderBy = append(yyDollar[1].orderBy, yyDollar[3].order)
		}
	case 244:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1295
		{
			yyVAL.order = &Order{Expr: yyDollar[1].expr, Direction: yyDollar[2].str}
		}
	case 245:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1300
		{
			yyVAL.str = AscScr
		}
	case 246:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1304
		{
			yyVAL.str = AscScr
		}
	case 247:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1308
		{
			yyVAL.str = DescScr
		}
	case 248:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1313
		{
			yyVAL.limit = nil
		}
	case 249:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1317
		{
			yyVAL.limit = &Limit{Rowcount: yyDollar[2].expr}
		}
	case 250:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1321
		{
			yyVAL.limit = &Limit{Offset: yyDollar[2].expr, Rowcount: yyDollar[4].expr}
		}
	case 251:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1325
		{
			yyVAL.limit = &Limit{Offset: yyDollar[4].expr, Rowcount: yyDollar[2].expr}
		}
	case 252:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1330
		{
			yyVAL.str = ""
		}
	case 253:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1334
		{
			yyVAL.str = ForUpdateStr
		}
	case 254:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1338
		{
			yyVAL.str = ShareModeStr
		}
	case 255:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1344
		{
			yyVAL.columns = Columns{yyDollar[1].colIdent}
		}
	case 256:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1348
		{
			yyVAL.columns = append(yyVAL.columns, yyDollar[3].colIdent)
		}
	case 257:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1353
		{
			yyVAL.columns = nil
		}
	case 258:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1357
		{
			yyVAL.columns = yyDollar[2].columns
		}
	case 259:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1363
		{
			yyVAL.columns = Columns{yyDollar[1].colIdent}
		}
	case 260:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1367
		{
			yyVAL.columns = Columns{yyDollar[3].colIdent}
		}
	case 261:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1371
		{
			yyVAL.columns = append(yyVAL.columns, yyDollar[3].colIdent)
		}
	case 262:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1375
		{
			yyVAL.columns = append(yyVAL.columns, yyDollar[5].colIdent)
		}
	case 263:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1380
		{
			yyVAL.updateExprs = nil
		}
	case 264:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1384
		{
			yyVAL.updateExprs = yyDollar[5].updateExprs
		}
	case 265:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1390
		{
			yyVAL.insRows = yyDollar[2].values
		}
	case 266:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1394
		{
			yyVAL.insRows = yyDollar[1].selStmt
		}
	case 267:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1400
		{
			yyVAL.values = Values{yyDollar[1].valTuple}
		}
	case 268:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1404
		{
			yyVAL.values = append(yyDollar[1].values, yyDollar[3].valTuple)
		}
	case 269:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1410
		{
			yyVAL.valTuple = ValTuple(yyDollar[2].exprs)
		}
	case 270:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1416
		{
			if len(yyDollar[1].valTuple) == 1 {
				yyVAL.expr = &ParenExpr{yyDollar[1].valTuple[0]}
			} else {
				yyVAL.expr = yyDollar[1].valTuple
			}
		}
	case 271:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1426
		{
			yyVAL.updateExprs = UpdateExprs{yyDollar[1].updateExpr}
		}
	case 272:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1430
		{
			yyVAL.updateExprs = append(yyDollar[1].updateExprs, yyDollar[3].updateExpr)
		}
	case 273:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1436
		{
			yyVAL.updateExpr = &UpdateExpr{Name: yyDollar[1].colName, Expr: yyDollar[3].expr}
		}
	case 276:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1445
		{
			yyVAL.byt = 0
		}
	case 277:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1447
		{
			yyVAL.byt = 1
		}
	case 278:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1450
		{
			yyVAL.empty = struct{}{}
		}
	case 279:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1452
		{
			yyVAL.empty = struct{}{}
		}
	case 280:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1455
		{
			yyVAL.str = ""
		}
	case 281:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1457
		{
			yyVAL.str = IgnoreStr
		}
	case 282:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1461
		{
			yyVAL.empty = struct{}{}
		}
	case 283:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1463
		{
			yyVAL.empty = struct{}{}
		}
	case 284:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1465
		{
			yyVAL.empty = struct{}{}
		}
	case 285:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1467
		{
			yyVAL.empty = struct{}{}
		}
	case 286:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1469
		{
			yyVAL.empty = struct{}{}
		}
	case 287:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1471
		{
			yyVAL.empty = struct{}{}
		}
	case 288:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1474
		{
			yyVAL.empty = struct{}{}
		}
	case 289:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1476
		{
			yyVAL.empty = struct{}{}
		}
	case 290:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1479
		{
			yyVAL.empty = struct{}{}
		}
	case 291:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1481
		{
			yyVAL.empty = struct{}{}
		}
	case 292:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1484
		{
			yyVAL.empty = struct{}{}
		}
	case 293:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1486
		{
			yyVAL.empty = struct{}{}
		}
	case 294:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1490
		{
			yyVAL.colIdent = NewColIdent(string(yyDollar[1].bytes))
		}
	case 295:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1496
		{
			yyVAL.tableIdent = NewTableIdent(string(yyDollar[1].bytes))
		}
	case 296:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1502
		{
			if incNesting(yylex) {
				yylex.Error("max nesting level reached")
				return 1
			}
		}
	case 297:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1511
		{
			decNesting(yylex)
		}
	case 298:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1516
		{
			forceEOF(yylex)
		}
	}
	goto yystack /* stack new state and value */
}
