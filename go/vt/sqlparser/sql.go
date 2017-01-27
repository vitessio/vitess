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
	yys         int
	empty       struct{}
	statement   Statement
	selStmt     SelectStatement
	byt         byte
	bytes       []byte
	bytes2      [][]byte
	str         string
	selectExprs SelectExprs
	selectExpr  SelectExpr
	columns     Columns
	colName     *ColName
	tableExprs  TableExprs
	tableExpr   TableExpr
	tableName   *TableName
	indexHints  *IndexHints
	expr        Expr
	boolExpr    BoolExpr
	boolVal     BoolVal
	valExpr     ValExpr
	colTuple    ColTuple
	valExprs    ValExprs
	values      Values
	valTuple    ValTuple
	subquery    *Subquery
	caseExpr    *CaseExpr
	whens       []*When
	when        *When
	orderBy     OrderBy
	order       *Order
	limit       *Limit
	insRows     InsertRows
	updateExprs UpdateExprs
	updateExpr  *UpdateExpr
	colIdent    ColIdent
	colIdents   []ColIdent
	tableIdent  TableIdent
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
const JOIN = 57377
const STRAIGHT_JOIN = 57378
const LEFT = 57379
const RIGHT = 57380
const INNER = 57381
const OUTER = 57382
const CROSS = 57383
const NATURAL = 57384
const USE = 57385
const FORCE = 57386
const ON = 57387
const ID = 57388
const HEX = 57389
const STRING = 57390
const INTEGRAL = 57391
const FLOAT = 57392
const HEXNUM = 57393
const VALUE_ARG = 57394
const LIST_ARG = 57395
const COMMENT = 57396
const NULL = 57397
const TRUE = 57398
const FALSE = 57399
const OR = 57400
const AND = 57401
const NOT = 57402
const BETWEEN = 57403
const CASE = 57404
const WHEN = 57405
const THEN = 57406
const ELSE = 57407
const END = 57408
const LE = 57409
const GE = 57410
const NE = 57411
const NULL_SAFE_EQUAL = 57412
const IS = 57413
const LIKE = 57414
const REGEXP = 57415
const IN = 57416
const SHIFT_LEFT = 57417
const SHIFT_RIGHT = 57418
const MOD = 57419
const UNARY = 57420
const COLLATE = 57421
const INTERVAL = 57422
const JSON_EXTRACT_OP = 57423
const JSON_UNQUOTE_EXTRACT_OP = 57424
const CREATE = 57425
const ALTER = 57426
const DROP = 57427
const RENAME = 57428
const ANALYZE = 57429
const TABLE = 57430
const INDEX = 57431
const VIEW = 57432
const TO = 57433
const IGNORE = 57434
const IF = 57435
const UNIQUE = 57436
const USING = 57437
const SHOW = 57438
const DESCRIBE = 57439
const EXPLAIN = 57440
const CURRENT_TIMESTAMP = 57441
const DATABASE = 57442
const CURRENT_DATE = 57443
const UNIX_TIMESTAMP = 57444
const CURRENT_TIME = 57445
const LOCALTIME = 57446
const LOCALTIMESTAMP = 57447
const UTC_DATE = 57448
const UTC_TIME = 57449
const UTC_TIMESTAMP = 57450
const REPLACE = 57451
const UNUSED = 57452

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
	"'%'",
	"MOD",
	"'^'",
	"'~'",
	"UNARY",
	"COLLATE",
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
	"UNUSED",
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
	-1, 113,
	96, 264,
	-2, 263,
	-1, 127,
	46, 182,
	-2, 172,
}

const yyNprod = 268
const yyPrivate = 57344

var yyTokenNames []string
var yyStates []string

const yyLast = 1288

var yyAct = [...]int{

	125, 111, 455, 335, 64, 386, 119, 230, 345, 310,
	252, 324, 120, 355, 284, 106, 264, 326, 117, 277,
	129, 172, 192, 107, 49, 268, 167, 266, 37, 265,
	39, 229, 3, 176, 40, 406, 408, 66, 182, 76,
	71, 69, 42, 73, 43, 43, 45, 46, 47, 438,
	50, 51, 180, 261, 101, 437, 436, 83, 70, 72,
	60, 52, 48, 44, 388, 14, 27, 16, 17, 232,
	233, 362, 255, 184, 212, 213, 214, 215, 209, 105,
	196, 218, 91, 218, 209, 89, 65, 218, 18, 92,
	67, 66, 291, 113, 66, 165, 98, 460, 100, 199,
	407, 197, 116, 198, 197, 94, 289, 290, 288, 163,
	194, 325, 418, 378, 164, 325, 199, 226, 228, 199,
	173, 96, 311, 86, 179, 181, 178, 62, 412, 358,
	187, 272, 67, 198, 197, 198, 197, 116, 116, 420,
	278, 280, 281, 240, 93, 279, 131, 238, 239, 199,
	183, 199, 241, 363, 364, 365, 287, 356, 19, 20,
	22, 21, 23, 66, 250, 14, 131, 248, 193, 62,
	254, 24, 25, 26, 308, 67, 309, 258, 84, 62,
	169, 85, 244, 88, 28, 62, 67, 358, 195, 113,
	116, 313, 251, 247, 194, 97, 67, 274, 195, 275,
	276, 77, 259, 269, 190, 131, 131, 62, 62, 462,
	311, 116, 270, 263, 286, 262, 271, 189, 311, 116,
	116, 282, 30, 285, 313, 311, 295, 210, 211, 212,
	213, 214, 215, 209, 343, 311, 218, 168, 317, 312,
	314, 189, 133, 132, 134, 135, 136, 137, 318, 321,
	138, 315, 316, 329, 319, 322, 274, 427, 428, 332,
	116, 116, 334, 333, 311, 311, 331, 327, 330, 93,
	425, 381, 343, 93, 227, 347, 350, 351, 352, 348,
	162, 349, 353, 269, 432, 433, 327, 361, 257, 75,
	104, 401, 270, 366, 399, 435, 402, 131, 81, 400,
	434, 286, 398, 367, 397, 347, 350, 351, 352, 348,
	285, 349, 353, 57, 41, 208, 207, 216, 217, 210,
	211, 212, 213, 214, 215, 209, 56, 379, 218, 373,
	160, 159, 375, 453, 382, 377, 374, 78, 383, 90,
	421, 403, 116, 351, 352, 454, 380, 116, 59, 174,
	424, 103, 14, 360, 336, 112, 269, 269, 269, 269,
	53, 54, 404, 413, 389, 270, 270, 270, 270, 170,
	409, 411, 394, 393, 396, 395, 414, 246, 158, 392,
	423, 337, 342, 253, 417, 391, 157, 168, 422, 413,
	31, 231, 63, 430, 459, 441, 234, 235, 236, 237,
	431, 429, 14, 330, 30, 32, 33, 34, 35, 36,
	1, 307, 359, 116, 208, 207, 216, 217, 210, 211,
	212, 213, 214, 215, 209, 354, 191, 218, 243, 175,
	38, 444, 260, 445, 446, 447, 177, 68, 156, 249,
	161, 452, 66, 426, 385, 127, 451, 256, 126, 456,
	456, 456, 457, 458, 116, 116, 390, 341, 448, 449,
	450, 465, 376, 466, 112, 461, 467, 463, 464, 242,
	323, 128, 118, 328, 415, 283, 82, 245, 292, 293,
	294, 171, 296, 297, 298, 299, 300, 301, 302, 303,
	304, 305, 306, 208, 207, 216, 217, 210, 211, 212,
	213, 214, 215, 209, 200, 114, 218, 405, 87, 346,
	344, 267, 188, 112, 112, 109, 61, 216, 217, 210,
	211, 212, 213, 214, 215, 209, 74, 80, 218, 55,
	79, 207, 216, 217, 210, 211, 212, 213, 214, 215,
	209, 61, 15, 218, 29, 61, 58, 13, 12, 11,
	95, 10, 9, 8, 99, 7, 6, 102, 5, 4,
	256, 2, 110, 0, 368, 369, 370, 0, 0, 0,
	61, 0, 0, 166, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 185, 0, 0, 186, 0, 372, 0,
	0, 0, 0, 0, 0, 112, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 256, 202, 205, 0, 0,
	0, 384, 387, 219, 220, 221, 222, 223, 224, 225,
	206, 203, 204, 201, 208, 207, 216, 217, 210, 211,
	212, 213, 214, 215, 209, 0, 371, 218, 61, 0,
	0, 0, 0, 0, 0, 0, 416, 0, 0, 0,
	0, 0, 0, 419, 0, 208, 207, 216, 217, 210,
	211, 212, 213, 214, 215, 209, 0, 256, 218, 0,
	0, 110, 61, 0, 0, 0, 0, 0, 273, 320,
	0, 130, 0, 0, 0, 0, 0, 0, 0, 0,
	439, 0, 0, 67, 0, 440, 144, 0, 442, 443,
	387, 0, 0, 0, 0, 131, 0, 311, 113, 133,
	132, 134, 135, 136, 137, 0, 0, 138, 154, 155,
	110, 110, 115, 0, 153, 208, 207, 216, 217, 210,
	211, 212, 213, 214, 215, 209, 0, 338, 218, 339,
	0, 0, 340, 0, 121, 122, 108, 0, 0, 142,
	357, 123, 61, 0, 124, 0, 208, 207, 216, 217,
	210, 211, 212, 213, 214, 215, 209, 0, 139, 218,
	0, 0, 0, 0, 145, 140, 146, 141, 147, 151,
	152, 150, 149, 148, 143, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 110, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 130, 0, 0, 0,
	0, 0, 0, 0, 0, 61, 61, 61, 61, 0,
	0, 144, 0, 0, 0, 0, 0, 0, 357, 0,
	131, 410, 311, 113, 133, 132, 134, 135, 136, 137,
	0, 0, 138, 154, 155, 0, 0, 115, 0, 153,
	0, 0, 0, 130, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 144, 121,
	122, 108, 0, 0, 142, 0, 123, 131, 0, 124,
	113, 133, 132, 134, 135, 136, 137, 0, 0, 138,
	154, 155, 0, 139, 115, 0, 153, 0, 0, 145,
	140, 146, 141, 147, 151, 152, 150, 149, 148, 143,
	0, 0, 0, 0, 0, 0, 121, 122, 108, 0,
	0, 142, 0, 123, 0, 0, 124, 14, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	139, 0, 0, 130, 0, 0, 145, 140, 146, 141,
	147, 151, 152, 150, 149, 148, 143, 0, 144, 0,
	0, 0, 0, 0, 0, 0, 0, 131, 0, 0,
	113, 133, 132, 134, 135, 136, 137, 0, 0, 138,
	154, 155, 0, 0, 115, 0, 153, 0, 0, 0,
	130, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 144, 121, 122, 0, 0,
	0, 142, 0, 123, 131, 0, 124, 113, 133, 132,
	134, 135, 136, 137, 0, 0, 138, 154, 155, 0,
	139, 115, 0, 153, 0, 0, 145, 140, 146, 141,
	147, 151, 152, 150, 149, 148, 143, 0, 0, 0,
	0, 0, 144, 121, 122, 0, 0, 0, 142, 0,
	123, 131, 0, 124, 113, 133, 132, 134, 135, 136,
	137, 0, 0, 138, 154, 155, 0, 139, 0, 0,
	153, 0, 0, 145, 140, 146, 141, 147, 151, 152,
	150, 149, 148, 143, 0, 0, 0, 0, 0, 0,
	121, 122, 0, 0, 0, 142, 0, 123, 0, 0,
	124, 14, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 139, 0, 0, 0, 0, 0,
	145, 140, 146, 141, 147, 151, 152, 150, 149, 148,
	143, 0, 144, 0, 0, 0, 0, 0, 0, 0,
	0, 131, 0, 0, 113, 133, 132, 134, 135, 136,
	137, 0, 0, 138, 0, 0, 0, 0, 0, 0,
	153, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 144,
	121, 122, 0, 0, 0, 142, 0, 123, 131, 0,
	124, 113, 133, 132, 134, 135, 136, 137, 0, 0,
	138, 0, 0, 0, 139, 0, 0, 153, 0, 0,
	145, 140, 146, 141, 147, 151, 152, 150, 149, 148,
	143, 0, 0, 0, 0, 0, 0, 121, 122, 0,
	0, 0, 142, 0, 123, 0, 0, 124, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 139, 0, 0, 0, 0, 0, 145, 140, 146,
	141, 147, 151, 152, 150, 149, 148, 143,
}
var yyPact = [...]int{

	59, -1000, -1000, 399, -1000, -1000, -1000, -1000, -1000, -1000,
	-1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, -76,
	-64, -41, -58, -42, -1000, -1000, -1000, -1000, -1000, 396,
	341, 293, -1000, -63, 130, 382, 83, -68, -47, 83,
	-1000, -45, 83, -1000, 130, -70, 152, -70, 130, -1000,
	-1000, -1000, -1000, -1000, -1000, 262, 126, -1000, 66, 158,
	310, -14, -1000, 130, 97, -1000, 35, -1000, 130, 58,
	146, -1000, 130, -1000, -53, 130, 329, 245, 83, -1000,
	841, -1000, 368, -1000, 300, 299, -1000, 251, 130, -1000,
	83, 130, 376, 83, 1162, -1000, 327, -78, -1000, 24,
	-1000, 130, -1000, -1000, 130, -1000, 194, -1000, -1000, 147,
	-16, 42, 543, -1000, -1000, 978, 931, -1000, -28, -1000,
	-1000, 1162, 1162, 1162, 1162, 160, 160, -1000, -1000, -1000,
	160, -1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000,
	-1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000,
	-1000, -1000, -1000, 1162, -1000, -1000, 130, -1000, -1000, -1000,
	-1000, 346, 83, 83, -1000, 226, -1000, 369, 978, -1000,
	675, -24, 1115, -1000, -1000, 243, 83, -1000, -54, -1000,
	-1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, 376, 841,
	120, -1000, -1000, 137, -1000, -1000, 44, 978, 978, 82,
	1025, 100, 28, 1162, 1162, 1162, 82, 1162, 1162, 1162,
	1162, 1162, 1162, 1162, 1162, 1162, 1162, 1162, 125, -1000,
	-1000, -1000, -1000, -1000, -1000, -1000, 22, 543, 74, 217,
	177, 543, 192, 192, -11, -11, -11, 644, 659, 794,
	-1000, 396, 49, 675, -1000, 241, 160, 399, 222, 216,
	-1000, 369, 338, 366, 42, 140, 675, 130, -1000, -1000,
	130, -1000, 370, -1000, 225, 270, -1000, -1000, 136, 332,
	159, -1000, -1000, -25, -1000, 22, 39, -1000, -1000, 95,
	-1000, -1000, -1000, 675, -1000, 1115, -1000, -1000, 100, 1162,
	1162, 1162, 675, 675, 574, -1000, 434, 449, -11, -13,
	-13, -7, -7, -7, -7, 142, 142, -1000, -1000, -1000,
	-1000, -1000, -1000, 1162, -1000, -1000, -1000, -1000, -1000, 170,
	841, -1000, 170, 45, -1000, 978, -1000, 320, 224, -1000,
	1162, -1000, -1000, 83, 338, -1000, 1162, 1162, -32, -1000,
	-1000, 372, 364, 120, 120, 120, 120, -1000, 269, 267,
	-1000, 259, 256, 306, -8, -1000, 78, -1000, -1000, 130,
	-1000, 187, 41, -1000, -1000, -1000, 177, -1000, 675, 675,
	412, 1162, 675, -1000, 170, -1000, 43, -1000, 1162, 72,
	313, 160, -1000, -1000, 333, 223, -1000, 234, 83, -1000,
	369, 978, 1162, 270, 239, 240, -1000, -1000, -1000, -1000,
	265, -1000, 260, -1000, -1000, -1000, -49, -50, -56, -1000,
	-1000, -1000, -1000, -1000, -1000, 1162, 675, -1000, -1000, 675,
	1162, 387, -1000, 1162, 1162, 1162, -1000, -1000, -1000, 338,
	42, 144, 978, 978, -1000, -1000, 160, 160, 160, 675,
	675, 83, 675, 675, -1000, 315, 42, 42, 83, 83,
	83, 97, -1000, 386, 17, 162, -1000, 162, 162, -1000,
	83, -1000, 83, -1000, -1000, 83, -1000, -1000,
}
var yyPgo = [...]int{

	0, 561, 31, 559, 558, 556, 555, 553, 552, 551,
	549, 548, 547, 390, 546, 544, 542, 529, 527, 15,
	23, 515, 512, 16, 29, 27, 511, 510, 8, 509,
	25, 508, 507, 2, 26, 1, 505, 20, 504, 477,
	18, 274, 476, 19, 14, 7, 473, 6, 12, 472,
	471, 470, 11, 469, 462, 457, 456, 448, 445, 10,
	444, 5, 443, 3, 441, 440, 439, 17, 4, 86,
	438, 314, 289, 437, 436, 432, 430, 429, 0, 22,
	426, 481, 13, 425, 412, 24, 411, 410, 405, 21,
	9,
}
var yyR1 = [...]int{

	0, 87, 1, 1, 1, 1, 1, 1, 1, 1,
	1, 1, 1, 2, 2, 2, 3, 3, 16, 16,
	4, 5, 6, 7, 7, 7, 8, 8, 8, 9,
	10, 10, 10, 11, 12, 12, 12, 88, 13, 14,
	14, 15, 15, 15, 17, 17, 18, 18, 19, 19,
	20, 20, 20, 20, 21, 21, 80, 80, 80, 79,
	79, 22, 22, 23, 23, 24, 24, 25, 25, 25,
	26, 26, 26, 26, 84, 84, 83, 83, 83, 82,
	82, 27, 27, 27, 27, 28, 28, 28, 28, 29,
	29, 31, 31, 30, 30, 32, 32, 32, 32, 33,
	33, 34, 34, 35, 35, 35, 35, 35, 35, 37,
	37, 36, 36, 36, 36, 36, 36, 36, 36, 36,
	36, 36, 36, 36, 43, 43, 43, 43, 43, 43,
	38, 38, 38, 38, 38, 38, 38, 44, 44, 44,
	48, 45, 45, 86, 86, 41, 41, 41, 41, 41,
	41, 41, 41, 41, 41, 41, 41, 41, 41, 41,
	41, 41, 41, 41, 41, 41, 41, 41, 41, 41,
	41, 41, 41, 41, 58, 58, 58, 58, 58, 58,
	58, 58, 57, 57, 57, 57, 57, 57, 57, 50,
	53, 53, 51, 51, 52, 54, 54, 49, 49, 49,
	40, 40, 40, 40, 40, 40, 40, 42, 42, 42,
	55, 55, 56, 56, 59, 59, 60, 60, 61, 62,
	62, 62, 63, 63, 63, 63, 64, 64, 64, 65,
	65, 66, 66, 67, 67, 39, 39, 46, 46, 47,
	68, 68, 69, 70, 70, 72, 72, 73, 73, 71,
	71, 74, 74, 74, 74, 74, 74, 75, 75, 76,
	76, 77, 77, 78, 81, 89, 90, 85,
}
var yyR2 = [...]int{

	0, 1, 1, 1, 1, 1, 1, 1, 1, 1,
	1, 1, 1, 12, 6, 3, 7, 7, 1, 1,
	8, 7, 3, 5, 8, 4, 6, 7, 4, 5,
	4, 5, 5, 3, 2, 2, 2, 0, 2, 0,
	2, 1, 2, 2, 0, 1, 0, 1, 1, 3,
	1, 2, 3, 5, 1, 1, 0, 1, 2, 1,
	1, 0, 2, 1, 3, 1, 1, 3, 3, 3,
	3, 5, 5, 3, 0, 1, 0, 1, 2, 1,
	1, 1, 2, 2, 1, 2, 3, 2, 3, 2,
	2, 2, 1, 1, 3, 0, 5, 5, 5, 1,
	3, 0, 2, 1, 3, 3, 2, 3, 3, 1,
	1, 1, 3, 3, 3, 4, 3, 4, 3, 4,
	5, 6, 3, 2, 1, 2, 1, 2, 1, 2,
	1, 1, 1, 1, 1, 1, 1, 3, 1, 1,
	3, 1, 3, 1, 1, 1, 1, 1, 1, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 2, 2, 2, 3, 3, 4, 5,
	3, 4, 1, 1, 1, 1, 1, 1, 1, 1,
	1, 1, 1, 1, 1, 1, 1, 1, 1, 5,
	0, 1, 1, 2, 4, 0, 2, 1, 3, 5,
	1, 1, 1, 1, 1, 1, 1, 1, 2, 2,
	0, 3, 0, 2, 0, 3, 1, 3, 2, 0,
	1, 1, 0, 2, 4, 4, 0, 2, 4, 0,
	3, 1, 3, 0, 5, 2, 1, 1, 3, 3,
	1, 3, 3, 1, 1, 0, 2, 0, 3, 0,
	1, 1, 1, 1, 1, 1, 1, 0, 1, 0,
	1, 0, 2, 1, 1, 1, 1, 0,
}
var yyChk = [...]int{

	-1000, -87, -1, -2, -3, -4, -5, -6, -7, -8,
	-9, -10, -11, -12, 6, -16, 8, 9, 29, 99,
	100, 102, 101, 103, 112, 113, 114, 7, 125, -15,
	5, -13, -88, -13, -13, -13, -13, 104, -76, 106,
	110, -71, 106, 108, 104, 104, 105, 106, 104, -85,
	-85, -85, -2, 19, 20, -17, 33, 20, -14, -71,
	-30, -81, 49, 10, -68, -69, -78, 49, -73, 109,
	105, -78, 104, -78, -81, -72, 109, 49, -72, -81,
	-18, 36, -42, -78, 52, 55, 57, -31, 25, -30,
	29, 96, -30, 47, 70, -81, 63, 49, -85, -81,
	-85, 107, -81, 22, 45, -78, -19, -20, 87, -21,
	-81, -35, -41, 49, -36, 63, -89, -40, -49, -47,
	-48, 85, 86, 92, 95, -78, -57, -58, -50, -37,
	22, 46, 51, 50, 52, 53, 54, 55, 58, 109,
	116, 118, 90, 125, 37, 115, 117, 119, 124, 123,
	122, 120, 121, 65, 59, 60, -70, 18, 10, 31,
	31, -65, 29, -89, -30, -68, -81, -34, 11, -69,
	-41, -81, -89, -85, 22, -77, 111, -74, 102, 100,
	28, 101, 14, 126, 49, -81, -81, -85, -22, 47,
	10, -80, -79, 21, -78, 51, 96, 62, 61, 77,
	-38, 80, 63, 78, 79, 64, 77, 82, 81, 91,
	85, 86, 87, 88, 89, 90, 83, 84, 94, 70,
	71, 72, 73, 74, 75, 76, -35, -41, -35, -2,
	-45, -41, 97, 98, -41, -41, -41, -41, -89, -89,
	-48, -89, -53, -41, -30, -39, 31, -2, -68, -66,
	-78, -34, -59, 14, -35, 96, -41, 45, -78, -85,
	-75, 107, -34, -20, -23, -24, -25, -26, -30, -48,
	-89, -79, 87, -81, -78, -35, -35, -43, 58, 63,
	59, 60, -37, -41, -44, -89, -48, 56, 80, 78,
	79, 64, -41, -41, -41, -43, -41, -41, -41, -41,
	-41, -41, -41, -41, -41, -41, -41, -86, 49, 51,
	-90, 48, -90, 47, -90, -40, -40, -78, -90, -19,
	20, -90, -19, -51, -52, 66, -67, 45, -46, -47,
	-89, -67, -90, 47, -59, -63, 16, 15, -81, -81,
	-81, -55, 12, 47, -27, -28, -29, 35, 39, 41,
	36, 37, 38, 42, -83, -82, 21, -81, 51, -84,
	21, -23, 96, 58, 59, 60, -45, -44, -41, -41,
	-41, 62, -41, -90, -19, -90, -54, -52, 68, -35,
	26, 47, -78, -63, -41, -60, -61, -41, 96, -85,
	-56, 13, 15, -24, -25, -24, -25, 35, 35, 35,
	40, 35, 40, 35, -28, -32, 43, 108, 44, -82,
	-81, -90, 87, -78, -90, 62, -41, -90, 69, -41,
	67, 27, -47, 47, 17, 47, -62, 23, 24, -59,
	-35, -45, 45, 45, 35, 35, 105, 105, 105, -41,
	-41, 8, -41, -41, -61, -63, -35, -35, -89, -89,
	-89, -68, -64, 18, 30, -33, -78, -33, -33, 8,
	80, -90, 47, -90, -90, -78, -78, -78,
}
var yyDef = [...]int{

	0, -2, 1, 2, 3, 4, 5, 6, 7, 8,
	9, 10, 11, 12, 37, 37, 37, 37, 37, 259,
	249, 0, 0, 0, 267, 267, 267, 18, 19, 0,
	41, 44, 39, 249, 0, 0, 0, 247, 0, 0,
	260, 0, 0, 250, 0, 245, 0, 245, 0, 34,
	35, 36, 15, 42, 43, 46, 0, 45, 38, 0,
	0, 93, 264, 0, 22, 240, 0, 263, 0, 0,
	0, 267, 0, 267, 0, 0, 0, 0, 0, 33,
	0, 47, 0, 207, 0, 0, 40, 229, 0, 92,
	0, 0, 101, 0, 0, 267, 0, 261, 25, 0,
	28, 0, 30, 246, 0, 267, 61, 48, 50, 56,
	0, 54, 55, -2, 103, 0, 0, 145, 146, 147,
	148, 0, 0, 0, 0, 197, 0, -2, 173, 111,
	0, 265, 200, 201, 202, 203, 204, 205, 206, 183,
	184, 185, 186, 187, 188, 174, 175, 176, 177, 178,
	179, 180, 181, 190, 109, 110, 0, 243, 244, 208,
	209, 0, 0, 0, 91, 101, 94, 214, 0, 241,
	242, 0, 0, 23, 248, 0, 0, 267, 257, 251,
	252, 253, 254, 255, 256, 29, 31, 32, 101, 0,
	0, 51, 57, 0, 59, 60, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 130,
	131, 132, 133, 134, 135, 136, 106, 0, 0, 0,
	0, 141, 0, 0, 163, 164, 165, 0, 0, 0,
	123, 0, 0, 191, 14, 233, 0, 236, 233, 0,
	231, 214, 222, 0, 102, 0, 141, 0, 262, 26,
	0, 258, 210, 49, 62, 63, 65, 66, 76, 74,
	0, 58, 52, 0, 198, 104, 105, 108, 124, 0,
	126, 128, 112, 113, 114, 0, 138, 139, 0, 0,
	0, 0, 116, 118, 0, 122, 149, 150, 151, 152,
	153, 154, 155, 156, 157, 158, 159, 162, 143, 144,
	107, 266, 140, 0, 239, 160, 161, 166, 167, 0,
	0, 170, 0, 195, 192, 0, 16, 0, 235, 237,
	0, 17, 230, 0, 222, 21, 0, 0, 0, 267,
	27, 212, 0, 0, 0, 0, 0, 81, 0, 0,
	84, 0, 0, 0, 95, 77, 0, 79, 80, 0,
	75, 0, 0, 125, 127, 129, 0, 115, 117, 119,
	0, 0, 142, 168, 0, 171, 0, 193, 0, 0,
	0, 0, 232, 20, 223, 215, 216, 219, 0, 24,
	214, 0, 0, 64, 70, 0, 73, 82, 83, 85,
	0, 87, 0, 89, 90, 67, 0, 0, 0, 78,
	68, 69, 53, 199, 137, 0, 120, 169, 189, 196,
	0, 0, 238, 0, 0, 0, 218, 220, 221, 222,
	213, 211, 0, 0, 86, 88, 0, 0, 0, 121,
	194, 0, 224, 225, 217, 226, 71, 72, 0, 0,
	0, 234, 13, 0, 0, 0, 99, 0, 0, 227,
	0, 96, 0, 97, 98, 0, 100, 228,
}
var yyTok1 = [...]int{

	1, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 89, 82, 3,
	46, 48, 87, 85, 47, 86, 96, 88, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	71, 70, 72, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 91, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 81, 3, 92,
}
var yyTok2 = [...]int{

	2, 3, 4, 5, 6, 7, 8, 9, 10, 11,
	12, 13, 14, 15, 16, 17, 18, 19, 20, 21,
	22, 23, 24, 25, 26, 27, 28, 29, 30, 31,
	32, 33, 34, 35, 36, 37, 38, 39, 40, 41,
	42, 43, 44, 45, 49, 50, 51, 52, 53, 54,
	55, 56, 57, 58, 59, 60, 61, 62, 63, 64,
	65, 66, 67, 68, 69, 73, 74, 75, 76, 77,
	78, 79, 80, 83, 84, 90, 93, 94, 95, 97,
	98, 99, 100, 101, 102, 103, 104, 105, 106, 107,
	108, 109, 110, 111, 112, 113, 114, 115, 116, 117,
	118, 119, 120, 121, 122, 123, 124, 125, 126,
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
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:186
		{
			setParseTree(yylex, yyDollar[1].statement)
		}
	case 2:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:192
		{
			yyVAL.statement = yyDollar[1].selStmt
		}
	case 13:
		yyDollar = yyS[yypt-12 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:208
		{
			yyVAL.selStmt = &Select{Comments: Comments(yyDollar[2].bytes2), Distinct: yyDollar[3].str, Hints: yyDollar[4].str, SelectExprs: yyDollar[5].selectExprs, From: yyDollar[6].tableExprs, Where: NewWhere(WhereStr, yyDollar[7].boolExpr), GroupBy: GroupBy(yyDollar[8].valExprs), Having: NewWhere(HavingStr, yyDollar[9].boolExpr), OrderBy: yyDollar[10].orderBy, Limit: yyDollar[11].limit, Lock: yyDollar[12].str}
		}
	case 14:
		yyDollar = yyS[yypt-6 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:212
		{
			yyVAL.selStmt = &Select{Comments: Comments(yyDollar[2].bytes2), SelectExprs: SelectExprs{Nextval{Expr: yyDollar[4].valExpr}}, From: TableExprs{&AliasedTableExpr{Expr: yyDollar[6].tableName}}}
		}
	case 15:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:216
		{
			yyVAL.selStmt = &Union{Type: yyDollar[2].str, Left: yyDollar[1].selStmt, Right: yyDollar[3].selStmt}
		}
	case 16:
		yyDollar = yyS[yypt-7 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:222
		{
			yyVAL.statement = &Insert{Action: yyDollar[1].str, Comments: Comments(yyDollar[2].bytes2), Ignore: yyDollar[3].str, Table: yyDollar[4].tableName, Columns: yyDollar[5].columns, Rows: yyDollar[6].insRows, OnDup: OnDup(yyDollar[7].updateExprs)}
		}
	case 17:
		yyDollar = yyS[yypt-7 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:226
		{
			cols := make(Columns, 0, len(yyDollar[6].updateExprs))
			vals := make(ValTuple, 0, len(yyDollar[7].updateExprs))
			for _, updateList := range yyDollar[6].updateExprs {
				cols = append(cols, updateList.Name)
				vals = append(vals, updateList.Expr)
			}
			yyVAL.statement = &Insert{Action: yyDollar[1].str, Comments: Comments(yyDollar[2].bytes2), Ignore: yyDollar[3].str, Table: yyDollar[4].tableName, Columns: cols, Rows: Values{vals}, OnDup: OnDup(yyDollar[7].updateExprs)}
		}
	case 18:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:238
		{
			yyVAL.str = InsertStr
		}
	case 19:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:242
		{
			yyVAL.str = ReplaceStr
		}
	case 20:
		yyDollar = yyS[yypt-8 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:248
		{
			yyVAL.statement = &Update{Comments: Comments(yyDollar[2].bytes2), Table: yyDollar[3].tableName, Exprs: yyDollar[5].updateExprs, Where: NewWhere(WhereStr, yyDollar[6].boolExpr), OrderBy: yyDollar[7].orderBy, Limit: yyDollar[8].limit}
		}
	case 21:
		yyDollar = yyS[yypt-7 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:254
		{
			yyVAL.statement = &Delete{Comments: Comments(yyDollar[2].bytes2), Table: yyDollar[4].tableName, Where: NewWhere(WhereStr, yyDollar[5].boolExpr), OrderBy: yyDollar[6].orderBy, Limit: yyDollar[7].limit}
		}
	case 22:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:260
		{
			yyVAL.statement = &Set{Comments: Comments(yyDollar[2].bytes2), Exprs: yyDollar[3].updateExprs}
		}
	case 23:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:266
		{
			yyVAL.statement = &DDL{Action: CreateStr, NewName: yyDollar[4].tableIdent}
		}
	case 24:
		yyDollar = yyS[yypt-8 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:270
		{
			// Change this to an alter statement
			yyVAL.statement = &DDL{Action: AlterStr, Table: yyDollar[7].tableIdent, NewName: yyDollar[7].tableIdent}
		}
	case 25:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:275
		{
			yyVAL.statement = &DDL{Action: CreateStr, NewName: NewTableIdent(yyDollar[3].colIdent.Lowered())}
		}
	case 26:
		yyDollar = yyS[yypt-6 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:281
		{
			yyVAL.statement = &DDL{Action: AlterStr, Table: yyDollar[4].tableIdent, NewName: yyDollar[4].tableIdent}
		}
	case 27:
		yyDollar = yyS[yypt-7 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:285
		{
			// Change this to a rename statement
			yyVAL.statement = &DDL{Action: RenameStr, Table: yyDollar[4].tableIdent, NewName: yyDollar[7].tableIdent}
		}
	case 28:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:290
		{
			yyVAL.statement = &DDL{Action: AlterStr, Table: NewTableIdent(yyDollar[3].colIdent.Lowered()), NewName: NewTableIdent(yyDollar[3].colIdent.Lowered())}
		}
	case 29:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:296
		{
			yyVAL.statement = &DDL{Action: RenameStr, Table: yyDollar[3].tableIdent, NewName: yyDollar[5].tableIdent}
		}
	case 30:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:302
		{
			var exists bool
			if yyDollar[3].byt != 0 {
				exists = true
			}
			yyVAL.statement = &DDL{Action: DropStr, Table: yyDollar[4].tableIdent, IfExists: exists}
		}
	case 31:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:310
		{
			// Change this to an alter statement
			yyVAL.statement = &DDL{Action: AlterStr, Table: yyDollar[5].tableIdent, NewName: yyDollar[5].tableIdent}
		}
	case 32:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:315
		{
			var exists bool
			if yyDollar[3].byt != 0 {
				exists = true
			}
			yyVAL.statement = &DDL{Action: DropStr, Table: NewTableIdent(yyDollar[4].colIdent.Lowered()), IfExists: exists}
		}
	case 33:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:325
		{
			yyVAL.statement = &DDL{Action: AlterStr, Table: yyDollar[3].tableIdent, NewName: yyDollar[3].tableIdent}
		}
	case 34:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:331
		{
			yyVAL.statement = &Other{}
		}
	case 35:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:335
		{
			yyVAL.statement = &Other{}
		}
	case 36:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:339
		{
			yyVAL.statement = &Other{}
		}
	case 37:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:344
		{
			setAllowComments(yylex, true)
		}
	case 38:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:348
		{
			yyVAL.bytes2 = yyDollar[2].bytes2
			setAllowComments(yylex, false)
		}
	case 39:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:354
		{
			yyVAL.bytes2 = nil
		}
	case 40:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:358
		{
			yyVAL.bytes2 = append(yyDollar[1].bytes2, yyDollar[2].bytes)
		}
	case 41:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:364
		{
			yyVAL.str = UnionStr
		}
	case 42:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:368
		{
			yyVAL.str = UnionAllStr
		}
	case 43:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:372
		{
			yyVAL.str = UnionDistinctStr
		}
	case 44:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:377
		{
			yyVAL.str = ""
		}
	case 45:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:381
		{
			yyVAL.str = DistinctStr
		}
	case 46:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:386
		{
			yyVAL.str = ""
		}
	case 47:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:390
		{
			yyVAL.str = StraightJoinHint
		}
	case 48:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:396
		{
			yyVAL.selectExprs = SelectExprs{yyDollar[1].selectExpr}
		}
	case 49:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:400
		{
			yyVAL.selectExprs = append(yyVAL.selectExprs, yyDollar[3].selectExpr)
		}
	case 50:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:406
		{
			yyVAL.selectExpr = &StarExpr{}
		}
	case 51:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:410
		{
			yyVAL.selectExpr = &NonStarExpr{Expr: yyDollar[1].expr, As: yyDollar[2].colIdent}
		}
	case 52:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:414
		{
			yyVAL.selectExpr = &StarExpr{TableName: &TableName{Name: yyDollar[1].tableIdent}}
		}
	case 53:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:418
		{
			yyVAL.selectExpr = &StarExpr{TableName: &TableName{Qualifier: yyDollar[1].tableIdent, Name: yyDollar[3].tableIdent}}
		}
	case 54:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:424
		{
			yyVAL.expr = yyDollar[1].boolExpr
		}
	case 55:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:428
		{
			yyVAL.expr = yyDollar[1].valExpr
		}
	case 56:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:433
		{
			yyVAL.colIdent = ColIdent{}
		}
	case 57:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:437
		{
			yyVAL.colIdent = yyDollar[1].colIdent
		}
	case 58:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:441
		{
			yyVAL.colIdent = yyDollar[2].colIdent
		}
	case 60:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:448
		{
			yyVAL.colIdent = NewColIdent(string(yyDollar[1].bytes))
		}
	case 61:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:453
		{
			yyVAL.tableExprs = TableExprs{&AliasedTableExpr{Expr: &TableName{Name: NewTableIdent("dual")}}}
		}
	case 62:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:457
		{
			yyVAL.tableExprs = yyDollar[2].tableExprs
		}
	case 63:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:463
		{
			yyVAL.tableExprs = TableExprs{yyDollar[1].tableExpr}
		}
	case 64:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:467
		{
			yyVAL.tableExprs = append(yyVAL.tableExprs, yyDollar[3].tableExpr)
		}
	case 67:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:477
		{
			yyVAL.tableExpr = &AliasedTableExpr{Expr: yyDollar[1].tableName, As: yyDollar[2].tableIdent, Hints: yyDollar[3].indexHints}
		}
	case 68:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:481
		{
			yyVAL.tableExpr = &AliasedTableExpr{Expr: yyDollar[1].subquery, As: yyDollar[3].tableIdent}
		}
	case 69:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:485
		{
			yyVAL.tableExpr = &ParenTableExpr{Exprs: yyDollar[2].tableExprs}
		}
	case 70:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:498
		{
			yyVAL.tableExpr = &JoinTableExpr{LeftExpr: yyDollar[1].tableExpr, Join: yyDollar[2].str, RightExpr: yyDollar[3].tableExpr}
		}
	case 71:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:502
		{
			yyVAL.tableExpr = &JoinTableExpr{LeftExpr: yyDollar[1].tableExpr, Join: yyDollar[2].str, RightExpr: yyDollar[3].tableExpr, On: yyDollar[5].boolExpr}
		}
	case 72:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:506
		{
			yyVAL.tableExpr = &JoinTableExpr{LeftExpr: yyDollar[1].tableExpr, Join: yyDollar[2].str, RightExpr: yyDollar[3].tableExpr, On: yyDollar[5].boolExpr}
		}
	case 73:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:510
		{
			yyVAL.tableExpr = &JoinTableExpr{LeftExpr: yyDollar[1].tableExpr, Join: yyDollar[2].str, RightExpr: yyDollar[3].tableExpr}
		}
	case 74:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:515
		{
			yyVAL.empty = struct{}{}
		}
	case 75:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:517
		{
			yyVAL.empty = struct{}{}
		}
	case 76:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:520
		{
			yyVAL.tableIdent = NewTableIdent("")
		}
	case 77:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:524
		{
			yyVAL.tableIdent = yyDollar[1].tableIdent
		}
	case 78:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:528
		{
			yyVAL.tableIdent = yyDollar[2].tableIdent
		}
	case 80:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:535
		{
			yyVAL.tableIdent = NewTableIdent(string(yyDollar[1].bytes))
		}
	case 81:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:541
		{
			yyVAL.str = JoinStr
		}
	case 82:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:545
		{
			yyVAL.str = JoinStr
		}
	case 83:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:549
		{
			yyVAL.str = JoinStr
		}
	case 84:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:553
		{
			yyVAL.str = StraightJoinStr
		}
	case 85:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:559
		{
			yyVAL.str = LeftJoinStr
		}
	case 86:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:563
		{
			yyVAL.str = LeftJoinStr
		}
	case 87:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:567
		{
			yyVAL.str = RightJoinStr
		}
	case 88:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:571
		{
			yyVAL.str = RightJoinStr
		}
	case 89:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:577
		{
			yyVAL.str = NaturalJoinStr
		}
	case 90:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:581
		{
			if yyDollar[2].str == LeftJoinStr {
				yyVAL.str = NaturalLeftJoinStr
			} else {
				yyVAL.str = NaturalRightJoinStr
			}
		}
	case 91:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:591
		{
			yyVAL.tableName = yyDollar[2].tableName
		}
	case 92:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:595
		{
			yyVAL.tableName = yyDollar[1].tableName
		}
	case 93:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:601
		{
			yyVAL.tableName = &TableName{Name: yyDollar[1].tableIdent}
		}
	case 94:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:605
		{
			yyVAL.tableName = &TableName{Qualifier: yyDollar[1].tableIdent, Name: yyDollar[3].tableIdent}
		}
	case 95:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:610
		{
			yyVAL.indexHints = nil
		}
	case 96:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:614
		{
			yyVAL.indexHints = &IndexHints{Type: UseStr, Indexes: yyDollar[4].colIdents}
		}
	case 97:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:618
		{
			yyVAL.indexHints = &IndexHints{Type: IgnoreStr, Indexes: yyDollar[4].colIdents}
		}
	case 98:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:622
		{
			yyVAL.indexHints = &IndexHints{Type: ForceStr, Indexes: yyDollar[4].colIdents}
		}
	case 99:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:628
		{
			yyVAL.colIdents = []ColIdent{yyDollar[1].colIdent}
		}
	case 100:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:632
		{
			yyVAL.colIdents = append(yyDollar[1].colIdents, yyDollar[3].colIdent)
		}
	case 101:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:637
		{
			yyVAL.boolExpr = nil
		}
	case 102:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:641
		{
			yyVAL.boolExpr = yyDollar[2].boolExpr
		}
	case 104:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:648
		{
			yyVAL.boolExpr = &AndExpr{Left: yyDollar[1].boolExpr, Right: yyDollar[3].boolExpr}
		}
	case 105:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:652
		{
			yyVAL.boolExpr = &OrExpr{Left: yyDollar[1].boolExpr, Right: yyDollar[3].boolExpr}
		}
	case 106:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:656
		{
			yyVAL.boolExpr = &NotExpr{Expr: yyDollar[2].boolExpr}
		}
	case 107:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:660
		{
			yyVAL.boolExpr = &ParenBoolExpr{Expr: yyDollar[2].boolExpr}
		}
	case 108:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:664
		{
			yyVAL.boolExpr = &IsExpr{Operator: yyDollar[3].str, Expr: yyDollar[1].boolExpr}
		}
	case 109:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:670
		{
			yyVAL.boolVal = BoolVal(true)
		}
	case 110:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:674
		{
			yyVAL.boolVal = BoolVal(false)
		}
	case 111:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:680
		{
			yyVAL.boolExpr = yyDollar[1].boolVal
		}
	case 112:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:684
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: yyDollar[2].str, Right: yyDollar[3].boolVal}
		}
	case 113:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:688
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: yyDollar[2].str, Right: yyDollar[3].valExpr}
		}
	case 114:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:692
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: InStr, Right: yyDollar[3].colTuple}
		}
	case 115:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:696
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: NotInStr, Right: yyDollar[4].colTuple}
		}
	case 116:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:700
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: LikeStr, Right: yyDollar[3].valExpr}
		}
	case 117:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:704
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: NotLikeStr, Right: yyDollar[4].valExpr}
		}
	case 118:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:708
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: RegexpStr, Right: yyDollar[3].valExpr}
		}
	case 119:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:712
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: NotRegexpStr, Right: yyDollar[4].valExpr}
		}
	case 120:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:716
		{
			yyVAL.boolExpr = &RangeCond{Left: yyDollar[1].valExpr, Operator: BetweenStr, From: yyDollar[3].valExpr, To: yyDollar[5].valExpr}
		}
	case 121:
		yyDollar = yyS[yypt-6 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:720
		{
			yyVAL.boolExpr = &RangeCond{Left: yyDollar[1].valExpr, Operator: NotBetweenStr, From: yyDollar[4].valExpr, To: yyDollar[6].valExpr}
		}
	case 122:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:724
		{
			yyVAL.boolExpr = &IsExpr{Operator: yyDollar[3].str, Expr: yyDollar[1].valExpr}
		}
	case 123:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:728
		{
			yyVAL.boolExpr = &ExistsExpr{Subquery: yyDollar[2].subquery}
		}
	case 124:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:734
		{
			yyVAL.str = IsNullStr
		}
	case 125:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:738
		{
			yyVAL.str = IsNotNullStr
		}
	case 126:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:742
		{
			yyVAL.str = IsTrueStr
		}
	case 127:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:746
		{
			yyVAL.str = IsNotTrueStr
		}
	case 128:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:750
		{
			yyVAL.str = IsFalseStr
		}
	case 129:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:754
		{
			yyVAL.str = IsNotFalseStr
		}
	case 130:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:760
		{
			yyVAL.str = EqualStr
		}
	case 131:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:764
		{
			yyVAL.str = LessThanStr
		}
	case 132:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:768
		{
			yyVAL.str = GreaterThanStr
		}
	case 133:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:772
		{
			yyVAL.str = LessEqualStr
		}
	case 134:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:776
		{
			yyVAL.str = GreaterEqualStr
		}
	case 135:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:780
		{
			yyVAL.str = NotEqualStr
		}
	case 136:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:784
		{
			yyVAL.str = NullSafeEqualStr
		}
	case 137:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:790
		{
			yyVAL.colTuple = ValTuple(yyDollar[2].valExprs)
		}
	case 138:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:794
		{
			yyVAL.colTuple = yyDollar[1].subquery
		}
	case 139:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:798
		{
			yyVAL.colTuple = ListArg(yyDollar[1].bytes)
		}
	case 140:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:804
		{
			yyVAL.subquery = &Subquery{yyDollar[2].selStmt}
		}
	case 141:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:810
		{
			yyVAL.valExprs = ValExprs{yyDollar[1].valExpr}
		}
	case 142:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:814
		{
			yyVAL.valExprs = append(yyDollar[1].valExprs, yyDollar[3].valExpr)
		}
	case 143:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:820
		{
			yyVAL.str = string(yyDollar[1].bytes)
		}
	case 144:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:824
		{
			yyVAL.str = string(yyDollar[1].bytes)
		}
	case 145:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:830
		{
			yyVAL.valExpr = yyDollar[1].valExpr
		}
	case 146:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:834
		{
			yyVAL.valExpr = yyDollar[1].colName
		}
	case 147:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:838
		{
			yyVAL.valExpr = yyDollar[1].valTuple
		}
	case 148:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:842
		{
			yyVAL.valExpr = yyDollar[1].subquery
		}
	case 149:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:846
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: BitAndStr, Right: yyDollar[3].valExpr}
		}
	case 150:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:850
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: BitOrStr, Right: yyDollar[3].valExpr}
		}
	case 151:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:854
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: BitXorStr, Right: yyDollar[3].valExpr}
		}
	case 152:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:858
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: PlusStr, Right: yyDollar[3].valExpr}
		}
	case 153:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:862
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: MinusStr, Right: yyDollar[3].valExpr}
		}
	case 154:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:866
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: MultStr, Right: yyDollar[3].valExpr}
		}
	case 155:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:870
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: DivStr, Right: yyDollar[3].valExpr}
		}
	case 156:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:874
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: ModStr, Right: yyDollar[3].valExpr}
		}
	case 157:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:878
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: ModStr, Right: yyDollar[3].valExpr}
		}
	case 158:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:882
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: ShiftLeftStr, Right: yyDollar[3].valExpr}
		}
	case 159:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:886
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: ShiftRightStr, Right: yyDollar[3].valExpr}
		}
	case 160:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:890
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].colName, Operator: JSONExtractOp, Right: yyDollar[3].valExpr}
		}
	case 161:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:894
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].colName, Operator: JSONUnquoteExtractOp, Right: yyDollar[3].valExpr}
		}
	case 162:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:898
		{
			yyVAL.valExpr = &CollateExpr{Expr: yyDollar[1].valExpr, Charset: yyDollar[3].str}
		}
	case 163:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:902
		{
			if num, ok := yyDollar[2].valExpr.(*SQLVal); ok && num.Type == IntVal {
				yyVAL.valExpr = num
			} else {
				yyVAL.valExpr = &UnaryExpr{Operator: UPlusStr, Expr: yyDollar[2].valExpr}
			}
		}
	case 164:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:910
		{
			if num, ok := yyDollar[2].valExpr.(*SQLVal); ok && num.Type == IntVal {
				// Handle double negative
				if num.Val[0] == '-' {
					num.Val = num.Val[1:]
					yyVAL.valExpr = num
				} else {
					yyVAL.valExpr = NewIntVal(append([]byte("-"), num.Val...))
				}
			} else {
				yyVAL.valExpr = &UnaryExpr{Operator: UMinusStr, Expr: yyDollar[2].valExpr}
			}
		}
	case 165:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:924
		{
			yyVAL.valExpr = &UnaryExpr{Operator: TildaStr, Expr: yyDollar[2].valExpr}
		}
	case 166:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:928
		{
			// This rule prevents the usage of INTERVAL
			// as a function. If support is needed for that,
			// we'll need to revisit this. The solution
			// will be non-trivial because of grammar conflicts.
			yyVAL.valExpr = &IntervalExpr{Expr: yyDollar[2].valExpr, Unit: yyDollar[3].colIdent}
		}
	case 167:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:936
		{
			yyVAL.valExpr = &FuncExpr{Name: yyDollar[1].colIdent}
		}
	case 168:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:940
		{
			yyVAL.valExpr = &FuncExpr{Name: yyDollar[1].colIdent, Exprs: yyDollar[3].selectExprs}
		}
	case 169:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:944
		{
			yyVAL.valExpr = &FuncExpr{Name: yyDollar[1].colIdent, Distinct: true, Exprs: yyDollar[4].selectExprs}
		}
	case 170:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:948
		{
			yyVAL.valExpr = &FuncExpr{Name: yyDollar[1].colIdent}
		}
	case 171:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:952
		{
			yyVAL.valExpr = &FuncExpr{Name: yyDollar[1].colIdent, Exprs: yyDollar[3].selectExprs}
		}
	case 172:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:956
		{
			yyVAL.valExpr = &FuncExpr{Name: yyDollar[1].colIdent}
		}
	case 173:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:960
		{
			yyVAL.valExpr = yyDollar[1].caseExpr
		}
	case 174:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:968
		{
			yyVAL.colIdent = NewColIdent("current_timestamp")
		}
	case 175:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:972
		{
			yyVAL.colIdent = NewColIdent("current_date")
		}
	case 176:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:976
		{
			yyVAL.colIdent = NewColIdent("current_time")
		}
	case 177:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:980
		{
			yyVAL.colIdent = NewColIdent("utc_timestamp")
		}
	case 178:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:984
		{
			yyVAL.colIdent = NewColIdent("utc_time")
		}
	case 179:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:988
		{
			yyVAL.colIdent = NewColIdent("utc_date")
		}
	case 180:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:992
		{
			yyVAL.colIdent = NewColIdent("localtime")
		}
	case 181:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:996
		{
			yyVAL.colIdent = NewColIdent("localtimestamp")
		}
	case 183:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1005
		{
			yyVAL.colIdent = NewColIdent("if")
		}
	case 184:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1009
		{
			yyVAL.colIdent = NewColIdent("database")
		}
	case 185:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1013
		{
			yyVAL.colIdent = NewColIdent("unix_timestamp")
		}
	case 186:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1017
		{
			yyVAL.colIdent = NewColIdent("mod")
		}
	case 187:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1021
		{
			yyVAL.colIdent = NewColIdent("replace")
		}
	case 188:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1025
		{
			yyVAL.colIdent = NewColIdent("left")
		}
	case 189:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1031
		{
			yyVAL.caseExpr = &CaseExpr{Expr: yyDollar[2].valExpr, Whens: yyDollar[3].whens, Else: yyDollar[4].valExpr}
		}
	case 190:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1036
		{
			yyVAL.valExpr = nil
		}
	case 191:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1040
		{
			yyVAL.valExpr = yyDollar[1].valExpr
		}
	case 192:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1046
		{
			yyVAL.whens = []*When{yyDollar[1].when}
		}
	case 193:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1050
		{
			yyVAL.whens = append(yyDollar[1].whens, yyDollar[2].when)
		}
	case 194:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1056
		{
			yyVAL.when = &When{Cond: yyDollar[2].boolExpr, Val: yyDollar[4].valExpr}
		}
	case 195:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1061
		{
			yyVAL.valExpr = nil
		}
	case 196:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1065
		{
			yyVAL.valExpr = yyDollar[2].valExpr
		}
	case 197:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1071
		{
			yyVAL.colName = &ColName{Name: yyDollar[1].colIdent}
		}
	case 198:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1075
		{
			yyVAL.colName = &ColName{Qualifier: &TableName{Name: yyDollar[1].tableIdent}, Name: yyDollar[3].colIdent}
		}
	case 199:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1079
		{
			yyVAL.colName = &ColName{Qualifier: &TableName{Qualifier: yyDollar[1].tableIdent, Name: yyDollar[3].tableIdent}, Name: yyDollar[5].colIdent}
		}
	case 200:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1085
		{
			yyVAL.valExpr = NewStrVal(yyDollar[1].bytes)
		}
	case 201:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1089
		{
			yyVAL.valExpr = NewHexVal(yyDollar[1].bytes)
		}
	case 202:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1093
		{
			yyVAL.valExpr = NewIntVal(yyDollar[1].bytes)
		}
	case 203:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1097
		{
			yyVAL.valExpr = NewFloatVal(yyDollar[1].bytes)
		}
	case 204:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1101
		{
			yyVAL.valExpr = NewHexNum(yyDollar[1].bytes)
		}
	case 205:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1105
		{
			yyVAL.valExpr = NewValArg(yyDollar[1].bytes)
		}
	case 206:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1109
		{
			yyVAL.valExpr = &NullVal{}
		}
	case 207:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1115
		{
			// TODO(sougou): Deprecate this construct.
			if yyDollar[1].colIdent.Lowered() != "value" {
				yylex.Error("expecting value after next")
				return 1
			}
			yyVAL.valExpr = NewIntVal([]byte("1"))
		}
	case 208:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1124
		{
			yyVAL.valExpr = NewIntVal(yyDollar[1].bytes)
		}
	case 209:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1128
		{
			yyVAL.valExpr = NewValArg(yyDollar[1].bytes)
		}
	case 210:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1133
		{
			yyVAL.valExprs = nil
		}
	case 211:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1137
		{
			yyVAL.valExprs = yyDollar[3].valExprs
		}
	case 212:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1142
		{
			yyVAL.boolExpr = nil
		}
	case 213:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1146
		{
			yyVAL.boolExpr = yyDollar[2].boolExpr
		}
	case 214:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1151
		{
			yyVAL.orderBy = nil
		}
	case 215:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1155
		{
			yyVAL.orderBy = yyDollar[3].orderBy
		}
	case 216:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1161
		{
			yyVAL.orderBy = OrderBy{yyDollar[1].order}
		}
	case 217:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1165
		{
			yyVAL.orderBy = append(yyDollar[1].orderBy, yyDollar[3].order)
		}
	case 218:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1171
		{
			yyVAL.order = &Order{Expr: yyDollar[1].valExpr, Direction: yyDollar[2].str}
		}
	case 219:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1176
		{
			yyVAL.str = AscScr
		}
	case 220:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1180
		{
			yyVAL.str = AscScr
		}
	case 221:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1184
		{
			yyVAL.str = DescScr
		}
	case 222:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1189
		{
			yyVAL.limit = nil
		}
	case 223:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1193
		{
			yyVAL.limit = &Limit{Rowcount: yyDollar[2].valExpr}
		}
	case 224:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1197
		{
			yyVAL.limit = &Limit{Offset: yyDollar[2].valExpr, Rowcount: yyDollar[4].valExpr}
		}
	case 225:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1201
		{
			yyVAL.limit = &Limit{Offset: yyDollar[4].valExpr, Rowcount: yyDollar[2].valExpr}
		}
	case 226:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1206
		{
			yyVAL.str = ""
		}
	case 227:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1210
		{
			yyVAL.str = ForUpdateStr
		}
	case 228:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1214
		{
			if yyDollar[3].colIdent.Lowered() != "share" {
				yylex.Error("expecting share")
				return 1
			}
			if yyDollar[4].colIdent.Lowered() != "mode" {
				yylex.Error("expecting mode")
				return 1
			}
			yyVAL.str = ShareModeStr
		}
	case 229:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1227
		{
			yyVAL.columns = nil
		}
	case 230:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1231
		{
			yyVAL.columns = yyDollar[2].columns
		}
	case 231:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1237
		{
			yyVAL.columns = Columns{yyDollar[1].colIdent}
		}
	case 232:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1241
		{
			yyVAL.columns = append(yyVAL.columns, yyDollar[3].colIdent)
		}
	case 233:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1246
		{
			yyVAL.updateExprs = nil
		}
	case 234:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1250
		{
			yyVAL.updateExprs = yyDollar[5].updateExprs
		}
	case 235:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1256
		{
			yyVAL.insRows = yyDollar[2].values
		}
	case 236:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1260
		{
			yyVAL.insRows = yyDollar[1].selStmt
		}
	case 237:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1266
		{
			yyVAL.values = Values{yyDollar[1].valTuple}
		}
	case 238:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1270
		{
			yyVAL.values = append(yyDollar[1].values, yyDollar[3].valTuple)
		}
	case 239:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1276
		{
			yyVAL.valTuple = ValTuple(yyDollar[2].valExprs)
		}
	case 240:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1282
		{
			yyVAL.updateExprs = UpdateExprs{yyDollar[1].updateExpr}
		}
	case 241:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1286
		{
			yyVAL.updateExprs = append(yyDollar[1].updateExprs, yyDollar[3].updateExpr)
		}
	case 242:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1292
		{
			yyVAL.updateExpr = &UpdateExpr{Name: yyDollar[1].colIdent, Expr: yyDollar[3].valExpr}
		}
	case 245:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1301
		{
			yyVAL.byt = 0
		}
	case 246:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1303
		{
			yyVAL.byt = 1
		}
	case 247:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1306
		{
			yyVAL.empty = struct{}{}
		}
	case 248:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1308
		{
			yyVAL.empty = struct{}{}
		}
	case 249:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1311
		{
			yyVAL.str = ""
		}
	case 250:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1313
		{
			yyVAL.str = IgnoreStr
		}
	case 251:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1317
		{
			yyVAL.empty = struct{}{}
		}
	case 252:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1319
		{
			yyVAL.empty = struct{}{}
		}
	case 253:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1321
		{
			yyVAL.empty = struct{}{}
		}
	case 254:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1323
		{
			yyVAL.empty = struct{}{}
		}
	case 255:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1325
		{
			yyVAL.empty = struct{}{}
		}
	case 256:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1327
		{
			yyVAL.empty = struct{}{}
		}
	case 257:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1330
		{
			yyVAL.empty = struct{}{}
		}
	case 258:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1332
		{
			yyVAL.empty = struct{}{}
		}
	case 259:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1335
		{
			yyVAL.empty = struct{}{}
		}
	case 260:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1337
		{
			yyVAL.empty = struct{}{}
		}
	case 261:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1340
		{
			yyVAL.empty = struct{}{}
		}
	case 262:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1342
		{
			yyVAL.empty = struct{}{}
		}
	case 263:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1346
		{
			yyVAL.colIdent = NewColIdent(string(yyDollar[1].bytes))
		}
	case 264:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1352
		{
			yyVAL.tableIdent = NewTableIdent(string(yyDollar[1].bytes))
		}
	case 265:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1358
		{
			if incNesting(yylex) {
				yylex.Error("max nesting level reached")
				return 1
			}
		}
	case 266:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1367
		{
			decNesting(yylex)
		}
	case 267:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1372
		{
			forceEOF(yylex)
		}
	}
	goto yystack /* stack new state and value */
}
