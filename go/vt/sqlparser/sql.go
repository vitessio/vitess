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
	-1, 112,
	96, 260,
	-2, 259,
	-1, 125,
	46, 178,
	-2, 167,
}

const yyNprod = 264
const yyPrivate = 57344

var yyTokenNames []string
var yyStates []string

const yyLast = 1129

var yyAct = [...]int{

	123, 137, 449, 328, 62, 380, 235, 348, 338, 306,
	117, 260, 247, 47, 104, 317, 319, 258, 113, 259,
	192, 278, 115, 234, 3, 35, 165, 37, 105, 173,
	41, 38, 179, 74, 67, 64, 262, 255, 69, 48,
	49, 71, 99, 432, 400, 402, 177, 40, 431, 41,
	430, 50, 43, 44, 45, 81, 68, 70, 46, 42,
	225, 226, 382, 358, 250, 107, 196, 181, 89, 58,
	207, 208, 209, 210, 211, 212, 206, 103, 206, 215,
	215, 215, 454, 96, 65, 98, 63, 161, 111, 64,
	112, 191, 64, 163, 87, 92, 285, 412, 90, 209,
	210, 211, 212, 206, 318, 94, 215, 170, 194, 401,
	283, 284, 282, 421, 422, 418, 224, 184, 176, 178,
	175, 84, 406, 162, 153, 231, 232, 60, 273, 233,
	14, 15, 16, 17, 204, 213, 214, 207, 208, 209,
	210, 211, 212, 206, 180, 417, 215, 309, 190, 189,
	65, 190, 189, 18, 414, 190, 189, 189, 168, 190,
	189, 64, 245, 153, 191, 243, 118, 191, 318, 86,
	372, 191, 191, 281, 252, 191, 197, 349, 167, 268,
	270, 271, 60, 242, 269, 355, 356, 357, 253, 264,
	246, 239, 65, 60, 194, 82, 112, 275, 83, 95,
	60, 279, 351, 236, 301, 60, 302, 351, 227, 228,
	229, 230, 256, 75, 272, 257, 187, 276, 238, 65,
	28, 195, 14, 19, 20, 22, 21, 23, 456, 309,
	91, 305, 249, 153, 312, 65, 24, 25, 26, 186,
	309, 314, 310, 323, 313, 315, 307, 311, 303, 304,
	419, 275, 322, 186, 325, 265, 266, 314, 309, 327,
	324, 375, 153, 309, 336, 60, 264, 205, 204, 213,
	214, 207, 208, 209, 210, 211, 212, 206, 336, 309,
	215, 223, 354, 320, 279, 91, 359, 277, 326, 309,
	286, 287, 288, 289, 290, 291, 292, 293, 294, 295,
	296, 297, 298, 299, 360, 205, 204, 213, 214, 207,
	208, 209, 210, 211, 212, 206, 166, 365, 215, 160,
	169, 367, 368, 366, 73, 426, 320, 376, 251, 102,
	395, 377, 371, 79, 429, 396, 153, 393, 264, 264,
	264, 264, 394, 428, 392, 236, 383, 391, 158, 388,
	157, 390, 91, 59, 263, 398, 387, 403, 389, 407,
	14, 88, 55, 72, 405, 415, 280, 77, 171, 408,
	76, 374, 361, 362, 363, 54, 411, 323, 59, 101,
	369, 353, 59, 407, 373, 241, 416, 93, 329, 236,
	156, 97, 39, 425, 100, 378, 381, 423, 155, 108,
	131, 130, 132, 133, 134, 135, 248, 59, 136, 447,
	164, 213, 214, 207, 208, 209, 210, 211, 212, 206,
	182, 448, 215, 183, 57, 438, 193, 439, 51, 52,
	386, 263, 442, 443, 444, 330, 64, 385, 413, 397,
	445, 344, 345, 450, 450, 450, 451, 452, 335, 280,
	166, 424, 236, 410, 65, 459, 195, 460, 453, 455,
	461, 457, 458, 61, 435, 14, 190, 189, 28, 30,
	340, 343, 344, 345, 341, 59, 342, 346, 29, 1,
	434, 300, 191, 436, 437, 381, 352, 347, 188, 172,
	36, 254, 440, 441, 31, 32, 33, 34, 433, 174,
	66, 154, 244, 263, 263, 263, 263, 108, 59, 159,
	446, 200, 203, 420, 379, 125, 124, 274, 216, 217,
	218, 219, 220, 221, 222, 384, 201, 202, 199, 205,
	204, 213, 214, 207, 208, 209, 210, 211, 212, 206,
	334, 409, 215, 370, 340, 343, 344, 345, 341, 237,
	342, 346, 108, 108, 427, 316, 126, 116, 321, 267,
	205, 204, 213, 214, 207, 208, 209, 210, 211, 212,
	206, 331, 332, 215, 80, 333, 240, 198, 109, 308,
	399, 114, 85, 350, 339, 59, 337, 261, 185, 78,
	127, 53, 27, 56, 13, 12, 143, 11, 10, 9,
	8, 7, 6, 5, 4, 153, 2, 309, 112, 131,
	130, 132, 133, 134, 135, 0, 0, 136, 128, 129,
	0, 0, 110, 0, 152, 0, 0, 0, 0, 108,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 119, 120, 106, 0, 0, 141,
	0, 121, 0, 0, 122, 0, 0, 59, 59, 59,
	59, 0, 0, 0, 0, 0, 0, 0, 138, 0,
	350, 0, 0, 404, 144, 139, 145, 140, 146, 150,
	151, 149, 148, 147, 142, 114, 0, 0, 0, 0,
	0, 0, 0, 0, 127, 0, 0, 0, 0, 0,
	143, 0, 0, 0, 0, 0, 0, 0, 0, 153,
	0, 309, 112, 131, 130, 132, 133, 134, 135, 0,
	0, 136, 128, 129, 0, 0, 110, 0, 152, 0,
	0, 0, 114, 0, 0, 0, 0, 0, 0, 0,
	0, 127, 0, 0, 0, 0, 0, 143, 119, 120,
	106, 0, 0, 141, 0, 121, 153, 0, 122, 112,
	131, 130, 132, 133, 134, 135, 0, 0, 136, 128,
	129, 0, 138, 110, 0, 152, 0, 0, 144, 139,
	145, 140, 146, 150, 151, 149, 148, 147, 142, 364,
	0, 0, 0, 0, 0, 119, 120, 106, 0, 0,
	141, 0, 121, 0, 0, 122, 0, 0, 205, 204,
	213, 214, 207, 208, 209, 210, 211, 212, 206, 138,
	14, 215, 0, 0, 0, 144, 139, 145, 140, 146,
	150, 151, 149, 148, 147, 142, 114, 0, 0, 0,
	0, 0, 0, 0, 0, 127, 0, 0, 0, 0,
	0, 143, 0, 0, 0, 0, 0, 0, 0, 0,
	153, 0, 0, 112, 131, 130, 132, 133, 134, 135,
	0, 0, 136, 128, 129, 0, 0, 110, 0, 152,
	0, 0, 0, 114, 0, 0, 0, 0, 0, 0,
	0, 0, 127, 0, 0, 0, 0, 0, 143, 119,
	120, 0, 0, 0, 141, 0, 121, 153, 0, 122,
	112, 131, 130, 132, 133, 134, 135, 0, 0, 136,
	128, 129, 0, 138, 110, 0, 152, 0, 0, 144,
	139, 145, 140, 146, 150, 151, 149, 148, 147, 142,
	0, 0, 0, 0, 0, 0, 119, 120, 0, 0,
	0, 141, 0, 121, 0, 0, 122, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	138, 0, 0, 0, 0, 0, 144, 139, 145, 140,
	146, 150, 151, 149, 148, 147, 142, 127, 0, 0,
	0, 0, 0, 143, 0, 0, 0, 0, 0, 0,
	0, 0, 153, 0, 0, 112, 131, 130, 132, 133,
	134, 135, 0, 0, 136, 128, 129, 0, 0, 0,
	0, 152, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 127, 0, 0, 0, 0, 0,
	143, 119, 120, 0, 0, 0, 141, 0, 121, 153,
	0, 122, 112, 131, 130, 132, 133, 134, 135, 0,
	0, 136, 0, 0, 0, 138, 0, 0, 152, 0,
	0, 144, 139, 145, 140, 146, 150, 151, 149, 148,
	147, 142, 0, 0, 0, 0, 0, 0, 119, 120,
	0, 0, 0, 141, 0, 121, 0, 0, 122, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 138, 0, 0, 0, 0, 0, 144, 139,
	145, 140, 146, 150, 151, 149, 148, 147, 142,
}
var yyPact = [...]int{

	124, -1000, -1000, 463, -1000, -1000, -1000, -1000, -1000, -1000,
	-1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, -79,
	-59, -45, -52, -46, -1000, -1000, -1000, 459, 409, 342,
	-1000, -78, 133, 453, 101, -75, -49, 101, -1000, -47,
	101, -1000, 133, -76, 164, -76, 133, -1000, -1000, -1000,
	-1000, -1000, -1000, 297, 143, -1000, 64, 144, 332, -28,
	-1000, 133, 183, -1000, 25, -1000, 133, 42, 150, -1000,
	133, -1000, -65, 133, 357, 284, 101, -1000, 710, -1000,
	380, -1000, 319, 317, -1000, 290, 133, -1000, 101, 133,
	439, 101, 861, -1000, 346, -82, -1000, 18, -1000, 133,
	-1000, -1000, 133, -1000, 206, -1000, -1000, 405, -30, -1000,
	861, 448, -1000, -1000, 187, -1000, -37, -1000, -1000, 1003,
	1003, 1003, 1003, 187, 187, -1000, -1000, 187, -1000, -1000,
	-1000, -1000, -1000, -1000, -1000, -1000, -1000, 814, -1000, -1000,
	-1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000,
	-1000, -1000, 861, -1000, 133, -1000, -1000, -1000, -1000, 354,
	101, 101, -1000, 305, -1000, 392, 861, -1000, 94, -32,
	-1000, -1000, 283, 101, -1000, -70, -1000, -1000, -1000, -1000,
	-1000, -1000, -1000, -1000, -1000, 439, 710, 78, -1000, 861,
	861, 121, -1000, 170, -1000, -1000, 41, 14, 956, 117,
	32, 1003, 1003, 1003, 1003, 1003, 1003, 1003, 1003, 1003,
	1003, 1003, 1003, 1003, 1003, 155, -1000, -1000, -1000, -1000,
	-1000, -1000, -1000, -1000, 459, 350, 350, -14, -14, -14,
	186, 559, 663, 101, 215, 210, 94, 38, 94, -1000,
	281, 187, 463, 238, 241, -1000, 392, 372, 420, 94,
	147, 133, -1000, -1000, 133, -1000, 436, -1000, 217, 435,
	-1000, -1000, 156, 360, 216, 14, 95, -1000, -1000, 127,
	-1000, -1000, -1000, -1000, -33, -1000, -1000, 224, -1000, 814,
	-1000, -1000, 117, 1003, 1003, 1003, 224, 224, 727, 328,
	52, -14, 12, 12, -13, -13, -13, -13, -15, -15,
	-1000, -1000, -1000, -1000, -1000, -1000, -1000, 192, 710, -1000,
	-1000, 192, 99, -1000, 861, -1000, 102, -1000, 861, -1000,
	345, 214, -1000, 861, -1000, -1000, 101, 372, -1000, 861,
	861, -34, -1000, -1000, 424, 415, 78, 78, 78, 78,
	-1000, 312, 309, -1000, 302, 295, 404, 1, -1000, 151,
	-1000, -1000, 133, -1000, 231, -1000, -1000, -1000, 35, 210,
	-1000, 224, 224, 479, 1003, -1000, 192, -1000, -1000, 94,
	28, -1000, 861, 87, 338, 187, -1000, -1000, 98, 203,
	-1000, 90, 101, -1000, 392, 861, 861, 435, 280, 509,
	-1000, -1000, -1000, -1000, 308, -1000, 299, -1000, -1000, -1000,
	-55, -57, -62, -1000, -1000, -1000, -1000, -1000, -1000, 1003,
	224, -1000, -1000, 94, 861, 456, -1000, 861, 861, 861,
	-1000, -1000, -1000, 372, 94, 194, 861, 861, -1000, -1000,
	187, 187, 187, 224, 94, 101, 94, 94, -1000, 391,
	94, 94, 101, 101, 101, 183, -1000, 450, 2, 181,
	-1000, 181, 181, -1000, 101, -1000, 101, -1000, -1000, 101,
	-1000, -1000,
}
var yyPgo = [...]int{

	0, 606, 23, 604, 603, 602, 601, 600, 599, 598,
	597, 595, 594, 478, 593, 592, 591, 589, 14, 28,
	65, 588, 17, 19, 11, 587, 586, 8, 584, 36,
	582, 580, 2, 26, 578, 18, 577, 576, 22, 88,
	574, 559, 21, 6, 558, 10, 166, 557, 556, 555,
	15, 549, 543, 540, 525, 516, 515, 12, 514, 5,
	513, 3, 510, 509, 502, 16, 4, 86, 501, 392,
	324, 500, 499, 491, 490, 489, 0, 20, 488, 320,
	7, 487, 486, 13, 481, 479, 469, 1, 9,
}
var yyR1 = [...]int{

	0, 85, 1, 1, 1, 1, 1, 1, 1, 1,
	1, 1, 1, 2, 2, 2, 3, 3, 4, 5,
	6, 7, 7, 7, 8, 8, 8, 9, 10, 10,
	10, 11, 12, 12, 12, 86, 13, 14, 14, 15,
	15, 15, 16, 16, 17, 17, 18, 18, 19, 19,
	19, 19, 78, 78, 78, 77, 77, 21, 21, 22,
	22, 23, 23, 24, 24, 24, 25, 25, 25, 25,
	82, 82, 81, 81, 81, 80, 80, 26, 26, 26,
	26, 27, 27, 27, 27, 28, 28, 30, 30, 29,
	29, 31, 31, 31, 31, 32, 32, 33, 33, 20,
	20, 20, 20, 20, 20, 35, 35, 34, 34, 34,
	34, 34, 34, 34, 34, 34, 34, 34, 34, 41,
	41, 41, 41, 41, 41, 36, 36, 36, 36, 36,
	36, 36, 42, 42, 42, 46, 43, 43, 84, 84,
	39, 39, 39, 39, 39, 39, 39, 39, 39, 39,
	39, 39, 39, 39, 39, 39, 39, 39, 39, 39,
	39, 39, 39, 39, 39, 39, 39, 39, 39, 39,
	56, 56, 56, 56, 56, 56, 56, 56, 55, 55,
	55, 55, 55, 55, 55, 48, 51, 51, 49, 49,
	50, 52, 52, 47, 47, 47, 38, 38, 38, 38,
	38, 38, 38, 40, 40, 40, 53, 53, 54, 54,
	57, 57, 58, 58, 59, 60, 60, 60, 61, 61,
	61, 61, 62, 62, 62, 63, 63, 64, 64, 65,
	65, 37, 37, 44, 44, 45, 66, 66, 67, 68,
	68, 70, 70, 71, 71, 69, 69, 72, 72, 72,
	72, 72, 72, 73, 73, 74, 74, 75, 75, 76,
	79, 87, 88, 83,
}
var yyR2 = [...]int{

	0, 1, 1, 1, 1, 1, 1, 1, 1, 1,
	1, 1, 1, 12, 6, 3, 7, 7, 8, 7,
	3, 5, 8, 4, 6, 7, 4, 5, 4, 5,
	5, 3, 2, 2, 2, 0, 2, 0, 2, 1,
	2, 2, 0, 1, 0, 1, 1, 3, 1, 2,
	3, 5, 0, 1, 2, 1, 1, 0, 2, 1,
	3, 1, 1, 3, 3, 3, 3, 5, 5, 3,
	0, 1, 0, 1, 2, 1, 1, 1, 2, 2,
	1, 2, 3, 2, 3, 2, 2, 2, 1, 1,
	3, 0, 5, 5, 5, 1, 3, 0, 2, 1,
	3, 3, 2, 3, 1, 1, 1, 1, 3, 3,
	3, 4, 3, 4, 3, 4, 5, 6, 2, 1,
	2, 1, 2, 1, 2, 1, 1, 1, 1, 1,
	1, 1, 3, 1, 1, 3, 1, 3, 1, 1,
	1, 1, 1, 1, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 2, 2,
	2, 3, 3, 4, 5, 3, 4, 1, 1, 4,
	1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
	1, 1, 1, 1, 1, 5, 0, 1, 1, 2,
	4, 0, 2, 1, 3, 5, 1, 1, 1, 1,
	1, 1, 1, 1, 2, 2, 0, 3, 0, 2,
	0, 3, 1, 3, 2, 0, 1, 1, 0, 2,
	4, 4, 0, 2, 4, 0, 3, 1, 3, 0,
	5, 2, 1, 1, 3, 3, 1, 3, 3, 1,
	1, 0, 2, 0, 3, 0, 1, 1, 1, 1,
	1, 1, 1, 0, 1, 0, 1, 0, 2, 1,
	1, 1, 1, 0,
}
var yyChk = [...]int{

	-1000, -85, -1, -2, -3, -4, -5, -6, -7, -8,
	-9, -10, -11, -12, 6, 7, 8, 9, 29, 99,
	100, 102, 101, 103, 112, 113, 114, -15, 5, -13,
	-86, -13, -13, -13, -13, 104, -74, 106, 110, -69,
	106, 108, 104, 104, 105, 106, 104, -83, -83, -83,
	-2, 19, 20, -16, 33, 20, -14, -69, -29, -79,
	49, 10, -66, -67, -76, 49, -71, 109, 105, -76,
	104, -76, -79, -70, 109, 49, -70, -79, -17, 36,
	-40, -76, 52, 55, 57, -30, 25, -29, 29, 96,
	-29, 47, 70, -79, 63, 49, -83, -79, -83, 107,
	-79, 22, 45, -76, -18, -19, 87, -20, -79, -34,
	63, -39, 49, -35, 22, -38, -47, -45, -46, 85,
	86, 92, 95, -76, -55, -56, -48, 31, 59, 60,
	51, 50, 52, 53, 54, 55, 58, -87, 109, 116,
	118, 90, 125, 37, 115, 117, 119, 124, 123, 122,
	120, 121, 65, 46, -68, 18, 10, 31, 31, -63,
	29, -87, -29, -66, -79, -33, 11, -67, -20, -79,
	-83, 22, -75, 111, -72, 102, 100, 28, 101, 14,
	126, 49, -79, -79, -83, -21, 47, 10, -78, 62,
	61, 77, -77, 21, -76, 51, 96, -20, -36, 80,
	63, 78, 79, 64, 82, 81, 91, 85, 86, 87,
	88, 89, 90, 83, 84, 94, 70, 71, 72, 73,
	74, 75, 76, -46, -87, 97, 98, -39, -39, -39,
	-39, -87, -87, -87, -2, -43, -20, -51, -20, -29,
	-37, 31, -2, -66, -64, -76, -33, -57, 14, -20,
	96, 45, -76, -83, -73, 107, -33, -19, -22, -23,
	-24, -25, -29, -46, -87, -20, -20, -41, 58, 63,
	59, 60, -77, 87, -79, -76, -35, -39, -42, -87,
	-46, 56, 80, 78, 79, 64, -39, -39, -39, -39,
	-39, -39, -39, -39, -39, -39, -39, -39, -39, -39,
	-84, 49, 51, -38, -38, -76, -88, -18, 20, 48,
	-88, -18, -76, -88, 47, -88, -49, -50, 66, -65,
	45, -44, -45, -87, -65, -88, 47, -57, -61, 16,
	15, -79, -79, -79, -53, 12, 47, -26, -27, -28,
	35, 39, 41, 36, 37, 38, 42, -81, -80, 21,
	-79, 51, -82, 21, -22, 58, 59, 60, 96, -43,
	-42, -39, -39, -39, 62, -88, -18, -88, -88, -20,
	-52, -50, 68, -20, 26, 47, -76, -61, -20, -58,
	-59, -20, 96, -83, -54, 13, 15, -23, -24, -23,
	-24, 35, 35, 35, 40, 35, 40, 35, -27, -31,
	43, 108, 44, -80, -79, -88, 87, -76, -88, 62,
	-39, -88, 69, -20, 67, 27, -45, 47, 17, 47,
	-60, 23, 24, -57, -20, -43, 45, 45, 35, 35,
	105, 105, 105, -39, -20, 8, -20, -20, -59, -61,
	-20, -20, -87, -87, -87, -66, -62, 18, 30, -32,
	-76, -32, -32, 8, 80, -88, 47, -88, -88, -76,
	-76, -76,
}
var yyDef = [...]int{

	0, -2, 1, 2, 3, 4, 5, 6, 7, 8,
	9, 10, 11, 12, 35, 35, 35, 35, 35, 255,
	245, 0, 0, 0, 263, 263, 263, 0, 39, 42,
	37, 245, 0, 0, 0, 243, 0, 0, 256, 0,
	0, 246, 0, 241, 0, 241, 0, 32, 33, 34,
	15, 40, 41, 44, 0, 43, 36, 0, 0, 89,
	260, 0, 20, 236, 0, 259, 0, 0, 0, 263,
	0, 263, 0, 0, 0, 0, 0, 31, 0, 45,
	0, 203, 0, 0, 38, 225, 0, 88, 0, 0,
	97, 0, 0, 263, 0, 257, 23, 0, 26, 0,
	28, 242, 0, 263, 57, 46, 48, 52, 0, 99,
	0, 104, -2, 107, 0, 140, 141, 142, 143, 0,
	0, 0, 0, 193, 0, -2, 168, 0, 105, 106,
	196, 197, 198, 199, 200, 201, 202, 0, 179, 180,
	181, 182, 183, 184, 170, 171, 172, 173, 174, 175,
	176, 177, 186, 261, 0, 239, 240, 204, 205, 0,
	0, 0, 87, 97, 90, 210, 0, 237, 238, 0,
	21, 244, 0, 0, 263, 253, 247, 248, 249, 250,
	251, 252, 27, 29, 30, 97, 0, 0, 49, 0,
	0, 0, 53, 0, 55, 56, 0, 102, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 125, 126, 127, 128,
	129, 130, 131, 118, 0, 0, 0, 158, 159, 160,
	0, 0, 0, 0, 0, 0, 136, 0, 187, 14,
	229, 0, 232, 229, 0, 227, 210, 218, 0, 98,
	0, 0, 258, 24, 0, 254, 206, 47, 58, 59,
	61, 62, 72, 70, 0, 100, 101, 103, 119, 0,
	121, 123, 54, 50, 0, 194, 108, 109, 110, 0,
	133, 134, 0, 0, 0, 0, 112, 114, 0, 144,
	145, 146, 147, 148, 149, 150, 151, 152, 153, 154,
	157, 138, 139, 155, 156, 161, 162, 0, 0, 262,
	165, 0, 0, 135, 0, 235, 191, 188, 0, 16,
	0, 231, 233, 0, 17, 226, 0, 218, 19, 0,
	0, 0, 263, 25, 208, 0, 0, 0, 0, 0,
	77, 0, 0, 80, 0, 0, 0, 91, 73, 0,
	75, 76, 0, 71, 0, 120, 122, 124, 0, 0,
	111, 113, 115, 0, 0, 163, 0, 166, 169, 137,
	0, 189, 0, 0, 0, 0, 228, 18, 219, 211,
	212, 215, 0, 22, 210, 0, 0, 60, 66, 0,
	69, 78, 79, 81, 0, 83, 0, 85, 86, 63,
	0, 0, 0, 74, 64, 65, 51, 195, 132, 0,
	116, 164, 185, 192, 0, 0, 234, 0, 0, 0,
	214, 216, 217, 218, 209, 207, 0, 0, 82, 84,
	0, 0, 0, 117, 190, 0, 220, 221, 213, 222,
	67, 68, 0, 0, 0, 230, 13, 0, 0, 0,
	95, 0, 0, 223, 0, 92, 0, 93, 94, 0,
	96, 224,
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
			yyVAL.statement = &Insert{Comments: Comments(yyDollar[2].bytes2), Ignore: yyDollar[3].str, Table: yyDollar[4].tableName, Columns: yyDollar[5].columns, Rows: yyDollar[6].insRows, OnDup: OnDup(yyDollar[7].updateExprs)}
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
			yyVAL.statement = &Insert{Comments: Comments(yyDollar[2].bytes2), Ignore: yyDollar[3].str, Table: yyDollar[4].tableName, Columns: cols, Rows: Values{vals}, OnDup: OnDup(yyDollar[7].updateExprs)}
		}
	case 18:
		yyDollar = yyS[yypt-8 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:238
		{
			yyVAL.statement = &Update{Comments: Comments(yyDollar[2].bytes2), Table: yyDollar[3].tableName, Exprs: yyDollar[5].updateExprs, Where: NewWhere(WhereStr, yyDollar[6].boolExpr), OrderBy: yyDollar[7].orderBy, Limit: yyDollar[8].limit}
		}
	case 19:
		yyDollar = yyS[yypt-7 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:244
		{
			yyVAL.statement = &Delete{Comments: Comments(yyDollar[2].bytes2), Table: yyDollar[4].tableName, Where: NewWhere(WhereStr, yyDollar[5].boolExpr), OrderBy: yyDollar[6].orderBy, Limit: yyDollar[7].limit}
		}
	case 20:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:250
		{
			yyVAL.statement = &Set{Comments: Comments(yyDollar[2].bytes2), Exprs: yyDollar[3].updateExprs}
		}
	case 21:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:256
		{
			yyVAL.statement = &DDL{Action: CreateStr, NewName: yyDollar[4].tableIdent}
		}
	case 22:
		yyDollar = yyS[yypt-8 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:260
		{
			// Change this to an alter statement
			yyVAL.statement = &DDL{Action: AlterStr, Table: yyDollar[7].tableIdent, NewName: yyDollar[7].tableIdent}
		}
	case 23:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:265
		{
			yyVAL.statement = &DDL{Action: CreateStr, NewName: NewTableIdent(yyDollar[3].colIdent.Lowered())}
		}
	case 24:
		yyDollar = yyS[yypt-6 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:271
		{
			yyVAL.statement = &DDL{Action: AlterStr, Table: yyDollar[4].tableIdent, NewName: yyDollar[4].tableIdent}
		}
	case 25:
		yyDollar = yyS[yypt-7 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:275
		{
			// Change this to a rename statement
			yyVAL.statement = &DDL{Action: RenameStr, Table: yyDollar[4].tableIdent, NewName: yyDollar[7].tableIdent}
		}
	case 26:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:280
		{
			yyVAL.statement = &DDL{Action: AlterStr, Table: NewTableIdent(yyDollar[3].colIdent.Lowered()), NewName: NewTableIdent(yyDollar[3].colIdent.Lowered())}
		}
	case 27:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:286
		{
			yyVAL.statement = &DDL{Action: RenameStr, Table: yyDollar[3].tableIdent, NewName: yyDollar[5].tableIdent}
		}
	case 28:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:292
		{
			var exists bool
			if yyDollar[3].byt != 0 {
				exists = true
			}
			yyVAL.statement = &DDL{Action: DropStr, Table: yyDollar[4].tableIdent, IfExists: exists}
		}
	case 29:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:300
		{
			// Change this to an alter statement
			yyVAL.statement = &DDL{Action: AlterStr, Table: yyDollar[5].tableIdent, NewName: yyDollar[5].tableIdent}
		}
	case 30:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:305
		{
			var exists bool
			if yyDollar[3].byt != 0 {
				exists = true
			}
			yyVAL.statement = &DDL{Action: DropStr, Table: NewTableIdent(yyDollar[4].colIdent.Lowered()), IfExists: exists}
		}
	case 31:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:315
		{
			yyVAL.statement = &DDL{Action: AlterStr, Table: yyDollar[3].tableIdent, NewName: yyDollar[3].tableIdent}
		}
	case 32:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:321
		{
			yyVAL.statement = &Other{}
		}
	case 33:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:325
		{
			yyVAL.statement = &Other{}
		}
	case 34:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:329
		{
			yyVAL.statement = &Other{}
		}
	case 35:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:334
		{
			setAllowComments(yylex, true)
		}
	case 36:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:338
		{
			yyVAL.bytes2 = yyDollar[2].bytes2
			setAllowComments(yylex, false)
		}
	case 37:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:344
		{
			yyVAL.bytes2 = nil
		}
	case 38:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:348
		{
			yyVAL.bytes2 = append(yyDollar[1].bytes2, yyDollar[2].bytes)
		}
	case 39:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:354
		{
			yyVAL.str = UnionStr
		}
	case 40:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:358
		{
			yyVAL.str = UnionAllStr
		}
	case 41:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:362
		{
			yyVAL.str = UnionDistinctStr
		}
	case 42:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:367
		{
			yyVAL.str = ""
		}
	case 43:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:371
		{
			yyVAL.str = DistinctStr
		}
	case 44:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:376
		{
			yyVAL.str = ""
		}
	case 45:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:380
		{
			yyVAL.str = StraightJoinHint
		}
	case 46:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:386
		{
			yyVAL.selectExprs = SelectExprs{yyDollar[1].selectExpr}
		}
	case 47:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:390
		{
			yyVAL.selectExprs = append(yyVAL.selectExprs, yyDollar[3].selectExpr)
		}
	case 48:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:396
		{
			yyVAL.selectExpr = &StarExpr{}
		}
	case 49:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:400
		{
			yyVAL.selectExpr = &NonStarExpr{Expr: yyDollar[1].expr, As: yyDollar[2].colIdent}
		}
	case 50:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:404
		{
			yyVAL.selectExpr = &StarExpr{TableName: &TableName{Name: yyDollar[1].tableIdent}}
		}
	case 51:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:408
		{
			yyVAL.selectExpr = &StarExpr{TableName: &TableName{Qualifier: yyDollar[1].tableIdent, Name: yyDollar[3].tableIdent}}
		}
	case 52:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:413
		{
			yyVAL.colIdent = ColIdent{}
		}
	case 53:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:417
		{
			yyVAL.colIdent = yyDollar[1].colIdent
		}
	case 54:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:421
		{
			yyVAL.colIdent = yyDollar[2].colIdent
		}
	case 56:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:428
		{
			yyVAL.colIdent = NewColIdent(string(yyDollar[1].bytes))
		}
	case 57:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:433
		{
			yyVAL.tableExprs = TableExprs{&AliasedTableExpr{Expr: &TableName{Name: NewTableIdent("dual")}}}
		}
	case 58:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:437
		{
			yyVAL.tableExprs = yyDollar[2].tableExprs
		}
	case 59:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:443
		{
			yyVAL.tableExprs = TableExprs{yyDollar[1].tableExpr}
		}
	case 60:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:447
		{
			yyVAL.tableExprs = append(yyVAL.tableExprs, yyDollar[3].tableExpr)
		}
	case 63:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:457
		{
			yyVAL.tableExpr = &AliasedTableExpr{Expr: yyDollar[1].tableName, As: yyDollar[2].tableIdent, Hints: yyDollar[3].indexHints}
		}
	case 64:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:461
		{
			yyVAL.tableExpr = &AliasedTableExpr{Expr: yyDollar[1].subquery, As: yyDollar[3].tableIdent}
		}
	case 65:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:465
		{
			yyVAL.tableExpr = &ParenTableExpr{Exprs: yyDollar[2].tableExprs}
		}
	case 66:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:478
		{
			yyVAL.tableExpr = &JoinTableExpr{LeftExpr: yyDollar[1].tableExpr, Join: yyDollar[2].str, RightExpr: yyDollar[3].tableExpr}
		}
	case 67:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:482
		{
			yyVAL.tableExpr = &JoinTableExpr{LeftExpr: yyDollar[1].tableExpr, Join: yyDollar[2].str, RightExpr: yyDollar[3].tableExpr, On: yyDollar[5].expr}
		}
	case 68:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:486
		{
			yyVAL.tableExpr = &JoinTableExpr{LeftExpr: yyDollar[1].tableExpr, Join: yyDollar[2].str, RightExpr: yyDollar[3].tableExpr, On: yyDollar[5].expr}
		}
	case 69:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:490
		{
			yyVAL.tableExpr = &JoinTableExpr{LeftExpr: yyDollar[1].tableExpr, Join: yyDollar[2].str, RightExpr: yyDollar[3].tableExpr}
		}
	case 70:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:495
		{
			yyVAL.empty = struct{}{}
		}
	case 71:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:497
		{
			yyVAL.empty = struct{}{}
		}
	case 72:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:500
		{
			yyVAL.tableIdent = NewTableIdent("")
		}
	case 73:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:504
		{
			yyVAL.tableIdent = yyDollar[1].tableIdent
		}
	case 74:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:508
		{
			yyVAL.tableIdent = yyDollar[2].tableIdent
		}
	case 76:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:515
		{
			yyVAL.tableIdent = NewTableIdent(string(yyDollar[1].bytes))
		}
	case 77:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:521
		{
			yyVAL.str = JoinStr
		}
	case 78:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:525
		{
			yyVAL.str = JoinStr
		}
	case 79:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:529
		{
			yyVAL.str = JoinStr
		}
	case 80:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:533
		{
			yyVAL.str = StraightJoinStr
		}
	case 81:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:539
		{
			yyVAL.str = LeftJoinStr
		}
	case 82:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:543
		{
			yyVAL.str = LeftJoinStr
		}
	case 83:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:547
		{
			yyVAL.str = RightJoinStr
		}
	case 84:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:551
		{
			yyVAL.str = RightJoinStr
		}
	case 85:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:557
		{
			yyVAL.str = NaturalJoinStr
		}
	case 86:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:561
		{
			if yyDollar[2].str == LeftJoinStr {
				yyVAL.str = NaturalLeftJoinStr
			} else {
				yyVAL.str = NaturalRightJoinStr
			}
		}
	case 87:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:571
		{
			yyVAL.tableName = yyDollar[2].tableName
		}
	case 88:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:575
		{
			yyVAL.tableName = yyDollar[1].tableName
		}
	case 89:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:581
		{
			yyVAL.tableName = &TableName{Name: yyDollar[1].tableIdent}
		}
	case 90:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:585
		{
			yyVAL.tableName = &TableName{Qualifier: yyDollar[1].tableIdent, Name: yyDollar[3].tableIdent}
		}
	case 91:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:590
		{
			yyVAL.indexHints = nil
		}
	case 92:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:594
		{
			yyVAL.indexHints = &IndexHints{Type: UseStr, Indexes: yyDollar[4].colIdents}
		}
	case 93:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:598
		{
			yyVAL.indexHints = &IndexHints{Type: IgnoreStr, Indexes: yyDollar[4].colIdents}
		}
	case 94:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:602
		{
			yyVAL.indexHints = &IndexHints{Type: ForceStr, Indexes: yyDollar[4].colIdents}
		}
	case 95:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:608
		{
			yyVAL.colIdents = []ColIdent{yyDollar[1].colIdent}
		}
	case 96:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:612
		{
			yyVAL.colIdents = append(yyDollar[1].colIdents, yyDollar[3].colIdent)
		}
	case 97:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:617
		{
			yyVAL.boolExpr = nil
		}
	case 98:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:621
		{
			yyVAL.boolExpr = yyDollar[2].expr
		}
	case 99:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:627
		{
			yyVAL.expr = nil
		}
	case 100:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:631
		{
			yyVAL.expr = &AndExpr{Left: yyDollar[1].expr, Right: yyDollar[3].expr}
		}
	case 101:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:635
		{
			yyVAL.expr = &OrExpr{Left: yyDollar[1].expr, Right: yyDollar[3].expr}
		}
	case 102:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:639
		{
			yyVAL.expr = &NotExpr{Expr: yyDollar[2].expr}
		}
	case 103:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:643
		{
			yyVAL.expr = &IsExpr{Operator: yyDollar[3].str, Expr: yyDollar[1].expr}
		}
	case 104:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:647
		{
			yyVAL.expr = nil
		}
	case 105:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:653
		{
			yyVAL.boolVal = BoolVal(true)
		}
	case 106:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:657
		{
			yyVAL.boolVal = BoolVal(false)
		}
	case 107:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:663
		{
			yyVAL.boolExpr = yyDollar[1].boolVal
		}
	case 108:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:667
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: yyDollar[2].str, Right: yyDollar[3].boolVal}
		}
	case 109:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:671
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: yyDollar[2].str, Right: yyDollar[3].valExpr}
		}
	case 110:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:675
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: InStr, Right: yyDollar[3].colTuple}
		}
	case 111:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:679
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: NotInStr, Right: yyDollar[4].colTuple}
		}
	case 112:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:683
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: LikeStr, Right: yyDollar[3].valExpr}
		}
	case 113:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:687
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: NotLikeStr, Right: yyDollar[4].valExpr}
		}
	case 114:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:691
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: RegexpStr, Right: yyDollar[3].valExpr}
		}
	case 115:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:695
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: NotRegexpStr, Right: yyDollar[4].valExpr}
		}
	case 116:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:699
		{
			yyVAL.boolExpr = &RangeCond{Left: yyDollar[1].valExpr, Operator: BetweenStr, From: yyDollar[3].valExpr, To: yyDollar[5].valExpr}
		}
	case 117:
		yyDollar = yyS[yypt-6 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:703
		{
			yyVAL.boolExpr = &RangeCond{Left: yyDollar[1].valExpr, Operator: NotBetweenStr, From: yyDollar[4].valExpr, To: yyDollar[6].valExpr}
		}
	case 118:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:707
		{
			yyVAL.boolExpr = &ExistsExpr{Subquery: yyDollar[2].subquery}
		}
	case 119:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:713
		{
			yyVAL.str = IsNullStr
		}
	case 120:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:717
		{
			yyVAL.str = IsNotNullStr
		}
	case 121:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:721
		{
			yyVAL.str = IsTrueStr
		}
	case 122:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:725
		{
			yyVAL.str = IsNotTrueStr
		}
	case 123:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:729
		{
			yyVAL.str = IsFalseStr
		}
	case 124:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:733
		{
			yyVAL.str = IsNotFalseStr
		}
	case 125:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:739
		{
			yyVAL.str = EqualStr
		}
	case 126:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:743
		{
			yyVAL.str = LessThanStr
		}
	case 127:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:747
		{
			yyVAL.str = GreaterThanStr
		}
	case 128:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:751
		{
			yyVAL.str = LessEqualStr
		}
	case 129:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:755
		{
			yyVAL.str = GreaterEqualStr
		}
	case 130:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:759
		{
			yyVAL.str = NotEqualStr
		}
	case 131:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:763
		{
			yyVAL.str = NullSafeEqualStr
		}
	case 132:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:769
		{
			yyVAL.colTuple = ValTuple(yyDollar[2].valExprs)
		}
	case 133:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:773
		{
			yyVAL.colTuple = yyDollar[1].subquery
		}
	case 134:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:777
		{
			yyVAL.colTuple = ListArg(yyDollar[1].bytes)
		}
	case 135:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:783
		{
			yyVAL.subquery = &Subquery{yyDollar[2].selStmt}
		}
	case 136:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:789
		{
			yyVAL.valExprs = ValExprs{yyDollar[1].expr}
		}
	case 137:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:793
		{
			yyVAL.valExprs = append(yyDollar[1].valExprs, yyDollar[3].expr)
		}
	case 138:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:799
		{
			yyVAL.str = string(yyDollar[1].bytes)
		}
	case 139:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:803
		{
			yyVAL.str = string(yyDollar[1].bytes)
		}
	case 140:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:809
		{
			yyVAL.valExpr = yyDollar[1].valExpr
		}
	case 141:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:813
		{
			yyVAL.valExpr = yyDollar[1].colName
		}
	case 142:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:817
		{
			yyVAL.valExpr = yyDollar[1].valTuple
		}
	case 143:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:821
		{
			yyVAL.valExpr = yyDollar[1].subquery
		}
	case 144:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:825
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: BitAndStr, Right: yyDollar[3].valExpr}
		}
	case 145:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:829
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: BitOrStr, Right: yyDollar[3].valExpr}
		}
	case 146:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:833
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: BitXorStr, Right: yyDollar[3].valExpr}
		}
	case 147:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:837
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: PlusStr, Right: yyDollar[3].valExpr}
		}
	case 148:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:841
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: MinusStr, Right: yyDollar[3].valExpr}
		}
	case 149:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:845
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: MultStr, Right: yyDollar[3].valExpr}
		}
	case 150:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:849
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: DivStr, Right: yyDollar[3].valExpr}
		}
	case 151:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:853
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: ModStr, Right: yyDollar[3].valExpr}
		}
	case 152:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:857
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: ModStr, Right: yyDollar[3].valExpr}
		}
	case 153:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:861
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: ShiftLeftStr, Right: yyDollar[3].valExpr}
		}
	case 154:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:865
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: ShiftRightStr, Right: yyDollar[3].valExpr}
		}
	case 155:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:869
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].colName, Operator: JSONExtractOp, Right: yyDollar[3].valExpr}
		}
	case 156:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:873
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].colName, Operator: JSONUnquoteExtractOp, Right: yyDollar[3].valExpr}
		}
	case 157:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:877
		{
			yyVAL.valExpr = &CollateExpr{Expr: yyDollar[1].valExpr, Charset: yyDollar[3].str}
		}
	case 158:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:881
		{
			if num, ok := yyDollar[2].valExpr.(*SQLVal); ok && num.Type == IntVal {
				yyVAL.valExpr = num
			} else {
				yyVAL.valExpr = &UnaryExpr{Operator: UPlusStr, Expr: yyDollar[2].valExpr}
			}
		}
	case 159:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:889
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
	case 160:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:903
		{
			yyVAL.valExpr = &UnaryExpr{Operator: TildaStr, Expr: yyDollar[2].valExpr}
		}
	case 161:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:907
		{
			// This rule prevents the usage of INTERVAL
			// as a function. If support is needed for that,
			// we'll need to revisit this. The solution
			// will be non-trivial because of grammar conflicts.
			yyVAL.valExpr = &IntervalExpr{Expr: yyDollar[2].valExpr, Unit: yyDollar[3].colIdent}
		}
	case 162:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:915
		{
			yyVAL.valExpr = &FuncExpr{Name: yyDollar[1].colIdent}
		}
	case 163:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:919
		{
			yyVAL.valExpr = &FuncExpr{Name: yyDollar[1].colIdent, Exprs: yyDollar[3].selectExprs}
		}
	case 164:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:923
		{
			yyVAL.valExpr = &FuncExpr{Name: yyDollar[1].colIdent, Distinct: true, Exprs: yyDollar[4].selectExprs}
		}
	case 165:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:927
		{
			yyVAL.valExpr = &FuncExpr{Name: yyDollar[1].colIdent}
		}
	case 166:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:931
		{
			yyVAL.valExpr = &FuncExpr{Name: yyDollar[1].colIdent, Exprs: yyDollar[3].selectExprs}
		}
	case 167:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:935
		{
			yyVAL.valExpr = &FuncExpr{Name: yyDollar[1].colIdent}
		}
	case 168:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:939
		{
			yyVAL.valExpr = yyDollar[1].caseExpr
		}
	case 169:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:943
		{
			yyVAL.valExpr = &ValuesFuncExpr{Name: yyDollar[3].colIdent}
		}
	case 170:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:951
		{
			yyVAL.colIdent = NewColIdent("current_timestamp")
		}
	case 171:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:955
		{
			yyVAL.colIdent = NewColIdent("current_date")
		}
	case 172:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:959
		{
			yyVAL.colIdent = NewColIdent("current_time")
		}
	case 173:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:963
		{
			yyVAL.colIdent = NewColIdent("utc_timestamp")
		}
	case 174:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:967
		{
			yyVAL.colIdent = NewColIdent("utc_time")
		}
	case 175:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:971
		{
			yyVAL.colIdent = NewColIdent("utc_date")
		}
	case 176:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:975
		{
			yyVAL.colIdent = NewColIdent("localtime")
		}
	case 177:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:979
		{
			yyVAL.colIdent = NewColIdent("localtimestamp")
		}
	case 179:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:988
		{
			yyVAL.colIdent = NewColIdent("if")
		}
	case 180:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:992
		{
			yyVAL.colIdent = NewColIdent("database")
		}
	case 181:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:996
		{
			yyVAL.colIdent = NewColIdent("unix_timestamp")
		}
	case 182:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1000
		{
			yyVAL.colIdent = NewColIdent("mod")
		}
	case 183:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1004
		{
			yyVAL.colIdent = NewColIdent("replace")
		}
	case 184:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1008
		{
			yyVAL.colIdent = NewColIdent("left")
		}
	case 185:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1014
		{
			yyVAL.caseExpr = &CaseExpr{Expr: yyDollar[2].valExpr, Whens: yyDollar[3].whens, Else: yyDollar[4].valExpr}
		}
	case 186:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1019
		{
			yyVAL.valExpr = nil
		}
	case 187:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1023
		{
			yyVAL.valExpr = yyDollar[1].expr
		}
	case 188:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1029
		{
			yyVAL.whens = []*When{yyDollar[1].when}
		}
	case 189:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1033
		{
			yyVAL.whens = append(yyDollar[1].whens, yyDollar[2].when)
		}
	case 190:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1039
		{
			yyVAL.when = &When{Cond: yyDollar[2].expr, Val: yyDollar[4].expr}
		}
	case 191:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1044
		{
			yyVAL.valExpr = nil
		}
	case 192:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1048
		{
			yyVAL.valExpr = yyDollar[2].expr
		}
	case 193:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1054
		{
			yyVAL.colName = &ColName{Name: yyDollar[1].colIdent}
		}
	case 194:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1058
		{
			yyVAL.colName = &ColName{Qualifier: &TableName{Name: yyDollar[1].tableIdent}, Name: yyDollar[3].colIdent}
		}
	case 195:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1062
		{
			yyVAL.colName = &ColName{Qualifier: &TableName{Qualifier: yyDollar[1].tableIdent, Name: yyDollar[3].tableIdent}, Name: yyDollar[5].colIdent}
		}
	case 196:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1068
		{
			yyVAL.valExpr = NewStrVal(yyDollar[1].bytes)
		}
	case 197:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1072
		{
			yyVAL.valExpr = NewHexVal(yyDollar[1].bytes)
		}
	case 198:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1076
		{
			yyVAL.valExpr = NewIntVal(yyDollar[1].bytes)
		}
	case 199:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1080
		{
			yyVAL.valExpr = NewFloatVal(yyDollar[1].bytes)
		}
	case 200:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1084
		{
			yyVAL.valExpr = NewHexNum(yyDollar[1].bytes)
		}
	case 201:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1088
		{
			yyVAL.valExpr = NewValArg(yyDollar[1].bytes)
		}
	case 202:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1092
		{
			yyVAL.valExpr = &NullVal{}
		}
	case 203:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1098
		{
			// TODO(sougou): Deprecate this construct.
			if yyDollar[1].colIdent.Lowered() != "value" {
				yylex.Error("expecting value after next")
				return 1
			}
			yyVAL.valExpr = NewIntVal([]byte("1"))
		}
	case 204:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1107
		{
			yyVAL.valExpr = NewIntVal(yyDollar[1].bytes)
		}
	case 205:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1111
		{
			yyVAL.valExpr = NewValArg(yyDollar[1].bytes)
		}
	case 206:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1116
		{
			yyVAL.valExprs = nil
		}
	case 207:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1120
		{
			yyVAL.valExprs = yyDollar[3].valExprs
		}
	case 208:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1125
		{
			yyVAL.boolExpr = nil
		}
	case 209:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1129
		{
			yyVAL.boolExpr = yyDollar[2].expr
		}
	case 210:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1134
		{
			yyVAL.orderBy = nil
		}
	case 211:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1138
		{
			yyVAL.orderBy = yyDollar[3].orderBy
		}
	case 212:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1144
		{
			yyVAL.orderBy = OrderBy{yyDollar[1].order}
		}
	case 213:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1148
		{
			yyVAL.orderBy = append(yyDollar[1].orderBy, yyDollar[3].order)
		}
	case 214:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1154
		{
			yyVAL.order = &Order{Expr: yyDollar[1].expr, Direction: yyDollar[2].str}
		}
	case 215:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1159
		{
			yyVAL.str = AscScr
		}
	case 216:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1163
		{
			yyVAL.str = AscScr
		}
	case 217:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1167
		{
			yyVAL.str = DescScr
		}
	case 218:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1172
		{
			yyVAL.limit = nil
		}
	case 219:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1176
		{
			yyVAL.limit = &Limit{Rowcount: yyDollar[2].expr}
		}
	case 220:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1180
		{
			yyVAL.limit = &Limit{Offset: yyDollar[2].expr, Rowcount: yyDollar[4].expr}
		}
	case 221:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1184
		{
			yyVAL.limit = &Limit{Offset: yyDollar[4].expr, Rowcount: yyDollar[2].expr}
		}
	case 222:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1189
		{
			yyVAL.str = ""
		}
	case 223:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1193
		{
			yyVAL.str = ForUpdateStr
		}
	case 224:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1197
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
	case 225:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1210
		{
			yyVAL.columns = nil
		}
	case 226:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1214
		{
			yyVAL.columns = yyDollar[2].columns
		}
	case 227:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1220
		{
			yyVAL.columns = Columns{yyDollar[1].colIdent}
		}
	case 228:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1224
		{
			yyVAL.columns = append(yyVAL.columns, yyDollar[3].colIdent)
		}
	case 229:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1229
		{
			yyVAL.updateExprs = nil
		}
	case 230:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1233
		{
			yyVAL.updateExprs = yyDollar[5].updateExprs
		}
	case 231:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1239
		{
			yyVAL.insRows = yyDollar[2].values
		}
	case 232:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1243
		{
			yyVAL.insRows = yyDollar[1].selStmt
		}
	case 233:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1249
		{
			yyVAL.values = Values{yyDollar[1].valTuple}
		}
	case 234:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1253
		{
			yyVAL.values = append(yyDollar[1].values, yyDollar[3].valTuple)
		}
	case 235:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1259
		{
			yyVAL.valTuple = ValTuple(yyDollar[2].valExprs)
		}
	case 236:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1265
		{
			yyVAL.updateExprs = UpdateExprs{yyDollar[1].updateExpr}
		}
	case 237:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1269
		{
			yyVAL.updateExprs = append(yyDollar[1].updateExprs, yyDollar[3].updateExpr)
		}
	case 238:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1275
		{
			yyVAL.updateExpr = &UpdateExpr{Name: yyDollar[1].colIdent, Expr: yyDollar[3].expr}
		}
	case 241:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1284
		{
			yyVAL.byt = 0
		}
	case 242:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1286
		{
			yyVAL.byt = 1
		}
	case 243:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1289
		{
			yyVAL.empty = struct{}{}
		}
	case 244:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1291
		{
			yyVAL.empty = struct{}{}
		}
	case 245:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1294
		{
			yyVAL.str = ""
		}
	case 246:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1296
		{
			yyVAL.str = IgnoreStr
		}
	case 247:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1300
		{
			yyVAL.empty = struct{}{}
		}
	case 248:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1302
		{
			yyVAL.empty = struct{}{}
		}
	case 249:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1304
		{
			yyVAL.empty = struct{}{}
		}
	case 250:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1306
		{
			yyVAL.empty = struct{}{}
		}
	case 251:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1308
		{
			yyVAL.empty = struct{}{}
		}
	case 252:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1310
		{
			yyVAL.empty = struct{}{}
		}
	case 253:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1313
		{
			yyVAL.empty = struct{}{}
		}
	case 254:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1315
		{
			yyVAL.empty = struct{}{}
		}
	case 255:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1318
		{
			yyVAL.empty = struct{}{}
		}
	case 256:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1320
		{
			yyVAL.empty = struct{}{}
		}
	case 257:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1323
		{
			yyVAL.empty = struct{}{}
		}
	case 258:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1325
		{
			yyVAL.empty = struct{}{}
		}
	case 259:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1329
		{
			yyVAL.colIdent = NewColIdent(string(yyDollar[1].bytes))
		}
	case 260:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1335
		{
			yyVAL.tableIdent = NewTableIdent(string(yyDollar[1].bytes))
		}
	case 261:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1341
		{
			if incNesting(yylex) {
				yylex.Error("max nesting level reached")
				return 1
			}
		}
	case 262:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1350
		{
			decNesting(yylex)
		}
	case 263:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1355
		{
			forceEOF(yylex)
		}
	}
	goto yystack /* stack new state and value */
}
