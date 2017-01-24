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
	-1, 111,
	96, 262,
	-2, 261,
	-1, 125,
	46, 180,
	-2, 170,
}

const yyNprod = 266
const yyPrivate = 57344

var yyTokenNames []string
var yyStates []string

const yyLast = 1188

var yyAct = [...]int{

	123, 384, 453, 333, 62, 109, 117, 353, 264, 308,
	263, 228, 322, 282, 250, 118, 343, 262, 47, 275,
	324, 170, 190, 104, 115, 174, 165, 127, 105, 266,
	74, 227, 3, 67, 35, 64, 37, 180, 69, 41,
	38, 71, 259, 99, 48, 49, 14, 15, 16, 17,
	436, 178, 404, 406, 40, 81, 41, 435, 434, 50,
	68, 70, 58, 43, 44, 45, 46, 42, 386, 18,
	230, 231, 182, 360, 253, 194, 89, 103, 210, 211,
	212, 213, 207, 207, 216, 216, 216, 87, 96, 64,
	98, 90, 64, 163, 63, 458, 195, 197, 196, 195,
	114, 92, 196, 195, 418, 416, 65, 161, 192, 65,
	225, 197, 171, 111, 197, 323, 162, 405, 197, 224,
	226, 94, 185, 177, 179, 176, 214, 215, 208, 209,
	210, 211, 212, 213, 207, 114, 114, 216, 129, 19,
	20, 22, 21, 23, 238, 236, 237, 410, 285, 181,
	239, 270, 24, 25, 26, 323, 84, 376, 276, 278,
	279, 64, 248, 277, 60, 246, 131, 130, 132, 133,
	134, 135, 252, 111, 136, 256, 208, 209, 210, 211,
	212, 213, 207, 289, 242, 216, 167, 95, 114, 110,
	249, 245, 192, 309, 257, 272, 75, 287, 288, 286,
	91, 273, 274, 168, 267, 129, 196, 195, 60, 114,
	268, 425, 426, 260, 269, 284, 261, 114, 114, 311,
	354, 283, 197, 423, 293, 229, 280, 361, 362, 363,
	232, 233, 234, 235, 379, 191, 315, 310, 312, 65,
	341, 60, 82, 356, 86, 83, 316, 319, 60, 129,
	356, 327, 430, 14, 272, 313, 314, 330, 114, 114,
	317, 320, 241, 65, 332, 193, 328, 329, 60, 206,
	205, 214, 215, 208, 209, 210, 211, 212, 213, 207,
	325, 254, 216, 306, 267, 307, 359, 65, 28, 193,
	268, 460, 309, 129, 255, 364, 60, 325, 110, 91,
	365, 79, 284, 187, 309, 311, 309, 166, 283, 281,
	341, 309, 290, 291, 292, 102, 294, 295, 296, 297,
	298, 299, 300, 301, 302, 303, 304, 371, 433, 377,
	373, 309, 380, 188, 375, 432, 381, 331, 309, 73,
	114, 399, 372, 91, 397, 114, 400, 110, 110, 398,
	160, 392, 391, 394, 393, 396, 387, 267, 267, 267,
	267, 411, 407, 268, 268, 268, 268, 129, 402, 409,
	187, 395, 14, 401, 412, 349, 350, 345, 348, 349,
	350, 346, 415, 347, 351, 76, 420, 411, 158, 157,
	88, 55, 419, 451, 254, 428, 39, 244, 366, 367,
	368, 328, 429, 427, 54, 452, 378, 172, 101, 358,
	334, 114, 205, 214, 215, 208, 209, 210, 211, 212,
	213, 207, 370, 390, 216, 442, 51, 52, 57, 110,
	156, 443, 335, 251, 389, 169, 444, 445, 155, 254,
	64, 340, 166, 61, 449, 382, 385, 454, 454, 454,
	455, 456, 114, 114, 457, 439, 446, 447, 448, 463,
	14, 464, 28, 459, 465, 461, 462, 29, 59, 345,
	348, 349, 350, 346, 30, 347, 351, 1, 72, 431,
	414, 305, 77, 31, 32, 33, 34, 417, 357, 352,
	189, 173, 36, 59, 258, 175, 422, 59, 66, 154,
	247, 254, 93, 159, 450, 424, 97, 383, 125, 100,
	124, 318, 388, 128, 108, 339, 374, 240, 321, 126,
	116, 326, 59, 80, 437, 164, 421, 243, 142, 438,
	198, 112, 440, 441, 385, 183, 403, 129, 184, 309,
	111, 131, 130, 132, 133, 134, 135, 85, 344, 136,
	152, 153, 342, 265, 113, 186, 151, 107, 78, 53,
	206, 205, 214, 215, 208, 209, 210, 211, 212, 213,
	207, 27, 56, 216, 13, 12, 119, 120, 106, 11,
	10, 140, 9, 121, 8, 7, 122, 6, 5, 4,
	59, 2, 0, 0, 0, 0, 0, 0, 0, 0,
	137, 0, 0, 0, 0, 0, 143, 138, 144, 139,
	145, 149, 150, 148, 147, 146, 141, 0, 0, 0,
	128, 0, 0, 108, 59, 0, 0, 0, 0, 0,
	271, 0, 0, 0, 0, 142, 0, 0, 0, 0,
	0, 0, 0, 0, 129, 0, 309, 111, 131, 130,
	132, 133, 134, 135, 0, 0, 136, 152, 153, 0,
	0, 113, 0, 151, 0, 0, 0, 0, 0, 0,
	0, 0, 108, 108, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 119, 120, 106, 0, 0, 140, 336,
	121, 337, 0, 122, 338, 0, 0, 0, 0, 0,
	65, 0, 355, 0, 59, 0, 0, 137, 0, 0,
	0, 0, 0, 143, 138, 144, 139, 145, 149, 150,
	148, 147, 146, 141, 0, 0, 0, 0, 0, 0,
	0, 128, 206, 205, 214, 215, 208, 209, 210, 211,
	212, 213, 207, 0, 0, 216, 142, 0, 0, 0,
	0, 0, 0, 0, 108, 129, 0, 0, 111, 131,
	130, 132, 133, 134, 135, 0, 0, 136, 152, 153,
	0, 0, 113, 0, 151, 0, 0, 59, 59, 59,
	59, 0, 0, 0, 0, 0, 0, 413, 0, 0,
	355, 0, 0, 408, 119, 120, 106, 0, 0, 140,
	0, 121, 0, 0, 122, 14, 206, 205, 214, 215,
	208, 209, 210, 211, 212, 213, 207, 0, 137, 216,
	0, 128, 0, 0, 143, 138, 144, 139, 145, 149,
	150, 148, 147, 146, 141, 0, 142, 0, 0, 0,
	0, 0, 0, 0, 0, 129, 0, 0, 111, 131,
	130, 132, 133, 134, 135, 0, 0, 136, 152, 153,
	0, 0, 113, 0, 151, 0, 0, 0, 128, 206,
	205, 214, 215, 208, 209, 210, 211, 212, 213, 207,
	0, 0, 216, 142, 119, 120, 0, 0, 0, 140,
	0, 121, 129, 0, 122, 111, 131, 130, 132, 133,
	134, 135, 0, 0, 136, 152, 153, 0, 137, 113,
	0, 151, 0, 0, 143, 138, 144, 139, 145, 149,
	150, 148, 147, 146, 141, 0, 0, 0, 0, 0,
	142, 119, 120, 0, 0, 0, 140, 0, 121, 129,
	0, 122, 111, 131, 130, 132, 133, 134, 135, 0,
	0, 136, 152, 153, 0, 137, 0, 0, 151, 0,
	0, 143, 138, 144, 139, 145, 149, 150, 148, 147,
	146, 141, 369, 0, 0, 0, 0, 0, 119, 120,
	0, 0, 0, 140, 0, 121, 0, 0, 122, 14,
	0, 206, 205, 214, 215, 208, 209, 210, 211, 212,
	213, 207, 137, 0, 216, 0, 0, 0, 143, 138,
	144, 139, 145, 149, 150, 148, 147, 146, 141, 0,
	142, 0, 0, 0, 0, 0, 0, 0, 0, 129,
	0, 0, 111, 131, 130, 132, 133, 134, 135, 0,
	0, 136, 0, 0, 0, 0, 0, 0, 151, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 142, 119, 120,
	0, 0, 0, 140, 0, 121, 129, 0, 122, 111,
	131, 130, 132, 133, 134, 135, 0, 0, 136, 0,
	0, 0, 137, 0, 0, 151, 0, 0, 143, 138,
	144, 139, 145, 149, 150, 148, 147, 146, 141, 0,
	0, 0, 0, 0, 0, 119, 120, 0, 0, 0,
	140, 0, 121, 0, 0, 122, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 137,
	0, 0, 0, 0, 0, 143, 138, 144, 139, 145,
	149, 150, 148, 147, 146, 141, 200, 203, 0, 0,
	0, 0, 0, 217, 218, 219, 220, 221, 222, 223,
	204, 201, 202, 199, 206, 205, 214, 215, 208, 209,
	210, 211, 212, 213, 207, 0, 0, 216,
}
var yyPact = [...]int{

	40, -1000, -1000, 457, -1000, -1000, -1000, -1000, -1000, -1000,
	-1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, -70,
	-52, -37, -41, -38, -1000, -1000, -1000, 454, 407, 371,
	-1000, -69, 115, 433, 57, -76, -45, 57, -1000, -43,
	57, -1000, 115, -79, 147, -79, 115, -1000, -1000, -1000,
	-1000, -1000, -1000, 265, 190, -1000, 99, 219, 361, -20,
	-1000, 115, 153, -1000, 31, -1000, 115, 58, 138, -1000,
	115, -1000, -64, 115, 386, 270, 57, -1000, 709, -1000,
	420, -1000, 358, 357, -1000, 321, 115, -1000, 57, 115,
	431, 57, 1030, -1000, 385, -86, -1000, 23, -1000, 115,
	-1000, -1000, 115, -1000, 323, -1000, -1000, 214, -21, 41,
	1093, -1000, -1000, 846, 799, -1000, -27, -1000, -1000, 1030,
	1030, 1030, 1030, 203, 203, -1000, -1000, -1000, 203, -1000,
	-1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000,
	-1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000,
	-1000, 1030, -1000, -1000, 115, -1000, -1000, -1000, -1000, 366,
	57, 57, -1000, 296, -1000, 419, 846, -1000, 788, -22,
	983, -1000, -1000, 249, 57, -1000, -65, -1000, -1000, -1000,
	-1000, -1000, -1000, -1000, -1000, -1000, 431, 709, 159, -1000,
	-1000, 238, -1000, -1000, 64, 846, 846, 100, 893, 92,
	119, 1030, 1030, 1030, 100, 1030, 1030, 1030, 1030, 1030,
	1030, 1030, 1030, 1030, 1030, 1030, 234, -1000, -1000, -1000,
	-1000, -1000, -1000, -1000, 20, 1093, 145, 283, 258, 1093,
	116, 116, -10, -10, -10, 651, 491, 598, -1000, 454,
	49, 788, -1000, 235, 203, 457, 252, 290, -1000, 419,
	394, 417, 41, 124, 788, 115, -1000, -1000, 115, -1000,
	429, -1000, 193, 342, -1000, -1000, 199, 388, 247, -1000,
	-1000, -23, -1000, 20, 34, -1000, -1000, 169, -1000, -1000,
	-1000, 788, -1000, 983, -1000, -1000, 92, 1030, 1030, 1030,
	788, 788, 910, -1000, 43, 330, -10, -9, -9, -8,
	-8, -8, -8, 91, 91, -1000, -1000, -1000, -1000, -1000,
	-1000, 1030, -1000, -1000, -1000, -1000, -1000, 256, 709, -1000,
	256, 89, -1000, 846, -1000, 380, 187, -1000, 1030, -1000,
	-1000, 57, 394, -1000, 1030, 1030, -28, -1000, -1000, 421,
	408, 159, 159, 159, 159, -1000, 336, 320, -1000, 309,
	306, 338, 9, -1000, 192, -1000, -1000, 115, -1000, 263,
	60, -1000, -1000, -1000, 258, -1000, 788, 788, 725, 1030,
	788, -1000, 256, -1000, 36, -1000, 1030, 37, 365, 203,
	-1000, -1000, 479, 176, -1000, 188, 57, -1000, 419, 846,
	1030, 342, 207, 434, -1000, -1000, -1000, -1000, 300, -1000,
	293, -1000, -1000, -1000, -47, -48, -55, -1000, -1000, -1000,
	-1000, -1000, -1000, 1030, 788, -1000, -1000, 788, 1030, 447,
	-1000, 1030, 1030, 1030, -1000, -1000, -1000, 394, 41, 172,
	846, 846, -1000, -1000, 203, 203, 203, 788, 788, 57,
	788, 788, -1000, 375, 41, 41, 57, 57, 57, 153,
	-1000, 446, 15, 244, -1000, 244, 244, -1000, 57, -1000,
	57, -1000, -1000, 57, -1000, -1000,
}
var yyPgo = [...]int{

	0, 591, 31, 589, 588, 587, 585, 584, 582, 580,
	579, 575, 574, 467, 572, 571, 559, 558, 23, 28,
	557, 555, 17, 10, 8, 553, 552, 16, 548, 29,
	547, 536, 2, 26, 5, 531, 27, 530, 527, 24,
	110, 523, 19, 13, 11, 521, 6, 15, 520, 519,
	518, 12, 517, 516, 515, 512, 510, 508, 14, 507,
	1, 505, 3, 504, 503, 500, 20, 4, 94, 499,
	396, 339, 498, 495, 494, 492, 491, 0, 22, 490,
	435, 7, 489, 488, 18, 481, 477, 474, 21, 9,
}
var yyR1 = [...]int{

	0, 86, 1, 1, 1, 1, 1, 1, 1, 1,
	1, 1, 1, 2, 2, 2, 3, 3, 4, 5,
	6, 7, 7, 7, 8, 8, 8, 9, 10, 10,
	10, 11, 12, 12, 12, 87, 13, 14, 14, 15,
	15, 15, 16, 16, 17, 17, 18, 18, 19, 19,
	19, 19, 20, 20, 79, 79, 79, 78, 78, 21,
	21, 22, 22, 23, 23, 24, 24, 24, 25, 25,
	25, 25, 83, 83, 82, 82, 82, 81, 81, 26,
	26, 26, 26, 27, 27, 27, 27, 28, 28, 30,
	30, 29, 29, 31, 31, 31, 31, 32, 32, 33,
	33, 34, 34, 34, 34, 34, 34, 36, 36, 35,
	35, 35, 35, 35, 35, 35, 35, 35, 35, 35,
	35, 35, 42, 42, 42, 42, 42, 42, 37, 37,
	37, 37, 37, 37, 37, 43, 43, 43, 47, 44,
	44, 85, 85, 40, 40, 40, 40, 40, 40, 40,
	40, 40, 40, 40, 40, 40, 40, 40, 40, 40,
	40, 40, 40, 40, 40, 40, 40, 40, 40, 40,
	40, 40, 57, 57, 57, 57, 57, 57, 57, 57,
	56, 56, 56, 56, 56, 56, 56, 49, 52, 52,
	50, 50, 51, 53, 53, 48, 48, 48, 39, 39,
	39, 39, 39, 39, 39, 41, 41, 41, 54, 54,
	55, 55, 58, 58, 59, 59, 60, 61, 61, 61,
	62, 62, 62, 62, 63, 63, 63, 64, 64, 65,
	65, 66, 66, 38, 38, 45, 45, 46, 67, 67,
	68, 69, 69, 71, 71, 72, 72, 70, 70, 73,
	73, 73, 73, 73, 73, 74, 74, 75, 75, 76,
	76, 77, 80, 88, 89, 84,
}
var yyR2 = [...]int{

	0, 1, 1, 1, 1, 1, 1, 1, 1, 1,
	1, 1, 1, 12, 6, 3, 7, 7, 8, 7,
	3, 5, 8, 4, 6, 7, 4, 5, 4, 5,
	5, 3, 2, 2, 2, 0, 2, 0, 2, 1,
	2, 2, 0, 1, 0, 1, 1, 3, 1, 2,
	3, 5, 1, 1, 0, 1, 2, 1, 1, 0,
	2, 1, 3, 1, 1, 3, 3, 3, 3, 5,
	5, 3, 0, 1, 0, 1, 2, 1, 1, 1,
	2, 2, 1, 2, 3, 2, 3, 2, 2, 2,
	1, 1, 3, 0, 5, 5, 5, 1, 3, 0,
	2, 1, 3, 3, 2, 3, 3, 1, 1, 1,
	3, 3, 3, 4, 3, 4, 3, 4, 5, 6,
	3, 2, 1, 2, 1, 2, 1, 2, 1, 1,
	1, 1, 1, 1, 1, 3, 1, 1, 3, 1,
	3, 1, 1, 1, 1, 1, 1, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 2, 2, 2, 3, 3, 4, 5, 3, 4,
	1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
	1, 1, 1, 1, 1, 1, 1, 5, 0, 1,
	1, 2, 4, 0, 2, 1, 3, 5, 1, 1,
	1, 1, 1, 1, 1, 1, 2, 2, 0, 3,
	0, 2, 0, 3, 1, 3, 2, 0, 1, 1,
	0, 2, 4, 4, 0, 2, 4, 0, 3, 1,
	3, 0, 5, 2, 1, 1, 3, 3, 1, 3,
	3, 1, 1, 0, 2, 0, 3, 0, 1, 1,
	1, 1, 1, 1, 1, 0, 1, 0, 1, 0,
	2, 1, 1, 1, 1, 0,
}
var yyChk = [...]int{

	-1000, -86, -1, -2, -3, -4, -5, -6, -7, -8,
	-9, -10, -11, -12, 6, 7, 8, 9, 29, 99,
	100, 102, 101, 103, 112, 113, 114, -15, 5, -13,
	-87, -13, -13, -13, -13, 104, -75, 106, 110, -70,
	106, 108, 104, 104, 105, 106, 104, -84, -84, -84,
	-2, 19, 20, -16, 33, 20, -14, -70, -29, -80,
	49, 10, -67, -68, -77, 49, -72, 109, 105, -77,
	104, -77, -80, -71, 109, 49, -71, -80, -17, 36,
	-41, -77, 52, 55, 57, -30, 25, -29, 29, 96,
	-29, 47, 70, -80, 63, 49, -84, -80, -84, 107,
	-80, 22, 45, -77, -18, -19, 87, -20, -80, -34,
	-40, 49, -35, 63, -88, -39, -48, -46, -47, 85,
	86, 92, 95, -77, -56, -57, -49, -36, 22, 46,
	51, 50, 52, 53, 54, 55, 58, 109, 116, 118,
	90, 125, 37, 115, 117, 119, 124, 123, 122, 120,
	121, 65, 59, 60, -69, 18, 10, 31, 31, -64,
	29, -88, -29, -67, -80, -33, 11, -68, -40, -80,
	-88, -84, 22, -76, 111, -73, 102, 100, 28, 101,
	14, 126, 49, -80, -80, -84, -21, 47, 10, -79,
	-78, 21, -77, 51, 96, 62, 61, 77, -37, 80,
	63, 78, 79, 64, 77, 82, 81, 91, 85, 86,
	87, 88, 89, 90, 83, 84, 94, 70, 71, 72,
	73, 74, 75, 76, -34, -40, -34, -2, -44, -40,
	97, 98, -40, -40, -40, -40, -88, -88, -47, -88,
	-52, -40, -29, -38, 31, -2, -67, -65, -77, -33,
	-58, 14, -34, 96, -40, 45, -77, -84, -74, 107,
	-33, -19, -22, -23, -24, -25, -29, -47, -88, -78,
	87, -80, -77, -34, -34, -42, 58, 63, 59, 60,
	-36, -40, -43, -88, -47, 56, 80, 78, 79, 64,
	-40, -40, -40, -42, -40, -40, -40, -40, -40, -40,
	-40, -40, -40, -40, -40, -85, 49, 51, -89, 48,
	-89, 47, -89, -39, -39, -77, -89, -18, 20, -89,
	-18, -50, -51, 66, -66, 45, -45, -46, -88, -66,
	-89, 47, -58, -62, 16, 15, -80, -80, -80, -54,
	12, 47, -26, -27, -28, 35, 39, 41, 36, 37,
	38, 42, -82, -81, 21, -80, 51, -83, 21, -22,
	96, 58, 59, 60, -44, -43, -40, -40, -40, 62,
	-40, -89, -18, -89, -53, -51, 68, -34, 26, 47,
	-77, -62, -40, -59, -60, -40, 96, -84, -55, 13,
	15, -23, -24, -23, -24, 35, 35, 35, 40, 35,
	40, 35, -27, -31, 43, 108, 44, -81, -80, -89,
	87, -77, -89, 62, -40, -89, 69, -40, 67, 27,
	-46, 47, 17, 47, -61, 23, 24, -58, -34, -44,
	45, 45, 35, 35, 105, 105, 105, -40, -40, 8,
	-40, -40, -60, -62, -34, -34, -88, -88, -88, -67,
	-63, 18, 30, -32, -77, -32, -32, 8, 80, -89,
	47, -89, -89, -77, -77, -77,
}
var yyDef = [...]int{

	0, -2, 1, 2, 3, 4, 5, 6, 7, 8,
	9, 10, 11, 12, 35, 35, 35, 35, 35, 257,
	247, 0, 0, 0, 265, 265, 265, 0, 39, 42,
	37, 247, 0, 0, 0, 245, 0, 0, 258, 0,
	0, 248, 0, 243, 0, 243, 0, 32, 33, 34,
	15, 40, 41, 44, 0, 43, 36, 0, 0, 91,
	262, 0, 20, 238, 0, 261, 0, 0, 0, 265,
	0, 265, 0, 0, 0, 0, 0, 31, 0, 45,
	0, 205, 0, 0, 38, 227, 0, 90, 0, 0,
	99, 0, 0, 265, 0, 259, 23, 0, 26, 0,
	28, 244, 0, 265, 59, 46, 48, 54, 0, 52,
	53, -2, 101, 0, 0, 143, 144, 145, 146, 0,
	0, 0, 0, 195, 0, -2, 171, 109, 0, 263,
	198, 199, 200, 201, 202, 203, 204, 181, 182, 183,
	184, 185, 186, 172, 173, 174, 175, 176, 177, 178,
	179, 188, 107, 108, 0, 241, 242, 206, 207, 0,
	0, 0, 89, 99, 92, 212, 0, 239, 240, 0,
	0, 21, 246, 0, 0, 265, 255, 249, 250, 251,
	252, 253, 254, 27, 29, 30, 99, 0, 0, 49,
	55, 0, 57, 58, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 128, 129, 130,
	131, 132, 133, 134, 104, 0, 0, 0, 0, 139,
	0, 0, 161, 162, 163, 0, 0, 0, 121, 0,
	0, 189, 14, 231, 0, 234, 231, 0, 229, 212,
	220, 0, 100, 0, 139, 0, 260, 24, 0, 256,
	208, 47, 60, 61, 63, 64, 74, 72, 0, 56,
	50, 0, 196, 102, 103, 106, 122, 0, 124, 126,
	110, 111, 112, 0, 136, 137, 0, 0, 0, 0,
	114, 116, 0, 120, 147, 148, 149, 150, 151, 152,
	153, 154, 155, 156, 157, 160, 141, 142, 105, 264,
	138, 0, 237, 158, 159, 164, 165, 0, 0, 168,
	0, 193, 190, 0, 16, 0, 233, 235, 0, 17,
	228, 0, 220, 19, 0, 0, 0, 265, 25, 210,
	0, 0, 0, 0, 0, 79, 0, 0, 82, 0,
	0, 0, 93, 75, 0, 77, 78, 0, 73, 0,
	0, 123, 125, 127, 0, 113, 115, 117, 0, 0,
	140, 166, 0, 169, 0, 191, 0, 0, 0, 0,
	230, 18, 221, 213, 214, 217, 0, 22, 212, 0,
	0, 62, 68, 0, 71, 80, 81, 83, 0, 85,
	0, 87, 88, 65, 0, 0, 0, 76, 66, 67,
	51, 197, 135, 0, 118, 167, 187, 194, 0, 0,
	236, 0, 0, 0, 216, 218, 219, 220, 211, 209,
	0, 0, 84, 86, 0, 0, 0, 119, 192, 0,
	222, 223, 215, 224, 69, 70, 0, 0, 0, 232,
	13, 0, 0, 0, 97, 0, 0, 225, 0, 94,
	0, 95, 96, 0, 98, 226,
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
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:414
		{
			yyVAL.expr = yyDollar[1].boolExpr
		}
	case 53:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:418
		{
			yyVAL.expr = yyDollar[1].valExpr
		}
	case 54:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:423
		{
			yyVAL.colIdent = ColIdent{}
		}
	case 55:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:427
		{
			yyVAL.colIdent = yyDollar[1].colIdent
		}
	case 56:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:431
		{
			yyVAL.colIdent = yyDollar[2].colIdent
		}
	case 58:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:438
		{
			yyVAL.colIdent = NewColIdent(string(yyDollar[1].bytes))
		}
	case 59:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:443
		{
			yyVAL.tableExprs = TableExprs{&AliasedTableExpr{Expr: &TableName{Name: NewTableIdent("dual")}}}
		}
	case 60:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:447
		{
			yyVAL.tableExprs = yyDollar[2].tableExprs
		}
	case 61:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:453
		{
			yyVAL.tableExprs = TableExprs{yyDollar[1].tableExpr}
		}
	case 62:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:457
		{
			yyVAL.tableExprs = append(yyVAL.tableExprs, yyDollar[3].tableExpr)
		}
	case 65:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:467
		{
			yyVAL.tableExpr = &AliasedTableExpr{Expr: yyDollar[1].tableName, As: yyDollar[2].tableIdent, Hints: yyDollar[3].indexHints}
		}
	case 66:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:471
		{
			yyVAL.tableExpr = &AliasedTableExpr{Expr: yyDollar[1].subquery, As: yyDollar[3].tableIdent}
		}
	case 67:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:475
		{
			yyVAL.tableExpr = &ParenTableExpr{Exprs: yyDollar[2].tableExprs}
		}
	case 68:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:488
		{
			yyVAL.tableExpr = &JoinTableExpr{LeftExpr: yyDollar[1].tableExpr, Join: yyDollar[2].str, RightExpr: yyDollar[3].tableExpr}
		}
	case 69:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:492
		{
			yyVAL.tableExpr = &JoinTableExpr{LeftExpr: yyDollar[1].tableExpr, Join: yyDollar[2].str, RightExpr: yyDollar[3].tableExpr, On: yyDollar[5].boolExpr}
		}
	case 70:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:496
		{
			yyVAL.tableExpr = &JoinTableExpr{LeftExpr: yyDollar[1].tableExpr, Join: yyDollar[2].str, RightExpr: yyDollar[3].tableExpr, On: yyDollar[5].boolExpr}
		}
	case 71:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:500
		{
			yyVAL.tableExpr = &JoinTableExpr{LeftExpr: yyDollar[1].tableExpr, Join: yyDollar[2].str, RightExpr: yyDollar[3].tableExpr}
		}
	case 72:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:505
		{
			yyVAL.empty = struct{}{}
		}
	case 73:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:507
		{
			yyVAL.empty = struct{}{}
		}
	case 74:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:510
		{
			yyVAL.tableIdent = NewTableIdent("")
		}
	case 75:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:514
		{
			yyVAL.tableIdent = yyDollar[1].tableIdent
		}
	case 76:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:518
		{
			yyVAL.tableIdent = yyDollar[2].tableIdent
		}
	case 78:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:525
		{
			yyVAL.tableIdent = NewTableIdent(string(yyDollar[1].bytes))
		}
	case 79:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:531
		{
			yyVAL.str = JoinStr
		}
	case 80:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:535
		{
			yyVAL.str = JoinStr
		}
	case 81:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:539
		{
			yyVAL.str = JoinStr
		}
	case 82:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:543
		{
			yyVAL.str = StraightJoinStr
		}
	case 83:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:549
		{
			yyVAL.str = LeftJoinStr
		}
	case 84:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:553
		{
			yyVAL.str = LeftJoinStr
		}
	case 85:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:557
		{
			yyVAL.str = RightJoinStr
		}
	case 86:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:561
		{
			yyVAL.str = RightJoinStr
		}
	case 87:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:567
		{
			yyVAL.str = NaturalJoinStr
		}
	case 88:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:571
		{
			if yyDollar[2].str == LeftJoinStr {
				yyVAL.str = NaturalLeftJoinStr
			} else {
				yyVAL.str = NaturalRightJoinStr
			}
		}
	case 89:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:581
		{
			yyVAL.tableName = yyDollar[2].tableName
		}
	case 90:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:585
		{
			yyVAL.tableName = yyDollar[1].tableName
		}
	case 91:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:591
		{
			yyVAL.tableName = &TableName{Name: yyDollar[1].tableIdent}
		}
	case 92:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:595
		{
			yyVAL.tableName = &TableName{Qualifier: yyDollar[1].tableIdent, Name: yyDollar[3].tableIdent}
		}
	case 93:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:600
		{
			yyVAL.indexHints = nil
		}
	case 94:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:604
		{
			yyVAL.indexHints = &IndexHints{Type: UseStr, Indexes: yyDollar[4].colIdents}
		}
	case 95:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:608
		{
			yyVAL.indexHints = &IndexHints{Type: IgnoreStr, Indexes: yyDollar[4].colIdents}
		}
	case 96:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:612
		{
			yyVAL.indexHints = &IndexHints{Type: ForceStr, Indexes: yyDollar[4].colIdents}
		}
	case 97:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:618
		{
			yyVAL.colIdents = []ColIdent{yyDollar[1].colIdent}
		}
	case 98:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:622
		{
			yyVAL.colIdents = append(yyDollar[1].colIdents, yyDollar[3].colIdent)
		}
	case 99:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:627
		{
			yyVAL.boolExpr = nil
		}
	case 100:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:631
		{
			yyVAL.boolExpr = yyDollar[2].boolExpr
		}
	case 102:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:638
		{
			yyVAL.boolExpr = &AndExpr{Left: yyDollar[1].boolExpr, Right: yyDollar[3].boolExpr}
		}
	case 103:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:642
		{
			yyVAL.boolExpr = &OrExpr{Left: yyDollar[1].boolExpr, Right: yyDollar[3].boolExpr}
		}
	case 104:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:646
		{
			yyVAL.boolExpr = &NotExpr{Expr: yyDollar[2].boolExpr}
		}
	case 105:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:650
		{
			yyVAL.boolExpr = &ParenBoolExpr{Expr: yyDollar[2].boolExpr}
		}
	case 106:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:654
		{
			yyVAL.boolExpr = &IsExpr{Operator: yyDollar[3].str, Expr: yyDollar[1].boolExpr}
		}
	case 107:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:660
		{
			yyVAL.boolVal = BoolVal(true)
		}
	case 108:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:664
		{
			yyVAL.boolVal = BoolVal(false)
		}
	case 109:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:670
		{
			yyVAL.boolExpr = yyDollar[1].boolVal
		}
	case 110:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:674
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: yyDollar[2].str, Right: yyDollar[3].boolVal}
		}
	case 111:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:678
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: yyDollar[2].str, Right: yyDollar[3].valExpr}
		}
	case 112:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:682
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: InStr, Right: yyDollar[3].colTuple}
		}
	case 113:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:686
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: NotInStr, Right: yyDollar[4].colTuple}
		}
	case 114:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:690
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: LikeStr, Right: yyDollar[3].valExpr}
		}
	case 115:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:694
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: NotLikeStr, Right: yyDollar[4].valExpr}
		}
	case 116:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:698
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: RegexpStr, Right: yyDollar[3].valExpr}
		}
	case 117:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:702
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: NotRegexpStr, Right: yyDollar[4].valExpr}
		}
	case 118:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:706
		{
			yyVAL.boolExpr = &RangeCond{Left: yyDollar[1].valExpr, Operator: BetweenStr, From: yyDollar[3].valExpr, To: yyDollar[5].valExpr}
		}
	case 119:
		yyDollar = yyS[yypt-6 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:710
		{
			yyVAL.boolExpr = &RangeCond{Left: yyDollar[1].valExpr, Operator: NotBetweenStr, From: yyDollar[4].valExpr, To: yyDollar[6].valExpr}
		}
	case 120:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:714
		{
			yyVAL.boolExpr = &IsExpr{Operator: yyDollar[3].str, Expr: yyDollar[1].valExpr}
		}
	case 121:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:718
		{
			yyVAL.boolExpr = &ExistsExpr{Subquery: yyDollar[2].subquery}
		}
	case 122:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:724
		{
			yyVAL.str = IsNullStr
		}
	case 123:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:728
		{
			yyVAL.str = IsNotNullStr
		}
	case 124:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:732
		{
			yyVAL.str = IsTrueStr
		}
	case 125:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:736
		{
			yyVAL.str = IsNotTrueStr
		}
	case 126:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:740
		{
			yyVAL.str = IsFalseStr
		}
	case 127:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:744
		{
			yyVAL.str = IsNotFalseStr
		}
	case 128:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:750
		{
			yyVAL.str = EqualStr
		}
	case 129:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:754
		{
			yyVAL.str = LessThanStr
		}
	case 130:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:758
		{
			yyVAL.str = GreaterThanStr
		}
	case 131:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:762
		{
			yyVAL.str = LessEqualStr
		}
	case 132:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:766
		{
			yyVAL.str = GreaterEqualStr
		}
	case 133:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:770
		{
			yyVAL.str = NotEqualStr
		}
	case 134:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:774
		{
			yyVAL.str = NullSafeEqualStr
		}
	case 135:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:780
		{
			yyVAL.colTuple = ValTuple(yyDollar[2].valExprs)
		}
	case 136:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:784
		{
			yyVAL.colTuple = yyDollar[1].subquery
		}
	case 137:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:788
		{
			yyVAL.colTuple = ListArg(yyDollar[1].bytes)
		}
	case 138:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:794
		{
			yyVAL.subquery = &Subquery{yyDollar[2].selStmt}
		}
	case 139:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:800
		{
			yyVAL.valExprs = ValExprs{yyDollar[1].valExpr}
		}
	case 140:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:804
		{
			yyVAL.valExprs = append(yyDollar[1].valExprs, yyDollar[3].valExpr)
		}
	case 141:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:810
		{
			yyVAL.str = string(yyDollar[1].bytes)
		}
	case 142:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:814
		{
			yyVAL.str = string(yyDollar[1].bytes)
		}
	case 143:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:820
		{
			yyVAL.valExpr = yyDollar[1].valExpr
		}
	case 144:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:824
		{
			yyVAL.valExpr = yyDollar[1].colName
		}
	case 145:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:828
		{
			yyVAL.valExpr = yyDollar[1].valTuple
		}
	case 146:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:832
		{
			yyVAL.valExpr = yyDollar[1].subquery
		}
	case 147:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:836
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: BitAndStr, Right: yyDollar[3].valExpr}
		}
	case 148:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:840
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: BitOrStr, Right: yyDollar[3].valExpr}
		}
	case 149:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:844
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: BitXorStr, Right: yyDollar[3].valExpr}
		}
	case 150:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:848
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: PlusStr, Right: yyDollar[3].valExpr}
		}
	case 151:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:852
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: MinusStr, Right: yyDollar[3].valExpr}
		}
	case 152:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:856
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: MultStr, Right: yyDollar[3].valExpr}
		}
	case 153:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:860
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: DivStr, Right: yyDollar[3].valExpr}
		}
	case 154:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:864
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: ModStr, Right: yyDollar[3].valExpr}
		}
	case 155:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:868
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: ModStr, Right: yyDollar[3].valExpr}
		}
	case 156:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:872
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: ShiftLeftStr, Right: yyDollar[3].valExpr}
		}
	case 157:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:876
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: ShiftRightStr, Right: yyDollar[3].valExpr}
		}
	case 158:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:880
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].colName, Operator: JSONExtractOp, Right: yyDollar[3].valExpr}
		}
	case 159:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:884
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].colName, Operator: JSONUnquoteExtractOp, Right: yyDollar[3].valExpr}
		}
	case 160:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:888
		{
			yyVAL.valExpr = &CollateExpr{Expr: yyDollar[1].valExpr, Charset: yyDollar[3].str}
		}
	case 161:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:892
		{
			if num, ok := yyDollar[2].valExpr.(*SQLVal); ok && num.Type == IntVal {
				yyVAL.valExpr = num
			} else {
				yyVAL.valExpr = &UnaryExpr{Operator: UPlusStr, Expr: yyDollar[2].valExpr}
			}
		}
	case 162:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:900
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
	case 163:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:914
		{
			yyVAL.valExpr = &UnaryExpr{Operator: TildaStr, Expr: yyDollar[2].valExpr}
		}
	case 164:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:918
		{
			// This rule prevents the usage of INTERVAL
			// as a function. If support is needed for that,
			// we'll need to revisit this. The solution
			// will be non-trivial because of grammar conflicts.
			yyVAL.valExpr = &IntervalExpr{Expr: yyDollar[2].valExpr, Unit: yyDollar[3].colIdent}
		}
	case 165:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:926
		{
			yyVAL.valExpr = &FuncExpr{Name: yyDollar[1].colIdent}
		}
	case 166:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:930
		{
			yyVAL.valExpr = &FuncExpr{Name: yyDollar[1].colIdent, Exprs: yyDollar[3].selectExprs}
		}
	case 167:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:934
		{
			yyVAL.valExpr = &FuncExpr{Name: yyDollar[1].colIdent, Distinct: true, Exprs: yyDollar[4].selectExprs}
		}
	case 168:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:938
		{
			yyVAL.valExpr = &FuncExpr{Name: yyDollar[1].colIdent}
		}
	case 169:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:942
		{
			yyVAL.valExpr = &FuncExpr{Name: yyDollar[1].colIdent, Exprs: yyDollar[3].selectExprs}
		}
	case 170:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:946
		{
			yyVAL.valExpr = &FuncExpr{Name: yyDollar[1].colIdent}
		}
	case 171:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:950
		{
			yyVAL.valExpr = yyDollar[1].caseExpr
		}
	case 172:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:958
		{
			yyVAL.colIdent = NewColIdent("current_timestamp")
		}
	case 173:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:962
		{
			yyVAL.colIdent = NewColIdent("current_date")
		}
	case 174:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:966
		{
			yyVAL.colIdent = NewColIdent("current_time")
		}
	case 175:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:970
		{
			yyVAL.colIdent = NewColIdent("utc_timestamp")
		}
	case 176:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:974
		{
			yyVAL.colIdent = NewColIdent("utc_time")
		}
	case 177:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:978
		{
			yyVAL.colIdent = NewColIdent("utc_date")
		}
	case 178:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:982
		{
			yyVAL.colIdent = NewColIdent("localtime")
		}
	case 179:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:986
		{
			yyVAL.colIdent = NewColIdent("localtimestamp")
		}
	case 181:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:995
		{
			yyVAL.colIdent = NewColIdent("if")
		}
	case 182:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:999
		{
			yyVAL.colIdent = NewColIdent("database")
		}
	case 183:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1003
		{
			yyVAL.colIdent = NewColIdent("unix_timestamp")
		}
	case 184:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1007
		{
			yyVAL.colIdent = NewColIdent("mod")
		}
	case 185:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1011
		{
			yyVAL.colIdent = NewColIdent("replace")
		}
	case 186:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1015
		{
			yyVAL.colIdent = NewColIdent("left")
		}
	case 187:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1021
		{
			yyVAL.caseExpr = &CaseExpr{Expr: yyDollar[2].valExpr, Whens: yyDollar[3].whens, Else: yyDollar[4].valExpr}
		}
	case 188:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1026
		{
			yyVAL.valExpr = nil
		}
	case 189:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1030
		{
			yyVAL.valExpr = yyDollar[1].valExpr
		}
	case 190:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1036
		{
			yyVAL.whens = []*When{yyDollar[1].when}
		}
	case 191:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1040
		{
			yyVAL.whens = append(yyDollar[1].whens, yyDollar[2].when)
		}
	case 192:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1046
		{
			yyVAL.when = &When{Cond: yyDollar[2].boolExpr, Val: yyDollar[4].valExpr}
		}
	case 193:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1051
		{
			yyVAL.valExpr = nil
		}
	case 194:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1055
		{
			yyVAL.valExpr = yyDollar[2].valExpr
		}
	case 195:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1061
		{
			yyVAL.colName = &ColName{Name: yyDollar[1].colIdent}
		}
	case 196:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1065
		{
			yyVAL.colName = &ColName{Qualifier: &TableName{Name: yyDollar[1].tableIdent}, Name: yyDollar[3].colIdent}
		}
	case 197:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1069
		{
			yyVAL.colName = &ColName{Qualifier: &TableName{Qualifier: yyDollar[1].tableIdent, Name: yyDollar[3].tableIdent}, Name: yyDollar[5].colIdent}
		}
	case 198:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1075
		{
			yyVAL.valExpr = NewStrVal(yyDollar[1].bytes)
		}
	case 199:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1079
		{
			yyVAL.valExpr = NewHexVal(yyDollar[1].bytes)
		}
	case 200:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1083
		{
			yyVAL.valExpr = NewIntVal(yyDollar[1].bytes)
		}
	case 201:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1087
		{
			yyVAL.valExpr = NewFloatVal(yyDollar[1].bytes)
		}
	case 202:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1091
		{
			yyVAL.valExpr = NewHexNum(yyDollar[1].bytes)
		}
	case 203:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1095
		{
			yyVAL.valExpr = NewValArg(yyDollar[1].bytes)
		}
	case 204:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1099
		{
			yyVAL.valExpr = &NullVal{}
		}
	case 205:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1105
		{
			// TODO(sougou): Deprecate this construct.
			if yyDollar[1].colIdent.Lowered() != "value" {
				yylex.Error("expecting value after next")
				return 1
			}
			yyVAL.valExpr = NewIntVal([]byte("1"))
		}
	case 206:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1114
		{
			yyVAL.valExpr = NewIntVal(yyDollar[1].bytes)
		}
	case 207:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1118
		{
			yyVAL.valExpr = NewValArg(yyDollar[1].bytes)
		}
	case 208:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1123
		{
			yyVAL.valExprs = nil
		}
	case 209:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1127
		{
			yyVAL.valExprs = yyDollar[3].valExprs
		}
	case 210:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1132
		{
			yyVAL.boolExpr = nil
		}
	case 211:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1136
		{
			yyVAL.boolExpr = yyDollar[2].boolExpr
		}
	case 212:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1141
		{
			yyVAL.orderBy = nil
		}
	case 213:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1145
		{
			yyVAL.orderBy = yyDollar[3].orderBy
		}
	case 214:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1151
		{
			yyVAL.orderBy = OrderBy{yyDollar[1].order}
		}
	case 215:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1155
		{
			yyVAL.orderBy = append(yyDollar[1].orderBy, yyDollar[3].order)
		}
	case 216:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1161
		{
			yyVAL.order = &Order{Expr: yyDollar[1].valExpr, Direction: yyDollar[2].str}
		}
	case 217:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1166
		{
			yyVAL.str = AscScr
		}
	case 218:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1170
		{
			yyVAL.str = AscScr
		}
	case 219:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1174
		{
			yyVAL.str = DescScr
		}
	case 220:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1179
		{
			yyVAL.limit = nil
		}
	case 221:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1183
		{
			yyVAL.limit = &Limit{Rowcount: yyDollar[2].valExpr}
		}
	case 222:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1187
		{
			yyVAL.limit = &Limit{Offset: yyDollar[2].valExpr, Rowcount: yyDollar[4].valExpr}
		}
	case 223:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1191
		{
			yyVAL.limit = &Limit{Offset: yyDollar[4].valExpr, Rowcount: yyDollar[2].valExpr}
		}
	case 224:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1196
		{
			yyVAL.str = ""
		}
	case 225:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1200
		{
			yyVAL.str = ForUpdateStr
		}
	case 226:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1204
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
	case 227:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1217
		{
			yyVAL.columns = nil
		}
	case 228:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1221
		{
			yyVAL.columns = yyDollar[2].columns
		}
	case 229:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1227
		{
			yyVAL.columns = Columns{yyDollar[1].colIdent}
		}
	case 230:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1231
		{
			yyVAL.columns = append(yyVAL.columns, yyDollar[3].colIdent)
		}
	case 231:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1236
		{
			yyVAL.updateExprs = nil
		}
	case 232:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1240
		{
			yyVAL.updateExprs = yyDollar[5].updateExprs
		}
	case 233:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1246
		{
			yyVAL.insRows = yyDollar[2].values
		}
	case 234:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1250
		{
			yyVAL.insRows = yyDollar[1].selStmt
		}
	case 235:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1256
		{
			yyVAL.values = Values{yyDollar[1].valTuple}
		}
	case 236:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1260
		{
			yyVAL.values = append(yyDollar[1].values, yyDollar[3].valTuple)
		}
	case 237:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1266
		{
			yyVAL.valTuple = ValTuple(yyDollar[2].valExprs)
		}
	case 238:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1272
		{
			yyVAL.updateExprs = UpdateExprs{yyDollar[1].updateExpr}
		}
	case 239:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1276
		{
			yyVAL.updateExprs = append(yyDollar[1].updateExprs, yyDollar[3].updateExpr)
		}
	case 240:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1282
		{
			yyVAL.updateExpr = &UpdateExpr{Name: yyDollar[1].colIdent, Expr: yyDollar[3].valExpr}
		}
	case 243:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1291
		{
			yyVAL.byt = 0
		}
	case 244:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1293
		{
			yyVAL.byt = 1
		}
	case 245:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1296
		{
			yyVAL.empty = struct{}{}
		}
	case 246:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1298
		{
			yyVAL.empty = struct{}{}
		}
	case 247:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1301
		{
			yyVAL.str = ""
		}
	case 248:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1303
		{
			yyVAL.str = IgnoreStr
		}
	case 249:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1307
		{
			yyVAL.empty = struct{}{}
		}
	case 250:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1309
		{
			yyVAL.empty = struct{}{}
		}
	case 251:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1311
		{
			yyVAL.empty = struct{}{}
		}
	case 252:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1313
		{
			yyVAL.empty = struct{}{}
		}
	case 253:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1315
		{
			yyVAL.empty = struct{}{}
		}
	case 254:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1317
		{
			yyVAL.empty = struct{}{}
		}
	case 255:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1320
		{
			yyVAL.empty = struct{}{}
		}
	case 256:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1322
		{
			yyVAL.empty = struct{}{}
		}
	case 257:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1325
		{
			yyVAL.empty = struct{}{}
		}
	case 258:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1327
		{
			yyVAL.empty = struct{}{}
		}
	case 259:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1330
		{
			yyVAL.empty = struct{}{}
		}
	case 260:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1332
		{
			yyVAL.empty = struct{}{}
		}
	case 261:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1336
		{
			yyVAL.colIdent = NewColIdent(string(yyDollar[1].bytes))
		}
	case 262:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1342
		{
			yyVAL.tableIdent = NewTableIdent(string(yyDollar[1].bytes))
		}
	case 263:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1348
		{
			if incNesting(yylex) {
				yylex.Error("max nesting level reached")
				return 1
			}
		}
	case 264:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1357
		{
			decNesting(yylex)
		}
	case 265:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1362
		{
			forceEOF(yylex)
		}
	}
	goto yystack /* stack new state and value */
}
