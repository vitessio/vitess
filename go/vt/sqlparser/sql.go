//line sql.y:6
package sqlparser

import __yyfmt__ "fmt"

//line sql.y:6
import "strings"

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

//line sql.y:36
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
	smTableExpr SimpleTableExpr
	tableName   *TableName
	indexHints  *IndexHints
	expr        Expr
	boolExpr    BoolExpr
	valExpr     ValExpr
	colTuple    ColTuple
	valExprs    ValExprs
	values      Values
	rowTuple    RowTuple
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
	sqlID       SQLName
	sqlIDs      []SQLName
}

const LEX_ERROR = 57346
const UNION = 57347
const MINUS = 57348
const EXCEPT = 57349
const INTERSECT = 57350
const SELECT = 57351
const INSERT = 57352
const UPDATE = 57353
const DELETE = 57354
const FROM = 57355
const WHERE = 57356
const GROUP = 57357
const HAVING = 57358
const ORDER = 57359
const BY = 57360
const LIMIT = 57361
const FOR = 57362
const ALL = 57363
const DISTINCT = 57364
const AS = 57365
const EXISTS = 57366
const ASC = 57367
const DESC = 57368
const INTO = 57369
const DUPLICATE = 57370
const KEY = 57371
const DEFAULT = 57372
const SET = 57373
const LOCK = 57374
const VALUES = 57375
const LAST_INSERT_ID = 57376
const NEXT = 57377
const VALUE = 57378
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
const STRING = 57391
const NUMBER = 57392
const VALUE_ARG = 57393
const LIST_ARG = 57394
const COMMENT = 57395
const NULL = 57396
const TRUE = 57397
const FALSE = 57398
const OR = 57399
const AND = 57400
const NOT = 57401
const BETWEEN = 57402
const CASE = 57403
const WHEN = 57404
const THEN = 57405
const ELSE = 57406
const LE = 57407
const GE = 57408
const NE = 57409
const NULL_SAFE_EQUAL = 57410
const IS = 57411
const LIKE = 57412
const REGEXP = 57413
const IN = 57414
const SHIFT_LEFT = 57415
const SHIFT_RIGHT = 57416
const UNARY = 57417
const INTERVAL = 57418
const END = 57419
const CREATE = 57420
const ALTER = 57421
const DROP = 57422
const RENAME = 57423
const ANALYZE = 57424
const TABLE = 57425
const INDEX = 57426
const VIEW = 57427
const TO = 57428
const IGNORE = 57429
const IF = 57430
const UNIQUE = 57431
const USING = 57432
const SHOW = 57433
const DESCRIBE = 57434
const EXPLAIN = 57435

var yyToknames = [...]string{
	"$end",
	"error",
	"$unk",
	"LEX_ERROR",
	"UNION",
	"MINUS",
	"EXCEPT",
	"INTERSECT",
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
	"STRING",
	"NUMBER",
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
	"'^'",
	"'~'",
	"UNARY",
	"INTERVAL",
	"'.'",
	"END",
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
	-1, 69,
	92, 221,
	-2, 220,
}

const yyNprod = 225
const yyPrivate = 57344

var yyTokenNames []string
var yyStates []string

const yyLast = 822

var yyAct = [...]int{

	100, 95, 167, 399, 94, 170, 279, 209, 64, 268,
	353, 319, 363, 83, 227, 249, 189, 210, 169, 3,
	261, 212, 221, 65, 208, 60, 197, 106, 84, 79,
	69, 107, 108, 109, 339, 341, 110, 71, 67, 50,
	44, 73, 66, 112, 76, 38, 53, 40, 43, 375,
	44, 41, 46, 47, 48, 374, 275, 113, 89, 128,
	373, 72, 96, 97, 75, 51, 52, 49, 98, 45,
	99, 350, 384, 385, 294, 138, 121, 88, 117, 151,
	74, 413, 132, 101, 139, 141, 250, 136, 74, 118,
	120, 262, 340, 310, 123, 171, 140, 139, 141, 172,
	174, 175, 176, 152, 153, 154, 155, 156, 151, 262,
	173, 141, 106, 125, 218, 182, 127, 67, 114, 230,
	67, 66, 193, 192, 66, 187, 150, 149, 157, 158,
	152, 153, 154, 155, 156, 151, 69, 89, 217, 193,
	62, 186, 124, 191, 106, 226, 290, 62, 235, 236,
	237, 80, 239, 240, 241, 242, 243, 244, 245, 246,
	247, 248, 194, 216, 140, 139, 92, 140, 139, 166,
	168, 238, 207, 352, 62, 234, 254, 255, 137, 141,
	89, 89, 141, 408, 250, 251, 253, 111, 232, 233,
	231, 257, 259, 256, 154, 155, 156, 151, 272, 295,
	296, 297, 92, 92, 267, 263, 74, 119, 222, 224,
	225, 177, 178, 223, 359, 250, 180, 219, 220, 252,
	281, 284, 285, 286, 282, 276, 283, 287, 134, 250,
	370, 254, 369, 382, 298, 300, 301, 302, 273, 293,
	14, 252, 250, 190, 214, 92, 299, 277, 250, 190,
	92, 92, 379, 106, 228, 304, 203, 28, 29, 30,
	31, 89, 354, 133, 119, 354, 67, 67, 270, 201,
	66, 317, 306, 305, 315, 307, 394, 318, 277, 106,
	78, 309, 62, 314, 119, 326, 265, 328, 92, 92,
	204, 271, 131, 179, 337, 334, 327, 266, 329, 134,
	335, 92, 250, 106, 372, 332, 348, 371, 331, 344,
	333, 330, 116, 351, 346, 356, 336, 378, 285, 286,
	57, 213, 349, 361, 364, 214, 357, 405, 42, 81,
	360, 229, 358, 56, 200, 202, 199, 14, 115, 406,
	311, 195, 228, 130, 292, 281, 284, 285, 286, 282,
	376, 283, 287, 54, 320, 377, 368, 321, 185, 269,
	67, 313, 365, 59, 380, 184, 367, 325, 190, 92,
	63, 254, 412, 92, 388, 68, 386, 403, 14, 33,
	1, 395, 291, 288, 396, 364, 135, 196, 214, 214,
	214, 214, 400, 400, 400, 397, 401, 402, 398, 39,
	274, 198, 213, 70, 67, 183, 316, 264, 66, 414,
	32, 61, 411, 404, 415, 407, 416, 409, 410, 229,
	383, 77, 362, 366, 324, 82, 34, 35, 36, 37,
	308, 87, 28, 29, 30, 31, 181, 260, 102, 61,
	355, 93, 312, 142, 90, 387, 122, 389, 390, 338,
	280, 126, 278, 211, 129, 149, 157, 158, 152, 153,
	154, 155, 156, 151, 381, 213, 213, 213, 213, 157,
	158, 152, 153, 154, 155, 156, 151, 86, 92, 55,
	92, 92, 27, 58, 391, 392, 393, 13, 258, 12,
	105, 61, 11, 188, 150, 149, 157, 158, 152, 153,
	154, 155, 156, 151, 205, 10, 9, 206, 8, 215,
	87, 7, 6, 5, 106, 4, 250, 69, 107, 108,
	109, 2, 0, 110, 103, 104, 0, 0, 91, 0,
	112, 0, 0, 0, 105, 0, 14, 0, 14, 15,
	16, 17, 0, 0, 0, 0, 0, 0, 0, 96,
	97, 85, 0, 87, 87, 98, 0, 99, 106, 215,
	18, 69, 107, 108, 109, 0, 0, 110, 103, 104,
	101, 0, 91, 0, 112, 106, 0, 0, 69, 107,
	108, 109, 0, 0, 110, 0, 0, 0, 289, 0,
	215, 112, 0, 96, 97, 85, 0, 0, 0, 98,
	0, 99, 0, 0, 0, 0, 0, 0, 0, 0,
	96, 97, 0, 0, 101, 0, 98, 0, 99, 0,
	0, 0, 0, 19, 20, 22, 21, 23, 0, 14,
	0, 101, 0, 0, 87, 0, 24, 25, 26, 0,
	0, 0, 0, 0, 105, 0, 0, 322, 0, 0,
	323, 0, 0, 215, 215, 215, 215, 0, 0, 0,
	0, 105, 0, 0, 0, 0, 342, 343, 106, 0,
	345, 69, 107, 108, 109, 0, 0, 110, 103, 104,
	0, 0, 91, 0, 112, 106, 0, 0, 69, 107,
	108, 109, 0, 0, 110, 103, 104, 0, 0, 91,
	0, 112, 0, 96, 97, 0, 0, 0, 0, 98,
	0, 99, 0, 0, 0, 0, 0, 0, 0, 0,
	96, 97, 0, 0, 101, 0, 98, 0, 99, 0,
	0, 0, 0, 0, 0, 0, 144, 147, 0, 0,
	0, 101, 159, 160, 161, 162, 163, 164, 165, 148,
	145, 146, 143, 150, 149, 157, 158, 152, 153, 154,
	155, 156, 151, 347, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 303, 74, 0, 0, 0, 0, 0,
	0, 150, 149, 157, 158, 152, 153, 154, 155, 156,
	151, 150, 149, 157, 158, 152, 153, 154, 155, 156,
	151, 0, 150, 149, 157, 158, 152, 153, 154, 155,
	156, 151, 150, 149, 157, 158, 152, 153, 154, 155,
	156, 151,
}
var yyPact = [...]int{

	529, -1000, -1000, 427, -1000, -1000, -1000, -1000, -1000, -1000,
	-1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, -54,
	-53, -30, -47, -32, -1000, -1000, -1000, 369, 332, -1000,
	-1000, -1000, 298, -1000, -63, 89, 357, 85, -67, -39,
	37, -1000, -35, 37, -1000, 89, -75, 100, -75, 89,
	-1000, -1000, -1000, -1000, -1000, 510, 37, -1000, 62, 311,
	281, -14, -1000, 89, 158, -1000, 22, -1000, -16, -1000,
	89, 32, 91, -1000, -1000, 89, -1000, -43, 89, 319,
	245, 37, -1000, 250, -1000, -1000, 155, -17, 104, 674,
	-1000, 637, 620, -1000, -1000, -1000, -21, -21, -21, -21,
	205, 205, -1000, -1000, -1000, 205, -1000, -1000, -1000, -1000,
	-1000, -1000, -21, 345, -1000, 89, 85, 89, 354, 85,
	-21, 37, -1000, 317, -80, -1000, 239, -1000, 89, -1000,
	-1000, 89, -1000, 96, 510, -1000, -1000, 37, 29, 637,
	637, 151, -21, 64, 112, -21, -21, -21, 151, -21,
	-21, -21, -21, -21, -21, -21, -21, -21, -21, -1000,
	-1000, -1000, -1000, -1000, -1000, -1000, 10, 674, 36, 252,
	192, 674, -1000, 527, -1000, -1000, 723, 466, 510, -1000,
	369, 44, 733, 89, -1000, -1000, 255, 235, -1000, 342,
	637, -1000, 733, -1000, -1000, -1000, 244, 37, -1000, -46,
	-1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, 229, 308,
	-1000, -1000, 123, 321, 231, -18, -1000, -1000, -1000, 10,
	23, -1000, -1000, 142, -1000, -1000, 733, -1000, 527, -1000,
	-1000, 64, -21, -21, -21, 733, 733, 712, -1000, 388,
	375, -1000, 109, 109, -9, -9, -9, 20, 20, -1000,
	-1000, -1000, -21, -1000, 733, -1000, -1000, 179, 510, 179,
	26, -1000, 637, -1000, 328, 85, 85, 342, 335, 339,
	104, 89, -1000, -1000, 89, -1000, 352, 96, 96, 96,
	96, -1000, 274, 271, -1000, 268, 258, 279, -11, -1000,
	89, 89, -1000, 198, 89, -1000, -1000, -1000, 192, -1000,
	733, 733, 702, -21, 733, -1000, 179, -1000, -22, -1000,
	-21, 107, 218, 205, 427, 215, 165, -1000, 335, -1000,
	-21, -21, -1000, -1000, 350, 338, 308, 185, 183, -1000,
	-1000, -1000, -1000, 270, -1000, 267, -1000, -1000, -1000, -40,
	-45, -51, -1000, -1000, -1000, -1000, -1000, -21, 733, -1000,
	-1000, 733, -21, -1000, 289, 203, -1000, -1000, -1000, 85,
	-1000, 415, 184, -1000, 47, -1000, 342, 637, -21, 637,
	637, -1000, -1000, 205, 205, 205, 733, 733, 247, 205,
	-1000, -21, -21, -1000, -1000, -1000, 335, 104, 170, 104,
	104, 37, 37, 37, 366, -1000, 733, -1000, 307, 134,
	-1000, 134, 134, 85, -1000, 361, 3, -1000, 37, -1000,
	-1000, 158, -1000, 37, -1000, 37, -1000,
}
var yyPgo = [...]int{

	0, 521, 18, 515, 513, 512, 511, 508, 506, 505,
	492, 489, 487, 410, 483, 482, 479, 13, 28, 477,
	24, 7, 17, 453, 452, 6, 450, 21, 25, 449,
	3, 16, 77, 444, 443, 442, 441, 2, 22, 14,
	5, 440, 1, 187, 4, 438, 437, 20, 436, 430,
	424, 423, 9, 422, 12, 420, 11, 413, 407, 406,
	10, 8, 23, 405, 328, 280, 403, 401, 400, 399,
	387, 0, 386, 375, 383, 382, 39, 380, 379, 110,
	15,
}
var yyR1 = [...]int{

	0, 77, 1, 1, 1, 1, 1, 1, 1, 1,
	1, 1, 1, 2, 2, 2, 3, 3, 4, 5,
	6, 7, 7, 7, 8, 8, 8, 9, 10, 10,
	10, 11, 12, 12, 12, 78, 13, 14, 14, 15,
	15, 15, 15, 15, 16, 16, 17, 17, 18, 18,
	18, 19, 19, 72, 72, 72, 20, 20, 21, 21,
	22, 22, 22, 23, 23, 23, 23, 75, 75, 74,
	74, 74, 24, 24, 24, 24, 25, 25, 25, 25,
	26, 26, 27, 27, 28, 28, 29, 29, 29, 29,
	30, 30, 31, 31, 32, 32, 32, 32, 32, 32,
	33, 33, 33, 33, 33, 33, 33, 33, 33, 33,
	33, 33, 33, 38, 38, 38, 38, 38, 38, 34,
	34, 34, 34, 34, 34, 34, 39, 39, 39, 43,
	40, 40, 37, 37, 37, 37, 37, 37, 37, 37,
	37, 37, 37, 37, 37, 37, 37, 37, 37, 37,
	37, 37, 37, 37, 45, 48, 48, 46, 46, 47,
	49, 49, 44, 44, 36, 36, 36, 36, 50, 50,
	51, 51, 52, 52, 53, 53, 54, 55, 55, 55,
	56, 56, 56, 57, 57, 57, 58, 58, 59, 59,
	60, 60, 35, 35, 41, 41, 42, 42, 61, 61,
	62, 63, 63, 65, 65, 66, 66, 64, 64, 67,
	67, 67, 67, 67, 68, 68, 69, 69, 70, 70,
	71, 73, 79, 80, 76,
}
var yyR2 = [...]int{

	0, 1, 1, 1, 1, 1, 1, 1, 1, 1,
	1, 1, 1, 12, 6, 3, 8, 8, 8, 7,
	3, 5, 8, 4, 6, 7, 4, 5, 4, 5,
	5, 3, 2, 2, 2, 0, 2, 0, 2, 1,
	2, 1, 1, 1, 0, 1, 1, 3, 1, 2,
	3, 1, 1, 0, 1, 2, 1, 3, 1, 1,
	3, 3, 3, 3, 5, 5, 3, 0, 1, 0,
	1, 2, 1, 2, 2, 1, 2, 3, 2, 3,
	2, 2, 1, 3, 1, 3, 0, 5, 5, 5,
	1, 3, 0, 2, 1, 3, 3, 2, 3, 3,
	1, 1, 3, 3, 4, 3, 4, 3, 4, 5,
	6, 3, 2, 1, 2, 1, 2, 1, 2, 1,
	1, 1, 1, 1, 1, 1, 3, 1, 1, 3,
	1, 3, 1, 1, 1, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 2, 2, 2, 3, 3,
	4, 5, 4, 1, 5, 0, 1, 1, 2, 4,
	0, 2, 1, 3, 1, 1, 1, 1, 0, 3,
	0, 2, 0, 3, 1, 3, 2, 0, 1, 1,
	0, 2, 4, 0, 2, 4, 0, 3, 1, 3,
	0, 5, 2, 1, 1, 3, 3, 1, 1, 3,
	3, 1, 1, 0, 2, 0, 3, 0, 1, 1,
	1, 1, 1, 1, 0, 1, 0, 1, 0, 2,
	1, 1, 1, 1, 0,
}
var yyChk = [...]int{

	-1000, -77, -1, -2, -3, -4, -5, -6, -7, -8,
	-9, -10, -11, -12, 9, 10, 11, 12, 31, 94,
	95, 97, 96, 98, 107, 108, 109, -15, 5, 6,
	7, 8, -13, -78, -13, -13, -13, -13, 99, -69,
	101, 105, -64, 101, 103, 99, 99, 100, 101, 99,
	-76, -76, -76, -2, 21, -16, 35, 22, -14, -64,
	-28, -73, 51, 13, -61, -62, -44, -71, -73, 51,
	-66, 104, 100, -71, 51, 99, -71, -73, -65, 104,
	51, -65, -73, -17, -18, 85, -19, -73, -32, -37,
	-33, 62, -79, -36, -44, -42, 83, 84, 89, 91,
	-71, 104, -45, 58, 59, 24, 48, 52, 53, 54,
	57, -43, 64, -71, 56, 27, 31, 92, -28, 49,
	68, 92, -73, 62, 51, -76, -73, -76, 102, -73,
	24, 47, -71, 13, 49, -72, -71, 23, 92, 61,
	60, 75, -34, 78, 62, 76, 77, 63, 75, 80,
	79, 88, 83, 84, 85, 86, 87, 81, 82, 68,
	69, 70, 71, 72, 73, 74, -32, -37, -32, -2,
	-40, -37, -37, -79, -37, -37, -37, -79, -79, -43,
	-79, -48, -37, -63, 20, 13, -28, -61, -73, -31,
	14, -62, -37, -71, -76, 24, -70, 106, -67, 97,
	95, 30, 96, 17, 51, -73, -73, -76, -20, -21,
	-22, -23, -27, -43, -79, -73, -18, -71, 85, -32,
	-32, -38, 57, 62, 58, 59, -37, -39, -79, -43,
	55, 78, 76, 77, 63, -37, -37, -37, -38, -37,
	-37, -37, -37, -37, -37, -37, -37, -37, -37, -80,
	50, -80, 49, -80, -37, -71, -80, -17, 22, -17,
	-46, -47, 65, -27, -58, 31, -79, -31, -52, 17,
	-32, 47, -71, -76, -68, 102, -31, 49, -24, -25,
	-26, 37, 41, 43, 38, 39, 40, 44, -74, -73,
	23, -75, 23, -20, 92, 57, 58, 59, -40, -39,
	-37, -37, -37, 61, -37, -80, -17, -80, -49, -47,
	67, -32, -35, 33, -2, -61, -59, -44, -52, -56,
	19, 18, -73, -73, -50, 15, -21, -22, -21, -22,
	37, 37, 37, 42, 37, 42, 37, -25, -29, 45,
	103, 46, -73, -73, -80, -73, -80, 61, -37, -80,
	93, -37, 66, -60, 47, -41, -42, -60, -80, 49,
	-56, -37, -53, -54, -37, -76, -51, 16, 18, 47,
	47, 37, 37, 100, 100, 100, -37, -37, 28, 49,
	-44, 49, 49, -55, 25, 26, -52, -32, -40, -32,
	-32, -79, -79, -79, 29, -42, -37, -54, -56, -30,
	-71, -30, -30, 11, -57, 20, 32, -80, 49, -80,
	-80, -61, 11, 78, -71, -71, -71,
}
var yyDef = [...]int{

	0, -2, 1, 2, 3, 4, 5, 6, 7, 8,
	9, 10, 11, 12, 35, 35, 35, 35, 35, 216,
	207, 0, 0, 0, 224, 224, 224, 0, 39, 41,
	42, 43, 44, 37, 207, 0, 0, 0, 205, 0,
	0, 217, 0, 0, 208, 0, 203, 0, 203, 0,
	32, 33, 34, 15, 40, 0, 0, 45, 36, 0,
	0, 84, 221, 0, 20, 198, 0, 162, 0, -2,
	0, 0, 0, 224, 220, 0, 224, 0, 0, 0,
	0, 0, 31, 0, 46, 48, 53, 0, 51, 52,
	94, 0, 0, 132, 133, 134, 0, 0, 0, 0,
	162, 0, 153, 100, 101, 0, 222, 164, 165, 166,
	167, 197, 155, 0, 38, 0, 0, 0, 92, 0,
	0, 0, 224, 0, 218, 23, 0, 26, 0, 28,
	204, 0, 224, 0, 0, 49, 54, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 119,
	120, 121, 122, 123, 124, 125, 97, 0, 0, 0,
	0, 130, 145, 0, 146, 147, 0, 0, 0, 112,
	0, 0, 156, 0, 201, 202, 186, 92, 85, 172,
	0, 199, 200, 163, 21, 206, 0, 0, 224, 214,
	209, 210, 211, 212, 213, 27, 29, 30, 92, 56,
	58, 59, 69, 67, 0, 82, 47, 55, 50, 95,
	96, 99, 113, 0, 115, 117, 102, 103, 0, 127,
	128, 0, 0, 0, 0, 105, 107, 0, 111, 135,
	136, 137, 138, 139, 140, 141, 142, 143, 144, 98,
	223, 129, 0, 196, 130, 148, 149, 0, 0, 0,
	160, 157, 0, 14, 0, 0, 0, 172, 180, 0,
	93, 0, 219, 24, 0, 215, 168, 0, 0, 0,
	0, 72, 0, 0, 75, 0, 0, 0, 86, 70,
	0, 0, 68, 0, 0, 114, 116, 118, 0, 104,
	106, 108, 0, 0, 131, 150, 0, 152, 0, 158,
	0, 0, 190, 0, 193, 190, 0, 188, 180, 19,
	0, 0, 224, 25, 170, 0, 57, 63, 0, 66,
	73, 74, 76, 0, 78, 0, 80, 81, 60, 0,
	0, 0, 71, 61, 62, 83, 126, 0, 109, 151,
	154, 161, 0, 16, 0, 192, 194, 17, 187, 0,
	18, 181, 173, 174, 177, 22, 172, 0, 0, 0,
	0, 77, 79, 0, 0, 0, 110, 159, 0, 0,
	189, 0, 0, 176, 178, 179, 180, 171, 169, 64,
	65, 0, 0, 0, 0, 195, 182, 175, 183, 0,
	90, 0, 0, 0, 13, 0, 0, 87, 0, 88,
	89, 191, 184, 0, 91, 0, 185,
}
var yyTok1 = [...]int{

	1, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 87, 80, 3,
	48, 50, 85, 83, 49, 84, 92, 86, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	69, 68, 70, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 88, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 79, 3, 89,
}
var yyTok2 = [...]int{

	2, 3, 4, 5, 6, 7, 8, 9, 10, 11,
	12, 13, 14, 15, 16, 17, 18, 19, 20, 21,
	22, 23, 24, 25, 26, 27, 28, 29, 30, 31,
	32, 33, 34, 35, 36, 37, 38, 39, 40, 41,
	42, 43, 44, 45, 46, 47, 51, 52, 53, 54,
	55, 56, 57, 58, 59, 60, 61, 62, 63, 64,
	65, 66, 67, 71, 72, 73, 74, 75, 76, 77,
	78, 81, 82, 90, 91, 93, 94, 95, 96, 97,
	98, 99, 100, 101, 102, 103, 104, 105, 106, 107,
	108, 109,
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
		//line sql.y:170
		{
			setParseTree(yylex, yyDollar[1].statement)
		}
	case 2:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:176
		{
			yyVAL.statement = yyDollar[1].selStmt
		}
	case 13:
		yyDollar = yyS[yypt-12 : yypt+1]
		//line sql.y:192
		{
			yyVAL.selStmt = &Select{Comments: Comments(yyDollar[2].bytes2), Distinct: yyDollar[3].str, SelectExprs: yyDollar[4].selectExprs, From: yyDollar[6].tableExprs, Where: NewWhere(WhereStr, yyDollar[7].boolExpr), GroupBy: GroupBy(yyDollar[8].valExprs), Having: NewWhere(HavingStr, yyDollar[9].boolExpr), OrderBy: yyDollar[10].orderBy, Limit: yyDollar[11].limit, Lock: yyDollar[12].str}
		}
	case 14:
		yyDollar = yyS[yypt-6 : yypt+1]
		//line sql.y:196
		{
			if yyDollar[4].sqlID != "value" {
				yylex.Error("expecting value after next")
				return 1
			}
			yyVAL.selStmt = &Select{Comments: Comments(yyDollar[2].bytes2), SelectExprs: SelectExprs{Nextval{}}, From: TableExprs{&AliasedTableExpr{Expr: yyDollar[6].smTableExpr}}}
		}
	case 15:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:204
		{
			yyVAL.selStmt = &Union{Type: yyDollar[2].str, Left: yyDollar[1].selStmt, Right: yyDollar[3].selStmt}
		}
	case 16:
		yyDollar = yyS[yypt-8 : yypt+1]
		//line sql.y:210
		{
			yyVAL.statement = &Insert{Comments: Comments(yyDollar[2].bytes2), Ignore: yyDollar[3].str, Table: yyDollar[5].tableName, Columns: yyDollar[6].columns, Rows: yyDollar[7].insRows, OnDup: OnDup(yyDollar[8].updateExprs)}
		}
	case 17:
		yyDollar = yyS[yypt-8 : yypt+1]
		//line sql.y:214
		{
			cols := make(Columns, 0, len(yyDollar[7].updateExprs))
			vals := make(ValTuple, 0, len(yyDollar[7].updateExprs))
			for _, col := range yyDollar[7].updateExprs {
				cols = append(cols, &NonStarExpr{Expr: col.Name})
				vals = append(vals, col.Expr)
			}
			yyVAL.statement = &Insert{Comments: Comments(yyDollar[2].bytes2), Ignore: yyDollar[3].str, Table: yyDollar[5].tableName, Columns: cols, Rows: Values{vals}, OnDup: OnDup(yyDollar[8].updateExprs)}
		}
	case 18:
		yyDollar = yyS[yypt-8 : yypt+1]
		//line sql.y:226
		{
			yyVAL.statement = &Update{Comments: Comments(yyDollar[2].bytes2), Table: yyDollar[3].tableName, Exprs: yyDollar[5].updateExprs, Where: NewWhere(WhereStr, yyDollar[6].boolExpr), OrderBy: yyDollar[7].orderBy, Limit: yyDollar[8].limit}
		}
	case 19:
		yyDollar = yyS[yypt-7 : yypt+1]
		//line sql.y:232
		{
			yyVAL.statement = &Delete{Comments: Comments(yyDollar[2].bytes2), Table: yyDollar[4].tableName, Where: NewWhere(WhereStr, yyDollar[5].boolExpr), OrderBy: yyDollar[6].orderBy, Limit: yyDollar[7].limit}
		}
	case 20:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:238
		{
			yyVAL.statement = &Set{Comments: Comments(yyDollar[2].bytes2), Exprs: yyDollar[3].updateExprs}
		}
	case 21:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:244
		{
			yyVAL.statement = &DDL{Action: CreateStr, NewName: yyDollar[4].sqlID}
		}
	case 22:
		yyDollar = yyS[yypt-8 : yypt+1]
		//line sql.y:248
		{
			// Change this to an alter statement
			yyVAL.statement = &DDL{Action: AlterStr, Table: yyDollar[7].sqlID, NewName: yyDollar[7].sqlID}
		}
	case 23:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:253
		{
			yyVAL.statement = &DDL{Action: CreateStr, NewName: SQLName(yyDollar[3].sqlID)}
		}
	case 24:
		yyDollar = yyS[yypt-6 : yypt+1]
		//line sql.y:259
		{
			yyVAL.statement = &DDL{Action: AlterStr, Table: yyDollar[4].sqlID, NewName: yyDollar[4].sqlID}
		}
	case 25:
		yyDollar = yyS[yypt-7 : yypt+1]
		//line sql.y:263
		{
			// Change this to a rename statement
			yyVAL.statement = &DDL{Action: RenameStr, Table: yyDollar[4].sqlID, NewName: yyDollar[7].sqlID}
		}
	case 26:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:268
		{
			yyVAL.statement = &DDL{Action: AlterStr, Table: SQLName(yyDollar[3].sqlID), NewName: SQLName(yyDollar[3].sqlID)}
		}
	case 27:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:274
		{
			yyVAL.statement = &DDL{Action: RenameStr, Table: yyDollar[3].sqlID, NewName: yyDollar[5].sqlID}
		}
	case 28:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:280
		{
			yyVAL.statement = &DDL{Action: DropStr, Table: yyDollar[4].sqlID}
		}
	case 29:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:284
		{
			// Change this to an alter statement
			yyVAL.statement = &DDL{Action: AlterStr, Table: yyDollar[5].sqlID, NewName: yyDollar[5].sqlID}
		}
	case 30:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:289
		{
			yyVAL.statement = &DDL{Action: DropStr, Table: SQLName(yyDollar[4].sqlID)}
		}
	case 31:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:295
		{
			yyVAL.statement = &DDL{Action: AlterStr, Table: yyDollar[3].sqlID, NewName: yyDollar[3].sqlID}
		}
	case 32:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:301
		{
			yyVAL.statement = &Other{}
		}
	case 33:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:305
		{
			yyVAL.statement = &Other{}
		}
	case 34:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:309
		{
			yyVAL.statement = &Other{}
		}
	case 35:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:314
		{
			setAllowComments(yylex, true)
		}
	case 36:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:318
		{
			yyVAL.bytes2 = yyDollar[2].bytes2
			setAllowComments(yylex, false)
		}
	case 37:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:324
		{
			yyVAL.bytes2 = nil
		}
	case 38:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:328
		{
			yyVAL.bytes2 = append(yyDollar[1].bytes2, yyDollar[2].bytes)
		}
	case 39:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:334
		{
			yyVAL.str = UnionStr
		}
	case 40:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:338
		{
			yyVAL.str = UnionAllStr
		}
	case 41:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:342
		{
			yyVAL.str = SetMinusStr
		}
	case 42:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:346
		{
			yyVAL.str = ExceptStr
		}
	case 43:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:350
		{
			yyVAL.str = IntersectStr
		}
	case 44:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:355
		{
			yyVAL.str = ""
		}
	case 45:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:359
		{
			yyVAL.str = DistinctStr
		}
	case 46:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:365
		{
			yyVAL.selectExprs = SelectExprs{yyDollar[1].selectExpr}
		}
	case 47:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:369
		{
			yyVAL.selectExprs = append(yyVAL.selectExprs, yyDollar[3].selectExpr)
		}
	case 48:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:375
		{
			yyVAL.selectExpr = &StarExpr{}
		}
	case 49:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:379
		{
			yyVAL.selectExpr = &NonStarExpr{Expr: yyDollar[1].expr, As: yyDollar[2].sqlID}
		}
	case 50:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:383
		{
			yyVAL.selectExpr = &StarExpr{TableName: yyDollar[1].sqlID}
		}
	case 51:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:389
		{
			yyVAL.expr = yyDollar[1].boolExpr
		}
	case 52:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:393
		{
			yyVAL.expr = yyDollar[1].valExpr
		}
	case 53:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:398
		{
			yyVAL.sqlID = ""
		}
	case 54:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:402
		{
			yyVAL.sqlID = yyDollar[1].sqlID
		}
	case 55:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:406
		{
			yyVAL.sqlID = yyDollar[2].sqlID
		}
	case 56:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:412
		{
			yyVAL.tableExprs = TableExprs{yyDollar[1].tableExpr}
		}
	case 57:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:416
		{
			yyVAL.tableExprs = append(yyVAL.tableExprs, yyDollar[3].tableExpr)
		}
	case 60:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:426
		{
			yyVAL.tableExpr = &AliasedTableExpr{Expr: yyDollar[1].smTableExpr, As: yyDollar[2].sqlID, Hints: yyDollar[3].indexHints}
		}
	case 61:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:430
		{
			yyVAL.tableExpr = &AliasedTableExpr{Expr: yyDollar[1].subquery, As: yyDollar[3].sqlID}
		}
	case 62:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:434
		{
			yyVAL.tableExpr = &ParenTableExpr{Exprs: yyDollar[2].tableExprs}
		}
	case 63:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:447
		{
			yyVAL.tableExpr = &JoinTableExpr{LeftExpr: yyDollar[1].tableExpr, Join: yyDollar[2].str, RightExpr: yyDollar[3].tableExpr}
		}
	case 64:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:451
		{
			yyVAL.tableExpr = &JoinTableExpr{LeftExpr: yyDollar[1].tableExpr, Join: yyDollar[2].str, RightExpr: yyDollar[3].tableExpr, On: yyDollar[5].boolExpr}
		}
	case 65:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:455
		{
			yyVAL.tableExpr = &JoinTableExpr{LeftExpr: yyDollar[1].tableExpr, Join: yyDollar[2].str, RightExpr: yyDollar[3].tableExpr, On: yyDollar[5].boolExpr}
		}
	case 66:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:459
		{
			yyVAL.tableExpr = &JoinTableExpr{LeftExpr: yyDollar[1].tableExpr, Join: yyDollar[2].str, RightExpr: yyDollar[3].tableExpr}
		}
	case 67:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:464
		{
			yyVAL.empty = struct{}{}
		}
	case 68:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:466
		{
			yyVAL.empty = struct{}{}
		}
	case 69:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:469
		{
			yyVAL.sqlID = ""
		}
	case 70:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:473
		{
			yyVAL.sqlID = yyDollar[1].sqlID
		}
	case 71:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:477
		{
			yyVAL.sqlID = yyDollar[2].sqlID
		}
	case 72:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:483
		{
			yyVAL.str = JoinStr
		}
	case 73:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:487
		{
			yyVAL.str = JoinStr
		}
	case 74:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:491
		{
			yyVAL.str = JoinStr
		}
	case 75:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:495
		{
			yyVAL.str = StraightJoinStr
		}
	case 76:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:501
		{
			yyVAL.str = LeftJoinStr
		}
	case 77:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:505
		{
			yyVAL.str = LeftJoinStr
		}
	case 78:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:509
		{
			yyVAL.str = RightJoinStr
		}
	case 79:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:513
		{
			yyVAL.str = RightJoinStr
		}
	case 80:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:519
		{
			yyVAL.str = NaturalJoinStr
		}
	case 81:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:523
		{
			if yyDollar[2].str == LeftJoinStr {
				yyVAL.str = NaturalLeftJoinStr
			} else {
				yyVAL.str = NaturalRightJoinStr
			}
		}
	case 82:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:533
		{
			yyVAL.smTableExpr = &TableName{Name: yyDollar[1].sqlID}
		}
	case 83:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:537
		{
			yyVAL.smTableExpr = &TableName{Qualifier: yyDollar[1].sqlID, Name: yyDollar[3].sqlID}
		}
	case 84:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:543
		{
			yyVAL.tableName = &TableName{Name: yyDollar[1].sqlID}
		}
	case 85:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:547
		{
			yyVAL.tableName = &TableName{Qualifier: yyDollar[1].sqlID, Name: yyDollar[3].sqlID}
		}
	case 86:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:552
		{
			yyVAL.indexHints = nil
		}
	case 87:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:556
		{
			yyVAL.indexHints = &IndexHints{Type: UseStr, Indexes: yyDollar[4].sqlIDs}
		}
	case 88:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:560
		{
			yyVAL.indexHints = &IndexHints{Type: IgnoreStr, Indexes: yyDollar[4].sqlIDs}
		}
	case 89:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:564
		{
			yyVAL.indexHints = &IndexHints{Type: ForceStr, Indexes: yyDollar[4].sqlIDs}
		}
	case 90:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:570
		{
			yyVAL.sqlIDs = []SQLName{yyDollar[1].sqlID}
		}
	case 91:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:574
		{
			yyVAL.sqlIDs = append(yyDollar[1].sqlIDs, yyDollar[3].sqlID)
		}
	case 92:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:579
		{
			yyVAL.boolExpr = nil
		}
	case 93:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:583
		{
			yyVAL.boolExpr = yyDollar[2].boolExpr
		}
	case 95:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:590
		{
			yyVAL.boolExpr = &AndExpr{Left: yyDollar[1].boolExpr, Right: yyDollar[3].boolExpr}
		}
	case 96:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:594
		{
			yyVAL.boolExpr = &OrExpr{Left: yyDollar[1].boolExpr, Right: yyDollar[3].boolExpr}
		}
	case 97:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:598
		{
			yyVAL.boolExpr = &NotExpr{Expr: yyDollar[2].boolExpr}
		}
	case 98:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:602
		{
			yyVAL.boolExpr = &ParenBoolExpr{Expr: yyDollar[2].boolExpr}
		}
	case 99:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:606
		{
			yyVAL.boolExpr = &IsExpr{Operator: yyDollar[3].str, Expr: yyDollar[1].boolExpr}
		}
	case 100:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:612
		{
			yyVAL.boolExpr = BoolVal(true)
		}
	case 101:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:616
		{
			yyVAL.boolExpr = BoolVal(false)
		}
	case 102:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:620
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: yyDollar[2].str, Right: yyDollar[3].valExpr}
		}
	case 103:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:624
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: InStr, Right: yyDollar[3].colTuple}
		}
	case 104:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:628
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: NotInStr, Right: yyDollar[4].colTuple}
		}
	case 105:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:632
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: LikeStr, Right: yyDollar[3].valExpr}
		}
	case 106:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:636
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: NotLikeStr, Right: yyDollar[4].valExpr}
		}
	case 107:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:640
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: RegexpStr, Right: yyDollar[3].valExpr}
		}
	case 108:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:644
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: NotRegexpStr, Right: yyDollar[4].valExpr}
		}
	case 109:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:648
		{
			yyVAL.boolExpr = &RangeCond{Left: yyDollar[1].valExpr, Operator: BetweenStr, From: yyDollar[3].valExpr, To: yyDollar[5].valExpr}
		}
	case 110:
		yyDollar = yyS[yypt-6 : yypt+1]
		//line sql.y:652
		{
			yyVAL.boolExpr = &RangeCond{Left: yyDollar[1].valExpr, Operator: NotBetweenStr, From: yyDollar[4].valExpr, To: yyDollar[6].valExpr}
		}
	case 111:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:656
		{
			yyVAL.boolExpr = &IsExpr{Operator: yyDollar[3].str, Expr: yyDollar[1].valExpr}
		}
	case 112:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:660
		{
			yyVAL.boolExpr = &ExistsExpr{Subquery: yyDollar[2].subquery}
		}
	case 113:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:666
		{
			yyVAL.str = IsNullStr
		}
	case 114:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:670
		{
			yyVAL.str = IsNotNullStr
		}
	case 115:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:674
		{
			yyVAL.str = IsTrueStr
		}
	case 116:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:678
		{
			yyVAL.str = IsNotTrueStr
		}
	case 117:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:682
		{
			yyVAL.str = IsFalseStr
		}
	case 118:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:686
		{
			yyVAL.str = IsNotFalseStr
		}
	case 119:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:692
		{
			yyVAL.str = EqualStr
		}
	case 120:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:696
		{
			yyVAL.str = LessThanStr
		}
	case 121:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:700
		{
			yyVAL.str = GreaterThanStr
		}
	case 122:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:704
		{
			yyVAL.str = LessEqualStr
		}
	case 123:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:708
		{
			yyVAL.str = GreaterEqualStr
		}
	case 124:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:712
		{
			yyVAL.str = NotEqualStr
		}
	case 125:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:716
		{
			yyVAL.str = NullSafeEqualStr
		}
	case 126:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:722
		{
			yyVAL.colTuple = ValTuple(yyDollar[2].valExprs)
		}
	case 127:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:726
		{
			yyVAL.colTuple = yyDollar[1].subquery
		}
	case 128:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:730
		{
			yyVAL.colTuple = ListArg(yyDollar[1].bytes)
		}
	case 129:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:736
		{
			yyVAL.subquery = &Subquery{yyDollar[2].selStmt}
		}
	case 130:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:742
		{
			yyVAL.valExprs = ValExprs{yyDollar[1].valExpr}
		}
	case 131:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:746
		{
			yyVAL.valExprs = append(yyDollar[1].valExprs, yyDollar[3].valExpr)
		}
	case 132:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:752
		{
			yyVAL.valExpr = yyDollar[1].valExpr
		}
	case 133:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:756
		{
			yyVAL.valExpr = yyDollar[1].colName
		}
	case 134:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:760
		{
			yyVAL.valExpr = yyDollar[1].rowTuple
		}
	case 135:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:764
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: BitAndStr, Right: yyDollar[3].valExpr}
		}
	case 136:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:768
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: BitOrStr, Right: yyDollar[3].valExpr}
		}
	case 137:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:772
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: BitXorStr, Right: yyDollar[3].valExpr}
		}
	case 138:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:776
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: PlusStr, Right: yyDollar[3].valExpr}
		}
	case 139:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:780
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: MinusStr, Right: yyDollar[3].valExpr}
		}
	case 140:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:784
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: MultStr, Right: yyDollar[3].valExpr}
		}
	case 141:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:788
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: DivStr, Right: yyDollar[3].valExpr}
		}
	case 142:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:792
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: ModStr, Right: yyDollar[3].valExpr}
		}
	case 143:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:796
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: ShiftLeftStr, Right: yyDollar[3].valExpr}
		}
	case 144:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:800
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: ShiftRightStr, Right: yyDollar[3].valExpr}
		}
	case 145:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:804
		{
			if num, ok := yyDollar[2].valExpr.(NumVal); ok {
				yyVAL.valExpr = num
			} else {
				yyVAL.valExpr = &UnaryExpr{Operator: UPlusStr, Expr: yyDollar[2].valExpr}
			}
		}
	case 146:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:812
		{
			if num, ok := yyDollar[2].valExpr.(NumVal); ok {
				// Handle double negative
				if num[0] == '-' {
					yyVAL.valExpr = num[1:]
				} else {
					yyVAL.valExpr = append(NumVal("-"), num...)
				}
			} else {
				yyVAL.valExpr = &UnaryExpr{Operator: UMinusStr, Expr: yyDollar[2].valExpr}
			}
		}
	case 147:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:825
		{
			yyVAL.valExpr = &UnaryExpr{Operator: TildaStr, Expr: yyDollar[2].valExpr}
		}
	case 148:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:829
		{
			// This rule prevents the usage of INTERVAL
			// as a function. If support is needed for that,
			// we'll need to revisit this. The solution
			// will be non-trivial because of grammar conflicts.
			yyVAL.valExpr = &IntervalExpr{Expr: yyDollar[2].valExpr, Unit: yyDollar[3].sqlID}
		}
	case 149:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:837
		{
			yyVAL.valExpr = &FuncExpr{Name: string(yyDollar[1].sqlID)}
		}
	case 150:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:841
		{
			yyVAL.valExpr = &FuncExpr{Name: string(yyDollar[1].sqlID), Exprs: yyDollar[3].selectExprs}
		}
	case 151:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:845
		{
			yyVAL.valExpr = &FuncExpr{Name: string(yyDollar[1].sqlID), Distinct: true, Exprs: yyDollar[4].selectExprs}
		}
	case 152:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:849
		{
			yyVAL.valExpr = &FuncExpr{Name: "if", Exprs: yyDollar[3].selectExprs}
		}
	case 153:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:853
		{
			yyVAL.valExpr = yyDollar[1].caseExpr
		}
	case 154:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:859
		{
			yyVAL.caseExpr = &CaseExpr{Expr: yyDollar[2].valExpr, Whens: yyDollar[3].whens, Else: yyDollar[4].valExpr}
		}
	case 155:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:864
		{
			yyVAL.valExpr = nil
		}
	case 156:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:868
		{
			yyVAL.valExpr = yyDollar[1].valExpr
		}
	case 157:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:874
		{
			yyVAL.whens = []*When{yyDollar[1].when}
		}
	case 158:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:878
		{
			yyVAL.whens = append(yyDollar[1].whens, yyDollar[2].when)
		}
	case 159:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:884
		{
			yyVAL.when = &When{Cond: yyDollar[2].boolExpr, Val: yyDollar[4].valExpr}
		}
	case 160:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:889
		{
			yyVAL.valExpr = nil
		}
	case 161:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:893
		{
			yyVAL.valExpr = yyDollar[2].valExpr
		}
	case 162:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:899
		{
			yyVAL.colName = &ColName{Name: yyDollar[1].sqlID}
		}
	case 163:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:903
		{
			yyVAL.colName = &ColName{Qualifier: yyDollar[1].sqlID, Name: yyDollar[3].sqlID}
		}
	case 164:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:909
		{
			yyVAL.valExpr = StrVal(yyDollar[1].bytes)
		}
	case 165:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:913
		{
			yyVAL.valExpr = NumVal(yyDollar[1].bytes)
		}
	case 166:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:917
		{
			yyVAL.valExpr = ValArg(yyDollar[1].bytes)
		}
	case 167:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:921
		{
			yyVAL.valExpr = &NullVal{}
		}
	case 168:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:926
		{
			yyVAL.valExprs = nil
		}
	case 169:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:930
		{
			yyVAL.valExprs = yyDollar[3].valExprs
		}
	case 170:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:935
		{
			yyVAL.boolExpr = nil
		}
	case 171:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:939
		{
			yyVAL.boolExpr = yyDollar[2].boolExpr
		}
	case 172:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:944
		{
			yyVAL.orderBy = nil
		}
	case 173:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:948
		{
			yyVAL.orderBy = yyDollar[3].orderBy
		}
	case 174:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:954
		{
			yyVAL.orderBy = OrderBy{yyDollar[1].order}
		}
	case 175:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:958
		{
			yyVAL.orderBy = append(yyDollar[1].orderBy, yyDollar[3].order)
		}
	case 176:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:964
		{
			yyVAL.order = &Order{Expr: yyDollar[1].valExpr, Direction: yyDollar[2].str}
		}
	case 177:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:969
		{
			yyVAL.str = AscScr
		}
	case 178:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:973
		{
			yyVAL.str = AscScr
		}
	case 179:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:977
		{
			yyVAL.str = DescScr
		}
	case 180:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:982
		{
			yyVAL.limit = nil
		}
	case 181:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:986
		{
			yyVAL.limit = &Limit{Rowcount: yyDollar[2].valExpr}
		}
	case 182:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:990
		{
			yyVAL.limit = &Limit{Offset: yyDollar[2].valExpr, Rowcount: yyDollar[4].valExpr}
		}
	case 183:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:995
		{
			yyVAL.str = ""
		}
	case 184:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:999
		{
			yyVAL.str = ForUpdateStr
		}
	case 185:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:1003
		{
			if yyDollar[3].sqlID != "share" {
				yylex.Error("expecting share")
				return 1
			}
			if yyDollar[4].sqlID != "mode" {
				yylex.Error("expecting mode")
				return 1
			}
			yyVAL.str = ShareModeStr
		}
	case 186:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1016
		{
			yyVAL.columns = nil
		}
	case 187:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1020
		{
			yyVAL.columns = yyDollar[2].columns
		}
	case 188:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1026
		{
			yyVAL.columns = Columns{&NonStarExpr{Expr: yyDollar[1].colName}}
		}
	case 189:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1030
		{
			yyVAL.columns = append(yyVAL.columns, &NonStarExpr{Expr: yyDollar[3].colName})
		}
	case 190:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1035
		{
			yyVAL.updateExprs = nil
		}
	case 191:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:1039
		{
			yyVAL.updateExprs = yyDollar[5].updateExprs
		}
	case 192:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:1045
		{
			yyVAL.insRows = yyDollar[2].values
		}
	case 193:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1049
		{
			yyVAL.insRows = yyDollar[1].selStmt
		}
	case 194:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1055
		{
			yyVAL.values = Values{yyDollar[1].rowTuple}
		}
	case 195:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1059
		{
			yyVAL.values = append(yyDollar[1].values, yyDollar[3].rowTuple)
		}
	case 196:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1065
		{
			yyVAL.rowTuple = ValTuple(yyDollar[2].valExprs)
		}
	case 197:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1069
		{
			yyVAL.rowTuple = yyDollar[1].subquery
		}
	case 198:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1075
		{
			yyVAL.updateExprs = UpdateExprs{yyDollar[1].updateExpr}
		}
	case 199:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1079
		{
			yyVAL.updateExprs = append(yyDollar[1].updateExprs, yyDollar[3].updateExpr)
		}
	case 200:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1085
		{
			yyVAL.updateExpr = &UpdateExpr{Name: yyDollar[1].colName, Expr: yyDollar[3].valExpr}
		}
	case 203:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1094
		{
			yyVAL.empty = struct{}{}
		}
	case 204:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:1096
		{
			yyVAL.empty = struct{}{}
		}
	case 205:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1099
		{
			yyVAL.empty = struct{}{}
		}
	case 206:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1101
		{
			yyVAL.empty = struct{}{}
		}
	case 207:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1104
		{
			yyVAL.str = ""
		}
	case 208:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1106
		{
			yyVAL.str = IgnoreStr
		}
	case 209:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1110
		{
			yyVAL.empty = struct{}{}
		}
	case 210:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1112
		{
			yyVAL.empty = struct{}{}
		}
	case 211:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1114
		{
			yyVAL.empty = struct{}{}
		}
	case 212:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1116
		{
			yyVAL.empty = struct{}{}
		}
	case 213:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1118
		{
			yyVAL.empty = struct{}{}
		}
	case 214:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1121
		{
			yyVAL.empty = struct{}{}
		}
	case 215:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1123
		{
			yyVAL.empty = struct{}{}
		}
	case 216:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1126
		{
			yyVAL.empty = struct{}{}
		}
	case 217:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1128
		{
			yyVAL.empty = struct{}{}
		}
	case 218:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1131
		{
			yyVAL.empty = struct{}{}
		}
	case 219:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:1133
		{
			yyVAL.empty = struct{}{}
		}
	case 220:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1137
		{
			yyVAL.sqlID = SQLName(strings.ToLower(string(yyDollar[1].bytes)))
		}
	case 221:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1143
		{
			yyVAL.sqlID = SQLName(yyDollar[1].bytes)
		}
	case 222:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1149
		{
			if incNesting(yylex) {
				yylex.Error("max nesting level reached")
				return 1
			}
		}
	case 223:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1158
		{
			decNesting(yylex)
		}
	case 224:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1163
		{
			forceEOF(yylex)
		}
	}
	goto yystack /* stack new state and value */
}
