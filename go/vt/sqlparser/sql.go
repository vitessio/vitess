//line sql.y:6
package sqlparser

import __yyfmt__ "fmt"

//line sql.y:6
import "bytes"

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

var (
	SHARE        = []byte("share")
	MODE         = []byte("mode")
	IF_BYTES     = []byte("if")
	VALUES_BYTES = []byte("values")
)

//line sql.y:43
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
}

const LEX_ERROR = 57346
const SELECT = 57347
const INSERT = 57348
const UPDATE = 57349
const DELETE = 57350
const FROM = 57351
const WHERE = 57352
const GROUP = 57353
const HAVING = 57354
const ORDER = 57355
const BY = 57356
const LIMIT = 57357
const FOR = 57358
const ALL = 57359
const DISTINCT = 57360
const AS = 57361
const EXISTS = 57362
const IN = 57363
const IS = 57364
const LIKE = 57365
const BETWEEN = 57366
const NULL = 57367
const ASC = 57368
const DESC = 57369
const VALUES = 57370
const INTO = 57371
const DUPLICATE = 57372
const KEY = 57373
const DEFAULT = 57374
const SET = 57375
const LOCK = 57376
const KEYRANGE = 57377
const ID = 57378
const STRING = 57379
const NUMBER = 57380
const VALUE_ARG = 57381
const LIST_ARG = 57382
const COMMENT = 57383
const LE = 57384
const GE = 57385
const NE = 57386
const NULL_SAFE_EQUAL = 57387
const UNION = 57388
const MINUS = 57389
const EXCEPT = 57390
const INTERSECT = 57391
const JOIN = 57392
const STRAIGHT_JOIN = 57393
const LEFT = 57394
const RIGHT = 57395
const INNER = 57396
const OUTER = 57397
const CROSS = 57398
const NATURAL = 57399
const USE = 57400
const FORCE = 57401
const ON = 57402
const OR = 57403
const AND = 57404
const NOT = 57405
const UNARY = 57406
const CASE = 57407
const WHEN = 57408
const THEN = 57409
const ELSE = 57410
const END = 57411
const CREATE = 57412
const ALTER = 57413
const DROP = 57414
const RENAME = 57415
const ANALYZE = 57416
const TABLE = 57417
const INDEX = 57418
const VIEW = 57419
const TO = 57420
const IGNORE = 57421
const IF = 57422
const UNIQUE = 57423
const USING = 57424
const SHOW = 57425
const DESCRIBE = 57426
const EXPLAIN = 57427

var yyToknames = []string{
	"LEX_ERROR",
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
	"IN",
	"IS",
	"LIKE",
	"BETWEEN",
	"NULL",
	"ASC",
	"DESC",
	"VALUES",
	"INTO",
	"DUPLICATE",
	"KEY",
	"DEFAULT",
	"SET",
	"LOCK",
	"KEYRANGE",
	"ID",
	"STRING",
	"NUMBER",
	"VALUE_ARG",
	"LIST_ARG",
	"COMMENT",
	"LE",
	"GE",
	"NE",
	"NULL_SAFE_EQUAL",
	"'('",
	"'='",
	"'<'",
	"'>'",
	"'~'",
	"UNION",
	"MINUS",
	"EXCEPT",
	"INTERSECT",
	"','",
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
	"OR",
	"AND",
	"NOT",
	"'&'",
	"'|'",
	"'^'",
	"'+'",
	"'-'",
	"'*'",
	"'/'",
	"'%'",
	"'.'",
	"UNARY",
	"CASE",
	"WHEN",
	"THEN",
	"ELSE",
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
var yyStatenames = []string{}

const yyEofCode = 1
const yyErrCode = 2
const yyMaxDepth = 200

//line yacctab:1
var yyExca = []int{
	-1, 1,
	1, -1,
	-2, 0,
}

const yyNprod = 205
const yyPrivate = 57344

var yyTokenNames []string
var yyStates []string

const yyLast = 625

var yyAct = []int{

	94, 163, 160, 366, 229, 333, 90, 91, 250, 62,
	199, 296, 241, 288, 92, 63, 210, 179, 28, 29,
	30, 31, 162, 3, 104, 80, 50, 81, 137, 136,
	375, 131, 230, 261, 262, 263, 264, 265, 65, 266,
	267, 70, 232, 294, 73, 64, 187, 76, 77, 85,
	53, 68, 51, 52, 237, 43, 97, 44, 86, 257,
	344, 103, 230, 38, 109, 40, 324, 125, 230, 41,
	121, 98, 84, 100, 101, 102, 230, 230, 230, 129,
	343, 342, 99, 69, 133, 72, 107, 49, 230, 230,
	45, 242, 164, 46, 47, 48, 165, 122, 313, 315,
	124, 242, 272, 286, 135, 88, 193, 118, 114, 105,
	106, 82, 120, 173, 65, 136, 110, 65, 339, 183,
	182, 64, 169, 177, 64, 191, 71, 289, 314, 194,
	219, 108, 181, 253, 86, 205, 183, 230, 159, 161,
	128, 209, 137, 136, 217, 218, 184, 221, 222, 223,
	224, 225, 226, 227, 228, 203, 197, 326, 307, 204,
	149, 150, 151, 308, 212, 206, 166, 231, 233, 234,
	86, 86, 235, 341, 220, 340, 65, 65, 239, 190,
	192, 189, 311, 64, 248, 246, 207, 208, 254, 137,
	136, 310, 309, 236, 238, 249, 103, 245, 116, 109,
	147, 148, 149, 150, 151, 75, 305, 66, 100, 101,
	102, 306, 271, 273, 234, 255, 258, 99, 275, 276,
	232, 107, 89, 116, 180, 351, 203, 180, 130, 59,
	252, 274, 328, 283, 289, 279, 14, 175, 117, 212,
	86, 280, 99, 282, 105, 106, 28, 29, 30, 31,
	99, 110, 293, 285, 78, 89, 89, 111, 295, 291,
	292, 167, 168, 281, 170, 171, 108, 202, 71, 259,
	303, 304, 116, 202, 131, 213, 317, 99, 319, 176,
	321, 99, 270, 99, 203, 203, 322, 66, 112, 325,
	323, 115, 287, 350, 318, 65, 316, 201, 89, 269,
	331, 334, 329, 89, 89, 134, 211, 330, 144, 145,
	146, 147, 148, 149, 150, 151, 300, 299, 14, 15,
	16, 17, 71, 345, 103, 196, 335, 195, 346, 347,
	178, 126, 123, 119, 89, 89, 100, 101, 102, 60,
	357, 234, 79, 349, 74, 355, 18, 89, 372, 113,
	348, 327, 14, 363, 334, 58, 214, 364, 215, 216,
	367, 367, 367, 65, 368, 369, 373, 365, 201, 278,
	64, 374, 370, 376, 377, 244, 380, 379, 185, 127,
	381, 211, 382, 56, 54, 297, 338, 356, 320, 358,
	144, 145, 146, 147, 148, 149, 150, 151, 19, 20,
	22, 21, 23, 298, 89, 251, 302, 353, 354, 89,
	337, 24, 25, 26, 97, 180, 61, 378, 362, 103,
	14, 33, 109, 186, 39, 256, 201, 201, 188, 98,
	84, 100, 101, 102, 42, 261, 262, 263, 264, 265,
	99, 266, 267, 32, 107, 67, 247, 174, 371, 352,
	14, 144, 145, 146, 147, 148, 149, 150, 151, 34,
	35, 36, 37, 88, 332, 97, 336, 105, 106, 82,
	103, 301, 284, 109, 110, 172, 240, 96, 93, 95,
	98, 66, 100, 101, 102, 290, 243, 138, 87, 108,
	312, 99, 200, 260, 277, 107, 144, 145, 146, 147,
	148, 149, 150, 151, 89, 198, 89, 83, 268, 359,
	360, 361, 97, 14, 88, 132, 55, 103, 105, 106,
	109, 27, 57, 13, 12, 110, 11, 98, 66, 100,
	101, 102, 10, 103, 9, 8, 109, 7, 99, 6,
	108, 5, 107, 4, 66, 100, 101, 102, 2, 1,
	0, 0, 0, 0, 99, 0, 0, 0, 107, 0,
	0, 88, 0, 0, 0, 105, 106, 0, 139, 143,
	141, 142, 110, 144, 145, 146, 147, 148, 149, 150,
	151, 105, 106, 0, 0, 0, 0, 108, 110, 155,
	156, 157, 158, 0, 152, 153, 154, 0, 0, 0,
	0, 0, 0, 108, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 140, 144, 145, 146,
	147, 148, 149, 150, 151,
}
var yyPact = []int{

	313, -1000, -1000, 195, -1000, -1000, -1000, -1000, -1000, -1000,
	-1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, -27,
	-37, 0, 3, -3, -1000, -1000, -1000, 415, 367, -1000,
	-1000, -1000, 365, -1000, 326, 303, 407, 251, -44, -8,
	232, -1000, -5, 232, -1000, 308, -48, 232, -48, 306,
	-1000, -1000, -1000, -1000, -1000, 394, -1000, 216, 303, 316,
	30, 303, 143, -1000, 191, -1000, 29, 297, 43, 232,
	-1000, -1000, 296, -1000, -26, 295, 359, 74, 232, -1000,
	219, -1000, -1000, 286, 26, 122, 547, -1000, 492, 445,
	-1000, -1000, -1000, 171, 196, 196, -1000, 196, 196, -1000,
	-1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000,
	171, -1000, 204, 251, 294, 405, 251, 171, 232, -1000,
	358, -51, -1000, 93, -1000, 291, -1000, -1000, 289, -1000,
	237, 394, -1000, -1000, 232, 90, 492, 492, 171, 235,
	335, 171, 171, 105, 171, 171, 171, 171, 171, 171,
	171, 171, -1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000,
	547, -39, -33, -13, 547, -1000, 508, 36, 394, -1000,
	415, 299, 10, 503, 347, 251, 251, 217, -1000, 392,
	492, -1000, 503, -1000, -1000, -1000, 67, 232, -1000, -34,
	-1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, 214, 379,
	263, 231, 24, -1000, -1000, -1000, -1000, -1000, 47, 503,
	-1000, 508, -1000, -1000, 235, 171, 171, 503, 426, -1000,
	344, 127, 127, 127, 85, 85, -1000, -1000, -1000, -1000,
	-1000, -1000, 171, -1000, 503, -1000, -24, 394, -24, 178,
	20, -1000, 492, 61, 196, 195, 168, -12, -1000, 392,
	370, 389, 122, 281, -1000, -1000, 280, -1000, 395, 237,
	237, -1000, -1000, 150, 102, 136, 135, 126, 34, -1000,
	260, -23, 258, -13, -1000, 503, 320, 171, -1000, 503,
	-1000, -24, -1000, 299, -18, -1000, 171, 75, -1000, 321,
	177, -1000, -1000, -1000, 251, 370, -1000, 171, 171, -1000,
	-1000, 398, 372, 379, 52, -1000, 119, -1000, 117, -1000,
	-1000, -1000, -1000, -10, -11, -31, -1000, -1000, -1000, -1000,
	171, 503, -1000, -69, -1000, 503, 171, 319, 196, -1000,
	-1000, 238, 170, -1000, 381, -1000, 392, 492, 171, 492,
	-1000, -1000, 196, 196, 196, 503, -1000, 503, 411, -1000,
	171, 171, -1000, -1000, -1000, 370, 122, 165, 122, 232,
	232, 232, 251, 503, -1000, 332, -25, -1000, -25, -25,
	143, -1000, 410, 356, -1000, 232, -1000, -1000, -1000, 232,
	-1000, 232, -1000,
}
var yyPgo = []int{

	0, 549, 548, 22, 543, 541, 539, 537, 535, 534,
	532, 526, 524, 523, 443, 522, 521, 516, 25, 27,
	515, 508, 507, 505, 10, 493, 492, 229, 490, 3,
	17, 49, 488, 487, 486, 6, 2, 16, 1, 485,
	14, 479, 24, 478, 7, 477, 476, 12, 475, 472,
	471, 466, 8, 464, 5, 449, 11, 448, 447, 446,
	13, 9, 15, 205, 445, 434, 428, 425, 424, 423,
	0, 26, 421, 166, 4,
}
var yyR1 = []int{

	0, 1, 2, 2, 2, 2, 2, 2, 2, 2,
	2, 2, 2, 3, 3, 4, 4, 5, 6, 7,
	8, 8, 8, 9, 9, 9, 10, 11, 11, 11,
	12, 13, 13, 13, 72, 14, 15, 15, 16, 16,
	16, 16, 16, 17, 17, 18, 18, 19, 19, 19,
	22, 22, 20, 20, 20, 23, 23, 24, 24, 24,
	24, 21, 21, 21, 25, 25, 25, 25, 25, 25,
	25, 25, 25, 26, 26, 26, 27, 27, 28, 28,
	28, 28, 29, 29, 30, 30, 31, 31, 31, 31,
	31, 32, 32, 32, 32, 32, 32, 32, 32, 32,
	32, 32, 33, 33, 33, 33, 33, 33, 33, 37,
	37, 37, 42, 38, 38, 36, 36, 36, 36, 36,
	36, 36, 36, 36, 36, 36, 36, 36, 36, 36,
	36, 36, 41, 41, 43, 43, 43, 45, 48, 48,
	46, 46, 47, 49, 49, 44, 44, 35, 35, 35,
	35, 50, 50, 51, 51, 52, 52, 53, 53, 54,
	55, 55, 55, 56, 56, 56, 57, 57, 57, 58,
	58, 59, 59, 60, 60, 34, 34, 39, 39, 40,
	40, 61, 61, 62, 63, 63, 64, 64, 65, 65,
	66, 66, 66, 66, 66, 67, 67, 68, 68, 69,
	69, 70, 73, 74, 71,
}
var yyR2 = []int{

	0, 1, 1, 1, 1, 1, 1, 1, 1, 1,
	1, 1, 1, 12, 3, 7, 7, 8, 7, 3,
	5, 8, 4, 6, 7, 4, 5, 4, 5, 5,
	3, 2, 2, 2, 0, 2, 0, 2, 1, 2,
	1, 1, 1, 0, 1, 1, 3, 1, 2, 3,
	1, 1, 0, 1, 2, 1, 3, 3, 3, 3,
	5, 0, 1, 2, 1, 1, 2, 3, 2, 3,
	2, 2, 2, 1, 3, 1, 1, 3, 0, 5,
	5, 5, 1, 3, 0, 2, 1, 3, 3, 2,
	3, 3, 3, 4, 3, 4, 5, 6, 3, 4,
	2, 6, 1, 1, 1, 1, 1, 1, 1, 3,
	1, 1, 3, 1, 3, 1, 1, 1, 3, 3,
	3, 3, 3, 3, 3, 3, 2, 3, 4, 5,
	4, 1, 1, 1, 1, 1, 1, 5, 0, 1,
	1, 2, 4, 0, 2, 1, 3, 1, 1, 1,
	1, 0, 3, 0, 2, 0, 3, 1, 3, 2,
	0, 1, 1, 0, 2, 4, 0, 2, 4, 0,
	3, 1, 3, 0, 5, 2, 1, 1, 3, 3,
	1, 1, 3, 3, 0, 2, 0, 3, 0, 1,
	1, 1, 1, 1, 1, 0, 1, 0, 1, 0,
	2, 1, 1, 1, 0,
}
var yyChk = []int{

	-1000, -1, -2, -3, -4, -5, -6, -7, -8, -9,
	-10, -11, -12, -13, 5, 6, 7, 8, 33, 85,
	86, 88, 87, 89, 98, 99, 100, -16, 51, 52,
	53, 54, -14, -72, -14, -14, -14, -14, 90, -68,
	92, 96, -65, 92, 94, 90, 90, 91, 92, 90,
	-71, -71, -71, -3, 17, -17, 18, -15, 29, -27,
	36, 9, -61, -62, -44, -70, 36, -64, 95, 91,
	-70, 36, 90, -70, 36, -63, 95, -70, -63, 36,
	-18, -19, 75, -22, 36, -31, -36, -32, 69, -73,
	-35, -44, -40, -43, -70, -41, -45, 20, 35, 46,
	37, 38, 39, 25, -42, 73, 74, 50, 95, 28,
	80, 41, -27, 33, 78, -27, 55, 47, 78, 36,
	69, -70, -71, 36, -71, 93, 36, 20, 66, -70,
	9, 55, -20, -70, 19, 78, 68, 67, -33, 21,
	69, 23, 24, 22, 70, 71, 72, 73, 74, 75,
	76, 77, 47, 48, 49, 42, 43, 44, 45, -31,
	-36, -31, -3, -38, -36, -36, -73, -73, -73, -42,
	-73, -73, -48, -36, -58, 33, -73, -61, 36, -30,
	10, -62, -36, -70, -71, 20, -69, 97, -66, 88,
	86, 32, 87, 13, 36, 36, 36, -71, -23, -24,
	-26, -73, 36, -42, -19, -70, 75, -31, -31, -36,
	-37, -73, -42, 40, 21, 23, 24, -36, -36, 25,
	69, -36, -36, -36, -36, -36, -36, -36, -36, -74,
	101, -74, 55, -74, -36, -74, -18, 18, -18, -35,
	-46, -47, 81, -34, 28, -3, -61, -59, -44, -30,
	-52, 13, -31, 66, -70, -71, -67, 93, -30, 55,
	-25, 56, 57, 58, 59, 60, 62, 63, -21, 36,
	19, -24, 78, -38, -37, -36, -36, 68, 25, -36,
	-74, -18, -74, 55, -49, -47, 83, -31, -60, 66,
	-39, -40, -60, -74, 55, -52, -56, 15, 14, 36,
	36, -50, 11, -24, -24, 56, 61, 56, 61, 56,
	56, 56, -28, 64, 94, 65, 36, -74, 36, -74,
	68, -36, -74, -35, 84, -36, 82, 30, 55, -44,
	-56, -36, -53, -54, -36, -71, -51, 12, 14, 66,
	56, 56, 91, 91, 91, -36, -74, -36, 31, -40,
	55, 55, -55, 26, 27, -52, -31, -38, -31, -73,
	-73, -73, 7, -36, -54, -56, -29, -70, -29, -29,
	-61, -57, 16, 34, -74, 55, -74, -74, 7, 21,
	-70, -70, -70,
}
var yyDef = []int{

	0, -2, 1, 2, 3, 4, 5, 6, 7, 8,
	9, 10, 11, 12, 34, 34, 34, 34, 34, 197,
	188, 0, 0, 0, 204, 204, 204, 0, 38, 40,
	41, 42, 43, 36, 0, 0, 0, 0, 186, 0,
	0, 198, 0, 0, 189, 0, 184, 0, 184, 0,
	31, 32, 33, 14, 39, 0, 44, 35, 0, 0,
	76, 0, 19, 181, 0, 145, 201, 0, 0, 0,
	204, 201, 0, 204, 0, 0, 0, 0, 0, 30,
	0, 45, 47, 52, 201, 50, 51, 86, 0, 0,
	115, 116, 117, 0, 145, 0, 131, 0, 0, 202,
	147, 148, 149, 150, 180, 134, 135, 136, 132, 133,
	138, 37, 169, 0, 0, 84, 0, 0, 0, 204,
	0, 199, 22, 0, 25, 0, 27, 185, 0, 204,
	0, 0, 48, 53, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 102, 103, 104, 105, 106, 107, 108, 89,
	0, 0, 0, 0, 113, 126, 0, 0, 0, 100,
	0, 0, 0, 139, 0, 0, 0, 84, 77, 155,
	0, 182, 183, 146, 20, 187, 0, 0, 204, 195,
	190, 191, 192, 193, 194, 26, 28, 29, 84, 55,
	61, 0, 73, 75, 46, 54, 49, 87, 88, 91,
	92, 0, 110, 111, 0, 0, 0, 94, 0, 98,
	0, 118, 119, 120, 121, 122, 123, 124, 125, 90,
	203, 112, 0, 179, 113, 127, 0, 0, 0, 0,
	143, 140, 0, 173, 0, 176, 173, 0, 171, 155,
	163, 0, 85, 0, 200, 23, 0, 196, 151, 0,
	0, 64, 65, 0, 0, 0, 0, 0, 78, 62,
	0, 0, 0, 0, 93, 95, 0, 0, 99, 114,
	128, 0, 130, 0, 0, 141, 0, 0, 15, 0,
	175, 177, 16, 170, 0, 163, 18, 0, 0, 204,
	24, 153, 0, 56, 59, 66, 0, 68, 0, 70,
	71, 72, 57, 0, 0, 0, 63, 58, 74, 109,
	0, 96, 129, 0, 137, 144, 0, 0, 0, 172,
	17, 164, 156, 157, 160, 21, 155, 0, 0, 0,
	67, 69, 0, 0, 0, 97, 101, 142, 0, 178,
	0, 0, 159, 161, 162, 163, 154, 152, 60, 0,
	0, 0, 0, 165, 158, 166, 0, 82, 0, 0,
	174, 13, 0, 0, 79, 0, 80, 81, 167, 0,
	83, 0, 168,
}
var yyTok1 = []int{

	1, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 77, 70, 3,
	46, 101, 75, 73, 55, 74, 78, 76, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	48, 47, 49, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 72, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 71, 3, 50,
}
var yyTok2 = []int{

	2, 3, 4, 5, 6, 7, 8, 9, 10, 11,
	12, 13, 14, 15, 16, 17, 18, 19, 20, 21,
	22, 23, 24, 25, 26, 27, 28, 29, 30, 31,
	32, 33, 34, 35, 36, 37, 38, 39, 40, 41,
	42, 43, 44, 45, 51, 52, 53, 54, 56, 57,
	58, 59, 60, 61, 62, 63, 64, 65, 66, 67,
	68, 69, 79, 80, 81, 82, 83, 84, 85, 86,
	87, 88, 89, 90, 91, 92, 93, 94, 95, 96,
	97, 98, 99, 100,
}
var yyTok3 = []int{
	0,
}

//line yaccpar:1

/*	parser for yacc output	*/

var yyDebug = 0

type yyLexer interface {
	Lex(lval *yySymType) int
	Error(s string)
}

const yyFlag = -1000

func yyTokname(c int) string {
	// 4 is TOKSTART above
	if c >= 4 && c-4 < len(yyToknames) {
		if yyToknames[c-4] != "" {
			return yyToknames[c-4]
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

func yylex1(lex yyLexer, lval *yySymType) int {
	c := 0
	char := lex.Lex(lval)
	if char <= 0 {
		c = yyTok1[0]
		goto out
	}
	if char < len(yyTok1) {
		c = yyTok1[char]
		goto out
	}
	if char >= yyPrivate {
		if char < yyPrivate+len(yyTok2) {
			c = yyTok2[char-yyPrivate]
			goto out
		}
	}
	for i := 0; i < len(yyTok3); i += 2 {
		c = yyTok3[i+0]
		if c == char {
			c = yyTok3[i+1]
			goto out
		}
	}

out:
	if c == 0 {
		c = yyTok2[1] /* unknown char */
	}
	if yyDebug >= 3 {
		__yyfmt__.Printf("lex %s(%d)\n", yyTokname(c), uint(char))
	}
	return c
}

func yyParse(yylex yyLexer) int {
	var yyn int
	var yylval yySymType
	var yyVAL yySymType
	yyS := make([]yySymType, yyMaxDepth)

	Nerrs := 0   /* number of errors */
	Errflag := 0 /* error recovery flag */
	yystate := 0
	yychar := -1
	yyp := -1
	goto yystack

ret0:
	return 0

ret1:
	return 1

yystack:
	/* put a state and value onto the stack */
	if yyDebug >= 4 {
		__yyfmt__.Printf("char %v in %v\n", yyTokname(yychar), yyStatname(yystate))
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
	if yychar < 0 {
		yychar = yylex1(yylex, &yylval)
	}
	yyn += yychar
	if yyn < 0 || yyn >= yyLast {
		goto yydefault
	}
	yyn = yyAct[yyn]
	if yyChk[yyn] == yychar { /* valid shift */
		yychar = -1
		yyVAL = yylval
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
		if yychar < 0 {
			yychar = yylex1(yylex, &yylval)
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
			if yyn < 0 || yyn == yychar {
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
			yylex.Error("syntax error")
			Nerrs++
			if yyDebug >= 1 {
				__yyfmt__.Printf("%s", yyStatname(yystate))
				__yyfmt__.Printf(" saw %s\n", yyTokname(yychar))
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
				__yyfmt__.Printf("error recovery discards %s\n", yyTokname(yychar))
			}
			if yychar == yyEofCode {
				goto ret1
			}
			yychar = -1
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
		//line sql.y:163
		{
			setParseTree(yylex, yyS[yypt-0].statement)
		}
	case 2:
		//line sql.y:169
		{
			yyVAL.statement = yyS[yypt-0].selStmt
		}
	case 3:
		yyVAL.statement = yyS[yypt-0].statement
	case 4:
		yyVAL.statement = yyS[yypt-0].statement
	case 5:
		yyVAL.statement = yyS[yypt-0].statement
	case 6:
		yyVAL.statement = yyS[yypt-0].statement
	case 7:
		yyVAL.statement = yyS[yypt-0].statement
	case 8:
		yyVAL.statement = yyS[yypt-0].statement
	case 9:
		yyVAL.statement = yyS[yypt-0].statement
	case 10:
		yyVAL.statement = yyS[yypt-0].statement
	case 11:
		yyVAL.statement = yyS[yypt-0].statement
	case 12:
		yyVAL.statement = yyS[yypt-0].statement
	case 13:
		//line sql.y:185
		{
			yyVAL.selStmt = &Select{Comments: Comments(yyS[yypt-10].bytes2), Distinct: yyS[yypt-9].str, SelectExprs: yyS[yypt-8].selectExprs, From: yyS[yypt-6].tableExprs, Where: NewWhere(AST_WHERE, yyS[yypt-5].boolExpr), GroupBy: GroupBy(yyS[yypt-4].valExprs), Having: NewWhere(AST_HAVING, yyS[yypt-3].boolExpr), OrderBy: yyS[yypt-2].orderBy, Limit: yyS[yypt-1].limit, Lock: yyS[yypt-0].str}
		}
	case 14:
		//line sql.y:189
		{
			yyVAL.selStmt = &Union{Type: yyS[yypt-1].str, Left: yyS[yypt-2].selStmt, Right: yyS[yypt-0].selStmt}
		}
	case 15:
		//line sql.y:195
		{
			yyVAL.statement = &Insert{Comments: Comments(yyS[yypt-5].bytes2), Table: yyS[yypt-3].tableName, Columns: yyS[yypt-2].columns, Rows: yyS[yypt-1].insRows, OnDup: OnDup(yyS[yypt-0].updateExprs)}
		}
	case 16:
		//line sql.y:199
		{
			cols := make(Columns, 0, len(yyS[yypt-1].updateExprs))
			vals := make(ValTuple, 0, len(yyS[yypt-1].updateExprs))
			for _, col := range yyS[yypt-1].updateExprs {
				cols = append(cols, &NonStarExpr{Expr: col.Name})
				vals = append(vals, col.Expr)
			}
			yyVAL.statement = &Insert{Comments: Comments(yyS[yypt-5].bytes2), Table: yyS[yypt-3].tableName, Columns: cols, Rows: Values{vals}, OnDup: OnDup(yyS[yypt-0].updateExprs)}
		}
	case 17:
		//line sql.y:211
		{
			yyVAL.statement = &Update{Comments: Comments(yyS[yypt-6].bytes2), Table: yyS[yypt-5].tableName, Exprs: yyS[yypt-3].updateExprs, Where: NewWhere(AST_WHERE, yyS[yypt-2].boolExpr), OrderBy: yyS[yypt-1].orderBy, Limit: yyS[yypt-0].limit}
		}
	case 18:
		//line sql.y:217
		{
			yyVAL.statement = &Delete{Comments: Comments(yyS[yypt-5].bytes2), Table: yyS[yypt-3].tableName, Where: NewWhere(AST_WHERE, yyS[yypt-2].boolExpr), OrderBy: yyS[yypt-1].orderBy, Limit: yyS[yypt-0].limit}
		}
	case 19:
		//line sql.y:223
		{
			yyVAL.statement = &Set{Comments: Comments(yyS[yypt-1].bytes2), Exprs: yyS[yypt-0].updateExprs}
		}
	case 20:
		//line sql.y:229
		{
			yyVAL.statement = &DDL{Action: AST_CREATE, NewName: yyS[yypt-1].bytes}
		}
	case 21:
		//line sql.y:233
		{
			// Change this to an alter statement
			yyVAL.statement = &DDL{Action: AST_ALTER, Table: yyS[yypt-1].bytes, NewName: yyS[yypt-1].bytes}
		}
	case 22:
		//line sql.y:238
		{
			yyVAL.statement = &DDL{Action: AST_CREATE, NewName: yyS[yypt-1].bytes}
		}
	case 23:
		//line sql.y:244
		{
			yyVAL.statement = &DDL{Action: AST_ALTER, Table: yyS[yypt-2].bytes, NewName: yyS[yypt-2].bytes}
		}
	case 24:
		//line sql.y:248
		{
			// Change this to a rename statement
			yyVAL.statement = &DDL{Action: AST_RENAME, Table: yyS[yypt-3].bytes, NewName: yyS[yypt-0].bytes}
		}
	case 25:
		//line sql.y:253
		{
			yyVAL.statement = &DDL{Action: AST_ALTER, Table: yyS[yypt-1].bytes, NewName: yyS[yypt-1].bytes}
		}
	case 26:
		//line sql.y:259
		{
			yyVAL.statement = &DDL{Action: AST_RENAME, Table: yyS[yypt-2].bytes, NewName: yyS[yypt-0].bytes}
		}
	case 27:
		//line sql.y:265
		{
			yyVAL.statement = &DDL{Action: AST_DROP, Table: yyS[yypt-0].bytes}
		}
	case 28:
		//line sql.y:269
		{
			// Change this to an alter statement
			yyVAL.statement = &DDL{Action: AST_ALTER, Table: yyS[yypt-0].bytes, NewName: yyS[yypt-0].bytes}
		}
	case 29:
		//line sql.y:274
		{
			yyVAL.statement = &DDL{Action: AST_DROP, Table: yyS[yypt-1].bytes}
		}
	case 30:
		//line sql.y:280
		{
			yyVAL.statement = &DDL{Action: AST_ALTER, Table: yyS[yypt-0].bytes, NewName: yyS[yypt-0].bytes}
		}
	case 31:
		//line sql.y:286
		{
			yyVAL.statement = &Other{}
		}
	case 32:
		//line sql.y:290
		{
			yyVAL.statement = &Other{}
		}
	case 33:
		//line sql.y:294
		{
			yyVAL.statement = &Other{}
		}
	case 34:
		//line sql.y:299
		{
			setAllowComments(yylex, true)
		}
	case 35:
		//line sql.y:303
		{
			yyVAL.bytes2 = yyS[yypt-0].bytes2
			setAllowComments(yylex, false)
		}
	case 36:
		//line sql.y:309
		{
			yyVAL.bytes2 = nil
		}
	case 37:
		//line sql.y:313
		{
			yyVAL.bytes2 = append(yyS[yypt-1].bytes2, yyS[yypt-0].bytes)
		}
	case 38:
		//line sql.y:319
		{
			yyVAL.str = AST_UNION
		}
	case 39:
		//line sql.y:323
		{
			yyVAL.str = AST_UNION_ALL
		}
	case 40:
		//line sql.y:327
		{
			yyVAL.str = AST_SET_MINUS
		}
	case 41:
		//line sql.y:331
		{
			yyVAL.str = AST_EXCEPT
		}
	case 42:
		//line sql.y:335
		{
			yyVAL.str = AST_INTERSECT
		}
	case 43:
		//line sql.y:340
		{
			yyVAL.str = ""
		}
	case 44:
		//line sql.y:344
		{
			yyVAL.str = AST_DISTINCT
		}
	case 45:
		//line sql.y:350
		{
			yyVAL.selectExprs = SelectExprs{yyS[yypt-0].selectExpr}
		}
	case 46:
		//line sql.y:354
		{
			yyVAL.selectExprs = append(yyVAL.selectExprs, yyS[yypt-0].selectExpr)
		}
	case 47:
		//line sql.y:360
		{
			yyVAL.selectExpr = &StarExpr{}
		}
	case 48:
		//line sql.y:364
		{
			yyVAL.selectExpr = &NonStarExpr{Expr: yyS[yypt-1].expr, As: yyS[yypt-0].bytes}
		}
	case 49:
		//line sql.y:368
		{
			yyVAL.selectExpr = &StarExpr{TableName: yyS[yypt-2].bytes}
		}
	case 50:
		//line sql.y:374
		{
			yyVAL.expr = yyS[yypt-0].boolExpr
		}
	case 51:
		//line sql.y:378
		{
			yyVAL.expr = yyS[yypt-0].valExpr
		}
	case 52:
		//line sql.y:383
		{
			yyVAL.bytes = nil
		}
	case 53:
		//line sql.y:387
		{
			yyVAL.bytes = yyS[yypt-0].bytes
		}
	case 54:
		//line sql.y:391
		{
			yyVAL.bytes = yyS[yypt-0].bytes
		}
	case 55:
		//line sql.y:397
		{
			yyVAL.tableExprs = TableExprs{yyS[yypt-0].tableExpr}
		}
	case 56:
		//line sql.y:401
		{
			yyVAL.tableExprs = append(yyVAL.tableExprs, yyS[yypt-0].tableExpr)
		}
	case 57:
		//line sql.y:407
		{
			yyVAL.tableExpr = &AliasedTableExpr{Expr: yyS[yypt-2].smTableExpr, As: yyS[yypt-1].bytes, Hints: yyS[yypt-0].indexHints}
		}
	case 58:
		//line sql.y:411
		{
			yyVAL.tableExpr = &ParenTableExpr{Expr: yyS[yypt-1].tableExpr}
		}
	case 59:
		//line sql.y:415
		{
			yyVAL.tableExpr = &JoinTableExpr{LeftExpr: yyS[yypt-2].tableExpr, Join: yyS[yypt-1].str, RightExpr: yyS[yypt-0].tableExpr}
		}
	case 60:
		//line sql.y:419
		{
			yyVAL.tableExpr = &JoinTableExpr{LeftExpr: yyS[yypt-4].tableExpr, Join: yyS[yypt-3].str, RightExpr: yyS[yypt-2].tableExpr, On: yyS[yypt-0].boolExpr}
		}
	case 61:
		//line sql.y:424
		{
			yyVAL.bytes = nil
		}
	case 62:
		//line sql.y:428
		{
			yyVAL.bytes = yyS[yypt-0].bytes
		}
	case 63:
		//line sql.y:432
		{
			yyVAL.bytes = yyS[yypt-0].bytes
		}
	case 64:
		//line sql.y:438
		{
			yyVAL.str = AST_JOIN
		}
	case 65:
		//line sql.y:442
		{
			yyVAL.str = AST_STRAIGHT_JOIN
		}
	case 66:
		//line sql.y:446
		{
			yyVAL.str = AST_LEFT_JOIN
		}
	case 67:
		//line sql.y:450
		{
			yyVAL.str = AST_LEFT_JOIN
		}
	case 68:
		//line sql.y:454
		{
			yyVAL.str = AST_RIGHT_JOIN
		}
	case 69:
		//line sql.y:458
		{
			yyVAL.str = AST_RIGHT_JOIN
		}
	case 70:
		//line sql.y:462
		{
			yyVAL.str = AST_JOIN
		}
	case 71:
		//line sql.y:466
		{
			yyVAL.str = AST_CROSS_JOIN
		}
	case 72:
		//line sql.y:470
		{
			yyVAL.str = AST_NATURAL_JOIN
		}
	case 73:
		//line sql.y:476
		{
			yyVAL.smTableExpr = &TableName{Name: yyS[yypt-0].bytes}
		}
	case 74:
		//line sql.y:480
		{
			yyVAL.smTableExpr = &TableName{Qualifier: yyS[yypt-2].bytes, Name: yyS[yypt-0].bytes}
		}
	case 75:
		//line sql.y:484
		{
			yyVAL.smTableExpr = yyS[yypt-0].subquery
		}
	case 76:
		//line sql.y:490
		{
			yyVAL.tableName = &TableName{Name: yyS[yypt-0].bytes}
		}
	case 77:
		//line sql.y:494
		{
			yyVAL.tableName = &TableName{Qualifier: yyS[yypt-2].bytes, Name: yyS[yypt-0].bytes}
		}
	case 78:
		//line sql.y:499
		{
			yyVAL.indexHints = nil
		}
	case 79:
		//line sql.y:503
		{
			yyVAL.indexHints = &IndexHints{Type: AST_USE, Indexes: yyS[yypt-1].bytes2}
		}
	case 80:
		//line sql.y:507
		{
			yyVAL.indexHints = &IndexHints{Type: AST_IGNORE, Indexes: yyS[yypt-1].bytes2}
		}
	case 81:
		//line sql.y:511
		{
			yyVAL.indexHints = &IndexHints{Type: AST_FORCE, Indexes: yyS[yypt-1].bytes2}
		}
	case 82:
		//line sql.y:517
		{
			yyVAL.bytes2 = [][]byte{yyS[yypt-0].bytes}
		}
	case 83:
		//line sql.y:521
		{
			yyVAL.bytes2 = append(yyS[yypt-2].bytes2, yyS[yypt-0].bytes)
		}
	case 84:
		//line sql.y:526
		{
			yyVAL.boolExpr = nil
		}
	case 85:
		//line sql.y:530
		{
			yyVAL.boolExpr = yyS[yypt-0].boolExpr
		}
	case 86:
		yyVAL.boolExpr = yyS[yypt-0].boolExpr
	case 87:
		//line sql.y:537
		{
			yyVAL.boolExpr = &AndExpr{Left: yyS[yypt-2].boolExpr, Right: yyS[yypt-0].boolExpr}
		}
	case 88:
		//line sql.y:541
		{
			yyVAL.boolExpr = &OrExpr{Left: yyS[yypt-2].boolExpr, Right: yyS[yypt-0].boolExpr}
		}
	case 89:
		//line sql.y:545
		{
			yyVAL.boolExpr = &NotExpr{Expr: yyS[yypt-0].boolExpr}
		}
	case 90:
		//line sql.y:549
		{
			yyVAL.boolExpr = &ParenBoolExpr{Expr: yyS[yypt-1].boolExpr}
		}
	case 91:
		//line sql.y:555
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyS[yypt-2].valExpr, Operator: yyS[yypt-1].str, Right: yyS[yypt-0].valExpr}
		}
	case 92:
		//line sql.y:559
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyS[yypt-2].valExpr, Operator: AST_IN, Right: yyS[yypt-0].colTuple}
		}
	case 93:
		//line sql.y:563
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyS[yypt-3].valExpr, Operator: AST_NOT_IN, Right: yyS[yypt-0].colTuple}
		}
	case 94:
		//line sql.y:567
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyS[yypt-2].valExpr, Operator: AST_LIKE, Right: yyS[yypt-0].valExpr}
		}
	case 95:
		//line sql.y:571
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyS[yypt-3].valExpr, Operator: AST_NOT_LIKE, Right: yyS[yypt-0].valExpr}
		}
	case 96:
		//line sql.y:575
		{
			yyVAL.boolExpr = &RangeCond{Left: yyS[yypt-4].valExpr, Operator: AST_BETWEEN, From: yyS[yypt-2].valExpr, To: yyS[yypt-0].valExpr}
		}
	case 97:
		//line sql.y:579
		{
			yyVAL.boolExpr = &RangeCond{Left: yyS[yypt-5].valExpr, Operator: AST_NOT_BETWEEN, From: yyS[yypt-2].valExpr, To: yyS[yypt-0].valExpr}
		}
	case 98:
		//line sql.y:583
		{
			yyVAL.boolExpr = &NullCheck{Operator: AST_IS_NULL, Expr: yyS[yypt-2].valExpr}
		}
	case 99:
		//line sql.y:587
		{
			yyVAL.boolExpr = &NullCheck{Operator: AST_IS_NOT_NULL, Expr: yyS[yypt-3].valExpr}
		}
	case 100:
		//line sql.y:591
		{
			yyVAL.boolExpr = &ExistsExpr{Subquery: yyS[yypt-0].subquery}
		}
	case 101:
		//line sql.y:595
		{
			yyVAL.boolExpr = &KeyrangeExpr{Start: yyS[yypt-3].valExpr, End: yyS[yypt-1].valExpr}
		}
	case 102:
		//line sql.y:601
		{
			yyVAL.str = AST_EQ
		}
	case 103:
		//line sql.y:605
		{
			yyVAL.str = AST_LT
		}
	case 104:
		//line sql.y:609
		{
			yyVAL.str = AST_GT
		}
	case 105:
		//line sql.y:613
		{
			yyVAL.str = AST_LE
		}
	case 106:
		//line sql.y:617
		{
			yyVAL.str = AST_GE
		}
	case 107:
		//line sql.y:621
		{
			yyVAL.str = AST_NE
		}
	case 108:
		//line sql.y:625
		{
			yyVAL.str = AST_NSE
		}
	case 109:
		//line sql.y:631
		{
			yyVAL.colTuple = ValTuple(yyS[yypt-1].valExprs)
		}
	case 110:
		//line sql.y:635
		{
			yyVAL.colTuple = yyS[yypt-0].subquery
		}
	case 111:
		//line sql.y:639
		{
			yyVAL.colTuple = ListArg(yyS[yypt-0].bytes)
		}
	case 112:
		//line sql.y:645
		{
			yyVAL.subquery = &Subquery{yyS[yypt-1].selStmt}
		}
	case 113:
		//line sql.y:651
		{
			yyVAL.valExprs = ValExprs{yyS[yypt-0].valExpr}
		}
	case 114:
		//line sql.y:655
		{
			yyVAL.valExprs = append(yyS[yypt-2].valExprs, yyS[yypt-0].valExpr)
		}
	case 115:
		//line sql.y:661
		{
			yyVAL.valExpr = yyS[yypt-0].valExpr
		}
	case 116:
		//line sql.y:665
		{
			yyVAL.valExpr = yyS[yypt-0].colName
		}
	case 117:
		//line sql.y:669
		{
			yyVAL.valExpr = yyS[yypt-0].rowTuple
		}
	case 118:
		//line sql.y:673
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyS[yypt-2].valExpr, Operator: AST_BITAND, Right: yyS[yypt-0].valExpr}
		}
	case 119:
		//line sql.y:677
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyS[yypt-2].valExpr, Operator: AST_BITOR, Right: yyS[yypt-0].valExpr}
		}
	case 120:
		//line sql.y:681
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyS[yypt-2].valExpr, Operator: AST_BITXOR, Right: yyS[yypt-0].valExpr}
		}
	case 121:
		//line sql.y:685
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyS[yypt-2].valExpr, Operator: AST_PLUS, Right: yyS[yypt-0].valExpr}
		}
	case 122:
		//line sql.y:689
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyS[yypt-2].valExpr, Operator: AST_MINUS, Right: yyS[yypt-0].valExpr}
		}
	case 123:
		//line sql.y:693
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyS[yypt-2].valExpr, Operator: AST_MULT, Right: yyS[yypt-0].valExpr}
		}
	case 124:
		//line sql.y:697
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyS[yypt-2].valExpr, Operator: AST_DIV, Right: yyS[yypt-0].valExpr}
		}
	case 125:
		//line sql.y:701
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyS[yypt-2].valExpr, Operator: AST_MOD, Right: yyS[yypt-0].valExpr}
		}
	case 126:
		//line sql.y:705
		{
			if num, ok := yyS[yypt-0].valExpr.(NumVal); ok {
				switch yyS[yypt-1].byt {
				case '-':
					yyVAL.valExpr = append(NumVal("-"), num...)
				case '+':
					yyVAL.valExpr = num
				default:
					yyVAL.valExpr = &UnaryExpr{Operator: yyS[yypt-1].byt, Expr: yyS[yypt-0].valExpr}
				}
			} else {
				yyVAL.valExpr = &UnaryExpr{Operator: yyS[yypt-1].byt, Expr: yyS[yypt-0].valExpr}
			}
		}
	case 127:
		//line sql.y:720
		{
			yyVAL.valExpr = &FuncExpr{Name: yyS[yypt-2].bytes}
		}
	case 128:
		//line sql.y:724
		{
			yyVAL.valExpr = &FuncExpr{Name: yyS[yypt-3].bytes, Exprs: yyS[yypt-1].selectExprs}
		}
	case 129:
		//line sql.y:728
		{
			yyVAL.valExpr = &FuncExpr{Name: yyS[yypt-4].bytes, Distinct: true, Exprs: yyS[yypt-1].selectExprs}
		}
	case 130:
		//line sql.y:732
		{
			yyVAL.valExpr = &FuncExpr{Name: yyS[yypt-3].bytes, Exprs: yyS[yypt-1].selectExprs}
		}
	case 131:
		//line sql.y:736
		{
			yyVAL.valExpr = yyS[yypt-0].caseExpr
		}
	case 132:
		//line sql.y:742
		{
			yyVAL.bytes = IF_BYTES
		}
	case 133:
		//line sql.y:746
		{
			yyVAL.bytes = VALUES_BYTES
		}
	case 134:
		//line sql.y:752
		{
			yyVAL.byt = AST_UPLUS
		}
	case 135:
		//line sql.y:756
		{
			yyVAL.byt = AST_UMINUS
		}
	case 136:
		//line sql.y:760
		{
			yyVAL.byt = AST_TILDA
		}
	case 137:
		//line sql.y:766
		{
			yyVAL.caseExpr = &CaseExpr{Expr: yyS[yypt-3].valExpr, Whens: yyS[yypt-2].whens, Else: yyS[yypt-1].valExpr}
		}
	case 138:
		//line sql.y:771
		{
			yyVAL.valExpr = nil
		}
	case 139:
		//line sql.y:775
		{
			yyVAL.valExpr = yyS[yypt-0].valExpr
		}
	case 140:
		//line sql.y:781
		{
			yyVAL.whens = []*When{yyS[yypt-0].when}
		}
	case 141:
		//line sql.y:785
		{
			yyVAL.whens = append(yyS[yypt-1].whens, yyS[yypt-0].when)
		}
	case 142:
		//line sql.y:791
		{
			yyVAL.when = &When{Cond: yyS[yypt-2].boolExpr, Val: yyS[yypt-0].valExpr}
		}
	case 143:
		//line sql.y:796
		{
			yyVAL.valExpr = nil
		}
	case 144:
		//line sql.y:800
		{
			yyVAL.valExpr = yyS[yypt-0].valExpr
		}
	case 145:
		//line sql.y:806
		{
			yyVAL.colName = &ColName{Name: yyS[yypt-0].bytes}
		}
	case 146:
		//line sql.y:810
		{
			yyVAL.colName = &ColName{Qualifier: yyS[yypt-2].bytes, Name: yyS[yypt-0].bytes}
		}
	case 147:
		//line sql.y:816
		{
			yyVAL.valExpr = StrVal(yyS[yypt-0].bytes)
		}
	case 148:
		//line sql.y:820
		{
			yyVAL.valExpr = NumVal(yyS[yypt-0].bytes)
		}
	case 149:
		//line sql.y:824
		{
			yyVAL.valExpr = ValArg(yyS[yypt-0].bytes)
		}
	case 150:
		//line sql.y:828
		{
			yyVAL.valExpr = &NullVal{}
		}
	case 151:
		//line sql.y:833
		{
			yyVAL.valExprs = nil
		}
	case 152:
		//line sql.y:837
		{
			yyVAL.valExprs = yyS[yypt-0].valExprs
		}
	case 153:
		//line sql.y:842
		{
			yyVAL.boolExpr = nil
		}
	case 154:
		//line sql.y:846
		{
			yyVAL.boolExpr = yyS[yypt-0].boolExpr
		}
	case 155:
		//line sql.y:851
		{
			yyVAL.orderBy = nil
		}
	case 156:
		//line sql.y:855
		{
			yyVAL.orderBy = yyS[yypt-0].orderBy
		}
	case 157:
		//line sql.y:861
		{
			yyVAL.orderBy = OrderBy{yyS[yypt-0].order}
		}
	case 158:
		//line sql.y:865
		{
			yyVAL.orderBy = append(yyS[yypt-2].orderBy, yyS[yypt-0].order)
		}
	case 159:
		//line sql.y:871
		{
			yyVAL.order = &Order{Expr: yyS[yypt-1].valExpr, Direction: yyS[yypt-0].str}
		}
	case 160:
		//line sql.y:876
		{
			yyVAL.str = AST_ASC
		}
	case 161:
		//line sql.y:880
		{
			yyVAL.str = AST_ASC
		}
	case 162:
		//line sql.y:884
		{
			yyVAL.str = AST_DESC
		}
	case 163:
		//line sql.y:889
		{
			yyVAL.limit = nil
		}
	case 164:
		//line sql.y:893
		{
			yyVAL.limit = &Limit{Rowcount: yyS[yypt-0].valExpr}
		}
	case 165:
		//line sql.y:897
		{
			yyVAL.limit = &Limit{Offset: yyS[yypt-2].valExpr, Rowcount: yyS[yypt-0].valExpr}
		}
	case 166:
		//line sql.y:902
		{
			yyVAL.str = ""
		}
	case 167:
		//line sql.y:906
		{
			yyVAL.str = AST_FOR_UPDATE
		}
	case 168:
		//line sql.y:910
		{
			if !bytes.Equal(yyS[yypt-1].bytes, SHARE) {
				yylex.Error("expecting share")
				return 1
			}
			if !bytes.Equal(yyS[yypt-0].bytes, MODE) {
				yylex.Error("expecting mode")
				return 1
			}
			yyVAL.str = AST_SHARE_MODE
		}
	case 169:
		//line sql.y:923
		{
			yyVAL.columns = nil
		}
	case 170:
		//line sql.y:927
		{
			yyVAL.columns = yyS[yypt-1].columns
		}
	case 171:
		//line sql.y:933
		{
			yyVAL.columns = Columns{&NonStarExpr{Expr: yyS[yypt-0].colName}}
		}
	case 172:
		//line sql.y:937
		{
			yyVAL.columns = append(yyVAL.columns, &NonStarExpr{Expr: yyS[yypt-0].colName})
		}
	case 173:
		//line sql.y:942
		{
			yyVAL.updateExprs = nil
		}
	case 174:
		//line sql.y:946
		{
			yyVAL.updateExprs = yyS[yypt-0].updateExprs
		}
	case 175:
		//line sql.y:952
		{
			yyVAL.insRows = yyS[yypt-0].values
		}
	case 176:
		//line sql.y:956
		{
			yyVAL.insRows = yyS[yypt-0].selStmt
		}
	case 177:
		//line sql.y:962
		{
			yyVAL.values = Values{yyS[yypt-0].rowTuple}
		}
	case 178:
		//line sql.y:966
		{
			yyVAL.values = append(yyS[yypt-2].values, yyS[yypt-0].rowTuple)
		}
	case 179:
		//line sql.y:972
		{
			yyVAL.rowTuple = ValTuple(yyS[yypt-1].valExprs)
		}
	case 180:
		//line sql.y:976
		{
			yyVAL.rowTuple = yyS[yypt-0].subquery
		}
	case 181:
		//line sql.y:982
		{
			yyVAL.updateExprs = UpdateExprs{yyS[yypt-0].updateExpr}
		}
	case 182:
		//line sql.y:986
		{
			yyVAL.updateExprs = append(yyS[yypt-2].updateExprs, yyS[yypt-0].updateExpr)
		}
	case 183:
		//line sql.y:992
		{
			yyVAL.updateExpr = &UpdateExpr{Name: yyS[yypt-2].colName, Expr: yyS[yypt-0].valExpr}
		}
	case 184:
		//line sql.y:997
		{
			yyVAL.empty = struct{}{}
		}
	case 185:
		//line sql.y:999
		{
			yyVAL.empty = struct{}{}
		}
	case 186:
		//line sql.y:1002
		{
			yyVAL.empty = struct{}{}
		}
	case 187:
		//line sql.y:1004
		{
			yyVAL.empty = struct{}{}
		}
	case 188:
		//line sql.y:1007
		{
			yyVAL.empty = struct{}{}
		}
	case 189:
		//line sql.y:1009
		{
			yyVAL.empty = struct{}{}
		}
	case 190:
		//line sql.y:1013
		{
			yyVAL.empty = struct{}{}
		}
	case 191:
		//line sql.y:1015
		{
			yyVAL.empty = struct{}{}
		}
	case 192:
		//line sql.y:1017
		{
			yyVAL.empty = struct{}{}
		}
	case 193:
		//line sql.y:1019
		{
			yyVAL.empty = struct{}{}
		}
	case 194:
		//line sql.y:1021
		{
			yyVAL.empty = struct{}{}
		}
	case 195:
		//line sql.y:1024
		{
			yyVAL.empty = struct{}{}
		}
	case 196:
		//line sql.y:1026
		{
			yyVAL.empty = struct{}{}
		}
	case 197:
		//line sql.y:1029
		{
			yyVAL.empty = struct{}{}
		}
	case 198:
		//line sql.y:1031
		{
			yyVAL.empty = struct{}{}
		}
	case 199:
		//line sql.y:1034
		{
			yyVAL.empty = struct{}{}
		}
	case 200:
		//line sql.y:1036
		{
			yyVAL.empty = struct{}{}
		}
	case 201:
		//line sql.y:1040
		{
			yyVAL.bytes = bytes.ToLower(yyS[yypt-0].bytes)
		}
	case 202:
		//line sql.y:1046
		{
			if incNesting(yylex) {
				yylex.Error("max nesting level reached")
				return 1
			}
		}
	case 203:
		//line sql.y:1055
		{
			decNesting(yylex)
		}
	case 204:
		//line sql.y:1060
		{
			forceEOF(yylex)
		}
	}
	goto yystack /* stack new state and value */
}
