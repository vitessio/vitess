//line sql.y:6
package sqlparser

import __yyfmt__ "fmt"

//line sql.y:6
import "bytes"

func SetParseTree(yylex interface{}, stmt Statement) {
	yylex.(*Tokenizer).ParseTree = stmt
}

func SetAllowComments(yylex interface{}, allow bool) {
	yylex.(*Tokenizer).AllowComments = allow
}

func ForceEOF(yylex interface{}) {
	yylex.(*Tokenizer).ForceEOF = true
}

var (
	SHARE        = []byte("share")
	MODE         = []byte("mode")
	IF_BYTES     = []byte("if")
	VALUES_BYTES = []byte("values")
)

//line sql.y:31
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
const ID = 57377
const STRING = 57378
const NUMBER = 57379
const VALUE_ARG = 57380
const LIST_ARG = 57381
const COMMENT = 57382
const LE = 57383
const GE = 57384
const NE = 57385
const NULL_SAFE_EQUAL = 57386
const UNION = 57387
const MINUS = 57388
const EXCEPT = 57389
const INTERSECT = 57390
const JOIN = 57391
const STRAIGHT_JOIN = 57392
const LEFT = 57393
const RIGHT = 57394
const INNER = 57395
const OUTER = 57396
const CROSS = 57397
const NATURAL = 57398
const USE = 57399
const FORCE = 57400
const ON = 57401
const AND = 57402
const OR = 57403
const NOT = 57404
const UNARY = 57405
const CASE = 57406
const WHEN = 57407
const THEN = 57408
const ELSE = 57409
const END = 57410
const CREATE = 57411
const ALTER = 57412
const DROP = 57413
const RENAME = 57414
const ANALYZE = 57415
const TABLE = 57416
const INDEX = 57417
const VIEW = 57418
const TO = 57419
const IGNORE = 57420
const IF = 57421
const UNIQUE = 57422
const USING = 57423
const SHOW = 57424
const DESCRIBE = 57425
const EXPLAIN = 57426

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
	" (",
	" =",
	" <",
	" >",
	" ~",
	"UNION",
	"MINUS",
	"EXCEPT",
	"INTERSECT",
	" ,",
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
	"AND",
	"OR",
	"NOT",
	" &",
	" |",
	" ^",
	" +",
	" -",
	" *",
	" /",
	" %",
	" .",
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

const yyNprod = 202
const yyPrivate = 57344

var yyTokenNames []string
var yyStates []string

const yyLast = 601

var yyAct = []int{

	94, 290, 158, 358, 91, 85, 326, 245, 62, 161,
	92, 282, 236, 80, 196, 207, 367, 160, 3, 367,
	63, 176, 184, 134, 135, 81, 50, 256, 257, 258,
	259, 260, 367, 261, 262, 129, 228, 288, 65, 129,
	76, 70, 64, 68, 73, 53, 252, 129, 77, 233,
	123, 97, 51, 52, 337, 102, 101, 226, 86, 107,
	228, 43, 369, 44, 336, 368, 84, 98, 99, 100,
	119, 317, 311, 46, 47, 48, 89, 335, 366, 127,
	105, 316, 313, 287, 131, 277, 28, 29, 30, 31,
	69, 72, 162, 275, 157, 159, 163, 120, 49, 88,
	122, 45, 237, 103, 104, 82, 229, 38, 118, 40,
	108, 170, 65, 41, 267, 65, 64, 180, 179, 64,
	174, 133, 134, 135, 116, 106, 307, 309, 237, 112,
	280, 231, 86, 202, 180, 178, 227, 319, 332, 206,
	204, 205, 214, 215, 181, 218, 219, 220, 221, 222,
	223, 224, 225, 167, 194, 201, 308, 142, 143, 144,
	145, 146, 147, 148, 149, 134, 135, 230, 86, 86,
	71, 216, 190, 65, 65, 283, 248, 64, 243, 232,
	234, 241, 342, 247, 200, 249, 147, 148, 149, 240,
	126, 188, 334, 209, 191, 333, 244, 142, 143, 144,
	145, 146, 147, 148, 149, 14, 15, 16, 17, 203,
	305, 230, 250, 266, 217, 270, 271, 253, 268, 145,
	146, 147, 148, 149, 114, 115, 304, 269, 303, 301,
	177, 274, 299, 18, 302, 283, 86, 300, 28, 29,
	30, 31, 14, 281, 187, 189, 186, 276, 279, 114,
	285, 128, 289, 286, 200, 228, 314, 345, 346, 142,
	143, 144, 145, 146, 147, 148, 149, 209, 343, 297,
	298, 321, 199, 353, 254, 315, 71, 352, 172, 177,
	59, 351, 198, 318, 19, 20, 22, 21, 23, 65,
	173, 323, 164, 322, 324, 327, 129, 24, 25, 26,
	142, 143, 144, 145, 146, 147, 148, 149, 168, 75,
	200, 200, 256, 257, 258, 259, 260, 338, 261, 262,
	328, 272, 339, 114, 142, 143, 144, 145, 146, 147,
	148, 149, 341, 199, 230, 166, 348, 347, 350, 110,
	165, 349, 113, 198, 109, 355, 327, 66, 312, 357,
	356, 265, 359, 359, 359, 65, 360, 361, 78, 64,
	340, 97, 310, 362, 210, 132, 101, 264, 372, 107,
	208, 364, 373, 294, 374, 293, 84, 98, 99, 100,
	193, 71, 192, 175, 124, 121, 89, 117, 60, 365,
	105, 79, 74, 111, 320, 14, 14, 58, 211, 273,
	212, 213, 371, 182, 125, 32, 56, 291, 331, 88,
	54, 97, 292, 103, 104, 82, 101, 246, 239, 107,
	108, 34, 35, 36, 37, 330, 66, 98, 99, 100,
	296, 177, 61, 370, 354, 106, 89, 14, 33, 183,
	105, 39, 251, 185, 42, 67, 242, 171, 363, 344,
	97, 14, 325, 329, 295, 101, 278, 169, 107, 88,
	235, 96, 93, 103, 104, 66, 98, 99, 100, 95,
	108, 101, 284, 90, 107, 89, 238, 136, 87, 105,
	306, 66, 98, 99, 100, 106, 197, 255, 195, 83,
	263, 164, 130, 55, 27, 105, 57, 13, 88, 12,
	11, 10, 103, 104, 9, 8, 7, 6, 5, 108,
	101, 4, 2, 107, 1, 0, 0, 0, 103, 104,
	66, 98, 99, 100, 106, 108, 0, 0, 0, 0,
	164, 0, 0, 0, 105, 0, 0, 0, 0, 0,
	106, 0, 0, 0, 0, 137, 141, 139, 140, 0,
	0, 0, 0, 0, 0, 0, 0, 103, 104, 0,
	0, 0, 0, 0, 108, 153, 154, 155, 156, 0,
	150, 151, 152, 0, 0, 0, 0, 0, 0, 106,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 138, 142, 143, 144, 145, 146, 147, 148,
	149,
}
var yyPact = []int{

	200, -1000, -1000, 188, -1000, -1000, -1000, -1000, -1000, -1000,
	-1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, 18,
	-30, 12, -16, 9, -1000, -1000, -1000, 432, 393, -1000,
	-1000, -1000, 388, -1000, 368, 353, 423, 312, -51, 0,
	241, -1000, 2, 241, -1000, 357, -54, 241, -54, 356,
	-1000, -1000, -1000, -1000, -1000, 341, -1000, 304, 353, 360,
	52, 353, 195, -1000, 179, -1000, 47, 352, 40, 241,
	-1000, -1000, 350, -1000, -42, 349, 384, 125, 241, -1000,
	242, -1000, -1000, 346, 44, 99, 524, -1000, 430, 391,
	-1000, -1000, -1000, 485, 295, 290, -1000, 263, -1000, -1000,
	-1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, 485, -1000,
	245, 312, 348, 421, 312, 485, 241, -1000, 383, -74,
	-1000, 159, -1000, 347, -1000, -1000, 345, -1000, 298, 341,
	-1000, -1000, 241, 135, 430, 430, 485, 325, 377, 485,
	485, 146, 485, 485, 485, 485, 485, 485, 485, 485,
	-1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, 524, -43,
	36, 6, 524, -1000, 446, 31, 341, -1000, 432, 22,
	88, 390, 312, 312, 269, -1000, 404, 430, -1000, 88,
	-1000, -1000, -1000, 111, 241, -1000, -46, -1000, -1000, -1000,
	-1000, -1000, -1000, -1000, -1000, 220, 257, 332, 237, 37,
	-1000, -1000, -1000, -1000, -1000, -1000, 88, -1000, 446, -1000,
	-1000, 325, 485, 485, 88, 255, -1000, 374, 147, 147,
	147, 112, 112, -1000, -1000, -1000, -1000, -1000, 485, -1000,
	88, -1000, -7, 341, -15, 48, -1000, 430, 110, 247,
	188, 170, -17, -1000, 404, 392, 398, 99, 340, -1000,
	-1000, 338, -1000, 419, 298, 298, -1000, -1000, 177, 174,
	173, 171, 155, 63, -1000, 327, -28, 313, -18, -1000,
	88, 190, 485, -1000, 88, -1000, -19, -1000, -12, -1000,
	485, 56, -1000, 364, 217, -1000, -1000, -1000, 312, 392,
	-1000, 485, 485, -1000, -1000, 413, 394, 257, 73, -1000,
	140, -1000, 137, -1000, -1000, -1000, -1000, -13, -26, -36,
	-1000, -1000, -1000, -1000, 485, 88, -1000, -1000, 88, 485,
	329, 247, -1000, -1000, 128, 214, -1000, 231, -1000, 404,
	430, 485, 430, -1000, -1000, 236, 232, 228, 88, 88,
	427, -1000, 485, 485, -1000, -1000, -1000, 392, 99, 201,
	99, 241, 241, 241, 312, 88, -1000, 355, -22, -1000,
	-35, -38, 195, -1000, 426, 381, -1000, 241, -1000, -1000,
	-1000, 241, -1000, 241, -1000,
}
var yyPgo = []int{

	0, 514, 512, 17, 511, 508, 507, 506, 505, 504,
	501, 500, 499, 497, 405, 496, 494, 493, 13, 25,
	492, 490, 489, 488, 14, 487, 486, 280, 480, 3,
	21, 5, 478, 477, 476, 473, 2, 15, 9, 472,
	10, 469, 55, 462, 4, 461, 460, 12, 457, 456,
	454, 453, 7, 452, 6, 449, 1, 448, 447, 446,
	11, 8, 20, 309, 445, 444, 443, 442, 441, 439,
	0, 26, 438,
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
	32, 33, 33, 33, 33, 33, 33, 33, 37, 37,
	37, 42, 38, 38, 36, 36, 36, 36, 36, 36,
	36, 36, 36, 36, 36, 36, 36, 36, 36, 36,
	36, 41, 41, 43, 43, 43, 45, 48, 48, 46,
	46, 47, 49, 49, 44, 44, 35, 35, 35, 35,
	50, 50, 51, 51, 52, 52, 53, 53, 54, 55,
	55, 55, 56, 56, 56, 57, 57, 57, 58, 58,
	59, 59, 60, 60, 34, 34, 39, 39, 40, 40,
	61, 61, 62, 63, 63, 64, 64, 65, 65, 66,
	66, 66, 66, 66, 67, 67, 68, 68, 69, 69,
	70, 71,
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
	2, 1, 1, 1, 1, 1, 1, 1, 3, 1,
	1, 3, 1, 3, 1, 1, 1, 3, 3, 3,
	3, 3, 3, 3, 3, 2, 3, 4, 5, 4,
	1, 1, 1, 1, 1, 1, 5, 0, 1, 1,
	2, 4, 0, 2, 1, 3, 1, 1, 1, 1,
	0, 3, 0, 2, 0, 3, 1, 3, 2, 0,
	1, 1, 0, 2, 4, 0, 2, 4, 0, 3,
	1, 3, 0, 5, 2, 1, 1, 3, 3, 1,
	1, 3, 3, 0, 2, 0, 3, 0, 1, 1,
	1, 1, 1, 1, 0, 1, 0, 1, 0, 2,
	1, 0,
}
var yyChk = []int{

	-1000, -1, -2, -3, -4, -5, -6, -7, -8, -9,
	-10, -11, -12, -13, 5, 6, 7, 8, 33, 84,
	85, 87, 86, 88, 97, 98, 99, -16, 50, 51,
	52, 53, -14, -72, -14, -14, -14, -14, 89, -68,
	91, 95, -65, 91, 93, 89, 89, 90, 91, 89,
	-71, -71, -71, -3, 17, -17, 18, -15, 29, -27,
	35, 9, -61, -62, -44, -70, 35, -64, 94, 90,
	-70, 35, 89, -70, 35, -63, 94, -70, -63, 35,
	-18, -19, 74, -22, 35, -31, -36, -32, 68, 45,
	-35, -44, -40, -43, -70, -41, -45, 20, 36, 37,
	38, 25, -42, 72, 73, 49, 94, 28, 79, 40,
	-27, 33, 77, -27, 54, 46, 77, 35, 68, -70,
	-71, 35, -71, 92, 35, 20, 65, -70, 9, 54,
	-20, -70, 19, 77, 66, 67, -33, 21, 68, 23,
	24, 22, 69, 70, 71, 72, 73, 74, 75, 76,
	46, 47, 48, 41, 42, 43, 44, -31, -36, -31,
	-3, -38, -36, -36, 45, 45, 45, -42, 45, -48,
	-36, -58, 33, 45, -61, 35, -30, 10, -62, -36,
	-70, -71, 20, -69, 96, -66, 87, 85, 32, 86,
	13, 35, 35, 35, -71, -23, -24, -26, 45, 35,
	-42, -19, -70, 74, -31, -31, -36, -37, 45, -42,
	39, 21, 23, 24, -36, -36, 25, 68, -36, -36,
	-36, -36, -36, -36, -36, -36, 100, 100, 54, 100,
	-36, 100, -18, 18, -18, -46, -47, 80, -34, 28,
	-3, -61, -59, -44, -30, -52, 13, -31, 65, -70,
	-71, -67, 92, -30, 54, -25, 55, 56, 57, 58,
	59, 61, 62, -21, 35, 19, -24, 77, -38, -37,
	-36, -36, 66, 25, -36, 100, -18, 100, -49, -47,
	82, -31, -60, 65, -39, -40, -60, 100, 54, -52,
	-56, 15, 14, 35, 35, -50, 11, -24, -24, 55,
	60, 55, 60, 55, 55, 55, -28, 63, 93, 64,
	35, 100, 35, 100, 66, -36, 100, 83, -36, 81,
	30, 54, -44, -56, -36, -53, -54, -36, -71, -51,
	12, 14, 65, 55, 55, 90, 90, 90, -36, -36,
	31, -40, 54, 54, -55, 26, 27, -52, -31, -38,
	-31, 45, 45, 45, 7, -36, -54, -56, -29, -70,
	-29, -29, -61, -57, 16, 34, 100, 54, 100, 100,
	7, 21, -70, -70, -70,
}
var yyDef = []int{

	0, -2, 1, 2, 3, 4, 5, 6, 7, 8,
	9, 10, 11, 12, 34, 34, 34, 34, 34, 196,
	187, 0, 0, 0, 201, 201, 201, 0, 38, 40,
	41, 42, 43, 36, 0, 0, 0, 0, 185, 0,
	0, 197, 0, 0, 188, 0, 183, 0, 183, 0,
	31, 32, 33, 14, 39, 0, 44, 35, 0, 0,
	76, 0, 19, 180, 0, 144, 200, 0, 0, 0,
	201, 200, 0, 201, 0, 0, 0, 0, 0, 30,
	0, 45, 47, 52, 200, 50, 51, 86, 0, 0,
	114, 115, 116, 0, 144, 0, 130, 0, 146, 147,
	148, 149, 179, 133, 134, 135, 131, 132, 137, 37,
	168, 0, 0, 84, 0, 0, 0, 201, 0, 198,
	22, 0, 25, 0, 27, 184, 0, 201, 0, 0,
	48, 53, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	101, 102, 103, 104, 105, 106, 107, 89, 0, 0,
	0, 0, 112, 125, 0, 0, 0, 100, 0, 0,
	138, 0, 0, 0, 84, 77, 154, 0, 181, 182,
	145, 20, 186, 0, 0, 201, 194, 189, 190, 191,
	192, 193, 26, 28, 29, 84, 55, 61, 0, 73,
	75, 46, 54, 49, 87, 88, 91, 92, 0, 109,
	110, 0, 0, 0, 94, 0, 98, 0, 117, 118,
	119, 120, 121, 122, 123, 124, 90, 111, 0, 178,
	112, 126, 0, 0, 0, 142, 139, 0, 172, 0,
	175, 172, 0, 170, 154, 162, 0, 85, 0, 199,
	23, 0, 195, 150, 0, 0, 64, 65, 0, 0,
	0, 0, 0, 78, 62, 0, 0, 0, 0, 93,
	95, 0, 0, 99, 113, 127, 0, 129, 0, 140,
	0, 0, 15, 0, 174, 176, 16, 169, 0, 162,
	18, 0, 0, 201, 24, 152, 0, 56, 59, 66,
	0, 68, 0, 70, 71, 72, 57, 0, 0, 0,
	63, 58, 74, 108, 0, 96, 128, 136, 143, 0,
	0, 0, 171, 17, 163, 155, 156, 159, 21, 154,
	0, 0, 0, 67, 69, 0, 0, 0, 97, 141,
	0, 177, 0, 0, 158, 160, 161, 162, 153, 151,
	60, 0, 0, 0, 0, 164, 157, 165, 0, 82,
	0, 0, 173, 13, 0, 0, 79, 0, 80, 81,
	166, 0, 83, 0, 167,
}
var yyTok1 = []int{

	1, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 76, 69, 3,
	45, 100, 74, 72, 54, 73, 77, 75, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	47, 46, 48, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 71, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 70, 3, 49,
}
var yyTok2 = []int{

	2, 3, 4, 5, 6, 7, 8, 9, 10, 11,
	12, 13, 14, 15, 16, 17, 18, 19, 20, 21,
	22, 23, 24, 25, 26, 27, 28, 29, 30, 31,
	32, 33, 34, 35, 36, 37, 38, 39, 40, 41,
	42, 43, 44, 50, 51, 52, 53, 55, 56, 57,
	58, 59, 60, 61, 62, 63, 64, 65, 66, 67,
	68, 78, 79, 80, 81, 82, 83, 84, 85, 86,
	87, 88, 89, 90, 91, 92, 93, 94, 95, 96,
	97, 98, 99,
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
		//line sql.y:150
		{
			SetParseTree(yylex, yyS[yypt-0].statement)
		}
	case 2:
		//line sql.y:156
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
		//line sql.y:172
		{
			yyVAL.selStmt = &Select{Comments: Comments(yyS[yypt-10].bytes2), Distinct: yyS[yypt-9].str, SelectExprs: yyS[yypt-8].selectExprs, From: yyS[yypt-6].tableExprs, Where: NewWhere(AST_WHERE, yyS[yypt-5].boolExpr), GroupBy: GroupBy(yyS[yypt-4].valExprs), Having: NewWhere(AST_HAVING, yyS[yypt-3].boolExpr), OrderBy: yyS[yypt-2].orderBy, Limit: yyS[yypt-1].limit, Lock: yyS[yypt-0].str}
		}
	case 14:
		//line sql.y:176
		{
			yyVAL.selStmt = &Union{Type: yyS[yypt-1].str, Left: yyS[yypt-2].selStmt, Right: yyS[yypt-0].selStmt}
		}
	case 15:
		//line sql.y:182
		{
			yyVAL.statement = &Insert{Comments: Comments(yyS[yypt-5].bytes2), Table: yyS[yypt-3].tableName, Columns: yyS[yypt-2].columns, Rows: yyS[yypt-1].insRows, OnDup: OnDup(yyS[yypt-0].updateExprs)}
		}
	case 16:
		//line sql.y:186
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
		//line sql.y:198
		{
			yyVAL.statement = &Update{Comments: Comments(yyS[yypt-6].bytes2), Table: yyS[yypt-5].tableName, Exprs: yyS[yypt-3].updateExprs, Where: NewWhere(AST_WHERE, yyS[yypt-2].boolExpr), OrderBy: yyS[yypt-1].orderBy, Limit: yyS[yypt-0].limit}
		}
	case 18:
		//line sql.y:204
		{
			yyVAL.statement = &Delete{Comments: Comments(yyS[yypt-5].bytes2), Table: yyS[yypt-3].tableName, Where: NewWhere(AST_WHERE, yyS[yypt-2].boolExpr), OrderBy: yyS[yypt-1].orderBy, Limit: yyS[yypt-0].limit}
		}
	case 19:
		//line sql.y:210
		{
			yyVAL.statement = &Set{Comments: Comments(yyS[yypt-1].bytes2), Exprs: yyS[yypt-0].updateExprs}
		}
	case 20:
		//line sql.y:216
		{
			yyVAL.statement = &DDL{Action: AST_CREATE, NewName: yyS[yypt-1].bytes}
		}
	case 21:
		//line sql.y:220
		{
			// Change this to an alter statement
			yyVAL.statement = &DDL{Action: AST_ALTER, Table: yyS[yypt-1].bytes, NewName: yyS[yypt-1].bytes}
		}
	case 22:
		//line sql.y:225
		{
			yyVAL.statement = &DDL{Action: AST_CREATE, NewName: yyS[yypt-1].bytes}
		}
	case 23:
		//line sql.y:231
		{
			yyVAL.statement = &DDL{Action: AST_ALTER, Table: yyS[yypt-2].bytes, NewName: yyS[yypt-2].bytes}
		}
	case 24:
		//line sql.y:235
		{
			// Change this to a rename statement
			yyVAL.statement = &DDL{Action: AST_RENAME, Table: yyS[yypt-3].bytes, NewName: yyS[yypt-0].bytes}
		}
	case 25:
		//line sql.y:240
		{
			yyVAL.statement = &DDL{Action: AST_ALTER, Table: yyS[yypt-1].bytes, NewName: yyS[yypt-1].bytes}
		}
	case 26:
		//line sql.y:246
		{
			yyVAL.statement = &DDL{Action: AST_RENAME, Table: yyS[yypt-2].bytes, NewName: yyS[yypt-0].bytes}
		}
	case 27:
		//line sql.y:252
		{
			yyVAL.statement = &DDL{Action: AST_DROP, Table: yyS[yypt-0].bytes}
		}
	case 28:
		//line sql.y:256
		{
			// Change this to an alter statement
			yyVAL.statement = &DDL{Action: AST_ALTER, Table: yyS[yypt-0].bytes, NewName: yyS[yypt-0].bytes}
		}
	case 29:
		//line sql.y:261
		{
			yyVAL.statement = &DDL{Action: AST_DROP, Table: yyS[yypt-1].bytes}
		}
	case 30:
		//line sql.y:267
		{
			yyVAL.statement = &DDL{Action: AST_ALTER, Table: yyS[yypt-0].bytes, NewName: yyS[yypt-0].bytes}
		}
	case 31:
		//line sql.y:273
		{
			yyVAL.statement = &Other{}
		}
	case 32:
		//line sql.y:277
		{
			yyVAL.statement = &Other{}
		}
	case 33:
		//line sql.y:281
		{
			yyVAL.statement = &Other{}
		}
	case 34:
		//line sql.y:286
		{
			SetAllowComments(yylex, true)
		}
	case 35:
		//line sql.y:290
		{
			yyVAL.bytes2 = yyS[yypt-0].bytes2
			SetAllowComments(yylex, false)
		}
	case 36:
		//line sql.y:296
		{
			yyVAL.bytes2 = nil
		}
	case 37:
		//line sql.y:300
		{
			yyVAL.bytes2 = append(yyS[yypt-1].bytes2, yyS[yypt-0].bytes)
		}
	case 38:
		//line sql.y:306
		{
			yyVAL.str = AST_UNION
		}
	case 39:
		//line sql.y:310
		{
			yyVAL.str = AST_UNION_ALL
		}
	case 40:
		//line sql.y:314
		{
			yyVAL.str = AST_SET_MINUS
		}
	case 41:
		//line sql.y:318
		{
			yyVAL.str = AST_EXCEPT
		}
	case 42:
		//line sql.y:322
		{
			yyVAL.str = AST_INTERSECT
		}
	case 43:
		//line sql.y:327
		{
			yyVAL.str = ""
		}
	case 44:
		//line sql.y:331
		{
			yyVAL.str = AST_DISTINCT
		}
	case 45:
		//line sql.y:337
		{
			yyVAL.selectExprs = SelectExprs{yyS[yypt-0].selectExpr}
		}
	case 46:
		//line sql.y:341
		{
			yyVAL.selectExprs = append(yyVAL.selectExprs, yyS[yypt-0].selectExpr)
		}
	case 47:
		//line sql.y:347
		{
			yyVAL.selectExpr = &StarExpr{}
		}
	case 48:
		//line sql.y:351
		{
			yyVAL.selectExpr = &NonStarExpr{Expr: yyS[yypt-1].expr, As: yyS[yypt-0].bytes}
		}
	case 49:
		//line sql.y:355
		{
			yyVAL.selectExpr = &StarExpr{TableName: yyS[yypt-2].bytes}
		}
	case 50:
		//line sql.y:361
		{
			yyVAL.expr = yyS[yypt-0].boolExpr
		}
	case 51:
		//line sql.y:365
		{
			yyVAL.expr = yyS[yypt-0].valExpr
		}
	case 52:
		//line sql.y:370
		{
			yyVAL.bytes = nil
		}
	case 53:
		//line sql.y:374
		{
			yyVAL.bytes = yyS[yypt-0].bytes
		}
	case 54:
		//line sql.y:378
		{
			yyVAL.bytes = yyS[yypt-0].bytes
		}
	case 55:
		//line sql.y:384
		{
			yyVAL.tableExprs = TableExprs{yyS[yypt-0].tableExpr}
		}
	case 56:
		//line sql.y:388
		{
			yyVAL.tableExprs = append(yyVAL.tableExprs, yyS[yypt-0].tableExpr)
		}
	case 57:
		//line sql.y:394
		{
			yyVAL.tableExpr = &AliasedTableExpr{Expr: yyS[yypt-2].smTableExpr, As: yyS[yypt-1].bytes, Hints: yyS[yypt-0].indexHints}
		}
	case 58:
		//line sql.y:398
		{
			yyVAL.tableExpr = &ParenTableExpr{Expr: yyS[yypt-1].tableExpr}
		}
	case 59:
		//line sql.y:402
		{
			yyVAL.tableExpr = &JoinTableExpr{LeftExpr: yyS[yypt-2].tableExpr, Join: yyS[yypt-1].str, RightExpr: yyS[yypt-0].tableExpr}
		}
	case 60:
		//line sql.y:406
		{
			yyVAL.tableExpr = &JoinTableExpr{LeftExpr: yyS[yypt-4].tableExpr, Join: yyS[yypt-3].str, RightExpr: yyS[yypt-2].tableExpr, On: yyS[yypt-0].boolExpr}
		}
	case 61:
		//line sql.y:411
		{
			yyVAL.bytes = nil
		}
	case 62:
		//line sql.y:415
		{
			yyVAL.bytes = yyS[yypt-0].bytes
		}
	case 63:
		//line sql.y:419
		{
			yyVAL.bytes = yyS[yypt-0].bytes
		}
	case 64:
		//line sql.y:425
		{
			yyVAL.str = AST_JOIN
		}
	case 65:
		//line sql.y:429
		{
			yyVAL.str = AST_STRAIGHT_JOIN
		}
	case 66:
		//line sql.y:433
		{
			yyVAL.str = AST_LEFT_JOIN
		}
	case 67:
		//line sql.y:437
		{
			yyVAL.str = AST_LEFT_JOIN
		}
	case 68:
		//line sql.y:441
		{
			yyVAL.str = AST_RIGHT_JOIN
		}
	case 69:
		//line sql.y:445
		{
			yyVAL.str = AST_RIGHT_JOIN
		}
	case 70:
		//line sql.y:449
		{
			yyVAL.str = AST_JOIN
		}
	case 71:
		//line sql.y:453
		{
			yyVAL.str = AST_CROSS_JOIN
		}
	case 72:
		//line sql.y:457
		{
			yyVAL.str = AST_NATURAL_JOIN
		}
	case 73:
		//line sql.y:463
		{
			yyVAL.smTableExpr = &TableName{Name: yyS[yypt-0].bytes}
		}
	case 74:
		//line sql.y:467
		{
			yyVAL.smTableExpr = &TableName{Qualifier: yyS[yypt-2].bytes, Name: yyS[yypt-0].bytes}
		}
	case 75:
		//line sql.y:471
		{
			yyVAL.smTableExpr = yyS[yypt-0].subquery
		}
	case 76:
		//line sql.y:477
		{
			yyVAL.tableName = &TableName{Name: yyS[yypt-0].bytes}
		}
	case 77:
		//line sql.y:481
		{
			yyVAL.tableName = &TableName{Qualifier: yyS[yypt-2].bytes, Name: yyS[yypt-0].bytes}
		}
	case 78:
		//line sql.y:486
		{
			yyVAL.indexHints = nil
		}
	case 79:
		//line sql.y:490
		{
			yyVAL.indexHints = &IndexHints{Type: AST_USE, Indexes: yyS[yypt-1].bytes2}
		}
	case 80:
		//line sql.y:494
		{
			yyVAL.indexHints = &IndexHints{Type: AST_IGNORE, Indexes: yyS[yypt-1].bytes2}
		}
	case 81:
		//line sql.y:498
		{
			yyVAL.indexHints = &IndexHints{Type: AST_FORCE, Indexes: yyS[yypt-1].bytes2}
		}
	case 82:
		//line sql.y:504
		{
			yyVAL.bytes2 = [][]byte{yyS[yypt-0].bytes}
		}
	case 83:
		//line sql.y:508
		{
			yyVAL.bytes2 = append(yyS[yypt-2].bytes2, yyS[yypt-0].bytes)
		}
	case 84:
		//line sql.y:513
		{
			yyVAL.boolExpr = nil
		}
	case 85:
		//line sql.y:517
		{
			yyVAL.boolExpr = yyS[yypt-0].boolExpr
		}
	case 86:
		yyVAL.boolExpr = yyS[yypt-0].boolExpr
	case 87:
		//line sql.y:524
		{
			yyVAL.boolExpr = &AndExpr{Left: yyS[yypt-2].boolExpr, Right: yyS[yypt-0].boolExpr}
		}
	case 88:
		//line sql.y:528
		{
			yyVAL.boolExpr = &OrExpr{Left: yyS[yypt-2].boolExpr, Right: yyS[yypt-0].boolExpr}
		}
	case 89:
		//line sql.y:532
		{
			yyVAL.boolExpr = &NotExpr{Expr: yyS[yypt-0].boolExpr}
		}
	case 90:
		//line sql.y:536
		{
			yyVAL.boolExpr = &ParenBoolExpr{Expr: yyS[yypt-1].boolExpr}
		}
	case 91:
		//line sql.y:542
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyS[yypt-2].valExpr, Operator: yyS[yypt-1].str, Right: yyS[yypt-0].valExpr}
		}
	case 92:
		//line sql.y:546
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyS[yypt-2].valExpr, Operator: AST_IN, Right: yyS[yypt-0].colTuple}
		}
	case 93:
		//line sql.y:550
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyS[yypt-3].valExpr, Operator: AST_NOT_IN, Right: yyS[yypt-0].colTuple}
		}
	case 94:
		//line sql.y:554
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyS[yypt-2].valExpr, Operator: AST_LIKE, Right: yyS[yypt-0].valExpr}
		}
	case 95:
		//line sql.y:558
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyS[yypt-3].valExpr, Operator: AST_NOT_LIKE, Right: yyS[yypt-0].valExpr}
		}
	case 96:
		//line sql.y:562
		{
			yyVAL.boolExpr = &RangeCond{Left: yyS[yypt-4].valExpr, Operator: AST_BETWEEN, From: yyS[yypt-2].valExpr, To: yyS[yypt-0].valExpr}
		}
	case 97:
		//line sql.y:566
		{
			yyVAL.boolExpr = &RangeCond{Left: yyS[yypt-5].valExpr, Operator: AST_NOT_BETWEEN, From: yyS[yypt-2].valExpr, To: yyS[yypt-0].valExpr}
		}
	case 98:
		//line sql.y:570
		{
			yyVAL.boolExpr = &NullCheck{Operator: AST_IS_NULL, Expr: yyS[yypt-2].valExpr}
		}
	case 99:
		//line sql.y:574
		{
			yyVAL.boolExpr = &NullCheck{Operator: AST_IS_NOT_NULL, Expr: yyS[yypt-3].valExpr}
		}
	case 100:
		//line sql.y:578
		{
			yyVAL.boolExpr = &ExistsExpr{Subquery: yyS[yypt-0].subquery}
		}
	case 101:
		//line sql.y:584
		{
			yyVAL.str = AST_EQ
		}
	case 102:
		//line sql.y:588
		{
			yyVAL.str = AST_LT
		}
	case 103:
		//line sql.y:592
		{
			yyVAL.str = AST_GT
		}
	case 104:
		//line sql.y:596
		{
			yyVAL.str = AST_LE
		}
	case 105:
		//line sql.y:600
		{
			yyVAL.str = AST_GE
		}
	case 106:
		//line sql.y:604
		{
			yyVAL.str = AST_NE
		}
	case 107:
		//line sql.y:608
		{
			yyVAL.str = AST_NSE
		}
	case 108:
		//line sql.y:614
		{
			yyVAL.colTuple = ValTuple(yyS[yypt-1].valExprs)
		}
	case 109:
		//line sql.y:618
		{
			yyVAL.colTuple = yyS[yypt-0].subquery
		}
	case 110:
		//line sql.y:622
		{
			yyVAL.colTuple = ListArg(yyS[yypt-0].bytes)
		}
	case 111:
		//line sql.y:628
		{
			yyVAL.subquery = &Subquery{yyS[yypt-1].selStmt}
		}
	case 112:
		//line sql.y:634
		{
			yyVAL.valExprs = ValExprs{yyS[yypt-0].valExpr}
		}
	case 113:
		//line sql.y:638
		{
			yyVAL.valExprs = append(yyS[yypt-2].valExprs, yyS[yypt-0].valExpr)
		}
	case 114:
		//line sql.y:644
		{
			yyVAL.valExpr = yyS[yypt-0].valExpr
		}
	case 115:
		//line sql.y:648
		{
			yyVAL.valExpr = yyS[yypt-0].colName
		}
	case 116:
		//line sql.y:652
		{
			yyVAL.valExpr = yyS[yypt-0].rowTuple
		}
	case 117:
		//line sql.y:656
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyS[yypt-2].valExpr, Operator: AST_BITAND, Right: yyS[yypt-0].valExpr}
		}
	case 118:
		//line sql.y:660
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyS[yypt-2].valExpr, Operator: AST_BITOR, Right: yyS[yypt-0].valExpr}
		}
	case 119:
		//line sql.y:664
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyS[yypt-2].valExpr, Operator: AST_BITXOR, Right: yyS[yypt-0].valExpr}
		}
	case 120:
		//line sql.y:668
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyS[yypt-2].valExpr, Operator: AST_PLUS, Right: yyS[yypt-0].valExpr}
		}
	case 121:
		//line sql.y:672
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyS[yypt-2].valExpr, Operator: AST_MINUS, Right: yyS[yypt-0].valExpr}
		}
	case 122:
		//line sql.y:676
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyS[yypt-2].valExpr, Operator: AST_MULT, Right: yyS[yypt-0].valExpr}
		}
	case 123:
		//line sql.y:680
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyS[yypt-2].valExpr, Operator: AST_DIV, Right: yyS[yypt-0].valExpr}
		}
	case 124:
		//line sql.y:684
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyS[yypt-2].valExpr, Operator: AST_MOD, Right: yyS[yypt-0].valExpr}
		}
	case 125:
		//line sql.y:688
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
	case 126:
		//line sql.y:703
		{
			yyVAL.valExpr = &FuncExpr{Name: yyS[yypt-2].bytes}
		}
	case 127:
		//line sql.y:707
		{
			yyVAL.valExpr = &FuncExpr{Name: yyS[yypt-3].bytes, Exprs: yyS[yypt-1].selectExprs}
		}
	case 128:
		//line sql.y:711
		{
			yyVAL.valExpr = &FuncExpr{Name: yyS[yypt-4].bytes, Distinct: true, Exprs: yyS[yypt-1].selectExprs}
		}
	case 129:
		//line sql.y:715
		{
			yyVAL.valExpr = &FuncExpr{Name: yyS[yypt-3].bytes, Exprs: yyS[yypt-1].selectExprs}
		}
	case 130:
		//line sql.y:719
		{
			yyVAL.valExpr = yyS[yypt-0].caseExpr
		}
	case 131:
		//line sql.y:725
		{
			yyVAL.bytes = IF_BYTES
		}
	case 132:
		//line sql.y:729
		{
			yyVAL.bytes = VALUES_BYTES
		}
	case 133:
		//line sql.y:735
		{
			yyVAL.byt = AST_UPLUS
		}
	case 134:
		//line sql.y:739
		{
			yyVAL.byt = AST_UMINUS
		}
	case 135:
		//line sql.y:743
		{
			yyVAL.byt = AST_TILDA
		}
	case 136:
		//line sql.y:749
		{
			yyVAL.caseExpr = &CaseExpr{Expr: yyS[yypt-3].valExpr, Whens: yyS[yypt-2].whens, Else: yyS[yypt-1].valExpr}
		}
	case 137:
		//line sql.y:754
		{
			yyVAL.valExpr = nil
		}
	case 138:
		//line sql.y:758
		{
			yyVAL.valExpr = yyS[yypt-0].valExpr
		}
	case 139:
		//line sql.y:764
		{
			yyVAL.whens = []*When{yyS[yypt-0].when}
		}
	case 140:
		//line sql.y:768
		{
			yyVAL.whens = append(yyS[yypt-1].whens, yyS[yypt-0].when)
		}
	case 141:
		//line sql.y:774
		{
			yyVAL.when = &When{Cond: yyS[yypt-2].boolExpr, Val: yyS[yypt-0].valExpr}
		}
	case 142:
		//line sql.y:779
		{
			yyVAL.valExpr = nil
		}
	case 143:
		//line sql.y:783
		{
			yyVAL.valExpr = yyS[yypt-0].valExpr
		}
	case 144:
		//line sql.y:789
		{
			yyVAL.colName = &ColName{Name: yyS[yypt-0].bytes}
		}
	case 145:
		//line sql.y:793
		{
			yyVAL.colName = &ColName{Qualifier: yyS[yypt-2].bytes, Name: yyS[yypt-0].bytes}
		}
	case 146:
		//line sql.y:799
		{
			yyVAL.valExpr = StrVal(yyS[yypt-0].bytes)
		}
	case 147:
		//line sql.y:803
		{
			yyVAL.valExpr = NumVal(yyS[yypt-0].bytes)
		}
	case 148:
		//line sql.y:807
		{
			yyVAL.valExpr = ValArg(yyS[yypt-0].bytes)
		}
	case 149:
		//line sql.y:811
		{
			yyVAL.valExpr = &NullVal{}
		}
	case 150:
		//line sql.y:816
		{
			yyVAL.valExprs = nil
		}
	case 151:
		//line sql.y:820
		{
			yyVAL.valExprs = yyS[yypt-0].valExprs
		}
	case 152:
		//line sql.y:825
		{
			yyVAL.boolExpr = nil
		}
	case 153:
		//line sql.y:829
		{
			yyVAL.boolExpr = yyS[yypt-0].boolExpr
		}
	case 154:
		//line sql.y:834
		{
			yyVAL.orderBy = nil
		}
	case 155:
		//line sql.y:838
		{
			yyVAL.orderBy = yyS[yypt-0].orderBy
		}
	case 156:
		//line sql.y:844
		{
			yyVAL.orderBy = OrderBy{yyS[yypt-0].order}
		}
	case 157:
		//line sql.y:848
		{
			yyVAL.orderBy = append(yyS[yypt-2].orderBy, yyS[yypt-0].order)
		}
	case 158:
		//line sql.y:854
		{
			yyVAL.order = &Order{Expr: yyS[yypt-1].valExpr, Direction: yyS[yypt-0].str}
		}
	case 159:
		//line sql.y:859
		{
			yyVAL.str = AST_ASC
		}
	case 160:
		//line sql.y:863
		{
			yyVAL.str = AST_ASC
		}
	case 161:
		//line sql.y:867
		{
			yyVAL.str = AST_DESC
		}
	case 162:
		//line sql.y:872
		{
			yyVAL.limit = nil
		}
	case 163:
		//line sql.y:876
		{
			yyVAL.limit = &Limit{Rowcount: yyS[yypt-0].valExpr}
		}
	case 164:
		//line sql.y:880
		{
			yyVAL.limit = &Limit{Offset: yyS[yypt-2].valExpr, Rowcount: yyS[yypt-0].valExpr}
		}
	case 165:
		//line sql.y:885
		{
			yyVAL.str = ""
		}
	case 166:
		//line sql.y:889
		{
			yyVAL.str = AST_FOR_UPDATE
		}
	case 167:
		//line sql.y:893
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
	case 168:
		//line sql.y:906
		{
			yyVAL.columns = nil
		}
	case 169:
		//line sql.y:910
		{
			yyVAL.columns = yyS[yypt-1].columns
		}
	case 170:
		//line sql.y:916
		{
			yyVAL.columns = Columns{&NonStarExpr{Expr: yyS[yypt-0].colName}}
		}
	case 171:
		//line sql.y:920
		{
			yyVAL.columns = append(yyVAL.columns, &NonStarExpr{Expr: yyS[yypt-0].colName})
		}
	case 172:
		//line sql.y:925
		{
			yyVAL.updateExprs = nil
		}
	case 173:
		//line sql.y:929
		{
			yyVAL.updateExprs = yyS[yypt-0].updateExprs
		}
	case 174:
		//line sql.y:935
		{
			yyVAL.insRows = yyS[yypt-0].values
		}
	case 175:
		//line sql.y:939
		{
			yyVAL.insRows = yyS[yypt-0].selStmt
		}
	case 176:
		//line sql.y:945
		{
			yyVAL.values = Values{yyS[yypt-0].rowTuple}
		}
	case 177:
		//line sql.y:949
		{
			yyVAL.values = append(yyS[yypt-2].values, yyS[yypt-0].rowTuple)
		}
	case 178:
		//line sql.y:955
		{
			yyVAL.rowTuple = ValTuple(yyS[yypt-1].valExprs)
		}
	case 179:
		//line sql.y:959
		{
			yyVAL.rowTuple = yyS[yypt-0].subquery
		}
	case 180:
		//line sql.y:965
		{
			yyVAL.updateExprs = UpdateExprs{yyS[yypt-0].updateExpr}
		}
	case 181:
		//line sql.y:969
		{
			yyVAL.updateExprs = append(yyS[yypt-2].updateExprs, yyS[yypt-0].updateExpr)
		}
	case 182:
		//line sql.y:975
		{
			yyVAL.updateExpr = &UpdateExpr{Name: yyS[yypt-2].colName, Expr: yyS[yypt-0].valExpr}
		}
	case 183:
		//line sql.y:980
		{
			yyVAL.empty = struct{}{}
		}
	case 184:
		//line sql.y:982
		{
			yyVAL.empty = struct{}{}
		}
	case 185:
		//line sql.y:985
		{
			yyVAL.empty = struct{}{}
		}
	case 186:
		//line sql.y:987
		{
			yyVAL.empty = struct{}{}
		}
	case 187:
		//line sql.y:990
		{
			yyVAL.empty = struct{}{}
		}
	case 188:
		//line sql.y:992
		{
			yyVAL.empty = struct{}{}
		}
	case 189:
		//line sql.y:996
		{
			yyVAL.empty = struct{}{}
		}
	case 190:
		//line sql.y:998
		{
			yyVAL.empty = struct{}{}
		}
	case 191:
		//line sql.y:1000
		{
			yyVAL.empty = struct{}{}
		}
	case 192:
		//line sql.y:1002
		{
			yyVAL.empty = struct{}{}
		}
	case 193:
		//line sql.y:1004
		{
			yyVAL.empty = struct{}{}
		}
	case 194:
		//line sql.y:1007
		{
			yyVAL.empty = struct{}{}
		}
	case 195:
		//line sql.y:1009
		{
			yyVAL.empty = struct{}{}
		}
	case 196:
		//line sql.y:1012
		{
			yyVAL.empty = struct{}{}
		}
	case 197:
		//line sql.y:1014
		{
			yyVAL.empty = struct{}{}
		}
	case 198:
		//line sql.y:1017
		{
			yyVAL.empty = struct{}{}
		}
	case 199:
		//line sql.y:1019
		{
			yyVAL.empty = struct{}{}
		}
	case 200:
		//line sql.y:1023
		{
			yyVAL.bytes = bytes.ToLower(yyS[yypt-0].bytes)
		}
	case 201:
		//line sql.y:1028
		{
			ForceEOF(yylex)
		}
	}
	goto yystack /* stack new state and value */
}
