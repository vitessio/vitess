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
	"OR",
	"AND",
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

const yyNprod = 203
const yyPrivate = 57344

var yyTokenNames []string
var yyStates []string

const yyLast = 652

var yyAct = []int{

	94, 294, 159, 364, 91, 85, 331, 248, 62, 162,
	92, 198, 286, 239, 80, 209, 63, 178, 90, 161,
	3, 373, 373, 81, 344, 373, 50, 103, 259, 260,
	261, 262, 263, 186, 264, 265, 130, 76, 65, 230,
	292, 70, 64, 130, 73, 130, 68, 53, 77, 136,
	135, 255, 51, 52, 28, 29, 30, 31, 86, 230,
	311, 313, 124, 38, 72, 40, 342, 375, 374, 41,
	120, 372, 43, 315, 44, 240, 46, 47, 48, 128,
	341, 49, 320, 228, 132, 317, 291, 192, 340, 280,
	312, 278, 163, 69, 158, 160, 164, 121, 45, 322,
	123, 240, 270, 284, 229, 231, 190, 136, 135, 134,
	193, 117, 172, 65, 113, 218, 65, 64, 182, 181,
	64, 176, 324, 119, 71, 168, 146, 147, 148, 149,
	150, 135, 180, 86, 204, 182, 148, 149, 150, 337,
	208, 206, 207, 216, 217, 183, 220, 221, 222, 223,
	224, 225, 226, 227, 203, 196, 115, 202, 287, 219,
	189, 191, 188, 205, 136, 135, 211, 287, 232, 86,
	86, 251, 127, 59, 339, 65, 65, 338, 309, 64,
	246, 234, 236, 244, 348, 250, 305, 252, 308, 237,
	303, 306, 307, 243, 247, 304, 115, 179, 179, 143,
	144, 145, 146, 147, 148, 149, 150, 230, 14, 15,
	16, 17, 269, 232, 253, 256, 129, 273, 274, 349,
	271, 28, 29, 30, 31, 14, 326, 281, 202, 272,
	201, 212, 111, 277, 75, 114, 18, 210, 86, 116,
	200, 211, 257, 115, 359, 174, 285, 358, 357, 112,
	279, 165, 283, 289, 170, 293, 201, 290, 175, 169,
	351, 352, 130, 110, 167, 166, 200, 268, 71, 301,
	302, 66, 316, 259, 260, 261, 262, 263, 319, 264,
	265, 314, 298, 78, 267, 202, 202, 323, 19, 20,
	22, 21, 23, 65, 346, 328, 297, 327, 329, 332,
	321, 24, 25, 26, 143, 144, 145, 146, 147, 148,
	149, 150, 143, 144, 145, 146, 147, 148, 149, 150,
	195, 343, 194, 133, 333, 177, 318, 345, 143, 144,
	145, 146, 147, 148, 149, 150, 102, 347, 125, 232,
	71, 354, 353, 356, 122, 118, 355, 276, 99, 100,
	101, 361, 332, 60, 79, 363, 362, 74, 365, 365,
	365, 65, 366, 367, 325, 64, 235, 14, 97, 368,
	370, 58, 377, 102, 378, 184, 108, 126, 379, 213,
	380, 214, 215, 98, 84, 99, 100, 101, 371, 56,
	242, 32, 54, 295, 89, 336, 296, 275, 106, 143,
	144, 145, 146, 147, 148, 149, 150, 34, 35, 36,
	37, 249, 335, 300, 179, 61, 33, 88, 376, 360,
	14, 104, 105, 82, 185, 39, 254, 187, 109, 97,
	42, 67, 245, 173, 102, 369, 350, 108, 330, 334,
	299, 282, 171, 107, 98, 84, 99, 100, 101, 233,
	238, 96, 93, 95, 288, 89, 241, 137, 87, 106,
	310, 199, 258, 197, 83, 14, 266, 131, 55, 27,
	57, 13, 12, 11, 10, 9, 8, 7, 88, 6,
	97, 5, 104, 105, 82, 102, 4, 2, 108, 109,
	1, 0, 0, 0, 0, 98, 66, 99, 100, 101,
	97, 0, 0, 0, 107, 102, 89, 0, 108, 0,
	106, 0, 0, 0, 0, 98, 66, 99, 100, 101,
	0, 0, 0, 0, 0, 14, 89, 0, 0, 88,
	106, 0, 0, 104, 105, 0, 0, 0, 0, 0,
	109, 0, 0, 0, 0, 102, 0, 0, 108, 88,
	0, 0, 0, 104, 105, 107, 66, 99, 100, 101,
	109, 0, 0, 0, 0, 102, 165, 0, 108, 0,
	106, 0, 0, 0, 0, 107, 66, 99, 100, 101,
	0, 0, 0, 0, 0, 0, 165, 0, 0, 0,
	106, 0, 0, 104, 105, 138, 142, 140, 141, 0,
	109, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 104, 105, 107, 154, 155, 156, 157,
	109, 151, 152, 153, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 107, 0, 0, 0, 0,
	0, 0, 0, 139, 143, 144, 145, 146, 147, 148,
	149, 150,
}
var yyPact = []int{

	203, -1000, -1000, 170, -1000, -1000, -1000, -1000, -1000, -1000,
	-1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, -27,
	-20, 8, -14, -9, -1000, -1000, -1000, 415, 375, -1000,
	-1000, -1000, 371, -1000, 342, 317, 406, 235, -49, 2,
	232, -1000, -26, 232, -1000, 321, -58, 232, -58, 318,
	-1000, -1000, -1000, -1000, -1000, 409, -1000, 222, 317, 216,
	36, 317, 141, -1000, 192, -1000, 33, 309, 54, 232,
	-1000, -1000, 308, -1000, -31, 302, 357, 106, 232, -1000,
	207, -1000, -1000, 304, 31, 97, 574, -1000, 480, 460,
	-1000, -1000, -1000, 540, 219, 218, -1000, 213, 208, -1000,
	-1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, 540,
	-1000, 212, 235, 289, 404, 235, 540, 232, -1000, 355,
	-64, -1000, 74, -1000, 286, -1000, -1000, 284, -1000, 194,
	409, -1000, -1000, 232, 88, 480, 480, 540, 191, 358,
	540, 540, 90, 540, 540, 540, 540, 540, 540, 540,
	540, -1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, 574,
	-18, 3, 4, 574, -1000, 520, 348, 409, -1000, 415,
	311, -6, 242, 362, 235, 235, 188, -1000, 398, 480,
	-1000, 242, -1000, -1000, -1000, 105, 232, -1000, -42, -1000,
	-1000, -1000, -1000, -1000, -1000, -1000, -1000, 187, 217, 248,
	220, 24, -1000, -1000, -1000, -1000, -1000, 63, 242, -1000,
	520, -1000, -1000, 191, 540, 540, 242, 329, -1000, 322,
	53, 53, 53, 61, 61, -1000, -1000, -1000, -1000, -1000,
	540, -1000, 242, -1000, -10, 409, -12, 172, 20, -1000,
	480, 92, 205, 170, 101, -15, -1000, 398, 378, 382,
	97, 260, -1000, -1000, 246, -1000, 402, 194, 194, -1000,
	-1000, 134, 130, 136, 132, 122, -4, -1000, 245, -28,
	236, -16, -1000, 242, 258, 540, -1000, 242, -1000, -19,
	-1000, 311, 15, -1000, 540, 40, -1000, 334, 171, -1000,
	-1000, -1000, 235, 378, -1000, 540, 540, -1000, -1000, 400,
	381, 217, 73, -1000, 121, -1000, 118, -1000, -1000, -1000,
	-1000, -3, -11, -25, -1000, -1000, -1000, -1000, 540, 242,
	-1000, -77, -1000, 242, 540, 263, 205, -1000, -1000, 129,
	164, -1000, 234, -1000, 398, 480, 540, 480, -1000, -1000,
	202, 201, 198, 242, -1000, 242, 412, -1000, 540, 540,
	-1000, -1000, -1000, 378, 97, 152, 97, 232, 232, 232,
	235, 242, -1000, 354, -30, -1000, -33, -34, 141, -1000,
	411, 351, -1000, 232, -1000, -1000, -1000, 232, -1000, 232,
	-1000,
}
var yyPgo = []int{

	0, 490, 487, 19, 486, 481, 479, 477, 476, 475,
	474, 473, 472, 471, 391, 470, 469, 468, 14, 23,
	467, 466, 464, 463, 11, 462, 461, 173, 460, 3,
	17, 5, 458, 457, 456, 18, 2, 15, 9, 454,
	10, 453, 27, 452, 4, 451, 450, 13, 442, 441,
	440, 439, 7, 438, 6, 436, 1, 435, 433, 432,
	12, 8, 16, 234, 431, 430, 427, 426, 425, 424,
	0, 26, 416,
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
	69, 70, 71,
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
	2, 1, 0,
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
	-18, -19, 75, -22, 36, -31, -36, -32, 69, 46,
	-35, -44, -40, -43, -70, -41, -45, 20, 35, 37,
	38, 39, 25, -42, 73, 74, 50, 95, 28, 80,
	41, -27, 33, 78, -27, 55, 47, 78, 36, 69,
	-70, -71, 36, -71, 93, 36, 20, 66, -70, 9,
	55, -20, -70, 19, 78, 68, 67, -33, 21, 69,
	23, 24, 22, 70, 71, 72, 73, 74, 75, 76,
	77, 47, 48, 49, 42, 43, 44, 45, -31, -36,
	-31, -3, -38, -36, -36, 46, 46, 46, -42, 46,
	46, -48, -36, -58, 33, 46, -61, 36, -30, 10,
	-62, -36, -70, -71, 20, -69, 97, -66, 88, 86,
	32, 87, 13, 36, 36, 36, -71, -23, -24, -26,
	46, 36, -42, -19, -70, 75, -31, -31, -36, -37,
	46, -42, 40, 21, 23, 24, -36, -36, 25, 69,
	-36, -36, -36, -36, -36, -36, -36, -36, 101, 101,
	55, 101, -36, 101, -18, 18, -18, -35, -46, -47,
	81, -34, 28, -3, -61, -59, -44, -30, -52, 13,
	-31, 66, -70, -71, -67, 93, -30, 55, -25, 56,
	57, 58, 59, 60, 62, 63, -21, 36, 19, -24,
	78, -38, -37, -36, -36, 68, 25, -36, 101, -18,
	101, 55, -49, -47, 83, -31, -60, 66, -39, -40,
	-60, 101, 55, -52, -56, 15, 14, 36, 36, -50,
	11, -24, -24, 56, 61, 56, 61, 56, 56, 56,
	-28, 64, 94, 65, 36, 101, 36, 101, 68, -36,
	101, -35, 84, -36, 82, 30, 55, -44, -56, -36,
	-53, -54, -36, -71, -51, 12, 14, 66, 56, 56,
	91, 91, 91, -36, 101, -36, 31, -40, 55, 55,
	-55, 26, 27, -52, -31, -38, -31, 46, 46, 46,
	7, -36, -54, -56, -29, -70, -29, -29, -61, -57,
	16, 34, 101, 55, 101, 101, 7, 21, -70, -70,
	-70,
}
var yyDef = []int{

	0, -2, 1, 2, 3, 4, 5, 6, 7, 8,
	9, 10, 11, 12, 34, 34, 34, 34, 34, 197,
	188, 0, 0, 0, 202, 202, 202, 0, 38, 40,
	41, 42, 43, 36, 0, 0, 0, 0, 186, 0,
	0, 198, 0, 0, 189, 0, 184, 0, 184, 0,
	31, 32, 33, 14, 39, 0, 44, 35, 0, 0,
	76, 0, 19, 181, 0, 145, 201, 0, 0, 0,
	202, 201, 0, 202, 0, 0, 0, 0, 0, 30,
	0, 45, 47, 52, 201, 50, 51, 86, 0, 0,
	115, 116, 117, 0, 145, 0, 131, 0, 0, 147,
	148, 149, 150, 180, 134, 135, 136, 132, 133, 138,
	37, 169, 0, 0, 84, 0, 0, 0, 202, 0,
	199, 22, 0, 25, 0, 27, 185, 0, 202, 0,
	0, 48, 53, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 102, 103, 104, 105, 106, 107, 108, 89, 0,
	0, 0, 0, 113, 126, 0, 0, 0, 100, 0,
	0, 0, 139, 0, 0, 0, 84, 77, 155, 0,
	182, 183, 146, 20, 187, 0, 0, 202, 195, 190,
	191, 192, 193, 194, 26, 28, 29, 84, 55, 61,
	0, 73, 75, 46, 54, 49, 87, 88, 91, 92,
	0, 110, 111, 0, 0, 0, 94, 0, 98, 0,
	118, 119, 120, 121, 122, 123, 124, 125, 90, 112,
	0, 179, 113, 127, 0, 0, 0, 0, 143, 140,
	0, 173, 0, 176, 173, 0, 171, 155, 163, 0,
	85, 0, 200, 23, 0, 196, 151, 0, 0, 64,
	65, 0, 0, 0, 0, 0, 78, 62, 0, 0,
	0, 0, 93, 95, 0, 0, 99, 114, 128, 0,
	130, 0, 0, 141, 0, 0, 15, 0, 175, 177,
	16, 170, 0, 163, 18, 0, 0, 202, 24, 153,
	0, 56, 59, 66, 0, 68, 0, 70, 71, 72,
	57, 0, 0, 0, 63, 58, 74, 109, 0, 96,
	129, 0, 137, 144, 0, 0, 0, 172, 17, 164,
	156, 157, 160, 21, 155, 0, 0, 0, 67, 69,
	0, 0, 0, 97, 101, 142, 0, 178, 0, 0,
	159, 161, 162, 163, 154, 152, 60, 0, 0, 0,
	0, 165, 158, 166, 0, 82, 0, 0, 174, 13,
	0, 0, 79, 0, 80, 81, 167, 0, 83, 0,
	168,
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
		//line sql.y:151
		{
			SetParseTree(yylex, yyS[yypt-0].statement)
		}
	case 2:
		//line sql.y:157
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
		//line sql.y:173
		{
			yyVAL.selStmt = &Select{Comments: Comments(yyS[yypt-10].bytes2), Distinct: yyS[yypt-9].str, SelectExprs: yyS[yypt-8].selectExprs, From: yyS[yypt-6].tableExprs, Where: NewWhere(AST_WHERE, yyS[yypt-5].boolExpr), GroupBy: GroupBy(yyS[yypt-4].valExprs), Having: NewWhere(AST_HAVING, yyS[yypt-3].boolExpr), OrderBy: yyS[yypt-2].orderBy, Limit: yyS[yypt-1].limit, Lock: yyS[yypt-0].str}
		}
	case 14:
		//line sql.y:177
		{
			yyVAL.selStmt = &Union{Type: yyS[yypt-1].str, Left: yyS[yypt-2].selStmt, Right: yyS[yypt-0].selStmt}
		}
	case 15:
		//line sql.y:183
		{
			yyVAL.statement = &Insert{Comments: Comments(yyS[yypt-5].bytes2), Table: yyS[yypt-3].tableName, Columns: yyS[yypt-2].columns, Rows: yyS[yypt-1].insRows, OnDup: OnDup(yyS[yypt-0].updateExprs)}
		}
	case 16:
		//line sql.y:187
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
		//line sql.y:199
		{
			yyVAL.statement = &Update{Comments: Comments(yyS[yypt-6].bytes2), Table: yyS[yypt-5].tableName, Exprs: yyS[yypt-3].updateExprs, Where: NewWhere(AST_WHERE, yyS[yypt-2].boolExpr), OrderBy: yyS[yypt-1].orderBy, Limit: yyS[yypt-0].limit}
		}
	case 18:
		//line sql.y:205
		{
			yyVAL.statement = &Delete{Comments: Comments(yyS[yypt-5].bytes2), Table: yyS[yypt-3].tableName, Where: NewWhere(AST_WHERE, yyS[yypt-2].boolExpr), OrderBy: yyS[yypt-1].orderBy, Limit: yyS[yypt-0].limit}
		}
	case 19:
		//line sql.y:211
		{
			yyVAL.statement = &Set{Comments: Comments(yyS[yypt-1].bytes2), Exprs: yyS[yypt-0].updateExprs}
		}
	case 20:
		//line sql.y:217
		{
			yyVAL.statement = &DDL{Action: AST_CREATE, NewName: yyS[yypt-1].bytes}
		}
	case 21:
		//line sql.y:221
		{
			// Change this to an alter statement
			yyVAL.statement = &DDL{Action: AST_ALTER, Table: yyS[yypt-1].bytes, NewName: yyS[yypt-1].bytes}
		}
	case 22:
		//line sql.y:226
		{
			yyVAL.statement = &DDL{Action: AST_CREATE, NewName: yyS[yypt-1].bytes}
		}
	case 23:
		//line sql.y:232
		{
			yyVAL.statement = &DDL{Action: AST_ALTER, Table: yyS[yypt-2].bytes, NewName: yyS[yypt-2].bytes}
		}
	case 24:
		//line sql.y:236
		{
			// Change this to a rename statement
			yyVAL.statement = &DDL{Action: AST_RENAME, Table: yyS[yypt-3].bytes, NewName: yyS[yypt-0].bytes}
		}
	case 25:
		//line sql.y:241
		{
			yyVAL.statement = &DDL{Action: AST_ALTER, Table: yyS[yypt-1].bytes, NewName: yyS[yypt-1].bytes}
		}
	case 26:
		//line sql.y:247
		{
			yyVAL.statement = &DDL{Action: AST_RENAME, Table: yyS[yypt-2].bytes, NewName: yyS[yypt-0].bytes}
		}
	case 27:
		//line sql.y:253
		{
			yyVAL.statement = &DDL{Action: AST_DROP, Table: yyS[yypt-0].bytes}
		}
	case 28:
		//line sql.y:257
		{
			// Change this to an alter statement
			yyVAL.statement = &DDL{Action: AST_ALTER, Table: yyS[yypt-0].bytes, NewName: yyS[yypt-0].bytes}
		}
	case 29:
		//line sql.y:262
		{
			yyVAL.statement = &DDL{Action: AST_DROP, Table: yyS[yypt-1].bytes}
		}
	case 30:
		//line sql.y:268
		{
			yyVAL.statement = &DDL{Action: AST_ALTER, Table: yyS[yypt-0].bytes, NewName: yyS[yypt-0].bytes}
		}
	case 31:
		//line sql.y:274
		{
			yyVAL.statement = &Other{}
		}
	case 32:
		//line sql.y:278
		{
			yyVAL.statement = &Other{}
		}
	case 33:
		//line sql.y:282
		{
			yyVAL.statement = &Other{}
		}
	case 34:
		//line sql.y:287
		{
			SetAllowComments(yylex, true)
		}
	case 35:
		//line sql.y:291
		{
			yyVAL.bytes2 = yyS[yypt-0].bytes2
			SetAllowComments(yylex, false)
		}
	case 36:
		//line sql.y:297
		{
			yyVAL.bytes2 = nil
		}
	case 37:
		//line sql.y:301
		{
			yyVAL.bytes2 = append(yyS[yypt-1].bytes2, yyS[yypt-0].bytes)
		}
	case 38:
		//line sql.y:307
		{
			yyVAL.str = AST_UNION
		}
	case 39:
		//line sql.y:311
		{
			yyVAL.str = AST_UNION_ALL
		}
	case 40:
		//line sql.y:315
		{
			yyVAL.str = AST_SET_MINUS
		}
	case 41:
		//line sql.y:319
		{
			yyVAL.str = AST_EXCEPT
		}
	case 42:
		//line sql.y:323
		{
			yyVAL.str = AST_INTERSECT
		}
	case 43:
		//line sql.y:328
		{
			yyVAL.str = ""
		}
	case 44:
		//line sql.y:332
		{
			yyVAL.str = AST_DISTINCT
		}
	case 45:
		//line sql.y:338
		{
			yyVAL.selectExprs = SelectExprs{yyS[yypt-0].selectExpr}
		}
	case 46:
		//line sql.y:342
		{
			yyVAL.selectExprs = append(yyVAL.selectExprs, yyS[yypt-0].selectExpr)
		}
	case 47:
		//line sql.y:348
		{
			yyVAL.selectExpr = &StarExpr{}
		}
	case 48:
		//line sql.y:352
		{
			yyVAL.selectExpr = &NonStarExpr{Expr: yyS[yypt-1].expr, As: yyS[yypt-0].bytes}
		}
	case 49:
		//line sql.y:356
		{
			yyVAL.selectExpr = &StarExpr{TableName: yyS[yypt-2].bytes}
		}
	case 50:
		//line sql.y:362
		{
			yyVAL.expr = yyS[yypt-0].boolExpr
		}
	case 51:
		//line sql.y:366
		{
			yyVAL.expr = yyS[yypt-0].valExpr
		}
	case 52:
		//line sql.y:371
		{
			yyVAL.bytes = nil
		}
	case 53:
		//line sql.y:375
		{
			yyVAL.bytes = yyS[yypt-0].bytes
		}
	case 54:
		//line sql.y:379
		{
			yyVAL.bytes = yyS[yypt-0].bytes
		}
	case 55:
		//line sql.y:385
		{
			yyVAL.tableExprs = TableExprs{yyS[yypt-0].tableExpr}
		}
	case 56:
		//line sql.y:389
		{
			yyVAL.tableExprs = append(yyVAL.tableExprs, yyS[yypt-0].tableExpr)
		}
	case 57:
		//line sql.y:395
		{
			yyVAL.tableExpr = &AliasedTableExpr{Expr: yyS[yypt-2].smTableExpr, As: yyS[yypt-1].bytes, Hints: yyS[yypt-0].indexHints}
		}
	case 58:
		//line sql.y:399
		{
			yyVAL.tableExpr = &ParenTableExpr{Expr: yyS[yypt-1].tableExpr}
		}
	case 59:
		//line sql.y:403
		{
			yyVAL.tableExpr = &JoinTableExpr{LeftExpr: yyS[yypt-2].tableExpr, Join: yyS[yypt-1].str, RightExpr: yyS[yypt-0].tableExpr}
		}
	case 60:
		//line sql.y:407
		{
			yyVAL.tableExpr = &JoinTableExpr{LeftExpr: yyS[yypt-4].tableExpr, Join: yyS[yypt-3].str, RightExpr: yyS[yypt-2].tableExpr, On: yyS[yypt-0].boolExpr}
		}
	case 61:
		//line sql.y:412
		{
			yyVAL.bytes = nil
		}
	case 62:
		//line sql.y:416
		{
			yyVAL.bytes = yyS[yypt-0].bytes
		}
	case 63:
		//line sql.y:420
		{
			yyVAL.bytes = yyS[yypt-0].bytes
		}
	case 64:
		//line sql.y:426
		{
			yyVAL.str = AST_JOIN
		}
	case 65:
		//line sql.y:430
		{
			yyVAL.str = AST_STRAIGHT_JOIN
		}
	case 66:
		//line sql.y:434
		{
			yyVAL.str = AST_LEFT_JOIN
		}
	case 67:
		//line sql.y:438
		{
			yyVAL.str = AST_LEFT_JOIN
		}
	case 68:
		//line sql.y:442
		{
			yyVAL.str = AST_RIGHT_JOIN
		}
	case 69:
		//line sql.y:446
		{
			yyVAL.str = AST_RIGHT_JOIN
		}
	case 70:
		//line sql.y:450
		{
			yyVAL.str = AST_JOIN
		}
	case 71:
		//line sql.y:454
		{
			yyVAL.str = AST_CROSS_JOIN
		}
	case 72:
		//line sql.y:458
		{
			yyVAL.str = AST_NATURAL_JOIN
		}
	case 73:
		//line sql.y:464
		{
			yyVAL.smTableExpr = &TableName{Name: yyS[yypt-0].bytes}
		}
	case 74:
		//line sql.y:468
		{
			yyVAL.smTableExpr = &TableName{Qualifier: yyS[yypt-2].bytes, Name: yyS[yypt-0].bytes}
		}
	case 75:
		//line sql.y:472
		{
			yyVAL.smTableExpr = yyS[yypt-0].subquery
		}
	case 76:
		//line sql.y:478
		{
			yyVAL.tableName = &TableName{Name: yyS[yypt-0].bytes}
		}
	case 77:
		//line sql.y:482
		{
			yyVAL.tableName = &TableName{Qualifier: yyS[yypt-2].bytes, Name: yyS[yypt-0].bytes}
		}
	case 78:
		//line sql.y:487
		{
			yyVAL.indexHints = nil
		}
	case 79:
		//line sql.y:491
		{
			yyVAL.indexHints = &IndexHints{Type: AST_USE, Indexes: yyS[yypt-1].bytes2}
		}
	case 80:
		//line sql.y:495
		{
			yyVAL.indexHints = &IndexHints{Type: AST_IGNORE, Indexes: yyS[yypt-1].bytes2}
		}
	case 81:
		//line sql.y:499
		{
			yyVAL.indexHints = &IndexHints{Type: AST_FORCE, Indexes: yyS[yypt-1].bytes2}
		}
	case 82:
		//line sql.y:505
		{
			yyVAL.bytes2 = [][]byte{yyS[yypt-0].bytes}
		}
	case 83:
		//line sql.y:509
		{
			yyVAL.bytes2 = append(yyS[yypt-2].bytes2, yyS[yypt-0].bytes)
		}
	case 84:
		//line sql.y:514
		{
			yyVAL.boolExpr = nil
		}
	case 85:
		//line sql.y:518
		{
			yyVAL.boolExpr = yyS[yypt-0].boolExpr
		}
	case 86:
		yyVAL.boolExpr = yyS[yypt-0].boolExpr
	case 87:
		//line sql.y:525
		{
			yyVAL.boolExpr = &AndExpr{Left: yyS[yypt-2].boolExpr, Right: yyS[yypt-0].boolExpr}
		}
	case 88:
		//line sql.y:529
		{
			yyVAL.boolExpr = &OrExpr{Left: yyS[yypt-2].boolExpr, Right: yyS[yypt-0].boolExpr}
		}
	case 89:
		//line sql.y:533
		{
			yyVAL.boolExpr = &NotExpr{Expr: yyS[yypt-0].boolExpr}
		}
	case 90:
		//line sql.y:537
		{
			yyVAL.boolExpr = &ParenBoolExpr{Expr: yyS[yypt-1].boolExpr}
		}
	case 91:
		//line sql.y:543
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyS[yypt-2].valExpr, Operator: yyS[yypt-1].str, Right: yyS[yypt-0].valExpr}
		}
	case 92:
		//line sql.y:547
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyS[yypt-2].valExpr, Operator: AST_IN, Right: yyS[yypt-0].colTuple}
		}
	case 93:
		//line sql.y:551
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyS[yypt-3].valExpr, Operator: AST_NOT_IN, Right: yyS[yypt-0].colTuple}
		}
	case 94:
		//line sql.y:555
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyS[yypt-2].valExpr, Operator: AST_LIKE, Right: yyS[yypt-0].valExpr}
		}
	case 95:
		//line sql.y:559
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyS[yypt-3].valExpr, Operator: AST_NOT_LIKE, Right: yyS[yypt-0].valExpr}
		}
	case 96:
		//line sql.y:563
		{
			yyVAL.boolExpr = &RangeCond{Left: yyS[yypt-4].valExpr, Operator: AST_BETWEEN, From: yyS[yypt-2].valExpr, To: yyS[yypt-0].valExpr}
		}
	case 97:
		//line sql.y:567
		{
			yyVAL.boolExpr = &RangeCond{Left: yyS[yypt-5].valExpr, Operator: AST_NOT_BETWEEN, From: yyS[yypt-2].valExpr, To: yyS[yypt-0].valExpr}
		}
	case 98:
		//line sql.y:571
		{
			yyVAL.boolExpr = &NullCheck{Operator: AST_IS_NULL, Expr: yyS[yypt-2].valExpr}
		}
	case 99:
		//line sql.y:575
		{
			yyVAL.boolExpr = &NullCheck{Operator: AST_IS_NOT_NULL, Expr: yyS[yypt-3].valExpr}
		}
	case 100:
		//line sql.y:579
		{
			yyVAL.boolExpr = &ExistsExpr{Subquery: yyS[yypt-0].subquery}
		}
	case 101:
		//line sql.y:583
		{
			yyVAL.boolExpr = &KeyrangeExpr{Start: yyS[yypt-3].valExpr, End: yyS[yypt-1].valExpr}
		}
	case 102:
		//line sql.y:589
		{
			yyVAL.str = AST_EQ
		}
	case 103:
		//line sql.y:593
		{
			yyVAL.str = AST_LT
		}
	case 104:
		//line sql.y:597
		{
			yyVAL.str = AST_GT
		}
	case 105:
		//line sql.y:601
		{
			yyVAL.str = AST_LE
		}
	case 106:
		//line sql.y:605
		{
			yyVAL.str = AST_GE
		}
	case 107:
		//line sql.y:609
		{
			yyVAL.str = AST_NE
		}
	case 108:
		//line sql.y:613
		{
			yyVAL.str = AST_NSE
		}
	case 109:
		//line sql.y:619
		{
			yyVAL.colTuple = ValTuple(yyS[yypt-1].valExprs)
		}
	case 110:
		//line sql.y:623
		{
			yyVAL.colTuple = yyS[yypt-0].subquery
		}
	case 111:
		//line sql.y:627
		{
			yyVAL.colTuple = ListArg(yyS[yypt-0].bytes)
		}
	case 112:
		//line sql.y:633
		{
			yyVAL.subquery = &Subquery{yyS[yypt-1].selStmt}
		}
	case 113:
		//line sql.y:639
		{
			yyVAL.valExprs = ValExprs{yyS[yypt-0].valExpr}
		}
	case 114:
		//line sql.y:643
		{
			yyVAL.valExprs = append(yyS[yypt-2].valExprs, yyS[yypt-0].valExpr)
		}
	case 115:
		//line sql.y:649
		{
			yyVAL.valExpr = yyS[yypt-0].valExpr
		}
	case 116:
		//line sql.y:653
		{
			yyVAL.valExpr = yyS[yypt-0].colName
		}
	case 117:
		//line sql.y:657
		{
			yyVAL.valExpr = yyS[yypt-0].rowTuple
		}
	case 118:
		//line sql.y:661
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyS[yypt-2].valExpr, Operator: AST_BITAND, Right: yyS[yypt-0].valExpr}
		}
	case 119:
		//line sql.y:665
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyS[yypt-2].valExpr, Operator: AST_BITOR, Right: yyS[yypt-0].valExpr}
		}
	case 120:
		//line sql.y:669
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyS[yypt-2].valExpr, Operator: AST_BITXOR, Right: yyS[yypt-0].valExpr}
		}
	case 121:
		//line sql.y:673
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyS[yypt-2].valExpr, Operator: AST_PLUS, Right: yyS[yypt-0].valExpr}
		}
	case 122:
		//line sql.y:677
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyS[yypt-2].valExpr, Operator: AST_MINUS, Right: yyS[yypt-0].valExpr}
		}
	case 123:
		//line sql.y:681
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyS[yypt-2].valExpr, Operator: AST_MULT, Right: yyS[yypt-0].valExpr}
		}
	case 124:
		//line sql.y:685
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyS[yypt-2].valExpr, Operator: AST_DIV, Right: yyS[yypt-0].valExpr}
		}
	case 125:
		//line sql.y:689
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyS[yypt-2].valExpr, Operator: AST_MOD, Right: yyS[yypt-0].valExpr}
		}
	case 126:
		//line sql.y:693
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
		//line sql.y:708
		{
			yyVAL.valExpr = &FuncExpr{Name: yyS[yypt-2].bytes}
		}
	case 128:
		//line sql.y:712
		{
			yyVAL.valExpr = &FuncExpr{Name: yyS[yypt-3].bytes, Exprs: yyS[yypt-1].selectExprs}
		}
	case 129:
		//line sql.y:716
		{
			yyVAL.valExpr = &FuncExpr{Name: yyS[yypt-4].bytes, Distinct: true, Exprs: yyS[yypt-1].selectExprs}
		}
	case 130:
		//line sql.y:720
		{
			yyVAL.valExpr = &FuncExpr{Name: yyS[yypt-3].bytes, Exprs: yyS[yypt-1].selectExprs}
		}
	case 131:
		//line sql.y:724
		{
			yyVAL.valExpr = yyS[yypt-0].caseExpr
		}
	case 132:
		//line sql.y:730
		{
			yyVAL.bytes = IF_BYTES
		}
	case 133:
		//line sql.y:734
		{
			yyVAL.bytes = VALUES_BYTES
		}
	case 134:
		//line sql.y:740
		{
			yyVAL.byt = AST_UPLUS
		}
	case 135:
		//line sql.y:744
		{
			yyVAL.byt = AST_UMINUS
		}
	case 136:
		//line sql.y:748
		{
			yyVAL.byt = AST_TILDA
		}
	case 137:
		//line sql.y:754
		{
			yyVAL.caseExpr = &CaseExpr{Expr: yyS[yypt-3].valExpr, Whens: yyS[yypt-2].whens, Else: yyS[yypt-1].valExpr}
		}
	case 138:
		//line sql.y:759
		{
			yyVAL.valExpr = nil
		}
	case 139:
		//line sql.y:763
		{
			yyVAL.valExpr = yyS[yypt-0].valExpr
		}
	case 140:
		//line sql.y:769
		{
			yyVAL.whens = []*When{yyS[yypt-0].when}
		}
	case 141:
		//line sql.y:773
		{
			yyVAL.whens = append(yyS[yypt-1].whens, yyS[yypt-0].when)
		}
	case 142:
		//line sql.y:779
		{
			yyVAL.when = &When{Cond: yyS[yypt-2].boolExpr, Val: yyS[yypt-0].valExpr}
		}
	case 143:
		//line sql.y:784
		{
			yyVAL.valExpr = nil
		}
	case 144:
		//line sql.y:788
		{
			yyVAL.valExpr = yyS[yypt-0].valExpr
		}
	case 145:
		//line sql.y:794
		{
			yyVAL.colName = &ColName{Name: yyS[yypt-0].bytes}
		}
	case 146:
		//line sql.y:798
		{
			yyVAL.colName = &ColName{Qualifier: yyS[yypt-2].bytes, Name: yyS[yypt-0].bytes}
		}
	case 147:
		//line sql.y:804
		{
			yyVAL.valExpr = StrVal(yyS[yypt-0].bytes)
		}
	case 148:
		//line sql.y:808
		{
			yyVAL.valExpr = NumVal(yyS[yypt-0].bytes)
		}
	case 149:
		//line sql.y:812
		{
			yyVAL.valExpr = ValArg(yyS[yypt-0].bytes)
		}
	case 150:
		//line sql.y:816
		{
			yyVAL.valExpr = &NullVal{}
		}
	case 151:
		//line sql.y:821
		{
			yyVAL.valExprs = nil
		}
	case 152:
		//line sql.y:825
		{
			yyVAL.valExprs = yyS[yypt-0].valExprs
		}
	case 153:
		//line sql.y:830
		{
			yyVAL.boolExpr = nil
		}
	case 154:
		//line sql.y:834
		{
			yyVAL.boolExpr = yyS[yypt-0].boolExpr
		}
	case 155:
		//line sql.y:839
		{
			yyVAL.orderBy = nil
		}
	case 156:
		//line sql.y:843
		{
			yyVAL.orderBy = yyS[yypt-0].orderBy
		}
	case 157:
		//line sql.y:849
		{
			yyVAL.orderBy = OrderBy{yyS[yypt-0].order}
		}
	case 158:
		//line sql.y:853
		{
			yyVAL.orderBy = append(yyS[yypt-2].orderBy, yyS[yypt-0].order)
		}
	case 159:
		//line sql.y:859
		{
			yyVAL.order = &Order{Expr: yyS[yypt-1].valExpr, Direction: yyS[yypt-0].str}
		}
	case 160:
		//line sql.y:864
		{
			yyVAL.str = AST_ASC
		}
	case 161:
		//line sql.y:868
		{
			yyVAL.str = AST_ASC
		}
	case 162:
		//line sql.y:872
		{
			yyVAL.str = AST_DESC
		}
	case 163:
		//line sql.y:877
		{
			yyVAL.limit = nil
		}
	case 164:
		//line sql.y:881
		{
			yyVAL.limit = &Limit{Rowcount: yyS[yypt-0].valExpr}
		}
	case 165:
		//line sql.y:885
		{
			yyVAL.limit = &Limit{Offset: yyS[yypt-2].valExpr, Rowcount: yyS[yypt-0].valExpr}
		}
	case 166:
		//line sql.y:890
		{
			yyVAL.str = ""
		}
	case 167:
		//line sql.y:894
		{
			yyVAL.str = AST_FOR_UPDATE
		}
	case 168:
		//line sql.y:898
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
		//line sql.y:911
		{
			yyVAL.columns = nil
		}
	case 170:
		//line sql.y:915
		{
			yyVAL.columns = yyS[yypt-1].columns
		}
	case 171:
		//line sql.y:921
		{
			yyVAL.columns = Columns{&NonStarExpr{Expr: yyS[yypt-0].colName}}
		}
	case 172:
		//line sql.y:925
		{
			yyVAL.columns = append(yyVAL.columns, &NonStarExpr{Expr: yyS[yypt-0].colName})
		}
	case 173:
		//line sql.y:930
		{
			yyVAL.updateExprs = nil
		}
	case 174:
		//line sql.y:934
		{
			yyVAL.updateExprs = yyS[yypt-0].updateExprs
		}
	case 175:
		//line sql.y:940
		{
			yyVAL.insRows = yyS[yypt-0].values
		}
	case 176:
		//line sql.y:944
		{
			yyVAL.insRows = yyS[yypt-0].selStmt
		}
	case 177:
		//line sql.y:950
		{
			yyVAL.values = Values{yyS[yypt-0].rowTuple}
		}
	case 178:
		//line sql.y:954
		{
			yyVAL.values = append(yyS[yypt-2].values, yyS[yypt-0].rowTuple)
		}
	case 179:
		//line sql.y:960
		{
			yyVAL.rowTuple = ValTuple(yyS[yypt-1].valExprs)
		}
	case 180:
		//line sql.y:964
		{
			yyVAL.rowTuple = yyS[yypt-0].subquery
		}
	case 181:
		//line sql.y:970
		{
			yyVAL.updateExprs = UpdateExprs{yyS[yypt-0].updateExpr}
		}
	case 182:
		//line sql.y:974
		{
			yyVAL.updateExprs = append(yyS[yypt-2].updateExprs, yyS[yypt-0].updateExpr)
		}
	case 183:
		//line sql.y:980
		{
			yyVAL.updateExpr = &UpdateExpr{Name: yyS[yypt-2].colName, Expr: yyS[yypt-0].valExpr}
		}
	case 184:
		//line sql.y:985
		{
			yyVAL.empty = struct{}{}
		}
	case 185:
		//line sql.y:987
		{
			yyVAL.empty = struct{}{}
		}
	case 186:
		//line sql.y:990
		{
			yyVAL.empty = struct{}{}
		}
	case 187:
		//line sql.y:992
		{
			yyVAL.empty = struct{}{}
		}
	case 188:
		//line sql.y:995
		{
			yyVAL.empty = struct{}{}
		}
	case 189:
		//line sql.y:997
		{
			yyVAL.empty = struct{}{}
		}
	case 190:
		//line sql.y:1001
		{
			yyVAL.empty = struct{}{}
		}
	case 191:
		//line sql.y:1003
		{
			yyVAL.empty = struct{}{}
		}
	case 192:
		//line sql.y:1005
		{
			yyVAL.empty = struct{}{}
		}
	case 193:
		//line sql.y:1007
		{
			yyVAL.empty = struct{}{}
		}
	case 194:
		//line sql.y:1009
		{
			yyVAL.empty = struct{}{}
		}
	case 195:
		//line sql.y:1012
		{
			yyVAL.empty = struct{}{}
		}
	case 196:
		//line sql.y:1014
		{
			yyVAL.empty = struct{}{}
		}
	case 197:
		//line sql.y:1017
		{
			yyVAL.empty = struct{}{}
		}
	case 198:
		//line sql.y:1019
		{
			yyVAL.empty = struct{}{}
		}
	case 199:
		//line sql.y:1022
		{
			yyVAL.empty = struct{}{}
		}
	case 200:
		//line sql.y:1024
		{
			yyVAL.empty = struct{}{}
		}
	case 201:
		//line sql.y:1028
		{
			yyVAL.bytes = bytes.ToLower(yyS[yypt-0].bytes)
		}
	case 202:
		//line sql.y:1033
		{
			ForceEOF(yylex)
		}
	}
	goto yystack /* stack new state and value */
}
