//line sql.y:6
package sqlparser

import __yyfmt__ "fmt"

//line sql.y:6
import "bytes"

func SetParseTree(yylex interface{}, stmt Statement) {
	tn := yylex.(*Tokenizer)
	tn.ParseTree = stmt
}

func SetAllowComments(yylex interface{}, allow bool) {
	tn := yylex.(*Tokenizer)
	tn.AllowComments = allow
}

func ForceEOF(yylex interface{}) {
	tn := yylex.(*Tokenizer)
	tn.ForceEOF = true
}

var (
	SHARE = []byte("share")
	MODE  = []byte("mode")
)

//line sql.y:32
type yySymType struct {
	yys         int
	empty       struct{}
	node        *Node
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
	valExpr     ValExpr
	tuple       Tuple
	valExprs    ValExprs
	values      Values
	subquery    *Subquery
	caseExpr    *CaseExpr
	whens       []*When
	when        *When
	orderBy     OrderBy
	order       *Order
	limit       *Limit
	updateExprs UpdateExprs
	updateExpr  *UpdateExpr
	sqlNode     SQLNode
}

const SELECT = 57346
const INSERT = 57347
const UPDATE = 57348
const DELETE = 57349
const FROM = 57350
const WHERE = 57351
const GROUP = 57352
const HAVING = 57353
const ORDER = 57354
const BY = 57355
const LIMIT = 57356
const COMMENT = 57357
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
const LE = 57381
const GE = 57382
const NE = 57383
const NULL_SAFE_EQUAL = 57384
const LEX_ERROR = 57385
const UNION = 57386
const MINUS = 57387
const EXCEPT = 57388
const INTERSECT = 57389
const JOIN = 57390
const STRAIGHT_JOIN = 57391
const LEFT = 57392
const RIGHT = 57393
const INNER = 57394
const OUTER = 57395
const CROSS = 57396
const NATURAL = 57397
const USE = 57398
const FORCE = 57399
const ON = 57400
const AND = 57401
const OR = 57402
const NOT = 57403
const UNARY = 57404
const CASE = 57405
const WHEN = 57406
const THEN = 57407
const ELSE = 57408
const END = 57409
const CREATE = 57410
const ALTER = 57411
const DROP = 57412
const RENAME = 57413
const TABLE = 57414
const INDEX = 57415
const VIEW = 57416
const TO = 57417
const IGNORE = 57418
const IF = 57419
const UNIQUE = 57420
const USING = 57421

var yyToknames = []string{
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
	"COMMENT",
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
	"LE",
	"GE",
	"NE",
	"NULL_SAFE_EQUAL",
	"LEX_ERROR",
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
	"TABLE",
	"INDEX",
	"VIEW",
	"TO",
	"IGNORE",
	"IF",
	"UNIQUE",
	"USING",
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

const yyNprod = 192
const yyPrivate = 57344

var yyTokenNames []string
var yyStates []string

const yyLast = 524

var yyAct = []int{

	83, 80, 147, 272, 339, 307, 149, 52, 91, 229,
	109, 184, 221, 69, 70, 81, 53, 150, 3, 164,
	123, 124, 12, 240, 241, 242, 243, 244, 348, 245,
	246, 172, 55, 54, 348, 60, 74, 348, 63, 43,
	118, 270, 67, 90, 118, 118, 96, 236, 75, 213,
	211, 289, 291, 56, 87, 88, 89, 32, 66, 34,
	108, 112, 153, 35, 293, 58, 94, 318, 317, 116,
	350, 316, 62, 120, 111, 37, 349, 38, 59, 347,
	290, 151, 297, 269, 39, 152, 260, 258, 298, 92,
	93, 212, 40, 41, 42, 156, 97, 123, 124, 222,
	159, 55, 54, 251, 55, 54, 168, 167, 162, 222,
	95, 263, 300, 122, 146, 148, 105, 169, 101, 107,
	166, 75, 190, 168, 123, 124, 188, 182, 194, 313,
	266, 199, 200, 189, 203, 204, 205, 206, 207, 208,
	209, 210, 195, 12, 22, 23, 24, 25, 131, 132,
	133, 134, 135, 136, 137, 138, 215, 75, 75, 86,
	192, 193, 55, 227, 90, 201, 61, 96, 217, 219,
	136, 137, 138, 233, 56, 87, 88, 89, 225, 232,
	326, 327, 228, 78, 234, 115, 283, 94, 281, 315,
	214, 284, 314, 282, 117, 188, 287, 286, 250, 285,
	253, 254, 231, 237, 191, 165, 77, 202, 103, 213,
	92, 93, 252, 165, 324, 302, 257, 97, 104, 334,
	333, 75, 131, 132, 133, 134, 135, 136, 137, 138,
	61, 95, 259, 262, 332, 218, 153, 86, 271, 118,
	268, 161, 90, 12, 157, 96, 155, 188, 188, 238,
	279, 280, 73, 87, 88, 89, 49, 103, 296, 264,
	154, 78, 56, 65, 187, 94, 299, 22, 23, 24,
	25, 55, 303, 186, 187, 304, 305, 308, 12, 13,
	14, 15, 294, 186, 77, 249, 309, 292, 92, 93,
	71, 276, 275, 178, 121, 97, 345, 181, 319, 180,
	163, 248, 113, 320, 110, 99, 68, 16, 102, 95,
	61, 106, 216, 176, 346, 215, 179, 50, 322, 330,
	328, 134, 135, 136, 137, 138, 336, 308, 64, 100,
	337, 321, 338, 340, 340, 340, 55, 54, 341, 342,
	301, 48, 256, 343, 352, 12, 86, 170, 329, 353,
	331, 90, 114, 354, 96, 355, 46, 17, 18, 20,
	19, 73, 87, 88, 89, 175, 177, 174, 86, 224,
	78, 98, 44, 90, 94, 196, 96, 197, 198, 273,
	312, 274, 230, 56, 87, 88, 89, 311, 278, 165,
	51, 351, 78, 77, 335, 12, 94, 92, 93, 71,
	27, 171, 33, 235, 97, 173, 240, 241, 242, 243,
	244, 90, 245, 246, 96, 77, 36, 57, 95, 92,
	93, 56, 87, 88, 89, 265, 97, 323, 226, 160,
	153, 344, 325, 306, 94, 126, 130, 128, 129, 310,
	95, 277, 131, 132, 133, 134, 135, 136, 137, 138,
	261, 158, 220, 142, 143, 144, 145, 92, 93, 139,
	140, 141, 295, 85, 97, 131, 132, 133, 134, 135,
	136, 137, 138, 82, 84, 267, 79, 223, 95, 125,
	76, 127, 131, 132, 133, 134, 135, 136, 137, 138,
	255, 26, 288, 131, 132, 133, 134, 135, 136, 137,
	138, 185, 239, 183, 72, 28, 29, 30, 31, 247,
	119, 45, 21, 47, 11, 10, 9, 8, 7, 6,
	5, 4, 2, 1,
}
var yyPact = []int{

	274, -1000, -1000, 218, -1000, -1000, -1000, -1000, -1000, -1000,
	-1000, -1000, -1000, -1000, -1000, -1000, -1000, -30, -14, -3,
	5, 391, 355, -1000, -1000, -1000, 338, -1000, 312, 282,
	382, 227, -27, -10, 195, -1000, -15, 195, -1000, 293,
	-34, 195, -34, -1000, -1000, 326, -1000, 356, 282, 296,
	42, 282, 155, -1000, 173, -1000, 40, 276, 52, 195,
	-1000, -1000, 269, -1000, -29, 267, 332, 121, 195, 186,
	-1000, -1000, 275, 37, 59, 414, -1000, 348, 139, -1000,
	-1000, -1000, 386, 216, 202, -1000, 200, -1000, -1000, -1000,
	-1000, -1000, -1000, -1000, -1000, -1000, -1000, 386, -1000, 197,
	227, 265, 380, 227, 386, 195, -1000, 327, -63, -1000,
	281, -1000, 264, -1000, -1000, 262, -1000, 229, 326, -1000,
	-1000, 195, 131, 348, 348, 386, 192, 354, 386, 386,
	140, 386, 386, 386, 386, 386, 386, 386, 386, -1000,
	-1000, -1000, -1000, -1000, -1000, -1000, -1000, 414, -45, -4,
	95, 414, -1000, 18, 217, 326, -1000, 391, 20, 80,
	341, 227, 204, -1000, 370, 348, -1000, 80, -1000, -1000,
	-1000, 115, 195, -1000, -43, -1000, -1000, -1000, -1000, -1000,
	-1000, -1000, -1000, 196, 352, 266, 239, 27, -1000, -1000,
	-1000, -1000, -1000, -1000, 80, -1000, 192, 386, 386, 80,
	425, -1000, 317, 250, 250, 250, 97, 97, -1000, -1000,
	-1000, -1000, -1000, 386, -1000, 80, -1000, -8, 326, -9,
	30, -1000, 348, 66, 192, 218, -12, -1000, 370, 365,
	368, 59, 257, -1000, -1000, 256, -1000, 378, 229, 229,
	-1000, -1000, 134, 132, 145, 143, 142, -11, -1000, 252,
	-31, 247, -1000, 80, 397, 386, -1000, 80, -1000, -13,
	-1000, 6, -1000, 386, 32, -1000, 310, 162, -1000, -1000,
	227, 365, -1000, 386, 386, -1000, -1000, 376, 367, 352,
	65, -1000, 138, -1000, 135, -1000, -1000, -1000, -1000, -17,
	-20, -21, -1000, -1000, -1000, 386, 80, -1000, -1000, 80,
	386, 300, 192, -1000, -1000, 374, 161, -1000, 154, -1000,
	370, 348, 386, 348, -1000, -1000, 190, 176, 175, 80,
	80, 388, -1000, 386, 386, -1000, -1000, -1000, 365, 59,
	156, 59, 195, 195, 195, 227, 80, -1000, 280, -16,
	-1000, -19, -25, 155, -1000, 385, 323, -1000, 195, -1000,
	-1000, -1000, 195, -1000, 195, -1000,
}
var yyPgo = []int{

	0, 523, 522, 17, 521, 520, 519, 518, 517, 516,
	515, 514, 491, 513, 512, 511, 13, 14, 510, 509,
	504, 503, 11, 502, 501, 256, 492, 4, 19, 36,
	480, 479, 477, 476, 2, 15, 6, 475, 474, 8,
	473, 1, 463, 452, 12, 451, 450, 441, 439, 9,
	433, 5, 432, 3, 431, 429, 428, 425, 7, 16,
	263, 417, 416, 405, 403, 402, 401, 0, 10, 400,
}
var yyR1 = []int{

	0, 1, 2, 2, 2, 2, 2, 2, 2, 2,
	2, 3, 3, 4, 5, 6, 7, 8, 8, 8,
	9, 9, 9, 10, 11, 11, 11, 69, 12, 13,
	13, 14, 14, 14, 14, 14, 15, 15, 16, 16,
	17, 17, 17, 20, 20, 18, 18, 18, 21, 21,
	22, 22, 22, 22, 19, 19, 19, 23, 23, 23,
	23, 23, 23, 23, 23, 23, 24, 24, 24, 25,
	25, 26, 26, 26, 26, 27, 27, 28, 28, 29,
	29, 29, 29, 29, 30, 30, 30, 30, 30, 30,
	30, 30, 30, 30, 31, 31, 31, 31, 31, 31,
	31, 32, 32, 37, 37, 35, 35, 39, 36, 36,
	34, 34, 34, 34, 34, 34, 34, 34, 34, 34,
	34, 34, 34, 34, 34, 34, 34, 38, 38, 40,
	40, 40, 42, 45, 45, 43, 43, 44, 46, 46,
	41, 41, 33, 33, 33, 33, 47, 47, 48, 48,
	49, 49, 50, 50, 51, 52, 52, 52, 53, 53,
	53, 54, 54, 54, 55, 55, 56, 56, 57, 57,
	58, 58, 59, 60, 60, 61, 61, 62, 62, 63,
	63, 63, 63, 63, 64, 64, 65, 65, 66, 66,
	67, 68,
}
var yyR2 = []int{

	0, 1, 1, 1, 1, 1, 1, 1, 1, 1,
	1, 12, 3, 7, 8, 7, 3, 5, 8, 4,
	6, 7, 4, 5, 4, 5, 5, 0, 2, 0,
	2, 1, 2, 1, 1, 1, 0, 1, 1, 3,
	1, 2, 3, 1, 1, 0, 1, 2, 1, 3,
	3, 3, 3, 5, 0, 1, 2, 1, 1, 2,
	3, 2, 3, 2, 2, 2, 1, 3, 1, 1,
	3, 0, 5, 5, 5, 1, 3, 0, 2, 1,
	3, 3, 2, 3, 3, 3, 4, 3, 4, 5,
	6, 3, 4, 2, 1, 1, 1, 1, 1, 1,
	1, 2, 1, 1, 3, 3, 1, 3, 1, 3,
	1, 1, 1, 3, 3, 3, 3, 3, 3, 3,
	3, 2, 3, 4, 5, 4, 1, 1, 1, 1,
	1, 1, 5, 0, 1, 1, 2, 4, 0, 2,
	1, 3, 1, 1, 1, 1, 0, 3, 0, 2,
	0, 3, 1, 3, 2, 0, 1, 1, 0, 2,
	4, 0, 2, 4, 0, 3, 1, 3, 0, 5,
	1, 3, 3, 0, 2, 0, 3, 0, 1, 1,
	1, 1, 1, 1, 0, 1, 0, 1, 0, 2,
	1, 0,
}
var yyChk = []int{

	-1000, -1, -2, -3, -4, -5, -6, -7, -8, -9,
	-10, -11, 4, 5, 6, 7, 33, 83, 84, 86,
	85, -14, 49, 50, 51, 52, -12, -69, -12, -12,
	-12, -12, 87, -65, 89, 93, -62, 89, 91, 87,
	87, 88, 89, -3, 17, -15, 18, -13, 29, -25,
	35, 8, -58, -59, -41, -67, 35, -61, 92, 88,
	-67, 35, 87, -67, 35, -60, 92, -67, -60, -16,
	-17, 73, -20, 35, -29, -34, -30, 67, 44, -33,
	-41, -35, -40, -67, -38, -42, 20, 36, 37, 38,
	25, -39, 71, 72, 48, 92, 28, 78, 15, -25,
	33, 76, -25, 53, 45, 76, 35, 67, -67, -68,
	35, -68, 90, 35, 20, 64, -67, 8, 53, -18,
	-67, 19, 76, 65, 66, -31, 21, 67, 23, 24,
	22, 68, 69, 70, 71, 72, 73, 74, 75, 45,
	46, 47, 39, 40, 41, 42, -29, -34, -29, -36,
	-3, -34, -34, 44, 44, 44, -39, 44, -45, -34,
	-55, 44, -58, 35, -28, 9, -59, -34, -67, -68,
	20, -66, 94, -63, 86, 84, 32, 85, 12, 35,
	35, 35, -68, -21, -22, -24, 44, 35, -39, -17,
	-67, 73, -29, -29, -34, -35, 21, 23, 24, -34,
	-34, 25, 67, -34, -34, -34, -34, -34, -34, -34,
	-34, 95, 95, 53, 95, -34, 95, -16, 18, -16,
	-43, -44, 79, -32, 28, -3, -56, -41, -28, -49,
	12, -29, 64, -67, -68, -64, 90, -28, 53, -23,
	54, 55, 56, 57, 58, 60, 61, -19, 35, 19,
	-22, 76, -35, -34, -34, 65, 25, -34, 95, -16,
	95, -46, -44, 81, -29, -57, 64, -37, -35, 95,
	53, -49, -53, 14, 13, 35, 35, -47, 10, -22,
	-22, 54, 59, 54, 59, 54, 54, 54, -26, 62,
	91, 63, 35, 95, 35, 65, -34, 95, 82, -34,
	80, 30, 53, -41, -53, -34, -50, -51, -34, -68,
	-48, 11, 13, 64, 54, 54, 88, 88, 88, -34,
	-34, 31, -35, 53, 53, -52, 26, 27, -49, -29,
	-36, -29, 44, 44, 44, 6, -34, -51, -53, -27,
	-67, -27, -27, -58, -54, 16, 34, 95, 53, 95,
	95, 6, 21, -67, -67, -67,
}
var yyDef = []int{

	0, -2, 1, 2, 3, 4, 5, 6, 7, 8,
	9, 10, 27, 27, 27, 27, 27, 186, 177, 0,
	0, 0, 31, 33, 34, 35, 36, 29, 0, 0,
	0, 0, 175, 0, 0, 187, 0, 0, 178, 0,
	173, 0, 173, 12, 32, 0, 37, 28, 0, 0,
	69, 0, 16, 170, 0, 140, 190, 0, 0, 0,
	191, 190, 0, 191, 0, 0, 0, 0, 0, 0,
	38, 40, 45, 190, 43, 44, 79, 0, 0, 110,
	111, 112, 0, 140, 0, 126, 0, 142, 143, 144,
	145, 106, 129, 130, 131, 127, 128, 133, 30, 164,
	0, 0, 77, 0, 0, 0, 191, 0, 188, 19,
	0, 22, 0, 24, 174, 0, 191, 0, 0, 41,
	46, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 94,
	95, 96, 97, 98, 99, 100, 82, 0, 0, 0,
	0, 108, 121, 0, 0, 0, 93, 0, 0, 134,
	0, 0, 77, 70, 150, 0, 171, 172, 141, 17,
	176, 0, 0, 191, 184, 179, 180, 181, 182, 183,
	23, 25, 26, 77, 48, 54, 0, 66, 68, 39,
	47, 42, 80, 81, 84, 85, 0, 0, 0, 87,
	0, 91, 0, 113, 114, 115, 116, 117, 118, 119,
	120, 83, 105, 0, 107, 108, 122, 0, 0, 0,
	138, 135, 0, 168, 0, 102, 0, 166, 150, 158,
	0, 78, 0, 189, 20, 0, 185, 146, 0, 0,
	57, 58, 0, 0, 0, 0, 0, 71, 55, 0,
	0, 0, 86, 88, 0, 0, 92, 109, 123, 0,
	125, 0, 136, 0, 0, 13, 0, 101, 103, 165,
	0, 158, 15, 0, 0, 191, 21, 148, 0, 49,
	52, 59, 0, 61, 0, 63, 64, 65, 50, 0,
	0, 0, 56, 51, 67, 0, 89, 124, 132, 139,
	0, 0, 0, 167, 14, 159, 151, 152, 155, 18,
	150, 0, 0, 0, 60, 62, 0, 0, 0, 90,
	137, 0, 104, 0, 0, 154, 156, 157, 158, 149,
	147, 53, 0, 0, 0, 0, 160, 153, 161, 0,
	75, 0, 0, 169, 11, 0, 0, 72, 0, 73,
	74, 162, 0, 76, 0, 163,
}
var yyTok1 = []int{

	1, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 75, 68, 3,
	44, 95, 73, 71, 53, 72, 76, 74, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	46, 45, 47, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 70, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 69, 3, 48,
}
var yyTok2 = []int{

	2, 3, 4, 5, 6, 7, 8, 9, 10, 11,
	12, 13, 14, 15, 16, 17, 18, 19, 20, 21,
	22, 23, 24, 25, 26, 27, 28, 29, 30, 31,
	32, 33, 34, 35, 36, 37, 38, 39, 40, 41,
	42, 43, 49, 50, 51, 52, 54, 55, 56, 57,
	58, 59, 60, 61, 62, 63, 64, 65, 66, 67,
	77, 78, 79, 80, 81, 82, 83, 84, 85, 86,
	87, 88, 89, 90, 91, 92, 93, 94,
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
		//line sql.y:147
		{
			SetParseTree(yylex, yyS[yypt-0].statement)
		}
	case 2:
		//line sql.y:153
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
		//line sql.y:167
		{
			yyVAL.selStmt = &Select{Comments: Comments(yyS[yypt-10].bytes2), Distinct: yyS[yypt-9].str, SelectExprs: yyS[yypt-8].selectExprs, From: yyS[yypt-6].tableExprs, Where: NewWhere("where", yyS[yypt-5].boolExpr), GroupBy: GroupBy(yyS[yypt-4].valExprs), Having: NewWhere("having", yyS[yypt-3].boolExpr), OrderBy: yyS[yypt-2].orderBy, Limit: yyS[yypt-1].limit, Lock: yyS[yypt-0].str}
		}
	case 12:
		//line sql.y:171
		{
			yyVAL.selStmt = &Union{Type: yyS[yypt-1].str, Select1: yyS[yypt-2].selStmt, Select2: yyS[yypt-0].selStmt}
		}
	case 13:
		//line sql.y:177
		{
			yyVAL.statement = &Insert{Comments: Comments(yyS[yypt-5].bytes2), Table: yyS[yypt-3].tableName, Columns: yyS[yypt-2].columns, Rows: yyS[yypt-1].sqlNode, OnDup: OnDup(yyS[yypt-0].updateExprs)}
		}
	case 14:
		//line sql.y:183
		{
			yyVAL.statement = &Update{Comments: Comments(yyS[yypt-6].bytes2), Table: yyS[yypt-5].tableName, List: yyS[yypt-3].updateExprs, Where: NewWhere("where", yyS[yypt-2].boolExpr), OrderBy: yyS[yypt-1].orderBy, Limit: yyS[yypt-0].limit}
		}
	case 15:
		//line sql.y:189
		{
			yyVAL.statement = &Delete{Comments: Comments(yyS[yypt-5].bytes2), Table: yyS[yypt-3].tableName, Where: NewWhere("where", yyS[yypt-2].boolExpr), OrderBy: yyS[yypt-1].orderBy, Limit: yyS[yypt-0].limit}
		}
	case 16:
		//line sql.y:195
		{
			yyVAL.statement = &Set{Comments: Comments(yyS[yypt-1].bytes2), Updates: yyS[yypt-0].updateExprs}
		}
	case 17:
		//line sql.y:201
		{
			yyVAL.statement = &DDLSimple{Action: CREATE, Table: yyS[yypt-1].node.Value}
		}
	case 18:
		//line sql.y:205
		{
			// Change this to an alter statement
			yyVAL.statement = &DDLSimple{Action: ALTER, Table: yyS[yypt-1].node.Value}
		}
	case 19:
		//line sql.y:210
		{
			yyVAL.statement = &DDLSimple{Action: CREATE, Table: yyS[yypt-1].node.Value}
		}
	case 20:
		//line sql.y:216
		{
			yyVAL.statement = &DDLSimple{Action: ALTER, Table: yyS[yypt-2].node.Value}
		}
	case 21:
		//line sql.y:220
		{
			// Change this to a rename statement
			yyVAL.statement = &Rename{OldName: yyS[yypt-3].node.Value, NewName: yyS[yypt-0].node.Value}
		}
	case 22:
		//line sql.y:225
		{
			yyVAL.statement = &DDLSimple{Action: ALTER, Table: yyS[yypt-1].node.Value}
		}
	case 23:
		//line sql.y:231
		{
			yyVAL.statement = &Rename{OldName: yyS[yypt-2].node.Value, NewName: yyS[yypt-0].node.Value}
		}
	case 24:
		//line sql.y:237
		{
			yyVAL.statement = &DDLSimple{Action: DROP, Table: yyS[yypt-0].node.Value}
		}
	case 25:
		//line sql.y:241
		{
			// Change this to an alter statement
			yyVAL.statement = &DDLSimple{Action: ALTER, Table: yyS[yypt-0].node.Value}
		}
	case 26:
		//line sql.y:246
		{
			yyVAL.statement = &DDLSimple{Action: DROP, Table: yyS[yypt-1].node.Value}
		}
	case 27:
		//line sql.y:251
		{
			SetAllowComments(yylex, true)
		}
	case 28:
		//line sql.y:255
		{
			yyVAL.bytes2 = yyS[yypt-0].bytes2
			SetAllowComments(yylex, false)
		}
	case 29:
		//line sql.y:261
		{
			yyVAL.bytes2 = nil
		}
	case 30:
		//line sql.y:265
		{
			yyVAL.bytes2 = append(yyS[yypt-1].bytes2, yyS[yypt-0].node.Value)
		}
	case 31:
		//line sql.y:271
		{
			yyVAL.str = "union"
		}
	case 32:
		//line sql.y:275
		{
			yyVAL.str = "union all"
		}
	case 33:
		//line sql.y:279
		{
			yyVAL.str = "minus"
		}
	case 34:
		//line sql.y:283
		{
			yyVAL.str = "except"
		}
	case 35:
		//line sql.y:287
		{
			yyVAL.str = "intersect"
		}
	case 36:
		//line sql.y:292
		{
			yyVAL.str = ""
		}
	case 37:
		//line sql.y:296
		{
			yyVAL.str = "distinct "
		}
	case 38:
		//line sql.y:302
		{
			yyVAL.selectExprs = SelectExprs{yyS[yypt-0].selectExpr}
		}
	case 39:
		//line sql.y:306
		{
			yyVAL.selectExprs = append(yyVAL.selectExprs, yyS[yypt-0].selectExpr)
		}
	case 40:
		//line sql.y:312
		{
			yyVAL.selectExpr = &StarExpr{}
		}
	case 41:
		//line sql.y:316
		{
			yyVAL.selectExpr = &NonStarExpr{Expr: yyS[yypt-1].expr, As: yyS[yypt-0].bytes}
		}
	case 42:
		//line sql.y:320
		{
			yyVAL.selectExpr = &StarExpr{TableName: yyS[yypt-2].node.Value}
		}
	case 43:
		//line sql.y:326
		{
			yyVAL.expr = yyS[yypt-0].boolExpr
		}
	case 44:
		//line sql.y:330
		{
			yyVAL.expr = yyS[yypt-0].valExpr
		}
	case 45:
		//line sql.y:335
		{
			yyVAL.bytes = nil
		}
	case 46:
		//line sql.y:339
		{
			yyVAL.bytes = yyS[yypt-0].node.Value
		}
	case 47:
		//line sql.y:343
		{
			yyVAL.bytes = yyS[yypt-0].node.Value
		}
	case 48:
		//line sql.y:349
		{
			yyVAL.tableExprs = TableExprs{yyS[yypt-0].tableExpr}
		}
	case 49:
		//line sql.y:353
		{
			yyVAL.tableExprs = append(yyVAL.tableExprs, yyS[yypt-0].tableExpr)
		}
	case 50:
		//line sql.y:359
		{
			yyVAL.tableExpr = &AliasedTableExpr{Expr: yyS[yypt-2].sqlNode, As: yyS[yypt-1].bytes, Hints: yyS[yypt-0].indexHints}
		}
	case 51:
		//line sql.y:363
		{
			yyVAL.tableExpr = &ParenTableExpr{Expr: yyS[yypt-1].tableExpr}
		}
	case 52:
		//line sql.y:367
		{
			yyVAL.tableExpr = &JoinTableExpr{LeftExpr: yyS[yypt-2].tableExpr, Join: yyS[yypt-1].str, RightExpr: yyS[yypt-0].tableExpr}
		}
	case 53:
		//line sql.y:371
		{
			yyVAL.tableExpr = &JoinTableExpr{LeftExpr: yyS[yypt-4].tableExpr, Join: yyS[yypt-3].str, RightExpr: yyS[yypt-2].tableExpr, On: yyS[yypt-0].boolExpr}
		}
	case 54:
		//line sql.y:376
		{
			yyVAL.bytes = nil
		}
	case 55:
		//line sql.y:380
		{
			yyVAL.bytes = yyS[yypt-0].node.Value
		}
	case 56:
		//line sql.y:384
		{
			yyVAL.bytes = yyS[yypt-0].node.Value
		}
	case 57:
		//line sql.y:390
		{
			yyVAL.str = "join"
		}
	case 58:
		//line sql.y:394
		{
			yyVAL.str = "straight_join"
		}
	case 59:
		//line sql.y:398
		{
			yyVAL.str = "left join"
		}
	case 60:
		//line sql.y:402
		{
			yyVAL.str = "left join"
		}
	case 61:
		//line sql.y:406
		{
			yyVAL.str = "right join"
		}
	case 62:
		//line sql.y:410
		{
			yyVAL.str = "right join"
		}
	case 63:
		//line sql.y:414
		{
			yyVAL.str = "join"
		}
	case 64:
		//line sql.y:418
		{
			yyVAL.str = "cross join"
		}
	case 65:
		//line sql.y:422
		{
			yyVAL.str = "natural join"
		}
	case 66:
		//line sql.y:428
		{
			yyVAL.sqlNode = &TableName{Name: yyS[yypt-0].node.Value}
		}
	case 67:
		//line sql.y:432
		{
			yyVAL.sqlNode = &TableName{Qualifier: yyS[yypt-2].node.Value, Name: yyS[yypt-0].node.Value}
		}
	case 68:
		//line sql.y:436
		{
			yyVAL.sqlNode = yyS[yypt-0].subquery
		}
	case 69:
		//line sql.y:442
		{
			yyVAL.tableName = &TableName{Name: yyS[yypt-0].node.Value}
		}
	case 70:
		//line sql.y:446
		{
			yyVAL.tableName = &TableName{Qualifier: yyS[yypt-2].node.Value, Name: yyS[yypt-0].node.Value}
		}
	case 71:
		//line sql.y:451
		{
			yyVAL.indexHints = nil
		}
	case 72:
		//line sql.y:455
		{
			yyVAL.indexHints = &IndexHints{Type: "use", Indexes: yyS[yypt-1].bytes2}
		}
	case 73:
		//line sql.y:459
		{
			yyVAL.indexHints = &IndexHints{Type: "ignore", Indexes: yyS[yypt-1].bytes2}
		}
	case 74:
		//line sql.y:463
		{
			yyVAL.indexHints = &IndexHints{Type: "force", Indexes: yyS[yypt-1].bytes2}
		}
	case 75:
		//line sql.y:469
		{
			yyVAL.bytes2 = [][]byte{yyS[yypt-0].node.Value}
		}
	case 76:
		//line sql.y:473
		{
			yyVAL.bytes2 = append(yyS[yypt-2].bytes2, yyS[yypt-0].node.Value)
		}
	case 77:
		//line sql.y:478
		{
			yyVAL.boolExpr = nil
		}
	case 78:
		//line sql.y:482
		{
			yyVAL.boolExpr = yyS[yypt-0].boolExpr
		}
	case 79:
		yyVAL.boolExpr = yyS[yypt-0].boolExpr
	case 80:
		//line sql.y:489
		{
			yyVAL.boolExpr = &AndExpr{Left: yyS[yypt-2].boolExpr, Right: yyS[yypt-0].boolExpr}
		}
	case 81:
		//line sql.y:493
		{
			yyVAL.boolExpr = &OrExpr{Left: yyS[yypt-2].boolExpr, Right: yyS[yypt-0].boolExpr}
		}
	case 82:
		//line sql.y:497
		{
			yyVAL.boolExpr = &NotExpr{Expr: yyS[yypt-0].boolExpr}
		}
	case 83:
		//line sql.y:501
		{
			yyVAL.boolExpr = &ParenBoolExpr{Expr: yyS[yypt-1].boolExpr}
		}
	case 84:
		//line sql.y:507
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyS[yypt-2].valExpr, Operator: yyS[yypt-1].str, Right: yyS[yypt-0].valExpr}
		}
	case 85:
		//line sql.y:511
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyS[yypt-2].valExpr, Operator: "in", Right: yyS[yypt-0].tuple}
		}
	case 86:
		//line sql.y:515
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyS[yypt-3].valExpr, Operator: "not in", Right: yyS[yypt-0].tuple}
		}
	case 87:
		//line sql.y:519
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyS[yypt-2].valExpr, Operator: "like", Right: yyS[yypt-0].valExpr}
		}
	case 88:
		//line sql.y:523
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyS[yypt-3].valExpr, Operator: "not like", Right: yyS[yypt-0].valExpr}
		}
	case 89:
		//line sql.y:527
		{
			yyVAL.boolExpr = &RangeCond{Left: yyS[yypt-4].valExpr, Operator: "between", From: yyS[yypt-2].valExpr, To: yyS[yypt-0].valExpr}
		}
	case 90:
		//line sql.y:531
		{
			yyVAL.boolExpr = &RangeCond{Left: yyS[yypt-5].valExpr, Operator: "not between", From: yyS[yypt-2].valExpr, To: yyS[yypt-0].valExpr}
		}
	case 91:
		//line sql.y:535
		{
			yyVAL.boolExpr = &NullCheck{Operator: "is null", Expr: yyS[yypt-2].valExpr}
		}
	case 92:
		//line sql.y:539
		{
			yyVAL.boolExpr = &NullCheck{Operator: "is not null", Expr: yyS[yypt-3].valExpr}
		}
	case 93:
		//line sql.y:543
		{
			yyVAL.boolExpr = &ExistsExpr{Subquery: yyS[yypt-0].subquery}
		}
	case 94:
		//line sql.y:549
		{
			yyVAL.str = "="
		}
	case 95:
		//line sql.y:553
		{
			yyVAL.str = "<"
		}
	case 96:
		//line sql.y:557
		{
			yyVAL.str = ">"
		}
	case 97:
		//line sql.y:561
		{
			yyVAL.str = "<="
		}
	case 98:
		//line sql.y:565
		{
			yyVAL.str = ">="
		}
	case 99:
		//line sql.y:569
		{
			yyVAL.str = string(yyS[yypt-0].node.Value)
		}
	case 100:
		//line sql.y:573
		{
			yyVAL.str = "<=>"
		}
	case 101:
		//line sql.y:579
		{
			yyVAL.sqlNode = yyS[yypt-0].values
		}
	case 102:
		//line sql.y:583
		{
			yyVAL.sqlNode = yyS[yypt-0].selStmt
		}
	case 103:
		//line sql.y:589
		{
			yyVAL.values = Values{yyS[yypt-0].tuple}
		}
	case 104:
		//line sql.y:593
		{
			yyVAL.values = append(yyS[yypt-2].values, yyS[yypt-0].tuple)
		}
	case 105:
		//line sql.y:599
		{
			yyVAL.tuple = ValueTuple(yyS[yypt-1].valExprs)
		}
	case 106:
		//line sql.y:603
		{
			yyVAL.tuple = yyS[yypt-0].subquery
		}
	case 107:
		//line sql.y:609
		{
			yyVAL.subquery = &Subquery{yyS[yypt-1].selStmt}
		}
	case 108:
		//line sql.y:615
		{
			yyVAL.valExprs = ValExprs{yyS[yypt-0].valExpr}
		}
	case 109:
		//line sql.y:619
		{
			yyVAL.valExprs = append(yyS[yypt-2].valExprs, yyS[yypt-0].valExpr)
		}
	case 110:
		//line sql.y:625
		{
			yyVAL.valExpr = yyS[yypt-0].valExpr
		}
	case 111:
		//line sql.y:629
		{
			yyVAL.valExpr = yyS[yypt-0].colName
		}
	case 112:
		//line sql.y:633
		{
			yyVAL.valExpr = yyS[yypt-0].tuple
		}
	case 113:
		//line sql.y:637
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyS[yypt-2].valExpr, Operator: '&', Right: yyS[yypt-0].valExpr}
		}
	case 114:
		//line sql.y:641
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyS[yypt-2].valExpr, Operator: '|', Right: yyS[yypt-0].valExpr}
		}
	case 115:
		//line sql.y:645
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyS[yypt-2].valExpr, Operator: '^', Right: yyS[yypt-0].valExpr}
		}
	case 116:
		//line sql.y:649
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyS[yypt-2].valExpr, Operator: '+', Right: yyS[yypt-0].valExpr}
		}
	case 117:
		//line sql.y:653
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyS[yypt-2].valExpr, Operator: '-', Right: yyS[yypt-0].valExpr}
		}
	case 118:
		//line sql.y:657
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyS[yypt-2].valExpr, Operator: '*', Right: yyS[yypt-0].valExpr}
		}
	case 119:
		//line sql.y:661
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyS[yypt-2].valExpr, Operator: '/', Right: yyS[yypt-0].valExpr}
		}
	case 120:
		//line sql.y:665
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyS[yypt-2].valExpr, Operator: '%', Right: yyS[yypt-0].valExpr}
		}
	case 121:
		//line sql.y:669
		{
			if num, ok := yyS[yypt-0].valExpr.(NumValue); ok {
				switch yyS[yypt-1].byt {
				case '-':
					yyVAL.valExpr = append(NumValue("-"), num...)
				case '+':
					yyVAL.valExpr = num
				default:
					yyVAL.valExpr = &UnaryExpr{Operator: yyS[yypt-1].byt, Expr: yyS[yypt-0].valExpr}
				}
			} else {
				yyVAL.valExpr = &UnaryExpr{Operator: yyS[yypt-1].byt, Expr: yyS[yypt-0].valExpr}
			}
		}
	case 122:
		//line sql.y:684
		{
			yyVAL.valExpr = &FuncExpr{Name: yyS[yypt-2].node.Value}
		}
	case 123:
		//line sql.y:688
		{
			yyVAL.valExpr = &FuncExpr{Name: yyS[yypt-3].node.Value, Exprs: yyS[yypt-1].selectExprs}
		}
	case 124:
		//line sql.y:692
		{
			yyVAL.valExpr = &FuncExpr{Name: yyS[yypt-4].node.Value, Distinct: true, Exprs: yyS[yypt-1].selectExprs}
		}
	case 125:
		//line sql.y:696
		{
			yyVAL.valExpr = &FuncExpr{Name: yyS[yypt-3].bytes, Exprs: yyS[yypt-1].selectExprs}
		}
	case 126:
		//line sql.y:700
		{
			yyVAL.valExpr = yyS[yypt-0].caseExpr
		}
	case 127:
		//line sql.y:706
		{
			yyVAL.bytes = yyS[yypt-0].node.Value
		}
	case 128:
		//line sql.y:710
		{
			yyVAL.bytes = yyS[yypt-0].node.Value
		}
	case 129:
		//line sql.y:716
		{
			yyVAL.byt = '+'
		}
	case 130:
		//line sql.y:720
		{
			yyVAL.byt = '-'
		}
	case 131:
		//line sql.y:724
		{
			yyVAL.byt = '~'
		}
	case 132:
		//line sql.y:730
		{
			yyVAL.caseExpr = &CaseExpr{Expr: yyS[yypt-3].valExpr, Whens: yyS[yypt-2].whens, Else: yyS[yypt-1].valExpr}
		}
	case 133:
		//line sql.y:735
		{
			yyVAL.valExpr = nil
		}
	case 134:
		//line sql.y:739
		{
			yyVAL.valExpr = yyS[yypt-0].valExpr
		}
	case 135:
		//line sql.y:745
		{
			yyVAL.whens = []*When{yyS[yypt-0].when}
		}
	case 136:
		//line sql.y:749
		{
			yyVAL.whens = append(yyS[yypt-1].whens, yyS[yypt-0].when)
		}
	case 137:
		//line sql.y:755
		{
			yyVAL.when = &When{Cond: yyS[yypt-2].boolExpr, Val: yyS[yypt-0].valExpr}
		}
	case 138:
		//line sql.y:760
		{
			yyVAL.valExpr = nil
		}
	case 139:
		//line sql.y:764
		{
			yyVAL.valExpr = yyS[yypt-0].valExpr
		}
	case 140:
		//line sql.y:770
		{
			yyVAL.colName = &ColName{Name: yyS[yypt-0].node.Value}
		}
	case 141:
		//line sql.y:774
		{
			yyVAL.colName = &ColName{Qualifier: yyS[yypt-2].node.Value, Name: yyS[yypt-0].node.Value}
		}
	case 142:
		//line sql.y:780
		{
			yyVAL.valExpr = StringValue(yyS[yypt-0].node.Value)
		}
	case 143:
		//line sql.y:784
		{
			yyVAL.valExpr = NumValue(yyS[yypt-0].node.Value)
		}
	case 144:
		//line sql.y:788
		{
			yyVAL.valExpr = ValueArg(yyS[yypt-0].node.Value)
		}
	case 145:
		//line sql.y:792
		{
			yyVAL.valExpr = &NullValue{}
		}
	case 146:
		//line sql.y:797
		{
			yyVAL.valExprs = nil
		}
	case 147:
		//line sql.y:801
		{
			yyVAL.valExprs = yyS[yypt-0].valExprs
		}
	case 148:
		//line sql.y:806
		{
			yyVAL.boolExpr = nil
		}
	case 149:
		//line sql.y:810
		{
			yyVAL.boolExpr = yyS[yypt-0].boolExpr
		}
	case 150:
		//line sql.y:815
		{
			yyVAL.orderBy = nil
		}
	case 151:
		//line sql.y:819
		{
			yyVAL.orderBy = yyS[yypt-0].orderBy
		}
	case 152:
		//line sql.y:825
		{
			yyVAL.orderBy = OrderBy{yyS[yypt-0].order}
		}
	case 153:
		//line sql.y:829
		{
			yyVAL.orderBy = append(yyS[yypt-2].orderBy, yyS[yypt-0].order)
		}
	case 154:
		//line sql.y:835
		{
			yyVAL.order = &Order{Expr: yyS[yypt-1].valExpr, Direction: yyS[yypt-0].str}
		}
	case 155:
		//line sql.y:840
		{
			yyVAL.str = "asc"
		}
	case 156:
		//line sql.y:844
		{
			yyVAL.str = "asc"
		}
	case 157:
		//line sql.y:848
		{
			yyVAL.str = "desc"
		}
	case 158:
		//line sql.y:853
		{
			yyVAL.limit = nil
		}
	case 159:
		//line sql.y:857
		{
			yyVAL.limit = &Limit{Rowcount: yyS[yypt-0].valExpr}
		}
	case 160:
		//line sql.y:861
		{
			yyVAL.limit = &Limit{Offset: yyS[yypt-2].valExpr, Rowcount: yyS[yypt-0].valExpr}
		}
	case 161:
		//line sql.y:866
		{
			yyVAL.str = ""
		}
	case 162:
		//line sql.y:870
		{
			yyVAL.str = " for update"
		}
	case 163:
		//line sql.y:874
		{
			if !bytes.Equal(yyS[yypt-1].node.Value, SHARE) {
				yylex.Error("expecting share")
				return 1
			}
			if !bytes.Equal(yyS[yypt-0].node.Value, MODE) {
				yylex.Error("expecting mode")
				return 1
			}
			yyVAL.str = " lock in share mode"
		}
	case 164:
		//line sql.y:887
		{
			yyVAL.columns = nil
		}
	case 165:
		//line sql.y:891
		{
			yyVAL.columns = yyS[yypt-1].columns
		}
	case 166:
		//line sql.y:897
		{
			yyVAL.columns = Columns{&NonStarExpr{Expr: yyS[yypt-0].colName}}
		}
	case 167:
		//line sql.y:901
		{
			yyVAL.columns = append(yyVAL.columns, &NonStarExpr{Expr: yyS[yypt-0].colName})
		}
	case 168:
		//line sql.y:906
		{
			yyVAL.updateExprs = nil
		}
	case 169:
		//line sql.y:910
		{
			yyVAL.updateExprs = yyS[yypt-0].updateExprs
		}
	case 170:
		//line sql.y:916
		{
			yyVAL.updateExprs = UpdateExprs{yyS[yypt-0].updateExpr}
		}
	case 171:
		//line sql.y:920
		{
			yyVAL.updateExprs = append(yyS[yypt-2].updateExprs, yyS[yypt-0].updateExpr)
		}
	case 172:
		//line sql.y:926
		{
			yyVAL.updateExpr = &UpdateExpr{Name: yyS[yypt-2].colName, Expr: yyS[yypt-0].valExpr}
		}
	case 173:
		//line sql.y:931
		{
			yyVAL.empty = struct{}{}
		}
	case 174:
		//line sql.y:933
		{
			yyVAL.empty = struct{}{}
		}
	case 175:
		//line sql.y:936
		{
			yyVAL.empty = struct{}{}
		}
	case 176:
		//line sql.y:938
		{
			yyVAL.empty = struct{}{}
		}
	case 177:
		//line sql.y:941
		{
			yyVAL.empty = struct{}{}
		}
	case 178:
		//line sql.y:943
		{
			yyVAL.empty = struct{}{}
		}
	case 179:
		//line sql.y:947
		{
			yyVAL.empty = struct{}{}
		}
	case 180:
		//line sql.y:949
		{
			yyVAL.empty = struct{}{}
		}
	case 181:
		//line sql.y:951
		{
			yyVAL.empty = struct{}{}
		}
	case 182:
		//line sql.y:953
		{
			yyVAL.empty = struct{}{}
		}
	case 183:
		//line sql.y:955
		{
			yyVAL.empty = struct{}{}
		}
	case 184:
		//line sql.y:958
		{
			yyVAL.empty = struct{}{}
		}
	case 185:
		//line sql.y:960
		{
			yyVAL.empty = struct{}{}
		}
	case 186:
		//line sql.y:963
		{
			yyVAL.empty = struct{}{}
		}
	case 187:
		//line sql.y:965
		{
			yyVAL.empty = struct{}{}
		}
	case 188:
		//line sql.y:968
		{
			yyVAL.empty = struct{}{}
		}
	case 189:
		//line sql.y:970
		{
			yyVAL.empty = struct{}{}
		}
	case 190:
		//line sql.y:974
		{
			yyVAL.node.LowerCase()
		}
	case 191:
		//line sql.y:979
		{
			ForceEOF(yylex)
		}
	}
	goto yystack /* stack new state and value */
}
