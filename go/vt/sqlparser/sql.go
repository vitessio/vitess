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
const COMMENT = 57381
const LE = 57382
const GE = 57383
const NE = 57384
const NULL_SAFE_EQUAL = 57385
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
	123, 124, 348, 12, 240, 241, 242, 243, 244, 348,
	245, 246, 55, 54, 348, 60, 74, 118, 63, 43,
	270, 118, 67, 90, 118, 213, 96, 172, 75, 66,
	211, 289, 291, 56, 87, 88, 89, 58, 236, 318,
	108, 32, 153, 34, 350, 293, 94, 35, 37, 116,
	38, 349, 112, 120, 111, 298, 347, 222, 317, 297,
	290, 151, 269, 260, 316, 152, 258, 212, 59, 92,
	93, 40, 41, 42, 62, 156, 97, 39, 123, 124,
	159, 55, 54, 251, 55, 54, 168, 167, 162, 222,
	95, 263, 122, 300, 146, 148, 105, 169, 101, 107,
	166, 75, 190, 168, 201, 313, 188, 182, 194, 123,
	124, 199, 200, 189, 203, 204, 205, 206, 207, 208,
	209, 210, 195, 266, 12, 22, 23, 24, 25, 134,
	135, 136, 137, 138, 61, 232, 215, 75, 75, 86,
	192, 193, 55, 227, 90, 115, 202, 96, 217, 219,
	136, 137, 138, 233, 56, 87, 88, 89, 225, 315,
	326, 327, 228, 78, 234, 283, 314, 94, 281, 287,
	284, 214, 191, 282, 286, 188, 178, 285, 250, 117,
	253, 254, 231, 237, 103, 213, 77, 104, 324, 302,
	92, 93, 252, 165, 165, 176, 257, 97, 179, 334,
	333, 75, 131, 132, 133, 134, 135, 136, 137, 138,
	187, 95, 259, 262, 12, 218, 332, 86, 271, 186,
	268, 153, 90, 118, 161, 96, 157, 188, 188, 155,
	279, 280, 73, 87, 88, 89, 238, 103, 296, 264,
	154, 78, 49, 98, 187, 94, 299, 175, 177, 174,
	65, 55, 303, 186, 61, 304, 305, 308, 12, 13,
	14, 15, 56, 249, 77, 121, 309, 345, 92, 93,
	71, 22, 23, 24, 25, 97, 294, 292, 319, 248,
	276, 61, 275, 320, 181, 346, 16, 180, 321, 95,
	163, 99, 216, 68, 102, 215, 113, 110, 322, 330,
	328, 106, 50, 64, 100, 301, 336, 308, 48, 12,
	337, 256, 338, 340, 340, 340, 55, 54, 341, 342,
	352, 46, 196, 343, 197, 198, 86, 170, 329, 353,
	331, 90, 224, 354, 96, 355, 17, 18, 20, 19,
	114, 73, 87, 88, 89, 26, 44, 312, 86, 273,
	78, 274, 230, 90, 94, 311, 96, 278, 165, 28,
	29, 30, 31, 56, 87, 88, 89, 351, 51, 335,
	12, 27, 78, 77, 171, 33, 94, 92, 93, 71,
	235, 173, 36, 57, 97, 265, 240, 241, 242, 243,
	244, 90, 245, 246, 96, 77, 226, 160, 95, 92,
	93, 56, 87, 88, 89, 344, 97, 325, 306, 310,
	153, 323, 277, 261, 94, 158, 220, 85, 82, 84,
	95, 126, 130, 128, 129, 267, 131, 132, 133, 134,
	135, 136, 137, 138, 79, 223, 125, 92, 93, 76,
	142, 143, 144, 145, 97, 139, 140, 141, 131, 132,
	133, 134, 135, 136, 137, 138, 295, 288, 95, 131,
	132, 133, 134, 135, 136, 137, 138, 127, 131, 132,
	133, 134, 135, 136, 137, 138, 255, 185, 239, 131,
	132, 133, 134, 135, 136, 137, 138, 183, 72, 247,
	119, 45, 21, 47, 11, 10, 9, 8, 7, 6,
	5, 4, 2, 1,
}
var yyPact = []int{

	273, -1000, -1000, 242, -1000, -1000, -1000, -1000, -1000, -1000,
	-1000, -1000, -1000, -1000, -1000, -1000, -1000, -26, -21, 10,
	4, 385, 349, -1000, -1000, -1000, 323, -1000, 299, 287,
	379, 247, -35, 0, 239, -1000, 7, 239, -1000, 288,
	-43, 239, -43, -1000, -1000, 326, -1000, 224, 287, 291,
	42, 287, 151, -1000, 162, -1000, 40, 286, 52, 239,
	-1000, -1000, 282, -1000, -18, 281, 340, 101, 239, 190,
	-1000, -1000, 266, 36, 64, 420, -1000, 348, 139, -1000,
	-1000, -1000, 386, 216, 205, -1000, 202, -1000, -1000, -1000,
	-1000, -1000, -1000, -1000, -1000, -1000, -1000, 386, -1000, 200,
	247, 275, 368, 247, 386, 239, -1000, 327, -47, -1000,
	183, -1000, 272, -1000, -1000, 269, -1000, 195, 326, -1000,
	-1000, 239, 119, 348, 348, 386, 197, 321, 386, 386,
	99, 386, 386, 386, 386, 386, 386, 386, 386, -1000,
	-1000, -1000, -1000, -1000, -1000, -1000, -1000, 420, -45, -8,
	96, 420, -1000, 18, 217, 326, -1000, 385, -2, 400,
	324, 247, 204, -1000, 359, 348, -1000, 400, -1000, -1000,
	-1000, 91, 239, -1000, -32, -1000, -1000, -1000, -1000, -1000,
	-1000, -1000, -1000, 203, 352, 264, 229, 27, -1000, -1000,
	-1000, -1000, -1000, -1000, 400, -1000, 197, 386, 386, 400,
	431, -1000, 306, 78, 78, 78, 97, 97, -1000, -1000,
	-1000, -1000, -1000, 386, -1000, 400, -1000, -9, 326, -12,
	30, -1000, 348, 79, 197, 242, -13, -1000, 359, 354,
	357, 64, 267, -1000, -1000, 265, -1000, 366, 195, 195,
	-1000, -1000, 134, 131, 143, 140, 135, -11, -1000, 262,
	-30, 261, -1000, 400, 411, 386, -1000, 400, -1000, -16,
	-1000, -7, -1000, 386, 33, -1000, 295, 156, -1000, -1000,
	247, 354, -1000, 386, 386, -1000, -1000, 363, 353, 352,
	61, -1000, 132, -1000, 125, -1000, -1000, -1000, -1000, -4,
	-10, -29, -1000, -1000, -1000, 386, 400, -1000, -1000, 400,
	386, 277, 197, -1000, -1000, 378, 155, -1000, 154, -1000,
	359, 348, 386, 348, -1000, -1000, 192, 176, 175, 400,
	400, 382, -1000, 386, 386, -1000, -1000, -1000, 354, 64,
	152, 64, 239, 239, 239, 247, 400, -1000, 271, -19,
	-1000, -24, -31, 151, -1000, 380, 319, -1000, 239, -1000,
	-1000, -1000, 239, -1000, 239, -1000,
}
var yyPgo = []int{

	0, 523, 522, 17, 521, 520, 519, 518, 517, 516,
	515, 514, 365, 513, 512, 511, 13, 14, 510, 509,
	508, 507, 11, 498, 497, 262, 477, 4, 19, 36,
	459, 456, 455, 454, 2, 15, 6, 445, 439, 8,
	438, 1, 437, 436, 12, 435, 433, 432, 429, 9,
	428, 5, 427, 3, 425, 417, 416, 405, 7, 16,
	270, 403, 402, 401, 400, 395, 394, 0, 10, 391,
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
	-10, -11, 5, 6, 7, 8, 33, 83, 84, 86,
	85, -14, 49, 50, 51, 52, -12, -69, -12, -12,
	-12, -12, 87, -65, 89, 93, -62, 89, 91, 87,
	87, 88, 89, -3, 17, -15, 18, -13, 29, -25,
	35, 9, -58, -59, -41, -67, 35, -61, 92, 88,
	-67, 35, 87, -67, 35, -60, 92, -67, -60, -16,
	-17, 73, -20, 35, -29, -34, -30, 67, 44, -33,
	-41, -35, -40, -67, -38, -42, 20, 36, 37, 38,
	25, -39, 71, 72, 48, 92, 28, 78, 39, -25,
	33, 76, -25, 53, 45, 76, 35, 67, -67, -68,
	35, -68, 90, 35, 20, 64, -67, 9, 53, -18,
	-67, 19, 76, 65, 66, -31, 21, 67, 23, 24,
	22, 68, 69, 70, 71, 72, 73, 74, 75, 45,
	46, 47, 40, 41, 42, 43, -29, -34, -29, -36,
	-3, -34, -34, 44, 44, 44, -39, 44, -45, -34,
	-55, 44, -58, 35, -28, 10, -59, -34, -67, -68,
	20, -66, 94, -63, 86, 84, 32, 85, 13, 35,
	35, 35, -68, -21, -22, -24, 44, 35, -39, -17,
	-67, 73, -29, -29, -34, -35, 21, 23, 24, -34,
	-34, 25, 67, -34, -34, -34, -34, -34, -34, -34,
	-34, 95, 95, 53, 95, -34, 95, -16, 18, -16,
	-43, -44, 79, -32, 28, -3, -56, -41, -28, -49,
	13, -29, 64, -67, -68, -64, 90, -28, 53, -23,
	54, 55, 56, 57, 58, 60, 61, -19, 35, 19,
	-22, 76, -35, -34, -34, 65, 25, -34, 95, -16,
	95, -46, -44, 81, -29, -57, 64, -37, -35, 95,
	53, -49, -53, 15, 14, 35, 35, -47, 11, -22,
	-22, 54, 59, 54, 59, 54, 54, 54, -26, 62,
	91, 63, 35, 95, 35, 65, -34, 95, 82, -34,
	80, 30, 53, -41, -53, -34, -50, -51, -34, -68,
	-48, 12, 14, 64, 54, 54, 88, 88, 88, -34,
	-34, 31, -35, 53, 53, -52, 26, 27, -49, -29,
	-36, -29, 44, 44, 44, 7, -34, -51, -53, -27,
	-67, -27, -27, -58, -54, 16, 34, 95, 53, 95,
	95, 7, 21, -67, -67, -67,
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
		//line sql.y:146
		{
			SetParseTree(yylex, yyS[yypt-0].statement)
		}
	case 2:
		//line sql.y:152
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
		//line sql.y:166
		{
			yyVAL.selStmt = &Select{Comments: Comments(yyS[yypt-10].bytes2), Distinct: yyS[yypt-9].str, SelectExprs: yyS[yypt-8].selectExprs, From: yyS[yypt-6].tableExprs, Where: NewWhere(AST_WHERE, yyS[yypt-5].boolExpr), GroupBy: GroupBy(yyS[yypt-4].valExprs), Having: NewWhere(AST_HAVING, yyS[yypt-3].boolExpr), OrderBy: yyS[yypt-2].orderBy, Limit: yyS[yypt-1].limit, Lock: yyS[yypt-0].str}
		}
	case 12:
		//line sql.y:170
		{
			yyVAL.selStmt = &Union{Type: yyS[yypt-1].str, Left: yyS[yypt-2].selStmt, Right: yyS[yypt-0].selStmt}
		}
	case 13:
		//line sql.y:176
		{
			yyVAL.statement = &Insert{Comments: Comments(yyS[yypt-5].bytes2), Table: yyS[yypt-3].tableName, Columns: yyS[yypt-2].columns, Rows: yyS[yypt-1].insRows, OnDup: OnDup(yyS[yypt-0].updateExprs)}
		}
	case 14:
		//line sql.y:182
		{
			yyVAL.statement = &Update{Comments: Comments(yyS[yypt-6].bytes2), Table: yyS[yypt-5].tableName, Exprs: yyS[yypt-3].updateExprs, Where: NewWhere(AST_WHERE, yyS[yypt-2].boolExpr), OrderBy: yyS[yypt-1].orderBy, Limit: yyS[yypt-0].limit}
		}
	case 15:
		//line sql.y:188
		{
			yyVAL.statement = &Delete{Comments: Comments(yyS[yypt-5].bytes2), Table: yyS[yypt-3].tableName, Where: NewWhere(AST_WHERE, yyS[yypt-2].boolExpr), OrderBy: yyS[yypt-1].orderBy, Limit: yyS[yypt-0].limit}
		}
	case 16:
		//line sql.y:194
		{
			yyVAL.statement = &Set{Comments: Comments(yyS[yypt-1].bytes2), Exprs: yyS[yypt-0].updateExprs}
		}
	case 17:
		//line sql.y:200
		{
			yyVAL.statement = &DDL{Action: AST_CREATE, NewName: yyS[yypt-1].bytes}
		}
	case 18:
		//line sql.y:204
		{
			// Change this to an alter statement
			yyVAL.statement = &DDL{Action: AST_ALTER, Table: yyS[yypt-1].bytes, NewName: yyS[yypt-1].bytes}
		}
	case 19:
		//line sql.y:209
		{
			yyVAL.statement = &DDL{Action: AST_CREATE, NewName: yyS[yypt-1].bytes}
		}
	case 20:
		//line sql.y:215
		{
			yyVAL.statement = &DDL{Action: AST_ALTER, Table: yyS[yypt-2].bytes, NewName: yyS[yypt-2].bytes}
		}
	case 21:
		//line sql.y:219
		{
			// Change this to a rename statement
			yyVAL.statement = &DDL{Action: AST_RENAME, Table: yyS[yypt-3].bytes, NewName: yyS[yypt-0].bytes}
		}
	case 22:
		//line sql.y:224
		{
			yyVAL.statement = &DDL{Action: AST_ALTER, Table: yyS[yypt-1].bytes, NewName: yyS[yypt-1].bytes}
		}
	case 23:
		//line sql.y:230
		{
			yyVAL.statement = &DDL{Action: AST_RENAME, Table: yyS[yypt-2].bytes, NewName: yyS[yypt-0].bytes}
		}
	case 24:
		//line sql.y:236
		{
			yyVAL.statement = &DDL{Action: AST_DROP, Table: yyS[yypt-0].bytes}
		}
	case 25:
		//line sql.y:240
		{
			// Change this to an alter statement
			yyVAL.statement = &DDL{Action: AST_ALTER, Table: yyS[yypt-0].bytes, NewName: yyS[yypt-0].bytes}
		}
	case 26:
		//line sql.y:245
		{
			yyVAL.statement = &DDL{Action: AST_DROP, Table: yyS[yypt-1].bytes}
		}
	case 27:
		//line sql.y:250
		{
			SetAllowComments(yylex, true)
		}
	case 28:
		//line sql.y:254
		{
			yyVAL.bytes2 = yyS[yypt-0].bytes2
			SetAllowComments(yylex, false)
		}
	case 29:
		//line sql.y:260
		{
			yyVAL.bytes2 = nil
		}
	case 30:
		//line sql.y:264
		{
			yyVAL.bytes2 = append(yyS[yypt-1].bytes2, yyS[yypt-0].bytes)
		}
	case 31:
		//line sql.y:270
		{
			yyVAL.str = AST_UNION
		}
	case 32:
		//line sql.y:274
		{
			yyVAL.str = AST_UNION_ALL
		}
	case 33:
		//line sql.y:278
		{
			yyVAL.str = AST_SET_MINUS
		}
	case 34:
		//line sql.y:282
		{
			yyVAL.str = AST_EXCEPT
		}
	case 35:
		//line sql.y:286
		{
			yyVAL.str = AST_INTERSECT
		}
	case 36:
		//line sql.y:291
		{
			yyVAL.str = ""
		}
	case 37:
		//line sql.y:295
		{
			yyVAL.str = AST_DISTINCT
		}
	case 38:
		//line sql.y:301
		{
			yyVAL.selectExprs = SelectExprs{yyS[yypt-0].selectExpr}
		}
	case 39:
		//line sql.y:305
		{
			yyVAL.selectExprs = append(yyVAL.selectExprs, yyS[yypt-0].selectExpr)
		}
	case 40:
		//line sql.y:311
		{
			yyVAL.selectExpr = &StarExpr{}
		}
	case 41:
		//line sql.y:315
		{
			yyVAL.selectExpr = &NonStarExpr{Expr: yyS[yypt-1].expr, As: yyS[yypt-0].bytes}
		}
	case 42:
		//line sql.y:319
		{
			yyVAL.selectExpr = &StarExpr{TableName: yyS[yypt-2].bytes}
		}
	case 43:
		//line sql.y:325
		{
			yyVAL.expr = yyS[yypt-0].boolExpr
		}
	case 44:
		//line sql.y:329
		{
			yyVAL.expr = yyS[yypt-0].valExpr
		}
	case 45:
		//line sql.y:334
		{
			yyVAL.bytes = nil
		}
	case 46:
		//line sql.y:338
		{
			yyVAL.bytes = yyS[yypt-0].bytes
		}
	case 47:
		//line sql.y:342
		{
			yyVAL.bytes = yyS[yypt-0].bytes
		}
	case 48:
		//line sql.y:348
		{
			yyVAL.tableExprs = TableExprs{yyS[yypt-0].tableExpr}
		}
	case 49:
		//line sql.y:352
		{
			yyVAL.tableExprs = append(yyVAL.tableExprs, yyS[yypt-0].tableExpr)
		}
	case 50:
		//line sql.y:358
		{
			yyVAL.tableExpr = &AliasedTableExpr{Expr: yyS[yypt-2].smTableExpr, As: yyS[yypt-1].bytes, Hints: yyS[yypt-0].indexHints}
		}
	case 51:
		//line sql.y:362
		{
			yyVAL.tableExpr = &ParenTableExpr{Expr: yyS[yypt-1].tableExpr}
		}
	case 52:
		//line sql.y:366
		{
			yyVAL.tableExpr = &JoinTableExpr{LeftExpr: yyS[yypt-2].tableExpr, Join: yyS[yypt-1].str, RightExpr: yyS[yypt-0].tableExpr}
		}
	case 53:
		//line sql.y:370
		{
			yyVAL.tableExpr = &JoinTableExpr{LeftExpr: yyS[yypt-4].tableExpr, Join: yyS[yypt-3].str, RightExpr: yyS[yypt-2].tableExpr, On: yyS[yypt-0].boolExpr}
		}
	case 54:
		//line sql.y:375
		{
			yyVAL.bytes = nil
		}
	case 55:
		//line sql.y:379
		{
			yyVAL.bytes = yyS[yypt-0].bytes
		}
	case 56:
		//line sql.y:383
		{
			yyVAL.bytes = yyS[yypt-0].bytes
		}
	case 57:
		//line sql.y:389
		{
			yyVAL.str = AST_JOIN
		}
	case 58:
		//line sql.y:393
		{
			yyVAL.str = AST_STRAIGHT_JOIN
		}
	case 59:
		//line sql.y:397
		{
			yyVAL.str = AST_LEFT_JOIN
		}
	case 60:
		//line sql.y:401
		{
			yyVAL.str = AST_LEFT_JOIN
		}
	case 61:
		//line sql.y:405
		{
			yyVAL.str = AST_RIGHT_JOIN
		}
	case 62:
		//line sql.y:409
		{
			yyVAL.str = AST_RIGHT_JOIN
		}
	case 63:
		//line sql.y:413
		{
			yyVAL.str = AST_JOIN
		}
	case 64:
		//line sql.y:417
		{
			yyVAL.str = AST_CROSS_JOIN
		}
	case 65:
		//line sql.y:421
		{
			yyVAL.str = AST_NATURAL_JOIN
		}
	case 66:
		//line sql.y:427
		{
			yyVAL.smTableExpr = &TableName{Name: yyS[yypt-0].bytes}
		}
	case 67:
		//line sql.y:431
		{
			yyVAL.smTableExpr = &TableName{Qualifier: yyS[yypt-2].bytes, Name: yyS[yypt-0].bytes}
		}
	case 68:
		//line sql.y:435
		{
			yyVAL.smTableExpr = yyS[yypt-0].subquery
		}
	case 69:
		//line sql.y:441
		{
			yyVAL.tableName = &TableName{Name: yyS[yypt-0].bytes}
		}
	case 70:
		//line sql.y:445
		{
			yyVAL.tableName = &TableName{Qualifier: yyS[yypt-2].bytes, Name: yyS[yypt-0].bytes}
		}
	case 71:
		//line sql.y:450
		{
			yyVAL.indexHints = nil
		}
	case 72:
		//line sql.y:454
		{
			yyVAL.indexHints = &IndexHints{Type: AST_USE, Indexes: yyS[yypt-1].bytes2}
		}
	case 73:
		//line sql.y:458
		{
			yyVAL.indexHints = &IndexHints{Type: AST_IGNORE, Indexes: yyS[yypt-1].bytes2}
		}
	case 74:
		//line sql.y:462
		{
			yyVAL.indexHints = &IndexHints{Type: AST_FORCE, Indexes: yyS[yypt-1].bytes2}
		}
	case 75:
		//line sql.y:468
		{
			yyVAL.bytes2 = [][]byte{yyS[yypt-0].bytes}
		}
	case 76:
		//line sql.y:472
		{
			yyVAL.bytes2 = append(yyS[yypt-2].bytes2, yyS[yypt-0].bytes)
		}
	case 77:
		//line sql.y:477
		{
			yyVAL.boolExpr = nil
		}
	case 78:
		//line sql.y:481
		{
			yyVAL.boolExpr = yyS[yypt-0].boolExpr
		}
	case 79:
		yyVAL.boolExpr = yyS[yypt-0].boolExpr
	case 80:
		//line sql.y:488
		{
			yyVAL.boolExpr = &AndExpr{Left: yyS[yypt-2].boolExpr, Right: yyS[yypt-0].boolExpr}
		}
	case 81:
		//line sql.y:492
		{
			yyVAL.boolExpr = &OrExpr{Left: yyS[yypt-2].boolExpr, Right: yyS[yypt-0].boolExpr}
		}
	case 82:
		//line sql.y:496
		{
			yyVAL.boolExpr = &NotExpr{Expr: yyS[yypt-0].boolExpr}
		}
	case 83:
		//line sql.y:500
		{
			yyVAL.boolExpr = &ParenBoolExpr{Expr: yyS[yypt-1].boolExpr}
		}
	case 84:
		//line sql.y:506
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyS[yypt-2].valExpr, Operator: yyS[yypt-1].str, Right: yyS[yypt-0].valExpr}
		}
	case 85:
		//line sql.y:510
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyS[yypt-2].valExpr, Operator: AST_IN, Right: yyS[yypt-0].tuple}
		}
	case 86:
		//line sql.y:514
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyS[yypt-3].valExpr, Operator: AST_NOT_IN, Right: yyS[yypt-0].tuple}
		}
	case 87:
		//line sql.y:518
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyS[yypt-2].valExpr, Operator: AST_LIKE, Right: yyS[yypt-0].valExpr}
		}
	case 88:
		//line sql.y:522
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyS[yypt-3].valExpr, Operator: AST_NOT_LIKE, Right: yyS[yypt-0].valExpr}
		}
	case 89:
		//line sql.y:526
		{
			yyVAL.boolExpr = &RangeCond{Left: yyS[yypt-4].valExpr, Operator: AST_BETWEEN, From: yyS[yypt-2].valExpr, To: yyS[yypt-0].valExpr}
		}
	case 90:
		//line sql.y:530
		{
			yyVAL.boolExpr = &RangeCond{Left: yyS[yypt-5].valExpr, Operator: AST_NOT_BETWEEN, From: yyS[yypt-2].valExpr, To: yyS[yypt-0].valExpr}
		}
	case 91:
		//line sql.y:534
		{
			yyVAL.boolExpr = &NullCheck{Operator: AST_IS_NULL, Expr: yyS[yypt-2].valExpr}
		}
	case 92:
		//line sql.y:538
		{
			yyVAL.boolExpr = &NullCheck{Operator: AST_IS_NOT_NULL, Expr: yyS[yypt-3].valExpr}
		}
	case 93:
		//line sql.y:542
		{
			yyVAL.boolExpr = &ExistsExpr{Subquery: yyS[yypt-0].subquery}
		}
	case 94:
		//line sql.y:548
		{
			yyVAL.str = AST_EQ
		}
	case 95:
		//line sql.y:552
		{
			yyVAL.str = AST_LT
		}
	case 96:
		//line sql.y:556
		{
			yyVAL.str = AST_GT
		}
	case 97:
		//line sql.y:560
		{
			yyVAL.str = AST_LE
		}
	case 98:
		//line sql.y:564
		{
			yyVAL.str = AST_GE
		}
	case 99:
		//line sql.y:568
		{
			yyVAL.str = AST_NE
		}
	case 100:
		//line sql.y:572
		{
			yyVAL.str = AST_NSE
		}
	case 101:
		//line sql.y:578
		{
			yyVAL.insRows = yyS[yypt-0].values
		}
	case 102:
		//line sql.y:582
		{
			yyVAL.insRows = yyS[yypt-0].selStmt
		}
	case 103:
		//line sql.y:588
		{
			yyVAL.values = Values{yyS[yypt-0].tuple}
		}
	case 104:
		//line sql.y:592
		{
			yyVAL.values = append(yyS[yypt-2].values, yyS[yypt-0].tuple)
		}
	case 105:
		//line sql.y:598
		{
			yyVAL.tuple = ValTuple(yyS[yypt-1].valExprs)
		}
	case 106:
		//line sql.y:602
		{
			yyVAL.tuple = yyS[yypt-0].subquery
		}
	case 107:
		//line sql.y:608
		{
			yyVAL.subquery = &Subquery{yyS[yypt-1].selStmt}
		}
	case 108:
		//line sql.y:614
		{
			yyVAL.valExprs = ValExprs{yyS[yypt-0].valExpr}
		}
	case 109:
		//line sql.y:618
		{
			yyVAL.valExprs = append(yyS[yypt-2].valExprs, yyS[yypt-0].valExpr)
		}
	case 110:
		//line sql.y:624
		{
			yyVAL.valExpr = yyS[yypt-0].valExpr
		}
	case 111:
		//line sql.y:628
		{
			yyVAL.valExpr = yyS[yypt-0].colName
		}
	case 112:
		//line sql.y:632
		{
			yyVAL.valExpr = yyS[yypt-0].tuple
		}
	case 113:
		//line sql.y:636
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyS[yypt-2].valExpr, Operator: AST_BITAND, Right: yyS[yypt-0].valExpr}
		}
	case 114:
		//line sql.y:640
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyS[yypt-2].valExpr, Operator: AST_BITOR, Right: yyS[yypt-0].valExpr}
		}
	case 115:
		//line sql.y:644
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyS[yypt-2].valExpr, Operator: AST_BITXOR, Right: yyS[yypt-0].valExpr}
		}
	case 116:
		//line sql.y:648
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyS[yypt-2].valExpr, Operator: AST_PLUS, Right: yyS[yypt-0].valExpr}
		}
	case 117:
		//line sql.y:652
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyS[yypt-2].valExpr, Operator: AST_MINUS, Right: yyS[yypt-0].valExpr}
		}
	case 118:
		//line sql.y:656
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyS[yypt-2].valExpr, Operator: AST_MULT, Right: yyS[yypt-0].valExpr}
		}
	case 119:
		//line sql.y:660
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyS[yypt-2].valExpr, Operator: AST_DIV, Right: yyS[yypt-0].valExpr}
		}
	case 120:
		//line sql.y:664
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyS[yypt-2].valExpr, Operator: AST_MOD, Right: yyS[yypt-0].valExpr}
		}
	case 121:
		//line sql.y:668
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
	case 122:
		//line sql.y:683
		{
			yyVAL.valExpr = &FuncExpr{Name: yyS[yypt-2].bytes}
		}
	case 123:
		//line sql.y:687
		{
			yyVAL.valExpr = &FuncExpr{Name: yyS[yypt-3].bytes, Exprs: yyS[yypt-1].selectExprs}
		}
	case 124:
		//line sql.y:691
		{
			yyVAL.valExpr = &FuncExpr{Name: yyS[yypt-4].bytes, Distinct: true, Exprs: yyS[yypt-1].selectExprs}
		}
	case 125:
		//line sql.y:695
		{
			yyVAL.valExpr = &FuncExpr{Name: yyS[yypt-3].bytes, Exprs: yyS[yypt-1].selectExprs}
		}
	case 126:
		//line sql.y:699
		{
			yyVAL.valExpr = yyS[yypt-0].caseExpr
		}
	case 127:
		//line sql.y:705
		{
			yyVAL.bytes = IF_BYTES
		}
	case 128:
		//line sql.y:709
		{
			yyVAL.bytes = VALUES_BYTES
		}
	case 129:
		//line sql.y:715
		{
			yyVAL.byt = AST_UPLUS
		}
	case 130:
		//line sql.y:719
		{
			yyVAL.byt = AST_UMINUS
		}
	case 131:
		//line sql.y:723
		{
			yyVAL.byt = AST_TILDA
		}
	case 132:
		//line sql.y:729
		{
			yyVAL.caseExpr = &CaseExpr{Expr: yyS[yypt-3].valExpr, Whens: yyS[yypt-2].whens, Else: yyS[yypt-1].valExpr}
		}
	case 133:
		//line sql.y:734
		{
			yyVAL.valExpr = nil
		}
	case 134:
		//line sql.y:738
		{
			yyVAL.valExpr = yyS[yypt-0].valExpr
		}
	case 135:
		//line sql.y:744
		{
			yyVAL.whens = []*When{yyS[yypt-0].when}
		}
	case 136:
		//line sql.y:748
		{
			yyVAL.whens = append(yyS[yypt-1].whens, yyS[yypt-0].when)
		}
	case 137:
		//line sql.y:754
		{
			yyVAL.when = &When{Cond: yyS[yypt-2].boolExpr, Val: yyS[yypt-0].valExpr}
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
			yyVAL.colName = &ColName{Name: yyS[yypt-0].bytes}
		}
	case 141:
		//line sql.y:773
		{
			yyVAL.colName = &ColName{Qualifier: yyS[yypt-2].bytes, Name: yyS[yypt-0].bytes}
		}
	case 142:
		//line sql.y:779
		{
			yyVAL.valExpr = StrVal(yyS[yypt-0].bytes)
		}
	case 143:
		//line sql.y:783
		{
			yyVAL.valExpr = NumVal(yyS[yypt-0].bytes)
		}
	case 144:
		//line sql.y:787
		{
			yyVAL.valExpr = ValArg(yyS[yypt-0].bytes)
		}
	case 145:
		//line sql.y:791
		{
			yyVAL.valExpr = &NullVal{}
		}
	case 146:
		//line sql.y:796
		{
			yyVAL.valExprs = nil
		}
	case 147:
		//line sql.y:800
		{
			yyVAL.valExprs = yyS[yypt-0].valExprs
		}
	case 148:
		//line sql.y:805
		{
			yyVAL.boolExpr = nil
		}
	case 149:
		//line sql.y:809
		{
			yyVAL.boolExpr = yyS[yypt-0].boolExpr
		}
	case 150:
		//line sql.y:814
		{
			yyVAL.orderBy = nil
		}
	case 151:
		//line sql.y:818
		{
			yyVAL.orderBy = yyS[yypt-0].orderBy
		}
	case 152:
		//line sql.y:824
		{
			yyVAL.orderBy = OrderBy{yyS[yypt-0].order}
		}
	case 153:
		//line sql.y:828
		{
			yyVAL.orderBy = append(yyS[yypt-2].orderBy, yyS[yypt-0].order)
		}
	case 154:
		//line sql.y:834
		{
			yyVAL.order = &Order{Expr: yyS[yypt-1].valExpr, Direction: yyS[yypt-0].str}
		}
	case 155:
		//line sql.y:839
		{
			yyVAL.str = AST_ASC
		}
	case 156:
		//line sql.y:843
		{
			yyVAL.str = AST_ASC
		}
	case 157:
		//line sql.y:847
		{
			yyVAL.str = AST_DESC
		}
	case 158:
		//line sql.y:852
		{
			yyVAL.limit = nil
		}
	case 159:
		//line sql.y:856
		{
			yyVAL.limit = &Limit{Rowcount: yyS[yypt-0].valExpr}
		}
	case 160:
		//line sql.y:860
		{
			yyVAL.limit = &Limit{Offset: yyS[yypt-2].valExpr, Rowcount: yyS[yypt-0].valExpr}
		}
	case 161:
		//line sql.y:865
		{
			yyVAL.str = ""
		}
	case 162:
		//line sql.y:869
		{
			yyVAL.str = AST_FOR_UPDATE
		}
	case 163:
		//line sql.y:873
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
	case 164:
		//line sql.y:886
		{
			yyVAL.columns = nil
		}
	case 165:
		//line sql.y:890
		{
			yyVAL.columns = yyS[yypt-1].columns
		}
	case 166:
		//line sql.y:896
		{
			yyVAL.columns = Columns{&NonStarExpr{Expr: yyS[yypt-0].colName}}
		}
	case 167:
		//line sql.y:900
		{
			yyVAL.columns = append(yyVAL.columns, &NonStarExpr{Expr: yyS[yypt-0].colName})
		}
	case 168:
		//line sql.y:905
		{
			yyVAL.updateExprs = nil
		}
	case 169:
		//line sql.y:909
		{
			yyVAL.updateExprs = yyS[yypt-0].updateExprs
		}
	case 170:
		//line sql.y:915
		{
			yyVAL.updateExprs = UpdateExprs{yyS[yypt-0].updateExpr}
		}
	case 171:
		//line sql.y:919
		{
			yyVAL.updateExprs = append(yyS[yypt-2].updateExprs, yyS[yypt-0].updateExpr)
		}
	case 172:
		//line sql.y:925
		{
			yyVAL.updateExpr = &UpdateExpr{Name: yyS[yypt-2].colName, Expr: yyS[yypt-0].valExpr}
		}
	case 173:
		//line sql.y:930
		{
			yyVAL.empty = struct{}{}
		}
	case 174:
		//line sql.y:932
		{
			yyVAL.empty = struct{}{}
		}
	case 175:
		//line sql.y:935
		{
			yyVAL.empty = struct{}{}
		}
	case 176:
		//line sql.y:937
		{
			yyVAL.empty = struct{}{}
		}
	case 177:
		//line sql.y:940
		{
			yyVAL.empty = struct{}{}
		}
	case 178:
		//line sql.y:942
		{
			yyVAL.empty = struct{}{}
		}
	case 179:
		//line sql.y:946
		{
			yyVAL.empty = struct{}{}
		}
	case 180:
		//line sql.y:948
		{
			yyVAL.empty = struct{}{}
		}
	case 181:
		//line sql.y:950
		{
			yyVAL.empty = struct{}{}
		}
	case 182:
		//line sql.y:952
		{
			yyVAL.empty = struct{}{}
		}
	case 183:
		//line sql.y:954
		{
			yyVAL.empty = struct{}{}
		}
	case 184:
		//line sql.y:957
		{
			yyVAL.empty = struct{}{}
		}
	case 185:
		//line sql.y:959
		{
			yyVAL.empty = struct{}{}
		}
	case 186:
		//line sql.y:962
		{
			yyVAL.empty = struct{}{}
		}
	case 187:
		//line sql.y:964
		{
			yyVAL.empty = struct{}{}
		}
	case 188:
		//line sql.y:967
		{
			yyVAL.empty = struct{}{}
		}
	case 189:
		//line sql.y:969
		{
			yyVAL.empty = struct{}{}
		}
	case 190:
		//line sql.y:973
		{
			yyVAL.bytes = bytes.ToLower(yyS[yypt-0].bytes)
		}
	case 191:
		//line sql.y:978
		{
			ForceEOF(yylex)
		}
	}
	goto yystack /* stack new state and value */
}
