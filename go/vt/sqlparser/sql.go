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
	node        *Node
	statement   Statement
	comments    Comments
	byt         byte
	bytes       []byte
	str         string
	distinct    Distinct
	selectExprs SelectExprs
	selectExpr  SelectExpr
	columns     Columns
	colName     *ColName
	tableExprs  TableExprs
	tableExpr   TableExpr
	tableName   *TableName
	where       *Where
	expr        Expr
	boolExpr    BoolExpr
	valExpr     ValExpr
	valExprs    ValExprs
	subquery    *Subquery
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
const NODE_LIST = 57422
const UPLUS = 57423
const UMINUS = 57424
const CASE_WHEN = 57425
const WHEN_LIST = 57426
const FUNCTION = 57427
const NO_LOCK = 57428
const FOR_UPDATE = 57429
const LOCK_IN_SHARE_MODE = 57430
const INDEX_LIST = 57431

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
	"NODE_LIST",
	"UPLUS",
	"UMINUS",
	"CASE_WHEN",
	"WHEN_LIST",
	"FUNCTION",
	"NO_LOCK",
	"FOR_UPDATE",
	"LOCK_IN_SHARE_MODE",
	"INDEX_LIST",
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

const yyNprod = 189
const yyPrivate = 57344

var yyTokenNames []string
var yyStates []string

const yyLast = 598

var yyAct = []int{

	83, 80, 75, 336, 307, 74, 81, 52, 149, 275,
	187, 234, 69, 160, 167, 72, 344, 150, 3, 158,
	344, 123, 124, 70, 118, 91, 22, 23, 24, 25,
	175, 66, 55, 54, 273, 60, 118, 118, 63, 43,
	216, 53, 67, 58, 37, 317, 38, 109, 245, 246,
	247, 248, 249, 316, 250, 251, 32, 62, 34, 39,
	108, 214, 35, 40, 41, 42, 59, 204, 345, 116,
	241, 267, 343, 120, 181, 112, 299, 161, 256, 162,
	147, 151, 217, 146, 148, 152, 272, 122, 265, 263,
	107, 105, 215, 161, 179, 162, 266, 182, 101, 295,
	313, 55, 54, 269, 55, 54, 171, 170, 165, 205,
	315, 111, 156, 159, 237, 12, 13, 14, 15, 136,
	137, 138, 193, 171, 61, 115, 147, 147, 197, 195,
	196, 202, 203, 198, 206, 207, 208, 209, 210, 211,
	212, 213, 192, 191, 16, 169, 178, 180, 177, 161,
	286, 162, 223, 314, 172, 287, 218, 134, 135, 136,
	137, 138, 194, 290, 185, 55, 232, 220, 222, 321,
	168, 147, 224, 289, 236, 288, 238, 226, 227, 225,
	233, 230, 123, 124, 131, 132, 133, 134, 135, 136,
	137, 138, 292, 293, 17, 18, 20, 19, 168, 117,
	255, 242, 284, 258, 259, 103, 257, 285, 216, 322,
	302, 221, 12, 86, 243, 191, 190, 104, 90, 262,
	331, 96, 330, 153, 239, 189, 164, 157, 73, 87,
	88, 89, 155, 65, 264, 61, 271, 78, 154, 224,
	254, 94, 103, 190, 118, 274, 324, 325, 22, 23,
	24, 25, 189, 56, 282, 283, 253, 121, 296, 341,
	77, 49, 294, 298, 92, 93, 71, 279, 100, 191,
	191, 97, 278, 61, 55, 303, 68, 342, 301, 305,
	308, 184, 183, 300, 304, 95, 166, 113, 131, 132,
	133, 134, 135, 136, 137, 138, 110, 106, 219, 50,
	318, 131, 132, 133, 134, 135, 136, 137, 138, 320,
	99, 64, 319, 102, 147, 218, 147, 327, 12, 329,
	48, 328, 326, 261, 333, 308, 309, 334, 347, 173,
	114, 337, 337, 55, 54, 338, 335, 46, 86, 44,
	339, 98, 229, 90, 276, 348, 96, 312, 349, 199,
	350, 200, 201, 56, 87, 88, 89, 26, 277, 235,
	86, 311, 78, 281, 168, 90, 94, 51, 96, 346,
	332, 28, 29, 30, 31, 73, 87, 88, 89, 12,
	27, 174, 33, 240, 78, 77, 176, 36, 94, 92,
	93, 57, 231, 12, 163, 268, 97, 161, 340, 162,
	323, 306, 310, 280, 85, 82, 84, 77, 270, 86,
	95, 92, 93, 71, 90, 79, 228, 96, 97, 125,
	76, 291, 188, 244, 56, 87, 88, 89, 186, 252,
	119, 86, 95, 78, 45, 21, 90, 94, 47, 96,
	11, 10, 9, 8, 7, 6, 56, 87, 88, 89,
	5, 4, 2, 12, 1, 78, 77, 0, 0, 94,
	92, 93, 245, 246, 247, 248, 249, 97, 250, 251,
	0, 0, 0, 0, 90, 0, 0, 96, 77, 0,
	0, 95, 92, 93, 56, 87, 88, 89, 0, 97,
	0, 0, 0, 153, 0, 0, 90, 94, 0, 96,
	0, 0, 0, 95, 0, 0, 56, 87, 88, 89,
	0, 0, 0, 0, 0, 153, 0, 0, 0, 94,
	92, 93, 0, 0, 0, 0, 0, 97, 0, 0,
	0, 0, 126, 130, 128, 129, 0, 0, 0, 0,
	0, 95, 92, 93, 0, 0, 0, 0, 0, 97,
	142, 143, 144, 145, 0, 0, 139, 140, 141, 0,
	0, 297, 0, 95, 131, 132, 133, 134, 135, 136,
	137, 138, 0, 0, 0, 0, 0, 0, 127, 131,
	132, 133, 134, 135, 136, 137, 138, 260, 0, 0,
	131, 132, 133, 134, 135, 136, 137, 138,
}
var yyPact = []int{

	111, -1000, -1000, 199, -1000, -1000, -1000, -1000, -1000, -1000,
	-1000, -1000, -1000, -1000, -1000, -1000, -1000, -31, -45, -28,
	-24, 375, 322, -1000, -1000, -1000, 319, -1000, 291, 264,
	359, 218, -49, -22, 200, -1000, -30, 200, -1000, 276,
	-61, 200, -61, -1000, -1000, 340, -1000, 326, 264, 235,
	22, 264, 152, -1000, 172, -1000, 15, 262, 23, 200,
	-1000, -1000, 261, -1000, -15, 252, 310, 61, 200, 191,
	-1000, -1000, 238, 11, 117, 511, -1000, 411, 389, -1000,
	-1000, -1000, 471, 194, 188, -1000, 183, -1000, -1000, -1000,
	-1000, -1000, -1000, -1000, -1000, -1000, -1000, 318, -1000, 182,
	218, 251, 355, 218, 471, 200, -1000, 309, -64, -1000,
	62, -1000, 247, -1000, -1000, 246, -1000, 181, 340, -1000,
	-1000, 200, 89, 411, 411, 471, 179, 328, 471, 471,
	42, 471, 471, 471, 471, 471, 471, 471, 471, -1000,
	-1000, -1000, -1000, -1000, -1000, -1000, -1000, 511, -44, -13,
	-23, 511, -1000, 449, 193, 340, -1000, 375, 70, -2,
	-1000, 411, 411, 314, 218, 189, -1000, 347, 411, -1000,
	233, -1000, -1000, -1000, 50, 200, -1000, -20, -1000, -1000,
	-1000, -1000, -1000, -1000, -1000, -1000, 161, 408, 221, 208,
	2, -1000, -1000, -1000, -1000, -1000, -1000, 233, -1000, 179,
	471, 471, 233, 522, -1000, 298, 86, 86, 86, 46,
	46, -1000, -1000, -1000, -1000, -1000, 471, -1000, 233, -1000,
	-16, 340, -17, -1000, -1000, 14, -9, -1000, 39, 179,
	199, -19, -1000, 347, 330, 345, 117, 237, -1000, -1000,
	232, -1000, 353, 181, 181, -1000, -1000, 148, 96, 121,
	119, 109, 130, -1000, 227, -6, 223, -1000, 233, 496,
	471, -1000, 233, -1000, -29, -1000, -1000, 411, -1000, 248,
	157, -1000, -1000, 218, 330, -1000, 471, 471, -1000, -1000,
	350, 334, 408, 36, -1000, 99, -1000, 56, -1000, -1000,
	-1000, -1000, -35, -43, -1000, -1000, -1000, 471, 233, -1000,
	-1000, 281, 179, -1000, -1000, 116, 156, -1000, 220, -1000,
	347, 411, 471, 411, -1000, -1000, 178, 176, 233, 364,
	-1000, 471, 471, -1000, -1000, -1000, 330, 117, 155, 117,
	200, 200, 218, 233, -1000, 243, -33, -1000, -37, 152,
	-1000, 363, 307, -1000, 200, -1000, -1000, 200, -1000, 200,
	-1000,
}
var yyPgo = []int{

	0, 454, 452, 17, 451, 450, 445, 444, 443, 442,
	441, 440, 357, 438, 435, 434, 12, 23, 430, 429,
	15, 428, 10, 423, 422, 261, 421, 14, 5, 420,
	419, 416, 415, 6, 2, 8, 408, 406, 25, 405,
	1, 404, 19, 13, 403, 402, 11, 401, 4, 400,
	9, 398, 395, 394, 392, 3, 7, 41, 233, 391,
	387, 386, 383, 382, 381, 0, 47, 380,
}
var yyR1 = []int{

	0, 1, 2, 2, 2, 2, 2, 2, 2, 2,
	2, 3, 3, 4, 5, 6, 7, 8, 8, 8,
	9, 9, 9, 10, 11, 11, 11, 67, 12, 13,
	13, 14, 14, 14, 14, 14, 15, 15, 16, 16,
	17, 17, 17, 20, 20, 18, 18, 18, 21, 21,
	22, 22, 22, 22, 19, 19, 19, 23, 23, 23,
	23, 23, 23, 23, 23, 23, 24, 24, 24, 25,
	25, 26, 26, 26, 27, 27, 28, 28, 28, 28,
	28, 29, 29, 29, 29, 29, 29, 29, 29, 29,
	29, 30, 30, 30, 30, 30, 30, 30, 31, 31,
	36, 36, 33, 33, 38, 35, 35, 34, 34, 34,
	34, 34, 34, 34, 34, 34, 34, 34, 34, 34,
	34, 34, 34, 34, 37, 37, 39, 39, 39, 41,
	41, 42, 42, 43, 43, 40, 40, 32, 32, 32,
	32, 44, 44, 45, 45, 46, 46, 47, 47, 48,
	49, 49, 49, 50, 50, 50, 51, 51, 51, 53,
	53, 54, 54, 55, 55, 52, 52, 56, 56, 57,
	58, 58, 59, 59, 60, 60, 61, 61, 61, 61,
	61, 62, 62, 63, 63, 64, 64, 65, 66,
}
var yyR2 = []int{

	0, 1, 1, 1, 1, 1, 1, 1, 1, 1,
	1, 12, 3, 7, 8, 7, 3, 5, 8, 4,
	6, 7, 4, 5, 4, 5, 5, 0, 2, 0,
	2, 1, 2, 1, 1, 1, 0, 1, 1, 3,
	1, 2, 3, 1, 1, 0, 1, 2, 1, 3,
	3, 3, 3, 5, 0, 1, 2, 1, 1, 2,
	3, 2, 3, 2, 2, 2, 1, 3, 1, 1,
	3, 0, 5, 5, 0, 2, 1, 3, 3, 2,
	3, 3, 3, 4, 3, 4, 5, 6, 3, 4,
	2, 1, 1, 1, 1, 1, 1, 1, 2, 1,
	1, 3, 3, 1, 3, 1, 3, 1, 1, 1,
	3, 3, 3, 3, 3, 3, 3, 3, 2, 3,
	4, 5, 4, 1, 1, 1, 1, 1, 1, 3,
	4, 1, 2, 4, 2, 1, 3, 1, 1, 1,
	1, 0, 3, 0, 2, 0, 3, 1, 3, 2,
	0, 1, 1, 0, 2, 4, 0, 2, 4, 0,
	3, 1, 3, 1, 3, 0, 5, 1, 3, 3,
	0, 2, 0, 3, 0, 1, 1, 1, 1, 1,
	1, 0, 1, 0, 1, 0, 2, 1, 0,
}
var yyChk = []int{

	-1000, -1, -2, -3, -4, -5, -6, -7, -8, -9,
	-10, -11, 4, 5, 6, 7, 33, 83, 84, 86,
	85, -14, 49, 50, 51, 52, -12, -67, -12, -12,
	-12, -12, 87, -63, 89, 93, -60, 89, 91, 87,
	87, 88, 89, -3, 17, -15, 18, -13, 29, -25,
	35, 8, -56, -57, -40, -65, 35, -59, 92, 88,
	-65, 35, 87, -65, 35, -58, 92, -65, -58, -16,
	-17, 73, -20, 35, -28, -34, -29, 67, 44, -32,
	-40, -33, -39, -65, -37, -41, 20, 36, 37, 38,
	25, -38, 71, 72, 48, 92, 28, 78, 15, -25,
	33, 76, -25, 53, 45, 76, 35, 67, -65, -66,
	35, -66, 90, 35, 20, 64, -65, 8, 53, -18,
	-65, 19, 76, 65, 66, -30, 21, 67, 23, 24,
	22, 68, 69, 70, 71, 72, 73, 74, 75, 45,
	46, 47, 39, 40, 41, 42, -28, -34, -28, -35,
	-3, -34, -34, 44, 44, 44, -38, 44, -42, -20,
	-43, 79, 81, -53, 44, -56, 35, -27, 9, -57,
	-34, -65, -66, 20, -64, 94, -61, 86, 84, 32,
	85, 12, 35, 35, 35, -66, -21, -22, -24, 44,
	35, -38, -17, -65, 73, -28, -28, -34, -33, 21,
	23, 24, -34, -34, 25, 67, -34, -34, -34, -34,
	-34, -34, -34, -34, 105, 105, 53, 105, -34, 105,
	-16, 18, -16, 82, -43, -42, -20, -20, -31, 28,
	-3, -54, -40, -27, -46, 12, -28, 64, -65, -66,
	-62, 90, -27, 53, -23, 54, 55, 56, 57, 58,
	60, 61, -19, 35, 19, -22, 76, -33, -34, -34,
	65, 25, -34, 105, -16, 105, 82, 80, -52, 64,
	-36, -33, 105, 53, -46, -50, 14, 13, 35, 35,
	-44, 10, -22, -22, 54, 59, 54, 59, 54, 54,
	54, -26, 62, 63, 35, 105, 35, 65, -34, 105,
	-20, 30, 53, -40, -50, -34, -47, -48, -34, -66,
	-45, 11, 13, 64, 54, 54, 88, 88, -34, 31,
	-33, 53, 53, -49, 26, 27, -46, -28, -35, -28,
	44, 44, 6, -34, -48, -50, -55, -65, -55, -56,
	-51, 16, 34, 105, 53, 105, 6, 21, -65, -65,
	-65,
}
var yyDef = []int{

	0, -2, 1, 2, 3, 4, 5, 6, 7, 8,
	9, 10, 27, 27, 27, 27, 27, 183, 174, 0,
	0, 0, 31, 33, 34, 35, 36, 29, 0, 0,
	0, 0, 172, 0, 0, 184, 0, 0, 175, 0,
	170, 0, 170, 12, 32, 0, 37, 28, 0, 0,
	69, 0, 16, 167, 0, 135, 187, 0, 0, 0,
	188, 187, 0, 188, 0, 0, 0, 0, 0, 0,
	38, 40, 45, 187, 43, 44, 76, 0, 0, 107,
	108, 109, 0, 135, 0, 123, 0, 137, 138, 139,
	140, 103, 126, 127, 128, 124, 125, 0, 30, 159,
	0, 0, 74, 0, 0, 0, 188, 0, 185, 19,
	0, 22, 0, 24, 171, 0, 188, 0, 0, 41,
	46, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 91,
	92, 93, 94, 95, 96, 97, 79, 0, 0, 0,
	0, 105, 118, 0, 0, 0, 90, 0, 0, 0,
	131, 0, 0, 0, 0, 74, 70, 145, 0, 168,
	169, 136, 17, 173, 0, 0, 188, 181, 176, 177,
	178, 179, 180, 23, 25, 26, 74, 48, 54, 0,
	66, 68, 39, 47, 42, 77, 78, 81, 82, 0,
	0, 0, 84, 0, 88, 0, 110, 111, 112, 113,
	114, 115, 116, 117, 80, 102, 0, 104, 105, 119,
	0, 0, 0, 129, 132, 0, 0, 134, 165, 0,
	99, 0, 161, 145, 153, 0, 75, 0, 186, 20,
	0, 182, 141, 0, 0, 57, 58, 0, 0, 0,
	0, 0, 71, 55, 0, 0, 0, 83, 85, 0,
	0, 89, 106, 120, 0, 122, 130, 0, 13, 0,
	98, 100, 160, 0, 153, 15, 0, 0, 188, 21,
	143, 0, 49, 52, 59, 0, 61, 0, 63, 64,
	65, 50, 0, 0, 56, 51, 67, 0, 86, 121,
	133, 0, 0, 162, 14, 154, 146, 147, 150, 18,
	145, 0, 0, 0, 60, 62, 0, 0, 87, 0,
	101, 0, 0, 149, 151, 152, 153, 144, 142, 53,
	0, 0, 0, 155, 148, 156, 0, 163, 0, 166,
	11, 0, 0, 72, 0, 73, 157, 0, 164, 0,
	158,
}
var yyTok1 = []int{

	1, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 75, 68, 3,
	44, 105, 73, 71, 53, 72, 76, 74, 3, 3,
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
	87, 88, 89, 90, 91, 92, 93, 94, 95, 96,
	97, 98, 99, 100, 101, 102, 103, 104,
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
		//line sql.y:125
		{
			SetParseTree(yylex, yyS[yypt-0].statement)
		}
	case 2:
		yyVAL.statement = yyS[yypt-0].statement
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
		//line sql.y:142
		{
			yyVAL.statement = &Select{Comments: yyS[yypt-10].comments, Distinct: yyS[yypt-9].distinct, SelectExprs: yyS[yypt-8].selectExprs, From: yyS[yypt-6].tableExprs, Where: yyS[yypt-5].where, GroupBy: yyS[yypt-4].node, Having: yyS[yypt-3].node, OrderBy: yyS[yypt-2].node, Limit: yyS[yypt-1].node, Lock: yyS[yypt-0].node}
		}
	case 12:
		//line sql.y:146
		{
			yyVAL.statement = &Union{Type: yyS[yypt-1].str, Select1: yyS[yypt-2].statement.(SelectStatement), Select2: yyS[yypt-0].statement.(SelectStatement)}
		}
	case 13:
		//line sql.y:152
		{
			yyVAL.statement = &Insert{Comments: yyS[yypt-5].comments, Table: yyS[yypt-3].tableName, Columns: yyS[yypt-2].columns, Values: yyS[yypt-1].sqlNode, OnDup: yyS[yypt-0].node}
		}
	case 14:
		//line sql.y:158
		{
			yyVAL.statement = &Update{Comments: yyS[yypt-6].comments, Table: yyS[yypt-5].tableName, List: yyS[yypt-3].node, Where: yyS[yypt-2].where, OrderBy: yyS[yypt-1].node, Limit: yyS[yypt-0].node}
		}
	case 15:
		//line sql.y:164
		{
			yyVAL.statement = &Delete{Comments: yyS[yypt-5].comments, Table: yyS[yypt-3].tableName, Where: yyS[yypt-2].where, OrderBy: yyS[yypt-1].node, Limit: yyS[yypt-0].node}
		}
	case 16:
		//line sql.y:170
		{
			yyVAL.statement = &Set{Comments: yyS[yypt-1].comments, Updates: yyS[yypt-0].node}
		}
	case 17:
		//line sql.y:176
		{
			yyVAL.statement = &DDLSimple{Action: CREATE, Table: yyS[yypt-1].node}
		}
	case 18:
		//line sql.y:180
		{
			// Change this to an alter statement
			yyVAL.statement = &DDLSimple{Action: ALTER, Table: yyS[yypt-1].node}
		}
	case 19:
		//line sql.y:185
		{
			yyVAL.statement = &DDLSimple{Action: CREATE, Table: yyS[yypt-1].node}
		}
	case 20:
		//line sql.y:191
		{
			yyVAL.statement = &DDLSimple{Action: ALTER, Table: yyS[yypt-2].node}
		}
	case 21:
		//line sql.y:195
		{
			// Change this to a rename statement
			yyVAL.statement = &Rename{OldName: yyS[yypt-3].node, NewName: yyS[yypt-0].node}
		}
	case 22:
		//line sql.y:200
		{
			yyVAL.statement = &DDLSimple{Action: ALTER, Table: yyS[yypt-1].node}
		}
	case 23:
		//line sql.y:206
		{
			yyVAL.statement = &Rename{OldName: yyS[yypt-2].node, NewName: yyS[yypt-0].node}
		}
	case 24:
		//line sql.y:212
		{
			yyVAL.statement = &DDLSimple{Action: DROP, Table: yyS[yypt-0].node}
		}
	case 25:
		//line sql.y:216
		{
			// Change this to an alter statement
			yyVAL.statement = &DDLSimple{Action: ALTER, Table: yyS[yypt-0].node}
		}
	case 26:
		//line sql.y:221
		{
			yyVAL.statement = &DDLSimple{Action: DROP, Table: yyS[yypt-1].node}
		}
	case 27:
		//line sql.y:226
		{
			SetAllowComments(yylex, true)
		}
	case 28:
		//line sql.y:230
		{
			yyVAL.comments = yyS[yypt-0].comments
			SetAllowComments(yylex, false)
		}
	case 29:
		//line sql.y:236
		{
			yyVAL.comments = nil
		}
	case 30:
		//line sql.y:240
		{
			yyVAL.comments = append(yyVAL.comments, Comment(yyS[yypt-0].node.Value))
		}
	case 31:
		//line sql.y:246
		{
			yyVAL.str = "union"
		}
	case 32:
		//line sql.y:250
		{
			yyVAL.str = "union all"
		}
	case 33:
		//line sql.y:254
		{
			yyVAL.str = "minus"
		}
	case 34:
		//line sql.y:258
		{
			yyVAL.str = "except"
		}
	case 35:
		//line sql.y:262
		{
			yyVAL.str = "intersect"
		}
	case 36:
		//line sql.y:267
		{
			yyVAL.distinct = Distinct(false)
		}
	case 37:
		//line sql.y:271
		{
			yyVAL.distinct = Distinct(true)
		}
	case 38:
		//line sql.y:277
		{
			yyVAL.selectExprs = SelectExprs{yyS[yypt-0].selectExpr}
		}
	case 39:
		//line sql.y:281
		{
			yyVAL.selectExprs = append(yyVAL.selectExprs, yyS[yypt-0].selectExpr)
		}
	case 40:
		//line sql.y:287
		{
			yyVAL.selectExpr = &StarExpr{}
		}
	case 41:
		//line sql.y:291
		{
			yyVAL.selectExpr = &NonStarExpr{Expr: yyS[yypt-1].expr, As: yyS[yypt-0].bytes}
		}
	case 42:
		//line sql.y:295
		{
			yyVAL.selectExpr = &StarExpr{TableName: yyS[yypt-2].node.Value}
		}
	case 43:
		//line sql.y:301
		{
			yyVAL.expr = yyS[yypt-0].boolExpr
		}
	case 44:
		//line sql.y:305
		{
			yyVAL.expr = yyS[yypt-0].valExpr
		}
	case 45:
		//line sql.y:310
		{
			yyVAL.bytes = nil
		}
	case 46:
		//line sql.y:314
		{
			yyVAL.bytes = yyS[yypt-0].node.Value
		}
	case 47:
		//line sql.y:318
		{
			yyVAL.bytes = yyS[yypt-0].node.Value
		}
	case 48:
		//line sql.y:324
		{
			yyVAL.tableExprs = TableExprs{yyS[yypt-0].tableExpr}
		}
	case 49:
		//line sql.y:328
		{
			yyVAL.tableExprs = append(yyVAL.tableExprs, yyS[yypt-0].tableExpr)
		}
	case 50:
		//line sql.y:334
		{
			yyVAL.tableExpr = &AliasedTableExpr{Expr: yyS[yypt-2].sqlNode, As: yyS[yypt-1].bytes, Hint: yyS[yypt-0].node}
		}
	case 51:
		//line sql.y:338
		{
			yyVAL.tableExpr = &ParenTableExpr{Expr: yyS[yypt-1].tableExpr}
		}
	case 52:
		//line sql.y:342
		{
			yyVAL.tableExpr = &JoinTableExpr{LeftExpr: yyS[yypt-2].tableExpr, Join: yyS[yypt-1].str, RightExpr: yyS[yypt-0].tableExpr}
		}
	case 53:
		//line sql.y:346
		{
			yyVAL.tableExpr = &JoinTableExpr{LeftExpr: yyS[yypt-4].tableExpr, Join: yyS[yypt-3].str, RightExpr: yyS[yypt-2].tableExpr, On: yyS[yypt-0].boolExpr}
		}
	case 54:
		//line sql.y:351
		{
			yyVAL.bytes = nil
		}
	case 55:
		//line sql.y:355
		{
			yyVAL.bytes = yyS[yypt-0].node.Value
		}
	case 56:
		//line sql.y:359
		{
			yyVAL.bytes = yyS[yypt-0].node.Value
		}
	case 57:
		//line sql.y:365
		{
			yyVAL.str = "join"
		}
	case 58:
		//line sql.y:369
		{
			yyVAL.str = "straight_join"
		}
	case 59:
		//line sql.y:373
		{
			yyVAL.str = "left join"
		}
	case 60:
		//line sql.y:377
		{
			yyVAL.str = "left join"
		}
	case 61:
		//line sql.y:381
		{
			yyVAL.str = "right join"
		}
	case 62:
		//line sql.y:385
		{
			yyVAL.str = "right join"
		}
	case 63:
		//line sql.y:389
		{
			yyVAL.str = "join"
		}
	case 64:
		//line sql.y:393
		{
			yyVAL.str = "cross join"
		}
	case 65:
		//line sql.y:397
		{
			yyVAL.str = "natural join"
		}
	case 66:
		//line sql.y:403
		{
			yyVAL.sqlNode = &TableName{Name: yyS[yypt-0].node.Value}
		}
	case 67:
		//line sql.y:407
		{
			yyVAL.sqlNode = &TableName{Qualifier: yyS[yypt-2].node.Value, Name: yyS[yypt-0].node.Value}
		}
	case 68:
		//line sql.y:411
		{
			yyVAL.sqlNode = yyS[yypt-0].subquery
		}
	case 69:
		//line sql.y:417
		{
			yyVAL.tableName = &TableName{Name: yyS[yypt-0].node.Value}
		}
	case 70:
		//line sql.y:421
		{
			yyVAL.tableName = &TableName{Qualifier: yyS[yypt-2].node.Value, Name: yyS[yypt-0].node.Value}
		}
	case 71:
		//line sql.y:426
		{
			yyVAL.node = nil
		}
	case 72:
		//line sql.y:430
		{
			yyVAL.node.Push(yyS[yypt-1].node)
		}
	case 73:
		//line sql.y:434
		{
			yyVAL.node.Push(yyS[yypt-1].node)
		}
	case 74:
		//line sql.y:439
		{
			yyVAL.where = nil
		}
	case 75:
		//line sql.y:443
		{
			yyVAL.where = &Where{Expr: yyS[yypt-0].boolExpr}
		}
	case 76:
		yyVAL.boolExpr = yyS[yypt-0].boolExpr
	case 77:
		//line sql.y:450
		{
			yyVAL.boolExpr = &AndExpr{Left: yyS[yypt-2].boolExpr, Right: yyS[yypt-0].boolExpr}
		}
	case 78:
		//line sql.y:454
		{
			yyVAL.boolExpr = &OrExpr{Left: yyS[yypt-2].boolExpr, Right: yyS[yypt-0].boolExpr}
		}
	case 79:
		//line sql.y:458
		{
			yyVAL.boolExpr = &NotExpr{Expr: yyS[yypt-0].boolExpr}
		}
	case 80:
		//line sql.y:462
		{
			yyVAL.boolExpr = &ParenBoolExpr{Expr: yyS[yypt-1].boolExpr}
		}
	case 81:
		//line sql.y:468
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyS[yypt-2].valExpr, Operator: yyS[yypt-1].str, Right: yyS[yypt-0].valExpr}
		}
	case 82:
		//line sql.y:472
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyS[yypt-2].valExpr, Operator: "in", Right: yyS[yypt-0].valExpr}
		}
	case 83:
		//line sql.y:476
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyS[yypt-3].valExpr, Operator: "not in", Right: yyS[yypt-0].valExpr}
		}
	case 84:
		//line sql.y:480
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyS[yypt-2].valExpr, Operator: "like", Right: yyS[yypt-0].valExpr}
		}
	case 85:
		//line sql.y:484
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyS[yypt-3].valExpr, Operator: "not like", Right: yyS[yypt-0].valExpr}
		}
	case 86:
		//line sql.y:488
		{
			yyVAL.boolExpr = &RangeCond{Left: yyS[yypt-4].valExpr, Operator: "between", From: yyS[yypt-2].valExpr, To: yyS[yypt-0].valExpr}
		}
	case 87:
		//line sql.y:492
		{
			yyVAL.boolExpr = &RangeCond{Left: yyS[yypt-5].valExpr, Operator: "not between", From: yyS[yypt-2].valExpr, To: yyS[yypt-0].valExpr}
		}
	case 88:
		//line sql.y:496
		{
			yyVAL.boolExpr = &NullCheck{Operator: "is null", Expr: yyS[yypt-2].valExpr}
		}
	case 89:
		//line sql.y:500
		{
			yyVAL.boolExpr = &NullCheck{Operator: "is not null", Expr: yyS[yypt-3].valExpr}
		}
	case 90:
		//line sql.y:504
		{
			yyVAL.boolExpr = &ExistsExpr{Subquery: yyS[yypt-0].subquery}
		}
	case 91:
		//line sql.y:510
		{
			yyVAL.str = "="
		}
	case 92:
		//line sql.y:514
		{
			yyVAL.str = "<"
		}
	case 93:
		//line sql.y:518
		{
			yyVAL.str = ">"
		}
	case 94:
		//line sql.y:522
		{
			yyVAL.str = "<="
		}
	case 95:
		//line sql.y:526
		{
			yyVAL.str = ">="
		}
	case 96:
		//line sql.y:530
		{
			yyVAL.str = string(yyS[yypt-0].node.Value)
		}
	case 97:
		//line sql.y:534
		{
			yyVAL.str = "<=>"
		}
	case 98:
		//line sql.y:540
		{
			yyVAL.sqlNode = yyS[yypt-1].node.Push(yyS[yypt-0].node)
		}
	case 99:
		//line sql.y:544
		{
			yyVAL.sqlNode = yyS[yypt-0].statement
		}
	case 100:
		//line sql.y:550
		{
			yyVAL.node = NewSimpleParseNode(NODE_LIST, "node_list")
			yyVAL.node.Push(yyS[yypt-0].valExpr)
		}
	case 101:
		//line sql.y:555
		{
			yyVAL.node.Push(yyS[yypt-0].valExpr)
		}
	case 102:
		//line sql.y:561
		{
			yyVAL.valExpr = ValueTuple(yyS[yypt-1].valExprs)
		}
	case 103:
		//line sql.y:565
		{
			yyVAL.valExpr = yyS[yypt-0].subquery
		}
	case 104:
		//line sql.y:571
		{
			yyVAL.subquery = &Subquery{yyS[yypt-1].statement.(SelectStatement)}
		}
	case 105:
		//line sql.y:577
		{
			yyVAL.valExprs = ValExprs{yyS[yypt-0].valExpr}
		}
	case 106:
		//line sql.y:581
		{
			yyVAL.valExprs = append(yyS[yypt-2].valExprs, yyS[yypt-0].valExpr)
		}
	case 107:
		//line sql.y:587
		{
			yyVAL.valExpr = yyS[yypt-0].valExpr
		}
	case 108:
		//line sql.y:591
		{
			yyVAL.valExpr = yyS[yypt-0].colName
		}
	case 109:
		//line sql.y:595
		{
			yyVAL.valExpr = yyS[yypt-0].valExpr
		}
	case 110:
		//line sql.y:599
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyS[yypt-2].valExpr, Operator: '&', Right: yyS[yypt-0].valExpr}
		}
	case 111:
		//line sql.y:603
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyS[yypt-2].valExpr, Operator: '|', Right: yyS[yypt-0].valExpr}
		}
	case 112:
		//line sql.y:607
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyS[yypt-2].valExpr, Operator: '^', Right: yyS[yypt-0].valExpr}
		}
	case 113:
		//line sql.y:611
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyS[yypt-2].valExpr, Operator: '+', Right: yyS[yypt-0].valExpr}
		}
	case 114:
		//line sql.y:615
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyS[yypt-2].valExpr, Operator: '-', Right: yyS[yypt-0].valExpr}
		}
	case 115:
		//line sql.y:619
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyS[yypt-2].valExpr, Operator: '*', Right: yyS[yypt-0].valExpr}
		}
	case 116:
		//line sql.y:623
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyS[yypt-2].valExpr, Operator: '/', Right: yyS[yypt-0].valExpr}
		}
	case 117:
		//line sql.y:627
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyS[yypt-2].valExpr, Operator: '%', Right: yyS[yypt-0].valExpr}
		}
	case 118:
		//line sql.y:631
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
	case 119:
		//line sql.y:646
		{
			yyVAL.valExpr = &FuncExpr{Name: yyS[yypt-2].node.Value}
		}
	case 120:
		//line sql.y:650
		{
			yyVAL.valExpr = &FuncExpr{Name: yyS[yypt-3].node.Value, Exprs: yyS[yypt-1].selectExprs}
		}
	case 121:
		//line sql.y:654
		{
			yyVAL.valExpr = &FuncExpr{Name: yyS[yypt-4].node.Value, Distinct: true, Exprs: yyS[yypt-1].selectExprs}
		}
	case 122:
		//line sql.y:658
		{
			yyVAL.valExpr = &FuncExpr{Name: yyS[yypt-3].node.Value, Exprs: yyS[yypt-1].selectExprs}
		}
	case 123:
		//line sql.y:662
		{
			c := CaseExpr(*yyS[yypt-0].node)
			yyVAL.valExpr = &c
		}
	case 124:
		yyVAL.node = yyS[yypt-0].node
	case 125:
		yyVAL.node = yyS[yypt-0].node
	case 126:
		//line sql.y:673
		{
			yyVAL.byt = '+'
		}
	case 127:
		//line sql.y:677
		{
			yyVAL.byt = '-'
		}
	case 128:
		//line sql.y:681
		{
			yyVAL.byt = '~'
		}
	case 129:
		//line sql.y:687
		{
			yyVAL.node = NewSimpleParseNode(CASE_WHEN, "case")
			yyVAL.node.Push(yyS[yypt-1].node)
		}
	case 130:
		//line sql.y:692
		{
			yyVAL.node.PushTwo(yyS[yypt-2].expr, yyS[yypt-1].node)
		}
	case 131:
		//line sql.y:698
		{
			yyVAL.node = NewSimpleParseNode(WHEN_LIST, "when_list")
			yyVAL.node.Push(yyS[yypt-0].node)
		}
	case 132:
		//line sql.y:703
		{
			yyVAL.node.Push(yyS[yypt-0].node)
		}
	case 133:
		//line sql.y:709
		{
			yyVAL.node.PushTwo(yyS[yypt-2].expr, yyS[yypt-0].expr)
		}
	case 134:
		//line sql.y:713
		{
			yyVAL.node.Push(yyS[yypt-0].expr)
		}
	case 135:
		//line sql.y:719
		{
			yyVAL.colName = &ColName{Name: yyS[yypt-0].node.Value}
		}
	case 136:
		//line sql.y:723
		{
			yyVAL.colName = &ColName{Qualifier: yyS[yypt-2].node.Value, Name: yyS[yypt-0].node.Value}
		}
	case 137:
		//line sql.y:729
		{
			yyVAL.valExpr = StringValue(yyS[yypt-0].node.Value)
		}
	case 138:
		//line sql.y:733
		{
			yyVAL.valExpr = NumValue(yyS[yypt-0].node.Value)
		}
	case 139:
		//line sql.y:737
		{
			yyVAL.valExpr = ValueArg(yyS[yypt-0].node.Value)
		}
	case 140:
		//line sql.y:741
		{
			yyVAL.valExpr = &NullValue{}
		}
	case 141:
		//line sql.y:746
		{
			yyVAL.node = NewSimpleParseNode(GROUP, "group")
		}
	case 142:
		//line sql.y:750
		{
			yyVAL.node = yyS[yypt-2].node.Push(yyS[yypt-0].valExprs)
		}
	case 143:
		//line sql.y:755
		{
			yyVAL.node = NewSimpleParseNode(HAVING, "having")
		}
	case 144:
		//line sql.y:759
		{
			yyVAL.node = yyS[yypt-1].node.Push(yyS[yypt-0].boolExpr)
		}
	case 145:
		//line sql.y:764
		{
			yyVAL.node = NewSimpleParseNode(ORDER, "order")
		}
	case 146:
		//line sql.y:768
		{
			yyVAL.node = yyS[yypt-2].node.Push(yyS[yypt-0].node)
		}
	case 147:
		//line sql.y:774
		{
			yyVAL.node = NewSimpleParseNode(NODE_LIST, "node_list")
			yyVAL.node.Push(yyS[yypt-0].node)
		}
	case 148:
		//line sql.y:779
		{
			yyVAL.node = yyS[yypt-2].node.Push(yyS[yypt-0].node)
		}
	case 149:
		//line sql.y:785
		{
			yyVAL.node = yyS[yypt-0].node.Push(yyS[yypt-1].valExpr)
		}
	case 150:
		//line sql.y:790
		{
			yyVAL.node = NewSimpleParseNode(ASC, "asc")
		}
	case 151:
		yyVAL.node = yyS[yypt-0].node
	case 152:
		yyVAL.node = yyS[yypt-0].node
	case 153:
		//line sql.y:797
		{
			yyVAL.node = NewSimpleParseNode(LIMIT, "limit")
		}
	case 154:
		//line sql.y:801
		{
			yyVAL.node = yyS[yypt-1].node.Push(yyS[yypt-0].valExpr)
		}
	case 155:
		//line sql.y:805
		{
			yyVAL.node = yyS[yypt-3].node.PushTwo(yyS[yypt-2].valExpr, yyS[yypt-0].valExpr)
		}
	case 156:
		//line sql.y:810
		{
			yyVAL.node = NewSimpleParseNode(NO_LOCK, "")
		}
	case 157:
		//line sql.y:814
		{
			yyVAL.node = NewSimpleParseNode(FOR_UPDATE, " for update")
		}
	case 158:
		//line sql.y:818
		{
			if !bytes.Equal(yyS[yypt-1].node.Value, SHARE) {
				yylex.Error("expecting share")
				return 1
			}
			if !bytes.Equal(yyS[yypt-0].node.Value, MODE) {
				yylex.Error("expecting mode")
				return 1
			}
			yyVAL.node = NewSimpleParseNode(LOCK_IN_SHARE_MODE, " lock in share mode")
		}
	case 159:
		//line sql.y:831
		{
			yyVAL.columns = nil
		}
	case 160:
		//line sql.y:835
		{
			yyVAL.columns = yyS[yypt-1].columns
		}
	case 161:
		//line sql.y:841
		{
			yyVAL.columns = Columns{&NonStarExpr{Expr: yyS[yypt-0].colName}}
		}
	case 162:
		//line sql.y:845
		{
			yyVAL.columns = append(yyVAL.columns, &NonStarExpr{Expr: yyS[yypt-0].colName})
		}
	case 163:
		//line sql.y:851
		{
			yyVAL.node = NewSimpleParseNode(INDEX_LIST, "")
			yyVAL.node.Push(yyS[yypt-0].node)
		}
	case 164:
		//line sql.y:856
		{
			yyVAL.node = yyS[yypt-2].node.Push(yyS[yypt-0].node)
		}
	case 165:
		//line sql.y:861
		{
			yyVAL.node = NewSimpleParseNode(DUPLICATE, "duplicate")
		}
	case 166:
		//line sql.y:865
		{
			yyVAL.node = yyS[yypt-3].node.Push(yyS[yypt-0].node)
		}
	case 167:
		//line sql.y:871
		{
			yyVAL.node = NewSimpleParseNode(NODE_LIST, "node_list")
			yyVAL.node.Push(yyS[yypt-0].node)
		}
	case 168:
		//line sql.y:876
		{
			yyVAL.node = yyS[yypt-2].node.Push(yyS[yypt-0].node)
		}
	case 169:
		//line sql.y:882
		{
			yyVAL.node = yyS[yypt-1].node.PushTwo(yyS[yypt-2].colName, yyS[yypt-0].valExpr)
		}
	case 170:
		//line sql.y:887
		{
			yyVAL.node = nil
		}
	case 171:
		yyVAL.node = yyS[yypt-0].node
	case 172:
		//line sql.y:891
		{
			yyVAL.node = nil
		}
	case 173:
		yyVAL.node = yyS[yypt-0].node
	case 174:
		//line sql.y:895
		{
			yyVAL.node = nil
		}
	case 175:
		yyVAL.node = yyS[yypt-0].node
	case 176:
		yyVAL.node = yyS[yypt-0].node
	case 177:
		yyVAL.node = yyS[yypt-0].node
	case 178:
		yyVAL.node = yyS[yypt-0].node
	case 179:
		yyVAL.node = yyS[yypt-0].node
	case 180:
		yyVAL.node = yyS[yypt-0].node
	case 181:
		//line sql.y:906
		{
			yyVAL.node = nil
		}
	case 182:
		yyVAL.node = yyS[yypt-0].node
	case 183:
		//line sql.y:910
		{
			yyVAL.node = nil
		}
	case 184:
		yyVAL.node = yyS[yypt-0].node
	case 185:
		//line sql.y:914
		{
			yyVAL.node = nil
		}
	case 186:
		yyVAL.node = yyS[yypt-0].node
	case 187:
		//line sql.y:919
		{
			yyVAL.node.LowerCase()
		}
	case 188:
		//line sql.y:924
		{
			ForceEOF(yylex)
		}
	}
	goto yystack /* stack new state and value */
}
