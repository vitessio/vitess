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
	bytes       []byte
	str         string
	distinct    Distinct
	selectExprs SelectExprs
	selectExpr  SelectExpr
	columns     Columns
	tableExprs  TableExprs
	tableExpr   TableExpr
	where       *Where
	expr        Expr
	boolExpr    BoolExpr
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
const NOT_IN = 57431
const NOT_LIKE = 57432
const NOT_BETWEEN = 57433
const IS_NULL = 57434
const IS_NOT_NULL = 57435
const UNION_ALL = 57436
const INDEX_LIST = 57437

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
	"NOT_IN",
	"NOT_LIKE",
	"NOT_BETWEEN",
	"IS_NULL",
	"IS_NOT_NULL",
	"UNION_ALL",
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

const yyLast = 601

var yyAct = []int{

	82, 75, 52, 312, 341, 148, 232, 80, 194, 184,
	277, 107, 69, 155, 164, 72, 157, 121, 122, 349,
	349, 147, 3, 70, 243, 244, 245, 246, 247, 74,
	248, 249, 55, 116, 213, 60, 275, 116, 63, 54,
	116, 53, 67, 43, 172, 213, 66, 58, 22, 23,
	24, 25, 22, 23, 24, 25, 32, 37, 34, 38,
	106, 239, 35, 211, 110, 22, 23, 24, 25, 114,
	322, 269, 62, 118, 321, 109, 59, 350, 348, 145,
	149, 297, 39, 150, 22, 23, 24, 25, 40, 41,
	42, 304, 300, 255, 274, 266, 120, 103, 264, 55,
	99, 162, 55, 214, 168, 105, 54, 144, 146, 54,
	301, 156, 329, 330, 298, 158, 169, 159, 167, 178,
	189, 168, 201, 145, 145, 193, 182, 267, 199, 200,
	61, 203, 204, 205, 206, 207, 208, 209, 210, 176,
	188, 318, 179, 166, 271, 158, 212, 159, 268, 121,
	122, 191, 192, 215, 129, 130, 131, 132, 133, 134,
	135, 136, 55, 320, 202, 217, 219, 145, 190, 230,
	223, 319, 222, 236, 224, 225, 220, 231, 158, 235,
	159, 221, 228, 294, 295, 237, 113, 12, 13, 14,
	15, 175, 177, 174, 292, 234, 253, 215, 240, 259,
	260, 256, 288, 291, 218, 258, 85, 289, 254, 290,
	165, 89, 165, 101, 94, 263, 16, 257, 134, 135,
	136, 73, 86, 87, 88, 213, 326, 98, 286, 327,
	78, 265, 115, 287, 92, 307, 273, 102, 276, 336,
	222, 129, 130, 131, 132, 133, 134, 135, 136, 49,
	335, 284, 285, 77, 241, 12, 101, 90, 91, 71,
	195, 161, 154, 303, 95, 153, 17, 18, 20, 19,
	132, 133, 134, 135, 136, 187, 55, 116, 93, 152,
	310, 313, 65, 308, 186, 305, 187, 309, 22, 23,
	24, 25, 314, 61, 56, 186, 299, 216, 97, 252,
	296, 100, 302, 346, 323, 129, 130, 131, 132, 133,
	134, 135, 136, 281, 119, 251, 325, 280, 145, 215,
	145, 347, 331, 333, 181, 68, 180, 163, 338, 313,
	61, 339, 111, 108, 104, 50, 342, 342, 55, 64,
	344, 343, 340, 324, 85, 54, 332, 306, 334, 89,
	353, 12, 94, 354, 48, 355, 262, 352, 170, 56,
	86, 87, 88, 112, 278, 44, 85, 46, 78, 96,
	317, 89, 92, 279, 94, 227, 196, 233, 197, 198,
	316, 73, 86, 87, 88, 283, 165, 51, 351, 337,
	78, 77, 26, 12, 92, 90, 91, 27, 171, 12,
	33, 238, 95, 158, 173, 159, 28, 29, 30, 31,
	36, 57, 229, 77, 160, 85, 93, 90, 91, 71,
	89, 270, 345, 94, 95, 328, 311, 315, 282, 79,
	56, 86, 87, 88, 84, 81, 83, 85, 93, 78,
	272, 226, 89, 92, 123, 94, 76, 293, 185, 242,
	183, 250, 56, 86, 87, 88, 117, 45, 21, 12,
	47, 78, 77, 11, 10, 92, 90, 91, 243, 244,
	245, 246, 247, 95, 248, 249, 9, 8, 7, 6,
	89, 5, 4, 94, 77, 2, 1, 93, 90, 91,
	56, 86, 87, 88, 0, 95, 0, 0, 0, 151,
	0, 0, 89, 92, 0, 94, 0, 0, 0, 93,
	0, 0, 56, 86, 87, 88, 0, 0, 0, 0,
	0, 151, 0, 0, 0, 92, 90, 91, 0, 0,
	0, 0, 0, 95, 0, 0, 0, 0, 124, 128,
	126, 127, 0, 0, 0, 0, 0, 93, 90, 91,
	0, 0, 0, 0, 0, 95, 140, 141, 142, 143,
	0, 0, 137, 138, 139, 0, 0, 261, 0, 93,
	129, 130, 131, 132, 133, 134, 135, 136, 0, 0,
	0, 0, 0, 0, 125, 129, 130, 131, 132, 133,
	134, 135, 136, 129, 130, 131, 132, 133, 134, 135,
	136,
}
var yyPact = []int{

	183, -1000, -1000, 239, -1000, -1000, -1000, -1000, -1000, -1000,
	-1000, -1000, -1000, -1000, -1000, -1000, -1000, -31, -32, -5,
	1, 389, 348, -1000, -1000, -1000, 349, -1000, 325, 300,
	379, 259, -45, -12, 258, -1000, -15, 258, -1000, 304,
	-46, 258, -46, -1000, -1000, 346, -1000, 354, 300, 194,
	24, 300, 160, -1000, 192, -1000, 21, 299, 38, 258,
	-1000, -1000, 298, -1000, -26, 297, 343, 122, 258, 224,
	-1000, -1000, 295, 20, 84, 517, -1000, 417, 395, -1000,
	-1000, 477, 235, 221, -1000, 218, -1000, -1000, -1000, -1000,
	-1000, -1000, -1000, -1000, -1000, 324, -1000, 217, 259, 292,
	377, 259, 417, 258, -1000, 338, -50, -1000, 107, -1000,
	291, -1000, -1000, 289, -1000, 240, 346, -1000, -1000, 258,
	95, 417, 417, 477, 216, 355, 477, 477, 97, 477,
	477, 477, 477, 477, 477, 477, 477, -1000, -1000, -1000,
	-1000, -1000, -1000, -1000, -1000, 517, -48, 35, -8, 517,
	-1000, 455, 186, 346, 389, 99, 36, -1000, 417, 417,
	347, 259, 203, -1000, 365, 417, -1000, -1000, -1000, -1000,
	-1000, 115, 258, -1000, -29, -1000, -1000, -1000, -1000, -1000,
	-1000, -1000, -1000, 201, 414, 280, 251, 17, -1000, -1000,
	-1000, -1000, -1000, 525, -1000, 455, 216, 477, 477, 525,
	502, -1000, 331, 199, 199, 199, 145, 145, -1000, -1000,
	-1000, -1000, -1000, 477, -1000, 525, -1000, -13, 346, -16,
	16, -1000, -1000, 66, -9, -1000, 80, 216, 239, -17,
	-1000, 365, 350, 360, 84, 282, -1000, -1000, 278, -1000,
	375, 240, 240, -1000, -1000, 174, 148, 155, 149, 140,
	121, -1000, 265, -30, 3, 261, -19, -1, -1000, 525,
	237, 477, -1000, 525, -1000, -20, -1000, -1000, -1000, 417,
	-1000, 317, 182, -1000, -1000, 259, 350, -1000, 477, 477,
	-1000, -1000, 369, 357, 414, 77, -1000, 117, -1000, 109,
	-1000, -1000, -1000, -1000, -14, -18, -1000, -1000, -1000, -1000,
	-1000, -1000, 477, 525, -1000, -1000, 312, 216, -1000, -1000,
	173, 176, -1000, 86, -1000, 365, 417, 477, 417, -1000,
	-1000, 206, 195, 525, 383, -1000, 477, 477, -1000, -1000,
	-1000, 350, 84, 172, 84, 258, 258, 259, 525, -1000,
	287, -33, -1000, -34, 160, -1000, 382, 336, -1000, 258,
	-1000, -1000, 258, -1000, 258, -1000,
}
var yyPgo = []int{

	0, 486, 485, 21, 482, 481, 479, 478, 477, 476,
	464, 463, 392, 460, 458, 457, 12, 23, 456, 451,
	15, 450, 9, 449, 448, 249, 447, 14, 29, 446,
	444, 441, 440, 8, 5, 436, 1, 435, 434, 13,
	16, 7, 429, 428, 427, 6, 426, 3, 425, 10,
	422, 421, 414, 412, 4, 2, 41, 282, 411, 410,
	404, 401, 400, 398, 0, 11, 397,
}
var yyR1 = []int{

	0, 1, 2, 2, 2, 2, 2, 2, 2, 2,
	2, 3, 3, 4, 5, 6, 7, 8, 8, 8,
	9, 9, 9, 10, 11, 11, 11, 66, 12, 13,
	13, 14, 14, 14, 14, 14, 15, 15, 16, 16,
	17, 17, 17, 20, 20, 18, 18, 18, 21, 21,
	22, 22, 22, 22, 19, 19, 19, 23, 23, 23,
	23, 23, 23, 23, 23, 23, 24, 24, 24, 25,
	25, 26, 26, 26, 27, 27, 28, 28, 28, 28,
	28, 29, 29, 29, 29, 29, 29, 29, 29, 29,
	29, 30, 30, 30, 30, 30, 30, 30, 31, 31,
	32, 32, 33, 33, 34, 34, 36, 36, 36, 36,
	36, 36, 36, 36, 36, 36, 36, 36, 36, 36,
	36, 36, 36, 36, 35, 35, 37, 37, 37, 38,
	38, 39, 39, 40, 40, 41, 41, 42, 42, 42,
	42, 43, 43, 44, 44, 45, 45, 46, 46, 47,
	48, 48, 48, 49, 49, 49, 50, 50, 50, 52,
	52, 53, 53, 54, 54, 51, 51, 55, 55, 56,
	57, 57, 58, 58, 59, 59, 60, 60, 60, 60,
	60, 61, 61, 62, 62, 63, 63, 64, 65,
}
var yyR2 = []int{

	0, 1, 1, 1, 1, 1, 1, 1, 1, 1,
	1, 12, 3, 7, 8, 7, 3, 5, 8, 4,
	6, 7, 4, 5, 4, 5, 5, 0, 2, 0,
	2, 1, 2, 1, 1, 1, 0, 1, 1, 3,
	1, 2, 3, 1, 1, 0, 1, 2, 1, 3,
	3, 3, 3, 5, 0, 1, 2, 1, 1, 2,
	3, 2, 3, 2, 2, 2, 1, 3, 3, 1,
	3, 0, 5, 5, 0, 2, 1, 3, 3, 2,
	3, 3, 3, 4, 3, 4, 5, 6, 3, 4,
	4, 1, 1, 1, 1, 1, 1, 1, 2, 1,
	1, 3, 3, 3, 1, 3, 1, 1, 3, 3,
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
	85, -14, 49, 50, 51, 52, -12, -66, -12, -12,
	-12, -12, 87, -62, 89, 93, -59, 89, 91, 87,
	87, 88, 89, -3, 17, -15, 18, -13, 29, -25,
	35, 8, -55, -56, -41, -64, 35, -58, 92, 88,
	-64, 35, 87, -64, 35, -57, 92, -64, -57, -16,
	-17, 73, -20, 35, -28, -36, -29, 67, 44, -42,
	-41, -37, -64, -35, -38, 20, 36, 37, 38, 25,
	71, 72, 48, 92, 28, 78, 15, -25, 33, 76,
	-25, 53, 45, 76, 35, 67, -64, -65, 35, -65,
	90, 35, 20, 64, -64, 8, 53, -18, -64, 19,
	76, 65, 66, -30, 21, 67, 23, 24, 22, 68,
	69, 70, 71, 72, 73, 74, 75, 45, 46, 47,
	39, 40, 41, 42, -28, -36, -28, -3, -34, -36,
	-36, 44, 44, 44, 44, -39, -20, -40, 79, 81,
	-52, 44, -55, 35, -27, 9, -56, -20, -64, -65,
	20, -63, 94, -60, 86, 84, 32, 85, 12, 35,
	35, 35, -65, -21, -22, -24, 44, 35, -17, -64,
	73, -28, -28, -36, -33, 44, 21, 23, 24, -36,
	-36, 25, 67, -36, -36, -36, -36, -36, -36, -36,
	-36, 111, 111, 53, 111, -36, 111, -16, 18, -16,
	-3, 82, -40, -39, -20, -20, -31, 28, -3, -53,
	-41, -27, -45, 12, -28, 64, -64, -65, -61, 90,
	-27, 53, -23, 54, 55, 56, 57, 58, 60, 61,
	-19, 35, 19, -22, -3, 76, -34, -3, -33, -36,
	-36, 65, 25, -36, 111, -16, 111, 111, 82, 80,
	-51, 64, -32, -33, 111, 53, -45, -49, 14, 13,
	35, 35, -43, 10, -22, -22, 54, 59, 54, 59,
	54, 54, 54, -26, 62, 63, 35, 111, 111, 35,
	111, 111, 65, -36, 111, -20, 30, 53, -41, -49,
	-36, -46, -47, -36, -65, -44, 11, 13, 64, 54,
	54, 88, 88, -36, 31, -33, 53, 53, -48, 26,
	27, -45, -28, -34, -28, 44, 44, 6, -36, -47,
	-49, -54, -64, -54, -55, -50, 16, 34, 111, 53,
	111, 6, 21, -64, -64, -64,
}
var yyDef = []int{

	0, -2, 1, 2, 3, 4, 5, 6, 7, 8,
	9, 10, 27, 27, 27, 27, 27, 183, 174, 0,
	0, 0, 31, 33, 34, 35, 36, 29, 0, 0,
	0, 0, 172, 0, 0, 184, 0, 0, 175, 0,
	170, 0, 170, 12, 32, 0, 37, 28, 0, 0,
	69, 0, 16, 167, 0, 135, 187, 0, 0, 0,
	188, 187, 0, 188, 0, 0, 0, 0, 0, 0,
	38, 40, 45, 187, 43, 44, 76, 0, 0, 106,
	107, 0, 135, 0, 123, 0, 137, 138, 139, 140,
	126, 127, 128, 124, 125, 0, 30, 159, 0, 0,
	74, 0, 0, 0, 188, 0, 185, 19, 0, 22,
	0, 24, 171, 0, 188, 0, 0, 41, 46, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 91, 92, 93,
	94, 95, 96, 97, 79, 0, 0, 0, 0, 104,
	118, 0, 0, 0, 0, 0, 0, 131, 0, 0,
	0, 0, 74, 70, 145, 0, 168, 169, 136, 17,
	173, 0, 0, 188, 181, 176, 177, 178, 179, 180,
	23, 25, 26, 74, 48, 54, 0, 66, 39, 47,
	42, 77, 78, 81, 82, 0, 0, 0, 0, 84,
	0, 88, 0, 110, 111, 112, 113, 114, 115, 116,
	117, 80, 108, 0, 109, 104, 119, 0, 0, 0,
	0, 129, 132, 0, 0, 134, 165, 0, 99, 0,
	161, 145, 153, 0, 75, 0, 186, 20, 0, 182,
	141, 0, 0, 57, 58, 0, 0, 0, 0, 0,
	71, 55, 0, 0, 0, 0, 0, 0, 83, 85,
	0, 0, 89, 105, 120, 0, 122, 90, 130, 0,
	13, 0, 98, 100, 160, 0, 153, 15, 0, 0,
	188, 21, 143, 0, 49, 52, 59, 0, 61, 0,
	63, 64, 65, 50, 0, 0, 56, 51, 68, 67,
	102, 103, 0, 86, 121, 133, 0, 0, 162, 14,
	154, 146, 147, 150, 18, 145, 0, 0, 0, 60,
	62, 0, 0, 87, 0, 101, 0, 0, 149, 151,
	152, 153, 144, 142, 53, 0, 0, 0, 155, 148,
	156, 0, 163, 0, 166, 11, 0, 0, 72, 0,
	73, 157, 0, 164, 0, 158,
}
var yyTok1 = []int{

	1, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 75, 68, 3,
	44, 111, 73, 71, 53, 72, 76, 74, 3, 3,
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
	97, 98, 99, 100, 101, 102, 103, 104, 105, 106,
	107, 108, 109, 110,
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
		//line sql.y:113
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
		//line sql.y:130
		{
			yyVAL.statement = &Select{Comments: yyS[yypt-10].comments, Distinct: yyS[yypt-9].distinct, SelectExprs: yyS[yypt-8].selectExprs, From: yyS[yypt-6].tableExprs, Where: yyS[yypt-5].where, GroupBy: yyS[yypt-4].node, Having: yyS[yypt-3].node, OrderBy: yyS[yypt-2].node, Limit: yyS[yypt-1].node, Lock: yyS[yypt-0].node}
		}
	case 12:
		//line sql.y:134
		{
			yyVAL.statement = &Union{Type: yyS[yypt-1].str, Select1: yyS[yypt-2].statement.(SelectStatement), Select2: yyS[yypt-0].statement.(SelectStatement)}
		}
	case 13:
		//line sql.y:140
		{
			yyVAL.statement = &Insert{Comments: yyS[yypt-5].comments, Table: yyS[yypt-3].node, Columns: yyS[yypt-2].columns, Values: yyS[yypt-1].sqlNode, OnDup: yyS[yypt-0].node}
		}
	case 14:
		//line sql.y:146
		{
			yyVAL.statement = &Update{Comments: yyS[yypt-6].comments, Table: yyS[yypt-5].node, List: yyS[yypt-3].node, Where: yyS[yypt-2].where, OrderBy: yyS[yypt-1].node, Limit: yyS[yypt-0].node}
		}
	case 15:
		//line sql.y:152
		{
			yyVAL.statement = &Delete{Comments: yyS[yypt-5].comments, Table: yyS[yypt-3].node, Where: yyS[yypt-2].where, OrderBy: yyS[yypt-1].node, Limit: yyS[yypt-0].node}
		}
	case 16:
		//line sql.y:158
		{
			yyVAL.statement = &Set{Comments: yyS[yypt-1].comments, Updates: yyS[yypt-0].node}
		}
	case 17:
		//line sql.y:164
		{
			yyVAL.statement = &DDLSimple{Action: CREATE, Table: yyS[yypt-1].node}
		}
	case 18:
		//line sql.y:168
		{
			// Change this to an alter statement
			yyVAL.statement = &DDLSimple{Action: ALTER, Table: yyS[yypt-1].node}
		}
	case 19:
		//line sql.y:173
		{
			yyVAL.statement = &DDLSimple{Action: CREATE, Table: yyS[yypt-1].node}
		}
	case 20:
		//line sql.y:179
		{
			yyVAL.statement = &DDLSimple{Action: ALTER, Table: yyS[yypt-2].node}
		}
	case 21:
		//line sql.y:183
		{
			// Change this to a rename statement
			yyVAL.statement = &Rename{OldName: yyS[yypt-3].node, NewName: yyS[yypt-0].node}
		}
	case 22:
		//line sql.y:188
		{
			yyVAL.statement = &DDLSimple{Action: ALTER, Table: yyS[yypt-1].node}
		}
	case 23:
		//line sql.y:194
		{
			yyVAL.statement = &Rename{OldName: yyS[yypt-2].node, NewName: yyS[yypt-0].node}
		}
	case 24:
		//line sql.y:200
		{
			yyVAL.statement = &DDLSimple{Action: DROP, Table: yyS[yypt-0].node}
		}
	case 25:
		//line sql.y:204
		{
			// Change this to an alter statement
			yyVAL.statement = &DDLSimple{Action: ALTER, Table: yyS[yypt-0].node}
		}
	case 26:
		//line sql.y:209
		{
			yyVAL.statement = &DDLSimple{Action: DROP, Table: yyS[yypt-1].node}
		}
	case 27:
		//line sql.y:214
		{
			SetAllowComments(yylex, true)
		}
	case 28:
		//line sql.y:218
		{
			yyVAL.comments = yyS[yypt-0].comments
			SetAllowComments(yylex, false)
		}
	case 29:
		//line sql.y:224
		{
			yyVAL.comments = nil
		}
	case 30:
		//line sql.y:228
		{
			yyVAL.comments = append(yyVAL.comments, Comment(yyS[yypt-0].node.Value))
		}
	case 31:
		//line sql.y:234
		{
			yyVAL.str = "union"
		}
	case 32:
		//line sql.y:238
		{
			yyVAL.str = "union all"
		}
	case 33:
		//line sql.y:242
		{
			yyVAL.str = "minus"
		}
	case 34:
		//line sql.y:246
		{
			yyVAL.str = "except"
		}
	case 35:
		//line sql.y:250
		{
			yyVAL.str = "intersect"
		}
	case 36:
		//line sql.y:255
		{
			yyVAL.distinct = Distinct(false)
		}
	case 37:
		//line sql.y:259
		{
			yyVAL.distinct = Distinct(true)
		}
	case 38:
		//line sql.y:265
		{
			yyVAL.selectExprs = SelectExprs{yyS[yypt-0].selectExpr}
		}
	case 39:
		//line sql.y:269
		{
			yyVAL.selectExprs = append(yyVAL.selectExprs, yyS[yypt-0].selectExpr)
		}
	case 40:
		//line sql.y:275
		{
			yyVAL.selectExpr = &StarExpr{}
		}
	case 41:
		//line sql.y:279
		{
			yyVAL.selectExpr = &NonStarExpr{Expr: yyS[yypt-1].expr, As: yyS[yypt-0].bytes}
		}
	case 42:
		//line sql.y:283
		{
			yyVAL.selectExpr = &StarExpr{TableName: yyS[yypt-2].node.Value}
		}
	case 43:
		//line sql.y:289
		{
			yyVAL.expr = yyS[yypt-0].boolExpr
		}
	case 44:
		//line sql.y:293
		{
			yyVAL.expr = yyS[yypt-0].node
		}
	case 45:
		//line sql.y:298
		{
			yyVAL.bytes = nil
		}
	case 46:
		//line sql.y:302
		{
			yyVAL.bytes = yyS[yypt-0].node.Value
		}
	case 47:
		//line sql.y:306
		{
			yyVAL.bytes = yyS[yypt-0].node.Value
		}
	case 48:
		//line sql.y:312
		{
			yyVAL.tableExprs = TableExprs{yyS[yypt-0].tableExpr}
		}
	case 49:
		//line sql.y:316
		{
			yyVAL.tableExprs = append(yyVAL.tableExprs, yyS[yypt-0].tableExpr)
		}
	case 50:
		//line sql.y:322
		{
			yyVAL.tableExpr = &AliasedTableExpr{Expr: yyS[yypt-2].node, As: yyS[yypt-1].bytes, Hint: yyS[yypt-0].node}
		}
	case 51:
		//line sql.y:326
		{
			yyVAL.tableExpr = &ParenTableExpr{Expr: yyS[yypt-1].tableExpr}
		}
	case 52:
		//line sql.y:330
		{
			yyVAL.tableExpr = &JoinTableExpr{LeftExpr: yyS[yypt-2].tableExpr, Join: yyS[yypt-1].str, RightExpr: yyS[yypt-0].tableExpr}
		}
	case 53:
		//line sql.y:334
		{
			yyVAL.tableExpr = &JoinTableExpr{LeftExpr: yyS[yypt-4].tableExpr, Join: yyS[yypt-3].str, RightExpr: yyS[yypt-2].tableExpr, On: yyS[yypt-0].boolExpr}
		}
	case 54:
		//line sql.y:339
		{
			yyVAL.bytes = nil
		}
	case 55:
		//line sql.y:343
		{
			yyVAL.bytes = yyS[yypt-0].node.Value
		}
	case 56:
		//line sql.y:347
		{
			yyVAL.bytes = yyS[yypt-0].node.Value
		}
	case 57:
		//line sql.y:353
		{
			yyVAL.str = "join"
		}
	case 58:
		//line sql.y:357
		{
			yyVAL.str = "straight_join"
		}
	case 59:
		//line sql.y:361
		{
			yyVAL.str = "left join"
		}
	case 60:
		//line sql.y:365
		{
			yyVAL.str = "left join"
		}
	case 61:
		//line sql.y:369
		{
			yyVAL.str = "right join"
		}
	case 62:
		//line sql.y:373
		{
			yyVAL.str = "right join"
		}
	case 63:
		//line sql.y:377
		{
			yyVAL.str = "join"
		}
	case 64:
		//line sql.y:381
		{
			yyVAL.str = "cross join"
		}
	case 65:
		//line sql.y:385
		{
			yyVAL.str = "natural join"
		}
	case 66:
		yyVAL.node = yyS[yypt-0].node
	case 67:
		//line sql.y:392
		{
			yyVAL.node = yyS[yypt-1].node.PushTwo(yyS[yypt-2].node, yyS[yypt-0].node)
		}
	case 68:
		//line sql.y:396
		{
			yyVAL.node = yyS[yypt-2].node.Push(yyS[yypt-1].statement)
		}
	case 69:
		yyVAL.node = yyS[yypt-0].node
	case 70:
		//line sql.y:403
		{
			yyVAL.node = yyS[yypt-1].node.PushTwo(yyS[yypt-2].node, yyS[yypt-0].node)
		}
	case 71:
		//line sql.y:408
		{
			yyVAL.node = nil
		}
	case 72:
		//line sql.y:412
		{
			yyVAL.node.Push(yyS[yypt-1].node)
		}
	case 73:
		//line sql.y:416
		{
			yyVAL.node.Push(yyS[yypt-1].node)
		}
	case 74:
		//line sql.y:421
		{
			yyVAL.where = nil
		}
	case 75:
		//line sql.y:425
		{
			yyVAL.where = &Where{Expr: yyS[yypt-0].boolExpr}
		}
	case 76:
		yyVAL.boolExpr = yyS[yypt-0].boolExpr
	case 77:
		//line sql.y:432
		{
			yyVAL.boolExpr = &AndExpr{Left: yyS[yypt-2].boolExpr, Right: yyS[yypt-0].boolExpr}
		}
	case 78:
		//line sql.y:436
		{
			yyVAL.boolExpr = &OrExpr{Left: yyS[yypt-2].boolExpr, Right: yyS[yypt-0].boolExpr}
		}
	case 79:
		//line sql.y:440
		{
			yyVAL.boolExpr = &NotExpr{Expr: yyS[yypt-0].boolExpr}
		}
	case 80:
		//line sql.y:444
		{
			yyVAL.boolExpr = &ParenBoolExpr{Expr: yyS[yypt-1].boolExpr}
		}
	case 81:
		//line sql.y:450
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyS[yypt-2].node, Operator: yyS[yypt-1].str, Right: yyS[yypt-0].node}
		}
	case 82:
		//line sql.y:454
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyS[yypt-2].node, Operator: "in", Right: yyS[yypt-0].node}
		}
	case 83:
		//line sql.y:458
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyS[yypt-3].node, Operator: "not in", Right: yyS[yypt-0].node}
		}
	case 84:
		//line sql.y:462
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyS[yypt-2].node, Operator: "like", Right: yyS[yypt-0].node}
		}
	case 85:
		//line sql.y:466
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyS[yypt-3].node, Operator: "not like", Right: yyS[yypt-0].node}
		}
	case 86:
		//line sql.y:470
		{
			yyVAL.boolExpr = &RangeCond{Left: yyS[yypt-4].node, Operator: "between", From: yyS[yypt-2].node, To: yyS[yypt-0].node}
		}
	case 87:
		//line sql.y:474
		{
			yyVAL.boolExpr = &RangeCond{Left: yyS[yypt-5].node, Operator: "not between", From: yyS[yypt-2].node, To: yyS[yypt-0].node}
		}
	case 88:
		//line sql.y:478
		{
			yyVAL.boolExpr = &NullCheck{Operator: "is null", Expr: yyS[yypt-2].node}
		}
	case 89:
		//line sql.y:482
		{
			yyVAL.boolExpr = &NullCheck{Operator: "is not null", Expr: yyS[yypt-3].node}
		}
	case 90:
		//line sql.y:486
		{
			yyVAL.boolExpr = &ExistsExpr{Expr: yyS[yypt-2].node.Push(yyS[yypt-1].statement)}
		}
	case 91:
		//line sql.y:492
		{
			yyVAL.str = "="
		}
	case 92:
		//line sql.y:496
		{
			yyVAL.str = "<"
		}
	case 93:
		//line sql.y:500
		{
			yyVAL.str = ">"
		}
	case 94:
		//line sql.y:504
		{
			yyVAL.str = "<="
		}
	case 95:
		//line sql.y:508
		{
			yyVAL.str = ">="
		}
	case 96:
		//line sql.y:512
		{
			yyVAL.str = string(yyS[yypt-0].node.Value)
		}
	case 97:
		//line sql.y:516
		{
			yyVAL.str = "<=>"
		}
	case 98:
		//line sql.y:522
		{
			yyVAL.sqlNode = yyS[yypt-1].node.Push(yyS[yypt-0].node)
		}
	case 99:
		//line sql.y:526
		{
			yyVAL.sqlNode = yyS[yypt-0].statement
		}
	case 100:
		//line sql.y:532
		{
			yyVAL.node = NewSimpleParseNode(NODE_LIST, "node_list")
			yyVAL.node.Push(yyS[yypt-0].node)
		}
	case 101:
		//line sql.y:537
		{
			yyVAL.node.Push(yyS[yypt-0].node)
		}
	case 102:
		//line sql.y:543
		{
			yyVAL.node = yyS[yypt-2].node.Push(yyS[yypt-1].node)
		}
	case 103:
		//line sql.y:547
		{
			yyVAL.node = yyS[yypt-2].node.Push(yyS[yypt-1].statement)
		}
	case 104:
		//line sql.y:553
		{
			yyVAL.node = NewSimpleParseNode(NODE_LIST, "node_list")
			yyVAL.node.Push(yyS[yypt-0].node)
		}
	case 105:
		//line sql.y:558
		{
			yyVAL.node.Push(yyS[yypt-0].node)
		}
	case 106:
		yyVAL.node = yyS[yypt-0].node
	case 107:
		yyVAL.node = yyS[yypt-0].node
	case 108:
		//line sql.y:566
		{
			yyVAL.node = yyS[yypt-2].node.Push(yyS[yypt-1].statement)
		}
	case 109:
		//line sql.y:570
		{
			if yyS[yypt-1].node.Len() == 1 {
				yyS[yypt-1].node = yyS[yypt-1].node.NodeAt(0)
			}
			switch yyS[yypt-1].node.Type {
			case NUMBER, STRING, ID, VALUE_ARG, '(', '.':
				yyVAL.node = yyS[yypt-1].node
			default:
				yyVAL.node = yyS[yypt-2].node.Push(yyS[yypt-1].node)
			}
		}
	case 110:
		//line sql.y:582
		{
			yyVAL.node = yyS[yypt-1].node.PushTwo(yyS[yypt-2].node, yyS[yypt-0].node)
		}
	case 111:
		//line sql.y:586
		{
			yyVAL.node = yyS[yypt-1].node.PushTwo(yyS[yypt-2].node, yyS[yypt-0].node)
		}
	case 112:
		//line sql.y:590
		{
			yyVAL.node = yyS[yypt-1].node.PushTwo(yyS[yypt-2].node, yyS[yypt-0].node)
		}
	case 113:
		//line sql.y:594
		{
			yyVAL.node = yyS[yypt-1].node.PushTwo(yyS[yypt-2].node, yyS[yypt-0].node)
		}
	case 114:
		//line sql.y:598
		{
			yyVAL.node = yyS[yypt-1].node.PushTwo(yyS[yypt-2].node, yyS[yypt-0].node)
		}
	case 115:
		//line sql.y:602
		{
			yyVAL.node = yyS[yypt-1].node.PushTwo(yyS[yypt-2].node, yyS[yypt-0].node)
		}
	case 116:
		//line sql.y:606
		{
			yyVAL.node = yyS[yypt-1].node.PushTwo(yyS[yypt-2].node, yyS[yypt-0].node)
		}
	case 117:
		//line sql.y:610
		{
			yyVAL.node = yyS[yypt-1].node.PushTwo(yyS[yypt-2].node, yyS[yypt-0].node)
		}
	case 118:
		//line sql.y:614
		{
			if yyS[yypt-0].node.Type == NUMBER { // Simplify trivial unary expressions
				switch yyS[yypt-1].node.Type {
				case UMINUS:
					yyS[yypt-0].node.Value = append(yyS[yypt-1].node.Value, yyS[yypt-0].node.Value...)
					yyVAL.node = yyS[yypt-0].node
				case UPLUS:
					yyVAL.node = yyS[yypt-0].node
				default:
					yyVAL.node = yyS[yypt-1].node.Push(yyS[yypt-0].node)
				}
			} else {
				yyVAL.node = yyS[yypt-1].node.Push(yyS[yypt-0].node)
			}
		}
	case 119:
		//line sql.y:630
		{
			yyS[yypt-2].node.Type = FUNCTION
			yyVAL.node = yyS[yypt-2].node.Push(NewSimpleParseNode(NODE_LIST, "node_list"))
		}
	case 120:
		//line sql.y:635
		{
			yyS[yypt-3].node.Type = FUNCTION
			yyVAL.node = yyS[yypt-3].node.Push(yyS[yypt-1].selectExprs)
		}
	case 121:
		//line sql.y:640
		{
			yyS[yypt-4].node.Type = FUNCTION
			yyS[yypt-4].node.Push(yyS[yypt-2].node)
			yyVAL.node = yyS[yypt-4].node.Push(yyS[yypt-1].selectExprs)
		}
	case 122:
		//line sql.y:646
		{
			yyS[yypt-3].node.Type = FUNCTION
			yyVAL.node = yyS[yypt-3].node.Push(yyS[yypt-1].selectExprs)
		}
	case 123:
		yyVAL.node = yyS[yypt-0].node
	case 124:
		yyVAL.node = yyS[yypt-0].node
	case 125:
		yyVAL.node = yyS[yypt-0].node
	case 126:
		//line sql.y:658
		{
			yyVAL.node = NewSimpleParseNode(UPLUS, "+")
		}
	case 127:
		//line sql.y:662
		{
			yyVAL.node = NewSimpleParseNode(UMINUS, "-")
		}
	case 128:
		yyVAL.node = yyS[yypt-0].node
	case 129:
		//line sql.y:669
		{
			yyVAL.node = NewSimpleParseNode(CASE_WHEN, "case")
			yyVAL.node.Push(yyS[yypt-1].node)
		}
	case 130:
		//line sql.y:674
		{
			yyVAL.node.PushTwo(yyS[yypt-2].expr, yyS[yypt-1].node)
		}
	case 131:
		//line sql.y:680
		{
			yyVAL.node = NewSimpleParseNode(WHEN_LIST, "when_list")
			yyVAL.node.Push(yyS[yypt-0].node)
		}
	case 132:
		//line sql.y:685
		{
			yyVAL.node.Push(yyS[yypt-0].node)
		}
	case 133:
		//line sql.y:691
		{
			yyVAL.node.PushTwo(yyS[yypt-2].expr, yyS[yypt-0].expr)
		}
	case 134:
		//line sql.y:695
		{
			yyVAL.node.Push(yyS[yypt-0].expr)
		}
	case 135:
		yyVAL.node = yyS[yypt-0].node
	case 136:
		//line sql.y:702
		{
			yyVAL.node = yyS[yypt-1].node.PushTwo(yyS[yypt-2].node, yyS[yypt-0].node)
		}
	case 137:
		yyVAL.node = yyS[yypt-0].node
	case 138:
		yyVAL.node = yyS[yypt-0].node
	case 139:
		yyVAL.node = yyS[yypt-0].node
	case 140:
		yyVAL.node = yyS[yypt-0].node
	case 141:
		//line sql.y:713
		{
			yyVAL.node = NewSimpleParseNode(GROUP, "group")
		}
	case 142:
		//line sql.y:717
		{
			yyVAL.node = yyS[yypt-2].node.Push(yyS[yypt-0].node)
		}
	case 143:
		//line sql.y:722
		{
			yyVAL.node = NewSimpleParseNode(HAVING, "having")
		}
	case 144:
		//line sql.y:726
		{
			yyVAL.node = yyS[yypt-1].node.Push(yyS[yypt-0].boolExpr)
		}
	case 145:
		//line sql.y:731
		{
			yyVAL.node = NewSimpleParseNode(ORDER, "order")
		}
	case 146:
		//line sql.y:735
		{
			yyVAL.node = yyS[yypt-2].node.Push(yyS[yypt-0].node)
		}
	case 147:
		//line sql.y:741
		{
			yyVAL.node = NewSimpleParseNode(NODE_LIST, "node_list")
			yyVAL.node.Push(yyS[yypt-0].node)
		}
	case 148:
		//line sql.y:746
		{
			yyVAL.node = yyS[yypt-2].node.Push(yyS[yypt-0].node)
		}
	case 149:
		//line sql.y:752
		{
			yyVAL.node = yyS[yypt-0].node.Push(yyS[yypt-1].node)
		}
	case 150:
		//line sql.y:757
		{
			yyVAL.node = NewSimpleParseNode(ASC, "asc")
		}
	case 151:
		yyVAL.node = yyS[yypt-0].node
	case 152:
		yyVAL.node = yyS[yypt-0].node
	case 153:
		//line sql.y:764
		{
			yyVAL.node = NewSimpleParseNode(LIMIT, "limit")
		}
	case 154:
		//line sql.y:768
		{
			yyVAL.node = yyS[yypt-1].node.Push(yyS[yypt-0].node)
		}
	case 155:
		//line sql.y:772
		{
			yyVAL.node = yyS[yypt-3].node.PushTwo(yyS[yypt-2].node, yyS[yypt-0].node)
		}
	case 156:
		//line sql.y:777
		{
			yyVAL.node = NewSimpleParseNode(NO_LOCK, "")
		}
	case 157:
		//line sql.y:781
		{
			yyVAL.node = NewSimpleParseNode(FOR_UPDATE, " for update")
		}
	case 158:
		//line sql.y:785
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
		//line sql.y:798
		{
			yyVAL.columns = nil
		}
	case 160:
		//line sql.y:802
		{
			yyVAL.columns = yyS[yypt-1].columns
		}
	case 161:
		//line sql.y:808
		{
			yyVAL.columns = Columns{&NonStarExpr{Expr: yyS[yypt-0].node}}
		}
	case 162:
		//line sql.y:812
		{
			yyVAL.columns = append(yyVAL.columns, &NonStarExpr{Expr: yyS[yypt-0].node})
		}
	case 163:
		//line sql.y:818
		{
			yyVAL.node = NewSimpleParseNode(INDEX_LIST, "")
			yyVAL.node.Push(yyS[yypt-0].node)
		}
	case 164:
		//line sql.y:823
		{
			yyVAL.node = yyS[yypt-2].node.Push(yyS[yypt-0].node)
		}
	case 165:
		//line sql.y:828
		{
			yyVAL.node = NewSimpleParseNode(DUPLICATE, "duplicate")
		}
	case 166:
		//line sql.y:832
		{
			yyVAL.node = yyS[yypt-3].node.Push(yyS[yypt-0].node)
		}
	case 167:
		//line sql.y:838
		{
			yyVAL.node = NewSimpleParseNode(NODE_LIST, "node_list")
			yyVAL.node.Push(yyS[yypt-0].node)
		}
	case 168:
		//line sql.y:843
		{
			yyVAL.node = yyS[yypt-2].node.Push(yyS[yypt-0].node)
		}
	case 169:
		//line sql.y:849
		{
			yyVAL.node = yyS[yypt-1].node.PushTwo(yyS[yypt-2].node, yyS[yypt-0].expr)
		}
	case 170:
		//line sql.y:854
		{
			yyVAL.node = nil
		}
	case 171:
		yyVAL.node = yyS[yypt-0].node
	case 172:
		//line sql.y:858
		{
			yyVAL.node = nil
		}
	case 173:
		yyVAL.node = yyS[yypt-0].node
	case 174:
		//line sql.y:862
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
		//line sql.y:873
		{
			yyVAL.node = nil
		}
	case 182:
		yyVAL.node = yyS[yypt-0].node
	case 183:
		//line sql.y:877
		{
			yyVAL.node = nil
		}
	case 184:
		yyVAL.node = yyS[yypt-0].node
	case 185:
		//line sql.y:881
		{
			yyVAL.node = nil
		}
	case 186:
		yyVAL.node = yyS[yypt-0].node
	case 187:
		//line sql.y:886
		{
			yyVAL.node.LowerCase()
		}
	case 188:
		//line sql.y:891
		{
			ForceEOF(yylex)
		}
	}
	goto yystack /* stack new state and value */
}
