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
	unionOp     []byte
	distinct    Distinct
	selectExprs SelectExprs
	selectExpr  SelectExpr
	columns     Columns
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
const TABLE_EXPR = 57438

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
	"TABLE_EXPR",
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
	-1, 72,
	35, 46,
	-2, 41,
	-1, 185,
	35, 46,
	-2, 69,
}

const yyNprod = 187
const yyPrivate = 57344

var yyTokenNames []string
var yyStates []string

const yyLast = 563

var yyAct = []int{

	82, 75, 52, 311, 335, 277, 231, 80, 147, 193,
	251, 107, 183, 154, 117, 156, 72, 74, 163, 22,
	23, 24, 25, 70, 69, 146, 3, 242, 243, 244,
	245, 246, 55, 247, 248, 60, 171, 66, 63, 54,
	120, 121, 67, 58, 343, 238, 343, 43, 22, 23,
	24, 25, 22, 23, 24, 25, 53, 37, 116, 38,
	106, 110, 22, 23, 24, 25, 32, 62, 34, 114,
	212, 297, 35, 296, 59, 109, 39, 177, 269, 144,
	148, 319, 300, 149, 275, 293, 116, 210, 116, 212,
	255, 40, 41, 42, 119, 143, 145, 175, 103, 55,
	178, 161, 55, 344, 167, 342, 54, 105, 157, 54,
	158, 294, 155, 99, 317, 267, 168, 303, 188, 166,
	167, 271, 144, 144, 192, 211, 181, 198, 199, 299,
	202, 203, 204, 205, 206, 207, 208, 209, 190, 191,
	187, 200, 318, 274, 234, 266, 113, 264, 213, 174,
	176, 173, 214, 157, 292, 158, 268, 157, 165, 158,
	220, 55, 133, 134, 135, 118, 144, 291, 229, 222,
	221, 326, 235, 61, 223, 224, 216, 218, 290, 219,
	230, 101, 233, 201, 236, 227, 128, 129, 130, 131,
	132, 133, 134, 135, 120, 121, 214, 249, 259, 260,
	252, 239, 164, 256, 115, 258, 253, 254, 253, 254,
	250, 189, 164, 288, 263, 217, 286, 85, 289, 212,
	257, 287, 89, 327, 306, 94, 131, 132, 133, 134,
	135, 102, 73, 86, 87, 88, 273, 276, 221, 49,
	61, 78, 265, 85, 194, 92, 240, 12, 89, 116,
	322, 94, 321, 284, 285, 160, 101, 186, 56, 86,
	87, 88, 153, 302, 77, 152, 184, 78, 90, 91,
	71, 92, 151, 56, 298, 95, 55, 65, 186, 295,
	309, 312, 308, 307, 281, 280, 304, 184, 97, 93,
	77, 100, 313, 180, 90, 91, 347, 12, 13, 14,
	15, 95, 157, 323, 158, 179, 320, 162, 111, 215,
	22, 23, 24, 25, 348, 93, 325, 144, 214, 144,
	68, 331, 336, 336, 108, 333, 16, 337, 339, 312,
	104, 340, 50, 332, 64, 334, 98, 341, 324, 55,
	85, 345, 305, 12, 349, 89, 54, 48, 94, 262,
	12, 351, 352, 353, 169, 73, 86, 87, 88, 329,
	330, 112, 118, 46, 78, 44, 85, 226, 92, 96,
	195, 89, 196, 197, 94, 278, 17, 18, 20, 19,
	316, 56, 86, 87, 88, 279, 232, 77, 315, 283,
	78, 90, 91, 71, 92, 164, 51, 27, 95, 350,
	338, 128, 129, 130, 131, 132, 133, 134, 135, 12,
	12, 85, 93, 77, 170, 33, 89, 90, 91, 94,
	237, 172, 36, 57, 95, 228, 56, 86, 87, 88,
	159, 89, 26, 270, 94, 78, 346, 328, 93, 92,
	310, 56, 86, 87, 88, 314, 28, 29, 30, 31,
	150, 282, 79, 84, 92, 81, 83, 272, 77, 225,
	122, 76, 90, 91, 242, 243, 244, 245, 246, 95,
	247, 248, 185, 241, 182, 45, 89, 90, 91, 94,
	21, 47, 11, 93, 95, 10, 56, 86, 87, 88,
	9, 8, 7, 6, 5, 150, 4, 2, 93, 92,
	123, 127, 125, 126, 301, 1, 0, 128, 129, 130,
	131, 132, 133, 134, 135, 0, 0, 0, 139, 140,
	141, 142, 90, 91, 136, 137, 138, 261, 0, 95,
	128, 129, 130, 131, 132, 133, 134, 135, 0, 0,
	0, 0, 0, 93, 0, 0, 124, 128, 129, 130,
	131, 132, 133, 134, 135, 128, 129, 130, 131, 132,
	133, 134, 135,
}
var yyPact = []int{

	293, -1000, -1000, 261, -1000, -1000, -1000, -1000, -1000, -1000,
	-1000, -1000, -1000, -1000, -1000, -1000, -1000, -21, -32, -11,
	4, 405, 348, -1000, -1000, -1000, 345, -1000, 318, 297,
	388, 238, -49, -14, 205, -1000, -20, 205, -1000, 299,
	-55, 205, -55, -1000, -1000, 320, -1000, 354, 297, 303,
	37, 297, 128, -1000, 186, -1000, 22, 295, 40, 205,
	-1000, -1000, 289, -1000, -29, 273, 341, 82, 205, 196,
	-1000, -1000, 343, 18, 129, 479, -1000, 391, 346, -1000,
	-1000, 451, 228, 221, -1000, 218, -1000, -1000, -1000, -1000,
	-1000, -1000, -1000, -1000, -1000, 223, -1000, 211, 238, 272,
	386, 238, 391, 205, -1000, 334, -58, -1000, 65, -1000,
	270, -1000, -1000, 258, -1000, 222, 320, 205, -1000, 138,
	391, 391, 451, 200, 349, 451, 451, 116, 451, 451,
	451, 451, 451, 451, 451, 451, -1000, -1000, -1000, -1000,
	-1000, -1000, -1000, -1000, 479, -25, 13, 36, 479, -1000,
	406, 197, 320, 405, 78, 29, -1000, 391, 391, 339,
	238, 203, -1000, 374, 391, -1000, -1000, -1000, -1000, -1000,
	80, 205, -1000, -45, -1000, -1000, -1000, -1000, -1000, -1000,
	-1000, -1000, 193, 410, 243, 146, 14, -1000, -1000, -1000,
	-1000, -1000, 487, -1000, 406, 200, 451, 451, 487, 462,
	-1000, 324, 155, 155, 155, 89, 89, -1000, -1000, -1000,
	-1000, -1000, 451, -1000, 487, -1000, 35, 320, 33, 3,
	-1000, -1000, 74, -2, -1000, 57, 200, 261, 31, -1000,
	374, 361, 372, 129, 250, -1000, -1000, 249, -1000, 379,
	222, 222, -1000, -1000, 162, 159, 124, 113, 100, -27,
	-1, -1000, 244, -15, -17, 239, 17, -30, -1000, 487,
	439, 451, -1000, 487, -1000, 5, -1000, -1000, -1000, 391,
	-1000, 312, 171, -1000, -1000, 238, 361, -1000, 451, 451,
	-1000, -1000, 377, 367, 410, 50, -1000, 88, -1000, 27,
	-1000, -1000, -1000, -1000, -1000, 144, 208, 206, -1000, -1000,
	-1000, 451, 487, -1000, -1000, 307, 200, -1000, -1000, 118,
	170, -1000, 333, -1000, 374, 391, 451, 391, -1000, -1000,
	-1000, 205, 205, 487, 394, -1000, 451, 451, -1000, -1000,
	-1000, 361, 129, 166, 129, -7, -1000, -9, 238, 487,
	-1000, 280, -1000, 205, -1000, 128, -1000, 393, 330, -1000,
	-1000, 205, 205, -1000,
}
var yyPgo = []int{

	0, 505, 497, 25, 496, 494, 493, 492, 491, 490,
	485, 482, 432, 481, 480, 475, 24, 23, 16, 14,
	474, 12, 473, 472, 239, 10, 18, 17, 461, 460,
	459, 457, 9, 8, 1, 456, 455, 453, 13, 15,
	7, 452, 451, 445, 6, 440, 3, 437, 5, 436,
	433, 430, 425, 4, 2, 56, 277, 423, 422, 421,
	420, 415, 414, 0, 11, 397,
}
var yyR1 = []int{

	0, 1, 2, 2, 2, 2, 2, 2, 2, 2,
	2, 3, 3, 4, 5, 6, 7, 8, 8, 8,
	9, 9, 9, 10, 11, 11, 11, 65, 12, 13,
	13, 14, 14, 14, 14, 14, 15, 15, 16, 16,
	17, 17, 17, 17, 18, 18, 19, 19, 20, 20,
	21, 21, 21, 21, 21, 22, 22, 22, 22, 22,
	22, 22, 22, 22, 23, 23, 23, 24, 24, 25,
	25, 25, 26, 26, 27, 27, 27, 27, 27, 28,
	28, 28, 28, 28, 28, 28, 28, 28, 28, 29,
	29, 29, 29, 29, 29, 29, 30, 30, 31, 31,
	32, 32, 33, 33, 34, 34, 34, 34, 34, 34,
	34, 34, 34, 34, 34, 34, 34, 34, 34, 34,
	34, 34, 35, 35, 36, 36, 36, 37, 37, 38,
	38, 39, 39, 40, 40, 41, 41, 41, 41, 42,
	42, 43, 43, 44, 44, 45, 45, 46, 47, 47,
	47, 48, 48, 48, 49, 49, 49, 51, 51, 52,
	52, 53, 53, 50, 50, 54, 54, 55, 56, 56,
	57, 57, 58, 58, 59, 59, 59, 59, 59, 60,
	60, 61, 61, 62, 62, 63, 64,
}
var yyR2 = []int{

	0, 1, 1, 1, 1, 1, 1, 1, 1, 1,
	1, 12, 3, 7, 8, 7, 3, 5, 8, 4,
	6, 7, 4, 5, 4, 5, 5, 0, 2, 0,
	2, 1, 2, 1, 1, 1, 0, 1, 1, 3,
	1, 1, 3, 3, 1, 1, 0, 1, 1, 3,
	3, 2, 4, 3, 5, 1, 1, 2, 3, 2,
	3, 2, 2, 2, 1, 3, 3, 1, 3, 0,
	5, 5, 0, 2, 1, 3, 3, 2, 3, 3,
	3, 4, 3, 4, 5, 6, 3, 4, 4, 1,
	1, 1, 1, 1, 1, 1, 2, 1, 1, 3,
	3, 3, 1, 3, 1, 1, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 2, 3, 4, 5,
	4, 1, 1, 1, 1, 1, 1, 3, 4, 1,
	2, 4, 2, 1, 3, 1, 1, 1, 1, 0,
	3, 0, 2, 0, 3, 1, 3, 2, 0, 1,
	1, 0, 2, 4, 0, 2, 4, 0, 3, 1,
	3, 1, 3, 0, 5, 1, 3, 3, 0, 2,
	0, 3, 0, 1, 1, 1, 1, 1, 1, 0,
	1, 0, 1, 0, 2, 1, 0,
}
var yyChk = []int{

	-1000, -1, -2, -3, -4, -5, -6, -7, -8, -9,
	-10, -11, 4, 5, 6, 7, 33, 83, 84, 86,
	85, -14, 49, 50, 51, 52, -12, -65, -12, -12,
	-12, -12, 87, -61, 89, 93, -58, 89, 91, 87,
	87, 88, 89, -3, 17, -15, 18, -13, 29, -24,
	35, 8, -54, -55, -40, -63, 35, -57, 92, 88,
	-63, 35, 87, -63, 35, -56, 92, -63, -56, -16,
	-17, 73, -18, 35, -27, -34, -28, 67, 44, -41,
	-40, -36, -63, -35, -37, 20, 36, 37, 38, 25,
	71, 72, 48, 92, 28, 78, 15, -24, 33, 76,
	-24, 53, 45, 76, 35, 67, -63, -64, 35, -64,
	90, 35, 20, 64, -63, 8, 53, -19, 19, 76,
	65, 66, -29, 21, 67, 23, 24, 22, 68, 69,
	70, 71, 72, 73, 74, 75, 45, 46, 47, 39,
	40, 41, 42, -27, -34, -27, -3, -33, -34, -34,
	44, 44, 44, 44, -38, -18, -39, 79, 81, -51,
	44, -54, 35, -26, 9, -55, -18, -63, -64, 20,
	-62, 94, -59, 86, 84, 32, 85, 12, 35, 35,
	35, -64, -20, -21, 44, -23, 35, -17, -63, 73,
	-27, -27, -34, -32, 44, 21, 23, 24, -34, -34,
	25, 67, -34, -34, -34, -34, -34, -34, -34, -34,
	112, 112, 53, 112, -34, 112, -16, 18, -16, -3,
	82, -39, -38, -18, -18, -30, 28, -3, -52, -40,
	-26, -44, 12, -27, 64, -63, -64, -60, 90, -26,
	53, -22, 54, 55, 56, 57, 58, 60, 61, -21,
	-3, -25, -19, 62, 63, 76, -33, -3, -32, -34,
	-34, 65, 25, -34, 112, -16, 112, 112, 82, 80,
	-50, 64, -31, -32, 112, 53, -44, -48, 14, 13,
	35, 35, -42, 10, -21, -21, 54, 59, 54, 59,
	54, 54, 54, 112, 112, 35, 88, 88, 35, 112,
	112, 65, -34, 112, -18, 30, 53, -40, -48, -34,
	-45, -46, -34, -64, -43, 11, 13, 64, 54, 54,
	-25, 44, 44, -34, 31, -32, 53, 53, -47, 26,
	27, -44, -27, -33, -27, -53, -63, -53, 6, -34,
	-46, -48, 112, 53, 112, -54, -49, 16, 34, -63,
	6, 21, -63, -63,
}
var yyDef = []int{

	0, -2, 1, 2, 3, 4, 5, 6, 7, 8,
	9, 10, 27, 27, 27, 27, 27, 181, 172, 0,
	0, 0, 31, 33, 34, 35, 36, 29, 0, 0,
	0, 0, 170, 0, 0, 182, 0, 0, 173, 0,
	168, 0, 168, 12, 32, 0, 37, 28, 0, 0,
	67, 0, 16, 165, 0, 133, 185, 0, 0, 0,
	186, 185, 0, 186, 0, 0, 0, 0, 0, 0,
	38, 40, -2, 185, 44, 45, 74, 0, 0, 104,
	105, 0, 133, 0, 121, 0, 135, 136, 137, 138,
	124, 125, 126, 122, 123, 0, 30, 157, 0, 0,
	72, 0, 0, 0, 186, 0, 183, 19, 0, 22,
	0, 24, 169, 0, 186, 0, 0, 0, 47, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 89, 90, 91, 92,
	93, 94, 95, 77, 0, 0, 0, 0, 102, 116,
	0, 0, 0, 0, 0, 0, 129, 0, 0, 0,
	0, 72, 68, 143, 0, 166, 167, 134, 17, 171,
	0, 0, 186, 179, 174, 175, 176, 177, 178, 23,
	25, 26, 72, 48, 0, -2, 64, 39, 42, 43,
	75, 76, 79, 80, 0, 0, 0, 0, 82, 0,
	86, 0, 108, 109, 110, 111, 112, 113, 114, 115,
	78, 106, 0, 107, 102, 117, 0, 0, 0, 0,
	127, 130, 0, 0, 132, 163, 0, 97, 0, 159,
	143, 151, 0, 73, 0, 184, 20, 0, 180, 139,
	0, 0, 55, 56, 0, 0, 0, 0, 0, 0,
	0, 51, 0, 0, 0, 0, 0, 0, 81, 83,
	0, 0, 87, 103, 118, 0, 120, 88, 128, 0,
	13, 0, 96, 98, 158, 0, 151, 15, 0, 0,
	186, 21, 141, 0, 49, 53, 57, 0, 59, 0,
	61, 62, 63, 50, 66, 69, 0, 0, 65, 100,
	101, 0, 84, 119, 131, 0, 0, 160, 14, 152,
	144, 145, 148, 18, 143, 0, 0, 0, 58, 60,
	52, 0, 0, 85, 0, 99, 0, 0, 147, 149,
	150, 151, 142, 140, 54, 0, 161, 0, 0, 153,
	146, 154, 70, 0, 71, 164, 11, 0, 0, 162,
	155, 0, 0, 156,
}
var yyTok1 = []int{

	1, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 75, 68, 3,
	44, 112, 73, 71, 53, 72, 76, 74, 3, 3,
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
	107, 108, 109, 110, 111,
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
		//line sql.y:100
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
		//line sql.y:117
		{
			yyVAL.statement = &Select{Comments: yyS[yypt-10].comments, Distinct: yyS[yypt-9].distinct, SelectExprs: yyS[yypt-8].selectExprs, From: yyS[yypt-6].node, Where: yyS[yypt-5].node, GroupBy: yyS[yypt-4].node, Having: yyS[yypt-3].node, OrderBy: yyS[yypt-2].node, Limit: yyS[yypt-1].node, Lock: yyS[yypt-0].node}
		}
	case 12:
		//line sql.y:121
		{
			yyVAL.statement = &Union{Type: yyS[yypt-1].unionOp, Select1: yyS[yypt-2].statement.(SelectStatement), Select2: yyS[yypt-0].statement.(SelectStatement)}
		}
	case 13:
		//line sql.y:127
		{
			yyVAL.statement = &Insert{Comments: yyS[yypt-5].comments, Table: yyS[yypt-3].node, Columns: yyS[yypt-2].columns, Values: yyS[yypt-1].sqlNode, OnDup: yyS[yypt-0].node}
		}
	case 14:
		//line sql.y:133
		{
			yyVAL.statement = &Update{Comments: yyS[yypt-6].comments, Table: yyS[yypt-5].node, List: yyS[yypt-3].node, Where: yyS[yypt-2].node, OrderBy: yyS[yypt-1].node, Limit: yyS[yypt-0].node}
		}
	case 15:
		//line sql.y:139
		{
			yyVAL.statement = &Delete{Comments: yyS[yypt-5].comments, Table: yyS[yypt-3].node, Where: yyS[yypt-2].node, OrderBy: yyS[yypt-1].node, Limit: yyS[yypt-0].node}
		}
	case 16:
		//line sql.y:145
		{
			yyVAL.statement = &Set{Comments: yyS[yypt-1].comments, Updates: yyS[yypt-0].node}
		}
	case 17:
		//line sql.y:151
		{
			yyVAL.statement = &DDLSimple{Action: CREATE, Table: yyS[yypt-1].node}
		}
	case 18:
		//line sql.y:155
		{
			// Change this to an alter statement
			yyVAL.statement = &DDLSimple{Action: ALTER, Table: yyS[yypt-1].node}
		}
	case 19:
		//line sql.y:160
		{
			yyVAL.statement = &DDLSimple{Action: CREATE, Table: yyS[yypt-1].node}
		}
	case 20:
		//line sql.y:166
		{
			yyVAL.statement = &DDLSimple{Action: ALTER, Table: yyS[yypt-2].node}
		}
	case 21:
		//line sql.y:170
		{
			// Change this to a rename statement
			yyVAL.statement = &Rename{OldName: yyS[yypt-3].node, NewName: yyS[yypt-0].node}
		}
	case 22:
		//line sql.y:175
		{
			yyVAL.statement = &DDLSimple{Action: ALTER, Table: yyS[yypt-1].node}
		}
	case 23:
		//line sql.y:181
		{
			yyVAL.statement = &Rename{OldName: yyS[yypt-2].node, NewName: yyS[yypt-0].node}
		}
	case 24:
		//line sql.y:187
		{
			yyVAL.statement = &DDLSimple{Action: DROP, Table: yyS[yypt-0].node}
		}
	case 25:
		//line sql.y:191
		{
			// Change this to an alter statement
			yyVAL.statement = &DDLSimple{Action: ALTER, Table: yyS[yypt-0].node}
		}
	case 26:
		//line sql.y:196
		{
			yyVAL.statement = &DDLSimple{Action: DROP, Table: yyS[yypt-1].node}
		}
	case 27:
		//line sql.y:201
		{
			SetAllowComments(yylex, true)
		}
	case 28:
		//line sql.y:205
		{
			yyVAL.comments = yyS[yypt-0].comments
			SetAllowComments(yylex, false)
		}
	case 29:
		//line sql.y:211
		{
			yyVAL.comments = nil
		}
	case 30:
		//line sql.y:215
		{
			yyVAL.comments = append(yyVAL.comments, Comment(yyS[yypt-0].node.Value))
		}
	case 31:
		//line sql.y:221
		{
			yyVAL.unionOp = yyS[yypt-0].node.Value
		}
	case 32:
		//line sql.y:225
		{
			yyVAL.unionOp = []byte("union all")
		}
	case 33:
		//line sql.y:229
		{
			yyVAL.unionOp = yyS[yypt-0].node.Value
		}
	case 34:
		//line sql.y:233
		{
			yyVAL.unionOp = yyS[yypt-0].node.Value
		}
	case 35:
		//line sql.y:237
		{
			yyVAL.unionOp = yyS[yypt-0].node.Value
		}
	case 36:
		//line sql.y:242
		{
			yyVAL.distinct = Distinct(false)
		}
	case 37:
		//line sql.y:246
		{
			yyVAL.distinct = Distinct(true)
		}
	case 38:
		//line sql.y:252
		{
			yyVAL.selectExprs = SelectExprs{yyS[yypt-0].selectExpr}
		}
	case 39:
		//line sql.y:256
		{
			yyVAL.selectExprs = append(yyVAL.selectExprs, yyS[yypt-0].selectExpr)
		}
	case 40:
		//line sql.y:262
		{
			yyVAL.selectExpr = &StarExpr{}
		}
	case 41:
		//line sql.y:266
		{
			yyVAL.selectExpr = &NonStarExpr{Expr: yyS[yypt-0].node}
		}
	case 42:
		//line sql.y:270
		{
			yyVAL.selectExpr = &NonStarExpr{Expr: yyS[yypt-2].node, As: yyS[yypt-0].node.Value}
		}
	case 43:
		//line sql.y:274
		{
			yyVAL.selectExpr = &StarExpr{TableName: yyS[yypt-2].node.Value}
		}
	case 44:
		yyVAL.node = yyS[yypt-0].node
	case 45:
		yyVAL.node = yyS[yypt-0].node
	case 46:
		//line sql.y:283
		{
			yyVAL.node = NewSimpleParseNode(AS, "as")
		}
	case 47:
		yyVAL.node = yyS[yypt-0].node
	case 48:
		//line sql.y:290
		{
			yyVAL.node = NewSimpleParseNode(NODE_LIST, "node_list")
			yyVAL.node.Push(yyS[yypt-0].node)
		}
	case 49:
		//line sql.y:295
		{
			yyVAL.node.Push(yyS[yypt-0].node)
		}
	case 50:
		//line sql.y:301
		{
			yyVAL.node = yyS[yypt-2].node.Push(yyS[yypt-1].node)
		}
	case 51:
		//line sql.y:305
		{
			yyVAL.node = NewSimpleParseNode(TABLE_EXPR, "")
			yyVAL.node.Push(yyS[yypt-1].node)
			yyVAL.node.Push(NewSimpleParseNode(NODE_LIST, "node_list"))
			yyVAL.node.Push(yyS[yypt-0].node)
		}
	case 52:
		//line sql.y:312
		{
			yyVAL.node = NewSimpleParseNode(TABLE_EXPR, "")
			yyVAL.node.Push(yyS[yypt-3].node)
			yyVAL.node.Push(NewSimpleParseNode(NODE_LIST, "node_list").Push(yyS[yypt-1].node))
			yyVAL.node.Push(yyS[yypt-0].node)
		}
	case 53:
		//line sql.y:319
		{
			yyVAL.node = yyS[yypt-1].node.PushTwo(yyS[yypt-2].node, yyS[yypt-0].node)
		}
	case 54:
		//line sql.y:323
		{
			yyVAL.node = yyS[yypt-3].node
			yyVAL.node.Push(yyS[yypt-4].node)
			yyVAL.node.Push(yyS[yypt-2].node)
			yyVAL.node.Push(yyS[yypt-0].node)
		}
	case 55:
		yyVAL.node = yyS[yypt-0].node
	case 56:
		yyVAL.node = yyS[yypt-0].node
	case 57:
		//line sql.y:334
		{
			yyVAL.node = NewSimpleParseNode(LEFT, "left join")
		}
	case 58:
		//line sql.y:338
		{
			yyVAL.node = NewSimpleParseNode(LEFT, "left join")
		}
	case 59:
		//line sql.y:342
		{
			yyVAL.node = NewSimpleParseNode(RIGHT, "right join")
		}
	case 60:
		//line sql.y:346
		{
			yyVAL.node = NewSimpleParseNode(RIGHT, "right join")
		}
	case 61:
		//line sql.y:350
		{
			yyVAL.node = yyS[yypt-0].node
		}
	case 62:
		//line sql.y:354
		{
			yyVAL.node = NewSimpleParseNode(CROSS, "cross join")
		}
	case 63:
		//line sql.y:358
		{
			yyVAL.node = NewSimpleParseNode(NATURAL, "natural join")
		}
	case 64:
		yyVAL.node = yyS[yypt-0].node
	case 65:
		//line sql.y:365
		{
			yyVAL.node = yyS[yypt-1].node.PushTwo(yyS[yypt-2].node, yyS[yypt-0].node)
		}
	case 66:
		//line sql.y:369
		{
			yyVAL.node = yyS[yypt-2].node.Push(yyS[yypt-1].statement)
		}
	case 67:
		yyVAL.node = yyS[yypt-0].node
	case 68:
		//line sql.y:376
		{
			yyVAL.node = yyS[yypt-1].node.PushTwo(yyS[yypt-2].node, yyS[yypt-0].node)
		}
	case 69:
		//line sql.y:381
		{
			yyVAL.node = NewSimpleParseNode(USE, "use")
		}
	case 70:
		//line sql.y:385
		{
			yyVAL.node.Push(yyS[yypt-1].node)
		}
	case 71:
		//line sql.y:389
		{
			yyVAL.node.Push(yyS[yypt-1].node)
		}
	case 72:
		//line sql.y:394
		{
			yyVAL.node = NewSimpleParseNode(WHERE, "where")
		}
	case 73:
		//line sql.y:398
		{
			yyVAL.node = yyS[yypt-1].node.Push(yyS[yypt-0].node)
		}
	case 74:
		yyVAL.node = yyS[yypt-0].node
	case 75:
		//line sql.y:405
		{
			yyVAL.node = yyS[yypt-1].node.PushTwo(yyS[yypt-2].node, yyS[yypt-0].node)
		}
	case 76:
		//line sql.y:409
		{
			yyVAL.node = yyS[yypt-1].node.PushTwo(yyS[yypt-2].node, yyS[yypt-0].node)
		}
	case 77:
		//line sql.y:413
		{
			yyVAL.node = yyS[yypt-1].node.Push(yyS[yypt-0].node)
		}
	case 78:
		//line sql.y:417
		{
			yyVAL.node = yyS[yypt-2].node.Push(yyS[yypt-1].node)
		}
	case 79:
		//line sql.y:423
		{
			yyVAL.node = yyS[yypt-1].node.PushTwo(yyS[yypt-2].node, yyS[yypt-0].node)
		}
	case 80:
		//line sql.y:427
		{
			yyVAL.node = yyS[yypt-1].node.PushTwo(yyS[yypt-2].node, yyS[yypt-0].node)
		}
	case 81:
		//line sql.y:431
		{
			yyVAL.node = NewSimpleParseNode(NOT_IN, "not in").PushTwo(yyS[yypt-3].node, yyS[yypt-0].node)
		}
	case 82:
		//line sql.y:435
		{
			yyVAL.node = yyS[yypt-1].node.PushTwo(yyS[yypt-2].node, yyS[yypt-0].node)
		}
	case 83:
		//line sql.y:439
		{
			yyVAL.node = NewSimpleParseNode(NOT_LIKE, "not like").PushTwo(yyS[yypt-3].node, yyS[yypt-0].node)
		}
	case 84:
		//line sql.y:443
		{
			yyVAL.node = yyS[yypt-3].node
			yyVAL.node.Push(yyS[yypt-4].node)
			yyVAL.node.Push(yyS[yypt-2].node)
			yyVAL.node.Push(yyS[yypt-0].node)
		}
	case 85:
		//line sql.y:450
		{
			yyVAL.node = NewSimpleParseNode(NOT_BETWEEN, "not between")
			yyVAL.node.Push(yyS[yypt-5].node)
			yyVAL.node.Push(yyS[yypt-2].node)
			yyVAL.node.Push(yyS[yypt-0].node)
		}
	case 86:
		//line sql.y:457
		{
			yyVAL.node = NewSimpleParseNode(IS_NULL, "is null").Push(yyS[yypt-2].node)
		}
	case 87:
		//line sql.y:461
		{
			yyVAL.node = NewSimpleParseNode(IS_NOT_NULL, "is not null").Push(yyS[yypt-3].node)
		}
	case 88:
		//line sql.y:465
		{
			yyVAL.node = yyS[yypt-3].node.Push(yyS[yypt-1].statement)
		}
	case 89:
		yyVAL.node = yyS[yypt-0].node
	case 90:
		yyVAL.node = yyS[yypt-0].node
	case 91:
		yyVAL.node = yyS[yypt-0].node
	case 92:
		yyVAL.node = yyS[yypt-0].node
	case 93:
		yyVAL.node = yyS[yypt-0].node
	case 94:
		yyVAL.node = yyS[yypt-0].node
	case 95:
		yyVAL.node = yyS[yypt-0].node
	case 96:
		//line sql.y:480
		{
			yyVAL.sqlNode = yyS[yypt-1].node.Push(yyS[yypt-0].node)
		}
	case 97:
		//line sql.y:484
		{
			yyVAL.sqlNode = yyS[yypt-0].statement
		}
	case 98:
		//line sql.y:490
		{
			yyVAL.node = NewSimpleParseNode(NODE_LIST, "node_list")
			yyVAL.node.Push(yyS[yypt-0].node)
		}
	case 99:
		//line sql.y:495
		{
			yyVAL.node.Push(yyS[yypt-0].node)
		}
	case 100:
		//line sql.y:501
		{
			yyVAL.node = yyS[yypt-2].node.Push(yyS[yypt-1].node)
		}
	case 101:
		//line sql.y:505
		{
			yyVAL.node = yyS[yypt-2].node.Push(yyS[yypt-1].statement)
		}
	case 102:
		//line sql.y:511
		{
			yyVAL.node = NewSimpleParseNode(NODE_LIST, "node_list")
			yyVAL.node.Push(yyS[yypt-0].node)
		}
	case 103:
		//line sql.y:516
		{
			yyVAL.node.Push(yyS[yypt-0].node)
		}
	case 104:
		yyVAL.node = yyS[yypt-0].node
	case 105:
		yyVAL.node = yyS[yypt-0].node
	case 106:
		//line sql.y:524
		{
			yyVAL.node = yyS[yypt-2].node.Push(yyS[yypt-1].statement)
		}
	case 107:
		//line sql.y:528
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
	case 108:
		//line sql.y:540
		{
			yyVAL.node = yyS[yypt-1].node.PushTwo(yyS[yypt-2].node, yyS[yypt-0].node)
		}
	case 109:
		//line sql.y:544
		{
			yyVAL.node = yyS[yypt-1].node.PushTwo(yyS[yypt-2].node, yyS[yypt-0].node)
		}
	case 110:
		//line sql.y:548
		{
			yyVAL.node = yyS[yypt-1].node.PushTwo(yyS[yypt-2].node, yyS[yypt-0].node)
		}
	case 111:
		//line sql.y:552
		{
			yyVAL.node = yyS[yypt-1].node.PushTwo(yyS[yypt-2].node, yyS[yypt-0].node)
		}
	case 112:
		//line sql.y:556
		{
			yyVAL.node = yyS[yypt-1].node.PushTwo(yyS[yypt-2].node, yyS[yypt-0].node)
		}
	case 113:
		//line sql.y:560
		{
			yyVAL.node = yyS[yypt-1].node.PushTwo(yyS[yypt-2].node, yyS[yypt-0].node)
		}
	case 114:
		//line sql.y:564
		{
			yyVAL.node = yyS[yypt-1].node.PushTwo(yyS[yypt-2].node, yyS[yypt-0].node)
		}
	case 115:
		//line sql.y:568
		{
			yyVAL.node = yyS[yypt-1].node.PushTwo(yyS[yypt-2].node, yyS[yypt-0].node)
		}
	case 116:
		//line sql.y:572
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
	case 117:
		//line sql.y:588
		{
			yyS[yypt-2].node.Type = FUNCTION
			yyVAL.node = yyS[yypt-2].node.Push(NewSimpleParseNode(NODE_LIST, "node_list"))
		}
	case 118:
		//line sql.y:593
		{
			yyS[yypt-3].node.Type = FUNCTION
			yyVAL.node = yyS[yypt-3].node.Push(yyS[yypt-1].selectExprs)
		}
	case 119:
		//line sql.y:598
		{
			yyS[yypt-4].node.Type = FUNCTION
			yyVAL.node = yyS[yypt-4].node.Push(yyS[yypt-2].node)
			yyVAL.node = yyS[yypt-4].node.Push(yyS[yypt-1].selectExprs)
		}
	case 120:
		//line sql.y:604
		{
			yyS[yypt-3].node.Type = FUNCTION
			yyVAL.node = yyS[yypt-3].node.Push(yyS[yypt-1].selectExprs)
		}
	case 121:
		yyVAL.node = yyS[yypt-0].node
	case 122:
		yyVAL.node = yyS[yypt-0].node
	case 123:
		yyVAL.node = yyS[yypt-0].node
	case 124:
		//line sql.y:616
		{
			yyVAL.node = NewSimpleParseNode(UPLUS, "+")
		}
	case 125:
		//line sql.y:620
		{
			yyVAL.node = NewSimpleParseNode(UMINUS, "-")
		}
	case 126:
		yyVAL.node = yyS[yypt-0].node
	case 127:
		//line sql.y:627
		{
			yyVAL.node = NewSimpleParseNode(CASE_WHEN, "case")
			yyVAL.node.Push(yyS[yypt-1].node)
		}
	case 128:
		//line sql.y:632
		{
			yyVAL.node.PushTwo(yyS[yypt-2].node, yyS[yypt-1].node)
		}
	case 129:
		//line sql.y:638
		{
			yyVAL.node = NewSimpleParseNode(WHEN_LIST, "when_list")
			yyVAL.node.Push(yyS[yypt-0].node)
		}
	case 130:
		//line sql.y:643
		{
			yyVAL.node.Push(yyS[yypt-0].node)
		}
	case 131:
		//line sql.y:649
		{
			yyVAL.node.PushTwo(yyS[yypt-2].node, yyS[yypt-0].node)
		}
	case 132:
		//line sql.y:653
		{
			yyVAL.node.Push(yyS[yypt-0].node)
		}
	case 133:
		yyVAL.node = yyS[yypt-0].node
	case 134:
		//line sql.y:660
		{
			yyVAL.node = yyS[yypt-1].node.PushTwo(yyS[yypt-2].node, yyS[yypt-0].node)
		}
	case 135:
		yyVAL.node = yyS[yypt-0].node
	case 136:
		yyVAL.node = yyS[yypt-0].node
	case 137:
		yyVAL.node = yyS[yypt-0].node
	case 138:
		yyVAL.node = yyS[yypt-0].node
	case 139:
		//line sql.y:671
		{
			yyVAL.node = NewSimpleParseNode(GROUP, "group")
		}
	case 140:
		//line sql.y:675
		{
			yyVAL.node = yyS[yypt-2].node.Push(yyS[yypt-0].node)
		}
	case 141:
		//line sql.y:680
		{
			yyVAL.node = NewSimpleParseNode(HAVING, "having")
		}
	case 142:
		//line sql.y:684
		{
			yyVAL.node = yyS[yypt-1].node.Push(yyS[yypt-0].node)
		}
	case 143:
		//line sql.y:689
		{
			yyVAL.node = NewSimpleParseNode(ORDER, "order")
		}
	case 144:
		//line sql.y:693
		{
			yyVAL.node = yyS[yypt-2].node.Push(yyS[yypt-0].node)
		}
	case 145:
		//line sql.y:699
		{
			yyVAL.node = NewSimpleParseNode(NODE_LIST, "node_list")
			yyVAL.node.Push(yyS[yypt-0].node)
		}
	case 146:
		//line sql.y:704
		{
			yyVAL.node = yyS[yypt-2].node.Push(yyS[yypt-0].node)
		}
	case 147:
		//line sql.y:710
		{
			yyVAL.node = yyS[yypt-0].node.Push(yyS[yypt-1].node)
		}
	case 148:
		//line sql.y:715
		{
			yyVAL.node = NewSimpleParseNode(ASC, "asc")
		}
	case 149:
		yyVAL.node = yyS[yypt-0].node
	case 150:
		yyVAL.node = yyS[yypt-0].node
	case 151:
		//line sql.y:722
		{
			yyVAL.node = NewSimpleParseNode(LIMIT, "limit")
		}
	case 152:
		//line sql.y:726
		{
			yyVAL.node = yyS[yypt-1].node.Push(yyS[yypt-0].node)
		}
	case 153:
		//line sql.y:730
		{
			yyVAL.node = yyS[yypt-3].node.PushTwo(yyS[yypt-2].node, yyS[yypt-0].node)
		}
	case 154:
		//line sql.y:735
		{
			yyVAL.node = NewSimpleParseNode(NO_LOCK, "")
		}
	case 155:
		//line sql.y:739
		{
			yyVAL.node = NewSimpleParseNode(FOR_UPDATE, " for update")
		}
	case 156:
		//line sql.y:743
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
	case 157:
		//line sql.y:756
		{
			yyVAL.columns = nil
		}
	case 158:
		//line sql.y:760
		{
			yyVAL.columns = yyS[yypt-1].columns
		}
	case 159:
		//line sql.y:766
		{
			yyVAL.columns = Columns{&NonStarExpr{Expr: yyS[yypt-0].node}}
		}
	case 160:
		//line sql.y:770
		{
			yyVAL.columns = append(yyVAL.columns, &NonStarExpr{Expr: yyS[yypt-0].node})
		}
	case 161:
		//line sql.y:776
		{
			yyVAL.node = NewSimpleParseNode(INDEX_LIST, "")
			yyVAL.node.Push(yyS[yypt-0].node)
		}
	case 162:
		//line sql.y:781
		{
			yyVAL.node = yyS[yypt-2].node.Push(yyS[yypt-0].node)
		}
	case 163:
		//line sql.y:786
		{
			yyVAL.node = NewSimpleParseNode(DUPLICATE, "duplicate")
		}
	case 164:
		//line sql.y:790
		{
			yyVAL.node = yyS[yypt-3].node.Push(yyS[yypt-0].node)
		}
	case 165:
		//line sql.y:796
		{
			yyVAL.node = NewSimpleParseNode(NODE_LIST, "node_list")
			yyVAL.node.Push(yyS[yypt-0].node)
		}
	case 166:
		//line sql.y:801
		{
			yyVAL.node = yyS[yypt-2].node.Push(yyS[yypt-0].node)
		}
	case 167:
		//line sql.y:807
		{
			yyVAL.node = yyS[yypt-1].node.PushTwo(yyS[yypt-2].node, yyS[yypt-0].node)
		}
	case 168:
		//line sql.y:812
		{
			yyVAL.node = nil
		}
	case 169:
		yyVAL.node = yyS[yypt-0].node
	case 170:
		//line sql.y:816
		{
			yyVAL.node = nil
		}
	case 171:
		yyVAL.node = yyS[yypt-0].node
	case 172:
		//line sql.y:820
		{
			yyVAL.node = nil
		}
	case 173:
		yyVAL.node = yyS[yypt-0].node
	case 174:
		yyVAL.node = yyS[yypt-0].node
	case 175:
		yyVAL.node = yyS[yypt-0].node
	case 176:
		yyVAL.node = yyS[yypt-0].node
	case 177:
		yyVAL.node = yyS[yypt-0].node
	case 178:
		yyVAL.node = yyS[yypt-0].node
	case 179:
		//line sql.y:831
		{
			yyVAL.node = nil
		}
	case 180:
		yyVAL.node = yyS[yypt-0].node
	case 181:
		//line sql.y:835
		{
			yyVAL.node = nil
		}
	case 182:
		yyVAL.node = yyS[yypt-0].node
	case 183:
		//line sql.y:839
		{
			yyVAL.node = nil
		}
	case 184:
		yyVAL.node = yyS[yypt-0].node
	case 185:
		//line sql.y:844
		{
			yyVAL.node.LowerCase()
		}
	case 186:
		//line sql.y:849
		{
			ForceEOF(yylex)
		}
	}
	goto yystack /* stack new state and value */
}
