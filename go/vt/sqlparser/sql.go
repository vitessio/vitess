//line sql.y:33
package sqlparser

import (
	"fmt"
)

func SetParseTree(yylex interface{}, root *Node) {
	tn := yylex.(*Tokenizer)
	tn.ParseTree = root
}

func SetAllowComments(yylex interface{}, allow bool) {
	tn := yylex.(*Tokenizer)
	tn.AllowComments = allow
}

func ForceEOF(yylex interface{}) {
	tn := yylex.(*Tokenizer)
	tn.ForceEOF = true
}

// Offsets for select parse tree. These need to match the Push order in the select_statement rule.
const (
	SELECT_COMMENT_OFFSET = iota
	SELECT_DISTINCT_OFFSET
	SELECT_EXPR_OFFSET
	SELECT_FROM_OFFSET
	SELECT_WHERE_OFFSET
	SELECT_GROUP_OFFSET
	SELECT_HAVING_OFFSET
	SELECT_ORDER_OFFSET
	SELECT_LIMIT_OFFSET
	SELECT_FOR_UPDATE_OFFSET
)

const (
	INSERT_COMMENT_OFFSET = iota
	INSERT_TABLE_OFFSET
	INSERT_COLUMN_LIST_OFFSET
	INSERT_VALUES_OFFSET
	INSERT_ON_DUP_OFFSET
)

const (
	UPDATE_COMMENT_OFFSET = iota
	UPDATE_TABLE_OFFSET
	UPDATE_LIST_OFFSET
	UPDATE_WHERE_OFFSET
	UPDATE_ORDER_OFFSET
	UPDATE_LIMIT_OFFSET
)

const (
	DELETE_COMMENT_OFFSET = iota
	DELETE_TABLE_OFFSET
	DELETE_WHERE_OFFSET
	DELETE_ORDER_OFFSET
	DELETE_LIMIT_OFFSET
)

//line sql.y:95
type yySymType struct {
	yys  int
	node *Node
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
const ID = 57376
const STRING = 57377
const NUMBER = 57378
const VALUE_ARG = 57379
const LE = 57380
const GE = 57381
const NE = 57382
const NULL_SAFE_EQUAL = 57383
const LEX_ERROR = 57384
const UNION = 57385
const MINUS = 57386
const EXCEPT = 57387
const INTERSECT = 57388
const JOIN = 57389
const STRAIGHT_JOIN = 57390
const LEFT = 57391
const RIGHT = 57392
const INNER = 57393
const OUTER = 57394
const CROSS = 57395
const NATURAL = 57396
const USE = 57397
const ON = 57398
const AND = 57399
const OR = 57400
const NOT = 57401
const UNARY = 57402
const CREATE = 57403
const ALTER = 57404
const DROP = 57405
const RENAME = 57406
const TABLE = 57407
const INDEX = 57408
const TO = 57409
const IGNORE = 57410
const IF = 57411
const UNIQUE = 57412
const USING = 57413
const NODE_LIST = 57414
const UPLUS = 57415
const UMINUS = 57416
const SELECT_STAR = 57417
const NO_DISTINCT = 57418
const FUNCTION = 57419
const FOR_UPDATE = 57420
const NOT_FOR_UPDATE = 57421
const NOT_IN = 57422
const NOT_LIKE = 57423
const NOT_BETWEEN = 57424
const IS_NULL = 57425
const IS_NOT_NULL = 57426
const UNION_ALL = 57427
const COMMENT_LIST = 57428
const COLUMN_LIST = 57429
const TABLE_EXPR = 57430

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
	"CREATE",
	"ALTER",
	"DROP",
	"RENAME",
	"TABLE",
	"INDEX",
	"TO",
	"IGNORE",
	"IF",
	"UNIQUE",
	"USING",
	"NODE_LIST",
	"UPLUS",
	"UMINUS",
	"SELECT_STAR",
	"NO_DISTINCT",
	"FUNCTION",
	"FOR_UPDATE",
	"NOT_FOR_UPDATE",
	"NOT_IN",
	"NOT_LIKE",
	"NOT_BETWEEN",
	"IS_NULL",
	"IS_NOT_NULL",
	"UNION_ALL",
	"COMMENT_LIST",
	"COLUMN_LIST",
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
	-1, 62,
	34, 43,
	-2, 38,
	-1, 160,
	34, 43,
	-2, 64,
}

const yyNprod = 170
const yyPrivate = 57344

var yyTokenNames []string
var yyStates []string

const yyLast = 474

var yyAct = []int{

	65, 48, 279, 200, 247, 131, 203, 172, 224, 158,
	144, 130, 3, 245, 99, 192, 64, 147, 57, 59,
	140, 52, 214, 215, 216, 217, 218, 98, 219, 220,
	103, 104, 60, 40, 22, 23, 24, 25, 49, 245,
	62, 168, 36, 73, 22, 23, 24, 25, 77, 98,
	98, 82, 22, 23, 24, 25, 32, 63, 74, 75,
	76, 34, 210, 93, 266, 307, 68, 269, 127, 132,
	80, 190, 133, 263, 22, 23, 24, 25, 90, 268,
	192, 38, 39, 53, 126, 129, 54, 139, 67, 37,
	270, 244, 78, 79, 61, 116, 117, 118, 285, 227,
	264, 238, 228, 102, 127, 127, 171, 81, 239, 177,
	178, 241, 181, 182, 183, 184, 185, 186, 187, 188,
	169, 170, 167, 179, 100, 153, 142, 166, 165, 143,
	191, 162, 193, 102, 189, 194, 103, 104, 12, 13,
	14, 15, 127, 97, 206, 151, 96, 154, 196, 199,
	114, 115, 116, 117, 118, 195, 226, 258, 205, 208,
	202, 287, 259, 180, 101, 164, 226, 16, 256, 221,
	286, 222, 262, 257, 194, 225, 233, 234, 211, 230,
	261, 73, 232, 260, 141, 231, 77, 98, 229, 82,
	150, 152, 149, 237, 87, 63, 74, 75, 76, 192,
	293, 294, 274, 141, 68, 173, 243, 88, 80, 246,
	17, 18, 20, 19, 111, 112, 113, 114, 115, 116,
	117, 118, 254, 255, 296, 297, 67, 212, 289, 50,
	78, 79, 61, 138, 271, 222, 272, 111, 112, 113,
	114, 115, 116, 117, 118, 81, 87, 136, 135, 277,
	280, 276, 111, 112, 113, 114, 115, 116, 117, 118,
	201, 281, 275, 267, 111, 112, 113, 114, 115, 116,
	117, 118, 290, 265, 288, 22, 23, 24, 25, 12,
	12, 251, 292, 161, 127, 194, 127, 250, 165, 298,
	300, 207, 223, 302, 304, 280, 73, 305, 161, 163,
	299, 77, 301, 306, 82, 308, 156, 159, 155, 161,
	128, 74, 75, 76, 94, 85, 73, 92, 223, 68,
	91, 77, 89, 80, 82, 86, 84, 58, 55, 46,
	128, 74, 75, 76, 291, 273, 45, 236, 12, 68,
	145, 67, 95, 80, 235, 78, 79, 111, 112, 113,
	114, 115, 116, 117, 118, 12, 100, 310, 43, 77,
	81, 67, 82, 41, 77, 78, 79, 82, 128, 74,
	75, 76, 83, 128, 74, 75, 76, 134, 248, 198,
	81, 80, 134, 284, 249, 204, 80, 283, 214, 215,
	216, 217, 218, 253, 219, 220, 106, 110, 108, 109,
	141, 47, 311, 78, 79, 303, 12, 27, 78, 79,
	146, 33, 26, 122, 123, 124, 125, 209, 81, 119,
	120, 121, 174, 81, 175, 176, 28, 29, 30, 31,
	148, 35, 51, 56, 137, 240, 309, 295, 278, 282,
	107, 111, 112, 113, 114, 115, 116, 117, 118, 252,
	69, 70, 71, 72, 242, 197, 105, 66, 160, 213,
	157, 42, 21, 44, 11, 10, 9, 8, 7, 6,
	5, 4, 2, 1,
}
var yyPact = []int{

	134, -1000, -1000, 227, -1000, -1000, -1000, -1000, -1000, -1000,
	-1000, -1000, -1000, -1000, -1000, -1000, -1000, -24, -41, 9,
	1, 402, 346, -1000, -1000, -1000, 340, -1000, 307, 295,
	393, 195, -63, 2, -1000, 6, -1000, 294, -66, 293,
	-1000, -1000, 161, -1000, 357, 292, 282, 291, 142, -1000,
	163, 288, 13, 286, 283, -19, 280, 322, 84, 135,
	-1000, -1000, 337, 90, 73, 375, -1000, 296, 276, -1000,
	-1000, 339, 205, 204, -1000, -1000, -1000, -1000, -1000, -1000,
	-1000, -1000, -1000, -1000, 190, 195, 391, 195, 296, -1000,
	320, -69, 113, 274, -1000, -1000, 272, 264, 161, 265,
	-1000, 94, 23, 296, 296, 339, 162, 401, 339, 339,
	98, 339, 339, 339, 339, 339, 339, 339, 339, -1000,
	-1000, -1000, -1000, -1000, -1000, -1000, -1000, 375, 60, -33,
	26, 28, 375, -1000, 334, 161, 402, 351, 226, 194,
	373, 296, -1000, -1000, -1000, -1000, 82, 257, -1000, -20,
	-1000, -1000, -1000, -1000, -1000, -1000, -1000, 175, 335, 275,
	105, 25, -1000, -1000, -1000, -1000, -1000, -2, 161, -1000,
	-1000, 186, -1000, 334, 162, 339, 339, 186, 281, -1000,
	312, 81, 81, 81, 24, 24, -1000, -1000, -1000, 254,
	-1000, -1000, 339, -1000, 186, -3, 4, 49, 162, 227,
	-13, -1000, 373, 364, 371, 73, 253, -1000, -1000, 247,
	-1000, 383, 249, 249, -1000, -1000, 115, 104, 130, 127,
	119, -31, -4, 402, -1000, 239, -17, 229, -1000, -25,
	-37, -14, -1000, 186, 171, 339, -1000, 186, -1000, -1000,
	-1000, 305, 150, -1000, -1000, 228, 364, -1000, 339, 339,
	-1000, -1000, 376, 370, 335, 36, -1000, 117, -1000, 108,
	-1000, -1000, -1000, -1000, -1000, 95, 185, -1000, -1000, -1000,
	-1000, 339, 186, 303, 162, -1000, -1000, 148, 149, -1000,
	198, -1000, 373, 296, 339, 296, -1000, -1000, -1000, 226,
	186, 399, -1000, 339, 339, -1000, -1000, -1000, 364, 73,
	147, 73, -39, 195, 186, -1000, 341, -1000, 142, -1000,
	396, -1000,
}
var yyPgo = []int{

	0, 473, 472, 11, 471, 470, 469, 468, 467, 466,
	465, 464, 412, 463, 462, 461, 19, 32, 40, 14,
	460, 9, 459, 458, 8, 20, 16, 457, 456, 455,
	454, 7, 5, 0, 453, 452, 451, 450, 449, 439,
	6, 438, 2, 437, 4, 436, 435, 434, 3, 1,
	38, 433, 432, 431, 430, 417, 411, 410, 10, 407,
}
var yyR1 = []int{

	0, 1, 2, 2, 2, 2, 2, 2, 2, 2,
	2, 3, 3, 4, 5, 6, 7, 8, 8, 9,
	9, 10, 11, 11, 59, 12, 13, 13, 14, 14,
	14, 14, 14, 15, 15, 16, 16, 17, 17, 17,
	17, 18, 18, 19, 19, 20, 20, 20, 21, 21,
	21, 21, 22, 22, 22, 22, 22, 22, 22, 22,
	22, 23, 23, 23, 24, 24, 25, 25, 26, 26,
	26, 26, 26, 27, 27, 27, 27, 27, 27, 27,
	27, 27, 27, 28, 28, 28, 28, 28, 28, 28,
	29, 29, 30, 30, 31, 31, 32, 32, 33, 33,
	33, 33, 33, 33, 33, 33, 33, 33, 33, 33,
	33, 33, 33, 33, 33, 34, 34, 35, 35, 35,
	36, 36, 37, 37, 37, 37, 38, 38, 39, 39,
	40, 40, 41, 41, 42, 43, 43, 43, 44, 44,
	44, 45, 45, 47, 47, 48, 48, 46, 46, 49,
	49, 50, 51, 51, 52, 52, 53, 53, 54, 54,
	54, 54, 54, 55, 55, 56, 56, 57, 57, 58,
}
var yyR2 = []int{

	0, 1, 1, 1, 1, 1, 1, 1, 1, 1,
	1, 12, 3, 7, 8, 7, 3, 5, 8, 6,
	7, 5, 4, 5, 0, 2, 0, 2, 1, 2,
	1, 1, 1, 0, 1, 1, 3, 1, 1, 3,
	3, 1, 1, 0, 1, 1, 3, 3, 2, 4,
	3, 5, 1, 1, 2, 3, 2, 3, 2, 2,
	2, 1, 3, 3, 0, 5, 0, 2, 1, 3,
	3, 2, 3, 3, 3, 4, 3, 4, 5, 6,
	3, 4, 4, 1, 1, 1, 1, 1, 1, 1,
	2, 1, 1, 3, 3, 3, 1, 3, 1, 1,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	2, 3, 4, 5, 4, 1, 1, 1, 1, 1,
	1, 3, 1, 1, 1, 1, 0, 3, 0, 2,
	0, 3, 1, 3, 2, 0, 1, 1, 0, 2,
	4, 0, 2, 0, 3, 1, 3, 0, 5, 1,
	3, 3, 0, 2, 0, 3, 0, 1, 1, 1,
	1, 1, 1, 0, 1, 0, 1, 0, 2, 0,
}
var yyChk = []int{

	-1000, -1, -2, -3, -4, -5, -6, -7, -8, -9,
	-10, -11, 4, 5, 6, 7, 33, 76, 77, 79,
	78, -14, 48, 49, 50, 51, -12, -59, -12, -12,
	-12, -12, 80, -56, 85, -53, 83, 80, 80, 81,
	-3, 17, -15, 18, -13, 29, 34, 8, -49, -50,
	34, -52, 84, 81, 80, 34, -51, 84, 34, -16,
	-17, 71, -18, 34, -26, -33, -27, 65, 43, -37,
	-36, -35, -34, 20, 35, 36, 37, 25, 69, 70,
	47, 84, 28, 15, 34, 33, 34, 52, 44, 34,
	65, 34, 34, 82, 34, 20, 62, 8, 52, -19,
	19, 74, 43, 63, 64, -28, 21, 65, 23, 24,
	22, 66, 67, 68, 69, 70, 71, 72, 73, 44,
	45, 46, 38, 39, 40, 41, -26, -33, 34, -26,
	-3, -32, -33, -33, 43, 43, 43, -47, 43, -49,
	-25, 9, -50, -18, -58, 20, -57, 86, -54, 79,
	77, 32, 78, 12, 34, 34, 34, -20, -21, 43,
	-23, 34, -17, 34, 71, 34, 104, -16, 18, -26,
	-26, -33, -31, 43, 21, 23, 24, -33, -33, 25,
	65, -33, -33, -33, -33, -33, -33, -33, -33, 74,
	104, 104, 52, 104, -33, -16, -3, -29, 28, -3,
	-48, 34, -25, -40, 12, -26, 62, 34, -58, -55,
	82, -25, 52, -22, 53, 54, 55, 56, 57, 59,
	60, -21, -3, 43, -24, -19, 61, 74, 104, -16,
	-32, -3, -31, -33, -33, 63, 25, -33, 104, 104,
	-46, 62, -30, -31, 104, 52, -40, -44, 14, 13,
	34, 34, -38, 10, -21, -21, 53, 58, 53, 58,
	53, 53, 53, 104, 104, 34, 81, 34, 104, 104,
	104, 63, -33, 30, 52, 34, -44, -33, -41, -42,
	-33, -58, -39, 11, 13, 62, 53, 53, -24, 43,
	-33, 31, -31, 52, 52, -43, 26, 27, -40, -26,
	-32, -26, -48, 6, -33, -42, -44, 104, -49, -45,
	16, 6,
}
var yyDef = []int{

	0, -2, 1, 2, 3, 4, 5, 6, 7, 8,
	9, 10, 24, 24, 24, 24, 24, 165, 156, 0,
	0, 0, 28, 30, 31, 32, 33, 26, 0, 0,
	0, 0, 154, 0, 166, 0, 157, 0, 152, 0,
	12, 29, 0, 34, 25, 0, 0, 0, 16, 149,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	35, 37, -2, 120, 41, 42, 68, 0, 0, 98,
	99, 0, 0, 0, 122, 123, 124, 125, 117, 118,
	119, 115, 116, 27, 143, 0, 66, 0, 0, 169,
	0, 167, 0, 0, 22, 153, 0, 0, 0, 0,
	44, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 83,
	84, 85, 86, 87, 88, 89, 71, 0, 120, 0,
	0, 0, 96, 110, 0, 0, 0, 0, 0, 66,
	130, 0, 150, 151, 17, 155, 0, 0, 169, 163,
	158, 159, 160, 161, 162, 21, 23, 66, 45, 0,
	-2, 61, 36, 39, 40, 121, 111, 0, 0, 69,
	70, 73, 74, 0, 0, 0, 0, 76, 0, 80,
	0, 102, 103, 104, 105, 106, 107, 108, 109, 0,
	72, 100, 0, 101, 96, 0, 0, 147, 0, 91,
	0, 145, 130, 138, 0, 67, 0, 168, 19, 0,
	164, 126, 0, 0, 52, 53, 0, 0, 0, 0,
	0, 0, 0, 0, 48, 0, 0, 0, 112, 0,
	0, 0, 75, 77, 0, 0, 81, 97, 114, 82,
	13, 0, 90, 92, 144, 0, 138, 15, 0, 0,
	169, 20, 128, 0, 47, 50, 54, 0, 56, 0,
	58, 59, 60, 46, 63, 64, 0, 62, 113, 94,
	95, 0, 78, 0, 0, 146, 14, 139, 131, 132,
	135, 18, 130, 0, 0, 0, 55, 57, 49, 0,
	79, 0, 93, 0, 0, 134, 136, 137, 138, 129,
	127, 51, 0, 0, 140, 133, 141, 65, 148, 11,
	0, 142,
}
var yyTok1 = []int{

	1, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 73, 66, 3,
	43, 104, 71, 69, 52, 70, 74, 72, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	45, 44, 46, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 68, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 67, 3, 47,
}
var yyTok2 = []int{

	2, 3, 4, 5, 6, 7, 8, 9, 10, 11,
	12, 13, 14, 15, 16, 17, 18, 19, 20, 21,
	22, 23, 24, 25, 26, 27, 28, 29, 30, 31,
	32, 33, 34, 35, 36, 37, 38, 39, 40, 41,
	42, 48, 49, 50, 51, 53, 54, 55, 56, 57,
	58, 59, 60, 61, 62, 63, 64, 65, 75, 76,
	77, 78, 79, 80, 81, 82, 83, 84, 85, 86,
	87, 88, 89, 90, 91, 92, 93, 94, 95, 96,
	97, 98, 99, 100, 101, 102, 103,
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
	if c > 0 && c <= len(yyToknames) {
		if yyToknames[c-1] != "" {
			return yyToknames[c-1]
		}
	}
	return fmt.Sprintf("tok-%v", c)
}

func yyStatname(s int) string {
	if s >= 0 && s < len(yyStatenames) {
		if yyStatenames[s] != "" {
			return yyStatenames[s]
		}
	}
	return fmt.Sprintf("state-%v", s)
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
		fmt.Printf("lex %U %s\n", uint(char), yyTokname(c))
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
		fmt.Printf("char %v in %v\n", yyTokname(yychar), yyStatname(yystate))
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
				fmt.Printf("%s", yyStatname(yystate))
				fmt.Printf("saw %s\n", yyTokname(yychar))
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
					fmt.Printf("error recovery pops state %d\n", yyS[yyp].yys)
				}
				yyp--
			}
			/* there is no state on the stack with an error shift ... abort */
			goto ret1

		case 3: /* no shift yet; clobber input char */
			if yyDebug >= 2 {
				fmt.Printf("error recovery discards %s\n", yyTokname(yychar))
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
		fmt.Printf("reduce %v in:\n\t%v\n", yyn, yyStatname(yystate))
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
			SetParseTree(yylex, yyS[yypt-0].node)
		}
	case 2:
		yyVAL.node = yyS[yypt-0].node
	case 3:
		yyVAL.node = yyS[yypt-0].node
	case 4:
		yyVAL.node = yyS[yypt-0].node
	case 5:
		yyVAL.node = yyS[yypt-0].node
	case 6:
		yyVAL.node = yyS[yypt-0].node
	case 7:
		yyVAL.node = yyS[yypt-0].node
	case 8:
		yyVAL.node = yyS[yypt-0].node
	case 9:
		yyVAL.node = yyS[yypt-0].node
	case 10:
		yyVAL.node = yyS[yypt-0].node
	case 11:
		//line sql.y:164
		{
			yyVAL.node = yyS[yypt-11].node
			yyVAL.node.Push(yyS[yypt-10].node) // 0: comment_opt
			yyVAL.node.Push(yyS[yypt-9].node)  // 1: distinct_opt
			yyVAL.node.Push(yyS[yypt-8].node)  // 2: select_expression_list
			yyVAL.node.Push(yyS[yypt-6].node)  // 3: table_expression_list
			yyVAL.node.Push(yyS[yypt-5].node)  // 4: where_expression_opt
			yyVAL.node.Push(yyS[yypt-4].node)  // 5: group_by_opt
			yyVAL.node.Push(yyS[yypt-3].node)  // 6: having_opt
			yyVAL.node.Push(yyS[yypt-2].node)  // 7: order_by_opt
			yyVAL.node.Push(yyS[yypt-1].node)  // 8: limit_opt
			yyVAL.node.Push(yyS[yypt-0].node)  // 9: for_update_opt
		}
	case 12:
		//line sql.y:178
		{
			yyVAL.node = yyS[yypt-1].node.PushTwo(yyS[yypt-2].node, yyS[yypt-0].node)
		}
	case 13:
		//line sql.y:184
		{
			yyVAL.node = yyS[yypt-6].node
			yyVAL.node.Push(yyS[yypt-5].node) // 0: comment_opt
			yyVAL.node.Push(yyS[yypt-3].node) // 1: table_name
			yyVAL.node.Push(yyS[yypt-2].node) // 2: column_list_opt
			yyVAL.node.Push(yyS[yypt-1].node) // 3: values
			yyVAL.node.Push(yyS[yypt-0].node) // 4: on_dup_opt
		}
	case 14:
		//line sql.y:195
		{
			yyVAL.node = yyS[yypt-7].node
			yyVAL.node.Push(yyS[yypt-6].node) // 0: comment_opt
			yyVAL.node.Push(yyS[yypt-5].node) // 1: table_name
			yyVAL.node.Push(yyS[yypt-3].node) // 2: update_list
			yyVAL.node.Push(yyS[yypt-2].node) // 3: where_expression_opt
			yyVAL.node.Push(yyS[yypt-1].node) // 4: order_by_opt
			yyVAL.node.Push(yyS[yypt-0].node) // 5: limit_opt
		}
	case 15:
		//line sql.y:207
		{
			yyVAL.node = yyS[yypt-6].node
			yyVAL.node.Push(yyS[yypt-5].node) // 0: comment_opt
			yyVAL.node.Push(yyS[yypt-3].node) // 1: table_name
			yyVAL.node.Push(yyS[yypt-2].node) // 2: where_expression_opt
			yyVAL.node.Push(yyS[yypt-1].node) // 3: order_by_opt
			yyVAL.node.Push(yyS[yypt-0].node) // 4: limit_opt
		}
	case 16:
		//line sql.y:218
		{
			yyVAL.node = yyS[yypt-2].node
			yyVAL.node.Push(yyS[yypt-1].node)
			yyVAL.node.Push(yyS[yypt-0].node)
		}
	case 17:
		//line sql.y:226
		{
			yyVAL.node.Push(yyS[yypt-1].node)
		}
	case 18:
		//line sql.y:230
		{
			// Change this to an alter statement
			yyVAL.node = NewSimpleParseNode(ALTER, "alter")
			yyVAL.node.Push(yyS[yypt-1].node)
		}
	case 19:
		//line sql.y:238
		{
			yyVAL.node.Push(yyS[yypt-2].node)
		}
	case 20:
		//line sql.y:242
		{
			// Change this to a rename statement
			yyVAL.node = NewSimpleParseNode(RENAME, "rename")
			yyVAL.node.PushTwo(yyS[yypt-3].node, yyS[yypt-0].node)
		}
	case 21:
		//line sql.y:250
		{
			yyVAL.node.PushTwo(yyS[yypt-2].node, yyS[yypt-0].node)
		}
	case 22:
		//line sql.y:256
		{
			yyVAL.node.Push(yyS[yypt-0].node)
		}
	case 23:
		//line sql.y:260
		{
			// Change this to an alter statement
			yyVAL.node = NewSimpleParseNode(ALTER, "alter")
			yyVAL.node.Push(yyS[yypt-0].node)
		}
	case 24:
		//line sql.y:267
		{
			SetAllowComments(yylex, true)
		}
	case 25:
		//line sql.y:271
		{
			yyVAL.node = yyS[yypt-0].node
			SetAllowComments(yylex, false)
		}
	case 26:
		//line sql.y:277
		{
			yyVAL.node = NewSimpleParseNode(COMMENT_LIST, "")
		}
	case 27:
		//line sql.y:281
		{
			yyVAL.node = yyS[yypt-1].node.Push(yyS[yypt-0].node)
		}
	case 28:
		yyVAL.node = yyS[yypt-0].node
	case 29:
		//line sql.y:288
		{
			yyVAL.node = NewSimpleParseNode(UNION_ALL, "union all")
		}
	case 30:
		yyVAL.node = yyS[yypt-0].node
	case 31:
		yyVAL.node = yyS[yypt-0].node
	case 32:
		yyVAL.node = yyS[yypt-0].node
	case 33:
		//line sql.y:296
		{
			yyVAL.node = NewSimpleParseNode(NO_DISTINCT, "")
		}
	case 34:
		//line sql.y:300
		{
			yyVAL.node = NewSimpleParseNode(DISTINCT, "distinct")
		}
	case 35:
		//line sql.y:306
		{
			yyVAL.node = NewSimpleParseNode(NODE_LIST, "node_list")
			yyVAL.node.Push(yyS[yypt-0].node)
		}
	case 36:
		//line sql.y:311
		{
			yyVAL.node.Push(yyS[yypt-0].node)
		}
	case 37:
		//line sql.y:317
		{
			yyVAL.node = NewSimpleParseNode(SELECT_STAR, "*")
		}
	case 38:
		yyVAL.node = yyS[yypt-0].node
	case 39:
		//line sql.y:322
		{
			yyVAL.node = yyS[yypt-1].node.PushTwo(yyS[yypt-2].node, yyS[yypt-0].node)
		}
	case 40:
		//line sql.y:326
		{
			yyVAL.node = yyS[yypt-1].node.PushTwo(yyS[yypt-2].node, NewSimpleParseNode(SELECT_STAR, "*"))
		}
	case 41:
		yyVAL.node = yyS[yypt-0].node
	case 42:
		yyVAL.node = yyS[yypt-0].node
	case 43:
		//line sql.y:335
		{
			yyVAL.node = NewSimpleParseNode(AS, "as")
		}
	case 44:
		yyVAL.node = yyS[yypt-0].node
	case 45:
		//line sql.y:342
		{
			yyVAL.node = NewSimpleParseNode(NODE_LIST, "node_list")
			yyVAL.node.Push(yyS[yypt-0].node)
		}
	case 46:
		//line sql.y:347
		{
			yyVAL.node = yyS[yypt-2].node.Push(yyS[yypt-1].node)
		}
	case 47:
		//line sql.y:351
		{
			yyVAL.node.Push(yyS[yypt-0].node)
		}
	case 48:
		//line sql.y:357
		{
			yyVAL.node = NewSimpleParseNode(TABLE_EXPR, "")
			yyVAL.node.Push(yyS[yypt-1].node)
			yyVAL.node.Push(NewSimpleParseNode(NODE_LIST, "node_list"))
			yyVAL.node.Push(yyS[yypt-0].node)
		}
	case 49:
		//line sql.y:364
		{
			yyVAL.node = NewSimpleParseNode(TABLE_EXPR, "")
			yyVAL.node.Push(yyS[yypt-3].node)
			yyVAL.node.Push(NewSimpleParseNode(NODE_LIST, "node_list").Push(yyS[yypt-1].node))
			yyVAL.node.Push(yyS[yypt-0].node)
		}
	case 50:
		//line sql.y:371
		{
			yyVAL.node = yyS[yypt-1].node.PushTwo(yyS[yypt-2].node, yyS[yypt-0].node)
		}
	case 51:
		//line sql.y:375
		{
			yyVAL.node = yyS[yypt-3].node
			yyVAL.node.Push(yyS[yypt-4].node)
			yyVAL.node.Push(yyS[yypt-2].node)
			yyVAL.node.Push(yyS[yypt-0].node)
		}
	case 52:
		yyVAL.node = yyS[yypt-0].node
	case 53:
		yyVAL.node = yyS[yypt-0].node
	case 54:
		//line sql.y:386
		{
			yyVAL.node = NewSimpleParseNode(LEFT, "left join")
		}
	case 55:
		//line sql.y:390
		{
			yyVAL.node = NewSimpleParseNode(LEFT, "left join")
		}
	case 56:
		//line sql.y:394
		{
			yyVAL.node = NewSimpleParseNode(RIGHT, "right join")
		}
	case 57:
		//line sql.y:398
		{
			yyVAL.node = NewSimpleParseNode(RIGHT, "right join")
		}
	case 58:
		//line sql.y:402
		{
			yyVAL.node = yyS[yypt-0].node
		}
	case 59:
		//line sql.y:406
		{
			yyVAL.node = NewSimpleParseNode(CROSS, "cross join")
		}
	case 60:
		//line sql.y:410
		{
			yyVAL.node = NewSimpleParseNode(NATURAL, "natural join")
		}
	case 61:
		yyVAL.node = yyS[yypt-0].node
	case 62:
		//line sql.y:417
		{
			yyVAL.node = yyS[yypt-1].node.PushTwo(yyS[yypt-2].node, yyS[yypt-0].node)
		}
	case 63:
		//line sql.y:421
		{
			yyVAL.node = yyS[yypt-2].node.Push(yyS[yypt-1].node)
		}
	case 64:
		//line sql.y:426
		{
			yyVAL.node = NewSimpleParseNode(USE, "use")
		}
	case 65:
		//line sql.y:430
		{
			yyVAL.node.Push(yyS[yypt-1].node)
		}
	case 66:
		//line sql.y:435
		{
			yyVAL.node = NewSimpleParseNode(WHERE, "where")
		}
	case 67:
		//line sql.y:439
		{
			yyVAL.node = yyS[yypt-1].node.Push(yyS[yypt-0].node)
		}
	case 68:
		yyVAL.node = yyS[yypt-0].node
	case 69:
		//line sql.y:446
		{
			yyVAL.node = yyS[yypt-1].node.PushTwo(yyS[yypt-2].node, yyS[yypt-0].node)
		}
	case 70:
		//line sql.y:450
		{
			yyVAL.node = yyS[yypt-1].node.PushTwo(yyS[yypt-2].node, yyS[yypt-0].node)
		}
	case 71:
		//line sql.y:454
		{
			yyVAL.node = yyS[yypt-1].node.Push(yyS[yypt-0].node)
		}
	case 72:
		//line sql.y:458
		{
			yyVAL.node = yyS[yypt-2].node.Push(yyS[yypt-1].node)
		}
	case 73:
		//line sql.y:464
		{
			yyVAL.node = yyS[yypt-1].node.PushTwo(yyS[yypt-2].node, yyS[yypt-0].node)
		}
	case 74:
		//line sql.y:468
		{
			yyVAL.node = yyS[yypt-1].node.PushTwo(yyS[yypt-2].node, yyS[yypt-0].node)
		}
	case 75:
		//line sql.y:472
		{
			yyVAL.node = NewSimpleParseNode(NOT_IN, "not in").PushTwo(yyS[yypt-3].node, yyS[yypt-0].node)
		}
	case 76:
		//line sql.y:476
		{
			yyVAL.node = yyS[yypt-1].node.PushTwo(yyS[yypt-2].node, yyS[yypt-0].node)
		}
	case 77:
		//line sql.y:480
		{
			yyVAL.node = NewSimpleParseNode(NOT_LIKE, "not like").PushTwo(yyS[yypt-3].node, yyS[yypt-0].node)
		}
	case 78:
		//line sql.y:484
		{
			yyVAL.node = yyS[yypt-3].node
			yyVAL.node.Push(yyS[yypt-4].node)
			yyVAL.node.Push(yyS[yypt-2].node)
			yyVAL.node.Push(yyS[yypt-0].node)
		}
	case 79:
		//line sql.y:491
		{
			yyVAL.node = NewSimpleParseNode(NOT_BETWEEN, "not between")
			yyVAL.node.Push(yyS[yypt-5].node)
			yyVAL.node.Push(yyS[yypt-2].node)
			yyVAL.node.Push(yyS[yypt-0].node)
		}
	case 80:
		//line sql.y:498
		{
			yyVAL.node = NewSimpleParseNode(IS_NULL, "is null").Push(yyS[yypt-2].node)
		}
	case 81:
		//line sql.y:502
		{
			yyVAL.node = NewSimpleParseNode(IS_NOT_NULL, "is not null").Push(yyS[yypt-3].node)
		}
	case 82:
		//line sql.y:506
		{
			yyVAL.node = yyS[yypt-3].node.Push(yyS[yypt-1].node)
		}
	case 83:
		yyVAL.node = yyS[yypt-0].node
	case 84:
		yyVAL.node = yyS[yypt-0].node
	case 85:
		yyVAL.node = yyS[yypt-0].node
	case 86:
		yyVAL.node = yyS[yypt-0].node
	case 87:
		yyVAL.node = yyS[yypt-0].node
	case 88:
		yyVAL.node = yyS[yypt-0].node
	case 89:
		yyVAL.node = yyS[yypt-0].node
	case 90:
		//line sql.y:521
		{
			yyVAL.node = yyS[yypt-1].node.Push(yyS[yypt-0].node)
		}
	case 91:
		yyVAL.node = yyS[yypt-0].node
	case 92:
		//line sql.y:528
		{
			yyVAL.node = NewSimpleParseNode(NODE_LIST, "node_list")
			yyVAL.node.Push(yyS[yypt-0].node)
		}
	case 93:
		//line sql.y:533
		{
			yyVAL.node.Push(yyS[yypt-0].node)
		}
	case 94:
		//line sql.y:539
		{
			yyVAL.node = yyS[yypt-2].node.Push(yyS[yypt-1].node)
		}
	case 95:
		//line sql.y:543
		{
			yyVAL.node = yyS[yypt-2].node.Push(yyS[yypt-1].node)
		}
	case 96:
		//line sql.y:549
		{
			yyVAL.node = NewSimpleParseNode(NODE_LIST, "node_list")
			yyVAL.node.Push(yyS[yypt-0].node)
		}
	case 97:
		//line sql.y:554
		{
			yyVAL.node.Push(yyS[yypt-0].node)
		}
	case 98:
		yyVAL.node = yyS[yypt-0].node
	case 99:
		yyVAL.node = yyS[yypt-0].node
	case 100:
		//line sql.y:562
		{
			yyVAL.node = yyS[yypt-2].node.Push(yyS[yypt-1].node)
		}
	case 101:
		//line sql.y:566
		{
			if yyS[yypt-1].node.Len() == 1 {
				yyS[yypt-1].node = yyS[yypt-1].node.At(0)
			}
			switch yyS[yypt-1].node.Type {
			case NUMBER, STRING, ID, VALUE_ARG, '(', '.':
				yyVAL.node = yyS[yypt-1].node
			default:
				yyVAL.node = yyS[yypt-2].node.Push(yyS[yypt-1].node)
			}
		}
	case 102:
		//line sql.y:578
		{
			yyVAL.node = yyS[yypt-1].node.PushTwo(yyS[yypt-2].node, yyS[yypt-0].node)
		}
	case 103:
		//line sql.y:582
		{
			yyVAL.node = yyS[yypt-1].node.PushTwo(yyS[yypt-2].node, yyS[yypt-0].node)
		}
	case 104:
		//line sql.y:586
		{
			yyVAL.node = yyS[yypt-1].node.PushTwo(yyS[yypt-2].node, yyS[yypt-0].node)
		}
	case 105:
		//line sql.y:590
		{
			yyVAL.node = yyS[yypt-1].node.PushTwo(yyS[yypt-2].node, yyS[yypt-0].node)
		}
	case 106:
		//line sql.y:594
		{
			yyVAL.node = yyS[yypt-1].node.PushTwo(yyS[yypt-2].node, yyS[yypt-0].node)
		}
	case 107:
		//line sql.y:598
		{
			yyVAL.node = yyS[yypt-1].node.PushTwo(yyS[yypt-2].node, yyS[yypt-0].node)
		}
	case 108:
		//line sql.y:602
		{
			yyVAL.node = yyS[yypt-1].node.PushTwo(yyS[yypt-2].node, yyS[yypt-0].node)
		}
	case 109:
		//line sql.y:606
		{
			yyVAL.node = yyS[yypt-1].node.PushTwo(yyS[yypt-2].node, yyS[yypt-0].node)
		}
	case 110:
		//line sql.y:610
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
	case 111:
		//line sql.y:626
		{
			yyS[yypt-2].node.Type = FUNCTION
			yyVAL.node = yyS[yypt-2].node.Push(NewSimpleParseNode(NODE_LIST, "node_list"))
		}
	case 112:
		//line sql.y:631
		{
			yyS[yypt-3].node.Type = FUNCTION
			yyVAL.node = yyS[yypt-3].node.Push(yyS[yypt-1].node)
		}
	case 113:
		//line sql.y:636
		{
			yyS[yypt-4].node.Type = FUNCTION
			yyVAL.node = yyS[yypt-4].node.Push(yyS[yypt-2].node)
			yyVAL.node = yyS[yypt-4].node.Push(yyS[yypt-1].node)
		}
	case 114:
		//line sql.y:642
		{
			yyS[yypt-3].node.Type = FUNCTION
			yyVAL.node = yyS[yypt-3].node.Push(yyS[yypt-1].node)
		}
	case 115:
		yyVAL.node = yyS[yypt-0].node
	case 116:
		yyVAL.node = yyS[yypt-0].node
	case 117:
		//line sql.y:653
		{
			yyVAL.node = NewSimpleParseNode(UPLUS, "+")
		}
	case 118:
		//line sql.y:657
		{
			yyVAL.node = NewSimpleParseNode(UMINUS, "-")
		}
	case 119:
		yyVAL.node = yyS[yypt-0].node
	case 120:
		yyVAL.node = yyS[yypt-0].node
	case 121:
		//line sql.y:665
		{
			yyVAL.node = yyS[yypt-1].node.PushTwo(yyS[yypt-2].node, yyS[yypt-0].node)
		}
	case 122:
		yyVAL.node = yyS[yypt-0].node
	case 123:
		yyVAL.node = yyS[yypt-0].node
	case 124:
		yyVAL.node = yyS[yypt-0].node
	case 125:
		yyVAL.node = yyS[yypt-0].node
	case 126:
		//line sql.y:676
		{
			yyVAL.node = NewSimpleParseNode(GROUP, "group")
		}
	case 127:
		//line sql.y:680
		{
			yyVAL.node = yyS[yypt-2].node.Push(yyS[yypt-0].node)
		}
	case 128:
		//line sql.y:685
		{
			yyVAL.node = NewSimpleParseNode(HAVING, "having")
		}
	case 129:
		//line sql.y:689
		{
			yyVAL.node = yyS[yypt-1].node.Push(yyS[yypt-0].node)
		}
	case 130:
		//line sql.y:694
		{
			yyVAL.node = NewSimpleParseNode(ORDER, "order")
		}
	case 131:
		//line sql.y:698
		{
			yyVAL.node = yyS[yypt-2].node.Push(yyS[yypt-0].node)
		}
	case 132:
		//line sql.y:704
		{
			yyVAL.node = NewSimpleParseNode(NODE_LIST, "node_list")
			yyVAL.node.Push(yyS[yypt-0].node)
		}
	case 133:
		//line sql.y:709
		{
			yyVAL.node = yyS[yypt-2].node.Push(yyS[yypt-0].node)
		}
	case 134:
		//line sql.y:715
		{
			yyVAL.node = yyS[yypt-0].node.Push(yyS[yypt-1].node)
		}
	case 135:
		//line sql.y:720
		{
			yyVAL.node = NewSimpleParseNode(ASC, "asc")
		}
	case 136:
		yyVAL.node = yyS[yypt-0].node
	case 137:
		yyVAL.node = yyS[yypt-0].node
	case 138:
		//line sql.y:727
		{
			yyVAL.node = NewSimpleParseNode(LIMIT, "limit")
		}
	case 139:
		//line sql.y:731
		{
			yyVAL.node = yyS[yypt-1].node.Push(yyS[yypt-0].node)
		}
	case 140:
		//line sql.y:735
		{
			yyVAL.node = yyS[yypt-3].node.PushTwo(yyS[yypt-2].node, yyS[yypt-0].node)
		}
	case 141:
		//line sql.y:740
		{
			yyVAL.node = NewSimpleParseNode(NOT_FOR_UPDATE, "")
		}
	case 142:
		//line sql.y:744
		{
			yyVAL.node = NewSimpleParseNode(FOR_UPDATE, " for update")
		}
	case 143:
		//line sql.y:749
		{
			yyVAL.node = NewSimpleParseNode(COLUMN_LIST, "")
		}
	case 144:
		//line sql.y:753
		{
			yyVAL.node = yyS[yypt-1].node
		}
	case 145:
		//line sql.y:759
		{
			yyVAL.node = NewSimpleParseNode(COLUMN_LIST, "")
			yyVAL.node.Push(yyS[yypt-0].node)
		}
	case 146:
		//line sql.y:764
		{
			yyVAL.node = yyS[yypt-2].node.Push(yyS[yypt-0].node)
		}
	case 147:
		//line sql.y:769
		{
			yyVAL.node = NewSimpleParseNode(DUPLICATE, "duplicate")
		}
	case 148:
		//line sql.y:773
		{
			yyVAL.node = yyS[yypt-3].node.Push(yyS[yypt-0].node)
		}
	case 149:
		//line sql.y:779
		{
			yyVAL.node = NewSimpleParseNode(NODE_LIST, "node_list")
			yyVAL.node.Push(yyS[yypt-0].node)
		}
	case 150:
		//line sql.y:784
		{
			yyVAL.node = yyS[yypt-2].node.Push(yyS[yypt-0].node)
		}
	case 151:
		//line sql.y:790
		{
			yyVAL.node = yyS[yypt-1].node.PushTwo(yyS[yypt-2].node, yyS[yypt-0].node)
		}
	case 152:
		//line sql.y:795
		{
			yyVAL.node = nil
		}
	case 153:
		yyVAL.node = yyS[yypt-0].node
	case 154:
		//line sql.y:799
		{
			yyVAL.node = nil
		}
	case 155:
		yyVAL.node = yyS[yypt-0].node
	case 156:
		//line sql.y:803
		{
			yyVAL.node = nil
		}
	case 157:
		yyVAL.node = yyS[yypt-0].node
	case 158:
		yyVAL.node = yyS[yypt-0].node
	case 159:
		yyVAL.node = yyS[yypt-0].node
	case 160:
		yyVAL.node = yyS[yypt-0].node
	case 161:
		yyVAL.node = yyS[yypt-0].node
	case 162:
		yyVAL.node = yyS[yypt-0].node
	case 163:
		//line sql.y:814
		{
			yyVAL.node = nil
		}
	case 164:
		yyVAL.node = yyS[yypt-0].node
	case 165:
		//line sql.y:818
		{
			yyVAL.node = nil
		}
	case 166:
		yyVAL.node = yyS[yypt-0].node
	case 167:
		//line sql.y:822
		{
			yyVAL.node = nil
		}
	case 168:
		yyVAL.node = yyS[yypt-0].node
	case 169:
		//line sql.y:826
		{
			ForceEOF(yylex)
		}
	}
	goto yystack /* stack new state and value */
}
