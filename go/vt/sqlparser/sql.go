
//line sql.y:33
package sqlparser

import(
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
	yys int
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
const LEFT = 57390
const RIGHT = 57391
const INNER = 57392
const OUTER = 57393
const CROSS = 57394
const NATURAL = 57395
const USE = 57396
const ON = 57397
const AND = 57398
const OR = 57399
const NOT = 57400
const UNARY = 57401
const CREATE = 57402
const ALTER = 57403
const DROP = 57404
const RENAME = 57405
const TABLE = 57406
const INDEX = 57407
const TO = 57408
const IGNORE = 57409
const IF = 57410
const UNIQUE = 57411
const USING = 57412
const NODE_LIST = 57413
const UPLUS = 57414
const UMINUS = 57415
const SELECT_STAR = 57416
const NO_DISTINCT = 57417
const FUNCTION = 57418
const FOR_UPDATE = 57419
const NOT_FOR_UPDATE = 57420
const NOT_IN = 57421
const NOT_LIKE = 57422
const NOT_BETWEEN = 57423
const IS_NULL = 57424
const IS_NOT_NULL = 57425
const UNION_ALL = 57426
const COMMENT_LIST = 57427
const COLUMN_LIST = 57428
const TABLE_EXPR = 57429

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
	-2, 63,
}

const yyNprod = 169
const yyPrivate = 57344

var yyTokenNames []string
var yyStates []string

const yyLast = 472

var yyAct = []int{

	65, 48, 278, 200, 246, 131, 203, 172, 223, 158,
	144, 130, 3, 244, 99, 192, 64, 147, 57, 59,
	140, 103, 104, 214, 215, 216, 217, 98, 218, 219,
	244, 98, 98, 40, 22, 23, 24, 25, 60, 192,
	49, 62, 168, 52, 73, 22, 23, 24, 25, 77,
	36, 265, 82, 22, 23, 24, 25, 210, 63, 74,
	75, 76, 190, 93, 306, 53, 268, 68, 127, 132,
	54, 80, 133, 262, 22, 23, 24, 25, 267, 38,
	39, 243, 237, 227, 126, 129, 32, 139, 67, 269,
	193, 34, 78, 79, 61, 37, 102, 116, 117, 118,
	263, 102, 226, 165, 127, 127, 171, 81, 238, 177,
	178, 90, 181, 182, 183, 184, 185, 186, 187, 188,
	169, 170, 167, 103, 104, 284, 189, 166, 142, 191,
	143, 101, 240, 206, 179, 194, 96, 162, 225, 164,
	286, 285, 127, 12, 13, 14, 15, 261, 196, 199,
	114, 115, 116, 117, 118, 195, 260, 259, 205, 208,
	202, 111, 112, 113, 114, 115, 116, 117, 118, 220,
	100, 221, 16, 180, 194, 224, 232, 233, 211, 229,
	257, 73, 231, 255, 258, 230, 77, 256, 228, 82,
	153, 173, 87, 236, 192, 63, 74, 75, 76, 293,
	22, 23, 24, 25, 68, 273, 242, 97, 80, 245,
	151, 225, 154, 88, 17, 18, 20, 19, 288, 161,
	138, 50, 253, 254, 136, 67, 141, 141, 222, 78,
	79, 61, 135, 270, 221, 271, 111, 112, 113, 114,
	115, 116, 117, 118, 81, 201, 274, 12, 276, 279,
	275, 98, 290, 266, 150, 152, 149, 264, 234, 250,
	280, 111, 112, 113, 114, 115, 116, 117, 118, 212,
	87, 289, 249, 287, 165, 207, 163, 161, 161, 12,
	156, 291, 155, 127, 194, 127, 222, 159, 297, 299,
	94, 92, 301, 303, 279, 73, 304, 91, 89, 298,
	77, 300, 305, 82, 307, 86, 295, 296, 84, 128,
	74, 75, 76, 58, 85, 73, 55, 46, 68, 272,
	77, 45, 80, 82, 174, 235, 175, 176, 145, 128,
	74, 75, 76, 95, 100, 43, 12, 41, 68, 67,
	309, 83, 80, 78, 79, 111, 112, 113, 114, 115,
	116, 117, 118, 12, 247, 283, 248, 77, 81, 67,
	82, 282, 77, 78, 79, 82, 128, 74, 75, 76,
	204, 128, 74, 75, 76, 134, 252, 198, 81, 80,
	134, 12, 141, 47, 80, 310, 214, 215, 216, 217,
	302, 218, 219, 106, 110, 108, 109, 27, 146, 33,
	78, 79, 209, 148, 35, 78, 79, 51, 292, 56,
	122, 123, 124, 125, 137, 81, 119, 120, 121, 239,
	81, 111, 112, 113, 114, 115, 116, 117, 118, 308,
	294, 26, 277, 281, 251, 69, 107, 111, 112, 113,
	114, 115, 116, 117, 118, 28, 29, 30, 31, 70,
	71, 72, 241, 197, 105, 66, 160, 213, 157, 42,
	21, 44, 11, 10, 9, 8, 7, 6, 5, 4,
	2, 1,
}
var yyPact = []int{

	139, -1000, -1000, 152, -1000, -1000, -1000, -1000, -1000, -1000,
	-1000, -1000, -1000, -1000, -1000, -1000, -1000, 7, -32, 16,
	0, 377, 320, -1000, -1000, -1000, 317, -1000, 292, 283,
	375, 187, -40, -15, -1000, -9, -1000, 282, -65, 279,
	-1000, -1000, 161, -1000, 326, 274, 281, 271, 140, -1000,
	169, 264, 47, 263, 257, -18, 256, 313, 75, 199,
	-1000, -1000, 315, 58, 61, 372, -1000, 295, 275, -1000,
	-1000, 337, 189, 181, -1000, -1000, -1000, -1000, -1000, -1000,
	-1000, -1000, -1000, -1000, 177, 187, 373, 187, 295, -1000,
	308, -68, 178, 248, -1000, -1000, 246, 244, 161, 242,
	-1000, 69, 24, 295, 295, 337, 148, 303, 337, 337,
	109, 337, 337, 337, 337, 337, 337, 337, 337, -1000,
	-1000, -1000, -1000, -1000, -1000, -1000, -1000, 372, 53, -41,
	26, -13, 372, -1000, 332, 161, 377, 349, 211, 218,
	358, 295, -1000, -1000, -1000, -1000, 72, 241, -1000, -24,
	-1000, -1000, -1000, -1000, -1000, -1000, -1000, 217, 333, 243,
	151, 29, -1000, -1000, -1000, -1000, -1000, -20, 161, -1000,
	-1000, 96, -1000, 332, 148, 337, 337, 96, 196, -1000,
	300, 82, 82, 82, 27, 27, -1000, -1000, -1000, 240,
	-1000, -1000, 337, -1000, 96, -21, 5, 71, 148, 152,
	-22, -1000, 358, 340, 343, 61, 238, -1000, -1000, 225,
	-1000, 366, 185, 185, -1000, 130, 127, 104, 103, 94,
	-30, -3, 377, -1000, 223, -29, 219, -1000, -25, -37,
	-14, -1000, 96, 171, 337, -1000, 96, -1000, -1000, -1000,
	289, 153, -1000, -1000, 212, 340, -1000, 337, 337, -1000,
	-1000, 350, 342, 333, 64, -1000, 88, -1000, 87, -1000,
	-1000, -1000, -1000, -1000, 78, 175, -1000, -1000, -1000, -1000,
	337, 96, 221, 148, -1000, -1000, 356, 147, -1000, 280,
	-1000, 358, 295, 337, 295, -1000, -1000, -1000, 211, 96,
	384, -1000, 337, 337, -1000, -1000, -1000, 340, 61, 142,
	61, -39, 187, 96, -1000, 324, -1000, 140, -1000, 379,
	-1000,
}
var yyPgo = []int{

	0, 471, 470, 11, 469, 468, 467, 466, 465, 464,
	463, 462, 431, 461, 460, 459, 19, 38, 41, 14,
	458, 9, 457, 456, 8, 20, 16, 455, 454, 453,
	452, 7, 5, 0, 451, 450, 449, 435, 434, 433,
	6, 432, 2, 430, 4, 429, 419, 414, 3, 1,
	40, 409, 407, 404, 403, 402, 399, 398, 10, 397,
}
var yyR1 = []int{

	0, 1, 2, 2, 2, 2, 2, 2, 2, 2,
	2, 3, 3, 4, 5, 6, 7, 8, 8, 9,
	9, 10, 11, 11, 59, 12, 13, 13, 14, 14,
	14, 14, 14, 15, 15, 16, 16, 17, 17, 17,
	17, 18, 18, 19, 19, 20, 20, 20, 21, 21,
	21, 21, 22, 22, 22, 22, 22, 22, 22, 22,
	23, 23, 23, 24, 24, 25, 25, 26, 26, 26,
	26, 26, 27, 27, 27, 27, 27, 27, 27, 27,
	27, 27, 28, 28, 28, 28, 28, 28, 28, 29,
	29, 30, 30, 31, 31, 32, 32, 33, 33, 33,
	33, 33, 33, 33, 33, 33, 33, 33, 33, 33,
	33, 33, 33, 33, 34, 34, 35, 35, 35, 36,
	36, 37, 37, 37, 37, 38, 38, 39, 39, 40,
	40, 41, 41, 42, 43, 43, 43, 44, 44, 44,
	45, 45, 47, 47, 48, 48, 46, 46, 49, 49,
	50, 51, 51, 52, 52, 53, 53, 54, 54, 54,
	54, 54, 55, 55, 56, 56, 57, 57, 58,
}
var yyR2 = []int{

	0, 1, 1, 1, 1, 1, 1, 1, 1, 1,
	1, 12, 3, 7, 8, 7, 3, 5, 8, 6,
	7, 5, 4, 5, 0, 2, 0, 2, 1, 2,
	1, 1, 1, 0, 1, 1, 3, 1, 1, 3,
	3, 1, 1, 0, 1, 1, 3, 3, 2, 4,
	3, 5, 1, 2, 3, 2, 3, 2, 2, 2,
	1, 3, 3, 0, 5, 0, 2, 1, 3, 3,
	2, 3, 3, 3, 4, 3, 4, 5, 6, 3,
	4, 4, 1, 1, 1, 1, 1, 1, 1, 2,
	1, 1, 3, 3, 3, 1, 3, 1, 1, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 2,
	3, 4, 5, 4, 1, 1, 1, 1, 1, 1,
	3, 1, 1, 1, 1, 0, 3, 0, 2, 0,
	3, 1, 3, 2, 0, 1, 1, 0, 2, 4,
	0, 2, 0, 3, 1, 3, 0, 5, 1, 3,
	3, 0, 2, 0, 3, 0, 1, 1, 1, 1,
	1, 1, 0, 1, 0, 1, 0, 2, 0,
}
var yyChk = []int{

	-1000, -1, -2, -3, -4, -5, -6, -7, -8, -9,
	-10, -11, 4, 5, 6, 7, 33, 75, 76, 78,
	77, -14, 48, 49, 50, 51, -12, -59, -12, -12,
	-12, -12, 79, -56, 84, -53, 82, 79, 79, 80,
	-3, 17, -15, 18, -13, 29, 34, 8, -49, -50,
	34, -52, 83, 80, 79, 34, -51, 83, 34, -16,
	-17, 70, -18, 34, -26, -33, -27, 64, 43, -37,
	-36, -35, -34, 20, 35, 36, 37, 25, 68, 69,
	47, 83, 28, 15, 34, 33, 34, 52, 44, 34,
	64, 34, 34, 81, 34, 20, 61, 8, 52, -19,
	19, 73, 43, 62, 63, -28, 21, 64, 23, 24,
	22, 65, 66, 67, 68, 69, 70, 71, 72, 44,
	45, 46, 38, 39, 40, 41, -26, -33, 34, -26,
	-3, -32, -33, -33, 43, 43, 43, -47, 43, -49,
	-25, 9, -50, -18, -58, 20, -57, 85, -54, 78,
	76, 32, 77, 12, 34, 34, 34, -20, -21, 43,
	-23, 34, -17, 34, 70, 34, 103, -16, 18, -26,
	-26, -33, -31, 43, 21, 23, 24, -33, -33, 25,
	64, -33, -33, -33, -33, -33, -33, -33, -33, 73,
	103, 103, 52, 103, -33, -16, -3, -29, 28, -3,
	-48, 34, -25, -40, 12, -26, 61, 34, -58, -55,
	81, -25, 52, -22, 53, 54, 55, 56, 58, 59,
	-21, -3, 43, -24, -19, 60, 73, 103, -16, -32,
	-3, -31, -33, -33, 62, 25, -33, 103, 103, -46,
	61, -30, -31, 103, 52, -40, -44, 14, 13, 34,
	34, -38, 10, -21, -21, 53, 57, 53, 57, 53,
	53, 53, 103, 103, 34, 80, 34, 103, 103, 103,
	62, -33, 30, 52, 34, -44, -33, -41, -42, -33,
	-58, -39, 11, 13, 61, 53, 53, -24, 43, -33,
	31, -31, 52, 52, -43, 26, 27, -40, -26, -32,
	-26, -48, 6, -33, -42, -44, 103, -49, -45, 16,
	6,
}
var yyDef = []int{

	0, -2, 1, 2, 3, 4, 5, 6, 7, 8,
	9, 10, 24, 24, 24, 24, 24, 164, 155, 0,
	0, 0, 28, 30, 31, 32, 33, 26, 0, 0,
	0, 0, 153, 0, 165, 0, 156, 0, 151, 0,
	12, 29, 0, 34, 25, 0, 0, 0, 16, 148,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	35, 37, -2, 119, 41, 42, 67, 0, 0, 97,
	98, 0, 0, 0, 121, 122, 123, 124, 116, 117,
	118, 114, 115, 27, 142, 0, 65, 0, 0, 168,
	0, 166, 0, 0, 22, 152, 0, 0, 0, 0,
	44, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 82,
	83, 84, 85, 86, 87, 88, 70, 0, 119, 0,
	0, 0, 95, 109, 0, 0, 0, 0, 0, 65,
	129, 0, 149, 150, 17, 154, 0, 0, 168, 162,
	157, 158, 159, 160, 161, 21, 23, 65, 45, 0,
	-2, 60, 36, 39, 40, 120, 110, 0, 0, 68,
	69, 72, 73, 0, 0, 0, 0, 75, 0, 79,
	0, 101, 102, 103, 104, 105, 106, 107, 108, 0,
	71, 99, 0, 100, 95, 0, 0, 146, 0, 90,
	0, 144, 129, 137, 0, 66, 0, 167, 19, 0,
	163, 125, 0, 0, 52, 0, 0, 0, 0, 0,
	0, 0, 0, 48, 0, 0, 0, 111, 0, 0,
	0, 74, 76, 0, 0, 80, 96, 113, 81, 13,
	0, 89, 91, 143, 0, 137, 15, 0, 0, 168,
	20, 127, 0, 47, 50, 53, 0, 55, 0, 57,
	58, 59, 46, 62, 63, 0, 61, 112, 93, 94,
	0, 77, 0, 0, 145, 14, 138, 130, 131, 134,
	18, 129, 0, 0, 0, 54, 56, 49, 0, 78,
	0, 92, 0, 0, 133, 135, 136, 137, 128, 126,
	51, 0, 0, 139, 132, 140, 64, 147, 11, 0,
	141,
}
var yyTok1 = []int{

	1, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 72, 65, 3,
	43, 103, 70, 68, 52, 69, 73, 71, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	45, 44, 46, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 67, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 66, 3, 47,
}
var yyTok2 = []int{

	2, 3, 4, 5, 6, 7, 8, 9, 10, 11,
	12, 13, 14, 15, 16, 17, 18, 19, 20, 21,
	22, 23, 24, 25, 26, 27, 28, 29, 30, 31,
	32, 33, 34, 35, 36, 37, 38, 39, 40, 41,
	42, 48, 49, 50, 51, 53, 54, 55, 56, 57,
	58, 59, 60, 61, 62, 63, 64, 74, 75, 76,
	77, 78, 79, 80, 81, 82, 83, 84, 85, 86,
	87, 88, 89, 90, 91, 92, 93, 94, 95, 96,
	97, 98, 99, 100, 101, 102,
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
		yyVAL.node.Push(yyS[yypt-9].node) // 1: distinct_opt
		yyVAL.node.Push(yyS[yypt-8].node) // 2: select_expression_list
		yyVAL.node.Push(yyS[yypt-6].node) // 3: table_expression_list
		yyVAL.node.Push(yyS[yypt-5].node) // 4: where_expression_opt
		yyVAL.node.Push(yyS[yypt-4].node) // 5: group_by_opt
		yyVAL.node.Push(yyS[yypt-3].node) // 6: having_opt
		yyVAL.node.Push(yyS[yypt-2].node) // 7: order_by_opt
		yyVAL.node.Push(yyS[yypt-1].node) // 8: limit_opt
		yyVAL.node.Push(yyS[yypt-0].node) // 9: for_update_opt
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
		//line sql.y:385
		{
			yyVAL.node = NewSimpleParseNode(LEFT, "left join")
		}
	case 54:
		//line sql.y:389
		{
			yyVAL.node = NewSimpleParseNode(LEFT, "left join")
		}
	case 55:
		//line sql.y:393
		{
			yyVAL.node = NewSimpleParseNode(RIGHT, "right join")
		}
	case 56:
		//line sql.y:397
		{
			yyVAL.node = NewSimpleParseNode(RIGHT, "right join")
		}
	case 57:
		//line sql.y:401
		{
			yyVAL.node = yyS[yypt-0].node
		}
	case 58:
		//line sql.y:405
		{
			yyVAL.node = NewSimpleParseNode(CROSS, "cross join")
		}
	case 59:
		//line sql.y:409
		{
			yyVAL.node = NewSimpleParseNode(NATURAL, "natural join")
		}
	case 60:
		yyVAL.node = yyS[yypt-0].node
	case 61:
		//line sql.y:416
		{
			yyVAL.node = yyS[yypt-1].node.PushTwo(yyS[yypt-2].node, yyS[yypt-0].node)
		}
	case 62:
		//line sql.y:420
		{
			yyVAL.node = yyS[yypt-2].node.Push(yyS[yypt-1].node)
		}
	case 63:
		//line sql.y:425
		{
			yyVAL.node = NewSimpleParseNode(USE, "use")
	  }
	case 64:
		//line sql.y:429
		{
	    yyVAL.node.Push(yyS[yypt-1].node)
	  }
	case 65:
		//line sql.y:434
		{
			yyVAL.node = NewSimpleParseNode(WHERE, "where")
		}
	case 66:
		//line sql.y:438
		{
			yyVAL.node = yyS[yypt-1].node.Push(yyS[yypt-0].node)
		}
	case 67:
		yyVAL.node = yyS[yypt-0].node
	case 68:
		//line sql.y:445
		{
			yyVAL.node = yyS[yypt-1].node.PushTwo(yyS[yypt-2].node, yyS[yypt-0].node)
		}
	case 69:
		//line sql.y:449
		{
			yyVAL.node = yyS[yypt-1].node.PushTwo(yyS[yypt-2].node, yyS[yypt-0].node)
		}
	case 70:
		//line sql.y:453
		{
			yyVAL.node = yyS[yypt-1].node.Push(yyS[yypt-0].node)
		}
	case 71:
		//line sql.y:457
		{
			yyVAL.node = yyS[yypt-2].node.Push(yyS[yypt-1].node)
		}
	case 72:
		//line sql.y:463
		{
			yyVAL.node = yyS[yypt-1].node.PushTwo(yyS[yypt-2].node, yyS[yypt-0].node)
		}
	case 73:
		//line sql.y:467
		{
			yyVAL.node = yyS[yypt-1].node.PushTwo(yyS[yypt-2].node, yyS[yypt-0].node)
		}
	case 74:
		//line sql.y:471
		{
			yyVAL.node = NewSimpleParseNode(NOT_IN, "not in").PushTwo(yyS[yypt-3].node, yyS[yypt-0].node)
		}
	case 75:
		//line sql.y:475
		{
			yyVAL.node = yyS[yypt-1].node.PushTwo(yyS[yypt-2].node, yyS[yypt-0].node)
		}
	case 76:
		//line sql.y:479
		{
			yyVAL.node = NewSimpleParseNode(NOT_LIKE, "not like").PushTwo(yyS[yypt-3].node, yyS[yypt-0].node)
		}
	case 77:
		//line sql.y:483
		{
			yyVAL.node = yyS[yypt-3].node
			yyVAL.node.Push(yyS[yypt-4].node)
			yyVAL.node.Push(yyS[yypt-2].node)
			yyVAL.node.Push(yyS[yypt-0].node)
		}
	case 78:
		//line sql.y:490
		{
			yyVAL.node = NewSimpleParseNode(NOT_BETWEEN, "not between")
			yyVAL.node.Push(yyS[yypt-5].node)
			yyVAL.node.Push(yyS[yypt-2].node)
			yyVAL.node.Push(yyS[yypt-0].node)
		}
	case 79:
		//line sql.y:497
		{
			yyVAL.node = NewSimpleParseNode(IS_NULL, "is null").Push(yyS[yypt-2].node)
		}
	case 80:
		//line sql.y:501
		{
			yyVAL.node = NewSimpleParseNode(IS_NOT_NULL, "is not null").Push(yyS[yypt-3].node)
		}
	case 81:
		//line sql.y:505
		{
			yyVAL.node = yyS[yypt-3].node.Push(yyS[yypt-1].node)
		}
	case 82:
		yyVAL.node = yyS[yypt-0].node
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
		//line sql.y:520
		{
			yyVAL.node = yyS[yypt-1].node.Push(yyS[yypt-0].node)
		}
	case 90:
		yyVAL.node = yyS[yypt-0].node
	case 91:
		//line sql.y:527
		{
			yyVAL.node = NewSimpleParseNode(NODE_LIST, "node_list")
			yyVAL.node.Push(yyS[yypt-0].node)
		}
	case 92:
		//line sql.y:532
		{
			yyVAL.node.Push(yyS[yypt-0].node)
		}
	case 93:
		//line sql.y:538
		{
			yyVAL.node = yyS[yypt-2].node.Push(yyS[yypt-1].node)
		}
	case 94:
		//line sql.y:542
		{
			yyVAL.node = yyS[yypt-2].node.Push(yyS[yypt-1].node)
		}
	case 95:
		//line sql.y:548
		{
			yyVAL.node = NewSimpleParseNode(NODE_LIST, "node_list")
			yyVAL.node.Push(yyS[yypt-0].node)
		}
	case 96:
		//line sql.y:553
		{
			yyVAL.node.Push(yyS[yypt-0].node)
		}
	case 97:
		yyVAL.node = yyS[yypt-0].node
	case 98:
		yyVAL.node = yyS[yypt-0].node
	case 99:
		//line sql.y:561
		{
			yyVAL.node = yyS[yypt-2].node.Push(yyS[yypt-1].node)
		}
	case 100:
		//line sql.y:565
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
	case 101:
		//line sql.y:577
		{
			yyVAL.node = yyS[yypt-1].node.PushTwo(yyS[yypt-2].node, yyS[yypt-0].node)
		}
	case 102:
		//line sql.y:581
		{
			yyVAL.node = yyS[yypt-1].node.PushTwo(yyS[yypt-2].node, yyS[yypt-0].node)
		}
	case 103:
		//line sql.y:585
		{
			yyVAL.node = yyS[yypt-1].node.PushTwo(yyS[yypt-2].node, yyS[yypt-0].node)
		}
	case 104:
		//line sql.y:589
		{
			yyVAL.node = yyS[yypt-1].node.PushTwo(yyS[yypt-2].node, yyS[yypt-0].node)
		}
	case 105:
		//line sql.y:593
		{
			yyVAL.node = yyS[yypt-1].node.PushTwo(yyS[yypt-2].node, yyS[yypt-0].node)
		}
	case 106:
		//line sql.y:597
		{
			yyVAL.node = yyS[yypt-1].node.PushTwo(yyS[yypt-2].node, yyS[yypt-0].node)
		}
	case 107:
		//line sql.y:601
		{
			yyVAL.node = yyS[yypt-1].node.PushTwo(yyS[yypt-2].node, yyS[yypt-0].node)
		}
	case 108:
		//line sql.y:605
		{
			yyVAL.node = yyS[yypt-1].node.PushTwo(yyS[yypt-2].node, yyS[yypt-0].node)
		}
	case 109:
		//line sql.y:609
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
	case 110:
		//line sql.y:625
		{
			yyS[yypt-2].node.Type = FUNCTION
			yyVAL.node = yyS[yypt-2].node.Push(NewSimpleParseNode(NODE_LIST, "node_list"))
		}
	case 111:
		//line sql.y:630
		{
			yyS[yypt-3].node.Type = FUNCTION
			yyVAL.node = yyS[yypt-3].node.Push(yyS[yypt-1].node)
		}
	case 112:
		//line sql.y:635
		{
			yyS[yypt-4].node.Type = FUNCTION
			yyVAL.node = yyS[yypt-4].node.Push(yyS[yypt-2].node)
			yyVAL.node = yyS[yypt-4].node.Push(yyS[yypt-1].node)
		}
	case 113:
		//line sql.y:641
		{
			yyS[yypt-3].node.Type = FUNCTION
			yyVAL.node = yyS[yypt-3].node.Push(yyS[yypt-1].node)
		}
	case 114:
		yyVAL.node = yyS[yypt-0].node
	case 115:
		yyVAL.node = yyS[yypt-0].node
	case 116:
		//line sql.y:652
		{
			yyVAL.node = NewSimpleParseNode(UPLUS, "+")
		}
	case 117:
		//line sql.y:656
		{
			yyVAL.node = NewSimpleParseNode(UMINUS, "-")
		}
	case 118:
		yyVAL.node = yyS[yypt-0].node
	case 119:
		yyVAL.node = yyS[yypt-0].node
	case 120:
		//line sql.y:664
		{
			yyVAL.node = yyS[yypt-1].node.PushTwo(yyS[yypt-2].node, yyS[yypt-0].node)
		}
	case 121:
		yyVAL.node = yyS[yypt-0].node
	case 122:
		yyVAL.node = yyS[yypt-0].node
	case 123:
		yyVAL.node = yyS[yypt-0].node
	case 124:
		yyVAL.node = yyS[yypt-0].node
	case 125:
		//line sql.y:675
		{
			yyVAL.node = NewSimpleParseNode(GROUP, "group")
		}
	case 126:
		//line sql.y:679
		{
			yyVAL.node = yyS[yypt-2].node.Push(yyS[yypt-0].node)
		}
	case 127:
		//line sql.y:684
		{
			yyVAL.node = NewSimpleParseNode(HAVING, "having")
		}
	case 128:
		//line sql.y:688
		{
			yyVAL.node = yyS[yypt-1].node.Push(yyS[yypt-0].node)
		}
	case 129:
		//line sql.y:693
		{
			yyVAL.node = NewSimpleParseNode(ORDER, "order")
		}
	case 130:
		//line sql.y:697
		{
			yyVAL.node = yyS[yypt-2].node.Push(yyS[yypt-0].node)
		}
	case 131:
		//line sql.y:703
		{
			yyVAL.node = NewSimpleParseNode(NODE_LIST, "node_list")
			yyVAL.node.Push(yyS[yypt-0].node)
		}
	case 132:
		//line sql.y:708
		{
			yyVAL.node = yyS[yypt-2].node.Push(yyS[yypt-0].node)
		}
	case 133:
		//line sql.y:714
		{
			yyVAL.node = yyS[yypt-0].node.Push(yyS[yypt-1].node)
		}
	case 134:
		//line sql.y:719
		{
			yyVAL.node = NewSimpleParseNode(ASC, "asc")
		}
	case 135:
		yyVAL.node = yyS[yypt-0].node
	case 136:
		yyVAL.node = yyS[yypt-0].node
	case 137:
		//line sql.y:726
		{
			yyVAL.node = NewSimpleParseNode(LIMIT, "limit")
		}
	case 138:
		//line sql.y:730
		{
			yyVAL.node = yyS[yypt-1].node.Push(yyS[yypt-0].node)
		}
	case 139:
		//line sql.y:734
		{
			yyVAL.node = yyS[yypt-3].node.PushTwo(yyS[yypt-2].node, yyS[yypt-0].node)
		}
	case 140:
		//line sql.y:739
		{
			yyVAL.node = NewSimpleParseNode(NOT_FOR_UPDATE, "")
		}
	case 141:
		//line sql.y:743
		{
			yyVAL.node = NewSimpleParseNode(FOR_UPDATE, " for update")
		}
	case 142:
		//line sql.y:748
		{
			yyVAL.node = NewSimpleParseNode(COLUMN_LIST, "")
		}
	case 143:
		//line sql.y:752
		{
			yyVAL.node = yyS[yypt-1].node
		}
	case 144:
		//line sql.y:758
		{
			yyVAL.node = NewSimpleParseNode(COLUMN_LIST, "")
			yyVAL.node.Push(yyS[yypt-0].node)
		}
	case 145:
		//line sql.y:763
		{
			yyVAL.node = yyS[yypt-2].node.Push(yyS[yypt-0].node)
		}
	case 146:
		//line sql.y:768
		{
			yyVAL.node = NewSimpleParseNode(DUPLICATE, "duplicate")
		}
	case 147:
		//line sql.y:772
		{
			yyVAL.node = yyS[yypt-3].node.Push(yyS[yypt-0].node)
		}
	case 148:
		//line sql.y:778
		{
			yyVAL.node = NewSimpleParseNode(NODE_LIST, "node_list")
			yyVAL.node.Push(yyS[yypt-0].node)
		}
	case 149:
		//line sql.y:783
		{
			yyVAL.node = yyS[yypt-2].node.Push(yyS[yypt-0].node)
		}
	case 150:
		//line sql.y:789
		{
			yyVAL.node = yyS[yypt-1].node.PushTwo(yyS[yypt-2].node, yyS[yypt-0].node)
		}
	case 151:
		//line sql.y:794
		{ yyVAL.node = nil }
	case 152:
		yyVAL.node = yyS[yypt-0].node
	case 153:
		//line sql.y:798
		{ yyVAL.node = nil }
	case 154:
		yyVAL.node = yyS[yypt-0].node
	case 155:
		//line sql.y:802
		{ yyVAL.node = nil }
	case 156:
		yyVAL.node = yyS[yypt-0].node
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
		//line sql.y:813
		{ yyVAL.node = nil }
	case 163:
		yyVAL.node = yyS[yypt-0].node
	case 164:
		//line sql.y:817
		{ yyVAL.node = nil }
	case 165:
		yyVAL.node = yyS[yypt-0].node
	case 166:
		//line sql.y:821
		{ yyVAL.node = nil }
	case 167:
		yyVAL.node = yyS[yypt-0].node
	case 168:
		//line sql.y:825
		{
		ForceEOF(yylex)
	}
	}
	goto yystack /* stack new state and value */
}
