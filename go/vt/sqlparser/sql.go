//line sql.y:33
package sqlparser

import __yyfmt__ "fmt"

//line sql.y:33
import "bytes"

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

var (
	SHARE = []byte("share")
	MODE  = []byte("mode")
)

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
	SELECT_LOCK_OFFSET
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

//line sql.y:98
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
const SELECT_STAR = 57427
const NO_DISTINCT = 57428
const FUNCTION = 57429
const NO_LOCK = 57430
const FOR_UPDATE = 57431
const LOCK_IN_SHARE_MODE = 57432
const NOT_IN = 57433
const NOT_LIKE = 57434
const NOT_BETWEEN = 57435
const IS_NULL = 57436
const IS_NOT_NULL = 57437
const UNION_ALL = 57438
const COMMENT_LIST = 57439
const COLUMN_LIST = 57440
const INDEX_LIST = 57441
const TABLE_EXPR = 57442

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
	"SELECT_STAR",
	"NO_DISTINCT",
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
	"COMMENT_LIST",
	"COLUMN_LIST",
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
	-1, 184,
	35, 46,
	-2, 69,
}

const yyNprod = 187
const yyPrivate = 57344

var yyTokenNames []string
var yyStates []string

const yyLast = 578

var yyAct = []int{

	82, 75, 52, 311, 335, 277, 230, 80, 147, 192,
	251, 69, 107, 154, 163, 182, 72, 74, 156, 343,
	117, 120, 121, 146, 3, 343, 70, 241, 242, 243,
	244, 245, 55, 246, 247, 60, 171, 116, 63, 54,
	211, 66, 67, 68, 275, 43, 22, 23, 24, 25,
	58, 116, 116, 211, 22, 23, 24, 25, 53, 37,
	106, 38, 22, 23, 24, 25, 237, 32, 110, 34,
	297, 296, 209, 35, 177, 62, 109, 59, 255, 144,
	148, 114, 344, 149, 22, 23, 24, 25, 342, 293,
	40, 41, 42, 39, 175, 143, 145, 178, 269, 55,
	303, 161, 55, 299, 167, 105, 54, 274, 157, 54,
	158, 268, 155, 300, 266, 264, 212, 168, 187, 166,
	167, 294, 144, 144, 191, 119, 103, 197, 198, 267,
	201, 202, 203, 204, 205, 206, 207, 208, 189, 190,
	99, 61, 157, 186, 158, 219, 174, 176, 173, 120,
	121, 210, 213, 157, 317, 158, 133, 134, 135, 271,
	165, 55, 233, 215, 217, 113, 144, 199, 228, 221,
	253, 254, 234, 220, 222, 223, 229, 218, 319, 188,
	318, 288, 232, 226, 292, 235, 289, 291, 118, 12,
	13, 14, 15, 290, 101, 213, 238, 259, 260, 248,
	329, 330, 256, 286, 258, 252, 85, 249, 287, 200,
	164, 89, 211, 263, 94, 327, 306, 257, 16, 61,
	102, 56, 86, 87, 88, 193, 322, 115, 265, 164,
	78, 253, 254, 321, 92, 273, 276, 160, 153, 152,
	220, 151, 128, 129, 130, 131, 132, 133, 134, 135,
	49, 98, 56, 77, 239, 284, 285, 90, 91, 22,
	23, 24, 25, 302, 95, 157, 185, 158, 17, 18,
	20, 19, 116, 101, 249, 250, 55, 347, 93, 298,
	309, 312, 308, 307, 295, 281, 304, 131, 132, 133,
	134, 135, 280, 313, 216, 348, 85, 180, 185, 97,
	179, 89, 100, 323, 94, 12, 320, 183, 162, 111,
	108, 73, 86, 87, 88, 104, 325, 144, 213, 144,
	78, 331, 336, 336, 92, 333, 50, 337, 339, 312,
	64, 340, 305, 332, 324, 334, 185, 341, 12, 55,
	48, 345, 262, 77, 349, 250, 54, 90, 91, 71,
	351, 169, 352, 353, 95, 85, 112, 118, 46, 44,
	89, 26, 225, 94, 194, 12, 195, 196, 93, 96,
	73, 86, 87, 88, 278, 28, 29, 30, 31, 78,
	316, 85, 279, 92, 231, 315, 89, 283, 164, 94,
	51, 350, 214, 338, 12, 27, 56, 86, 87, 88,
	326, 170, 77, 33, 236, 78, 90, 91, 71, 92,
	172, 36, 57, 95, 65, 128, 129, 130, 131, 132,
	133, 134, 135, 227, 159, 12, 85, 93, 77, 270,
	346, 89, 90, 91, 94, 328, 310, 314, 282, 95,
	79, 56, 86, 87, 88, 84, 89, 81, 83, 94,
	78, 272, 224, 93, 92, 122, 56, 86, 87, 88,
	76, 184, 240, 181, 45, 150, 21, 47, 11, 92,
	10, 9, 8, 77, 7, 6, 5, 90, 91, 241,
	242, 243, 244, 245, 95, 246, 247, 4, 2, 1,
	0, 89, 90, 91, 94, 0, 0, 0, 93, 95,
	0, 56, 86, 87, 88, 0, 0, 0, 0, 0,
	150, 0, 0, 93, 92, 123, 127, 125, 126, 301,
	0, 0, 128, 129, 130, 131, 132, 133, 134, 135,
	0, 0, 0, 139, 140, 141, 142, 90, 91, 136,
	137, 138, 261, 0, 95, 128, 129, 130, 131, 132,
	133, 134, 135, 0, 0, 0, 0, 0, 93, 0,
	0, 124, 128, 129, 130, 131, 132, 133, 134, 135,
	128, 129, 130, 131, 132, 133, 134, 135,
}
var yyPact = []int{

	185, -1000, -1000, 210, -1000, -1000, -1000, -1000, -1000, -1000,
	-1000, -1000, -1000, -1000, -1000, -1000, -1000, -20, -30, 6,
	3, 390, 342, -1000, -1000, -1000, 340, -1000, 311, 291,
	382, 217, -42, -11, 184, -1000, -12, 184, -1000, 295,
	-51, 184, 184, -1000, -1000, 335, -1000, 354, 291, 218,
	64, 291, 141, -1000, 175, -1000, 50, 280, 38, 184,
	-1000, -1000, 275, -1000, -22, 274, 336, 101, -1000, 219,
	-1000, -1000, 338, 49, 84, 494, -1000, 406, 361, -1000,
	-1000, 466, 197, 195, -1000, 194, -1000, -1000, -1000, -1000,
	-1000, -1000, -1000, -1000, -1000, 186, -1000, 193, 217, 273,
	379, 217, 406, 184, -1000, 331, -58, -1000, 62, -1000,
	265, -1000, -1000, 262, -1000, 263, 335, 184, -1000, 106,
	406, 406, 466, 181, 343, 466, 466, 142, 466, 466,
	466, 466, 466, 466, 466, 466, -1000, -1000, -1000, -1000,
	-1000, -1000, -1000, -1000, 494, -44, 35, 0, 494, -1000,
	421, 276, 335, 390, 63, 74, -1000, 406, 406, 334,
	217, 220, -1000, 372, 406, -1000, -1000, -1000, -1000, -1000,
	98, 184, -1000, -24, -1000, -1000, -1000, -1000, -1000, -1000,
	-1000, 201, 425, 301, 169, 2, -1000, -1000, -1000, -1000,
	-1000, 502, -1000, 421, 181, 466, 466, 502, 477, -1000,
	317, 216, 216, 216, 83, 83, -1000, -1000, -1000, -1000,
	-1000, 466, -1000, 502, -1000, -1, 335, -2, 13, -1000,
	-1000, 29, 18, -1000, 95, 181, 210, -9, -1000, 372,
	360, 369, 84, 257, -1000, -1000, 250, -1000, 377, 231,
	231, -1000, -1000, 149, 127, 139, 133, 130, -27, 5,
	390, -1000, 249, -17, -18, 244, -13, -3, -1000, 502,
	454, 466, -1000, 502, -1000, -16, -1000, -1000, -1000, 406,
	-1000, 302, 163, -1000, -1000, 217, 360, -1000, 466, 466,
	-1000, -1000, 374, 367, 425, 90, -1000, 126, -1000, 124,
	-1000, -1000, -1000, -1000, -1000, 108, 189, 182, -1000, -1000,
	-1000, 466, 502, -1000, -1000, 303, 181, -1000, -1000, 347,
	162, -1000, 174, -1000, 372, 406, 466, 406, -1000, -1000,
	-1000, 184, 184, 502, 387, -1000, 466, 466, -1000, -1000,
	-1000, 360, 84, 159, 84, -28, -1000, -34, 217, 502,
	-1000, 261, -1000, 184, -1000, 141, -1000, 385, 329, -1000,
	-1000, 184, 184, -1000,
}
var yyPgo = []int{

	0, 489, 488, 23, 487, 476, 475, 474, 472, 471,
	470, 468, 361, 467, 466, 464, 11, 26, 16, 20,
	463, 15, 462, 461, 250, 10, 14, 17, 460, 455,
	452, 451, 9, 8, 1, 448, 447, 445, 13, 18,
	7, 440, 438, 437, 6, 436, 3, 435, 5, 430,
	429, 424, 423, 4, 2, 58, 414, 412, 411, 410,
	404, 403, 401, 0, 12, 395,
}
var yyR1 = []int{

	0, 1, 2, 2, 2, 2, 2, 2, 2, 2,
	2, 3, 3, 4, 5, 6, 7, 8, 8, 8,
	9, 9, 9, 10, 11, 11, 11, 65, 12, 13,
	13, 14, 14, 14, 14, 14, 15, 15, 16, 16,
	17, 17, 17, 17, 18, 18, 19, 19, 20, 20,
	20, 21, 21, 21, 21, 22, 22, 22, 22, 22,
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
	6, 7, 4, 5, 4, 5, 4, 0, 2, 0,
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
	-63, 35, 87, -63, 35, -56, 92, -63, -63, -16,
	-17, 73, -18, 35, -27, -34, -28, 67, 44, -41,
	-40, -36, -63, -35, -37, 20, 36, 37, 38, 25,
	71, 72, 48, 92, 28, 78, 15, -24, 33, 76,
	-24, 53, 45, 76, 35, 67, -63, -64, 35, -64,
	90, 35, 20, 64, -64, 8, 53, -19, 19, 76,
	65, 66, -29, 21, 67, 23, 24, 22, 68, 69,
	70, 71, 72, 73, 74, 75, 45, 46, 47, 39,
	40, 41, 42, -27, -34, -27, -3, -33, -34, -34,
	44, 44, 44, 44, -38, -18, -39, 79, 81, -51,
	44, -54, 35, -26, 9, -55, -18, -63, -64, 20,
	-62, 94, -59, 86, 84, 32, 85, 12, 35, 35,
	35, -20, -21, 44, -23, 35, -17, -63, 73, -27,
	-27, -34, -32, 44, 21, 23, 24, -34, -34, 25,
	67, -34, -34, -34, -34, -34, -34, -34, -34, 116,
	116, 53, 116, -34, 116, -16, 18, -16, -3, 82,
	-39, -38, -18, -18, -30, 28, -3, -52, -40, -26,
	-44, 12, -27, 64, -63, -64, -60, 90, -26, 53,
	-22, 54, 55, 56, 57, 58, 60, 61, -21, -3,
	44, -25, -19, 62, 63, 76, -33, -3, -32, -34,
	-34, 65, 25, -34, 116, -16, 116, 116, 82, 80,
	-50, 64, -31, -32, 116, 53, -44, -48, 14, 13,
	35, 35, -42, 10, -21, -21, 54, 59, 54, 59,
	54, 54, 54, 116, 116, 35, 88, 88, 35, 116,
	116, 65, -34, 116, -18, 30, 53, -40, -48, -34,
	-45, -46, -34, -64, -43, 11, 13, 64, 54, 54,
	-25, 44, 44, -34, 31, -32, 53, 53, -47, 26,
	27, -44, -27, -33, -27, -53, -63, -53, 6, -34,
	-46, -48, 116, 53, 116, -54, -49, 16, 34, -63,
	6, 21, -63, -63,
}
var yyDef = []int{

	0, -2, 1, 2, 3, 4, 5, 6, 7, 8,
	9, 10, 27, 27, 27, 27, 27, 181, 172, 0,
	0, 0, 31, 33, 34, 35, 36, 29, 0, 0,
	0, 0, 170, 0, 0, 182, 0, 0, 173, 0,
	168, 0, 0, 12, 32, 0, 37, 28, 0, 0,
	67, 0, 16, 165, 0, 133, 185, 0, 0, 0,
	186, 185, 0, 186, 0, 0, 0, 0, 186, 0,
	38, 40, -2, 185, 44, 45, 74, 0, 0, 104,
	105, 0, 133, 0, 121, 0, 135, 136, 137, 138,
	124, 125, 126, 122, 123, 0, 30, 157, 0, 0,
	72, 0, 0, 0, 186, 0, 183, 19, 0, 22,
	0, 24, 169, 0, 26, 0, 0, 0, 47, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 89, 90, 91, 92,
	93, 94, 95, 77, 0, 0, 0, 0, 102, 116,
	0, 0, 0, 0, 0, 0, 129, 0, 0, 0,
	0, 72, 68, 143, 0, 166, 167, 134, 17, 171,
	0, 0, 186, 179, 174, 175, 176, 177, 178, 23,
	25, 72, 48, 0, -2, 64, 39, 42, 43, 75,
	76, 79, 80, 0, 0, 0, 0, 82, 0, 86,
	0, 108, 109, 110, 111, 112, 113, 114, 115, 78,
	106, 0, 107, 102, 117, 0, 0, 0, 0, 127,
	130, 0, 0, 132, 163, 0, 97, 0, 159, 143,
	151, 0, 73, 0, 184, 20, 0, 180, 139, 0,
	0, 55, 56, 0, 0, 0, 0, 0, 0, 0,
	0, 51, 0, 0, 0, 0, 0, 0, 81, 83,
	0, 0, 87, 103, 118, 0, 120, 88, 128, 0,
	13, 0, 96, 98, 158, 0, 151, 15, 0, 0,
	186, 21, 141, 0, 50, 53, 57, 0, 59, 0,
	61, 62, 63, 49, 66, 69, 0, 0, 65, 100,
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
	44, 116, 73, 71, 53, 72, 76, 74, 3, 3,
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
	107, 108, 109, 110, 111, 112, 113, 114, 115,
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
		//line sql.y:153
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
		//line sql.y:170
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
			yyVAL.node.Push(yyS[yypt-0].node)  // 9: lock_opt
		}
	case 12:
		//line sql.y:184
		{
			yyVAL.node = yyS[yypt-1].node.PushTwo(yyS[yypt-2].node, yyS[yypt-0].node)
		}
	case 13:
		//line sql.y:190
		{
			yyVAL.node = yyS[yypt-6].node
			yyVAL.node.Push(yyS[yypt-5].node) // 0: comment_opt
			yyVAL.node.Push(yyS[yypt-3].node) // 1: dml_table_expression
			yyVAL.node.Push(yyS[yypt-2].node) // 2: column_list_opt
			yyVAL.node.Push(yyS[yypt-1].node) // 3: values
			yyVAL.node.Push(yyS[yypt-0].node) // 4: on_dup_opt
		}
	case 14:
		//line sql.y:201
		{
			yyVAL.node = yyS[yypt-7].node
			yyVAL.node.Push(yyS[yypt-6].node) // 0: comment_opt
			yyVAL.node.Push(yyS[yypt-5].node) // 1: dml_table_expression
			yyVAL.node.Push(yyS[yypt-3].node) // 2: update_list
			yyVAL.node.Push(yyS[yypt-2].node) // 3: where_expression_opt
			yyVAL.node.Push(yyS[yypt-1].node) // 4: order_by_opt
			yyVAL.node.Push(yyS[yypt-0].node) // 5: limit_opt
		}
	case 15:
		//line sql.y:213
		{
			yyVAL.node = yyS[yypt-6].node
			yyVAL.node.Push(yyS[yypt-5].node) // 0: comment_opt
			yyVAL.node.Push(yyS[yypt-3].node) // 1: dml_table_expression
			yyVAL.node.Push(yyS[yypt-2].node) // 2: where_expression_opt
			yyVAL.node.Push(yyS[yypt-1].node) // 3: order_by_opt
			yyVAL.node.Push(yyS[yypt-0].node) // 4: limit_opt
		}
	case 16:
		//line sql.y:224
		{
			yyVAL.node = yyS[yypt-2].node
			yyVAL.node.Push(yyS[yypt-1].node)
			yyVAL.node.Push(yyS[yypt-0].node)
		}
	case 17:
		//line sql.y:232
		{
			yyVAL.node.Push(yyS[yypt-1].node)
		}
	case 18:
		//line sql.y:236
		{
			// Change this to an alter statement
			yyVAL.node = NewSimpleParseNode(ALTER, "alter")
			yyVAL.node.Push(yyS[yypt-1].node)
		}
	case 19:
		//line sql.y:242
		{
			yyVAL.node.Push(yyS[yypt-1].node)
		}
	case 20:
		//line sql.y:248
		{
			yyVAL.node.Push(yyS[yypt-2].node)
		}
	case 21:
		//line sql.y:252
		{
			// Change this to a rename statement
			yyVAL.node = NewSimpleParseNode(RENAME, "rename")
			yyVAL.node.PushTwo(yyS[yypt-3].node, yyS[yypt-0].node)
		}
	case 22:
		//line sql.y:258
		{
			yyVAL.node.Push(yyS[yypt-1].node)
		}
	case 23:
		//line sql.y:264
		{
			yyVAL.node.PushTwo(yyS[yypt-2].node, yyS[yypt-0].node)
		}
	case 24:
		//line sql.y:270
		{
			yyVAL.node.Push(yyS[yypt-0].node)
		}
	case 25:
		//line sql.y:274
		{
			// Change this to an alter statement
			yyVAL.node = NewSimpleParseNode(ALTER, "alter")
			yyVAL.node.Push(yyS[yypt-0].node)
		}
	case 26:
		//line sql.y:280
		{
			yyVAL.node.Push(yyS[yypt-1].node)
		}
	case 27:
		//line sql.y:285
		{
			SetAllowComments(yylex, true)
		}
	case 28:
		//line sql.y:289
		{
			yyVAL.node = yyS[yypt-0].node
			SetAllowComments(yylex, false)
		}
	case 29:
		//line sql.y:295
		{
			yyVAL.node = NewSimpleParseNode(COMMENT_LIST, "")
		}
	case 30:
		//line sql.y:299
		{
			yyVAL.node = yyS[yypt-1].node.Push(yyS[yypt-0].node)
		}
	case 31:
		yyVAL.node = yyS[yypt-0].node
	case 32:
		//line sql.y:306
		{
			yyVAL.node = NewSimpleParseNode(UNION_ALL, "union all")
		}
	case 33:
		yyVAL.node = yyS[yypt-0].node
	case 34:
		yyVAL.node = yyS[yypt-0].node
	case 35:
		yyVAL.node = yyS[yypt-0].node
	case 36:
		//line sql.y:314
		{
			yyVAL.node = NewSimpleParseNode(NO_DISTINCT, "")
		}
	case 37:
		//line sql.y:318
		{
			yyVAL.node = NewSimpleParseNode(DISTINCT, "distinct")
		}
	case 38:
		//line sql.y:324
		{
			yyVAL.node = NewSimpleParseNode(NODE_LIST, "node_list")
			yyVAL.node.Push(yyS[yypt-0].node)
		}
	case 39:
		//line sql.y:329
		{
			yyVAL.node.Push(yyS[yypt-0].node)
		}
	case 40:
		//line sql.y:335
		{
			yyVAL.node = NewSimpleParseNode(SELECT_STAR, "*")
		}
	case 41:
		yyVAL.node = yyS[yypt-0].node
	case 42:
		//line sql.y:340
		{
			yyVAL.node = yyS[yypt-1].node.PushTwo(yyS[yypt-2].node, yyS[yypt-0].node)
		}
	case 43:
		//line sql.y:344
		{
			yyVAL.node = yyS[yypt-1].node.PushTwo(yyS[yypt-2].node, NewSimpleParseNode(SELECT_STAR, "*"))
		}
	case 44:
		yyVAL.node = yyS[yypt-0].node
	case 45:
		yyVAL.node = yyS[yypt-0].node
	case 46:
		//line sql.y:353
		{
			yyVAL.node = NewSimpleParseNode(AS, "as")
		}
	case 47:
		yyVAL.node = yyS[yypt-0].node
	case 48:
		//line sql.y:360
		{
			yyVAL.node = NewSimpleParseNode(NODE_LIST, "node_list")
			yyVAL.node.Push(yyS[yypt-0].node)
		}
	case 49:
		//line sql.y:365
		{
			yyVAL.node = yyS[yypt-2].node.Push(yyS[yypt-1].node)
		}
	case 50:
		//line sql.y:369
		{
			yyVAL.node.Push(yyS[yypt-0].node)
		}
	case 51:
		//line sql.y:375
		{
			yyVAL.node = NewSimpleParseNode(TABLE_EXPR, "")
			yyVAL.node.Push(yyS[yypt-1].node)
			yyVAL.node.Push(NewSimpleParseNode(NODE_LIST, "node_list"))
			yyVAL.node.Push(yyS[yypt-0].node)
		}
	case 52:
		//line sql.y:382
		{
			yyVAL.node = NewSimpleParseNode(TABLE_EXPR, "")
			yyVAL.node.Push(yyS[yypt-3].node)
			yyVAL.node.Push(NewSimpleParseNode(NODE_LIST, "node_list").Push(yyS[yypt-1].node))
			yyVAL.node.Push(yyS[yypt-0].node)
		}
	case 53:
		//line sql.y:389
		{
			yyVAL.node = yyS[yypt-1].node.PushTwo(yyS[yypt-2].node, yyS[yypt-0].node)
		}
	case 54:
		//line sql.y:393
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
		//line sql.y:404
		{
			yyVAL.node = NewSimpleParseNode(LEFT, "left join")
		}
	case 58:
		//line sql.y:408
		{
			yyVAL.node = NewSimpleParseNode(LEFT, "left join")
		}
	case 59:
		//line sql.y:412
		{
			yyVAL.node = NewSimpleParseNode(RIGHT, "right join")
		}
	case 60:
		//line sql.y:416
		{
			yyVAL.node = NewSimpleParseNode(RIGHT, "right join")
		}
	case 61:
		//line sql.y:420
		{
			yyVAL.node = yyS[yypt-0].node
		}
	case 62:
		//line sql.y:424
		{
			yyVAL.node = NewSimpleParseNode(CROSS, "cross join")
		}
	case 63:
		//line sql.y:428
		{
			yyVAL.node = NewSimpleParseNode(NATURAL, "natural join")
		}
	case 64:
		yyVAL.node = yyS[yypt-0].node
	case 65:
		//line sql.y:435
		{
			yyVAL.node = yyS[yypt-1].node.PushTwo(yyS[yypt-2].node, yyS[yypt-0].node)
		}
	case 66:
		//line sql.y:439
		{
			yyVAL.node = yyS[yypt-2].node.Push(yyS[yypt-1].node)
		}
	case 67:
		yyVAL.node = yyS[yypt-0].node
	case 68:
		//line sql.y:446
		{
			yyVAL.node = yyS[yypt-1].node.PushTwo(yyS[yypt-2].node, yyS[yypt-0].node)
		}
	case 69:
		//line sql.y:451
		{
			yyVAL.node = NewSimpleParseNode(USE, "use")
		}
	case 70:
		//line sql.y:455
		{
			yyVAL.node.Push(yyS[yypt-1].node)
		}
	case 71:
		//line sql.y:459
		{
			yyVAL.node.Push(yyS[yypt-1].node)
		}
	case 72:
		//line sql.y:464
		{
			yyVAL.node = NewSimpleParseNode(WHERE, "where")
		}
	case 73:
		//line sql.y:468
		{
			yyVAL.node = yyS[yypt-1].node.Push(yyS[yypt-0].node)
		}
	case 74:
		yyVAL.node = yyS[yypt-0].node
	case 75:
		//line sql.y:475
		{
			yyVAL.node = yyS[yypt-1].node.PushTwo(yyS[yypt-2].node, yyS[yypt-0].node)
		}
	case 76:
		//line sql.y:479
		{
			yyVAL.node = yyS[yypt-1].node.PushTwo(yyS[yypt-2].node, yyS[yypt-0].node)
		}
	case 77:
		//line sql.y:483
		{
			yyVAL.node = yyS[yypt-1].node.Push(yyS[yypt-0].node)
		}
	case 78:
		//line sql.y:487
		{
			yyVAL.node = yyS[yypt-2].node.Push(yyS[yypt-1].node)
		}
	case 79:
		//line sql.y:493
		{
			yyVAL.node = yyS[yypt-1].node.PushTwo(yyS[yypt-2].node, yyS[yypt-0].node)
		}
	case 80:
		//line sql.y:497
		{
			yyVAL.node = yyS[yypt-1].node.PushTwo(yyS[yypt-2].node, yyS[yypt-0].node)
		}
	case 81:
		//line sql.y:501
		{
			yyVAL.node = NewSimpleParseNode(NOT_IN, "not in").PushTwo(yyS[yypt-3].node, yyS[yypt-0].node)
		}
	case 82:
		//line sql.y:505
		{
			yyVAL.node = yyS[yypt-1].node.PushTwo(yyS[yypt-2].node, yyS[yypt-0].node)
		}
	case 83:
		//line sql.y:509
		{
			yyVAL.node = NewSimpleParseNode(NOT_LIKE, "not like").PushTwo(yyS[yypt-3].node, yyS[yypt-0].node)
		}
	case 84:
		//line sql.y:513
		{
			yyVAL.node = yyS[yypt-3].node
			yyVAL.node.Push(yyS[yypt-4].node)
			yyVAL.node.Push(yyS[yypt-2].node)
			yyVAL.node.Push(yyS[yypt-0].node)
		}
	case 85:
		//line sql.y:520
		{
			yyVAL.node = NewSimpleParseNode(NOT_BETWEEN, "not between")
			yyVAL.node.Push(yyS[yypt-5].node)
			yyVAL.node.Push(yyS[yypt-2].node)
			yyVAL.node.Push(yyS[yypt-0].node)
		}
	case 86:
		//line sql.y:527
		{
			yyVAL.node = NewSimpleParseNode(IS_NULL, "is null").Push(yyS[yypt-2].node)
		}
	case 87:
		//line sql.y:531
		{
			yyVAL.node = NewSimpleParseNode(IS_NOT_NULL, "is not null").Push(yyS[yypt-3].node)
		}
	case 88:
		//line sql.y:535
		{
			yyVAL.node = yyS[yypt-3].node.Push(yyS[yypt-1].node)
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
		//line sql.y:550
		{
			yyVAL.node = yyS[yypt-1].node.Push(yyS[yypt-0].node)
		}
	case 97:
		yyVAL.node = yyS[yypt-0].node
	case 98:
		//line sql.y:557
		{
			yyVAL.node = NewSimpleParseNode(NODE_LIST, "node_list")
			yyVAL.node.Push(yyS[yypt-0].node)
		}
	case 99:
		//line sql.y:562
		{
			yyVAL.node.Push(yyS[yypt-0].node)
		}
	case 100:
		//line sql.y:568
		{
			yyVAL.node = yyS[yypt-2].node.Push(yyS[yypt-1].node)
		}
	case 101:
		//line sql.y:572
		{
			yyVAL.node = yyS[yypt-2].node.Push(yyS[yypt-1].node)
		}
	case 102:
		//line sql.y:578
		{
			yyVAL.node = NewSimpleParseNode(NODE_LIST, "node_list")
			yyVAL.node.Push(yyS[yypt-0].node)
		}
	case 103:
		//line sql.y:583
		{
			yyVAL.node.Push(yyS[yypt-0].node)
		}
	case 104:
		yyVAL.node = yyS[yypt-0].node
	case 105:
		yyVAL.node = yyS[yypt-0].node
	case 106:
		//line sql.y:591
		{
			yyVAL.node = yyS[yypt-2].node.Push(yyS[yypt-1].node)
		}
	case 107:
		//line sql.y:595
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
	case 108:
		//line sql.y:607
		{
			yyVAL.node = yyS[yypt-1].node.PushTwo(yyS[yypt-2].node, yyS[yypt-0].node)
		}
	case 109:
		//line sql.y:611
		{
			yyVAL.node = yyS[yypt-1].node.PushTwo(yyS[yypt-2].node, yyS[yypt-0].node)
		}
	case 110:
		//line sql.y:615
		{
			yyVAL.node = yyS[yypt-1].node.PushTwo(yyS[yypt-2].node, yyS[yypt-0].node)
		}
	case 111:
		//line sql.y:619
		{
			yyVAL.node = yyS[yypt-1].node.PushTwo(yyS[yypt-2].node, yyS[yypt-0].node)
		}
	case 112:
		//line sql.y:623
		{
			yyVAL.node = yyS[yypt-1].node.PushTwo(yyS[yypt-2].node, yyS[yypt-0].node)
		}
	case 113:
		//line sql.y:627
		{
			yyVAL.node = yyS[yypt-1].node.PushTwo(yyS[yypt-2].node, yyS[yypt-0].node)
		}
	case 114:
		//line sql.y:631
		{
			yyVAL.node = yyS[yypt-1].node.PushTwo(yyS[yypt-2].node, yyS[yypt-0].node)
		}
	case 115:
		//line sql.y:635
		{
			yyVAL.node = yyS[yypt-1].node.PushTwo(yyS[yypt-2].node, yyS[yypt-0].node)
		}
	case 116:
		//line sql.y:639
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
		//line sql.y:655
		{
			yyS[yypt-2].node.Type = FUNCTION
			yyVAL.node = yyS[yypt-2].node.Push(NewSimpleParseNode(NODE_LIST, "node_list"))
		}
	case 118:
		//line sql.y:660
		{
			yyS[yypt-3].node.Type = FUNCTION
			yyVAL.node = yyS[yypt-3].node.Push(yyS[yypt-1].node)
		}
	case 119:
		//line sql.y:665
		{
			yyS[yypt-4].node.Type = FUNCTION
			yyVAL.node = yyS[yypt-4].node.Push(yyS[yypt-2].node)
			yyVAL.node = yyS[yypt-4].node.Push(yyS[yypt-1].node)
		}
	case 120:
		//line sql.y:671
		{
			yyS[yypt-3].node.Type = FUNCTION
			yyVAL.node = yyS[yypt-3].node.Push(yyS[yypt-1].node)
		}
	case 121:
		yyVAL.node = yyS[yypt-0].node
	case 122:
		yyVAL.node = yyS[yypt-0].node
	case 123:
		yyVAL.node = yyS[yypt-0].node
	case 124:
		//line sql.y:683
		{
			yyVAL.node = NewSimpleParseNode(UPLUS, "+")
		}
	case 125:
		//line sql.y:687
		{
			yyVAL.node = NewSimpleParseNode(UMINUS, "-")
		}
	case 126:
		yyVAL.node = yyS[yypt-0].node
	case 127:
		//line sql.y:694
		{
			yyVAL.node = NewSimpleParseNode(CASE_WHEN, "case")
			yyVAL.node.Push(yyS[yypt-1].node)
		}
	case 128:
		//line sql.y:699
		{
			yyVAL.node.PushTwo(yyS[yypt-2].node, yyS[yypt-1].node)
		}
	case 129:
		//line sql.y:705
		{
			yyVAL.node = NewSimpleParseNode(WHEN_LIST, "when_list")
			yyVAL.node.Push(yyS[yypt-0].node)
		}
	case 130:
		//line sql.y:710
		{
			yyVAL.node.Push(yyS[yypt-0].node)
		}
	case 131:
		//line sql.y:716
		{
			yyVAL.node.PushTwo(yyS[yypt-2].node, yyS[yypt-0].node)
		}
	case 132:
		//line sql.y:720
		{
			yyVAL.node.Push(yyS[yypt-0].node)
		}
	case 133:
		yyVAL.node = yyS[yypt-0].node
	case 134:
		//line sql.y:727
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
		//line sql.y:738
		{
			yyVAL.node = NewSimpleParseNode(GROUP, "group")
		}
	case 140:
		//line sql.y:742
		{
			yyVAL.node = yyS[yypt-2].node.Push(yyS[yypt-0].node)
		}
	case 141:
		//line sql.y:747
		{
			yyVAL.node = NewSimpleParseNode(HAVING, "having")
		}
	case 142:
		//line sql.y:751
		{
			yyVAL.node = yyS[yypt-1].node.Push(yyS[yypt-0].node)
		}
	case 143:
		//line sql.y:756
		{
			yyVAL.node = NewSimpleParseNode(ORDER, "order")
		}
	case 144:
		//line sql.y:760
		{
			yyVAL.node = yyS[yypt-2].node.Push(yyS[yypt-0].node)
		}
	case 145:
		//line sql.y:766
		{
			yyVAL.node = NewSimpleParseNode(NODE_LIST, "node_list")
			yyVAL.node.Push(yyS[yypt-0].node)
		}
	case 146:
		//line sql.y:771
		{
			yyVAL.node = yyS[yypt-2].node.Push(yyS[yypt-0].node)
		}
	case 147:
		//line sql.y:777
		{
			yyVAL.node = yyS[yypt-0].node.Push(yyS[yypt-1].node)
		}
	case 148:
		//line sql.y:782
		{
			yyVAL.node = NewSimpleParseNode(ASC, "asc")
		}
	case 149:
		yyVAL.node = yyS[yypt-0].node
	case 150:
		yyVAL.node = yyS[yypt-0].node
	case 151:
		//line sql.y:789
		{
			yyVAL.node = NewSimpleParseNode(LIMIT, "limit")
		}
	case 152:
		//line sql.y:793
		{
			yyVAL.node = yyS[yypt-1].node.Push(yyS[yypt-0].node)
		}
	case 153:
		//line sql.y:797
		{
			yyVAL.node = yyS[yypt-3].node.PushTwo(yyS[yypt-2].node, yyS[yypt-0].node)
		}
	case 154:
		//line sql.y:802
		{
			yyVAL.node = NewSimpleParseNode(NO_LOCK, "")
		}
	case 155:
		//line sql.y:806
		{
			yyVAL.node = NewSimpleParseNode(FOR_UPDATE, " for update")
		}
	case 156:
		//line sql.y:810
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
		//line sql.y:823
		{
			yyVAL.node = NewSimpleParseNode(COLUMN_LIST, "")
		}
	case 158:
		//line sql.y:827
		{
			yyVAL.node = yyS[yypt-1].node
		}
	case 159:
		//line sql.y:833
		{
			yyVAL.node = NewSimpleParseNode(COLUMN_LIST, "")
			yyVAL.node.Push(yyS[yypt-0].node)
		}
	case 160:
		//line sql.y:838
		{
			yyVAL.node = yyS[yypt-2].node.Push(yyS[yypt-0].node)
		}
	case 161:
		//line sql.y:844
		{
			yyVAL.node = NewSimpleParseNode(INDEX_LIST, "")
			yyVAL.node.Push(yyS[yypt-0].node)
		}
	case 162:
		//line sql.y:849
		{
			yyVAL.node = yyS[yypt-2].node.Push(yyS[yypt-0].node)
		}
	case 163:
		//line sql.y:854
		{
			yyVAL.node = NewSimpleParseNode(DUPLICATE, "duplicate")
		}
	case 164:
		//line sql.y:858
		{
			yyVAL.node = yyS[yypt-3].node.Push(yyS[yypt-0].node)
		}
	case 165:
		//line sql.y:864
		{
			yyVAL.node = NewSimpleParseNode(NODE_LIST, "node_list")
			yyVAL.node.Push(yyS[yypt-0].node)
		}
	case 166:
		//line sql.y:869
		{
			yyVAL.node = yyS[yypt-2].node.Push(yyS[yypt-0].node)
		}
	case 167:
		//line sql.y:875
		{
			yyVAL.node = yyS[yypt-1].node.PushTwo(yyS[yypt-2].node, yyS[yypt-0].node)
		}
	case 168:
		//line sql.y:880
		{
			yyVAL.node = nil
		}
	case 169:
		yyVAL.node = yyS[yypt-0].node
	case 170:
		//line sql.y:884
		{
			yyVAL.node = nil
		}
	case 171:
		yyVAL.node = yyS[yypt-0].node
	case 172:
		//line sql.y:888
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
		//line sql.y:899
		{
			yyVAL.node = nil
		}
	case 180:
		yyVAL.node = yyS[yypt-0].node
	case 181:
		//line sql.y:903
		{
			yyVAL.node = nil
		}
	case 182:
		yyVAL.node = yyS[yypt-0].node
	case 183:
		//line sql.y:907
		{
			yyVAL.node = nil
		}
	case 184:
		yyVAL.node = yyS[yypt-0].node
	case 185:
		//line sql.y:912
		{
			yyVAL.node.LowerCase()
		}
	case 186:
		//line sql.y:917
		{
			ForceEOF(yylex)
		}
	}
	goto yystack /* stack new state and value */
}
