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
const TO = 57416
const IGNORE = 57417
const IF = 57418
const UNIQUE = 57419
const USING = 57420
const NODE_LIST = 57421
const UPLUS = 57422
const UMINUS = 57423
const CASE_WHEN = 57424
const WHEN_LIST = 57425
const SELECT_STAR = 57426
const NO_DISTINCT = 57427
const FUNCTION = 57428
const NO_LOCK = 57429
const FOR_UPDATE = 57430
const LOCK_IN_SHARE_MODE = 57431
const NOT_IN = 57432
const NOT_LIKE = 57433
const NOT_BETWEEN = 57434
const IS_NULL = 57435
const IS_NOT_NULL = 57436
const UNION_ALL = 57437
const COMMENT_LIST = 57438
const COLUMN_LIST = 57439
const INDEX_LIST = 57440
const TABLE_EXPR = 57441

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
	-1, 64,
	35, 43,
	-2, 38,
	-1, 172,
	35, 43,
	-2, 66,
}

const yyNprod = 184
const yyPrivate = 57344

var yyTokenNames []string
var yyStates []string

const yyLast = 549

var yyAct = []int{

	74, 67, 49, 302, 326, 268, 221, 136, 181, 242,
	170, 156, 72, 145, 143, 105, 159, 66, 334, 64,
	334, 135, 3, 104, 152, 62, 32, 61, 108, 109,
	201, 34, 51, 266, 22, 23, 24, 25, 59, 104,
	60, 104, 201, 40, 50, 54, 36, 22, 23, 24,
	25, 22, 23, 24, 25, 228, 97, 99, 38, 39,
	288, 287, 55, 22, 23, 24, 25, 260, 56, 37,
	246, 132, 137, 198, 146, 138, 147, 259, 199, 206,
	335, 77, 333, 107, 91, 294, 81, 131, 134, 86,
	96, 51, 290, 150, 51, 265, 65, 78, 79, 80,
	291, 257, 52, 255, 202, 70, 175, 144, 177, 84,
	132, 132, 180, 285, 155, 186, 187, 258, 190, 191,
	192, 193, 194, 195, 196, 197, 178, 179, 69, 200,
	174, 310, 82, 83, 63, 146, 106, 147, 154, 87,
	176, 203, 146, 188, 147, 209, 121, 122, 123, 308,
	219, 262, 85, 108, 109, 132, 224, 210, 165, 211,
	225, 102, 218, 309, 208, 283, 212, 213, 205, 207,
	216, 223, 226, 320, 321, 220, 204, 282, 163, 244,
	245, 166, 239, 281, 203, 189, 250, 251, 243, 153,
	247, 93, 249, 240, 229, 244, 245, 279, 103, 177,
	153, 201, 280, 254, 248, 232, 233, 234, 235, 236,
	318, 237, 238, 297, 182, 116, 117, 118, 119, 120,
	121, 122, 123, 277, 264, 210, 94, 267, 278, 46,
	162, 164, 161, 230, 256, 119, 120, 121, 122, 123,
	77, 275, 276, 104, 93, 81, 12, 313, 86, 22,
	23, 24, 25, 173, 293, 133, 78, 79, 80, 312,
	173, 149, 241, 240, 70, 142, 284, 219, 84, 171,
	141, 300, 303, 299, 140, 89, 52, 173, 92, 298,
	295, 133, 338, 304, 289, 286, 241, 69, 272, 271,
	168, 82, 83, 167, 314, 151, 311, 100, 87, 146,
	339, 147, 98, 95, 47, 57, 316, 90, 132, 203,
	132, 85, 322, 327, 327, 324, 315, 296, 328, 330,
	303, 45, 331, 253, 323, 342, 325, 12, 332, 12,
	51, 77, 336, 157, 101, 340, 81, 106, 183, 86,
	184, 185, 43, 343, 344, 77, 65, 78, 79, 80,
	81, 215, 41, 86, 88, 70, 269, 307, 270, 84,
	133, 78, 79, 80, 222, 12, 13, 14, 15, 70,
	306, 48, 274, 84, 153, 27, 12, 341, 69, 329,
	158, 33, 82, 83, 63, 227, 160, 35, 12, 87,
	77, 53, 69, 58, 16, 81, 82, 83, 86, 217,
	148, 261, 85, 87, 337, 133, 78, 79, 80, 81,
	319, 301, 86, 305, 70, 273, 85, 71, 84, 133,
	78, 79, 80, 232, 233, 234, 235, 236, 139, 237,
	238, 76, 84, 73, 75, 263, 214, 69, 110, 68,
	81, 82, 83, 86, 17, 18, 20, 19, 87, 172,
	133, 78, 79, 80, 26, 82, 83, 231, 169, 139,
	42, 85, 87, 84, 111, 115, 113, 114, 28, 29,
	30, 31, 21, 44, 11, 85, 10, 9, 8, 317,
	7, 6, 127, 128, 129, 130, 82, 83, 124, 125,
	126, 5, 4, 87, 116, 117, 118, 119, 120, 121,
	122, 123, 2, 1, 0, 0, 85, 0, 0, 0,
	112, 116, 117, 118, 119, 120, 121, 122, 123, 292,
	0, 0, 116, 117, 118, 119, 120, 121, 122, 123,
	252, 0, 0, 116, 117, 118, 119, 120, 121, 122,
	123, 116, 117, 118, 119, 120, 121, 122, 123,
}
var yyPact = []int{

	361, -1000, -1000, 200, -1000, -1000, -1000, -1000, -1000, -1000,
	-1000, -1000, -1000, -1000, -1000, -1000, -1000, -61, -44, -18,
	-29, 372, 335, -1000, -1000, -1000, 324, -1000, 292, 269,
	363, 241, -46, -26, -1000, -19, -1000, 270, -53, 241,
	-1000, -1000, 311, -1000, 339, 269, 274, 8, 269, 138,
	-1000, 181, -1000, 268, 23, 241, 267, -32, 262, 314,
	97, 190, -1000, -1000, 318, 7, 88, 443, -1000, 370,
	325, -1000, -1000, 415, 230, 226, -1000, 221, -1000, -1000,
	-1000, -1000, -1000, -1000, -1000, -1000, -1000, 220, -1000, 217,
	241, 260, 365, 241, 370, -1000, 313, -77, 146, 258,
	-1000, -1000, 255, 225, 311, 241, -1000, 67, 370, 370,
	415, 170, 317, 415, 415, 118, 415, 415, 415, 415,
	415, 415, 415, 415, -1000, -1000, -1000, -1000, -1000, -1000,
	-1000, -1000, 443, -3, -37, 14, -11, 443, -1000, 384,
	61, 311, 372, 63, 56, -1000, 370, 370, 323, 246,
	191, -1000, 352, 370, -1000, -1000, -1000, -1000, 92, 241,
	-1000, -34, -1000, -1000, -1000, -1000, -1000, -1000, -1000, 180,
	369, 242, 117, -6, -1000, -1000, -1000, -1000, -1000, -1000,
	473, -1000, 384, 170, 415, 415, 473, 465, -1000, 298,
	164, 164, 164, 73, 73, -1000, -1000, -1000, 241, -1000,
	-1000, 415, -1000, 473, -1000, -12, 311, -14, 2, -1000,
	-1000, -5, -13, -1000, 87, 170, 200, -20, -1000, -1000,
	352, 342, 345, 88, 254, -1000, -1000, 253, -1000, 362,
	218, 218, -1000, -1000, 169, 143, 129, 123, 111, 151,
	-2, 372, -1000, 250, -27, -28, 249, -23, -15, -1000,
	473, 454, 415, -1000, 473, -1000, -30, -1000, -1000, -1000,
	370, -1000, 287, 160, -1000, -1000, 246, 342, -1000, 415,
	415, -1000, -1000, 359, 344, 369, 85, -1000, 109, -1000,
	77, -1000, -1000, -1000, -1000, -1000, 133, 215, 203, -1000,
	-1000, -1000, 415, 473, -1000, -1000, 285, 170, -1000, -1000,
	426, 157, -1000, 147, -1000, 352, 370, 415, 370, -1000,
	-1000, -1000, 241, 241, 473, 373, -1000, 415, 415, -1000,
	-1000, -1000, 342, 88, 148, 88, -33, -1000, -35, 241,
	473, -1000, 266, -1000, 241, -1000, 138, -1000, 371, 304,
	-1000, -1000, 241, 241, -1000,
}
var yyPgo = []int{

	0, 503, 502, 21, 492, 491, 481, 480, 478, 477,
	476, 474, 454, 473, 472, 460, 27, 25, 19, 15,
	458, 10, 457, 449, 229, 9, 24, 17, 439, 438,
	436, 435, 8, 7, 1, 434, 433, 431, 14, 13,
	12, 417, 415, 413, 6, 411, 3, 410, 5, 404,
	401, 400, 399, 4, 2, 44, 393, 391, 387, 386,
	385, 381, 380, 0, 11, 375,
}
var yyR1 = []int{

	0, 1, 2, 2, 2, 2, 2, 2, 2, 2,
	2, 3, 3, 4, 5, 6, 7, 8, 8, 9,
	9, 10, 11, 11, 65, 12, 13, 13, 14, 14,
	14, 14, 14, 15, 15, 16, 16, 17, 17, 17,
	17, 18, 18, 19, 19, 20, 20, 20, 21, 21,
	21, 21, 22, 22, 22, 22, 22, 22, 22, 22,
	22, 23, 23, 23, 24, 24, 25, 25, 25, 26,
	26, 27, 27, 27, 27, 27, 28, 28, 28, 28,
	28, 28, 28, 28, 28, 28, 29, 29, 29, 29,
	29, 29, 29, 30, 30, 31, 31, 32, 32, 33,
	33, 34, 34, 34, 34, 34, 34, 34, 34, 34,
	34, 34, 34, 34, 34, 34, 34, 34, 34, 35,
	35, 36, 36, 36, 37, 37, 38, 38, 39, 39,
	40, 40, 41, 41, 41, 41, 42, 42, 43, 43,
	44, 44, 45, 45, 46, 47, 47, 47, 48, 48,
	48, 49, 49, 49, 51, 51, 52, 52, 53, 53,
	50, 50, 54, 54, 55, 56, 56, 57, 57, 58,
	58, 59, 59, 59, 59, 59, 60, 60, 61, 61,
	62, 62, 63, 64,
}
var yyR2 = []int{

	0, 1, 1, 1, 1, 1, 1, 1, 1, 1,
	1, 12, 3, 7, 8, 7, 3, 5, 8, 6,
	7, 5, 4, 5, 0, 2, 0, 2, 1, 2,
	1, 1, 1, 0, 1, 1, 3, 1, 1, 3,
	3, 1, 1, 0, 1, 1, 3, 3, 2, 4,
	3, 5, 1, 1, 2, 3, 2, 3, 2, 2,
	2, 1, 3, 3, 1, 3, 0, 5, 5, 0,
	2, 1, 3, 3, 2, 3, 3, 3, 4, 3,
	4, 5, 6, 3, 4, 4, 1, 1, 1, 1,
	1, 1, 1, 2, 1, 1, 3, 3, 3, 1,
	3, 1, 1, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 2, 3, 4, 5, 4, 1, 1,
	1, 1, 1, 1, 3, 4, 1, 2, 4, 2,
	1, 3, 1, 1, 1, 1, 0, 3, 0, 2,
	0, 3, 1, 3, 2, 0, 1, 1, 0, 2,
	4, 0, 2, 4, 0, 3, 1, 3, 1, 3,
	0, 5, 1, 3, 3, 0, 2, 0, 3, 0,
	1, 1, 1, 1, 1, 1, 0, 1, 0, 1,
	0, 2, 1, 0,
}
var yyChk = []int{

	-1000, -1, -2, -3, -4, -5, -6, -7, -8, -9,
	-10, -11, 4, 5, 6, 7, 33, 83, 84, 86,
	85, -14, 49, 50, 51, 52, -12, -65, -12, -12,
	-12, -12, 87, -61, 92, -58, 90, 87, 87, 88,
	-3, 17, -15, 18, -13, 29, -24, 35, 8, -54,
	-55, -63, 35, -57, 91, 88, 87, 35, -56, 91,
	-63, -16, -17, 73, -18, 35, -27, -34, -28, 67,
	44, -41, -40, -36, -63, -35, -37, 20, 36, 37,
	38, 25, 71, 72, 48, 91, 28, 78, 15, -24,
	33, 76, -24, 53, 45, 35, 67, -63, 35, 89,
	35, 20, 64, 8, 53, -19, 19, 76, 65, 66,
	-29, 21, 67, 23, 24, 22, 68, 69, 70, 71,
	72, 73, 74, 75, 45, 46, 47, 39, 40, 41,
	42, -27, -34, 35, -27, -3, -33, -34, -34, 44,
	44, 44, 44, -38, -18, -39, 79, 81, -51, 44,
	-54, 35, -26, 9, -55, -18, -64, 20, -62, 93,
	-59, 86, 84, 32, 85, 12, 35, 35, 35, -20,
	-21, 44, -23, 35, -17, -63, 73, -63, -27, -27,
	-34, -32, 44, 21, 23, 24, -34, -34, 25, 67,
	-34, -34, -34, -34, -34, -34, -34, -34, 76, 115,
	115, 53, 115, -34, 115, -16, 18, -16, -3, 82,
	-39, -38, -18, -18, -30, 28, -3, -52, -40, -63,
	-26, -44, 12, -27, 64, -63, -64, -60, 89, -26,
	53, -22, 54, 55, 56, 57, 58, 60, 61, -21,
	-3, 44, -25, -19, 62, 63, 76, -33, -3, -32,
	-34, -34, 65, 25, -34, 115, -16, 115, 115, 82,
	80, -50, 64, -31, -32, 115, 53, -44, -48, 14,
	13, 35, 35, -42, 10, -21, -21, 54, 59, 54,
	59, 54, 54, 54, 115, 115, 35, 88, 88, 35,
	115, 115, 65, -34, 115, -18, 30, 53, -40, -48,
	-34, -45, -46, -34, -64, -43, 11, 13, 64, 54,
	54, -25, 44, 44, -34, 31, -32, 53, 53, -47,
	26, 27, -44, -27, -33, -27, -53, -63, -53, 6,
	-34, -46, -48, 115, 53, 115, -54, -49, 16, 34,
	-63, 6, 21, -63, -63,
}
var yyDef = []int{

	0, -2, 1, 2, 3, 4, 5, 6, 7, 8,
	9, 10, 24, 24, 24, 24, 24, 178, 169, 0,
	0, 0, 28, 30, 31, 32, 33, 26, 0, 0,
	0, 0, 167, 0, 179, 0, 170, 0, 165, 0,
	12, 29, 0, 34, 25, 0, 0, 64, 0, 16,
	162, 0, 182, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 35, 37, -2, 182, 41, 42, 71, 0,
	0, 101, 102, 0, 130, 0, 118, 0, 132, 133,
	134, 135, 121, 122, 123, 119, 120, 0, 27, 154,
	0, 0, 69, 0, 0, 183, 0, 180, 0, 0,
	22, 166, 0, 0, 0, 0, 44, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 86, 87, 88, 89, 90, 91,
	92, 74, 0, 182, 0, 0, 0, 99, 113, 0,
	0, 0, 0, 0, 0, 126, 0, 0, 0, 0,
	69, 65, 140, 0, 163, 164, 17, 168, 0, 0,
	183, 176, 171, 172, 173, 174, 175, 21, 23, 69,
	45, 0, -2, 61, 36, 39, 40, 131, 72, 73,
	76, 77, 0, 0, 0, 0, 79, 0, 83, 0,
	105, 106, 107, 108, 109, 110, 111, 112, 0, 75,
	103, 0, 104, 99, 114, 0, 0, 0, 0, 124,
	127, 0, 0, 129, 160, 0, 94, 0, 156, 130,
	140, 148, 0, 70, 0, 181, 19, 0, 177, 136,
	0, 0, 52, 53, 0, 0, 0, 0, 0, 0,
	0, 0, 48, 0, 0, 0, 0, 0, 0, 78,
	80, 0, 0, 84, 100, 115, 0, 117, 85, 125,
	0, 13, 0, 93, 95, 155, 0, 148, 15, 0,
	0, 183, 20, 138, 0, 47, 50, 54, 0, 56,
	0, 58, 59, 60, 46, 63, 66, 0, 0, 62,
	97, 98, 0, 81, 116, 128, 0, 0, 157, 14,
	149, 141, 142, 145, 18, 140, 0, 0, 0, 55,
	57, 49, 0, 0, 82, 0, 96, 0, 0, 144,
	146, 147, 148, 139, 137, 51, 0, 158, 0, 0,
	150, 143, 151, 67, 0, 68, 161, 11, 0, 0,
	159, 152, 0, 0, 153,
}
var yyTok1 = []int{

	1, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 75, 68, 3,
	44, 115, 73, 71, 53, 72, 76, 74, 3, 3,
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
	107, 108, 109, 110, 111, 112, 113, 114,
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
		//line sql.y:244
		{
			yyVAL.node.Push(yyS[yypt-2].node)
		}
	case 20:
		//line sql.y:248
		{
			// Change this to a rename statement
			yyVAL.node = NewSimpleParseNode(RENAME, "rename")
			yyVAL.node.PushTwo(yyS[yypt-3].node, yyS[yypt-0].node)
		}
	case 21:
		//line sql.y:256
		{
			yyVAL.node.PushTwo(yyS[yypt-2].node, yyS[yypt-0].node)
		}
	case 22:
		//line sql.y:262
		{
			yyVAL.node.Push(yyS[yypt-0].node)
		}
	case 23:
		//line sql.y:266
		{
			// Change this to an alter statement
			yyVAL.node = NewSimpleParseNode(ALTER, "alter")
			yyVAL.node.Push(yyS[yypt-0].node)
		}
	case 24:
		//line sql.y:273
		{
			SetAllowComments(yylex, true)
		}
	case 25:
		//line sql.y:277
		{
			yyVAL.node = yyS[yypt-0].node
			SetAllowComments(yylex, false)
		}
	case 26:
		//line sql.y:283
		{
			yyVAL.node = NewSimpleParseNode(COMMENT_LIST, "")
		}
	case 27:
		//line sql.y:287
		{
			yyVAL.node = yyS[yypt-1].node.Push(yyS[yypt-0].node)
		}
	case 28:
		yyVAL.node = yyS[yypt-0].node
	case 29:
		//line sql.y:294
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
		//line sql.y:302
		{
			yyVAL.node = NewSimpleParseNode(NO_DISTINCT, "")
		}
	case 34:
		//line sql.y:306
		{
			yyVAL.node = NewSimpleParseNode(DISTINCT, "distinct")
		}
	case 35:
		//line sql.y:312
		{
			yyVAL.node = NewSimpleParseNode(NODE_LIST, "node_list")
			yyVAL.node.Push(yyS[yypt-0].node)
		}
	case 36:
		//line sql.y:317
		{
			yyVAL.node.Push(yyS[yypt-0].node)
		}
	case 37:
		//line sql.y:323
		{
			yyVAL.node = NewSimpleParseNode(SELECT_STAR, "*")
		}
	case 38:
		yyVAL.node = yyS[yypt-0].node
	case 39:
		//line sql.y:328
		{
			yyVAL.node = yyS[yypt-1].node.PushTwo(yyS[yypt-2].node, yyS[yypt-0].node)
		}
	case 40:
		//line sql.y:332
		{
			yyVAL.node = yyS[yypt-1].node.PushTwo(yyS[yypt-2].node, NewSimpleParseNode(SELECT_STAR, "*"))
		}
	case 41:
		yyVAL.node = yyS[yypt-0].node
	case 42:
		yyVAL.node = yyS[yypt-0].node
	case 43:
		//line sql.y:341
		{
			yyVAL.node = NewSimpleParseNode(AS, "as")
		}
	case 44:
		yyVAL.node = yyS[yypt-0].node
	case 45:
		//line sql.y:348
		{
			yyVAL.node = NewSimpleParseNode(NODE_LIST, "node_list")
			yyVAL.node.Push(yyS[yypt-0].node)
		}
	case 46:
		//line sql.y:353
		{
			yyVAL.node = yyS[yypt-2].node.Push(yyS[yypt-1].node)
		}
	case 47:
		//line sql.y:357
		{
			yyVAL.node.Push(yyS[yypt-0].node)
		}
	case 48:
		//line sql.y:363
		{
			yyVAL.node = NewSimpleParseNode(TABLE_EXPR, "")
			yyVAL.node.Push(yyS[yypt-1].node)
			yyVAL.node.Push(NewSimpleParseNode(NODE_LIST, "node_list"))
			yyVAL.node.Push(yyS[yypt-0].node)
		}
	case 49:
		//line sql.y:370
		{
			yyVAL.node = NewSimpleParseNode(TABLE_EXPR, "")
			yyVAL.node.Push(yyS[yypt-3].node)
			yyVAL.node.Push(NewSimpleParseNode(NODE_LIST, "node_list").Push(yyS[yypt-1].node))
			yyVAL.node.Push(yyS[yypt-0].node)
		}
	case 50:
		//line sql.y:377
		{
			yyVAL.node = yyS[yypt-1].node.PushTwo(yyS[yypt-2].node, yyS[yypt-0].node)
		}
	case 51:
		//line sql.y:381
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
		//line sql.y:392
		{
			yyVAL.node = NewSimpleParseNode(LEFT, "left join")
		}
	case 55:
		//line sql.y:396
		{
			yyVAL.node = NewSimpleParseNode(LEFT, "left join")
		}
	case 56:
		//line sql.y:400
		{
			yyVAL.node = NewSimpleParseNode(RIGHT, "right join")
		}
	case 57:
		//line sql.y:404
		{
			yyVAL.node = NewSimpleParseNode(RIGHT, "right join")
		}
	case 58:
		//line sql.y:408
		{
			yyVAL.node = yyS[yypt-0].node
		}
	case 59:
		//line sql.y:412
		{
			yyVAL.node = NewSimpleParseNode(CROSS, "cross join")
		}
	case 60:
		//line sql.y:416
		{
			yyVAL.node = NewSimpleParseNode(NATURAL, "natural join")
		}
	case 61:
		yyVAL.node = yyS[yypt-0].node
	case 62:
		//line sql.y:423
		{
			yyVAL.node = yyS[yypt-1].node.PushTwo(yyS[yypt-2].node, yyS[yypt-0].node)
		}
	case 63:
		//line sql.y:427
		{
			yyVAL.node = yyS[yypt-2].node.Push(yyS[yypt-1].node)
		}
	case 64:
		yyVAL.node = yyS[yypt-0].node
	case 65:
		//line sql.y:434
		{
			yyVAL.node = yyS[yypt-1].node.PushTwo(yyS[yypt-2].node, yyS[yypt-0].node)
		}
	case 66:
		//line sql.y:439
		{
			yyVAL.node = NewSimpleParseNode(USE, "use")
		}
	case 67:
		//line sql.y:443
		{
			yyVAL.node.Push(yyS[yypt-1].node)
		}
	case 68:
		//line sql.y:447
		{
			yyVAL.node.Push(yyS[yypt-1].node)
		}
	case 69:
		//line sql.y:452
		{
			yyVAL.node = NewSimpleParseNode(WHERE, "where")
		}
	case 70:
		//line sql.y:456
		{
			yyVAL.node = yyS[yypt-1].node.Push(yyS[yypt-0].node)
		}
	case 71:
		yyVAL.node = yyS[yypt-0].node
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
			yyVAL.node = yyS[yypt-1].node.Push(yyS[yypt-0].node)
		}
	case 75:
		//line sql.y:475
		{
			yyVAL.node = yyS[yypt-2].node.Push(yyS[yypt-1].node)
		}
	case 76:
		//line sql.y:481
		{
			yyVAL.node = yyS[yypt-1].node.PushTwo(yyS[yypt-2].node, yyS[yypt-0].node)
		}
	case 77:
		//line sql.y:485
		{
			yyVAL.node = yyS[yypt-1].node.PushTwo(yyS[yypt-2].node, yyS[yypt-0].node)
		}
	case 78:
		//line sql.y:489
		{
			yyVAL.node = NewSimpleParseNode(NOT_IN, "not in").PushTwo(yyS[yypt-3].node, yyS[yypt-0].node)
		}
	case 79:
		//line sql.y:493
		{
			yyVAL.node = yyS[yypt-1].node.PushTwo(yyS[yypt-2].node, yyS[yypt-0].node)
		}
	case 80:
		//line sql.y:497
		{
			yyVAL.node = NewSimpleParseNode(NOT_LIKE, "not like").PushTwo(yyS[yypt-3].node, yyS[yypt-0].node)
		}
	case 81:
		//line sql.y:501
		{
			yyVAL.node = yyS[yypt-3].node
			yyVAL.node.Push(yyS[yypt-4].node)
			yyVAL.node.Push(yyS[yypt-2].node)
			yyVAL.node.Push(yyS[yypt-0].node)
		}
	case 82:
		//line sql.y:508
		{
			yyVAL.node = NewSimpleParseNode(NOT_BETWEEN, "not between")
			yyVAL.node.Push(yyS[yypt-5].node)
			yyVAL.node.Push(yyS[yypt-2].node)
			yyVAL.node.Push(yyS[yypt-0].node)
		}
	case 83:
		//line sql.y:515
		{
			yyVAL.node = NewSimpleParseNode(IS_NULL, "is null").Push(yyS[yypt-2].node)
		}
	case 84:
		//line sql.y:519
		{
			yyVAL.node = NewSimpleParseNode(IS_NOT_NULL, "is not null").Push(yyS[yypt-3].node)
		}
	case 85:
		//line sql.y:523
		{
			yyVAL.node = yyS[yypt-3].node.Push(yyS[yypt-1].node)
		}
	case 86:
		yyVAL.node = yyS[yypt-0].node
	case 87:
		yyVAL.node = yyS[yypt-0].node
	case 88:
		yyVAL.node = yyS[yypt-0].node
	case 89:
		yyVAL.node = yyS[yypt-0].node
	case 90:
		yyVAL.node = yyS[yypt-0].node
	case 91:
		yyVAL.node = yyS[yypt-0].node
	case 92:
		yyVAL.node = yyS[yypt-0].node
	case 93:
		//line sql.y:538
		{
			yyVAL.node = yyS[yypt-1].node.Push(yyS[yypt-0].node)
		}
	case 94:
		yyVAL.node = yyS[yypt-0].node
	case 95:
		//line sql.y:545
		{
			yyVAL.node = NewSimpleParseNode(NODE_LIST, "node_list")
			yyVAL.node.Push(yyS[yypt-0].node)
		}
	case 96:
		//line sql.y:550
		{
			yyVAL.node.Push(yyS[yypt-0].node)
		}
	case 97:
		//line sql.y:556
		{
			yyVAL.node = yyS[yypt-2].node.Push(yyS[yypt-1].node)
		}
	case 98:
		//line sql.y:560
		{
			yyVAL.node = yyS[yypt-2].node.Push(yyS[yypt-1].node)
		}
	case 99:
		//line sql.y:566
		{
			yyVAL.node = NewSimpleParseNode(NODE_LIST, "node_list")
			yyVAL.node.Push(yyS[yypt-0].node)
		}
	case 100:
		//line sql.y:571
		{
			yyVAL.node.Push(yyS[yypt-0].node)
		}
	case 101:
		yyVAL.node = yyS[yypt-0].node
	case 102:
		yyVAL.node = yyS[yypt-0].node
	case 103:
		//line sql.y:579
		{
			yyVAL.node = yyS[yypt-2].node.Push(yyS[yypt-1].node)
		}
	case 104:
		//line sql.y:583
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
	case 105:
		//line sql.y:595
		{
			yyVAL.node = yyS[yypt-1].node.PushTwo(yyS[yypt-2].node, yyS[yypt-0].node)
		}
	case 106:
		//line sql.y:599
		{
			yyVAL.node = yyS[yypt-1].node.PushTwo(yyS[yypt-2].node, yyS[yypt-0].node)
		}
	case 107:
		//line sql.y:603
		{
			yyVAL.node = yyS[yypt-1].node.PushTwo(yyS[yypt-2].node, yyS[yypt-0].node)
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
	case 114:
		//line sql.y:643
		{
			yyS[yypt-2].node.Type = FUNCTION
			yyVAL.node = yyS[yypt-2].node.Push(NewSimpleParseNode(NODE_LIST, "node_list"))
		}
	case 115:
		//line sql.y:648
		{
			yyS[yypt-3].node.Type = FUNCTION
			yyVAL.node = yyS[yypt-3].node.Push(yyS[yypt-1].node)
		}
	case 116:
		//line sql.y:653
		{
			yyS[yypt-4].node.Type = FUNCTION
			yyVAL.node = yyS[yypt-4].node.Push(yyS[yypt-2].node)
			yyVAL.node = yyS[yypt-4].node.Push(yyS[yypt-1].node)
		}
	case 117:
		//line sql.y:659
		{
			yyS[yypt-3].node.Type = FUNCTION
			yyVAL.node = yyS[yypt-3].node.Push(yyS[yypt-1].node)
		}
	case 118:
		yyVAL.node = yyS[yypt-0].node
	case 119:
		yyVAL.node = yyS[yypt-0].node
	case 120:
		yyVAL.node = yyS[yypt-0].node
	case 121:
		//line sql.y:671
		{
			yyVAL.node = NewSimpleParseNode(UPLUS, "+")
		}
	case 122:
		//line sql.y:675
		{
			yyVAL.node = NewSimpleParseNode(UMINUS, "-")
		}
	case 123:
		yyVAL.node = yyS[yypt-0].node
	case 124:
		//line sql.y:682
		{
			yyVAL.node = NewSimpleParseNode(CASE_WHEN, "case")
			yyVAL.node.Push(yyS[yypt-1].node)
		}
	case 125:
		//line sql.y:687
		{
			yyVAL.node.PushTwo(yyS[yypt-2].node, yyS[yypt-1].node)
		}
	case 126:
		//line sql.y:693
		{
			yyVAL.node = NewSimpleParseNode(WHEN_LIST, "when_list")
			yyVAL.node.Push(yyS[yypt-0].node)
		}
	case 127:
		//line sql.y:698
		{
			yyVAL.node.Push(yyS[yypt-0].node)
		}
	case 128:
		//line sql.y:704
		{
			yyVAL.node.PushTwo(yyS[yypt-2].node, yyS[yypt-0].node)
		}
	case 129:
		//line sql.y:708
		{
			yyVAL.node.Push(yyS[yypt-0].node)
		}
	case 130:
		yyVAL.node = yyS[yypt-0].node
	case 131:
		//line sql.y:715
		{
			yyVAL.node = yyS[yypt-1].node.PushTwo(yyS[yypt-2].node, yyS[yypt-0].node)
		}
	case 132:
		yyVAL.node = yyS[yypt-0].node
	case 133:
		yyVAL.node = yyS[yypt-0].node
	case 134:
		yyVAL.node = yyS[yypt-0].node
	case 135:
		yyVAL.node = yyS[yypt-0].node
	case 136:
		//line sql.y:726
		{
			yyVAL.node = NewSimpleParseNode(GROUP, "group")
		}
	case 137:
		//line sql.y:730
		{
			yyVAL.node = yyS[yypt-2].node.Push(yyS[yypt-0].node)
		}
	case 138:
		//line sql.y:735
		{
			yyVAL.node = NewSimpleParseNode(HAVING, "having")
		}
	case 139:
		//line sql.y:739
		{
			yyVAL.node = yyS[yypt-1].node.Push(yyS[yypt-0].node)
		}
	case 140:
		//line sql.y:744
		{
			yyVAL.node = NewSimpleParseNode(ORDER, "order")
		}
	case 141:
		//line sql.y:748
		{
			yyVAL.node = yyS[yypt-2].node.Push(yyS[yypt-0].node)
		}
	case 142:
		//line sql.y:754
		{
			yyVAL.node = NewSimpleParseNode(NODE_LIST, "node_list")
			yyVAL.node.Push(yyS[yypt-0].node)
		}
	case 143:
		//line sql.y:759
		{
			yyVAL.node = yyS[yypt-2].node.Push(yyS[yypt-0].node)
		}
	case 144:
		//line sql.y:765
		{
			yyVAL.node = yyS[yypt-0].node.Push(yyS[yypt-1].node)
		}
	case 145:
		//line sql.y:770
		{
			yyVAL.node = NewSimpleParseNode(ASC, "asc")
		}
	case 146:
		yyVAL.node = yyS[yypt-0].node
	case 147:
		yyVAL.node = yyS[yypt-0].node
	case 148:
		//line sql.y:777
		{
			yyVAL.node = NewSimpleParseNode(LIMIT, "limit")
		}
	case 149:
		//line sql.y:781
		{
			yyVAL.node = yyS[yypt-1].node.Push(yyS[yypt-0].node)
		}
	case 150:
		//line sql.y:785
		{
			yyVAL.node = yyS[yypt-3].node.PushTwo(yyS[yypt-2].node, yyS[yypt-0].node)
		}
	case 151:
		//line sql.y:790
		{
			yyVAL.node = NewSimpleParseNode(NO_LOCK, "")
		}
	case 152:
		//line sql.y:794
		{
			yyVAL.node = NewSimpleParseNode(FOR_UPDATE, " for update")
		}
	case 153:
		//line sql.y:798
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
	case 154:
		//line sql.y:811
		{
			yyVAL.node = NewSimpleParseNode(COLUMN_LIST, "")
		}
	case 155:
		//line sql.y:815
		{
			yyVAL.node = yyS[yypt-1].node
		}
	case 156:
		//line sql.y:821
		{
			yyVAL.node = NewSimpleParseNode(COLUMN_LIST, "")
			yyVAL.node.Push(yyS[yypt-0].node)
		}
	case 157:
		//line sql.y:826
		{
			yyVAL.node = yyS[yypt-2].node.Push(yyS[yypt-0].node)
		}
	case 158:
		//line sql.y:832
		{
			yyVAL.node = NewSimpleParseNode(INDEX_LIST, "")
			yyVAL.node.Push(yyS[yypt-0].node)
		}
	case 159:
		//line sql.y:837
		{
			yyVAL.node = yyS[yypt-2].node.Push(yyS[yypt-0].node)
		}
	case 160:
		//line sql.y:842
		{
			yyVAL.node = NewSimpleParseNode(DUPLICATE, "duplicate")
		}
	case 161:
		//line sql.y:846
		{
			yyVAL.node = yyS[yypt-3].node.Push(yyS[yypt-0].node)
		}
	case 162:
		//line sql.y:852
		{
			yyVAL.node = NewSimpleParseNode(NODE_LIST, "node_list")
			yyVAL.node.Push(yyS[yypt-0].node)
		}
	case 163:
		//line sql.y:857
		{
			yyVAL.node = yyS[yypt-2].node.Push(yyS[yypt-0].node)
		}
	case 164:
		//line sql.y:863
		{
			yyVAL.node = yyS[yypt-1].node.PushTwo(yyS[yypt-2].node, yyS[yypt-0].node)
		}
	case 165:
		//line sql.y:868
		{
			yyVAL.node = nil
		}
	case 166:
		yyVAL.node = yyS[yypt-0].node
	case 167:
		//line sql.y:872
		{
			yyVAL.node = nil
		}
	case 168:
		yyVAL.node = yyS[yypt-0].node
	case 169:
		//line sql.y:876
		{
			yyVAL.node = nil
		}
	case 170:
		yyVAL.node = yyS[yypt-0].node
	case 171:
		yyVAL.node = yyS[yypt-0].node
	case 172:
		yyVAL.node = yyS[yypt-0].node
	case 173:
		yyVAL.node = yyS[yypt-0].node
	case 174:
		yyVAL.node = yyS[yypt-0].node
	case 175:
		yyVAL.node = yyS[yypt-0].node
	case 176:
		//line sql.y:887
		{
			yyVAL.node = nil
		}
	case 177:
		yyVAL.node = yyS[yypt-0].node
	case 178:
		//line sql.y:891
		{
			yyVAL.node = nil
		}
	case 179:
		yyVAL.node = yyS[yypt-0].node
	case 180:
		//line sql.y:895
		{
			yyVAL.node = nil
		}
	case 181:
		yyVAL.node = yyS[yypt-0].node
	case 182:
		//line sql.y:900
		{
			yyVAL.node.LowerCase()
		}
	case 183:
		//line sql.y:905
		{
			ForceEOF(yylex)
		}
	}
	goto yystack /* stack new state and value */
}
