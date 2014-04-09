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
	-1, 66,
	35, 43,
	-2, 38,
	-1, 175,
	35, 43,
	-2, 66,
}

const yyNprod = 184
const yyPrivate = 57344

var yyTokenNames []string
var yyStates []string

const yyLast = 562

var yyAct = []int{

	76, 69, 49, 302, 326, 221, 74, 138, 268, 242,
	159, 63, 173, 145, 183, 147, 334, 68, 108, 334,
	66, 137, 3, 107, 154, 111, 112, 232, 233, 234,
	235, 236, 52, 237, 238, 202, 64, 50, 51, 266,
	61, 107, 107, 40, 202, 162, 22, 23, 24, 25,
	22, 23, 24, 25, 57, 60, 55, 100, 12, 13,
	14, 15, 36, 228, 102, 22, 23, 24, 25, 22,
	23, 24, 25, 135, 139, 200, 260, 140, 335, 32,
	37, 333, 38, 39, 34, 294, 288, 16, 284, 134,
	136, 287, 56, 52, 246, 152, 52, 290, 158, 51,
	110, 265, 51, 257, 255, 148, 203, 149, 259, 178,
	146, 158, 291, 135, 135, 182, 285, 157, 188, 189,
	97, 192, 193, 194, 195, 196, 197, 198, 199, 180,
	181, 258, 148, 156, 149, 201, 93, 17, 18, 20,
	19, 99, 168, 204, 177, 148, 62, 149, 210, 124,
	125, 126, 52, 190, 206, 208, 308, 135, 219, 262,
	212, 211, 166, 225, 109, 169, 209, 111, 112, 213,
	214, 224, 217, 223, 226, 244, 245, 220, 12, 122,
	123, 124, 125, 126, 179, 105, 204, 239, 250, 251,
	310, 309, 247, 279, 243, 191, 240, 229, 280, 83,
	249, 283, 88, 282, 254, 281, 248, 244, 245, 53,
	80, 81, 82, 155, 165, 167, 164, 155, 141, 256,
	277, 106, 86, 95, 202, 278, 267, 96, 211, 318,
	207, 264, 79, 22, 23, 24, 25, 83, 297, 184,
	88, 313, 46, 275, 276, 84, 85, 67, 80, 81,
	82, 312, 89, 176, 293, 12, 72, 230, 151, 176,
	86, 95, 241, 240, 144, 87, 107, 52, 174, 143,
	142, 300, 303, 298, 62, 338, 299, 53, 289, 71,
	286, 295, 304, 84, 85, 65, 176, 272, 91, 271,
	89, 94, 171, 339, 314, 241, 311, 170, 232, 233,
	234, 235, 236, 87, 237, 238, 153, 103, 135, 204,
	135, 322, 316, 327, 327, 324, 101, 98, 328, 330,
	303, 47, 331, 58, 323, 92, 325, 205, 315, 296,
	52, 332, 336, 79, 12, 340, 51, 45, 83, 253,
	342, 88, 160, 343, 344, 104, 109, 43, 53, 80,
	81, 82, 41, 185, 79, 186, 187, 72, 216, 83,
	26, 86, 88, 90, 269, 307, 270, 222, 306, 67,
	80, 81, 82, 274, 28, 29, 30, 31, 72, 155,
	71, 48, 86, 341, 84, 85, 329, 12, 12, 27,
	161, 89, 148, 33, 149, 227, 163, 35, 54, 59,
	218, 71, 150, 79, 87, 84, 85, 65, 83, 261,
	337, 88, 89, 319, 301, 305, 273, 73, 53, 80,
	81, 82, 78, 75, 79, 87, 77, 72, 263, 83,
	215, 86, 88, 113, 83, 70, 175, 88, 231, 53,
	80, 81, 82, 172, 53, 80, 81, 82, 72, 42,
	71, 21, 86, 141, 84, 85, 44, 86, 11, 320,
	321, 89, 10, 9, 8, 114, 118, 116, 117, 7,
	6, 71, 5, 4, 87, 84, 85, 2, 1, 0,
	84, 85, 89, 130, 131, 132, 133, 89, 0, 127,
	128, 129, 0, 0, 0, 87, 0, 0, 0, 0,
	87, 119, 120, 121, 122, 123, 124, 125, 126, 317,
	0, 115, 119, 120, 121, 122, 123, 124, 125, 126,
	0, 0, 0, 0, 119, 120, 121, 122, 123, 124,
	125, 126, 292, 0, 0, 119, 120, 121, 122, 123,
	124, 125, 126, 252, 0, 0, 119, 120, 121, 122,
	123, 124, 125, 126, 119, 120, 121, 122, 123, 124,
	125, 126,
}
var yyPact = []int{

	54, -1000, -1000, 184, -1000, -1000, -1000, -1000, -1000, -1000,
	-1000, -1000, -1000, -1000, -1000, -1000, -1000, -8, -28, -7,
	-5, 384, 335, -1000, -1000, -1000, 329, -1000, 308, 286,
	373, 242, -35, 4, -1000, -33, -1000, 288, -36, 239,
	-1000, -1000, 334, -1000, 348, 286, 292, 60, 286, 170,
	-1000, 182, -1000, 44, 282, 74, 239, 281, -25, 272,
	325, 121, -1000, 213, -1000, -1000, 327, 24, 102, 444,
	-1000, 404, 383, -1000, -1000, 409, 226, 225, -1000, 220,
	-1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, 313,
	-1000, 214, 242, 271, 370, 242, 404, 239, -1000, 322,
	-48, 130, 262, -1000, -1000, 257, 224, 334, 239, -1000,
	111, 404, 404, 409, 195, 332, 409, 409, 128, 409,
	409, 409, 409, 409, 409, 409, 409, -1000, -1000, -1000,
	-1000, -1000, -1000, -1000, -1000, 444, -40, 20, -9, 444,
	-1000, 174, 212, 334, 384, 66, 53, -1000, 404, 404,
	330, 242, 208, -1000, 355, 404, -1000, -1000, -1000, -1000,
	-1000, 107, 239, -1000, -26, -1000, -1000, -1000, -1000, -1000,
	-1000, -1000, 204, 244, 251, 145, 18, -1000, -1000, -1000,
	-1000, -1000, 486, -1000, 174, 195, 409, 409, 486, 478,
	-1000, 314, 108, 108, 108, 76, 76, -1000, -1000, -1000,
	-1000, -1000, 409, -1000, 486, -1000, -11, 334, -12, 16,
	-1000, -1000, 26, -4, -1000, 95, 195, 184, -14, -1000,
	355, 350, 353, 102, 254, -1000, -1000, 252, -1000, 363,
	218, 218, -1000, -1000, 166, 139, 151, 149, 147, -27,
	1, 384, -1000, 245, 3, -2, 243, -18, -3, -1000,
	486, 467, 409, -1000, 486, -1000, -30, -1000, -1000, -1000,
	404, -1000, 299, 185, -1000, -1000, 242, 350, -1000, 409,
	409, -1000, -1000, 357, 352, 244, 92, -1000, 137, -1000,
	136, -1000, -1000, -1000, -1000, -1000, 113, 207, 197, -1000,
	-1000, -1000, 409, 486, -1000, -1000, 297, 195, -1000, -1000,
	456, 176, -1000, 433, -1000, 355, 404, 409, 404, -1000,
	-1000, -1000, 239, 239, 486, 380, -1000, 409, 409, -1000,
	-1000, -1000, 350, 102, 171, 102, -34, -1000, -37, 242,
	486, -1000, 259, -1000, 239, -1000, 170, -1000, 377, 319,
	-1000, -1000, 239, 239, -1000,
}
var yyPgo = []int{

	0, 478, 477, 21, 473, 472, 470, 469, 464, 463,
	462, 458, 360, 456, 451, 449, 11, 36, 20, 18,
	443, 12, 438, 436, 242, 9, 24, 17, 435, 433,
	430, 428, 14, 7, 1, 426, 423, 422, 13, 15,
	6, 417, 416, 415, 5, 414, 3, 413, 8, 410,
	409, 402, 400, 4, 2, 37, 399, 398, 397, 396,
	395, 393, 390, 0, 10, 389,
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
	-55, -40, -63, 35, -57, 91, 88, 87, 35, -56,
	91, -63, 35, -16, -17, 73, -18, 35, -27, -34,
	-28, 67, 44, -41, -40, -36, -63, -35, -37, 20,
	36, 37, 38, 25, 71, 72, 48, 91, 28, 78,
	15, -24, 33, 76, -24, 53, 45, 76, 35, 67,
	-63, 35, 89, 35, 20, 64, 8, 53, -19, 19,
	76, 65, 66, -29, 21, 67, 23, 24, 22, 68,
	69, 70, 71, 72, 73, 74, 75, 45, 46, 47,
	39, 40, 41, 42, -27, -34, -27, -3, -33, -34,
	-34, 44, 44, 44, 44, -38, -18, -39, 79, 81,
	-51, 44, -54, 35, -26, 9, -55, -18, -63, -64,
	20, -62, 93, -59, 86, 84, 32, 85, 12, 35,
	35, 35, -20, -21, 44, -23, 35, -17, -63, 73,
	-27, -27, -34, -32, 44, 21, 23, 24, -34, -34,
	25, 67, -34, -34, -34, -34, -34, -34, -34, -34,
	115, 115, 53, 115, -34, 115, -16, 18, -16, -3,
	82, -39, -38, -18, -18, -30, 28, -3, -52, -40,
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
	162, 0, 130, 182, 0, 0, 0, 0, 0, 0,
	0, 0, 182, 0, 35, 37, -2, 182, 41, 42,
	71, 0, 0, 101, 102, 0, 130, 0, 118, 0,
	132, 133, 134, 135, 121, 122, 123, 119, 120, 0,
	27, 154, 0, 0, 69, 0, 0, 0, 183, 0,
	180, 0, 0, 22, 166, 0, 0, 0, 0, 44,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 86, 87, 88,
	89, 90, 91, 92, 74, 0, 0, 0, 0, 99,
	113, 0, 0, 0, 0, 0, 0, 126, 0, 0,
	0, 0, 69, 65, 140, 0, 163, 164, 131, 17,
	168, 0, 0, 183, 176, 171, 172, 173, 174, 175,
	21, 23, 69, 45, 0, -2, 61, 36, 39, 40,
	72, 73, 76, 77, 0, 0, 0, 0, 79, 0,
	83, 0, 105, 106, 107, 108, 109, 110, 111, 112,
	75, 103, 0, 104, 99, 114, 0, 0, 0, 0,
	124, 127, 0, 0, 129, 160, 0, 94, 0, 156,
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
