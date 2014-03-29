//line sql.y:33
package sqlparser

import __yyfmt__ "fmt"

//line sql.y:33
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

//line sql.y:91
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
const SHARE = 57377
const MODE = 57378
const ID = 57379
const STRING = 57380
const NUMBER = 57381
const VALUE_ARG = 57382
const LE = 57383
const GE = 57384
const NE = 57385
const NULL_SAFE_EQUAL = 57386
const LEX_ERROR = 57387
const UNION = 57388
const MINUS = 57389
const EXCEPT = 57390
const INTERSECT = 57391
const JOIN = 57392
const STRAIGHT_JOIN = 57393
const LEFT = 57394
const RIGHT = 57395
const INNER = 57396
const OUTER = 57397
const CROSS = 57398
const NATURAL = 57399
const USE = 57400
const FORCE = 57401
const ON = 57402
const AND = 57403
const OR = 57404
const NOT = 57405
const UNARY = 57406
const CASE = 57407
const WHEN = 57408
const THEN = 57409
const ELSE = 57410
const END = 57411
const CREATE = 57412
const ALTER = 57413
const DROP = 57414
const RENAME = 57415
const TABLE = 57416
const INDEX = 57417
const TO = 57418
const IGNORE = 57419
const IF = 57420
const UNIQUE = 57421
const USING = 57422
const NODE_LIST = 57423
const UPLUS = 57424
const UMINUS = 57425
const CASE_WHEN = 57426
const WHEN_LIST = 57427
const SELECT_STAR = 57428
const NO_DISTINCT = 57429
const FUNCTION = 57430
const NO_LOCK = 57431
const FOR_UPDATE = 57432
const LOCK_IN_SHARE_MODE = 57433
const NOT_IN = 57434
const NOT_LIKE = 57435
const NOT_BETWEEN = 57436
const IS_NULL = 57437
const IS_NOT_NULL = 57438
const UNION_ALL = 57439
const COMMENT_LIST = 57440
const COLUMN_LIST = 57441
const INDEX_LIST = 57442
const TABLE_EXPR = 57443

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
	"SHARE",
	"MODE",
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
	37, 43,
	-2, 38,
	-1, 172,
	37, 43,
	-2, 66,
}

const yyNprod = 184
const yyPrivate = 57344

var yyTokenNames []string
var yyStates []string

const yyLast = 583

var yyAct = []int{

	74, 67, 49, 302, 326, 268, 221, 136, 181, 242,
	170, 156, 72, 145, 143, 105, 159, 66, 334, 64,
	334, 135, 3, 104, 152, 62, 32, 61, 108, 109,
	201, 34, 51, 266, 59, 232, 233, 234, 235, 236,
	60, 237, 238, 40, 54, 50, 36, 22, 23, 24,
	25, 22, 23, 24, 25, 228, 97, 99, 38, 39,
	288, 287, 22, 23, 24, 25, 22, 23, 24, 25,
	55, 132, 137, 104, 104, 138, 201, 165, 199, 56,
	335, 37, 333, 260, 146, 294, 147, 131, 134, 308,
	246, 51, 290, 150, 51, 265, 284, 163, 320, 321,
	198, 146, 166, 147, 259, 107, 175, 144, 177, 91,
	132, 132, 180, 291, 155, 186, 187, 285, 190, 191,
	192, 193, 194, 195, 196, 197, 178, 179, 258, 188,
	174, 146, 200, 147, 209, 257, 255, 96, 202, 154,
	262, 203, 116, 117, 118, 119, 120, 121, 122, 123,
	219, 162, 164, 161, 224, 132, 102, 210, 310, 211,
	225, 309, 218, 52, 208, 283, 212, 213, 205, 207,
	216, 223, 226, 189, 282, 220, 279, 12, 13, 14,
	15, 280, 239, 281, 203, 94, 250, 251, 243, 93,
	247, 201, 249, 240, 229, 318, 317, 108, 109, 177,
	206, 176, 77, 254, 248, 297, 16, 81, 244, 245,
	86, 116, 117, 118, 119, 120, 121, 122, 123, 65,
	78, 79, 80, 106, 264, 210, 182, 267, 70, 121,
	122, 123, 84, 52, 256, 119, 120, 121, 122, 123,
	313, 275, 276, 232, 233, 234, 235, 236, 277, 237,
	238, 69, 153, 278, 293, 82, 83, 63, 17, 18,
	20, 19, 87, 240, 312, 153, 149, 219, 244, 245,
	103, 300, 303, 299, 12, 85, 142, 141, 140, 298,
	295, 292, 133, 304, 116, 117, 118, 119, 120, 121,
	122, 123, 46, 344, 314, 173, 311, 289, 230, 204,
	22, 23, 24, 25, 241, 286, 316, 173, 132, 203,
	132, 93, 322, 327, 327, 324, 241, 104, 328, 330,
	303, 272, 331, 271, 323, 168, 325, 173, 332, 77,
	51, 167, 336, 151, 81, 340, 171, 86, 89, 100,
	98, 92, 95, 47, 57, 77, 133, 78, 79, 80,
	81, 343, 338, 86, 90, 70, 45, 315, 296, 84,
	12, 342, 65, 78, 79, 80, 183, 253, 184, 185,
	339, 70, 157, 101, 106, 84, 43, 41, 69, 88,
	12, 269, 82, 83, 215, 307, 270, 222, 306, 87,
	146, 274, 147, 26, 69, 153, 77, 48, 82, 83,
	63, 81, 85, 341, 86, 87, 329, 28, 29, 30,
	31, 12, 77, 133, 78, 79, 80, 81, 85, 27,
	86, 158, 70, 33, 227, 160, 84, 35, 53, 133,
	78, 79, 80, 58, 217, 148, 261, 337, 70, 319,
	301, 305, 84, 273, 71, 69, 12, 76, 73, 82,
	83, 75, 263, 214, 110, 68, 87, 172, 231, 169,
	42, 69, 21, 44, 11, 82, 83, 81, 10, 85,
	86, 9, 87, 8, 7, 6, 5, 4, 2, 133,
	78, 79, 80, 81, 1, 85, 86, 0, 139, 0,
	0, 0, 84, 0, 0, 133, 78, 79, 80, 0,
	0, 0, 0, 0, 139, 0, 0, 0, 84, 0,
	0, 0, 0, 0, 0, 82, 83, 0, 0, 0,
	0, 0, 87, 0, 0, 0, 111, 115, 113, 114,
	0, 82, 83, 0, 0, 85, 0, 0, 87, 0,
	0, 0, 0, 0, 0, 0, 127, 128, 129, 130,
	0, 85, 124, 125, 126, 252, 0, 0, 116, 117,
	118, 119, 120, 121, 122, 123, 116, 117, 118, 119,
	120, 121, 122, 123, 112, 116, 117, 118, 119, 120,
	121, 122, 123,
}
var yyPact = []int{

	173, -1000, -1000, 249, -1000, -1000, -1000, -1000, -1000, -1000,
	-1000, -1000, -1000, -1000, -1000, -1000, -1000, -63, -46, -8,
	-31, 407, 360, -1000, -1000, -1000, 358, -1000, 327, 306,
	389, 196, -49, -20, -1000, -10, -1000, 307, -59, 196,
	-1000, -1000, 325, -1000, 364, 306, 321, 31, 306, 134,
	-1000, 138, -1000, 305, 68, 196, 303, -34, 302, 353,
	90, 262, -1000, -1000, 355, 27, 130, 505, -1000, 392,
	376, -1000, -1000, 458, 232, 231, -1000, 230, -1000, -1000,
	-1000, -1000, -1000, -1000, -1000, -1000, -1000, 309, -1000, 220,
	196, 296, 386, 196, 392, -1000, 352, -79, 65, 294,
	-1000, -1000, 288, 290, 325, 196, -1000, 126, 392, 392,
	458, 180, 345, 458, 458, 104, 458, 458, 458, 458,
	458, 458, 458, 458, -1000, -1000, -1000, -1000, -1000, -1000,
	-1000, -1000, 505, 22, -39, 15, 21, 505, -1000, 442,
	182, 325, 407, 50, 3, -1000, 392, 392, 356, 245,
	256, -1000, 375, 392, -1000, -1000, -1000, -1000, 88, 196,
	-1000, -36, -1000, -1000, -1000, -1000, -1000, -1000, -1000, 243,
	187, 270, 204, 12, -1000, -1000, -1000, -1000, -1000, -1000,
	496, -1000, 442, 180, 458, 458, 496, 488, -1000, 342,
	162, 162, 162, 154, 154, -1000, -1000, -1000, 196, -1000,
	-1000, 458, -1000, 496, -1000, 19, 325, 18, 11, -1000,
	-1000, 20, 1, -1000, 74, 180, 249, -22, -1000, -1000,
	375, 367, 373, 130, 286, -1000, -1000, 284, -1000, 381,
	258, 258, -1000, -1000, 192, 120, 127, 118, 109, -21,
	0, 407, -1000, 268, -29, -30, 260, -25, -4, -1000,
	496, 214, 458, -1000, 496, -1000, -32, -1000, -1000, -1000,
	392, -1000, 328, 150, -1000, -1000, 245, 367, -1000, 458,
	458, -1000, -1000, 377, 372, 187, 23, -1000, 105, -1000,
	102, -1000, -1000, -1000, -1000, -1000, 144, 218, 194, -1000,
	-1000, -1000, 458, 496, -1000, -1000, 326, 180, -1000, -1000,
	141, 140, -1000, 72, -1000, 375, 392, 458, 392, -1000,
	-1000, -1000, 196, 196, 496, 400, -1000, 458, 458, -1000,
	-1000, -1000, 367, 130, 136, 130, -35, -1000, -37, 196,
	496, -1000, 336, -1000, 196, -1000, 134, -1000, 397, 340,
	-1000, -1000, 316, 257, -1000,
}
var yyPgo = []int{

	0, 484, 478, 21, 477, 476, 475, 474, 473, 471,
	468, 464, 393, 463, 462, 460, 27, 25, 19, 15,
	459, 10, 458, 457, 292, 9, 24, 17, 455, 454,
	453, 452, 8, 7, 1, 451, 448, 447, 14, 13,
	12, 444, 443, 441, 6, 440, 3, 439, 5, 437,
	436, 435, 434, 4, 2, 45, 433, 428, 427, 425,
	424, 423, 421, 0, 11, 419,
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
	-10, -11, 4, 5, 6, 7, 33, 85, 86, 88,
	87, -14, 51, 52, 53, 54, -12, -65, -12, -12,
	-12, -12, 89, -61, 94, -58, 92, 89, 89, 90,
	-3, 17, -15, 18, -13, 29, -24, 37, 8, -54,
	-55, -63, 37, -57, 93, 90, 89, 37, -56, 93,
	-63, -16, -17, 75, -18, 37, -27, -34, -28, 69,
	46, -41, -40, -36, -63, -35, -37, 20, 38, 39,
	40, 25, 73, 74, 50, 93, 28, 80, 15, -24,
	33, 78, -24, 55, 47, 37, 69, -63, 37, 91,
	37, 20, 66, 8, 55, -19, 19, 78, 67, 68,
	-29, 21, 69, 23, 24, 22, 70, 71, 72, 73,
	74, 75, 76, 77, 47, 48, 49, 41, 42, 43,
	44, -27, -34, 37, -27, -3, -33, -34, -34, 46,
	46, 46, 46, -38, -18, -39, 81, 83, -51, 46,
	-54, 37, -26, 9, -55, -18, -64, 20, -62, 95,
	-59, 88, 86, 32, 87, 12, 37, 37, 37, -20,
	-21, 46, -23, 37, -17, -63, 75, -63, -27, -27,
	-34, -32, 46, 21, 23, 24, -34, -34, 25, 69,
	-34, -34, -34, -34, -34, -34, -34, -34, 78, 117,
	117, 55, 117, -34, 117, -16, 18, -16, -3, 84,
	-39, -38, -18, -18, -30, 28, -3, -52, -40, -63,
	-26, -44, 12, -27, 66, -63, -64, -60, 91, -26,
	55, -22, 56, 57, 58, 59, 60, 62, 63, -21,
	-3, 46, -25, -19, 64, 65, 78, -33, -3, -32,
	-34, -34, 67, 25, -34, 117, -16, 117, 117, 84,
	82, -50, 66, -31, -32, 117, 55, -44, -48, 14,
	13, 37, 37, -42, 10, -21, -21, 56, 61, 56,
	61, 56, 56, 56, 117, 117, 37, 90, 90, 37,
	117, 117, 67, -34, 117, -18, 30, 55, -40, -48,
	-34, -45, -46, -34, -64, -43, 11, 13, 66, 56,
	56, -25, 46, 46, -34, 31, -32, 55, 55, -47,
	26, 27, -44, -27, -33, -27, -53, -63, -53, 6,
	-34, -46, -48, 117, 55, 117, -54, -49, 16, 34,
	-63, 6, 21, 35, 36,
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
	3, 3, 3, 3, 3, 3, 3, 77, 70, 3,
	46, 117, 75, 73, 55, 74, 78, 76, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	48, 47, 49, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 72, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 71, 3, 50,
}
var yyTok2 = []int{

	2, 3, 4, 5, 6, 7, 8, 9, 10, 11,
	12, 13, 14, 15, 16, 17, 18, 19, 20, 21,
	22, 23, 24, 25, 26, 27, 28, 29, 30, 31,
	32, 33, 34, 35, 36, 37, 38, 39, 40, 41,
	42, 43, 44, 45, 51, 52, 53, 54, 56, 57,
	58, 59, 60, 61, 62, 63, 64, 65, 66, 67,
	68, 69, 79, 80, 81, 82, 83, 84, 85, 86,
	87, 88, 89, 90, 91, 92, 93, 94, 95, 96,
	97, 98, 99, 100, 101, 102, 103, 104, 105, 106,
	107, 108, 109, 110, 111, 112, 113, 114, 115, 116,
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
		//line sql.y:163
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
		//line sql.y:177
		{
			yyVAL.node = yyS[yypt-1].node.PushTwo(yyS[yypt-2].node, yyS[yypt-0].node)
		}
	case 13:
		//line sql.y:183
		{
			yyVAL.node = yyS[yypt-6].node
			yyVAL.node.Push(yyS[yypt-5].node) // 0: comment_opt
			yyVAL.node.Push(yyS[yypt-3].node) // 1: dml_table_expression
			yyVAL.node.Push(yyS[yypt-2].node) // 2: column_list_opt
			yyVAL.node.Push(yyS[yypt-1].node) // 3: values
			yyVAL.node.Push(yyS[yypt-0].node) // 4: on_dup_opt
		}
	case 14:
		//line sql.y:194
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
		//line sql.y:206
		{
			yyVAL.node = yyS[yypt-6].node
			yyVAL.node.Push(yyS[yypt-5].node) // 0: comment_opt
			yyVAL.node.Push(yyS[yypt-3].node) // 1: dml_table_expression
			yyVAL.node.Push(yyS[yypt-2].node) // 2: where_expression_opt
			yyVAL.node.Push(yyS[yypt-1].node) // 3: order_by_opt
			yyVAL.node.Push(yyS[yypt-0].node) // 4: limit_opt
		}
	case 16:
		//line sql.y:217
		{
			yyVAL.node = yyS[yypt-2].node
			yyVAL.node.Push(yyS[yypt-1].node)
			yyVAL.node.Push(yyS[yypt-0].node)
		}
	case 17:
		//line sql.y:225
		{
			yyVAL.node.Push(yyS[yypt-1].node)
		}
	case 18:
		//line sql.y:229
		{
			// Change this to an alter statement
			yyVAL.node = NewSimpleParseNode(ALTER, "alter")
			yyVAL.node.Push(yyS[yypt-1].node)
		}
	case 19:
		//line sql.y:237
		{
			yyVAL.node.Push(yyS[yypt-2].node)
		}
	case 20:
		//line sql.y:241
		{
			// Change this to a rename statement
			yyVAL.node = NewSimpleParseNode(RENAME, "rename")
			yyVAL.node.PushTwo(yyS[yypt-3].node, yyS[yypt-0].node)
		}
	case 21:
		//line sql.y:249
		{
			yyVAL.node.PushTwo(yyS[yypt-2].node, yyS[yypt-0].node)
		}
	case 22:
		//line sql.y:255
		{
			yyVAL.node.Push(yyS[yypt-0].node)
		}
	case 23:
		//line sql.y:259
		{
			// Change this to an alter statement
			yyVAL.node = NewSimpleParseNode(ALTER, "alter")
			yyVAL.node.Push(yyS[yypt-0].node)
		}
	case 24:
		//line sql.y:266
		{
			SetAllowComments(yylex, true)
		}
	case 25:
		//line sql.y:270
		{
			yyVAL.node = yyS[yypt-0].node
			SetAllowComments(yylex, false)
		}
	case 26:
		//line sql.y:276
		{
			yyVAL.node = NewSimpleParseNode(COMMENT_LIST, "")
		}
	case 27:
		//line sql.y:280
		{
			yyVAL.node = yyS[yypt-1].node.Push(yyS[yypt-0].node)
		}
	case 28:
		yyVAL.node = yyS[yypt-0].node
	case 29:
		//line sql.y:287
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
		//line sql.y:295
		{
			yyVAL.node = NewSimpleParseNode(NO_DISTINCT, "")
		}
	case 34:
		//line sql.y:299
		{
			yyVAL.node = NewSimpleParseNode(DISTINCT, "distinct")
		}
	case 35:
		//line sql.y:305
		{
			yyVAL.node = NewSimpleParseNode(NODE_LIST, "node_list")
			yyVAL.node.Push(yyS[yypt-0].node)
		}
	case 36:
		//line sql.y:310
		{
			yyVAL.node.Push(yyS[yypt-0].node)
		}
	case 37:
		//line sql.y:316
		{
			yyVAL.node = NewSimpleParseNode(SELECT_STAR, "*")
		}
	case 38:
		yyVAL.node = yyS[yypt-0].node
	case 39:
		//line sql.y:321
		{
			yyVAL.node = yyS[yypt-1].node.PushTwo(yyS[yypt-2].node, yyS[yypt-0].node)
		}
	case 40:
		//line sql.y:325
		{
			yyVAL.node = yyS[yypt-1].node.PushTwo(yyS[yypt-2].node, NewSimpleParseNode(SELECT_STAR, "*"))
		}
	case 41:
		yyVAL.node = yyS[yypt-0].node
	case 42:
		yyVAL.node = yyS[yypt-0].node
	case 43:
		//line sql.y:334
		{
			yyVAL.node = NewSimpleParseNode(AS, "as")
		}
	case 44:
		yyVAL.node = yyS[yypt-0].node
	case 45:
		//line sql.y:341
		{
			yyVAL.node = NewSimpleParseNode(NODE_LIST, "node_list")
			yyVAL.node.Push(yyS[yypt-0].node)
		}
	case 46:
		//line sql.y:346
		{
			yyVAL.node = yyS[yypt-2].node.Push(yyS[yypt-1].node)
		}
	case 47:
		//line sql.y:350
		{
			yyVAL.node.Push(yyS[yypt-0].node)
		}
	case 48:
		//line sql.y:356
		{
			yyVAL.node = NewSimpleParseNode(TABLE_EXPR, "")
			yyVAL.node.Push(yyS[yypt-1].node)
			yyVAL.node.Push(NewSimpleParseNode(NODE_LIST, "node_list"))
			yyVAL.node.Push(yyS[yypt-0].node)
		}
	case 49:
		//line sql.y:363
		{
			yyVAL.node = NewSimpleParseNode(TABLE_EXPR, "")
			yyVAL.node.Push(yyS[yypt-3].node)
			yyVAL.node.Push(NewSimpleParseNode(NODE_LIST, "node_list").Push(yyS[yypt-1].node))
			yyVAL.node.Push(yyS[yypt-0].node)
		}
	case 50:
		//line sql.y:370
		{
			yyVAL.node = yyS[yypt-1].node.PushTwo(yyS[yypt-2].node, yyS[yypt-0].node)
		}
	case 51:
		//line sql.y:374
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
		//line sql.y:385
		{
			yyVAL.node = NewSimpleParseNode(LEFT, "left join")
		}
	case 55:
		//line sql.y:389
		{
			yyVAL.node = NewSimpleParseNode(LEFT, "left join")
		}
	case 56:
		//line sql.y:393
		{
			yyVAL.node = NewSimpleParseNode(RIGHT, "right join")
		}
	case 57:
		//line sql.y:397
		{
			yyVAL.node = NewSimpleParseNode(RIGHT, "right join")
		}
	case 58:
		//line sql.y:401
		{
			yyVAL.node = yyS[yypt-0].node
		}
	case 59:
		//line sql.y:405
		{
			yyVAL.node = NewSimpleParseNode(CROSS, "cross join")
		}
	case 60:
		//line sql.y:409
		{
			yyVAL.node = NewSimpleParseNode(NATURAL, "natural join")
		}
	case 61:
		yyVAL.node = yyS[yypt-0].node
	case 62:
		//line sql.y:416
		{
			yyVAL.node = yyS[yypt-1].node.PushTwo(yyS[yypt-2].node, yyS[yypt-0].node)
		}
	case 63:
		//line sql.y:420
		{
			yyVAL.node = yyS[yypt-2].node.Push(yyS[yypt-1].node)
		}
	case 64:
		yyVAL.node = yyS[yypt-0].node
	case 65:
		//line sql.y:427
		{
			yyVAL.node = yyS[yypt-1].node.PushTwo(yyS[yypt-2].node, yyS[yypt-0].node)
		}
	case 66:
		//line sql.y:432
		{
			yyVAL.node = NewSimpleParseNode(USE, "use")
		}
	case 67:
		//line sql.y:436
		{
			yyVAL.node.Push(yyS[yypt-1].node)
		}
	case 68:
		//line sql.y:440
		{
			yyVAL.node.Push(yyS[yypt-1].node)
		}
	case 69:
		//line sql.y:445
		{
			yyVAL.node = NewSimpleParseNode(WHERE, "where")
		}
	case 70:
		//line sql.y:449
		{
			yyVAL.node = yyS[yypt-1].node.Push(yyS[yypt-0].node)
		}
	case 71:
		yyVAL.node = yyS[yypt-0].node
	case 72:
		//line sql.y:456
		{
			yyVAL.node = yyS[yypt-1].node.PushTwo(yyS[yypt-2].node, yyS[yypt-0].node)
		}
	case 73:
		//line sql.y:460
		{
			yyVAL.node = yyS[yypt-1].node.PushTwo(yyS[yypt-2].node, yyS[yypt-0].node)
		}
	case 74:
		//line sql.y:464
		{
			yyVAL.node = yyS[yypt-1].node.Push(yyS[yypt-0].node)
		}
	case 75:
		//line sql.y:468
		{
			yyVAL.node = yyS[yypt-2].node.Push(yyS[yypt-1].node)
		}
	case 76:
		//line sql.y:474
		{
			yyVAL.node = yyS[yypt-1].node.PushTwo(yyS[yypt-2].node, yyS[yypt-0].node)
		}
	case 77:
		//line sql.y:478
		{
			yyVAL.node = yyS[yypt-1].node.PushTwo(yyS[yypt-2].node, yyS[yypt-0].node)
		}
	case 78:
		//line sql.y:482
		{
			yyVAL.node = NewSimpleParseNode(NOT_IN, "not in").PushTwo(yyS[yypt-3].node, yyS[yypt-0].node)
		}
	case 79:
		//line sql.y:486
		{
			yyVAL.node = yyS[yypt-1].node.PushTwo(yyS[yypt-2].node, yyS[yypt-0].node)
		}
	case 80:
		//line sql.y:490
		{
			yyVAL.node = NewSimpleParseNode(NOT_LIKE, "not like").PushTwo(yyS[yypt-3].node, yyS[yypt-0].node)
		}
	case 81:
		//line sql.y:494
		{
			yyVAL.node = yyS[yypt-3].node
			yyVAL.node.Push(yyS[yypt-4].node)
			yyVAL.node.Push(yyS[yypt-2].node)
			yyVAL.node.Push(yyS[yypt-0].node)
		}
	case 82:
		//line sql.y:501
		{
			yyVAL.node = NewSimpleParseNode(NOT_BETWEEN, "not between")
			yyVAL.node.Push(yyS[yypt-5].node)
			yyVAL.node.Push(yyS[yypt-2].node)
			yyVAL.node.Push(yyS[yypt-0].node)
		}
	case 83:
		//line sql.y:508
		{
			yyVAL.node = NewSimpleParseNode(IS_NULL, "is null").Push(yyS[yypt-2].node)
		}
	case 84:
		//line sql.y:512
		{
			yyVAL.node = NewSimpleParseNode(IS_NOT_NULL, "is not null").Push(yyS[yypt-3].node)
		}
	case 85:
		//line sql.y:516
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
		//line sql.y:531
		{
			yyVAL.node = yyS[yypt-1].node.Push(yyS[yypt-0].node)
		}
	case 94:
		yyVAL.node = yyS[yypt-0].node
	case 95:
		//line sql.y:538
		{
			yyVAL.node = NewSimpleParseNode(NODE_LIST, "node_list")
			yyVAL.node.Push(yyS[yypt-0].node)
		}
	case 96:
		//line sql.y:543
		{
			yyVAL.node.Push(yyS[yypt-0].node)
		}
	case 97:
		//line sql.y:549
		{
			yyVAL.node = yyS[yypt-2].node.Push(yyS[yypt-1].node)
		}
	case 98:
		//line sql.y:553
		{
			yyVAL.node = yyS[yypt-2].node.Push(yyS[yypt-1].node)
		}
	case 99:
		//line sql.y:559
		{
			yyVAL.node = NewSimpleParseNode(NODE_LIST, "node_list")
			yyVAL.node.Push(yyS[yypt-0].node)
		}
	case 100:
		//line sql.y:564
		{
			yyVAL.node.Push(yyS[yypt-0].node)
		}
	case 101:
		yyVAL.node = yyS[yypt-0].node
	case 102:
		yyVAL.node = yyS[yypt-0].node
	case 103:
		//line sql.y:572
		{
			yyVAL.node = yyS[yypt-2].node.Push(yyS[yypt-1].node)
		}
	case 104:
		//line sql.y:576
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
		//line sql.y:588
		{
			yyVAL.node = yyS[yypt-1].node.PushTwo(yyS[yypt-2].node, yyS[yypt-0].node)
		}
	case 106:
		//line sql.y:592
		{
			yyVAL.node = yyS[yypt-1].node.PushTwo(yyS[yypt-2].node, yyS[yypt-0].node)
		}
	case 107:
		//line sql.y:596
		{
			yyVAL.node = yyS[yypt-1].node.PushTwo(yyS[yypt-2].node, yyS[yypt-0].node)
		}
	case 108:
		//line sql.y:600
		{
			yyVAL.node = yyS[yypt-1].node.PushTwo(yyS[yypt-2].node, yyS[yypt-0].node)
		}
	case 109:
		//line sql.y:604
		{
			yyVAL.node = yyS[yypt-1].node.PushTwo(yyS[yypt-2].node, yyS[yypt-0].node)
		}
	case 110:
		//line sql.y:608
		{
			yyVAL.node = yyS[yypt-1].node.PushTwo(yyS[yypt-2].node, yyS[yypt-0].node)
		}
	case 111:
		//line sql.y:612
		{
			yyVAL.node = yyS[yypt-1].node.PushTwo(yyS[yypt-2].node, yyS[yypt-0].node)
		}
	case 112:
		//line sql.y:616
		{
			yyVAL.node = yyS[yypt-1].node.PushTwo(yyS[yypt-2].node, yyS[yypt-0].node)
		}
	case 113:
		//line sql.y:620
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
		//line sql.y:636
		{
			yyS[yypt-2].node.Type = FUNCTION
			yyVAL.node = yyS[yypt-2].node.Push(NewSimpleParseNode(NODE_LIST, "node_list"))
		}
	case 115:
		//line sql.y:641
		{
			yyS[yypt-3].node.Type = FUNCTION
			yyVAL.node = yyS[yypt-3].node.Push(yyS[yypt-1].node)
		}
	case 116:
		//line sql.y:646
		{
			yyS[yypt-4].node.Type = FUNCTION
			yyVAL.node = yyS[yypt-4].node.Push(yyS[yypt-2].node)
			yyVAL.node = yyS[yypt-4].node.Push(yyS[yypt-1].node)
		}
	case 117:
		//line sql.y:652
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
		//line sql.y:664
		{
			yyVAL.node = NewSimpleParseNode(UPLUS, "+")
		}
	case 122:
		//line sql.y:668
		{
			yyVAL.node = NewSimpleParseNode(UMINUS, "-")
		}
	case 123:
		yyVAL.node = yyS[yypt-0].node
	case 124:
		//line sql.y:675
		{
			yyVAL.node = NewSimpleParseNode(CASE_WHEN, "case")
			yyVAL.node.Push(yyS[yypt-1].node)
		}
	case 125:
		//line sql.y:680
		{
			yyVAL.node.PushTwo(yyS[yypt-2].node, yyS[yypt-1].node)
		}
	case 126:
		//line sql.y:686
		{
			yyVAL.node = NewSimpleParseNode(WHEN_LIST, "when_list")
			yyVAL.node.Push(yyS[yypt-0].node)
		}
	case 127:
		//line sql.y:691
		{
			yyVAL.node.Push(yyS[yypt-0].node)
		}
	case 128:
		//line sql.y:697
		{
			yyVAL.node.PushTwo(yyS[yypt-2].node, yyS[yypt-0].node)
		}
	case 129:
		//line sql.y:701
		{
			yyVAL.node.Push(yyS[yypt-0].node)
		}
	case 130:
		yyVAL.node = yyS[yypt-0].node
	case 131:
		//line sql.y:708
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
		//line sql.y:719
		{
			yyVAL.node = NewSimpleParseNode(GROUP, "group")
		}
	case 137:
		//line sql.y:723
		{
			yyVAL.node = yyS[yypt-2].node.Push(yyS[yypt-0].node)
		}
	case 138:
		//line sql.y:728
		{
			yyVAL.node = NewSimpleParseNode(HAVING, "having")
		}
	case 139:
		//line sql.y:732
		{
			yyVAL.node = yyS[yypt-1].node.Push(yyS[yypt-0].node)
		}
	case 140:
		//line sql.y:737
		{
			yyVAL.node = NewSimpleParseNode(ORDER, "order")
		}
	case 141:
		//line sql.y:741
		{
			yyVAL.node = yyS[yypt-2].node.Push(yyS[yypt-0].node)
		}
	case 142:
		//line sql.y:747
		{
			yyVAL.node = NewSimpleParseNode(NODE_LIST, "node_list")
			yyVAL.node.Push(yyS[yypt-0].node)
		}
	case 143:
		//line sql.y:752
		{
			yyVAL.node = yyS[yypt-2].node.Push(yyS[yypt-0].node)
		}
	case 144:
		//line sql.y:758
		{
			yyVAL.node = yyS[yypt-0].node.Push(yyS[yypt-1].node)
		}
	case 145:
		//line sql.y:763
		{
			yyVAL.node = NewSimpleParseNode(ASC, "asc")
		}
	case 146:
		yyVAL.node = yyS[yypt-0].node
	case 147:
		yyVAL.node = yyS[yypt-0].node
	case 148:
		//line sql.y:770
		{
			yyVAL.node = NewSimpleParseNode(LIMIT, "limit")
		}
	case 149:
		//line sql.y:774
		{
			yyVAL.node = yyS[yypt-1].node.Push(yyS[yypt-0].node)
		}
	case 150:
		//line sql.y:778
		{
			yyVAL.node = yyS[yypt-3].node.PushTwo(yyS[yypt-2].node, yyS[yypt-0].node)
		}
	case 151:
		//line sql.y:783
		{
			yyVAL.node = NewSimpleParseNode(NO_LOCK, "")
		}
	case 152:
		//line sql.y:787
		{
			yyVAL.node = NewSimpleParseNode(FOR_UPDATE, " for update")
		}
	case 153:
		//line sql.y:791
		{
			yyVAL.node = NewSimpleParseNode(LOCK_IN_SHARE_MODE, " lock in share mode")
		}
	case 154:
		//line sql.y:796
		{
			yyVAL.node = NewSimpleParseNode(COLUMN_LIST, "")
		}
	case 155:
		//line sql.y:800
		{
			yyVAL.node = yyS[yypt-1].node
		}
	case 156:
		//line sql.y:806
		{
			yyVAL.node = NewSimpleParseNode(COLUMN_LIST, "")
			yyVAL.node.Push(yyS[yypt-0].node)
		}
	case 157:
		//line sql.y:811
		{
			yyVAL.node = yyS[yypt-2].node.Push(yyS[yypt-0].node)
		}
	case 158:
		//line sql.y:817
		{
			yyVAL.node = NewSimpleParseNode(INDEX_LIST, "")
			yyVAL.node.Push(yyS[yypt-0].node)
		}
	case 159:
		//line sql.y:822
		{
			yyVAL.node = yyS[yypt-2].node.Push(yyS[yypt-0].node)
		}
	case 160:
		//line sql.y:827
		{
			yyVAL.node = NewSimpleParseNode(DUPLICATE, "duplicate")
		}
	case 161:
		//line sql.y:831
		{
			yyVAL.node = yyS[yypt-3].node.Push(yyS[yypt-0].node)
		}
	case 162:
		//line sql.y:837
		{
			yyVAL.node = NewSimpleParseNode(NODE_LIST, "node_list")
			yyVAL.node.Push(yyS[yypt-0].node)
		}
	case 163:
		//line sql.y:842
		{
			yyVAL.node = yyS[yypt-2].node.Push(yyS[yypt-0].node)
		}
	case 164:
		//line sql.y:848
		{
			yyVAL.node = yyS[yypt-1].node.PushTwo(yyS[yypt-2].node, yyS[yypt-0].node)
		}
	case 165:
		//line sql.y:853
		{
			yyVAL.node = nil
		}
	case 166:
		yyVAL.node = yyS[yypt-0].node
	case 167:
		//line sql.y:857
		{
			yyVAL.node = nil
		}
	case 168:
		yyVAL.node = yyS[yypt-0].node
	case 169:
		//line sql.y:861
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
		//line sql.y:872
		{
			yyVAL.node = nil
		}
	case 177:
		yyVAL.node = yyS[yypt-0].node
	case 178:
		//line sql.y:876
		{
			yyVAL.node = nil
		}
	case 179:
		yyVAL.node = yyS[yypt-0].node
	case 180:
		//line sql.y:880
		{
			yyVAL.node = nil
		}
	case 181:
		yyVAL.node = yyS[yypt-0].node
	case 182:
		//line sql.y:885
		{
			yyVAL.node.LowerCase()
		}
	case 183:
		//line sql.y:890
		{
			ForceEOF(yylex)
		}
	}
	goto yystack /* stack new state and value */
}
