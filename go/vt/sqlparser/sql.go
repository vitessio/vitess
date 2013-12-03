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
const FORCE = 57398
const ON = 57399
const AND = 57400
const OR = 57401
const NOT = 57402
const UNARY = 57403
const CASE = 57404
const WHEN = 57405
const THEN = 57406
const ELSE = 57407
const END = 57408
const CREATE = 57409
const ALTER = 57410
const DROP = 57411
const RENAME = 57412
const TABLE = 57413
const INDEX = 57414
const TO = 57415
const IGNORE = 57416
const IF = 57417
const UNIQUE = 57418
const USING = 57419
const NODE_LIST = 57420
const UPLUS = 57421
const UMINUS = 57422
const CASE_WHEN = 57423
const WHEN_LIST = 57424
const SELECT_STAR = 57425
const NO_DISTINCT = 57426
const FUNCTION = 57427
const FOR_UPDATE = 57428
const NOT_FOR_UPDATE = 57429
const NOT_IN = 57430
const NOT_LIKE = 57431
const NOT_BETWEEN = 57432
const IS_NULL = 57433
const IS_NOT_NULL = 57434
const UNION_ALL = 57435
const COMMENT_LIST = 57436
const COLUMN_LIST = 57437
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
	-1, 167,
	34, 43,
	-2, 64,
}

const yyNprod = 178
const yyPrivate = 57344

var yyTokenNames []string
var yyStates []string

const yyLast = 522

var yyAct = []int{

	65, 48, 296, 212, 262, 133, 215, 179, 236, 165,
	151, 101, 62, 141, 260, 147, 64, 260, 132, 3,
	59, 199, 100, 139, 22, 23, 24, 25, 226, 227,
	228, 229, 230, 60, 231, 232, 22, 23, 24, 25,
	40, 154, 105, 106, 260, 22, 23, 24, 25, 22,
	23, 24, 25, 32, 57, 240, 52, 36, 34, 222,
	49, 95, 92, 38, 39, 54, 100, 100, 129, 134,
	282, 199, 135, 281, 327, 37, 175, 326, 74, 254,
	53, 285, 284, 78, 128, 131, 83, 278, 286, 146,
	197, 186, 63, 75, 76, 77, 142, 140, 143, 253,
	279, 68, 302, 150, 259, 81, 129, 129, 178, 252,
	104, 184, 185, 198, 188, 189, 190, 191, 192, 193,
	194, 195, 176, 177, 67, 174, 251, 241, 79, 80,
	61, 200, 187, 256, 169, 84, 172, 201, 142, 104,
	143, 204, 196, 218, 12, 13, 14, 15, 82, 129,
	149, 102, 142, 205, 143, 207, 208, 203, 202, 118,
	119, 120, 214, 211, 206, 217, 220, 98, 160, 304,
	173, 103, 303, 16, 171, 277, 233, 105, 106, 237,
	223, 201, 276, 246, 247, 234, 243, 74, 158, 245,
	161, 275, 78, 238, 239, 83, 242, 238, 239, 244,
	250, 130, 75, 76, 77, 116, 117, 118, 119, 120,
	68, 273, 271, 148, 81, 148, 274, 272, 258, 99,
	205, 261, 17, 18, 20, 19, 89, 314, 315, 22,
	23, 24, 25, 67, 269, 270, 199, 79, 80, 157,
	159, 156, 312, 291, 84, 142, 168, 143, 12, 288,
	168, 90, 180, 307, 234, 235, 224, 82, 89, 166,
	306, 145, 138, 100, 294, 297, 293, 289, 113, 114,
	115, 116, 117, 118, 119, 120, 298, 74, 168, 137,
	50, 213, 78, 292, 283, 83, 280, 235, 308, 305,
	266, 63, 75, 76, 77, 265, 172, 219, 170, 310,
	68, 129, 201, 129, 81, 163, 316, 318, 162, 12,
	320, 321, 323, 297, 96, 324, 94, 317, 93, 319,
	91, 325, 88, 67, 328, 74, 86, 79, 80, 61,
	78, 58, 55, 83, 84, 12, 46, 87, 309, 130,
	75, 76, 77, 290, 45, 249, 74, 82, 68, 152,
	97, 78, 81, 12, 83, 181, 78, 182, 183, 83,
	130, 75, 76, 77, 102, 130, 75, 76, 77, 68,
	43, 67, 41, 81, 136, 79, 80, 210, 81, 330,
	287, 85, 84, 113, 114, 115, 116, 117, 118, 119,
	120, 263, 67, 301, 264, 82, 79, 80, 216, 300,
	268, 79, 80, 84, 148, 47, 12, 331, 84, 322,
	27, 153, 33, 221, 78, 155, 82, 83, 35, 51,
	56, 82, 144, 130, 75, 76, 77, 311, 255, 329,
	313, 295, 136, 299, 267, 69, 81, 26, 108, 112,
	110, 111, 113, 114, 115, 116, 117, 118, 119, 120,
	70, 28, 29, 30, 31, 124, 125, 126, 127, 79,
	80, 121, 122, 123, 248, 73, 84, 113, 114, 115,
	116, 117, 118, 119, 120, 71, 72, 257, 209, 82,
	107, 66, 167, 109, 113, 114, 115, 116, 117, 118,
	119, 120, 113, 114, 115, 116, 117, 118, 119, 120,
	226, 227, 228, 229, 230, 225, 231, 232, 164, 42,
	21, 44, 11, 10, 9, 8, 7, 6, 5, 4,
	2, 1,
}
var yyPact = []int{

	140, -1000, -1000, 181, -1000, -1000, -1000, -1000, -1000, -1000,
	-1000, -1000, -1000, -1000, -1000, -1000, -1000, -33, -32, -11,
	-23, 402, 355, -1000, -1000, -1000, 352, -1000, 315, 302,
	397, 246, -34, -7, -1000, -21, -1000, 298, -36, 297,
	-1000, -1000, 257, -1000, 366, 292, 304, 288, 174, -1000,
	207, 286, -4, 284, 282, -27, 280, 330, 104, 211,
	-1000, -1000, 345, 96, 113, 417, -1000, 326, 305, -1000,
	-1000, 389, 236, -1000, 219, -1000, -1000, -1000, -1000, -1000,
	-1000, -1000, -1000, -1000, 167, -1000, 218, 246, 395, 246,
	326, -1000, 329, -51, 156, 274, -1000, -1000, 271, 216,
	257, 264, -1000, 102, 58, 326, 326, 389, 209, 334,
	389, 389, 66, 389, 389, 389, 389, 389, 389, 389,
	389, -1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, 417,
	67, -22, 1, 19, 417, -1000, 331, 257, 402, 60,
	74, -1000, 326, 326, 349, 247, 206, 386, 326, -1000,
	-1000, -1000, -1000, 80, 263, -1000, -29, -1000, -1000, -1000,
	-1000, -1000, -1000, -1000, 204, 447, 244, 132, -20, -1000,
	-1000, -1000, -1000, -1000, 15, 257, -1000, -1000, 425, -1000,
	331, 209, 389, 389, 425, 400, -1000, 320, 135, 135,
	135, 87, 87, -1000, -1000, -1000, 262, -1000, -1000, 389,
	-1000, 425, 14, -3, -1000, -1000, 18, 0, -1000, 70,
	209, 181, -8, -1000, 386, 377, 381, 113, 261, -1000,
	-1000, 256, -1000, 390, 212, 212, -1000, -1000, 159, 158,
	138, 129, 122, -25, -12, 402, -1000, 252, -14, -17,
	250, -1000, -30, -31, -24, -1000, 425, 316, 389, -1000,
	425, -1000, -1000, -1000, 326, -1000, 313, 191, -1000, -1000,
	249, 377, -1000, 389, 389, -1000, -1000, 388, 380, 447,
	39, -1000, 119, -1000, 116, -1000, -1000, -1000, -1000, -1000,
	136, 217, 210, -1000, -1000, -1000, -1000, 389, 425, -1000,
	307, 209, -1000, -1000, 375, 190, -1000, 201, -1000, 386,
	326, 389, 326, -1000, -1000, -1000, 247, 247, 425, 403,
	-1000, 389, 389, -1000, -1000, -1000, 377, 113, 184, 113,
	-35, -38, 246, 425, -1000, 363, -1000, -1000, 174, -1000,
	401, -1000,
}
var yyPgo = []int{

	0, 521, 520, 18, 519, 518, 517, 516, 515, 514,
	513, 512, 437, 511, 510, 509, 20, 33, 12, 11,
	508, 9, 505, 482, 8, 15, 16, 481, 480, 478,
	477, 7, 5, 0, 476, 475, 465, 23, 13, 450,
	435, 434, 433, 6, 431, 2, 430, 4, 429, 428,
	422, 3, 1, 60, 420, 419, 418, 415, 413, 412,
	411, 10, 410,
}
var yyR1 = []int{

	0, 1, 2, 2, 2, 2, 2, 2, 2, 2,
	2, 3, 3, 4, 5, 6, 7, 8, 8, 9,
	9, 10, 11, 11, 62, 12, 13, 13, 14, 14,
	14, 14, 14, 15, 15, 16, 16, 17, 17, 17,
	17, 18, 18, 19, 19, 20, 20, 20, 21, 21,
	21, 21, 22, 22, 22, 22, 22, 22, 22, 22,
	22, 23, 23, 23, 24, 24, 24, 25, 25, 26,
	26, 26, 26, 26, 27, 27, 27, 27, 27, 27,
	27, 27, 27, 27, 28, 28, 28, 28, 28, 28,
	28, 29, 29, 30, 30, 31, 31, 32, 32, 33,
	33, 33, 33, 33, 33, 33, 33, 33, 33, 33,
	33, 33, 33, 33, 33, 33, 33, 34, 34, 35,
	35, 35, 36, 36, 37, 37, 38, 38, 39, 39,
	40, 40, 40, 40, 41, 41, 42, 42, 43, 43,
	44, 44, 45, 46, 46, 46, 47, 47, 47, 48,
	48, 50, 50, 51, 51, 49, 49, 52, 52, 53,
	54, 54, 55, 55, 56, 56, 57, 57, 57, 57,
	57, 58, 58, 59, 59, 60, 60, 61,
}
var yyR2 = []int{

	0, 1, 1, 1, 1, 1, 1, 1, 1, 1,
	1, 12, 3, 7, 8, 7, 3, 5, 8, 6,
	7, 5, 4, 5, 0, 2, 0, 2, 1, 2,
	1, 1, 1, 0, 1, 1, 3, 1, 1, 3,
	3, 1, 1, 0, 1, 1, 3, 3, 2, 4,
	3, 5, 1, 1, 2, 3, 2, 3, 2, 2,
	2, 1, 3, 3, 0, 5, 5, 0, 2, 1,
	3, 3, 2, 3, 3, 3, 4, 3, 4, 5,
	6, 3, 4, 4, 1, 1, 1, 1, 1, 1,
	1, 2, 1, 1, 3, 3, 3, 1, 3, 1,
	1, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 2, 3, 4, 5, 4, 1, 1, 1, 1,
	1, 1, 3, 4, 1, 2, 4, 2, 1, 3,
	1, 1, 1, 1, 0, 3, 0, 2, 0, 3,
	1, 3, 2, 0, 1, 1, 0, 2, 4, 0,
	2, 0, 3, 1, 3, 0, 5, 1, 3, 3,
	0, 2, 0, 3, 0, 1, 1, 1, 1, 1,
	1, 0, 1, 0, 1, 0, 2, 0,
}
var yyChk = []int{

	-1000, -1, -2, -3, -4, -5, -6, -7, -8, -9,
	-10, -11, 4, 5, 6, 7, 33, 82, 83, 85,
	84, -14, 48, 49, 50, 51, -12, -62, -12, -12,
	-12, -12, 86, -59, 91, -56, 89, 86, 86, 87,
	-3, 17, -15, 18, -13, 29, 34, 8, -52, -53,
	34, -55, 90, 87, 86, 34, -54, 90, 34, -16,
	-17, 72, -18, 34, -26, -33, -27, 66, 43, -40,
	-39, -35, -34, -36, 20, 35, 36, 37, 25, 70,
	71, 47, 90, 28, 77, 15, 34, 33, 34, 52,
	44, 34, 66, 34, 34, 88, 34, 20, 63, 8,
	52, -19, 19, 75, 43, 64, 65, -28, 21, 66,
	23, 24, 22, 67, 68, 69, 70, 71, 72, 73,
	74, 44, 45, 46, 38, 39, 40, 41, -26, -33,
	34, -26, -3, -32, -33, -33, 43, 43, 43, -37,
	-18, -38, 78, 80, -50, 43, -52, -25, 9, -53,
	-18, -61, 20, -60, 92, -57, 85, 83, 32, 84,
	12, 34, 34, 34, -20, -21, 43, -23, 34, -17,
	34, 72, 34, 112, -16, 18, -26, -26, -33, -31,
	43, 21, 23, 24, -33, -33, 25, 66, -33, -33,
	-33, -33, -33, -33, -33, -33, 75, 112, 112, 52,
	112, -33, -16, -3, 81, -38, -37, -18, -18, -29,
	28, -3, -51, 34, -25, -43, 12, -26, 63, 34,
	-61, -58, 88, -25, 52, -22, 53, 54, 55, 56,
	57, 59, 60, -21, -3, 43, -24, -19, 61, 62,
	75, 112, -16, -32, -3, -31, -33, -33, 64, 25,
	-33, 112, 112, 81, 79, -49, 63, -30, -31, 112,
	52, -43, -47, 14, 13, 34, 34, -41, 10, -21,
	-21, 53, 58, 53, 58, 53, 53, 53, 112, 112,
	34, 87, 87, 34, 112, 112, 112, 64, -33, -18,
	30, 52, 34, -47, -33, -44, -45, -33, -61, -42,
	11, 13, 63, 53, 53, -24, 43, 43, -33, 31,
	-31, 52, 52, -46, 26, 27, -43, -26, -32, -26,
	-51, -51, 6, -33, -45, -47, 112, 112, -52, -48,
	16, 6,
}
var yyDef = []int{

	0, -2, 1, 2, 3, 4, 5, 6, 7, 8,
	9, 10, 24, 24, 24, 24, 24, 173, 164, 0,
	0, 0, 28, 30, 31, 32, 33, 26, 0, 0,
	0, 0, 162, 0, 174, 0, 165, 0, 160, 0,
	12, 29, 0, 34, 25, 0, 0, 0, 16, 157,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	35, 37, -2, 128, 41, 42, 69, 0, 0, 99,
	100, 0, 0, 116, 0, 130, 131, 132, 133, 119,
	120, 121, 117, 118, 0, 27, 151, 0, 67, 0,
	0, 177, 0, 175, 0, 0, 22, 161, 0, 0,
	0, 0, 44, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 84, 85, 86, 87, 88, 89, 90, 72, 0,
	128, 0, 0, 0, 97, 111, 0, 0, 0, 0,
	0, 124, 0, 0, 0, 0, 67, 138, 0, 158,
	159, 17, 163, 0, 0, 177, 171, 166, 167, 168,
	169, 170, 21, 23, 67, 45, 0, -2, 61, 36,
	39, 40, 129, 112, 0, 0, 70, 71, 74, 75,
	0, 0, 0, 0, 77, 0, 81, 0, 103, 104,
	105, 106, 107, 108, 109, 110, 0, 73, 101, 0,
	102, 97, 0, 0, 122, 125, 0, 0, 127, 155,
	0, 92, 0, 153, 138, 146, 0, 68, 0, 176,
	19, 0, 172, 134, 0, 0, 52, 53, 0, 0,
	0, 0, 0, 0, 0, 0, 48, 0, 0, 0,
	0, 113, 0, 0, 0, 76, 78, 0, 0, 82,
	98, 115, 83, 123, 0, 13, 0, 91, 93, 152,
	0, 146, 15, 0, 0, 177, 20, 136, 0, 47,
	50, 54, 0, 56, 0, 58, 59, 60, 46, 63,
	64, 0, 0, 62, 114, 95, 96, 0, 79, 126,
	0, 0, 154, 14, 147, 139, 140, 143, 18, 138,
	0, 0, 0, 55, 57, 49, 0, 0, 80, 0,
	94, 0, 0, 142, 144, 145, 146, 137, 135, 51,
	0, 0, 0, 148, 141, 149, 65, 66, 156, 11,
	0, 150,
}
var yyTok1 = []int{

	1, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 74, 67, 3,
	43, 112, 72, 70, 52, 71, 75, 73, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	45, 44, 46, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 69, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 68, 3, 47,
}
var yyTok2 = []int{

	2, 3, 4, 5, 6, 7, 8, 9, 10, 11,
	12, 13, 14, 15, 16, 17, 18, 19, 20, 21,
	22, 23, 24, 25, 26, 27, 28, 29, 30, 31,
	32, 33, 34, 35, 36, 37, 38, 39, 40, 41,
	42, 48, 49, 50, 51, 53, 54, 55, 56, 57,
	58, 59, 60, 61, 62, 63, 64, 65, 66, 76,
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
		//line sql.y:145
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
		//line sql.y:162
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
		//line sql.y:176
		{
			yyVAL.node = yyS[yypt-1].node.PushTwo(yyS[yypt-2].node, yyS[yypt-0].node)
		}
	case 13:
		//line sql.y:182
		{
			yyVAL.node = yyS[yypt-6].node
			yyVAL.node.Push(yyS[yypt-5].node) // 0: comment_opt
			yyVAL.node.Push(yyS[yypt-3].node) // 1: table_name
			yyVAL.node.Push(yyS[yypt-2].node) // 2: column_list_opt
			yyVAL.node.Push(yyS[yypt-1].node) // 3: values
			yyVAL.node.Push(yyS[yypt-0].node) // 4: on_dup_opt
		}
	case 14:
		//line sql.y:193
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
		//line sql.y:205
		{
			yyVAL.node = yyS[yypt-6].node
			yyVAL.node.Push(yyS[yypt-5].node) // 0: comment_opt
			yyVAL.node.Push(yyS[yypt-3].node) // 1: table_name
			yyVAL.node.Push(yyS[yypt-2].node) // 2: where_expression_opt
			yyVAL.node.Push(yyS[yypt-1].node) // 3: order_by_opt
			yyVAL.node.Push(yyS[yypt-0].node) // 4: limit_opt
		}
	case 16:
		//line sql.y:216
		{
			yyVAL.node = yyS[yypt-2].node
			yyVAL.node.Push(yyS[yypt-1].node)
			yyVAL.node.Push(yyS[yypt-0].node)
		}
	case 17:
		//line sql.y:224
		{
			yyVAL.node.Push(yyS[yypt-1].node)
		}
	case 18:
		//line sql.y:228
		{
			// Change this to an alter statement
			yyVAL.node = NewSimpleParseNode(ALTER, "alter")
			yyVAL.node.Push(yyS[yypt-1].node)
		}
	case 19:
		//line sql.y:236
		{
			yyVAL.node.Push(yyS[yypt-2].node)
		}
	case 20:
		//line sql.y:240
		{
			// Change this to a rename statement
			yyVAL.node = NewSimpleParseNode(RENAME, "rename")
			yyVAL.node.PushTwo(yyS[yypt-3].node, yyS[yypt-0].node)
		}
	case 21:
		//line sql.y:248
		{
			yyVAL.node.PushTwo(yyS[yypt-2].node, yyS[yypt-0].node)
		}
	case 22:
		//line sql.y:254
		{
			yyVAL.node.Push(yyS[yypt-0].node)
		}
	case 23:
		//line sql.y:258
		{
			// Change this to an alter statement
			yyVAL.node = NewSimpleParseNode(ALTER, "alter")
			yyVAL.node.Push(yyS[yypt-0].node)
		}
	case 24:
		//line sql.y:265
		{
			SetAllowComments(yylex, true)
		}
	case 25:
		//line sql.y:269
		{
			yyVAL.node = yyS[yypt-0].node
			SetAllowComments(yylex, false)
		}
	case 26:
		//line sql.y:275
		{
			yyVAL.node = NewSimpleParseNode(COMMENT_LIST, "")
		}
	case 27:
		//line sql.y:279
		{
			yyVAL.node = yyS[yypt-1].node.Push(yyS[yypt-0].node)
		}
	case 28:
		yyVAL.node = yyS[yypt-0].node
	case 29:
		//line sql.y:286
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
		//line sql.y:294
		{
			yyVAL.node = NewSimpleParseNode(NO_DISTINCT, "")
		}
	case 34:
		//line sql.y:298
		{
			yyVAL.node = NewSimpleParseNode(DISTINCT, "distinct")
		}
	case 35:
		//line sql.y:304
		{
			yyVAL.node = NewSimpleParseNode(NODE_LIST, "node_list")
			yyVAL.node.Push(yyS[yypt-0].node)
		}
	case 36:
		//line sql.y:309
		{
			yyVAL.node.Push(yyS[yypt-0].node)
		}
	case 37:
		//line sql.y:315
		{
			yyVAL.node = NewSimpleParseNode(SELECT_STAR, "*")
		}
	case 38:
		yyVAL.node = yyS[yypt-0].node
	case 39:
		//line sql.y:320
		{
			yyVAL.node = yyS[yypt-1].node.PushTwo(yyS[yypt-2].node, yyS[yypt-0].node)
		}
	case 40:
		//line sql.y:324
		{
			yyVAL.node = yyS[yypt-1].node.PushTwo(yyS[yypt-2].node, NewSimpleParseNode(SELECT_STAR, "*"))
		}
	case 41:
		yyVAL.node = yyS[yypt-0].node
	case 42:
		yyVAL.node = yyS[yypt-0].node
	case 43:
		//line sql.y:333
		{
			yyVAL.node = NewSimpleParseNode(AS, "as")
		}
	case 44:
		yyVAL.node = yyS[yypt-0].node
	case 45:
		//line sql.y:340
		{
			yyVAL.node = NewSimpleParseNode(NODE_LIST, "node_list")
			yyVAL.node.Push(yyS[yypt-0].node)
		}
	case 46:
		//line sql.y:345
		{
			yyVAL.node = yyS[yypt-2].node.Push(yyS[yypt-1].node)
		}
	case 47:
		//line sql.y:349
		{
			yyVAL.node.Push(yyS[yypt-0].node)
		}
	case 48:
		//line sql.y:355
		{
			yyVAL.node = NewSimpleParseNode(TABLE_EXPR, "")
			yyVAL.node.Push(yyS[yypt-1].node)
			yyVAL.node.Push(NewSimpleParseNode(NODE_LIST, "node_list"))
			yyVAL.node.Push(yyS[yypt-0].node)
		}
	case 49:
		//line sql.y:362
		{
			yyVAL.node = NewSimpleParseNode(TABLE_EXPR, "")
			yyVAL.node.Push(yyS[yypt-3].node)
			yyVAL.node.Push(NewSimpleParseNode(NODE_LIST, "node_list").Push(yyS[yypt-1].node))
			yyVAL.node.Push(yyS[yypt-0].node)
		}
	case 50:
		//line sql.y:369
		{
			yyVAL.node = yyS[yypt-1].node.PushTwo(yyS[yypt-2].node, yyS[yypt-0].node)
		}
	case 51:
		//line sql.y:373
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
		//line sql.y:384
		{
			yyVAL.node = NewSimpleParseNode(LEFT, "left join")
		}
	case 55:
		//line sql.y:388
		{
			yyVAL.node = NewSimpleParseNode(LEFT, "left join")
		}
	case 56:
		//line sql.y:392
		{
			yyVAL.node = NewSimpleParseNode(RIGHT, "right join")
		}
	case 57:
		//line sql.y:396
		{
			yyVAL.node = NewSimpleParseNode(RIGHT, "right join")
		}
	case 58:
		//line sql.y:400
		{
			yyVAL.node = yyS[yypt-0].node
		}
	case 59:
		//line sql.y:404
		{
			yyVAL.node = NewSimpleParseNode(CROSS, "cross join")
		}
	case 60:
		//line sql.y:408
		{
			yyVAL.node = NewSimpleParseNode(NATURAL, "natural join")
		}
	case 61:
		yyVAL.node = yyS[yypt-0].node
	case 62:
		//line sql.y:415
		{
			yyVAL.node = yyS[yypt-1].node.PushTwo(yyS[yypt-2].node, yyS[yypt-0].node)
		}
	case 63:
		//line sql.y:419
		{
			yyVAL.node = yyS[yypt-2].node.Push(yyS[yypt-1].node)
		}
	case 64:
		//line sql.y:424
		{
			yyVAL.node = NewSimpleParseNode(USE, "use")
		}
	case 65:
		//line sql.y:428
		{
			yyVAL.node.Push(yyS[yypt-1].node)
		}
	case 66:
		//line sql.y:432
		{
			yyVAL.node.Push(yyS[yypt-1].node)
		}
	case 67:
		//line sql.y:437
		{
			yyVAL.node = NewSimpleParseNode(WHERE, "where")
		}
	case 68:
		//line sql.y:441
		{
			yyVAL.node = yyS[yypt-1].node.Push(yyS[yypt-0].node)
		}
	case 69:
		yyVAL.node = yyS[yypt-0].node
	case 70:
		//line sql.y:448
		{
			yyVAL.node = yyS[yypt-1].node.PushTwo(yyS[yypt-2].node, yyS[yypt-0].node)
		}
	case 71:
		//line sql.y:452
		{
			yyVAL.node = yyS[yypt-1].node.PushTwo(yyS[yypt-2].node, yyS[yypt-0].node)
		}
	case 72:
		//line sql.y:456
		{
			yyVAL.node = yyS[yypt-1].node.Push(yyS[yypt-0].node)
		}
	case 73:
		//line sql.y:460
		{
			yyVAL.node = yyS[yypt-2].node.Push(yyS[yypt-1].node)
		}
	case 74:
		//line sql.y:466
		{
			yyVAL.node = yyS[yypt-1].node.PushTwo(yyS[yypt-2].node, yyS[yypt-0].node)
		}
	case 75:
		//line sql.y:470
		{
			yyVAL.node = yyS[yypt-1].node.PushTwo(yyS[yypt-2].node, yyS[yypt-0].node)
		}
	case 76:
		//line sql.y:474
		{
			yyVAL.node = NewSimpleParseNode(NOT_IN, "not in").PushTwo(yyS[yypt-3].node, yyS[yypt-0].node)
		}
	case 77:
		//line sql.y:478
		{
			yyVAL.node = yyS[yypt-1].node.PushTwo(yyS[yypt-2].node, yyS[yypt-0].node)
		}
	case 78:
		//line sql.y:482
		{
			yyVAL.node = NewSimpleParseNode(NOT_LIKE, "not like").PushTwo(yyS[yypt-3].node, yyS[yypt-0].node)
		}
	case 79:
		//line sql.y:486
		{
			yyVAL.node = yyS[yypt-3].node
			yyVAL.node.Push(yyS[yypt-4].node)
			yyVAL.node.Push(yyS[yypt-2].node)
			yyVAL.node.Push(yyS[yypt-0].node)
		}
	case 80:
		//line sql.y:493
		{
			yyVAL.node = NewSimpleParseNode(NOT_BETWEEN, "not between")
			yyVAL.node.Push(yyS[yypt-5].node)
			yyVAL.node.Push(yyS[yypt-2].node)
			yyVAL.node.Push(yyS[yypt-0].node)
		}
	case 81:
		//line sql.y:500
		{
			yyVAL.node = NewSimpleParseNode(IS_NULL, "is null").Push(yyS[yypt-2].node)
		}
	case 82:
		//line sql.y:504
		{
			yyVAL.node = NewSimpleParseNode(IS_NOT_NULL, "is not null").Push(yyS[yypt-3].node)
		}
	case 83:
		//line sql.y:508
		{
			yyVAL.node = yyS[yypt-3].node.Push(yyS[yypt-1].node)
		}
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
		yyVAL.node = yyS[yypt-0].node
	case 91:
		//line sql.y:523
		{
			yyVAL.node = yyS[yypt-1].node.Push(yyS[yypt-0].node)
		}
	case 92:
		yyVAL.node = yyS[yypt-0].node
	case 93:
		//line sql.y:530
		{
			yyVAL.node = NewSimpleParseNode(NODE_LIST, "node_list")
			yyVAL.node.Push(yyS[yypt-0].node)
		}
	case 94:
		//line sql.y:535
		{
			yyVAL.node.Push(yyS[yypt-0].node)
		}
	case 95:
		//line sql.y:541
		{
			yyVAL.node = yyS[yypt-2].node.Push(yyS[yypt-1].node)
		}
	case 96:
		//line sql.y:545
		{
			yyVAL.node = yyS[yypt-2].node.Push(yyS[yypt-1].node)
		}
	case 97:
		//line sql.y:551
		{
			yyVAL.node = NewSimpleParseNode(NODE_LIST, "node_list")
			yyVAL.node.Push(yyS[yypt-0].node)
		}
	case 98:
		//line sql.y:556
		{
			yyVAL.node.Push(yyS[yypt-0].node)
		}
	case 99:
		yyVAL.node = yyS[yypt-0].node
	case 100:
		yyVAL.node = yyS[yypt-0].node
	case 101:
		//line sql.y:564
		{
			yyVAL.node = yyS[yypt-2].node.Push(yyS[yypt-1].node)
		}
	case 102:
		//line sql.y:568
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
	case 103:
		//line sql.y:580
		{
			yyVAL.node = yyS[yypt-1].node.PushTwo(yyS[yypt-2].node, yyS[yypt-0].node)
		}
	case 104:
		//line sql.y:584
		{
			yyVAL.node = yyS[yypt-1].node.PushTwo(yyS[yypt-2].node, yyS[yypt-0].node)
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
	case 112:
		//line sql.y:628
		{
			yyS[yypt-2].node.Type = FUNCTION
			yyVAL.node = yyS[yypt-2].node.Push(NewSimpleParseNode(NODE_LIST, "node_list"))
		}
	case 113:
		//line sql.y:633
		{
			yyS[yypt-3].node.Type = FUNCTION
			yyVAL.node = yyS[yypt-3].node.Push(yyS[yypt-1].node)
		}
	case 114:
		//line sql.y:638
		{
			yyS[yypt-4].node.Type = FUNCTION
			yyVAL.node = yyS[yypt-4].node.Push(yyS[yypt-2].node)
			yyVAL.node = yyS[yypt-4].node.Push(yyS[yypt-1].node)
		}
	case 115:
		//line sql.y:644
		{
			yyS[yypt-3].node.Type = FUNCTION
			yyVAL.node = yyS[yypt-3].node.Push(yyS[yypt-1].node)
		}
	case 116:
		yyVAL.node = yyS[yypt-0].node
	case 117:
		yyVAL.node = yyS[yypt-0].node
	case 118:
		yyVAL.node = yyS[yypt-0].node
	case 119:
		//line sql.y:656
		{
			yyVAL.node = NewSimpleParseNode(UPLUS, "+")
		}
	case 120:
		//line sql.y:660
		{
			yyVAL.node = NewSimpleParseNode(UMINUS, "-")
		}
	case 121:
		yyVAL.node = yyS[yypt-0].node
	case 122:
		//line sql.y:667
		{
			yyVAL.node = NewSimpleParseNode(CASE_WHEN, "case")
			yyVAL.node.Push(yyS[yypt-1].node)
		}
	case 123:
		//line sql.y:672
		{
			yyVAL.node.PushTwo(yyS[yypt-2].node, yyS[yypt-1].node)
		}
	case 124:
		//line sql.y:678
		{
			yyVAL.node = NewSimpleParseNode(WHEN_LIST, "when_list")
			yyVAL.node.Push(yyS[yypt-0].node)
		}
	case 125:
		//line sql.y:683
		{
			yyVAL.node.Push(yyS[yypt-0].node)
		}
	case 126:
		//line sql.y:689
		{
			yyVAL.node.PushTwo(yyS[yypt-2].node, yyS[yypt-0].node)
		}
	case 127:
		//line sql.y:693
		{
			yyVAL.node.Push(yyS[yypt-0].node)
		}
	case 128:
		yyVAL.node = yyS[yypt-0].node
	case 129:
		//line sql.y:700
		{
			yyVAL.node = yyS[yypt-1].node.PushTwo(yyS[yypt-2].node, yyS[yypt-0].node)
		}
	case 130:
		yyVAL.node = yyS[yypt-0].node
	case 131:
		yyVAL.node = yyS[yypt-0].node
	case 132:
		yyVAL.node = yyS[yypt-0].node
	case 133:
		yyVAL.node = yyS[yypt-0].node
	case 134:
		//line sql.y:711
		{
			yyVAL.node = NewSimpleParseNode(GROUP, "group")
		}
	case 135:
		//line sql.y:715
		{
			yyVAL.node = yyS[yypt-2].node.Push(yyS[yypt-0].node)
		}
	case 136:
		//line sql.y:720
		{
			yyVAL.node = NewSimpleParseNode(HAVING, "having")
		}
	case 137:
		//line sql.y:724
		{
			yyVAL.node = yyS[yypt-1].node.Push(yyS[yypt-0].node)
		}
	case 138:
		//line sql.y:729
		{
			yyVAL.node = NewSimpleParseNode(ORDER, "order")
		}
	case 139:
		//line sql.y:733
		{
			yyVAL.node = yyS[yypt-2].node.Push(yyS[yypt-0].node)
		}
	case 140:
		//line sql.y:739
		{
			yyVAL.node = NewSimpleParseNode(NODE_LIST, "node_list")
			yyVAL.node.Push(yyS[yypt-0].node)
		}
	case 141:
		//line sql.y:744
		{
			yyVAL.node = yyS[yypt-2].node.Push(yyS[yypt-0].node)
		}
	case 142:
		//line sql.y:750
		{
			yyVAL.node = yyS[yypt-0].node.Push(yyS[yypt-1].node)
		}
	case 143:
		//line sql.y:755
		{
			yyVAL.node = NewSimpleParseNode(ASC, "asc")
		}
	case 144:
		yyVAL.node = yyS[yypt-0].node
	case 145:
		yyVAL.node = yyS[yypt-0].node
	case 146:
		//line sql.y:762
		{
			yyVAL.node = NewSimpleParseNode(LIMIT, "limit")
		}
	case 147:
		//line sql.y:766
		{
			yyVAL.node = yyS[yypt-1].node.Push(yyS[yypt-0].node)
		}
	case 148:
		//line sql.y:770
		{
			yyVAL.node = yyS[yypt-3].node.PushTwo(yyS[yypt-2].node, yyS[yypt-0].node)
		}
	case 149:
		//line sql.y:775
		{
			yyVAL.node = NewSimpleParseNode(NOT_FOR_UPDATE, "")
		}
	case 150:
		//line sql.y:779
		{
			yyVAL.node = NewSimpleParseNode(FOR_UPDATE, " for update")
		}
	case 151:
		//line sql.y:784
		{
			yyVAL.node = NewSimpleParseNode(COLUMN_LIST, "")
		}
	case 152:
		//line sql.y:788
		{
			yyVAL.node = yyS[yypt-1].node
		}
	case 153:
		//line sql.y:794
		{
			yyVAL.node = NewSimpleParseNode(COLUMN_LIST, "")
			yyVAL.node.Push(yyS[yypt-0].node)
		}
	case 154:
		//line sql.y:799
		{
			yyVAL.node = yyS[yypt-2].node.Push(yyS[yypt-0].node)
		}
	case 155:
		//line sql.y:804
		{
			yyVAL.node = NewSimpleParseNode(DUPLICATE, "duplicate")
		}
	case 156:
		//line sql.y:808
		{
			yyVAL.node = yyS[yypt-3].node.Push(yyS[yypt-0].node)
		}
	case 157:
		//line sql.y:814
		{
			yyVAL.node = NewSimpleParseNode(NODE_LIST, "node_list")
			yyVAL.node.Push(yyS[yypt-0].node)
		}
	case 158:
		//line sql.y:819
		{
			yyVAL.node = yyS[yypt-2].node.Push(yyS[yypt-0].node)
		}
	case 159:
		//line sql.y:825
		{
			yyVAL.node = yyS[yypt-1].node.PushTwo(yyS[yypt-2].node, yyS[yypt-0].node)
		}
	case 160:
		//line sql.y:830
		{
			yyVAL.node = nil
		}
	case 161:
		yyVAL.node = yyS[yypt-0].node
	case 162:
		//line sql.y:834
		{
			yyVAL.node = nil
		}
	case 163:
		yyVAL.node = yyS[yypt-0].node
	case 164:
		//line sql.y:838
		{
			yyVAL.node = nil
		}
	case 165:
		yyVAL.node = yyS[yypt-0].node
	case 166:
		yyVAL.node = yyS[yypt-0].node
	case 167:
		yyVAL.node = yyS[yypt-0].node
	case 168:
		yyVAL.node = yyS[yypt-0].node
	case 169:
		yyVAL.node = yyS[yypt-0].node
	case 170:
		yyVAL.node = yyS[yypt-0].node
	case 171:
		//line sql.y:849
		{
			yyVAL.node = nil
		}
	case 172:
		yyVAL.node = yyS[yypt-0].node
	case 173:
		//line sql.y:853
		{
			yyVAL.node = nil
		}
	case 174:
		yyVAL.node = yyS[yypt-0].node
	case 175:
		//line sql.y:857
		{
			yyVAL.node = nil
		}
	case 176:
		yyVAL.node = yyS[yypt-0].node
	case 177:
		//line sql.y:861
		{
			ForceEOF(yylex)
		}
	}
	goto yystack /* stack new state and value */
}
