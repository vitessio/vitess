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
const ON = 57398
const AND = 57399
const OR = 57400
const NOT = 57401
const UNARY = 57402
const CASE = 57403
const WHEN = 57404
const THEN = 57405
const ELSE = 57406
const END = 57407
const CREATE = 57408
const ALTER = 57409
const DROP = 57410
const RENAME = 57411
const TABLE = 57412
const INDEX = 57413
const TO = 57414
const IGNORE = 57415
const IF = 57416
const UNIQUE = 57417
const USING = 57418
const NODE_LIST = 57419
const UPLUS = 57420
const UMINUS = 57421
const CASE_WHEN = 57422
const WHEN_LIST = 57423
const SELECT_STAR = 57424
const NO_DISTINCT = 57425
const FUNCTION = 57426
const FOR_UPDATE = 57427
const NOT_FOR_UPDATE = 57428
const NOT_IN = 57429
const NOT_LIKE = 57430
const NOT_BETWEEN = 57431
const IS_NULL = 57432
const IS_NOT_NULL = 57433
const UNION_ALL = 57434
const COMMENT_LIST = 57435
const COLUMN_LIST = 57436
const TABLE_EXPR = 57437

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

const yyNprod = 177
const yyPrivate = 57344

var yyTokenNames []string
var yyStates []string

const yyLast = 565

var yyAct = []int{

	65, 48, 294, 212, 261, 133, 215, 179, 236, 165,
	151, 101, 62, 132, 3, 259, 64, 141, 199, 139,
	147, 59, 226, 227, 228, 229, 230, 100, 231, 232,
	22, 23, 24, 25, 60, 40, 22, 23, 24, 25,
	154, 22, 23, 24, 25, 57, 22, 23, 24, 25,
	105, 106, 32, 49, 259, 253, 36, 34, 222, 52,
	95, 12, 13, 14, 15, 100, 100, 280, 129, 134,
	199, 53, 135, 239, 322, 38, 39, 283, 300, 54,
	277, 37, 105, 106, 128, 131, 282, 92, 255, 146,
	16, 218, 142, 284, 143, 252, 104, 140, 197, 278,
	142, 98, 143, 150, 251, 104, 129, 129, 178, 198,
	186, 184, 185, 258, 188, 189, 190, 191, 192, 193,
	194, 195, 176, 177, 250, 240, 174, 196, 142, 200,
	143, 204, 118, 119, 120, 169, 103, 201, 17, 18,
	20, 19, 238, 149, 116, 117, 118, 119, 120, 129,
	187, 102, 203, 302, 172, 207, 208, 205, 211, 202,
	206, 301, 276, 275, 272, 217, 220, 214, 160, 273,
	274, 226, 227, 228, 229, 230, 233, 231, 232, 237,
	234, 201, 148, 245, 246, 223, 242, 74, 158, 244,
	161, 171, 78, 238, 243, 83, 89, 241, 148, 199,
	249, 130, 75, 76, 77, 90, 270, 309, 168, 289,
	68, 271, 180, 99, 81, 12, 50, 235, 257, 304,
	168, 260, 145, 138, 205, 224, 137, 311, 312, 166,
	213, 290, 67, 281, 268, 269, 79, 80, 157, 159,
	156, 89, 279, 84, 142, 168, 143, 265, 286, 234,
	22, 23, 24, 25, 235, 264, 82, 100, 172, 219,
	170, 163, 162, 292, 295, 291, 287, 113, 114, 115,
	116, 117, 118, 119, 120, 296, 96, 175, 94, 74,
	93, 91, 88, 86, 78, 58, 305, 83, 303, 55,
	46, 87, 306, 63, 75, 76, 77, 307, 288, 129,
	201, 129, 68, 12, 313, 315, 81, 45, 317, 319,
	295, 181, 320, 182, 183, 314, 248, 316, 321, 102,
	323, 152, 97, 43, 67, 41, 74, 210, 79, 80,
	61, 78, 325, 85, 83, 84, 262, 299, 263, 216,
	63, 75, 76, 77, 308, 298, 267, 148, 82, 68,
	47, 326, 26, 81, 318, 12, 27, 12, 113, 114,
	115, 116, 117, 118, 119, 120, 28, 29, 30, 31,
	173, 67, 153, 74, 33, 79, 80, 61, 78, 221,
	155, 83, 84, 35, 51, 56, 144, 130, 75, 76,
	77, 254, 324, 310, 74, 82, 68, 293, 297, 78,
	81, 266, 83, 69, 70, 73, 71, 72, 130, 75,
	76, 77, 256, 209, 107, 66, 12, 68, 67, 167,
	225, 81, 79, 80, 164, 42, 21, 44, 11, 84,
	10, 9, 8, 7, 6, 5, 4, 78, 2, 67,
	83, 1, 82, 79, 80, 0, 130, 75, 76, 77,
	84, 0, 0, 0, 0, 136, 0, 0, 78, 81,
	0, 83, 0, 82, 0, 0, 0, 130, 75, 76,
	77, 0, 0, 0, 0, 0, 136, 0, 0, 0,
	81, 79, 80, 0, 0, 0, 0, 0, 84, 0,
	0, 0, 0, 108, 112, 110, 111, 0, 0, 0,
	0, 82, 79, 80, 0, 0, 0, 0, 0, 84,
	124, 125, 126, 127, 0, 0, 121, 122, 123, 0,
	285, 0, 82, 113, 114, 115, 116, 117, 118, 119,
	120, 0, 0, 0, 0, 0, 0, 109, 113, 114,
	115, 116, 117, 118, 119, 120, 247, 0, 0, 113,
	114, 115, 116, 117, 118, 119, 120, 113, 114, 115,
	116, 117, 118, 119, 120,
}
var yyPact = []int{

	57, -1000, -1000, 202, -1000, -1000, -1000, -1000, -1000, -1000,
	-1000, -1000, -1000, -1000, -1000, -1000, -1000, -33, -32, -4,
	-10, 351, 308, -1000, -1000, -1000, 305, -1000, 278, 256,
	342, 182, -30, -15, -1000, -6, -1000, 255, -44, 251,
	-1000, -1000, 306, -1000, 318, 249, 258, 248, 144, -1000,
	161, 247, 22, 246, 244, -27, 242, 302, 39, 205,
	-1000, -1000, 300, 62, 19, 472, -1000, 374, 353, -1000,
	-1000, 433, 183, -1000, 180, -1000, -1000, -1000, -1000, -1000,
	-1000, -1000, -1000, -1000, 167, -1000, 179, 182, 338, 182,
	374, -1000, 301, -51, 156, 228, -1000, -1000, 227, 186,
	306, 226, -1000, 120, 259, 374, 374, 433, 169, 290,
	433, 433, 85, 433, 433, 433, 433, 433, 433, 433,
	433, -1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, 472,
	53, -13, -2, 18, 472, -1000, 412, 306, 351, 51,
	23, -1000, 374, 374, 299, 196, 189, 327, 374, -1000,
	-1000, -1000, -1000, 29, 225, -1000, -29, -1000, -1000, -1000,
	-1000, -1000, -1000, -1000, 173, 118, 211, 132, -1, -1000,
	-1000, -1000, -1000, -1000, 14, 306, -1000, -1000, 491, -1000,
	412, 169, 433, 433, 491, 483, -1000, 291, 75, 75,
	75, 61, 61, -1000, -1000, -1000, 224, -1000, -1000, 433,
	-1000, 491, 13, -7, -1000, -1000, 15, -23, -1000, 26,
	169, 202, 2, -1000, 327, 322, 325, 19, 221, -1000,
	-1000, 213, -1000, 336, 174, 174, -1000, -1000, 153, 111,
	117, 110, 109, -31, -12, 351, -1000, 208, -19, 199,
	-1000, -25, -34, -18, -1000, 491, 457, 433, -1000, 491,
	-1000, -1000, -1000, 374, -1000, 268, 157, -1000, -1000, 197,
	322, -1000, 433, 433, -1000, -1000, 334, 324, 118, 16,
	-1000, 108, -1000, 100, -1000, -1000, -1000, -1000, -1000, 81,
	176, -1000, -1000, -1000, -1000, 433, 491, -1000, 261, 169,
	-1000, -1000, 292, 155, -1000, 201, -1000, 327, 374, 433,
	374, -1000, -1000, -1000, 196, 491, 348, -1000, 433, 433,
	-1000, -1000, -1000, 322, 19, 147, 19, -37, 182, 491,
	-1000, 316, -1000, 144, -1000, 345, -1000,
}
var yyPgo = []int{

	0, 441, 438, 13, 436, 435, 434, 433, 432, 431,
	430, 428, 352, 427, 426, 425, 21, 34, 12, 11,
	424, 9, 420, 419, 8, 20, 16, 415, 414, 413,
	412, 7, 5, 0, 407, 406, 405, 19, 17, 404,
	403, 401, 398, 6, 397, 2, 393, 4, 392, 391,
	386, 3, 1, 53, 385, 384, 383, 380, 379, 374,
	372, 10, 356,
}
var yyR1 = []int{

	0, 1, 2, 2, 2, 2, 2, 2, 2, 2,
	2, 3, 3, 4, 5, 6, 7, 8, 8, 9,
	9, 10, 11, 11, 62, 12, 13, 13, 14, 14,
	14, 14, 14, 15, 15, 16, 16, 17, 17, 17,
	17, 18, 18, 19, 19, 20, 20, 20, 21, 21,
	21, 21, 22, 22, 22, 22, 22, 22, 22, 22,
	22, 23, 23, 23, 24, 24, 25, 25, 26, 26,
	26, 26, 26, 27, 27, 27, 27, 27, 27, 27,
	27, 27, 27, 28, 28, 28, 28, 28, 28, 28,
	29, 29, 30, 30, 31, 31, 32, 32, 33, 33,
	33, 33, 33, 33, 33, 33, 33, 33, 33, 33,
	33, 33, 33, 33, 33, 33, 34, 34, 35, 35,
	35, 36, 36, 37, 37, 38, 38, 39, 39, 40,
	40, 40, 40, 41, 41, 42, 42, 43, 43, 44,
	44, 45, 46, 46, 46, 47, 47, 47, 48, 48,
	50, 50, 51, 51, 49, 49, 52, 52, 53, 54,
	54, 55, 55, 56, 56, 57, 57, 57, 57, 57,
	58, 58, 59, 59, 60, 60, 61,
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
	1, 3, 4, 1, 2, 4, 2, 1, 3, 1,
	1, 1, 1, 0, 3, 0, 2, 0, 3, 1,
	3, 2, 0, 1, 1, 0, 2, 4, 0, 2,
	0, 3, 1, 3, 0, 5, 1, 3, 3, 0,
	2, 0, 3, 0, 1, 1, 1, 1, 1, 1,
	0, 1, 0, 1, 0, 2, 0,
}
var yyChk = []int{

	-1000, -1, -2, -3, -4, -5, -6, -7, -8, -9,
	-10, -11, 4, 5, 6, 7, 33, 81, 82, 84,
	83, -14, 48, 49, 50, 51, -12, -62, -12, -12,
	-12, -12, 85, -59, 90, -56, 88, 85, 85, 86,
	-3, 17, -15, 18, -13, 29, 34, 8, -52, -53,
	34, -55, 89, 86, 85, 34, -54, 89, 34, -16,
	-17, 71, -18, 34, -26, -33, -27, 65, 43, -40,
	-39, -35, -34, -36, 20, 35, 36, 37, 25, 69,
	70, 47, 89, 28, 76, 15, 34, 33, 34, 52,
	44, 34, 65, 34, 34, 87, 34, 20, 62, 8,
	52, -19, 19, 74, 43, 63, 64, -28, 21, 65,
	23, 24, 22, 66, 67, 68, 69, 70, 71, 72,
	73, 44, 45, 46, 38, 39, 40, 41, -26, -33,
	34, -26, -3, -32, -33, -33, 43, 43, 43, -37,
	-18, -38, 77, 79, -50, 43, -52, -25, 9, -53,
	-18, -61, 20, -60, 91, -57, 84, 82, 32, 83,
	12, 34, 34, 34, -20, -21, 43, -23, 34, -17,
	34, 71, 34, 111, -16, 18, -26, -26, -33, -31,
	43, 21, 23, 24, -33, -33, 25, 65, -33, -33,
	-33, -33, -33, -33, -33, -33, 74, 111, 111, 52,
	111, -33, -16, -3, 80, -38, -37, -18, -18, -29,
	28, -3, -51, 34, -25, -43, 12, -26, 62, 34,
	-61, -58, 87, -25, 52, -22, 53, 54, 55, 56,
	57, 59, 60, -21, -3, 43, -24, -19, 61, 74,
	111, -16, -32, -3, -31, -33, -33, 63, 25, -33,
	111, 111, 80, 78, -49, 62, -30, -31, 111, 52,
	-43, -47, 14, 13, 34, 34, -41, 10, -21, -21,
	53, 58, 53, 58, 53, 53, 53, 111, 111, 34,
	86, 34, 111, 111, 111, 63, -33, -18, 30, 52,
	34, -47, -33, -44, -45, -33, -61, -42, 11, 13,
	62, 53, 53, -24, 43, -33, 31, -31, 52, 52,
	-46, 26, 27, -43, -26, -32, -26, -51, 6, -33,
	-45, -47, 111, -52, -48, 16, 6,
}
var yyDef = []int{

	0, -2, 1, 2, 3, 4, 5, 6, 7, 8,
	9, 10, 24, 24, 24, 24, 24, 172, 163, 0,
	0, 0, 28, 30, 31, 32, 33, 26, 0, 0,
	0, 0, 161, 0, 173, 0, 164, 0, 159, 0,
	12, 29, 0, 34, 25, 0, 0, 0, 16, 156,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	35, 37, -2, 127, 41, 42, 68, 0, 0, 98,
	99, 0, 0, 115, 0, 129, 130, 131, 132, 118,
	119, 120, 116, 117, 0, 27, 150, 0, 66, 0,
	0, 176, 0, 174, 0, 0, 22, 160, 0, 0,
	0, 0, 44, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 83, 84, 85, 86, 87, 88, 89, 71, 0,
	127, 0, 0, 0, 96, 110, 0, 0, 0, 0,
	0, 123, 0, 0, 0, 0, 66, 137, 0, 157,
	158, 17, 162, 0, 0, 176, 170, 165, 166, 167,
	168, 169, 21, 23, 66, 45, 0, -2, 61, 36,
	39, 40, 128, 111, 0, 0, 69, 70, 73, 74,
	0, 0, 0, 0, 76, 0, 80, 0, 102, 103,
	104, 105, 106, 107, 108, 109, 0, 72, 100, 0,
	101, 96, 0, 0, 121, 124, 0, 0, 126, 154,
	0, 91, 0, 152, 137, 145, 0, 67, 0, 175,
	19, 0, 171, 133, 0, 0, 52, 53, 0, 0,
	0, 0, 0, 0, 0, 0, 48, 0, 0, 0,
	112, 0, 0, 0, 75, 77, 0, 0, 81, 97,
	114, 82, 122, 0, 13, 0, 90, 92, 151, 0,
	145, 15, 0, 0, 176, 20, 135, 0, 47, 50,
	54, 0, 56, 0, 58, 59, 60, 46, 63, 64,
	0, 62, 113, 94, 95, 0, 78, 125, 0, 0,
	153, 14, 146, 138, 139, 142, 18, 137, 0, 0,
	0, 55, 57, 49, 0, 79, 0, 93, 0, 0,
	141, 143, 144, 145, 136, 134, 51, 0, 0, 147,
	140, 148, 65, 155, 11, 0, 149,
}
var yyTok1 = []int{

	1, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 73, 66, 3,
	43, 111, 71, 69, 52, 70, 74, 72, 3, 3,
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
		__yyfmt__.Printf("lex %U %s\n", uint(char), yyTokname(c))
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
				__yyfmt__.Printf("saw %s\n", yyTokname(yychar))
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
		//line sql.y:433
		{
			yyVAL.node = NewSimpleParseNode(WHERE, "where")
		}
	case 67:
		//line sql.y:437
		{
			yyVAL.node = yyS[yypt-1].node.Push(yyS[yypt-0].node)
		}
	case 68:
		yyVAL.node = yyS[yypt-0].node
	case 69:
		//line sql.y:444
		{
			yyVAL.node = yyS[yypt-1].node.PushTwo(yyS[yypt-2].node, yyS[yypt-0].node)
		}
	case 70:
		//line sql.y:448
		{
			yyVAL.node = yyS[yypt-1].node.PushTwo(yyS[yypt-2].node, yyS[yypt-0].node)
		}
	case 71:
		//line sql.y:452
		{
			yyVAL.node = yyS[yypt-1].node.Push(yyS[yypt-0].node)
		}
	case 72:
		//line sql.y:456
		{
			yyVAL.node = yyS[yypt-2].node.Push(yyS[yypt-1].node)
		}
	case 73:
		//line sql.y:462
		{
			yyVAL.node = yyS[yypt-1].node.PushTwo(yyS[yypt-2].node, yyS[yypt-0].node)
		}
	case 74:
		//line sql.y:466
		{
			yyVAL.node = yyS[yypt-1].node.PushTwo(yyS[yypt-2].node, yyS[yypt-0].node)
		}
	case 75:
		//line sql.y:470
		{
			yyVAL.node = NewSimpleParseNode(NOT_IN, "not in").PushTwo(yyS[yypt-3].node, yyS[yypt-0].node)
		}
	case 76:
		//line sql.y:474
		{
			yyVAL.node = yyS[yypt-1].node.PushTwo(yyS[yypt-2].node, yyS[yypt-0].node)
		}
	case 77:
		//line sql.y:478
		{
			yyVAL.node = NewSimpleParseNode(NOT_LIKE, "not like").PushTwo(yyS[yypt-3].node, yyS[yypt-0].node)
		}
	case 78:
		//line sql.y:482
		{
			yyVAL.node = yyS[yypt-3].node
			yyVAL.node.Push(yyS[yypt-4].node)
			yyVAL.node.Push(yyS[yypt-2].node)
			yyVAL.node.Push(yyS[yypt-0].node)
		}
	case 79:
		//line sql.y:489
		{
			yyVAL.node = NewSimpleParseNode(NOT_BETWEEN, "not between")
			yyVAL.node.Push(yyS[yypt-5].node)
			yyVAL.node.Push(yyS[yypt-2].node)
			yyVAL.node.Push(yyS[yypt-0].node)
		}
	case 80:
		//line sql.y:496
		{
			yyVAL.node = NewSimpleParseNode(IS_NULL, "is null").Push(yyS[yypt-2].node)
		}
	case 81:
		//line sql.y:500
		{
			yyVAL.node = NewSimpleParseNode(IS_NOT_NULL, "is not null").Push(yyS[yypt-3].node)
		}
	case 82:
		//line sql.y:504
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
		//line sql.y:519
		{
			yyVAL.node = yyS[yypt-1].node.Push(yyS[yypt-0].node)
		}
	case 91:
		yyVAL.node = yyS[yypt-0].node
	case 92:
		//line sql.y:526
		{
			yyVAL.node = NewSimpleParseNode(NODE_LIST, "node_list")
			yyVAL.node.Push(yyS[yypt-0].node)
		}
	case 93:
		//line sql.y:531
		{
			yyVAL.node.Push(yyS[yypt-0].node)
		}
	case 94:
		//line sql.y:537
		{
			yyVAL.node = yyS[yypt-2].node.Push(yyS[yypt-1].node)
		}
	case 95:
		//line sql.y:541
		{
			yyVAL.node = yyS[yypt-2].node.Push(yyS[yypt-1].node)
		}
	case 96:
		//line sql.y:547
		{
			yyVAL.node = NewSimpleParseNode(NODE_LIST, "node_list")
			yyVAL.node.Push(yyS[yypt-0].node)
		}
	case 97:
		//line sql.y:552
		{
			yyVAL.node.Push(yyS[yypt-0].node)
		}
	case 98:
		yyVAL.node = yyS[yypt-0].node
	case 99:
		yyVAL.node = yyS[yypt-0].node
	case 100:
		//line sql.y:560
		{
			yyVAL.node = yyS[yypt-2].node.Push(yyS[yypt-1].node)
		}
	case 101:
		//line sql.y:564
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
		//line sql.y:576
		{
			yyVAL.node = yyS[yypt-1].node.PushTwo(yyS[yypt-2].node, yyS[yypt-0].node)
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
		//line sql.y:624
		{
			yyS[yypt-2].node.Type = FUNCTION
			yyVAL.node = yyS[yypt-2].node.Push(NewSimpleParseNode(NODE_LIST, "node_list"))
		}
	case 112:
		//line sql.y:629
		{
			yyS[yypt-3].node.Type = FUNCTION
			yyVAL.node = yyS[yypt-3].node.Push(yyS[yypt-1].node)
		}
	case 113:
		//line sql.y:634
		{
			yyS[yypt-4].node.Type = FUNCTION
			yyVAL.node = yyS[yypt-4].node.Push(yyS[yypt-2].node)
			yyVAL.node = yyS[yypt-4].node.Push(yyS[yypt-1].node)
		}
	case 114:
		//line sql.y:640
		{
			yyS[yypt-3].node.Type = FUNCTION
			yyVAL.node = yyS[yypt-3].node.Push(yyS[yypt-1].node)
		}
	case 115:
		yyVAL.node = yyS[yypt-0].node
	case 116:
		yyVAL.node = yyS[yypt-0].node
	case 117:
		yyVAL.node = yyS[yypt-0].node
	case 118:
		//line sql.y:652
		{
			yyVAL.node = NewSimpleParseNode(UPLUS, "+")
		}
	case 119:
		//line sql.y:656
		{
			yyVAL.node = NewSimpleParseNode(UMINUS, "-")
		}
	case 120:
		yyVAL.node = yyS[yypt-0].node
	case 121:
		//line sql.y:663
		{
			yyVAL.node = NewSimpleParseNode(CASE_WHEN, "case")
			yyVAL.node.Push(yyS[yypt-1].node)
		}
	case 122:
		//line sql.y:668
		{
			yyVAL.node.PushTwo(yyS[yypt-2].node, yyS[yypt-1].node)
		}
	case 123:
		//line sql.y:674
		{
			yyVAL.node = NewSimpleParseNode(WHEN_LIST, "when_list")
			yyVAL.node.Push(yyS[yypt-0].node)
		}
	case 124:
		//line sql.y:679
		{
			yyVAL.node.Push(yyS[yypt-0].node)
		}
	case 125:
		//line sql.y:685
		{
			yyVAL.node.PushTwo(yyS[yypt-2].node, yyS[yypt-0].node)
		}
	case 126:
		//line sql.y:689
		{
			yyVAL.node.Push(yyS[yypt-0].node)
		}
	case 127:
		yyVAL.node = yyS[yypt-0].node
	case 128:
		//line sql.y:696
		{
			yyVAL.node = yyS[yypt-1].node.PushTwo(yyS[yypt-2].node, yyS[yypt-0].node)
		}
	case 129:
		yyVAL.node = yyS[yypt-0].node
	case 130:
		yyVAL.node = yyS[yypt-0].node
	case 131:
		yyVAL.node = yyS[yypt-0].node
	case 132:
		yyVAL.node = yyS[yypt-0].node
	case 133:
		//line sql.y:707
		{
			yyVAL.node = NewSimpleParseNode(GROUP, "group")
		}
	case 134:
		//line sql.y:711
		{
			yyVAL.node = yyS[yypt-2].node.Push(yyS[yypt-0].node)
		}
	case 135:
		//line sql.y:716
		{
			yyVAL.node = NewSimpleParseNode(HAVING, "having")
		}
	case 136:
		//line sql.y:720
		{
			yyVAL.node = yyS[yypt-1].node.Push(yyS[yypt-0].node)
		}
	case 137:
		//line sql.y:725
		{
			yyVAL.node = NewSimpleParseNode(ORDER, "order")
		}
	case 138:
		//line sql.y:729
		{
			yyVAL.node = yyS[yypt-2].node.Push(yyS[yypt-0].node)
		}
	case 139:
		//line sql.y:735
		{
			yyVAL.node = NewSimpleParseNode(NODE_LIST, "node_list")
			yyVAL.node.Push(yyS[yypt-0].node)
		}
	case 140:
		//line sql.y:740
		{
			yyVAL.node = yyS[yypt-2].node.Push(yyS[yypt-0].node)
		}
	case 141:
		//line sql.y:746
		{
			yyVAL.node = yyS[yypt-0].node.Push(yyS[yypt-1].node)
		}
	case 142:
		//line sql.y:751
		{
			yyVAL.node = NewSimpleParseNode(ASC, "asc")
		}
	case 143:
		yyVAL.node = yyS[yypt-0].node
	case 144:
		yyVAL.node = yyS[yypt-0].node
	case 145:
		//line sql.y:758
		{
			yyVAL.node = NewSimpleParseNode(LIMIT, "limit")
		}
	case 146:
		//line sql.y:762
		{
			yyVAL.node = yyS[yypt-1].node.Push(yyS[yypt-0].node)
		}
	case 147:
		//line sql.y:766
		{
			yyVAL.node = yyS[yypt-3].node.PushTwo(yyS[yypt-2].node, yyS[yypt-0].node)
		}
	case 148:
		//line sql.y:771
		{
			yyVAL.node = NewSimpleParseNode(NOT_FOR_UPDATE, "")
		}
	case 149:
		//line sql.y:775
		{
			yyVAL.node = NewSimpleParseNode(FOR_UPDATE, " for update")
		}
	case 150:
		//line sql.y:780
		{
			yyVAL.node = NewSimpleParseNode(COLUMN_LIST, "")
		}
	case 151:
		//line sql.y:784
		{
			yyVAL.node = yyS[yypt-1].node
		}
	case 152:
		//line sql.y:790
		{
			yyVAL.node = NewSimpleParseNode(COLUMN_LIST, "")
			yyVAL.node.Push(yyS[yypt-0].node)
		}
	case 153:
		//line sql.y:795
		{
			yyVAL.node = yyS[yypt-2].node.Push(yyS[yypt-0].node)
		}
	case 154:
		//line sql.y:800
		{
			yyVAL.node = NewSimpleParseNode(DUPLICATE, "duplicate")
		}
	case 155:
		//line sql.y:804
		{
			yyVAL.node = yyS[yypt-3].node.Push(yyS[yypt-0].node)
		}
	case 156:
		//line sql.y:810
		{
			yyVAL.node = NewSimpleParseNode(NODE_LIST, "node_list")
			yyVAL.node.Push(yyS[yypt-0].node)
		}
	case 157:
		//line sql.y:815
		{
			yyVAL.node = yyS[yypt-2].node.Push(yyS[yypt-0].node)
		}
	case 158:
		//line sql.y:821
		{
			yyVAL.node = yyS[yypt-1].node.PushTwo(yyS[yypt-2].node, yyS[yypt-0].node)
		}
	case 159:
		//line sql.y:826
		{
			yyVAL.node = nil
		}
	case 160:
		yyVAL.node = yyS[yypt-0].node
	case 161:
		//line sql.y:830
		{
			yyVAL.node = nil
		}
	case 162:
		yyVAL.node = yyS[yypt-0].node
	case 163:
		//line sql.y:834
		{
			yyVAL.node = nil
		}
	case 164:
		yyVAL.node = yyS[yypt-0].node
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
		//line sql.y:845
		{
			yyVAL.node = nil
		}
	case 171:
		yyVAL.node = yyS[yypt-0].node
	case 172:
		//line sql.y:849
		{
			yyVAL.node = nil
		}
	case 173:
		yyVAL.node = yyS[yypt-0].node
	case 174:
		//line sql.y:853
		{
			yyVAL.node = nil
		}
	case 175:
		yyVAL.node = yyS[yypt-0].node
	case 176:
		//line sql.y:857
		{
			ForceEOF(yylex)
		}
	}
	goto yystack /* stack new state and value */
}
