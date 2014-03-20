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
	-1, 64,
	34, 43,
	-2, 38,
	-1, 172,
	34, 43,
	-2, 66,
}

const yyNprod = 181
const yyPrivate = 57344

var yyTokenNames []string
var yyStates []string

const yyLast = 564

var yyAct = []int{

	67, 49, 301, 217, 267, 136, 220, 181, 241, 170,
	156, 61, 64, 145, 105, 265, 66, 143, 135, 3,
	108, 109, 74, 152, 231, 232, 233, 234, 235, 265,
	236, 237, 22, 23, 24, 25, 62, 104, 201, 265,
	40, 22, 23, 24, 25, 50, 104, 159, 22, 23,
	24, 25, 59, 36, 51, 12, 13, 14, 15, 54,
	227, 99, 60, 22, 23, 24, 25, 287, 199, 104,
	132, 137, 201, 165, 138, 332, 32, 56, 97, 286,
	259, 34, 55, 283, 16, 37, 131, 134, 245, 331,
	38, 39, 150, 163, 198, 166, 290, 293, 289, 264,
	144, 146, 107, 147, 258, 284, 256, 155, 91, 132,
	132, 180, 257, 51, 186, 187, 51, 190, 191, 192,
	193, 194, 195, 196, 197, 178, 179, 200, 175, 254,
	177, 52, 202, 17, 18, 20, 19, 108, 109, 154,
	203, 174, 96, 188, 162, 164, 161, 146, 307, 147,
	209, 261, 205, 207, 132, 106, 146, 210, 147, 212,
	213, 208, 211, 319, 320, 243, 244, 216, 309, 176,
	222, 225, 218, 223, 219, 119, 120, 121, 122, 123,
	102, 238, 224, 203, 189, 249, 250, 242, 246, 153,
	239, 248, 308, 228, 121, 122, 123, 243, 244, 282,
	281, 247, 253, 280, 116, 117, 118, 119, 120, 121,
	122, 123, 153, 231, 232, 233, 234, 235, 255, 236,
	237, 177, 278, 263, 276, 210, 266, 279, 93, 277,
	103, 206, 229, 77, 22, 23, 24, 25, 81, 274,
	275, 86, 201, 317, 296, 173, 316, 65, 78, 79,
	80, 94, 292, 182, 240, 93, 70, 312, 311, 239,
	84, 116, 117, 118, 119, 120, 121, 122, 123, 299,
	302, 298, 294, 173, 104, 12, 149, 142, 141, 69,
	140, 303, 171, 82, 83, 63, 52, 288, 297, 285,
	87, 46, 313, 271, 310, 116, 117, 118, 119, 120,
	121, 122, 123, 85, 315, 173, 132, 203, 132, 270,
	168, 321, 323, 167, 240, 325, 326, 328, 302, 151,
	329, 100, 322, 98, 324, 204, 330, 95, 77, 333,
	47, 57, 90, 81, 218, 218, 86, 89, 314, 295,
	92, 45, 133, 78, 79, 80, 12, 252, 157, 77,
	51, 70, 101, 106, 81, 84, 183, 86, 184, 185,
	335, 43, 41, 65, 78, 79, 80, 88, 268, 306,
	215, 269, 70, 221, 69, 26, 84, 305, 82, 83,
	273, 12, 153, 48, 336, 87, 146, 327, 147, 28,
	29, 30, 31, 12, 27, 69, 158, 77, 85, 82,
	83, 63, 81, 33, 226, 86, 87, 12, 160, 35,
	53, 133, 78, 79, 80, 58, 148, 260, 77, 85,
	70, 334, 318, 81, 84, 300, 86, 304, 81, 272,
	71, 86, 133, 78, 79, 80, 72, 133, 78, 79,
	80, 70, 76, 69, 73, 84, 139, 82, 83, 75,
	84, 262, 291, 214, 87, 116, 117, 118, 119, 120,
	121, 122, 123, 110, 69, 68, 172, 85, 82, 83,
	230, 169, 42, 82, 83, 87, 21, 44, 11, 10,
	87, 9, 8, 7, 6, 5, 81, 4, 85, 86,
	2, 1, 0, 85, 0, 133, 78, 79, 80, 0,
	0, 0, 0, 0, 139, 0, 0, 0, 84, 0,
	111, 115, 113, 114, 251, 0, 0, 116, 117, 118,
	119, 120, 121, 122, 123, 0, 0, 127, 128, 129,
	130, 82, 83, 124, 125, 126, 0, 0, 87, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 85, 0, 0, 0, 112, 116, 117, 118, 119,
	120, 121, 122, 123,
}
var yyPact = []int{

	51, -1000, -1000, 186, -1000, -1000, -1000, -1000, -1000, -1000,
	-1000, -1000, -1000, -1000, -1000, -1000, -1000, -10, -36, -1,
	4, 389, 345, -1000, -1000, -1000, 343, -1000, 312, 296,
	375, 252, -31, -5, -1000, -9, -1000, 297, -38, 252,
	-1000, -1000, 329, -1000, 352, 296, 299, 33, 296, 176,
	-1000, 207, -1000, 293, 76, 252, 289, -27, 287, 332,
	117, 222, -1000, -1000, 334, 27, 73, 489, -1000, 398,
	377, -1000, -1000, 461, 237, 235, -1000, 234, -1000, -1000,
	-1000, -1000, -1000, -1000, -1000, -1000, -1000, 308, -1000, 233,
	252, 285, 373, 252, 398, -1000, 328, -45, 61, 279,
	-1000, -1000, 276, 239, 329, 252, -1000, 97, 398, 398,
	461, 210, 335, 461, 461, 118, 461, 461, 461, 461,
	461, 461, 461, 461, -1000, -1000, -1000, -1000, -1000, -1000,
	-1000, -1000, 489, 19, -44, 15, 20, 489, -1000, 403,
	213, 329, 389, 69, 78, -1000, 398, 398, 342, 252,
	203, -1000, 361, 398, -1000, -1000, -1000, -1000, 110, 252,
	-1000, -28, -1000, -1000, -1000, -1000, -1000, -1000, -1000, 180,
	160, 271, 136, 13, -1000, -1000, -1000, -1000, -1000, -1000,
	228, -1000, 403, 210, 461, 461, 228, 450, -1000, 322,
	105, 105, 105, 122, 122, -1000, -1000, -1000, 252, -1000,
	-1000, 461, -1000, 228, -1000, 17, 329, -6, 0, -1000,
	-1000, 23, 1, -1000, 88, 210, 186, -13, -1000, 361,
	354, 358, 73, 275, -1000, -1000, 259, -1000, 370, 211,
	211, -1000, -1000, 171, 169, 150, 147, 146, -29, -7,
	389, -1000, 255, -8, -20, 253, -14, -16, -1000, 228,
	388, 461, -1000, 228, -1000, -15, -1000, -1000, -1000, 398,
	-1000, 309, 192, -1000, -1000, 252, 354, -1000, 461, 461,
	-1000, -1000, 366, 356, 160, 85, -1000, 139, -1000, 115,
	-1000, -1000, -1000, -1000, -1000, 104, 215, 214, -1000, -1000,
	-1000, 461, 228, -1000, -1000, 307, 210, -1000, -1000, 194,
	191, -1000, 137, -1000, 361, 398, 461, 398, -1000, -1000,
	-1000, 252, 252, 228, 381, -1000, 461, 461, -1000, -1000,
	-1000, 354, 73, 190, 73, -23, -37, 252, 228, -1000,
	344, -1000, -1000, 176, -1000, 378, -1000,
}
var yyPgo = []int{

	0, 491, 490, 18, 487, 485, 484, 483, 482, 481,
	479, 478, 375, 477, 476, 472, 11, 36, 12, 14,
	471, 9, 470, 466, 291, 8, 23, 16, 465, 463,
	453, 451, 7, 5, 0, 449, 444, 442, 17, 13,
	436, 430, 429, 427, 6, 425, 2, 422, 4, 421,
	417, 416, 3, 1, 45, 415, 410, 409, 408, 404,
	403, 396, 22, 10, 394,
}
var yyR1 = []int{

	0, 1, 2, 2, 2, 2, 2, 2, 2, 2,
	2, 3, 3, 4, 5, 6, 7, 8, 8, 9,
	9, 10, 11, 11, 64, 12, 13, 13, 14, 14,
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
	48, 49, 49, 51, 51, 52, 52, 50, 50, 53,
	53, 54, 55, 55, 56, 56, 57, 57, 58, 58,
	58, 58, 58, 59, 59, 60, 60, 61, 61, 62,
	63,
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
	4, 0, 2, 0, 3, 1, 3, 0, 5, 1,
	3, 3, 0, 2, 0, 3, 0, 1, 1, 1,
	1, 1, 1, 0, 1, 0, 1, 0, 2, 1,
	0,
}
var yyChk = []int{

	-1000, -1, -2, -3, -4, -5, -6, -7, -8, -9,
	-10, -11, 4, 5, 6, 7, 33, 82, 83, 85,
	84, -14, 48, 49, 50, 51, -12, -64, -12, -12,
	-12, -12, 86, -60, 91, -57, 89, 86, 86, 87,
	-3, 17, -15, 18, -13, 29, -24, 34, 8, -53,
	-54, -62, 34, -56, 90, 87, 86, 34, -55, 90,
	-62, -16, -17, 72, -18, 34, -27, -34, -28, 66,
	43, -41, -40, -36, -62, -35, -37, 20, 35, 36,
	37, 25, 70, 71, 47, 90, 28, 77, 15, -24,
	33, 75, -24, 52, 44, 34, 66, -62, 34, 88,
	34, 20, 63, 8, 52, -19, 19, 75, 64, 65,
	-29, 21, 66, 23, 24, 22, 67, 68, 69, 70,
	71, 72, 73, 74, 44, 45, 46, 38, 39, 40,
	41, -27, -34, 34, -27, -3, -33, -34, -34, 43,
	43, 43, 43, -38, -18, -39, 78, 80, -51, 43,
	-53, 34, -26, 9, -54, -18, -63, 20, -61, 92,
	-58, 85, 83, 32, 84, 12, 34, 34, 34, -20,
	-21, 43, -23, 34, -17, -62, 72, -62, -27, -27,
	-34, -32, 43, 21, 23, 24, -34, -34, 25, 66,
	-34, -34, -34, -34, -34, -34, -34, -34, 75, 112,
	112, 52, 112, -34, 112, -16, 18, -16, -3, 81,
	-39, -38, -18, -18, -30, 28, -3, -52, -62, -26,
	-44, 12, -27, 63, -62, -63, -59, 88, -26, 52,
	-22, 53, 54, 55, 56, 57, 59, 60, -21, -3,
	43, -25, -19, 61, 62, 75, -33, -3, -32, -34,
	-34, 64, 25, -34, 112, -16, 112, 112, 81, 79,
	-50, 63, -31, -32, 112, 52, -44, -48, 14, 13,
	34, 34, -42, 10, -21, -21, 53, 58, 53, 58,
	53, 53, 53, 112, 112, 34, 87, 87, 34, 112,
	112, 64, -34, 112, -18, 30, 52, -62, -48, -34,
	-45, -46, -34, -63, -43, 11, 13, 63, 53, 53,
	-25, 43, 43, -34, 31, -32, 52, 52, -47, 26,
	27, -44, -27, -33, -27, -52, -52, 6, -34, -46,
	-48, 112, 112, -53, -49, 16, 6,
}
var yyDef = []int{

	0, -2, 1, 2, 3, 4, 5, 6, 7, 8,
	9, 10, 24, 24, 24, 24, 24, 175, 166, 0,
	0, 0, 28, 30, 31, 32, 33, 26, 0, 0,
	0, 0, 164, 0, 176, 0, 167, 0, 162, 0,
	12, 29, 0, 34, 25, 0, 0, 64, 0, 16,
	159, 0, 179, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 35, 37, -2, 179, 41, 42, 71, 0,
	0, 101, 102, 0, 130, 0, 118, 0, 132, 133,
	134, 135, 121, 122, 123, 119, 120, 0, 27, 153,
	0, 0, 69, 0, 0, 180, 0, 177, 0, 0,
	22, 163, 0, 0, 0, 0, 44, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 86, 87, 88, 89, 90, 91,
	92, 74, 0, 179, 0, 0, 0, 99, 113, 0,
	0, 0, 0, 0, 0, 126, 0, 0, 0, 0,
	69, 65, 140, 0, 160, 161, 17, 165, 0, 0,
	180, 173, 168, 169, 170, 171, 172, 21, 23, 69,
	45, 0, -2, 61, 36, 39, 40, 131, 72, 73,
	76, 77, 0, 0, 0, 0, 79, 0, 83, 0,
	105, 106, 107, 108, 109, 110, 111, 112, 0, 75,
	103, 0, 104, 99, 114, 0, 0, 0, 0, 124,
	127, 0, 0, 129, 157, 0, 94, 0, 155, 140,
	148, 0, 70, 0, 178, 19, 0, 174, 136, 0,
	0, 52, 53, 0, 0, 0, 0, 0, 0, 0,
	0, 48, 0, 0, 0, 0, 0, 0, 78, 80,
	0, 0, 84, 100, 115, 0, 117, 85, 125, 0,
	13, 0, 93, 95, 154, 0, 148, 15, 0, 0,
	180, 20, 138, 0, 47, 50, 54, 0, 56, 0,
	58, 59, 60, 46, 63, 66, 0, 0, 62, 97,
	98, 0, 81, 116, 128, 0, 0, 156, 14, 149,
	141, 142, 145, 18, 140, 0, 0, 0, 55, 57,
	49, 0, 0, 82, 0, 96, 0, 0, 144, 146,
	147, 148, 139, 137, 51, 0, 0, 0, 150, 143,
	151, 67, 68, 158, 11, 0, 152,
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
			yyVAL.node.Push(yyS[yypt-0].node)  // 9: for_update_opt
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
			yyVAL.node.Push(yyS[yypt-3].node) // 1: table_name
			yyVAL.node.Push(yyS[yypt-2].node) // 2: column_list_opt
			yyVAL.node.Push(yyS[yypt-1].node) // 3: values
			yyVAL.node.Push(yyS[yypt-0].node) // 4: on_dup_opt
		}
	case 14:
		//line sql.y:194
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
		//line sql.y:206
		{
			yyVAL.node = yyS[yypt-6].node
			yyVAL.node.Push(yyS[yypt-5].node) // 0: comment_opt
			yyVAL.node.Push(yyS[yypt-3].node) // 1: table_name
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
			yyVAL.node = NewSimpleParseNode(NOT_FOR_UPDATE, "")
		}
	case 152:
		//line sql.y:787
		{
			yyVAL.node = NewSimpleParseNode(FOR_UPDATE, " for update")
		}
	case 153:
		//line sql.y:792
		{
			yyVAL.node = NewSimpleParseNode(COLUMN_LIST, "")
		}
	case 154:
		//line sql.y:796
		{
			yyVAL.node = yyS[yypt-1].node
		}
	case 155:
		//line sql.y:802
		{
			yyVAL.node = NewSimpleParseNode(COLUMN_LIST, "")
			yyVAL.node.Push(yyS[yypt-0].node)
		}
	case 156:
		//line sql.y:807
		{
			yyVAL.node = yyS[yypt-2].node.Push(yyS[yypt-0].node)
		}
	case 157:
		//line sql.y:812
		{
			yyVAL.node = NewSimpleParseNode(DUPLICATE, "duplicate")
		}
	case 158:
		//line sql.y:816
		{
			yyVAL.node = yyS[yypt-3].node.Push(yyS[yypt-0].node)
		}
	case 159:
		//line sql.y:822
		{
			yyVAL.node = NewSimpleParseNode(NODE_LIST, "node_list")
			yyVAL.node.Push(yyS[yypt-0].node)
		}
	case 160:
		//line sql.y:827
		{
			yyVAL.node = yyS[yypt-2].node.Push(yyS[yypt-0].node)
		}
	case 161:
		//line sql.y:833
		{
			yyVAL.node = yyS[yypt-1].node.PushTwo(yyS[yypt-2].node, yyS[yypt-0].node)
		}
	case 162:
		//line sql.y:838
		{
			yyVAL.node = nil
		}
	case 163:
		yyVAL.node = yyS[yypt-0].node
	case 164:
		//line sql.y:842
		{
			yyVAL.node = nil
		}
	case 165:
		yyVAL.node = yyS[yypt-0].node
	case 166:
		//line sql.y:846
		{
			yyVAL.node = nil
		}
	case 167:
		yyVAL.node = yyS[yypt-0].node
	case 168:
		yyVAL.node = yyS[yypt-0].node
	case 169:
		yyVAL.node = yyS[yypt-0].node
	case 170:
		yyVAL.node = yyS[yypt-0].node
	case 171:
		yyVAL.node = yyS[yypt-0].node
	case 172:
		yyVAL.node = yyS[yypt-0].node
	case 173:
		//line sql.y:857
		{
			yyVAL.node = nil
		}
	case 174:
		yyVAL.node = yyS[yypt-0].node
	case 175:
		//line sql.y:861
		{
			yyVAL.node = nil
		}
	case 176:
		yyVAL.node = yyS[yypt-0].node
	case 177:
		//line sql.y:865
		{
			yyVAL.node = nil
		}
	case 178:
		yyVAL.node = yyS[yypt-0].node
	case 179:
		//line sql.y:870
		{
			yyVAL.node.LowerCase()
		}
	case 180:
		//line sql.y:875
		{
			ForceEOF(yylex)
		}
	}
	goto yystack /* stack new state and value */
}
