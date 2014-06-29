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
	byt         byte
	bytes       []byte
	str         string
	distinct    Distinct
	selectExprs SelectExprs
	selectExpr  SelectExpr
	columns     Columns
	colName     *ColName
	tableExprs  TableExprs
	tableExpr   TableExpr
	tableName   *TableName
	indexHints  *IndexHints
	names       [][]byte
	where       *Where
	expr        Expr
	boolExpr    BoolExpr
	valExpr     ValExpr
	valExprs    ValExprs
	values      Values
	subquery    *Subquery
	groupBy     GroupBy
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
const CASE_WHEN = 57423
const WHEN_LIST = 57424
const NO_LOCK = 57425
const FOR_UPDATE = 57426
const LOCK_IN_SHARE_MODE = 57427

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
	"CASE_WHEN",
	"WHEN_LIST",
	"NO_LOCK",
	"FOR_UPDATE",
	"LOCK_IN_SHARE_MODE",
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
}

const yyNprod = 190
const yyPrivate = 57344

var yyTokenNames []string
var yyStates []string

const yyLast = 569

var yyAct = []int{

	83, 80, 75, 275, 339, 308, 149, 52, 187, 234,
	160, 70, 109, 53, 167, 81, 72, 69, 150, 3,
	348, 158, 22, 23, 24, 25, 91, 123, 124, 221,
	175, 86, 55, 54, 66, 60, 90, 74, 63, 96,
	43, 58, 67, 348, 348, 118, 73, 87, 88, 89,
	245, 246, 247, 248, 249, 78, 250, 251, 241, 94,
	108, 273, 118, 214, 37, 32, 38, 34, 350, 116,
	118, 35, 216, 120, 217, 112, 111, 319, 77, 256,
	147, 151, 92, 93, 71, 152, 40, 41, 42, 97,
	318, 349, 347, 300, 317, 292, 294, 296, 59, 62,
	39, 55, 54, 95, 55, 54, 171, 170, 165, 272,
	265, 267, 219, 156, 159, 146, 148, 169, 263, 172,
	215, 122, 193, 171, 293, 105, 147, 147, 197, 185,
	192, 202, 203, 101, 206, 207, 208, 209, 210, 211,
	212, 213, 198, 161, 191, 162, 266, 161, 107, 162,
	223, 161, 61, 162, 204, 314, 218, 136, 137, 138,
	269, 195, 196, 123, 124, 55, 232, 237, 115, 224,
	286, 147, 220, 222, 316, 287, 238, 315, 226, 227,
	233, 225, 230, 134, 135, 136, 137, 138, 290, 239,
	194, 289, 284, 12, 288, 117, 205, 285, 255, 103,
	168, 242, 216, 258, 259, 324, 236, 22, 23, 24,
	25, 168, 303, 190, 90, 257, 191, 96, 104, 262,
	334, 333, 189, 332, 56, 87, 88, 89, 153, 164,
	157, 181, 155, 153, 49, 61, 224, 94, 154, 264,
	118, 86, 12, 274, 243, 271, 90, 65, 254, 96,
	121, 179, 282, 283, 182, 103, 56, 87, 88, 89,
	92, 93, 56, 299, 253, 78, 61, 97, 297, 94,
	191, 191, 295, 190, 55, 304, 345, 279, 305, 306,
	309, 95, 189, 99, 301, 278, 102, 184, 77, 183,
	68, 310, 92, 93, 346, 321, 166, 113, 110, 97,
	161, 320, 162, 178, 180, 177, 106, 50, 64, 100,
	302, 48, 261, 95, 352, 147, 218, 147, 12, 322,
	330, 328, 199, 173, 200, 201, 336, 309, 114, 46,
	337, 98, 338, 340, 340, 340, 55, 54, 341, 342,
	44, 276, 229, 343, 313, 277, 86, 235, 312, 353,
	329, 90, 331, 354, 96, 355, 12, 281, 168, 51,
	351, 73, 87, 88, 89, 326, 327, 26, 335, 12,
	78, 27, 86, 174, 94, 33, 240, 90, 176, 36,
	96, 28, 29, 30, 31, 57, 231, 56, 87, 88,
	89, 163, 268, 77, 344, 325, 78, 92, 93, 71,
	94, 307, 311, 280, 97, 85, 82, 131, 132, 133,
	134, 135, 136, 137, 138, 84, 270, 86, 95, 77,
	79, 228, 90, 92, 93, 96, 12, 13, 14, 15,
	97, 125, 56, 87, 88, 89, 76, 90, 291, 188,
	96, 78, 244, 186, 95, 94, 252, 56, 87, 88,
	89, 119, 45, 21, 47, 16, 153, 11, 10, 9,
	94, 8, 7, 6, 77, 5, 4, 2, 92, 93,
	1, 0, 0, 0, 0, 97, 126, 130, 128, 129,
	0, 0, 0, 92, 93, 0, 0, 0, 0, 95,
	97, 0, 0, 0, 142, 143, 144, 145, 323, 0,
	139, 140, 141, 0, 95, 17, 18, 20, 19, 0,
	0, 0, 0, 131, 132, 133, 134, 135, 136, 137,
	138, 0, 127, 131, 132, 133, 134, 135, 136, 137,
	138, 298, 0, 0, 131, 132, 133, 134, 135, 136,
	137, 138, 260, 0, 0, 131, 132, 133, 134, 135,
	136, 137, 138, 131, 132, 133, 134, 135, 136, 137,
	138, 245, 246, 247, 248, 249, 0, 250, 251,
}
var yyPact = []int{

	422, -1000, -1000, 158, -1000, -1000, -1000, -1000, -1000, -1000,
	-1000, -1000, -1000, -1000, -1000, -1000, -1000, -22, -25, 13,
	-1, 365, 323, -1000, -1000, -1000, 311, -1000, 282, 272,
	351, 227, -51, 10, 200, -1000, 12, 200, -1000, 273,
	-58, 200, -58, -1000, -1000, 326, -1000, 316, 272, 276,
	57, 272, 146, -1000, 173, -1000, 49, 271, 81, 200,
	-1000, -1000, 263, -1000, -15, 262, 308, 104, 200, 187,
	-1000, -1000, 231, 45, 98, 455, -1000, 397, 352, -1000,
	-1000, -1000, 412, 194, 188, -1000, 186, -1000, -1000, -1000,
	-1000, -1000, -1000, -1000, -1000, -1000, -1000, 221, -1000, 185,
	227, 261, 349, 227, 412, 200, -1000, 303, -64, -1000,
	219, -1000, 254, -1000, -1000, 252, -1000, 178, 326, -1000,
	-1000, 200, 117, 397, 397, 412, 184, 301, 412, 412,
	129, 412, 412, 412, 412, 412, 412, 412, 412, -1000,
	-1000, -1000, -1000, -1000, -1000, -1000, -1000, 455, -38, 19,
	-27, 455, -1000, 189, 11, 326, -1000, 365, 68, 72,
	-1000, 397, 397, 314, 227, 202, -1000, 335, 397, -1000,
	485, -1000, -1000, -1000, 103, 200, -1000, -32, -1000, -1000,
	-1000, -1000, -1000, -1000, -1000, -1000, 191, 507, 229, 238,
	3, -1000, -1000, -1000, -1000, -1000, -1000, 485, -1000, 184,
	412, 412, 485, 477, -1000, 287, 112, 112, 112, 84,
	84, -1000, -1000, -1000, -1000, -1000, 412, -1000, 485, -1000,
	17, 326, 9, -1000, -1000, 64, 31, -1000, 96, 184,
	158, 8, -1000, 335, 327, 332, 98, 250, -1000, -1000,
	242, -1000, 347, 178, 178, -1000, -1000, 138, 116, 140,
	137, 134, 33, -1000, 237, -4, 233, -1000, 485, 466,
	412, -1000, 485, -1000, -8, -1000, -1000, 397, -1000, 280,
	159, -1000, -1000, 227, 327, -1000, 412, 412, -1000, -1000,
	337, 331, 507, 91, -1000, 123, -1000, 120, -1000, -1000,
	-1000, -1000, 6, 2, -11, -1000, -1000, -1000, 412, 485,
	-1000, -1000, 264, 184, -1000, -1000, 445, 152, -1000, 339,
	-1000, 335, 397, 412, 397, -1000, -1000, 179, 177, 176,
	485, 362, -1000, 412, 412, -1000, -1000, -1000, 327, 98,
	149, 98, 200, 200, 200, 227, 485, -1000, 260, -9,
	-1000, -10, -33, 146, -1000, 354, 293, -1000, 200, -1000,
	-1000, -1000, 200, -1000, 200, -1000,
}
var yyPgo = []int{

	0, 470, 467, 18, 466, 465, 463, 462, 461, 459,
	458, 457, 367, 454, 453, 452, 17, 11, 451, 446,
	16, 443, 8, 442, 439, 234, 438, 4, 14, 37,
	436, 431, 421, 420, 15, 2, 6, 416, 415, 26,
	406, 1, 405, 21, 10, 403, 402, 9, 401, 5,
	395, 3, 394, 392, 391, 386, 7, 13, 247, 385,
	379, 378, 376, 375, 373, 0, 12, 371,
}
var yyR1 = []int{

	0, 1, 2, 2, 2, 2, 2, 2, 2, 2,
	2, 3, 3, 4, 5, 6, 7, 8, 8, 8,
	9, 9, 9, 10, 11, 11, 11, 67, 12, 13,
	13, 14, 14, 14, 14, 14, 15, 15, 16, 16,
	17, 17, 17, 20, 20, 18, 18, 18, 21, 21,
	22, 22, 22, 22, 19, 19, 19, 23, 23, 23,
	23, 23, 23, 23, 23, 23, 24, 24, 24, 25,
	25, 26, 26, 26, 26, 27, 27, 28, 28, 29,
	29, 29, 29, 29, 30, 30, 30, 30, 30, 30,
	30, 30, 30, 30, 31, 31, 31, 31, 31, 31,
	31, 32, 32, 37, 37, 34, 34, 39, 36, 36,
	35, 35, 35, 35, 35, 35, 35, 35, 35, 35,
	35, 35, 35, 35, 35, 35, 35, 38, 38, 40,
	40, 40, 42, 42, 43, 43, 44, 44, 41, 41,
	33, 33, 33, 33, 45, 45, 46, 46, 47, 47,
	48, 48, 49, 50, 50, 50, 51, 51, 51, 52,
	52, 52, 54, 54, 55, 55, 53, 53, 56, 56,
	57, 58, 58, 59, 59, 60, 60, 61, 61, 61,
	61, 61, 62, 62, 63, 63, 64, 64, 65, 66,
}
var yyR2 = []int{

	0, 1, 1, 1, 1, 1, 1, 1, 1, 1,
	1, 12, 3, 7, 8, 7, 3, 5, 8, 4,
	6, 7, 4, 5, 4, 5, 5, 0, 2, 0,
	2, 1, 2, 1, 1, 1, 0, 1, 1, 3,
	1, 2, 3, 1, 1, 0, 1, 2, 1, 3,
	3, 3, 3, 5, 0, 1, 2, 1, 1, 2,
	3, 2, 3, 2, 2, 2, 1, 3, 1, 1,
	3, 0, 5, 5, 5, 1, 3, 0, 2, 1,
	3, 3, 2, 3, 3, 3, 4, 3, 4, 5,
	6, 3, 4, 2, 1, 1, 1, 1, 1, 1,
	1, 2, 1, 1, 3, 3, 1, 3, 1, 3,
	1, 1, 1, 3, 3, 3, 3, 3, 3, 3,
	3, 2, 3, 4, 5, 4, 1, 1, 1, 1,
	1, 1, 3, 4, 1, 2, 4, 2, 1, 3,
	1, 1, 1, 1, 0, 3, 0, 2, 0, 3,
	1, 3, 2, 0, 1, 1, 0, 2, 4, 0,
	2, 4, 0, 3, 1, 3, 0, 5, 1, 3,
	3, 0, 2, 0, 3, 0, 1, 1, 1, 1,
	1, 1, 0, 1, 0, 1, 0, 2, 1, 0,
}
var yyChk = []int{

	-1000, -1, -2, -3, -4, -5, -6, -7, -8, -9,
	-10, -11, 4, 5, 6, 7, 33, 83, 84, 86,
	85, -14, 49, 50, 51, 52, -12, -67, -12, -12,
	-12, -12, 87, -63, 89, 93, -60, 89, 91, 87,
	87, 88, 89, -3, 17, -15, 18, -13, 29, -25,
	35, 8, -56, -57, -41, -65, 35, -59, 92, 88,
	-65, 35, 87, -65, 35, -58, 92, -65, -58, -16,
	-17, 73, -20, 35, -29, -35, -30, 67, 44, -33,
	-41, -34, -40, -65, -38, -42, 20, 36, 37, 38,
	25, -39, 71, 72, 48, 92, 28, 78, 15, -25,
	33, 76, -25, 53, 45, 76, 35, 67, -65, -66,
	35, -66, 90, 35, 20, 64, -65, 8, 53, -18,
	-65, 19, 76, 65, 66, -31, 21, 67, 23, 24,
	22, 68, 69, 70, 71, 72, 73, 74, 75, 45,
	46, 47, 39, 40, 41, 42, -29, -35, -29, -36,
	-3, -35, -35, 44, 44, 44, -39, 44, -43, -20,
	-44, 79, 81, -54, 44, -56, 35, -28, 9, -57,
	-35, -65, -66, 20, -64, 94, -61, 86, 84, 32,
	85, 12, 35, 35, 35, -66, -21, -22, -24, 44,
	35, -39, -17, -65, 73, -29, -29, -35, -34, 21,
	23, 24, -35, -35, 25, 67, -35, -35, -35, -35,
	-35, -35, -35, -35, 101, 101, 53, 101, -35, 101,
	-16, 18, -16, 82, -44, -43, -20, -20, -32, 28,
	-3, -55, -41, -28, -47, 12, -29, 64, -65, -66,
	-62, 90, -28, 53, -23, 54, 55, 56, 57, 58,
	60, 61, -19, 35, 19, -22, 76, -34, -35, -35,
	65, 25, -35, 101, -16, 101, 82, 80, -53, 64,
	-37, -34, 101, 53, -47, -51, 14, 13, 35, 35,
	-45, 10, -22, -22, 54, 59, 54, 59, 54, 54,
	54, -26, 62, 91, 63, 35, 101, 35, 65, -35,
	101, -20, 30, 53, -41, -51, -35, -48, -49, -35,
	-66, -46, 11, 13, 64, 54, 54, 88, 88, 88,
	-35, 31, -34, 53, 53, -50, 26, 27, -47, -29,
	-36, -29, 44, 44, 44, 6, -35, -49, -51, -27,
	-65, -27, -27, -56, -52, 16, 34, 101, 53, 101,
	101, 6, 21, -65, -65, -65,
}
var yyDef = []int{

	0, -2, 1, 2, 3, 4, 5, 6, 7, 8,
	9, 10, 27, 27, 27, 27, 27, 184, 175, 0,
	0, 0, 31, 33, 34, 35, 36, 29, 0, 0,
	0, 0, 173, 0, 0, 185, 0, 0, 176, 0,
	171, 0, 171, 12, 32, 0, 37, 28, 0, 0,
	69, 0, 16, 168, 0, 138, 188, 0, 0, 0,
	189, 188, 0, 189, 0, 0, 0, 0, 0, 0,
	38, 40, 45, 188, 43, 44, 79, 0, 0, 110,
	111, 112, 0, 138, 0, 126, 0, 140, 141, 142,
	143, 106, 129, 130, 131, 127, 128, 0, 30, 162,
	0, 0, 77, 0, 0, 0, 189, 0, 186, 19,
	0, 22, 0, 24, 172, 0, 189, 0, 0, 41,
	46, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 94,
	95, 96, 97, 98, 99, 100, 82, 0, 0, 0,
	0, 108, 121, 0, 0, 0, 93, 0, 0, 0,
	134, 0, 0, 0, 0, 77, 70, 148, 0, 169,
	170, 139, 17, 174, 0, 0, 189, 182, 177, 178,
	179, 180, 181, 23, 25, 26, 77, 48, 54, 0,
	66, 68, 39, 47, 42, 80, 81, 84, 85, 0,
	0, 0, 87, 0, 91, 0, 113, 114, 115, 116,
	117, 118, 119, 120, 83, 105, 0, 107, 108, 122,
	0, 0, 0, 132, 135, 0, 0, 137, 166, 0,
	102, 0, 164, 148, 156, 0, 78, 0, 187, 20,
	0, 183, 144, 0, 0, 57, 58, 0, 0, 0,
	0, 0, 71, 55, 0, 0, 0, 86, 88, 0,
	0, 92, 109, 123, 0, 125, 133, 0, 13, 0,
	101, 103, 163, 0, 156, 15, 0, 0, 189, 21,
	146, 0, 49, 52, 59, 0, 61, 0, 63, 64,
	65, 50, 0, 0, 0, 56, 51, 67, 0, 89,
	124, 136, 0, 0, 165, 14, 157, 149, 150, 153,
	18, 148, 0, 0, 0, 60, 62, 0, 0, 0,
	90, 0, 104, 0, 0, 152, 154, 155, 156, 147,
	145, 53, 0, 0, 0, 0, 158, 151, 159, 0,
	75, 0, 0, 167, 11, 0, 0, 72, 0, 73,
	74, 160, 0, 76, 0, 161,
}
var yyTok1 = []int{

	1, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 75, 68, 3,
	44, 101, 73, 71, 53, 72, 76, 74, 3, 3,
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
	97, 98, 99, 100,
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
		//line sql.y:132
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
		//line sql.y:149
		{
			yyVAL.statement = &Select{Comments: yyS[yypt-10].comments, Distinct: yyS[yypt-9].distinct, SelectExprs: yyS[yypt-8].selectExprs, From: yyS[yypt-6].tableExprs, Where: yyS[yypt-5].where, GroupBy: yyS[yypt-4].groupBy, Having: yyS[yypt-3].where, OrderBy: yyS[yypt-2].node, Limit: yyS[yypt-1].node, Lock: yyS[yypt-0].node}
		}
	case 12:
		//line sql.y:153
		{
			yyVAL.statement = &Union{Type: yyS[yypt-1].str, Select1: yyS[yypt-2].statement.(SelectStatement), Select2: yyS[yypt-0].statement.(SelectStatement)}
		}
	case 13:
		//line sql.y:159
		{
			yyVAL.statement = &Insert{Comments: yyS[yypt-5].comments, Table: yyS[yypt-3].tableName, Columns: yyS[yypt-2].columns, Rows: yyS[yypt-1].sqlNode, OnDup: yyS[yypt-0].node}
		}
	case 14:
		//line sql.y:165
		{
			yyVAL.statement = &Update{Comments: yyS[yypt-6].comments, Table: yyS[yypt-5].tableName, List: yyS[yypt-3].node, Where: yyS[yypt-2].where, OrderBy: yyS[yypt-1].node, Limit: yyS[yypt-0].node}
		}
	case 15:
		//line sql.y:171
		{
			yyVAL.statement = &Delete{Comments: yyS[yypt-5].comments, Table: yyS[yypt-3].tableName, Where: yyS[yypt-2].where, OrderBy: yyS[yypt-1].node, Limit: yyS[yypt-0].node}
		}
	case 16:
		//line sql.y:177
		{
			yyVAL.statement = &Set{Comments: yyS[yypt-1].comments, Updates: yyS[yypt-0].node}
		}
	case 17:
		//line sql.y:183
		{
			yyVAL.statement = &DDLSimple{Action: CREATE, Table: yyS[yypt-1].node}
		}
	case 18:
		//line sql.y:187
		{
			// Change this to an alter statement
			yyVAL.statement = &DDLSimple{Action: ALTER, Table: yyS[yypt-1].node}
		}
	case 19:
		//line sql.y:192
		{
			yyVAL.statement = &DDLSimple{Action: CREATE, Table: yyS[yypt-1].node}
		}
	case 20:
		//line sql.y:198
		{
			yyVAL.statement = &DDLSimple{Action: ALTER, Table: yyS[yypt-2].node}
		}
	case 21:
		//line sql.y:202
		{
			// Change this to a rename statement
			yyVAL.statement = &Rename{OldName: yyS[yypt-3].node, NewName: yyS[yypt-0].node}
		}
	case 22:
		//line sql.y:207
		{
			yyVAL.statement = &DDLSimple{Action: ALTER, Table: yyS[yypt-1].node}
		}
	case 23:
		//line sql.y:213
		{
			yyVAL.statement = &Rename{OldName: yyS[yypt-2].node, NewName: yyS[yypt-0].node}
		}
	case 24:
		//line sql.y:219
		{
			yyVAL.statement = &DDLSimple{Action: DROP, Table: yyS[yypt-0].node}
		}
	case 25:
		//line sql.y:223
		{
			// Change this to an alter statement
			yyVAL.statement = &DDLSimple{Action: ALTER, Table: yyS[yypt-0].node}
		}
	case 26:
		//line sql.y:228
		{
			yyVAL.statement = &DDLSimple{Action: DROP, Table: yyS[yypt-1].node}
		}
	case 27:
		//line sql.y:233
		{
			SetAllowComments(yylex, true)
		}
	case 28:
		//line sql.y:237
		{
			yyVAL.comments = yyS[yypt-0].comments
			SetAllowComments(yylex, false)
		}
	case 29:
		//line sql.y:243
		{
			yyVAL.comments = nil
		}
	case 30:
		//line sql.y:247
		{
			yyVAL.comments = append(yyVAL.comments, Comment(yyS[yypt-0].node.Value))
		}
	case 31:
		//line sql.y:253
		{
			yyVAL.str = "union"
		}
	case 32:
		//line sql.y:257
		{
			yyVAL.str = "union all"
		}
	case 33:
		//line sql.y:261
		{
			yyVAL.str = "minus"
		}
	case 34:
		//line sql.y:265
		{
			yyVAL.str = "except"
		}
	case 35:
		//line sql.y:269
		{
			yyVAL.str = "intersect"
		}
	case 36:
		//line sql.y:274
		{
			yyVAL.distinct = Distinct(false)
		}
	case 37:
		//line sql.y:278
		{
			yyVAL.distinct = Distinct(true)
		}
	case 38:
		//line sql.y:284
		{
			yyVAL.selectExprs = SelectExprs{yyS[yypt-0].selectExpr}
		}
	case 39:
		//line sql.y:288
		{
			yyVAL.selectExprs = append(yyVAL.selectExprs, yyS[yypt-0].selectExpr)
		}
	case 40:
		//line sql.y:294
		{
			yyVAL.selectExpr = &StarExpr{}
		}
	case 41:
		//line sql.y:298
		{
			yyVAL.selectExpr = &NonStarExpr{Expr: yyS[yypt-1].expr, As: yyS[yypt-0].bytes}
		}
	case 42:
		//line sql.y:302
		{
			yyVAL.selectExpr = &StarExpr{TableName: yyS[yypt-2].node.Value}
		}
	case 43:
		//line sql.y:308
		{
			yyVAL.expr = yyS[yypt-0].boolExpr
		}
	case 44:
		//line sql.y:312
		{
			yyVAL.expr = yyS[yypt-0].valExpr
		}
	case 45:
		//line sql.y:317
		{
			yyVAL.bytes = nil
		}
	case 46:
		//line sql.y:321
		{
			yyVAL.bytes = yyS[yypt-0].node.Value
		}
	case 47:
		//line sql.y:325
		{
			yyVAL.bytes = yyS[yypt-0].node.Value
		}
	case 48:
		//line sql.y:331
		{
			yyVAL.tableExprs = TableExprs{yyS[yypt-0].tableExpr}
		}
	case 49:
		//line sql.y:335
		{
			yyVAL.tableExprs = append(yyVAL.tableExprs, yyS[yypt-0].tableExpr)
		}
	case 50:
		//line sql.y:341
		{
			yyVAL.tableExpr = &AliasedTableExpr{Expr: yyS[yypt-2].sqlNode, As: yyS[yypt-1].bytes, Hints: yyS[yypt-0].indexHints}
		}
	case 51:
		//line sql.y:345
		{
			yyVAL.tableExpr = &ParenTableExpr{Expr: yyS[yypt-1].tableExpr}
		}
	case 52:
		//line sql.y:349
		{
			yyVAL.tableExpr = &JoinTableExpr{LeftExpr: yyS[yypt-2].tableExpr, Join: yyS[yypt-1].str, RightExpr: yyS[yypt-0].tableExpr}
		}
	case 53:
		//line sql.y:353
		{
			yyVAL.tableExpr = &JoinTableExpr{LeftExpr: yyS[yypt-4].tableExpr, Join: yyS[yypt-3].str, RightExpr: yyS[yypt-2].tableExpr, On: yyS[yypt-0].boolExpr}
		}
	case 54:
		//line sql.y:358
		{
			yyVAL.bytes = nil
		}
	case 55:
		//line sql.y:362
		{
			yyVAL.bytes = yyS[yypt-0].node.Value
		}
	case 56:
		//line sql.y:366
		{
			yyVAL.bytes = yyS[yypt-0].node.Value
		}
	case 57:
		//line sql.y:372
		{
			yyVAL.str = "join"
		}
	case 58:
		//line sql.y:376
		{
			yyVAL.str = "straight_join"
		}
	case 59:
		//line sql.y:380
		{
			yyVAL.str = "left join"
		}
	case 60:
		//line sql.y:384
		{
			yyVAL.str = "left join"
		}
	case 61:
		//line sql.y:388
		{
			yyVAL.str = "right join"
		}
	case 62:
		//line sql.y:392
		{
			yyVAL.str = "right join"
		}
	case 63:
		//line sql.y:396
		{
			yyVAL.str = "join"
		}
	case 64:
		//line sql.y:400
		{
			yyVAL.str = "cross join"
		}
	case 65:
		//line sql.y:404
		{
			yyVAL.str = "natural join"
		}
	case 66:
		//line sql.y:410
		{
			yyVAL.sqlNode = &TableName{Name: yyS[yypt-0].node.Value}
		}
	case 67:
		//line sql.y:414
		{
			yyVAL.sqlNode = &TableName{Qualifier: yyS[yypt-2].node.Value, Name: yyS[yypt-0].node.Value}
		}
	case 68:
		//line sql.y:418
		{
			yyVAL.sqlNode = yyS[yypt-0].subquery
		}
	case 69:
		//line sql.y:424
		{
			yyVAL.tableName = &TableName{Name: yyS[yypt-0].node.Value}
		}
	case 70:
		//line sql.y:428
		{
			yyVAL.tableName = &TableName{Qualifier: yyS[yypt-2].node.Value, Name: yyS[yypt-0].node.Value}
		}
	case 71:
		//line sql.y:433
		{
			yyVAL.indexHints = nil
		}
	case 72:
		//line sql.y:437
		{
			yyVAL.indexHints = &IndexHints{Type: "use", Indexes: yyS[yypt-1].names}
		}
	case 73:
		//line sql.y:441
		{
			yyVAL.indexHints = &IndexHints{Type: "ignore", Indexes: yyS[yypt-1].names}
		}
	case 74:
		//line sql.y:445
		{
			yyVAL.indexHints = &IndexHints{Type: "force", Indexes: yyS[yypt-1].names}
		}
	case 75:
		//line sql.y:451
		{
			yyVAL.names = [][]byte{yyS[yypt-0].node.Value}
		}
	case 76:
		//line sql.y:455
		{
			yyVAL.names = append(yyS[yypt-2].names, yyS[yypt-0].node.Value)
		}
	case 77:
		//line sql.y:460
		{
			yyVAL.where = nil
		}
	case 78:
		//line sql.y:464
		{
			yyVAL.where = &Where{Type: "where", Expr: yyS[yypt-0].boolExpr}
		}
	case 79:
		yyVAL.boolExpr = yyS[yypt-0].boolExpr
	case 80:
		//line sql.y:471
		{
			yyVAL.boolExpr = &AndExpr{Left: yyS[yypt-2].boolExpr, Right: yyS[yypt-0].boolExpr}
		}
	case 81:
		//line sql.y:475
		{
			yyVAL.boolExpr = &OrExpr{Left: yyS[yypt-2].boolExpr, Right: yyS[yypt-0].boolExpr}
		}
	case 82:
		//line sql.y:479
		{
			yyVAL.boolExpr = &NotExpr{Expr: yyS[yypt-0].boolExpr}
		}
	case 83:
		//line sql.y:483
		{
			yyVAL.boolExpr = &ParenBoolExpr{Expr: yyS[yypt-1].boolExpr}
		}
	case 84:
		//line sql.y:489
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyS[yypt-2].valExpr, Operator: yyS[yypt-1].str, Right: yyS[yypt-0].valExpr}
		}
	case 85:
		//line sql.y:493
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyS[yypt-2].valExpr, Operator: "in", Right: yyS[yypt-0].valExpr}
		}
	case 86:
		//line sql.y:497
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyS[yypt-3].valExpr, Operator: "not in", Right: yyS[yypt-0].valExpr}
		}
	case 87:
		//line sql.y:501
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyS[yypt-2].valExpr, Operator: "like", Right: yyS[yypt-0].valExpr}
		}
	case 88:
		//line sql.y:505
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyS[yypt-3].valExpr, Operator: "not like", Right: yyS[yypt-0].valExpr}
		}
	case 89:
		//line sql.y:509
		{
			yyVAL.boolExpr = &RangeCond{Left: yyS[yypt-4].valExpr, Operator: "between", From: yyS[yypt-2].valExpr, To: yyS[yypt-0].valExpr}
		}
	case 90:
		//line sql.y:513
		{
			yyVAL.boolExpr = &RangeCond{Left: yyS[yypt-5].valExpr, Operator: "not between", From: yyS[yypt-2].valExpr, To: yyS[yypt-0].valExpr}
		}
	case 91:
		//line sql.y:517
		{
			yyVAL.boolExpr = &NullCheck{Operator: "is null", Expr: yyS[yypt-2].valExpr}
		}
	case 92:
		//line sql.y:521
		{
			yyVAL.boolExpr = &NullCheck{Operator: "is not null", Expr: yyS[yypt-3].valExpr}
		}
	case 93:
		//line sql.y:525
		{
			yyVAL.boolExpr = &ExistsExpr{Subquery: yyS[yypt-0].subquery}
		}
	case 94:
		//line sql.y:531
		{
			yyVAL.str = "="
		}
	case 95:
		//line sql.y:535
		{
			yyVAL.str = "<"
		}
	case 96:
		//line sql.y:539
		{
			yyVAL.str = ">"
		}
	case 97:
		//line sql.y:543
		{
			yyVAL.str = "<="
		}
	case 98:
		//line sql.y:547
		{
			yyVAL.str = ">="
		}
	case 99:
		//line sql.y:551
		{
			yyVAL.str = string(yyS[yypt-0].node.Value)
		}
	case 100:
		//line sql.y:555
		{
			yyVAL.str = "<=>"
		}
	case 101:
		//line sql.y:561
		{
			yyVAL.sqlNode = yyS[yypt-0].values
		}
	case 102:
		//line sql.y:565
		{
			yyVAL.sqlNode = yyS[yypt-0].statement
		}
	case 103:
		//line sql.y:571
		{
			yyVAL.values = Values{yyS[yypt-0].valExpr.(Tuple)}
		}
	case 104:
		//line sql.y:575
		{
			yyVAL.values = append(yyS[yypt-2].values, yyS[yypt-0].valExpr.(Tuple))
		}
	case 105:
		//line sql.y:581
		{
			yyVAL.valExpr = ValueTuple(yyS[yypt-1].valExprs)
		}
	case 106:
		//line sql.y:585
		{
			yyVAL.valExpr = yyS[yypt-0].subquery
		}
	case 107:
		//line sql.y:591
		{
			yyVAL.subquery = &Subquery{yyS[yypt-1].statement.(SelectStatement)}
		}
	case 108:
		//line sql.y:597
		{
			yyVAL.valExprs = ValExprs{yyS[yypt-0].valExpr}
		}
	case 109:
		//line sql.y:601
		{
			yyVAL.valExprs = append(yyS[yypt-2].valExprs, yyS[yypt-0].valExpr)
		}
	case 110:
		//line sql.y:607
		{
			yyVAL.valExpr = yyS[yypt-0].valExpr
		}
	case 111:
		//line sql.y:611
		{
			yyVAL.valExpr = yyS[yypt-0].colName
		}
	case 112:
		//line sql.y:615
		{
			yyVAL.valExpr = yyS[yypt-0].valExpr
		}
	case 113:
		//line sql.y:619
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyS[yypt-2].valExpr, Operator: '&', Right: yyS[yypt-0].valExpr}
		}
	case 114:
		//line sql.y:623
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyS[yypt-2].valExpr, Operator: '|', Right: yyS[yypt-0].valExpr}
		}
	case 115:
		//line sql.y:627
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyS[yypt-2].valExpr, Operator: '^', Right: yyS[yypt-0].valExpr}
		}
	case 116:
		//line sql.y:631
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyS[yypt-2].valExpr, Operator: '+', Right: yyS[yypt-0].valExpr}
		}
	case 117:
		//line sql.y:635
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyS[yypt-2].valExpr, Operator: '-', Right: yyS[yypt-0].valExpr}
		}
	case 118:
		//line sql.y:639
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyS[yypt-2].valExpr, Operator: '*', Right: yyS[yypt-0].valExpr}
		}
	case 119:
		//line sql.y:643
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyS[yypt-2].valExpr, Operator: '/', Right: yyS[yypt-0].valExpr}
		}
	case 120:
		//line sql.y:647
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyS[yypt-2].valExpr, Operator: '%', Right: yyS[yypt-0].valExpr}
		}
	case 121:
		//line sql.y:651
		{
			if num, ok := yyS[yypt-0].valExpr.(NumValue); ok {
				switch yyS[yypt-1].byt {
				case '-':
					yyVAL.valExpr = append(NumValue("-"), num...)
				case '+':
					yyVAL.valExpr = num
				default:
					yyVAL.valExpr = &UnaryExpr{Operator: yyS[yypt-1].byt, Expr: yyS[yypt-0].valExpr}
				}
			} else {
				yyVAL.valExpr = &UnaryExpr{Operator: yyS[yypt-1].byt, Expr: yyS[yypt-0].valExpr}
			}
		}
	case 122:
		//line sql.y:666
		{
			yyVAL.valExpr = &FuncExpr{Name: yyS[yypt-2].node.Value}
		}
	case 123:
		//line sql.y:670
		{
			yyVAL.valExpr = &FuncExpr{Name: yyS[yypt-3].node.Value, Exprs: yyS[yypt-1].selectExprs}
		}
	case 124:
		//line sql.y:674
		{
			yyVAL.valExpr = &FuncExpr{Name: yyS[yypt-4].node.Value, Distinct: true, Exprs: yyS[yypt-1].selectExprs}
		}
	case 125:
		//line sql.y:678
		{
			yyVAL.valExpr = &FuncExpr{Name: yyS[yypt-3].node.Value, Exprs: yyS[yypt-1].selectExprs}
		}
	case 126:
		//line sql.y:682
		{
			c := CaseExpr(*yyS[yypt-0].node)
			yyVAL.valExpr = &c
		}
	case 127:
		yyVAL.node = yyS[yypt-0].node
	case 128:
		yyVAL.node = yyS[yypt-0].node
	case 129:
		//line sql.y:693
		{
			yyVAL.byt = '+'
		}
	case 130:
		//line sql.y:697
		{
			yyVAL.byt = '-'
		}
	case 131:
		//line sql.y:701
		{
			yyVAL.byt = '~'
		}
	case 132:
		//line sql.y:707
		{
			yyVAL.node = NewSimpleParseNode(CASE_WHEN, "case")
			yyVAL.node.Push(yyS[yypt-1].node)
		}
	case 133:
		//line sql.y:712
		{
			yyVAL.node.PushTwo(yyS[yypt-2].expr, yyS[yypt-1].node)
		}
	case 134:
		//line sql.y:718
		{
			yyVAL.node = NewSimpleParseNode(WHEN_LIST, "when_list")
			yyVAL.node.Push(yyS[yypt-0].node)
		}
	case 135:
		//line sql.y:723
		{
			yyVAL.node.Push(yyS[yypt-0].node)
		}
	case 136:
		//line sql.y:729
		{
			yyVAL.node.PushTwo(yyS[yypt-2].expr, yyS[yypt-0].expr)
		}
	case 137:
		//line sql.y:733
		{
			yyVAL.node.Push(yyS[yypt-0].expr)
		}
	case 138:
		//line sql.y:739
		{
			yyVAL.colName = &ColName{Name: yyS[yypt-0].node.Value}
		}
	case 139:
		//line sql.y:743
		{
			yyVAL.colName = &ColName{Qualifier: yyS[yypt-2].node.Value, Name: yyS[yypt-0].node.Value}
		}
	case 140:
		//line sql.y:749
		{
			yyVAL.valExpr = StringValue(yyS[yypt-0].node.Value)
		}
	case 141:
		//line sql.y:753
		{
			yyVAL.valExpr = NumValue(yyS[yypt-0].node.Value)
		}
	case 142:
		//line sql.y:757
		{
			yyVAL.valExpr = ValueArg(yyS[yypt-0].node.Value)
		}
	case 143:
		//line sql.y:761
		{
			yyVAL.valExpr = &NullValue{}
		}
	case 144:
		//line sql.y:766
		{
			yyVAL.groupBy = nil
		}
	case 145:
		//line sql.y:770
		{
			yyVAL.groupBy = GroupBy(yyS[yypt-0].valExprs)
		}
	case 146:
		//line sql.y:775
		{
			yyVAL.where = nil
		}
	case 147:
		//line sql.y:779
		{
			yyVAL.where = &Where{Type: "having", Expr: yyS[yypt-0].boolExpr}
		}
	case 148:
		//line sql.y:784
		{
			yyVAL.node = NewSimpleParseNode(ORDER, "order")
		}
	case 149:
		//line sql.y:788
		{
			yyVAL.node = yyS[yypt-2].node.Push(yyS[yypt-0].node)
		}
	case 150:
		//line sql.y:794
		{
			yyVAL.node = NewSimpleParseNode(NODE_LIST, "node_list")
			yyVAL.node.Push(yyS[yypt-0].node)
		}
	case 151:
		//line sql.y:799
		{
			yyVAL.node = yyS[yypt-2].node.Push(yyS[yypt-0].node)
		}
	case 152:
		//line sql.y:805
		{
			yyVAL.node = yyS[yypt-0].node.Push(yyS[yypt-1].valExpr)
		}
	case 153:
		//line sql.y:810
		{
			yyVAL.node = NewSimpleParseNode(ASC, "asc")
		}
	case 154:
		yyVAL.node = yyS[yypt-0].node
	case 155:
		yyVAL.node = yyS[yypt-0].node
	case 156:
		//line sql.y:817
		{
			yyVAL.node = NewSimpleParseNode(LIMIT, "limit")
		}
	case 157:
		//line sql.y:821
		{
			yyVAL.node = yyS[yypt-1].node.Push(yyS[yypt-0].valExpr)
		}
	case 158:
		//line sql.y:825
		{
			yyVAL.node = yyS[yypt-3].node.PushTwo(yyS[yypt-2].valExpr, yyS[yypt-0].valExpr)
		}
	case 159:
		//line sql.y:830
		{
			yyVAL.node = NewSimpleParseNode(NO_LOCK, "")
		}
	case 160:
		//line sql.y:834
		{
			yyVAL.node = NewSimpleParseNode(FOR_UPDATE, " for update")
		}
	case 161:
		//line sql.y:838
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
	case 162:
		//line sql.y:851
		{
			yyVAL.columns = nil
		}
	case 163:
		//line sql.y:855
		{
			yyVAL.columns = yyS[yypt-1].columns
		}
	case 164:
		//line sql.y:861
		{
			yyVAL.columns = Columns{&NonStarExpr{Expr: yyS[yypt-0].colName}}
		}
	case 165:
		//line sql.y:865
		{
			yyVAL.columns = append(yyVAL.columns, &NonStarExpr{Expr: yyS[yypt-0].colName})
		}
	case 166:
		//line sql.y:870
		{
			yyVAL.node = NewSimpleParseNode(DUPLICATE, "duplicate")
		}
	case 167:
		//line sql.y:874
		{
			yyVAL.node = yyS[yypt-3].node.Push(yyS[yypt-0].node)
		}
	case 168:
		//line sql.y:880
		{
			yyVAL.node = NewSimpleParseNode(NODE_LIST, "node_list")
			yyVAL.node.Push(yyS[yypt-0].node)
		}
	case 169:
		//line sql.y:885
		{
			yyVAL.node = yyS[yypt-2].node.Push(yyS[yypt-0].node)
		}
	case 170:
		//line sql.y:891
		{
			yyVAL.node = yyS[yypt-1].node.PushTwo(yyS[yypt-2].colName, yyS[yypt-0].valExpr)
		}
	case 171:
		//line sql.y:896
		{
			yyVAL.node = nil
		}
	case 172:
		yyVAL.node = yyS[yypt-0].node
	case 173:
		//line sql.y:900
		{
			yyVAL.node = nil
		}
	case 174:
		yyVAL.node = yyS[yypt-0].node
	case 175:
		//line sql.y:904
		{
			yyVAL.node = nil
		}
	case 176:
		yyVAL.node = yyS[yypt-0].node
	case 177:
		yyVAL.node = yyS[yypt-0].node
	case 178:
		yyVAL.node = yyS[yypt-0].node
	case 179:
		yyVAL.node = yyS[yypt-0].node
	case 180:
		yyVAL.node = yyS[yypt-0].node
	case 181:
		yyVAL.node = yyS[yypt-0].node
	case 182:
		//line sql.y:915
		{
			yyVAL.node = nil
		}
	case 183:
		yyVAL.node = yyS[yypt-0].node
	case 184:
		//line sql.y:919
		{
			yyVAL.node = nil
		}
	case 185:
		yyVAL.node = yyS[yypt-0].node
	case 186:
		//line sql.y:923
		{
			yyVAL.node = nil
		}
	case 187:
		yyVAL.node = yyS[yypt-0].node
	case 188:
		//line sql.y:928
		{
			yyVAL.node.LowerCase()
		}
	case 189:
		//line sql.y:933
		{
			ForceEOF(yylex)
		}
	}
	goto yystack /* stack new state and value */
}
