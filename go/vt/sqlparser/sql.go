//line sql.y:6
package sqlparser

import __yyfmt__ "fmt"

//line sql.y:6
import "bytes"

func SetParseTree(yylex interface{}, stmt Statement) {
	yylex.(*Tokenizer).ParseTree = stmt
}

func SetAllowComments(yylex interface{}, allow bool) {
	yylex.(*Tokenizer).AllowComments = allow
}

func ForceEOF(yylex interface{}) {
	yylex.(*Tokenizer).ForceEOF = true
}

var (
	SHARE        = []byte("share")
	MODE         = []byte("mode")
	IF_BYTES     = []byte("if")
	VALUES_BYTES = []byte("values")
)

//line sql.y:31
type yySymType struct {
	yys         int
	empty       struct{}
	statement   Statement
	selStmt     SelectStatement
	byt         byte
	bytes       []byte
	bytes2      [][]byte
	str         string
	selectExprs SelectExprs
	selectExpr  SelectExpr
	columns     Columns
	colName     *ColName
	tableExprs  TableExprs
	tableExpr   TableExpr
	smTableExpr SimpleTableExpr
	tableName   *TableName
	indexHints  *IndexHints
	expr        Expr
	boolExpr    BoolExpr
	valExpr     ValExpr
	tuple       Tuple
	valExprs    ValExprs
	values      Values
	subquery    *Subquery
	caseExpr    *CaseExpr
	whens       []*When
	when        *When
	orderBy     OrderBy
	order       *Order
	limit       *Limit
	insRows     InsertRows
	updateExprs UpdateExprs
	updateExpr  *UpdateExpr
}

const LEX_ERROR = 57346
const SELECT = 57347
const INSERT = 57348
const UPDATE = 57349
const DELETE = 57350
const FROM = 57351
const WHERE = 57352
const GROUP = 57353
const HAVING = 57354
const ORDER = 57355
const BY = 57356
const LIMIT = 57357
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
const COMMENT = 57381
const LE = 57382
const GE = 57383
const NE = 57384
const NULL_SAFE_EQUAL = 57385
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
const ANALYZE = 57414
const TABLE = 57415
const INDEX = 57416
const VIEW = 57417
const TO = 57418
const IGNORE = 57419
const IF = 57420
const UNIQUE = 57421
const USING = 57422
const SHOW = 57423
const DESCRIBE = 57424
const EXPLAIN = 57425

var yyToknames = []string{
	"LEX_ERROR",
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
	"COMMENT",
	"LE",
	"GE",
	"NE",
	"NULL_SAFE_EQUAL",
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
	"ANALYZE",
	"TABLE",
	"INDEX",
	"VIEW",
	"TO",
	"IGNORE",
	"IF",
	"UNIQUE",
	"USING",
	"SHOW",
	"DESCRIBE",
	"EXPLAIN",
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

const yyNprod = 199
const yyPrivate = 57344

var yyTokenNames []string
var yyStates []string

const yyLast = 567

var yyAct = []int{

	94, 286, 158, 353, 91, 85, 321, 160, 62, 50,
	242, 196, 278, 233, 63, 102, 134, 135, 184, 176,
	92, 76, 80, 81, 161, 3, 253, 254, 255, 256,
	257, 68, 258, 259, 14, 51, 52, 249, 65, 362,
	362, 70, 64, 362, 73, 129, 123, 284, 77, 332,
	223, 129, 53, 129, 101, 331, 225, 107, 86, 28,
	29, 30, 31, 330, 66, 98, 99, 100, 303, 305,
	119, 307, 38, 164, 40, 234, 72, 105, 41, 127,
	120, 264, 69, 122, 131, 364, 363, 49, 133, 361,
	45, 311, 162, 283, 157, 159, 163, 273, 304, 271,
	103, 104, 224, 43, 312, 44, 234, 108, 276, 226,
	116, 170, 65, 167, 112, 65, 64, 180, 179, 64,
	174, 118, 106, 46, 47, 48, 71, 181, 327, 178,
	213, 279, 86, 202, 180, 134, 135, 194, 245, 206,
	204, 205, 211, 212, 200, 215, 216, 217, 218, 219,
	220, 221, 222, 201, 329, 134, 135, 126, 207, 145,
	146, 147, 148, 149, 203, 190, 328, 227, 86, 86,
	314, 114, 214, 65, 65, 297, 301, 64, 240, 300,
	298, 238, 279, 244, 188, 246, 299, 191, 229, 231,
	147, 148, 149, 295, 241, 247, 237, 114, 296, 225,
	338, 230, 316, 97, 115, 348, 177, 59, 101, 347,
	263, 107, 266, 267, 200, 250, 75, 177, 84, 98,
	99, 100, 28, 29, 30, 31, 128, 89, 270, 265,
	199, 105, 346, 86, 164, 168, 187, 189, 186, 198,
	277, 253, 254, 255, 256, 257, 275, 258, 259, 251,
	88, 282, 285, 272, 103, 104, 82, 281, 340, 341,
	114, 108, 172, 293, 294, 78, 110, 200, 200, 113,
	129, 310, 166, 173, 165, 109, 106, 71, 66, 313,
	14, 262, 228, 308, 306, 65, 290, 318, 289, 317,
	319, 322, 14, 15, 16, 17, 193, 261, 132, 323,
	142, 143, 144, 145, 146, 147, 148, 149, 97, 359,
	199, 192, 333, 101, 71, 175, 107, 334, 124, 198,
	18, 121, 111, 84, 98, 99, 100, 360, 335, 227,
	117, 343, 89, 345, 344, 342, 105, 336, 60, 79,
	350, 322, 74, 315, 352, 351, 58, 354, 354, 354,
	65, 355, 356, 14, 64, 88, 269, 366, 357, 103,
	104, 82, 208, 367, 209, 210, 108, 368, 56, 369,
	19, 20, 22, 21, 23, 14, 236, 182, 125, 54,
	287, 106, 326, 24, 25, 26, 288, 243, 325, 292,
	97, 177, 61, 365, 349, 101, 14, 33, 107, 183,
	39, 248, 185, 42, 97, 66, 98, 99, 100, 101,
	67, 239, 107, 171, 89, 358, 339, 32, 105, 66,
	98, 99, 100, 320, 324, 291, 274, 169, 89, 232,
	96, 93, 105, 34, 35, 36, 37, 88, 95, 280,
	90, 103, 104, 235, 136, 87, 302, 101, 108, 197,
	107, 88, 252, 195, 83, 103, 104, 66, 98, 99,
	100, 260, 108, 106, 130, 55, 164, 27, 57, 13,
	105, 12, 11, 10, 9, 8, 7, 106, 6, 5,
	4, 2, 137, 141, 139, 140, 1, 0, 0, 0,
	0, 0, 0, 103, 104, 0, 0, 0, 0, 0,
	108, 153, 154, 155, 156, 337, 150, 151, 152, 0,
	0, 0, 0, 0, 0, 106, 0, 0, 0, 0,
	142, 143, 144, 145, 146, 147, 148, 149, 138, 142,
	143, 144, 145, 146, 147, 148, 149, 309, 0, 0,
	142, 143, 144, 145, 146, 147, 148, 149, 268, 0,
	0, 142, 143, 144, 145, 146, 147, 148, 149, 142,
	143, 144, 145, 146, 147, 148, 149,
}
var yyPact = []int{

	287, -1000, -1000, 173, -1000, -1000, -1000, -1000, -1000, -1000,
	-1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, -16,
	13, 2, 35, -1, -1000, -1000, -1000, 391, 362, -1000,
	-1000, -1000, 350, -1000, 317, 303, 383, 243, -62, -7,
	242, -1000, -12, 242, -1000, 307, -72, 242, -72, 304,
	-1000, -1000, -1000, -1000, -1000, 288, -1000, 236, 303, 289,
	38, 303, 144, -1000, 159, -1000, 34, 295, 54, 242,
	-1000, -1000, 286, -1000, -45, 283, 358, 93, 242, -1000,
	217, -1000, -1000, 279, 12, 70, 461, -1000, 384, 370,
	-1000, -1000, -1000, 422, 230, 228, -1000, 191, -1000, -1000,
	-1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, 422, -1000,
	229, 243, 280, 381, 243, 422, 242, -1000, 357, -77,
	-1000, 152, -1000, 276, -1000, -1000, 261, -1000, 195, 288,
	-1000, -1000, 242, 91, 384, 384, 422, 190, 341, 422,
	422, 105, 422, 422, 422, 422, 422, 422, 422, 422,
	-1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, 461, -49,
	3, 10, 461, -1000, 29, 183, 288, -1000, 391, -4,
	491, 348, 243, 243, 207, -1000, 374, 384, -1000, 491,
	-1000, -1000, -1000, 74, 242, -1000, -54, -1000, -1000, -1000,
	-1000, -1000, -1000, -1000, -1000, 196, 187, 262, 275, 5,
	-1000, -1000, -1000, -1000, -1000, -1000, 491, -1000, 190, 422,
	422, 491, 483, -1000, 331, 88, 88, 88, 117, 117,
	-1000, -1000, -1000, -1000, -1000, 422, -1000, 491, -1000, 0,
	288, -2, 27, -1000, 384, 67, 190, 173, 118, -6,
	-1000, 374, 365, 372, 70, 253, -1000, -1000, 251, -1000,
	378, 195, 195, -1000, -1000, 139, 121, 132, 125, 122,
	6, -1000, 249, -28, 248, -1000, 491, 472, 422, -1000,
	491, -1000, -8, -1000, 22, -1000, 422, 90, -1000, 313,
	149, -1000, -1000, -1000, 243, 365, -1000, 422, 422, -1000,
	-1000, 376, 368, 187, 64, -1000, 112, -1000, 100, -1000,
	-1000, -1000, -1000, -26, -34, -40, -1000, -1000, -1000, 422,
	491, -1000, -1000, 491, 422, 297, 190, -1000, -1000, 452,
	147, -1000, 232, -1000, 374, 384, 422, 384, -1000, -1000,
	188, 165, 161, 491, 491, 387, -1000, 422, 422, -1000,
	-1000, -1000, 365, 70, 146, 70, 242, 242, 242, 243,
	491, -1000, 293, -10, -1000, -13, -14, 144, -1000, 386,
	336, -1000, 242, -1000, -1000, -1000, 242, -1000, 242, -1000,
}
var yyPgo = []int{

	0, 486, 481, 24, 480, 479, 478, 476, 475, 474,
	473, 472, 471, 469, 417, 468, 467, 465, 22, 23,
	464, 461, 454, 453, 11, 452, 449, 207, 446, 3,
	19, 5, 445, 444, 443, 440, 2, 20, 7, 439,
	438, 15, 431, 4, 430, 429, 13, 427, 426, 425,
	424, 10, 423, 6, 416, 1, 415, 413, 411, 12,
	8, 14, 216, 410, 403, 402, 401, 400, 399, 0,
	9, 397,
}
var yyR1 = []int{

	0, 1, 2, 2, 2, 2, 2, 2, 2, 2,
	2, 2, 2, 3, 3, 4, 4, 5, 6, 7,
	8, 8, 8, 9, 9, 9, 10, 11, 11, 11,
	12, 13, 13, 13, 71, 14, 15, 15, 16, 16,
	16, 16, 16, 17, 17, 18, 18, 19, 19, 19,
	22, 22, 20, 20, 20, 23, 23, 24, 24, 24,
	24, 21, 21, 21, 25, 25, 25, 25, 25, 25,
	25, 25, 25, 26, 26, 26, 27, 27, 28, 28,
	28, 28, 29, 29, 30, 30, 31, 31, 31, 31,
	31, 32, 32, 32, 32, 32, 32, 32, 32, 32,
	32, 33, 33, 33, 33, 33, 33, 33, 34, 34,
	39, 39, 37, 37, 41, 38, 38, 36, 36, 36,
	36, 36, 36, 36, 36, 36, 36, 36, 36, 36,
	36, 36, 36, 36, 40, 40, 42, 42, 42, 44,
	47, 47, 45, 45, 46, 48, 48, 43, 43, 35,
	35, 35, 35, 49, 49, 50, 50, 51, 51, 52,
	52, 53, 54, 54, 54, 55, 55, 55, 56, 56,
	56, 57, 57, 58, 58, 59, 59, 60, 60, 61,
	62, 62, 63, 63, 64, 64, 65, 65, 65, 65,
	65, 66, 66, 67, 67, 68, 68, 69, 70,
}
var yyR2 = []int{

	0, 1, 1, 1, 1, 1, 1, 1, 1, 1,
	1, 1, 1, 12, 3, 7, 7, 8, 7, 3,
	5, 8, 4, 6, 7, 4, 5, 4, 5, 5,
	3, 2, 2, 2, 0, 2, 0, 2, 1, 2,
	1, 1, 1, 0, 1, 1, 3, 1, 2, 3,
	1, 1, 0, 1, 2, 1, 3, 3, 3, 3,
	5, 0, 1, 2, 1, 1, 2, 3, 2, 3,
	2, 2, 2, 1, 3, 1, 1, 3, 0, 5,
	5, 5, 1, 3, 0, 2, 1, 3, 3, 2,
	3, 3, 3, 4, 3, 4, 5, 6, 3, 4,
	2, 1, 1, 1, 1, 1, 1, 1, 2, 1,
	1, 3, 3, 1, 3, 1, 3, 1, 1, 1,
	3, 3, 3, 3, 3, 3, 3, 3, 2, 3,
	4, 5, 4, 1, 1, 1, 1, 1, 1, 5,
	0, 1, 1, 2, 4, 0, 2, 1, 3, 1,
	1, 1, 1, 0, 3, 0, 2, 0, 3, 1,
	3, 2, 0, 1, 1, 0, 2, 4, 0, 2,
	4, 0, 3, 1, 3, 0, 5, 1, 3, 3,
	0, 2, 0, 3, 0, 1, 1, 1, 1, 1,
	1, 0, 1, 0, 1, 0, 2, 1, 0,
}
var yyChk = []int{

	-1000, -1, -2, -3, -4, -5, -6, -7, -8, -9,
	-10, -11, -12, -13, 5, 6, 7, 8, 33, 83,
	84, 86, 85, 87, 96, 97, 98, -16, 49, 50,
	51, 52, -14, -71, -14, -14, -14, -14, 88, -67,
	90, 94, -64, 90, 92, 88, 88, 89, 90, 88,
	-70, -70, -70, -3, 17, -17, 18, -15, 29, -27,
	35, 9, -60, -61, -43, -69, 35, -63, 93, 89,
	-69, 35, 88, -69, 35, -62, 93, -69, -62, 35,
	-18, -19, 73, -22, 35, -31, -36, -32, 67, 44,
	-35, -43, -37, -42, -69, -40, -44, 20, 36, 37,
	38, 25, -41, 71, 72, 48, 93, 28, 78, 39,
	-27, 33, 76, -27, 53, 45, 76, 35, 67, -69,
	-70, 35, -70, 91, 35, 20, 64, -69, 9, 53,
	-20, -69, 19, 76, 65, 66, -33, 21, 67, 23,
	24, 22, 68, 69, 70, 71, 72, 73, 74, 75,
	45, 46, 47, 40, 41, 42, 43, -31, -36, -31,
	-38, -3, -36, -36, 44, 44, 44, -41, 44, -47,
	-36, -57, 33, 44, -60, 35, -30, 10, -61, -36,
	-69, -70, 20, -68, 95, -65, 86, 84, 32, 85,
	13, 35, 35, 35, -70, -23, -24, -26, 44, 35,
	-41, -19, -69, 73, -31, -31, -36, -37, 21, 23,
	24, -36, -36, 25, 67, -36, -36, -36, -36, -36,
	-36, -36, -36, 99, 99, 53, 99, -36, 99, -18,
	18, -18, -45, -46, 79, -34, 28, -3, -60, -58,
	-43, -30, -51, 13, -31, 64, -69, -70, -66, 91,
	-30, 53, -25, 54, 55, 56, 57, 58, 60, 61,
	-21, 35, 19, -24, 76, -37, -36, -36, 65, 25,
	-36, 99, -18, 99, -48, -46, 81, -31, -59, 64,
	-39, -37, -59, 99, 53, -51, -55, 15, 14, 35,
	35, -49, 11, -24, -24, 54, 59, 54, 59, 54,
	54, 54, -28, 62, 92, 63, 35, 99, 35, 65,
	-36, 99, 82, -36, 80, 30, 53, -43, -55, -36,
	-52, -53, -36, -70, -50, 12, 14, 64, 54, 54,
	89, 89, 89, -36, -36, 31, -37, 53, 53, -54,
	26, 27, -51, -31, -38, -31, 44, 44, 44, 7,
	-36, -53, -55, -29, -69, -29, -29, -60, -56, 16,
	34, 99, 53, 99, 99, 7, 21, -69, -69, -69,
}
var yyDef = []int{

	0, -2, 1, 2, 3, 4, 5, 6, 7, 8,
	9, 10, 11, 12, 34, 34, 34, 34, 34, 193,
	184, 0, 0, 0, 198, 198, 198, 0, 38, 40,
	41, 42, 43, 36, 0, 0, 0, 0, 182, 0,
	0, 194, 0, 0, 185, 0, 180, 0, 180, 0,
	31, 32, 33, 14, 39, 0, 44, 35, 0, 0,
	76, 0, 19, 177, 0, 147, 197, 0, 0, 0,
	198, 197, 0, 198, 0, 0, 0, 0, 0, 30,
	0, 45, 47, 52, 197, 50, 51, 86, 0, 0,
	117, 118, 119, 0, 147, 0, 133, 0, 149, 150,
	151, 152, 113, 136, 137, 138, 134, 135, 140, 37,
	171, 0, 0, 84, 0, 0, 0, 198, 0, 195,
	22, 0, 25, 0, 27, 181, 0, 198, 0, 0,
	48, 53, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	101, 102, 103, 104, 105, 106, 107, 89, 0, 0,
	0, 0, 115, 128, 0, 0, 0, 100, 0, 0,
	141, 0, 0, 0, 84, 77, 157, 0, 178, 179,
	148, 20, 183, 0, 0, 198, 191, 186, 187, 188,
	189, 190, 26, 28, 29, 84, 55, 61, 0, 73,
	75, 46, 54, 49, 87, 88, 91, 92, 0, 0,
	0, 94, 0, 98, 0, 120, 121, 122, 123, 124,
	125, 126, 127, 90, 112, 0, 114, 115, 129, 0,
	0, 0, 145, 142, 0, 175, 0, 109, 175, 0,
	173, 157, 165, 0, 85, 0, 196, 23, 0, 192,
	153, 0, 0, 64, 65, 0, 0, 0, 0, 0,
	78, 62, 0, 0, 0, 93, 95, 0, 0, 99,
	116, 130, 0, 132, 0, 143, 0, 0, 15, 0,
	108, 110, 16, 172, 0, 165, 18, 0, 0, 198,
	24, 155, 0, 56, 59, 66, 0, 68, 0, 70,
	71, 72, 57, 0, 0, 0, 63, 58, 74, 0,
	96, 131, 139, 146, 0, 0, 0, 174, 17, 166,
	158, 159, 162, 21, 157, 0, 0, 0, 67, 69,
	0, 0, 0, 97, 144, 0, 111, 0, 0, 161,
	163, 164, 165, 156, 154, 60, 0, 0, 0, 0,
	167, 160, 168, 0, 82, 0, 0, 176, 13, 0,
	0, 79, 0, 80, 81, 169, 0, 83, 0, 170,
}
var yyTok1 = []int{

	1, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 75, 68, 3,
	44, 99, 73, 71, 53, 72, 76, 74, 3, 3,
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
	97, 98,
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
		//line sql.y:148
		{
			SetParseTree(yylex, yyS[yypt-0].statement)
		}
	case 2:
		//line sql.y:154
		{
			yyVAL.statement = yyS[yypt-0].selStmt
		}
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
		yyVAL.statement = yyS[yypt-0].statement
	case 12:
		yyVAL.statement = yyS[yypt-0].statement
	case 13:
		//line sql.y:170
		{
			yyVAL.selStmt = &Select{Comments: Comments(yyS[yypt-10].bytes2), Distinct: yyS[yypt-9].str, SelectExprs: yyS[yypt-8].selectExprs, From: yyS[yypt-6].tableExprs, Where: NewWhere(AST_WHERE, yyS[yypt-5].boolExpr), GroupBy: GroupBy(yyS[yypt-4].valExprs), Having: NewWhere(AST_HAVING, yyS[yypt-3].boolExpr), OrderBy: yyS[yypt-2].orderBy, Limit: yyS[yypt-1].limit, Lock: yyS[yypt-0].str}
		}
	case 14:
		//line sql.y:174
		{
			yyVAL.selStmt = &Union{Type: yyS[yypt-1].str, Left: yyS[yypt-2].selStmt, Right: yyS[yypt-0].selStmt}
		}
	case 15:
		//line sql.y:180
		{
			yyVAL.statement = &Insert{Comments: Comments(yyS[yypt-5].bytes2), Table: yyS[yypt-3].tableName, Columns: yyS[yypt-2].columns, Rows: yyS[yypt-1].insRows, OnDup: OnDup(yyS[yypt-0].updateExprs)}
		}
	case 16:
		//line sql.y:184
		{
			cols := make(Columns, 0, len(yyS[yypt-1].updateExprs))
			vals := make(ValTuple, 0, len(yyS[yypt-1].updateExprs))
			for _, col := range yyS[yypt-1].updateExprs {
				cols = append(cols, &NonStarExpr{Expr: col.Name})
				vals = append(vals, col.Expr)
			}
			yyVAL.statement = &Insert{Comments: Comments(yyS[yypt-5].bytes2), Table: yyS[yypt-3].tableName, Columns: cols, Rows: Values{vals}, OnDup: OnDup(yyS[yypt-0].updateExprs)}
		}
	case 17:
		//line sql.y:196
		{
			yyVAL.statement = &Update{Comments: Comments(yyS[yypt-6].bytes2), Table: yyS[yypt-5].tableName, Exprs: yyS[yypt-3].updateExprs, Where: NewWhere(AST_WHERE, yyS[yypt-2].boolExpr), OrderBy: yyS[yypt-1].orderBy, Limit: yyS[yypt-0].limit}
		}
	case 18:
		//line sql.y:202
		{
			yyVAL.statement = &Delete{Comments: Comments(yyS[yypt-5].bytes2), Table: yyS[yypt-3].tableName, Where: NewWhere(AST_WHERE, yyS[yypt-2].boolExpr), OrderBy: yyS[yypt-1].orderBy, Limit: yyS[yypt-0].limit}
		}
	case 19:
		//line sql.y:208
		{
			yyVAL.statement = &Set{Comments: Comments(yyS[yypt-1].bytes2), Exprs: yyS[yypt-0].updateExprs}
		}
	case 20:
		//line sql.y:214
		{
			yyVAL.statement = &DDL{Action: AST_CREATE, NewName: yyS[yypt-1].bytes}
		}
	case 21:
		//line sql.y:218
		{
			// Change this to an alter statement
			yyVAL.statement = &DDL{Action: AST_ALTER, Table: yyS[yypt-1].bytes, NewName: yyS[yypt-1].bytes}
		}
	case 22:
		//line sql.y:223
		{
			yyVAL.statement = &DDL{Action: AST_CREATE, NewName: yyS[yypt-1].bytes}
		}
	case 23:
		//line sql.y:229
		{
			yyVAL.statement = &DDL{Action: AST_ALTER, Table: yyS[yypt-2].bytes, NewName: yyS[yypt-2].bytes}
		}
	case 24:
		//line sql.y:233
		{
			// Change this to a rename statement
			yyVAL.statement = &DDL{Action: AST_RENAME, Table: yyS[yypt-3].bytes, NewName: yyS[yypt-0].bytes}
		}
	case 25:
		//line sql.y:238
		{
			yyVAL.statement = &DDL{Action: AST_ALTER, Table: yyS[yypt-1].bytes, NewName: yyS[yypt-1].bytes}
		}
	case 26:
		//line sql.y:244
		{
			yyVAL.statement = &DDL{Action: AST_RENAME, Table: yyS[yypt-2].bytes, NewName: yyS[yypt-0].bytes}
		}
	case 27:
		//line sql.y:250
		{
			yyVAL.statement = &DDL{Action: AST_DROP, Table: yyS[yypt-0].bytes}
		}
	case 28:
		//line sql.y:254
		{
			// Change this to an alter statement
			yyVAL.statement = &DDL{Action: AST_ALTER, Table: yyS[yypt-0].bytes, NewName: yyS[yypt-0].bytes}
		}
	case 29:
		//line sql.y:259
		{
			yyVAL.statement = &DDL{Action: AST_DROP, Table: yyS[yypt-1].bytes}
		}
	case 30:
		//line sql.y:265
		{
			yyVAL.statement = &DDL{Action: AST_ALTER, Table: yyS[yypt-0].bytes, NewName: yyS[yypt-0].bytes}
		}
	case 31:
		//line sql.y:271
		{
			yyVAL.statement = &Other{}
		}
	case 32:
		//line sql.y:275
		{
			yyVAL.statement = &Other{}
		}
	case 33:
		//line sql.y:279
		{
			yyVAL.statement = &Other{}
		}
	case 34:
		//line sql.y:284
		{
			SetAllowComments(yylex, true)
		}
	case 35:
		//line sql.y:288
		{
			yyVAL.bytes2 = yyS[yypt-0].bytes2
			SetAllowComments(yylex, false)
		}
	case 36:
		//line sql.y:294
		{
			yyVAL.bytes2 = nil
		}
	case 37:
		//line sql.y:298
		{
			yyVAL.bytes2 = append(yyS[yypt-1].bytes2, yyS[yypt-0].bytes)
		}
	case 38:
		//line sql.y:304
		{
			yyVAL.str = AST_UNION
		}
	case 39:
		//line sql.y:308
		{
			yyVAL.str = AST_UNION_ALL
		}
	case 40:
		//line sql.y:312
		{
			yyVAL.str = AST_SET_MINUS
		}
	case 41:
		//line sql.y:316
		{
			yyVAL.str = AST_EXCEPT
		}
	case 42:
		//line sql.y:320
		{
			yyVAL.str = AST_INTERSECT
		}
	case 43:
		//line sql.y:325
		{
			yyVAL.str = ""
		}
	case 44:
		//line sql.y:329
		{
			yyVAL.str = AST_DISTINCT
		}
	case 45:
		//line sql.y:335
		{
			yyVAL.selectExprs = SelectExprs{yyS[yypt-0].selectExpr}
		}
	case 46:
		//line sql.y:339
		{
			yyVAL.selectExprs = append(yyVAL.selectExprs, yyS[yypt-0].selectExpr)
		}
	case 47:
		//line sql.y:345
		{
			yyVAL.selectExpr = &StarExpr{}
		}
	case 48:
		//line sql.y:349
		{
			yyVAL.selectExpr = &NonStarExpr{Expr: yyS[yypt-1].expr, As: yyS[yypt-0].bytes}
		}
	case 49:
		//line sql.y:353
		{
			yyVAL.selectExpr = &StarExpr{TableName: yyS[yypt-2].bytes}
		}
	case 50:
		//line sql.y:359
		{
			yyVAL.expr = yyS[yypt-0].boolExpr
		}
	case 51:
		//line sql.y:363
		{
			yyVAL.expr = yyS[yypt-0].valExpr
		}
	case 52:
		//line sql.y:368
		{
			yyVAL.bytes = nil
		}
	case 53:
		//line sql.y:372
		{
			yyVAL.bytes = yyS[yypt-0].bytes
		}
	case 54:
		//line sql.y:376
		{
			yyVAL.bytes = yyS[yypt-0].bytes
		}
	case 55:
		//line sql.y:382
		{
			yyVAL.tableExprs = TableExprs{yyS[yypt-0].tableExpr}
		}
	case 56:
		//line sql.y:386
		{
			yyVAL.tableExprs = append(yyVAL.tableExprs, yyS[yypt-0].tableExpr)
		}
	case 57:
		//line sql.y:392
		{
			yyVAL.tableExpr = &AliasedTableExpr{Expr: yyS[yypt-2].smTableExpr, As: yyS[yypt-1].bytes, Hints: yyS[yypt-0].indexHints}
		}
	case 58:
		//line sql.y:396
		{
			yyVAL.tableExpr = &ParenTableExpr{Expr: yyS[yypt-1].tableExpr}
		}
	case 59:
		//line sql.y:400
		{
			yyVAL.tableExpr = &JoinTableExpr{LeftExpr: yyS[yypt-2].tableExpr, Join: yyS[yypt-1].str, RightExpr: yyS[yypt-0].tableExpr}
		}
	case 60:
		//line sql.y:404
		{
			yyVAL.tableExpr = &JoinTableExpr{LeftExpr: yyS[yypt-4].tableExpr, Join: yyS[yypt-3].str, RightExpr: yyS[yypt-2].tableExpr, On: yyS[yypt-0].boolExpr}
		}
	case 61:
		//line sql.y:409
		{
			yyVAL.bytes = nil
		}
	case 62:
		//line sql.y:413
		{
			yyVAL.bytes = yyS[yypt-0].bytes
		}
	case 63:
		//line sql.y:417
		{
			yyVAL.bytes = yyS[yypt-0].bytes
		}
	case 64:
		//line sql.y:423
		{
			yyVAL.str = AST_JOIN
		}
	case 65:
		//line sql.y:427
		{
			yyVAL.str = AST_STRAIGHT_JOIN
		}
	case 66:
		//line sql.y:431
		{
			yyVAL.str = AST_LEFT_JOIN
		}
	case 67:
		//line sql.y:435
		{
			yyVAL.str = AST_LEFT_JOIN
		}
	case 68:
		//line sql.y:439
		{
			yyVAL.str = AST_RIGHT_JOIN
		}
	case 69:
		//line sql.y:443
		{
			yyVAL.str = AST_RIGHT_JOIN
		}
	case 70:
		//line sql.y:447
		{
			yyVAL.str = AST_JOIN
		}
	case 71:
		//line sql.y:451
		{
			yyVAL.str = AST_CROSS_JOIN
		}
	case 72:
		//line sql.y:455
		{
			yyVAL.str = AST_NATURAL_JOIN
		}
	case 73:
		//line sql.y:461
		{
			yyVAL.smTableExpr = &TableName{Name: yyS[yypt-0].bytes}
		}
	case 74:
		//line sql.y:465
		{
			yyVAL.smTableExpr = &TableName{Qualifier: yyS[yypt-2].bytes, Name: yyS[yypt-0].bytes}
		}
	case 75:
		//line sql.y:469
		{
			yyVAL.smTableExpr = yyS[yypt-0].subquery
		}
	case 76:
		//line sql.y:475
		{
			yyVAL.tableName = &TableName{Name: yyS[yypt-0].bytes}
		}
	case 77:
		//line sql.y:479
		{
			yyVAL.tableName = &TableName{Qualifier: yyS[yypt-2].bytes, Name: yyS[yypt-0].bytes}
		}
	case 78:
		//line sql.y:484
		{
			yyVAL.indexHints = nil
		}
	case 79:
		//line sql.y:488
		{
			yyVAL.indexHints = &IndexHints{Type: AST_USE, Indexes: yyS[yypt-1].bytes2}
		}
	case 80:
		//line sql.y:492
		{
			yyVAL.indexHints = &IndexHints{Type: AST_IGNORE, Indexes: yyS[yypt-1].bytes2}
		}
	case 81:
		//line sql.y:496
		{
			yyVAL.indexHints = &IndexHints{Type: AST_FORCE, Indexes: yyS[yypt-1].bytes2}
		}
	case 82:
		//line sql.y:502
		{
			yyVAL.bytes2 = [][]byte{yyS[yypt-0].bytes}
		}
	case 83:
		//line sql.y:506
		{
			yyVAL.bytes2 = append(yyS[yypt-2].bytes2, yyS[yypt-0].bytes)
		}
	case 84:
		//line sql.y:511
		{
			yyVAL.boolExpr = nil
		}
	case 85:
		//line sql.y:515
		{
			yyVAL.boolExpr = yyS[yypt-0].boolExpr
		}
	case 86:
		yyVAL.boolExpr = yyS[yypt-0].boolExpr
	case 87:
		//line sql.y:522
		{
			yyVAL.boolExpr = &AndExpr{Left: yyS[yypt-2].boolExpr, Right: yyS[yypt-0].boolExpr}
		}
	case 88:
		//line sql.y:526
		{
			yyVAL.boolExpr = &OrExpr{Left: yyS[yypt-2].boolExpr, Right: yyS[yypt-0].boolExpr}
		}
	case 89:
		//line sql.y:530
		{
			yyVAL.boolExpr = &NotExpr{Expr: yyS[yypt-0].boolExpr}
		}
	case 90:
		//line sql.y:534
		{
			yyVAL.boolExpr = &ParenBoolExpr{Expr: yyS[yypt-1].boolExpr}
		}
	case 91:
		//line sql.y:540
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyS[yypt-2].valExpr, Operator: yyS[yypt-1].str, Right: yyS[yypt-0].valExpr}
		}
	case 92:
		//line sql.y:544
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyS[yypt-2].valExpr, Operator: AST_IN, Right: yyS[yypt-0].tuple}
		}
	case 93:
		//line sql.y:548
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyS[yypt-3].valExpr, Operator: AST_NOT_IN, Right: yyS[yypt-0].tuple}
		}
	case 94:
		//line sql.y:552
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyS[yypt-2].valExpr, Operator: AST_LIKE, Right: yyS[yypt-0].valExpr}
		}
	case 95:
		//line sql.y:556
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyS[yypt-3].valExpr, Operator: AST_NOT_LIKE, Right: yyS[yypt-0].valExpr}
		}
	case 96:
		//line sql.y:560
		{
			yyVAL.boolExpr = &RangeCond{Left: yyS[yypt-4].valExpr, Operator: AST_BETWEEN, From: yyS[yypt-2].valExpr, To: yyS[yypt-0].valExpr}
		}
	case 97:
		//line sql.y:564
		{
			yyVAL.boolExpr = &RangeCond{Left: yyS[yypt-5].valExpr, Operator: AST_NOT_BETWEEN, From: yyS[yypt-2].valExpr, To: yyS[yypt-0].valExpr}
		}
	case 98:
		//line sql.y:568
		{
			yyVAL.boolExpr = &NullCheck{Operator: AST_IS_NULL, Expr: yyS[yypt-2].valExpr}
		}
	case 99:
		//line sql.y:572
		{
			yyVAL.boolExpr = &NullCheck{Operator: AST_IS_NOT_NULL, Expr: yyS[yypt-3].valExpr}
		}
	case 100:
		//line sql.y:576
		{
			yyVAL.boolExpr = &ExistsExpr{Subquery: yyS[yypt-0].subquery}
		}
	case 101:
		//line sql.y:582
		{
			yyVAL.str = AST_EQ
		}
	case 102:
		//line sql.y:586
		{
			yyVAL.str = AST_LT
		}
	case 103:
		//line sql.y:590
		{
			yyVAL.str = AST_GT
		}
	case 104:
		//line sql.y:594
		{
			yyVAL.str = AST_LE
		}
	case 105:
		//line sql.y:598
		{
			yyVAL.str = AST_GE
		}
	case 106:
		//line sql.y:602
		{
			yyVAL.str = AST_NE
		}
	case 107:
		//line sql.y:606
		{
			yyVAL.str = AST_NSE
		}
	case 108:
		//line sql.y:612
		{
			yyVAL.insRows = yyS[yypt-0].values
		}
	case 109:
		//line sql.y:616
		{
			yyVAL.insRows = yyS[yypt-0].selStmt
		}
	case 110:
		//line sql.y:622
		{
			yyVAL.values = Values{yyS[yypt-0].tuple}
		}
	case 111:
		//line sql.y:626
		{
			yyVAL.values = append(yyS[yypt-2].values, yyS[yypt-0].tuple)
		}
	case 112:
		//line sql.y:632
		{
			yyVAL.tuple = ValTuple(yyS[yypt-1].valExprs)
		}
	case 113:
		//line sql.y:636
		{
			yyVAL.tuple = yyS[yypt-0].subquery
		}
	case 114:
		//line sql.y:642
		{
			yyVAL.subquery = &Subquery{yyS[yypt-1].selStmt}
		}
	case 115:
		//line sql.y:648
		{
			yyVAL.valExprs = ValExprs{yyS[yypt-0].valExpr}
		}
	case 116:
		//line sql.y:652
		{
			yyVAL.valExprs = append(yyS[yypt-2].valExprs, yyS[yypt-0].valExpr)
		}
	case 117:
		//line sql.y:658
		{
			yyVAL.valExpr = yyS[yypt-0].valExpr
		}
	case 118:
		//line sql.y:662
		{
			yyVAL.valExpr = yyS[yypt-0].colName
		}
	case 119:
		//line sql.y:666
		{
			yyVAL.valExpr = yyS[yypt-0].tuple
		}
	case 120:
		//line sql.y:670
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyS[yypt-2].valExpr, Operator: AST_BITAND, Right: yyS[yypt-0].valExpr}
		}
	case 121:
		//line sql.y:674
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyS[yypt-2].valExpr, Operator: AST_BITOR, Right: yyS[yypt-0].valExpr}
		}
	case 122:
		//line sql.y:678
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyS[yypt-2].valExpr, Operator: AST_BITXOR, Right: yyS[yypt-0].valExpr}
		}
	case 123:
		//line sql.y:682
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyS[yypt-2].valExpr, Operator: AST_PLUS, Right: yyS[yypt-0].valExpr}
		}
	case 124:
		//line sql.y:686
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyS[yypt-2].valExpr, Operator: AST_MINUS, Right: yyS[yypt-0].valExpr}
		}
	case 125:
		//line sql.y:690
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyS[yypt-2].valExpr, Operator: AST_MULT, Right: yyS[yypt-0].valExpr}
		}
	case 126:
		//line sql.y:694
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyS[yypt-2].valExpr, Operator: AST_DIV, Right: yyS[yypt-0].valExpr}
		}
	case 127:
		//line sql.y:698
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyS[yypt-2].valExpr, Operator: AST_MOD, Right: yyS[yypt-0].valExpr}
		}
	case 128:
		//line sql.y:702
		{
			if num, ok := yyS[yypt-0].valExpr.(NumVal); ok {
				switch yyS[yypt-1].byt {
				case '-':
					yyVAL.valExpr = append(NumVal("-"), num...)
				case '+':
					yyVAL.valExpr = num
				default:
					yyVAL.valExpr = &UnaryExpr{Operator: yyS[yypt-1].byt, Expr: yyS[yypt-0].valExpr}
				}
			} else {
				yyVAL.valExpr = &UnaryExpr{Operator: yyS[yypt-1].byt, Expr: yyS[yypt-0].valExpr}
			}
		}
	case 129:
		//line sql.y:717
		{
			yyVAL.valExpr = &FuncExpr{Name: yyS[yypt-2].bytes}
		}
	case 130:
		//line sql.y:721
		{
			yyVAL.valExpr = &FuncExpr{Name: yyS[yypt-3].bytes, Exprs: yyS[yypt-1].selectExprs}
		}
	case 131:
		//line sql.y:725
		{
			yyVAL.valExpr = &FuncExpr{Name: yyS[yypt-4].bytes, Distinct: true, Exprs: yyS[yypt-1].selectExprs}
		}
	case 132:
		//line sql.y:729
		{
			yyVAL.valExpr = &FuncExpr{Name: yyS[yypt-3].bytes, Exprs: yyS[yypt-1].selectExprs}
		}
	case 133:
		//line sql.y:733
		{
			yyVAL.valExpr = yyS[yypt-0].caseExpr
		}
	case 134:
		//line sql.y:739
		{
			yyVAL.bytes = IF_BYTES
		}
	case 135:
		//line sql.y:743
		{
			yyVAL.bytes = VALUES_BYTES
		}
	case 136:
		//line sql.y:749
		{
			yyVAL.byt = AST_UPLUS
		}
	case 137:
		//line sql.y:753
		{
			yyVAL.byt = AST_UMINUS
		}
	case 138:
		//line sql.y:757
		{
			yyVAL.byt = AST_TILDA
		}
	case 139:
		//line sql.y:763
		{
			yyVAL.caseExpr = &CaseExpr{Expr: yyS[yypt-3].valExpr, Whens: yyS[yypt-2].whens, Else: yyS[yypt-1].valExpr}
		}
	case 140:
		//line sql.y:768
		{
			yyVAL.valExpr = nil
		}
	case 141:
		//line sql.y:772
		{
			yyVAL.valExpr = yyS[yypt-0].valExpr
		}
	case 142:
		//line sql.y:778
		{
			yyVAL.whens = []*When{yyS[yypt-0].when}
		}
	case 143:
		//line sql.y:782
		{
			yyVAL.whens = append(yyS[yypt-1].whens, yyS[yypt-0].when)
		}
	case 144:
		//line sql.y:788
		{
			yyVAL.when = &When{Cond: yyS[yypt-2].boolExpr, Val: yyS[yypt-0].valExpr}
		}
	case 145:
		//line sql.y:793
		{
			yyVAL.valExpr = nil
		}
	case 146:
		//line sql.y:797
		{
			yyVAL.valExpr = yyS[yypt-0].valExpr
		}
	case 147:
		//line sql.y:803
		{
			yyVAL.colName = &ColName{Name: yyS[yypt-0].bytes}
		}
	case 148:
		//line sql.y:807
		{
			yyVAL.colName = &ColName{Qualifier: yyS[yypt-2].bytes, Name: yyS[yypt-0].bytes}
		}
	case 149:
		//line sql.y:813
		{
			yyVAL.valExpr = StrVal(yyS[yypt-0].bytes)
		}
	case 150:
		//line sql.y:817
		{
			yyVAL.valExpr = NumVal(yyS[yypt-0].bytes)
		}
	case 151:
		//line sql.y:821
		{
			yyVAL.valExpr = ValArg(yyS[yypt-0].bytes)
		}
	case 152:
		//line sql.y:825
		{
			yyVAL.valExpr = &NullVal{}
		}
	case 153:
		//line sql.y:830
		{
			yyVAL.valExprs = nil
		}
	case 154:
		//line sql.y:834
		{
			yyVAL.valExprs = yyS[yypt-0].valExprs
		}
	case 155:
		//line sql.y:839
		{
			yyVAL.boolExpr = nil
		}
	case 156:
		//line sql.y:843
		{
			yyVAL.boolExpr = yyS[yypt-0].boolExpr
		}
	case 157:
		//line sql.y:848
		{
			yyVAL.orderBy = nil
		}
	case 158:
		//line sql.y:852
		{
			yyVAL.orderBy = yyS[yypt-0].orderBy
		}
	case 159:
		//line sql.y:858
		{
			yyVAL.orderBy = OrderBy{yyS[yypt-0].order}
		}
	case 160:
		//line sql.y:862
		{
			yyVAL.orderBy = append(yyS[yypt-2].orderBy, yyS[yypt-0].order)
		}
	case 161:
		//line sql.y:868
		{
			yyVAL.order = &Order{Expr: yyS[yypt-1].valExpr, Direction: yyS[yypt-0].str}
		}
	case 162:
		//line sql.y:873
		{
			yyVAL.str = AST_ASC
		}
	case 163:
		//line sql.y:877
		{
			yyVAL.str = AST_ASC
		}
	case 164:
		//line sql.y:881
		{
			yyVAL.str = AST_DESC
		}
	case 165:
		//line sql.y:886
		{
			yyVAL.limit = nil
		}
	case 166:
		//line sql.y:890
		{
			yyVAL.limit = &Limit{Rowcount: yyS[yypt-0].valExpr}
		}
	case 167:
		//line sql.y:894
		{
			yyVAL.limit = &Limit{Offset: yyS[yypt-2].valExpr, Rowcount: yyS[yypt-0].valExpr}
		}
	case 168:
		//line sql.y:899
		{
			yyVAL.str = ""
		}
	case 169:
		//line sql.y:903
		{
			yyVAL.str = AST_FOR_UPDATE
		}
	case 170:
		//line sql.y:907
		{
			if !bytes.Equal(yyS[yypt-1].bytes, SHARE) {
				yylex.Error("expecting share")
				return 1
			}
			if !bytes.Equal(yyS[yypt-0].bytes, MODE) {
				yylex.Error("expecting mode")
				return 1
			}
			yyVAL.str = AST_SHARE_MODE
		}
	case 171:
		//line sql.y:920
		{
			yyVAL.columns = nil
		}
	case 172:
		//line sql.y:924
		{
			yyVAL.columns = yyS[yypt-1].columns
		}
	case 173:
		//line sql.y:930
		{
			yyVAL.columns = Columns{&NonStarExpr{Expr: yyS[yypt-0].colName}}
		}
	case 174:
		//line sql.y:934
		{
			yyVAL.columns = append(yyVAL.columns, &NonStarExpr{Expr: yyS[yypt-0].colName})
		}
	case 175:
		//line sql.y:939
		{
			yyVAL.updateExprs = nil
		}
	case 176:
		//line sql.y:943
		{
			yyVAL.updateExprs = yyS[yypt-0].updateExprs
		}
	case 177:
		//line sql.y:949
		{
			yyVAL.updateExprs = UpdateExprs{yyS[yypt-0].updateExpr}
		}
	case 178:
		//line sql.y:953
		{
			yyVAL.updateExprs = append(yyS[yypt-2].updateExprs, yyS[yypt-0].updateExpr)
		}
	case 179:
		//line sql.y:959
		{
			yyVAL.updateExpr = &UpdateExpr{Name: yyS[yypt-2].colName, Expr: yyS[yypt-0].valExpr}
		}
	case 180:
		//line sql.y:964
		{
			yyVAL.empty = struct{}{}
		}
	case 181:
		//line sql.y:966
		{
			yyVAL.empty = struct{}{}
		}
	case 182:
		//line sql.y:969
		{
			yyVAL.empty = struct{}{}
		}
	case 183:
		//line sql.y:971
		{
			yyVAL.empty = struct{}{}
		}
	case 184:
		//line sql.y:974
		{
			yyVAL.empty = struct{}{}
		}
	case 185:
		//line sql.y:976
		{
			yyVAL.empty = struct{}{}
		}
	case 186:
		//line sql.y:980
		{
			yyVAL.empty = struct{}{}
		}
	case 187:
		//line sql.y:982
		{
			yyVAL.empty = struct{}{}
		}
	case 188:
		//line sql.y:984
		{
			yyVAL.empty = struct{}{}
		}
	case 189:
		//line sql.y:986
		{
			yyVAL.empty = struct{}{}
		}
	case 190:
		//line sql.y:988
		{
			yyVAL.empty = struct{}{}
		}
	case 191:
		//line sql.y:991
		{
			yyVAL.empty = struct{}{}
		}
	case 192:
		//line sql.y:993
		{
			yyVAL.empty = struct{}{}
		}
	case 193:
		//line sql.y:996
		{
			yyVAL.empty = struct{}{}
		}
	case 194:
		//line sql.y:998
		{
			yyVAL.empty = struct{}{}
		}
	case 195:
		//line sql.y:1001
		{
			yyVAL.empty = struct{}{}
		}
	case 196:
		//line sql.y:1003
		{
			yyVAL.empty = struct{}{}
		}
	case 197:
		//line sql.y:1007
		{
			yyVAL.bytes = bytes.ToLower(yyS[yypt-0].bytes)
		}
	case 198:
		//line sql.y:1012
		{
			ForceEOF(yylex)
		}
	}
	goto yystack /* stack new state and value */
}
