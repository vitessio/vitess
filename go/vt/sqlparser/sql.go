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
	colTuple    ColTuple
	valExprs    ValExprs
	values      Values
	rowTuple    RowTuple
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
const TRUE = 57368
const FALSE = 57369
const ASC = 57370
const DESC = 57371
const VALUES = 57372
const INTO = 57373
const DUPLICATE = 57374
const KEY = 57375
const DEFAULT = 57376
const SET = 57377
const LOCK = 57378
const KEYRANGE = 57379
const ID = 57380
const STRING = 57381
const NUMBER = 57382
const VALUE_ARG = 57383
const LIST_ARG = 57384
const COMMENT = 57385
const LE = 57386
const GE = 57387
const NE = 57388
const NULL_SAFE_EQUAL = 57389
const UNION = 57390
const MINUS = 57391
const EXCEPT = 57392
const INTERSECT = 57393
const JOIN = 57394
const STRAIGHT_JOIN = 57395
const LEFT = 57396
const RIGHT = 57397
const INNER = 57398
const OUTER = 57399
const CROSS = 57400
const NATURAL = 57401
const USE = 57402
const FORCE = 57403
const ON = 57404
const OR = 57405
const AND = 57406
const NOT = 57407
const UNARY = 57408
const CASE = 57409
const WHEN = 57410
const THEN = 57411
const ELSE = 57412
const END = 57413
const CREATE = 57414
const ALTER = 57415
const DROP = 57416
const RENAME = 57417
const ANALYZE = 57418
const TABLE = 57419
const INDEX = 57420
const VIEW = 57421
const TO = 57422
const IGNORE = 57423
const IF = 57424
const UNIQUE = 57425
const USING = 57426
const SHOW = 57427
const DESCRIBE = 57428
const EXPLAIN = 57429

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
	"TRUE",
	"FALSE",
	"ASC",
	"DESC",
	"VALUES",
	"INTO",
	"DUPLICATE",
	"KEY",
	"DEFAULT",
	"SET",
	"LOCK",
	"KEYRANGE",
	"ID",
	"STRING",
	"NUMBER",
	"VALUE_ARG",
	"LIST_ARG",
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
	"OR",
	"AND",
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

const yyNprod = 215
const yyPrivate = 57344

var yyTokenNames []string
var yyStates []string

const yyLast = 612

var yyAct = []int{

	94, 308, 162, 378, 91, 165, 345, 92, 62, 257,
	90, 201, 248, 300, 216, 181, 63, 387, 387, 164,
	3, 139, 358, 80, 81, 387, 189, 50, 28, 29,
	30, 31, 76, 68, 132, 105, 239, 264, 65, 306,
	132, 70, 64, 132, 73, 239, 126, 53, 77, 85,
	38, 72, 40, 51, 52, 97, 41, 43, 86, 44,
	102, 103, 104, 389, 388, 110, 356, 355, 138, 137,
	122, 386, 98, 84, 99, 100, 101, 354, 238, 130,
	334, 49, 331, 89, 134, 305, 294, 108, 195, 292,
	69, 240, 166, 46, 47, 48, 167, 45, 123, 325,
	327, 125, 237, 336, 249, 249, 88, 298, 279, 193,
	106, 107, 82, 196, 175, 65, 136, 111, 65, 64,
	185, 184, 64, 179, 268, 269, 270, 271, 272, 326,
	273, 274, 109, 171, 183, 86, 207, 185, 161, 163,
	151, 152, 153, 215, 119, 115, 223, 224, 186, 229,
	230, 231, 232, 233, 234, 235, 236, 206, 199, 365,
	366, 121, 351, 192, 194, 191, 139, 205, 301, 329,
	260, 241, 86, 86, 139, 129, 353, 218, 65, 65,
	71, 139, 64, 255, 246, 352, 253, 209, 210, 59,
	261, 117, 131, 243, 245, 256, 252, 149, 150, 151,
	152, 153, 301, 146, 147, 148, 149, 150, 151, 152,
	153, 323, 319, 138, 137, 278, 265, 320, 262, 208,
	241, 138, 137, 283, 285, 286, 317, 322, 338, 137,
	321, 318, 259, 182, 182, 284, 117, 239, 363, 205,
	132, 340, 291, 228, 225, 227, 295, 86, 113, 219,
	14, 116, 214, 211, 213, 217, 218, 112, 118, 303,
	297, 28, 29, 30, 31, 97, 307, 304, 293, 75,
	102, 103, 104, 373, 372, 110, 371, 204, 315, 316,
	266, 117, 98, 66, 99, 100, 101, 203, 168, 226,
	333, 177, 173, 89, 172, 170, 169, 108, 212, 299,
	14, 337, 205, 205, 178, 71, 335, 65, 66, 342,
	330, 341, 343, 346, 328, 277, 88, 135, 78, 312,
	106, 107, 268, 269, 270, 271, 272, 111, 273, 274,
	102, 103, 104, 204, 276, 357, 71, 311, 198, 347,
	197, 359, 109, 203, 99, 100, 101, 180, 361, 127,
	124, 120, 60, 241, 79, 74, 369, 114, 367, 360,
	14, 15, 16, 17, 384, 375, 346, 339, 14, 377,
	376, 58, 379, 379, 379, 65, 380, 381, 220, 64,
	221, 222, 391, 382, 385, 290, 288, 289, 392, 139,
	18, 187, 393, 251, 394, 244, 128, 97, 56, 368,
	309, 370, 102, 103, 104, 32, 54, 110, 282, 280,
	281, 350, 310, 258, 98, 84, 99, 100, 101, 362,
	349, 34, 35, 36, 37, 89, 314, 182, 61, 108,
	390, 374, 14, 33, 146, 147, 148, 149, 150, 151,
	152, 153, 19, 20, 22, 21, 23, 188, 88, 39,
	263, 190, 106, 107, 82, 24, 25, 26, 97, 111,
	42, 67, 254, 102, 103, 104, 176, 383, 110, 364,
	344, 348, 14, 313, 109, 98, 66, 99, 100, 101,
	242, 296, 174, 247, 96, 93, 89, 95, 302, 250,
	108, 140, 102, 103, 104, 87, 324, 110, 202, 267,
	200, 83, 275, 133, 55, 66, 99, 100, 101, 88,
	27, 57, 13, 106, 107, 168, 12, 11, 10, 108,
	111, 9, 8, 102, 103, 104, 7, 6, 110, 5,
	4, 141, 145, 143, 144, 109, 66, 99, 100, 101,
	2, 1, 106, 107, 0, 0, 168, 0, 0, 111,
	108, 0, 0, 0, 157, 158, 159, 160, 0, 154,
	155, 156, 0, 332, 109, 146, 147, 148, 149, 150,
	151, 152, 153, 106, 107, 0, 0, 0, 0, 0,
	111, 142, 146, 147, 148, 149, 150, 151, 152, 153,
	0, 0, 0, 0, 287, 109, 146, 147, 148, 149,
	150, 151, 152, 153, 146, 147, 148, 149, 150, 151,
	152, 153,
}
var yyPact = []int{

	355, -1000, -1000, 208, -1000, -1000, -1000, -1000, -1000, -1000,
	-1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, -42,
	-37, 5, 1, -11, -1000, -1000, -1000, 427, 389, -1000,
	-1000, -1000, 380, -1000, 340, 314, 419, 270, -64, -3,
	267, -1000, -41, 267, -1000, 317, -65, 267, -65, 316,
	-1000, -1000, -1000, -1000, -1000, 35, -1000, 214, 314, 322,
	65, 314, 179, -1000, 209, -1000, 64, 313, 90, 267,
	-1000, -1000, 312, -1000, -49, 311, 376, 107, 267, -1000,
	183, -1000, -1000, 298, 36, 152, 510, -1000, 438, 245,
	-1000, -1000, -1000, 498, 248, 247, -1000, 246, 244, -1000,
	-1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000,
	-1000, 498, -1000, 256, 270, 309, 417, 270, 498, 267,
	-1000, 371, -73, -1000, 75, -1000, 302, -1000, -1000, 300,
	-1000, 239, 35, -1000, -1000, 267, 142, 438, 438, 227,
	498, 207, 357, 498, 498, 218, 498, 498, 498, 498,
	498, 498, 498, 498, -1000, -1000, -1000, -1000, -1000, -1000,
	-1000, 367, 510, -1, -25, -12, 510, -1000, 467, 377,
	35, -1000, 427, 305, 21, 532, 363, 270, 270, 224,
	-1000, 400, 438, -1000, 532, -1000, -1000, -1000, 102, 267,
	-1000, -58, -1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000,
	223, 264, 296, 295, 28, -1000, -1000, -1000, -1000, 367,
	159, -1000, 383, -1000, -1000, 532, -1000, 467, -1000, -1000,
	207, 498, 498, 532, 524, -1000, 360, -1000, -1000, 122,
	122, 122, 63, 63, -1000, -1000, -1000, -1000, -1000, 498,
	-1000, 532, -1000, -14, 35, -17, 189, 22, -1000, 438,
	100, 240, 208, 134, -18, -1000, 400, 385, 398, 152,
	299, -1000, -1000, 281, -1000, 415, 239, 239, -1000, -1000,
	168, 154, 172, 169, 153, 33, -1000, 276, 66, 272,
	-1000, -1000, -1000, -21, -1000, 532, 493, 498, -1000, -1000,
	-1000, 532, -1000, -23, -1000, 305, 17, -1000, 498, 144,
	-1000, 335, 184, -1000, -1000, -1000, 270, 385, -1000, 498,
	498, -1000, -1000, 408, 397, 264, 94, -1000, 127, -1000,
	118, -1000, -1000, -1000, -1000, -16, -26, -27, -1000, -1000,
	-1000, -1000, 498, 532, -1000, -81, -1000, 532, 498, 326,
	240, -1000, -1000, 362, 181, -1000, 131, -1000, 400, 438,
	498, 438, -1000, -1000, 228, 226, 225, 532, -1000, 532,
	424, -1000, 498, 498, -1000, -1000, -1000, 385, 152, 180,
	152, 267, 267, 267, 270, 532, -1000, 348, -32, -1000,
	-39, -40, 179, -1000, 423, 361, -1000, 267, -1000, -1000,
	-1000, 267, -1000, 267, -1000,
}
var yyPgo = []int{

	0, 541, 540, 19, 530, 529, 527, 526, 522, 521,
	518, 517, 516, 512, 405, 511, 510, 504, 23, 24,
	503, 502, 501, 500, 11, 499, 498, 189, 496, 3,
	15, 49, 495, 491, 489, 10, 2, 14, 5, 488,
	7, 487, 35, 485, 4, 484, 483, 12, 482, 481,
	473, 471, 9, 470, 6, 469, 1, 467, 466, 462,
	13, 8, 16, 269, 461, 460, 451, 450, 449, 447,
	0, 27, 433,
}
var yyR1 = []int{

	0, 1, 2, 2, 2, 2, 2, 2, 2, 2,
	2, 2, 2, 3, 3, 4, 4, 5, 6, 7,
	8, 8, 8, 9, 9, 9, 10, 11, 11, 11,
	12, 13, 13, 13, 72, 14, 15, 15, 16, 16,
	16, 16, 16, 17, 17, 18, 18, 19, 19, 19,
	22, 22, 20, 20, 20, 23, 23, 24, 24, 24,
	24, 21, 21, 21, 25, 25, 25, 25, 25, 25,
	25, 25, 25, 26, 26, 26, 27, 27, 28, 28,
	28, 28, 29, 29, 30, 30, 31, 31, 31, 31,
	31, 31, 31, 31, 31, 31, 31, 32, 32, 32,
	32, 32, 32, 32, 32, 32, 32, 32, 32, 32,
	32, 32, 33, 33, 33, 33, 33, 33, 33, 37,
	37, 37, 42, 38, 38, 36, 36, 36, 36, 36,
	36, 36, 36, 36, 36, 36, 36, 36, 36, 36,
	36, 36, 41, 41, 43, 43, 43, 45, 48, 48,
	46, 46, 47, 49, 49, 44, 44, 35, 35, 35,
	35, 35, 35, 50, 50, 51, 51, 52, 52, 53,
	53, 54, 55, 55, 55, 56, 56, 56, 57, 57,
	57, 58, 58, 59, 59, 60, 60, 34, 34, 39,
	39, 40, 40, 61, 61, 62, 63, 63, 64, 64,
	65, 65, 66, 66, 66, 66, 66, 67, 67, 68,
	68, 69, 69, 70, 71,
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
	3, 3, 4, 3, 4, 3, 4, 3, 3, 4,
	3, 4, 5, 6, 3, 4, 3, 4, 3, 4,
	2, 6, 1, 1, 1, 1, 1, 1, 1, 3,
	1, 1, 3, 1, 3, 1, 1, 1, 3, 3,
	3, 3, 3, 3, 3, 3, 2, 3, 4, 5,
	4, 1, 1, 1, 1, 1, 1, 5, 0, 1,
	1, 2, 4, 0, 2, 1, 3, 1, 1, 1,
	1, 1, 1, 0, 3, 0, 2, 0, 3, 1,
	3, 2, 0, 1, 1, 0, 2, 4, 0, 2,
	4, 0, 3, 1, 3, 0, 5, 2, 1, 1,
	3, 3, 1, 1, 3, 3, 0, 2, 0, 3,
	0, 1, 1, 1, 1, 1, 1, 0, 1, 0,
	1, 0, 2, 1, 0,
}
var yyChk = []int{

	-1000, -1, -2, -3, -4, -5, -6, -7, -8, -9,
	-10, -11, -12, -13, 5, 6, 7, 8, 35, 87,
	88, 90, 89, 91, 100, 101, 102, -16, 53, 54,
	55, 56, -14, -72, -14, -14, -14, -14, 92, -68,
	94, 98, -65, 94, 96, 92, 92, 93, 94, 92,
	-71, -71, -71, -3, 17, -17, 18, -15, 31, -27,
	38, 9, -61, -62, -44, -70, 38, -64, 97, 93,
	-70, 38, 92, -70, 38, -63, 97, -70, -63, 38,
	-18, -19, 77, -22, 38, -31, -36, -32, 71, 48,
	-35, -44, -40, -43, -70, -41, -45, 20, 37, 39,
	40, 41, 25, 26, 27, -42, 75, 76, 52, 97,
	30, 82, 43, -27, 35, 80, -27, 57, 49, 80,
	38, 71, -70, -71, 38, -71, 95, 38, 20, 68,
	-70, 9, 57, -20, -70, 19, 80, 70, 69, 22,
	-33, 21, 71, 23, 24, 22, 72, 73, 74, 75,
	76, 77, 78, 79, 49, 50, 51, 44, 45, 46,
	47, -31, -36, -31, -3, -38, -36, -36, 48, 48,
	48, -42, 48, 48, -48, -36, -58, 35, 48, -61,
	38, -30, 10, -62, -36, -70, -71, 20, -69, 99,
	-66, 90, 88, 34, 89, 13, 38, 38, 38, -71,
	-23, -24, -26, 48, 38, -42, -19, -70, 77, -31,
	-31, 26, 71, 27, 25, -36, -37, 48, -42, 42,
	21, 23, 24, -36, -36, 26, 71, 27, 25, -36,
	-36, -36, -36, -36, -36, -36, -36, 103, 103, 57,
	103, -36, 103, -18, 18, -18, -35, -46, -47, 83,
	-34, 30, -3, -61, -59, -44, -30, -52, 13, -31,
	68, -70, -71, -67, 95, -30, 57, -25, 58, 59,
	60, 61, 62, 64, 65, -21, 38, 19, -24, 80,
	26, 27, 25, -38, -37, -36, -36, 70, 26, 27,
	25, -36, 103, -18, 103, 57, -49, -47, 85, -31,
	-60, 68, -39, -40, -60, 103, 57, -52, -56, 15,
	14, 38, 38, -50, 11, -24, -24, 58, 63, 58,
	63, 58, 58, 58, -28, 66, 96, 67, 38, 103,
	38, 103, 70, -36, 103, -35, 86, -36, 84, 32,
	57, -44, -56, -36, -53, -54, -36, -71, -51, 12,
	14, 68, 58, 58, 93, 93, 93, -36, 103, -36,
	33, -40, 57, 57, -55, 28, 29, -52, -31, -38,
	-31, 48, 48, 48, 7, -36, -54, -56, -29, -70,
	-29, -29, -61, -57, 16, 36, 103, 57, 103, 103,
	7, 21, -70, -70, -70,
}
var yyDef = []int{

	0, -2, 1, 2, 3, 4, 5, 6, 7, 8,
	9, 10, 11, 12, 34, 34, 34, 34, 34, 209,
	200, 0, 0, 0, 214, 214, 214, 0, 38, 40,
	41, 42, 43, 36, 0, 0, 0, 0, 198, 0,
	0, 210, 0, 0, 201, 0, 196, 0, 196, 0,
	31, 32, 33, 14, 39, 0, 44, 35, 0, 0,
	76, 0, 19, 193, 0, 155, 213, 0, 0, 0,
	214, 213, 0, 214, 0, 0, 0, 0, 0, 30,
	0, 45, 47, 52, 213, 50, 51, 86, 0, 0,
	125, 126, 127, 0, 155, 0, 141, 0, 0, 157,
	158, 159, 160, 161, 162, 192, 144, 145, 146, 142,
	143, 148, 37, 181, 0, 0, 84, 0, 0, 0,
	214, 0, 211, 22, 0, 25, 0, 27, 197, 0,
	214, 0, 0, 48, 53, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 112, 113, 114, 115, 116, 117,
	118, 89, 0, 0, 0, 0, 123, 136, 0, 0,
	0, 110, 0, 0, 0, 149, 0, 0, 0, 84,
	77, 167, 0, 194, 195, 156, 20, 199, 0, 0,
	214, 207, 202, 203, 204, 205, 206, 26, 28, 29,
	84, 55, 61, 0, 73, 75, 46, 54, 49, 87,
	88, 91, 0, 93, 95, 97, 98, 0, 120, 121,
	0, 0, 0, 100, 0, 104, 0, 106, 108, 128,
	129, 130, 131, 132, 133, 134, 135, 90, 122, 0,
	191, 123, 137, 0, 0, 0, 0, 153, 150, 0,
	185, 0, 188, 185, 0, 183, 167, 175, 0, 85,
	0, 212, 23, 0, 208, 163, 0, 0, 64, 65,
	0, 0, 0, 0, 0, 78, 62, 0, 0, 0,
	92, 94, 96, 0, 99, 101, 0, 0, 105, 107,
	109, 124, 138, 0, 140, 0, 0, 151, 0, 0,
	15, 0, 187, 189, 16, 182, 0, 175, 18, 0,
	0, 214, 24, 165, 0, 56, 59, 66, 0, 68,
	0, 70, 71, 72, 57, 0, 0, 0, 63, 58,
	74, 119, 0, 102, 139, 0, 147, 154, 0, 0,
	0, 184, 17, 176, 168, 169, 172, 21, 167, 0,
	0, 0, 67, 69, 0, 0, 0, 103, 111, 152,
	0, 190, 0, 0, 171, 173, 174, 175, 166, 164,
	60, 0, 0, 0, 0, 177, 170, 178, 0, 82,
	0, 0, 186, 13, 0, 0, 79, 0, 80, 81,
	179, 0, 83, 0, 180,
}
var yyTok1 = []int{

	1, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 79, 72, 3,
	48, 103, 77, 75, 57, 76, 80, 78, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	50, 49, 51, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 74, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 73, 3, 52,
}
var yyTok2 = []int{

	2, 3, 4, 5, 6, 7, 8, 9, 10, 11,
	12, 13, 14, 15, 16, 17, 18, 19, 20, 21,
	22, 23, 24, 25, 26, 27, 28, 29, 30, 31,
	32, 33, 34, 35, 36, 37, 38, 39, 40, 41,
	42, 43, 44, 45, 46, 47, 53, 54, 55, 56,
	58, 59, 60, 61, 62, 63, 64, 65, 66, 67,
	68, 69, 70, 71, 81, 82, 83, 84, 85, 86,
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
		//line sql.y:151
		{
			SetParseTree(yylex, yyS[yypt-0].statement)
		}
	case 2:
		//line sql.y:157
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
		//line sql.y:173
		{
			yyVAL.selStmt = &Select{Comments: Comments(yyS[yypt-10].bytes2), Distinct: yyS[yypt-9].str, SelectExprs: yyS[yypt-8].selectExprs, From: yyS[yypt-6].tableExprs, Where: NewWhere(AST_WHERE, yyS[yypt-5].boolExpr), GroupBy: GroupBy(yyS[yypt-4].valExprs), Having: NewWhere(AST_HAVING, yyS[yypt-3].boolExpr), OrderBy: yyS[yypt-2].orderBy, Limit: yyS[yypt-1].limit, Lock: yyS[yypt-0].str}
		}
	case 14:
		//line sql.y:177
		{
			yyVAL.selStmt = &Union{Type: yyS[yypt-1].str, Left: yyS[yypt-2].selStmt, Right: yyS[yypt-0].selStmt}
		}
	case 15:
		//line sql.y:183
		{
			yyVAL.statement = &Insert{Comments: Comments(yyS[yypt-5].bytes2), Table: yyS[yypt-3].tableName, Columns: yyS[yypt-2].columns, Rows: yyS[yypt-1].insRows, OnDup: OnDup(yyS[yypt-0].updateExprs)}
		}
	case 16:
		//line sql.y:187
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
		//line sql.y:199
		{
			yyVAL.statement = &Update{Comments: Comments(yyS[yypt-6].bytes2), Table: yyS[yypt-5].tableName, Exprs: yyS[yypt-3].updateExprs, Where: NewWhere(AST_WHERE, yyS[yypt-2].boolExpr), OrderBy: yyS[yypt-1].orderBy, Limit: yyS[yypt-0].limit}
		}
	case 18:
		//line sql.y:205
		{
			yyVAL.statement = &Delete{Comments: Comments(yyS[yypt-5].bytes2), Table: yyS[yypt-3].tableName, Where: NewWhere(AST_WHERE, yyS[yypt-2].boolExpr), OrderBy: yyS[yypt-1].orderBy, Limit: yyS[yypt-0].limit}
		}
	case 19:
		//line sql.y:211
		{
			yyVAL.statement = &Set{Comments: Comments(yyS[yypt-1].bytes2), Exprs: yyS[yypt-0].updateExprs}
		}
	case 20:
		//line sql.y:217
		{
			yyVAL.statement = &DDL{Action: AST_CREATE, NewName: yyS[yypt-1].bytes}
		}
	case 21:
		//line sql.y:221
		{
			// Change this to an alter statement
			yyVAL.statement = &DDL{Action: AST_ALTER, Table: yyS[yypt-1].bytes, NewName: yyS[yypt-1].bytes}
		}
	case 22:
		//line sql.y:226
		{
			yyVAL.statement = &DDL{Action: AST_CREATE, NewName: yyS[yypt-1].bytes}
		}
	case 23:
		//line sql.y:232
		{
			yyVAL.statement = &DDL{Action: AST_ALTER, Table: yyS[yypt-2].bytes, NewName: yyS[yypt-2].bytes}
		}
	case 24:
		//line sql.y:236
		{
			// Change this to a rename statement
			yyVAL.statement = &DDL{Action: AST_RENAME, Table: yyS[yypt-3].bytes, NewName: yyS[yypt-0].bytes}
		}
	case 25:
		//line sql.y:241
		{
			yyVAL.statement = &DDL{Action: AST_ALTER, Table: yyS[yypt-1].bytes, NewName: yyS[yypt-1].bytes}
		}
	case 26:
		//line sql.y:247
		{
			yyVAL.statement = &DDL{Action: AST_RENAME, Table: yyS[yypt-2].bytes, NewName: yyS[yypt-0].bytes}
		}
	case 27:
		//line sql.y:253
		{
			yyVAL.statement = &DDL{Action: AST_DROP, Table: yyS[yypt-0].bytes}
		}
	case 28:
		//line sql.y:257
		{
			// Change this to an alter statement
			yyVAL.statement = &DDL{Action: AST_ALTER, Table: yyS[yypt-0].bytes, NewName: yyS[yypt-0].bytes}
		}
	case 29:
		//line sql.y:262
		{
			yyVAL.statement = &DDL{Action: AST_DROP, Table: yyS[yypt-1].bytes}
		}
	case 30:
		//line sql.y:268
		{
			yyVAL.statement = &DDL{Action: AST_ALTER, Table: yyS[yypt-0].bytes, NewName: yyS[yypt-0].bytes}
		}
	case 31:
		//line sql.y:274
		{
			yyVAL.statement = &Other{}
		}
	case 32:
		//line sql.y:278
		{
			yyVAL.statement = &Other{}
		}
	case 33:
		//line sql.y:282
		{
			yyVAL.statement = &Other{}
		}
	case 34:
		//line sql.y:287
		{
			SetAllowComments(yylex, true)
		}
	case 35:
		//line sql.y:291
		{
			yyVAL.bytes2 = yyS[yypt-0].bytes2
			SetAllowComments(yylex, false)
		}
	case 36:
		//line sql.y:297
		{
			yyVAL.bytes2 = nil
		}
	case 37:
		//line sql.y:301
		{
			yyVAL.bytes2 = append(yyS[yypt-1].bytes2, yyS[yypt-0].bytes)
		}
	case 38:
		//line sql.y:307
		{
			yyVAL.str = AST_UNION
		}
	case 39:
		//line sql.y:311
		{
			yyVAL.str = AST_UNION_ALL
		}
	case 40:
		//line sql.y:315
		{
			yyVAL.str = AST_SET_MINUS
		}
	case 41:
		//line sql.y:319
		{
			yyVAL.str = AST_EXCEPT
		}
	case 42:
		//line sql.y:323
		{
			yyVAL.str = AST_INTERSECT
		}
	case 43:
		//line sql.y:328
		{
			yyVAL.str = ""
		}
	case 44:
		//line sql.y:332
		{
			yyVAL.str = AST_DISTINCT
		}
	case 45:
		//line sql.y:338
		{
			yyVAL.selectExprs = SelectExprs{yyS[yypt-0].selectExpr}
		}
	case 46:
		//line sql.y:342
		{
			yyVAL.selectExprs = append(yyVAL.selectExprs, yyS[yypt-0].selectExpr)
		}
	case 47:
		//line sql.y:348
		{
			yyVAL.selectExpr = &StarExpr{}
		}
	case 48:
		//line sql.y:352
		{
			yyVAL.selectExpr = &NonStarExpr{Expr: yyS[yypt-1].expr, As: yyS[yypt-0].bytes}
		}
	case 49:
		//line sql.y:356
		{
			yyVAL.selectExpr = &StarExpr{TableName: yyS[yypt-2].bytes}
		}
	case 50:
		//line sql.y:362
		{
			yyVAL.expr = yyS[yypt-0].boolExpr
		}
	case 51:
		//line sql.y:366
		{
			yyVAL.expr = yyS[yypt-0].valExpr
		}
	case 52:
		//line sql.y:371
		{
			yyVAL.bytes = nil
		}
	case 53:
		//line sql.y:375
		{
			yyVAL.bytes = yyS[yypt-0].bytes
		}
	case 54:
		//line sql.y:379
		{
			yyVAL.bytes = yyS[yypt-0].bytes
		}
	case 55:
		//line sql.y:385
		{
			yyVAL.tableExprs = TableExprs{yyS[yypt-0].tableExpr}
		}
	case 56:
		//line sql.y:389
		{
			yyVAL.tableExprs = append(yyVAL.tableExprs, yyS[yypt-0].tableExpr)
		}
	case 57:
		//line sql.y:395
		{
			yyVAL.tableExpr = &AliasedTableExpr{Expr: yyS[yypt-2].smTableExpr, As: yyS[yypt-1].bytes, Hints: yyS[yypt-0].indexHints}
		}
	case 58:
		//line sql.y:399
		{
			yyVAL.tableExpr = &ParenTableExpr{Expr: yyS[yypt-1].tableExpr}
		}
	case 59:
		//line sql.y:403
		{
			yyVAL.tableExpr = &JoinTableExpr{LeftExpr: yyS[yypt-2].tableExpr, Join: yyS[yypt-1].str, RightExpr: yyS[yypt-0].tableExpr}
		}
	case 60:
		//line sql.y:407
		{
			yyVAL.tableExpr = &JoinTableExpr{LeftExpr: yyS[yypt-4].tableExpr, Join: yyS[yypt-3].str, RightExpr: yyS[yypt-2].tableExpr, On: yyS[yypt-0].boolExpr}
		}
	case 61:
		//line sql.y:412
		{
			yyVAL.bytes = nil
		}
	case 62:
		//line sql.y:416
		{
			yyVAL.bytes = yyS[yypt-0].bytes
		}
	case 63:
		//line sql.y:420
		{
			yyVAL.bytes = yyS[yypt-0].bytes
		}
	case 64:
		//line sql.y:426
		{
			yyVAL.str = AST_JOIN
		}
	case 65:
		//line sql.y:430
		{
			yyVAL.str = AST_STRAIGHT_JOIN
		}
	case 66:
		//line sql.y:434
		{
			yyVAL.str = AST_LEFT_JOIN
		}
	case 67:
		//line sql.y:438
		{
			yyVAL.str = AST_LEFT_JOIN
		}
	case 68:
		//line sql.y:442
		{
			yyVAL.str = AST_RIGHT_JOIN
		}
	case 69:
		//line sql.y:446
		{
			yyVAL.str = AST_RIGHT_JOIN
		}
	case 70:
		//line sql.y:450
		{
			yyVAL.str = AST_JOIN
		}
	case 71:
		//line sql.y:454
		{
			yyVAL.str = AST_CROSS_JOIN
		}
	case 72:
		//line sql.y:458
		{
			yyVAL.str = AST_NATURAL_JOIN
		}
	case 73:
		//line sql.y:464
		{
			yyVAL.smTableExpr = &TableName{Name: yyS[yypt-0].bytes}
		}
	case 74:
		//line sql.y:468
		{
			yyVAL.smTableExpr = &TableName{Qualifier: yyS[yypt-2].bytes, Name: yyS[yypt-0].bytes}
		}
	case 75:
		//line sql.y:472
		{
			yyVAL.smTableExpr = yyS[yypt-0].subquery
		}
	case 76:
		//line sql.y:478
		{
			yyVAL.tableName = &TableName{Name: yyS[yypt-0].bytes}
		}
	case 77:
		//line sql.y:482
		{
			yyVAL.tableName = &TableName{Qualifier: yyS[yypt-2].bytes, Name: yyS[yypt-0].bytes}
		}
	case 78:
		//line sql.y:487
		{
			yyVAL.indexHints = nil
		}
	case 79:
		//line sql.y:491
		{
			yyVAL.indexHints = &IndexHints{Type: AST_USE, Indexes: yyS[yypt-1].bytes2}
		}
	case 80:
		//line sql.y:495
		{
			yyVAL.indexHints = &IndexHints{Type: AST_IGNORE, Indexes: yyS[yypt-1].bytes2}
		}
	case 81:
		//line sql.y:499
		{
			yyVAL.indexHints = &IndexHints{Type: AST_FORCE, Indexes: yyS[yypt-1].bytes2}
		}
	case 82:
		//line sql.y:505
		{
			yyVAL.bytes2 = [][]byte{yyS[yypt-0].bytes}
		}
	case 83:
		//line sql.y:509
		{
			yyVAL.bytes2 = append(yyS[yypt-2].bytes2, yyS[yypt-0].bytes)
		}
	case 84:
		//line sql.y:514
		{
			yyVAL.boolExpr = nil
		}
	case 85:
		//line sql.y:518
		{
			yyVAL.boolExpr = yyS[yypt-0].boolExpr
		}
	case 86:
		yyVAL.boolExpr = yyS[yypt-0].boolExpr
	case 87:
		//line sql.y:525
		{
			yyVAL.boolExpr = &AndExpr{Left: yyS[yypt-2].boolExpr, Right: yyS[yypt-0].boolExpr}
		}
	case 88:
		//line sql.y:529
		{
			yyVAL.boolExpr = &OrExpr{Left: yyS[yypt-2].boolExpr, Right: yyS[yypt-0].boolExpr}
		}
	case 89:
		//line sql.y:533
		{
			yyVAL.boolExpr = &NotExpr{Expr: yyS[yypt-0].boolExpr}
		}
	case 90:
		//line sql.y:537
		{
			yyVAL.boolExpr = &ParenBoolExpr{Expr: yyS[yypt-1].boolExpr}
		}
	case 91:
		//line sql.y:541
		{
			yyVAL.boolExpr = &TrueExpr{Operator: AST_IS_TRUE, Expr: yyS[yypt-2].boolExpr}
		}
	case 92:
		//line sql.y:545
		{
			yyVAL.boolExpr = &TrueExpr{Operator: AST_IS_NOT_TRUE, Expr: yyS[yypt-3].boolExpr}
		}
	case 93:
		//line sql.y:549
		{
			yyVAL.boolExpr = &FalseExpr{Operator: AST_IS_FALSE, Expr: yyS[yypt-2].boolExpr}
		}
	case 94:
		//line sql.y:553
		{
			yyVAL.boolExpr = &FalseExpr{Operator: AST_IS_NOT_FALSE, Expr: yyS[yypt-3].boolExpr}
		}
	case 95:
		//line sql.y:557
		{
			yyVAL.boolExpr = &NullExpr{Operator: AST_IS_NULL, Expr: yyS[yypt-2].boolExpr}
		}
	case 96:
		//line sql.y:561
		{
			yyVAL.boolExpr = &NullExpr{Operator: AST_IS_NOT_NULL, Expr: yyS[yypt-3].boolExpr}
		}
	case 97:
		//line sql.y:567
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyS[yypt-2].valExpr, Operator: yyS[yypt-1].str, Right: yyS[yypt-0].valExpr}
		}
	case 98:
		//line sql.y:571
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyS[yypt-2].valExpr, Operator: AST_IN, Right: yyS[yypt-0].colTuple}
		}
	case 99:
		//line sql.y:575
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyS[yypt-3].valExpr, Operator: AST_NOT_IN, Right: yyS[yypt-0].colTuple}
		}
	case 100:
		//line sql.y:579
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyS[yypt-2].valExpr, Operator: AST_LIKE, Right: yyS[yypt-0].valExpr}
		}
	case 101:
		//line sql.y:583
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyS[yypt-3].valExpr, Operator: AST_NOT_LIKE, Right: yyS[yypt-0].valExpr}
		}
	case 102:
		//line sql.y:587
		{
			yyVAL.boolExpr = &RangeCond{Left: yyS[yypt-4].valExpr, Operator: AST_BETWEEN, From: yyS[yypt-2].valExpr, To: yyS[yypt-0].valExpr}
		}
	case 103:
		//line sql.y:591
		{
			yyVAL.boolExpr = &RangeCond{Left: yyS[yypt-5].valExpr, Operator: AST_NOT_BETWEEN, From: yyS[yypt-2].valExpr, To: yyS[yypt-0].valExpr}
		}
	case 104:
		//line sql.y:595
		{
			yyVAL.boolExpr = &TrueCheck{Operator: AST_IS_TRUE, Expr: yyS[yypt-2].valExpr}
		}
	case 105:
		//line sql.y:599
		{
			yyVAL.boolExpr = &TrueCheck{Operator: AST_IS_NOT_TRUE, Expr: yyS[yypt-3].valExpr}
		}
	case 106:
		//line sql.y:603
		{
			yyVAL.boolExpr = &FalseCheck{Operator: AST_IS_FALSE, Expr: yyS[yypt-2].valExpr}
		}
	case 107:
		//line sql.y:607
		{
			yyVAL.boolExpr = &FalseCheck{Operator: AST_IS_NOT_FALSE, Expr: yyS[yypt-3].valExpr}
		}
	case 108:
		//line sql.y:611
		{
			yyVAL.boolExpr = &NullCheck{Operator: AST_IS_NULL, Expr: yyS[yypt-2].valExpr}
		}
	case 109:
		//line sql.y:615
		{
			yyVAL.boolExpr = &NullCheck{Operator: AST_IS_NOT_NULL, Expr: yyS[yypt-3].valExpr}
		}
	case 110:
		//line sql.y:619
		{
			yyVAL.boolExpr = &ExistsExpr{Subquery: yyS[yypt-0].subquery}
		}
	case 111:
		//line sql.y:623
		{
			yyVAL.boolExpr = &KeyrangeExpr{Start: yyS[yypt-3].valExpr, End: yyS[yypt-1].valExpr}
		}
	case 112:
		//line sql.y:629
		{
			yyVAL.str = AST_EQ
		}
	case 113:
		//line sql.y:633
		{
			yyVAL.str = AST_LT
		}
	case 114:
		//line sql.y:637
		{
			yyVAL.str = AST_GT
		}
	case 115:
		//line sql.y:641
		{
			yyVAL.str = AST_LE
		}
	case 116:
		//line sql.y:645
		{
			yyVAL.str = AST_GE
		}
	case 117:
		//line sql.y:649
		{
			yyVAL.str = AST_NE
		}
	case 118:
		//line sql.y:653
		{
			yyVAL.str = AST_NSE
		}
	case 119:
		//line sql.y:659
		{
			yyVAL.colTuple = ValTuple(yyS[yypt-1].valExprs)
		}
	case 120:
		//line sql.y:663
		{
			yyVAL.colTuple = yyS[yypt-0].subquery
		}
	case 121:
		//line sql.y:667
		{
			yyVAL.colTuple = ListArg(yyS[yypt-0].bytes)
		}
	case 122:
		//line sql.y:673
		{
			yyVAL.subquery = &Subquery{yyS[yypt-1].selStmt}
		}
	case 123:
		//line sql.y:679
		{
			yyVAL.valExprs = ValExprs{yyS[yypt-0].valExpr}
		}
	case 124:
		//line sql.y:683
		{
			yyVAL.valExprs = append(yyS[yypt-2].valExprs, yyS[yypt-0].valExpr)
		}
	case 125:
		//line sql.y:689
		{
			yyVAL.valExpr = yyS[yypt-0].valExpr
		}
	case 126:
		//line sql.y:693
		{
			yyVAL.valExpr = yyS[yypt-0].colName
		}
	case 127:
		//line sql.y:697
		{
			yyVAL.valExpr = yyS[yypt-0].rowTuple
		}
	case 128:
		//line sql.y:701
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyS[yypt-2].valExpr, Operator: AST_BITAND, Right: yyS[yypt-0].valExpr}
		}
	case 129:
		//line sql.y:705
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyS[yypt-2].valExpr, Operator: AST_BITOR, Right: yyS[yypt-0].valExpr}
		}
	case 130:
		//line sql.y:709
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyS[yypt-2].valExpr, Operator: AST_BITXOR, Right: yyS[yypt-0].valExpr}
		}
	case 131:
		//line sql.y:713
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyS[yypt-2].valExpr, Operator: AST_PLUS, Right: yyS[yypt-0].valExpr}
		}
	case 132:
		//line sql.y:717
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyS[yypt-2].valExpr, Operator: AST_MINUS, Right: yyS[yypt-0].valExpr}
		}
	case 133:
		//line sql.y:721
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyS[yypt-2].valExpr, Operator: AST_MULT, Right: yyS[yypt-0].valExpr}
		}
	case 134:
		//line sql.y:725
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyS[yypt-2].valExpr, Operator: AST_DIV, Right: yyS[yypt-0].valExpr}
		}
	case 135:
		//line sql.y:729
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyS[yypt-2].valExpr, Operator: AST_MOD, Right: yyS[yypt-0].valExpr}
		}
	case 136:
		//line sql.y:733
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
	case 137:
		//line sql.y:748
		{
			yyVAL.valExpr = &FuncExpr{Name: yyS[yypt-2].bytes}
		}
	case 138:
		//line sql.y:752
		{
			yyVAL.valExpr = &FuncExpr{Name: yyS[yypt-3].bytes, Exprs: yyS[yypt-1].selectExprs}
		}
	case 139:
		//line sql.y:756
		{
			yyVAL.valExpr = &FuncExpr{Name: yyS[yypt-4].bytes, Distinct: true, Exprs: yyS[yypt-1].selectExprs}
		}
	case 140:
		//line sql.y:760
		{
			yyVAL.valExpr = &FuncExpr{Name: yyS[yypt-3].bytes, Exprs: yyS[yypt-1].selectExprs}
		}
	case 141:
		//line sql.y:764
		{
			yyVAL.valExpr = yyS[yypt-0].caseExpr
		}
	case 142:
		//line sql.y:770
		{
			yyVAL.bytes = IF_BYTES
		}
	case 143:
		//line sql.y:774
		{
			yyVAL.bytes = VALUES_BYTES
		}
	case 144:
		//line sql.y:780
		{
			yyVAL.byt = AST_UPLUS
		}
	case 145:
		//line sql.y:784
		{
			yyVAL.byt = AST_UMINUS
		}
	case 146:
		//line sql.y:788
		{
			yyVAL.byt = AST_TILDA
		}
	case 147:
		//line sql.y:794
		{
			yyVAL.caseExpr = &CaseExpr{Expr: yyS[yypt-3].valExpr, Whens: yyS[yypt-2].whens, Else: yyS[yypt-1].valExpr}
		}
	case 148:
		//line sql.y:799
		{
			yyVAL.valExpr = nil
		}
	case 149:
		//line sql.y:803
		{
			yyVAL.valExpr = yyS[yypt-0].valExpr
		}
	case 150:
		//line sql.y:809
		{
			yyVAL.whens = []*When{yyS[yypt-0].when}
		}
	case 151:
		//line sql.y:813
		{
			yyVAL.whens = append(yyS[yypt-1].whens, yyS[yypt-0].when)
		}
	case 152:
		//line sql.y:819
		{
			yyVAL.when = &When{Cond: yyS[yypt-2].boolExpr, Val: yyS[yypt-0].valExpr}
		}
	case 153:
		//line sql.y:824
		{
			yyVAL.valExpr = nil
		}
	case 154:
		//line sql.y:828
		{
			yyVAL.valExpr = yyS[yypt-0].valExpr
		}
	case 155:
		//line sql.y:834
		{
			yyVAL.colName = &ColName{Name: yyS[yypt-0].bytes}
		}
	case 156:
		//line sql.y:838
		{
			yyVAL.colName = &ColName{Qualifier: yyS[yypt-2].bytes, Name: yyS[yypt-0].bytes}
		}
	case 157:
		//line sql.y:844
		{
			yyVAL.valExpr = StrVal(yyS[yypt-0].bytes)
		}
	case 158:
		//line sql.y:848
		{
			yyVAL.valExpr = NumVal(yyS[yypt-0].bytes)
		}
	case 159:
		//line sql.y:852
		{
			yyVAL.valExpr = ValArg(yyS[yypt-0].bytes)
		}
	case 160:
		//line sql.y:856
		{
			yyVAL.valExpr = &NullVal{}
		}
	case 161:
		//line sql.y:860
		{
			yyVAL.valExpr = &TrueVal{}
		}
	case 162:
		//line sql.y:864
		{
			yyVAL.valExpr = &FalseVal{}
		}
	case 163:
		//line sql.y:869
		{
			yyVAL.valExprs = nil
		}
	case 164:
		//line sql.y:873
		{
			yyVAL.valExprs = yyS[yypt-0].valExprs
		}
	case 165:
		//line sql.y:878
		{
			yyVAL.boolExpr = nil
		}
	case 166:
		//line sql.y:882
		{
			yyVAL.boolExpr = yyS[yypt-0].boolExpr
		}
	case 167:
		//line sql.y:887
		{
			yyVAL.orderBy = nil
		}
	case 168:
		//line sql.y:891
		{
			yyVAL.orderBy = yyS[yypt-0].orderBy
		}
	case 169:
		//line sql.y:897
		{
			yyVAL.orderBy = OrderBy{yyS[yypt-0].order}
		}
	case 170:
		//line sql.y:901
		{
			yyVAL.orderBy = append(yyS[yypt-2].orderBy, yyS[yypt-0].order)
		}
	case 171:
		//line sql.y:907
		{
			yyVAL.order = &Order{Expr: yyS[yypt-1].valExpr, Direction: yyS[yypt-0].str}
		}
	case 172:
		//line sql.y:912
		{
			yyVAL.str = AST_ASC
		}
	case 173:
		//line sql.y:916
		{
			yyVAL.str = AST_ASC
		}
	case 174:
		//line sql.y:920
		{
			yyVAL.str = AST_DESC
		}
	case 175:
		//line sql.y:925
		{
			yyVAL.limit = nil
		}
	case 176:
		//line sql.y:929
		{
			yyVAL.limit = &Limit{Rowcount: yyS[yypt-0].valExpr}
		}
	case 177:
		//line sql.y:933
		{
			yyVAL.limit = &Limit{Offset: yyS[yypt-2].valExpr, Rowcount: yyS[yypt-0].valExpr}
		}
	case 178:
		//line sql.y:938
		{
			yyVAL.str = ""
		}
	case 179:
		//line sql.y:942
		{
			yyVAL.str = AST_FOR_UPDATE
		}
	case 180:
		//line sql.y:946
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
	case 181:
		//line sql.y:959
		{
			yyVAL.columns = nil
		}
	case 182:
		//line sql.y:963
		{
			yyVAL.columns = yyS[yypt-1].columns
		}
	case 183:
		//line sql.y:969
		{
			yyVAL.columns = Columns{&NonStarExpr{Expr: yyS[yypt-0].colName}}
		}
	case 184:
		//line sql.y:973
		{
			yyVAL.columns = append(yyVAL.columns, &NonStarExpr{Expr: yyS[yypt-0].colName})
		}
	case 185:
		//line sql.y:978
		{
			yyVAL.updateExprs = nil
		}
	case 186:
		//line sql.y:982
		{
			yyVAL.updateExprs = yyS[yypt-0].updateExprs
		}
	case 187:
		//line sql.y:988
		{
			yyVAL.insRows = yyS[yypt-0].values
		}
	case 188:
		//line sql.y:992
		{
			yyVAL.insRows = yyS[yypt-0].selStmt
		}
	case 189:
		//line sql.y:998
		{
			yyVAL.values = Values{yyS[yypt-0].rowTuple}
		}
	case 190:
		//line sql.y:1002
		{
			yyVAL.values = append(yyS[yypt-2].values, yyS[yypt-0].rowTuple)
		}
	case 191:
		//line sql.y:1008
		{
			yyVAL.rowTuple = ValTuple(yyS[yypt-1].valExprs)
		}
	case 192:
		//line sql.y:1012
		{
			yyVAL.rowTuple = yyS[yypt-0].subquery
		}
	case 193:
		//line sql.y:1018
		{
			yyVAL.updateExprs = UpdateExprs{yyS[yypt-0].updateExpr}
		}
	case 194:
		//line sql.y:1022
		{
			yyVAL.updateExprs = append(yyS[yypt-2].updateExprs, yyS[yypt-0].updateExpr)
		}
	case 195:
		//line sql.y:1028
		{
			yyVAL.updateExpr = &UpdateExpr{Name: yyS[yypt-2].colName, Expr: yyS[yypt-0].valExpr}
		}
	case 196:
		//line sql.y:1033
		{
			yyVAL.empty = struct{}{}
		}
	case 197:
		//line sql.y:1035
		{
			yyVAL.empty = struct{}{}
		}
	case 198:
		//line sql.y:1038
		{
			yyVAL.empty = struct{}{}
		}
	case 199:
		//line sql.y:1040
		{
			yyVAL.empty = struct{}{}
		}
	case 200:
		//line sql.y:1043
		{
			yyVAL.empty = struct{}{}
		}
	case 201:
		//line sql.y:1045
		{
			yyVAL.empty = struct{}{}
		}
	case 202:
		//line sql.y:1049
		{
			yyVAL.empty = struct{}{}
		}
	case 203:
		//line sql.y:1051
		{
			yyVAL.empty = struct{}{}
		}
	case 204:
		//line sql.y:1053
		{
			yyVAL.empty = struct{}{}
		}
	case 205:
		//line sql.y:1055
		{
			yyVAL.empty = struct{}{}
		}
	case 206:
		//line sql.y:1057
		{
			yyVAL.empty = struct{}{}
		}
	case 207:
		//line sql.y:1060
		{
			yyVAL.empty = struct{}{}
		}
	case 208:
		//line sql.y:1062
		{
			yyVAL.empty = struct{}{}
		}
	case 209:
		//line sql.y:1065
		{
			yyVAL.empty = struct{}{}
		}
	case 210:
		//line sql.y:1067
		{
			yyVAL.empty = struct{}{}
		}
	case 211:
		//line sql.y:1070
		{
			yyVAL.empty = struct{}{}
		}
	case 212:
		//line sql.y:1072
		{
			yyVAL.empty = struct{}{}
		}
	case 213:
		//line sql.y:1076
		{
			yyVAL.bytes = bytes.ToLower(yyS[yypt-0].bytes)
		}
	case 214:
		//line sql.y:1081
		{
			ForceEOF(yylex)
		}
	}
	goto yystack /* stack new state and value */
}
