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
const TABLE = 57414
const INDEX = 57415
const VIEW = 57416
const TO = 57417
const IGNORE = 57418
const IF = 57419
const UNIQUE = 57420
const USING = 57421

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
	"TABLE",
	"INDEX",
	"VIEW",
	"TO",
	"IGNORE",
	"IF",
	"UNIQUE",
	"USING",
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

const yyNprod = 193
const yyPrivate = 57344

var yyTokenNames []string
var yyStates []string

const yyLast = 531

var yyAct = []int{

	83, 275, 147, 342, 80, 74, 310, 149, 52, 91,
	231, 185, 267, 222, 70, 165, 53, 351, 173, 351,
	81, 66, 69, 150, 3, 58, 238, 242, 243, 244,
	245, 246, 55, 247, 248, 60, 54, 219, 63, 86,
	123, 124, 67, 112, 90, 43, 351, 96, 75, 321,
	320, 118, 109, 273, 73, 87, 88, 89, 319, 353,
	108, 352, 59, 78, 62, 292, 294, 94, 296, 116,
	212, 118, 32, 120, 34, 118, 214, 37, 35, 38,
	223, 151, 39, 146, 148, 152, 77, 301, 350, 253,
	92, 93, 71, 300, 293, 272, 156, 97, 123, 124,
	159, 55, 122, 105, 55, 54, 169, 168, 54, 163,
	223, 95, 265, 262, 217, 101, 111, 260, 213, 107,
	167, 75, 191, 169, 40, 41, 42, 189, 195, 193,
	194, 200, 201, 190, 204, 205, 206, 207, 208, 209,
	210, 211, 90, 202, 316, 96, 268, 196, 22, 23,
	24, 25, 56, 87, 88, 89, 216, 75, 75, 170,
	61, 153, 55, 55, 234, 94, 54, 229, 115, 183,
	227, 166, 233, 318, 235, 123, 124, 218, 220, 230,
	136, 137, 138, 317, 226, 203, 104, 286, 92, 93,
	303, 284, 287, 179, 215, 97, 285, 189, 192, 252,
	239, 255, 256, 134, 135, 136, 137, 138, 103, 95,
	290, 289, 177, 288, 240, 180, 166, 259, 254, 268,
	103, 214, 75, 22, 23, 24, 25, 236, 327, 266,
	242, 243, 244, 245, 246, 264, 247, 248, 305, 86,
	271, 274, 261, 117, 90, 12, 270, 96, 337, 336,
	189, 189, 282, 283, 73, 87, 88, 89, 49, 103,
	299, 188, 161, 78, 176, 178, 175, 94, 302, 335,
	187, 153, 157, 162, 55, 188, 307, 155, 306, 308,
	311, 154, 12, 98, 187, 65, 77, 118, 251, 61,
	92, 93, 71, 121, 56, 297, 295, 97, 348, 279,
	278, 322, 90, 182, 250, 96, 323, 99, 181, 61,
	102, 95, 56, 87, 88, 89, 349, 324, 216, 164,
	332, 153, 334, 333, 331, 94, 325, 113, 68, 339,
	311, 312, 110, 341, 340, 106, 343, 343, 343, 55,
	344, 345, 12, 54, 50, 64, 100, 346, 92, 93,
	304, 48, 356, 12, 258, 97, 357, 86, 358, 355,
	46, 197, 90, 198, 199, 96, 171, 114, 44, 95,
	276, 315, 56, 87, 88, 89, 225, 277, 232, 86,
	314, 78, 281, 166, 90, 94, 51, 96, 354, 338,
	12, 27, 172, 33, 56, 87, 88, 89, 237, 12,
	13, 14, 15, 78, 77, 174, 36, 94, 92, 93,
	329, 330, 57, 228, 160, 97, 347, 328, 26, 126,
	130, 128, 129, 309, 313, 280, 77, 16, 263, 95,
	92, 93, 28, 29, 30, 31, 158, 97, 142, 143,
	144, 145, 221, 139, 140, 141, 85, 82, 84, 269,
	79, 95, 131, 132, 133, 134, 135, 136, 137, 138,
	224, 125, 76, 291, 186, 127, 131, 132, 133, 134,
	135, 136, 137, 138, 326, 241, 184, 17, 18, 20,
	19, 131, 132, 133, 134, 135, 136, 137, 138, 131,
	132, 133, 134, 135, 136, 137, 138, 298, 72, 249,
	131, 132, 133, 134, 135, 136, 137, 138, 257, 119,
	45, 131, 132, 133, 134, 135, 136, 137, 138, 21,
	47, 11, 10, 9, 8, 7, 6, 5, 4, 2,
	1,
}
var yyPact = []int{

	394, -1000, -1000, 174, -1000, -1000, -1000, -1000, -1000, -1000,
	-1000, -1000, -1000, -1000, -1000, -1000, -1000, -15, -12, -5,
	37, 385, 351, -1000, -1000, -1000, 342, -1000, 322, 309,
	377, 259, -67, -26, 254, -1000, -23, 254, -1000, 310,
	-71, 254, -71, -1000, -1000, 219, -1000, 244, 309, 313,
	39, 309, 167, -1000, 141, -1000, 27, 300, 52, 254,
	-1000, -1000, 297, -1000, -47, 292, 347, 104, 254, 234,
	-1000, -1000, 274, 26, 33, 398, -1000, 359, 337, -1000,
	-1000, -1000, 117, 237, 233, -1000, 228, -1000, -1000, -1000,
	-1000, -1000, -1000, -1000, -1000, -1000, -1000, 117, -1000, 229,
	259, 284, 373, 259, 117, 254, -1000, 346, -76, -1000,
	180, -1000, 273, -1000, -1000, 268, -1000, 226, 219, -1000,
	-1000, 254, 125, 359, 359, 117, 227, 340, 117, 117,
	118, 117, 117, 117, 117, 117, 117, 117, 117, -1000,
	-1000, -1000, -1000, -1000, -1000, -1000, -1000, 398, -25, 23,
	99, 398, -1000, 277, 19, 219, -1000, 385, 1, 413,
	348, 259, 259, 206, -1000, 365, 359, -1000, 413, -1000,
	-1000, -1000, 100, 254, -1000, -64, -1000, -1000, -1000, -1000,
	-1000, -1000, -1000, -1000, 161, 176, 269, 240, 13, -1000,
	-1000, -1000, -1000, -1000, -1000, 413, -1000, 227, 117, 117,
	413, 443, -1000, 329, 132, 132, 132, 107, 107, -1000,
	-1000, -1000, -1000, -1000, 117, -1000, 413, -1000, 22, 219,
	18, 31, -1000, 359, 82, 227, 174, 155, 0, -1000,
	365, 355, 363, 33, 265, -1000, -1000, 264, -1000, 371,
	226, 226, -1000, -1000, 137, 133, 159, 157, 156, 3,
	-1000, 261, -27, 260, -1000, 413, 432, 117, -1000, 413,
	-1000, -2, -1000, 5, -1000, 117, 110, -1000, 320, 185,
	-1000, -1000, -1000, 259, 355, -1000, 117, 117, -1000, -1000,
	368, 357, 176, 80, -1000, 129, -1000, 119, -1000, -1000,
	-1000, -1000, -30, -38, -39, -1000, -1000, -1000, 117, 413,
	-1000, -1000, 413, 117, 286, 227, -1000, -1000, 421, 175,
	-1000, 384, -1000, 365, 359, 117, 359, -1000, -1000, 225,
	205, 204, 413, 413, 382, -1000, 117, 117, -1000, -1000,
	-1000, 355, 33, 168, 33, 254, 254, 254, 259, 413,
	-1000, 282, -7, -1000, -34, -36, 167, -1000, 381, 338,
	-1000, 254, -1000, -1000, -1000, 254, -1000, 254, -1000,
}
var yyPgo = []int{

	0, 530, 529, 23, 528, 527, 526, 525, 524, 523,
	522, 521, 418, 520, 519, 510, 22, 14, 509, 499,
	498, 476, 11, 475, 464, 258, 463, 3, 15, 5,
	462, 461, 460, 450, 2, 20, 7, 449, 448, 9,
	447, 4, 446, 442, 13, 436, 428, 425, 424, 10,
	423, 6, 417, 1, 416, 414, 413, 12, 8, 16,
	285, 412, 406, 405, 398, 393, 392, 0, 52, 391,
}
var yyR1 = []int{

	0, 1, 2, 2, 2, 2, 2, 2, 2, 2,
	2, 3, 3, 4, 4, 5, 6, 7, 8, 8,
	8, 9, 9, 9, 10, 11, 11, 11, 69, 12,
	13, 13, 14, 14, 14, 14, 14, 15, 15, 16,
	16, 17, 17, 17, 20, 20, 18, 18, 18, 21,
	21, 22, 22, 22, 22, 19, 19, 19, 23, 23,
	23, 23, 23, 23, 23, 23, 23, 24, 24, 24,
	25, 25, 26, 26, 26, 26, 27, 27, 28, 28,
	29, 29, 29, 29, 29, 30, 30, 30, 30, 30,
	30, 30, 30, 30, 30, 31, 31, 31, 31, 31,
	31, 31, 32, 32, 37, 37, 35, 35, 39, 36,
	36, 34, 34, 34, 34, 34, 34, 34, 34, 34,
	34, 34, 34, 34, 34, 34, 34, 34, 38, 38,
	40, 40, 40, 42, 45, 45, 43, 43, 44, 46,
	46, 41, 41, 33, 33, 33, 33, 47, 47, 48,
	48, 49, 49, 50, 50, 51, 52, 52, 52, 53,
	53, 53, 54, 54, 54, 55, 55, 56, 56, 57,
	57, 58, 58, 59, 60, 60, 61, 61, 62, 62,
	63, 63, 63, 63, 63, 64, 64, 65, 65, 66,
	66, 67, 68,
}
var yyR2 = []int{

	0, 1, 1, 1, 1, 1, 1, 1, 1, 1,
	1, 12, 3, 7, 7, 8, 7, 3, 5, 8,
	4, 6, 7, 4, 5, 4, 5, 5, 0, 2,
	0, 2, 1, 2, 1, 1, 1, 0, 1, 1,
	3, 1, 2, 3, 1, 1, 0, 1, 2, 1,
	3, 3, 3, 3, 5, 0, 1, 2, 1, 1,
	2, 3, 2, 3, 2, 2, 2, 1, 3, 1,
	1, 3, 0, 5, 5, 5, 1, 3, 0, 2,
	1, 3, 3, 2, 3, 3, 3, 4, 3, 4,
	5, 6, 3, 4, 2, 1, 1, 1, 1, 1,
	1, 1, 2, 1, 1, 3, 3, 1, 3, 1,
	3, 1, 1, 1, 3, 3, 3, 3, 3, 3,
	3, 3, 2, 3, 4, 5, 4, 1, 1, 1,
	1, 1, 1, 5, 0, 1, 1, 2, 4, 0,
	2, 1, 3, 1, 1, 1, 1, 0, 3, 0,
	2, 0, 3, 1, 3, 2, 0, 1, 1, 0,
	2, 4, 0, 2, 4, 0, 3, 1, 3, 0,
	5, 1, 3, 3, 0, 2, 0, 3, 0, 1,
	1, 1, 1, 1, 1, 0, 1, 0, 1, 0,
	2, 1, 0,
}
var yyChk = []int{

	-1000, -1, -2, -3, -4, -5, -6, -7, -8, -9,
	-10, -11, 5, 6, 7, 8, 33, 83, 84, 86,
	85, -14, 49, 50, 51, 52, -12, -69, -12, -12,
	-12, -12, 87, -65, 89, 93, -62, 89, 91, 87,
	87, 88, 89, -3, 17, -15, 18, -13, 29, -25,
	35, 9, -58, -59, -41, -67, 35, -61, 92, 88,
	-67, 35, 87, -67, 35, -60, 92, -67, -60, -16,
	-17, 73, -20, 35, -29, -34, -30, 67, 44, -33,
	-41, -35, -40, -67, -38, -42, 20, 36, 37, 38,
	25, -39, 71, 72, 48, 92, 28, 78, 39, -25,
	33, 76, -25, 53, 45, 76, 35, 67, -67, -68,
	35, -68, 90, 35, 20, 64, -67, 9, 53, -18,
	-67, 19, 76, 65, 66, -31, 21, 67, 23, 24,
	22, 68, 69, 70, 71, 72, 73, 74, 75, 45,
	46, 47, 40, 41, 42, 43, -29, -34, -29, -36,
	-3, -34, -34, 44, 44, 44, -39, 44, -45, -34,
	-55, 33, 44, -58, 35, -28, 10, -59, -34, -67,
	-68, 20, -66, 94, -63, 86, 84, 32, 85, 13,
	35, 35, 35, -68, -21, -22, -24, 44, 35, -39,
	-17, -67, 73, -29, -29, -34, -35, 21, 23, 24,
	-34, -34, 25, 67, -34, -34, -34, -34, -34, -34,
	-34, -34, 95, 95, 53, 95, -34, 95, -16, 18,
	-16, -43, -44, 79, -32, 28, -3, -58, -56, -41,
	-28, -49, 13, -29, 64, -67, -68, -64, 90, -28,
	53, -23, 54, 55, 56, 57, 58, 60, 61, -19,
	35, 19, -22, 76, -35, -34, -34, 65, 25, -34,
	95, -16, 95, -46, -44, 81, -29, -57, 64, -37,
	-35, -57, 95, 53, -49, -53, 15, 14, 35, 35,
	-47, 11, -22, -22, 54, 59, 54, 59, 54, 54,
	54, -26, 62, 91, 63, 35, 95, 35, 65, -34,
	95, 82, -34, 80, 30, 53, -41, -53, -34, -50,
	-51, -34, -68, -48, 12, 14, 64, 54, 54, 88,
	88, 88, -34, -34, 31, -35, 53, 53, -52, 26,
	27, -49, -29, -36, -29, 44, 44, 44, 7, -34,
	-51, -53, -27, -67, -27, -27, -58, -54, 16, 34,
	95, 53, 95, 95, 7, 21, -67, -67, -67,
}
var yyDef = []int{

	0, -2, 1, 2, 3, 4, 5, 6, 7, 8,
	9, 10, 28, 28, 28, 28, 28, 187, 178, 0,
	0, 0, 32, 34, 35, 36, 37, 30, 0, 0,
	0, 0, 176, 0, 0, 188, 0, 0, 179, 0,
	174, 0, 174, 12, 33, 0, 38, 29, 0, 0,
	70, 0, 17, 171, 0, 141, 191, 0, 0, 0,
	192, 191, 0, 192, 0, 0, 0, 0, 0, 0,
	39, 41, 46, 191, 44, 45, 80, 0, 0, 111,
	112, 113, 0, 141, 0, 127, 0, 143, 144, 145,
	146, 107, 130, 131, 132, 128, 129, 134, 31, 165,
	0, 0, 78, 0, 0, 0, 192, 0, 189, 20,
	0, 23, 0, 25, 175, 0, 192, 0, 0, 42,
	47, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 95,
	96, 97, 98, 99, 100, 101, 83, 0, 0, 0,
	0, 109, 122, 0, 0, 0, 94, 0, 0, 135,
	0, 0, 0, 78, 71, 151, 0, 172, 173, 142,
	18, 177, 0, 0, 192, 185, 180, 181, 182, 183,
	184, 24, 26, 27, 78, 49, 55, 0, 67, 69,
	40, 48, 43, 81, 82, 85, 86, 0, 0, 0,
	88, 0, 92, 0, 114, 115, 116, 117, 118, 119,
	120, 121, 84, 106, 0, 108, 109, 123, 0, 0,
	0, 139, 136, 0, 169, 0, 103, 169, 0, 167,
	151, 159, 0, 79, 0, 190, 21, 0, 186, 147,
	0, 0, 58, 59, 0, 0, 0, 0, 0, 72,
	56, 0, 0, 0, 87, 89, 0, 0, 93, 110,
	124, 0, 126, 0, 137, 0, 0, 13, 0, 102,
	104, 14, 166, 0, 159, 16, 0, 0, 192, 22,
	149, 0, 50, 53, 60, 0, 62, 0, 64, 65,
	66, 51, 0, 0, 0, 57, 52, 68, 0, 90,
	125, 133, 140, 0, 0, 0, 168, 15, 160, 152,
	153, 156, 19, 151, 0, 0, 0, 61, 63, 0,
	0, 0, 91, 138, 0, 105, 0, 0, 155, 157,
	158, 159, 150, 148, 54, 0, 0, 0, 0, 161,
	154, 162, 0, 76, 0, 0, 170, 11, 0, 0,
	73, 0, 74, 75, 163, 0, 77, 0, 164,
}
var yyTok1 = []int{

	1, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 75, 68, 3,
	44, 95, 73, 71, 53, 72, 76, 74, 3, 3,
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
	87, 88, 89, 90, 91, 92, 93, 94,
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
			SetParseTree(yylex, yyS[yypt-0].statement)
		}
	case 2:
		//line sql.y:152
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
		//line sql.y:166
		{
			yyVAL.selStmt = &Select{Comments: Comments(yyS[yypt-10].bytes2), Distinct: yyS[yypt-9].str, SelectExprs: yyS[yypt-8].selectExprs, From: yyS[yypt-6].tableExprs, Where: NewWhere(AST_WHERE, yyS[yypt-5].boolExpr), GroupBy: GroupBy(yyS[yypt-4].valExprs), Having: NewWhere(AST_HAVING, yyS[yypt-3].boolExpr), OrderBy: yyS[yypt-2].orderBy, Limit: yyS[yypt-1].limit, Lock: yyS[yypt-0].str}
		}
	case 12:
		//line sql.y:170
		{
			yyVAL.selStmt = &Union{Type: yyS[yypt-1].str, Left: yyS[yypt-2].selStmt, Right: yyS[yypt-0].selStmt}
		}
	case 13:
		//line sql.y:176
		{
			yyVAL.statement = &Insert{Comments: Comments(yyS[yypt-5].bytes2), Table: yyS[yypt-3].tableName, Columns: yyS[yypt-2].columns, Rows: yyS[yypt-1].insRows, OnDup: OnDup(yyS[yypt-0].updateExprs)}
		}
	case 14:
		//line sql.y:180
		{
			cols := make(Columns, 0, len(yyS[yypt-1].updateExprs))
			vals := make(ValTuple, 0, len(yyS[yypt-1].updateExprs))
			for _, col := range yyS[yypt-1].updateExprs {
				cols = append(cols, &NonStarExpr{Expr: col.Name})
				vals = append(vals, col.Expr)
			}
			yyVAL.statement = &Insert{Comments: Comments(yyS[yypt-5].bytes2), Table: yyS[yypt-3].tableName, Columns: cols, Rows: Values{vals}, OnDup: OnDup(yyS[yypt-0].updateExprs)}
		}
	case 15:
		//line sql.y:192
		{
			yyVAL.statement = &Update{Comments: Comments(yyS[yypt-6].bytes2), Table: yyS[yypt-5].tableName, Exprs: yyS[yypt-3].updateExprs, Where: NewWhere(AST_WHERE, yyS[yypt-2].boolExpr), OrderBy: yyS[yypt-1].orderBy, Limit: yyS[yypt-0].limit}
		}
	case 16:
		//line sql.y:198
		{
			yyVAL.statement = &Delete{Comments: Comments(yyS[yypt-5].bytes2), Table: yyS[yypt-3].tableName, Where: NewWhere(AST_WHERE, yyS[yypt-2].boolExpr), OrderBy: yyS[yypt-1].orderBy, Limit: yyS[yypt-0].limit}
		}
	case 17:
		//line sql.y:204
		{
			yyVAL.statement = &Set{Comments: Comments(yyS[yypt-1].bytes2), Exprs: yyS[yypt-0].updateExprs}
		}
	case 18:
		//line sql.y:210
		{
			yyVAL.statement = &DDL{Action: AST_CREATE, NewName: yyS[yypt-1].bytes}
		}
	case 19:
		//line sql.y:214
		{
			// Change this to an alter statement
			yyVAL.statement = &DDL{Action: AST_ALTER, Table: yyS[yypt-1].bytes, NewName: yyS[yypt-1].bytes}
		}
	case 20:
		//line sql.y:219
		{
			yyVAL.statement = &DDL{Action: AST_CREATE, NewName: yyS[yypt-1].bytes}
		}
	case 21:
		//line sql.y:225
		{
			yyVAL.statement = &DDL{Action: AST_ALTER, Table: yyS[yypt-2].bytes, NewName: yyS[yypt-2].bytes}
		}
	case 22:
		//line sql.y:229
		{
			// Change this to a rename statement
			yyVAL.statement = &DDL{Action: AST_RENAME, Table: yyS[yypt-3].bytes, NewName: yyS[yypt-0].bytes}
		}
	case 23:
		//line sql.y:234
		{
			yyVAL.statement = &DDL{Action: AST_ALTER, Table: yyS[yypt-1].bytes, NewName: yyS[yypt-1].bytes}
		}
	case 24:
		//line sql.y:240
		{
			yyVAL.statement = &DDL{Action: AST_RENAME, Table: yyS[yypt-2].bytes, NewName: yyS[yypt-0].bytes}
		}
	case 25:
		//line sql.y:246
		{
			yyVAL.statement = &DDL{Action: AST_DROP, Table: yyS[yypt-0].bytes}
		}
	case 26:
		//line sql.y:250
		{
			// Change this to an alter statement
			yyVAL.statement = &DDL{Action: AST_ALTER, Table: yyS[yypt-0].bytes, NewName: yyS[yypt-0].bytes}
		}
	case 27:
		//line sql.y:255
		{
			yyVAL.statement = &DDL{Action: AST_DROP, Table: yyS[yypt-1].bytes}
		}
	case 28:
		//line sql.y:260
		{
			SetAllowComments(yylex, true)
		}
	case 29:
		//line sql.y:264
		{
			yyVAL.bytes2 = yyS[yypt-0].bytes2
			SetAllowComments(yylex, false)
		}
	case 30:
		//line sql.y:270
		{
			yyVAL.bytes2 = nil
		}
	case 31:
		//line sql.y:274
		{
			yyVAL.bytes2 = append(yyS[yypt-1].bytes2, yyS[yypt-0].bytes)
		}
	case 32:
		//line sql.y:280
		{
			yyVAL.str = AST_UNION
		}
	case 33:
		//line sql.y:284
		{
			yyVAL.str = AST_UNION_ALL
		}
	case 34:
		//line sql.y:288
		{
			yyVAL.str = AST_SET_MINUS
		}
	case 35:
		//line sql.y:292
		{
			yyVAL.str = AST_EXCEPT
		}
	case 36:
		//line sql.y:296
		{
			yyVAL.str = AST_INTERSECT
		}
	case 37:
		//line sql.y:301
		{
			yyVAL.str = ""
		}
	case 38:
		//line sql.y:305
		{
			yyVAL.str = AST_DISTINCT
		}
	case 39:
		//line sql.y:311
		{
			yyVAL.selectExprs = SelectExprs{yyS[yypt-0].selectExpr}
		}
	case 40:
		//line sql.y:315
		{
			yyVAL.selectExprs = append(yyVAL.selectExprs, yyS[yypt-0].selectExpr)
		}
	case 41:
		//line sql.y:321
		{
			yyVAL.selectExpr = &StarExpr{}
		}
	case 42:
		//line sql.y:325
		{
			yyVAL.selectExpr = &NonStarExpr{Expr: yyS[yypt-1].expr, As: yyS[yypt-0].bytes}
		}
	case 43:
		//line sql.y:329
		{
			yyVAL.selectExpr = &StarExpr{TableName: yyS[yypt-2].bytes}
		}
	case 44:
		//line sql.y:335
		{
			yyVAL.expr = yyS[yypt-0].boolExpr
		}
	case 45:
		//line sql.y:339
		{
			yyVAL.expr = yyS[yypt-0].valExpr
		}
	case 46:
		//line sql.y:344
		{
			yyVAL.bytes = nil
		}
	case 47:
		//line sql.y:348
		{
			yyVAL.bytes = yyS[yypt-0].bytes
		}
	case 48:
		//line sql.y:352
		{
			yyVAL.bytes = yyS[yypt-0].bytes
		}
	case 49:
		//line sql.y:358
		{
			yyVAL.tableExprs = TableExprs{yyS[yypt-0].tableExpr}
		}
	case 50:
		//line sql.y:362
		{
			yyVAL.tableExprs = append(yyVAL.tableExprs, yyS[yypt-0].tableExpr)
		}
	case 51:
		//line sql.y:368
		{
			yyVAL.tableExpr = &AliasedTableExpr{Expr: yyS[yypt-2].smTableExpr, As: yyS[yypt-1].bytes, Hints: yyS[yypt-0].indexHints}
		}
	case 52:
		//line sql.y:372
		{
			yyVAL.tableExpr = &ParenTableExpr{Expr: yyS[yypt-1].tableExpr}
		}
	case 53:
		//line sql.y:376
		{
			yyVAL.tableExpr = &JoinTableExpr{LeftExpr: yyS[yypt-2].tableExpr, Join: yyS[yypt-1].str, RightExpr: yyS[yypt-0].tableExpr}
		}
	case 54:
		//line sql.y:380
		{
			yyVAL.tableExpr = &JoinTableExpr{LeftExpr: yyS[yypt-4].tableExpr, Join: yyS[yypt-3].str, RightExpr: yyS[yypt-2].tableExpr, On: yyS[yypt-0].boolExpr}
		}
	case 55:
		//line sql.y:385
		{
			yyVAL.bytes = nil
		}
	case 56:
		//line sql.y:389
		{
			yyVAL.bytes = yyS[yypt-0].bytes
		}
	case 57:
		//line sql.y:393
		{
			yyVAL.bytes = yyS[yypt-0].bytes
		}
	case 58:
		//line sql.y:399
		{
			yyVAL.str = AST_JOIN
		}
	case 59:
		//line sql.y:403
		{
			yyVAL.str = AST_STRAIGHT_JOIN
		}
	case 60:
		//line sql.y:407
		{
			yyVAL.str = AST_LEFT_JOIN
		}
	case 61:
		//line sql.y:411
		{
			yyVAL.str = AST_LEFT_JOIN
		}
	case 62:
		//line sql.y:415
		{
			yyVAL.str = AST_RIGHT_JOIN
		}
	case 63:
		//line sql.y:419
		{
			yyVAL.str = AST_RIGHT_JOIN
		}
	case 64:
		//line sql.y:423
		{
			yyVAL.str = AST_JOIN
		}
	case 65:
		//line sql.y:427
		{
			yyVAL.str = AST_CROSS_JOIN
		}
	case 66:
		//line sql.y:431
		{
			yyVAL.str = AST_NATURAL_JOIN
		}
	case 67:
		//line sql.y:437
		{
			yyVAL.smTableExpr = &TableName{Name: yyS[yypt-0].bytes}
		}
	case 68:
		//line sql.y:441
		{
			yyVAL.smTableExpr = &TableName{Qualifier: yyS[yypt-2].bytes, Name: yyS[yypt-0].bytes}
		}
	case 69:
		//line sql.y:445
		{
			yyVAL.smTableExpr = yyS[yypt-0].subquery
		}
	case 70:
		//line sql.y:451
		{
			yyVAL.tableName = &TableName{Name: yyS[yypt-0].bytes}
		}
	case 71:
		//line sql.y:455
		{
			yyVAL.tableName = &TableName{Qualifier: yyS[yypt-2].bytes, Name: yyS[yypt-0].bytes}
		}
	case 72:
		//line sql.y:460
		{
			yyVAL.indexHints = nil
		}
	case 73:
		//line sql.y:464
		{
			yyVAL.indexHints = &IndexHints{Type: AST_USE, Indexes: yyS[yypt-1].bytes2}
		}
	case 74:
		//line sql.y:468
		{
			yyVAL.indexHints = &IndexHints{Type: AST_IGNORE, Indexes: yyS[yypt-1].bytes2}
		}
	case 75:
		//line sql.y:472
		{
			yyVAL.indexHints = &IndexHints{Type: AST_FORCE, Indexes: yyS[yypt-1].bytes2}
		}
	case 76:
		//line sql.y:478
		{
			yyVAL.bytes2 = [][]byte{yyS[yypt-0].bytes}
		}
	case 77:
		//line sql.y:482
		{
			yyVAL.bytes2 = append(yyS[yypt-2].bytes2, yyS[yypt-0].bytes)
		}
	case 78:
		//line sql.y:487
		{
			yyVAL.boolExpr = nil
		}
	case 79:
		//line sql.y:491
		{
			yyVAL.boolExpr = yyS[yypt-0].boolExpr
		}
	case 80:
		yyVAL.boolExpr = yyS[yypt-0].boolExpr
	case 81:
		//line sql.y:498
		{
			yyVAL.boolExpr = &AndExpr{Left: yyS[yypt-2].boolExpr, Right: yyS[yypt-0].boolExpr}
		}
	case 82:
		//line sql.y:502
		{
			yyVAL.boolExpr = &OrExpr{Left: yyS[yypt-2].boolExpr, Right: yyS[yypt-0].boolExpr}
		}
	case 83:
		//line sql.y:506
		{
			yyVAL.boolExpr = &NotExpr{Expr: yyS[yypt-0].boolExpr}
		}
	case 84:
		//line sql.y:510
		{
			yyVAL.boolExpr = &ParenBoolExpr{Expr: yyS[yypt-1].boolExpr}
		}
	case 85:
		//line sql.y:516
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyS[yypt-2].valExpr, Operator: yyS[yypt-1].str, Right: yyS[yypt-0].valExpr}
		}
	case 86:
		//line sql.y:520
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyS[yypt-2].valExpr, Operator: AST_IN, Right: yyS[yypt-0].tuple}
		}
	case 87:
		//line sql.y:524
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyS[yypt-3].valExpr, Operator: AST_NOT_IN, Right: yyS[yypt-0].tuple}
		}
	case 88:
		//line sql.y:528
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyS[yypt-2].valExpr, Operator: AST_LIKE, Right: yyS[yypt-0].valExpr}
		}
	case 89:
		//line sql.y:532
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyS[yypt-3].valExpr, Operator: AST_NOT_LIKE, Right: yyS[yypt-0].valExpr}
		}
	case 90:
		//line sql.y:536
		{
			yyVAL.boolExpr = &RangeCond{Left: yyS[yypt-4].valExpr, Operator: AST_BETWEEN, From: yyS[yypt-2].valExpr, To: yyS[yypt-0].valExpr}
		}
	case 91:
		//line sql.y:540
		{
			yyVAL.boolExpr = &RangeCond{Left: yyS[yypt-5].valExpr, Operator: AST_NOT_BETWEEN, From: yyS[yypt-2].valExpr, To: yyS[yypt-0].valExpr}
		}
	case 92:
		//line sql.y:544
		{
			yyVAL.boolExpr = &NullCheck{Operator: AST_IS_NULL, Expr: yyS[yypt-2].valExpr}
		}
	case 93:
		//line sql.y:548
		{
			yyVAL.boolExpr = &NullCheck{Operator: AST_IS_NOT_NULL, Expr: yyS[yypt-3].valExpr}
		}
	case 94:
		//line sql.y:552
		{
			yyVAL.boolExpr = &ExistsExpr{Subquery: yyS[yypt-0].subquery}
		}
	case 95:
		//line sql.y:558
		{
			yyVAL.str = AST_EQ
		}
	case 96:
		//line sql.y:562
		{
			yyVAL.str = AST_LT
		}
	case 97:
		//line sql.y:566
		{
			yyVAL.str = AST_GT
		}
	case 98:
		//line sql.y:570
		{
			yyVAL.str = AST_LE
		}
	case 99:
		//line sql.y:574
		{
			yyVAL.str = AST_GE
		}
	case 100:
		//line sql.y:578
		{
			yyVAL.str = AST_NE
		}
	case 101:
		//line sql.y:582
		{
			yyVAL.str = AST_NSE
		}
	case 102:
		//line sql.y:588
		{
			yyVAL.insRows = yyS[yypt-0].values
		}
	case 103:
		//line sql.y:592
		{
			yyVAL.insRows = yyS[yypt-0].selStmt
		}
	case 104:
		//line sql.y:598
		{
			yyVAL.values = Values{yyS[yypt-0].tuple}
		}
	case 105:
		//line sql.y:602
		{
			yyVAL.values = append(yyS[yypt-2].values, yyS[yypt-0].tuple)
		}
	case 106:
		//line sql.y:608
		{
			yyVAL.tuple = ValTuple(yyS[yypt-1].valExprs)
		}
	case 107:
		//line sql.y:612
		{
			yyVAL.tuple = yyS[yypt-0].subquery
		}
	case 108:
		//line sql.y:618
		{
			yyVAL.subquery = &Subquery{yyS[yypt-1].selStmt}
		}
	case 109:
		//line sql.y:624
		{
			yyVAL.valExprs = ValExprs{yyS[yypt-0].valExpr}
		}
	case 110:
		//line sql.y:628
		{
			yyVAL.valExprs = append(yyS[yypt-2].valExprs, yyS[yypt-0].valExpr)
		}
	case 111:
		//line sql.y:634
		{
			yyVAL.valExpr = yyS[yypt-0].valExpr
		}
	case 112:
		//line sql.y:638
		{
			yyVAL.valExpr = yyS[yypt-0].colName
		}
	case 113:
		//line sql.y:642
		{
			yyVAL.valExpr = yyS[yypt-0].tuple
		}
	case 114:
		//line sql.y:646
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyS[yypt-2].valExpr, Operator: AST_BITAND, Right: yyS[yypt-0].valExpr}
		}
	case 115:
		//line sql.y:650
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyS[yypt-2].valExpr, Operator: AST_BITOR, Right: yyS[yypt-0].valExpr}
		}
	case 116:
		//line sql.y:654
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyS[yypt-2].valExpr, Operator: AST_BITXOR, Right: yyS[yypt-0].valExpr}
		}
	case 117:
		//line sql.y:658
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyS[yypt-2].valExpr, Operator: AST_PLUS, Right: yyS[yypt-0].valExpr}
		}
	case 118:
		//line sql.y:662
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyS[yypt-2].valExpr, Operator: AST_MINUS, Right: yyS[yypt-0].valExpr}
		}
	case 119:
		//line sql.y:666
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyS[yypt-2].valExpr, Operator: AST_MULT, Right: yyS[yypt-0].valExpr}
		}
	case 120:
		//line sql.y:670
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyS[yypt-2].valExpr, Operator: AST_DIV, Right: yyS[yypt-0].valExpr}
		}
	case 121:
		//line sql.y:674
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyS[yypt-2].valExpr, Operator: AST_MOD, Right: yyS[yypt-0].valExpr}
		}
	case 122:
		//line sql.y:678
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
	case 123:
		//line sql.y:693
		{
			yyVAL.valExpr = &FuncExpr{Name: yyS[yypt-2].bytes}
		}
	case 124:
		//line sql.y:697
		{
			yyVAL.valExpr = &FuncExpr{Name: yyS[yypt-3].bytes, Exprs: yyS[yypt-1].selectExprs}
		}
	case 125:
		//line sql.y:701
		{
			yyVAL.valExpr = &FuncExpr{Name: yyS[yypt-4].bytes, Distinct: true, Exprs: yyS[yypt-1].selectExprs}
		}
	case 126:
		//line sql.y:705
		{
			yyVAL.valExpr = &FuncExpr{Name: yyS[yypt-3].bytes, Exprs: yyS[yypt-1].selectExprs}
		}
	case 127:
		//line sql.y:709
		{
			yyVAL.valExpr = yyS[yypt-0].caseExpr
		}
	case 128:
		//line sql.y:715
		{
			yyVAL.bytes = IF_BYTES
		}
	case 129:
		//line sql.y:719
		{
			yyVAL.bytes = VALUES_BYTES
		}
	case 130:
		//line sql.y:725
		{
			yyVAL.byt = AST_UPLUS
		}
	case 131:
		//line sql.y:729
		{
			yyVAL.byt = AST_UMINUS
		}
	case 132:
		//line sql.y:733
		{
			yyVAL.byt = AST_TILDA
		}
	case 133:
		//line sql.y:739
		{
			yyVAL.caseExpr = &CaseExpr{Expr: yyS[yypt-3].valExpr, Whens: yyS[yypt-2].whens, Else: yyS[yypt-1].valExpr}
		}
	case 134:
		//line sql.y:744
		{
			yyVAL.valExpr = nil
		}
	case 135:
		//line sql.y:748
		{
			yyVAL.valExpr = yyS[yypt-0].valExpr
		}
	case 136:
		//line sql.y:754
		{
			yyVAL.whens = []*When{yyS[yypt-0].when}
		}
	case 137:
		//line sql.y:758
		{
			yyVAL.whens = append(yyS[yypt-1].whens, yyS[yypt-0].when)
		}
	case 138:
		//line sql.y:764
		{
			yyVAL.when = &When{Cond: yyS[yypt-2].boolExpr, Val: yyS[yypt-0].valExpr}
		}
	case 139:
		//line sql.y:769
		{
			yyVAL.valExpr = nil
		}
	case 140:
		//line sql.y:773
		{
			yyVAL.valExpr = yyS[yypt-0].valExpr
		}
	case 141:
		//line sql.y:779
		{
			yyVAL.colName = &ColName{Name: yyS[yypt-0].bytes}
		}
	case 142:
		//line sql.y:783
		{
			yyVAL.colName = &ColName{Qualifier: yyS[yypt-2].bytes, Name: yyS[yypt-0].bytes}
		}
	case 143:
		//line sql.y:789
		{
			yyVAL.valExpr = StrVal(yyS[yypt-0].bytes)
		}
	case 144:
		//line sql.y:793
		{
			yyVAL.valExpr = NumVal(yyS[yypt-0].bytes)
		}
	case 145:
		//line sql.y:797
		{
			yyVAL.valExpr = ValArg(yyS[yypt-0].bytes)
		}
	case 146:
		//line sql.y:801
		{
			yyVAL.valExpr = &NullVal{}
		}
	case 147:
		//line sql.y:806
		{
			yyVAL.valExprs = nil
		}
	case 148:
		//line sql.y:810
		{
			yyVAL.valExprs = yyS[yypt-0].valExprs
		}
	case 149:
		//line sql.y:815
		{
			yyVAL.boolExpr = nil
		}
	case 150:
		//line sql.y:819
		{
			yyVAL.boolExpr = yyS[yypt-0].boolExpr
		}
	case 151:
		//line sql.y:824
		{
			yyVAL.orderBy = nil
		}
	case 152:
		//line sql.y:828
		{
			yyVAL.orderBy = yyS[yypt-0].orderBy
		}
	case 153:
		//line sql.y:834
		{
			yyVAL.orderBy = OrderBy{yyS[yypt-0].order}
		}
	case 154:
		//line sql.y:838
		{
			yyVAL.orderBy = append(yyS[yypt-2].orderBy, yyS[yypt-0].order)
		}
	case 155:
		//line sql.y:844
		{
			yyVAL.order = &Order{Expr: yyS[yypt-1].valExpr, Direction: yyS[yypt-0].str}
		}
	case 156:
		//line sql.y:849
		{
			yyVAL.str = AST_ASC
		}
	case 157:
		//line sql.y:853
		{
			yyVAL.str = AST_ASC
		}
	case 158:
		//line sql.y:857
		{
			yyVAL.str = AST_DESC
		}
	case 159:
		//line sql.y:862
		{
			yyVAL.limit = nil
		}
	case 160:
		//line sql.y:866
		{
			yyVAL.limit = &Limit{Rowcount: yyS[yypt-0].valExpr}
		}
	case 161:
		//line sql.y:870
		{
			yyVAL.limit = &Limit{Offset: yyS[yypt-2].valExpr, Rowcount: yyS[yypt-0].valExpr}
		}
	case 162:
		//line sql.y:875
		{
			yyVAL.str = ""
		}
	case 163:
		//line sql.y:879
		{
			yyVAL.str = AST_FOR_UPDATE
		}
	case 164:
		//line sql.y:883
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
	case 165:
		//line sql.y:896
		{
			yyVAL.columns = nil
		}
	case 166:
		//line sql.y:900
		{
			yyVAL.columns = yyS[yypt-1].columns
		}
	case 167:
		//line sql.y:906
		{
			yyVAL.columns = Columns{&NonStarExpr{Expr: yyS[yypt-0].colName}}
		}
	case 168:
		//line sql.y:910
		{
			yyVAL.columns = append(yyVAL.columns, &NonStarExpr{Expr: yyS[yypt-0].colName})
		}
	case 169:
		//line sql.y:915
		{
			yyVAL.updateExprs = nil
		}
	case 170:
		//line sql.y:919
		{
			yyVAL.updateExprs = yyS[yypt-0].updateExprs
		}
	case 171:
		//line sql.y:925
		{
			yyVAL.updateExprs = UpdateExprs{yyS[yypt-0].updateExpr}
		}
	case 172:
		//line sql.y:929
		{
			yyVAL.updateExprs = append(yyS[yypt-2].updateExprs, yyS[yypt-0].updateExpr)
		}
	case 173:
		//line sql.y:935
		{
			yyVAL.updateExpr = &UpdateExpr{Name: yyS[yypt-2].colName, Expr: yyS[yypt-0].valExpr}
		}
	case 174:
		//line sql.y:940
		{
			yyVAL.empty = struct{}{}
		}
	case 175:
		//line sql.y:942
		{
			yyVAL.empty = struct{}{}
		}
	case 176:
		//line sql.y:945
		{
			yyVAL.empty = struct{}{}
		}
	case 177:
		//line sql.y:947
		{
			yyVAL.empty = struct{}{}
		}
	case 178:
		//line sql.y:950
		{
			yyVAL.empty = struct{}{}
		}
	case 179:
		//line sql.y:952
		{
			yyVAL.empty = struct{}{}
		}
	case 180:
		//line sql.y:956
		{
			yyVAL.empty = struct{}{}
		}
	case 181:
		//line sql.y:958
		{
			yyVAL.empty = struct{}{}
		}
	case 182:
		//line sql.y:960
		{
			yyVAL.empty = struct{}{}
		}
	case 183:
		//line sql.y:962
		{
			yyVAL.empty = struct{}{}
		}
	case 184:
		//line sql.y:964
		{
			yyVAL.empty = struct{}{}
		}
	case 185:
		//line sql.y:967
		{
			yyVAL.empty = struct{}{}
		}
	case 186:
		//line sql.y:969
		{
			yyVAL.empty = struct{}{}
		}
	case 187:
		//line sql.y:972
		{
			yyVAL.empty = struct{}{}
		}
	case 188:
		//line sql.y:974
		{
			yyVAL.empty = struct{}{}
		}
	case 189:
		//line sql.y:977
		{
			yyVAL.empty = struct{}{}
		}
	case 190:
		//line sql.y:979
		{
			yyVAL.empty = struct{}{}
		}
	case 191:
		//line sql.y:983
		{
			yyVAL.bytes = bytes.ToLower(yyS[yypt-0].bytes)
		}
	case 192:
		//line sql.y:988
		{
			ForceEOF(yylex)
		}
	}
	goto yystack /* stack new state and value */
}
