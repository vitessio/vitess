//line sql.y:6
package sqlparser

import __yyfmt__ "fmt"

//line sql.y:6
import "strings"

func setParseTree(yylex interface{}, stmt Statement) {
	yylex.(*Tokenizer).ParseTree = stmt
}

func setAllowComments(yylex interface{}, allow bool) {
	yylex.(*Tokenizer).AllowComments = allow
}

func incNesting(yylex interface{}) bool {
	yylex.(*Tokenizer).nesting++
	if yylex.(*Tokenizer).nesting == 200 {
		return true
	}
	return false
}

func decNesting(yylex interface{}) {
	yylex.(*Tokenizer).nesting--
}

func forceEOF(yylex interface{}) {
	yylex.(*Tokenizer).ForceEOF = true
}

//line sql.y:36
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
	sqlID       SQLName
	sqlIDs      []SQLName
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
const ASC = 57363
const DESC = 57364
const INTO = 57365
const DUPLICATE = 57366
const KEY = 57367
const DEFAULT = 57368
const SET = 57369
const LOCK = 57370
const KEYRANGE = 57371
const VALUES = 57372
const LAST_INSERT_ID = 57373
const ID = 57374
const STRING = 57375
const NUMBER = 57376
const VALUE_ARG = 57377
const LIST_ARG = 57378
const COMMENT = 57379
const UNION = 57380
const MINUS = 57381
const EXCEPT = 57382
const INTERSECT = 57383
const JOIN = 57384
const STRAIGHT_JOIN = 57385
const LEFT = 57386
const RIGHT = 57387
const INNER = 57388
const OUTER = 57389
const CROSS = 57390
const NATURAL = 57391
const USE = 57392
const FORCE = 57393
const ON = 57394
const NULL = 57395
const TRUE = 57396
const FALSE = 57397
const OR = 57398
const AND = 57399
const NOT = 57400
const BETWEEN = 57401
const CASE = 57402
const WHEN = 57403
const THEN = 57404
const ELSE = 57405
const LE = 57406
const GE = 57407
const NE = 57408
const NULL_SAFE_EQUAL = 57409
const IS = 57410
const LIKE = 57411
const IN = 57412
const SHIFT_LEFT = 57413
const SHIFT_RIGHT = 57414
const UNARY = 57415
const END = 57416
const CREATE = 57417
const ALTER = 57418
const DROP = 57419
const RENAME = 57420
const ANALYZE = 57421
const TABLE = 57422
const INDEX = 57423
const VIEW = 57424
const TO = 57425
const IGNORE = 57426
const IF = 57427
const UNIQUE = 57428
const USING = 57429
const SHOW = 57430
const DESCRIBE = 57431
const EXPLAIN = 57432

var yyToknames = [...]string{
	"$end",
	"error",
	"$unk",
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
	"ASC",
	"DESC",
	"INTO",
	"DUPLICATE",
	"KEY",
	"DEFAULT",
	"SET",
	"LOCK",
	"KEYRANGE",
	"VALUES",
	"LAST_INSERT_ID",
	"ID",
	"STRING",
	"NUMBER",
	"VALUE_ARG",
	"LIST_ARG",
	"COMMENT",
	"'('",
	"')'",
	"UNION",
	"MINUS",
	"EXCEPT",
	"INTERSECT",
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
	"','",
	"ON",
	"NULL",
	"TRUE",
	"FALSE",
	"OR",
	"AND",
	"NOT",
	"BETWEEN",
	"CASE",
	"WHEN",
	"THEN",
	"ELSE",
	"'='",
	"'<'",
	"'>'",
	"LE",
	"GE",
	"NE",
	"NULL_SAFE_EQUAL",
	"IS",
	"LIKE",
	"IN",
	"'|'",
	"'&'",
	"SHIFT_LEFT",
	"SHIFT_RIGHT",
	"'+'",
	"'-'",
	"'*'",
	"'/'",
	"'%'",
	"'^'",
	"'~'",
	"UNARY",
	"'.'",
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
var yyStatenames = [...]string{}

const yyEofCode = 1
const yyErrCode = 2
const yyMaxDepth = 200

//line yacctab:1
var yyExca = [...]int{
	-1, 1,
	1, -1,
	-2, 0,
	-1, 68,
	89, 210,
	-2, 209,
}

const yyNprod = 214
const yyPrivate = 57344

var yyTokenNames []string
var yyStates []string

const yyLast = 780

var yyAct = [...]int{

	98, 305, 165, 381, 93, 168, 336, 50, 63, 258,
	92, 346, 83, 204, 252, 240, 220, 94, 167, 3,
	214, 64, 82, 38, 192, 40, 59, 184, 110, 41,
	322, 324, 78, 51, 52, 70, 44, 43, 66, 44,
	265, 72, 65, 127, 75, 357, 53, 46, 47, 48,
	356, 355, 71, 74, 198, 49, 45, 333, 88, 155,
	156, 150, 151, 152, 153, 154, 149, 196, 152, 153,
	154, 149, 87, 199, 73, 280, 137, 120, 323, 116,
	124, 131, 364, 126, 149, 395, 135, 140, 226, 117,
	119, 253, 352, 296, 169, 253, 122, 354, 170, 172,
	173, 225, 224, 138, 171, 148, 147, 155, 156, 150,
	151, 152, 153, 154, 149, 180, 66, 140, 337, 66,
	65, 188, 187, 65, 182, 211, 261, 139, 138, 189,
	118, 337, 176, 195, 197, 194, 88, 210, 188, 202,
	186, 181, 140, 132, 219, 130, 209, 227, 228, 118,
	230, 231, 232, 233, 234, 235, 236, 237, 238, 239,
	91, 208, 243, 164, 166, 365, 241, 229, 215, 217,
	218, 222, 241, 216, 245, 185, 241, 88, 88, 139,
	138, 390, 185, 242, 244, 335, 241, 342, 133, 250,
	246, 133, 362, 262, 140, 91, 91, 247, 249, 293,
	316, 263, 353, 174, 175, 317, 139, 138, 177, 178,
	257, 212, 213, 150, 151, 152, 153, 154, 149, 267,
	279, 140, 320, 77, 245, 241, 118, 284, 286, 287,
	241, 266, 281, 282, 283, 208, 319, 206, 91, 318,
	243, 285, 61, 91, 91, 314, 289, 221, 105, 105,
	315, 88, 223, 222, 105, 113, 66, 66, 260, 255,
	65, 303, 73, 290, 301, 292, 295, 304, 42, 68,
	105, 291, 80, 300, 278, 106, 107, 108, 61, 91,
	91, 312, 313, 123, 79, 115, 256, 61, 136, 376,
	91, 330, 361, 387, 114, 326, 208, 208, 109, 334,
	328, 73, 14, 58, 332, 388, 343, 331, 340, 344,
	347, 206, 28, 29, 30, 31, 348, 339, 341, 241,
	28, 29, 30, 31, 190, 129, 297, 299, 241, 221,
	56, 54, 358, 269, 270, 271, 272, 273, 360, 274,
	275, 306, 351, 66, 307, 259, 350, 363, 359, 311,
	185, 62, 394, 91, 245, 385, 67, 371, 91, 369,
	14, 33, 14, 276, 134, 191, 39, 378, 347, 264,
	193, 380, 206, 206, 382, 382, 382, 379, 383, 384,
	377, 269, 270, 271, 272, 273, 66, 274, 275, 61,
	65, 396, 60, 69, 393, 105, 397, 389, 398, 391,
	392, 302, 76, 254, 386, 366, 81, 345, 349, 310,
	294, 32, 86, 179, 251, 100, 99, 14, 338, 60,
	298, 141, 89, 370, 321, 372, 121, 34, 35, 36,
	37, 125, 205, 268, 128, 203, 14, 15, 16, 17,
	85, 55, 27, 57, 68, 106, 107, 108, 13, 12,
	105, 11, 10, 9, 8, 91, 7, 91, 18, 6,
	373, 374, 375, 5, 4, 248, 2, 103, 109, 1,
	0, 60, 0, 183, 0, 112, 104, 0, 0, 68,
	106, 107, 108, 0, 200, 105, 241, 201, 0, 207,
	86, 0, 0, 95, 96, 0, 0, 0, 0, 97,
	0, 0, 0, 109, 101, 102, 0, 0, 90, 0,
	112, 0, 0, 111, 0, 0, 0, 0, 0, 0,
	0, 0, 19, 20, 22, 21, 23, 0, 95, 96,
	84, 86, 86, 0, 97, 24, 25, 26, 0, 0,
	0, 103, 0, 68, 106, 107, 108, 0, 111, 105,
	104, 0, 0, 68, 106, 107, 108, 0, 0, 105,
	0, 0, 277, 207, 0, 0, 0, 109, 0, 0,
	0, 0, 0, 0, 112, 0, 0, 109, 101, 102,
	0, 0, 90, 0, 112, 0, 0, 0, 0, 0,
	0, 0, 95, 96, 0, 0, 0, 0, 97, 14,
	0, 0, 95, 96, 84, 86, 0, 0, 97, 0,
	0, 0, 111, 0, 103, 0, 0, 0, 308, 0,
	0, 309, 111, 104, 207, 207, 68, 106, 107, 108,
	103, 0, 105, 0, 0, 325, 0, 327, 0, 104,
	0, 0, 68, 106, 107, 108, 0, 0, 105, 0,
	109, 101, 102, 0, 0, 90, 0, 112, 0, 0,
	0, 0, 0, 0, 0, 0, 109, 101, 102, 0,
	0, 90, 0, 112, 0, 95, 96, 367, 368, 0,
	0, 97, 147, 155, 156, 150, 151, 152, 153, 154,
	149, 95, 96, 0, 0, 111, 0, 97, 0, 0,
	0, 0, 0, 0, 0, 0, 143, 145, 0, 0,
	0, 111, 157, 158, 159, 160, 161, 162, 163, 146,
	144, 142, 148, 147, 155, 156, 150, 151, 152, 153,
	154, 149, 329, 148, 147, 155, 156, 150, 151, 152,
	153, 154, 149, 288, 0, 0, 0, 0, 0, 148,
	147, 155, 156, 150, 151, 152, 153, 154, 149, 0,
	148, 147, 155, 156, 150, 151, 152, 153, 154, 149,
	148, 147, 155, 156, 150, 151, 152, 153, 154, 149,
}
var yyPact = [...]int{

	431, -1000, -1000, 272, -1000, -1000, -1000, -1000, -1000, -1000,
	-1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, -73,
	-61, -40, -49, -41, -1000, -1000, -1000, 355, 314, -1000,
	-1000, -1000, 312, -1000, -64, 246, 342, 237, -66, -45,
	230, -1000, -43, 230, -1000, 246, -69, 252, -69, 246,
	-1000, -1000, -1000, -1000, -1000, 521, -1000, 218, 271, 258,
	-10, -1000, 246, 95, -1000, 23, -1000, -12, -1000, 246,
	35, 251, -1000, -1000, 246, -1000, -56, 246, 305, 90,
	230, -1000, 134, -1000, -1000, 269, -13, 68, 645, -1000,
	610, 594, -1000, -1000, -1000, 511, 511, 511, 211, 211,
	-1000, -1000, -1000, 211, 211, -1000, -1000, -1000, -1000, -1000,
	-1000, -1000, 511, -1000, 246, 237, 246, 340, 237, 511,
	230, -1000, 304, -79, -1000, 41, -1000, 246, -1000, -1000,
	246, -1000, 210, 521, -1000, -1000, 230, 42, 610, 610,
	112, 511, 216, 26, 511, 511, 112, 511, 511, 511,
	511, 511, 511, 511, 511, 511, 511, -1000, -1000, -1000,
	-1000, -1000, -1000, -1000, 13, 645, 147, 280, 186, 645,
	-1000, 412, -1000, -1000, 447, 521, -1000, 355, 242, 31,
	693, 232, 172, -1000, 332, 610, -1000, 693, -1000, -1000,
	-1000, 71, 230, -1000, -59, -1000, -1000, -1000, -1000, -1000,
	-1000, -1000, -1000, 165, 337, 255, 357, -14, -1000, -1000,
	-1000, -1000, 13, 43, -1000, -1000, 176, -1000, -1000, 693,
	-1000, 412, -1000, -1000, 216, 511, 511, 693, 683, -1000,
	-20, 604, -1000, -15, -15, -2, -2, -2, 132, 132,
	-1000, -1000, -1000, 511, -1000, 693, -1000, 137, 521, 137,
	145, 27, -1000, 610, 297, 237, 237, 332, 326, 330,
	68, 246, -1000, -1000, 246, -1000, 338, 210, 210, -1000,
	-1000, 201, 156, 195, 192, 178, -22, -1000, 246, 289,
	246, -1000, -1000, -1000, 186, -1000, 693, 672, 511, 693,
	-1000, 137, -1000, 242, -33, -1000, 511, 120, 63, 211,
	272, 76, 133, -1000, 326, -1000, 511, 511, -1000, -1000,
	334, 328, 337, 37, -1000, 158, -1000, 53, -1000, -1000,
	-1000, -1000, -46, -47, -52, -1000, -1000, -1000, -1000, 511,
	693, -1000, 191, -1000, 693, 511, -1000, 268, 138, -1000,
	-1000, -1000, 237, -1000, 28, 111, -1000, 656, -1000, 332,
	610, 511, 610, -1000, -1000, 211, 211, 211, 693, -1000,
	693, 264, 211, -1000, 511, 511, -1000, -1000, -1000, 326,
	68, 108, 68, 230, 230, 230, 348, -1000, 693, -1000,
	277, 127, -1000, 127, 127, 237, -1000, 345, 9, -1000,
	230, -1000, -1000, 95, -1000, 230, -1000, 230, -1000,
}
var yyPgo = [...]int{

	0, 469, 466, 18, 464, 463, 459, 456, 454, 453,
	452, 451, 449, 448, 411, 443, 442, 441, 22, 12,
	440, 435, 13, 433, 432, 26, 424, 3, 27, 72,
	422, 421, 420, 10, 2, 20, 16, 5, 418, 17,
	416, 28, 4, 415, 414, 14, 413, 410, 409, 408,
	9, 407, 11, 405, 1, 404, 403, 401, 6, 8,
	21, 268, 223, 393, 370, 369, 366, 365, 0, 364,
	356, 363, 7, 361, 104, 15,
}
var yyR1 = [...]int{

	0, 1, 2, 2, 2, 2, 2, 2, 2, 2,
	2, 2, 2, 3, 3, 4, 4, 5, 6, 7,
	8, 8, 8, 9, 9, 9, 10, 11, 11, 11,
	12, 13, 13, 13, 73, 14, 15, 15, 16, 16,
	16, 16, 16, 17, 17, 18, 18, 19, 19, 19,
	20, 20, 69, 69, 69, 21, 21, 22, 22, 22,
	22, 71, 71, 71, 23, 23, 23, 23, 23, 23,
	23, 23, 23, 24, 24, 24, 25, 25, 26, 26,
	26, 26, 27, 27, 28, 28, 29, 29, 29, 29,
	29, 29, 30, 30, 30, 30, 30, 30, 30, 30,
	30, 30, 30, 30, 35, 35, 35, 35, 35, 35,
	31, 31, 31, 31, 31, 31, 31, 36, 36, 36,
	41, 37, 37, 34, 34, 34, 34, 34, 34, 34,
	34, 34, 34, 34, 34, 34, 34, 34, 34, 34,
	34, 34, 34, 34, 40, 43, 46, 46, 44, 44,
	45, 47, 47, 42, 42, 33, 33, 33, 33, 48,
	48, 49, 49, 50, 50, 51, 51, 52, 53, 53,
	53, 54, 54, 54, 55, 55, 55, 56, 56, 57,
	57, 58, 58, 32, 32, 38, 38, 39, 39, 59,
	59, 60, 62, 62, 63, 63, 61, 61, 64, 64,
	64, 64, 64, 65, 65, 66, 66, 67, 67, 68,
	70, 74, 75, 72,
}
var yyR2 = [...]int{

	0, 1, 1, 1, 1, 1, 1, 1, 1, 1,
	1, 1, 1, 12, 3, 8, 8, 8, 7, 3,
	5, 8, 4, 6, 7, 4, 5, 4, 5, 5,
	3, 2, 2, 2, 0, 2, 0, 2, 1, 2,
	1, 1, 1, 0, 1, 1, 3, 1, 2, 3,
	1, 1, 0, 1, 2, 1, 3, 3, 3, 3,
	5, 0, 1, 2, 1, 1, 2, 3, 2, 3,
	2, 2, 2, 1, 3, 1, 1, 3, 0, 5,
	5, 5, 1, 3, 0, 2, 1, 3, 3, 2,
	3, 3, 1, 1, 3, 3, 4, 3, 4, 5,
	6, 3, 2, 6, 1, 2, 1, 2, 1, 2,
	1, 1, 1, 1, 1, 1, 1, 3, 1, 1,
	3, 1, 3, 1, 1, 1, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 2, 2, 2, 3,
	4, 5, 4, 1, 1, 5, 0, 1, 1, 2,
	4, 0, 2, 1, 3, 1, 1, 1, 1, 0,
	3, 0, 2, 0, 3, 1, 3, 2, 0, 1,
	1, 0, 2, 4, 0, 2, 4, 0, 3, 1,
	3, 0, 5, 2, 1, 1, 3, 3, 1, 1,
	3, 3, 0, 2, 0, 3, 0, 1, 1, 1,
	1, 1, 1, 0, 1, 0, 1, 0, 2, 1,
	1, 1, 1, 0,
}
var yyChk = [...]int{

	-1000, -1, -2, -3, -4, -5, -6, -7, -8, -9,
	-10, -11, -12, -13, 5, 6, 7, 8, 27, 91,
	92, 94, 93, 95, 104, 105, 106, -16, 40, 41,
	42, 43, -14, -73, -14, -14, -14, -14, 96, -66,
	98, 102, -61, 98, 100, 96, 96, 97, 98, 96,
	-72, -72, -72, -3, 17, -17, 18, -15, -61, -25,
	-70, 32, 9, -59, -60, -42, -68, -70, 32, -63,
	101, 97, -68, 32, 96, -68, -70, -62, 101, 32,
	-62, -70, -18, -19, 83, -20, -70, -29, -34, -30,
	61, -74, -33, -42, -39, 81, 82, 87, -68, -40,
	-43, 57, 58, 20, 29, 38, 33, 34, 35, 56,
	-41, 101, 63, 37, 23, 27, 89, -25, 54, 67,
	89, -70, 61, 32, -72, -70, -72, 99, -70, 20,
	55, -68, 9, 54, -69, -68, 19, 89, 60, 59,
	74, -31, 76, 61, 75, 62, 74, 78, 77, 86,
	81, 82, 83, 84, 85, 79, 80, 67, 68, 69,
	70, 71, 72, 73, -29, -34, -29, -3, -37, -34,
	-34, -74, -34, -34, -74, -74, -41, -74, -74, -46,
	-34, -25, -59, -70, -28, 10, -60, -34, -68, -72,
	20, -67, 103, -64, 94, 92, 26, 93, 13, 32,
	-70, -70, -72, -21, -22, -24, -74, -70, -41, -19,
	-68, 83, -29, -29, -35, 56, 61, 57, 58, -34,
	-36, -74, -41, 36, 76, 75, 62, -34, -34, -35,
	-34, -34, -34, -34, -34, -34, -34, -34, -34, -34,
	-75, 39, -75, 54, -75, -34, -75, -18, 18, -18,
	-33, -44, -45, 64, -56, 27, -74, -28, -50, 13,
	-29, 55, -68, -72, -65, 99, -28, 54, -23, 44,
	45, 46, 47, 48, 50, 51, -71, -70, 19, -22,
	89, 56, 57, 58, -37, -36, -34, -34, 60, -34,
	-75, -18, -75, 54, -47, -45, 66, -29, -32, 30,
	-3, -59, -57, -42, -50, -54, 15, 14, -70, -70,
	-48, 11, -22, -22, 44, 49, 44, 49, 44, 44,
	44, -26, 52, 100, 53, -70, -75, -70, -75, 60,
	-34, -75, -33, 90, -34, 65, -58, 55, -38, -39,
	-58, -75, 54, -54, -34, -51, -52, -34, -72, -49,
	12, 14, 55, 44, 44, 97, 97, 97, -34, -75,
	-34, 24, 54, -42, 54, 54, -53, 21, 22, -50,
	-29, -37, -29, -74, -74, -74, 25, -39, -34, -52,
	-54, -27, -68, -27, -27, 7, -55, 16, 28, -75,
	54, -75, -75, -59, 7, 76, -68, -68, -68,
}
var yyDef = [...]int{

	0, -2, 1, 2, 3, 4, 5, 6, 7, 8,
	9, 10, 11, 12, 34, 34, 34, 34, 34, 205,
	196, 0, 0, 0, 213, 213, 213, 0, 38, 40,
	41, 42, 43, 36, 196, 0, 0, 0, 194, 0,
	0, 206, 0, 0, 197, 0, 192, 0, 192, 0,
	31, 32, 33, 14, 39, 0, 44, 35, 0, 0,
	76, 210, 0, 19, 189, 0, 153, 0, -2, 0,
	0, 0, 213, 209, 0, 213, 0, 0, 0, 0,
	0, 30, 0, 45, 47, 52, 0, 50, 51, 86,
	0, 0, 123, 124, 125, 0, 0, 0, 153, 0,
	143, 92, 93, 0, 0, 211, 155, 156, 157, 158,
	188, 144, 146, 37, 0, 0, 0, 84, 0, 0,
	0, 213, 0, 207, 22, 0, 25, 0, 27, 193,
	0, 213, 0, 0, 48, 53, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 110, 111, 112,
	113, 114, 115, 116, 89, 0, 0, 0, 0, 121,
	136, 0, 137, 138, 0, 0, 102, 0, 0, 0,
	147, 177, 84, 77, 163, 0, 190, 191, 154, 20,
	195, 0, 0, 213, 203, 198, 199, 200, 201, 202,
	26, 28, 29, 84, 55, 61, 0, 73, 75, 46,
	54, 49, 87, 88, 91, 104, 0, 106, 108, 94,
	95, 0, 118, 119, 0, 0, 0, 97, 0, 101,
	126, 127, 128, 129, 130, 131, 132, 133, 134, 135,
	90, 212, 120, 0, 187, 121, 139, 0, 0, 0,
	0, 151, 148, 0, 0, 0, 0, 163, 171, 0,
	85, 0, 208, 23, 0, 204, 159, 0, 0, 64,
	65, 0, 0, 0, 0, 0, 78, 62, 0, 0,
	0, 105, 107, 109, 0, 96, 98, 0, 0, 122,
	140, 0, 142, 0, 0, 149, 0, 0, 181, 0,
	184, 181, 0, 179, 171, 18, 0, 0, 213, 24,
	161, 0, 56, 59, 66, 0, 68, 0, 70, 71,
	72, 57, 0, 0, 0, 63, 58, 74, 117, 0,
	99, 141, 0, 145, 152, 0, 15, 0, 183, 185,
	16, 178, 0, 17, 172, 164, 165, 168, 21, 163,
	0, 0, 0, 67, 69, 0, 0, 0, 100, 103,
	150, 0, 0, 180, 0, 0, 167, 169, 170, 171,
	162, 160, 60, 0, 0, 0, 0, 186, 173, 166,
	174, 0, 82, 0, 0, 0, 13, 0, 0, 79,
	0, 80, 81, 182, 175, 0, 83, 0, 176,
}
var yyTok1 = [...]int{

	1, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 85, 78, 3,
	38, 39, 83, 81, 54, 82, 89, 84, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	68, 67, 69, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 86, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 77, 3, 87,
}
var yyTok2 = [...]int{

	2, 3, 4, 5, 6, 7, 8, 9, 10, 11,
	12, 13, 14, 15, 16, 17, 18, 19, 20, 21,
	22, 23, 24, 25, 26, 27, 28, 29, 30, 31,
	32, 33, 34, 35, 36, 37, 40, 41, 42, 43,
	44, 45, 46, 47, 48, 49, 50, 51, 52, 53,
	55, 56, 57, 58, 59, 60, 61, 62, 63, 64,
	65, 66, 70, 71, 72, 73, 74, 75, 76, 79,
	80, 88, 90, 91, 92, 93, 94, 95, 96, 97,
	98, 99, 100, 101, 102, 103, 104, 105, 106,
}
var yyTok3 = [...]int{
	0,
}

var yyErrorMessages = [...]struct {
	state int
	token int
	msg   string
}{}

//line yaccpar:1

/*	parser for yacc output	*/

var (
	yyDebug        = 0
	yyErrorVerbose = false
)

type yyLexer interface {
	Lex(lval *yySymType) int
	Error(s string)
}

type yyParser interface {
	Parse(yyLexer) int
	Lookahead() int
}

type yyParserImpl struct {
	lookahead func() int
}

func (p *yyParserImpl) Lookahead() int {
	return p.lookahead()
}

func yyNewParser() yyParser {
	p := &yyParserImpl{
		lookahead: func() int { return -1 },
	}
	return p
}

const yyFlag = -1000

func yyTokname(c int) string {
	if c >= 1 && c-1 < len(yyToknames) {
		if yyToknames[c-1] != "" {
			return yyToknames[c-1]
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

func yyErrorMessage(state, lookAhead int) string {
	const TOKSTART = 4

	if !yyErrorVerbose {
		return "syntax error"
	}

	for _, e := range yyErrorMessages {
		if e.state == state && e.token == lookAhead {
			return "syntax error: " + e.msg
		}
	}

	res := "syntax error: unexpected " + yyTokname(lookAhead)

	// To match Bison, suggest at most four expected tokens.
	expected := make([]int, 0, 4)

	// Look for shiftable tokens.
	base := yyPact[state]
	for tok := TOKSTART; tok-1 < len(yyToknames); tok++ {
		if n := base + tok; n >= 0 && n < yyLast && yyChk[yyAct[n]] == tok {
			if len(expected) == cap(expected) {
				return res
			}
			expected = append(expected, tok)
		}
	}

	if yyDef[state] == -2 {
		i := 0
		for yyExca[i] != -1 || yyExca[i+1] != state {
			i += 2
		}

		// Look for tokens that we accept or reduce.
		for i += 2; yyExca[i] >= 0; i += 2 {
			tok := yyExca[i]
			if tok < TOKSTART || yyExca[i+1] == 0 {
				continue
			}
			if len(expected) == cap(expected) {
				return res
			}
			expected = append(expected, tok)
		}

		// If the default action is to accept or reduce, give up.
		if yyExca[i+1] != 0 {
			return res
		}
	}

	for i, tok := range expected {
		if i == 0 {
			res += ", expecting "
		} else {
			res += " or "
		}
		res += yyTokname(tok)
	}
	return res
}

func yylex1(lex yyLexer, lval *yySymType) (char, token int) {
	token = 0
	char = lex.Lex(lval)
	if char <= 0 {
		token = yyTok1[0]
		goto out
	}
	if char < len(yyTok1) {
		token = yyTok1[char]
		goto out
	}
	if char >= yyPrivate {
		if char < yyPrivate+len(yyTok2) {
			token = yyTok2[char-yyPrivate]
			goto out
		}
	}
	for i := 0; i < len(yyTok3); i += 2 {
		token = yyTok3[i+0]
		if token == char {
			token = yyTok3[i+1]
			goto out
		}
	}

out:
	if token == 0 {
		token = yyTok2[1] /* unknown char */
	}
	if yyDebug >= 3 {
		__yyfmt__.Printf("lex %s(%d)\n", yyTokname(token), uint(char))
	}
	return char, token
}

func yyParse(yylex yyLexer) int {
	return yyNewParser().Parse(yylex)
}

func (yyrcvr *yyParserImpl) Parse(yylex yyLexer) int {
	var yyn int
	var yylval yySymType
	var yyVAL yySymType
	var yyDollar []yySymType
	yyS := make([]yySymType, yyMaxDepth)

	Nerrs := 0   /* number of errors */
	Errflag := 0 /* error recovery flag */
	yystate := 0
	yychar := -1
	yytoken := -1 // yychar translated into internal numbering
	yyrcvr.lookahead = func() int { return yychar }
	defer func() {
		// Make sure we report no lookahead when not parsing.
		yystate = -1
		yychar = -1
		yytoken = -1
	}()
	yyp := -1
	goto yystack

ret0:
	return 0

ret1:
	return 1

yystack:
	/* put a state and value onto the stack */
	if yyDebug >= 4 {
		__yyfmt__.Printf("char %v in %v\n", yyTokname(yytoken), yyStatname(yystate))
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
		yychar, yytoken = yylex1(yylex, &yylval)
	}
	yyn += yytoken
	if yyn < 0 || yyn >= yyLast {
		goto yydefault
	}
	yyn = yyAct[yyn]
	if yyChk[yyn] == yytoken { /* valid shift */
		yychar = -1
		yytoken = -1
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
			yychar, yytoken = yylex1(yylex, &yylval)
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
			if yyn < 0 || yyn == yytoken {
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
			yylex.Error(yyErrorMessage(yystate, yytoken))
			Nerrs++
			if yyDebug >= 1 {
				__yyfmt__.Printf("%s", yyStatname(yystate))
				__yyfmt__.Printf(" saw %s\n", yyTokname(yytoken))
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
				__yyfmt__.Printf("error recovery discards %s\n", yyTokname(yytoken))
			}
			if yytoken == yyEofCode {
				goto ret1
			}
			yychar = -1
			yytoken = -1
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
	// yyp is now the index of $0. Perform the default action. Iff the
	// reduced production is ε, $1 is possibly out of range.
	if yyp+1 >= len(yyS) {
		nyys := make([]yySymType, len(yyS)*2)
		copy(nyys, yyS)
		yyS = nyys
	}
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
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:168
		{
			setParseTree(yylex, yyDollar[1].statement)
		}
	case 2:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:174
		{
			yyVAL.statement = yyDollar[1].selStmt
		}
	case 13:
		yyDollar = yyS[yypt-12 : yypt+1]
		//line sql.y:190
		{
			yyVAL.selStmt = &Select{Comments: Comments(yyDollar[2].bytes2), Distinct: yyDollar[3].str, SelectExprs: yyDollar[4].selectExprs, From: yyDollar[6].tableExprs, Where: NewWhere(AST_WHERE, yyDollar[7].boolExpr), GroupBy: GroupBy(yyDollar[8].valExprs), Having: NewWhere(AST_HAVING, yyDollar[9].boolExpr), OrderBy: yyDollar[10].orderBy, Limit: yyDollar[11].limit, Lock: yyDollar[12].str}
		}
	case 14:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:194
		{
			yyVAL.selStmt = &Union{Type: yyDollar[2].str, Left: yyDollar[1].selStmt, Right: yyDollar[3].selStmt}
		}
	case 15:
		yyDollar = yyS[yypt-8 : yypt+1]
		//line sql.y:200
		{
			yyVAL.statement = &Insert{Comments: Comments(yyDollar[2].bytes2), Ignore: yyDollar[3].str, Table: yyDollar[5].tableName, Columns: yyDollar[6].columns, Rows: yyDollar[7].insRows, OnDup: OnDup(yyDollar[8].updateExprs)}
		}
	case 16:
		yyDollar = yyS[yypt-8 : yypt+1]
		//line sql.y:204
		{
			cols := make(Columns, 0, len(yyDollar[7].updateExprs))
			vals := make(ValTuple, 0, len(yyDollar[7].updateExprs))
			for _, col := range yyDollar[7].updateExprs {
				cols = append(cols, &NonStarExpr{Expr: col.Name})
				vals = append(vals, col.Expr)
			}
			yyVAL.statement = &Insert{Comments: Comments(yyDollar[2].bytes2), Ignore: yyDollar[3].str, Table: yyDollar[5].tableName, Columns: cols, Rows: Values{vals}, OnDup: OnDup(yyDollar[8].updateExprs)}
		}
	case 17:
		yyDollar = yyS[yypt-8 : yypt+1]
		//line sql.y:216
		{
			yyVAL.statement = &Update{Comments: Comments(yyDollar[2].bytes2), Table: yyDollar[3].tableName, Exprs: yyDollar[5].updateExprs, Where: NewWhere(AST_WHERE, yyDollar[6].boolExpr), OrderBy: yyDollar[7].orderBy, Limit: yyDollar[8].limit}
		}
	case 18:
		yyDollar = yyS[yypt-7 : yypt+1]
		//line sql.y:222
		{
			yyVAL.statement = &Delete{Comments: Comments(yyDollar[2].bytes2), Table: yyDollar[4].tableName, Where: NewWhere(AST_WHERE, yyDollar[5].boolExpr), OrderBy: yyDollar[6].orderBy, Limit: yyDollar[7].limit}
		}
	case 19:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:228
		{
			yyVAL.statement = &Set{Comments: Comments(yyDollar[2].bytes2), Exprs: yyDollar[3].updateExprs}
		}
	case 20:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:234
		{
			yyVAL.statement = &DDL{Action: AST_CREATE, NewName: yyDollar[4].sqlID}
		}
	case 21:
		yyDollar = yyS[yypt-8 : yypt+1]
		//line sql.y:238
		{
			// Change this to an alter statement
			yyVAL.statement = &DDL{Action: AST_ALTER, Table: yyDollar[7].sqlID, NewName: yyDollar[7].sqlID}
		}
	case 22:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:243
		{
			yyVAL.statement = &DDL{Action: AST_CREATE, NewName: SQLName(yyDollar[3].sqlID)}
		}
	case 23:
		yyDollar = yyS[yypt-6 : yypt+1]
		//line sql.y:249
		{
			yyVAL.statement = &DDL{Action: AST_ALTER, Table: yyDollar[4].sqlID, NewName: yyDollar[4].sqlID}
		}
	case 24:
		yyDollar = yyS[yypt-7 : yypt+1]
		//line sql.y:253
		{
			// Change this to a rename statement
			yyVAL.statement = &DDL{Action: AST_RENAME, Table: yyDollar[4].sqlID, NewName: yyDollar[7].sqlID}
		}
	case 25:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:258
		{
			yyVAL.statement = &DDL{Action: AST_ALTER, Table: SQLName(yyDollar[3].sqlID), NewName: SQLName(yyDollar[3].sqlID)}
		}
	case 26:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:264
		{
			yyVAL.statement = &DDL{Action: AST_RENAME, Table: yyDollar[3].sqlID, NewName: yyDollar[5].sqlID}
		}
	case 27:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:270
		{
			yyVAL.statement = &DDL{Action: AST_DROP, Table: yyDollar[4].sqlID}
		}
	case 28:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:274
		{
			// Change this to an alter statement
			yyVAL.statement = &DDL{Action: AST_ALTER, Table: yyDollar[5].sqlID, NewName: yyDollar[5].sqlID}
		}
	case 29:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:279
		{
			yyVAL.statement = &DDL{Action: AST_DROP, Table: SQLName(yyDollar[4].sqlID)}
		}
	case 30:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:285
		{
			yyVAL.statement = &DDL{Action: AST_ALTER, Table: yyDollar[3].sqlID, NewName: yyDollar[3].sqlID}
		}
	case 31:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:291
		{
			yyVAL.statement = &Other{}
		}
	case 32:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:295
		{
			yyVAL.statement = &Other{}
		}
	case 33:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:299
		{
			yyVAL.statement = &Other{}
		}
	case 34:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:304
		{
			setAllowComments(yylex, true)
		}
	case 35:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:308
		{
			yyVAL.bytes2 = yyDollar[2].bytes2
			setAllowComments(yylex, false)
		}
	case 36:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:314
		{
			yyVAL.bytes2 = nil
		}
	case 37:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:318
		{
			yyVAL.bytes2 = append(yyDollar[1].bytes2, yyDollar[2].bytes)
		}
	case 38:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:324
		{
			yyVAL.str = AST_UNION
		}
	case 39:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:328
		{
			yyVAL.str = AST_UNION_ALL
		}
	case 40:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:332
		{
			yyVAL.str = AST_SET_MINUS
		}
	case 41:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:336
		{
			yyVAL.str = AST_EXCEPT
		}
	case 42:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:340
		{
			yyVAL.str = AST_INTERSECT
		}
	case 43:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:345
		{
			yyVAL.str = ""
		}
	case 44:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:349
		{
			yyVAL.str = AST_DISTINCT
		}
	case 45:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:355
		{
			yyVAL.selectExprs = SelectExprs{yyDollar[1].selectExpr}
		}
	case 46:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:359
		{
			yyVAL.selectExprs = append(yyVAL.selectExprs, yyDollar[3].selectExpr)
		}
	case 47:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:365
		{
			yyVAL.selectExpr = &StarExpr{}
		}
	case 48:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:369
		{
			yyVAL.selectExpr = &NonStarExpr{Expr: yyDollar[1].expr, As: yyDollar[2].sqlID}
		}
	case 49:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:373
		{
			yyVAL.selectExpr = &StarExpr{TableName: yyDollar[1].sqlID}
		}
	case 50:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:379
		{
			yyVAL.expr = yyDollar[1].boolExpr
		}
	case 51:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:383
		{
			yyVAL.expr = yyDollar[1].valExpr
		}
	case 52:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:388
		{
			yyVAL.sqlID = ""
		}
	case 53:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:392
		{
			yyVAL.sqlID = yyDollar[1].sqlID
		}
	case 54:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:396
		{
			yyVAL.sqlID = yyDollar[2].sqlID
		}
	case 55:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:402
		{
			yyVAL.tableExprs = TableExprs{yyDollar[1].tableExpr}
		}
	case 56:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:406
		{
			yyVAL.tableExprs = append(yyVAL.tableExprs, yyDollar[3].tableExpr)
		}
	case 57:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:412
		{
			yyVAL.tableExpr = &AliasedTableExpr{Expr: yyDollar[1].smTableExpr, As: yyDollar[2].sqlID, Hints: yyDollar[3].indexHints}
		}
	case 58:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:416
		{
			yyVAL.tableExpr = &ParenTableExpr{Expr: yyDollar[2].tableExpr}
		}
	case 59:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:420
		{
			yyVAL.tableExpr = &JoinTableExpr{LeftExpr: yyDollar[1].tableExpr, Join: yyDollar[2].str, RightExpr: yyDollar[3].tableExpr}
		}
	case 60:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:424
		{
			yyVAL.tableExpr = &JoinTableExpr{LeftExpr: yyDollar[1].tableExpr, Join: yyDollar[2].str, RightExpr: yyDollar[3].tableExpr, On: yyDollar[5].boolExpr}
		}
	case 61:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:429
		{
			yyVAL.sqlID = ""
		}
	case 62:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:433
		{
			yyVAL.sqlID = yyDollar[1].sqlID
		}
	case 63:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:437
		{
			yyVAL.sqlID = yyDollar[2].sqlID
		}
	case 64:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:443
		{
			yyVAL.str = AST_JOIN
		}
	case 65:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:447
		{
			yyVAL.str = AST_STRAIGHT_JOIN
		}
	case 66:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:451
		{
			yyVAL.str = AST_LEFT_JOIN
		}
	case 67:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:455
		{
			yyVAL.str = AST_LEFT_JOIN
		}
	case 68:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:459
		{
			yyVAL.str = AST_RIGHT_JOIN
		}
	case 69:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:463
		{
			yyVAL.str = AST_RIGHT_JOIN
		}
	case 70:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:467
		{
			yyVAL.str = AST_JOIN
		}
	case 71:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:471
		{
			yyVAL.str = AST_CROSS_JOIN
		}
	case 72:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:475
		{
			yyVAL.str = AST_NATURAL_JOIN
		}
	case 73:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:481
		{
			yyVAL.smTableExpr = &TableName{Name: yyDollar[1].sqlID}
		}
	case 74:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:485
		{
			yyVAL.smTableExpr = &TableName{Qualifier: yyDollar[1].sqlID, Name: yyDollar[3].sqlID}
		}
	case 75:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:489
		{
			yyVAL.smTableExpr = yyDollar[1].subquery
		}
	case 76:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:495
		{
			yyVAL.tableName = &TableName{Name: yyDollar[1].sqlID}
		}
	case 77:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:499
		{
			yyVAL.tableName = &TableName{Qualifier: yyDollar[1].sqlID, Name: yyDollar[3].sqlID}
		}
	case 78:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:504
		{
			yyVAL.indexHints = nil
		}
	case 79:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:508
		{
			yyVAL.indexHints = &IndexHints{Type: AST_USE, Indexes: yyDollar[4].sqlIDs}
		}
	case 80:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:512
		{
			yyVAL.indexHints = &IndexHints{Type: AST_IGNORE, Indexes: yyDollar[4].sqlIDs}
		}
	case 81:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:516
		{
			yyVAL.indexHints = &IndexHints{Type: AST_FORCE, Indexes: yyDollar[4].sqlIDs}
		}
	case 82:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:522
		{
			yyVAL.sqlIDs = []SQLName{yyDollar[1].sqlID}
		}
	case 83:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:526
		{
			yyVAL.sqlIDs = append(yyDollar[1].sqlIDs, yyDollar[3].sqlID)
		}
	case 84:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:531
		{
			yyVAL.boolExpr = nil
		}
	case 85:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:535
		{
			yyVAL.boolExpr = yyDollar[2].boolExpr
		}
	case 87:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:542
		{
			yyVAL.boolExpr = &AndExpr{Left: yyDollar[1].boolExpr, Right: yyDollar[3].boolExpr}
		}
	case 88:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:546
		{
			yyVAL.boolExpr = &OrExpr{Left: yyDollar[1].boolExpr, Right: yyDollar[3].boolExpr}
		}
	case 89:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:550
		{
			yyVAL.boolExpr = &NotExpr{Expr: yyDollar[2].boolExpr}
		}
	case 90:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:554
		{
			yyVAL.boolExpr = &ParenBoolExpr{Expr: yyDollar[2].boolExpr}
		}
	case 91:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:558
		{
			yyVAL.boolExpr = &IsExpr{Operator: yyDollar[3].str, Expr: yyDollar[1].boolExpr}
		}
	case 92:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:564
		{
			yyVAL.boolExpr = BoolVal(true)
		}
	case 93:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:568
		{
			yyVAL.boolExpr = BoolVal(false)
		}
	case 94:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:572
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: yyDollar[2].str, Right: yyDollar[3].valExpr}
		}
	case 95:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:576
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: AST_IN, Right: yyDollar[3].colTuple}
		}
	case 96:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:580
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: AST_NOT_IN, Right: yyDollar[4].colTuple}
		}
	case 97:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:584
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: AST_LIKE, Right: yyDollar[3].valExpr}
		}
	case 98:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:588
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: AST_NOT_LIKE, Right: yyDollar[4].valExpr}
		}
	case 99:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:592
		{
			yyVAL.boolExpr = &RangeCond{Left: yyDollar[1].valExpr, Operator: AST_BETWEEN, From: yyDollar[3].valExpr, To: yyDollar[5].valExpr}
		}
	case 100:
		yyDollar = yyS[yypt-6 : yypt+1]
		//line sql.y:596
		{
			yyVAL.boolExpr = &RangeCond{Left: yyDollar[1].valExpr, Operator: AST_NOT_BETWEEN, From: yyDollar[4].valExpr, To: yyDollar[6].valExpr}
		}
	case 101:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:600
		{
			yyVAL.boolExpr = &IsExpr{Operator: yyDollar[3].str, Expr: yyDollar[1].valExpr}
		}
	case 102:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:604
		{
			yyVAL.boolExpr = &ExistsExpr{Subquery: yyDollar[2].subquery}
		}
	case 103:
		yyDollar = yyS[yypt-6 : yypt+1]
		//line sql.y:608
		{
			yyVAL.boolExpr = &KeyrangeExpr{Start: yyDollar[3].valExpr, End: yyDollar[5].valExpr}
		}
	case 104:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:614
		{
			yyVAL.str = AST_IS_NULL
		}
	case 105:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:618
		{
			yyVAL.str = AST_IS_NOT_NULL
		}
	case 106:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:622
		{
			yyVAL.str = AST_IS_TRUE
		}
	case 107:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:626
		{
			yyVAL.str = AST_IS_NOT_TRUE
		}
	case 108:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:630
		{
			yyVAL.str = AST_IS_FALSE
		}
	case 109:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:634
		{
			yyVAL.str = AST_IS_NOT_FALSE
		}
	case 110:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:640
		{
			yyVAL.str = AST_EQ
		}
	case 111:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:644
		{
			yyVAL.str = AST_LT
		}
	case 112:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:648
		{
			yyVAL.str = AST_GT
		}
	case 113:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:652
		{
			yyVAL.str = AST_LE
		}
	case 114:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:656
		{
			yyVAL.str = AST_GE
		}
	case 115:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:660
		{
			yyVAL.str = AST_NE
		}
	case 116:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:664
		{
			yyVAL.str = AST_NSE
		}
	case 117:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:670
		{
			yyVAL.colTuple = ValTuple(yyDollar[2].valExprs)
		}
	case 118:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:674
		{
			yyVAL.colTuple = yyDollar[1].subquery
		}
	case 119:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:678
		{
			yyVAL.colTuple = ListArg(yyDollar[1].bytes)
		}
	case 120:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:684
		{
			yyVAL.subquery = &Subquery{yyDollar[2].selStmt}
		}
	case 121:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:690
		{
			yyVAL.valExprs = ValExprs{yyDollar[1].valExpr}
		}
	case 122:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:694
		{
			yyVAL.valExprs = append(yyDollar[1].valExprs, yyDollar[3].valExpr)
		}
	case 123:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:700
		{
			yyVAL.valExpr = yyDollar[1].valExpr
		}
	case 124:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:704
		{
			yyVAL.valExpr = yyDollar[1].colName
		}
	case 125:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:708
		{
			yyVAL.valExpr = yyDollar[1].rowTuple
		}
	case 126:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:712
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: AST_BITAND, Right: yyDollar[3].valExpr}
		}
	case 127:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:716
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: AST_BITOR, Right: yyDollar[3].valExpr}
		}
	case 128:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:720
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: AST_BITXOR, Right: yyDollar[3].valExpr}
		}
	case 129:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:724
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: AST_PLUS, Right: yyDollar[3].valExpr}
		}
	case 130:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:728
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: AST_MINUS, Right: yyDollar[3].valExpr}
		}
	case 131:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:732
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: AST_MULT, Right: yyDollar[3].valExpr}
		}
	case 132:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:736
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: AST_DIV, Right: yyDollar[3].valExpr}
		}
	case 133:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:740
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: AST_MOD, Right: yyDollar[3].valExpr}
		}
	case 134:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:744
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: AST_SHIFT_LEFT, Right: yyDollar[3].valExpr}
		}
	case 135:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:748
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: AST_SHIFT_RIGHT, Right: yyDollar[3].valExpr}
		}
	case 136:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:752
		{
			if num, ok := yyDollar[2].valExpr.(NumVal); ok {
				yyVAL.valExpr = num
			} else {
				yyVAL.valExpr = &UnaryExpr{Operator: AST_UPLUS, Expr: yyDollar[2].valExpr}
			}
		}
	case 137:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:760
		{
			if num, ok := yyDollar[2].valExpr.(NumVal); ok {
				// Handle double negative
				if num[0] == '-' {
					yyVAL.valExpr = num[1:]
				} else {
					yyVAL.valExpr = append(NumVal("-"), num...)
				}
			} else {
				yyVAL.valExpr = &UnaryExpr{Operator: AST_UMINUS, Expr: yyDollar[2].valExpr}
			}
		}
	case 138:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:773
		{
			yyVAL.valExpr = &UnaryExpr{Operator: AST_TILDA, Expr: yyDollar[2].valExpr}
		}
	case 139:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:777
		{
			yyVAL.valExpr = &FuncExpr{Name: string(yyDollar[1].sqlID)}
		}
	case 140:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:781
		{
			yyVAL.valExpr = &FuncExpr{Name: string(yyDollar[1].sqlID), Exprs: yyDollar[3].selectExprs}
		}
	case 141:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:785
		{
			yyVAL.valExpr = &FuncExpr{Name: string(yyDollar[1].sqlID), Distinct: true, Exprs: yyDollar[4].selectExprs}
		}
	case 142:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:789
		{
			yyVAL.valExpr = &FuncExpr{Name: yyDollar[1].str, Exprs: yyDollar[3].selectExprs}
		}
	case 143:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:793
		{
			yyVAL.valExpr = yyDollar[1].caseExpr
		}
	case 144:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:799
		{
			yyVAL.str = "if"
		}
	case 145:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:805
		{
			yyVAL.caseExpr = &CaseExpr{Expr: yyDollar[2].valExpr, Whens: yyDollar[3].whens, Else: yyDollar[4].valExpr}
		}
	case 146:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:810
		{
			yyVAL.valExpr = nil
		}
	case 147:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:814
		{
			yyVAL.valExpr = yyDollar[1].valExpr
		}
	case 148:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:820
		{
			yyVAL.whens = []*When{yyDollar[1].when}
		}
	case 149:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:824
		{
			yyVAL.whens = append(yyDollar[1].whens, yyDollar[2].when)
		}
	case 150:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:830
		{
			yyVAL.when = &When{Cond: yyDollar[2].boolExpr, Val: yyDollar[4].valExpr}
		}
	case 151:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:835
		{
			yyVAL.valExpr = nil
		}
	case 152:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:839
		{
			yyVAL.valExpr = yyDollar[2].valExpr
		}
	case 153:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:845
		{
			yyVAL.colName = &ColName{Name: yyDollar[1].sqlID}
		}
	case 154:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:849
		{
			yyVAL.colName = &ColName{Qualifier: yyDollar[1].sqlID, Name: yyDollar[3].sqlID}
		}
	case 155:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:855
		{
			yyVAL.valExpr = StrVal(yyDollar[1].bytes)
		}
	case 156:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:859
		{
			yyVAL.valExpr = NumVal(yyDollar[1].bytes)
		}
	case 157:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:863
		{
			yyVAL.valExpr = ValArg(yyDollar[1].bytes)
		}
	case 158:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:867
		{
			yyVAL.valExpr = &NullVal{}
		}
	case 159:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:872
		{
			yyVAL.valExprs = nil
		}
	case 160:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:876
		{
			yyVAL.valExprs = yyDollar[3].valExprs
		}
	case 161:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:881
		{
			yyVAL.boolExpr = nil
		}
	case 162:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:885
		{
			yyVAL.boolExpr = yyDollar[2].boolExpr
		}
	case 163:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:890
		{
			yyVAL.orderBy = nil
		}
	case 164:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:894
		{
			yyVAL.orderBy = yyDollar[3].orderBy
		}
	case 165:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:900
		{
			yyVAL.orderBy = OrderBy{yyDollar[1].order}
		}
	case 166:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:904
		{
			yyVAL.orderBy = append(yyDollar[1].orderBy, yyDollar[3].order)
		}
	case 167:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:910
		{
			yyVAL.order = &Order{Expr: yyDollar[1].valExpr, Direction: yyDollar[2].str}
		}
	case 168:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:915
		{
			yyVAL.str = AST_ASC
		}
	case 169:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:919
		{
			yyVAL.str = AST_ASC
		}
	case 170:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:923
		{
			yyVAL.str = AST_DESC
		}
	case 171:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:928
		{
			yyVAL.limit = nil
		}
	case 172:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:932
		{
			yyVAL.limit = &Limit{Rowcount: yyDollar[2].valExpr}
		}
	case 173:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:936
		{
			yyVAL.limit = &Limit{Offset: yyDollar[2].valExpr, Rowcount: yyDollar[4].valExpr}
		}
	case 174:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:941
		{
			yyVAL.str = ""
		}
	case 175:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:945
		{
			yyVAL.str = AST_FOR_UPDATE
		}
	case 176:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:949
		{
			if yyDollar[3].sqlID != "share" {
				yylex.Error("expecting share")
				return 1
			}
			if yyDollar[4].sqlID != "mode" {
				yylex.Error("expecting mode")
				return 1
			}
			yyVAL.str = AST_SHARE_MODE
		}
	case 177:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:962
		{
			yyVAL.columns = nil
		}
	case 178:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:966
		{
			yyVAL.columns = yyDollar[2].columns
		}
	case 179:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:972
		{
			yyVAL.columns = Columns{&NonStarExpr{Expr: yyDollar[1].colName}}
		}
	case 180:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:976
		{
			yyVAL.columns = append(yyVAL.columns, &NonStarExpr{Expr: yyDollar[3].colName})
		}
	case 181:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:981
		{
			yyVAL.updateExprs = nil
		}
	case 182:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:985
		{
			yyVAL.updateExprs = yyDollar[5].updateExprs
		}
	case 183:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:991
		{
			yyVAL.insRows = yyDollar[2].values
		}
	case 184:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:995
		{
			yyVAL.insRows = yyDollar[1].selStmt
		}
	case 185:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1001
		{
			yyVAL.values = Values{yyDollar[1].rowTuple}
		}
	case 186:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1005
		{
			yyVAL.values = append(yyDollar[1].values, yyDollar[3].rowTuple)
		}
	case 187:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1011
		{
			yyVAL.rowTuple = ValTuple(yyDollar[2].valExprs)
		}
	case 188:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1015
		{
			yyVAL.rowTuple = yyDollar[1].subquery
		}
	case 189:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1021
		{
			yyVAL.updateExprs = UpdateExprs{yyDollar[1].updateExpr}
		}
	case 190:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1025
		{
			yyVAL.updateExprs = append(yyDollar[1].updateExprs, yyDollar[3].updateExpr)
		}
	case 191:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1031
		{
			yyVAL.updateExpr = &UpdateExpr{Name: yyDollar[1].colName, Expr: yyDollar[3].valExpr}
		}
	case 192:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1036
		{
			yyVAL.empty = struct{}{}
		}
	case 193:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:1038
		{
			yyVAL.empty = struct{}{}
		}
	case 194:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1041
		{
			yyVAL.empty = struct{}{}
		}
	case 195:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1043
		{
			yyVAL.empty = struct{}{}
		}
	case 196:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1046
		{
			yyVAL.str = ""
		}
	case 197:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1048
		{
			yyVAL.str = AST_IGNORE
		}
	case 198:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1052
		{
			yyVAL.empty = struct{}{}
		}
	case 199:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1054
		{
			yyVAL.empty = struct{}{}
		}
	case 200:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1056
		{
			yyVAL.empty = struct{}{}
		}
	case 201:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1058
		{
			yyVAL.empty = struct{}{}
		}
	case 202:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1060
		{
			yyVAL.empty = struct{}{}
		}
	case 203:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1063
		{
			yyVAL.empty = struct{}{}
		}
	case 204:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1065
		{
			yyVAL.empty = struct{}{}
		}
	case 205:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1068
		{
			yyVAL.empty = struct{}{}
		}
	case 206:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1070
		{
			yyVAL.empty = struct{}{}
		}
	case 207:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1073
		{
			yyVAL.empty = struct{}{}
		}
	case 208:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:1075
		{
			yyVAL.empty = struct{}{}
		}
	case 209:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1079
		{
			yyVAL.sqlID = SQLName(strings.ToLower(string(yyDollar[1].bytes)))
		}
	case 210:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1085
		{
			yyVAL.sqlID = SQLName(yyDollar[1].bytes)
		}
	case 211:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1091
		{
			if incNesting(yylex) {
				yylex.Error("max nesting level reached")
				return 1
			}
		}
	case 212:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1100
		{
			decNesting(yylex)
		}
	case 213:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1105
		{
			forceEOF(yylex)
		}
	}
	goto yystack /* stack new state and value */
}
