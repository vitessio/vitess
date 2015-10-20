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
const UNION = 57347
const MINUS = 57348
const EXCEPT = 57349
const INTERSECT = 57350
const SELECT = 57351
const INSERT = 57352
const UPDATE = 57353
const DELETE = 57354
const FROM = 57355
const WHERE = 57356
const GROUP = 57357
const HAVING = 57358
const ORDER = 57359
const BY = 57360
const LIMIT = 57361
const FOR = 57362
const ALL = 57363
const DISTINCT = 57364
const AS = 57365
const EXISTS = 57366
const ASC = 57367
const DESC = 57368
const INTO = 57369
const DUPLICATE = 57370
const KEY = 57371
const DEFAULT = 57372
const SET = 57373
const LOCK = 57374
const KEYRANGE = 57375
const VALUES = 57376
const LAST_INSERT_ID = 57377
const JOIN = 57378
const STRAIGHT_JOIN = 57379
const LEFT = 57380
const RIGHT = 57381
const INNER = 57382
const OUTER = 57383
const CROSS = 57384
const NATURAL = 57385
const USE = 57386
const FORCE = 57387
const ON = 57388
const ID = 57389
const STRING = 57390
const NUMBER = 57391
const VALUE_ARG = 57392
const LIST_ARG = 57393
const COMMENT = 57394
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
	"UNION",
	"MINUS",
	"EXCEPT",
	"INTERSECT",
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
	"'('",
	"','",
	"')'",
	"ID",
	"STRING",
	"NUMBER",
	"VALUE_ARG",
	"LIST_ARG",
	"COMMENT",
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

const yyLast = 753

var yyAct = [...]int{

	98, 305, 165, 381, 93, 168, 336, 50, 63, 258,
	92, 346, 83, 204, 252, 240, 220, 94, 167, 3,
	214, 64, 82, 38, 192, 40, 59, 184, 110, 41,
	78, 322, 324, 51, 52, 70, 44, 43, 66, 44,
	265, 72, 65, 127, 75, 357, 53, 46, 47, 48,
	356, 198, 355, 71, 74, 49, 45, 105, 88, 333,
	68, 106, 107, 108, 196, 280, 109, 152, 153, 154,
	149, 137, 87, 112, 150, 151, 152, 153, 154, 149,
	124, 131, 226, 126, 199, 120, 135, 323, 116, 117,
	149, 95, 96, 395, 169, 225, 224, 97, 170, 172,
	173, 73, 139, 138, 171, 140, 119, 105, 335, 253,
	253, 111, 296, 122, 223, 180, 66, 140, 113, 66,
	65, 188, 187, 65, 182, 241, 195, 197, 194, 189,
	139, 138, 176, 73, 211, 138, 88, 210, 188, 202,
	186, 181, 68, 61, 219, 140, 209, 227, 228, 140,
	230, 231, 232, 233, 234, 235, 236, 237, 238, 239,
	91, 208, 241, 164, 166, 390, 241, 229, 281, 282,
	283, 222, 139, 138, 245, 278, 105, 88, 88, 61,
	215, 217, 218, 242, 244, 216, 123, 140, 79, 250,
	246, 132, 14, 262, 136, 91, 91, 247, 249, 342,
	241, 263, 61, 174, 175, 133, 241, 118, 177, 178,
	257, 212, 213, 106, 107, 108, 243, 241, 109, 105,
	279, 73, 243, 337, 245, 118, 133, 284, 286, 287,
	105, 266, 365, 61, 185, 208, 185, 206, 91, 77,
	362, 285, 293, 91, 91, 352, 289, 221, 337, 261,
	130, 88, 354, 222, 316, 353, 66, 66, 260, 317,
	65, 303, 320, 290, 301, 292, 295, 304, 267, 255,
	118, 291, 319, 300, 115, 28, 29, 30, 31, 91,
	91, 312, 313, 318, 314, 105, 256, 14, 80, 315,
	91, 330, 14, 387, 376, 326, 208, 208, 114, 334,
	328, 190, 42, 361, 332, 388, 343, 331, 340, 344,
	347, 206, 299, 129, 56, 54, 348, 339, 341, 241,
	306, 269, 270, 271, 272, 273, 297, 274, 275, 221,
	105, 351, 358, 68, 106, 107, 108, 58, 360, 109,
	307, 259, 350, 66, 32, 311, 112, 363, 359, 185,
	62, 394, 385, 91, 245, 14, 67, 371, 91, 369,
	34, 35, 36, 37, 95, 96, 33, 378, 347, 276,
	97, 380, 206, 206, 382, 382, 382, 379, 383, 384,
	377, 134, 191, 39, 111, 264, 66, 193, 69, 302,
	65, 396, 60, 254, 393, 386, 397, 389, 398, 391,
	392, 366, 76, 345, 349, 310, 81, 294, 364, 179,
	251, 100, 86, 99, 14, 15, 16, 17, 338, 60,
	298, 141, 89, 370, 321, 372, 121, 28, 29, 30,
	31, 125, 205, 268, 128, 203, 18, 148, 147, 155,
	156, 150, 151, 152, 153, 154, 149, 248, 85, 103,
	55, 27, 57, 13, 12, 91, 11, 91, 104, 10,
	373, 374, 375, 155, 156, 150, 151, 152, 153, 154,
	149, 60, 105, 183, 241, 68, 106, 107, 108, 9,
	8, 109, 101, 102, 200, 7, 90, 201, 112, 207,
	86, 6, 5, 4, 2, 1, 19, 20, 22, 21,
	23, 0, 0, 0, 0, 0, 95, 96, 84, 24,
	25, 26, 97, 103, 269, 270, 271, 272, 273, 0,
	274, 275, 104, 0, 0, 0, 111, 241, 0, 0,
	0, 86, 86, 0, 0, 0, 105, 0, 0, 68,
	106, 107, 108, 0, 14, 109, 101, 102, 0, 0,
	90, 0, 112, 0, 367, 368, 0, 0, 0, 103,
	0, 0, 277, 207, 0, 0, 0, 0, 104, 0,
	95, 96, 84, 0, 0, 0, 97, 0, 0, 0,
	0, 0, 105, 0, 0, 68, 106, 107, 108, 0,
	111, 109, 101, 102, 0, 0, 90, 0, 112, 0,
	0, 0, 0, 0, 0, 86, 148, 147, 155, 156,
	150, 151, 152, 153, 154, 149, 95, 96, 308, 103,
	0, 309, 97, 0, 207, 207, 0, 0, 104, 0,
	0, 0, 0, 0, 0, 325, 111, 327, 0, 0,
	0, 0, 105, 329, 0, 68, 106, 107, 108, 0,
	0, 109, 101, 102, 0, 0, 90, 0, 112, 0,
	148, 147, 155, 156, 150, 151, 152, 153, 154, 149,
	0, 0, 0, 0, 0, 0, 95, 96, 0, 0,
	0, 0, 97, 0, 0, 0, 0, 0, 0, 0,
	0, 143, 145, 0, 0, 0, 111, 157, 158, 159,
	160, 161, 162, 163, 146, 144, 142, 148, 147, 155,
	156, 150, 151, 152, 153, 154, 149, 288, 148, 147,
	155, 156, 150, 151, 152, 153, 154, 149, 0, 0,
	0, 0, 0, 0, 148, 147, 155, 156, 150, 151,
	152, 153, 154, 149, 147, 155, 156, 150, 151, 152,
	153, 154, 149,
}
var yyPact = [...]int{

	405, -1000, -1000, 422, -1000, -1000, -1000, -1000, -1000, -1000,
	-1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, -73,
	-61, -40, -49, -41, -1000, -1000, -1000, 346, 294, -1000,
	-1000, -1000, 292, -1000, -64, 93, 337, 92, -66, -44,
	83, -1000, -42, 83, -1000, 93, -71, 138, -71, 93,
	-1000, -1000, -1000, -1000, -1000, 489, -1000, 63, 271, 243,
	-1, -1000, 93, 159, -1000, 39, -1000, -4, -1000, 93,
	52, 136, -1000, -1000, 93, -1000, -56, 93, 289, 204,
	83, -1000, 178, -1000, -1000, 171, -18, 71, 630, -1000,
	595, 535, -1000, -1000, -1000, 10, 10, 10, 172, 172,
	-1000, -1000, -1000, 172, 172, -1000, -1000, -1000, -1000, -1000,
	-1000, -1000, 10, -1000, 93, 92, 93, 335, 92, 10,
	83, -1000, 277, -79, -1000, 34, -1000, 93, -1000, -1000,
	93, -1000, 129, 489, -1000, -1000, 83, 51, 595, 595,
	124, 10, 60, 20, 10, 10, 124, 10, 10, 10,
	10, 10, 10, 10, 10, 10, 10, -1000, -1000, -1000,
	-1000, -1000, -1000, -1000, 31, 630, 113, 270, 168, 630,
	-1000, 283, -1000, -1000, 425, 489, -1000, 346, 162, 45,
	641, 238, 222, -1000, 324, 595, -1000, 641, -1000, -1000,
	-1000, 203, 83, -1000, -59, -1000, -1000, -1000, -1000, -1000,
	-1000, -1000, -1000, 220, 285, 152, 183, -24, -1000, -1000,
	-1000, -1000, 31, 75, -1000, -1000, 112, -1000, -1000, 641,
	-1000, 283, -1000, -1000, 60, 10, 10, 641, 657, -1000,
	384, 666, -1000, -16, -16, 4, 4, 4, -7, -7,
	-1000, -1000, -1000, 10, -1000, 641, -1000, 157, 489, 157,
	194, 46, -1000, 595, 278, 92, 92, 324, 301, 322,
	71, 93, -1000, -1000, 93, -1000, 330, 129, 129, -1000,
	-1000, 248, 218, 247, 236, 226, -13, -1000, 93, 478,
	93, -1000, -1000, -1000, 168, -1000, 641, 583, 10, 641,
	-1000, 157, -1000, 162, -31, -1000, 10, 43, 202, 172,
	422, 177, 151, -1000, 301, -1000, 10, 10, -1000, -1000,
	326, 313, 285, 199, -1000, 219, -1000, 216, -1000, -1000,
	-1000, -1000, -45, -47, -52, -1000, -1000, -1000, -1000, 10,
	641, -1000, 76, -1000, 641, 10, -1000, 275, 192, -1000,
	-1000, -1000, 92, -1000, 360, 184, -1000, 529, -1000, 324,
	595, 10, 595, -1000, -1000, 172, 172, 172, 641, -1000,
	641, 265, 172, -1000, 10, 10, -1000, -1000, -1000, 301,
	71, 174, 71, 83, 83, 83, 341, -1000, 641, -1000,
	273, 117, -1000, 117, 117, 92, -1000, 340, 17, -1000,
	83, -1000, -1000, 159, -1000, 83, -1000, 83, -1000,
}
var yyPgo = [...]int{

	0, 495, 494, 18, 493, 492, 491, 485, 480, 479,
	459, 456, 454, 453, 344, 452, 451, 450, 22, 12,
	448, 435, 13, 433, 432, 26, 424, 3, 27, 72,
	422, 421, 420, 10, 2, 20, 16, 5, 418, 17,
	413, 28, 4, 411, 410, 14, 409, 407, 405, 404,
	9, 403, 11, 401, 1, 395, 393, 389, 6, 8,
	21, 302, 239, 388, 387, 385, 383, 382, 0, 381,
	356, 369, 7, 366, 104, 15,
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
	-10, -11, -12, -13, 9, 10, 11, 12, 31, 91,
	92, 94, 93, 95, 104, 105, 106, -16, 5, 6,
	7, 8, -14, -73, -14, -14, -14, -14, 96, -66,
	98, 102, -61, 98, 100, 96, 96, 97, 98, 96,
	-72, -72, -72, -3, 21, -17, 22, -15, -61, -25,
	-70, 50, 13, -59, -60, -42, -68, -70, 50, -63,
	101, 97, -68, 50, 96, -68, -70, -62, 101, 50,
	-62, -70, -18, -19, 83, -20, -70, -29, -34, -30,
	61, -74, -33, -42, -39, 81, 82, 87, -68, -40,
	-43, 57, 58, 24, 33, 47, 51, 52, 53, 56,
	-41, 101, 63, 55, 27, 31, 89, -25, 48, 67,
	89, -70, 61, 50, -72, -70, -72, 99, -70, 24,
	46, -68, 13, 48, -69, -68, 23, 89, 60, 59,
	74, -31, 76, 61, 75, 62, 74, 78, 77, 86,
	81, 82, 83, 84, 85, 79, 80, 67, 68, 69,
	70, 71, 72, 73, -29, -34, -29, -3, -37, -34,
	-34, -74, -34, -34, -74, -74, -41, -74, -74, -46,
	-34, -25, -59, -70, -28, 14, -60, -34, -68, -72,
	24, -67, 103, -64, 94, 92, 30, 93, 17, 50,
	-70, -70, -72, -21, -22, -24, -74, -70, -41, -19,
	-68, 83, -29, -29, -35, 56, 61, 57, 58, -34,
	-36, -74, -41, 54, 76, 75, 62, -34, -34, -35,
	-34, -34, -34, -34, -34, -34, -34, -34, -34, -34,
	-75, 49, -75, 48, -75, -34, -75, -18, 22, -18,
	-33, -44, -45, 64, -56, 31, -74, -28, -50, 17,
	-29, 46, -68, -72, -65, 99, -28, 48, -23, 36,
	37, 38, 39, 40, 42, 43, -71, -70, 23, -22,
	89, 56, 57, 58, -37, -36, -34, -34, 60, -34,
	-75, -18, -75, 48, -47, -45, 66, -29, -32, 34,
	-3, -59, -57, -42, -50, -54, 19, 18, -70, -70,
	-48, 15, -22, -22, 36, 41, 36, 41, 36, 36,
	36, -26, 44, 100, 45, -70, -75, -70, -75, 60,
	-34, -75, -33, 90, -34, 65, -58, 46, -38, -39,
	-58, -75, 48, -54, -34, -51, -52, -34, -72, -49,
	16, 18, 46, 36, 36, 97, 97, 97, -34, -75,
	-34, 28, 48, -42, 48, 48, -53, 25, 26, -50,
	-29, -37, -29, -74, -74, -74, 29, -39, -34, -52,
	-54, -27, -68, -27, -27, 11, -55, 20, 32, -75,
	48, -75, -75, -59, 11, 76, -68, -68, -68,
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
	47, 49, 83, 81, 48, 82, 89, 84, 3, 3,
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
	32, 33, 34, 35, 36, 37, 38, 39, 40, 41,
	42, 43, 44, 45, 46, 50, 51, 52, 53, 54,
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
		//line sql.y:167
		{
			setParseTree(yylex, yyDollar[1].statement)
		}
	case 2:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:173
		{
			yyVAL.statement = yyDollar[1].selStmt
		}
	case 13:
		yyDollar = yyS[yypt-12 : yypt+1]
		//line sql.y:189
		{
			yyVAL.selStmt = &Select{Comments: Comments(yyDollar[2].bytes2), Distinct: yyDollar[3].str, SelectExprs: yyDollar[4].selectExprs, From: yyDollar[6].tableExprs, Where: NewWhere(WhereStr, yyDollar[7].boolExpr), GroupBy: GroupBy(yyDollar[8].valExprs), Having: NewWhere(HavingStr, yyDollar[9].boolExpr), OrderBy: yyDollar[10].orderBy, Limit: yyDollar[11].limit, Lock: yyDollar[12].str}
		}
	case 14:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:193
		{
			yyVAL.selStmt = &Union{Type: yyDollar[2].str, Left: yyDollar[1].selStmt, Right: yyDollar[3].selStmt}
		}
	case 15:
		yyDollar = yyS[yypt-8 : yypt+1]
		//line sql.y:199
		{
			yyVAL.statement = &Insert{Comments: Comments(yyDollar[2].bytes2), Ignore: yyDollar[3].str, Table: yyDollar[5].tableName, Columns: yyDollar[6].columns, Rows: yyDollar[7].insRows, OnDup: OnDup(yyDollar[8].updateExprs)}
		}
	case 16:
		yyDollar = yyS[yypt-8 : yypt+1]
		//line sql.y:203
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
		//line sql.y:215
		{
			yyVAL.statement = &Update{Comments: Comments(yyDollar[2].bytes2), Table: yyDollar[3].tableName, Exprs: yyDollar[5].updateExprs, Where: NewWhere(WhereStr, yyDollar[6].boolExpr), OrderBy: yyDollar[7].orderBy, Limit: yyDollar[8].limit}
		}
	case 18:
		yyDollar = yyS[yypt-7 : yypt+1]
		//line sql.y:221
		{
			yyVAL.statement = &Delete{Comments: Comments(yyDollar[2].bytes2), Table: yyDollar[4].tableName, Where: NewWhere(WhereStr, yyDollar[5].boolExpr), OrderBy: yyDollar[6].orderBy, Limit: yyDollar[7].limit}
		}
	case 19:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:227
		{
			yyVAL.statement = &Set{Comments: Comments(yyDollar[2].bytes2), Exprs: yyDollar[3].updateExprs}
		}
	case 20:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:233
		{
			yyVAL.statement = &DDL{Action: CreateStr, NewName: yyDollar[4].sqlID}
		}
	case 21:
		yyDollar = yyS[yypt-8 : yypt+1]
		//line sql.y:237
		{
			// Change this to an alter statement
			yyVAL.statement = &DDL{Action: AlterStr, Table: yyDollar[7].sqlID, NewName: yyDollar[7].sqlID}
		}
	case 22:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:242
		{
			yyVAL.statement = &DDL{Action: CreateStr, NewName: SQLName(yyDollar[3].sqlID)}
		}
	case 23:
		yyDollar = yyS[yypt-6 : yypt+1]
		//line sql.y:248
		{
			yyVAL.statement = &DDL{Action: AlterStr, Table: yyDollar[4].sqlID, NewName: yyDollar[4].sqlID}
		}
	case 24:
		yyDollar = yyS[yypt-7 : yypt+1]
		//line sql.y:252
		{
			// Change this to a rename statement
			yyVAL.statement = &DDL{Action: RenameStr, Table: yyDollar[4].sqlID, NewName: yyDollar[7].sqlID}
		}
	case 25:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:257
		{
			yyVAL.statement = &DDL{Action: AlterStr, Table: SQLName(yyDollar[3].sqlID), NewName: SQLName(yyDollar[3].sqlID)}
		}
	case 26:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:263
		{
			yyVAL.statement = &DDL{Action: RenameStr, Table: yyDollar[3].sqlID, NewName: yyDollar[5].sqlID}
		}
	case 27:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:269
		{
			yyVAL.statement = &DDL{Action: DropStr, Table: yyDollar[4].sqlID}
		}
	case 28:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:273
		{
			// Change this to an alter statement
			yyVAL.statement = &DDL{Action: AlterStr, Table: yyDollar[5].sqlID, NewName: yyDollar[5].sqlID}
		}
	case 29:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:278
		{
			yyVAL.statement = &DDL{Action: DropStr, Table: SQLName(yyDollar[4].sqlID)}
		}
	case 30:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:284
		{
			yyVAL.statement = &DDL{Action: AlterStr, Table: yyDollar[3].sqlID, NewName: yyDollar[3].sqlID}
		}
	case 31:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:290
		{
			yyVAL.statement = &Other{}
		}
	case 32:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:294
		{
			yyVAL.statement = &Other{}
		}
	case 33:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:298
		{
			yyVAL.statement = &Other{}
		}
	case 34:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:303
		{
			setAllowComments(yylex, true)
		}
	case 35:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:307
		{
			yyVAL.bytes2 = yyDollar[2].bytes2
			setAllowComments(yylex, false)
		}
	case 36:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:313
		{
			yyVAL.bytes2 = nil
		}
	case 37:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:317
		{
			yyVAL.bytes2 = append(yyDollar[1].bytes2, yyDollar[2].bytes)
		}
	case 38:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:323
		{
			yyVAL.str = UnionStr
		}
	case 39:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:327
		{
			yyVAL.str = UnionAllStr
		}
	case 40:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:331
		{
			yyVAL.str = SetMinusStr
		}
	case 41:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:335
		{
			yyVAL.str = ExceptStr
		}
	case 42:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:339
		{
			yyVAL.str = IntersectStr
		}
	case 43:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:344
		{
			yyVAL.str = ""
		}
	case 44:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:348
		{
			yyVAL.str = DistinctStr
		}
	case 45:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:354
		{
			yyVAL.selectExprs = SelectExprs{yyDollar[1].selectExpr}
		}
	case 46:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:358
		{
			yyVAL.selectExprs = append(yyVAL.selectExprs, yyDollar[3].selectExpr)
		}
	case 47:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:364
		{
			yyVAL.selectExpr = &StarExpr{}
		}
	case 48:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:368
		{
			yyVAL.selectExpr = &NonStarExpr{Expr: yyDollar[1].expr, As: yyDollar[2].sqlID}
		}
	case 49:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:372
		{
			yyVAL.selectExpr = &StarExpr{TableName: yyDollar[1].sqlID}
		}
	case 50:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:378
		{
			yyVAL.expr = yyDollar[1].boolExpr
		}
	case 51:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:382
		{
			yyVAL.expr = yyDollar[1].valExpr
		}
	case 52:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:387
		{
			yyVAL.sqlID = ""
		}
	case 53:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:391
		{
			yyVAL.sqlID = yyDollar[1].sqlID
		}
	case 54:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:395
		{
			yyVAL.sqlID = yyDollar[2].sqlID
		}
	case 55:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:401
		{
			yyVAL.tableExprs = TableExprs{yyDollar[1].tableExpr}
		}
	case 56:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:405
		{
			yyVAL.tableExprs = append(yyVAL.tableExprs, yyDollar[3].tableExpr)
		}
	case 57:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:411
		{
			yyVAL.tableExpr = &AliasedTableExpr{Expr: yyDollar[1].smTableExpr, As: yyDollar[2].sqlID, Hints: yyDollar[3].indexHints}
		}
	case 58:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:415
		{
			yyVAL.tableExpr = &ParenTableExpr{Expr: yyDollar[2].tableExpr}
		}
	case 59:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:419
		{
			yyVAL.tableExpr = &JoinTableExpr{LeftExpr: yyDollar[1].tableExpr, Join: yyDollar[2].str, RightExpr: yyDollar[3].tableExpr}
		}
	case 60:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:423
		{
			yyVAL.tableExpr = &JoinTableExpr{LeftExpr: yyDollar[1].tableExpr, Join: yyDollar[2].str, RightExpr: yyDollar[3].tableExpr, On: yyDollar[5].boolExpr}
		}
	case 61:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:428
		{
			yyVAL.sqlID = ""
		}
	case 62:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:432
		{
			yyVAL.sqlID = yyDollar[1].sqlID
		}
	case 63:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:436
		{
			yyVAL.sqlID = yyDollar[2].sqlID
		}
	case 64:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:442
		{
			yyVAL.str = JoinStr
		}
	case 65:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:446
		{
			yyVAL.str = StraightJoinStr
		}
	case 66:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:450
		{
			yyVAL.str = LeftJoinStr
		}
	case 67:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:454
		{
			yyVAL.str = LeftJoinStr
		}
	case 68:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:458
		{
			yyVAL.str = RightJoinStr
		}
	case 69:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:462
		{
			yyVAL.str = RightJoinStr
		}
	case 70:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:466
		{
			yyVAL.str = JoinStr
		}
	case 71:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:470
		{
			yyVAL.str = CrossJoinStr
		}
	case 72:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:474
		{
			yyVAL.str = NaturalJoinStr
		}
	case 73:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:480
		{
			yyVAL.smTableExpr = &TableName{Name: yyDollar[1].sqlID}
		}
	case 74:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:484
		{
			yyVAL.smTableExpr = &TableName{Qualifier: yyDollar[1].sqlID, Name: yyDollar[3].sqlID}
		}
	case 75:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:488
		{
			yyVAL.smTableExpr = yyDollar[1].subquery
		}
	case 76:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:494
		{
			yyVAL.tableName = &TableName{Name: yyDollar[1].sqlID}
		}
	case 77:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:498
		{
			yyVAL.tableName = &TableName{Qualifier: yyDollar[1].sqlID, Name: yyDollar[3].sqlID}
		}
	case 78:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:503
		{
			yyVAL.indexHints = nil
		}
	case 79:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:507
		{
			yyVAL.indexHints = &IndexHints{Type: UseStr, Indexes: yyDollar[4].sqlIDs}
		}
	case 80:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:511
		{
			yyVAL.indexHints = &IndexHints{Type: IgnoreStr, Indexes: yyDollar[4].sqlIDs}
		}
	case 81:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:515
		{
			yyVAL.indexHints = &IndexHints{Type: ForceStr, Indexes: yyDollar[4].sqlIDs}
		}
	case 82:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:521
		{
			yyVAL.sqlIDs = []SQLName{yyDollar[1].sqlID}
		}
	case 83:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:525
		{
			yyVAL.sqlIDs = append(yyDollar[1].sqlIDs, yyDollar[3].sqlID)
		}
	case 84:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:530
		{
			yyVAL.boolExpr = nil
		}
	case 85:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:534
		{
			yyVAL.boolExpr = yyDollar[2].boolExpr
		}
	case 87:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:541
		{
			yyVAL.boolExpr = &AndExpr{Left: yyDollar[1].boolExpr, Right: yyDollar[3].boolExpr}
		}
	case 88:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:545
		{
			yyVAL.boolExpr = &OrExpr{Left: yyDollar[1].boolExpr, Right: yyDollar[3].boolExpr}
		}
	case 89:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:549
		{
			yyVAL.boolExpr = &NotExpr{Expr: yyDollar[2].boolExpr}
		}
	case 90:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:553
		{
			yyVAL.boolExpr = &ParenBoolExpr{Expr: yyDollar[2].boolExpr}
		}
	case 91:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:557
		{
			yyVAL.boolExpr = &IsExpr{Operator: yyDollar[3].str, Expr: yyDollar[1].boolExpr}
		}
	case 92:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:563
		{
			yyVAL.boolExpr = BoolVal(true)
		}
	case 93:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:567
		{
			yyVAL.boolExpr = BoolVal(false)
		}
	case 94:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:571
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: yyDollar[2].str, Right: yyDollar[3].valExpr}
		}
	case 95:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:575
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: InStr, Right: yyDollar[3].colTuple}
		}
	case 96:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:579
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: NotInStr, Right: yyDollar[4].colTuple}
		}
	case 97:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:583
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: LikeStr, Right: yyDollar[3].valExpr}
		}
	case 98:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:587
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: NotLikeStr, Right: yyDollar[4].valExpr}
		}
	case 99:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:591
		{
			yyVAL.boolExpr = &RangeCond{Left: yyDollar[1].valExpr, Operator: BetweenStr, From: yyDollar[3].valExpr, To: yyDollar[5].valExpr}
		}
	case 100:
		yyDollar = yyS[yypt-6 : yypt+1]
		//line sql.y:595
		{
			yyVAL.boolExpr = &RangeCond{Left: yyDollar[1].valExpr, Operator: NotBetweenStr, From: yyDollar[4].valExpr, To: yyDollar[6].valExpr}
		}
	case 101:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:599
		{
			yyVAL.boolExpr = &IsExpr{Operator: yyDollar[3].str, Expr: yyDollar[1].valExpr}
		}
	case 102:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:603
		{
			yyVAL.boolExpr = &ExistsExpr{Subquery: yyDollar[2].subquery}
		}
	case 103:
		yyDollar = yyS[yypt-6 : yypt+1]
		//line sql.y:607
		{
			yyVAL.boolExpr = &KeyrangeExpr{Start: yyDollar[3].valExpr, End: yyDollar[5].valExpr}
		}
	case 104:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:613
		{
			yyVAL.str = IsNullStr
		}
	case 105:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:617
		{
			yyVAL.str = IsNotNullStr
		}
	case 106:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:621
		{
			yyVAL.str = IsTrueStr
		}
	case 107:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:625
		{
			yyVAL.str = IsNotTrueStr
		}
	case 108:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:629
		{
			yyVAL.str = IsFalseStr
		}
	case 109:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:633
		{
			yyVAL.str = IsNotFalseStr
		}
	case 110:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:639
		{
			yyVAL.str = EqualStr
		}
	case 111:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:643
		{
			yyVAL.str = LessThanStr
		}
	case 112:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:647
		{
			yyVAL.str = GreaterThanStr
		}
	case 113:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:651
		{
			yyVAL.str = LessEqualStr
		}
	case 114:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:655
		{
			yyVAL.str = GreaterEqualStr
		}
	case 115:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:659
		{
			yyVAL.str = NotEqualStr
		}
	case 116:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:663
		{
			yyVAL.str = NullSafeEqualStr
		}
	case 117:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:669
		{
			yyVAL.colTuple = ValTuple(yyDollar[2].valExprs)
		}
	case 118:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:673
		{
			yyVAL.colTuple = yyDollar[1].subquery
		}
	case 119:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:677
		{
			yyVAL.colTuple = ListArg(yyDollar[1].bytes)
		}
	case 120:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:683
		{
			yyVAL.subquery = &Subquery{yyDollar[2].selStmt}
		}
	case 121:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:689
		{
			yyVAL.valExprs = ValExprs{yyDollar[1].valExpr}
		}
	case 122:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:693
		{
			yyVAL.valExprs = append(yyDollar[1].valExprs, yyDollar[3].valExpr)
		}
	case 123:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:699
		{
			yyVAL.valExpr = yyDollar[1].valExpr
		}
	case 124:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:703
		{
			yyVAL.valExpr = yyDollar[1].colName
		}
	case 125:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:707
		{
			yyVAL.valExpr = yyDollar[1].rowTuple
		}
	case 126:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:711
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: BitAndStr, Right: yyDollar[3].valExpr}
		}
	case 127:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:715
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: BitOrStr, Right: yyDollar[3].valExpr}
		}
	case 128:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:719
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: BitXorStr, Right: yyDollar[3].valExpr}
		}
	case 129:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:723
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: PlusStr, Right: yyDollar[3].valExpr}
		}
	case 130:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:727
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: MinusStr, Right: yyDollar[3].valExpr}
		}
	case 131:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:731
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: MultStr, Right: yyDollar[3].valExpr}
		}
	case 132:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:735
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: DivStr, Right: yyDollar[3].valExpr}
		}
	case 133:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:739
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: ModStr, Right: yyDollar[3].valExpr}
		}
	case 134:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:743
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: ShiftLeftStr, Right: yyDollar[3].valExpr}
		}
	case 135:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:747
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: ShiftRightStr, Right: yyDollar[3].valExpr}
		}
	case 136:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:751
		{
			if num, ok := yyDollar[2].valExpr.(NumVal); ok {
				yyVAL.valExpr = num
			} else {
				yyVAL.valExpr = &UnaryExpr{Operator: UPlusStr, Expr: yyDollar[2].valExpr}
			}
		}
	case 137:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:759
		{
			if num, ok := yyDollar[2].valExpr.(NumVal); ok {
				// Handle double negative
				if num[0] == '-' {
					yyVAL.valExpr = num[1:]
				} else {
					yyVAL.valExpr = append(NumVal("-"), num...)
				}
			} else {
				yyVAL.valExpr = &UnaryExpr{Operator: UMinusStr, Expr: yyDollar[2].valExpr}
			}
		}
	case 138:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:772
		{
			yyVAL.valExpr = &UnaryExpr{Operator: TildaStr, Expr: yyDollar[2].valExpr}
		}
	case 139:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:776
		{
			yyVAL.valExpr = &FuncExpr{Name: string(yyDollar[1].sqlID)}
		}
	case 140:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:780
		{
			yyVAL.valExpr = &FuncExpr{Name: string(yyDollar[1].sqlID), Exprs: yyDollar[3].selectExprs}
		}
	case 141:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:784
		{
			yyVAL.valExpr = &FuncExpr{Name: string(yyDollar[1].sqlID), Distinct: true, Exprs: yyDollar[4].selectExprs}
		}
	case 142:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:788
		{
			yyVAL.valExpr = &FuncExpr{Name: yyDollar[1].str, Exprs: yyDollar[3].selectExprs}
		}
	case 143:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:792
		{
			yyVAL.valExpr = yyDollar[1].caseExpr
		}
	case 144:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:798
		{
			yyVAL.str = "if"
		}
	case 145:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:804
		{
			yyVAL.caseExpr = &CaseExpr{Expr: yyDollar[2].valExpr, Whens: yyDollar[3].whens, Else: yyDollar[4].valExpr}
		}
	case 146:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:809
		{
			yyVAL.valExpr = nil
		}
	case 147:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:813
		{
			yyVAL.valExpr = yyDollar[1].valExpr
		}
	case 148:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:819
		{
			yyVAL.whens = []*When{yyDollar[1].when}
		}
	case 149:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:823
		{
			yyVAL.whens = append(yyDollar[1].whens, yyDollar[2].when)
		}
	case 150:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:829
		{
			yyVAL.when = &When{Cond: yyDollar[2].boolExpr, Val: yyDollar[4].valExpr}
		}
	case 151:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:834
		{
			yyVAL.valExpr = nil
		}
	case 152:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:838
		{
			yyVAL.valExpr = yyDollar[2].valExpr
		}
	case 153:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:844
		{
			yyVAL.colName = &ColName{Name: yyDollar[1].sqlID}
		}
	case 154:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:848
		{
			yyVAL.colName = &ColName{Qualifier: yyDollar[1].sqlID, Name: yyDollar[3].sqlID}
		}
	case 155:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:854
		{
			yyVAL.valExpr = StrVal(yyDollar[1].bytes)
		}
	case 156:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:858
		{
			yyVAL.valExpr = NumVal(yyDollar[1].bytes)
		}
	case 157:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:862
		{
			yyVAL.valExpr = ValArg(yyDollar[1].bytes)
		}
	case 158:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:866
		{
			yyVAL.valExpr = &NullVal{}
		}
	case 159:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:871
		{
			yyVAL.valExprs = nil
		}
	case 160:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:875
		{
			yyVAL.valExprs = yyDollar[3].valExprs
		}
	case 161:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:880
		{
			yyVAL.boolExpr = nil
		}
	case 162:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:884
		{
			yyVAL.boolExpr = yyDollar[2].boolExpr
		}
	case 163:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:889
		{
			yyVAL.orderBy = nil
		}
	case 164:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:893
		{
			yyVAL.orderBy = yyDollar[3].orderBy
		}
	case 165:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:899
		{
			yyVAL.orderBy = OrderBy{yyDollar[1].order}
		}
	case 166:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:903
		{
			yyVAL.orderBy = append(yyDollar[1].orderBy, yyDollar[3].order)
		}
	case 167:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:909
		{
			yyVAL.order = &Order{Expr: yyDollar[1].valExpr, Direction: yyDollar[2].str}
		}
	case 168:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:914
		{
			yyVAL.str = AscScr
		}
	case 169:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:918
		{
			yyVAL.str = AscScr
		}
	case 170:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:922
		{
			yyVAL.str = DescScr
		}
	case 171:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:927
		{
			yyVAL.limit = nil
		}
	case 172:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:931
		{
			yyVAL.limit = &Limit{Rowcount: yyDollar[2].valExpr}
		}
	case 173:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:935
		{
			yyVAL.limit = &Limit{Offset: yyDollar[2].valExpr, Rowcount: yyDollar[4].valExpr}
		}
	case 174:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:940
		{
			yyVAL.str = ""
		}
	case 175:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:944
		{
			yyVAL.str = ForUpdateStr
		}
	case 176:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:948
		{
			if yyDollar[3].sqlID != "share" {
				yylex.Error("expecting share")
				return 1
			}
			if yyDollar[4].sqlID != "mode" {
				yylex.Error("expecting mode")
				return 1
			}
			yyVAL.str = ShareModeStr
		}
	case 177:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:961
		{
			yyVAL.columns = nil
		}
	case 178:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:965
		{
			yyVAL.columns = yyDollar[2].columns
		}
	case 179:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:971
		{
			yyVAL.columns = Columns{&NonStarExpr{Expr: yyDollar[1].colName}}
		}
	case 180:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:975
		{
			yyVAL.columns = append(yyVAL.columns, &NonStarExpr{Expr: yyDollar[3].colName})
		}
	case 181:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:980
		{
			yyVAL.updateExprs = nil
		}
	case 182:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:984
		{
			yyVAL.updateExprs = yyDollar[5].updateExprs
		}
	case 183:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:990
		{
			yyVAL.insRows = yyDollar[2].values
		}
	case 184:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:994
		{
			yyVAL.insRows = yyDollar[1].selStmt
		}
	case 185:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1000
		{
			yyVAL.values = Values{yyDollar[1].rowTuple}
		}
	case 186:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1004
		{
			yyVAL.values = append(yyDollar[1].values, yyDollar[3].rowTuple)
		}
	case 187:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1010
		{
			yyVAL.rowTuple = ValTuple(yyDollar[2].valExprs)
		}
	case 188:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1014
		{
			yyVAL.rowTuple = yyDollar[1].subquery
		}
	case 189:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1020
		{
			yyVAL.updateExprs = UpdateExprs{yyDollar[1].updateExpr}
		}
	case 190:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1024
		{
			yyVAL.updateExprs = append(yyDollar[1].updateExprs, yyDollar[3].updateExpr)
		}
	case 191:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1030
		{
			yyVAL.updateExpr = &UpdateExpr{Name: yyDollar[1].colName, Expr: yyDollar[3].valExpr}
		}
	case 192:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1035
		{
			yyVAL.empty = struct{}{}
		}
	case 193:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:1037
		{
			yyVAL.empty = struct{}{}
		}
	case 194:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1040
		{
			yyVAL.empty = struct{}{}
		}
	case 195:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1042
		{
			yyVAL.empty = struct{}{}
		}
	case 196:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1045
		{
			yyVAL.str = ""
		}
	case 197:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1047
		{
			yyVAL.str = IgnoreStr
		}
	case 198:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1051
		{
			yyVAL.empty = struct{}{}
		}
	case 199:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1053
		{
			yyVAL.empty = struct{}{}
		}
	case 200:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1055
		{
			yyVAL.empty = struct{}{}
		}
	case 201:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1057
		{
			yyVAL.empty = struct{}{}
		}
	case 202:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1059
		{
			yyVAL.empty = struct{}{}
		}
	case 203:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1062
		{
			yyVAL.empty = struct{}{}
		}
	case 204:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1064
		{
			yyVAL.empty = struct{}{}
		}
	case 205:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1067
		{
			yyVAL.empty = struct{}{}
		}
	case 206:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1069
		{
			yyVAL.empty = struct{}{}
		}
	case 207:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1072
		{
			yyVAL.empty = struct{}{}
		}
	case 208:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:1074
		{
			yyVAL.empty = struct{}{}
		}
	case 209:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1078
		{
			yyVAL.sqlID = SQLName(strings.ToLower(string(yyDollar[1].bytes)))
		}
	case 210:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1084
		{
			yyVAL.sqlID = SQLName(yyDollar[1].bytes)
		}
	case 211:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1090
		{
			if incNesting(yylex) {
				yylex.Error("max nesting level reached")
				return 1
			}
		}
	case 212:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1099
		{
			decNesting(yylex)
		}
	case 213:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1104
		{
			forceEOF(yylex)
		}
	}
	goto yystack /* stack new state and value */
}
