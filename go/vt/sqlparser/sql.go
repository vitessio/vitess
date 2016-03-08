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
const NEXT = 57378
const VALUE = 57379
const JOIN = 57380
const STRAIGHT_JOIN = 57381
const LEFT = 57382
const RIGHT = 57383
const INNER = 57384
const OUTER = 57385
const CROSS = 57386
const NATURAL = 57387
const USE = 57388
const FORCE = 57389
const ON = 57390
const ID = 57391
const STRING = 57392
const NUMBER = 57393
const VALUE_ARG = 57394
const LIST_ARG = 57395
const COMMENT = 57396
const NULL = 57397
const TRUE = 57398
const FALSE = 57399
const OR = 57400
const AND = 57401
const NOT = 57402
const BETWEEN = 57403
const CASE = 57404
const WHEN = 57405
const THEN = 57406
const ELSE = 57407
const LE = 57408
const GE = 57409
const NE = 57410
const NULL_SAFE_EQUAL = 57411
const IS = 57412
const LIKE = 57413
const REGEXP = 57414
const IN = 57415
const SHIFT_LEFT = 57416
const SHIFT_RIGHT = 57417
const UNARY = 57418
const END = 57419
const CREATE = 57420
const ALTER = 57421
const DROP = 57422
const RENAME = 57423
const ANALYZE = 57424
const TABLE = 57425
const INDEX = 57426
const VIEW = 57427
const TO = 57428
const IGNORE = 57429
const IF = 57430
const UNIQUE = 57431
const USING = 57432
const SHOW = 57433
const DESCRIBE = 57434
const EXPLAIN = 57435

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
	"NEXT",
	"VALUE",
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
	"REGEXP",
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
const yyInitialStackSize = 16

//line yacctab:1
var yyExca = [...]int{
	-1, 1,
	1, -1,
	-2, 0,
	-1, 68,
	92, 220,
	-2, 219,
}

const yyNprod = 224
const yyPrivate = 57344

var yyTokenNames []string
var yyStates []string

const yyLast = 796

var yyAct = [...]int{

	99, 319, 168, 401, 94, 171, 364, 50, 63, 277,
	266, 354, 208, 93, 207, 95, 206, 170, 3, 248,
	226, 260, 82, 220, 187, 64, 59, 195, 339, 341,
	78, 111, 83, 51, 52, 70, 44, 43, 66, 44,
	273, 72, 65, 106, 75, 53, 68, 107, 108, 109,
	128, 38, 110, 40, 46, 47, 48, 41, 89, 113,
	376, 375, 374, 71, 74, 49, 45, 351, 292, 138,
	121, 88, 117, 152, 415, 383, 140, 249, 96, 97,
	125, 132, 142, 127, 98, 340, 136, 141, 140, 118,
	142, 201, 120, 73, 261, 172, 310, 261, 112, 173,
	175, 176, 142, 123, 199, 151, 150, 158, 159, 153,
	154, 155, 156, 157, 152, 114, 183, 66, 288, 137,
	66, 65, 191, 190, 65, 185, 202, 216, 106, 73,
	192, 155, 156, 157, 152, 229, 179, 89, 215, 191,
	205, 106, 184, 14, 61, 189, 225, 61, 73, 234,
	235, 236, 68, 238, 239, 240, 241, 242, 243, 244,
	245, 246, 247, 167, 169, 211, 61, 214, 124, 198,
	200, 197, 79, 237, 410, 249, 228, 253, 360, 249,
	89, 89, 249, 106, 370, 119, 68, 107, 108, 109,
	250, 252, 110, 233, 251, 258, 270, 254, 384, 113,
	255, 257, 141, 140, 271, 381, 231, 232, 230, 133,
	265, 106, 218, 219, 294, 295, 296, 142, 96, 97,
	134, 249, 141, 140, 98, 251, 249, 14, 353, 291,
	253, 274, 355, 297, 299, 300, 301, 142, 112, 307,
	221, 223, 224, 373, 211, 222, 134, 174, 107, 108,
	109, 298, 188, 110, 303, 28, 29, 30, 31, 89,
	268, 355, 228, 119, 66, 66, 188, 106, 65, 317,
	61, 372, 315, 275, 249, 304, 318, 306, 263, 305,
	314, 309, 153, 154, 155, 156, 157, 152, 275, 327,
	326, 329, 328, 77, 269, 337, 106, 131, 331, 334,
	330, 249, 119, 92, 335, 348, 139, 211, 211, 211,
	211, 344, 14, 352, 332, 116, 42, 346, 396, 333,
	361, 350, 115, 362, 365, 349, 407, 358, 380, 357,
	366, 193, 336, 311, 283, 284, 359, 313, 408, 92,
	92, 130, 80, 290, 56, 54, 217, 177, 178, 320,
	377, 58, 180, 181, 369, 321, 379, 267, 368, 325,
	188, 66, 62, 414, 405, 382, 28, 29, 30, 31,
	378, 14, 253, 33, 1, 390, 289, 286, 388, 135,
	194, 212, 92, 67, 39, 272, 398, 365, 92, 92,
	400, 399, 227, 196, 402, 402, 402, 397, 403, 404,
	69, 279, 282, 283, 284, 280, 66, 281, 285, 316,
	65, 416, 262, 406, 413, 385, 417, 363, 418, 60,
	367, 409, 324, 411, 412, 92, 92, 308, 182, 76,
	259, 32, 264, 81, 386, 387, 92, 101, 100, 86,
	389, 356, 391, 392, 312, 143, 60, 34, 35, 36,
	37, 90, 338, 122, 210, 278, 276, 209, 126, 85,
	212, 129, 55, 27, 57, 14, 15, 16, 17, 150,
	158, 159, 153, 154, 155, 156, 157, 152, 227, 158,
	159, 153, 154, 155, 156, 157, 152, 18, 13, 151,
	150, 158, 159, 153, 154, 155, 156, 157, 152, 60,
	12, 186, 11, 10, 92, 256, 9, 104, 8, 92,
	7, 6, 203, 5, 4, 204, 105, 213, 86, 87,
	2, 0, 0, 212, 212, 212, 212, 0, 0, 0,
	0, 0, 106, 0, 249, 68, 107, 108, 109, 0,
	0, 110, 102, 103, 0, 0, 91, 104, 113, 0,
	19, 20, 22, 21, 23, 0, 105, 0, 0, 87,
	0, 86, 86, 24, 25, 26, 0, 96, 97, 84,
	0, 0, 106, 98, 0, 68, 107, 108, 109, 0,
	0, 110, 102, 103, 0, 0, 91, 112, 113, 0,
	0, 0, 0, 0, 287, 0, 213, 0, 0, 0,
	0, 293, 0, 0, 0, 0, 0, 96, 97, 84,
	0, 0, 0, 98, 0, 0, 92, 0, 92, 92,
	0, 0, 393, 394, 395, 0, 0, 112, 0, 279,
	282, 283, 284, 280, 14, 281, 285, 0, 0, 371,
	86, 0, 0, 0, 0, 0, 0, 0, 0, 104,
	0, 0, 0, 322, 0, 0, 323, 0, 105, 213,
	213, 213, 213, 0, 0, 0, 104, 0, 0, 0,
	0, 0, 342, 343, 106, 105, 345, 68, 107, 108,
	109, 0, 0, 110, 102, 103, 0, 0, 91, 0,
	113, 106, 0, 0, 68, 107, 108, 109, 347, 0,
	110, 102, 103, 0, 0, 91, 0, 113, 0, 96,
	97, 0, 0, 0, 0, 98, 151, 150, 158, 159,
	153, 154, 155, 156, 157, 152, 96, 97, 0, 112,
	0, 0, 98, 0, 0, 0, 0, 0, 0, 0,
	0, 145, 148, 0, 0, 0, 112, 160, 161, 162,
	163, 164, 165, 166, 149, 146, 147, 144, 151, 150,
	158, 159, 153, 154, 155, 156, 157, 152, 302, 151,
	150, 158, 159, 153, 154, 155, 156, 157, 152, 0,
	0, 0, 0, 0, 0, 0, 151, 150, 158, 159,
	153, 154, 155, 156, 157, 152,
}
var yyPact = [...]int{

	456, -1000, -1000, 361, -1000, -1000, -1000, -1000, -1000, -1000,
	-1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, -48,
	-64, -33, -45, -34, -1000, -1000, -1000, 362, 324, -1000,
	-1000, -1000, 322, -1000, -67, 114, 349, 100, -69, -37,
	77, -1000, -35, 77, -1000, 114, -74, 120, -74, 114,
	-1000, -1000, -1000, -1000, -1000, 523, -1000, 58, 295, 284,
	-20, -1000, 114, 135, -1000, 23, -1000, -22, -1000, 114,
	40, 116, -1000, -1000, 114, -1000, -52, 114, 317, 249,
	77, -1000, 196, -1000, -1000, 96, -23, 269, 141, 678,
	-1000, 642, 625, -1000, -1000, -1000, -6, -6, -6, 162,
	162, -1000, -1000, -1000, 162, 162, -1000, -1000, -1000, -1000,
	-1000, -1000, -1000, -6, -1000, 114, 100, 114, 346, 100,
	-6, 77, -1000, 307, -79, -1000, 74, -1000, 114, -1000,
	-1000, 114, -1000, 92, 523, -1000, -1000, 77, 41, 326,
	642, 642, 182, -6, 79, 129, -6, -6, -6, 182,
	-6, -6, -6, -6, -6, -6, -6, -6, -6, -6,
	-1000, -1000, -1000, -1000, -1000, -1000, -1000, 6, 678, 26,
	250, 175, 678, -1000, 134, -1000, -1000, 483, 523, -1000,
	362, 195, 31, 689, 247, 252, -1000, 340, 642, -1000,
	689, -1000, -1000, -1000, 246, 77, -1000, -62, -1000, -1000,
	-1000, -1000, -1000, -1000, -1000, -1000, 238, 363, -1000, -1000,
	95, 320, 218, -24, -1000, -1000, -1000, 114, 6, 14,
	-1000, -1000, 156, -1000, -1000, 689, -1000, 134, -1000, -1000,
	79, -6, -6, -6, 689, 689, 706, -1000, 397, 388,
	-1000, 45, 45, -16, -16, -16, 198, 198, -1000, -1000,
	-1000, -6, -1000, 689, -1000, 170, 523, 170, 189, 28,
	-1000, 642, 303, 100, 100, 340, 330, 337, 141, 114,
	-1000, -1000, 114, -1000, 344, 92, 92, 92, 92, -1000,
	262, 260, -1000, 276, 261, 294, -18, -1000, 114, 114,
	-1000, 223, 114, -1000, -1000, -1000, -1000, 175, -1000, 689,
	689, 636, -6, 689, -1000, 170, -1000, 195, -26, -1000,
	-6, 161, 184, 162, 361, 213, 128, -1000, 330, -1000,
	-6, -6, -1000, -1000, 342, 336, 363, 136, 591, -1000,
	-1000, -1000, -1000, 233, -1000, 205, -1000, -1000, -1000, -38,
	-39, -40, -1000, -1000, -1000, -1000, -1000, -6, 689, -1000,
	131, -1000, 689, -6, -1000, 300, 155, -1000, -1000, -1000,
	100, -1000, 25, 148, -1000, 409, -1000, 340, 642, -6,
	642, 642, -1000, -1000, 162, 162, 162, 689, -1000, 689,
	289, 162, -1000, -6, -6, -1000, -1000, -1000, 330, 141,
	144, 141, 141, 77, 77, 77, 353, -1000, 689, -1000,
	306, 124, -1000, 124, 124, 100, -1000, 352, -5, -1000,
	77, -1000, -1000, 135, -1000, 77, -1000, 77, -1000,
}
var yyPgo = [...]int{

	0, 520, 17, 514, 513, 511, 510, 508, 506, 503,
	502, 500, 488, 431, 464, 463, 462, 22, 32, 459,
	16, 14, 12, 457, 456, 9, 455, 454, 26, 452,
	3, 24, 71, 451, 445, 444, 13, 2, 23, 20,
	5, 441, 15, 438, 31, 4, 437, 430, 21, 428,
	427, 422, 420, 10, 417, 6, 415, 1, 413, 412,
	409, 11, 8, 25, 316, 293, 400, 393, 385, 384,
	380, 0, 379, 383, 377, 376, 7, 374, 373, 247,
	19,
}
var yyR1 = [...]int{

	0, 77, 1, 1, 1, 1, 1, 1, 1, 1,
	1, 1, 1, 2, 2, 3, 3, 4, 5, 6,
	7, 7, 7, 8, 8, 8, 9, 10, 10, 10,
	11, 12, 12, 12, 78, 13, 14, 14, 15, 15,
	15, 15, 15, 16, 16, 17, 17, 18, 18, 18,
	18, 19, 19, 72, 72, 72, 20, 20, 21, 21,
	22, 22, 22, 23, 23, 23, 23, 75, 75, 74,
	74, 74, 24, 24, 24, 24, 25, 25, 25, 25,
	26, 26, 27, 27, 28, 28, 29, 29, 29, 29,
	30, 30, 31, 31, 32, 32, 32, 32, 32, 32,
	33, 33, 33, 33, 33, 33, 33, 33, 33, 33,
	33, 33, 33, 33, 38, 38, 38, 38, 38, 38,
	34, 34, 34, 34, 34, 34, 34, 39, 39, 39,
	44, 40, 40, 37, 37, 37, 37, 37, 37, 37,
	37, 37, 37, 37, 37, 37, 37, 37, 37, 37,
	37, 37, 37, 37, 43, 46, 49, 49, 47, 47,
	48, 50, 50, 45, 45, 36, 36, 36, 36, 51,
	51, 52, 52, 53, 53, 54, 54, 55, 56, 56,
	56, 57, 57, 57, 58, 58, 58, 59, 59, 60,
	60, 61, 61, 35, 35, 41, 41, 42, 42, 62,
	62, 63, 65, 65, 66, 66, 64, 64, 67, 67,
	67, 67, 67, 68, 68, 69, 69, 70, 70, 71,
	73, 79, 80, 76,
}
var yyR2 = [...]int{

	0, 1, 1, 1, 1, 1, 1, 1, 1, 1,
	1, 1, 1, 12, 3, 8, 8, 8, 7, 3,
	5, 8, 4, 6, 7, 4, 5, 4, 5, 5,
	3, 2, 2, 2, 0, 2, 0, 2, 1, 2,
	1, 1, 1, 0, 1, 1, 3, 1, 2, 3,
	4, 1, 1, 0, 1, 2, 1, 3, 1, 1,
	3, 3, 3, 3, 5, 5, 3, 0, 1, 0,
	1, 2, 1, 2, 2, 1, 2, 3, 2, 3,
	2, 2, 1, 3, 1, 3, 0, 5, 5, 5,
	1, 3, 0, 2, 1, 3, 3, 2, 3, 3,
	1, 1, 3, 3, 4, 3, 4, 3, 4, 5,
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

	-1000, -77, -1, -2, -3, -4, -5, -6, -7, -8,
	-9, -10, -11, -12, 9, 10, 11, 12, 31, 94,
	95, 97, 96, 98, 107, 108, 109, -15, 5, 6,
	7, 8, -13, -78, -13, -13, -13, -13, 99, -69,
	101, 105, -64, 101, 103, 99, 99, 100, 101, 99,
	-76, -76, -76, -2, 21, -16, 22, -14, -64, -28,
	-73, 52, 13, -62, -63, -45, -71, -73, 52, -66,
	104, 100, -71, 52, 99, -71, -73, -65, 104, 52,
	-65, -73, -17, -18, 86, -19, -73, 36, -32, -37,
	-33, 63, -79, -36, -45, -42, 84, 85, 90, -71,
	-43, -46, 59, 60, 24, 33, 49, 53, 54, 55,
	58, -44, 104, 65, 57, 27, 31, 92, -28, 50,
	69, 92, -73, 63, 52, -76, -73, -76, 102, -73,
	24, 48, -71, 13, 50, -72, -71, 23, 92, 37,
	62, 61, 76, -34, 79, 63, 77, 78, 64, 76,
	81, 80, 89, 84, 85, 86, 87, 88, 82, 83,
	69, 70, 71, 72, 73, 74, 75, -32, -37, -32,
	-2, -40, -37, -37, -79, -37, -37, -79, -79, -44,
	-79, -79, -49, -37, -28, -62, -73, -31, 14, -63,
	-37, -71, -76, 24, -70, 106, -67, 97, 95, 30,
	96, 17, 52, -73, -73, -76, -20, -21, -22, -23,
	-27, -44, -79, -73, -18, -71, 86, 20, -32, -32,
	-38, 58, 63, 59, 60, -37, -39, -79, -44, 56,
	79, 77, 78, 64, -37, -37, -37, -38, -37, -37,
	-37, -37, -37, -37, -37, -37, -37, -37, -80, 51,
	-80, 50, -80, -37, -80, -17, 22, -17, -36, -47,
	-48, 66, -59, 31, -79, -31, -53, 17, -32, 48,
	-71, -76, -68, 102, -31, 50, -24, -25, -26, 38,
	42, 44, 39, 40, 41, 45, -74, -73, 23, -75,
	23, -20, 92, -73, 58, 59, 60, -40, -39, -37,
	-37, -37, 62, -37, -80, -17, -80, 50, -50, -48,
	68, -32, -35, 34, -2, -62, -60, -45, -53, -57,
	19, 18, -73, -73, -51, 15, -21, -22, -21, -22,
	38, 38, 38, 43, 38, 43, 38, -25, -29, 46,
	103, 47, -73, -73, -80, -73, -80, 62, -37, -80,
	-36, 93, -37, 67, -61, 48, -41, -42, -61, -80,
	50, -57, -37, -54, -55, -37, -76, -52, 16, 18,
	48, 48, 38, 38, 100, 100, 100, -37, -80, -37,
	28, 50, -45, 50, 50, -56, 25, 26, -53, -32,
	-40, -32, -32, -79, -79, -79, 29, -42, -37, -55,
	-57, -30, -71, -30, -30, 11, -58, 20, 32, -80,
	50, -80, -80, -62, 11, 79, -71, -71, -71,
}
var yyDef = [...]int{

	0, -2, 1, 2, 3, 4, 5, 6, 7, 8,
	9, 10, 11, 12, 34, 34, 34, 34, 34, 215,
	206, 0, 0, 0, 223, 223, 223, 0, 38, 40,
	41, 42, 43, 36, 206, 0, 0, 0, 204, 0,
	0, 216, 0, 0, 207, 0, 202, 0, 202, 0,
	31, 32, 33, 14, 39, 0, 44, 35, 0, 0,
	84, 220, 0, 19, 199, 0, 163, 0, -2, 0,
	0, 0, 223, 219, 0, 223, 0, 0, 0, 0,
	0, 30, 0, 45, 47, 53, 0, 0, 51, 52,
	94, 0, 0, 133, 134, 135, 0, 0, 0, 163,
	0, 153, 100, 101, 0, 0, 221, 165, 166, 167,
	168, 198, 154, 156, 37, 0, 0, 0, 92, 0,
	0, 0, 223, 0, 217, 22, 0, 25, 0, 27,
	203, 0, 223, 0, 0, 48, 54, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	120, 121, 122, 123, 124, 125, 126, 97, 0, 0,
	0, 0, 131, 146, 0, 147, 148, 0, 0, 112,
	0, 0, 0, 157, 187, 92, 85, 173, 0, 200,
	201, 164, 20, 205, 0, 0, 223, 213, 208, 209,
	210, 211, 212, 26, 28, 29, 92, 56, 58, 59,
	69, 67, 0, 82, 46, 55, 49, 0, 95, 96,
	99, 114, 0, 116, 118, 102, 103, 0, 128, 129,
	0, 0, 0, 0, 105, 107, 0, 111, 136, 137,
	138, 139, 140, 141, 142, 143, 144, 145, 98, 222,
	130, 0, 197, 131, 149, 0, 0, 0, 0, 161,
	158, 0, 0, 0, 0, 173, 181, 0, 93, 0,
	218, 23, 0, 214, 169, 0, 0, 0, 0, 72,
	0, 0, 75, 0, 0, 0, 86, 70, 0, 0,
	68, 0, 0, 50, 115, 117, 119, 0, 104, 106,
	108, 0, 0, 132, 150, 0, 152, 0, 0, 159,
	0, 0, 191, 0, 194, 191, 0, 189, 181, 18,
	0, 0, 223, 24, 171, 0, 57, 63, 0, 66,
	73, 74, 76, 0, 78, 0, 80, 81, 60, 0,
	0, 0, 71, 61, 62, 83, 127, 0, 109, 151,
	0, 155, 162, 0, 15, 0, 193, 195, 16, 188,
	0, 17, 182, 174, 175, 178, 21, 173, 0, 0,
	0, 0, 77, 79, 0, 0, 0, 110, 113, 160,
	0, 0, 190, 0, 0, 177, 179, 180, 181, 172,
	170, 64, 65, 0, 0, 0, 0, 196, 183, 176,
	184, 0, 90, 0, 0, 0, 13, 0, 0, 87,
	0, 88, 89, 192, 185, 0, 91, 0, 186,
}
var yyTok1 = [...]int{

	1, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 88, 81, 3,
	49, 51, 86, 84, 50, 85, 92, 87, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	70, 69, 71, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 89, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 80, 3, 90,
}
var yyTok2 = [...]int{

	2, 3, 4, 5, 6, 7, 8, 9, 10, 11,
	12, 13, 14, 15, 16, 17, 18, 19, 20, 21,
	22, 23, 24, 25, 26, 27, 28, 29, 30, 31,
	32, 33, 34, 35, 36, 37, 38, 39, 40, 41,
	42, 43, 44, 45, 46, 47, 48, 52, 53, 54,
	55, 56, 57, 58, 59, 60, 61, 62, 63, 64,
	65, 66, 67, 68, 72, 73, 74, 75, 76, 77,
	78, 79, 82, 83, 91, 93, 94, 95, 96, 97,
	98, 99, 100, 101, 102, 103, 104, 105, 106, 107,
	108, 109,
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
	lval  yySymType
	stack [yyInitialStackSize]yySymType
	char  int
}

func (p *yyParserImpl) Lookahead() int {
	return p.char
}

func yyNewParser() yyParser {
	return &yyParserImpl{}
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
	var yyVAL yySymType
	var yyDollar []yySymType
	_ = yyDollar // silence set and not used
	yyS := yyrcvr.stack[:]

	Nerrs := 0   /* number of errors */
	Errflag := 0 /* error recovery flag */
	yystate := 0
	yyrcvr.char = -1
	yytoken := -1 // yyrcvr.char translated into internal numbering
	defer func() {
		// Make sure we report no lookahead when not parsing.
		yystate = -1
		yyrcvr.char = -1
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
	if yyrcvr.char < 0 {
		yyrcvr.char, yytoken = yylex1(yylex, &yyrcvr.lval)
	}
	yyn += yytoken
	if yyn < 0 || yyn >= yyLast {
		goto yydefault
	}
	yyn = yyAct[yyn]
	if yyChk[yyn] == yytoken { /* valid shift */
		yyrcvr.char = -1
		yytoken = -1
		yyVAL = yyrcvr.lval
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
		if yyrcvr.char < 0 {
			yyrcvr.char, yytoken = yylex1(yylex, &yyrcvr.lval)
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
			yyrcvr.char = -1
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
		//line sql.y:169
		{
			setParseTree(yylex, yyDollar[1].statement)
		}
	case 2:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:175
		{
			yyVAL.statement = yyDollar[1].selStmt
		}
	case 13:
		yyDollar = yyS[yypt-12 : yypt+1]
		//line sql.y:191
		{
			yyVAL.selStmt = &Select{Comments: Comments(yyDollar[2].bytes2), Distinct: yyDollar[3].str, SelectExprs: yyDollar[4].selectExprs, From: yyDollar[6].tableExprs, Where: NewWhere(WhereStr, yyDollar[7].boolExpr), GroupBy: GroupBy(yyDollar[8].valExprs), Having: NewWhere(HavingStr, yyDollar[9].boolExpr), OrderBy: yyDollar[10].orderBy, Limit: yyDollar[11].limit, Lock: yyDollar[12].str}
		}
	case 14:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:195
		{
			yyVAL.selStmt = &Union{Type: yyDollar[2].str, Left: yyDollar[1].selStmt, Right: yyDollar[3].selStmt}
		}
	case 15:
		yyDollar = yyS[yypt-8 : yypt+1]
		//line sql.y:201
		{
			yyVAL.statement = &Insert{Comments: Comments(yyDollar[2].bytes2), Ignore: yyDollar[3].str, Table: yyDollar[5].tableName, Columns: yyDollar[6].columns, Rows: yyDollar[7].insRows, OnDup: OnDup(yyDollar[8].updateExprs)}
		}
	case 16:
		yyDollar = yyS[yypt-8 : yypt+1]
		//line sql.y:205
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
		//line sql.y:217
		{
			yyVAL.statement = &Update{Comments: Comments(yyDollar[2].bytes2), Table: yyDollar[3].tableName, Exprs: yyDollar[5].updateExprs, Where: NewWhere(WhereStr, yyDollar[6].boolExpr), OrderBy: yyDollar[7].orderBy, Limit: yyDollar[8].limit}
		}
	case 18:
		yyDollar = yyS[yypt-7 : yypt+1]
		//line sql.y:223
		{
			yyVAL.statement = &Delete{Comments: Comments(yyDollar[2].bytes2), Table: yyDollar[4].tableName, Where: NewWhere(WhereStr, yyDollar[5].boolExpr), OrderBy: yyDollar[6].orderBy, Limit: yyDollar[7].limit}
		}
	case 19:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:229
		{
			yyVAL.statement = &Set{Comments: Comments(yyDollar[2].bytes2), Exprs: yyDollar[3].updateExprs}
		}
	case 20:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:235
		{
			yyVAL.statement = &DDL{Action: CreateStr, NewName: yyDollar[4].sqlID}
		}
	case 21:
		yyDollar = yyS[yypt-8 : yypt+1]
		//line sql.y:239
		{
			// Change this to an alter statement
			yyVAL.statement = &DDL{Action: AlterStr, Table: yyDollar[7].sqlID, NewName: yyDollar[7].sqlID}
		}
	case 22:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:244
		{
			yyVAL.statement = &DDL{Action: CreateStr, NewName: SQLName(yyDollar[3].sqlID)}
		}
	case 23:
		yyDollar = yyS[yypt-6 : yypt+1]
		//line sql.y:250
		{
			yyVAL.statement = &DDL{Action: AlterStr, Table: yyDollar[4].sqlID, NewName: yyDollar[4].sqlID}
		}
	case 24:
		yyDollar = yyS[yypt-7 : yypt+1]
		//line sql.y:254
		{
			// Change this to a rename statement
			yyVAL.statement = &DDL{Action: RenameStr, Table: yyDollar[4].sqlID, NewName: yyDollar[7].sqlID}
		}
	case 25:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:259
		{
			yyVAL.statement = &DDL{Action: AlterStr, Table: SQLName(yyDollar[3].sqlID), NewName: SQLName(yyDollar[3].sqlID)}
		}
	case 26:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:265
		{
			yyVAL.statement = &DDL{Action: RenameStr, Table: yyDollar[3].sqlID, NewName: yyDollar[5].sqlID}
		}
	case 27:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:271
		{
			yyVAL.statement = &DDL{Action: DropStr, Table: yyDollar[4].sqlID}
		}
	case 28:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:275
		{
			// Change this to an alter statement
			yyVAL.statement = &DDL{Action: AlterStr, Table: yyDollar[5].sqlID, NewName: yyDollar[5].sqlID}
		}
	case 29:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:280
		{
			yyVAL.statement = &DDL{Action: DropStr, Table: SQLName(yyDollar[4].sqlID)}
		}
	case 30:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:286
		{
			yyVAL.statement = &DDL{Action: AlterStr, Table: yyDollar[3].sqlID, NewName: yyDollar[3].sqlID}
		}
	case 31:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:292
		{
			yyVAL.statement = &Other{}
		}
	case 32:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:296
		{
			yyVAL.statement = &Other{}
		}
	case 33:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:300
		{
			yyVAL.statement = &Other{}
		}
	case 34:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:305
		{
			setAllowComments(yylex, true)
		}
	case 35:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:309
		{
			yyVAL.bytes2 = yyDollar[2].bytes2
			setAllowComments(yylex, false)
		}
	case 36:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:315
		{
			yyVAL.bytes2 = nil
		}
	case 37:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:319
		{
			yyVAL.bytes2 = append(yyDollar[1].bytes2, yyDollar[2].bytes)
		}
	case 38:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:325
		{
			yyVAL.str = UnionStr
		}
	case 39:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:329
		{
			yyVAL.str = UnionAllStr
		}
	case 40:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:333
		{
			yyVAL.str = SetMinusStr
		}
	case 41:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:337
		{
			yyVAL.str = ExceptStr
		}
	case 42:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:341
		{
			yyVAL.str = IntersectStr
		}
	case 43:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:346
		{
			yyVAL.str = ""
		}
	case 44:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:350
		{
			yyVAL.str = DistinctStr
		}
	case 45:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:356
		{
			yyVAL.selectExprs = SelectExprs{yyDollar[1].selectExpr}
		}
	case 46:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:360
		{
			yyVAL.selectExprs = append(yyVAL.selectExprs, yyDollar[3].selectExpr)
		}
	case 47:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:366
		{
			yyVAL.selectExpr = &StarExpr{}
		}
	case 48:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:370
		{
			yyVAL.selectExpr = &NonStarExpr{Expr: yyDollar[1].expr, As: yyDollar[2].sqlID}
		}
	case 49:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:374
		{
			yyVAL.selectExpr = &StarExpr{TableName: yyDollar[1].sqlID}
		}
	case 50:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:378
		{
			yyVAL.selectExpr = &Nextval{TableName: yyDollar[4].sqlID}
		}
	case 51:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:384
		{
			yyVAL.expr = yyDollar[1].boolExpr
		}
	case 52:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:388
		{
			yyVAL.expr = yyDollar[1].valExpr
		}
	case 53:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:393
		{
			yyVAL.sqlID = ""
		}
	case 54:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:397
		{
			yyVAL.sqlID = yyDollar[1].sqlID
		}
	case 55:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:401
		{
			yyVAL.sqlID = yyDollar[2].sqlID
		}
	case 56:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:407
		{
			yyVAL.tableExprs = TableExprs{yyDollar[1].tableExpr}
		}
	case 57:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:411
		{
			yyVAL.tableExprs = append(yyVAL.tableExprs, yyDollar[3].tableExpr)
		}
	case 60:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:421
		{
			yyVAL.tableExpr = &AliasedTableExpr{Expr: yyDollar[1].smTableExpr, As: yyDollar[2].sqlID, Hints: yyDollar[3].indexHints}
		}
	case 61:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:425
		{
			yyVAL.tableExpr = &AliasedTableExpr{Expr: yyDollar[1].subquery, As: yyDollar[3].sqlID}
		}
	case 62:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:429
		{
			yyVAL.tableExpr = &ParenTableExpr{Exprs: yyDollar[2].tableExprs}
		}
	case 63:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:442
		{
			yyVAL.tableExpr = &JoinTableExpr{LeftExpr: yyDollar[1].tableExpr, Join: yyDollar[2].str, RightExpr: yyDollar[3].tableExpr}
		}
	case 64:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:446
		{
			yyVAL.tableExpr = &JoinTableExpr{LeftExpr: yyDollar[1].tableExpr, Join: yyDollar[2].str, RightExpr: yyDollar[3].tableExpr, On: yyDollar[5].boolExpr}
		}
	case 65:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:450
		{
			yyVAL.tableExpr = &JoinTableExpr{LeftExpr: yyDollar[1].tableExpr, Join: yyDollar[2].str, RightExpr: yyDollar[3].tableExpr, On: yyDollar[5].boolExpr}
		}
	case 66:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:454
		{
			yyVAL.tableExpr = &JoinTableExpr{LeftExpr: yyDollar[1].tableExpr, Join: yyDollar[2].str, RightExpr: yyDollar[3].tableExpr}
		}
	case 67:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:459
		{
			yyVAL.empty = struct{}{}
		}
	case 68:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:461
		{
			yyVAL.empty = struct{}{}
		}
	case 69:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:464
		{
			yyVAL.sqlID = ""
		}
	case 70:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:468
		{
			yyVAL.sqlID = yyDollar[1].sqlID
		}
	case 71:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:472
		{
			yyVAL.sqlID = yyDollar[2].sqlID
		}
	case 72:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:478
		{
			yyVAL.str = JoinStr
		}
	case 73:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:482
		{
			yyVAL.str = JoinStr
		}
	case 74:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:486
		{
			yyVAL.str = JoinStr
		}
	case 75:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:490
		{
			yyVAL.str = StraightJoinStr
		}
	case 76:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:496
		{
			yyVAL.str = LeftJoinStr
		}
	case 77:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:500
		{
			yyVAL.str = LeftJoinStr
		}
	case 78:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:504
		{
			yyVAL.str = RightJoinStr
		}
	case 79:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:508
		{
			yyVAL.str = RightJoinStr
		}
	case 80:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:514
		{
			yyVAL.str = NaturalJoinStr
		}
	case 81:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:518
		{
			if yyDollar[2].str == LeftJoinStr {
				yyVAL.str = NaturalLeftJoinStr
			} else {
				yyVAL.str = NaturalRightJoinStr
			}
		}
	case 82:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:528
		{
			yyVAL.smTableExpr = &TableName{Name: yyDollar[1].sqlID}
		}
	case 83:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:532
		{
			yyVAL.smTableExpr = &TableName{Qualifier: yyDollar[1].sqlID, Name: yyDollar[3].sqlID}
		}
	case 84:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:538
		{
			yyVAL.tableName = &TableName{Name: yyDollar[1].sqlID}
		}
	case 85:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:542
		{
			yyVAL.tableName = &TableName{Qualifier: yyDollar[1].sqlID, Name: yyDollar[3].sqlID}
		}
	case 86:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:547
		{
			yyVAL.indexHints = nil
		}
	case 87:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:551
		{
			yyVAL.indexHints = &IndexHints{Type: UseStr, Indexes: yyDollar[4].sqlIDs}
		}
	case 88:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:555
		{
			yyVAL.indexHints = &IndexHints{Type: IgnoreStr, Indexes: yyDollar[4].sqlIDs}
		}
	case 89:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:559
		{
			yyVAL.indexHints = &IndexHints{Type: ForceStr, Indexes: yyDollar[4].sqlIDs}
		}
	case 90:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:565
		{
			yyVAL.sqlIDs = []SQLName{yyDollar[1].sqlID}
		}
	case 91:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:569
		{
			yyVAL.sqlIDs = append(yyDollar[1].sqlIDs, yyDollar[3].sqlID)
		}
	case 92:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:574
		{
			yyVAL.boolExpr = nil
		}
	case 93:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:578
		{
			yyVAL.boolExpr = yyDollar[2].boolExpr
		}
	case 95:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:585
		{
			yyVAL.boolExpr = &AndExpr{Left: yyDollar[1].boolExpr, Right: yyDollar[3].boolExpr}
		}
	case 96:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:589
		{
			yyVAL.boolExpr = &OrExpr{Left: yyDollar[1].boolExpr, Right: yyDollar[3].boolExpr}
		}
	case 97:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:593
		{
			yyVAL.boolExpr = &NotExpr{Expr: yyDollar[2].boolExpr}
		}
	case 98:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:597
		{
			yyVAL.boolExpr = &ParenBoolExpr{Expr: yyDollar[2].boolExpr}
		}
	case 99:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:601
		{
			yyVAL.boolExpr = &IsExpr{Operator: yyDollar[3].str, Expr: yyDollar[1].boolExpr}
		}
	case 100:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:607
		{
			yyVAL.boolExpr = BoolVal(true)
		}
	case 101:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:611
		{
			yyVAL.boolExpr = BoolVal(false)
		}
	case 102:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:615
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: yyDollar[2].str, Right: yyDollar[3].valExpr}
		}
	case 103:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:619
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: InStr, Right: yyDollar[3].colTuple}
		}
	case 104:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:623
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: NotInStr, Right: yyDollar[4].colTuple}
		}
	case 105:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:627
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: LikeStr, Right: yyDollar[3].valExpr}
		}
	case 106:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:631
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: NotLikeStr, Right: yyDollar[4].valExpr}
		}
	case 107:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:635
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: RegexpStr, Right: yyDollar[3].valExpr}
		}
	case 108:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:639
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: NotRegexpStr, Right: yyDollar[4].valExpr}
		}
	case 109:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:643
		{
			yyVAL.boolExpr = &RangeCond{Left: yyDollar[1].valExpr, Operator: BetweenStr, From: yyDollar[3].valExpr, To: yyDollar[5].valExpr}
		}
	case 110:
		yyDollar = yyS[yypt-6 : yypt+1]
		//line sql.y:647
		{
			yyVAL.boolExpr = &RangeCond{Left: yyDollar[1].valExpr, Operator: NotBetweenStr, From: yyDollar[4].valExpr, To: yyDollar[6].valExpr}
		}
	case 111:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:651
		{
			yyVAL.boolExpr = &IsExpr{Operator: yyDollar[3].str, Expr: yyDollar[1].valExpr}
		}
	case 112:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:655
		{
			yyVAL.boolExpr = &ExistsExpr{Subquery: yyDollar[2].subquery}
		}
	case 113:
		yyDollar = yyS[yypt-6 : yypt+1]
		//line sql.y:659
		{
			yyVAL.boolExpr = &KeyrangeExpr{Start: yyDollar[3].valExpr, End: yyDollar[5].valExpr}
		}
	case 114:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:665
		{
			yyVAL.str = IsNullStr
		}
	case 115:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:669
		{
			yyVAL.str = IsNotNullStr
		}
	case 116:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:673
		{
			yyVAL.str = IsTrueStr
		}
	case 117:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:677
		{
			yyVAL.str = IsNotTrueStr
		}
	case 118:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:681
		{
			yyVAL.str = IsFalseStr
		}
	case 119:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:685
		{
			yyVAL.str = IsNotFalseStr
		}
	case 120:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:691
		{
			yyVAL.str = EqualStr
		}
	case 121:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:695
		{
			yyVAL.str = LessThanStr
		}
	case 122:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:699
		{
			yyVAL.str = GreaterThanStr
		}
	case 123:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:703
		{
			yyVAL.str = LessEqualStr
		}
	case 124:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:707
		{
			yyVAL.str = GreaterEqualStr
		}
	case 125:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:711
		{
			yyVAL.str = NotEqualStr
		}
	case 126:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:715
		{
			yyVAL.str = NullSafeEqualStr
		}
	case 127:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:721
		{
			yyVAL.colTuple = ValTuple(yyDollar[2].valExprs)
		}
	case 128:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:725
		{
			yyVAL.colTuple = yyDollar[1].subquery
		}
	case 129:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:729
		{
			yyVAL.colTuple = ListArg(yyDollar[1].bytes)
		}
	case 130:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:735
		{
			yyVAL.subquery = &Subquery{yyDollar[2].selStmt}
		}
	case 131:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:741
		{
			yyVAL.valExprs = ValExprs{yyDollar[1].valExpr}
		}
	case 132:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:745
		{
			yyVAL.valExprs = append(yyDollar[1].valExprs, yyDollar[3].valExpr)
		}
	case 133:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:751
		{
			yyVAL.valExpr = yyDollar[1].valExpr
		}
	case 134:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:755
		{
			yyVAL.valExpr = yyDollar[1].colName
		}
	case 135:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:759
		{
			yyVAL.valExpr = yyDollar[1].rowTuple
		}
	case 136:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:763
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: BitAndStr, Right: yyDollar[3].valExpr}
		}
	case 137:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:767
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: BitOrStr, Right: yyDollar[3].valExpr}
		}
	case 138:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:771
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: BitXorStr, Right: yyDollar[3].valExpr}
		}
	case 139:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:775
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: PlusStr, Right: yyDollar[3].valExpr}
		}
	case 140:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:779
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: MinusStr, Right: yyDollar[3].valExpr}
		}
	case 141:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:783
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: MultStr, Right: yyDollar[3].valExpr}
		}
	case 142:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:787
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: DivStr, Right: yyDollar[3].valExpr}
		}
	case 143:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:791
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: ModStr, Right: yyDollar[3].valExpr}
		}
	case 144:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:795
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: ShiftLeftStr, Right: yyDollar[3].valExpr}
		}
	case 145:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:799
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: ShiftRightStr, Right: yyDollar[3].valExpr}
		}
	case 146:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:803
		{
			if num, ok := yyDollar[2].valExpr.(NumVal); ok {
				yyVAL.valExpr = num
			} else {
				yyVAL.valExpr = &UnaryExpr{Operator: UPlusStr, Expr: yyDollar[2].valExpr}
			}
		}
	case 147:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:811
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
	case 148:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:824
		{
			yyVAL.valExpr = &UnaryExpr{Operator: TildaStr, Expr: yyDollar[2].valExpr}
		}
	case 149:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:828
		{
			yyVAL.valExpr = &FuncExpr{Name: string(yyDollar[1].sqlID)}
		}
	case 150:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:832
		{
			yyVAL.valExpr = &FuncExpr{Name: string(yyDollar[1].sqlID), Exprs: yyDollar[3].selectExprs}
		}
	case 151:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:836
		{
			yyVAL.valExpr = &FuncExpr{Name: string(yyDollar[1].sqlID), Distinct: true, Exprs: yyDollar[4].selectExprs}
		}
	case 152:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:840
		{
			yyVAL.valExpr = &FuncExpr{Name: yyDollar[1].str, Exprs: yyDollar[3].selectExprs}
		}
	case 153:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:844
		{
			yyVAL.valExpr = yyDollar[1].caseExpr
		}
	case 154:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:850
		{
			yyVAL.str = "if"
		}
	case 155:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:856
		{
			yyVAL.caseExpr = &CaseExpr{Expr: yyDollar[2].valExpr, Whens: yyDollar[3].whens, Else: yyDollar[4].valExpr}
		}
	case 156:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:861
		{
			yyVAL.valExpr = nil
		}
	case 157:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:865
		{
			yyVAL.valExpr = yyDollar[1].valExpr
		}
	case 158:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:871
		{
			yyVAL.whens = []*When{yyDollar[1].when}
		}
	case 159:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:875
		{
			yyVAL.whens = append(yyDollar[1].whens, yyDollar[2].when)
		}
	case 160:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:881
		{
			yyVAL.when = &When{Cond: yyDollar[2].boolExpr, Val: yyDollar[4].valExpr}
		}
	case 161:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:886
		{
			yyVAL.valExpr = nil
		}
	case 162:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:890
		{
			yyVAL.valExpr = yyDollar[2].valExpr
		}
	case 163:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:896
		{
			yyVAL.colName = &ColName{Name: yyDollar[1].sqlID}
		}
	case 164:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:900
		{
			yyVAL.colName = &ColName{Qualifier: yyDollar[1].sqlID, Name: yyDollar[3].sqlID}
		}
	case 165:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:906
		{
			yyVAL.valExpr = StrVal(yyDollar[1].bytes)
		}
	case 166:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:910
		{
			yyVAL.valExpr = NumVal(yyDollar[1].bytes)
		}
	case 167:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:914
		{
			yyVAL.valExpr = ValArg(yyDollar[1].bytes)
		}
	case 168:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:918
		{
			yyVAL.valExpr = &NullVal{}
		}
	case 169:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:923
		{
			yyVAL.valExprs = nil
		}
	case 170:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:927
		{
			yyVAL.valExprs = yyDollar[3].valExprs
		}
	case 171:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:932
		{
			yyVAL.boolExpr = nil
		}
	case 172:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:936
		{
			yyVAL.boolExpr = yyDollar[2].boolExpr
		}
	case 173:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:941
		{
			yyVAL.orderBy = nil
		}
	case 174:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:945
		{
			yyVAL.orderBy = yyDollar[3].orderBy
		}
	case 175:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:951
		{
			yyVAL.orderBy = OrderBy{yyDollar[1].order}
		}
	case 176:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:955
		{
			yyVAL.orderBy = append(yyDollar[1].orderBy, yyDollar[3].order)
		}
	case 177:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:961
		{
			yyVAL.order = &Order{Expr: yyDollar[1].valExpr, Direction: yyDollar[2].str}
		}
	case 178:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:966
		{
			yyVAL.str = AscScr
		}
	case 179:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:970
		{
			yyVAL.str = AscScr
		}
	case 180:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:974
		{
			yyVAL.str = DescScr
		}
	case 181:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:979
		{
			yyVAL.limit = nil
		}
	case 182:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:983
		{
			yyVAL.limit = &Limit{Rowcount: yyDollar[2].valExpr}
		}
	case 183:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:987
		{
			yyVAL.limit = &Limit{Offset: yyDollar[2].valExpr, Rowcount: yyDollar[4].valExpr}
		}
	case 184:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:992
		{
			yyVAL.str = ""
		}
	case 185:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:996
		{
			yyVAL.str = ForUpdateStr
		}
	case 186:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:1000
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
	case 187:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1013
		{
			yyVAL.columns = nil
		}
	case 188:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1017
		{
			yyVAL.columns = yyDollar[2].columns
		}
	case 189:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1023
		{
			yyVAL.columns = Columns{&NonStarExpr{Expr: yyDollar[1].colName}}
		}
	case 190:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1027
		{
			yyVAL.columns = append(yyVAL.columns, &NonStarExpr{Expr: yyDollar[3].colName})
		}
	case 191:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1032
		{
			yyVAL.updateExprs = nil
		}
	case 192:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:1036
		{
			yyVAL.updateExprs = yyDollar[5].updateExprs
		}
	case 193:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:1042
		{
			yyVAL.insRows = yyDollar[2].values
		}
	case 194:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1046
		{
			yyVAL.insRows = yyDollar[1].selStmt
		}
	case 195:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1052
		{
			yyVAL.values = Values{yyDollar[1].rowTuple}
		}
	case 196:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1056
		{
			yyVAL.values = append(yyDollar[1].values, yyDollar[3].rowTuple)
		}
	case 197:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1062
		{
			yyVAL.rowTuple = ValTuple(yyDollar[2].valExprs)
		}
	case 198:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1066
		{
			yyVAL.rowTuple = yyDollar[1].subquery
		}
	case 199:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1072
		{
			yyVAL.updateExprs = UpdateExprs{yyDollar[1].updateExpr}
		}
	case 200:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1076
		{
			yyVAL.updateExprs = append(yyDollar[1].updateExprs, yyDollar[3].updateExpr)
		}
	case 201:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1082
		{
			yyVAL.updateExpr = &UpdateExpr{Name: yyDollar[1].colName, Expr: yyDollar[3].valExpr}
		}
	case 202:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1087
		{
			yyVAL.empty = struct{}{}
		}
	case 203:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:1089
		{
			yyVAL.empty = struct{}{}
		}
	case 204:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1092
		{
			yyVAL.empty = struct{}{}
		}
	case 205:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1094
		{
			yyVAL.empty = struct{}{}
		}
	case 206:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1097
		{
			yyVAL.str = ""
		}
	case 207:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1099
		{
			yyVAL.str = IgnoreStr
		}
	case 208:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1103
		{
			yyVAL.empty = struct{}{}
		}
	case 209:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1105
		{
			yyVAL.empty = struct{}{}
		}
	case 210:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1107
		{
			yyVAL.empty = struct{}{}
		}
	case 211:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1109
		{
			yyVAL.empty = struct{}{}
		}
	case 212:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1111
		{
			yyVAL.empty = struct{}{}
		}
	case 213:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1114
		{
			yyVAL.empty = struct{}{}
		}
	case 214:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1116
		{
			yyVAL.empty = struct{}{}
		}
	case 215:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1119
		{
			yyVAL.empty = struct{}{}
		}
	case 216:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1121
		{
			yyVAL.empty = struct{}{}
		}
	case 217:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1124
		{
			yyVAL.empty = struct{}{}
		}
	case 218:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:1126
		{
			yyVAL.empty = struct{}{}
		}
	case 219:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1130
		{
			yyVAL.sqlID = SQLName(strings.ToLower(string(yyDollar[1].bytes)))
		}
	case 220:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1136
		{
			yyVAL.sqlID = SQLName(yyDollar[1].bytes)
		}
	case 221:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1142
		{
			if incNesting(yylex) {
				yylex.Error("max nesting level reached")
				return 1
			}
		}
	case 222:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1151
		{
			decNesting(yylex)
		}
	case 223:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1156
		{
			forceEOF(yylex)
		}
	}
	goto yystack /* stack new state and value */
}
