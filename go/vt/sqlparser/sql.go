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
const VALUES = 57375
const LAST_INSERT_ID = 57376
const NEXT = 57377
const VALUE = 57378
const JOIN = 57379
const STRAIGHT_JOIN = 57380
const LEFT = 57381
const RIGHT = 57382
const INNER = 57383
const OUTER = 57384
const CROSS = 57385
const NATURAL = 57386
const USE = 57387
const FORCE = 57388
const ON = 57389
const ID = 57390
const STRING = 57391
const NUMBER = 57392
const VALUE_ARG = 57393
const LIST_ARG = 57394
const COMMENT = 57395
const NULL = 57396
const TRUE = 57397
const FALSE = 57398
const OR = 57399
const AND = 57400
const NOT = 57401
const BETWEEN = 57402
const CASE = 57403
const WHEN = 57404
const THEN = 57405
const ELSE = 57406
const LE = 57407
const GE = 57408
const NE = 57409
const NULL_SAFE_EQUAL = 57410
const IS = 57411
const LIKE = 57412
const REGEXP = 57413
const IN = 57414
const SHIFT_LEFT = 57415
const SHIFT_RIGHT = 57416
const UNARY = 57417
const INTERVAL = 57418
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
	"INTERVAL",
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
	-1, 69,
	92, 222,
	-2, 221,
}

const yyNprod = 226
const yyPrivate = 57344

var yyTokenNames []string
var yyStates []string

const yyLast = 786

var yyAct = [...]int{

	122, 286, 196, 402, 266, 337, 216, 133, 295, 199,
	327, 226, 116, 228, 117, 277, 64, 227, 244, 141,
	238, 151, 105, 198, 3, 38, 79, 40, 71, 106,
	44, 41, 14, 355, 357, 43, 385, 44, 67, 50,
	225, 73, 100, 384, 76, 46, 47, 48, 110, 383,
	66, 53, 72, 75, 49, 45, 365, 85, 220, 167,
	93, 230, 89, 65, 180, 51, 52, 183, 184, 185,
	180, 128, 415, 170, 69, 129, 130, 131, 169, 168,
	132, 69, 104, 92, 367, 251, 111, 134, 168, 67,
	278, 356, 67, 170, 146, 144, 95, 60, 249, 250,
	248, 66, 170, 86, 66, 139, 118, 119, 278, 165,
	325, 74, 120, 97, 121, 235, 99, 200, 310, 311,
	312, 201, 202, 203, 204, 90, 69, 123, 181, 182,
	183, 184, 185, 180, 148, 207, 145, 210, 169, 168,
	267, 239, 241, 242, 161, 306, 240, 128, 219, 138,
	169, 168, 222, 170, 247, 143, 62, 14, 128, 215,
	96, 62, 195, 197, 80, 170, 111, 234, 146, 91,
	231, 128, 166, 62, 243, 411, 267, 252, 253, 254,
	246, 256, 257, 258, 259, 260, 261, 262, 263, 264,
	265, 218, 223, 233, 163, 267, 128, 211, 255, 62,
	74, 269, 267, 268, 270, 271, 293, 267, 111, 111,
	272, 333, 267, 162, 67, 67, 269, 236, 237, 372,
	114, 289, 285, 328, 369, 91, 66, 284, 273, 275,
	282, 297, 300, 301, 302, 298, 281, 299, 303, 142,
	231, 380, 379, 328, 309, 213, 292, 142, 219, 163,
	114, 114, 315, 316, 317, 313, 246, 221, 103, 205,
	206, 84, 128, 350, 208, 348, 78, 314, 351, 382,
	349, 381, 319, 157, 293, 214, 347, 111, 320, 114,
	322, 352, 91, 301, 302, 346, 155, 334, 332, 88,
	335, 338, 324, 331, 388, 330, 42, 321, 14, 232,
	114, 231, 231, 231, 231, 114, 114, 158, 343, 245,
	345, 342, 353, 344, 360, 81, 57, 87, 361, 368,
	408, 363, 280, 149, 371, 102, 364, 326, 366, 56,
	339, 59, 409, 308, 67, 28, 29, 30, 31, 54,
	137, 287, 114, 114, 378, 217, 370, 136, 377, 288,
	341, 154, 156, 153, 179, 178, 186, 187, 181, 182,
	183, 184, 185, 180, 142, 386, 63, 414, 400, 232,
	387, 14, 33, 1, 390, 338, 307, 304, 391, 164,
	267, 219, 150, 392, 389, 245, 39, 224, 394, 68,
	28, 29, 30, 31, 401, 152, 70, 135, 403, 403,
	403, 67, 404, 405, 283, 212, 407, 410, 373, 412,
	413, 114, 416, 66, 336, 114, 417, 406, 418, 274,
	376, 127, 340, 323, 209, 61, 393, 276, 395, 396,
	232, 232, 232, 232, 124, 77, 329, 115, 279, 82,
	171, 14, 15, 16, 17, 128, 112, 267, 69, 129,
	130, 131, 354, 61, 132, 125, 126, 296, 294, 113,
	94, 134, 229, 18, 108, 98, 83, 55, 101, 27,
	58, 13, 12, 109, 11, 10, 9, 61, 32, 140,
	118, 119, 107, 147, 8, 7, 120, 6, 121, 5,
	159, 4, 2, 160, 34, 35, 36, 37, 0, 0,
	0, 123, 128, 0, 0, 69, 129, 130, 131, 0,
	0, 132, 0, 0, 114, 0, 114, 114, 134, 0,
	397, 398, 399, 0, 0, 61, 19, 20, 22, 21,
	23, 0, 127, 0, 0, 14, 0, 118, 119, 24,
	25, 26, 0, 120, 0, 121, 0, 0, 0, 0,
	127, 0, 61, 109, 0, 0, 128, 147, 123, 69,
	129, 130, 131, 0, 0, 132, 125, 126, 0, 0,
	113, 0, 134, 0, 128, 0, 0, 69, 129, 130,
	131, 0, 0, 132, 125, 126, 0, 0, 113, 0,
	134, 118, 119, 107, 0, 109, 109, 120, 0, 121,
	374, 375, 0, 0, 0, 0, 0, 127, 0, 118,
	119, 290, 123, 0, 291, 120, 0, 121, 0, 0,
	305, 0, 61, 0, 0, 0, 0, 0, 0, 0,
	123, 128, 0, 0, 69, 129, 130, 131, 0, 0,
	132, 125, 126, 0, 0, 113, 0, 134, 0, 0,
	0, 0, 0, 0, 179, 178, 186, 187, 181, 182,
	183, 184, 185, 180, 109, 0, 118, 119, 0, 0,
	0, 0, 120, 0, 121, 186, 187, 181, 182, 183,
	184, 185, 180, 61, 61, 61, 61, 123, 0, 0,
	0, 0, 173, 176, 0, 0, 358, 359, 188, 189,
	190, 191, 192, 193, 194, 177, 174, 175, 172, 179,
	178, 186, 187, 181, 182, 183, 184, 185, 180, 362,
	178, 186, 187, 181, 182, 183, 184, 185, 180, 318,
	74, 0, 0, 0, 0, 0, 0, 179, 178, 186,
	187, 181, 182, 183, 184, 185, 180, 179, 178, 186,
	187, 181, 182, 183, 184, 185, 180, 0, 179, 178,
	186, 187, 181, 182, 183, 184, 185, 180, 179, 178,
	186, 187, 181, 182, 183, 184, 185, 180, 297, 300,
	301, 302, 298, 0, 299, 303,
}
var yyPact = [...]int{

	432, -1000, -1000, 385, -1000, -1000, -1000, -1000, -1000, -1000,
	-1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, -74,
	-66, -44, -54, -45, -1000, -1000, -1000, 362, 318, -1000,
	-1000, -1000, 294, -1000, -73, 105, 353, 75, -76, -48,
	60, -1000, -46, 60, -1000, 105, -78, 113, -78, 105,
	-1000, -1000, -1000, -1000, -1000, 223, 60, -1000, 47, 290,
	258, -30, -1000, 105, 120, -1000, 15, -1000, -32, -1000,
	105, 34, 109, -1000, -1000, 105, -1000, -60, 105, 301,
	211, 60, -1000, 508, -1000, 327, -1000, 105, 75, 105,
	350, 75, 454, 75, -1000, 299, -85, -1000, 256, -1000,
	105, -1000, -1000, 105, -1000, 200, -1000, -1000, 149, -33,
	78, 630, -1000, 583, 526, -1000, -1000, -1000, 454, 454,
	454, 454, 123, 123, -1000, -1000, -1000, 123, -1000, -1000,
	-1000, -1000, -1000, -1000, 454, 105, -1000, -1000, 214, 233,
	-1000, 328, 583, -1000, 689, 23, -1000, -34, -1000, -1000,
	210, 60, -1000, -62, -1000, -1000, -1000, -1000, -1000, -1000,
	-1000, -1000, 110, 508, -1000, -1000, 60, 30, 583, 583,
	84, 454, 99, 22, 454, 454, 454, 84, 454, 454,
	454, 454, 454, 454, 454, 454, 454, 454, -1000, -1000,
	-1000, -1000, -1000, -1000, -1000, -2, 630, 90, 330, 152,
	630, -1000, -1000, -1000, 679, 397, 508, -1000, 362, 25,
	689, -1000, 289, 75, 75, 328, 322, 331, 78, 689,
	60, 105, -1000, -1000, 105, -1000, 225, 741, -1000, -1000,
	122, 310, 148, -1000, -1000, -1000, -2, 27, -1000, -1000,
	61, -1000, -1000, 689, -1000, 23, -1000, -1000, 99, 454,
	454, 454, 689, 689, 668, -1000, 594, 640, -1000, -18,
	-18, -24, -24, -24, 45, 45, -1000, -1000, -1000, 454,
	-1000, -1000, -1000, 145, 508, 145, 43, -1000, 583, 196,
	123, 385, 176, 162, -1000, 322, -1000, 454, 454, -1000,
	-1000, -1000, 335, 110, 110, 110, 110, -1000, 248, 239,
	-1000, 228, 226, 244, -12, -1000, 105, 105, -1000, 157,
	-1000, -1000, -1000, 152, -1000, 689, 689, 658, 454, 689,
	-1000, 145, -1000, -37, -1000, 454, 18, -1000, 291, 175,
	-1000, -1000, -1000, 75, -1000, 275, 170, -1000, 575, -1000,
	332, 326, 741, 195, 194, -1000, -1000, -1000, -1000, 234,
	-1000, 232, -1000, -1000, -1000, -51, -57, -64, -1000, -1000,
	-1000, -1000, 454, 689, -1000, -1000, 689, 454, 265, 123,
	-1000, 454, 454, -1000, -1000, -1000, 328, 583, 454, 583,
	583, -1000, -1000, 123, 123, 123, 689, 689, 357, -1000,
	689, -1000, 322, 78, 167, 78, 78, 60, 60, 60,
	75, 300, 126, -1000, 126, 126, 120, -1000, 356, -6,
	-1000, 60, -1000, -1000, -1000, 60, -1000, 60, -1000,
}
var yyPgo = [...]int{

	0, 492, 23, 491, 489, 487, 485, 484, 476, 475,
	474, 472, 471, 478, 470, 469, 467, 466, 22, 29,
	464, 11, 17, 13, 462, 458, 8, 457, 61, 452,
	3, 19, 48, 446, 440, 438, 437, 2, 20, 18,
	9, 436, 14, 7, 12, 434, 427, 15, 424, 423,
	422, 420, 6, 414, 5, 408, 1, 406, 405, 404,
	10, 16, 63, 397, 296, 266, 396, 395, 387, 386,
	382, 0, 379, 389, 377, 376, 39, 373, 372, 136,
	4,
}
var yyR1 = [...]int{

	0, 77, 1, 1, 1, 1, 1, 1, 1, 1,
	1, 1, 1, 2, 2, 2, 3, 3, 4, 5,
	6, 7, 7, 7, 8, 8, 8, 9, 10, 10,
	10, 11, 12, 12, 12, 78, 13, 14, 14, 15,
	15, 15, 15, 15, 16, 16, 17, 17, 18, 18,
	19, 19, 19, 20, 20, 72, 72, 72, 21, 21,
	22, 22, 23, 23, 23, 24, 24, 24, 24, 75,
	75, 74, 74, 74, 25, 25, 25, 25, 26, 26,
	26, 26, 27, 27, 28, 28, 29, 29, 29, 29,
	30, 30, 31, 31, 32, 32, 32, 32, 32, 32,
	33, 33, 33, 33, 33, 33, 33, 33, 33, 33,
	33, 33, 33, 38, 38, 38, 38, 38, 38, 34,
	34, 34, 34, 34, 34, 34, 39, 39, 39, 43,
	40, 40, 37, 37, 37, 37, 37, 37, 37, 37,
	37, 37, 37, 37, 37, 37, 37, 37, 37, 37,
	37, 37, 37, 37, 45, 48, 48, 46, 46, 47,
	49, 49, 44, 44, 44, 36, 36, 36, 36, 50,
	50, 51, 51, 52, 52, 53, 53, 54, 55, 55,
	55, 56, 56, 56, 57, 57, 57, 58, 58, 59,
	59, 60, 60, 35, 35, 41, 41, 42, 42, 61,
	61, 62, 63, 63, 65, 65, 66, 66, 64, 64,
	67, 67, 67, 67, 67, 68, 68, 69, 69, 70,
	70, 71, 73, 79, 80, 76,
}
var yyR2 = [...]int{

	0, 1, 1, 1, 1, 1, 1, 1, 1, 1,
	1, 1, 1, 13, 6, 3, 8, 8, 8, 7,
	3, 5, 8, 4, 6, 7, 4, 5, 4, 5,
	5, 3, 2, 2, 2, 0, 2, 0, 2, 1,
	2, 1, 1, 1, 0, 1, 0, 1, 1, 3,
	1, 2, 3, 1, 1, 0, 1, 2, 1, 3,
	1, 1, 3, 3, 3, 3, 5, 5, 3, 0,
	1, 0, 1, 2, 1, 2, 2, 1, 2, 3,
	2, 3, 2, 2, 1, 3, 0, 5, 5, 5,
	1, 3, 0, 2, 1, 3, 3, 2, 3, 3,
	1, 1, 3, 3, 4, 3, 4, 3, 4, 5,
	6, 3, 2, 1, 2, 1, 2, 1, 2, 1,
	1, 1, 1, 1, 1, 1, 3, 1, 1, 3,
	1, 3, 1, 1, 1, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 2, 2, 2, 3, 3,
	4, 5, 4, 1, 5, 0, 1, 1, 2, 4,
	0, 2, 1, 3, 5, 1, 1, 1, 1, 0,
	3, 0, 2, 0, 3, 1, 3, 2, 0, 1,
	1, 0, 2, 4, 0, 2, 4, 0, 3, 1,
	3, 0, 5, 2, 1, 1, 3, 3, 1, 1,
	3, 3, 1, 1, 0, 2, 0, 3, 0, 1,
	1, 1, 1, 1, 1, 0, 1, 0, 1, 0,
	2, 1, 1, 1, 1, 0,
}
var yyChk = [...]int{

	-1000, -77, -1, -2, -3, -4, -5, -6, -7, -8,
	-9, -10, -11, -12, 9, 10, 11, 12, 31, 94,
	95, 97, 96, 98, 107, 108, 109, -15, 5, 6,
	7, 8, -13, -78, -13, -13, -13, -13, 99, -69,
	101, 105, -64, 101, 103, 99, 99, 100, 101, 99,
	-76, -76, -76, -2, 21, -16, 35, 22, -14, -64,
	-28, -73, 51, 13, -61, -62, -44, -71, -73, 51,
	-66, 104, 100, -71, 51, 99, -71, -73, -65, 104,
	51, -65, -73, -17, 38, -71, 56, 27, 31, 92,
	-28, 49, 68, 92, -73, 62, 51, -76, -73, -76,
	102, -73, 24, 47, -71, -18, -19, 85, -20, -73,
	-32, -37, -33, 62, -79, -36, -44, -42, 83, 84,
	89, 91, -71, 104, -45, 58, 59, 24, 48, 52,
	53, 54, 57, -43, 64, -63, 20, 13, -28, -61,
	-73, -31, 14, -62, -37, -79, -71, -73, -76, 24,
	-70, 106, -67, 97, 95, 30, 96, 17, 51, -73,
	-73, -76, 13, 49, -72, -71, 23, 92, 61, 60,
	75, -34, 78, 62, 76, 77, 63, 75, 80, 79,
	88, 83, 84, 85, 86, 87, 81, 82, 68, 69,
	70, 71, 72, 73, 74, -32, -37, -32, -2, -40,
	-37, -37, -37, -37, -37, -79, -79, -43, -79, -48,
	-37, -28, -58, 31, -79, -31, -52, 17, -32, -37,
	92, 47, -71, -76, -68, 102, -21, -22, -23, -24,
	-28, -43, -79, -19, -71, 85, -32, -32, -38, 57,
	62, 58, 59, -37, -39, -79, -43, 55, 78, 76,
	77, 63, -37, -37, -37, -38, -37, -37, -37, -37,
	-37, -37, -37, -37, -37, -37, -80, 50, -80, 49,
	-80, -71, -80, -18, 22, -18, -46, -47, 65, -35,
	33, -2, -61, -59, -44, -52, -56, 19, 18, -71,
	-73, -73, -31, 49, -25, -26, -27, 37, 41, 43,
	38, 39, 40, 44, -74, -73, 23, -75, 23, -21,
	57, 58, 59, -40, -39, -37, -37, -37, 61, -37,
	-80, -18, -80, -49, -47, 67, -32, -60, 47, -41,
	-42, -60, -80, 49, -56, -37, -53, -54, -37, -76,
	-50, 15, -22, -23, -22, -23, 37, 37, 37, 42,
	37, 42, 37, -26, -29, 45, 103, 46, -73, -73,
	-80, -80, 61, -37, -80, 93, -37, 66, 28, 49,
	-44, 49, 49, -55, 25, 26, -51, 16, 18, 47,
	47, 37, 37, 100, 100, 100, -37, -37, 29, -42,
	-37, -54, -52, -32, -40, -32, -32, -79, -79, -79,
	11, -56, -30, -71, -30, -30, -61, -57, 20, 32,
	-80, 49, -80, -80, 11, 78, -71, -71, -71,
}
var yyDef = [...]int{

	0, -2, 1, 2, 3, 4, 5, 6, 7, 8,
	9, 10, 11, 12, 35, 35, 35, 35, 35, 217,
	208, 0, 0, 0, 225, 225, 225, 0, 39, 41,
	42, 43, 44, 37, 208, 0, 0, 0, 206, 0,
	0, 218, 0, 0, 209, 0, 204, 0, 204, 0,
	32, 33, 34, 15, 40, 46, 0, 45, 36, 0,
	0, 84, 222, 0, 20, 199, 0, 162, 0, -2,
	0, 0, 0, 225, 221, 0, 225, 0, 0, 0,
	0, 0, 31, 0, 47, 0, 38, 0, 0, 0,
	92, 0, 0, 0, 225, 0, 219, 23, 0, 26,
	0, 28, 205, 0, 225, 0, 48, 50, 55, 0,
	53, 54, 94, 0, 0, 132, 133, 134, 0, 0,
	0, 0, 162, 0, 153, 100, 101, 0, 223, 165,
	166, 167, 168, 198, 155, 0, 202, 203, 187, 92,
	85, 173, 0, 200, 201, 0, 163, 0, 21, 207,
	0, 0, 225, 215, 210, 211, 212, 213, 214, 27,
	29, 30, 0, 0, 51, 56, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 119, 120,
	121, 122, 123, 124, 125, 97, 0, 0, 0, 0,
	130, 145, 146, 147, 0, 0, 0, 112, 0, 0,
	156, 14, 0, 0, 0, 173, 181, 0, 93, 130,
	0, 0, 220, 24, 0, 216, 92, 58, 60, 61,
	71, 69, 0, 49, 57, 52, 95, 96, 99, 113,
	0, 115, 117, 102, 103, 0, 127, 128, 0, 0,
	0, 0, 105, 107, 0, 111, 135, 136, 137, 138,
	139, 140, 141, 142, 143, 144, 98, 224, 129, 0,
	197, 148, 149, 0, 0, 0, 160, 157, 0, 191,
	0, 194, 191, 0, 189, 181, 19, 0, 0, 164,
	225, 25, 169, 0, 0, 0, 0, 74, 0, 0,
	77, 0, 0, 0, 86, 72, 0, 0, 70, 0,
	114, 116, 118, 0, 104, 106, 108, 0, 0, 131,
	150, 0, 152, 0, 158, 0, 0, 16, 0, 193,
	195, 17, 188, 0, 18, 182, 174, 175, 178, 22,
	171, 0, 59, 65, 0, 68, 75, 76, 78, 0,
	80, 0, 82, 83, 62, 0, 0, 0, 73, 63,
	64, 126, 0, 109, 151, 154, 161, 0, 0, 0,
	190, 0, 0, 177, 179, 180, 173, 0, 0, 0,
	0, 79, 81, 0, 0, 0, 110, 159, 0, 196,
	183, 176, 181, 172, 170, 66, 67, 0, 0, 0,
	0, 184, 0, 90, 0, 0, 192, 13, 0, 0,
	87, 0, 88, 89, 185, 0, 91, 0, 186,
}
var yyTok1 = [...]int{

	1, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 87, 80, 3,
	48, 50, 85, 83, 49, 84, 92, 86, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	69, 68, 70, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 88, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 79, 3, 89,
}
var yyTok2 = [...]int{

	2, 3, 4, 5, 6, 7, 8, 9, 10, 11,
	12, 13, 14, 15, 16, 17, 18, 19, 20, 21,
	22, 23, 24, 25, 26, 27, 28, 29, 30, 31,
	32, 33, 34, 35, 36, 37, 38, 39, 40, 41,
	42, 43, 44, 45, 46, 47, 51, 52, 53, 54,
	55, 56, 57, 58, 59, 60, 61, 62, 63, 64,
	65, 66, 67, 71, 72, 73, 74, 75, 76, 77,
	78, 81, 82, 90, 91, 93, 94, 95, 96, 97,
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
		yyDollar = yyS[yypt-13 : yypt+1]
		//line sql.y:190
		{
			yyVAL.selStmt = &Select{Comments: Comments(yyDollar[2].bytes2), Distinct: yyDollar[3].str, Hints: yyDollar[4].str, SelectExprs: yyDollar[5].selectExprs, From: yyDollar[7].tableExprs, Where: NewWhere(WhereStr, yyDollar[8].boolExpr), GroupBy: GroupBy(yyDollar[9].valExprs), Having: NewWhere(HavingStr, yyDollar[10].boolExpr), OrderBy: yyDollar[11].orderBy, Limit: yyDollar[12].limit, Lock: yyDollar[13].str}
		}
	case 14:
		yyDollar = yyS[yypt-6 : yypt+1]
		//line sql.y:194
		{
			if yyDollar[4].sqlID != "value" {
				yylex.Error("expecting value after next")
				return 1
			}
			yyVAL.selStmt = &Select{Comments: Comments(yyDollar[2].bytes2), SelectExprs: SelectExprs{Nextval{}}, From: TableExprs{&AliasedTableExpr{Expr: yyDollar[6].tableName}}}
		}
	case 15:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:202
		{
			yyVAL.selStmt = &Union{Type: yyDollar[2].str, Left: yyDollar[1].selStmt, Right: yyDollar[3].selStmt}
		}
	case 16:
		yyDollar = yyS[yypt-8 : yypt+1]
		//line sql.y:208
		{
			yyVAL.statement = &Insert{Comments: Comments(yyDollar[2].bytes2), Ignore: yyDollar[3].str, Table: yyDollar[5].tableName, Columns: yyDollar[6].columns, Rows: yyDollar[7].insRows, OnDup: OnDup(yyDollar[8].updateExprs)}
		}
	case 17:
		yyDollar = yyS[yypt-8 : yypt+1]
		//line sql.y:212
		{
			cols := make(Columns, 0, len(yyDollar[7].updateExprs))
			vals := make(ValTuple, 0, len(yyDollar[7].updateExprs))
			for _, col := range yyDollar[7].updateExprs {
				cols = append(cols, &NonStarExpr{Expr: col.Name})
				vals = append(vals, col.Expr)
			}
			yyVAL.statement = &Insert{Comments: Comments(yyDollar[2].bytes2), Ignore: yyDollar[3].str, Table: yyDollar[5].tableName, Columns: cols, Rows: Values{vals}, OnDup: OnDup(yyDollar[8].updateExprs)}
		}
	case 18:
		yyDollar = yyS[yypt-8 : yypt+1]
		//line sql.y:224
		{
			yyVAL.statement = &Update{Comments: Comments(yyDollar[2].bytes2), Table: yyDollar[3].tableName, Exprs: yyDollar[5].updateExprs, Where: NewWhere(WhereStr, yyDollar[6].boolExpr), OrderBy: yyDollar[7].orderBy, Limit: yyDollar[8].limit}
		}
	case 19:
		yyDollar = yyS[yypt-7 : yypt+1]
		//line sql.y:230
		{
			yyVAL.statement = &Delete{Comments: Comments(yyDollar[2].bytes2), Table: yyDollar[4].tableName, Where: NewWhere(WhereStr, yyDollar[5].boolExpr), OrderBy: yyDollar[6].orderBy, Limit: yyDollar[7].limit}
		}
	case 20:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:236
		{
			yyVAL.statement = &Set{Comments: Comments(yyDollar[2].bytes2), Exprs: yyDollar[3].updateExprs}
		}
	case 21:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:242
		{
			yyVAL.statement = &DDL{Action: CreateStr, NewName: yyDollar[4].sqlID}
		}
	case 22:
		yyDollar = yyS[yypt-8 : yypt+1]
		//line sql.y:246
		{
			// Change this to an alter statement
			yyVAL.statement = &DDL{Action: AlterStr, Table: yyDollar[7].sqlID, NewName: yyDollar[7].sqlID}
		}
	case 23:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:251
		{
			yyVAL.statement = &DDL{Action: CreateStr, NewName: SQLName(yyDollar[3].sqlID)}
		}
	case 24:
		yyDollar = yyS[yypt-6 : yypt+1]
		//line sql.y:257
		{
			yyVAL.statement = &DDL{Action: AlterStr, Table: yyDollar[4].sqlID, NewName: yyDollar[4].sqlID}
		}
	case 25:
		yyDollar = yyS[yypt-7 : yypt+1]
		//line sql.y:261
		{
			// Change this to a rename statement
			yyVAL.statement = &DDL{Action: RenameStr, Table: yyDollar[4].sqlID, NewName: yyDollar[7].sqlID}
		}
	case 26:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:266
		{
			yyVAL.statement = &DDL{Action: AlterStr, Table: SQLName(yyDollar[3].sqlID), NewName: SQLName(yyDollar[3].sqlID)}
		}
	case 27:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:272
		{
			yyVAL.statement = &DDL{Action: RenameStr, Table: yyDollar[3].sqlID, NewName: yyDollar[5].sqlID}
		}
	case 28:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:278
		{
			yyVAL.statement = &DDL{Action: DropStr, Table: yyDollar[4].sqlID}
		}
	case 29:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:282
		{
			// Change this to an alter statement
			yyVAL.statement = &DDL{Action: AlterStr, Table: yyDollar[5].sqlID, NewName: yyDollar[5].sqlID}
		}
	case 30:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:287
		{
			yyVAL.statement = &DDL{Action: DropStr, Table: SQLName(yyDollar[4].sqlID)}
		}
	case 31:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:293
		{
			yyVAL.statement = &DDL{Action: AlterStr, Table: yyDollar[3].sqlID, NewName: yyDollar[3].sqlID}
		}
	case 32:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:299
		{
			yyVAL.statement = &Other{}
		}
	case 33:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:303
		{
			yyVAL.statement = &Other{}
		}
	case 34:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:307
		{
			yyVAL.statement = &Other{}
		}
	case 35:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:312
		{
			setAllowComments(yylex, true)
		}
	case 36:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:316
		{
			yyVAL.bytes2 = yyDollar[2].bytes2
			setAllowComments(yylex, false)
		}
	case 37:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:322
		{
			yyVAL.bytes2 = nil
		}
	case 38:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:326
		{
			yyVAL.bytes2 = append(yyDollar[1].bytes2, yyDollar[2].bytes)
		}
	case 39:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:332
		{
			yyVAL.str = UnionStr
		}
	case 40:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:336
		{
			yyVAL.str = UnionAllStr
		}
	case 41:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:340
		{
			yyVAL.str = SetMinusStr
		}
	case 42:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:344
		{
			yyVAL.str = ExceptStr
		}
	case 43:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:348
		{
			yyVAL.str = IntersectStr
		}
	case 44:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:353
		{
			yyVAL.str = ""
		}
	case 45:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:357
		{
			yyVAL.str = DistinctStr
		}
	case 46:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:362
		{
			yyVAL.str = ""
		}
	case 47:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:366
		{
			yyVAL.str = StraightJoinHint
		}
	case 48:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:372
		{
			yyVAL.selectExprs = SelectExprs{yyDollar[1].selectExpr}
		}
	case 49:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:376
		{
			yyVAL.selectExprs = append(yyVAL.selectExprs, yyDollar[3].selectExpr)
		}
	case 50:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:382
		{
			yyVAL.selectExpr = &StarExpr{}
		}
	case 51:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:386
		{
			yyVAL.selectExpr = &NonStarExpr{Expr: yyDollar[1].expr, As: yyDollar[2].sqlID}
		}
	case 52:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:390
		{
			yyVAL.selectExpr = &StarExpr{TableName: yyDollar[1].sqlID}
		}
	case 53:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:396
		{
			yyVAL.expr = yyDollar[1].boolExpr
		}
	case 54:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:400
		{
			yyVAL.expr = yyDollar[1].valExpr
		}
	case 55:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:405
		{
			yyVAL.sqlID = ""
		}
	case 56:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:409
		{
			yyVAL.sqlID = yyDollar[1].sqlID
		}
	case 57:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:413
		{
			yyVAL.sqlID = yyDollar[2].sqlID
		}
	case 58:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:419
		{
			yyVAL.tableExprs = TableExprs{yyDollar[1].tableExpr}
		}
	case 59:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:423
		{
			yyVAL.tableExprs = append(yyVAL.tableExprs, yyDollar[3].tableExpr)
		}
	case 62:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:433
		{
			yyVAL.tableExpr = &AliasedTableExpr{Expr: yyDollar[1].tableName, As: yyDollar[2].sqlID, Hints: yyDollar[3].indexHints}
		}
	case 63:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:437
		{
			yyVAL.tableExpr = &AliasedTableExpr{Expr: yyDollar[1].subquery, As: yyDollar[3].sqlID}
		}
	case 64:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:441
		{
			yyVAL.tableExpr = &ParenTableExpr{Exprs: yyDollar[2].tableExprs}
		}
	case 65:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:454
		{
			yyVAL.tableExpr = &JoinTableExpr{LeftExpr: yyDollar[1].tableExpr, Join: yyDollar[2].str, RightExpr: yyDollar[3].tableExpr}
		}
	case 66:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:458
		{
			yyVAL.tableExpr = &JoinTableExpr{LeftExpr: yyDollar[1].tableExpr, Join: yyDollar[2].str, RightExpr: yyDollar[3].tableExpr, On: yyDollar[5].boolExpr}
		}
	case 67:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:462
		{
			yyVAL.tableExpr = &JoinTableExpr{LeftExpr: yyDollar[1].tableExpr, Join: yyDollar[2].str, RightExpr: yyDollar[3].tableExpr, On: yyDollar[5].boolExpr}
		}
	case 68:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:466
		{
			yyVAL.tableExpr = &JoinTableExpr{LeftExpr: yyDollar[1].tableExpr, Join: yyDollar[2].str, RightExpr: yyDollar[3].tableExpr}
		}
	case 69:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:471
		{
			yyVAL.empty = struct{}{}
		}
	case 70:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:473
		{
			yyVAL.empty = struct{}{}
		}
	case 71:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:476
		{
			yyVAL.sqlID = ""
		}
	case 72:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:480
		{
			yyVAL.sqlID = yyDollar[1].sqlID
		}
	case 73:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:484
		{
			yyVAL.sqlID = yyDollar[2].sqlID
		}
	case 74:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:490
		{
			yyVAL.str = JoinStr
		}
	case 75:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:494
		{
			yyVAL.str = JoinStr
		}
	case 76:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:498
		{
			yyVAL.str = JoinStr
		}
	case 77:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:502
		{
			yyVAL.str = StraightJoinStr
		}
	case 78:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:508
		{
			yyVAL.str = LeftJoinStr
		}
	case 79:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:512
		{
			yyVAL.str = LeftJoinStr
		}
	case 80:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:516
		{
			yyVAL.str = RightJoinStr
		}
	case 81:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:520
		{
			yyVAL.str = RightJoinStr
		}
	case 82:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:526
		{
			yyVAL.str = NaturalJoinStr
		}
	case 83:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:530
		{
			if yyDollar[2].str == LeftJoinStr {
				yyVAL.str = NaturalLeftJoinStr
			} else {
				yyVAL.str = NaturalRightJoinStr
			}
		}
	case 84:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:540
		{
			yyVAL.tableName = &TableName{Name: yyDollar[1].sqlID}
		}
	case 85:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:544
		{
			yyVAL.tableName = &TableName{Qualifier: yyDollar[1].sqlID, Name: yyDollar[3].sqlID}
		}
	case 86:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:549
		{
			yyVAL.indexHints = nil
		}
	case 87:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:553
		{
			yyVAL.indexHints = &IndexHints{Type: UseStr, Indexes: yyDollar[4].sqlIDs}
		}
	case 88:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:557
		{
			yyVAL.indexHints = &IndexHints{Type: IgnoreStr, Indexes: yyDollar[4].sqlIDs}
		}
	case 89:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:561
		{
			yyVAL.indexHints = &IndexHints{Type: ForceStr, Indexes: yyDollar[4].sqlIDs}
		}
	case 90:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:567
		{
			yyVAL.sqlIDs = []SQLName{yyDollar[1].sqlID}
		}
	case 91:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:571
		{
			yyVAL.sqlIDs = append(yyDollar[1].sqlIDs, yyDollar[3].sqlID)
		}
	case 92:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:576
		{
			yyVAL.boolExpr = nil
		}
	case 93:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:580
		{
			yyVAL.boolExpr = yyDollar[2].boolExpr
		}
	case 95:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:587
		{
			yyVAL.boolExpr = &AndExpr{Left: yyDollar[1].boolExpr, Right: yyDollar[3].boolExpr}
		}
	case 96:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:591
		{
			yyVAL.boolExpr = &OrExpr{Left: yyDollar[1].boolExpr, Right: yyDollar[3].boolExpr}
		}
	case 97:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:595
		{
			yyVAL.boolExpr = &NotExpr{Expr: yyDollar[2].boolExpr}
		}
	case 98:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:599
		{
			yyVAL.boolExpr = &ParenBoolExpr{Expr: yyDollar[2].boolExpr}
		}
	case 99:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:603
		{
			yyVAL.boolExpr = &IsExpr{Operator: yyDollar[3].str, Expr: yyDollar[1].boolExpr}
		}
	case 100:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:609
		{
			yyVAL.boolExpr = BoolVal(true)
		}
	case 101:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:613
		{
			yyVAL.boolExpr = BoolVal(false)
		}
	case 102:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:617
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: yyDollar[2].str, Right: yyDollar[3].valExpr}
		}
	case 103:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:621
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: InStr, Right: yyDollar[3].colTuple}
		}
	case 104:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:625
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: NotInStr, Right: yyDollar[4].colTuple}
		}
	case 105:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:629
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: LikeStr, Right: yyDollar[3].valExpr}
		}
	case 106:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:633
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: NotLikeStr, Right: yyDollar[4].valExpr}
		}
	case 107:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:637
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: RegexpStr, Right: yyDollar[3].valExpr}
		}
	case 108:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:641
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: NotRegexpStr, Right: yyDollar[4].valExpr}
		}
	case 109:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:645
		{
			yyVAL.boolExpr = &RangeCond{Left: yyDollar[1].valExpr, Operator: BetweenStr, From: yyDollar[3].valExpr, To: yyDollar[5].valExpr}
		}
	case 110:
		yyDollar = yyS[yypt-6 : yypt+1]
		//line sql.y:649
		{
			yyVAL.boolExpr = &RangeCond{Left: yyDollar[1].valExpr, Operator: NotBetweenStr, From: yyDollar[4].valExpr, To: yyDollar[6].valExpr}
		}
	case 111:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:653
		{
			yyVAL.boolExpr = &IsExpr{Operator: yyDollar[3].str, Expr: yyDollar[1].valExpr}
		}
	case 112:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:657
		{
			yyVAL.boolExpr = &ExistsExpr{Subquery: yyDollar[2].subquery}
		}
	case 113:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:663
		{
			yyVAL.str = IsNullStr
		}
	case 114:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:667
		{
			yyVAL.str = IsNotNullStr
		}
	case 115:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:671
		{
			yyVAL.str = IsTrueStr
		}
	case 116:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:675
		{
			yyVAL.str = IsNotTrueStr
		}
	case 117:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:679
		{
			yyVAL.str = IsFalseStr
		}
	case 118:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:683
		{
			yyVAL.str = IsNotFalseStr
		}
	case 119:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:689
		{
			yyVAL.str = EqualStr
		}
	case 120:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:693
		{
			yyVAL.str = LessThanStr
		}
	case 121:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:697
		{
			yyVAL.str = GreaterThanStr
		}
	case 122:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:701
		{
			yyVAL.str = LessEqualStr
		}
	case 123:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:705
		{
			yyVAL.str = GreaterEqualStr
		}
	case 124:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:709
		{
			yyVAL.str = NotEqualStr
		}
	case 125:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:713
		{
			yyVAL.str = NullSafeEqualStr
		}
	case 126:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:719
		{
			yyVAL.colTuple = ValTuple(yyDollar[2].valExprs)
		}
	case 127:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:723
		{
			yyVAL.colTuple = yyDollar[1].subquery
		}
	case 128:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:727
		{
			yyVAL.colTuple = ListArg(yyDollar[1].bytes)
		}
	case 129:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:733
		{
			yyVAL.subquery = &Subquery{yyDollar[2].selStmt}
		}
	case 130:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:739
		{
			yyVAL.valExprs = ValExprs{yyDollar[1].valExpr}
		}
	case 131:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:743
		{
			yyVAL.valExprs = append(yyDollar[1].valExprs, yyDollar[3].valExpr)
		}
	case 132:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:749
		{
			yyVAL.valExpr = yyDollar[1].valExpr
		}
	case 133:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:753
		{
			yyVAL.valExpr = yyDollar[1].colName
		}
	case 134:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:757
		{
			yyVAL.valExpr = yyDollar[1].rowTuple
		}
	case 135:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:761
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: BitAndStr, Right: yyDollar[3].valExpr}
		}
	case 136:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:765
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: BitOrStr, Right: yyDollar[3].valExpr}
		}
	case 137:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:769
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: BitXorStr, Right: yyDollar[3].valExpr}
		}
	case 138:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:773
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: PlusStr, Right: yyDollar[3].valExpr}
		}
	case 139:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:777
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: MinusStr, Right: yyDollar[3].valExpr}
		}
	case 140:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:781
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: MultStr, Right: yyDollar[3].valExpr}
		}
	case 141:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:785
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: DivStr, Right: yyDollar[3].valExpr}
		}
	case 142:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:789
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: ModStr, Right: yyDollar[3].valExpr}
		}
	case 143:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:793
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: ShiftLeftStr, Right: yyDollar[3].valExpr}
		}
	case 144:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:797
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: ShiftRightStr, Right: yyDollar[3].valExpr}
		}
	case 145:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:801
		{
			if num, ok := yyDollar[2].valExpr.(NumVal); ok {
				yyVAL.valExpr = num
			} else {
				yyVAL.valExpr = &UnaryExpr{Operator: UPlusStr, Expr: yyDollar[2].valExpr}
			}
		}
	case 146:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:809
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
	case 147:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:822
		{
			yyVAL.valExpr = &UnaryExpr{Operator: TildaStr, Expr: yyDollar[2].valExpr}
		}
	case 148:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:826
		{
			// This rule prevents the usage of INTERVAL
			// as a function. If support is needed for that,
			// we'll need to revisit this. The solution
			// will be non-trivial because of grammar conflicts.
			yyVAL.valExpr = &IntervalExpr{Expr: yyDollar[2].valExpr, Unit: yyDollar[3].sqlID}
		}
	case 149:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:834
		{
			yyVAL.valExpr = &FuncExpr{Name: string(yyDollar[1].sqlID)}
		}
	case 150:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:838
		{
			yyVAL.valExpr = &FuncExpr{Name: string(yyDollar[1].sqlID), Exprs: yyDollar[3].selectExprs}
		}
	case 151:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:842
		{
			yyVAL.valExpr = &FuncExpr{Name: string(yyDollar[1].sqlID), Distinct: true, Exprs: yyDollar[4].selectExprs}
		}
	case 152:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:846
		{
			yyVAL.valExpr = &FuncExpr{Name: "if", Exprs: yyDollar[3].selectExprs}
		}
	case 153:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:850
		{
			yyVAL.valExpr = yyDollar[1].caseExpr
		}
	case 154:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:856
		{
			yyVAL.caseExpr = &CaseExpr{Expr: yyDollar[2].valExpr, Whens: yyDollar[3].whens, Else: yyDollar[4].valExpr}
		}
	case 155:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:861
		{
			yyVAL.valExpr = nil
		}
	case 156:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:865
		{
			yyVAL.valExpr = yyDollar[1].valExpr
		}
	case 157:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:871
		{
			yyVAL.whens = []*When{yyDollar[1].when}
		}
	case 158:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:875
		{
			yyVAL.whens = append(yyDollar[1].whens, yyDollar[2].when)
		}
	case 159:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:881
		{
			yyVAL.when = &When{Cond: yyDollar[2].boolExpr, Val: yyDollar[4].valExpr}
		}
	case 160:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:886
		{
			yyVAL.valExpr = nil
		}
	case 161:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:890
		{
			yyVAL.valExpr = yyDollar[2].valExpr
		}
	case 162:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:896
		{
			yyVAL.colName = &ColName{Name: yyDollar[1].sqlID}
		}
	case 163:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:900
		{
			yyVAL.colName = &ColName{Qualifier: &TableName{Name: yyDollar[1].sqlID}, Name: yyDollar[3].sqlID}
		}
	case 164:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:904
		{
			yyVAL.colName = &ColName{Qualifier: &TableName{Qualifier: yyDollar[1].sqlID, Name: yyDollar[3].sqlID}, Name: yyDollar[5].sqlID}
		}
	case 165:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:910
		{
			yyVAL.valExpr = StrVal(yyDollar[1].bytes)
		}
	case 166:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:914
		{
			yyVAL.valExpr = NumVal(yyDollar[1].bytes)
		}
	case 167:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:918
		{
			yyVAL.valExpr = ValArg(yyDollar[1].bytes)
		}
	case 168:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:922
		{
			yyVAL.valExpr = &NullVal{}
		}
	case 169:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:927
		{
			yyVAL.valExprs = nil
		}
	case 170:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:931
		{
			yyVAL.valExprs = yyDollar[3].valExprs
		}
	case 171:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:936
		{
			yyVAL.boolExpr = nil
		}
	case 172:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:940
		{
			yyVAL.boolExpr = yyDollar[2].boolExpr
		}
	case 173:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:945
		{
			yyVAL.orderBy = nil
		}
	case 174:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:949
		{
			yyVAL.orderBy = yyDollar[3].orderBy
		}
	case 175:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:955
		{
			yyVAL.orderBy = OrderBy{yyDollar[1].order}
		}
	case 176:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:959
		{
			yyVAL.orderBy = append(yyDollar[1].orderBy, yyDollar[3].order)
		}
	case 177:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:965
		{
			yyVAL.order = &Order{Expr: yyDollar[1].valExpr, Direction: yyDollar[2].str}
		}
	case 178:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:970
		{
			yyVAL.str = AscScr
		}
	case 179:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:974
		{
			yyVAL.str = AscScr
		}
	case 180:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:978
		{
			yyVAL.str = DescScr
		}
	case 181:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:983
		{
			yyVAL.limit = nil
		}
	case 182:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:987
		{
			yyVAL.limit = &Limit{Rowcount: yyDollar[2].valExpr}
		}
	case 183:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:991
		{
			yyVAL.limit = &Limit{Offset: yyDollar[2].valExpr, Rowcount: yyDollar[4].valExpr}
		}
	case 184:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:996
		{
			yyVAL.str = ""
		}
	case 185:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:1000
		{
			yyVAL.str = ForUpdateStr
		}
	case 186:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:1004
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
		//line sql.y:1017
		{
			yyVAL.columns = nil
		}
	case 188:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1021
		{
			yyVAL.columns = yyDollar[2].columns
		}
	case 189:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1027
		{
			yyVAL.columns = Columns{&NonStarExpr{Expr: yyDollar[1].colName}}
		}
	case 190:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1031
		{
			yyVAL.columns = append(yyVAL.columns, &NonStarExpr{Expr: yyDollar[3].colName})
		}
	case 191:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1036
		{
			yyVAL.updateExprs = nil
		}
	case 192:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:1040
		{
			yyVAL.updateExprs = yyDollar[5].updateExprs
		}
	case 193:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:1046
		{
			yyVAL.insRows = yyDollar[2].values
		}
	case 194:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1050
		{
			yyVAL.insRows = yyDollar[1].selStmt
		}
	case 195:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1056
		{
			yyVAL.values = Values{yyDollar[1].rowTuple}
		}
	case 196:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1060
		{
			yyVAL.values = append(yyDollar[1].values, yyDollar[3].rowTuple)
		}
	case 197:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1066
		{
			yyVAL.rowTuple = ValTuple(yyDollar[2].valExprs)
		}
	case 198:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1070
		{
			yyVAL.rowTuple = yyDollar[1].subquery
		}
	case 199:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1076
		{
			yyVAL.updateExprs = UpdateExprs{yyDollar[1].updateExpr}
		}
	case 200:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1080
		{
			yyVAL.updateExprs = append(yyDollar[1].updateExprs, yyDollar[3].updateExpr)
		}
	case 201:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1086
		{
			yyVAL.updateExpr = &UpdateExpr{Name: yyDollar[1].colName, Expr: yyDollar[3].valExpr}
		}
	case 204:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1095
		{
			yyVAL.empty = struct{}{}
		}
	case 205:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:1097
		{
			yyVAL.empty = struct{}{}
		}
	case 206:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1100
		{
			yyVAL.empty = struct{}{}
		}
	case 207:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1102
		{
			yyVAL.empty = struct{}{}
		}
	case 208:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1105
		{
			yyVAL.str = ""
		}
	case 209:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1107
		{
			yyVAL.str = IgnoreStr
		}
	case 210:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1111
		{
			yyVAL.empty = struct{}{}
		}
	case 211:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1113
		{
			yyVAL.empty = struct{}{}
		}
	case 212:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1115
		{
			yyVAL.empty = struct{}{}
		}
	case 213:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1117
		{
			yyVAL.empty = struct{}{}
		}
	case 214:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1119
		{
			yyVAL.empty = struct{}{}
		}
	case 215:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1122
		{
			yyVAL.empty = struct{}{}
		}
	case 216:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1124
		{
			yyVAL.empty = struct{}{}
		}
	case 217:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1127
		{
			yyVAL.empty = struct{}{}
		}
	case 218:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1129
		{
			yyVAL.empty = struct{}{}
		}
	case 219:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1132
		{
			yyVAL.empty = struct{}{}
		}
	case 220:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:1134
		{
			yyVAL.empty = struct{}{}
		}
	case 221:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1138
		{
			yyVAL.sqlID = SQLName(strings.ToLower(string(yyDollar[1].bytes)))
		}
	case 222:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1144
		{
			yyVAL.sqlID = SQLName(yyDollar[1].bytes)
		}
	case 223:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1150
		{
			if incNesting(yylex) {
				yylex.Error("max nesting level reached")
				return 1
			}
		}
	case 224:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1159
		{
			decNesting(yylex)
		}
	case 225:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1164
		{
			forceEOF(yylex)
		}
	}
	goto yystack /* stack new state and value */
}
