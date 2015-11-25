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
	89, 215,
	-2, 214,
}

const yyNprod = 219
const yyPrivate = 57344

var yyTokenNames []string
var yyStates []string

const yyLast = 757

var yyAct = [...]int{

	98, 94, 165, 390, 93, 168, 343, 353, 63, 260,
	271, 309, 92, 205, 254, 242, 222, 167, 3, 82,
	184, 216, 64, 203, 204, 59, 83, 192, 38, 78,
	40, 70, 267, 14, 41, 43, 87, 44, 66, 329,
	331, 72, 65, 44, 75, 53, 46, 47, 48, 365,
	127, 364, 363, 71, 74, 49, 105, 50, 88, 68,
	106, 107, 108, 45, 340, 109, 152, 153, 154, 149,
	284, 105, 112, 137, 68, 106, 107, 108, 120, 116,
	109, 131, 149, 51, 52, 73, 135, 112, 117, 228,
	95, 96, 404, 140, 169, 330, 97, 119, 170, 172,
	173, 255, 227, 226, 122, 95, 96, 171, 113, 73,
	111, 97, 138, 68, 105, 180, 66, 61, 213, 66,
	65, 188, 187, 65, 182, 111, 140, 164, 166, 14,
	124, 139, 138, 126, 282, 105, 88, 212, 188, 61,
	181, 186, 225, 255, 221, 300, 140, 229, 230, 123,
	232, 233, 234, 235, 236, 237, 238, 239, 240, 241,
	211, 61, 243, 91, 79, 139, 138, 105, 231, 136,
	61, 342, 139, 138, 247, 214, 215, 88, 88, 189,
	140, 399, 243, 244, 246, 349, 243, 140, 243, 202,
	248, 252, 118, 264, 249, 251, 73, 245, 91, 91,
	198, 359, 373, 259, 110, 370, 174, 175, 285, 286,
	287, 177, 178, 196, 155, 156, 150, 151, 152, 153,
	154, 149, 262, 344, 268, 118, 247, 133, 243, 288,
	290, 291, 283, 199, 150, 151, 152, 153, 154, 149,
	208, 91, 77, 289, 245, 243, 91, 91, 293, 297,
	223, 265, 132, 88, 185, 217, 219, 220, 66, 66,
	218, 105, 65, 307, 185, 294, 305, 296, 299, 308,
	295, 375, 376, 344, 304, 195, 197, 194, 269, 243,
	263, 130, 91, 91, 317, 257, 319, 133, 269, 258,
	327, 80, 301, 91, 316, 337, 318, 324, 118, 333,
	362, 105, 325, 341, 335, 346, 361, 321, 176, 320,
	339, 338, 347, 351, 354, 326, 208, 277, 278, 14,
	350, 42, 348, 148, 147, 155, 156, 150, 151, 152,
	153, 154, 149, 322, 223, 115, 396, 210, 323, 366,
	385, 106, 107, 108, 303, 368, 109, 224, 397, 369,
	66, 114, 190, 129, 371, 367, 58, 56, 91, 54,
	310, 247, 358, 91, 379, 67, 377, 311, 261, 357,
	355, 315, 386, 185, 62, 387, 354, 208, 208, 208,
	208, 388, 403, 391, 391, 391, 394, 392, 393, 389,
	28, 29, 30, 31, 378, 66, 380, 381, 14, 65,
	405, 60, 33, 402, 280, 406, 398, 407, 400, 401,
	134, 76, 191, 210, 39, 81, 273, 276, 277, 278,
	274, 86, 275, 279, 266, 250, 360, 103, 60, 32,
	193, 224, 69, 306, 243, 121, 104, 256, 395, 374,
	125, 352, 356, 128, 314, 34, 35, 36, 37, 298,
	105, 179, 243, 68, 106, 107, 108, 253, 100, 109,
	101, 102, 99, 345, 90, 91, 112, 91, 91, 302,
	141, 382, 383, 384, 210, 210, 210, 210, 372, 89,
	60, 328, 183, 207, 95, 96, 84, 272, 270, 206,
	97, 85, 55, 200, 27, 57, 201, 13, 209, 86,
	14, 15, 16, 17, 111, 12, 11, 148, 147, 155,
	156, 150, 151, 152, 153, 154, 149, 273, 276, 277,
	278, 274, 18, 275, 279, 336, 28, 29, 30, 31,
	103, 10, 9, 8, 7, 6, 5, 4, 2, 104,
	86, 86, 148, 147, 155, 156, 150, 151, 152, 153,
	154, 149, 1, 105, 0, 0, 68, 106, 107, 108,
	0, 0, 109, 101, 102, 0, 0, 90, 0, 112,
	0, 0, 0, 281, 209, 0, 0, 0, 0, 14,
	0, 0, 19, 20, 22, 21, 23, 95, 96, 84,
	0, 0, 0, 97, 103, 24, 25, 26, 0, 0,
	0, 0, 0, 104, 0, 0, 0, 111, 0, 0,
	0, 0, 0, 0, 0, 0, 86, 105, 0, 0,
	68, 106, 107, 108, 0, 0, 109, 101, 102, 312,
	0, 90, 313, 112, 0, 209, 209, 209, 209, 0,
	103, 0, 0, 0, 0, 0, 0, 0, 332, 104,
	334, 95, 96, 0, 0, 0, 0, 97, 0, 0,
	0, 0, 0, 105, 292, 0, 68, 106, 107, 108,
	0, 111, 109, 101, 102, 0, 0, 90, 0, 112,
	0, 148, 147, 155, 156, 150, 151, 152, 153, 154,
	149, 0, 0, 0, 0, 0, 0, 95, 96, 0,
	0, 0, 0, 97, 0, 0, 0, 0, 0, 0,
	0, 0, 143, 145, 0, 0, 0, 111, 157, 158,
	159, 160, 161, 162, 163, 146, 144, 142, 148, 147,
	155, 156, 150, 151, 152, 153, 154, 149, 148, 147,
	155, 156, 150, 151, 152, 153, 154, 149, 147, 155,
	156, 150, 151, 152, 153, 154, 149,
}
var yyPact = [...]int{

	491, -1000, -1000, 521, -1000, -1000, -1000, -1000, -1000, -1000,
	-1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, -68,
	-63, -33, -50, -41, -1000, -1000, -1000, 389, 338, -1000,
	-1000, -1000, 335, -1000, -57, 89, 361, 63, -70, -44,
	59, -1000, -42, 59, -1000, 89, -72, 114, -72, 89,
	-1000, -1000, -1000, -1000, -1000, 506, -1000, 53, 324, 304,
	-10, -1000, 89, 144, -1000, 30, -1000, -11, -1000, 89,
	43, 99, -1000, -1000, 89, -1000, -49, 89, 329, 235,
	59, -1000, 239, -1000, -1000, 146, -16, 72, 651, -1000,
	616, 570, -1000, -1000, -1000, 9, 9, 9, 214, 214,
	-1000, -1000, -1000, 214, 214, -1000, -1000, -1000, -1000, -1000,
	-1000, -1000, 9, -1000, 89, 63, 89, 359, 63, 9,
	59, -1000, 328, -76, -1000, 183, -1000, 89, -1000, -1000,
	89, -1000, 67, 506, -1000, -1000, 59, 35, 616, 616,
	199, 9, 88, 27, 9, 9, 199, 9, 9, 9,
	9, 9, 9, 9, 9, 9, 9, -1000, -1000, -1000,
	-1000, -1000, -1000, -1000, 19, 651, 113, 385, 196, 651,
	-1000, 24, -1000, -1000, 403, 506, -1000, 389, 290, 37,
	661, 254, 250, -1000, 351, 616, -1000, 661, -1000, -1000,
	-1000, 234, 59, -1000, -67, -1000, -1000, -1000, -1000, -1000,
	-1000, -1000, -1000, 240, 481, -1000, -1000, 111, 120, -19,
	-1000, -1000, -1000, -1000, 19, 52, -1000, -1000, 152, -1000,
	-1000, 661, -1000, 24, -1000, -1000, 88, 9, 9, 661,
	604, -1000, 135, 670, -1000, -17, -17, -4, -4, -4,
	153, 153, -1000, -1000, -1000, 9, -1000, 661, -1000, 179,
	506, 179, 201, 79, -1000, 616, 310, 63, 63, 351,
	341, 349, 72, 89, -1000, -1000, 89, -1000, 356, 67,
	67, 67, 67, -1000, 273, 271, -1000, 297, 261, 279,
	-5, -1000, 89, 230, 89, -1000, -1000, -1000, 196, -1000,
	661, 465, 9, 661, -1000, 179, -1000, 290, -26, -1000,
	9, 106, 227, 214, 521, 177, 137, -1000, 341, -1000,
	9, 9, -1000, -1000, 353, 344, 481, 155, 380, -1000,
	-1000, -1000, -1000, 270, -1000, 264, -1000, -1000, -1000, -45,
	-46, -48, -1000, -1000, -1000, -1000, 9, 661, -1000, 139,
	-1000, 661, 9, -1000, 321, 157, -1000, -1000, -1000, 63,
	-1000, 430, 154, -1000, 246, -1000, 351, 616, 9, 616,
	616, -1000, -1000, 214, 214, 214, 661, -1000, 661, 311,
	214, -1000, 9, 9, -1000, -1000, -1000, 341, 72, 149,
	72, 72, 59, 59, 59, 375, -1000, 661, -1000, 316,
	133, -1000, 133, 133, 63, -1000, 371, 16, -1000, 59,
	-1000, -1000, 144, -1000, 59, -1000, 59, -1000,
}
var yyPgo = [...]int{

	0, 552, 538, 17, 537, 536, 535, 534, 533, 532,
	531, 506, 505, 497, 429, 495, 494, 492, 19, 26,
	491, 23, 24, 13, 489, 488, 10, 487, 483, 25,
	481, 3, 20, 36, 479, 470, 469, 12, 2, 21,
	16, 5, 463, 1, 462, 204, 4, 458, 457, 14,
	451, 449, 444, 442, 9, 441, 7, 439, 11, 438,
	437, 433, 6, 8, 22, 321, 242, 432, 430, 424,
	414, 412, 0, 410, 365, 404, 57, 402, 107, 15,
}
var yyR1 = [...]int{

	0, 1, 2, 2, 2, 2, 2, 2, 2, 2,
	2, 2, 2, 3, 3, 4, 4, 5, 6, 7,
	8, 8, 8, 9, 9, 9, 10, 11, 11, 11,
	12, 13, 13, 13, 77, 14, 15, 15, 16, 16,
	16, 16, 16, 17, 17, 18, 18, 19, 19, 19,
	20, 20, 73, 73, 73, 21, 21, 22, 22, 23,
	23, 24, 24, 24, 24, 75, 75, 75, 25, 25,
	25, 25, 26, 26, 26, 26, 27, 27, 28, 28,
	28, 29, 29, 30, 30, 30, 30, 31, 31, 32,
	32, 33, 33, 33, 33, 33, 33, 34, 34, 34,
	34, 34, 34, 34, 34, 34, 34, 34, 34, 39,
	39, 39, 39, 39, 39, 35, 35, 35, 35, 35,
	35, 35, 40, 40, 40, 45, 41, 41, 38, 38,
	38, 38, 38, 38, 38, 38, 38, 38, 38, 38,
	38, 38, 38, 38, 38, 38, 38, 38, 38, 44,
	47, 50, 50, 48, 48, 49, 51, 51, 46, 46,
	37, 37, 37, 37, 52, 52, 53, 53, 54, 54,
	55, 55, 56, 57, 57, 57, 58, 58, 58, 59,
	59, 59, 60, 60, 61, 61, 62, 62, 36, 36,
	42, 42, 43, 43, 63, 63, 64, 66, 66, 67,
	67, 65, 65, 68, 68, 68, 68, 68, 69, 69,
	70, 70, 71, 71, 72, 74, 78, 79, 76,
}
var yyR2 = [...]int{

	0, 1, 1, 1, 1, 1, 1, 1, 1, 1,
	1, 1, 1, 12, 3, 8, 8, 8, 7, 3,
	5, 8, 4, 6, 7, 4, 5, 4, 5, 5,
	3, 2, 2, 2, 0, 2, 0, 2, 1, 2,
	1, 1, 1, 0, 1, 1, 3, 1, 2, 3,
	1, 1, 0, 1, 2, 1, 3, 1, 1, 3,
	3, 3, 5, 5, 3, 0, 1, 2, 1, 2,
	2, 1, 2, 3, 2, 3, 2, 2, 1, 3,
	1, 1, 3, 0, 5, 5, 5, 1, 3, 0,
	2, 1, 3, 3, 2, 3, 3, 1, 1, 3,
	3, 4, 3, 4, 5, 6, 3, 2, 6, 1,
	2, 1, 2, 1, 2, 1, 1, 1, 1, 1,
	1, 1, 3, 1, 1, 3, 1, 3, 1, 1,
	1, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 2, 2, 2, 3, 4, 5, 4, 1, 1,
	5, 0, 1, 1, 2, 4, 0, 2, 1, 3,
	1, 1, 1, 1, 0, 3, 0, 2, 0, 3,
	1, 3, 2, 0, 1, 1, 0, 2, 4, 0,
	2, 4, 0, 3, 1, 3, 0, 5, 2, 1,
	1, 3, 3, 1, 1, 3, 3, 0, 2, 0,
	3, 0, 1, 1, 1, 1, 1, 1, 0, 1,
	0, 1, 0, 2, 1, 1, 1, 1, 0,
}
var yyChk = [...]int{

	-1000, -1, -2, -3, -4, -5, -6, -7, -8, -9,
	-10, -11, -12, -13, 9, 10, 11, 12, 31, 91,
	92, 94, 93, 95, 104, 105, 106, -16, 5, 6,
	7, 8, -14, -77, -14, -14, -14, -14, 96, -70,
	98, 102, -65, 98, 100, 96, 96, 97, 98, 96,
	-76, -76, -76, -3, 21, -17, 22, -15, -65, -29,
	-74, 50, 13, -63, -64, -46, -72, -74, 50, -67,
	101, 97, -72, 50, 96, -72, -74, -66, 101, 50,
	-66, -74, -18, -19, 83, -20, -74, -33, -38, -34,
	61, -78, -37, -46, -43, 81, 82, 87, -72, -44,
	-47, 57, 58, 24, 33, 47, 51, 52, 53, 56,
	-45, 101, 63, 55, 27, 31, 89, -29, 48, 67,
	89, -74, 61, 50, -76, -74, -76, 99, -74, 24,
	46, -72, 13, 48, -73, -72, 23, 89, 60, 59,
	74, -35, 76, 61, 75, 62, 74, 78, 77, 86,
	81, 82, 83, 84, 85, 79, 80, 67, 68, 69,
	70, 71, 72, 73, -33, -38, -33, -3, -41, -38,
	-38, -78, -38, -38, -78, -78, -45, -78, -78, -50,
	-38, -29, -63, -74, -32, 14, -64, -38, -72, -76,
	24, -71, 103, -68, 94, 92, 30, 93, 17, 50,
	-74, -74, -76, -21, -22, -23, -24, -28, -78, -74,
	-45, -19, -72, 83, -33, -33, -39, 56, 61, 57,
	58, -38, -40, -78, -45, 54, 76, 75, 62, -38,
	-38, -39, -38, -38, -38, -38, -38, -38, -38, -38,
	-38, -38, -79, 49, -79, 48, -79, -38, -79, -18,
	22, -18, -37, -48, -49, 64, -60, 31, -78, -32,
	-54, 17, -33, 46, -72, -76, -69, 99, -32, 48,
	-25, -26, -27, 36, 40, 42, 37, 38, 39, 43,
	-75, -74, 23, -21, 89, 56, 57, 58, -41, -40,
	-38, -38, 60, -38, -79, -18, -79, 48, -51, -49,
	66, -33, -36, 34, -3, -63, -61, -46, -54, -58,
	19, 18, -74, -74, -52, 15, -22, -23, -22, -23,
	36, 36, 36, 41, 36, 41, 36, -26, -30, 44,
	100, 45, -74, -79, -74, -79, 60, -38, -79, -37,
	90, -38, 65, -62, 46, -42, -43, -62, -79, 48,
	-58, -38, -55, -56, -38, -76, -53, 16, 18, 46,
	46, 36, 36, 97, 97, 97, -38, -79, -38, 28,
	48, -46, 48, 48, -57, 25, 26, -54, -33, -41,
	-33, -33, -78, -78, -78, 29, -43, -38, -56, -58,
	-31, -72, -31, -31, 11, -59, 20, 32, -79, 48,
	-79, -79, -63, 11, 76, -72, -72, -72,
}
var yyDef = [...]int{

	0, -2, 1, 2, 3, 4, 5, 6, 7, 8,
	9, 10, 11, 12, 34, 34, 34, 34, 34, 210,
	201, 0, 0, 0, 218, 218, 218, 0, 38, 40,
	41, 42, 43, 36, 201, 0, 0, 0, 199, 0,
	0, 211, 0, 0, 202, 0, 197, 0, 197, 0,
	31, 32, 33, 14, 39, 0, 44, 35, 0, 0,
	81, 215, 0, 19, 194, 0, 158, 0, -2, 0,
	0, 0, 218, 214, 0, 218, 0, 0, 0, 0,
	0, 30, 0, 45, 47, 52, 0, 50, 51, 91,
	0, 0, 128, 129, 130, 0, 0, 0, 158, 0,
	148, 97, 98, 0, 0, 216, 160, 161, 162, 163,
	193, 149, 151, 37, 0, 0, 0, 89, 0, 0,
	0, 218, 0, 212, 22, 0, 25, 0, 27, 198,
	0, 218, 0, 0, 48, 53, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 115, 116, 117,
	118, 119, 120, 121, 94, 0, 0, 0, 0, 126,
	141, 0, 142, 143, 0, 0, 107, 0, 0, 0,
	152, 182, 89, 82, 168, 0, 195, 196, 159, 20,
	200, 0, 0, 218, 208, 203, 204, 205, 206, 207,
	26, 28, 29, 89, 55, 57, 58, 65, 0, 78,
	80, 46, 54, 49, 92, 93, 96, 109, 0, 111,
	113, 99, 100, 0, 123, 124, 0, 0, 0, 102,
	0, 106, 131, 132, 133, 134, 135, 136, 137, 138,
	139, 140, 95, 217, 125, 0, 192, 126, 144, 0,
	0, 0, 0, 156, 153, 0, 0, 0, 0, 168,
	176, 0, 90, 0, 213, 23, 0, 209, 164, 0,
	0, 0, 0, 68, 0, 0, 71, 0, 0, 0,
	83, 66, 0, 0, 0, 110, 112, 114, 0, 101,
	103, 0, 0, 127, 145, 0, 147, 0, 0, 154,
	0, 0, 186, 0, 189, 186, 0, 184, 176, 18,
	0, 0, 218, 24, 166, 0, 56, 61, 0, 64,
	69, 70, 72, 0, 74, 0, 76, 77, 59, 0,
	0, 0, 67, 60, 79, 122, 0, 104, 146, 0,
	150, 157, 0, 15, 0, 188, 190, 16, 183, 0,
	17, 177, 169, 170, 173, 21, 168, 0, 0, 0,
	0, 73, 75, 0, 0, 0, 105, 108, 155, 0,
	0, 185, 0, 0, 172, 174, 175, 176, 167, 165,
	62, 63, 0, 0, 0, 0, 191, 178, 171, 179,
	0, 87, 0, 0, 0, 13, 0, 0, 84, 0,
	85, 86, 187, 180, 0, 88, 0, 181,
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
	case 59:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:415
		{
			yyVAL.tableExpr = &AliasedTableExpr{Expr: yyDollar[1].smTableExpr, As: yyDollar[2].sqlID, Hints: yyDollar[3].indexHints}
		}
	case 60:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:419
		{
			yyVAL.tableExpr = &ParenTableExpr{Exprs: yyDollar[2].tableExprs}
		}
	case 61:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:432
		{
			yyVAL.tableExpr = &JoinTableExpr{LeftExpr: yyDollar[1].tableExpr, Join: yyDollar[2].str, RightExpr: yyDollar[3].tableExpr}
		}
	case 62:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:436
		{
			yyVAL.tableExpr = &JoinTableExpr{LeftExpr: yyDollar[1].tableExpr, Join: yyDollar[2].str, RightExpr: yyDollar[3].tableExpr, On: yyDollar[5].boolExpr}
		}
	case 63:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:440
		{
			yyVAL.tableExpr = &JoinTableExpr{LeftExpr: yyDollar[1].tableExpr, Join: yyDollar[2].str, RightExpr: yyDollar[3].tableExpr, On: yyDollar[5].boolExpr}
		}
	case 64:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:444
		{
			yyVAL.tableExpr = &JoinTableExpr{LeftExpr: yyDollar[1].tableExpr, Join: yyDollar[2].str, RightExpr: yyDollar[3].tableExpr}
		}
	case 65:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:449
		{
			yyVAL.sqlID = ""
		}
	case 66:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:453
		{
			yyVAL.sqlID = yyDollar[1].sqlID
		}
	case 67:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:457
		{
			yyVAL.sqlID = yyDollar[2].sqlID
		}
	case 68:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:463
		{
			yyVAL.str = JoinStr
		}
	case 69:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:467
		{
			yyVAL.str = JoinStr
		}
	case 70:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:471
		{
			yyVAL.str = JoinStr
		}
	case 71:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:475
		{
			yyVAL.str = StraightJoinStr
		}
	case 72:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:481
		{
			yyVAL.str = LeftJoinStr
		}
	case 73:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:485
		{
			yyVAL.str = LeftJoinStr
		}
	case 74:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:489
		{
			yyVAL.str = RightJoinStr
		}
	case 75:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:493
		{
			yyVAL.str = RightJoinStr
		}
	case 76:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:499
		{
			yyVAL.str = NaturalJoinStr
		}
	case 77:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:503
		{
			if yyDollar[2].str == LeftJoinStr {
				yyVAL.str = NaturalLeftJoinStr
			} else {
				yyVAL.str = NaturalRightJoinStr
			}
		}
	case 78:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:513
		{
			yyVAL.smTableExpr = &TableName{Name: yyDollar[1].sqlID}
		}
	case 79:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:517
		{
			yyVAL.smTableExpr = &TableName{Qualifier: yyDollar[1].sqlID, Name: yyDollar[3].sqlID}
		}
	case 80:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:521
		{
			yyVAL.smTableExpr = yyDollar[1].subquery
		}
	case 81:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:527
		{
			yyVAL.tableName = &TableName{Name: yyDollar[1].sqlID}
		}
	case 82:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:531
		{
			yyVAL.tableName = &TableName{Qualifier: yyDollar[1].sqlID, Name: yyDollar[3].sqlID}
		}
	case 83:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:536
		{
			yyVAL.indexHints = nil
		}
	case 84:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:540
		{
			yyVAL.indexHints = &IndexHints{Type: UseStr, Indexes: yyDollar[4].sqlIDs}
		}
	case 85:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:544
		{
			yyVAL.indexHints = &IndexHints{Type: IgnoreStr, Indexes: yyDollar[4].sqlIDs}
		}
	case 86:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:548
		{
			yyVAL.indexHints = &IndexHints{Type: ForceStr, Indexes: yyDollar[4].sqlIDs}
		}
	case 87:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:554
		{
			yyVAL.sqlIDs = []SQLName{yyDollar[1].sqlID}
		}
	case 88:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:558
		{
			yyVAL.sqlIDs = append(yyDollar[1].sqlIDs, yyDollar[3].sqlID)
		}
	case 89:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:563
		{
			yyVAL.boolExpr = nil
		}
	case 90:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:567
		{
			yyVAL.boolExpr = yyDollar[2].boolExpr
		}
	case 92:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:574
		{
			yyVAL.boolExpr = &AndExpr{Left: yyDollar[1].boolExpr, Right: yyDollar[3].boolExpr}
		}
	case 93:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:578
		{
			yyVAL.boolExpr = &OrExpr{Left: yyDollar[1].boolExpr, Right: yyDollar[3].boolExpr}
		}
	case 94:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:582
		{
			yyVAL.boolExpr = &NotExpr{Expr: yyDollar[2].boolExpr}
		}
	case 95:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:586
		{
			yyVAL.boolExpr = &ParenBoolExpr{Expr: yyDollar[2].boolExpr}
		}
	case 96:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:590
		{
			yyVAL.boolExpr = &IsExpr{Operator: yyDollar[3].str, Expr: yyDollar[1].boolExpr}
		}
	case 97:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:596
		{
			yyVAL.boolExpr = BoolVal(true)
		}
	case 98:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:600
		{
			yyVAL.boolExpr = BoolVal(false)
		}
	case 99:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:604
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: yyDollar[2].str, Right: yyDollar[3].valExpr}
		}
	case 100:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:608
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: InStr, Right: yyDollar[3].colTuple}
		}
	case 101:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:612
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: NotInStr, Right: yyDollar[4].colTuple}
		}
	case 102:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:616
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: LikeStr, Right: yyDollar[3].valExpr}
		}
	case 103:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:620
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: NotLikeStr, Right: yyDollar[4].valExpr}
		}
	case 104:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:624
		{
			yyVAL.boolExpr = &RangeCond{Left: yyDollar[1].valExpr, Operator: BetweenStr, From: yyDollar[3].valExpr, To: yyDollar[5].valExpr}
		}
	case 105:
		yyDollar = yyS[yypt-6 : yypt+1]
		//line sql.y:628
		{
			yyVAL.boolExpr = &RangeCond{Left: yyDollar[1].valExpr, Operator: NotBetweenStr, From: yyDollar[4].valExpr, To: yyDollar[6].valExpr}
		}
	case 106:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:632
		{
			yyVAL.boolExpr = &IsExpr{Operator: yyDollar[3].str, Expr: yyDollar[1].valExpr}
		}
	case 107:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:636
		{
			yyVAL.boolExpr = &ExistsExpr{Subquery: yyDollar[2].subquery}
		}
	case 108:
		yyDollar = yyS[yypt-6 : yypt+1]
		//line sql.y:640
		{
			yyVAL.boolExpr = &KeyrangeExpr{Start: yyDollar[3].valExpr, End: yyDollar[5].valExpr}
		}
	case 109:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:646
		{
			yyVAL.str = IsNullStr
		}
	case 110:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:650
		{
			yyVAL.str = IsNotNullStr
		}
	case 111:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:654
		{
			yyVAL.str = IsTrueStr
		}
	case 112:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:658
		{
			yyVAL.str = IsNotTrueStr
		}
	case 113:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:662
		{
			yyVAL.str = IsFalseStr
		}
	case 114:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:666
		{
			yyVAL.str = IsNotFalseStr
		}
	case 115:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:672
		{
			yyVAL.str = EqualStr
		}
	case 116:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:676
		{
			yyVAL.str = LessThanStr
		}
	case 117:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:680
		{
			yyVAL.str = GreaterThanStr
		}
	case 118:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:684
		{
			yyVAL.str = LessEqualStr
		}
	case 119:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:688
		{
			yyVAL.str = GreaterEqualStr
		}
	case 120:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:692
		{
			yyVAL.str = NotEqualStr
		}
	case 121:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:696
		{
			yyVAL.str = NullSafeEqualStr
		}
	case 122:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:702
		{
			yyVAL.colTuple = ValTuple(yyDollar[2].valExprs)
		}
	case 123:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:706
		{
			yyVAL.colTuple = yyDollar[1].subquery
		}
	case 124:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:710
		{
			yyVAL.colTuple = ListArg(yyDollar[1].bytes)
		}
	case 125:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:716
		{
			yyVAL.subquery = &Subquery{yyDollar[2].selStmt}
		}
	case 126:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:722
		{
			yyVAL.valExprs = ValExprs{yyDollar[1].valExpr}
		}
	case 127:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:726
		{
			yyVAL.valExprs = append(yyDollar[1].valExprs, yyDollar[3].valExpr)
		}
	case 128:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:732
		{
			yyVAL.valExpr = yyDollar[1].valExpr
		}
	case 129:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:736
		{
			yyVAL.valExpr = yyDollar[1].colName
		}
	case 130:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:740
		{
			yyVAL.valExpr = yyDollar[1].rowTuple
		}
	case 131:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:744
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: BitAndStr, Right: yyDollar[3].valExpr}
		}
	case 132:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:748
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: BitOrStr, Right: yyDollar[3].valExpr}
		}
	case 133:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:752
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: BitXorStr, Right: yyDollar[3].valExpr}
		}
	case 134:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:756
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: PlusStr, Right: yyDollar[3].valExpr}
		}
	case 135:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:760
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: MinusStr, Right: yyDollar[3].valExpr}
		}
	case 136:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:764
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: MultStr, Right: yyDollar[3].valExpr}
		}
	case 137:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:768
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: DivStr, Right: yyDollar[3].valExpr}
		}
	case 138:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:772
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: ModStr, Right: yyDollar[3].valExpr}
		}
	case 139:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:776
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: ShiftLeftStr, Right: yyDollar[3].valExpr}
		}
	case 140:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:780
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: ShiftRightStr, Right: yyDollar[3].valExpr}
		}
	case 141:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:784
		{
			if num, ok := yyDollar[2].valExpr.(NumVal); ok {
				yyVAL.valExpr = num
			} else {
				yyVAL.valExpr = &UnaryExpr{Operator: UPlusStr, Expr: yyDollar[2].valExpr}
			}
		}
	case 142:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:792
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
	case 143:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:805
		{
			yyVAL.valExpr = &UnaryExpr{Operator: TildaStr, Expr: yyDollar[2].valExpr}
		}
	case 144:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:809
		{
			yyVAL.valExpr = &FuncExpr{Name: string(yyDollar[1].sqlID)}
		}
	case 145:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:813
		{
			yyVAL.valExpr = &FuncExpr{Name: string(yyDollar[1].sqlID), Exprs: yyDollar[3].selectExprs}
		}
	case 146:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:817
		{
			yyVAL.valExpr = &FuncExpr{Name: string(yyDollar[1].sqlID), Distinct: true, Exprs: yyDollar[4].selectExprs}
		}
	case 147:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:821
		{
			yyVAL.valExpr = &FuncExpr{Name: yyDollar[1].str, Exprs: yyDollar[3].selectExprs}
		}
	case 148:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:825
		{
			yyVAL.valExpr = yyDollar[1].caseExpr
		}
	case 149:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:831
		{
			yyVAL.str = "if"
		}
	case 150:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:837
		{
			yyVAL.caseExpr = &CaseExpr{Expr: yyDollar[2].valExpr, Whens: yyDollar[3].whens, Else: yyDollar[4].valExpr}
		}
	case 151:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:842
		{
			yyVAL.valExpr = nil
		}
	case 152:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:846
		{
			yyVAL.valExpr = yyDollar[1].valExpr
		}
	case 153:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:852
		{
			yyVAL.whens = []*When{yyDollar[1].when}
		}
	case 154:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:856
		{
			yyVAL.whens = append(yyDollar[1].whens, yyDollar[2].when)
		}
	case 155:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:862
		{
			yyVAL.when = &When{Cond: yyDollar[2].boolExpr, Val: yyDollar[4].valExpr}
		}
	case 156:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:867
		{
			yyVAL.valExpr = nil
		}
	case 157:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:871
		{
			yyVAL.valExpr = yyDollar[2].valExpr
		}
	case 158:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:877
		{
			yyVAL.colName = &ColName{Name: yyDollar[1].sqlID}
		}
	case 159:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:881
		{
			yyVAL.colName = &ColName{Qualifier: yyDollar[1].sqlID, Name: yyDollar[3].sqlID}
		}
	case 160:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:887
		{
			yyVAL.valExpr = StrVal(yyDollar[1].bytes)
		}
	case 161:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:891
		{
			yyVAL.valExpr = NumVal(yyDollar[1].bytes)
		}
	case 162:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:895
		{
			yyVAL.valExpr = ValArg(yyDollar[1].bytes)
		}
	case 163:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:899
		{
			yyVAL.valExpr = &NullVal{}
		}
	case 164:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:904
		{
			yyVAL.valExprs = nil
		}
	case 165:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:908
		{
			yyVAL.valExprs = yyDollar[3].valExprs
		}
	case 166:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:913
		{
			yyVAL.boolExpr = nil
		}
	case 167:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:917
		{
			yyVAL.boolExpr = yyDollar[2].boolExpr
		}
	case 168:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:922
		{
			yyVAL.orderBy = nil
		}
	case 169:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:926
		{
			yyVAL.orderBy = yyDollar[3].orderBy
		}
	case 170:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:932
		{
			yyVAL.orderBy = OrderBy{yyDollar[1].order}
		}
	case 171:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:936
		{
			yyVAL.orderBy = append(yyDollar[1].orderBy, yyDollar[3].order)
		}
	case 172:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:942
		{
			yyVAL.order = &Order{Expr: yyDollar[1].valExpr, Direction: yyDollar[2].str}
		}
	case 173:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:947
		{
			yyVAL.str = AscScr
		}
	case 174:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:951
		{
			yyVAL.str = AscScr
		}
	case 175:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:955
		{
			yyVAL.str = DescScr
		}
	case 176:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:960
		{
			yyVAL.limit = nil
		}
	case 177:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:964
		{
			yyVAL.limit = &Limit{Rowcount: yyDollar[2].valExpr}
		}
	case 178:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:968
		{
			yyVAL.limit = &Limit{Offset: yyDollar[2].valExpr, Rowcount: yyDollar[4].valExpr}
		}
	case 179:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:973
		{
			yyVAL.str = ""
		}
	case 180:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:977
		{
			yyVAL.str = ForUpdateStr
		}
	case 181:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:981
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
	case 182:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:994
		{
			yyVAL.columns = nil
		}
	case 183:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:998
		{
			yyVAL.columns = yyDollar[2].columns
		}
	case 184:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1004
		{
			yyVAL.columns = Columns{&NonStarExpr{Expr: yyDollar[1].colName}}
		}
	case 185:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1008
		{
			yyVAL.columns = append(yyVAL.columns, &NonStarExpr{Expr: yyDollar[3].colName})
		}
	case 186:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1013
		{
			yyVAL.updateExprs = nil
		}
	case 187:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:1017
		{
			yyVAL.updateExprs = yyDollar[5].updateExprs
		}
	case 188:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:1023
		{
			yyVAL.insRows = yyDollar[2].values
		}
	case 189:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1027
		{
			yyVAL.insRows = yyDollar[1].selStmt
		}
	case 190:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1033
		{
			yyVAL.values = Values{yyDollar[1].rowTuple}
		}
	case 191:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1037
		{
			yyVAL.values = append(yyDollar[1].values, yyDollar[3].rowTuple)
		}
	case 192:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1043
		{
			yyVAL.rowTuple = ValTuple(yyDollar[2].valExprs)
		}
	case 193:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1047
		{
			yyVAL.rowTuple = yyDollar[1].subquery
		}
	case 194:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1053
		{
			yyVAL.updateExprs = UpdateExprs{yyDollar[1].updateExpr}
		}
	case 195:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1057
		{
			yyVAL.updateExprs = append(yyDollar[1].updateExprs, yyDollar[3].updateExpr)
		}
	case 196:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1063
		{
			yyVAL.updateExpr = &UpdateExpr{Name: yyDollar[1].colName, Expr: yyDollar[3].valExpr}
		}
	case 197:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1068
		{
			yyVAL.empty = struct{}{}
		}
	case 198:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:1070
		{
			yyVAL.empty = struct{}{}
		}
	case 199:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1073
		{
			yyVAL.empty = struct{}{}
		}
	case 200:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1075
		{
			yyVAL.empty = struct{}{}
		}
	case 201:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1078
		{
			yyVAL.str = ""
		}
	case 202:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1080
		{
			yyVAL.str = IgnoreStr
		}
	case 203:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1084
		{
			yyVAL.empty = struct{}{}
		}
	case 204:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1086
		{
			yyVAL.empty = struct{}{}
		}
	case 205:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1088
		{
			yyVAL.empty = struct{}{}
		}
	case 206:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1090
		{
			yyVAL.empty = struct{}{}
		}
	case 207:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1092
		{
			yyVAL.empty = struct{}{}
		}
	case 208:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1095
		{
			yyVAL.empty = struct{}{}
		}
	case 209:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1097
		{
			yyVAL.empty = struct{}{}
		}
	case 210:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1100
		{
			yyVAL.empty = struct{}{}
		}
	case 211:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1102
		{
			yyVAL.empty = struct{}{}
		}
	case 212:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1105
		{
			yyVAL.empty = struct{}{}
		}
	case 213:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:1107
		{
			yyVAL.empty = struct{}{}
		}
	case 214:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1111
		{
			yyVAL.sqlID = SQLName(strings.ToLower(string(yyDollar[1].bytes)))
		}
	case 215:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1117
		{
			yyVAL.sqlID = SQLName(yyDollar[1].bytes)
		}
	case 216:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1123
		{
			if incNesting(yylex) {
				yylex.Error("max nesting level reached")
				return 1
			}
		}
	case 217:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1132
		{
			decNesting(yylex)
		}
	case 218:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1137
		{
			forceEOF(yylex)
		}
	}
	goto yystack /* stack new state and value */
}
