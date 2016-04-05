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
	92, 220,
	-2, 219,
}

const yyNprod = 224
const yyPrivate = 57344

var yyTokenNames []string
var yyStates []string

const yyLast = 785

var yyAct = [...]int{

	100, 319, 167, 399, 94, 170, 363, 95, 64, 268,
	353, 280, 211, 209, 210, 249, 227, 169, 3, 221,
	261, 198, 189, 65, 83, 213, 79, 106, 84, 71,
	69, 107, 108, 109, 340, 342, 110, 44, 67, 50,
	276, 73, 66, 112, 76, 53, 38, 43, 40, 44,
	128, 375, 41, 374, 88, 373, 204, 113, 89, 72,
	75, 60, 96, 97, 111, 51, 52, 49, 98, 202,
	99, 45, 173, 46, 47, 48, 350, 271, 138, 121,
	117, 151, 132, 101, 69, 347, 413, 136, 141, 118,
	205, 262, 341, 310, 262, 171, 120, 123, 114, 172,
	174, 175, 176, 150, 149, 157, 158, 152, 153, 154,
	155, 156, 151, 125, 291, 182, 127, 67, 218, 139,
	67, 66, 193, 192, 66, 187, 74, 69, 92, 154,
	155, 156, 151, 141, 201, 203, 200, 89, 217, 193,
	62, 186, 62, 191, 106, 226, 166, 168, 235, 236,
	237, 230, 239, 240, 241, 242, 243, 244, 245, 246,
	247, 248, 195, 216, 92, 92, 408, 250, 238, 234,
	179, 124, 208, 177, 178, 80, 254, 255, 180, 78,
	89, 89, 232, 233, 231, 251, 253, 381, 14, 106,
	140, 139, 62, 256, 219, 220, 352, 119, 214, 273,
	140, 139, 257, 259, 252, 141, 215, 92, 229, 263,
	267, 382, 92, 92, 137, 141, 228, 150, 149, 157,
	158, 152, 153, 154, 155, 156, 151, 106, 81, 294,
	62, 254, 277, 250, 298, 300, 301, 302, 379, 274,
	106, 369, 74, 140, 139, 270, 359, 250, 299, 354,
	92, 92, 222, 224, 225, 304, 190, 223, 141, 266,
	354, 89, 119, 92, 134, 250, 67, 67, 252, 250,
	66, 317, 322, 305, 315, 307, 190, 318, 278, 250,
	214, 309, 314, 306, 295, 296, 297, 272, 215, 133,
	265, 278, 328, 327, 330, 329, 229, 131, 335, 57,
	338, 372, 371, 336, 228, 332, 348, 106, 331, 116,
	345, 119, 56, 351, 346, 394, 337, 311, 286, 287,
	360, 356, 349, 361, 364, 134, 357, 28, 29, 30,
	31, 92, 358, 42, 14, 92, 152, 153, 154, 155,
	156, 151, 333, 214, 214, 214, 214, 334, 378, 405,
	376, 215, 215, 215, 215, 377, 196, 115, 313, 130,
	67, 406, 293, 365, 380, 54, 185, 320, 59, 326,
	368, 254, 250, 184, 388, 321, 386, 269, 190, 367,
	63, 412, 403, 14, 396, 364, 33, 395, 398, 397,
	1, 292, 400, 400, 400, 68, 401, 402, 289, 282,
	285, 286, 287, 283, 67, 284, 288, 135, 66, 414,
	32, 197, 411, 39, 415, 407, 416, 409, 410, 275,
	199, 70, 387, 183, 389, 390, 34, 35, 36, 37,
	316, 61, 157, 158, 152, 153, 154, 155, 156, 151,
	92, 77, 92, 92, 264, 82, 391, 392, 393, 404,
	383, 87, 362, 258, 366, 105, 325, 308, 181, 61,
	28, 29, 30, 31, 260, 102, 122, 355, 93, 312,
	142, 126, 74, 90, 129, 339, 281, 279, 212, 106,
	86, 250, 69, 107, 108, 109, 384, 385, 110, 103,
	104, 55, 27, 91, 58, 112, 14, 15, 16, 17,
	150, 149, 157, 158, 152, 153, 154, 155, 156, 151,
	13, 61, 12, 188, 96, 97, 85, 194, 18, 11,
	98, 10, 99, 9, 206, 8, 7, 207, 6, 61,
	87, 5, 105, 4, 194, 101, 2, 0, 0, 0,
	150, 149, 157, 158, 152, 153, 154, 155, 156, 151,
	0, 0, 0, 0, 0, 0, 106, 0, 0, 69,
	107, 108, 109, 0, 0, 110, 103, 104, 0, 0,
	91, 14, 112, 87, 87, 0, 0, 0, 0, 61,
	0, 19, 20, 22, 21, 23, 105, 0, 0, 0,
	0, 96, 97, 85, 24, 25, 26, 98, 0, 99,
	149, 157, 158, 152, 153, 154, 155, 156, 151, 290,
	106, 61, 101, 69, 107, 108, 109, 303, 0, 110,
	103, 104, 0, 0, 91, 0, 112, 0, 0, 0,
	0, 0, 0, 0, 0, 150, 149, 157, 158, 152,
	153, 154, 155, 156, 151, 96, 97, 0, 0, 0,
	0, 98, 0, 99, 87, 0, 105, 0, 14, 0,
	0, 0, 0, 0, 0, 0, 101, 0, 323, 0,
	0, 324, 0, 0, 61, 61, 61, 61, 0, 0,
	106, 0, 0, 69, 107, 108, 109, 343, 344, 110,
	103, 104, 0, 0, 91, 0, 112, 106, 0, 0,
	69, 107, 108, 109, 0, 0, 110, 0, 0, 0,
	0, 0, 0, 112, 0, 96, 97, 0, 0, 0,
	0, 98, 0, 99, 282, 285, 286, 287, 283, 0,
	284, 288, 96, 97, 370, 0, 101, 0, 98, 0,
	99, 0, 0, 0, 0, 0, 0, 0, 144, 147,
	0, 0, 0, 101, 159, 160, 161, 162, 163, 164,
	165, 148, 145, 146, 143, 150, 149, 157, 158, 152,
	153, 154, 155, 156, 151, 150, 149, 157, 158, 152,
	153, 154, 155, 156, 151,
}
var yyPact = [...]int{

	487, -1000, -1000, 455, -1000, -1000, -1000, -1000, -1000, -1000,
	-1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, -53,
	-54, -28, -26, -32, -1000, -1000, -1000, 374, 344, -1000,
	-1000, -1000, 277, -1000, -66, 89, 367, 76, -75, -41,
	75, -1000, -39, 75, -1000, 89, -78, 124, -78, 89,
	-1000, -1000, -1000, -1000, -1000, 508, 75, -1000, 42, 330,
	278, -12, -1000, 89, 148, -1000, 28, -1000, -13, -1000,
	89, 35, 120, -1000, -1000, 89, -1000, -52, 89, 335,
	250, 75, -1000, 276, -1000, -1000, 191, -14, 140, 686,
	-1000, 632, 562, -1000, -1000, -1000, -21, -21, -21, -21,
	192, 192, -1000, -1000, -1000, 192, -1000, -1000, -1000, -1000,
	-1000, -1000, -21, 353, -1000, 89, 76, 89, 364, 76,
	-21, 76, -1000, 332, -85, -1000, 39, -1000, 89, -1000,
	-1000, 89, -1000, 141, 508, -1000, -1000, 75, 33, 632,
	632, 195, -21, 96, 106, -21, -21, -21, 195, -21,
	-21, -21, -21, -21, -21, -21, -21, -21, -21, -1000,
	-1000, -1000, -1000, -1000, -1000, -1000, 13, 686, 183, 322,
	219, 686, -1000, 649, -1000, -1000, 421, 431, 508, -1000,
	374, 29, 696, 89, -1000, -1000, 259, 262, -1000, 360,
	632, -1000, 696, -1000, -15, -1000, -1000, 240, 75, -1000,
	-62, -1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, 242,
	362, -1000, -1000, 91, 339, 179, -1000, -1000, -1000, 13,
	58, -1000, -1000, 227, -1000, -1000, 696, -1000, 649, -1000,
	-1000, 96, -21, -21, -21, 696, 696, 556, -1000, 351,
	520, -1000, 44, 44, -7, -7, -7, 253, 253, -1000,
	-1000, -1000, -21, -1000, 696, -1000, -1000, 215, 508, 215,
	26, -1000, 632, -1000, 325, 76, 76, 360, 348, 357,
	140, 75, 89, -1000, -1000, 89, -1000, 354, 141, 141,
	141, 141, -1000, 271, 268, -1000, 305, 261, 279, -11,
	-1000, 89, 89, -1000, 229, -1000, -1000, -1000, 219, -1000,
	696, 696, 24, -21, 696, -1000, 215, -1000, -17, -1000,
	-21, 130, 202, 192, 455, 213, 197, -1000, 348, -1000,
	-21, -21, -1000, -1000, -1000, 363, 352, 362, 194, 687,
	-1000, -1000, -1000, -1000, 265, -1000, 264, -1000, -1000, -1000,
	-45, -47, -49, -1000, -1000, -1000, -1000, -21, 696, -1000,
	-1000, 696, -21, -1000, 320, 189, -1000, -1000, -1000, 76,
	-1000, 138, 162, -1000, 461, -1000, 360, 632, -21, 632,
	632, -1000, -1000, 192, 192, 192, 696, 696, 286, 192,
	-1000, -21, -21, -1000, -1000, -1000, 348, 140, 155, 140,
	140, 75, 75, 75, 371, -1000, 696, -1000, 329, 117,
	-1000, 117, 117, 76, -1000, 370, 8, -1000, 75, -1000,
	-1000, 148, -1000, 75, -1000, 75, -1000,
}
var yyPgo = [...]int{

	0, 536, 17, 533, 531, 528, 526, 525, 523, 521,
	519, 512, 510, 410, 494, 492, 491, 24, 28, 480,
	13, 14, 12, 478, 477, 11, 476, 25, 475, 3,
	22, 54, 473, 470, 469, 468, 2, 19, 16, 5,
	467, 7, 64, 4, 465, 464, 20, 458, 457, 456,
	454, 9, 452, 6, 450, 1, 449, 444, 430, 10,
	8, 23, 423, 333, 179, 421, 420, 419, 413, 411,
	0, 407, 395, 398, 391, 39, 390, 386, 72, 15,
}
var yyR1 = [...]int{

	0, 76, 1, 1, 1, 1, 1, 1, 1, 1,
	1, 1, 1, 2, 2, 2, 3, 3, 4, 5,
	6, 7, 7, 7, 8, 8, 8, 9, 10, 10,
	10, 11, 12, 12, 12, 77, 13, 14, 14, 15,
	15, 15, 15, 15, 16, 16, 17, 17, 18, 18,
	18, 19, 19, 71, 71, 71, 20, 20, 21, 21,
	22, 22, 22, 23, 23, 23, 23, 74, 74, 73,
	73, 73, 24, 24, 24, 24, 25, 25, 25, 25,
	26, 26, 27, 27, 28, 28, 28, 28, 29, 29,
	30, 30, 31, 31, 31, 31, 31, 31, 32, 32,
	32, 32, 32, 32, 32, 32, 32, 32, 32, 32,
	32, 37, 37, 37, 37, 37, 37, 33, 33, 33,
	33, 33, 33, 33, 38, 38, 38, 42, 39, 39,
	36, 36, 36, 36, 36, 36, 36, 36, 36, 36,
	36, 36, 36, 36, 36, 36, 36, 36, 36, 36,
	36, 36, 44, 47, 47, 45, 45, 46, 48, 48,
	43, 43, 43, 35, 35, 35, 35, 49, 49, 50,
	50, 51, 51, 52, 52, 53, 54, 54, 54, 55,
	55, 55, 56, 56, 56, 57, 57, 58, 58, 59,
	59, 34, 34, 40, 40, 41, 41, 60, 60, 61,
	62, 62, 64, 64, 65, 65, 63, 63, 66, 66,
	66, 66, 66, 67, 67, 68, 68, 69, 69, 70,
	72, 78, 79, 75,
}
var yyR2 = [...]int{

	0, 1, 1, 1, 1, 1, 1, 1, 1, 1,
	1, 1, 1, 12, 6, 3, 8, 8, 8, 7,
	3, 5, 8, 4, 6, 7, 4, 5, 4, 5,
	5, 3, 2, 2, 2, 0, 2, 0, 2, 1,
	2, 1, 1, 1, 0, 1, 1, 3, 1, 2,
	3, 1, 1, 0, 1, 2, 1, 3, 1, 1,
	3, 3, 3, 3, 5, 5, 3, 0, 1, 0,
	1, 2, 1, 2, 2, 1, 2, 3, 2, 3,
	2, 2, 1, 3, 0, 5, 5, 5, 1, 3,
	0, 2, 1, 3, 3, 2, 3, 3, 1, 1,
	3, 3, 4, 3, 4, 3, 4, 5, 6, 3,
	2, 1, 2, 1, 2, 1, 2, 1, 1, 1,
	1, 1, 1, 1, 3, 1, 1, 3, 1, 3,
	1, 1, 1, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 2, 2, 2, 3, 3, 4, 5,
	4, 1, 5, 0, 1, 1, 2, 4, 0, 2,
	1, 3, 5, 1, 1, 1, 1, 0, 3, 0,
	2, 0, 3, 1, 3, 2, 0, 1, 1, 0,
	2, 4, 0, 2, 4, 0, 3, 1, 3, 0,
	5, 2, 1, 1, 3, 3, 1, 1, 3, 3,
	1, 1, 0, 2, 0, 3, 0, 1, 1, 1,
	1, 1, 1, 0, 1, 0, 1, 0, 2, 1,
	1, 1, 1, 0,
}
var yyChk = [...]int{

	-1000, -76, -1, -2, -3, -4, -5, -6, -7, -8,
	-9, -10, -11, -12, 9, 10, 11, 12, 31, 94,
	95, 97, 96, 98, 107, 108, 109, -15, 5, 6,
	7, 8, -13, -77, -13, -13, -13, -13, 99, -68,
	101, 105, -63, 101, 103, 99, 99, 100, 101, 99,
	-75, -75, -75, -2, 21, -16, 35, 22, -14, -63,
	-27, -72, 51, 13, -60, -61, -43, -70, -72, 51,
	-65, 104, 100, -70, 51, 99, -70, -72, -64, 104,
	51, -64, -72, -17, -18, 85, -19, -72, -31, -36,
	-32, 62, -78, -35, -43, -41, 83, 84, 89, 91,
	-70, 104, -44, 58, 59, 24, 48, 52, 53, 54,
	57, -42, 64, -70, 56, 27, 31, 92, -27, 49,
	68, 92, -72, 62, 51, -75, -72, -75, 102, -72,
	24, 47, -70, 13, 49, -71, -70, 23, 92, 61,
	60, 75, -33, 78, 62, 76, 77, 63, 75, 80,
	79, 88, 83, 84, 85, 86, 87, 81, 82, 68,
	69, 70, 71, 72, 73, 74, -31, -36, -31, -2,
	-39, -36, -36, -78, -36, -36, -36, -78, -78, -42,
	-78, -47, -36, -62, 20, 13, -27, -60, -72, -30,
	14, -61, -36, -70, -72, -75, 24, -69, 106, -66,
	97, 95, 30, 96, 17, 51, -72, -72, -75, -20,
	-21, -22, -23, -27, -42, -78, -18, -70, 85, -31,
	-31, -37, 57, 62, 58, 59, -36, -38, -78, -42,
	55, 78, 76, 77, 63, -36, -36, -36, -37, -36,
	-36, -36, -36, -36, -36, -36, -36, -36, -36, -79,
	50, -79, 49, -79, -36, -70, -79, -17, 22, -17,
	-45, -46, 65, -27, -57, 31, -78, -30, -51, 17,
	-31, 92, 47, -70, -75, -67, 102, -30, 49, -24,
	-25, -26, 37, 41, 43, 38, 39, 40, 44, -73,
	-72, 23, -74, 23, -20, 57, 58, 59, -39, -38,
	-36, -36, -36, 61, -36, -79, -17, -79, -48, -46,
	67, -31, -34, 33, -2, -60, -58, -43, -51, -55,
	19, 18, -70, -72, -72, -49, 15, -21, -22, -21,
	-22, 37, 37, 37, 42, 37, 42, 37, -25, -28,
	45, 103, 46, -72, -72, -79, -79, 61, -36, -79,
	93, -36, 66, -59, 47, -40, -41, -59, -79, 49,
	-55, -36, -52, -53, -36, -75, -50, 16, 18, 47,
	47, 37, 37, 100, 100, 100, -36, -36, 28, 49,
	-43, 49, 49, -54, 25, 26, -51, -31, -39, -31,
	-31, -78, -78, -78, 29, -41, -36, -53, -55, -29,
	-70, -29, -29, 11, -56, 20, 32, -79, 49, -79,
	-79, -60, 11, 78, -70, -70, -70,
}
var yyDef = [...]int{

	0, -2, 1, 2, 3, 4, 5, 6, 7, 8,
	9, 10, 11, 12, 35, 35, 35, 35, 35, 215,
	206, 0, 0, 0, 223, 223, 223, 0, 39, 41,
	42, 43, 44, 37, 206, 0, 0, 0, 204, 0,
	0, 216, 0, 0, 207, 0, 202, 0, 202, 0,
	32, 33, 34, 15, 40, 0, 0, 45, 36, 0,
	0, 82, 220, 0, 20, 197, 0, 160, 0, -2,
	0, 0, 0, 223, 219, 0, 223, 0, 0, 0,
	0, 0, 31, 0, 46, 48, 53, 0, 51, 52,
	92, 0, 0, 130, 131, 132, 0, 0, 0, 0,
	160, 0, 151, 98, 99, 0, 221, 163, 164, 165,
	166, 196, 153, 0, 38, 0, 0, 0, 90, 0,
	0, 0, 223, 0, 217, 23, 0, 26, 0, 28,
	203, 0, 223, 0, 0, 49, 54, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 117,
	118, 119, 120, 121, 122, 123, 95, 0, 0, 0,
	0, 128, 143, 0, 144, 145, 0, 0, 0, 110,
	0, 0, 154, 0, 200, 201, 185, 90, 83, 171,
	0, 198, 199, 161, 0, 21, 205, 0, 0, 223,
	213, 208, 209, 210, 211, 212, 27, 29, 30, 90,
	56, 58, 59, 69, 67, 0, 47, 55, 50, 93,
	94, 97, 111, 0, 113, 115, 100, 101, 0, 125,
	126, 0, 0, 0, 0, 103, 105, 0, 109, 133,
	134, 135, 136, 137, 138, 139, 140, 141, 142, 96,
	222, 127, 0, 195, 128, 146, 147, 0, 0, 0,
	158, 155, 0, 14, 0, 0, 0, 171, 179, 0,
	91, 0, 0, 218, 24, 0, 214, 167, 0, 0,
	0, 0, 72, 0, 0, 75, 0, 0, 0, 84,
	70, 0, 0, 68, 0, 112, 114, 116, 0, 102,
	104, 106, 0, 0, 129, 148, 0, 150, 0, 156,
	0, 0, 189, 0, 192, 189, 0, 187, 179, 19,
	0, 0, 162, 223, 25, 169, 0, 57, 63, 0,
	66, 73, 74, 76, 0, 78, 0, 80, 81, 60,
	0, 0, 0, 71, 61, 62, 124, 0, 107, 149,
	152, 159, 0, 16, 0, 191, 193, 17, 186, 0,
	18, 180, 172, 173, 176, 22, 171, 0, 0, 0,
	0, 77, 79, 0, 0, 0, 108, 157, 0, 0,
	188, 0, 0, 175, 177, 178, 179, 170, 168, 64,
	65, 0, 0, 0, 0, 194, 181, 174, 182, 0,
	88, 0, 0, 0, 13, 0, 0, 85, 0, 86,
	87, 190, 183, 0, 89, 0, 184,
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
		yyDollar = yyS[yypt-12 : yypt+1]
		//line sql.y:190
		{
			yyVAL.selStmt = &Select{Comments: Comments(yyDollar[2].bytes2), Distinct: yyDollar[3].str, SelectExprs: yyDollar[4].selectExprs, From: yyDollar[6].tableExprs, Where: NewWhere(WhereStr, yyDollar[7].boolExpr), GroupBy: GroupBy(yyDollar[8].valExprs), Having: NewWhere(HavingStr, yyDollar[9].boolExpr), OrderBy: yyDollar[10].orderBy, Limit: yyDollar[11].limit, Lock: yyDollar[12].str}
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
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:363
		{
			yyVAL.selectExprs = SelectExprs{yyDollar[1].selectExpr}
		}
	case 47:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:367
		{
			yyVAL.selectExprs = append(yyVAL.selectExprs, yyDollar[3].selectExpr)
		}
	case 48:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:373
		{
			yyVAL.selectExpr = &StarExpr{}
		}
	case 49:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:377
		{
			yyVAL.selectExpr = &NonStarExpr{Expr: yyDollar[1].expr, As: yyDollar[2].sqlID}
		}
	case 50:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:381
		{
			yyVAL.selectExpr = &StarExpr{TableName: yyDollar[1].sqlID}
		}
	case 51:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:387
		{
			yyVAL.expr = yyDollar[1].boolExpr
		}
	case 52:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:391
		{
			yyVAL.expr = yyDollar[1].valExpr
		}
	case 53:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:396
		{
			yyVAL.sqlID = ""
		}
	case 54:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:400
		{
			yyVAL.sqlID = yyDollar[1].sqlID
		}
	case 55:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:404
		{
			yyVAL.sqlID = yyDollar[2].sqlID
		}
	case 56:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:410
		{
			yyVAL.tableExprs = TableExprs{yyDollar[1].tableExpr}
		}
	case 57:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:414
		{
			yyVAL.tableExprs = append(yyVAL.tableExprs, yyDollar[3].tableExpr)
		}
	case 60:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:424
		{
			yyVAL.tableExpr = &AliasedTableExpr{Expr: yyDollar[1].tableName, As: yyDollar[2].sqlID, Hints: yyDollar[3].indexHints}
		}
	case 61:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:428
		{
			yyVAL.tableExpr = &AliasedTableExpr{Expr: yyDollar[1].subquery, As: yyDollar[3].sqlID}
		}
	case 62:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:432
		{
			yyVAL.tableExpr = &ParenTableExpr{Exprs: yyDollar[2].tableExprs}
		}
	case 63:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:445
		{
			yyVAL.tableExpr = &JoinTableExpr{LeftExpr: yyDollar[1].tableExpr, Join: yyDollar[2].str, RightExpr: yyDollar[3].tableExpr}
		}
	case 64:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:449
		{
			yyVAL.tableExpr = &JoinTableExpr{LeftExpr: yyDollar[1].tableExpr, Join: yyDollar[2].str, RightExpr: yyDollar[3].tableExpr, On: yyDollar[5].boolExpr}
		}
	case 65:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:453
		{
			yyVAL.tableExpr = &JoinTableExpr{LeftExpr: yyDollar[1].tableExpr, Join: yyDollar[2].str, RightExpr: yyDollar[3].tableExpr, On: yyDollar[5].boolExpr}
		}
	case 66:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:457
		{
			yyVAL.tableExpr = &JoinTableExpr{LeftExpr: yyDollar[1].tableExpr, Join: yyDollar[2].str, RightExpr: yyDollar[3].tableExpr}
		}
	case 67:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:462
		{
			yyVAL.empty = struct{}{}
		}
	case 68:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:464
		{
			yyVAL.empty = struct{}{}
		}
	case 69:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:467
		{
			yyVAL.sqlID = ""
		}
	case 70:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:471
		{
			yyVAL.sqlID = yyDollar[1].sqlID
		}
	case 71:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:475
		{
			yyVAL.sqlID = yyDollar[2].sqlID
		}
	case 72:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:481
		{
			yyVAL.str = JoinStr
		}
	case 73:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:485
		{
			yyVAL.str = JoinStr
		}
	case 74:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:489
		{
			yyVAL.str = JoinStr
		}
	case 75:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:493
		{
			yyVAL.str = StraightJoinStr
		}
	case 76:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:499
		{
			yyVAL.str = LeftJoinStr
		}
	case 77:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:503
		{
			yyVAL.str = LeftJoinStr
		}
	case 78:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:507
		{
			yyVAL.str = RightJoinStr
		}
	case 79:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:511
		{
			yyVAL.str = RightJoinStr
		}
	case 80:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:517
		{
			yyVAL.str = NaturalJoinStr
		}
	case 81:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:521
		{
			if yyDollar[2].str == LeftJoinStr {
				yyVAL.str = NaturalLeftJoinStr
			} else {
				yyVAL.str = NaturalRightJoinStr
			}
		}
	case 82:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:531
		{
			yyVAL.tableName = &TableName{Name: yyDollar[1].sqlID}
		}
	case 83:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:535
		{
			yyVAL.tableName = &TableName{Qualifier: yyDollar[1].sqlID, Name: yyDollar[3].sqlID}
		}
	case 84:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:540
		{
			yyVAL.indexHints = nil
		}
	case 85:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:544
		{
			yyVAL.indexHints = &IndexHints{Type: UseStr, Indexes: yyDollar[4].sqlIDs}
		}
	case 86:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:548
		{
			yyVAL.indexHints = &IndexHints{Type: IgnoreStr, Indexes: yyDollar[4].sqlIDs}
		}
	case 87:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:552
		{
			yyVAL.indexHints = &IndexHints{Type: ForceStr, Indexes: yyDollar[4].sqlIDs}
		}
	case 88:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:558
		{
			yyVAL.sqlIDs = []SQLName{yyDollar[1].sqlID}
		}
	case 89:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:562
		{
			yyVAL.sqlIDs = append(yyDollar[1].sqlIDs, yyDollar[3].sqlID)
		}
	case 90:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:567
		{
			yyVAL.boolExpr = nil
		}
	case 91:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:571
		{
			yyVAL.boolExpr = yyDollar[2].boolExpr
		}
	case 93:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:578
		{
			yyVAL.boolExpr = &AndExpr{Left: yyDollar[1].boolExpr, Right: yyDollar[3].boolExpr}
		}
	case 94:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:582
		{
			yyVAL.boolExpr = &OrExpr{Left: yyDollar[1].boolExpr, Right: yyDollar[3].boolExpr}
		}
	case 95:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:586
		{
			yyVAL.boolExpr = &NotExpr{Expr: yyDollar[2].boolExpr}
		}
	case 96:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:590
		{
			yyVAL.boolExpr = &ParenBoolExpr{Expr: yyDollar[2].boolExpr}
		}
	case 97:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:594
		{
			yyVAL.boolExpr = &IsExpr{Operator: yyDollar[3].str, Expr: yyDollar[1].boolExpr}
		}
	case 98:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:600
		{
			yyVAL.boolExpr = BoolVal(true)
		}
	case 99:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:604
		{
			yyVAL.boolExpr = BoolVal(false)
		}
	case 100:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:608
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: yyDollar[2].str, Right: yyDollar[3].valExpr}
		}
	case 101:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:612
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: InStr, Right: yyDollar[3].colTuple}
		}
	case 102:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:616
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: NotInStr, Right: yyDollar[4].colTuple}
		}
	case 103:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:620
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: LikeStr, Right: yyDollar[3].valExpr}
		}
	case 104:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:624
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: NotLikeStr, Right: yyDollar[4].valExpr}
		}
	case 105:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:628
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: RegexpStr, Right: yyDollar[3].valExpr}
		}
	case 106:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:632
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: NotRegexpStr, Right: yyDollar[4].valExpr}
		}
	case 107:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:636
		{
			yyVAL.boolExpr = &RangeCond{Left: yyDollar[1].valExpr, Operator: BetweenStr, From: yyDollar[3].valExpr, To: yyDollar[5].valExpr}
		}
	case 108:
		yyDollar = yyS[yypt-6 : yypt+1]
		//line sql.y:640
		{
			yyVAL.boolExpr = &RangeCond{Left: yyDollar[1].valExpr, Operator: NotBetweenStr, From: yyDollar[4].valExpr, To: yyDollar[6].valExpr}
		}
	case 109:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:644
		{
			yyVAL.boolExpr = &IsExpr{Operator: yyDollar[3].str, Expr: yyDollar[1].valExpr}
		}
	case 110:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:648
		{
			yyVAL.boolExpr = &ExistsExpr{Subquery: yyDollar[2].subquery}
		}
	case 111:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:654
		{
			yyVAL.str = IsNullStr
		}
	case 112:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:658
		{
			yyVAL.str = IsNotNullStr
		}
	case 113:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:662
		{
			yyVAL.str = IsTrueStr
		}
	case 114:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:666
		{
			yyVAL.str = IsNotTrueStr
		}
	case 115:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:670
		{
			yyVAL.str = IsFalseStr
		}
	case 116:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:674
		{
			yyVAL.str = IsNotFalseStr
		}
	case 117:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:680
		{
			yyVAL.str = EqualStr
		}
	case 118:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:684
		{
			yyVAL.str = LessThanStr
		}
	case 119:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:688
		{
			yyVAL.str = GreaterThanStr
		}
	case 120:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:692
		{
			yyVAL.str = LessEqualStr
		}
	case 121:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:696
		{
			yyVAL.str = GreaterEqualStr
		}
	case 122:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:700
		{
			yyVAL.str = NotEqualStr
		}
	case 123:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:704
		{
			yyVAL.str = NullSafeEqualStr
		}
	case 124:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:710
		{
			yyVAL.colTuple = ValTuple(yyDollar[2].valExprs)
		}
	case 125:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:714
		{
			yyVAL.colTuple = yyDollar[1].subquery
		}
	case 126:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:718
		{
			yyVAL.colTuple = ListArg(yyDollar[1].bytes)
		}
	case 127:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:724
		{
			yyVAL.subquery = &Subquery{yyDollar[2].selStmt}
		}
	case 128:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:730
		{
			yyVAL.valExprs = ValExprs{yyDollar[1].valExpr}
		}
	case 129:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:734
		{
			yyVAL.valExprs = append(yyDollar[1].valExprs, yyDollar[3].valExpr)
		}
	case 130:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:740
		{
			yyVAL.valExpr = yyDollar[1].valExpr
		}
	case 131:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:744
		{
			yyVAL.valExpr = yyDollar[1].colName
		}
	case 132:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:748
		{
			yyVAL.valExpr = yyDollar[1].rowTuple
		}
	case 133:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:752
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: BitAndStr, Right: yyDollar[3].valExpr}
		}
	case 134:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:756
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: BitOrStr, Right: yyDollar[3].valExpr}
		}
	case 135:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:760
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: BitXorStr, Right: yyDollar[3].valExpr}
		}
	case 136:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:764
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: PlusStr, Right: yyDollar[3].valExpr}
		}
	case 137:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:768
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: MinusStr, Right: yyDollar[3].valExpr}
		}
	case 138:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:772
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: MultStr, Right: yyDollar[3].valExpr}
		}
	case 139:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:776
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: DivStr, Right: yyDollar[3].valExpr}
		}
	case 140:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:780
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: ModStr, Right: yyDollar[3].valExpr}
		}
	case 141:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:784
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: ShiftLeftStr, Right: yyDollar[3].valExpr}
		}
	case 142:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:788
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: ShiftRightStr, Right: yyDollar[3].valExpr}
		}
	case 143:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:792
		{
			if num, ok := yyDollar[2].valExpr.(NumVal); ok {
				yyVAL.valExpr = num
			} else {
				yyVAL.valExpr = &UnaryExpr{Operator: UPlusStr, Expr: yyDollar[2].valExpr}
			}
		}
	case 144:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:800
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
	case 145:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:813
		{
			yyVAL.valExpr = &UnaryExpr{Operator: TildaStr, Expr: yyDollar[2].valExpr}
		}
	case 146:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:817
		{
			// This rule prevents the usage of INTERVAL
			// as a function. If support is needed for that,
			// we'll need to revisit this. The solution
			// will be non-trivial because of grammar conflicts.
			yyVAL.valExpr = &IntervalExpr{Expr: yyDollar[2].valExpr, Unit: yyDollar[3].sqlID}
		}
	case 147:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:825
		{
			yyVAL.valExpr = &FuncExpr{Name: string(yyDollar[1].sqlID)}
		}
	case 148:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:829
		{
			yyVAL.valExpr = &FuncExpr{Name: string(yyDollar[1].sqlID), Exprs: yyDollar[3].selectExprs}
		}
	case 149:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:833
		{
			yyVAL.valExpr = &FuncExpr{Name: string(yyDollar[1].sqlID), Distinct: true, Exprs: yyDollar[4].selectExprs}
		}
	case 150:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:837
		{
			yyVAL.valExpr = &FuncExpr{Name: "if", Exprs: yyDollar[3].selectExprs}
		}
	case 151:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:841
		{
			yyVAL.valExpr = yyDollar[1].caseExpr
		}
	case 152:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:847
		{
			yyVAL.caseExpr = &CaseExpr{Expr: yyDollar[2].valExpr, Whens: yyDollar[3].whens, Else: yyDollar[4].valExpr}
		}
	case 153:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:852
		{
			yyVAL.valExpr = nil
		}
	case 154:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:856
		{
			yyVAL.valExpr = yyDollar[1].valExpr
		}
	case 155:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:862
		{
			yyVAL.whens = []*When{yyDollar[1].when}
		}
	case 156:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:866
		{
			yyVAL.whens = append(yyDollar[1].whens, yyDollar[2].when)
		}
	case 157:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:872
		{
			yyVAL.when = &When{Cond: yyDollar[2].boolExpr, Val: yyDollar[4].valExpr}
		}
	case 158:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:877
		{
			yyVAL.valExpr = nil
		}
	case 159:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:881
		{
			yyVAL.valExpr = yyDollar[2].valExpr
		}
	case 160:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:887
		{
			yyVAL.colName = &ColName{Name: yyDollar[1].sqlID}
		}
	case 161:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:891
		{
			yyVAL.colName = &ColName{Qualifier: &TableName{Name: yyDollar[1].sqlID}, Name: yyDollar[3].sqlID}
		}
	case 162:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:895
		{
			yyVAL.colName = &ColName{Qualifier: &TableName{Qualifier: yyDollar[1].sqlID, Name: yyDollar[3].sqlID}, Name: yyDollar[5].sqlID}
		}
	case 163:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:901
		{
			yyVAL.valExpr = StrVal(yyDollar[1].bytes)
		}
	case 164:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:905
		{
			yyVAL.valExpr = NumVal(yyDollar[1].bytes)
		}
	case 165:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:909
		{
			yyVAL.valExpr = ValArg(yyDollar[1].bytes)
		}
	case 166:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:913
		{
			yyVAL.valExpr = &NullVal{}
		}
	case 167:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:918
		{
			yyVAL.valExprs = nil
		}
	case 168:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:922
		{
			yyVAL.valExprs = yyDollar[3].valExprs
		}
	case 169:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:927
		{
			yyVAL.boolExpr = nil
		}
	case 170:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:931
		{
			yyVAL.boolExpr = yyDollar[2].boolExpr
		}
	case 171:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:936
		{
			yyVAL.orderBy = nil
		}
	case 172:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:940
		{
			yyVAL.orderBy = yyDollar[3].orderBy
		}
	case 173:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:946
		{
			yyVAL.orderBy = OrderBy{yyDollar[1].order}
		}
	case 174:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:950
		{
			yyVAL.orderBy = append(yyDollar[1].orderBy, yyDollar[3].order)
		}
	case 175:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:956
		{
			yyVAL.order = &Order{Expr: yyDollar[1].valExpr, Direction: yyDollar[2].str}
		}
	case 176:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:961
		{
			yyVAL.str = AscScr
		}
	case 177:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:965
		{
			yyVAL.str = AscScr
		}
	case 178:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:969
		{
			yyVAL.str = DescScr
		}
	case 179:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:974
		{
			yyVAL.limit = nil
		}
	case 180:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:978
		{
			yyVAL.limit = &Limit{Rowcount: yyDollar[2].valExpr}
		}
	case 181:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:982
		{
			yyVAL.limit = &Limit{Offset: yyDollar[2].valExpr, Rowcount: yyDollar[4].valExpr}
		}
	case 182:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:987
		{
			yyVAL.str = ""
		}
	case 183:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:991
		{
			yyVAL.str = ForUpdateStr
		}
	case 184:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:995
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
	case 185:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1008
		{
			yyVAL.columns = nil
		}
	case 186:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1012
		{
			yyVAL.columns = yyDollar[2].columns
		}
	case 187:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1018
		{
			yyVAL.columns = Columns{&NonStarExpr{Expr: yyDollar[1].colName}}
		}
	case 188:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1022
		{
			yyVAL.columns = append(yyVAL.columns, &NonStarExpr{Expr: yyDollar[3].colName})
		}
	case 189:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1027
		{
			yyVAL.updateExprs = nil
		}
	case 190:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:1031
		{
			yyVAL.updateExprs = yyDollar[5].updateExprs
		}
	case 191:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:1037
		{
			yyVAL.insRows = yyDollar[2].values
		}
	case 192:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1041
		{
			yyVAL.insRows = yyDollar[1].selStmt
		}
	case 193:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1047
		{
			yyVAL.values = Values{yyDollar[1].rowTuple}
		}
	case 194:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1051
		{
			yyVAL.values = append(yyDollar[1].values, yyDollar[3].rowTuple)
		}
	case 195:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1057
		{
			yyVAL.rowTuple = ValTuple(yyDollar[2].valExprs)
		}
	case 196:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1061
		{
			yyVAL.rowTuple = yyDollar[1].subquery
		}
	case 197:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1067
		{
			yyVAL.updateExprs = UpdateExprs{yyDollar[1].updateExpr}
		}
	case 198:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1071
		{
			yyVAL.updateExprs = append(yyDollar[1].updateExprs, yyDollar[3].updateExpr)
		}
	case 199:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1077
		{
			yyVAL.updateExpr = &UpdateExpr{Name: yyDollar[1].colName, Expr: yyDollar[3].valExpr}
		}
	case 202:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1086
		{
			yyVAL.empty = struct{}{}
		}
	case 203:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:1088
		{
			yyVAL.empty = struct{}{}
		}
	case 204:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1091
		{
			yyVAL.empty = struct{}{}
		}
	case 205:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1093
		{
			yyVAL.empty = struct{}{}
		}
	case 206:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1096
		{
			yyVAL.str = ""
		}
	case 207:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1098
		{
			yyVAL.str = IgnoreStr
		}
	case 208:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1102
		{
			yyVAL.empty = struct{}{}
		}
	case 209:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1104
		{
			yyVAL.empty = struct{}{}
		}
	case 210:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1106
		{
			yyVAL.empty = struct{}{}
		}
	case 211:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1108
		{
			yyVAL.empty = struct{}{}
		}
	case 212:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1110
		{
			yyVAL.empty = struct{}{}
		}
	case 213:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1113
		{
			yyVAL.empty = struct{}{}
		}
	case 214:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1115
		{
			yyVAL.empty = struct{}{}
		}
	case 215:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1118
		{
			yyVAL.empty = struct{}{}
		}
	case 216:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1120
		{
			yyVAL.empty = struct{}{}
		}
	case 217:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1123
		{
			yyVAL.empty = struct{}{}
		}
	case 218:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:1125
		{
			yyVAL.empty = struct{}{}
		}
	case 219:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1129
		{
			yyVAL.sqlID = SQLName(strings.ToLower(string(yyDollar[1].bytes)))
		}
	case 220:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1135
		{
			yyVAL.sqlID = SQLName(yyDollar[1].bytes)
		}
	case 221:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1141
		{
			if incNesting(yylex) {
				yylex.Error("max nesting level reached")
				return 1
			}
		}
	case 222:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1150
		{
			decNesting(yylex)
		}
	case 223:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1155
		{
			forceEOF(yylex)
		}
	}
	goto yystack /* stack new state and value */
}
