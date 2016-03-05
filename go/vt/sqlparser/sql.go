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
const NEXTVAL = 57378
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
const END = 57418
const CREATE = 57419
const ALTER = 57420
const DROP = 57421
const RENAME = 57422
const ANALYZE = 57423
const TABLE = 57424
const INDEX = 57425
const VIEW = 57426
const TO = 57427
const IGNORE = 57428
const IF = 57429
const UNIQUE = 57430
const USING = 57431
const SHOW = 57432
const DESCRIBE = 57433
const EXPLAIN = 57434

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
	"NEXTVAL",
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
	91, 220,
	-2, 219,
}

const yyNprod = 224
const yyPrivate = 57344

var yyTokenNames []string
var yyStates []string

const yyLast = 801

var yyAct = [...]int{

	99, 319, 168, 401, 94, 171, 364, 50, 63, 277,
	266, 354, 208, 93, 207, 95, 206, 170, 3, 248,
	226, 260, 82, 220, 187, 64, 59, 195, 339, 341,
	78, 111, 83, 51, 52, 70, 44, 43, 66, 44,
	273, 72, 65, 106, 75, 53, 68, 107, 108, 109,
	128, 38, 110, 40, 46, 47, 48, 41, 89, 113,
	376, 375, 374, 71, 74, 49, 45, 351, 292, 138,
	121, 88, 155, 156, 157, 152, 383, 117, 96, 97,
	125, 132, 152, 127, 98, 340, 136, 415, 142, 118,
	140, 120, 201, 73, 123, 172, 261, 114, 112, 173,
	175, 176, 73, 68, 142, 199, 151, 150, 158, 159,
	153, 154, 155, 156, 157, 152, 183, 66, 233, 288,
	66, 65, 191, 190, 65, 185, 202, 216, 141, 140,
	192, 231, 232, 230, 353, 61, 179, 89, 215, 191,
	205, 106, 184, 142, 14, 189, 225, 61, 229, 234,
	235, 236, 124, 238, 239, 240, 241, 242, 243, 244,
	245, 246, 247, 167, 169, 211, 137, 214, 249, 198,
	200, 197, 261, 237, 310, 79, 228, 253, 141, 140,
	89, 89, 106, 106, 14, 61, 68, 107, 108, 109,
	250, 252, 110, 142, 73, 258, 270, 254, 249, 113,
	255, 257, 141, 140, 271, 293, 221, 223, 224, 119,
	265, 222, 218, 219, 294, 295, 296, 142, 96, 97,
	107, 108, 109, 106, 98, 110, 61, 410, 249, 291,
	253, 274, 251, 297, 299, 300, 301, 384, 112, 279,
	282, 283, 284, 280, 211, 281, 285, 174, 355, 371,
	119, 298, 360, 249, 303, 28, 29, 30, 31, 89,
	268, 188, 228, 188, 66, 66, 134, 249, 65, 317,
	251, 249, 315, 275, 249, 304, 318, 306, 133, 305,
	314, 309, 153, 154, 155, 156, 157, 152, 381, 327,
	326, 329, 328, 307, 77, 337, 275, 373, 119, 263,
	249, 106, 139, 92, 370, 348, 355, 211, 211, 211,
	211, 344, 372, 352, 134, 331, 106, 346, 269, 131,
	361, 350, 330, 362, 365, 349, 334, 358, 42, 357,
	366, 335, 332, 311, 14, 116, 359, 333, 396, 92,
	92, 380, 336, 80, 283, 284, 407, 177, 178, 115,
	377, 193, 180, 181, 130, 290, 379, 56, 408, 313,
	54, 66, 320, 58, 369, 382, 321, 267, 368, 325,
	378, 188, 253, 62, 414, 390, 405, 14, 388, 33,
	1, 212, 92, 289, 286, 135, 398, 365, 92, 92,
	400, 399, 227, 194, 402, 402, 402, 397, 403, 404,
	39, 67, 28, 29, 30, 31, 66, 272, 196, 69,
	65, 416, 316, 262, 413, 406, 417, 385, 418, 363,
	367, 409, 324, 411, 412, 92, 92, 279, 282, 283,
	284, 280, 264, 281, 285, 308, 92, 60, 182, 259,
	389, 101, 391, 392, 100, 356, 312, 76, 143, 90,
	32, 81, 338, 104, 210, 278, 276, 86, 209, 85,
	212, 55, 105, 27, 60, 87, 34, 35, 36, 37,
	57, 122, 13, 12, 11, 10, 126, 106, 227, 129,
	68, 107, 108, 109, 9, 8, 110, 102, 103, 7,
	6, 91, 5, 113, 151, 150, 158, 159, 153, 154,
	155, 156, 157, 152, 92, 14, 15, 16, 17, 92,
	386, 387, 96, 97, 84, 4, 2, 60, 98, 186,
	256, 0, 104, 212, 212, 212, 212, 18, 0, 0,
	203, 105, 112, 204, 87, 213, 86, 0, 0, 0,
	0, 217, 0, 0, 0, 0, 106, 0, 249, 68,
	107, 108, 109, 0, 0, 110, 102, 103, 0, 0,
	91, 0, 113, 0, 151, 150, 158, 159, 153, 154,
	155, 156, 157, 152, 0, 0, 0, 0, 0, 86,
	86, 96, 97, 84, 0, 0, 0, 98, 0, 19,
	20, 22, 21, 23, 0, 0, 0, 0, 0, 0,
	0, 112, 24, 25, 26, 0, 0, 0, 104, 0,
	0, 0, 287, 0, 213, 0, 92, 105, 92, 92,
	0, 0, 393, 394, 395, 0, 0, 0, 0, 0,
	0, 0, 106, 0, 0, 68, 107, 108, 109, 0,
	0, 110, 102, 103, 0, 0, 91, 0, 113, 158,
	159, 153, 154, 155, 156, 157, 152, 14, 86, 0,
	0, 0, 0, 0, 0, 0, 0, 96, 97, 0,
	0, 322, 104, 98, 323, 0, 0, 213, 213, 213,
	213, 105, 0, 0, 0, 0, 0, 112, 0, 0,
	342, 343, 0, 0, 345, 347, 106, 0, 0, 68,
	107, 108, 109, 0, 0, 110, 102, 103, 0, 0,
	91, 0, 113, 151, 150, 158, 159, 153, 154, 155,
	156, 157, 152, 0, 0, 0, 0, 0, 0, 0,
	0, 96, 97, 0, 0, 0, 0, 98, 0, 0,
	0, 0, 0, 0, 0, 0, 145, 148, 0, 0,
	0, 112, 160, 161, 162, 163, 164, 165, 166, 149,
	146, 147, 144, 151, 150, 158, 159, 153, 154, 155,
	156, 157, 152, 302, 150, 158, 159, 153, 154, 155,
	156, 157, 152, 0, 0, 0, 0, 0, 0, 0,
	0, 151, 150, 158, 159, 153, 154, 155, 156, 157,
	152,
}
var yyPact = [...]int{

	496, -1000, -1000, 397, -1000, -1000, -1000, -1000, -1000, -1000,
	-1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, -47,
	-63, -32, -44, -33, -1000, -1000, -1000, 368, 339, -1000,
	-1000, -1000, 335, -1000, -66, 84, 360, 52, -68, -36,
	51, -1000, -34, 51, -1000, 84, -73, 124, -73, 84,
	-1000, -1000, -1000, -1000, -1000, 429, -1000, 41, 322, 304,
	-14, -1000, 84, 160, -1000, 23, -1000, -21, -1000, 84,
	32, 101, -1000, -1000, 84, -1000, -51, 84, 330, 272,
	51, -1000, 265, -1000, -1000, 143, -22, 254, 142, 684,
	-1000, 584, 648, -1000, -1000, -1000, -5, -5, -5, 253,
	253, -1000, -1000, -1000, 253, 253, -1000, -1000, -1000, -1000,
	-1000, -1000, -1000, -5, -1000, 84, 52, 84, 357, 52,
	-5, 51, -1000, 327, -78, -1000, 75, -1000, 84, -1000,
	-1000, 84, -1000, 134, 429, -1000, -1000, 51, 42, 84,
	584, 584, 149, -5, 93, 55, -5, -5, -5, 149,
	-5, -5, -5, -5, -5, -5, -5, -5, -5, -5,
	-1000, -1000, -1000, -1000, -1000, -1000, -1000, 13, 684, 118,
	250, 221, 684, -1000, 135, -1000, -1000, 498, 429, -1000,
	368, 168, 31, 415, 268, 249, -1000, 350, 584, -1000,
	415, -1000, -1000, -1000, 271, 51, -1000, -61, -1000, -1000,
	-1000, -1000, -1000, -1000, -1000, -1000, 247, 390, -1000, -1000,
	96, 332, 175, -23, -1000, -1000, -1000, 155, 13, 29,
	-1000, -1000, 157, -1000, -1000, 415, -1000, 135, -1000, -1000,
	93, -5, -5, -5, 415, 415, 712, -1000, 568, 694,
	-1000, -13, -13, -6, -6, -6, 199, 199, -1000, -1000,
	-1000, -5, -1000, 415, -1000, 217, 429, 217, 244, 107,
	-1000, 584, 325, 52, 52, 350, 343, 348, 142, 84,
	-1000, -1000, 84, -1000, 354, 134, 134, 134, 134, -1000,
	285, 278, -1000, 295, 289, 305, -17, -1000, 84, 84,
	-1000, 224, 84, -1000, -1000, -1000, -1000, 221, -1000, 415,
	415, 634, -5, 415, -1000, 217, -1000, 168, -25, -1000,
	-5, 68, 259, 253, 397, 201, 203, -1000, 343, -1000,
	-5, -5, -1000, -1000, 352, 346, 390, 257, 202, -1000,
	-1000, -1000, -1000, 275, -1000, 260, -1000, -1000, -1000, -37,
	-38, -39, -1000, -1000, -1000, -1000, -1000, -5, 415, -1000,
	148, -1000, 415, -5, -1000, 313, 239, -1000, -1000, -1000,
	52, -1000, 27, 188, -1000, 485, -1000, 350, 584, -5,
	584, 584, -1000, -1000, 253, 253, 253, 415, -1000, 415,
	309, 253, -1000, -5, -5, -1000, -1000, -1000, 343, 142,
	183, 142, 142, 51, 51, 51, 365, -1000, 415, -1000,
	326, 178, -1000, 178, 178, 52, -1000, 363, 9, -1000,
	51, -1000, -1000, 160, -1000, 51, -1000, 51, -1000,
}
var yyPgo = [...]int{

	0, 516, 17, 515, 492, 490, 489, 485, 484, 475,
	474, 473, 472, 450, 470, 463, 461, 22, 32, 459,
	16, 14, 12, 458, 456, 9, 455, 454, 26, 452,
	3, 24, 71, 449, 448, 446, 13, 2, 23, 20,
	5, 445, 15, 444, 31, 4, 441, 439, 21, 438,
	435, 422, 420, 10, 419, 6, 417, 1, 415, 413,
	412, 11, 8, 25, 328, 294, 409, 408, 407, 400,
	393, 0, 385, 401, 384, 383, 7, 380, 379, 247,
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
	-9, -10, -11, -12, 9, 10, 11, 12, 31, 93,
	94, 96, 95, 97, 106, 107, 108, -15, 5, 6,
	7, 8, -13, -78, -13, -13, -13, -13, 98, -69,
	100, 104, -64, 100, 102, 98, 98, 99, 100, 98,
	-76, -76, -76, -2, 21, -16, 22, -14, -64, -28,
	-73, 51, 13, -62, -63, -45, -71, -73, 51, -66,
	103, 99, -71, 51, 98, -71, -73, -65, 103, 51,
	-65, -73, -17, -18, 85, -19, -73, 36, -32, -37,
	-33, 62, -79, -36, -45, -42, 83, 84, 89, -71,
	-43, -46, 58, 59, 24, 33, 48, 52, 53, 54,
	57, -44, 103, 64, 56, 27, 31, 91, -28, 49,
	68, 91, -73, 62, 51, -76, -73, -76, 101, -73,
	24, 47, -71, 13, 49, -72, -71, 23, 91, 48,
	61, 60, 75, -34, 78, 62, 76, 77, 63, 75,
	80, 79, 88, 83, 84, 85, 86, 87, 81, 82,
	68, 69, 70, 71, 72, 73, 74, -32, -37, -32,
	-2, -40, -37, -37, -79, -37, -37, -79, -79, -44,
	-79, -79, -49, -37, -28, -62, -73, -31, 14, -63,
	-37, -71, -76, 24, -70, 105, -67, 96, 94, 30,
	95, 17, 51, -73, -73, -76, -20, -21, -22, -23,
	-27, -44, -79, -73, -18, -71, 85, -73, -32, -32,
	-38, 57, 62, 58, 59, -37, -39, -79, -44, 55,
	78, 76, 77, 63, -37, -37, -37, -38, -37, -37,
	-37, -37, -37, -37, -37, -37, -37, -37, -80, 50,
	-80, 49, -80, -37, -80, -17, 22, -17, -36, -47,
	-48, 65, -59, 31, -79, -31, -53, 17, -32, 47,
	-71, -76, -68, 101, -31, 49, -24, -25, -26, 37,
	41, 43, 38, 39, 40, 44, -74, -73, 23, -75,
	23, -20, 91, 50, 57, 58, 59, -40, -39, -37,
	-37, -37, 61, -37, -80, -17, -80, 49, -50, -48,
	67, -32, -35, 34, -2, -62, -60, -45, -53, -57,
	19, 18, -73, -73, -51, 15, -21, -22, -21, -22,
	37, 37, 37, 42, 37, 42, 37, -25, -29, 45,
	102, 46, -73, -73, -80, -73, -80, 61, -37, -80,
	-36, 92, -37, 66, -61, 47, -41, -42, -61, -80,
	49, -57, -37, -54, -55, -37, -76, -52, 16, 18,
	47, 47, 37, 37, 99, 99, 99, -37, -80, -37,
	28, 49, -45, 49, 49, -56, 25, 26, -53, -32,
	-40, -32, -32, -79, -79, -79, 29, -42, -37, -55,
	-57, -30, -71, -30, -30, 11, -58, 20, 32, -80,
	49, -80, -80, -62, 11, 78, -71, -71, -71,
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
	3, 3, 3, 3, 3, 3, 3, 87, 80, 3,
	48, 50, 85, 83, 49, 84, 91, 86, 3, 3,
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
	78, 81, 82, 90, 92, 93, 94, 95, 96, 97,
	98, 99, 100, 101, 102, 103, 104, 105, 106, 107,
	108,
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
			yyVAL.selectExpr = &Nextval{TableName: yyDollar[3].sqlID}
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
