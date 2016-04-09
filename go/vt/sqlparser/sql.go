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
const SELECT = 57348
const INSERT = 57349
const UPDATE = 57350
const DELETE = 57351
const FROM = 57352
const WHERE = 57353
const GROUP = 57354
const HAVING = 57355
const ORDER = 57356
const BY = 57357
const LIMIT = 57358
const FOR = 57359
const ALL = 57360
const DISTINCT = 57361
const AS = 57362
const EXISTS = 57363
const ASC = 57364
const DESC = 57365
const INTO = 57366
const DUPLICATE = 57367
const KEY = 57368
const DEFAULT = 57369
const SET = 57370
const LOCK = 57371
const VALUES = 57372
const LAST_INSERT_ID = 57373
const NEXT = 57374
const VALUE = 57375
const JOIN = 57376
const STRAIGHT_JOIN = 57377
const LEFT = 57378
const RIGHT = 57379
const INNER = 57380
const OUTER = 57381
const CROSS = 57382
const NATURAL = 57383
const USE = 57384
const FORCE = 57385
const ON = 57386
const ID = 57387
const STRING = 57388
const NUMBER = 57389
const VALUE_ARG = 57390
const LIST_ARG = 57391
const COMMENT = 57392
const NULL = 57393
const TRUE = 57394
const FALSE = 57395
const OR = 57396
const AND = 57397
const NOT = 57398
const BETWEEN = 57399
const CASE = 57400
const WHEN = 57401
const THEN = 57402
const ELSE = 57403
const LE = 57404
const GE = 57405
const NE = 57406
const NULL_SAFE_EQUAL = 57407
const IS = 57408
const LIKE = 57409
const REGEXP = 57410
const IN = 57411
const SHIFT_LEFT = 57412
const SHIFT_RIGHT = 57413
const UNARY = 57414
const INTERVAL = 57415
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
const UNUSED = 57433

var yyToknames = [...]string{
	"$end",
	"error",
	"$unk",
	"LEX_ERROR",
	"UNION",
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
	"UNUSED",
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
	-1, 67,
	89, 221,
	-2, 220,
}

const yyNprod = 225
const yyPrivate = 57344

var yyTokenNames []string
var yyStates []string

const yyLast = 778

var yyAct = [...]int{

	120, 285, 195, 401, 265, 198, 215, 131, 294, 336,
	114, 227, 62, 226, 326, 115, 276, 243, 139, 237,
	197, 3, 225, 229, 35, 103, 37, 149, 77, 104,
	38, 354, 356, 69, 40, 65, 41, 41, 71, 47,
	224, 74, 98, 125, 384, 64, 383, 382, 50, 70,
	108, 43, 44, 45, 73, 83, 58, 46, 42, 364,
	219, 155, 63, 166, 91, 48, 49, 126, 87, 179,
	67, 127, 128, 129, 153, 414, 130, 123, 124, 67,
	102, 111, 167, 132, 109, 88, 169, 65, 90, 355,
	65, 277, 144, 142, 93, 157, 169, 64, 126, 137,
	64, 84, 116, 117, 105, 246, 305, 164, 118, 136,
	119, 95, 72, 234, 97, 199, 277, 67, 324, 200,
	201, 202, 203, 121, 185, 186, 180, 181, 182, 183,
	184, 179, 146, 206, 60, 209, 168, 167, 60, 152,
	154, 151, 160, 238, 240, 241, 218, 143, 239, 94,
	221, 169, 141, 78, 156, 89, 214, 210, 182, 183,
	184, 179, 194, 196, 268, 109, 233, 144, 371, 230,
	309, 310, 311, 242, 410, 266, 251, 252, 253, 245,
	255, 256, 257, 258, 259, 260, 261, 262, 263, 264,
	222, 217, 232, 368, 165, 126, 254, 250, 60, 126,
	168, 167, 267, 269, 270, 378, 366, 109, 109, 271,
	248, 249, 247, 65, 65, 169, 266, 327, 235, 236,
	288, 284, 72, 64, 283, 281, 168, 167, 14, 112,
	272, 274, 280, 180, 181, 182, 183, 184, 179, 230,
	28, 169, 162, 266, 291, 268, 266, 218, 292, 266,
	312, 314, 315, 316, 308, 245, 332, 266, 140, 112,
	112, 14, 15, 16, 17, 313, 76, 126, 204, 205,
	60, 318, 327, 207, 89, 220, 109, 319, 161, 321,
	212, 101, 266, 18, 213, 82, 333, 331, 112, 334,
	337, 140, 323, 292, 381, 329, 330, 126, 380, 320,
	230, 230, 230, 230, 346, 342, 341, 344, 343, 231,
	112, 352, 79, 359, 162, 112, 112, 360, 349, 244,
	362, 345, 86, 350, 55, 363, 89, 365, 325, 338,
	39, 347, 387, 65, 14, 126, 348, 54, 67, 127,
	128, 129, 367, 369, 130, 85, 19, 20, 22, 21,
	23, 132, 112, 112, 351, 407, 300, 301, 279, 24,
	25, 26, 57, 147, 385, 100, 307, 408, 135, 386,
	116, 117, 286, 389, 337, 134, 118, 377, 119, 231,
	218, 390, 391, 393, 388, 66, 51, 52, 287, 216,
	376, 121, 340, 400, 140, 244, 61, 402, 402, 402,
	65, 403, 404, 413, 399, 14, 409, 28, 411, 412,
	64, 415, 405, 30, 1, 416, 306, 417, 59, 303,
	29, 112, 163, 148, 36, 112, 223, 392, 75, 394,
	395, 150, 80, 68, 373, 374, 31, 32, 33, 34,
	231, 231, 231, 231, 133, 282, 211, 59, 406, 296,
	299, 300, 301, 297, 92, 298, 302, 372, 335, 96,
	375, 339, 99, 322, 208, 275, 122, 107, 328, 113,
	278, 59, 273, 138, 125, 170, 110, 145, 353, 295,
	293, 228, 106, 81, 158, 53, 27, 159, 178, 177,
	185, 186, 180, 181, 182, 183, 184, 179, 126, 56,
	266, 67, 127, 128, 129, 13, 12, 130, 123, 124,
	11, 10, 111, 9, 132, 8, 7, 6, 5, 59,
	4, 14, 2, 0, 112, 0, 112, 112, 0, 0,
	396, 397, 398, 116, 117, 105, 125, 0, 0, 118,
	0, 119, 0, 0, 0, 0, 0, 59, 107, 0,
	0, 0, 145, 0, 121, 0, 0, 0, 0, 125,
	126, 0, 0, 67, 127, 128, 129, 0, 0, 130,
	123, 124, 0, 0, 111, 0, 132, 0, 0, 0,
	0, 0, 0, 126, 0, 0, 67, 127, 128, 129,
	107, 107, 130, 123, 124, 116, 117, 111, 370, 132,
	0, 118, 14, 119, 0, 0, 289, 0, 0, 290,
	0, 0, 0, 0, 0, 304, 121, 59, 116, 117,
	0, 0, 0, 0, 118, 0, 119, 0, 178, 177,
	185, 186, 180, 181, 182, 183, 184, 179, 0, 121,
	0, 126, 361, 0, 67, 127, 128, 129, 0, 0,
	130, 0, 0, 0, 0, 0, 0, 132, 0, 107,
	178, 177, 185, 186, 180, 181, 182, 183, 184, 179,
	0, 0, 0, 0, 0, 0, 116, 117, 59, 59,
	59, 59, 118, 0, 119, 0, 0, 0, 0, 0,
	0, 357, 358, 172, 175, 72, 0, 121, 0, 187,
	188, 189, 190, 191, 192, 193, 176, 173, 174, 171,
	178, 177, 185, 186, 180, 181, 182, 183, 184, 179,
	317, 0, 0, 178, 177, 185, 186, 180, 181, 182,
	183, 184, 179, 0, 0, 0, 0, 0, 178, 177,
	185, 186, 180, 181, 182, 183, 184, 179, 178, 177,
	185, 186, 180, 181, 182, 183, 184, 179, 177, 185,
	186, 180, 181, 182, 183, 184, 179, 296, 299, 300,
	301, 297, 0, 298, 302, 0, 0, 379,
}
var yyPact = [...]int{

	255, -1000, -1000, 402, -1000, -1000, -1000, -1000, -1000, -1000,
	-1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, -72,
	-64, -38, -45, -39, -1000, -1000, -1000, 399, 368, 305,
	-1000, -63, 90, 386, 69, -68, -48, 64, -1000, -42,
	64, -1000, 90, -73, 105, -73, 90, -1000, -1000, -1000,
	-1000, -1000, -1000, 250, 64, -1000, 48, 321, 294, -21,
	-1000, 90, 109, -1000, 23, -1000, -25, -1000, 90, 35,
	101, -1000, -1000, 90, -1000, -57, 90, 344, 237, 64,
	-1000, 22, -1000, 358, -1000, 90, 69, 90, 383, 69,
	290, 69, -1000, 342, -76, -1000, 47, -1000, 90, -1000,
	-1000, 90, -1000, 268, -1000, -1000, 174, -26, 79, 634,
	-1000, 538, 515, -1000, -1000, -1000, 290, 290, 290, 290,
	154, 154, -1000, -1000, -1000, 154, -1000, -1000, -1000, -1000,
	-1000, -1000, 290, 90, -1000, -1000, 252, 280, -1000, 375,
	538, -1000, 672, 596, -1000, -29, -1000, -1000, 231, 64,
	-1000, -59, -1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000,
	-1000, 150, 22, -1000, -1000, 64, 31, 538, 538, 89,
	290, 53, 137, 290, 290, 290, 89, 290, 290, 290,
	290, 290, 290, 290, 290, 290, 290, -1000, -1000, -1000,
	-1000, -1000, -1000, -1000, 14, 634, 169, 235, 199, 634,
	-1000, -1000, -1000, 647, 453, 22, -1000, 399, 29, 672,
	-1000, 328, 69, 69, 375, 356, 373, 79, 672, 64,
	90, -1000, -1000, 90, -1000, 247, 415, -1000, -1000, 86,
	346, 222, -1000, -1000, -1000, 14, 24, -1000, -1000, 116,
	-1000, -1000, 672, -1000, 596, -1000, -1000, 53, 290, 290,
	290, 672, 672, 662, -1000, 46, 681, -1000, 76, 76,
	-16, -16, -16, 153, 153, -1000, -1000, -1000, 290, -1000,
	-1000, -1000, 196, 22, 196, 54, -1000, 538, 173, 154,
	402, 228, 210, -1000, 356, -1000, 290, 290, -1000, -1000,
	-1000, 380, 150, 150, 150, 150, -1000, 287, 270, -1000,
	297, 284, 320, -11, -1000, 90, 90, -1000, 202, -1000,
	-1000, -1000, 199, -1000, 672, 672, 584, 290, 672, -1000,
	196, -1000, -31, -1000, 290, 143, -1000, 317, 147, -1000,
	-1000, -1000, 69, -1000, 552, 122, -1000, 412, -1000, 377,
	362, 415, 161, 733, -1000, -1000, -1000, -1000, 264, -1000,
	260, -1000, -1000, -1000, -50, -51, -53, -1000, -1000, -1000,
	-1000, 290, 672, -1000, -1000, 672, 290, 306, 154, -1000,
	290, 290, -1000, -1000, -1000, 375, 538, 290, 538, 538,
	-1000, -1000, 154, 154, 154, 672, 672, 396, -1000, 672,
	-1000, 356, 79, 118, 79, 79, 64, 64, 64, 69,
	338, 128, -1000, 128, 128, 109, -1000, 395, 0, -1000,
	64, -1000, -1000, -1000, 64, -1000, 64, -1000,
}
var yyPgo = [...]int{

	0, 522, 20, 520, 518, 517, 516, 515, 513, 511,
	510, 506, 505, 420, 499, 486, 485, 483, 25, 29,
	482, 22, 13, 11, 481, 480, 8, 479, 23, 478,
	3, 18, 50, 476, 475, 470, 469, 2, 19, 17,
	5, 468, 15, 7, 10, 466, 465, 16, 464, 463,
	461, 460, 6, 458, 9, 457, 1, 448, 446, 445,
	14, 12, 62, 444, 330, 266, 433, 431, 426, 424,
	423, 0, 422, 385, 419, 416, 39, 414, 413, 147,
	4,
}
var yyR1 = [...]int{

	0, 77, 1, 1, 1, 1, 1, 1, 1, 1,
	1, 1, 1, 2, 2, 2, 3, 3, 4, 5,
	6, 7, 7, 7, 8, 8, 8, 9, 10, 10,
	10, 11, 12, 12, 12, 78, 13, 14, 14, 15,
	15, 15, 16, 16, 17, 17, 18, 18, 19, 19,
	19, 20, 20, 72, 72, 72, 21, 21, 22, 22,
	23, 23, 23, 24, 24, 24, 24, 75, 75, 74,
	74, 74, 25, 25, 25, 25, 26, 26, 26, 26,
	27, 27, 28, 28, 29, 29, 29, 29, 30, 30,
	31, 31, 32, 32, 32, 32, 32, 32, 33, 33,
	33, 33, 33, 33, 33, 33, 33, 33, 33, 33,
	33, 38, 38, 38, 38, 38, 38, 34, 34, 34,
	34, 34, 34, 34, 39, 39, 39, 43, 40, 40,
	37, 37, 37, 37, 37, 37, 37, 37, 37, 37,
	37, 37, 37, 37, 37, 37, 37, 37, 37, 37,
	37, 37, 45, 48, 48, 46, 46, 47, 49, 49,
	44, 44, 44, 36, 36, 36, 36, 50, 50, 51,
	51, 52, 52, 53, 53, 54, 55, 55, 55, 56,
	56, 56, 57, 57, 57, 58, 58, 59, 59, 60,
	60, 35, 35, 41, 41, 42, 42, 61, 61, 62,
	63, 63, 65, 65, 66, 66, 64, 64, 67, 67,
	67, 67, 67, 67, 68, 68, 69, 69, 70, 70,
	71, 73, 79, 80, 76,
}
var yyR2 = [...]int{

	0, 1, 1, 1, 1, 1, 1, 1, 1, 1,
	1, 1, 1, 13, 6, 3, 8, 8, 8, 7,
	3, 5, 8, 4, 6, 7, 4, 5, 4, 5,
	5, 3, 2, 2, 2, 0, 2, 0, 2, 1,
	2, 2, 0, 1, 0, 1, 1, 3, 1, 2,
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
	1, 1, 1, 1, 0, 1, 0, 1, 0, 2,
	1, 1, 1, 1, 0,
}
var yyChk = [...]int{

	-1000, -77, -1, -2, -3, -4, -5, -6, -7, -8,
	-9, -10, -11, -12, 6, 7, 8, 9, 28, 91,
	92, 94, 93, 95, 104, 105, 106, -15, 5, -13,
	-78, -13, -13, -13, -13, 96, -69, 98, 102, -64,
	98, 100, 96, 96, 97, 98, 96, -76, -76, -76,
	-2, 18, 19, -16, 32, 19, -14, -64, -28, -73,
	48, 10, -61, -62, -44, -71, -73, 48, -66, 101,
	97, -71, 48, 96, -71, -73, -65, 101, 48, -65,
	-73, -17, 35, -71, 53, 24, 28, 89, -28, 46,
	65, 89, -73, 59, 48, -76, -73, -76, 99, -73,
	21, 44, -71, -18, -19, 82, -20, -73, -32, -37,
	-33, 59, -79, -36, -44, -42, 80, 81, 86, 88,
	-71, 101, -45, 55, 56, 21, 45, 49, 50, 51,
	54, -43, 61, -63, 17, 10, -28, -61, -73, -31,
	11, -62, -37, -79, -71, -73, -76, 21, -70, 103,
	-67, 94, 92, 27, 93, 14, 107, 48, -73, -73,
	-76, 10, 46, -72, -71, 20, 89, 58, 57, 72,
	-34, 75, 59, 73, 74, 60, 72, 77, 76, 85,
	80, 81, 82, 83, 84, 78, 79, 65, 66, 67,
	68, 69, 70, 71, -32, -37, -32, -2, -40, -37,
	-37, -37, -37, -37, -79, -79, -43, -79, -48, -37,
	-28, -58, 28, -79, -31, -52, 14, -32, -37, 89,
	44, -71, -76, -68, 99, -21, -22, -23, -24, -28,
	-43, -79, -19, -71, 82, -32, -32, -38, 54, 59,
	55, 56, -37, -39, -79, -43, 52, 75, 73, 74,
	60, -37, -37, -37, -38, -37, -37, -37, -37, -37,
	-37, -37, -37, -37, -37, -80, 47, -80, 46, -80,
	-71, -80, -18, 19, -18, -46, -47, 62, -35, 30,
	-2, -61, -59, -44, -52, -56, 16, 15, -71, -73,
	-73, -31, 46, -25, -26, -27, 34, 38, 40, 35,
	36, 37, 41, -74, -73, 20, -75, 20, -21, 54,
	55, 56, -40, -39, -37, -37, -37, 58, -37, -80,
	-18, -80, -49, -47, 64, -32, -60, 44, -41, -42,
	-60, -80, 46, -56, -37, -53, -54, -37, -76, -50,
	12, -22, -23, -22, -23, 34, 34, 34, 39, 34,
	39, 34, -26, -29, 42, 100, 43, -73, -73, -80,
	-80, 58, -37, -80, 90, -37, 63, 25, 46, -44,
	46, 46, -55, 22, 23, -51, 13, 15, 44, 44,
	34, 34, 97, 97, 97, -37, -37, 26, -42, -37,
	-54, -52, -32, -40, -32, -32, -79, -79, -79, 8,
	-56, -30, -71, -30, -30, -61, -57, 17, 29, -80,
	46, -80, -80, 8, 75, -71, -71, -71,
}
var yyDef = [...]int{

	0, -2, 1, 2, 3, 4, 5, 6, 7, 8,
	9, 10, 11, 12, 35, 35, 35, 35, 35, 216,
	206, 0, 0, 0, 224, 224, 224, 0, 39, 42,
	37, 206, 0, 0, 0, 204, 0, 0, 217, 0,
	0, 207, 0, 202, 0, 202, 0, 32, 33, 34,
	15, 40, 41, 44, 0, 43, 36, 0, 0, 82,
	221, 0, 20, 197, 0, 160, 0, -2, 0, 0,
	0, 224, 220, 0, 224, 0, 0, 0, 0, 0,
	31, 0, 45, 0, 38, 0, 0, 0, 90, 0,
	0, 0, 224, 0, 218, 23, 0, 26, 0, 28,
	203, 0, 224, 0, 46, 48, 53, 0, 51, 52,
	92, 0, 0, 130, 131, 132, 0, 0, 0, 0,
	160, 0, 151, 98, 99, 0, 222, 163, 164, 165,
	166, 196, 153, 0, 200, 201, 185, 90, 83, 171,
	0, 198, 199, 0, 161, 0, 21, 205, 0, 0,
	224, 214, 208, 209, 210, 211, 212, 213, 27, 29,
	30, 0, 0, 49, 54, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 117, 118, 119,
	120, 121, 122, 123, 95, 0, 0, 0, 0, 128,
	143, 144, 145, 0, 0, 0, 110, 0, 0, 154,
	14, 0, 0, 0, 171, 179, 0, 91, 128, 0,
	0, 219, 24, 0, 215, 90, 56, 58, 59, 69,
	67, 0, 47, 55, 50, 93, 94, 97, 111, 0,
	113, 115, 100, 101, 0, 125, 126, 0, 0, 0,
	0, 103, 105, 0, 109, 133, 134, 135, 136, 137,
	138, 139, 140, 141, 142, 96, 223, 127, 0, 195,
	146, 147, 0, 0, 0, 158, 155, 0, 189, 0,
	192, 189, 0, 187, 179, 19, 0, 0, 162, 224,
	25, 167, 0, 0, 0, 0, 72, 0, 0, 75,
	0, 0, 0, 84, 70, 0, 0, 68, 0, 112,
	114, 116, 0, 102, 104, 106, 0, 0, 129, 148,
	0, 150, 0, 156, 0, 0, 16, 0, 191, 193,
	17, 186, 0, 18, 180, 172, 173, 176, 22, 169,
	0, 57, 63, 0, 66, 73, 74, 76, 0, 78,
	0, 80, 81, 60, 0, 0, 0, 71, 61, 62,
	124, 0, 107, 149, 152, 159, 0, 0, 0, 188,
	0, 0, 175, 177, 178, 171, 0, 0, 0, 0,
	77, 79, 0, 0, 0, 108, 157, 0, 194, 181,
	174, 179, 170, 168, 64, 65, 0, 0, 0, 0,
	182, 0, 88, 0, 0, 190, 13, 0, 0, 85,
	0, 86, 87, 183, 0, 89, 0, 184,
}
var yyTok1 = [...]int{

	1, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 84, 77, 3,
	45, 47, 82, 80, 46, 81, 89, 83, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	66, 65, 67, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 85, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 76, 3, 86,
}
var yyTok2 = [...]int{

	2, 3, 4, 5, 6, 7, 8, 9, 10, 11,
	12, 13, 14, 15, 16, 17, 18, 19, 20, 21,
	22, 23, 24, 25, 26, 27, 28, 29, 30, 31,
	32, 33, 34, 35, 36, 37, 38, 39, 40, 41,
	42, 43, 44, 48, 49, 50, 51, 52, 53, 54,
	55, 56, 57, 58, 59, 60, 61, 62, 63, 64,
	68, 69, 70, 71, 72, 73, 74, 75, 78, 79,
	87, 88, 90, 91, 92, 93, 94, 95, 96, 97,
	98, 99, 100, 101, 102, 103, 104, 105, 106, 107,
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
		//line sql.y:171
		{
			setParseTree(yylex, yyDollar[1].statement)
		}
	case 2:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:177
		{
			yyVAL.statement = yyDollar[1].selStmt
		}
	case 13:
		yyDollar = yyS[yypt-13 : yypt+1]
		//line sql.y:193
		{
			yyVAL.selStmt = &Select{Comments: Comments(yyDollar[2].bytes2), Distinct: yyDollar[3].str, Hints: yyDollar[4].str, SelectExprs: yyDollar[5].selectExprs, From: yyDollar[7].tableExprs, Where: NewWhere(WhereStr, yyDollar[8].boolExpr), GroupBy: GroupBy(yyDollar[9].valExprs), Having: NewWhere(HavingStr, yyDollar[10].boolExpr), OrderBy: yyDollar[11].orderBy, Limit: yyDollar[12].limit, Lock: yyDollar[13].str}
		}
	case 14:
		yyDollar = yyS[yypt-6 : yypt+1]
		//line sql.y:197
		{
			if yyDollar[4].sqlID != "value" {
				yylex.Error("expecting value after next")
				return 1
			}
			yyVAL.selStmt = &Select{Comments: Comments(yyDollar[2].bytes2), SelectExprs: SelectExprs{Nextval{}}, From: TableExprs{&AliasedTableExpr{Expr: yyDollar[6].tableName}}}
		}
	case 15:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:205
		{
			yyVAL.selStmt = &Union{Type: yyDollar[2].str, Left: yyDollar[1].selStmt, Right: yyDollar[3].selStmt}
		}
	case 16:
		yyDollar = yyS[yypt-8 : yypt+1]
		//line sql.y:211
		{
			yyVAL.statement = &Insert{Comments: Comments(yyDollar[2].bytes2), Ignore: yyDollar[3].str, Table: yyDollar[5].tableName, Columns: yyDollar[6].columns, Rows: yyDollar[7].insRows, OnDup: OnDup(yyDollar[8].updateExprs)}
		}
	case 17:
		yyDollar = yyS[yypt-8 : yypt+1]
		//line sql.y:215
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
		//line sql.y:227
		{
			yyVAL.statement = &Update{Comments: Comments(yyDollar[2].bytes2), Table: yyDollar[3].tableName, Exprs: yyDollar[5].updateExprs, Where: NewWhere(WhereStr, yyDollar[6].boolExpr), OrderBy: yyDollar[7].orderBy, Limit: yyDollar[8].limit}
		}
	case 19:
		yyDollar = yyS[yypt-7 : yypt+1]
		//line sql.y:233
		{
			yyVAL.statement = &Delete{Comments: Comments(yyDollar[2].bytes2), Table: yyDollar[4].tableName, Where: NewWhere(WhereStr, yyDollar[5].boolExpr), OrderBy: yyDollar[6].orderBy, Limit: yyDollar[7].limit}
		}
	case 20:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:239
		{
			yyVAL.statement = &Set{Comments: Comments(yyDollar[2].bytes2), Exprs: yyDollar[3].updateExprs}
		}
	case 21:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:245
		{
			yyVAL.statement = &DDL{Action: CreateStr, NewName: yyDollar[4].sqlID}
		}
	case 22:
		yyDollar = yyS[yypt-8 : yypt+1]
		//line sql.y:249
		{
			// Change this to an alter statement
			yyVAL.statement = &DDL{Action: AlterStr, Table: yyDollar[7].sqlID, NewName: yyDollar[7].sqlID}
		}
	case 23:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:254
		{
			yyVAL.statement = &DDL{Action: CreateStr, NewName: SQLName(yyDollar[3].sqlID)}
		}
	case 24:
		yyDollar = yyS[yypt-6 : yypt+1]
		//line sql.y:260
		{
			yyVAL.statement = &DDL{Action: AlterStr, Table: yyDollar[4].sqlID, NewName: yyDollar[4].sqlID}
		}
	case 25:
		yyDollar = yyS[yypt-7 : yypt+1]
		//line sql.y:264
		{
			// Change this to a rename statement
			yyVAL.statement = &DDL{Action: RenameStr, Table: yyDollar[4].sqlID, NewName: yyDollar[7].sqlID}
		}
	case 26:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:269
		{
			yyVAL.statement = &DDL{Action: AlterStr, Table: SQLName(yyDollar[3].sqlID), NewName: SQLName(yyDollar[3].sqlID)}
		}
	case 27:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:275
		{
			yyVAL.statement = &DDL{Action: RenameStr, Table: yyDollar[3].sqlID, NewName: yyDollar[5].sqlID}
		}
	case 28:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:281
		{
			yyVAL.statement = &DDL{Action: DropStr, Table: yyDollar[4].sqlID}
		}
	case 29:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:285
		{
			// Change this to an alter statement
			yyVAL.statement = &DDL{Action: AlterStr, Table: yyDollar[5].sqlID, NewName: yyDollar[5].sqlID}
		}
	case 30:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:290
		{
			yyVAL.statement = &DDL{Action: DropStr, Table: SQLName(yyDollar[4].sqlID)}
		}
	case 31:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:296
		{
			yyVAL.statement = &DDL{Action: AlterStr, Table: yyDollar[3].sqlID, NewName: yyDollar[3].sqlID}
		}
	case 32:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:302
		{
			yyVAL.statement = &Other{}
		}
	case 33:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:306
		{
			yyVAL.statement = &Other{}
		}
	case 34:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:310
		{
			yyVAL.statement = &Other{}
		}
	case 35:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:315
		{
			setAllowComments(yylex, true)
		}
	case 36:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:319
		{
			yyVAL.bytes2 = yyDollar[2].bytes2
			setAllowComments(yylex, false)
		}
	case 37:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:325
		{
			yyVAL.bytes2 = nil
		}
	case 38:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:329
		{
			yyVAL.bytes2 = append(yyDollar[1].bytes2, yyDollar[2].bytes)
		}
	case 39:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:335
		{
			yyVAL.str = UnionStr
		}
	case 40:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:339
		{
			yyVAL.str = UnionAllStr
		}
	case 41:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:343
		{
			yyVAL.str = UnionDistinctStr
		}
	case 42:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:348
		{
			yyVAL.str = ""
		}
	case 43:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:352
		{
			yyVAL.str = DistinctStr
		}
	case 44:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:357
		{
			yyVAL.str = ""
		}
	case 45:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:361
		{
			yyVAL.str = StraightJoinHint
		}
	case 46:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:367
		{
			yyVAL.selectExprs = SelectExprs{yyDollar[1].selectExpr}
		}
	case 47:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:371
		{
			yyVAL.selectExprs = append(yyVAL.selectExprs, yyDollar[3].selectExpr)
		}
	case 48:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:377
		{
			yyVAL.selectExpr = &StarExpr{}
		}
	case 49:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:381
		{
			yyVAL.selectExpr = &NonStarExpr{Expr: yyDollar[1].expr, As: yyDollar[2].sqlID}
		}
	case 50:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:385
		{
			yyVAL.selectExpr = &StarExpr{TableName: yyDollar[1].sqlID}
		}
	case 51:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:391
		{
			yyVAL.expr = yyDollar[1].boolExpr
		}
	case 52:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:395
		{
			yyVAL.expr = yyDollar[1].valExpr
		}
	case 53:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:400
		{
			yyVAL.sqlID = ""
		}
	case 54:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:404
		{
			yyVAL.sqlID = yyDollar[1].sqlID
		}
	case 55:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:408
		{
			yyVAL.sqlID = yyDollar[2].sqlID
		}
	case 56:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:414
		{
			yyVAL.tableExprs = TableExprs{yyDollar[1].tableExpr}
		}
	case 57:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:418
		{
			yyVAL.tableExprs = append(yyVAL.tableExprs, yyDollar[3].tableExpr)
		}
	case 60:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:428
		{
			yyVAL.tableExpr = &AliasedTableExpr{Expr: yyDollar[1].tableName, As: yyDollar[2].sqlID, Hints: yyDollar[3].indexHints}
		}
	case 61:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:432
		{
			yyVAL.tableExpr = &AliasedTableExpr{Expr: yyDollar[1].subquery, As: yyDollar[3].sqlID}
		}
	case 62:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:436
		{
			yyVAL.tableExpr = &ParenTableExpr{Exprs: yyDollar[2].tableExprs}
		}
	case 63:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:449
		{
			yyVAL.tableExpr = &JoinTableExpr{LeftExpr: yyDollar[1].tableExpr, Join: yyDollar[2].str, RightExpr: yyDollar[3].tableExpr}
		}
	case 64:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:453
		{
			yyVAL.tableExpr = &JoinTableExpr{LeftExpr: yyDollar[1].tableExpr, Join: yyDollar[2].str, RightExpr: yyDollar[3].tableExpr, On: yyDollar[5].boolExpr}
		}
	case 65:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:457
		{
			yyVAL.tableExpr = &JoinTableExpr{LeftExpr: yyDollar[1].tableExpr, Join: yyDollar[2].str, RightExpr: yyDollar[3].tableExpr, On: yyDollar[5].boolExpr}
		}
	case 66:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:461
		{
			yyVAL.tableExpr = &JoinTableExpr{LeftExpr: yyDollar[1].tableExpr, Join: yyDollar[2].str, RightExpr: yyDollar[3].tableExpr}
		}
	case 67:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:466
		{
			yyVAL.empty = struct{}{}
		}
	case 68:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:468
		{
			yyVAL.empty = struct{}{}
		}
	case 69:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:471
		{
			yyVAL.sqlID = ""
		}
	case 70:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:475
		{
			yyVAL.sqlID = yyDollar[1].sqlID
		}
	case 71:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:479
		{
			yyVAL.sqlID = yyDollar[2].sqlID
		}
	case 72:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:485
		{
			yyVAL.str = JoinStr
		}
	case 73:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:489
		{
			yyVAL.str = JoinStr
		}
	case 74:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:493
		{
			yyVAL.str = JoinStr
		}
	case 75:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:497
		{
			yyVAL.str = StraightJoinStr
		}
	case 76:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:503
		{
			yyVAL.str = LeftJoinStr
		}
	case 77:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:507
		{
			yyVAL.str = LeftJoinStr
		}
	case 78:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:511
		{
			yyVAL.str = RightJoinStr
		}
	case 79:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:515
		{
			yyVAL.str = RightJoinStr
		}
	case 80:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:521
		{
			yyVAL.str = NaturalJoinStr
		}
	case 81:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:525
		{
			if yyDollar[2].str == LeftJoinStr {
				yyVAL.str = NaturalLeftJoinStr
			} else {
				yyVAL.str = NaturalRightJoinStr
			}
		}
	case 82:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:535
		{
			yyVAL.tableName = &TableName{Name: yyDollar[1].sqlID}
		}
	case 83:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:539
		{
			yyVAL.tableName = &TableName{Qualifier: yyDollar[1].sqlID, Name: yyDollar[3].sqlID}
		}
	case 84:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:544
		{
			yyVAL.indexHints = nil
		}
	case 85:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:548
		{
			yyVAL.indexHints = &IndexHints{Type: UseStr, Indexes: yyDollar[4].sqlIDs}
		}
	case 86:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:552
		{
			yyVAL.indexHints = &IndexHints{Type: IgnoreStr, Indexes: yyDollar[4].sqlIDs}
		}
	case 87:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:556
		{
			yyVAL.indexHints = &IndexHints{Type: ForceStr, Indexes: yyDollar[4].sqlIDs}
		}
	case 88:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:562
		{
			yyVAL.sqlIDs = []SQLName{yyDollar[1].sqlID}
		}
	case 89:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:566
		{
			yyVAL.sqlIDs = append(yyDollar[1].sqlIDs, yyDollar[3].sqlID)
		}
	case 90:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:571
		{
			yyVAL.boolExpr = nil
		}
	case 91:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:575
		{
			yyVAL.boolExpr = yyDollar[2].boolExpr
		}
	case 93:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:582
		{
			yyVAL.boolExpr = &AndExpr{Left: yyDollar[1].boolExpr, Right: yyDollar[3].boolExpr}
		}
	case 94:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:586
		{
			yyVAL.boolExpr = &OrExpr{Left: yyDollar[1].boolExpr, Right: yyDollar[3].boolExpr}
		}
	case 95:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:590
		{
			yyVAL.boolExpr = &NotExpr{Expr: yyDollar[2].boolExpr}
		}
	case 96:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:594
		{
			yyVAL.boolExpr = &ParenBoolExpr{Expr: yyDollar[2].boolExpr}
		}
	case 97:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:598
		{
			yyVAL.boolExpr = &IsExpr{Operator: yyDollar[3].str, Expr: yyDollar[1].boolExpr}
		}
	case 98:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:604
		{
			yyVAL.boolExpr = BoolVal(true)
		}
	case 99:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:608
		{
			yyVAL.boolExpr = BoolVal(false)
		}
	case 100:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:612
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: yyDollar[2].str, Right: yyDollar[3].valExpr}
		}
	case 101:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:616
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: InStr, Right: yyDollar[3].colTuple}
		}
	case 102:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:620
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: NotInStr, Right: yyDollar[4].colTuple}
		}
	case 103:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:624
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: LikeStr, Right: yyDollar[3].valExpr}
		}
	case 104:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:628
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: NotLikeStr, Right: yyDollar[4].valExpr}
		}
	case 105:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:632
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: RegexpStr, Right: yyDollar[3].valExpr}
		}
	case 106:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:636
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: NotRegexpStr, Right: yyDollar[4].valExpr}
		}
	case 107:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:640
		{
			yyVAL.boolExpr = &RangeCond{Left: yyDollar[1].valExpr, Operator: BetweenStr, From: yyDollar[3].valExpr, To: yyDollar[5].valExpr}
		}
	case 108:
		yyDollar = yyS[yypt-6 : yypt+1]
		//line sql.y:644
		{
			yyVAL.boolExpr = &RangeCond{Left: yyDollar[1].valExpr, Operator: NotBetweenStr, From: yyDollar[4].valExpr, To: yyDollar[6].valExpr}
		}
	case 109:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:648
		{
			yyVAL.boolExpr = &IsExpr{Operator: yyDollar[3].str, Expr: yyDollar[1].valExpr}
		}
	case 110:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:652
		{
			yyVAL.boolExpr = &ExistsExpr{Subquery: yyDollar[2].subquery}
		}
	case 111:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:658
		{
			yyVAL.str = IsNullStr
		}
	case 112:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:662
		{
			yyVAL.str = IsNotNullStr
		}
	case 113:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:666
		{
			yyVAL.str = IsTrueStr
		}
	case 114:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:670
		{
			yyVAL.str = IsNotTrueStr
		}
	case 115:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:674
		{
			yyVAL.str = IsFalseStr
		}
	case 116:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:678
		{
			yyVAL.str = IsNotFalseStr
		}
	case 117:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:684
		{
			yyVAL.str = EqualStr
		}
	case 118:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:688
		{
			yyVAL.str = LessThanStr
		}
	case 119:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:692
		{
			yyVAL.str = GreaterThanStr
		}
	case 120:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:696
		{
			yyVAL.str = LessEqualStr
		}
	case 121:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:700
		{
			yyVAL.str = GreaterEqualStr
		}
	case 122:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:704
		{
			yyVAL.str = NotEqualStr
		}
	case 123:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:708
		{
			yyVAL.str = NullSafeEqualStr
		}
	case 124:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:714
		{
			yyVAL.colTuple = ValTuple(yyDollar[2].valExprs)
		}
	case 125:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:718
		{
			yyVAL.colTuple = yyDollar[1].subquery
		}
	case 126:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:722
		{
			yyVAL.colTuple = ListArg(yyDollar[1].bytes)
		}
	case 127:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:728
		{
			yyVAL.subquery = &Subquery{yyDollar[2].selStmt}
		}
	case 128:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:734
		{
			yyVAL.valExprs = ValExprs{yyDollar[1].valExpr}
		}
	case 129:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:738
		{
			yyVAL.valExprs = append(yyDollar[1].valExprs, yyDollar[3].valExpr)
		}
	case 130:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:744
		{
			yyVAL.valExpr = yyDollar[1].valExpr
		}
	case 131:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:748
		{
			yyVAL.valExpr = yyDollar[1].colName
		}
	case 132:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:752
		{
			yyVAL.valExpr = yyDollar[1].rowTuple
		}
	case 133:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:756
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: BitAndStr, Right: yyDollar[3].valExpr}
		}
	case 134:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:760
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: BitOrStr, Right: yyDollar[3].valExpr}
		}
	case 135:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:764
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: BitXorStr, Right: yyDollar[3].valExpr}
		}
	case 136:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:768
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: PlusStr, Right: yyDollar[3].valExpr}
		}
	case 137:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:772
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: MinusStr, Right: yyDollar[3].valExpr}
		}
	case 138:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:776
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: MultStr, Right: yyDollar[3].valExpr}
		}
	case 139:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:780
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: DivStr, Right: yyDollar[3].valExpr}
		}
	case 140:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:784
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: ModStr, Right: yyDollar[3].valExpr}
		}
	case 141:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:788
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: ShiftLeftStr, Right: yyDollar[3].valExpr}
		}
	case 142:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:792
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: ShiftRightStr, Right: yyDollar[3].valExpr}
		}
	case 143:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:796
		{
			if num, ok := yyDollar[2].valExpr.(NumVal); ok {
				yyVAL.valExpr = num
			} else {
				yyVAL.valExpr = &UnaryExpr{Operator: UPlusStr, Expr: yyDollar[2].valExpr}
			}
		}
	case 144:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:804
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
		//line sql.y:817
		{
			yyVAL.valExpr = &UnaryExpr{Operator: TildaStr, Expr: yyDollar[2].valExpr}
		}
	case 146:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:821
		{
			// This rule prevents the usage of INTERVAL
			// as a function. If support is needed for that,
			// we'll need to revisit this. The solution
			// will be non-trivial because of grammar conflicts.
			yyVAL.valExpr = &IntervalExpr{Expr: yyDollar[2].valExpr, Unit: yyDollar[3].sqlID}
		}
	case 147:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:829
		{
			yyVAL.valExpr = &FuncExpr{Name: string(yyDollar[1].sqlID)}
		}
	case 148:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:833
		{
			yyVAL.valExpr = &FuncExpr{Name: string(yyDollar[1].sqlID), Exprs: yyDollar[3].selectExprs}
		}
	case 149:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:837
		{
			yyVAL.valExpr = &FuncExpr{Name: string(yyDollar[1].sqlID), Distinct: true, Exprs: yyDollar[4].selectExprs}
		}
	case 150:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:841
		{
			yyVAL.valExpr = &FuncExpr{Name: "if", Exprs: yyDollar[3].selectExprs}
		}
	case 151:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:845
		{
			yyVAL.valExpr = yyDollar[1].caseExpr
		}
	case 152:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:851
		{
			yyVAL.caseExpr = &CaseExpr{Expr: yyDollar[2].valExpr, Whens: yyDollar[3].whens, Else: yyDollar[4].valExpr}
		}
	case 153:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:856
		{
			yyVAL.valExpr = nil
		}
	case 154:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:860
		{
			yyVAL.valExpr = yyDollar[1].valExpr
		}
	case 155:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:866
		{
			yyVAL.whens = []*When{yyDollar[1].when}
		}
	case 156:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:870
		{
			yyVAL.whens = append(yyDollar[1].whens, yyDollar[2].when)
		}
	case 157:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:876
		{
			yyVAL.when = &When{Cond: yyDollar[2].boolExpr, Val: yyDollar[4].valExpr}
		}
	case 158:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:881
		{
			yyVAL.valExpr = nil
		}
	case 159:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:885
		{
			yyVAL.valExpr = yyDollar[2].valExpr
		}
	case 160:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:891
		{
			yyVAL.colName = &ColName{Name: yyDollar[1].sqlID}
		}
	case 161:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:895
		{
			yyVAL.colName = &ColName{Qualifier: &TableName{Name: yyDollar[1].sqlID}, Name: yyDollar[3].sqlID}
		}
	case 162:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:899
		{
			yyVAL.colName = &ColName{Qualifier: &TableName{Qualifier: yyDollar[1].sqlID, Name: yyDollar[3].sqlID}, Name: yyDollar[5].sqlID}
		}
	case 163:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:905
		{
			yyVAL.valExpr = StrVal(yyDollar[1].bytes)
		}
	case 164:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:909
		{
			yyVAL.valExpr = NumVal(yyDollar[1].bytes)
		}
	case 165:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:913
		{
			yyVAL.valExpr = ValArg(yyDollar[1].bytes)
		}
	case 166:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:917
		{
			yyVAL.valExpr = &NullVal{}
		}
	case 167:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:922
		{
			yyVAL.valExprs = nil
		}
	case 168:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:926
		{
			yyVAL.valExprs = yyDollar[3].valExprs
		}
	case 169:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:931
		{
			yyVAL.boolExpr = nil
		}
	case 170:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:935
		{
			yyVAL.boolExpr = yyDollar[2].boolExpr
		}
	case 171:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:940
		{
			yyVAL.orderBy = nil
		}
	case 172:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:944
		{
			yyVAL.orderBy = yyDollar[3].orderBy
		}
	case 173:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:950
		{
			yyVAL.orderBy = OrderBy{yyDollar[1].order}
		}
	case 174:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:954
		{
			yyVAL.orderBy = append(yyDollar[1].orderBy, yyDollar[3].order)
		}
	case 175:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:960
		{
			yyVAL.order = &Order{Expr: yyDollar[1].valExpr, Direction: yyDollar[2].str}
		}
	case 176:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:965
		{
			yyVAL.str = AscScr
		}
	case 177:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:969
		{
			yyVAL.str = AscScr
		}
	case 178:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:973
		{
			yyVAL.str = DescScr
		}
	case 179:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:978
		{
			yyVAL.limit = nil
		}
	case 180:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:982
		{
			yyVAL.limit = &Limit{Rowcount: yyDollar[2].valExpr}
		}
	case 181:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:986
		{
			yyVAL.limit = &Limit{Offset: yyDollar[2].valExpr, Rowcount: yyDollar[4].valExpr}
		}
	case 182:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:991
		{
			yyVAL.str = ""
		}
	case 183:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:995
		{
			yyVAL.str = ForUpdateStr
		}
	case 184:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:999
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
		//line sql.y:1012
		{
			yyVAL.columns = nil
		}
	case 186:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1016
		{
			yyVAL.columns = yyDollar[2].columns
		}
	case 187:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1022
		{
			yyVAL.columns = Columns{&NonStarExpr{Expr: yyDollar[1].colName}}
		}
	case 188:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1026
		{
			yyVAL.columns = append(yyVAL.columns, &NonStarExpr{Expr: yyDollar[3].colName})
		}
	case 189:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1031
		{
			yyVAL.updateExprs = nil
		}
	case 190:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:1035
		{
			yyVAL.updateExprs = yyDollar[5].updateExprs
		}
	case 191:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:1041
		{
			yyVAL.insRows = yyDollar[2].values
		}
	case 192:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1045
		{
			yyVAL.insRows = yyDollar[1].selStmt
		}
	case 193:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1051
		{
			yyVAL.values = Values{yyDollar[1].rowTuple}
		}
	case 194:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1055
		{
			yyVAL.values = append(yyDollar[1].values, yyDollar[3].rowTuple)
		}
	case 195:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1061
		{
			yyVAL.rowTuple = ValTuple(yyDollar[2].valExprs)
		}
	case 196:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1065
		{
			yyVAL.rowTuple = yyDollar[1].subquery
		}
	case 197:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1071
		{
			yyVAL.updateExprs = UpdateExprs{yyDollar[1].updateExpr}
		}
	case 198:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1075
		{
			yyVAL.updateExprs = append(yyDollar[1].updateExprs, yyDollar[3].updateExpr)
		}
	case 199:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1081
		{
			yyVAL.updateExpr = &UpdateExpr{Name: yyDollar[1].colName, Expr: yyDollar[3].valExpr}
		}
	case 202:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1090
		{
			yyVAL.empty = struct{}{}
		}
	case 203:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:1092
		{
			yyVAL.empty = struct{}{}
		}
	case 204:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1095
		{
			yyVAL.empty = struct{}{}
		}
	case 205:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1097
		{
			yyVAL.empty = struct{}{}
		}
	case 206:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1100
		{
			yyVAL.str = ""
		}
	case 207:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1102
		{
			yyVAL.str = IgnoreStr
		}
	case 208:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1106
		{
			yyVAL.empty = struct{}{}
		}
	case 209:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1108
		{
			yyVAL.empty = struct{}{}
		}
	case 210:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1110
		{
			yyVAL.empty = struct{}{}
		}
	case 211:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1112
		{
			yyVAL.empty = struct{}{}
		}
	case 212:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1114
		{
			yyVAL.empty = struct{}{}
		}
	case 213:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1116
		{
			yyVAL.empty = struct{}{}
		}
	case 214:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1119
		{
			yyVAL.empty = struct{}{}
		}
	case 215:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1121
		{
			yyVAL.empty = struct{}{}
		}
	case 216:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1124
		{
			yyVAL.empty = struct{}{}
		}
	case 217:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1126
		{
			yyVAL.empty = struct{}{}
		}
	case 218:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1129
		{
			yyVAL.empty = struct{}{}
		}
	case 219:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:1131
		{
			yyVAL.empty = struct{}{}
		}
	case 220:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1135
		{
			yyVAL.sqlID = SQLName(strings.ToLower(string(yyDollar[1].bytes)))
		}
	case 221:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1141
		{
			yyVAL.sqlID = SQLName(yyDollar[1].bytes)
		}
	case 222:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1147
		{
			if incNesting(yylex) {
				yylex.Error("max nesting level reached")
				return 1
			}
		}
	case 223:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1156
		{
			decNesting(yylex)
		}
	case 224:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1161
		{
			forceEOF(yylex)
		}
	}
	goto yystack /* stack new state and value */
}
