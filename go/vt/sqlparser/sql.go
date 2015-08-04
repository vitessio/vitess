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
const KEYRANGE = 57377
const ID = 57378
const STRING = 57379
const NUMBER = 57380
const VALUE_ARG = 57381
const LIST_ARG = 57382
const COMMENT = 57383
const LE = 57384
const GE = 57385
const NE = 57386
const NULL_SAFE_EQUAL = 57387
const UNION = 57388
const MINUS = 57389
const EXCEPT = 57390
const INTERSECT = 57391
const JOIN = 57392
const STRAIGHT_JOIN = 57393
const LEFT = 57394
const RIGHT = 57395
const INNER = 57396
const OUTER = 57397
const CROSS = 57398
const NATURAL = 57399
const USE = 57400
const FORCE = 57401
const ON = 57402
const OR = 57403
const AND = 57404
const NOT = 57405
const UNARY = 57406
const CASE = 57407
const WHEN = 57408
const THEN = 57409
const ELSE = 57410
const END = 57411
const CREATE = 57412
const ALTER = 57413
const DROP = 57414
const RENAME = 57415
const ANALYZE = 57416
const TABLE = 57417
const INDEX = 57418
const VIEW = 57419
const TO = 57420
const IGNORE = 57421
const IF = 57422
const UNIQUE = 57423
const USING = 57424
const SHOW = 57425
const DESCRIBE = 57426
const EXPLAIN = 57427

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
	"'('",
	"'='",
	"'<'",
	"'>'",
	"UNION",
	"MINUS",
	"EXCEPT",
	"INTERSECT",
	"','",
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
	"'|'",
	"'&'",
	"'+'",
	"'-'",
	"'*'",
	"'/'",
	"'%'",
	"'^'",
	"'~'",
	"UNARY",
	"'.'",
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
	"')'",
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
	79, 201,
	-2, 200,
}

const yyNprod = 205
const yyPrivate = 57344

var yyTokenNames []string
var yyStates []string

const yyLast = 718

var yyAct = [...]int{

	98, 299, 161, 369, 232, 87, 336, 63, 164, 253,
	94, 291, 92, 202, 213, 93, 244, 378, 163, 3,
	64, 132, 182, 235, 83, 297, 233, 82, 138, 137,
	264, 265, 266, 267, 268, 50, 269, 270, 66, 190,
	78, 72, 70, 38, 75, 40, 53, 316, 318, 41,
	43, 347, 44, 65, 28, 29, 30, 31, 88, 260,
	108, 51, 52, 233, 233, 126, 196, 346, 233, 345,
	233, 71, 233, 46, 47, 48, 233, 74, 317, 49,
	45, 130, 327, 245, 275, 194, 134, 138, 137, 197,
	245, 136, 289, 119, 165, 115, 160, 162, 166, 168,
	169, 147, 121, 329, 137, 233, 138, 137, 123, 342,
	73, 125, 292, 256, 176, 66, 129, 344, 66, 59,
	186, 185, 180, 148, 149, 150, 151, 152, 147, 222,
	65, 343, 314, 65, 167, 88, 208, 186, 184, 193,
	195, 192, 212, 210, 211, 220, 221, 209, 224, 225,
	226, 227, 228, 229, 230, 231, 187, 207, 150, 151,
	152, 147, 172, 356, 357, 117, 200, 313, 234, 236,
	237, 312, 223, 88, 88, 238, 292, 117, 113, 66,
	66, 235, 116, 310, 354, 331, 249, 242, 311, 255,
	91, 257, 206, 286, 65, 251, 248, 308, 239, 241,
	131, 215, 309, 252, 77, 183, 146, 145, 148, 149,
	150, 151, 152, 147, 216, 183, 14, 237, 274, 118,
	103, 278, 279, 276, 261, 91, 91, 258, 28, 29,
	30, 31, 277, 170, 171, 103, 173, 174, 282, 178,
	61, 112, 114, 88, 283, 132, 285, 61, 179, 262,
	103, 290, 103, 80, 273, 296, 73, 103, 294, 117,
	288, 295, 298, 135, 68, 206, 204, 91, 284, 61,
	122, 61, 91, 91, 107, 214, 306, 307, 215, 320,
	73, 322, 79, 324, 67, 351, 104, 105, 106, 325,
	330, 58, 328, 14, 217, 281, 218, 219, 66, 326,
	333, 375, 382, 334, 337, 91, 91, 188, 264, 265,
	266, 267, 268, 332, 269, 270, 247, 128, 91, 376,
	60, 56, 54, 206, 206, 300, 348, 341, 301, 254,
	76, 349, 350, 340, 81, 305, 183, 62, 338, 204,
	86, 381, 352, 60, 237, 365, 359, 60, 361, 358,
	360, 14, 214, 32, 120, 33, 366, 337, 271, 124,
	368, 367, 127, 370, 370, 370, 66, 371, 372, 34,
	35, 36, 37, 373, 377, 91, 379, 380, 133, 383,
	91, 65, 189, 384, 240, 385, 101, 39, 259, 191,
	42, 107, 69, 250, 110, 177, 374, 204, 204, 355,
	181, 102, 68, 104, 105, 106, 335, 339, 353, 304,
	287, 198, 103, 175, 199, 243, 205, 86, 100, 14,
	15, 16, 17, 146, 145, 148, 149, 150, 151, 152,
	147, 99, 293, 246, 90, 139, 89, 95, 96, 84,
	315, 203, 263, 97, 201, 85, 111, 18, 145, 148,
	149, 150, 151, 152, 147, 86, 86, 55, 27, 57,
	13, 109, 12, 11, 10, 9, 8, 233, 7, 6,
	5, 4, 2, 1, 0, 91, 0, 91, 101, 0,
	362, 363, 364, 107, 0, 0, 110, 0, 272, 205,
	0, 0, 0, 102, 68, 104, 105, 106, 0, 19,
	20, 22, 21, 23, 103, 0, 0, 0, 0, 0,
	0, 0, 24, 25, 26, 323, 0, 146, 145, 148,
	149, 150, 151, 152, 147, 86, 90, 0, 0, 95,
	96, 84, 0, 0, 14, 97, 0, 0, 111, 0,
	0, 302, 0, 0, 303, 0, 0, 205, 205, 101,
	0, 0, 0, 109, 107, 0, 0, 110, 319, 0,
	321, 0, 0, 0, 102, 68, 104, 105, 106, 101,
	0, 14, 0, 0, 107, 103, 0, 110, 0, 0,
	0, 0, 0, 0, 102, 68, 104, 105, 106, 0,
	0, 107, 0, 0, 110, 103, 0, 90, 0, 0,
	95, 96, 68, 104, 105, 106, 97, 0, 0, 111,
	0, 0, 103, 0, 0, 0, 0, 90, 0, 107,
	95, 96, 110, 0, 109, 0, 97, 0, 0, 111,
	68, 104, 105, 106, 0, 0, 0, 95, 96, 0,
	103, 0, 0, 97, 109, 0, 111, 0, 0, 0,
	0, 0, 0, 0, 140, 144, 142, 143, 0, 0,
	0, 109, 0, 0, 0, 95, 96, 0, 0, 0,
	0, 97, 0, 0, 111, 156, 157, 158, 159, 0,
	153, 154, 155, 0, 0, 0, 0, 0, 280, 109,
	146, 145, 148, 149, 150, 151, 152, 147, 0, 0,
	0, 141, 146, 145, 148, 149, 150, 151, 152, 147,
	146, 145, 148, 149, 150, 151, 152, 147,
}
var yyPact = [...]int{

	414, -1000, -1000, 178, -1000, -1000, -1000, -1000, -1000, -1000,
	-1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, -47,
	-42, -10, -17, -11, -1000, -1000, -1000, 346, 305, -1000,
	-1000, -1000, 303, -1000, 262, 233, 328, 228, -53, -20,
	220, -1000, -13, 220, -1000, 233, -55, 246, -55, 233,
	-1000, -1000, -1000, -1000, -1000, 458, -1000, 200, 233, 209,
	16, -1000, 233, 123, -1000, 172, -1000, 14, -1000, 233,
	34, 234, -1000, -1000, 233, -1000, -28, 233, 297, 51,
	220, -1000, 191, -1000, -1000, 244, 12, 40, 633, -1000,
	549, 529, -1000, -1000, -1000, 594, 594, 594, 189, 189,
	-1000, 189, 189, -1000, -1000, -1000, -1000, -1000, -1000, -1000,
	-1000, 594, -1000, 206, 228, 233, 326, 228, 594, 220,
	-1000, 287, -58, -1000, 53, -1000, 233, -1000, -1000, 233,
	-1000, 204, 458, -1000, -1000, 220, 74, 549, 549, 594,
	174, 273, 594, 594, 104, 594, 594, 594, 594, 594,
	594, 594, 594, -1000, -1000, -1000, -1000, -1000, -1000, -1000,
	-1000, 633, -38, 4, -31, 633, -1000, 566, -1000, -1000,
	366, 458, -1000, 346, 249, 2, 641, 288, 228, 228,
	205, -1000, 316, 549, -1000, 641, -1000, -1000, -1000, 48,
	220, -1000, -34, -1000, -1000, -1000, -1000, -1000, -1000, -1000,
	-1000, 195, 253, 235, 211, 5, -1000, -1000, -1000, -1000,
	-1000, 37, 641, -1000, 566, -1000, -1000, 174, 594, 594,
	641, 621, -1000, 270, 52, 378, -1000, 85, 85, 25,
	25, 25, -1000, -1000, -1000, 594, -1000, 641, -1000, -33,
	458, -33, 139, 9, -1000, 549, 47, 189, 178, 111,
	-29, -1000, 316, 310, 314, 40, 233, -1000, -1000, 233,
	-1000, 324, 204, 204, -1000, -1000, 142, 128, 116, 112,
	77, -16, -1000, 233, -25, 233, -31, -1000, 641, 448,
	594, -1000, 641, -1000, -33, -1000, 249, -2, -1000, 594,
	21, -1000, 260, 131, -1000, -1000, -1000, 228, 310, -1000,
	594, 594, -1000, -1000, 321, 313, 253, 44, -1000, 76,
	-1000, 62, -1000, -1000, -1000, -1000, -22, -24, -40, -1000,
	-1000, -1000, -1000, 594, 641, -1000, -75, -1000, 641, 594,
	254, 189, -1000, -1000, 354, 130, -1000, 137, -1000, 316,
	549, 594, 549, -1000, -1000, 189, 189, 189, 641, -1000,
	641, 338, -1000, 594, 594, -1000, -1000, -1000, 310, 40,
	127, 40, 220, 220, 220, 228, 641, -1000, 285, -37,
	-1000, -37, -37, 123, -1000, 334, 281, -1000, 220, -1000,
	-1000, -1000, 220, -1000, 220, -1000,
}
var yyPgo = [...]int{

	0, 473, 472, 18, 471, 470, 469, 468, 466, 465,
	464, 463, 462, 460, 353, 459, 458, 457, 27, 24,
	445, 444, 13, 442, 441, 119, 440, 3, 22, 5,
	436, 435, 433, 12, 2, 14, 8, 432, 10, 431,
	60, 15, 418, 415, 16, 413, 410, 409, 407, 9,
	406, 6, 399, 1, 396, 395, 393, 11, 7, 20,
	204, 392, 390, 389, 388, 387, 382, 0, 378, 284,
	358, 35, 355, 134, 4,
}
var yyR1 = [...]int{

	0, 1, 2, 2, 2, 2, 2, 2, 2, 2,
	2, 2, 2, 3, 3, 4, 4, 5, 6, 7,
	8, 8, 8, 9, 9, 9, 10, 11, 11, 11,
	12, 13, 13, 13, 72, 14, 15, 15, 16, 16,
	16, 16, 16, 17, 17, 18, 18, 19, 19, 19,
	20, 20, 68, 68, 68, 21, 21, 22, 22, 22,
	22, 70, 70, 70, 23, 23, 23, 23, 23, 23,
	23, 23, 23, 24, 24, 24, 25, 25, 26, 26,
	26, 26, 27, 27, 28, 28, 29, 29, 29, 29,
	29, 30, 30, 30, 30, 30, 30, 30, 30, 30,
	30, 30, 31, 31, 31, 31, 31, 31, 31, 35,
	35, 35, 40, 36, 36, 34, 34, 34, 34, 34,
	34, 34, 34, 34, 34, 34, 34, 34, 34, 34,
	34, 34, 34, 34, 39, 39, 42, 45, 45, 43,
	43, 44, 46, 46, 41, 41, 33, 33, 33, 33,
	47, 47, 48, 48, 49, 49, 50, 50, 51, 52,
	52, 52, 53, 53, 53, 54, 54, 54, 55, 55,
	56, 56, 57, 57, 32, 32, 37, 37, 38, 38,
	58, 58, 59, 60, 60, 61, 61, 62, 62, 63,
	63, 63, 63, 63, 64, 64, 65, 65, 66, 66,
	67, 69, 73, 74, 71,
}
var yyR2 = [...]int{

	0, 1, 1, 1, 1, 1, 1, 1, 1, 1,
	1, 1, 1, 12, 3, 7, 7, 8, 7, 3,
	5, 8, 4, 6, 7, 4, 5, 4, 5, 5,
	3, 2, 2, 2, 0, 2, 0, 2, 1, 2,
	1, 1, 1, 0, 1, 1, 3, 1, 2, 3,
	1, 1, 0, 1, 2, 1, 3, 3, 3, 3,
	5, 0, 1, 2, 1, 1, 2, 3, 2, 3,
	2, 2, 2, 1, 3, 1, 1, 3, 0, 5,
	5, 5, 1, 3, 0, 2, 1, 3, 3, 2,
	3, 3, 3, 4, 3, 4, 5, 6, 3, 4,
	2, 6, 1, 1, 1, 1, 1, 1, 1, 3,
	1, 1, 3, 1, 3, 1, 1, 1, 3, 3,
	3, 3, 3, 3, 3, 3, 2, 2, 2, 3,
	4, 5, 4, 1, 1, 1, 5, 0, 1, 1,
	2, 4, 0, 2, 1, 3, 1, 1, 1, 1,
	0, 3, 0, 2, 0, 3, 1, 3, 2, 0,
	1, 1, 0, 2, 4, 0, 2, 4, 0, 3,
	1, 3, 0, 5, 2, 1, 1, 3, 3, 1,
	1, 3, 3, 0, 2, 0, 3, 0, 1, 1,
	1, 1, 1, 1, 0, 1, 0, 1, 0, 2,
	1, 1, 1, 1, 0,
}
var yyChk = [...]int{

	-1000, -1, -2, -3, -4, -5, -6, -7, -8, -9,
	-10, -11, -12, -13, 5, 6, 7, 8, 33, 85,
	86, 88, 87, 89, 98, 99, 100, -16, 50, 51,
	52, 53, -14, -72, -14, -14, -14, -14, 90, -65,
	92, 96, -62, 92, 94, 90, 90, 91, 92, 90,
	-71, -71, -71, -3, 17, -17, 18, -15, 29, -25,
	-69, 36, 9, -58, -59, -41, -67, -69, 36, -61,
	95, 91, -67, 36, 90, -67, -69, -60, 95, 36,
	-60, -69, -18, -19, 73, -20, -69, -29, -34, -30,
	68, -73, -33, -41, -38, 71, 72, 77, -67, -39,
	-42, 20, 35, 46, 37, 38, 39, 25, -40, 95,
	28, 80, 41, -25, 33, 79, -25, 54, 47, 79,
	-69, 68, 36, -71, -69, -71, 93, -69, 20, 65,
	-67, 9, 54, -68, -67, 19, 79, 67, 66, -31,
	21, 68, 23, 24, 22, 70, 69, 76, 71, 72,
	73, 74, 75, 47, 48, 49, 42, 43, 44, 45,
	-29, -34, -29, -3, -36, -34, -34, -73, -34, -34,
	-73, -73, -40, -73, -73, -45, -34, -55, 33, -73,
	-58, -69, -28, 10, -59, -34, -67, -71, 20, -66,
	97, -63, 88, 86, 32, 87, 13, 36, -69, -69,
	-71, -21, -22, -24, -73, -69, -40, -19, -67, 73,
	-29, -29, -34, -35, -73, -40, 40, 21, 23, 24,
	-34, -34, 25, 68, -34, -34, -34, -34, -34, -34,
	-34, -34, -74, 101, -74, 54, -74, -34, -74, -18,
	18, -18, -33, -43, -44, 81, -32, 28, -3, -58,
	-56, -41, -28, -49, 13, -29, 65, -67, -71, -64,
	93, -28, 54, -23, 55, 56, 57, 58, 59, 61,
	62, -70, -69, 19, -22, 79, -36, -35, -34, -34,
	67, 25, -34, -74, -18, -74, 54, -46, -44, 83,
	-29, -57, 65, -37, -38, -57, -74, 54, -49, -53,
	15, 14, -69, -69, -47, 11, -22, -22, 55, 60,
	55, 60, 55, 55, 55, -26, 63, 94, 64, -69,
	-74, -69, -74, 67, -34, -74, -33, 84, -34, 82,
	30, 54, -41, -53, -34, -50, -51, -34, -71, -48,
	12, 14, 65, 55, 55, 91, 91, 91, -34, -74,
	-34, 31, -38, 54, 54, -52, 26, 27, -49, -29,
	-36, -29, -73, -73, -73, 7, -34, -51, -53, -27,
	-67, -27, -27, -58, -54, 16, 34, -74, 54, -74,
	-74, 7, 21, -67, -67, -67,
}
var yyDef = [...]int{

	0, -2, 1, 2, 3, 4, 5, 6, 7, 8,
	9, 10, 11, 12, 34, 34, 34, 34, 34, 196,
	187, 0, 0, 0, 204, 204, 204, 0, 38, 40,
	41, 42, 43, 36, 0, 0, 0, 0, 185, 0,
	0, 197, 0, 0, 188, 0, 183, 0, 183, 0,
	31, 32, 33, 14, 39, 0, 44, 35, 0, 0,
	76, 201, 0, 19, 180, 0, 144, 0, -2, 0,
	0, 0, 204, 200, 0, 204, 0, 0, 0, 0,
	0, 30, 0, 45, 47, 52, 0, 50, 51, 86,
	0, 0, 115, 116, 117, 0, 0, 0, 144, 0,
	133, 0, 0, 202, 146, 147, 148, 149, 179, 134,
	135, 137, 37, 168, 0, 0, 84, 0, 0, 0,
	204, 0, 198, 22, 0, 25, 0, 27, 184, 0,
	204, 0, 0, 48, 53, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 102, 103, 104, 105, 106, 107, 108,
	89, 0, 0, 0, 0, 113, 126, 0, 127, 128,
	0, 0, 100, 0, 0, 0, 138, 0, 0, 0,
	84, 77, 154, 0, 181, 182, 145, 20, 186, 0,
	0, 204, 194, 189, 190, 191, 192, 193, 26, 28,
	29, 84, 55, 61, 0, 73, 75, 46, 54, 49,
	87, 88, 91, 92, 0, 110, 111, 0, 0, 0,
	94, 0, 98, 0, 118, 119, 120, 121, 122, 123,
	124, 125, 90, 203, 112, 0, 178, 113, 129, 0,
	0, 0, 0, 142, 139, 0, 172, 0, 175, 172,
	0, 170, 154, 162, 0, 85, 0, 199, 23, 0,
	195, 150, 0, 0, 64, 65, 0, 0, 0, 0,
	0, 78, 62, 0, 0, 0, 0, 93, 95, 0,
	0, 99, 114, 130, 0, 132, 0, 0, 140, 0,
	0, 15, 0, 174, 176, 16, 169, 0, 162, 18,
	0, 0, 204, 24, 152, 0, 56, 59, 66, 0,
	68, 0, 70, 71, 72, 57, 0, 0, 0, 63,
	58, 74, 109, 0, 96, 131, 0, 136, 143, 0,
	0, 0, 171, 17, 163, 155, 156, 159, 21, 154,
	0, 0, 0, 67, 69, 0, 0, 0, 97, 101,
	141, 0, 177, 0, 0, 158, 160, 161, 162, 153,
	151, 60, 0, 0, 0, 0, 164, 157, 165, 0,
	82, 0, 0, 173, 13, 0, 0, 79, 0, 80,
	81, 166, 0, 83, 0, 167,
}
var yyTok1 = [...]int{

	1, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 75, 70, 3,
	46, 101, 73, 71, 54, 72, 79, 74, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	48, 47, 49, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 76, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 69, 3, 77,
}
var yyTok2 = [...]int{

	2, 3, 4, 5, 6, 7, 8, 9, 10, 11,
	12, 13, 14, 15, 16, 17, 18, 19, 20, 21,
	22, 23, 24, 25, 26, 27, 28, 29, 30, 31,
	32, 33, 34, 35, 36, 37, 38, 39, 40, 41,
	42, 43, 44, 45, 50, 51, 52, 53, 55, 56,
	57, 58, 59, 60, 61, 62, 63, 64, 65, 66,
	67, 68, 78, 80, 81, 82, 83, 84, 85, 86,
	87, 88, 89, 90, 91, 92, 93, 94, 95, 96,
	97, 98, 99, 100,
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
		//line sql.y:159
		{
			setParseTree(yylex, yyDollar[1].statement)
		}
	case 2:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:165
		{
			yyVAL.statement = yyDollar[1].selStmt
		}
	case 13:
		yyDollar = yyS[yypt-12 : yypt+1]
		//line sql.y:181
		{
			yyVAL.selStmt = &Select{Comments: Comments(yyDollar[2].bytes2), Distinct: yyDollar[3].str, SelectExprs: yyDollar[4].selectExprs, From: yyDollar[6].tableExprs, Where: NewWhere(AST_WHERE, yyDollar[7].boolExpr), GroupBy: GroupBy(yyDollar[8].valExprs), Having: NewWhere(AST_HAVING, yyDollar[9].boolExpr), OrderBy: yyDollar[10].orderBy, Limit: yyDollar[11].limit, Lock: yyDollar[12].str}
		}
	case 14:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:185
		{
			yyVAL.selStmt = &Union{Type: yyDollar[2].str, Left: yyDollar[1].selStmt, Right: yyDollar[3].selStmt}
		}
	case 15:
		yyDollar = yyS[yypt-7 : yypt+1]
		//line sql.y:191
		{
			yyVAL.statement = &Insert{Comments: Comments(yyDollar[2].bytes2), Table: yyDollar[4].tableName, Columns: yyDollar[5].columns, Rows: yyDollar[6].insRows, OnDup: OnDup(yyDollar[7].updateExprs)}
		}
	case 16:
		yyDollar = yyS[yypt-7 : yypt+1]
		//line sql.y:195
		{
			cols := make(Columns, 0, len(yyDollar[6].updateExprs))
			vals := make(ValTuple, 0, len(yyDollar[6].updateExprs))
			for _, col := range yyDollar[6].updateExprs {
				cols = append(cols, &NonStarExpr{Expr: col.Name})
				vals = append(vals, col.Expr)
			}
			yyVAL.statement = &Insert{Comments: Comments(yyDollar[2].bytes2), Table: yyDollar[4].tableName, Columns: cols, Rows: Values{vals}, OnDup: OnDup(yyDollar[7].updateExprs)}
		}
	case 17:
		yyDollar = yyS[yypt-8 : yypt+1]
		//line sql.y:207
		{
			yyVAL.statement = &Update{Comments: Comments(yyDollar[2].bytes2), Table: yyDollar[3].tableName, Exprs: yyDollar[5].updateExprs, Where: NewWhere(AST_WHERE, yyDollar[6].boolExpr), OrderBy: yyDollar[7].orderBy, Limit: yyDollar[8].limit}
		}
	case 18:
		yyDollar = yyS[yypt-7 : yypt+1]
		//line sql.y:213
		{
			yyVAL.statement = &Delete{Comments: Comments(yyDollar[2].bytes2), Table: yyDollar[4].tableName, Where: NewWhere(AST_WHERE, yyDollar[5].boolExpr), OrderBy: yyDollar[6].orderBy, Limit: yyDollar[7].limit}
		}
	case 19:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:219
		{
			yyVAL.statement = &Set{Comments: Comments(yyDollar[2].bytes2), Exprs: yyDollar[3].updateExprs}
		}
	case 20:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:225
		{
			yyVAL.statement = &DDL{Action: AST_CREATE, NewName: yyDollar[4].sqlID}
		}
	case 21:
		yyDollar = yyS[yypt-8 : yypt+1]
		//line sql.y:229
		{
			// Change this to an alter statement
			yyVAL.statement = &DDL{Action: AST_ALTER, Table: yyDollar[7].sqlID, NewName: yyDollar[7].sqlID}
		}
	case 22:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:234
		{
			yyVAL.statement = &DDL{Action: AST_CREATE, NewName: SQLName(yyDollar[3].sqlID)}
		}
	case 23:
		yyDollar = yyS[yypt-6 : yypt+1]
		//line sql.y:240
		{
			yyVAL.statement = &DDL{Action: AST_ALTER, Table: yyDollar[4].sqlID, NewName: yyDollar[4].sqlID}
		}
	case 24:
		yyDollar = yyS[yypt-7 : yypt+1]
		//line sql.y:244
		{
			// Change this to a rename statement
			yyVAL.statement = &DDL{Action: AST_RENAME, Table: yyDollar[4].sqlID, NewName: yyDollar[7].sqlID}
		}
	case 25:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:249
		{
			yyVAL.statement = &DDL{Action: AST_ALTER, Table: SQLName(yyDollar[3].sqlID), NewName: SQLName(yyDollar[3].sqlID)}
		}
	case 26:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:255
		{
			yyVAL.statement = &DDL{Action: AST_RENAME, Table: yyDollar[3].sqlID, NewName: yyDollar[5].sqlID}
		}
	case 27:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:261
		{
			yyVAL.statement = &DDL{Action: AST_DROP, Table: yyDollar[4].sqlID}
		}
	case 28:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:265
		{
			// Change this to an alter statement
			yyVAL.statement = &DDL{Action: AST_ALTER, Table: yyDollar[5].sqlID, NewName: yyDollar[5].sqlID}
		}
	case 29:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:270
		{
			yyVAL.statement = &DDL{Action: AST_DROP, Table: SQLName(yyDollar[4].sqlID)}
		}
	case 30:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:276
		{
			yyVAL.statement = &DDL{Action: AST_ALTER, Table: yyDollar[3].sqlID, NewName: yyDollar[3].sqlID}
		}
	case 31:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:282
		{
			yyVAL.statement = &Other{}
		}
	case 32:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:286
		{
			yyVAL.statement = &Other{}
		}
	case 33:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:290
		{
			yyVAL.statement = &Other{}
		}
	case 34:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:295
		{
			setAllowComments(yylex, true)
		}
	case 35:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:299
		{
			yyVAL.bytes2 = yyDollar[2].bytes2
			setAllowComments(yylex, false)
		}
	case 36:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:305
		{
			yyVAL.bytes2 = nil
		}
	case 37:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:309
		{
			yyVAL.bytes2 = append(yyDollar[1].bytes2, yyDollar[2].bytes)
		}
	case 38:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:315
		{
			yyVAL.str = AST_UNION
		}
	case 39:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:319
		{
			yyVAL.str = AST_UNION_ALL
		}
	case 40:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:323
		{
			yyVAL.str = AST_SET_MINUS
		}
	case 41:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:327
		{
			yyVAL.str = AST_EXCEPT
		}
	case 42:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:331
		{
			yyVAL.str = AST_INTERSECT
		}
	case 43:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:336
		{
			yyVAL.str = ""
		}
	case 44:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:340
		{
			yyVAL.str = AST_DISTINCT
		}
	case 45:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:346
		{
			yyVAL.selectExprs = SelectExprs{yyDollar[1].selectExpr}
		}
	case 46:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:350
		{
			yyVAL.selectExprs = append(yyVAL.selectExprs, yyDollar[3].selectExpr)
		}
	case 47:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:356
		{
			yyVAL.selectExpr = &StarExpr{}
		}
	case 48:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:360
		{
			yyVAL.selectExpr = &NonStarExpr{Expr: yyDollar[1].expr, As: yyDollar[2].sqlID}
		}
	case 49:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:364
		{
			yyVAL.selectExpr = &StarExpr{TableName: yyDollar[1].sqlID}
		}
	case 50:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:370
		{
			yyVAL.expr = yyDollar[1].boolExpr
		}
	case 51:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:374
		{
			yyVAL.expr = yyDollar[1].valExpr
		}
	case 52:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:379
		{
			yyVAL.sqlID = ""
		}
	case 53:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:383
		{
			yyVAL.sqlID = yyDollar[1].sqlID
		}
	case 54:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:387
		{
			yyVAL.sqlID = yyDollar[2].sqlID
		}
	case 55:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:393
		{
			yyVAL.tableExprs = TableExprs{yyDollar[1].tableExpr}
		}
	case 56:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:397
		{
			yyVAL.tableExprs = append(yyVAL.tableExprs, yyDollar[3].tableExpr)
		}
	case 57:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:403
		{
			yyVAL.tableExpr = &AliasedTableExpr{Expr: yyDollar[1].smTableExpr, As: yyDollar[2].sqlID, Hints: yyDollar[3].indexHints}
		}
	case 58:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:407
		{
			yyVAL.tableExpr = &ParenTableExpr{Expr: yyDollar[2].tableExpr}
		}
	case 59:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:411
		{
			yyVAL.tableExpr = &JoinTableExpr{LeftExpr: yyDollar[1].tableExpr, Join: yyDollar[2].str, RightExpr: yyDollar[3].tableExpr}
		}
	case 60:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:415
		{
			yyVAL.tableExpr = &JoinTableExpr{LeftExpr: yyDollar[1].tableExpr, Join: yyDollar[2].str, RightExpr: yyDollar[3].tableExpr, On: yyDollar[5].boolExpr}
		}
	case 61:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:420
		{
			yyVAL.sqlID = ""
		}
	case 62:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:424
		{
			yyVAL.sqlID = yyDollar[1].sqlID
		}
	case 63:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:428
		{
			yyVAL.sqlID = yyDollar[2].sqlID
		}
	case 64:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:434
		{
			yyVAL.str = AST_JOIN
		}
	case 65:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:438
		{
			yyVAL.str = AST_STRAIGHT_JOIN
		}
	case 66:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:442
		{
			yyVAL.str = AST_LEFT_JOIN
		}
	case 67:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:446
		{
			yyVAL.str = AST_LEFT_JOIN
		}
	case 68:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:450
		{
			yyVAL.str = AST_RIGHT_JOIN
		}
	case 69:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:454
		{
			yyVAL.str = AST_RIGHT_JOIN
		}
	case 70:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:458
		{
			yyVAL.str = AST_JOIN
		}
	case 71:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:462
		{
			yyVAL.str = AST_CROSS_JOIN
		}
	case 72:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:466
		{
			yyVAL.str = AST_NATURAL_JOIN
		}
	case 73:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:472
		{
			yyVAL.smTableExpr = &TableName{Name: yyDollar[1].sqlID}
		}
	case 74:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:476
		{
			yyVAL.smTableExpr = &TableName{Qualifier: yyDollar[1].sqlID, Name: yyDollar[3].sqlID}
		}
	case 75:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:480
		{
			yyVAL.smTableExpr = yyDollar[1].subquery
		}
	case 76:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:486
		{
			yyVAL.tableName = &TableName{Name: yyDollar[1].sqlID}
		}
	case 77:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:490
		{
			yyVAL.tableName = &TableName{Qualifier: yyDollar[1].sqlID, Name: yyDollar[3].sqlID}
		}
	case 78:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:495
		{
			yyVAL.indexHints = nil
		}
	case 79:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:499
		{
			yyVAL.indexHints = &IndexHints{Type: AST_USE, Indexes: yyDollar[4].sqlIDs}
		}
	case 80:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:503
		{
			yyVAL.indexHints = &IndexHints{Type: AST_IGNORE, Indexes: yyDollar[4].sqlIDs}
		}
	case 81:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:507
		{
			yyVAL.indexHints = &IndexHints{Type: AST_FORCE, Indexes: yyDollar[4].sqlIDs}
		}
	case 82:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:513
		{
			yyVAL.sqlIDs = []SQLName{yyDollar[1].sqlID}
		}
	case 83:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:517
		{
			yyVAL.sqlIDs = append(yyDollar[1].sqlIDs, yyDollar[3].sqlID)
		}
	case 84:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:522
		{
			yyVAL.boolExpr = nil
		}
	case 85:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:526
		{
			yyVAL.boolExpr = yyDollar[2].boolExpr
		}
	case 87:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:533
		{
			yyVAL.boolExpr = &AndExpr{Left: yyDollar[1].boolExpr, Right: yyDollar[3].boolExpr}
		}
	case 88:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:537
		{
			yyVAL.boolExpr = &OrExpr{Left: yyDollar[1].boolExpr, Right: yyDollar[3].boolExpr}
		}
	case 89:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:541
		{
			yyVAL.boolExpr = &NotExpr{Expr: yyDollar[2].boolExpr}
		}
	case 90:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:545
		{
			yyVAL.boolExpr = &ParenBoolExpr{Expr: yyDollar[2].boolExpr}
		}
	case 91:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:551
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: yyDollar[2].str, Right: yyDollar[3].valExpr}
		}
	case 92:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:555
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: AST_IN, Right: yyDollar[3].colTuple}
		}
	case 93:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:559
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: AST_NOT_IN, Right: yyDollar[4].colTuple}
		}
	case 94:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:563
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: AST_LIKE, Right: yyDollar[3].valExpr}
		}
	case 95:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:567
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: AST_NOT_LIKE, Right: yyDollar[4].valExpr}
		}
	case 96:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:571
		{
			yyVAL.boolExpr = &RangeCond{Left: yyDollar[1].valExpr, Operator: AST_BETWEEN, From: yyDollar[3].valExpr, To: yyDollar[5].valExpr}
		}
	case 97:
		yyDollar = yyS[yypt-6 : yypt+1]
		//line sql.y:575
		{
			yyVAL.boolExpr = &RangeCond{Left: yyDollar[1].valExpr, Operator: AST_NOT_BETWEEN, From: yyDollar[4].valExpr, To: yyDollar[6].valExpr}
		}
	case 98:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:579
		{
			yyVAL.boolExpr = &NullCheck{Operator: AST_IS_NULL, Expr: yyDollar[1].valExpr}
		}
	case 99:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:583
		{
			yyVAL.boolExpr = &NullCheck{Operator: AST_IS_NOT_NULL, Expr: yyDollar[1].valExpr}
		}
	case 100:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:587
		{
			yyVAL.boolExpr = &ExistsExpr{Subquery: yyDollar[2].subquery}
		}
	case 101:
		yyDollar = yyS[yypt-6 : yypt+1]
		//line sql.y:591
		{
			yyVAL.boolExpr = &KeyrangeExpr{Start: yyDollar[3].valExpr, End: yyDollar[5].valExpr}
		}
	case 102:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:597
		{
			yyVAL.str = AST_EQ
		}
	case 103:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:601
		{
			yyVAL.str = AST_LT
		}
	case 104:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:605
		{
			yyVAL.str = AST_GT
		}
	case 105:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:609
		{
			yyVAL.str = AST_LE
		}
	case 106:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:613
		{
			yyVAL.str = AST_GE
		}
	case 107:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:617
		{
			yyVAL.str = AST_NE
		}
	case 108:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:621
		{
			yyVAL.str = AST_NSE
		}
	case 109:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:627
		{
			yyVAL.colTuple = ValTuple(yyDollar[2].valExprs)
		}
	case 110:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:631
		{
			yyVAL.colTuple = yyDollar[1].subquery
		}
	case 111:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:635
		{
			yyVAL.colTuple = ListArg(yyDollar[1].bytes)
		}
	case 112:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:641
		{
			yyVAL.subquery = &Subquery{yyDollar[2].selStmt}
		}
	case 113:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:647
		{
			yyVAL.valExprs = ValExprs{yyDollar[1].valExpr}
		}
	case 114:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:651
		{
			yyVAL.valExprs = append(yyDollar[1].valExprs, yyDollar[3].valExpr)
		}
	case 115:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:657
		{
			yyVAL.valExpr = yyDollar[1].valExpr
		}
	case 116:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:661
		{
			yyVAL.valExpr = yyDollar[1].colName
		}
	case 117:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:665
		{
			yyVAL.valExpr = yyDollar[1].rowTuple
		}
	case 118:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:669
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: AST_BITAND, Right: yyDollar[3].valExpr}
		}
	case 119:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:673
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: AST_BITOR, Right: yyDollar[3].valExpr}
		}
	case 120:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:677
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: AST_BITXOR, Right: yyDollar[3].valExpr}
		}
	case 121:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:681
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: AST_PLUS, Right: yyDollar[3].valExpr}
		}
	case 122:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:685
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: AST_MINUS, Right: yyDollar[3].valExpr}
		}
	case 123:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:689
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: AST_MULT, Right: yyDollar[3].valExpr}
		}
	case 124:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:693
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: AST_DIV, Right: yyDollar[3].valExpr}
		}
	case 125:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:697
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: AST_MOD, Right: yyDollar[3].valExpr}
		}
	case 126:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:701
		{
			if num, ok := yyDollar[2].valExpr.(NumVal); ok {
				yyVAL.valExpr = num
			} else {
				yyVAL.valExpr = &UnaryExpr{Operator: AST_PLUS, Expr: yyDollar[2].valExpr}
			}
		}
	case 127:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:709
		{
			if num, ok := yyDollar[2].valExpr.(NumVal); ok {
				// Handle double negative
				if num[0] == '-' {
					yyVAL.valExpr = num[1:]
				} else {
					yyVAL.valExpr = append(NumVal("-"), num...)
				}
			} else {
				yyVAL.valExpr = &UnaryExpr{Operator: AST_MINUS, Expr: yyDollar[2].valExpr}
			}
		}
	case 128:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:722
		{
			yyVAL.valExpr = &UnaryExpr{Operator: AST_TILDA, Expr: yyDollar[2].valExpr}
		}
	case 129:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:726
		{
			yyVAL.valExpr = &FuncExpr{Name: string(yyDollar[1].sqlID)}
		}
	case 130:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:730
		{
			yyVAL.valExpr = &FuncExpr{Name: string(yyDollar[1].sqlID), Exprs: yyDollar[3].selectExprs}
		}
	case 131:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:734
		{
			yyVAL.valExpr = &FuncExpr{Name: string(yyDollar[1].sqlID), Distinct: true, Exprs: yyDollar[4].selectExprs}
		}
	case 132:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:738
		{
			yyVAL.valExpr = &FuncExpr{Name: yyDollar[1].str, Exprs: yyDollar[3].selectExprs}
		}
	case 133:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:742
		{
			yyVAL.valExpr = yyDollar[1].caseExpr
		}
	case 134:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:748
		{
			yyVAL.str = "if"
		}
	case 135:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:752
		{
			yyVAL.str = "values"
		}
	case 136:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:758
		{
			yyVAL.caseExpr = &CaseExpr{Expr: yyDollar[2].valExpr, Whens: yyDollar[3].whens, Else: yyDollar[4].valExpr}
		}
	case 137:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:763
		{
			yyVAL.valExpr = nil
		}
	case 138:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:767
		{
			yyVAL.valExpr = yyDollar[1].valExpr
		}
	case 139:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:773
		{
			yyVAL.whens = []*When{yyDollar[1].when}
		}
	case 140:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:777
		{
			yyVAL.whens = append(yyDollar[1].whens, yyDollar[2].when)
		}
	case 141:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:783
		{
			yyVAL.when = &When{Cond: yyDollar[2].boolExpr, Val: yyDollar[4].valExpr}
		}
	case 142:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:788
		{
			yyVAL.valExpr = nil
		}
	case 143:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:792
		{
			yyVAL.valExpr = yyDollar[2].valExpr
		}
	case 144:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:798
		{
			yyVAL.colName = &ColName{Name: yyDollar[1].sqlID}
		}
	case 145:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:802
		{
			yyVAL.colName = &ColName{Qualifier: yyDollar[1].sqlID, Name: yyDollar[3].sqlID}
		}
	case 146:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:808
		{
			yyVAL.valExpr = StrVal(yyDollar[1].bytes)
		}
	case 147:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:812
		{
			yyVAL.valExpr = NumVal(yyDollar[1].bytes)
		}
	case 148:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:816
		{
			yyVAL.valExpr = ValArg(yyDollar[1].bytes)
		}
	case 149:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:820
		{
			yyVAL.valExpr = &NullVal{}
		}
	case 150:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:825
		{
			yyVAL.valExprs = nil
		}
	case 151:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:829
		{
			yyVAL.valExprs = yyDollar[3].valExprs
		}
	case 152:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:834
		{
			yyVAL.boolExpr = nil
		}
	case 153:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:838
		{
			yyVAL.boolExpr = yyDollar[2].boolExpr
		}
	case 154:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:843
		{
			yyVAL.orderBy = nil
		}
	case 155:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:847
		{
			yyVAL.orderBy = yyDollar[3].orderBy
		}
	case 156:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:853
		{
			yyVAL.orderBy = OrderBy{yyDollar[1].order}
		}
	case 157:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:857
		{
			yyVAL.orderBy = append(yyDollar[1].orderBy, yyDollar[3].order)
		}
	case 158:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:863
		{
			yyVAL.order = &Order{Expr: yyDollar[1].valExpr, Direction: yyDollar[2].str}
		}
	case 159:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:868
		{
			yyVAL.str = AST_ASC
		}
	case 160:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:872
		{
			yyVAL.str = AST_ASC
		}
	case 161:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:876
		{
			yyVAL.str = AST_DESC
		}
	case 162:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:881
		{
			yyVAL.limit = nil
		}
	case 163:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:885
		{
			yyVAL.limit = &Limit{Rowcount: yyDollar[2].valExpr}
		}
	case 164:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:889
		{
			yyVAL.limit = &Limit{Offset: yyDollar[2].valExpr, Rowcount: yyDollar[4].valExpr}
		}
	case 165:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:894
		{
			yyVAL.str = ""
		}
	case 166:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:898
		{
			yyVAL.str = AST_FOR_UPDATE
		}
	case 167:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:902
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
	case 168:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:915
		{
			yyVAL.columns = nil
		}
	case 169:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:919
		{
			yyVAL.columns = yyDollar[2].columns
		}
	case 170:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:925
		{
			yyVAL.columns = Columns{&NonStarExpr{Expr: yyDollar[1].colName}}
		}
	case 171:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:929
		{
			yyVAL.columns = append(yyVAL.columns, &NonStarExpr{Expr: yyDollar[3].colName})
		}
	case 172:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:934
		{
			yyVAL.updateExprs = nil
		}
	case 173:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:938
		{
			yyVAL.updateExprs = yyDollar[5].updateExprs
		}
	case 174:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:944
		{
			yyVAL.insRows = yyDollar[2].values
		}
	case 175:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:948
		{
			yyVAL.insRows = yyDollar[1].selStmt
		}
	case 176:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:954
		{
			yyVAL.values = Values{yyDollar[1].rowTuple}
		}
	case 177:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:958
		{
			yyVAL.values = append(yyDollar[1].values, yyDollar[3].rowTuple)
		}
	case 178:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:964
		{
			yyVAL.rowTuple = ValTuple(yyDollar[2].valExprs)
		}
	case 179:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:968
		{
			yyVAL.rowTuple = yyDollar[1].subquery
		}
	case 180:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:974
		{
			yyVAL.updateExprs = UpdateExprs{yyDollar[1].updateExpr}
		}
	case 181:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:978
		{
			yyVAL.updateExprs = append(yyDollar[1].updateExprs, yyDollar[3].updateExpr)
		}
	case 182:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:984
		{
			yyVAL.updateExpr = &UpdateExpr{Name: yyDollar[1].colName, Expr: yyDollar[3].valExpr}
		}
	case 183:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:989
		{
			yyVAL.empty = struct{}{}
		}
	case 184:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:991
		{
			yyVAL.empty = struct{}{}
		}
	case 185:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:994
		{
			yyVAL.empty = struct{}{}
		}
	case 186:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:996
		{
			yyVAL.empty = struct{}{}
		}
	case 187:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:999
		{
			yyVAL.empty = struct{}{}
		}
	case 188:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1001
		{
			yyVAL.empty = struct{}{}
		}
	case 189:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1005
		{
			yyVAL.empty = struct{}{}
		}
	case 190:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1007
		{
			yyVAL.empty = struct{}{}
		}
	case 191:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1009
		{
			yyVAL.empty = struct{}{}
		}
	case 192:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1011
		{
			yyVAL.empty = struct{}{}
		}
	case 193:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1013
		{
			yyVAL.empty = struct{}{}
		}
	case 194:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1016
		{
			yyVAL.empty = struct{}{}
		}
	case 195:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1018
		{
			yyVAL.empty = struct{}{}
		}
	case 196:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1021
		{
			yyVAL.empty = struct{}{}
		}
	case 197:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1023
		{
			yyVAL.empty = struct{}{}
		}
	case 198:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1026
		{
			yyVAL.empty = struct{}{}
		}
	case 199:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:1028
		{
			yyVAL.empty = struct{}{}
		}
	case 200:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1032
		{
			yyVAL.sqlID = SQLName(strings.ToLower(string(yyDollar[1].bytes)))
		}
	case 201:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1038
		{
			yyVAL.sqlID = SQLName(yyDollar[1].bytes)
		}
	case 202:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1044
		{
			if incNesting(yylex) {
				yylex.Error("max nesting level reached")
				return 1
			}
		}
	case 203:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1053
		{
			decNesting(yylex)
		}
	case 204:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1058
		{
			forceEOF(yylex)
		}
	}
	goto yystack /* stack new state and value */
}
