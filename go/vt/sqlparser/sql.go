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
const INTO = 57370
const DUPLICATE = 57371
const KEY = 57372
const DEFAULT = 57373
const SET = 57374
const LOCK = 57375
const KEYRANGE = 57376
const VALUES = 57377
const LAST_INSERT_ID = 57378
const ID = 57379
const STRING = 57380
const NUMBER = 57381
const VALUE_ARG = 57382
const LIST_ARG = 57383
const COMMENT = 57384
const LE = 57385
const GE = 57386
const NE = 57387
const NULL_SAFE_EQUAL = 57388
const UNION = 57389
const MINUS = 57390
const EXCEPT = 57391
const INTERSECT = 57392
const JOIN = 57393
const STRAIGHT_JOIN = 57394
const LEFT = 57395
const RIGHT = 57396
const INNER = 57397
const OUTER = 57398
const CROSS = 57399
const NATURAL = 57400
const USE = 57401
const FORCE = 57402
const ON = 57403
const OR = 57404
const AND = 57405
const NOT = 57406
const SHIFT_LEFT = 57407
const SHIFT_RIGHT = 57408
const UNARY = 57409
const CASE = 57410
const WHEN = 57411
const THEN = 57412
const ELSE = 57413
const END = 57414
const CREATE = 57415
const ALTER = 57416
const DROP = 57417
const RENAME = 57418
const ANALYZE = 57419
const TABLE = 57420
const INDEX = 57421
const VIEW = 57422
const TO = 57423
const IGNORE = 57424
const IF = 57425
const UNIQUE = 57426
const USING = 57427
const SHOW = 57428
const DESCRIBE = 57429
const EXPLAIN = 57430

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
	"SHIFT_LEFT",
	"SHIFT_RIGHT",
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
	82, 202,
	-2, 201,
}

const yyNprod = 206
const yyPrivate = 57344

var yyTokenNames []string
var yyStates []string

const yyLast = 707

var yyAct = [...]int{

	98, 296, 162, 372, 93, 87, 337, 83, 63, 212,
	165, 92, 181, 251, 64, 327, 245, 59, 94, 233,
	381, 201, 234, 164, 3, 333, 82, 262, 263, 264,
	265, 266, 189, 267, 268, 107, 137, 136, 66, 131,
	236, 72, 65, 78, 75, 70, 44, 68, 104, 105,
	106, 53, 50, 28, 29, 30, 31, 103, 88, 38,
	258, 40, 313, 315, 125, 41, 348, 74, 43, 234,
	44, 108, 347, 234, 234, 234, 346, 71, 51, 52,
	115, 129, 95, 96, 49, 45, 133, 195, 234, 234,
	97, 137, 136, 110, 166, 314, 161, 163, 167, 169,
	170, 46, 47, 48, 324, 193, 234, 246, 109, 326,
	246, 196, 287, 177, 66, 273, 135, 66, 65, 185,
	184, 65, 179, 118, 114, 122, 14, 146, 124, 73,
	178, 183, 137, 136, 88, 207, 185, 120, 136, 206,
	345, 211, 209, 210, 219, 220, 107, 223, 224, 225,
	226, 227, 228, 229, 230, 231, 232, 343, 68, 104,
	105, 106, 221, 192, 194, 191, 208, 168, 103, 328,
	254, 238, 186, 173, 88, 88, 149, 150, 151, 152,
	153, 146, 199, 128, 235, 237, 344, 243, 253, 116,
	255, 239, 250, 95, 96, 152, 153, 146, 240, 242,
	328, 97, 205, 307, 110, 305, 222, 311, 308, 310,
	306, 214, 355, 259, 309, 116, 238, 236, 182, 109,
	276, 277, 356, 91, 274, 272, 275, 145, 144, 147,
	148, 149, 150, 151, 152, 153, 146, 353, 130, 280,
	182, 284, 77, 256, 88, 28, 29, 30, 31, 66,
	66, 117, 288, 65, 294, 61, 103, 292, 91, 91,
	281, 286, 283, 260, 295, 103, 171, 172, 282, 174,
	175, 291, 215, 73, 248, 205, 111, 271, 103, 113,
	14, 321, 303, 304, 131, 116, 68, 61, 214, 103,
	325, 80, 317, 134, 319, 61, 323, 334, 203, 91,
	335, 338, 322, 121, 91, 91, 79, 213, 331, 330,
	290, 73, 367, 332, 144, 147, 148, 149, 150, 151,
	152, 153, 146, 349, 262, 263, 264, 265, 266, 351,
	267, 268, 205, 205, 66, 352, 14, 112, 354, 91,
	91, 42, 279, 350, 378, 238, 249, 361, 386, 363,
	91, 56, 339, 362, 360, 67, 187, 127, 369, 338,
	107, 379, 371, 370, 54, 373, 373, 373, 61, 374,
	375, 203, 368, 104, 105, 106, 58, 66, 103, 297,
	342, 65, 387, 298, 213, 384, 252, 388, 341, 389,
	302, 60, 380, 182, 382, 383, 216, 62, 217, 218,
	385, 76, 376, 14, 33, 81, 269, 132, 188, 91,
	39, 86, 358, 359, 91, 257, 190, 69, 60, 293,
	241, 247, 101, 377, 357, 119, 336, 107, 203, 203,
	123, 340, 301, 126, 285, 176, 102, 244, 100, 68,
	104, 105, 106, 99, 329, 289, 138, 89, 312, 103,
	14, 15, 16, 17, 202, 261, 145, 144, 147, 148,
	149, 150, 151, 152, 153, 146, 200, 85, 60, 55,
	180, 90, 27, 57, 95, 96, 84, 18, 13, 12,
	11, 197, 97, 10, 198, 110, 204, 86, 320, 9,
	145, 144, 147, 148, 149, 150, 151, 152, 153, 146,
	109, 8, 7, 101, 6, 5, 234, 4, 107, 91,
	2, 91, 1, 0, 364, 365, 366, 102, 0, 0,
	68, 104, 105, 106, 0, 0, 0, 86, 86, 0,
	103, 0, 0, 19, 20, 22, 21, 23, 147, 148,
	149, 150, 151, 152, 153, 146, 24, 25, 26, 0,
	0, 0, 90, 0, 0, 95, 96, 84, 270, 204,
	0, 0, 0, 97, 0, 0, 110, 14, 278, 0,
	145, 144, 147, 148, 149, 150, 151, 152, 153, 146,
	0, 109, 101, 0, 0, 0, 0, 107, 0, 0,
	0, 0, 0, 0, 0, 0, 102, 86, 0, 68,
	104, 105, 106, 0, 0, 0, 101, 0, 0, 103,
	299, 107, 0, 300, 0, 0, 204, 204, 0, 0,
	102, 32, 0, 68, 104, 105, 106, 316, 0, 318,
	0, 90, 0, 103, 95, 96, 0, 34, 35, 36,
	37, 0, 97, 0, 0, 110, 0, 0, 139, 143,
	141, 142, 0, 0, 0, 90, 0, 0, 95, 96,
	109, 0, 0, 0, 0, 0, 97, 0, 0, 110,
	157, 158, 159, 160, 0, 154, 155, 156, 0, 0,
	0, 0, 0, 0, 109, 145, 144, 147, 148, 149,
	150, 151, 152, 153, 146, 0, 140, 145, 144, 147,
	148, 149, 150, 151, 152, 153, 146,
}
var yyPact = [...]int{

	445, -1000, -1000, 194, -1000, -1000, -1000, -1000, -1000, -1000,
	-1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, -34,
	-27, -8, 8, -9, -1000, -1000, -1000, 398, 347, -1000,
	-1000, -1000, 333, -1000, -51, 250, 388, 249, -53, -17,
	236, -1000, -26, 236, -1000, 250, -55, 269, -55, 250,
	-1000, -1000, -1000, -1000, -1000, 483, -1000, 234, 309, 247,
	42, -1000, 250, 160, -1000, 203, -1000, 41, -1000, 250,
	68, 266, -1000, -1000, 250, -1000, -32, 250, 337, 117,
	236, -1000, 229, -1000, -1000, 274, 34, 65, 627, -1000,
	586, 562, -1000, -1000, -1000, 10, 10, 10, 209, 209,
	-1000, 209, 209, -1000, -1000, -1000, -1000, -1000, -1000, -1000,
	10, -1000, 250, 249, 250, 383, 249, 10, 236, -1000,
	336, -68, -1000, 74, -1000, 250, -1000, -1000, 250, -1000,
	218, 483, -1000, -1000, 236, 92, 586, 586, 10, 231,
	375, 10, 10, 137, 10, 10, 10, 10, 10, 10,
	10, 10, 10, 10, -1000, -1000, -1000, -1000, -1000, -1000,
	-1000, -1000, 627, -31, 2, -15, 627, -1000, 121, -1000,
	-1000, 402, 483, -1000, 398, 335, 23, 615, 242, 230,
	-1000, 373, 586, -1000, 615, -1000, -1000, -1000, 104, 236,
	-1000, -36, -1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000,
	208, 268, 258, 331, 33, -1000, -1000, -1000, -1000, -1000,
	70, 615, -1000, 121, -1000, -1000, 231, 10, 10, 615,
	500, -1000, 317, 466, 243, -1000, 102, 102, 118, 118,
	118, 48, 48, -1000, -1000, -1000, 10, -1000, 615, -1000,
	-16, 483, -16, 186, 26, -1000, 586, 275, 249, 249,
	373, 364, 369, 65, 250, -1000, -1000, 250, -1000, 379,
	218, 218, -1000, -1000, 149, 147, 158, 153, 151, -2,
	-1000, 250, -29, 250, -15, -1000, 615, 420, 10, -1000,
	615, -1000, -16, -1000, 335, 17, -1000, 10, 24, 103,
	209, 194, 134, -30, -1000, 364, -1000, 10, 10, -1000,
	-1000, 376, 366, 268, 91, -1000, 130, -1000, 84, -1000,
	-1000, -1000, -1000, -18, -22, -28, -1000, -1000, -1000, -1000,
	10, 615, -1000, -82, -1000, 615, 10, -1000, 306, 182,
	-1000, -1000, -1000, 249, -1000, 157, 167, -1000, 386, -1000,
	373, 586, 10, 586, -1000, -1000, 209, 209, 209, 615,
	-1000, 615, 282, 209, -1000, 10, 10, -1000, -1000, -1000,
	364, 65, 162, 65, 236, 236, 236, 395, -1000, 615,
	-1000, 328, -35, -1000, -35, -35, 249, -1000, 393, 327,
	-1000, 236, -1000, -1000, 160, -1000, 236, -1000, 236, -1000,
}
var yyPgo = [...]int{

	0, 512, 510, 23, 507, 505, 504, 502, 501, 489,
	483, 480, 479, 478, 621, 473, 472, 469, 26, 7,
	467, 466, 21, 455, 454, 17, 448, 3, 12, 5,
	447, 446, 445, 11, 2, 9, 10, 444, 18, 443,
	71, 4, 438, 437, 16, 435, 434, 432, 431, 13,
	426, 6, 424, 1, 423, 421, 419, 15, 8, 14,
	341, 242, 417, 416, 415, 410, 408, 0, 407, 355,
	406, 52, 404, 167, 19,
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
	34, 34, 34, 34, 34, 34, 39, 42, 45, 45,
	43, 43, 44, 46, 46, 41, 41, 33, 33, 33,
	33, 47, 47, 48, 48, 49, 49, 50, 50, 51,
	52, 52, 52, 53, 53, 53, 54, 54, 54, 55,
	55, 56, 56, 57, 57, 32, 32, 37, 37, 38,
	38, 58, 58, 59, 61, 61, 62, 62, 60, 60,
	63, 63, 63, 63, 63, 64, 64, 65, 65, 66,
	66, 67, 69, 73, 74, 71,
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
	3, 3, 3, 4, 3, 4, 5, 6, 3, 4,
	2, 6, 1, 1, 1, 1, 1, 1, 1, 3,
	1, 1, 3, 1, 3, 1, 1, 1, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 2, 2,
	2, 3, 4, 5, 4, 1, 1, 5, 0, 1,
	1, 2, 4, 0, 2, 1, 3, 1, 1, 1,
	1, 0, 3, 0, 2, 0, 3, 1, 3, 2,
	0, 1, 1, 0, 2, 4, 0, 2, 4, 0,
	3, 1, 3, 0, 5, 2, 1, 1, 3, 3,
	1, 1, 3, 3, 0, 2, 0, 3, 0, 1,
	1, 1, 1, 1, 1, 0, 1, 0, 1, 0,
	2, 1, 1, 1, 1, 0,
}
var yyChk = [...]int{

	-1000, -1, -2, -3, -4, -5, -6, -7, -8, -9,
	-10, -11, -12, -13, 5, 6, 7, 8, 32, 88,
	89, 91, 90, 92, 101, 102, 103, -16, 51, 52,
	53, 54, -14, -72, -14, -14, -14, -14, 93, -65,
	95, 99, -60, 95, 97, 93, 93, 94, 95, 93,
	-71, -71, -71, -3, 17, -17, 18, -15, -60, -25,
	-69, 37, 9, -58, -59, -41, -67, -69, 37, -62,
	98, 94, -67, 37, 93, -67, -69, -61, 98, 37,
	-61, -69, -18, -19, 74, -20, -69, -29, -34, -30,
	69, -73, -33, -41, -38, 72, 73, 80, -67, -39,
	-42, 20, 34, 47, 38, 39, 40, 25, -40, 98,
	83, 42, 28, 32, 82, -25, 55, 48, 82, -69,
	69, 37, -71, -69, -71, 96, -69, 20, 66, -67,
	9, 55, -68, -67, 19, 82, 68, 67, -31, 21,
	69, 23, 24, 22, 71, 70, 79, 72, 73, 74,
	75, 76, 77, 78, 48, 49, 50, 43, 44, 45,
	46, -29, -34, -29, -3, -36, -34, -34, -73, -34,
	-34, -73, -73, -40, -73, -73, -45, -34, -25, -58,
	-69, -28, 10, -59, -34, -67, -71, 20, -66, 100,
	-63, 91, 89, 31, 90, 13, 37, -69, -69, -71,
	-21, -22, -24, -73, -69, -40, -19, -67, 74, -29,
	-29, -34, -35, -73, -40, 41, 21, 23, 24, -34,
	-34, 25, 69, -34, -34, -34, -34, -34, -34, -34,
	-34, -34, -34, -74, 104, -74, 55, -74, -34, -74,
	-18, 18, -18, -33, -43, -44, 84, -55, 32, -73,
	-28, -49, 13, -29, 66, -67, -71, -64, 96, -28,
	55, -23, 56, 57, 58, 59, 60, 62, 63, -70,
	-69, 19, -22, 82, -36, -35, -34, -34, 68, 25,
	-34, -74, -18, -74, 55, -46, -44, 86, -29, -32,
	35, -3, -58, -56, -41, -49, -53, 15, 14, -69,
	-69, -47, 11, -22, -22, 56, 61, 56, 61, 56,
	56, 56, -26, 64, 97, 65, -69, -74, -69, -74,
	68, -34, -74, -33, 87, -34, 85, -57, 66, -37,
	-38, -57, -74, 55, -53, -34, -50, -51, -34, -71,
	-48, 12, 14, 66, 56, 56, 94, 94, 94, -34,
	-74, -34, 29, 55, -41, 55, 55, -52, 26, 27,
	-49, -29, -36, -29, -73, -73, -73, 30, -38, -34,
	-51, -53, -27, -67, -27, -27, 7, -54, 16, 33,
	-74, 55, -74, -74, -58, 7, 21, -67, -67, -67,
}
var yyDef = [...]int{

	0, -2, 1, 2, 3, 4, 5, 6, 7, 8,
	9, 10, 11, 12, 34, 34, 34, 34, 34, 197,
	188, 0, 0, 0, 205, 205, 205, 0, 38, 40,
	41, 42, 43, 36, 188, 0, 0, 0, 186, 0,
	0, 198, 0, 0, 189, 0, 184, 0, 184, 0,
	31, 32, 33, 14, 39, 0, 44, 35, 0, 0,
	76, 202, 0, 19, 181, 0, 145, 0, -2, 0,
	0, 0, 205, 201, 0, 205, 0, 0, 0, 0,
	0, 30, 0, 45, 47, 52, 0, 50, 51, 86,
	0, 0, 115, 116, 117, 0, 0, 0, 145, 0,
	135, 0, 0, 203, 147, 148, 149, 150, 180, 136,
	138, 37, 0, 0, 0, 84, 0, 0, 0, 205,
	0, 199, 22, 0, 25, 0, 27, 185, 0, 205,
	0, 0, 48, 53, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 102, 103, 104, 105, 106, 107,
	108, 89, 0, 0, 0, 0, 113, 128, 0, 129,
	130, 0, 0, 100, 0, 0, 0, 139, 169, 84,
	77, 155, 0, 182, 183, 146, 20, 187, 0, 0,
	205, 195, 190, 191, 192, 193, 194, 26, 28, 29,
	84, 55, 61, 0, 73, 75, 46, 54, 49, 87,
	88, 91, 92, 0, 110, 111, 0, 0, 0, 94,
	0, 98, 0, 118, 119, 120, 121, 122, 123, 124,
	125, 126, 127, 90, 204, 112, 0, 179, 113, 131,
	0, 0, 0, 0, 143, 140, 0, 0, 0, 0,
	155, 163, 0, 85, 0, 200, 23, 0, 196, 151,
	0, 0, 64, 65, 0, 0, 0, 0, 0, 78,
	62, 0, 0, 0, 0, 93, 95, 0, 0, 99,
	114, 132, 0, 134, 0, 0, 141, 0, 0, 173,
	0, 176, 173, 0, 171, 163, 18, 0, 0, 205,
	24, 153, 0, 56, 59, 66, 0, 68, 0, 70,
	71, 72, 57, 0, 0, 0, 63, 58, 74, 109,
	0, 96, 133, 0, 137, 144, 0, 15, 0, 175,
	177, 16, 170, 0, 17, 164, 156, 157, 160, 21,
	155, 0, 0, 0, 67, 69, 0, 0, 0, 97,
	101, 142, 0, 0, 172, 0, 0, 159, 161, 162,
	163, 154, 152, 60, 0, 0, 0, 0, 178, 165,
	158, 166, 0, 82, 0, 0, 0, 13, 0, 0,
	79, 0, 80, 81, 174, 167, 0, 83, 0, 168,
}
var yyTok1 = [...]int{

	1, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 76, 71, 3,
	47, 104, 74, 72, 55, 73, 82, 75, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	49, 48, 50, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 79, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 70, 3, 80,
}
var yyTok2 = [...]int{

	2, 3, 4, 5, 6, 7, 8, 9, 10, 11,
	12, 13, 14, 15, 16, 17, 18, 19, 20, 21,
	22, 23, 24, 25, 26, 27, 28, 29, 30, 31,
	32, 33, 34, 35, 36, 37, 38, 39, 40, 41,
	42, 43, 44, 45, 46, 51, 52, 53, 54, 56,
	57, 58, 59, 60, 61, 62, 63, 64, 65, 66,
	67, 68, 69, 77, 78, 81, 83, 84, 85, 86,
	87, 88, 89, 90, 91, 92, 93, 94, 95, 96,
	97, 98, 99, 100, 101, 102, 103,
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
		//line sql.y:162
		{
			setParseTree(yylex, yyDollar[1].statement)
		}
	case 2:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:168
		{
			yyVAL.statement = yyDollar[1].selStmt
		}
	case 13:
		yyDollar = yyS[yypt-12 : yypt+1]
		//line sql.y:184
		{
			yyVAL.selStmt = &Select{Comments: Comments(yyDollar[2].bytes2), Distinct: yyDollar[3].str, SelectExprs: yyDollar[4].selectExprs, From: yyDollar[6].tableExprs, Where: NewWhere(AST_WHERE, yyDollar[7].boolExpr), GroupBy: GroupBy(yyDollar[8].valExprs), Having: NewWhere(AST_HAVING, yyDollar[9].boolExpr), OrderBy: yyDollar[10].orderBy, Limit: yyDollar[11].limit, Lock: yyDollar[12].str}
		}
	case 14:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:188
		{
			yyVAL.selStmt = &Union{Type: yyDollar[2].str, Left: yyDollar[1].selStmt, Right: yyDollar[3].selStmt}
		}
	case 15:
		yyDollar = yyS[yypt-8 : yypt+1]
		//line sql.y:194
		{
			yyVAL.statement = &Insert{Comments: Comments(yyDollar[2].bytes2), Ignore: yyDollar[3].str, Table: yyDollar[5].tableName, Columns: yyDollar[6].columns, Rows: yyDollar[7].insRows, OnDup: OnDup(yyDollar[8].updateExprs)}
		}
	case 16:
		yyDollar = yyS[yypt-8 : yypt+1]
		//line sql.y:198
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
		//line sql.y:210
		{
			yyVAL.statement = &Update{Comments: Comments(yyDollar[2].bytes2), Table: yyDollar[3].tableName, Exprs: yyDollar[5].updateExprs, Where: NewWhere(AST_WHERE, yyDollar[6].boolExpr), OrderBy: yyDollar[7].orderBy, Limit: yyDollar[8].limit}
		}
	case 18:
		yyDollar = yyS[yypt-7 : yypt+1]
		//line sql.y:216
		{
			yyVAL.statement = &Delete{Comments: Comments(yyDollar[2].bytes2), Table: yyDollar[4].tableName, Where: NewWhere(AST_WHERE, yyDollar[5].boolExpr), OrderBy: yyDollar[6].orderBy, Limit: yyDollar[7].limit}
		}
	case 19:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:222
		{
			yyVAL.statement = &Set{Comments: Comments(yyDollar[2].bytes2), Exprs: yyDollar[3].updateExprs}
		}
	case 20:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:228
		{
			yyVAL.statement = &DDL{Action: AST_CREATE, NewName: yyDollar[4].sqlID}
		}
	case 21:
		yyDollar = yyS[yypt-8 : yypt+1]
		//line sql.y:232
		{
			// Change this to an alter statement
			yyVAL.statement = &DDL{Action: AST_ALTER, Table: yyDollar[7].sqlID, NewName: yyDollar[7].sqlID}
		}
	case 22:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:237
		{
			yyVAL.statement = &DDL{Action: AST_CREATE, NewName: SQLName(yyDollar[3].sqlID)}
		}
	case 23:
		yyDollar = yyS[yypt-6 : yypt+1]
		//line sql.y:243
		{
			yyVAL.statement = &DDL{Action: AST_ALTER, Table: yyDollar[4].sqlID, NewName: yyDollar[4].sqlID}
		}
	case 24:
		yyDollar = yyS[yypt-7 : yypt+1]
		//line sql.y:247
		{
			// Change this to a rename statement
			yyVAL.statement = &DDL{Action: AST_RENAME, Table: yyDollar[4].sqlID, NewName: yyDollar[7].sqlID}
		}
	case 25:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:252
		{
			yyVAL.statement = &DDL{Action: AST_ALTER, Table: SQLName(yyDollar[3].sqlID), NewName: SQLName(yyDollar[3].sqlID)}
		}
	case 26:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:258
		{
			yyVAL.statement = &DDL{Action: AST_RENAME, Table: yyDollar[3].sqlID, NewName: yyDollar[5].sqlID}
		}
	case 27:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:264
		{
			yyVAL.statement = &DDL{Action: AST_DROP, Table: yyDollar[4].sqlID}
		}
	case 28:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:268
		{
			// Change this to an alter statement
			yyVAL.statement = &DDL{Action: AST_ALTER, Table: yyDollar[5].sqlID, NewName: yyDollar[5].sqlID}
		}
	case 29:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:273
		{
			yyVAL.statement = &DDL{Action: AST_DROP, Table: SQLName(yyDollar[4].sqlID)}
		}
	case 30:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:279
		{
			yyVAL.statement = &DDL{Action: AST_ALTER, Table: yyDollar[3].sqlID, NewName: yyDollar[3].sqlID}
		}
	case 31:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:285
		{
			yyVAL.statement = &Other{}
		}
	case 32:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:289
		{
			yyVAL.statement = &Other{}
		}
	case 33:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:293
		{
			yyVAL.statement = &Other{}
		}
	case 34:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:298
		{
			setAllowComments(yylex, true)
		}
	case 35:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:302
		{
			yyVAL.bytes2 = yyDollar[2].bytes2
			setAllowComments(yylex, false)
		}
	case 36:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:308
		{
			yyVAL.bytes2 = nil
		}
	case 37:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:312
		{
			yyVAL.bytes2 = append(yyDollar[1].bytes2, yyDollar[2].bytes)
		}
	case 38:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:318
		{
			yyVAL.str = AST_UNION
		}
	case 39:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:322
		{
			yyVAL.str = AST_UNION_ALL
		}
	case 40:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:326
		{
			yyVAL.str = AST_SET_MINUS
		}
	case 41:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:330
		{
			yyVAL.str = AST_EXCEPT
		}
	case 42:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:334
		{
			yyVAL.str = AST_INTERSECT
		}
	case 43:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:339
		{
			yyVAL.str = ""
		}
	case 44:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:343
		{
			yyVAL.str = AST_DISTINCT
		}
	case 45:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:349
		{
			yyVAL.selectExprs = SelectExprs{yyDollar[1].selectExpr}
		}
	case 46:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:353
		{
			yyVAL.selectExprs = append(yyVAL.selectExprs, yyDollar[3].selectExpr)
		}
	case 47:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:359
		{
			yyVAL.selectExpr = &StarExpr{}
		}
	case 48:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:363
		{
			yyVAL.selectExpr = &NonStarExpr{Expr: yyDollar[1].expr, As: yyDollar[2].sqlID}
		}
	case 49:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:367
		{
			yyVAL.selectExpr = &StarExpr{TableName: yyDollar[1].sqlID}
		}
	case 50:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:373
		{
			yyVAL.expr = yyDollar[1].boolExpr
		}
	case 51:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:377
		{
			yyVAL.expr = yyDollar[1].valExpr
		}
	case 52:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:382
		{
			yyVAL.sqlID = ""
		}
	case 53:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:386
		{
			yyVAL.sqlID = yyDollar[1].sqlID
		}
	case 54:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:390
		{
			yyVAL.sqlID = yyDollar[2].sqlID
		}
	case 55:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:396
		{
			yyVAL.tableExprs = TableExprs{yyDollar[1].tableExpr}
		}
	case 56:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:400
		{
			yyVAL.tableExprs = append(yyVAL.tableExprs, yyDollar[3].tableExpr)
		}
	case 57:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:406
		{
			yyVAL.tableExpr = &AliasedTableExpr{Expr: yyDollar[1].smTableExpr, As: yyDollar[2].sqlID, Hints: yyDollar[3].indexHints}
		}
	case 58:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:410
		{
			yyVAL.tableExpr = &ParenTableExpr{Expr: yyDollar[2].tableExpr}
		}
	case 59:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:414
		{
			yyVAL.tableExpr = &JoinTableExpr{LeftExpr: yyDollar[1].tableExpr, Join: yyDollar[2].str, RightExpr: yyDollar[3].tableExpr}
		}
	case 60:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:418
		{
			yyVAL.tableExpr = &JoinTableExpr{LeftExpr: yyDollar[1].tableExpr, Join: yyDollar[2].str, RightExpr: yyDollar[3].tableExpr, On: yyDollar[5].boolExpr}
		}
	case 61:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:423
		{
			yyVAL.sqlID = ""
		}
	case 62:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:427
		{
			yyVAL.sqlID = yyDollar[1].sqlID
		}
	case 63:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:431
		{
			yyVAL.sqlID = yyDollar[2].sqlID
		}
	case 64:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:437
		{
			yyVAL.str = AST_JOIN
		}
	case 65:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:441
		{
			yyVAL.str = AST_STRAIGHT_JOIN
		}
	case 66:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:445
		{
			yyVAL.str = AST_LEFT_JOIN
		}
	case 67:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:449
		{
			yyVAL.str = AST_LEFT_JOIN
		}
	case 68:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:453
		{
			yyVAL.str = AST_RIGHT_JOIN
		}
	case 69:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:457
		{
			yyVAL.str = AST_RIGHT_JOIN
		}
	case 70:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:461
		{
			yyVAL.str = AST_JOIN
		}
	case 71:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:465
		{
			yyVAL.str = AST_CROSS_JOIN
		}
	case 72:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:469
		{
			yyVAL.str = AST_NATURAL_JOIN
		}
	case 73:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:475
		{
			yyVAL.smTableExpr = &TableName{Name: yyDollar[1].sqlID}
		}
	case 74:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:479
		{
			yyVAL.smTableExpr = &TableName{Qualifier: yyDollar[1].sqlID, Name: yyDollar[3].sqlID}
		}
	case 75:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:483
		{
			yyVAL.smTableExpr = yyDollar[1].subquery
		}
	case 76:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:489
		{
			yyVAL.tableName = &TableName{Name: yyDollar[1].sqlID}
		}
	case 77:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:493
		{
			yyVAL.tableName = &TableName{Qualifier: yyDollar[1].sqlID, Name: yyDollar[3].sqlID}
		}
	case 78:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:498
		{
			yyVAL.indexHints = nil
		}
	case 79:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:502
		{
			yyVAL.indexHints = &IndexHints{Type: AST_USE, Indexes: yyDollar[4].sqlIDs}
		}
	case 80:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:506
		{
			yyVAL.indexHints = &IndexHints{Type: AST_IGNORE, Indexes: yyDollar[4].sqlIDs}
		}
	case 81:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:510
		{
			yyVAL.indexHints = &IndexHints{Type: AST_FORCE, Indexes: yyDollar[4].sqlIDs}
		}
	case 82:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:516
		{
			yyVAL.sqlIDs = []SQLName{yyDollar[1].sqlID}
		}
	case 83:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:520
		{
			yyVAL.sqlIDs = append(yyDollar[1].sqlIDs, yyDollar[3].sqlID)
		}
	case 84:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:525
		{
			yyVAL.boolExpr = nil
		}
	case 85:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:529
		{
			yyVAL.boolExpr = yyDollar[2].boolExpr
		}
	case 87:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:536
		{
			yyVAL.boolExpr = &AndExpr{Left: yyDollar[1].boolExpr, Right: yyDollar[3].boolExpr}
		}
	case 88:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:540
		{
			yyVAL.boolExpr = &OrExpr{Left: yyDollar[1].boolExpr, Right: yyDollar[3].boolExpr}
		}
	case 89:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:544
		{
			yyVAL.boolExpr = &NotExpr{Expr: yyDollar[2].boolExpr}
		}
	case 90:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:548
		{
			yyVAL.boolExpr = &ParenBoolExpr{Expr: yyDollar[2].boolExpr}
		}
	case 91:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:554
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: yyDollar[2].str, Right: yyDollar[3].valExpr}
		}
	case 92:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:558
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: AST_IN, Right: yyDollar[3].colTuple}
		}
	case 93:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:562
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: AST_NOT_IN, Right: yyDollar[4].colTuple}
		}
	case 94:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:566
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: AST_LIKE, Right: yyDollar[3].valExpr}
		}
	case 95:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:570
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: AST_NOT_LIKE, Right: yyDollar[4].valExpr}
		}
	case 96:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:574
		{
			yyVAL.boolExpr = &RangeCond{Left: yyDollar[1].valExpr, Operator: AST_BETWEEN, From: yyDollar[3].valExpr, To: yyDollar[5].valExpr}
		}
	case 97:
		yyDollar = yyS[yypt-6 : yypt+1]
		//line sql.y:578
		{
			yyVAL.boolExpr = &RangeCond{Left: yyDollar[1].valExpr, Operator: AST_NOT_BETWEEN, From: yyDollar[4].valExpr, To: yyDollar[6].valExpr}
		}
	case 98:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:582
		{
			yyVAL.boolExpr = &NullCheck{Operator: AST_IS_NULL, Expr: yyDollar[1].valExpr}
		}
	case 99:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:586
		{
			yyVAL.boolExpr = &NullCheck{Operator: AST_IS_NOT_NULL, Expr: yyDollar[1].valExpr}
		}
	case 100:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:590
		{
			yyVAL.boolExpr = &ExistsExpr{Subquery: yyDollar[2].subquery}
		}
	case 101:
		yyDollar = yyS[yypt-6 : yypt+1]
		//line sql.y:594
		{
			yyVAL.boolExpr = &KeyrangeExpr{Start: yyDollar[3].valExpr, End: yyDollar[5].valExpr}
		}
	case 102:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:600
		{
			yyVAL.str = AST_EQ
		}
	case 103:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:604
		{
			yyVAL.str = AST_LT
		}
	case 104:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:608
		{
			yyVAL.str = AST_GT
		}
	case 105:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:612
		{
			yyVAL.str = AST_LE
		}
	case 106:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:616
		{
			yyVAL.str = AST_GE
		}
	case 107:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:620
		{
			yyVAL.str = AST_NE
		}
	case 108:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:624
		{
			yyVAL.str = AST_NSE
		}
	case 109:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:630
		{
			yyVAL.colTuple = ValTuple(yyDollar[2].valExprs)
		}
	case 110:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:634
		{
			yyVAL.colTuple = yyDollar[1].subquery
		}
	case 111:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:638
		{
			yyVAL.colTuple = ListArg(yyDollar[1].bytes)
		}
	case 112:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:644
		{
			yyVAL.subquery = &Subquery{yyDollar[2].selStmt}
		}
	case 113:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:650
		{
			yyVAL.valExprs = ValExprs{yyDollar[1].valExpr}
		}
	case 114:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:654
		{
			yyVAL.valExprs = append(yyDollar[1].valExprs, yyDollar[3].valExpr)
		}
	case 115:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:660
		{
			yyVAL.valExpr = yyDollar[1].valExpr
		}
	case 116:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:664
		{
			yyVAL.valExpr = yyDollar[1].colName
		}
	case 117:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:668
		{
			yyVAL.valExpr = yyDollar[1].rowTuple
		}
	case 118:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:672
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: AST_BITAND, Right: yyDollar[3].valExpr}
		}
	case 119:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:676
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: AST_BITOR, Right: yyDollar[3].valExpr}
		}
	case 120:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:680
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: AST_BITXOR, Right: yyDollar[3].valExpr}
		}
	case 121:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:684
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: AST_PLUS, Right: yyDollar[3].valExpr}
		}
	case 122:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:688
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: AST_MINUS, Right: yyDollar[3].valExpr}
		}
	case 123:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:692
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: AST_MULT, Right: yyDollar[3].valExpr}
		}
	case 124:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:696
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: AST_DIV, Right: yyDollar[3].valExpr}
		}
	case 125:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:700
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: AST_MOD, Right: yyDollar[3].valExpr}
		}
	case 126:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:704
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: AST_SHIFT_LEFT, Right: yyDollar[3].valExpr}
		}
	case 127:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:708
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: AST_SHIFT_RIGHT, Right: yyDollar[3].valExpr}
		}
	case 128:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:712
		{
			if num, ok := yyDollar[2].valExpr.(NumVal); ok {
				yyVAL.valExpr = num
			} else {
				yyVAL.valExpr = &UnaryExpr{Operator: AST_UPLUS, Expr: yyDollar[2].valExpr}
			}
		}
	case 129:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:720
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
	case 130:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:733
		{
			yyVAL.valExpr = &UnaryExpr{Operator: AST_TILDA, Expr: yyDollar[2].valExpr}
		}
	case 131:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:737
		{
			yyVAL.valExpr = &FuncExpr{Name: string(yyDollar[1].sqlID)}
		}
	case 132:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:741
		{
			yyVAL.valExpr = &FuncExpr{Name: string(yyDollar[1].sqlID), Exprs: yyDollar[3].selectExprs}
		}
	case 133:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:745
		{
			yyVAL.valExpr = &FuncExpr{Name: string(yyDollar[1].sqlID), Distinct: true, Exprs: yyDollar[4].selectExprs}
		}
	case 134:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:749
		{
			yyVAL.valExpr = &FuncExpr{Name: yyDollar[1].str, Exprs: yyDollar[3].selectExprs}
		}
	case 135:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:753
		{
			yyVAL.valExpr = yyDollar[1].caseExpr
		}
	case 136:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:759
		{
			yyVAL.str = "if"
		}
	case 137:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:765
		{
			yyVAL.caseExpr = &CaseExpr{Expr: yyDollar[2].valExpr, Whens: yyDollar[3].whens, Else: yyDollar[4].valExpr}
		}
	case 138:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:770
		{
			yyVAL.valExpr = nil
		}
	case 139:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:774
		{
			yyVAL.valExpr = yyDollar[1].valExpr
		}
	case 140:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:780
		{
			yyVAL.whens = []*When{yyDollar[1].when}
		}
	case 141:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:784
		{
			yyVAL.whens = append(yyDollar[1].whens, yyDollar[2].when)
		}
	case 142:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:790
		{
			yyVAL.when = &When{Cond: yyDollar[2].boolExpr, Val: yyDollar[4].valExpr}
		}
	case 143:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:795
		{
			yyVAL.valExpr = nil
		}
	case 144:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:799
		{
			yyVAL.valExpr = yyDollar[2].valExpr
		}
	case 145:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:805
		{
			yyVAL.colName = &ColName{Name: yyDollar[1].sqlID}
		}
	case 146:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:809
		{
			yyVAL.colName = &ColName{Qualifier: yyDollar[1].sqlID, Name: yyDollar[3].sqlID}
		}
	case 147:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:815
		{
			yyVAL.valExpr = StrVal(yyDollar[1].bytes)
		}
	case 148:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:819
		{
			yyVAL.valExpr = NumVal(yyDollar[1].bytes)
		}
	case 149:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:823
		{
			yyVAL.valExpr = ValArg(yyDollar[1].bytes)
		}
	case 150:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:827
		{
			yyVAL.valExpr = &NullVal{}
		}
	case 151:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:832
		{
			yyVAL.valExprs = nil
		}
	case 152:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:836
		{
			yyVAL.valExprs = yyDollar[3].valExprs
		}
	case 153:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:841
		{
			yyVAL.boolExpr = nil
		}
	case 154:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:845
		{
			yyVAL.boolExpr = yyDollar[2].boolExpr
		}
	case 155:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:850
		{
			yyVAL.orderBy = nil
		}
	case 156:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:854
		{
			yyVAL.orderBy = yyDollar[3].orderBy
		}
	case 157:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:860
		{
			yyVAL.orderBy = OrderBy{yyDollar[1].order}
		}
	case 158:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:864
		{
			yyVAL.orderBy = append(yyDollar[1].orderBy, yyDollar[3].order)
		}
	case 159:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:870
		{
			yyVAL.order = &Order{Expr: yyDollar[1].valExpr, Direction: yyDollar[2].str}
		}
	case 160:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:875
		{
			yyVAL.str = AST_ASC
		}
	case 161:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:879
		{
			yyVAL.str = AST_ASC
		}
	case 162:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:883
		{
			yyVAL.str = AST_DESC
		}
	case 163:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:888
		{
			yyVAL.limit = nil
		}
	case 164:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:892
		{
			yyVAL.limit = &Limit{Rowcount: yyDollar[2].valExpr}
		}
	case 165:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:896
		{
			yyVAL.limit = &Limit{Offset: yyDollar[2].valExpr, Rowcount: yyDollar[4].valExpr}
		}
	case 166:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:901
		{
			yyVAL.str = ""
		}
	case 167:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:905
		{
			yyVAL.str = AST_FOR_UPDATE
		}
	case 168:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:909
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
	case 169:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:922
		{
			yyVAL.columns = nil
		}
	case 170:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:926
		{
			yyVAL.columns = yyDollar[2].columns
		}
	case 171:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:932
		{
			yyVAL.columns = Columns{&NonStarExpr{Expr: yyDollar[1].colName}}
		}
	case 172:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:936
		{
			yyVAL.columns = append(yyVAL.columns, &NonStarExpr{Expr: yyDollar[3].colName})
		}
	case 173:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:941
		{
			yyVAL.updateExprs = nil
		}
	case 174:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:945
		{
			yyVAL.updateExprs = yyDollar[5].updateExprs
		}
	case 175:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:951
		{
			yyVAL.insRows = yyDollar[2].values
		}
	case 176:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:955
		{
			yyVAL.insRows = yyDollar[1].selStmt
		}
	case 177:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:961
		{
			yyVAL.values = Values{yyDollar[1].rowTuple}
		}
	case 178:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:965
		{
			yyVAL.values = append(yyDollar[1].values, yyDollar[3].rowTuple)
		}
	case 179:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:971
		{
			yyVAL.rowTuple = ValTuple(yyDollar[2].valExprs)
		}
	case 180:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:975
		{
			yyVAL.rowTuple = yyDollar[1].subquery
		}
	case 181:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:981
		{
			yyVAL.updateExprs = UpdateExprs{yyDollar[1].updateExpr}
		}
	case 182:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:985
		{
			yyVAL.updateExprs = append(yyDollar[1].updateExprs, yyDollar[3].updateExpr)
		}
	case 183:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:991
		{
			yyVAL.updateExpr = &UpdateExpr{Name: yyDollar[1].colName, Expr: yyDollar[3].valExpr}
		}
	case 184:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:996
		{
			yyVAL.empty = struct{}{}
		}
	case 185:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:998
		{
			yyVAL.empty = struct{}{}
		}
	case 186:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1001
		{
			yyVAL.empty = struct{}{}
		}
	case 187:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1003
		{
			yyVAL.empty = struct{}{}
		}
	case 188:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1006
		{
			yyVAL.str = ""
		}
	case 189:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1008
		{
			yyVAL.str = AST_IGNORE
		}
	case 190:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1012
		{
			yyVAL.empty = struct{}{}
		}
	case 191:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1014
		{
			yyVAL.empty = struct{}{}
		}
	case 192:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1016
		{
			yyVAL.empty = struct{}{}
		}
	case 193:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1018
		{
			yyVAL.empty = struct{}{}
		}
	case 194:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1020
		{
			yyVAL.empty = struct{}{}
		}
	case 195:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1023
		{
			yyVAL.empty = struct{}{}
		}
	case 196:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1025
		{
			yyVAL.empty = struct{}{}
		}
	case 197:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1028
		{
			yyVAL.empty = struct{}{}
		}
	case 198:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1030
		{
			yyVAL.empty = struct{}{}
		}
	case 199:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1033
		{
			yyVAL.empty = struct{}{}
		}
	case 200:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:1035
		{
			yyVAL.empty = struct{}{}
		}
	case 201:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1039
		{
			yyVAL.sqlID = SQLName(strings.ToLower(string(yyDollar[1].bytes)))
		}
	case 202:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1045
		{
			yyVAL.sqlID = SQLName(yyDollar[1].bytes)
		}
	case 203:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1051
		{
			if incNesting(yylex) {
				yylex.Error("max nesting level reached")
				return 1
			}
		}
	case 204:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1060
		{
			decNesting(yylex)
		}
	case 205:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1065
		{
			forceEOF(yylex)
		}
	}
	goto yystack /* stack new state and value */
}
