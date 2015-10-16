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
	89, 208,
	-2, 207,
}

const yyNprod = 212
const yyPrivate = 57344

var yyTokenNames []string
var yyStates []string

const yyLast = 739

var yyAct = [...]int{

	98, 303, 163, 379, 93, 166, 344, 202, 63, 256,
	334, 218, 182, 82, 94, 238, 250, 165, 3, 92,
	212, 64, 190, 38, 78, 40, 59, 70, 83, 41,
	320, 322, 246, 43, 101, 44, 87, 44, 66, 263,
	125, 72, 65, 102, 75, 53, 68, 104, 105, 106,
	196, 355, 103, 239, 46, 47, 48, 354, 88, 108,
	353, 71, 74, 194, 50, 49, 45, 169, 278, 197,
	107, 331, 135, 118, 114, 90, 147, 110, 321, 362,
	393, 129, 150, 151, 152, 147, 133, 138, 117, 115,
	51, 52, 73, 251, 167, 95, 96, 84, 168, 170,
	171, 97, 146, 145, 153, 154, 148, 149, 150, 151,
	152, 147, 224, 178, 66, 109, 120, 66, 65, 186,
	185, 65, 180, 91, 350, 223, 222, 162, 164, 193,
	195, 192, 137, 136, 88, 208, 186, 122, 184, 179,
	124, 335, 217, 209, 259, 225, 226, 138, 228, 229,
	230, 231, 232, 233, 234, 235, 236, 237, 91, 91,
	207, 174, 137, 136, 136, 227, 172, 173, 333, 175,
	176, 128, 243, 210, 211, 88, 88, 138, 138, 116,
	335, 240, 242, 251, 187, 294, 245, 247, 244, 239,
	206, 260, 239, 255, 200, 183, 248, 130, 204, 91,
	220, 213, 215, 216, 91, 91, 214, 388, 219, 137,
	136, 116, 277, 183, 264, 279, 280, 281, 241, 363,
	258, 239, 243, 239, 138, 282, 284, 285, 148, 149,
	150, 151, 152, 147, 283, 239, 340, 360, 131, 265,
	91, 91, 131, 291, 287, 77, 352, 254, 314, 88,
	241, 91, 351, 315, 66, 66, 261, 116, 65, 301,
	289, 288, 299, 290, 206, 302, 293, 318, 317, 316,
	298, 239, 204, 310, 311, 312, 104, 105, 106, 103,
	313, 253, 220, 239, 28, 29, 30, 31, 295, 328,
	219, 14, 103, 324, 80, 111, 42, 332, 326, 107,
	28, 29, 30, 31, 341, 329, 73, 342, 345, 61,
	338, 330, 337, 68, 91, 103, 339, 61, 61, 91,
	221, 121, 103, 79, 103, 206, 206, 276, 14, 134,
	356, 58, 385, 204, 204, 56, 358, 374, 113, 359,
	61, 66, 73, 112, 386, 361, 357, 188, 127, 54,
	304, 349, 243, 297, 67, 369, 305, 367, 14, 15,
	16, 17, 257, 348, 183, 376, 345, 309, 62, 378,
	377, 346, 380, 380, 380, 375, 381, 382, 365, 366,
	18, 392, 383, 14, 66, 368, 33, 370, 65, 394,
	60, 274, 391, 132, 395, 387, 396, 389, 390, 189,
	76, 39, 262, 191, 81, 267, 268, 269, 270, 271,
	86, 272, 273, 69, 300, 252, 91, 60, 91, 384,
	364, 371, 372, 373, 119, 343, 347, 308, 292, 123,
	177, 249, 126, 100, 146, 145, 153, 154, 148, 149,
	150, 151, 152, 147, 19, 20, 22, 21, 23, 153,
	154, 148, 149, 150, 151, 152, 147, 24, 25, 26,
	99, 336, 296, 101, 14, 139, 89, 60, 319, 181,
	203, 266, 102, 201, 85, 68, 104, 105, 106, 101,
	198, 103, 55, 199, 27, 205, 86, 57, 102, 13,
	12, 68, 104, 105, 106, 11, 10, 103, 9, 107,
	32, 8, 239, 7, 90, 6, 110, 267, 268, 269,
	270, 271, 5, 272, 273, 107, 34, 35, 36, 37,
	90, 4, 110, 2, 95, 96, 84, 86, 86, 1,
	97, 0, 0, 0, 14, 0, 0, 0, 101, 0,
	95, 96, 0, 0, 109, 0, 97, 102, 0, 0,
	68, 104, 105, 106, 0, 0, 103, 0, 275, 205,
	109, 68, 104, 105, 106, 0, 0, 103, 0, 0,
	0, 0, 0, 0, 107, 0, 0, 0, 0, 90,
	0, 110, 0, 0, 0, 107, 0, 0, 0, 0,
	0, 0, 110, 0, 0, 0, 0, 0, 0, 95,
	96, 86, 0, 0, 0, 97, 0, 0, 0, 0,
	95, 96, 0, 0, 306, 0, 97, 307, 327, 109,
	205, 205, 68, 104, 105, 106, 0, 0, 103, 0,
	109, 323, 0, 325, 0, 146, 145, 153, 154, 148,
	149, 150, 151, 152, 147, 0, 107, 0, 0, 0,
	0, 0, 0, 110, 146, 145, 153, 154, 148, 149,
	150, 151, 152, 147, 0, 0, 0, 0, 0, 0,
	0, 95, 96, 0, 0, 0, 0, 97, 0, 0,
	0, 0, 0, 0, 0, 0, 141, 143, 0, 0,
	0, 109, 155, 156, 157, 158, 159, 160, 161, 144,
	142, 140, 146, 145, 153, 154, 148, 149, 150, 151,
	152, 147, 286, 145, 153, 154, 148, 149, 150, 151,
	152, 147, 0, 0, 0, 0, 0, 0, 0, 146,
	145, 153, 154, 148, 149, 150, 151, 152, 147,
}
var yyPact = [...]int{

	353, -1000, -1000, 260, -1000, -1000, -1000, -1000, -1000, -1000,
	-1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, -73,
	-65, -30, -42, -31, -1000, -1000, -1000, 378, 332, -1000,
	-1000, -1000, 317, -1000, -63, 285, 359, 281, -74, -36,
	274, -1000, -34, 274, -1000, 285, -77, 291, -77, 285,
	-1000, -1000, -1000, -1000, -1000, 443, -1000, 258, 320, 311,
	-15, -1000, 285, 157, -1000, 21, -1000, -16, -1000, 285,
	55, 289, -1000, -1000, 285, -1000, -59, 285, 328, 116,
	274, -1000, 188, -1000, -1000, 310, -17, 73, 625, -1000,
	518, 459, -1000, -1000, -1000, 590, 590, 590, 241, 241,
	-1000, 241, 241, -1000, -1000, -1000, -1000, -1000, -1000, -1000,
	590, -1000, 285, 281, 285, 354, 281, 590, 274, -1000,
	327, -81, -1000, 37, -1000, 285, -1000, -1000, 285, -1000,
	277, 443, -1000, -1000, 274, 60, 518, 518, 145, 590,
	284, 50, 590, 590, 145, 590, 590, 590, 590, 590,
	590, 590, 590, 590, 590, -1000, -1000, -1000, -1000, -1000,
	-1000, -1000, 13, 625, 150, 244, 196, 625, -1000, 529,
	-1000, -1000, 14, 443, -1000, 378, 243, 29, 577, 254,
	203, -1000, 349, 518, -1000, 577, -1000, -1000, -1000, 89,
	274, -1000, -60, -1000, -1000, -1000, -1000, -1000, -1000, -1000,
	-1000, 185, 361, 308, 286, -21, -1000, -1000, -1000, -1000,
	13, 104, -1000, -1000, 159, -1000, -1000, 577, -1000, 529,
	-1000, -1000, 284, 590, 590, 577, 652, -1000, 370, 635,
	-1000, -1, -1, -10, -10, -10, 147, 147, -1000, -1000,
	-1000, 590, -1000, 577, -1000, 184, 443, 184, 189, 119,
	-1000, 518, 323, 281, 281, 349, 335, 342, 73, 285,
	-1000, -1000, 285, -1000, 356, 277, 277, -1000, -1000, 231,
	204, 225, 224, 223, -22, -1000, 285, 463, 285, -1000,
	-1000, -1000, 196, -1000, 577, 558, 590, 577, -1000, 184,
	-1000, 243, -19, -1000, 590, 103, 86, 241, 260, 125,
	182, -1000, 335, -1000, 590, 590, -1000, -1000, 351, 337,
	361, 69, -1000, 208, -1000, 202, -1000, -1000, -1000, -1000,
	-37, -40, -46, -1000, -1000, -1000, -1000, 590, 577, -1000,
	232, -1000, 577, 590, -1000, 315, 183, -1000, -1000, -1000,
	281, -1000, 25, 165, -1000, 357, -1000, 349, 518, 590,
	518, -1000, -1000, 241, 241, 241, 577, -1000, 577, 312,
	241, -1000, 590, 590, -1000, -1000, -1000, 335, 73, 164,
	73, 274, 274, 274, 375, -1000, 577, -1000, 316, 153,
	-1000, 153, 153, 281, -1000, 374, 4, -1000, 274, -1000,
	-1000, 157, -1000, 274, -1000, 274, -1000,
}
var yyPgo = [...]int{

	0, 529, 523, 17, 521, 512, 505, 503, 501, 498,
	496, 495, 490, 489, 500, 487, 484, 482, 13, 28,
	474, 473, 7, 471, 470, 26, 468, 3, 12, 36,
	466, 465, 462, 19, 2, 20, 11, 5, 461, 14,
	460, 59, 4, 433, 431, 16, 430, 428, 427, 426,
	9, 425, 6, 420, 1, 419, 415, 414, 10, 8,
	21, 296, 245, 413, 403, 402, 401, 399, 0, 393,
	354, 391, 64, 386, 67, 15,
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
	30, 30, 35, 35, 35, 35, 35, 35, 31, 31,
	31, 31, 31, 31, 31, 36, 36, 36, 41, 37,
	37, 34, 34, 34, 34, 34, 34, 34, 34, 34,
	34, 34, 34, 34, 34, 34, 34, 34, 34, 34,
	34, 34, 40, 43, 46, 46, 44, 44, 45, 47,
	47, 42, 42, 33, 33, 33, 33, 48, 48, 49,
	49, 50, 50, 51, 51, 52, 53, 53, 53, 54,
	54, 54, 55, 55, 55, 56, 56, 57, 57, 58,
	58, 32, 32, 38, 38, 39, 39, 59, 59, 60,
	62, 62, 63, 63, 61, 61, 64, 64, 64, 64,
	64, 65, 65, 66, 66, 67, 67, 68, 70, 74,
	75, 72,
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
	3, 3, 3, 3, 4, 3, 4, 5, 6, 3,
	2, 6, 1, 2, 1, 2, 1, 2, 1, 1,
	1, 1, 1, 1, 1, 3, 1, 1, 3, 1,
	3, 1, 1, 1, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 2, 2, 2, 3, 4, 5,
	4, 1, 1, 5, 0, 1, 1, 2, 4, 0,
	2, 1, 3, 1, 1, 1, 1, 0, 3, 0,
	2, 0, 3, 1, 3, 2, 0, 1, 1, 0,
	2, 4, 0, 2, 4, 0, 3, 1, 3, 0,
	5, 2, 1, 1, 3, 3, 1, 1, 3, 3,
	0, 2, 0, 3, 0, 1, 1, 1, 1, 1,
	1, 0, 1, 0, 1, 0, 2, 1, 1, 1,
	1, 0,
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
	-43, 20, 29, 38, 33, 34, 35, 56, -41, 101,
	63, 37, 23, 27, 89, -25, 54, 67, 89, -70,
	61, 32, -72, -70, -72, 99, -70, 20, 55, -68,
	9, 54, -69, -68, 19, 89, 60, 59, 74, -31,
	76, 61, 75, 62, 74, 78, 77, 86, 81, 82,
	83, 84, 85, 79, 80, 67, 68, 69, 70, 71,
	72, 73, -29, -34, -29, -3, -37, -34, -34, -74,
	-34, -34, -74, -74, -41, -74, -74, -46, -34, -25,
	-59, -70, -28, 10, -60, -34, -68, -72, 20, -67,
	103, -64, 94, 92, 26, 93, 13, 32, -70, -70,
	-72, -21, -22, -24, -74, -70, -41, -19, -68, 83,
	-29, -29, -35, 56, 61, 57, 58, -34, -36, -74,
	-41, 36, 76, 75, 62, -34, -34, -35, -34, -34,
	-34, -34, -34, -34, -34, -34, -34, -34, -75, 39,
	-75, 54, -75, -34, -75, -18, 18, -18, -33, -44,
	-45, 64, -56, 27, -74, -28, -50, 13, -29, 55,
	-68, -72, -65, 99, -28, 54, -23, 44, 45, 46,
	47, 48, 50, 51, -71, -70, 19, -22, 89, 56,
	57, 58, -37, -36, -34, -34, 60, -34, -75, -18,
	-75, 54, -47, -45, 66, -29, -32, 30, -3, -59,
	-57, -42, -50, -54, 15, 14, -70, -70, -48, 11,
	-22, -22, 44, 49, 44, 49, 44, 44, 44, -26,
	52, 100, 53, -70, -75, -70, -75, 60, -34, -75,
	-33, 90, -34, 65, -58, 55, -38, -39, -58, -75,
	54, -54, -34, -51, -52, -34, -72, -49, 12, 14,
	55, 44, 44, 97, 97, 97, -34, -75, -34, 24,
	54, -42, 54, 54, -53, 21, 22, -50, -29, -37,
	-29, -74, -74, -74, 25, -39, -34, -52, -54, -27,
	-68, -27, -27, 7, -55, 16, 28, -75, 54, -75,
	-75, -59, 7, 76, -68, -68, -68,
}
var yyDef = [...]int{

	0, -2, 1, 2, 3, 4, 5, 6, 7, 8,
	9, 10, 11, 12, 34, 34, 34, 34, 34, 203,
	194, 0, 0, 0, 211, 211, 211, 0, 38, 40,
	41, 42, 43, 36, 194, 0, 0, 0, 192, 0,
	0, 204, 0, 0, 195, 0, 190, 0, 190, 0,
	31, 32, 33, 14, 39, 0, 44, 35, 0, 0,
	76, 208, 0, 19, 187, 0, 151, 0, -2, 0,
	0, 0, 211, 207, 0, 211, 0, 0, 0, 0,
	0, 30, 0, 45, 47, 52, 0, 50, 51, 86,
	0, 0, 121, 122, 123, 0, 0, 0, 151, 0,
	141, 0, 0, 209, 153, 154, 155, 156, 186, 142,
	144, 37, 0, 0, 0, 84, 0, 0, 0, 211,
	0, 205, 22, 0, 25, 0, 27, 191, 0, 211,
	0, 0, 48, 53, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 108, 109, 110, 111, 112,
	113, 114, 89, 0, 0, 0, 0, 119, 134, 0,
	135, 136, 0, 0, 100, 0, 0, 0, 145, 175,
	84, 77, 161, 0, 188, 189, 152, 20, 193, 0,
	0, 211, 201, 196, 197, 198, 199, 200, 26, 28,
	29, 84, 55, 61, 0, 73, 75, 46, 54, 49,
	87, 88, 91, 102, 0, 104, 106, 92, 93, 0,
	116, 117, 0, 0, 0, 95, 0, 99, 124, 125,
	126, 127, 128, 129, 130, 131, 132, 133, 90, 210,
	118, 0, 185, 119, 137, 0, 0, 0, 0, 149,
	146, 0, 0, 0, 0, 161, 169, 0, 85, 0,
	206, 23, 0, 202, 157, 0, 0, 64, 65, 0,
	0, 0, 0, 0, 78, 62, 0, 0, 0, 103,
	105, 107, 0, 94, 96, 0, 0, 120, 138, 0,
	140, 0, 0, 147, 0, 0, 179, 0, 182, 179,
	0, 177, 169, 18, 0, 0, 211, 24, 159, 0,
	56, 59, 66, 0, 68, 0, 70, 71, 72, 57,
	0, 0, 0, 63, 58, 74, 115, 0, 97, 139,
	0, 143, 150, 0, 15, 0, 181, 183, 16, 176,
	0, 17, 170, 162, 163, 166, 21, 161, 0, 0,
	0, 67, 69, 0, 0, 0, 98, 101, 148, 0,
	0, 178, 0, 0, 165, 167, 168, 169, 160, 158,
	60, 0, 0, 0, 0, 184, 171, 164, 172, 0,
	82, 0, 0, 0, 13, 0, 0, 79, 0, 80,
	81, 180, 173, 0, 83, 0, 174,
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
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:564
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: yyDollar[2].str, Right: yyDollar[3].valExpr}
		}
	case 93:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:568
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: AST_IN, Right: yyDollar[3].colTuple}
		}
	case 94:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:572
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: AST_NOT_IN, Right: yyDollar[4].colTuple}
		}
	case 95:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:576
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: AST_LIKE, Right: yyDollar[3].valExpr}
		}
	case 96:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:580
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: AST_NOT_LIKE, Right: yyDollar[4].valExpr}
		}
	case 97:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:584
		{
			yyVAL.boolExpr = &RangeCond{Left: yyDollar[1].valExpr, Operator: AST_BETWEEN, From: yyDollar[3].valExpr, To: yyDollar[5].valExpr}
		}
	case 98:
		yyDollar = yyS[yypt-6 : yypt+1]
		//line sql.y:588
		{
			yyVAL.boolExpr = &RangeCond{Left: yyDollar[1].valExpr, Operator: AST_NOT_BETWEEN, From: yyDollar[4].valExpr, To: yyDollar[6].valExpr}
		}
	case 99:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:592
		{
			yyVAL.boolExpr = &IsExpr{Operator: yyDollar[3].str, Expr: yyDollar[1].valExpr}
		}
	case 100:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:596
		{
			yyVAL.boolExpr = &ExistsExpr{Subquery: yyDollar[2].subquery}
		}
	case 101:
		yyDollar = yyS[yypt-6 : yypt+1]
		//line sql.y:600
		{
			yyVAL.boolExpr = &KeyrangeExpr{Start: yyDollar[3].valExpr, End: yyDollar[5].valExpr}
		}
	case 102:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:606
		{
			yyVAL.str = AST_IS_NULL
		}
	case 103:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:610
		{
			yyVAL.str = AST_IS_NOT_NULL
		}
	case 104:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:614
		{
			yyVAL.str = AST_IS_TRUE
		}
	case 105:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:618
		{
			yyVAL.str = AST_IS_NOT_TRUE
		}
	case 106:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:622
		{
			yyVAL.str = AST_IS_FALSE
		}
	case 107:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:626
		{
			yyVAL.str = AST_IS_NOT_FALSE
		}
	case 108:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:632
		{
			yyVAL.str = AST_EQ
		}
	case 109:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:636
		{
			yyVAL.str = AST_LT
		}
	case 110:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:640
		{
			yyVAL.str = AST_GT
		}
	case 111:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:644
		{
			yyVAL.str = AST_LE
		}
	case 112:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:648
		{
			yyVAL.str = AST_GE
		}
	case 113:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:652
		{
			yyVAL.str = AST_NE
		}
	case 114:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:656
		{
			yyVAL.str = AST_NSE
		}
	case 115:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:662
		{
			yyVAL.colTuple = ValTuple(yyDollar[2].valExprs)
		}
	case 116:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:666
		{
			yyVAL.colTuple = yyDollar[1].subquery
		}
	case 117:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:670
		{
			yyVAL.colTuple = ListArg(yyDollar[1].bytes)
		}
	case 118:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:676
		{
			yyVAL.subquery = &Subquery{yyDollar[2].selStmt}
		}
	case 119:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:682
		{
			yyVAL.valExprs = ValExprs{yyDollar[1].valExpr}
		}
	case 120:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:686
		{
			yyVAL.valExprs = append(yyDollar[1].valExprs, yyDollar[3].valExpr)
		}
	case 121:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:692
		{
			yyVAL.valExpr = yyDollar[1].valExpr
		}
	case 122:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:696
		{
			yyVAL.valExpr = yyDollar[1].colName
		}
	case 123:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:700
		{
			yyVAL.valExpr = yyDollar[1].rowTuple
		}
	case 124:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:704
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: AST_BITAND, Right: yyDollar[3].valExpr}
		}
	case 125:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:708
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: AST_BITOR, Right: yyDollar[3].valExpr}
		}
	case 126:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:712
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: AST_BITXOR, Right: yyDollar[3].valExpr}
		}
	case 127:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:716
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: AST_PLUS, Right: yyDollar[3].valExpr}
		}
	case 128:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:720
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: AST_MINUS, Right: yyDollar[3].valExpr}
		}
	case 129:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:724
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: AST_MULT, Right: yyDollar[3].valExpr}
		}
	case 130:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:728
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: AST_DIV, Right: yyDollar[3].valExpr}
		}
	case 131:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:732
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: AST_MOD, Right: yyDollar[3].valExpr}
		}
	case 132:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:736
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: AST_SHIFT_LEFT, Right: yyDollar[3].valExpr}
		}
	case 133:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:740
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: AST_SHIFT_RIGHT, Right: yyDollar[3].valExpr}
		}
	case 134:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:744
		{
			if num, ok := yyDollar[2].valExpr.(NumVal); ok {
				yyVAL.valExpr = num
			} else {
				yyVAL.valExpr = &UnaryExpr{Operator: AST_UPLUS, Expr: yyDollar[2].valExpr}
			}
		}
	case 135:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:752
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
	case 136:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:765
		{
			yyVAL.valExpr = &UnaryExpr{Operator: AST_TILDA, Expr: yyDollar[2].valExpr}
		}
	case 137:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:769
		{
			yyVAL.valExpr = &FuncExpr{Name: string(yyDollar[1].sqlID)}
		}
	case 138:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:773
		{
			yyVAL.valExpr = &FuncExpr{Name: string(yyDollar[1].sqlID), Exprs: yyDollar[3].selectExprs}
		}
	case 139:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:777
		{
			yyVAL.valExpr = &FuncExpr{Name: string(yyDollar[1].sqlID), Distinct: true, Exprs: yyDollar[4].selectExprs}
		}
	case 140:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:781
		{
			yyVAL.valExpr = &FuncExpr{Name: yyDollar[1].str, Exprs: yyDollar[3].selectExprs}
		}
	case 141:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:785
		{
			yyVAL.valExpr = yyDollar[1].caseExpr
		}
	case 142:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:791
		{
			yyVAL.str = "if"
		}
	case 143:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:797
		{
			yyVAL.caseExpr = &CaseExpr{Expr: yyDollar[2].valExpr, Whens: yyDollar[3].whens, Else: yyDollar[4].valExpr}
		}
	case 144:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:802
		{
			yyVAL.valExpr = nil
		}
	case 145:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:806
		{
			yyVAL.valExpr = yyDollar[1].valExpr
		}
	case 146:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:812
		{
			yyVAL.whens = []*When{yyDollar[1].when}
		}
	case 147:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:816
		{
			yyVAL.whens = append(yyDollar[1].whens, yyDollar[2].when)
		}
	case 148:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:822
		{
			yyVAL.when = &When{Cond: yyDollar[2].boolExpr, Val: yyDollar[4].valExpr}
		}
	case 149:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:827
		{
			yyVAL.valExpr = nil
		}
	case 150:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:831
		{
			yyVAL.valExpr = yyDollar[2].valExpr
		}
	case 151:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:837
		{
			yyVAL.colName = &ColName{Name: yyDollar[1].sqlID}
		}
	case 152:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:841
		{
			yyVAL.colName = &ColName{Qualifier: yyDollar[1].sqlID, Name: yyDollar[3].sqlID}
		}
	case 153:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:847
		{
			yyVAL.valExpr = StrVal(yyDollar[1].bytes)
		}
	case 154:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:851
		{
			yyVAL.valExpr = NumVal(yyDollar[1].bytes)
		}
	case 155:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:855
		{
			yyVAL.valExpr = ValArg(yyDollar[1].bytes)
		}
	case 156:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:859
		{
			yyVAL.valExpr = &NullVal{}
		}
	case 157:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:864
		{
			yyVAL.valExprs = nil
		}
	case 158:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:868
		{
			yyVAL.valExprs = yyDollar[3].valExprs
		}
	case 159:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:873
		{
			yyVAL.boolExpr = nil
		}
	case 160:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:877
		{
			yyVAL.boolExpr = yyDollar[2].boolExpr
		}
	case 161:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:882
		{
			yyVAL.orderBy = nil
		}
	case 162:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:886
		{
			yyVAL.orderBy = yyDollar[3].orderBy
		}
	case 163:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:892
		{
			yyVAL.orderBy = OrderBy{yyDollar[1].order}
		}
	case 164:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:896
		{
			yyVAL.orderBy = append(yyDollar[1].orderBy, yyDollar[3].order)
		}
	case 165:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:902
		{
			yyVAL.order = &Order{Expr: yyDollar[1].valExpr, Direction: yyDollar[2].str}
		}
	case 166:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:907
		{
			yyVAL.str = AST_ASC
		}
	case 167:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:911
		{
			yyVAL.str = AST_ASC
		}
	case 168:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:915
		{
			yyVAL.str = AST_DESC
		}
	case 169:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:920
		{
			yyVAL.limit = nil
		}
	case 170:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:924
		{
			yyVAL.limit = &Limit{Rowcount: yyDollar[2].valExpr}
		}
	case 171:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:928
		{
			yyVAL.limit = &Limit{Offset: yyDollar[2].valExpr, Rowcount: yyDollar[4].valExpr}
		}
	case 172:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:933
		{
			yyVAL.str = ""
		}
	case 173:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:937
		{
			yyVAL.str = AST_FOR_UPDATE
		}
	case 174:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:941
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
	case 175:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:954
		{
			yyVAL.columns = nil
		}
	case 176:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:958
		{
			yyVAL.columns = yyDollar[2].columns
		}
	case 177:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:964
		{
			yyVAL.columns = Columns{&NonStarExpr{Expr: yyDollar[1].colName}}
		}
	case 178:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:968
		{
			yyVAL.columns = append(yyVAL.columns, &NonStarExpr{Expr: yyDollar[3].colName})
		}
	case 179:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:973
		{
			yyVAL.updateExprs = nil
		}
	case 180:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:977
		{
			yyVAL.updateExprs = yyDollar[5].updateExprs
		}
	case 181:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:983
		{
			yyVAL.insRows = yyDollar[2].values
		}
	case 182:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:987
		{
			yyVAL.insRows = yyDollar[1].selStmt
		}
	case 183:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:993
		{
			yyVAL.values = Values{yyDollar[1].rowTuple}
		}
	case 184:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:997
		{
			yyVAL.values = append(yyDollar[1].values, yyDollar[3].rowTuple)
		}
	case 185:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1003
		{
			yyVAL.rowTuple = ValTuple(yyDollar[2].valExprs)
		}
	case 186:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1007
		{
			yyVAL.rowTuple = yyDollar[1].subquery
		}
	case 187:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1013
		{
			yyVAL.updateExprs = UpdateExprs{yyDollar[1].updateExpr}
		}
	case 188:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1017
		{
			yyVAL.updateExprs = append(yyDollar[1].updateExprs, yyDollar[3].updateExpr)
		}
	case 189:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1023
		{
			yyVAL.updateExpr = &UpdateExpr{Name: yyDollar[1].colName, Expr: yyDollar[3].valExpr}
		}
	case 190:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1028
		{
			yyVAL.empty = struct{}{}
		}
	case 191:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:1030
		{
			yyVAL.empty = struct{}{}
		}
	case 192:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1033
		{
			yyVAL.empty = struct{}{}
		}
	case 193:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1035
		{
			yyVAL.empty = struct{}{}
		}
	case 194:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1038
		{
			yyVAL.str = ""
		}
	case 195:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1040
		{
			yyVAL.str = AST_IGNORE
		}
	case 196:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1044
		{
			yyVAL.empty = struct{}{}
		}
	case 197:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1046
		{
			yyVAL.empty = struct{}{}
		}
	case 198:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1048
		{
			yyVAL.empty = struct{}{}
		}
	case 199:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1050
		{
			yyVAL.empty = struct{}{}
		}
	case 200:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1052
		{
			yyVAL.empty = struct{}{}
		}
	case 201:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1055
		{
			yyVAL.empty = struct{}{}
		}
	case 202:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1057
		{
			yyVAL.empty = struct{}{}
		}
	case 203:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1060
		{
			yyVAL.empty = struct{}{}
		}
	case 204:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1062
		{
			yyVAL.empty = struct{}{}
		}
	case 205:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1065
		{
			yyVAL.empty = struct{}{}
		}
	case 206:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:1067
		{
			yyVAL.empty = struct{}{}
		}
	case 207:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1071
		{
			yyVAL.sqlID = SQLName(strings.ToLower(string(yyDollar[1].bytes)))
		}
	case 208:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1077
		{
			yyVAL.sqlID = SQLName(yyDollar[1].bytes)
		}
	case 209:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1083
		{
			if incNesting(yylex) {
				yylex.Error("max nesting level reached")
				return 1
			}
		}
	case 210:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1092
		{
			decNesting(yylex)
		}
	case 211:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1097
		{
			forceEOF(yylex)
		}
	}
	goto yystack /* stack new state and value */
}
