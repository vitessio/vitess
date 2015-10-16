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
	88, 207,
	-2, 206,
}

const yyNprod = 211
const yyPrivate = 57344

var yyTokenNames []string
var yyStates []string

const yyLast = 734

var yyAct = [...]int{

	98, 94, 162, 377, 93, 87, 332, 301, 63, 165,
	254, 164, 3, 82, 342, 236, 212, 181, 248, 59,
	92, 64, 386, 201, 338, 83, 265, 266, 267, 268,
	269, 131, 270, 271, 28, 29, 30, 31, 66, 53,
	237, 72, 65, 189, 75, 137, 136, 78, 70, 239,
	101, 44, 261, 50, 38, 43, 40, 44, 88, 102,
	41, 108, 68, 104, 105, 106, 125, 74, 103, 46,
	47, 48, 353, 352, 49, 237, 351, 237, 71, 51,
	52, 129, 115, 45, 237, 107, 133, 329, 276, 237,
	90, 135, 110, 237, 166, 118, 161, 163, 167, 169,
	170, 237, 237, 168, 114, 152, 153, 146, 95, 96,
	84, 146, 73, 177, 66, 391, 97, 66, 65, 185,
	184, 65, 179, 117, 318, 320, 122, 137, 136, 124,
	109, 249, 178, 331, 88, 207, 185, 249, 183, 292,
	136, 211, 209, 210, 219, 220, 120, 226, 227, 228,
	229, 230, 231, 232, 233, 234, 235, 206, 348, 91,
	208, 137, 136, 173, 149, 150, 151, 152, 153, 146,
	333, 241, 319, 186, 88, 88, 182, 222, 224, 225,
	238, 240, 223, 199, 257, 243, 245, 242, 256, 182,
	258, 195, 205, 130, 91, 91, 246, 253, 218, 116,
	333, 214, 171, 172, 193, 174, 175, 282, 283, 284,
	196, 217, 216, 128, 116, 239, 241, 361, 262, 263,
	279, 280, 358, 277, 104, 105, 106, 275, 289, 350,
	77, 312, 116, 278, 203, 91, 313, 131, 349, 316,
	91, 91, 285, 213, 259, 310, 107, 88, 315, 314,
	311, 103, 66, 66, 111, 293, 65, 299, 287, 286,
	297, 288, 296, 14, 300, 205, 291, 61, 113, 192,
	194, 191, 215, 103, 103, 91, 91, 42, 214, 80,
	73, 251, 252, 68, 326, 61, 91, 308, 309, 274,
	61, 322, 103, 324, 121, 330, 103, 335, 28, 29,
	30, 31, 61, 327, 336, 340, 343, 203, 339, 134,
	328, 79, 58, 372, 337, 68, 104, 105, 106, 357,
	213, 103, 73, 112, 14, 205, 205, 187, 354, 265,
	266, 267, 268, 269, 356, 270, 271, 127, 107, 66,
	56, 54, 302, 359, 355, 110, 347, 383, 91, 295,
	241, 303, 366, 91, 368, 67, 365, 367, 344, 384,
	373, 95, 96, 374, 343, 255, 346, 203, 203, 97,
	378, 378, 378, 376, 379, 380, 375, 307, 182, 62,
	390, 32, 66, 109, 381, 33, 65, 392, 14, 272,
	389, 60, 393, 385, 394, 387, 388, 34, 35, 36,
	37, 76, 132, 188, 39, 81, 260, 190, 69, 298,
	250, 86, 382, 362, 14, 15, 16, 17, 60, 341,
	345, 306, 290, 176, 247, 119, 100, 99, 334, 244,
	123, 101, 221, 126, 294, 138, 18, 89, 317, 202,
	102, 264, 200, 68, 104, 105, 106, 85, 55, 103,
	91, 27, 91, 57, 13, 369, 370, 371, 147, 148,
	149, 150, 151, 152, 153, 146, 107, 12, 60, 11,
	180, 90, 10, 110, 363, 364, 9, 8, 7, 6,
	5, 197, 4, 2, 198, 1, 204, 86, 0, 95,
	96, 84, 14, 0, 0, 0, 0, 97, 0, 19,
	20, 22, 21, 23, 0, 0, 0, 101, 0, 0,
	0, 109, 24, 25, 26, 0, 102, 237, 0, 68,
	104, 105, 106, 0, 0, 103, 0, 86, 86, 145,
	144, 147, 148, 149, 150, 151, 152, 153, 146, 0,
	0, 0, 107, 0, 0, 0, 0, 90, 0, 110,
	0, 0, 0, 0, 0, 0, 0, 0, 273, 204,
	0, 14, 0, 101, 0, 95, 96, 0, 0, 0,
	0, 0, 102, 97, 0, 68, 104, 105, 106, 0,
	0, 103, 0, 0, 0, 0, 0, 109, 68, 104,
	105, 106, 0, 0, 103, 0, 0, 0, 107, 0,
	86, 0, 0, 90, 0, 110, 0, 0, 0, 0,
	0, 107, 0, 304, 0, 0, 305, 0, 110, 204,
	204, 95, 96, 0, 0, 0, 0, 0, 0, 97,
	321, 0, 323, 0, 95, 96, 0, 0, 0, 0,
	0, 0, 97, 109, 0, 0, 0, 0, 0, 0,
	0, 140, 142, 0, 360, 0, 109, 154, 155, 156,
	157, 158, 159, 160, 143, 141, 139, 145, 144, 147,
	148, 149, 150, 151, 152, 153, 146, 145, 144, 147,
	148, 149, 150, 151, 152, 153, 146, 325, 144, 147,
	148, 149, 150, 151, 152, 153, 146, 281, 0, 0,
	0, 0, 0, 0, 145, 144, 147, 148, 149, 150,
	151, 152, 153, 146, 145, 144, 147, 148, 149, 150,
	151, 152, 153, 146, 145, 144, 147, 148, 149, 150,
	151, 152, 153, 146,
}
var yyPact = [...]int{

	409, -1000, -1000, 259, -1000, -1000, -1000, -1000, -1000, -1000,
	-1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, -41,
	-42, -12, -26, -21, -1000, -1000, -1000, 383, 324, -1000,
	-1000, -1000, 322, -1000, -48, 253, 370, 251, -52, -18,
	248, -1000, -28, 248, -1000, 253, -53, 279, -53, 253,
	-1000, -1000, -1000, -1000, -1000, 30, -1000, 217, 300, 241,
	16, -1000, 253, 161, -1000, 57, -1000, 7, -1000, 253,
	86, 262, -1000, -1000, 253, -1000, -32, 253, 317, 159,
	248, -1000, 184, -1000, -1000, 290, 3, 103, 591, -1000,
	543, 487, -1000, -1000, -1000, 283, 283, 283, 213, 213,
	-1000, 213, 213, -1000, -1000, -1000, -1000, -1000, -1000, -1000,
	283, -1000, 253, 251, 253, 368, 251, 283, 248, -1000,
	307, -59, -1000, 178, -1000, 253, -1000, -1000, 253, -1000,
	235, 30, -1000, -1000, 248, 80, 543, 543, 283, 236,
	137, 283, 283, 122, 283, 283, 283, 283, 283, 283,
	283, 283, 283, 283, -1000, -1000, -1000, -1000, -1000, -1000,
	-1000, -1000, 591, -13, -5, -4, 591, -1000, 556, -1000,
	-1000, 411, 30, -1000, 383, 191, 68, 648, 254, 179,
	-1000, 352, 543, -1000, 648, -1000, -1000, -1000, 130, 248,
	-1000, -46, -1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000,
	166, 286, 270, 258, 0, -1000, -1000, -1000, -1000, -1000,
	81, 648, -1000, 556, -1000, -1000, 236, 283, 283, 648,
	638, -1000, -1000, 152, -1000, -1000, 380, 611, -1000, 84,
	84, 22, 22, 22, 26, 26, -1000, -1000, -1000, 283,
	-1000, 648, -1000, -22, 30, -22, 175, 74, -1000, 543,
	319, 251, 251, 352, 327, 337, 103, 253, -1000, -1000,
	253, -1000, 366, 235, 235, -1000, -1000, 202, 188, 206,
	205, 196, 73, -1000, 253, -17, 253, -4, -1000, 648,
	628, 283, -1000, -1000, -1000, 648, -1000, -22, -1000, 191,
	-2, -1000, 283, 69, 116, 213, 259, 146, -29, -1000,
	327, -1000, 283, 283, -1000, -1000, 354, 332, 286, 104,
	-1000, 195, -1000, 186, -1000, -1000, -1000, -1000, -20, -23,
	-24, -1000, -1000, -1000, -1000, 283, 648, -1000, -66, -1000,
	648, 283, -1000, 295, 169, -1000, -1000, -1000, 251, -1000,
	601, 164, -1000, 453, -1000, 352, 543, 283, 543, -1000,
	-1000, 213, 213, 213, 648, -1000, 648, 288, 213, -1000,
	283, 283, -1000, -1000, -1000, 327, 103, 162, 103, 248,
	248, 248, 377, -1000, 648, -1000, 331, -31, -1000, -31,
	-31, 251, -1000, 373, 40, -1000, 248, -1000, -1000, 161,
	-1000, 248, -1000, 248, -1000,
}
var yyPgo = [...]int{

	0, 485, 483, 11, 482, 480, 479, 478, 477, 476,
	472, 469, 467, 454, 381, 453, 451, 448, 13, 25,
	447, 442, 23, 441, 439, 19, 438, 3, 17, 5,
	437, 435, 434, 20, 2, 432, 16, 9, 428, 1,
	427, 61, 4, 426, 424, 18, 423, 422, 421, 420,
	10, 419, 14, 413, 7, 412, 410, 409, 6, 8,
	21, 277, 230, 408, 407, 406, 404, 403, 0, 402,
	355, 389, 53, 385, 103, 15,
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
	29, 30, 30, 30, 30, 30, 30, 30, 30, 30,
	30, 35, 35, 35, 35, 35, 35, 31, 31, 31,
	31, 31, 31, 31, 36, 36, 36, 41, 37, 37,
	34, 34, 34, 34, 34, 34, 34, 34, 34, 34,
	34, 34, 34, 34, 34, 34, 34, 34, 34, 34,
	34, 40, 43, 46, 46, 44, 44, 45, 47, 47,
	42, 42, 33, 33, 33, 33, 48, 48, 49, 49,
	50, 50, 51, 51, 52, 53, 53, 53, 54, 54,
	54, 55, 55, 55, 56, 56, 57, 57, 58, 58,
	32, 32, 38, 38, 39, 39, 59, 59, 60, 62,
	62, 63, 63, 61, 61, 64, 64, 64, 64, 64,
	65, 65, 66, 66, 67, 67, 68, 70, 74, 75,
	72,
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
	3, 3, 3, 4, 3, 4, 5, 6, 3, 2,
	6, 1, 2, 1, 2, 1, 2, 1, 1, 1,
	1, 1, 1, 1, 3, 1, 1, 3, 1, 3,
	1, 1, 1, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 2, 2, 2, 3, 4, 5, 4,
	1, 1, 5, 0, 1, 1, 2, 4, 0, 2,
	1, 3, 1, 1, 1, 1, 0, 3, 0, 2,
	0, 3, 1, 3, 2, 0, 1, 1, 0, 2,
	4, 0, 2, 4, 0, 3, 1, 3, 0, 5,
	2, 1, 1, 3, 3, 1, 1, 3, 3, 0,
	2, 0, 3, 0, 1, 1, 1, 1, 1, 1,
	0, 1, 0, 1, 0, 2, 1, 1, 1, 1,
	0,
}
var yyChk = [...]int{

	-1000, -1, -2, -3, -4, -5, -6, -7, -8, -9,
	-10, -11, -12, -13, 5, 6, 7, 8, 27, 90,
	91, 93, 92, 94, 103, 104, 105, -16, 39, 40,
	41, 42, -14, -73, -14, -14, -14, -14, 95, -66,
	97, 101, -61, 97, 99, 95, 95, 96, 97, 95,
	-72, -72, -72, -3, 17, -17, 18, -15, -61, -25,
	-70, 32, 9, -59, -60, -42, -68, -70, 32, -63,
	100, 96, -68, 32, 95, -68, -70, -62, 100, 32,
	-62, -70, -18, -19, 80, -20, -70, -29, -34, -30,
	60, -74, -33, -42, -39, 78, 79, 86, -68, -40,
	-43, 20, 29, 38, 33, 34, 35, 55, -41, 100,
	62, 37, 23, 27, 88, -25, 53, 66, 88, -70,
	60, 32, -72, -70, -72, 98, -70, 20, 54, -68,
	9, 53, -69, -68, 19, 88, 59, 58, -31, 75,
	60, 74, 61, 73, 77, 76, 85, 78, 79, 80,
	81, 82, 83, 84, 66, 67, 68, 69, 70, 71,
	72, -29, -34, -29, -3, -37, -34, -34, -74, -34,
	-34, -74, -74, -41, -74, -74, -46, -34, -25, -59,
	-70, -28, 10, -60, -34, -68, -72, 20, -67, 102,
	-64, 93, 91, 26, 92, 13, 32, -70, -70, -72,
	-21, -22, -24, -74, -70, -41, -19, -68, 80, -29,
	-29, -34, -36, -74, -41, 36, 75, 74, 61, -34,
	-34, -35, 55, 60, 56, 57, -34, -34, -34, -34,
	-34, -34, -34, -34, -34, -34, -75, 106, -75, 53,
	-75, -34, -75, -18, 18, -18, -33, -44, -45, 63,
	-56, 27, -74, -28, -50, 13, -29, 54, -68, -72,
	-65, 98, -28, 53, -23, 43, 44, 45, 46, 47,
	49, 50, -71, -70, 19, -22, 88, -37, -36, -34,
	-34, 59, 55, 56, 57, -34, -75, -18, -75, 53,
	-47, -45, 65, -29, -32, 30, -3, -59, -57, -42,
	-50, -54, 15, 14, -70, -70, -48, 11, -22, -22,
	43, 48, 43, 48, 43, 43, 43, -26, 51, 99,
	52, -70, -75, -70, -75, 59, -34, -75, -33, 89,
	-34, 64, -58, 54, -38, -39, -58, -75, 53, -54,
	-34, -51, -52, -34, -72, -49, 12, 14, 54, 43,
	43, 96, 96, 96, -34, -75, -34, 24, 53, -42,
	53, 53, -53, 21, 22, -50, -29, -37, -29, -74,
	-74, -74, 25, -39, -34, -52, -54, -27, -68, -27,
	-27, 7, -55, 16, 28, -75, 53, -75, -75, -59,
	7, 75, -68, -68, -68,
}
var yyDef = [...]int{

	0, -2, 1, 2, 3, 4, 5, 6, 7, 8,
	9, 10, 11, 12, 34, 34, 34, 34, 34, 202,
	193, 0, 0, 0, 210, 210, 210, 0, 38, 40,
	41, 42, 43, 36, 193, 0, 0, 0, 191, 0,
	0, 203, 0, 0, 194, 0, 189, 0, 189, 0,
	31, 32, 33, 14, 39, 0, 44, 35, 0, 0,
	76, 207, 0, 19, 186, 0, 150, 0, -2, 0,
	0, 0, 210, 206, 0, 210, 0, 0, 0, 0,
	0, 30, 0, 45, 47, 52, 0, 50, 51, 86,
	0, 0, 120, 121, 122, 0, 0, 0, 150, 0,
	140, 0, 0, 208, 152, 153, 154, 155, 185, 141,
	143, 37, 0, 0, 0, 84, 0, 0, 0, 210,
	0, 204, 22, 0, 25, 0, 27, 190, 0, 210,
	0, 0, 48, 53, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 107, 108, 109, 110, 111, 112,
	113, 89, 0, 0, 0, 0, 118, 133, 0, 134,
	135, 0, 0, 99, 0, 0, 0, 144, 174, 84,
	77, 160, 0, 187, 188, 151, 20, 192, 0, 0,
	210, 200, 195, 196, 197, 198, 199, 26, 28, 29,
	84, 55, 61, 0, 73, 75, 46, 54, 49, 87,
	88, 91, 92, 0, 115, 116, 0, 0, 0, 94,
	0, 98, 101, 0, 103, 105, 123, 124, 125, 126,
	127, 128, 129, 130, 131, 132, 90, 209, 117, 0,
	184, 118, 136, 0, 0, 0, 0, 148, 145, 0,
	0, 0, 0, 160, 168, 0, 85, 0, 205, 23,
	0, 201, 156, 0, 0, 64, 65, 0, 0, 0,
	0, 0, 78, 62, 0, 0, 0, 0, 93, 95,
	0, 0, 102, 104, 106, 119, 137, 0, 139, 0,
	0, 146, 0, 0, 178, 0, 181, 178, 0, 176,
	168, 18, 0, 0, 210, 24, 158, 0, 56, 59,
	66, 0, 68, 0, 70, 71, 72, 57, 0, 0,
	0, 63, 58, 74, 114, 0, 96, 138, 0, 142,
	149, 0, 15, 0, 180, 182, 16, 175, 0, 17,
	169, 161, 162, 165, 21, 160, 0, 0, 0, 67,
	69, 0, 0, 0, 97, 100, 147, 0, 0, 177,
	0, 0, 164, 166, 167, 168, 159, 157, 60, 0,
	0, 0, 0, 183, 170, 163, 171, 0, 82, 0,
	0, 0, 13, 0, 0, 79, 0, 80, 81, 179,
	172, 0, 83, 0, 173,
}
var yyTok1 = [...]int{

	1, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 82, 77, 3,
	38, 106, 80, 78, 53, 79, 88, 81, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	67, 66, 68, 3, 3, 3, 3, 3, 3, 3,
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
	32, 33, 34, 35, 36, 37, 39, 40, 41, 42,
	43, 44, 45, 46, 47, 48, 49, 50, 51, 52,
	54, 55, 56, 57, 58, 59, 60, 61, 62, 63,
	64, 65, 69, 70, 71, 72, 73, 74, 75, 83,
	84, 87, 89, 90, 91, 92, 93, 94, 95, 96,
	97, 98, 99, 100, 101, 102, 103, 104, 105,
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
		//line sql.y:560
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: yyDollar[2].str, Right: yyDollar[3].valExpr}
		}
	case 92:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:564
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: AST_IN, Right: yyDollar[3].colTuple}
		}
	case 93:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:568
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: AST_NOT_IN, Right: yyDollar[4].colTuple}
		}
	case 94:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:572
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: AST_LIKE, Right: yyDollar[3].valExpr}
		}
	case 95:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:576
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: AST_NOT_LIKE, Right: yyDollar[4].valExpr}
		}
	case 96:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:580
		{
			yyVAL.boolExpr = &RangeCond{Left: yyDollar[1].valExpr, Operator: AST_BETWEEN, From: yyDollar[3].valExpr, To: yyDollar[5].valExpr}
		}
	case 97:
		yyDollar = yyS[yypt-6 : yypt+1]
		//line sql.y:584
		{
			yyVAL.boolExpr = &RangeCond{Left: yyDollar[1].valExpr, Operator: AST_NOT_BETWEEN, From: yyDollar[4].valExpr, To: yyDollar[6].valExpr}
		}
	case 98:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:588
		{
			yyVAL.boolExpr = &IsExpr{Operator: yyDollar[3].str, Expr: yyDollar[1].valExpr}
		}
	case 99:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:592
		{
			yyVAL.boolExpr = &ExistsExpr{Subquery: yyDollar[2].subquery}
		}
	case 100:
		yyDollar = yyS[yypt-6 : yypt+1]
		//line sql.y:596
		{
			yyVAL.boolExpr = &KeyrangeExpr{Start: yyDollar[3].valExpr, End: yyDollar[5].valExpr}
		}
	case 101:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:602
		{
			yyVAL.str = AST_IS_NULL
		}
	case 102:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:606
		{
			yyVAL.str = AST_IS_NOT_NULL
		}
	case 103:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:610
		{
			yyVAL.str = AST_IS_TRUE
		}
	case 104:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:614
		{
			yyVAL.str = AST_IS_NOT_TRUE
		}
	case 105:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:618
		{
			yyVAL.str = AST_IS_FALSE
		}
	case 106:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:622
		{
			yyVAL.str = AST_IS_NOT_FALSE
		}
	case 107:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:628
		{
			yyVAL.str = AST_EQ
		}
	case 108:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:632
		{
			yyVAL.str = AST_LT
		}
	case 109:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:636
		{
			yyVAL.str = AST_GT
		}
	case 110:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:640
		{
			yyVAL.str = AST_LE
		}
	case 111:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:644
		{
			yyVAL.str = AST_GE
		}
	case 112:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:648
		{
			yyVAL.str = AST_NE
		}
	case 113:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:652
		{
			yyVAL.str = AST_NSE
		}
	case 114:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:658
		{
			yyVAL.colTuple = ValTuple(yyDollar[2].valExprs)
		}
	case 115:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:662
		{
			yyVAL.colTuple = yyDollar[1].subquery
		}
	case 116:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:666
		{
			yyVAL.colTuple = ListArg(yyDollar[1].bytes)
		}
	case 117:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:672
		{
			yyVAL.subquery = &Subquery{yyDollar[2].selStmt}
		}
	case 118:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:678
		{
			yyVAL.valExprs = ValExprs{yyDollar[1].valExpr}
		}
	case 119:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:682
		{
			yyVAL.valExprs = append(yyDollar[1].valExprs, yyDollar[3].valExpr)
		}
	case 120:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:688
		{
			yyVAL.valExpr = yyDollar[1].valExpr
		}
	case 121:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:692
		{
			yyVAL.valExpr = yyDollar[1].colName
		}
	case 122:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:696
		{
			yyVAL.valExpr = yyDollar[1].rowTuple
		}
	case 123:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:700
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: AST_BITAND, Right: yyDollar[3].valExpr}
		}
	case 124:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:704
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: AST_BITOR, Right: yyDollar[3].valExpr}
		}
	case 125:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:708
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: AST_BITXOR, Right: yyDollar[3].valExpr}
		}
	case 126:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:712
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: AST_PLUS, Right: yyDollar[3].valExpr}
		}
	case 127:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:716
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: AST_MINUS, Right: yyDollar[3].valExpr}
		}
	case 128:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:720
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: AST_MULT, Right: yyDollar[3].valExpr}
		}
	case 129:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:724
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: AST_DIV, Right: yyDollar[3].valExpr}
		}
	case 130:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:728
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: AST_MOD, Right: yyDollar[3].valExpr}
		}
	case 131:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:732
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: AST_SHIFT_LEFT, Right: yyDollar[3].valExpr}
		}
	case 132:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:736
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: AST_SHIFT_RIGHT, Right: yyDollar[3].valExpr}
		}
	case 133:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:740
		{
			if num, ok := yyDollar[2].valExpr.(NumVal); ok {
				yyVAL.valExpr = num
			} else {
				yyVAL.valExpr = &UnaryExpr{Operator: AST_UPLUS, Expr: yyDollar[2].valExpr}
			}
		}
	case 134:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:748
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
	case 135:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:761
		{
			yyVAL.valExpr = &UnaryExpr{Operator: AST_TILDA, Expr: yyDollar[2].valExpr}
		}
	case 136:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:765
		{
			yyVAL.valExpr = &FuncExpr{Name: string(yyDollar[1].sqlID)}
		}
	case 137:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:769
		{
			yyVAL.valExpr = &FuncExpr{Name: string(yyDollar[1].sqlID), Exprs: yyDollar[3].selectExprs}
		}
	case 138:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:773
		{
			yyVAL.valExpr = &FuncExpr{Name: string(yyDollar[1].sqlID), Distinct: true, Exprs: yyDollar[4].selectExprs}
		}
	case 139:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:777
		{
			yyVAL.valExpr = &FuncExpr{Name: yyDollar[1].str, Exprs: yyDollar[3].selectExprs}
		}
	case 140:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:781
		{
			yyVAL.valExpr = yyDollar[1].caseExpr
		}
	case 141:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:787
		{
			yyVAL.str = "if"
		}
	case 142:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:793
		{
			yyVAL.caseExpr = &CaseExpr{Expr: yyDollar[2].valExpr, Whens: yyDollar[3].whens, Else: yyDollar[4].valExpr}
		}
	case 143:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:798
		{
			yyVAL.valExpr = nil
		}
	case 144:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:802
		{
			yyVAL.valExpr = yyDollar[1].valExpr
		}
	case 145:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:808
		{
			yyVAL.whens = []*When{yyDollar[1].when}
		}
	case 146:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:812
		{
			yyVAL.whens = append(yyDollar[1].whens, yyDollar[2].when)
		}
	case 147:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:818
		{
			yyVAL.when = &When{Cond: yyDollar[2].boolExpr, Val: yyDollar[4].valExpr}
		}
	case 148:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:823
		{
			yyVAL.valExpr = nil
		}
	case 149:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:827
		{
			yyVAL.valExpr = yyDollar[2].valExpr
		}
	case 150:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:833
		{
			yyVAL.colName = &ColName{Name: yyDollar[1].sqlID}
		}
	case 151:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:837
		{
			yyVAL.colName = &ColName{Qualifier: yyDollar[1].sqlID, Name: yyDollar[3].sqlID}
		}
	case 152:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:843
		{
			yyVAL.valExpr = StrVal(yyDollar[1].bytes)
		}
	case 153:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:847
		{
			yyVAL.valExpr = NumVal(yyDollar[1].bytes)
		}
	case 154:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:851
		{
			yyVAL.valExpr = ValArg(yyDollar[1].bytes)
		}
	case 155:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:855
		{
			yyVAL.valExpr = &NullVal{}
		}
	case 156:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:860
		{
			yyVAL.valExprs = nil
		}
	case 157:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:864
		{
			yyVAL.valExprs = yyDollar[3].valExprs
		}
	case 158:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:869
		{
			yyVAL.boolExpr = nil
		}
	case 159:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:873
		{
			yyVAL.boolExpr = yyDollar[2].boolExpr
		}
	case 160:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:878
		{
			yyVAL.orderBy = nil
		}
	case 161:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:882
		{
			yyVAL.orderBy = yyDollar[3].orderBy
		}
	case 162:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:888
		{
			yyVAL.orderBy = OrderBy{yyDollar[1].order}
		}
	case 163:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:892
		{
			yyVAL.orderBy = append(yyDollar[1].orderBy, yyDollar[3].order)
		}
	case 164:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:898
		{
			yyVAL.order = &Order{Expr: yyDollar[1].valExpr, Direction: yyDollar[2].str}
		}
	case 165:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:903
		{
			yyVAL.str = AST_ASC
		}
	case 166:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:907
		{
			yyVAL.str = AST_ASC
		}
	case 167:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:911
		{
			yyVAL.str = AST_DESC
		}
	case 168:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:916
		{
			yyVAL.limit = nil
		}
	case 169:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:920
		{
			yyVAL.limit = &Limit{Rowcount: yyDollar[2].valExpr}
		}
	case 170:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:924
		{
			yyVAL.limit = &Limit{Offset: yyDollar[2].valExpr, Rowcount: yyDollar[4].valExpr}
		}
	case 171:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:929
		{
			yyVAL.str = ""
		}
	case 172:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:933
		{
			yyVAL.str = AST_FOR_UPDATE
		}
	case 173:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:937
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
	case 174:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:950
		{
			yyVAL.columns = nil
		}
	case 175:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:954
		{
			yyVAL.columns = yyDollar[2].columns
		}
	case 176:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:960
		{
			yyVAL.columns = Columns{&NonStarExpr{Expr: yyDollar[1].colName}}
		}
	case 177:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:964
		{
			yyVAL.columns = append(yyVAL.columns, &NonStarExpr{Expr: yyDollar[3].colName})
		}
	case 178:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:969
		{
			yyVAL.updateExprs = nil
		}
	case 179:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:973
		{
			yyVAL.updateExprs = yyDollar[5].updateExprs
		}
	case 180:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:979
		{
			yyVAL.insRows = yyDollar[2].values
		}
	case 181:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:983
		{
			yyVAL.insRows = yyDollar[1].selStmt
		}
	case 182:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:989
		{
			yyVAL.values = Values{yyDollar[1].rowTuple}
		}
	case 183:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:993
		{
			yyVAL.values = append(yyDollar[1].values, yyDollar[3].rowTuple)
		}
	case 184:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:999
		{
			yyVAL.rowTuple = ValTuple(yyDollar[2].valExprs)
		}
	case 185:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1003
		{
			yyVAL.rowTuple = yyDollar[1].subquery
		}
	case 186:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1009
		{
			yyVAL.updateExprs = UpdateExprs{yyDollar[1].updateExpr}
		}
	case 187:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1013
		{
			yyVAL.updateExprs = append(yyDollar[1].updateExprs, yyDollar[3].updateExpr)
		}
	case 188:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1019
		{
			yyVAL.updateExpr = &UpdateExpr{Name: yyDollar[1].colName, Expr: yyDollar[3].valExpr}
		}
	case 189:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1024
		{
			yyVAL.empty = struct{}{}
		}
	case 190:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:1026
		{
			yyVAL.empty = struct{}{}
		}
	case 191:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1029
		{
			yyVAL.empty = struct{}{}
		}
	case 192:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1031
		{
			yyVAL.empty = struct{}{}
		}
	case 193:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1034
		{
			yyVAL.str = ""
		}
	case 194:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1036
		{
			yyVAL.str = AST_IGNORE
		}
	case 195:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1040
		{
			yyVAL.empty = struct{}{}
		}
	case 196:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1042
		{
			yyVAL.empty = struct{}{}
		}
	case 197:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1044
		{
			yyVAL.empty = struct{}{}
		}
	case 198:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1046
		{
			yyVAL.empty = struct{}{}
		}
	case 199:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1048
		{
			yyVAL.empty = struct{}{}
		}
	case 200:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1051
		{
			yyVAL.empty = struct{}{}
		}
	case 201:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1053
		{
			yyVAL.empty = struct{}{}
		}
	case 202:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1056
		{
			yyVAL.empty = struct{}{}
		}
	case 203:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1058
		{
			yyVAL.empty = struct{}{}
		}
	case 204:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1061
		{
			yyVAL.empty = struct{}{}
		}
	case 205:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:1063
		{
			yyVAL.empty = struct{}{}
		}
	case 206:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1067
		{
			yyVAL.sqlID = SQLName(strings.ToLower(string(yyDollar[1].bytes)))
		}
	case 207:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1073
		{
			yyVAL.sqlID = SQLName(yyDollar[1].bytes)
		}
	case 208:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1079
		{
			if incNesting(yylex) {
				yylex.Error("max nesting level reached")
				return 1
			}
		}
	case 209:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1088
		{
			decNesting(yylex)
		}
	case 210:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1093
		{
			forceEOF(yylex)
		}
	}
	goto yystack /* stack new state and value */
}
