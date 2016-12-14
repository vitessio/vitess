//line ./go/vt/sqlparser/sql.y:6
package sqlparser

import __yyfmt__ "fmt"

//line ./go/vt/sqlparser/sql.y:6
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

//line ./go/vt/sqlparser/sql.y:34
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
	valTuple    ValTuple
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
	colIdent    ColIdent
	colIdents   []ColIdent
	tableIdent  TableIdent
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
const ID_SPECIAL = 57388
const HEX = 57389
const STRING = 57390
const NUMBER = 57391
const HEXNUM = 57392
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
const END = 57407
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
const MOD = 57418
const UNARY = 57419
const INTERVAL = 57420
const JSON_EXTRACT_OP = 57421
const JSON_UNQUOTE_EXTRACT_OP = 57422
const CREATE = 57423
const ALTER = 57424
const DROP = 57425
const RENAME = 57426
const ANALYZE = 57427
const TABLE = 57428
const INDEX = 57429
const VIEW = 57430
const TO = 57431
const IGNORE = 57432
const IF = 57433
const UNIQUE = 57434
const USING = 57435
const SHOW = 57436
const DESCRIBE = 57437
const EXPLAIN = 57438
const CURRENT_TIMESTAMP = 57439
const DATABASE = 57440
const UNUSED = 57441

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
	"ID_SPECIAL",
	"HEX",
	"STRING",
	"NUMBER",
	"HEXNUM",
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
	"END",
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
	"MOD",
	"'^'",
	"'~'",
	"UNARY",
	"INTERVAL",
	"'.'",
	"JSON_EXTRACT_OP",
	"JSON_UNQUOTE_EXTRACT_OP",
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
	"CURRENT_TIMESTAMP",
	"DATABASE",
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
	-1, 109,
	45, 238,
	94, 238,
	-2, 236,
}

const yyNprod = 242
const yyPrivate = 57344

var yyTokenNames []string
var yyStates []string

const yyLast = 930

var yyAct = [...]int{

	133, 358, 251, 425, 62, 226, 152, 208, 305, 314,
	240, 296, 115, 347, 263, 113, 102, 238, 207, 3,
	257, 242, 147, 103, 156, 74, 116, 67, 35, 125,
	37, 239, 377, 379, 38, 64, 41, 107, 69, 162,
	235, 71, 47, 40, 408, 41, 50, 43, 44, 45,
	407, 97, 160, 126, 58, 81, 109, 406, 128, 127,
	129, 130, 131, 68, 70, 132, 123, 124, 48, 49,
	111, 46, 138, 164, 42, 210, 211, 101, 126, 329,
	126, 87, 188, 88, 431, 112, 178, 64, 63, 270,
	64, 145, 117, 118, 104, 109, 378, 137, 90, 119,
	388, 120, 297, 268, 269, 267, 172, 144, 92, 84,
	177, 176, 94, 175, 96, 134, 390, 254, 112, 112,
	14, 135, 136, 159, 161, 158, 178, 229, 216, 174,
	177, 176, 218, 248, 153, 191, 192, 193, 194, 188,
	163, 176, 65, 297, 167, 345, 178, 177, 176, 204,
	206, 224, 217, 65, 247, 112, 178, 232, 175, 126,
	60, 221, 60, 178, 258, 260, 261, 325, 225, 259,
	332, 333, 334, 109, 245, 249, 112, 244, 149, 126,
	93, 75, 112, 112, 112, 126, 228, 264, 60, 266,
	205, 236, 252, 237, 173, 60, 170, 243, 433, 254,
	233, 128, 127, 129, 130, 131, 274, 265, 132, 286,
	287, 289, 288, 254, 255, 256, 292, 169, 254, 293,
	73, 28, 65, 112, 64, 303, 290, 291, 301, 148,
	249, 304, 169, 294, 189, 190, 191, 192, 193, 194,
	188, 300, 65, 312, 254, 348, 82, 89, 83, 394,
	89, 244, 354, 254, 288, 330, 395, 392, 312, 223,
	112, 402, 328, 254, 89, 126, 76, 348, 231, 108,
	331, 243, 335, 100, 264, 372, 126, 79, 405, 404,
	373, 150, 336, 187, 186, 195, 196, 189, 190, 191,
	192, 193, 194, 188, 265, 370, 374, 342, 320, 321,
	371, 369, 368, 209, 112, 353, 351, 344, 212, 213,
	214, 215, 350, 355, 55, 352, 39, 143, 142, 244,
	244, 244, 244, 14, 365, 423, 367, 54, 86, 220,
	383, 382, 375, 411, 384, 346, 391, 424, 385, 243,
	243, 243, 243, 230, 364, 85, 366, 299, 57, 154,
	99, 360, 327, 51, 52, 393, 141, 306, 363, 307,
	108, 227, 362, 140, 311, 148, 108, 399, 61, 112,
	262, 401, 430, 271, 272, 273, 421, 275, 276, 277,
	278, 279, 280, 281, 282, 283, 284, 285, 195, 196,
	189, 190, 191, 192, 193, 194, 188, 414, 14, 351,
	400, 28, 30, 151, 1, 412, 326, 108, 415, 112,
	112, 323, 246, 418, 419, 420, 171, 155, 36, 426,
	426, 426, 64, 427, 428, 234, 429, 157, 432, 66,
	434, 435, 436, 139, 437, 29, 59, 438, 302, 222,
	416, 417, 422, 396, 108, 357, 72, 121, 361, 310,
	77, 31, 32, 33, 34, 230, 343, 219, 295, 337,
	338, 339, 122, 114, 349, 59, 14, 15, 16, 17,
	91, 80, 298, 179, 95, 110, 376, 98, 315, 341,
	313, 253, 106, 125, 241, 168, 105, 78, 18, 59,
	53, 146, 27, 56, 13, 12, 11, 356, 359, 10,
	9, 165, 8, 7, 166, 6, 5, 126, 4, 254,
	109, 2, 128, 127, 129, 130, 131, 0, 0, 132,
	123, 124, 0, 0, 111, 0, 138, 0, 0, 0,
	0, 387, 0, 0, 0, 0, 389, 0, 0, 0,
	0, 0, 230, 59, 0, 0, 117, 118, 104, 0,
	0, 137, 0, 119, 230, 120, 0, 19, 20, 22,
	21, 23, 0, 0, 0, 0, 0, 0, 0, 134,
	24, 25, 26, 106, 59, 135, 136, 409, 250, 106,
	0, 410, 181, 184, 0, 413, 359, 0, 0, 197,
	198, 199, 200, 201, 202, 203, 185, 182, 183, 180,
	187, 186, 195, 196, 189, 190, 191, 192, 193, 194,
	188, 0, 0, 0, 0, 397, 398, 0, 0, 0,
	106, 316, 319, 320, 321, 317, 0, 318, 322, 125,
	0, 403, 0, 250, 0, 308, 0, 0, 309, 0,
	0, 0, 0, 0, 0, 0, 324, 0, 59, 0,
	0, 0, 0, 126, 0, 254, 109, 106, 128, 127,
	129, 130, 131, 0, 0, 132, 123, 124, 0, 0,
	111, 0, 138, 187, 186, 195, 196, 189, 190, 191,
	192, 193, 194, 188, 316, 319, 320, 321, 317, 14,
	318, 322, 117, 118, 104, 0, 0, 137, 0, 119,
	0, 120, 0, 0, 125, 186, 195, 196, 189, 190,
	191, 192, 193, 194, 188, 134, 59, 59, 59, 59,
	0, 135, 136, 0, 0, 0, 0, 0, 126, 380,
	381, 109, 0, 128, 127, 129, 130, 131, 0, 0,
	132, 123, 124, 0, 0, 111, 0, 138, 0, 0,
	0, 125, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 14, 0, 0, 0, 0, 117, 118, 0,
	0, 0, 137, 0, 119, 126, 120, 0, 109, 0,
	128, 127, 129, 130, 131, 0, 0, 132, 123, 124,
	134, 0, 111, 0, 138, 0, 135, 136, 0, 0,
	0, 126, 0, 0, 109, 0, 128, 127, 129, 130,
	131, 0, 0, 132, 117, 118, 0, 0, 0, 137,
	138, 119, 0, 120, 0, 0, 0, 126, 0, 0,
	109, 386, 128, 127, 129, 130, 131, 134, 0, 132,
	117, 118, 0, 135, 136, 137, 138, 119, 0, 120,
	187, 186, 195, 196, 189, 190, 191, 192, 193, 194,
	188, 0, 0, 134, 0, 0, 117, 118, 0, 135,
	136, 137, 0, 119, 0, 120, 65, 0, 340, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 134,
	0, 0, 0, 0, 0, 135, 136, 187, 186, 195,
	196, 189, 190, 191, 192, 193, 194, 188, 187, 186,
	195, 196, 189, 190, 191, 192, 193, 194, 188, 187,
	186, 195, 196, 189, 190, 191, 192, 193, 194, 188,
}
var yyPact = [...]int{

	460, -1000, -1000, 396, -1000, -1000, -1000, -1000, -1000, -1000,
	-1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, -74,
	-61, -28, -55, -31, -1000, -1000, -1000, 392, 335, 295,
	-1000, -70, 112, 358, 94, -80, -40, 94, -1000, -38,
	94, -1000, 112, -82, 133, -82, 112, -1000, -1000, -1000,
	-1000, -1000, -1000, 242, 194, -1000, 53, 321, 300, -13,
	-1000, 112, 204, -1000, 29, -1000, 112, 46, 132, -1000,
	112, -1000, -54, 112, 329, 229, 94, -1000, 8, -1000,
	346, -1000, 288, 287, -1000, 112, 94, 112, 354, 94,
	782, -1000, 328, -85, -1000, 25, -1000, 112, -1000, -1000,
	112, -1000, 186, -1000, -1000, 174, 35, 87, 520, -1000,
	-1000, 730, 683, -1000, -20, -1000, -1000, 782, 782, 782,
	782, 220, -1000, -1000, -1000, 220, -1000, -1000, -1000, -1000,
	-1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, 782, 112,
	-1000, -1000, -1000, -1000, 231, 218, -1000, 347, 730, -1000,
	839, 33, 756, -1000, -1000, 224, 94, -1000, -65, -1000,
	-1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, 354, 8,
	140, -1000, -1000, 105, 47, 462, 730, 730, 107, 782,
	134, 26, 782, 782, 782, 107, 782, 782, 782, 782,
	782, 782, 782, 782, 782, 782, 782, -1000, -1000, -1000,
	-1000, -1000, -1000, -1000, 10, 520, 70, 216, 166, 520,
	151, 151, -1000, -1000, -1000, 828, 608, -1000, 392, 37,
	839, -1000, 317, 94, 94, 347, 341, 344, 87, 125,
	839, 112, -1000, -1000, 112, -1000, 352, -1000, 212, 650,
	-1000, -1000, 147, 332, 114, -1000, -1000, -1000, -1000, -1000,
	-15, -1000, 171, 8, -1000, 10, 80, -1000, -1000, 113,
	-1000, -1000, 839, -1000, 756, -1000, -1000, 134, 782, 782,
	782, 839, 839, 817, -1000, 306, 624, -1000, 49, 49,
	-8, -8, -8, -8, 150, 150, -1000, -1000, 782, -1000,
	-1000, -1000, -1000, -1000, 171, 78, -1000, 730, 223, 220,
	396, 201, 206, -1000, 341, -1000, 782, 782, -1000, -1000,
	349, 343, 140, 140, 140, 140, -1000, 268, 267, -1000,
	261, 241, 262, -10, -1000, 112, 112, -1000, 197, 94,
	-1000, 171, -1000, -1000, -1000, 166, -1000, 839, 839, 770,
	782, 839, -1000, 32, -1000, 782, 50, -1000, 311, 211,
	-1000, 782, -1000, -1000, 94, -1000, 203, 210, -1000, 593,
	-1000, 347, 730, 782, 650, 217, 587, -1000, -1000, -1000,
	-1000, 245, -1000, 244, -1000, -1000, -1000, -46, -53, -59,
	-1000, -1000, -1000, -1000, -1000, -1000, 782, 839, -1000, 839,
	782, 307, 220, -1000, 782, 782, -1000, -1000, -1000, 341,
	87, 208, 730, 730, -1000, -1000, 220, 220, 220, 839,
	839, 368, -1000, 839, -1000, 308, 87, 87, 94, 94,
	94, 94, -1000, 364, 5, 152, -1000, 152, 152, 204,
	-1000, 94, -1000, 94, -1000, -1000, 94, -1000, -1000,
}
var yyPgo = [...]int{

	0, 511, 18, 508, 506, 505, 503, 502, 500, 499,
	496, 495, 494, 435, 493, 492, 490, 487, 16, 23,
	486, 485, 17, 31, 10, 484, 480, 9, 478, 21,
	476, 3, 22, 37, 475, 473, 472, 15, 190, 471,
	20, 14, 7, 464, 12, 26, 463, 462, 458, 11,
	457, 456, 449, 448, 447, 5, 445, 1, 443, 8,
	442, 439, 438, 13, 4, 88, 433, 316, 220, 429,
	427, 425, 418, 417, 0, 416, 412, 403, 411, 406,
	42, 404, 402, 6, 2,
}
var yyR1 = [...]int{

	0, 81, 1, 1, 1, 1, 1, 1, 1, 1,
	1, 1, 1, 2, 2, 2, 3, 3, 4, 5,
	6, 7, 7, 7, 8, 8, 8, 9, 10, 10,
	10, 11, 12, 12, 12, 82, 13, 14, 14, 15,
	15, 15, 16, 16, 17, 17, 18, 18, 19, 19,
	19, 20, 20, 75, 75, 75, 75, 21, 21, 22,
	22, 23, 23, 24, 24, 24, 25, 25, 25, 25,
	79, 79, 78, 78, 78, 26, 26, 26, 26, 27,
	27, 27, 27, 28, 28, 29, 29, 30, 30, 30,
	30, 31, 31, 32, 32, 33, 33, 33, 33, 33,
	33, 34, 34, 34, 34, 34, 34, 34, 34, 34,
	34, 34, 34, 34, 40, 40, 40, 40, 40, 40,
	35, 35, 35, 35, 35, 35, 35, 41, 41, 41,
	45, 42, 42, 38, 38, 38, 38, 38, 38, 38,
	38, 38, 38, 38, 38, 38, 38, 38, 38, 38,
	38, 38, 38, 38, 38, 38, 38, 38, 38, 38,
	54, 54, 54, 54, 47, 50, 50, 48, 48, 49,
	51, 51, 46, 46, 46, 37, 37, 37, 37, 37,
	37, 39, 39, 39, 52, 52, 53, 53, 55, 55,
	56, 56, 57, 58, 58, 58, 59, 59, 59, 60,
	60, 60, 61, 61, 62, 62, 63, 63, 36, 36,
	43, 43, 44, 64, 64, 65, 66, 66, 68, 68,
	69, 69, 67, 67, 70, 70, 70, 70, 70, 70,
	71, 71, 72, 72, 73, 73, 74, 76, 77, 83,
	84, 80,
}
var yyR2 = [...]int{

	0, 1, 1, 1, 1, 1, 1, 1, 1, 1,
	1, 1, 1, 12, 6, 3, 8, 8, 8, 7,
	3, 5, 8, 4, 6, 7, 4, 5, 4, 5,
	5, 3, 2, 2, 2, 0, 2, 0, 2, 1,
	2, 2, 0, 1, 0, 1, 1, 3, 1, 2,
	3, 1, 1, 0, 1, 2, 2, 0, 2, 1,
	3, 1, 1, 3, 3, 3, 3, 5, 5, 3,
	0, 1, 0, 1, 2, 1, 2, 2, 1, 2,
	3, 2, 3, 2, 2, 1, 3, 0, 5, 5,
	5, 1, 3, 0, 2, 1, 3, 3, 2, 3,
	3, 1, 1, 3, 3, 4, 3, 4, 3, 4,
	5, 6, 3, 2, 1, 2, 1, 2, 1, 2,
	1, 1, 1, 1, 1, 1, 1, 3, 1, 1,
	3, 1, 3, 1, 1, 1, 1, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	2, 2, 2, 3, 3, 4, 5, 3, 4, 1,
	1, 1, 1, 1, 5, 0, 1, 1, 2, 4,
	0, 2, 1, 3, 5, 1, 1, 1, 1, 1,
	1, 1, 2, 2, 0, 3, 0, 2, 0, 3,
	1, 3, 2, 0, 1, 1, 0, 2, 4, 0,
	2, 4, 0, 3, 1, 3, 0, 5, 2, 1,
	1, 3, 3, 1, 3, 3, 1, 1, 0, 2,
	0, 3, 0, 1, 1, 1, 1, 1, 1, 1,
	0, 1, 0, 1, 0, 2, 1, 1, 1, 1,
	1, 0,
}
var yyChk = [...]int{

	-1000, -81, -1, -2, -3, -4, -5, -6, -7, -8,
	-9, -10, -11, -12, 6, 7, 8, 9, 28, 97,
	98, 100, 99, 101, 110, 111, 112, -15, 5, -13,
	-82, -13, -13, -13, -13, 102, -72, 104, 108, -67,
	104, 106, 102, 102, 103, 104, 102, -80, -80, -80,
	-2, 18, 19, -16, 32, 19, -14, -67, -29, -77,
	48, 10, -64, -65, -74, 48, -69, 107, 103, -74,
	102, -74, -77, -68, 107, 48, -68, -77, -17, 35,
	-39, -74, 52, 54, 56, 24, 28, 94, -29, 46,
	69, -77, 62, 48, -80, -77, -80, 105, -77, 21,
	44, -74, -18, -19, 86, -20, -77, -33, -38, 48,
	-34, 62, -83, -37, -46, -44, -45, 84, 85, 91,
	93, -54, -47, 58, 59, 21, 45, 51, 50, 52,
	53, 54, 57, -74, 107, 113, 114, 89, 64, -66,
	17, 10, 30, 30, -29, -64, -77, -32, 11, -65,
	-38, -77, -83, -80, 21, -73, 109, -70, 100, 98,
	27, 99, 14, 115, 48, -77, -77, -80, -21, 46,
	10, -75, -74, 20, 94, -83, 61, 60, 76, -35,
	79, 62, 77, 78, 63, 76, 81, 80, 90, 84,
	85, 86, 87, 88, 89, 82, 83, 69, 70, 71,
	72, 73, 74, 75, -33, -38, -33, -2, -42, -38,
	95, 96, -38, -38, -38, -38, -83, -45, -83, -50,
	-38, -29, -61, 28, -83, -32, -55, 14, -33, 94,
	-38, 44, -74, -80, -71, 105, -32, -19, -22, -23,
	-24, -25, -29, -45, -83, -74, -76, 49, 86, -74,
	-77, -84, -18, 19, 47, -33, -33, -40, 57, 62,
	58, 59, -38, -41, -83, -45, 55, 79, 77, 78,
	63, -38, -38, -38, -40, -38, -38, -38, -38, -38,
	-38, -38, -38, -38, -38, -38, -84, -84, 46, -84,
	-37, -37, -74, -84, -18, -48, -49, 65, -36, 30,
	-2, -64, -62, -74, -55, -59, 16, 15, -77, -77,
	-52, 12, 46, -26, -27, -28, 34, 38, 40, 35,
	36, 37, 41, -78, -77, 20, -79, 20, -22, 94,
	-84, -18, 57, 58, 59, -42, -41, -38, -38, -38,
	61, -38, -84, -51, -49, 67, -33, -63, 44, -43,
	-44, -83, -63, -84, 46, -59, -38, -56, -57, -38,
	-80, -53, 13, 15, -23, -24, -23, -24, 34, 34,
	34, 39, 34, 39, 34, -27, -30, 42, 106, 43,
	-77, -77, -84, -74, -84, -84, 61, -38, 68, -38,
	66, 25, 46, -74, 46, 46, -58, 22, 23, -55,
	-33, -42, 44, 44, 34, 34, 103, 103, 103, -38,
	-38, 26, -44, -38, -57, -59, -33, -33, -83, -83,
	-83, 8, -60, 17, 29, -31, -74, -31, -31, -64,
	8, 79, -84, 46, -84, -84, -74, -74, -74,
}
var yyDef = [...]int{

	0, -2, 1, 2, 3, 4, 5, 6, 7, 8,
	9, 10, 11, 12, 35, 35, 35, 35, 35, 232,
	222, 0, 0, 0, 241, 241, 241, 0, 39, 42,
	37, 222, 0, 0, 0, 220, 0, 0, 233, 0,
	0, 223, 0, 218, 0, 218, 0, 32, 33, 34,
	15, 40, 41, 44, 0, 43, 36, 0, 0, 85,
	238, 0, 20, 213, 0, 236, 0, 0, 0, 241,
	0, 241, 0, 0, 0, 0, 0, 31, 0, 45,
	0, 181, 0, 0, 38, 0, 0, 0, 93, 0,
	0, 241, 0, 234, 23, 0, 26, 0, 28, 219,
	0, 241, 57, 46, 48, 53, 0, 51, 52, -2,
	95, 0, 0, 133, 134, 135, 136, 0, 0, 0,
	0, 0, 159, 101, 102, 0, 239, 175, 176, 177,
	178, 179, 180, 172, 160, 161, 162, 163, 165, 0,
	216, 217, 182, 183, 202, 93, 86, 188, 0, 214,
	215, 0, 0, 21, 221, 0, 0, 241, 230, 224,
	225, 226, 227, 228, 229, 27, 29, 30, 93, 0,
	0, 49, 54, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 120, 121, 122,
	123, 124, 125, 126, 98, 0, 0, 0, 0, 131,
	0, 0, 150, 151, 152, 0, 0, 113, 0, 0,
	166, 14, 0, 0, 0, 188, 196, 0, 94, 0,
	131, 0, 235, 24, 0, 231, 184, 47, 58, 59,
	61, 62, 72, 70, 0, 55, 56, 237, 50, 173,
	0, 154, 0, 0, 240, 96, 97, 100, 114, 0,
	116, 118, 103, 104, 0, 128, 129, 0, 0, 0,
	0, 106, 108, 0, 112, 137, 138, 139, 140, 141,
	142, 143, 144, 145, 146, 147, 99, 130, 0, 212,
	148, 149, 153, 157, 0, 170, 167, 0, 206, 0,
	209, 206, 0, 204, 196, 19, 0, 0, 241, 25,
	186, 0, 0, 0, 0, 0, 75, 0, 0, 78,
	0, 0, 0, 87, 73, 0, 0, 71, 0, 0,
	155, 0, 115, 117, 119, 0, 105, 107, 109, 0,
	0, 132, 158, 0, 168, 0, 0, 16, 0, 208,
	210, 0, 17, 203, 0, 18, 197, 189, 190, 193,
	22, 188, 0, 0, 60, 66, 0, 69, 76, 77,
	79, 0, 81, 0, 83, 84, 63, 0, 0, 0,
	74, 64, 65, 174, 156, 127, 0, 110, 164, 171,
	0, 0, 0, 205, 0, 0, 192, 194, 195, 196,
	187, 185, 0, 0, 80, 82, 0, 0, 0, 111,
	169, 0, 211, 198, 191, 199, 67, 68, 0, 0,
	0, 0, 13, 0, 0, 0, 91, 0, 0, 207,
	200, 0, 88, 0, 89, 90, 0, 92, 201,
}
var yyTok1 = [...]int{

	1, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 88, 81, 3,
	45, 47, 86, 84, 46, 85, 94, 87, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	70, 69, 71, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 90, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 80, 3, 91,
}
var yyTok2 = [...]int{

	2, 3, 4, 5, 6, 7, 8, 9, 10, 11,
	12, 13, 14, 15, 16, 17, 18, 19, 20, 21,
	22, 23, 24, 25, 26, 27, 28, 29, 30, 31,
	32, 33, 34, 35, 36, 37, 38, 39, 40, 41,
	42, 43, 44, 48, 49, 50, 51, 52, 53, 54,
	55, 56, 57, 58, 59, 60, 61, 62, 63, 64,
	65, 66, 67, 68, 72, 73, 74, 75, 76, 77,
	78, 79, 82, 83, 89, 92, 93, 95, 96, 97,
	98, 99, 100, 101, 102, 103, 104, 105, 106, 107,
	108, 109, 110, 111, 112, 113, 114, 115,
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
		//line ./go/vt/sqlparser/sql.y:179
		{
			setParseTree(yylex, yyDollar[1].statement)
		}
	case 2:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:185
		{
			yyVAL.statement = yyDollar[1].selStmt
		}
	case 13:
		yyDollar = yyS[yypt-12 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:201
		{
			yyVAL.selStmt = &Select{Comments: Comments(yyDollar[2].bytes2), Distinct: yyDollar[3].str, Hints: yyDollar[4].str, SelectExprs: yyDollar[5].selectExprs, From: yyDollar[6].tableExprs, Where: NewWhere(WhereStr, yyDollar[7].boolExpr), GroupBy: GroupBy(yyDollar[8].valExprs), Having: NewWhere(HavingStr, yyDollar[9].boolExpr), OrderBy: yyDollar[10].orderBy, Limit: yyDollar[11].limit, Lock: yyDollar[12].str}
		}
	case 14:
		yyDollar = yyS[yypt-6 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:205
		{
			yyVAL.selStmt = &Select{Comments: Comments(yyDollar[2].bytes2), SelectExprs: SelectExprs{Nextval{Expr: yyDollar[4].valExpr}}, From: TableExprs{&AliasedTableExpr{Expr: yyDollar[6].tableName}}}
		}
	case 15:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:209
		{
			yyVAL.selStmt = &Union{Type: yyDollar[2].str, Left: yyDollar[1].selStmt, Right: yyDollar[3].selStmt}
		}
	case 16:
		yyDollar = yyS[yypt-8 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:215
		{
			yyVAL.statement = &Insert{Comments: Comments(yyDollar[2].bytes2), Ignore: yyDollar[3].str, Table: yyDollar[5].tableName, Columns: yyDollar[6].columns, Rows: yyDollar[7].insRows, OnDup: OnDup(yyDollar[8].updateExprs)}
		}
	case 17:
		yyDollar = yyS[yypt-8 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:219
		{
			cols := make(Columns, 0, len(yyDollar[7].updateExprs))
			vals := make(ValTuple, 0, len(yyDollar[7].updateExprs))
			for _, updateList := range yyDollar[7].updateExprs {
				cols = append(cols, updateList.Name)
				vals = append(vals, updateList.Expr)
			}
			yyVAL.statement = &Insert{Comments: Comments(yyDollar[2].bytes2), Ignore: yyDollar[3].str, Table: yyDollar[5].tableName, Columns: cols, Rows: Values{vals}, OnDup: OnDup(yyDollar[8].updateExprs)}
		}
	case 18:
		yyDollar = yyS[yypt-8 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:231
		{
			yyVAL.statement = &Update{Comments: Comments(yyDollar[2].bytes2), Table: yyDollar[3].tableName, Exprs: yyDollar[5].updateExprs, Where: NewWhere(WhereStr, yyDollar[6].boolExpr), OrderBy: yyDollar[7].orderBy, Limit: yyDollar[8].limit}
		}
	case 19:
		yyDollar = yyS[yypt-7 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:237
		{
			yyVAL.statement = &Delete{Comments: Comments(yyDollar[2].bytes2), Table: yyDollar[4].tableName, Where: NewWhere(WhereStr, yyDollar[5].boolExpr), OrderBy: yyDollar[6].orderBy, Limit: yyDollar[7].limit}
		}
	case 20:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:243
		{
			yyVAL.statement = &Set{Comments: Comments(yyDollar[2].bytes2), Exprs: yyDollar[3].updateExprs}
		}
	case 21:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:249
		{
			yyVAL.statement = &DDL{Action: CreateStr, NewName: yyDollar[4].tableIdent}
		}
	case 22:
		yyDollar = yyS[yypt-8 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:253
		{
			// Change this to an alter statement
			yyVAL.statement = &DDL{Action: AlterStr, Table: yyDollar[7].tableIdent, NewName: yyDollar[7].tableIdent}
		}
	case 23:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:258
		{
			yyVAL.statement = &DDL{Action: CreateStr, NewName: TableIdent(yyDollar[3].colIdent.Lowered())}
		}
	case 24:
		yyDollar = yyS[yypt-6 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:264
		{
			yyVAL.statement = &DDL{Action: AlterStr, Table: yyDollar[4].tableIdent, NewName: yyDollar[4].tableIdent}
		}
	case 25:
		yyDollar = yyS[yypt-7 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:268
		{
			// Change this to a rename statement
			yyVAL.statement = &DDL{Action: RenameStr, Table: yyDollar[4].tableIdent, NewName: yyDollar[7].tableIdent}
		}
	case 26:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:273
		{
			yyVAL.statement = &DDL{Action: AlterStr, Table: TableIdent(yyDollar[3].colIdent.Lowered()), NewName: TableIdent(yyDollar[3].colIdent.Lowered())}
		}
	case 27:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:279
		{
			yyVAL.statement = &DDL{Action: RenameStr, Table: yyDollar[3].tableIdent, NewName: yyDollar[5].tableIdent}
		}
	case 28:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:285
		{
			var exists bool
			if yyDollar[3].byt != 0 {
				exists = true
			}
			yyVAL.statement = &DDL{Action: DropStr, Table: yyDollar[4].tableIdent, IfExists: exists}
		}
	case 29:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:293
		{
			// Change this to an alter statement
			yyVAL.statement = &DDL{Action: AlterStr, Table: yyDollar[5].tableIdent, NewName: yyDollar[5].tableIdent}
		}
	case 30:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:298
		{
			var exists bool
			if yyDollar[3].byt != 0 {
				exists = true
			}
			yyVAL.statement = &DDL{Action: DropStr, Table: TableIdent(yyDollar[4].colIdent.Lowered()), IfExists: exists}
		}
	case 31:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:308
		{
			yyVAL.statement = &DDL{Action: AlterStr, Table: yyDollar[3].tableIdent, NewName: yyDollar[3].tableIdent}
		}
	case 32:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:314
		{
			yyVAL.statement = &Other{}
		}
	case 33:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:318
		{
			yyVAL.statement = &Other{}
		}
	case 34:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:322
		{
			yyVAL.statement = &Other{}
		}
	case 35:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:327
		{
			setAllowComments(yylex, true)
		}
	case 36:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:331
		{
			yyVAL.bytes2 = yyDollar[2].bytes2
			setAllowComments(yylex, false)
		}
	case 37:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:337
		{
			yyVAL.bytes2 = nil
		}
	case 38:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:341
		{
			yyVAL.bytes2 = append(yyDollar[1].bytes2, yyDollar[2].bytes)
		}
	case 39:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:347
		{
			yyVAL.str = UnionStr
		}
	case 40:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:351
		{
			yyVAL.str = UnionAllStr
		}
	case 41:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:355
		{
			yyVAL.str = UnionDistinctStr
		}
	case 42:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:360
		{
			yyVAL.str = ""
		}
	case 43:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:364
		{
			yyVAL.str = DistinctStr
		}
	case 44:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:369
		{
			yyVAL.str = ""
		}
	case 45:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:373
		{
			yyVAL.str = StraightJoinHint
		}
	case 46:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:379
		{
			yyVAL.selectExprs = SelectExprs{yyDollar[1].selectExpr}
		}
	case 47:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:383
		{
			yyVAL.selectExprs = append(yyVAL.selectExprs, yyDollar[3].selectExpr)
		}
	case 48:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:389
		{
			yyVAL.selectExpr = &StarExpr{}
		}
	case 49:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:393
		{
			yyVAL.selectExpr = &NonStarExpr{Expr: yyDollar[1].expr, As: yyDollar[2].colIdent}
		}
	case 50:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:397
		{
			yyVAL.selectExpr = &StarExpr{TableName: yyDollar[1].tableIdent}
		}
	case 51:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:403
		{
			yyVAL.expr = yyDollar[1].boolExpr
		}
	case 52:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:407
		{
			yyVAL.expr = yyDollar[1].valExpr
		}
	case 53:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:412
		{
			yyVAL.colIdent = ColIdent{}
		}
	case 54:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:416
		{
			yyVAL.colIdent = yyDollar[1].colIdent
		}
	case 55:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:420
		{
			yyVAL.colIdent = yyDollar[2].colIdent
		}
	case 56:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:424
		{
			yyVAL.colIdent = yyDollar[2].colIdent
		}
	case 57:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:429
		{
			yyVAL.tableExprs = TableExprs{&AliasedTableExpr{Expr: &TableName{Name: "dual"}}}
		}
	case 58:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:433
		{
			yyVAL.tableExprs = yyDollar[2].tableExprs
		}
	case 59:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:439
		{
			yyVAL.tableExprs = TableExprs{yyDollar[1].tableExpr}
		}
	case 60:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:443
		{
			yyVAL.tableExprs = append(yyVAL.tableExprs, yyDollar[3].tableExpr)
		}
	case 63:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:453
		{
			yyVAL.tableExpr = &AliasedTableExpr{Expr: yyDollar[1].tableName, As: yyDollar[2].tableIdent, Hints: yyDollar[3].indexHints}
		}
	case 64:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:457
		{
			yyVAL.tableExpr = &AliasedTableExpr{Expr: yyDollar[1].subquery, As: yyDollar[3].tableIdent}
		}
	case 65:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:461
		{
			yyVAL.tableExpr = &ParenTableExpr{Exprs: yyDollar[2].tableExprs}
		}
	case 66:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:474
		{
			yyVAL.tableExpr = &JoinTableExpr{LeftExpr: yyDollar[1].tableExpr, Join: yyDollar[2].str, RightExpr: yyDollar[3].tableExpr}
		}
	case 67:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:478
		{
			yyVAL.tableExpr = &JoinTableExpr{LeftExpr: yyDollar[1].tableExpr, Join: yyDollar[2].str, RightExpr: yyDollar[3].tableExpr, On: yyDollar[5].boolExpr}
		}
	case 68:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:482
		{
			yyVAL.tableExpr = &JoinTableExpr{LeftExpr: yyDollar[1].tableExpr, Join: yyDollar[2].str, RightExpr: yyDollar[3].tableExpr, On: yyDollar[5].boolExpr}
		}
	case 69:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:486
		{
			yyVAL.tableExpr = &JoinTableExpr{LeftExpr: yyDollar[1].tableExpr, Join: yyDollar[2].str, RightExpr: yyDollar[3].tableExpr}
		}
	case 70:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:491
		{
			yyVAL.empty = struct{}{}
		}
	case 71:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:493
		{
			yyVAL.empty = struct{}{}
		}
	case 72:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:496
		{
			yyVAL.tableIdent = ""
		}
	case 73:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:500
		{
			yyVAL.tableIdent = yyDollar[1].tableIdent
		}
	case 74:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:504
		{
			yyVAL.tableIdent = yyDollar[2].tableIdent
		}
	case 75:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:510
		{
			yyVAL.str = JoinStr
		}
	case 76:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:514
		{
			yyVAL.str = JoinStr
		}
	case 77:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:518
		{
			yyVAL.str = JoinStr
		}
	case 78:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:522
		{
			yyVAL.str = StraightJoinStr
		}
	case 79:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:528
		{
			yyVAL.str = LeftJoinStr
		}
	case 80:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:532
		{
			yyVAL.str = LeftJoinStr
		}
	case 81:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:536
		{
			yyVAL.str = RightJoinStr
		}
	case 82:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:540
		{
			yyVAL.str = RightJoinStr
		}
	case 83:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:546
		{
			yyVAL.str = NaturalJoinStr
		}
	case 84:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:550
		{
			if yyDollar[2].str == LeftJoinStr {
				yyVAL.str = NaturalLeftJoinStr
			} else {
				yyVAL.str = NaturalRightJoinStr
			}
		}
	case 85:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:560
		{
			yyVAL.tableName = &TableName{Name: yyDollar[1].tableIdent}
		}
	case 86:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:564
		{
			yyVAL.tableName = &TableName{Qualifier: yyDollar[1].tableIdent, Name: yyDollar[3].tableIdent}
		}
	case 87:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:569
		{
			yyVAL.indexHints = nil
		}
	case 88:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:573
		{
			yyVAL.indexHints = &IndexHints{Type: UseStr, Indexes: yyDollar[4].colIdents}
		}
	case 89:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:577
		{
			yyVAL.indexHints = &IndexHints{Type: IgnoreStr, Indexes: yyDollar[4].colIdents}
		}
	case 90:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:581
		{
			yyVAL.indexHints = &IndexHints{Type: ForceStr, Indexes: yyDollar[4].colIdents}
		}
	case 91:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:587
		{
			yyVAL.colIdents = []ColIdent{yyDollar[1].colIdent}
		}
	case 92:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:591
		{
			yyVAL.colIdents = append(yyDollar[1].colIdents, yyDollar[3].colIdent)
		}
	case 93:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:596
		{
			yyVAL.boolExpr = nil
		}
	case 94:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:600
		{
			yyVAL.boolExpr = yyDollar[2].boolExpr
		}
	case 96:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:607
		{
			yyVAL.boolExpr = &AndExpr{Left: yyDollar[1].boolExpr, Right: yyDollar[3].boolExpr}
		}
	case 97:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:611
		{
			yyVAL.boolExpr = &OrExpr{Left: yyDollar[1].boolExpr, Right: yyDollar[3].boolExpr}
		}
	case 98:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:615
		{
			yyVAL.boolExpr = &NotExpr{Expr: yyDollar[2].boolExpr}
		}
	case 99:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:619
		{
			yyVAL.boolExpr = &ParenBoolExpr{Expr: yyDollar[2].boolExpr}
		}
	case 100:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:623
		{
			yyVAL.boolExpr = &IsExpr{Operator: yyDollar[3].str, Expr: yyDollar[1].boolExpr}
		}
	case 101:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:629
		{
			yyVAL.boolExpr = BoolVal(true)
		}
	case 102:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:633
		{
			yyVAL.boolExpr = BoolVal(false)
		}
	case 103:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:637
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: yyDollar[2].str, Right: yyDollar[3].valExpr}
		}
	case 104:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:641
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: InStr, Right: yyDollar[3].colTuple}
		}
	case 105:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:645
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: NotInStr, Right: yyDollar[4].colTuple}
		}
	case 106:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:649
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: LikeStr, Right: yyDollar[3].valExpr}
		}
	case 107:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:653
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: NotLikeStr, Right: yyDollar[4].valExpr}
		}
	case 108:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:657
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: RegexpStr, Right: yyDollar[3].valExpr}
		}
	case 109:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:661
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: NotRegexpStr, Right: yyDollar[4].valExpr}
		}
	case 110:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:665
		{
			yyVAL.boolExpr = &RangeCond{Left: yyDollar[1].valExpr, Operator: BetweenStr, From: yyDollar[3].valExpr, To: yyDollar[5].valExpr}
		}
	case 111:
		yyDollar = yyS[yypt-6 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:669
		{
			yyVAL.boolExpr = &RangeCond{Left: yyDollar[1].valExpr, Operator: NotBetweenStr, From: yyDollar[4].valExpr, To: yyDollar[6].valExpr}
		}
	case 112:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:673
		{
			yyVAL.boolExpr = &IsExpr{Operator: yyDollar[3].str, Expr: yyDollar[1].valExpr}
		}
	case 113:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:677
		{
			yyVAL.boolExpr = &ExistsExpr{Subquery: yyDollar[2].subquery}
		}
	case 114:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:683
		{
			yyVAL.str = IsNullStr
		}
	case 115:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:687
		{
			yyVAL.str = IsNotNullStr
		}
	case 116:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:691
		{
			yyVAL.str = IsTrueStr
		}
	case 117:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:695
		{
			yyVAL.str = IsNotTrueStr
		}
	case 118:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:699
		{
			yyVAL.str = IsFalseStr
		}
	case 119:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:703
		{
			yyVAL.str = IsNotFalseStr
		}
	case 120:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:709
		{
			yyVAL.str = EqualStr
		}
	case 121:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:713
		{
			yyVAL.str = LessThanStr
		}
	case 122:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:717
		{
			yyVAL.str = GreaterThanStr
		}
	case 123:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:721
		{
			yyVAL.str = LessEqualStr
		}
	case 124:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:725
		{
			yyVAL.str = GreaterEqualStr
		}
	case 125:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:729
		{
			yyVAL.str = NotEqualStr
		}
	case 126:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:733
		{
			yyVAL.str = NullSafeEqualStr
		}
	case 127:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:739
		{
			yyVAL.colTuple = ValTuple(yyDollar[2].valExprs)
		}
	case 128:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:743
		{
			yyVAL.colTuple = yyDollar[1].subquery
		}
	case 129:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:747
		{
			yyVAL.colTuple = ListArg(yyDollar[1].bytes)
		}
	case 130:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:753
		{
			yyVAL.subquery = &Subquery{yyDollar[2].selStmt}
		}
	case 131:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:759
		{
			yyVAL.valExprs = ValExprs{yyDollar[1].valExpr}
		}
	case 132:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:763
		{
			yyVAL.valExprs = append(yyDollar[1].valExprs, yyDollar[3].valExpr)
		}
	case 133:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:769
		{
			yyVAL.valExpr = yyDollar[1].valExpr
		}
	case 134:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:773
		{
			yyVAL.valExpr = yyDollar[1].colName
		}
	case 135:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:777
		{
			yyVAL.valExpr = yyDollar[1].valTuple
		}
	case 136:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:781
		{
			yyVAL.valExpr = yyDollar[1].subquery
		}
	case 137:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:785
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: BitAndStr, Right: yyDollar[3].valExpr}
		}
	case 138:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:789
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: BitOrStr, Right: yyDollar[3].valExpr}
		}
	case 139:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:793
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: BitXorStr, Right: yyDollar[3].valExpr}
		}
	case 140:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:797
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: PlusStr, Right: yyDollar[3].valExpr}
		}
	case 141:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:801
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: MinusStr, Right: yyDollar[3].valExpr}
		}
	case 142:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:805
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: MultStr, Right: yyDollar[3].valExpr}
		}
	case 143:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:809
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: DivStr, Right: yyDollar[3].valExpr}
		}
	case 144:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:813
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: ModStr, Right: yyDollar[3].valExpr}
		}
	case 145:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:817
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: ModStr, Right: yyDollar[3].valExpr}
		}
	case 146:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:821
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: ShiftLeftStr, Right: yyDollar[3].valExpr}
		}
	case 147:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:825
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: ShiftRightStr, Right: yyDollar[3].valExpr}
		}
	case 148:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:829
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].colName, Operator: JSONExtractOp, Right: yyDollar[3].valExpr}
		}
	case 149:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:833
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].colName, Operator: JSONUnquoteExtractOp, Right: yyDollar[3].valExpr}
		}
	case 150:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:837
		{
			if num, ok := yyDollar[2].valExpr.(NumVal); ok {
				yyVAL.valExpr = num
			} else {
				yyVAL.valExpr = &UnaryExpr{Operator: UPlusStr, Expr: yyDollar[2].valExpr}
			}
		}
	case 151:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:845
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
	case 152:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:858
		{
			yyVAL.valExpr = &UnaryExpr{Operator: TildaStr, Expr: yyDollar[2].valExpr}
		}
	case 153:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:862
		{
			// This rule prevents the usage of INTERVAL
			// as a function. If support is needed for that,
			// we'll need to revisit this. The solution
			// will be non-trivial because of grammar conflicts.
			yyVAL.valExpr = &IntervalExpr{Expr: yyDollar[2].valExpr, Unit: yyDollar[3].colIdent}
		}
	case 154:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:870
		{
			yyVAL.valExpr = &FuncExpr{Name: string(yyDollar[1].tableIdent)}
		}
	case 155:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:874
		{
			yyVAL.valExpr = &FuncExpr{Name: string(yyDollar[1].tableIdent), Exprs: yyDollar[3].selectExprs}
		}
	case 156:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:878
		{
			yyVAL.valExpr = &FuncExpr{Name: string(yyDollar[1].tableIdent), Distinct: true, Exprs: yyDollar[4].selectExprs}
		}
	case 157:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:882
		{
			yyVAL.valExpr = &FuncExpr{Name: yyDollar[1].str}
		}
	case 158:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:886
		{
			yyVAL.valExpr = &FuncExpr{Name: yyDollar[1].str, Exprs: yyDollar[3].selectExprs}
		}
	case 159:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:890
		{
			yyVAL.valExpr = yyDollar[1].caseExpr
		}
	case 160:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:896
		{
			yyVAL.str = "if"
		}
	case 161:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:900
		{
			yyVAL.str = "current_timestamp"
		}
	case 162:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:904
		{
			yyVAL.str = "database"
		}
	case 163:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:908
		{
			yyVAL.str = "mod"
		}
	case 164:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:914
		{
			yyVAL.caseExpr = &CaseExpr{Expr: yyDollar[2].valExpr, Whens: yyDollar[3].whens, Else: yyDollar[4].valExpr}
		}
	case 165:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:919
		{
			yyVAL.valExpr = nil
		}
	case 166:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:923
		{
			yyVAL.valExpr = yyDollar[1].valExpr
		}
	case 167:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:929
		{
			yyVAL.whens = []*When{yyDollar[1].when}
		}
	case 168:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:933
		{
			yyVAL.whens = append(yyDollar[1].whens, yyDollar[2].when)
		}
	case 169:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:939
		{
			yyVAL.when = &When{Cond: yyDollar[2].boolExpr, Val: yyDollar[4].valExpr}
		}
	case 170:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:944
		{
			yyVAL.valExpr = nil
		}
	case 171:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:948
		{
			yyVAL.valExpr = yyDollar[2].valExpr
		}
	case 172:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:954
		{
			yyVAL.colName = &ColName{Name: yyDollar[1].colIdent}
		}
	case 173:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:958
		{
			yyVAL.colName = &ColName{Qualifier: &TableName{Name: yyDollar[1].tableIdent}, Name: yyDollar[3].colIdent}
		}
	case 174:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:962
		{
			yyVAL.colName = &ColName{Qualifier: &TableName{Qualifier: yyDollar[1].tableIdent, Name: yyDollar[3].tableIdent}, Name: yyDollar[5].colIdent}
		}
	case 175:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:968
		{
			yyVAL.valExpr = StrVal(yyDollar[1].bytes)
		}
	case 176:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:972
		{
			yyVAL.valExpr = HexVal(yyDollar[1].bytes)
		}
	case 177:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:976
		{
			yyVAL.valExpr = NumVal(yyDollar[1].bytes)
		}
	case 178:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:980
		{
			yyVAL.valExpr = HexNum(yyDollar[1].bytes)
		}
	case 179:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:984
		{
			yyVAL.valExpr = ValArg(yyDollar[1].bytes)
		}
	case 180:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:988
		{
			yyVAL.valExpr = &NullVal{}
		}
	case 181:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:994
		{
			// TODO(sougou): Deprecate this construct.
			if yyDollar[1].colIdent.Lowered() != "value" {
				yylex.Error("expecting value after next")
				return 1
			}
			yyVAL.valExpr = NumVal("1")
		}
	case 182:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1003
		{
			yyVAL.valExpr = NumVal(yyDollar[1].bytes)
		}
	case 183:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1007
		{
			yyVAL.valExpr = ValArg(yyDollar[1].bytes)
		}
	case 184:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1012
		{
			yyVAL.valExprs = nil
		}
	case 185:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1016
		{
			yyVAL.valExprs = yyDollar[3].valExprs
		}
	case 186:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1021
		{
			yyVAL.boolExpr = nil
		}
	case 187:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1025
		{
			yyVAL.boolExpr = yyDollar[2].boolExpr
		}
	case 188:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1030
		{
			yyVAL.orderBy = nil
		}
	case 189:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1034
		{
			yyVAL.orderBy = yyDollar[3].orderBy
		}
	case 190:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1040
		{
			yyVAL.orderBy = OrderBy{yyDollar[1].order}
		}
	case 191:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1044
		{
			yyVAL.orderBy = append(yyDollar[1].orderBy, yyDollar[3].order)
		}
	case 192:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1050
		{
			yyVAL.order = &Order{Expr: yyDollar[1].valExpr, Direction: yyDollar[2].str}
		}
	case 193:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1055
		{
			yyVAL.str = AscScr
		}
	case 194:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1059
		{
			yyVAL.str = AscScr
		}
	case 195:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1063
		{
			yyVAL.str = DescScr
		}
	case 196:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1068
		{
			yyVAL.limit = nil
		}
	case 197:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1072
		{
			yyVAL.limit = &Limit{Rowcount: yyDollar[2].valExpr}
		}
	case 198:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1076
		{
			yyVAL.limit = &Limit{Offset: yyDollar[2].valExpr, Rowcount: yyDollar[4].valExpr}
		}
	case 199:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1081
		{
			yyVAL.str = ""
		}
	case 200:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1085
		{
			yyVAL.str = ForUpdateStr
		}
	case 201:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1089
		{
			if yyDollar[3].colIdent.Lowered() != "share" {
				yylex.Error("expecting share")
				return 1
			}
			if yyDollar[4].colIdent.Lowered() != "mode" {
				yylex.Error("expecting mode")
				return 1
			}
			yyVAL.str = ShareModeStr
		}
	case 202:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1102
		{
			yyVAL.columns = nil
		}
	case 203:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1106
		{
			yyVAL.columns = yyDollar[2].columns
		}
	case 204:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1112
		{
			yyVAL.columns = Columns{yyDollar[1].colIdent}
		}
	case 205:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1116
		{
			yyVAL.columns = append(yyVAL.columns, yyDollar[3].colIdent)
		}
	case 206:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1121
		{
			yyVAL.updateExprs = nil
		}
	case 207:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1125
		{
			yyVAL.updateExprs = yyDollar[5].updateExprs
		}
	case 208:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1131
		{
			yyVAL.insRows = yyDollar[2].values
		}
	case 209:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1135
		{
			yyVAL.insRows = yyDollar[1].selStmt
		}
	case 210:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1141
		{
			yyVAL.values = Values{yyDollar[1].valTuple}
		}
	case 211:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1145
		{
			yyVAL.values = append(yyDollar[1].values, yyDollar[3].valTuple)
		}
	case 212:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1151
		{
			yyVAL.valTuple = ValTuple(yyDollar[2].valExprs)
		}
	case 213:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1157
		{
			yyVAL.updateExprs = UpdateExprs{yyDollar[1].updateExpr}
		}
	case 214:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1161
		{
			yyVAL.updateExprs = append(yyDollar[1].updateExprs, yyDollar[3].updateExpr)
		}
	case 215:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1167
		{
			yyVAL.updateExpr = &UpdateExpr{Name: yyDollar[1].colIdent, Expr: yyDollar[3].valExpr}
		}
	case 218:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1176
		{
			yyVAL.byt = 0
		}
	case 219:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1178
		{
			yyVAL.byt = 1
		}
	case 220:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1181
		{
			yyVAL.empty = struct{}{}
		}
	case 221:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1183
		{
			yyVAL.empty = struct{}{}
		}
	case 222:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1186
		{
			yyVAL.str = ""
		}
	case 223:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1188
		{
			yyVAL.str = IgnoreStr
		}
	case 224:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1192
		{
			yyVAL.empty = struct{}{}
		}
	case 225:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1194
		{
			yyVAL.empty = struct{}{}
		}
	case 226:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1196
		{
			yyVAL.empty = struct{}{}
		}
	case 227:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1198
		{
			yyVAL.empty = struct{}{}
		}
	case 228:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1200
		{
			yyVAL.empty = struct{}{}
		}
	case 229:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1202
		{
			yyVAL.empty = struct{}{}
		}
	case 230:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1205
		{
			yyVAL.empty = struct{}{}
		}
	case 231:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1207
		{
			yyVAL.empty = struct{}{}
		}
	case 232:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1210
		{
			yyVAL.empty = struct{}{}
		}
	case 233:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1212
		{
			yyVAL.empty = struct{}{}
		}
	case 234:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1215
		{
			yyVAL.empty = struct{}{}
		}
	case 235:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1217
		{
			yyVAL.empty = struct{}{}
		}
	case 236:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1221
		{
			yyVAL.colIdent = NewColIdent(string(yyDollar[1].bytes))
		}
	case 237:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1227
		{
			yyVAL.colIdent = NewColIdentMaybeWithSpecialChar(string(yyDollar[1].bytes), true)
		}
	case 238:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1233
		{
			yyVAL.tableIdent = TableIdent(yyDollar[1].bytes)
		}
	case 239:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1239
		{
			if incNesting(yylex) {
				yylex.Error("max nesting level reached")
				return 1
			}
		}
	case 240:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1248
		{
			decNesting(yylex)
		}
	case 241:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1253
		{
			forceEOF(yylex)
		}
	}
	goto yystack /* stack new state and value */
}
