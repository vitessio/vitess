//line sql.y:6
package sqlparser

import __yyfmt__ "fmt"

//line sql.y:6
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

//line sql.y:34
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
const HEX = 57388
const STRING = 57389
const NUMBER = 57390
const VALUE_ARG = 57391
const LIST_ARG = 57392
const COMMENT = 57393
const NULL = 57394
const TRUE = 57395
const FALSE = 57396
const OR = 57397
const AND = 57398
const NOT = 57399
const BETWEEN = 57400
const CASE = 57401
const WHEN = 57402
const THEN = 57403
const ELSE = 57404
const END = 57405
const LE = 57406
const GE = 57407
const NE = 57408
const NULL_SAFE_EQUAL = 57409
const IS = 57410
const LIKE = 57411
const REGEXP = 57412
const IN = 57413
const SHIFT_LEFT = 57414
const SHIFT_RIGHT = 57415
const MOD = 57416
const UNARY = 57417
const INTERVAL = 57418
const JSON_EXTRACT_OP = 57419
const JSON_UNQUOTE_EXTRACT_OP = 57420
const CREATE = 57421
const ALTER = 57422
const DROP = 57423
const RENAME = 57424
const ANALYZE = 57425
const TABLE = 57426
const INDEX = 57427
const VIEW = 57428
const TO = 57429
const IGNORE = 57430
const IF = 57431
const UNIQUE = 57432
const USING = 57433
const SHOW = 57434
const DESCRIBE = 57435
const EXPLAIN = 57436
const CURRENT_TIMESTAMP = 57437
const DATABASE = 57438
const UNUSED = 57439

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
	"HEX",
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
	45, 235,
	92, 235,
	-2, 234,
}

const yyNprod = 239
const yyPrivate = 57344

var yyTokenNames []string
var yyStates []string

const yyLast = 977

var yyAct = [...]int{

	132, 355, 248, 422, 62, 302, 151, 115, 207, 225,
	311, 239, 344, 107, 260, 237, 293, 102, 206, 3,
	113, 254, 238, 146, 103, 155, 241, 74, 35, 67,
	37, 47, 161, 116, 38, 64, 374, 376, 69, 41,
	40, 71, 41, 234, 97, 159, 50, 405, 404, 14,
	15, 16, 17, 403, 68, 81, 70, 48, 49, 58,
	43, 44, 45, 46, 42, 326, 163, 209, 210, 87,
	126, 18, 63, 126, 187, 109, 428, 101, 188, 189,
	190, 191, 192, 193, 187, 112, 177, 64, 88, 175,
	64, 144, 190, 191, 192, 193, 187, 251, 375, 90,
	385, 94, 294, 96, 177, 294, 171, 342, 176, 175,
	84, 245, 143, 174, 158, 160, 157, 228, 112, 112,
	173, 267, 92, 152, 177, 203, 205, 322, 215, 176,
	175, 162, 217, 166, 65, 265, 266, 264, 19, 20,
	22, 21, 23, 430, 251, 177, 329, 330, 331, 60,
	223, 24, 25, 26, 112, 60, 231, 174, 126, 216,
	109, 227, 148, 176, 175, 220, 263, 65, 224, 387,
	82, 83, 14, 244, 246, 112, 243, 126, 93, 177,
	60, 112, 112, 112, 172, 75, 261, 89, 232, 252,
	253, 235, 249, 236, 194, 195, 188, 189, 190, 191,
	192, 193, 187, 242, 285, 251, 271, 28, 283, 284,
	286, 126, 65, 262, 60, 289, 168, 251, 290, 309,
	251, 147, 112, 64, 300, 394, 395, 298, 169, 246,
	287, 288, 285, 291, 301, 392, 255, 257, 258, 126,
	297, 256, 128, 127, 129, 130, 351, 251, 131, 251,
	243, 345, 327, 89, 389, 309, 89, 112, 222, 325,
	73, 399, 79, 204, 168, 345, 230, 100, 328, 369,
	332, 261, 402, 367, 370, 126, 401, 242, 368, 333,
	366, 186, 185, 194, 195, 188, 189, 190, 191, 192,
	193, 187, 365, 371, 339, 317, 318, 55, 262, 142,
	420, 112, 350, 348, 347, 14, 76, 352, 343, 341,
	54, 349, 421, 86, 141, 39, 243, 243, 243, 243,
	408, 388, 362, 391, 364, 85, 153, 380, 379, 296,
	372, 381, 361, 99, 363, 382, 324, 357, 51, 52,
	303, 140, 108, 242, 242, 242, 242, 57, 139, 360,
	304, 226, 390, 359, 149, 186, 185, 194, 195, 188,
	189, 190, 191, 192, 193, 187, 112, 308, 396, 398,
	147, 61, 427, 397, 418, 14, 208, 28, 30, 1,
	323, 211, 212, 213, 214, 313, 316, 317, 318, 314,
	320, 315, 319, 170, 411, 400, 348, 409, 154, 36,
	233, 219, 412, 156, 66, 138, 112, 112, 299, 221,
	415, 416, 417, 413, 414, 229, 423, 423, 423, 64,
	424, 425, 419, 426, 393, 429, 354, 431, 432, 433,
	121, 434, 108, 358, 435, 307, 340, 218, 108, 292,
	122, 114, 259, 346, 80, 268, 269, 270, 383, 272,
	273, 274, 275, 276, 277, 278, 279, 280, 281, 282,
	295, 337, 178, 110, 373, 150, 312, 186, 185, 194,
	195, 188, 189, 190, 191, 192, 193, 187, 310, 108,
	186, 185, 194, 195, 188, 189, 190, 191, 192, 193,
	187, 240, 167, 313, 316, 317, 318, 314, 59, 315,
	319, 105, 78, 53, 27, 56, 13, 12, 72, 11,
	10, 9, 77, 8, 108, 185, 194, 195, 188, 189,
	190, 191, 192, 193, 187, 229, 7, 59, 6, 334,
	335, 336, 91, 5, 4, 2, 95, 0, 29, 98,
	0, 0, 0, 0, 106, 250, 0, 125, 0, 338,
	0, 59, 0, 145, 31, 32, 33, 34, 0, 65,
	0, 0, 0, 164, 0, 0, 165, 353, 356, 0,
	0, 126, 0, 251, 109, 128, 127, 129, 130, 0,
	0, 131, 123, 124, 0, 0, 111, 0, 137, 186,
	185, 194, 195, 188, 189, 190, 191, 192, 193, 187,
	0, 384, 0, 0, 59, 0, 386, 0, 117, 118,
	104, 0, 229, 136, 0, 119, 0, 120, 0, 0,
	0, 0, 0, 0, 229, 0, 0, 0, 0, 0,
	0, 133, 0, 0, 106, 59, 0, 134, 135, 247,
	106, 0, 0, 0, 0, 0, 0, 406, 0, 125,
	0, 407, 0, 0, 0, 410, 356, 186, 185, 194,
	195, 188, 189, 190, 191, 192, 193, 187, 0, 0,
	0, 0, 0, 126, 0, 251, 109, 128, 127, 129,
	130, 106, 0, 131, 123, 124, 0, 0, 111, 0,
	137, 0, 0, 0, 247, 0, 305, 0, 0, 306,
	0, 0, 0, 0, 0, 0, 0, 321, 0, 59,
	117, 118, 104, 0, 0, 136, 106, 119, 0, 120,
	0, 0, 0, 0, 125, 0, 0, 0, 0, 0,
	0, 0, 0, 133, 0, 0, 0, 0, 0, 134,
	135, 0, 0, 0, 0, 0, 0, 0, 126, 0,
	0, 109, 128, 127, 129, 130, 14, 0, 131, 123,
	124, 0, 0, 111, 0, 137, 0, 0, 0, 0,
	0, 125, 0, 0, 0, 59, 59, 59, 59, 0,
	0, 0, 0, 0, 0, 117, 118, 104, 377, 378,
	136, 0, 119, 0, 120, 126, 0, 0, 109, 128,
	127, 129, 130, 0, 0, 131, 123, 124, 133, 0,
	111, 0, 137, 0, 134, 135, 0, 0, 125, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 14,
	0, 0, 117, 118, 0, 0, 0, 136, 0, 119,
	0, 120, 126, 0, 0, 109, 128, 127, 129, 130,
	0, 0, 131, 123, 124, 133, 0, 111, 0, 137,
	0, 134, 135, 0, 0, 0, 0, 0, 126, 0,
	0, 109, 128, 127, 129, 130, 0, 0, 131, 117,
	118, 0, 0, 0, 136, 137, 119, 126, 120, 0,
	109, 128, 127, 129, 130, 0, 0, 131, 0, 0,
	0, 0, 133, 0, 137, 117, 118, 0, 134, 135,
	136, 0, 119, 0, 120, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 117, 118, 0, 0, 133, 136,
	0, 119, 0, 120, 134, 135, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 133, 180, 183,
	0, 0, 0, 134, 135, 196, 197, 198, 199, 200,
	201, 202, 184, 181, 182, 179, 186, 185, 194, 195,
	188, 189, 190, 191, 192, 193, 187,
}
var yyPact = [...]int{

	43, -1000, -1000, 372, -1000, -1000, -1000, -1000, -1000, -1000,
	-1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, -72,
	-62, -36, -40, -37, -1000, -1000, -1000, 369, 320, 278,
	-1000, -65, 101, 361, 86, -76, -47, 86, -1000, -44,
	86, -1000, 101, -78, 137, -78, 101, -1000, -1000, -1000,
	-1000, -1000, -1000, 227, 119, -1000, 56, 301, 285, -23,
	-1000, 101, 141, -1000, 32, -1000, 101, 62, 130, -1000,
	101, -1000, -59, 101, 312, 223, 86, -1000, 703, -1000,
	331, -1000, 284, 269, -1000, 101, 86, 101, 359, 86,
	842, -1000, 305, -82, -1000, 18, -1000, 101, -1000, -1000,
	101, -1000, 218, -1000, -1000, 164, 28, 71, 888, -1000,
	-1000, 797, 750, -1000, -26, -1000, -1000, 842, 842, 842,
	842, 194, -1000, -1000, -1000, 194, -1000, -1000, -1000, -1000,
	-1000, -1000, -1000, -1000, -1000, -1000, -1000, 842, 101, -1000,
	-1000, -1000, -1000, 230, 210, -1000, 337, 797, -1000, 579,
	25, 823, -1000, -1000, 222, 86, -1000, -60, -1000, -1000,
	-1000, -1000, -1000, -1000, -1000, -1000, -1000, 359, 703, 132,
	-1000, -1000, 86, 27, 526, 797, 797, 181, 842, 113,
	60, 842, 842, 842, 181, 842, 842, 842, 842, 842,
	842, 842, 842, 842, 842, 842, -1000, -1000, -1000, -1000,
	-1000, -1000, -1000, 12, 888, 50, 202, 158, 888, 193,
	193, -1000, -1000, -1000, 511, 628, -1000, 369, 39, 579,
	-1000, 299, 86, 86, 337, 324, 335, 71, 112, 579,
	101, -1000, -1000, 101, -1000, 355, -1000, 209, 459, -1000,
	-1000, 107, 316, 166, -1000, -1000, -1000, -27, -1000, 170,
	703, -1000, 12, 30, -1000, -1000, 91, -1000, -1000, 579,
	-1000, 823, -1000, -1000, 113, 842, 842, 842, 579, 579,
	402, -1000, 114, 436, -1000, 8, 8, -14, -14, -14,
	-14, -4, -4, -1000, -1000, 842, -1000, -1000, -1000, -1000,
	-1000, 170, 42, -1000, 797, 221, 194, 372, 207, 200,
	-1000, 324, -1000, 842, 842, -1000, -1000, 340, 334, 132,
	132, 132, 132, -1000, 258, 246, -1000, 239, 235, 259,
	-6, -1000, 101, 101, -1000, 173, 86, -1000, 170, -1000,
	-1000, -1000, 158, -1000, 579, 579, 389, 842, 579, -1000,
	34, -1000, 842, 105, -1000, 296, 208, -1000, 842, -1000,
	-1000, 86, -1000, 277, 189, -1000, 203, -1000, 337, 797,
	842, 459, 217, 351, -1000, -1000, -1000, -1000, 242, -1000,
	238, -1000, -1000, -1000, -48, -53, -54, -1000, -1000, -1000,
	-1000, -1000, -1000, 842, 579, -1000, 579, 842, 294, 194,
	-1000, 842, 842, -1000, -1000, -1000, 324, 71, 186, 797,
	797, -1000, -1000, 194, 194, 194, 579, 579, 366, -1000,
	579, -1000, 283, 71, 71, 86, 86, 86, 86, -1000,
	364, -1, 97, -1000, 97, 97, 141, -1000, 86, -1000,
	86, -1000, -1000, 86, -1000, -1000,
}
var yyPgo = [...]int{

	0, 535, 18, 534, 533, 528, 526, 513, 511, 510,
	509, 507, 506, 538, 505, 504, 503, 502, 17, 24,
	501, 492, 15, 22, 11, 491, 478, 10, 466, 26,
	464, 3, 23, 13, 463, 462, 460, 20, 263, 444,
	21, 14, 8, 443, 7, 33, 441, 440, 439, 16,
	437, 436, 435, 433, 430, 9, 426, 1, 424, 5,
	422, 409, 408, 12, 4, 72, 405, 315, 260, 404,
	403, 400, 399, 398, 0, 393, 465, 390, 380, 31,
	379, 378, 6, 2,
}
var yyR1 = [...]int{

	0, 80, 1, 1, 1, 1, 1, 1, 1, 1,
	1, 1, 1, 2, 2, 2, 3, 3, 4, 5,
	6, 7, 7, 7, 8, 8, 8, 9, 10, 10,
	10, 11, 12, 12, 12, 81, 13, 14, 14, 15,
	15, 15, 16, 16, 17, 17, 18, 18, 19, 19,
	19, 20, 20, 75, 75, 75, 21, 21, 22, 22,
	23, 23, 24, 24, 24, 25, 25, 25, 25, 78,
	78, 77, 77, 77, 26, 26, 26, 26, 27, 27,
	27, 27, 28, 28, 29, 29, 30, 30, 30, 30,
	31, 31, 32, 32, 33, 33, 33, 33, 33, 33,
	34, 34, 34, 34, 34, 34, 34, 34, 34, 34,
	34, 34, 34, 40, 40, 40, 40, 40, 40, 35,
	35, 35, 35, 35, 35, 35, 41, 41, 41, 45,
	42, 42, 38, 38, 38, 38, 38, 38, 38, 38,
	38, 38, 38, 38, 38, 38, 38, 38, 38, 38,
	38, 38, 38, 38, 38, 38, 38, 38, 38, 54,
	54, 54, 54, 47, 50, 50, 48, 48, 49, 51,
	51, 46, 46, 46, 37, 37, 37, 37, 37, 39,
	39, 39, 52, 52, 53, 53, 55, 55, 56, 56,
	57, 58, 58, 58, 59, 59, 59, 60, 60, 60,
	61, 61, 62, 62, 63, 63, 36, 36, 43, 43,
	44, 64, 64, 65, 66, 66, 68, 68, 69, 69,
	67, 67, 70, 70, 70, 70, 70, 70, 71, 71,
	72, 72, 73, 73, 74, 76, 82, 83, 79,
}
var yyR2 = [...]int{

	0, 1, 1, 1, 1, 1, 1, 1, 1, 1,
	1, 1, 1, 12, 6, 3, 8, 8, 8, 7,
	3, 5, 8, 4, 6, 7, 4, 5, 4, 5,
	5, 3, 2, 2, 2, 0, 2, 0, 2, 1,
	2, 2, 0, 1, 0, 1, 1, 3, 1, 2,
	3, 1, 1, 0, 1, 2, 0, 2, 1, 3,
	1, 1, 3, 3, 3, 3, 5, 5, 3, 0,
	1, 0, 1, 2, 1, 2, 2, 1, 2, 3,
	2, 3, 2, 2, 1, 3, 0, 5, 5, 5,
	1, 3, 0, 2, 1, 3, 3, 2, 3, 3,
	1, 1, 3, 3, 4, 3, 4, 3, 4, 5,
	6, 3, 2, 1, 2, 1, 2, 1, 2, 1,
	1, 1, 1, 1, 1, 1, 3, 1, 1, 3,
	1, 3, 1, 1, 1, 1, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 2,
	2, 2, 3, 3, 4, 5, 3, 4, 1, 1,
	1, 1, 1, 5, 0, 1, 1, 2, 4, 0,
	2, 1, 3, 5, 1, 1, 1, 1, 1, 1,
	2, 2, 0, 3, 0, 2, 0, 3, 1, 3,
	2, 0, 1, 1, 0, 2, 4, 0, 2, 4,
	0, 3, 1, 3, 0, 5, 2, 1, 1, 3,
	3, 1, 3, 3, 1, 1, 0, 2, 0, 3,
	0, 1, 1, 1, 1, 1, 1, 1, 0, 1,
	0, 1, 0, 2, 1, 1, 1, 1, 0,
}
var yyChk = [...]int{

	-1000, -80, -1, -2, -3, -4, -5, -6, -7, -8,
	-9, -10, -11, -12, 6, 7, 8, 9, 28, 95,
	96, 98, 97, 99, 108, 109, 110, -15, 5, -13,
	-81, -13, -13, -13, -13, 100, -72, 102, 106, -67,
	102, 104, 100, 100, 101, 102, 100, -79, -79, -79,
	-2, 18, 19, -16, 32, 19, -14, -67, -29, -76,
	48, 10, -64, -65, -74, 48, -69, 105, 101, -74,
	100, -74, -76, -68, 105, 48, -68, -76, -17, 35,
	-39, -74, 51, 52, 54, 24, 28, 92, -29, 46,
	67, -76, 60, 48, -79, -76, -79, 103, -76, 21,
	44, -74, -18, -19, 84, -20, -76, -33, -38, 48,
	-34, 60, -82, -37, -46, -44, -45, 82, 83, 89,
	91, -54, -47, 56, 57, 21, 45, 50, 49, 51,
	52, 55, -74, 105, 111, 112, 87, 62, -66, 17,
	10, 30, 30, -29, -64, -76, -32, 11, -65, -38,
	-76, -82, -79, 21, -73, 107, -70, 98, 96, 27,
	97, 14, 113, 48, -76, -76, -79, -21, 46, 10,
	-75, -74, 20, 92, -82, 59, 58, 74, -35, 77,
	60, 75, 76, 61, 74, 79, 78, 88, 82, 83,
	84, 85, 86, 87, 80, 81, 67, 68, 69, 70,
	71, 72, 73, -33, -38, -33, -2, -42, -38, 93,
	94, -38, -38, -38, -38, -82, -45, -82, -50, -38,
	-29, -61, 28, -82, -32, -55, 14, -33, 92, -38,
	44, -74, -79, -71, 103, -32, -19, -22, -23, -24,
	-25, -29, -45, -82, -74, 84, -74, -76, -83, -18,
	19, 47, -33, -33, -40, 55, 60, 56, 57, -38,
	-41, -82, -45, 53, 77, 75, 76, 61, -38, -38,
	-38, -40, -38, -38, -38, -38, -38, -38, -38, -38,
	-38, -38, -38, -83, -83, 46, -83, -37, -37, -74,
	-83, -18, -48, -49, 63, -36, 30, -2, -64, -62,
	-74, -55, -59, 16, 15, -76, -76, -52, 12, 46,
	-26, -27, -28, 34, 38, 40, 35, 36, 37, 41,
	-77, -76, 20, -78, 20, -22, 92, -83, -18, 55,
	56, 57, -42, -41, -38, -38, -38, 59, -38, -83,
	-51, -49, 65, -33, -63, 44, -43, -44, -82, -63,
	-83, 46, -59, -38, -56, -57, -38, -79, -53, 13,
	15, -23, -24, -23, -24, 34, 34, 34, 39, 34,
	39, 34, -27, -30, 42, 104, 43, -76, -76, -83,
	-74, -83, -83, 59, -38, 66, -38, 64, 25, 46,
	-74, 46, 46, -58, 22, 23, -55, -33, -42, 44,
	44, 34, 34, 101, 101, 101, -38, -38, 26, -44,
	-38, -57, -59, -33, -33, -82, -82, -82, 8, -60,
	17, 29, -31, -74, -31, -31, -64, 8, 77, -83,
	46, -83, -83, -74, -74, -74,
}
var yyDef = [...]int{

	0, -2, 1, 2, 3, 4, 5, 6, 7, 8,
	9, 10, 11, 12, 35, 35, 35, 35, 35, 230,
	220, 0, 0, 0, 238, 238, 238, 0, 39, 42,
	37, 220, 0, 0, 0, 218, 0, 0, 231, 0,
	0, 221, 0, 216, 0, 216, 0, 32, 33, 34,
	15, 40, 41, 44, 0, 43, 36, 0, 0, 84,
	235, 0, 20, 211, 0, 234, 0, 0, 0, 238,
	0, 238, 0, 0, 0, 0, 0, 31, 0, 45,
	0, 179, 0, 0, 38, 0, 0, 0, 92, 0,
	0, 238, 0, 232, 23, 0, 26, 0, 28, 217,
	0, 238, 56, 46, 48, 53, 0, 51, 52, -2,
	94, 0, 0, 132, 133, 134, 135, 0, 0, 0,
	0, 0, 158, 100, 101, 0, 236, 174, 175, 176,
	177, 178, 171, 159, 160, 161, 162, 164, 0, 214,
	215, 180, 181, 200, 92, 85, 186, 0, 212, 213,
	0, 0, 21, 219, 0, 0, 238, 228, 222, 223,
	224, 225, 226, 227, 27, 29, 30, 92, 0, 0,
	49, 54, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 119, 120, 121, 122,
	123, 124, 125, 97, 0, 0, 0, 0, 130, 0,
	0, 149, 150, 151, 0, 0, 112, 0, 0, 165,
	14, 0, 0, 0, 186, 194, 0, 93, 0, 130,
	0, 233, 24, 0, 229, 182, 47, 57, 58, 60,
	61, 71, 69, 0, 55, 50, 172, 0, 153, 0,
	0, 237, 95, 96, 99, 113, 0, 115, 117, 102,
	103, 0, 127, 128, 0, 0, 0, 0, 105, 107,
	0, 111, 136, 137, 138, 139, 140, 141, 142, 143,
	144, 145, 146, 98, 129, 0, 210, 147, 148, 152,
	156, 0, 169, 166, 0, 204, 0, 207, 204, 0,
	202, 194, 19, 0, 0, 238, 25, 184, 0, 0,
	0, 0, 0, 74, 0, 0, 77, 0, 0, 0,
	86, 72, 0, 0, 70, 0, 0, 154, 0, 114,
	116, 118, 0, 104, 106, 108, 0, 0, 131, 157,
	0, 167, 0, 0, 16, 0, 206, 208, 0, 17,
	201, 0, 18, 195, 187, 188, 191, 22, 186, 0,
	0, 59, 65, 0, 68, 75, 76, 78, 0, 80,
	0, 82, 83, 62, 0, 0, 0, 73, 63, 64,
	173, 155, 126, 0, 109, 163, 170, 0, 0, 0,
	203, 0, 0, 190, 192, 193, 194, 185, 183, 0,
	0, 79, 81, 0, 0, 0, 110, 168, 0, 209,
	196, 189, 197, 66, 67, 0, 0, 0, 0, 13,
	0, 0, 0, 90, 0, 0, 205, 198, 0, 87,
	0, 88, 89, 0, 91, 199,
}
var yyTok1 = [...]int{

	1, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 86, 79, 3,
	45, 47, 84, 82, 46, 83, 92, 85, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	68, 67, 69, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 88, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 78, 3, 89,
}
var yyTok2 = [...]int{

	2, 3, 4, 5, 6, 7, 8, 9, 10, 11,
	12, 13, 14, 15, 16, 17, 18, 19, 20, 21,
	22, 23, 24, 25, 26, 27, 28, 29, 30, 31,
	32, 33, 34, 35, 36, 37, 38, 39, 40, 41,
	42, 43, 44, 48, 49, 50, 51, 52, 53, 54,
	55, 56, 57, 58, 59, 60, 61, 62, 63, 64,
	65, 66, 70, 71, 72, 73, 74, 75, 76, 77,
	80, 81, 87, 90, 91, 93, 94, 95, 96, 97,
	98, 99, 100, 101, 102, 103, 104, 105, 106, 107,
	108, 109, 110, 111, 112, 113,
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
		//line sql.y:179
		{
			setParseTree(yylex, yyDollar[1].statement)
		}
	case 2:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:185
		{
			yyVAL.statement = yyDollar[1].selStmt
		}
	case 13:
		yyDollar = yyS[yypt-12 : yypt+1]
		//line sql.y:201
		{
			yyVAL.selStmt = &Select{Comments: Comments(yyDollar[2].bytes2), Distinct: yyDollar[3].str, Hints: yyDollar[4].str, SelectExprs: yyDollar[5].selectExprs, From: yyDollar[6].tableExprs, Where: NewWhere(WhereStr, yyDollar[7].boolExpr), GroupBy: GroupBy(yyDollar[8].valExprs), Having: NewWhere(HavingStr, yyDollar[9].boolExpr), OrderBy: yyDollar[10].orderBy, Limit: yyDollar[11].limit, Lock: yyDollar[12].str}
		}
	case 14:
		yyDollar = yyS[yypt-6 : yypt+1]
		//line sql.y:205
		{
			yyVAL.selStmt = &Select{Comments: Comments(yyDollar[2].bytes2), SelectExprs: SelectExprs{Nextval{Expr: yyDollar[4].valExpr}}, From: TableExprs{&AliasedTableExpr{Expr: yyDollar[6].tableName}}}
		}
	case 15:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:209
		{
			yyVAL.selStmt = &Union{Type: yyDollar[2].str, Left: yyDollar[1].selStmt, Right: yyDollar[3].selStmt}
		}
	case 16:
		yyDollar = yyS[yypt-8 : yypt+1]
		//line sql.y:215
		{
			yyVAL.statement = &Insert{Comments: Comments(yyDollar[2].bytes2), Ignore: yyDollar[3].str, Table: yyDollar[5].tableName, Columns: yyDollar[6].columns, Rows: yyDollar[7].insRows, OnDup: OnDup(yyDollar[8].updateExprs)}
		}
	case 17:
		yyDollar = yyS[yypt-8 : yypt+1]
		//line sql.y:219
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
		//line sql.y:231
		{
			yyVAL.statement = &Update{Comments: Comments(yyDollar[2].bytes2), Table: yyDollar[3].tableName, Exprs: yyDollar[5].updateExprs, Where: NewWhere(WhereStr, yyDollar[6].boolExpr), OrderBy: yyDollar[7].orderBy, Limit: yyDollar[8].limit}
		}
	case 19:
		yyDollar = yyS[yypt-7 : yypt+1]
		//line sql.y:237
		{
			yyVAL.statement = &Delete{Comments: Comments(yyDollar[2].bytes2), Table: yyDollar[4].tableName, Where: NewWhere(WhereStr, yyDollar[5].boolExpr), OrderBy: yyDollar[6].orderBy, Limit: yyDollar[7].limit}
		}
	case 20:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:243
		{
			yyVAL.statement = &Set{Comments: Comments(yyDollar[2].bytes2), Exprs: yyDollar[3].updateExprs}
		}
	case 21:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:249
		{
			yyVAL.statement = &DDL{Action: CreateStr, NewName: yyDollar[4].tableIdent}
		}
	case 22:
		yyDollar = yyS[yypt-8 : yypt+1]
		//line sql.y:253
		{
			// Change this to an alter statement
			yyVAL.statement = &DDL{Action: AlterStr, Table: yyDollar[7].tableIdent, NewName: yyDollar[7].tableIdent}
		}
	case 23:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:258
		{
			yyVAL.statement = &DDL{Action: CreateStr, NewName: TableIdent(yyDollar[3].colIdent.Lowered())}
		}
	case 24:
		yyDollar = yyS[yypt-6 : yypt+1]
		//line sql.y:264
		{
			yyVAL.statement = &DDL{Action: AlterStr, Table: yyDollar[4].tableIdent, NewName: yyDollar[4].tableIdent}
		}
	case 25:
		yyDollar = yyS[yypt-7 : yypt+1]
		//line sql.y:268
		{
			// Change this to a rename statement
			yyVAL.statement = &DDL{Action: RenameStr, Table: yyDollar[4].tableIdent, NewName: yyDollar[7].tableIdent}
		}
	case 26:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:273
		{
			yyVAL.statement = &DDL{Action: AlterStr, Table: TableIdent(yyDollar[3].colIdent.Lowered()), NewName: TableIdent(yyDollar[3].colIdent.Lowered())}
		}
	case 27:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:279
		{
			yyVAL.statement = &DDL{Action: RenameStr, Table: yyDollar[3].tableIdent, NewName: yyDollar[5].tableIdent}
		}
	case 28:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:285
		{
			var exists bool
			if yyDollar[3].byt != 0 {
				exists = true
			}
			yyVAL.statement = &DDL{Action: DropStr, Table: yyDollar[4].tableIdent, IfExists: exists}
		}
	case 29:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:293
		{
			// Change this to an alter statement
			yyVAL.statement = &DDL{Action: AlterStr, Table: yyDollar[5].tableIdent, NewName: yyDollar[5].tableIdent}
		}
	case 30:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:298
		{
			var exists bool
			if yyDollar[3].byt != 0 {
				exists = true
			}
			yyVAL.statement = &DDL{Action: DropStr, Table: TableIdent(yyDollar[4].colIdent.Lowered()), IfExists: exists}
		}
	case 31:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:308
		{
			yyVAL.statement = &DDL{Action: AlterStr, Table: yyDollar[3].tableIdent, NewName: yyDollar[3].tableIdent}
		}
	case 32:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:314
		{
			yyVAL.statement = &Other{}
		}
	case 33:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:318
		{
			yyVAL.statement = &Other{}
		}
	case 34:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:322
		{
			yyVAL.statement = &Other{}
		}
	case 35:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:327
		{
			setAllowComments(yylex, true)
		}
	case 36:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:331
		{
			yyVAL.bytes2 = yyDollar[2].bytes2
			setAllowComments(yylex, false)
		}
	case 37:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:337
		{
			yyVAL.bytes2 = nil
		}
	case 38:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:341
		{
			yyVAL.bytes2 = append(yyDollar[1].bytes2, yyDollar[2].bytes)
		}
	case 39:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:347
		{
			yyVAL.str = UnionStr
		}
	case 40:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:351
		{
			yyVAL.str = UnionAllStr
		}
	case 41:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:355
		{
			yyVAL.str = UnionDistinctStr
		}
	case 42:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:360
		{
			yyVAL.str = ""
		}
	case 43:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:364
		{
			yyVAL.str = DistinctStr
		}
	case 44:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:369
		{
			yyVAL.str = ""
		}
	case 45:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:373
		{
			yyVAL.str = StraightJoinHint
		}
	case 46:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:379
		{
			yyVAL.selectExprs = SelectExprs{yyDollar[1].selectExpr}
		}
	case 47:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:383
		{
			yyVAL.selectExprs = append(yyVAL.selectExprs, yyDollar[3].selectExpr)
		}
	case 48:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:389
		{
			yyVAL.selectExpr = &StarExpr{}
		}
	case 49:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:393
		{
			yyVAL.selectExpr = &NonStarExpr{Expr: yyDollar[1].expr, As: yyDollar[2].colIdent}
		}
	case 50:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:397
		{
			yyVAL.selectExpr = &StarExpr{TableName: yyDollar[1].tableIdent}
		}
	case 51:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:403
		{
			yyVAL.expr = yyDollar[1].boolExpr
		}
	case 52:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:407
		{
			yyVAL.expr = yyDollar[1].valExpr
		}
	case 53:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:412
		{
			yyVAL.colIdent = ColIdent{}
		}
	case 54:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:416
		{
			yyVAL.colIdent = yyDollar[1].colIdent
		}
	case 55:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:420
		{
			yyVAL.colIdent = yyDollar[2].colIdent
		}
	case 56:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:425
		{
			yyVAL.tableExprs = TableExprs{&AliasedTableExpr{Expr: &TableName{Name: "dual"}}}
		}
	case 57:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:429
		{
			yyVAL.tableExprs = yyDollar[2].tableExprs
		}
	case 58:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:435
		{
			yyVAL.tableExprs = TableExprs{yyDollar[1].tableExpr}
		}
	case 59:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:439
		{
			yyVAL.tableExprs = append(yyVAL.tableExprs, yyDollar[3].tableExpr)
		}
	case 62:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:449
		{
			yyVAL.tableExpr = &AliasedTableExpr{Expr: yyDollar[1].tableName, As: yyDollar[2].tableIdent, Hints: yyDollar[3].indexHints}
		}
	case 63:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:453
		{
			yyVAL.tableExpr = &AliasedTableExpr{Expr: yyDollar[1].subquery, As: yyDollar[3].tableIdent}
		}
	case 64:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:457
		{
			yyVAL.tableExpr = &ParenTableExpr{Exprs: yyDollar[2].tableExprs}
		}
	case 65:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:470
		{
			yyVAL.tableExpr = &JoinTableExpr{LeftExpr: yyDollar[1].tableExpr, Join: yyDollar[2].str, RightExpr: yyDollar[3].tableExpr}
		}
	case 66:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:474
		{
			yyVAL.tableExpr = &JoinTableExpr{LeftExpr: yyDollar[1].tableExpr, Join: yyDollar[2].str, RightExpr: yyDollar[3].tableExpr, On: yyDollar[5].boolExpr}
		}
	case 67:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:478
		{
			yyVAL.tableExpr = &JoinTableExpr{LeftExpr: yyDollar[1].tableExpr, Join: yyDollar[2].str, RightExpr: yyDollar[3].tableExpr, On: yyDollar[5].boolExpr}
		}
	case 68:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:482
		{
			yyVAL.tableExpr = &JoinTableExpr{LeftExpr: yyDollar[1].tableExpr, Join: yyDollar[2].str, RightExpr: yyDollar[3].tableExpr}
		}
	case 69:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:487
		{
			yyVAL.empty = struct{}{}
		}
	case 70:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:489
		{
			yyVAL.empty = struct{}{}
		}
	case 71:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:492
		{
			yyVAL.tableIdent = ""
		}
	case 72:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:496
		{
			yyVAL.tableIdent = yyDollar[1].tableIdent
		}
	case 73:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:500
		{
			yyVAL.tableIdent = yyDollar[2].tableIdent
		}
	case 74:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:506
		{
			yyVAL.str = JoinStr
		}
	case 75:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:510
		{
			yyVAL.str = JoinStr
		}
	case 76:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:514
		{
			yyVAL.str = JoinStr
		}
	case 77:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:518
		{
			yyVAL.str = StraightJoinStr
		}
	case 78:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:524
		{
			yyVAL.str = LeftJoinStr
		}
	case 79:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:528
		{
			yyVAL.str = LeftJoinStr
		}
	case 80:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:532
		{
			yyVAL.str = RightJoinStr
		}
	case 81:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:536
		{
			yyVAL.str = RightJoinStr
		}
	case 82:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:542
		{
			yyVAL.str = NaturalJoinStr
		}
	case 83:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:546
		{
			if yyDollar[2].str == LeftJoinStr {
				yyVAL.str = NaturalLeftJoinStr
			} else {
				yyVAL.str = NaturalRightJoinStr
			}
		}
	case 84:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:556
		{
			yyVAL.tableName = &TableName{Name: yyDollar[1].tableIdent}
		}
	case 85:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:560
		{
			yyVAL.tableName = &TableName{Qualifier: yyDollar[1].tableIdent, Name: yyDollar[3].tableIdent}
		}
	case 86:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:565
		{
			yyVAL.indexHints = nil
		}
	case 87:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:569
		{
			yyVAL.indexHints = &IndexHints{Type: UseStr, Indexes: yyDollar[4].colIdents}
		}
	case 88:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:573
		{
			yyVAL.indexHints = &IndexHints{Type: IgnoreStr, Indexes: yyDollar[4].colIdents}
		}
	case 89:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:577
		{
			yyVAL.indexHints = &IndexHints{Type: ForceStr, Indexes: yyDollar[4].colIdents}
		}
	case 90:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:583
		{
			yyVAL.colIdents = []ColIdent{yyDollar[1].colIdent}
		}
	case 91:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:587
		{
			yyVAL.colIdents = append(yyDollar[1].colIdents, yyDollar[3].colIdent)
		}
	case 92:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:592
		{
			yyVAL.boolExpr = nil
		}
	case 93:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:596
		{
			yyVAL.boolExpr = yyDollar[2].boolExpr
		}
	case 95:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:603
		{
			yyVAL.boolExpr = &AndExpr{Left: yyDollar[1].boolExpr, Right: yyDollar[3].boolExpr}
		}
	case 96:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:607
		{
			yyVAL.boolExpr = &OrExpr{Left: yyDollar[1].boolExpr, Right: yyDollar[3].boolExpr}
		}
	case 97:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:611
		{
			yyVAL.boolExpr = &NotExpr{Expr: yyDollar[2].boolExpr}
		}
	case 98:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:615
		{
			yyVAL.boolExpr = &ParenBoolExpr{Expr: yyDollar[2].boolExpr}
		}
	case 99:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:619
		{
			yyVAL.boolExpr = &IsExpr{Operator: yyDollar[3].str, Expr: yyDollar[1].boolExpr}
		}
	case 100:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:625
		{
			yyVAL.boolExpr = BoolVal(true)
		}
	case 101:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:629
		{
			yyVAL.boolExpr = BoolVal(false)
		}
	case 102:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:633
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: yyDollar[2].str, Right: yyDollar[3].valExpr}
		}
	case 103:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:637
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: InStr, Right: yyDollar[3].colTuple}
		}
	case 104:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:641
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: NotInStr, Right: yyDollar[4].colTuple}
		}
	case 105:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:645
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: LikeStr, Right: yyDollar[3].valExpr}
		}
	case 106:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:649
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: NotLikeStr, Right: yyDollar[4].valExpr}
		}
	case 107:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:653
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: RegexpStr, Right: yyDollar[3].valExpr}
		}
	case 108:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:657
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: NotRegexpStr, Right: yyDollar[4].valExpr}
		}
	case 109:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:661
		{
			yyVAL.boolExpr = &RangeCond{Left: yyDollar[1].valExpr, Operator: BetweenStr, From: yyDollar[3].valExpr, To: yyDollar[5].valExpr}
		}
	case 110:
		yyDollar = yyS[yypt-6 : yypt+1]
		//line sql.y:665
		{
			yyVAL.boolExpr = &RangeCond{Left: yyDollar[1].valExpr, Operator: NotBetweenStr, From: yyDollar[4].valExpr, To: yyDollar[6].valExpr}
		}
	case 111:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:669
		{
			yyVAL.boolExpr = &IsExpr{Operator: yyDollar[3].str, Expr: yyDollar[1].valExpr}
		}
	case 112:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:673
		{
			yyVAL.boolExpr = &ExistsExpr{Subquery: yyDollar[2].subquery}
		}
	case 113:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:679
		{
			yyVAL.str = IsNullStr
		}
	case 114:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:683
		{
			yyVAL.str = IsNotNullStr
		}
	case 115:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:687
		{
			yyVAL.str = IsTrueStr
		}
	case 116:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:691
		{
			yyVAL.str = IsNotTrueStr
		}
	case 117:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:695
		{
			yyVAL.str = IsFalseStr
		}
	case 118:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:699
		{
			yyVAL.str = IsNotFalseStr
		}
	case 119:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:705
		{
			yyVAL.str = EqualStr
		}
	case 120:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:709
		{
			yyVAL.str = LessThanStr
		}
	case 121:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:713
		{
			yyVAL.str = GreaterThanStr
		}
	case 122:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:717
		{
			yyVAL.str = LessEqualStr
		}
	case 123:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:721
		{
			yyVAL.str = GreaterEqualStr
		}
	case 124:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:725
		{
			yyVAL.str = NotEqualStr
		}
	case 125:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:729
		{
			yyVAL.str = NullSafeEqualStr
		}
	case 126:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:735
		{
			yyVAL.colTuple = ValTuple(yyDollar[2].valExprs)
		}
	case 127:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:739
		{
			yyVAL.colTuple = yyDollar[1].subquery
		}
	case 128:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:743
		{
			yyVAL.colTuple = ListArg(yyDollar[1].bytes)
		}
	case 129:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:749
		{
			yyVAL.subquery = &Subquery{yyDollar[2].selStmt}
		}
	case 130:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:755
		{
			yyVAL.valExprs = ValExprs{yyDollar[1].valExpr}
		}
	case 131:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:759
		{
			yyVAL.valExprs = append(yyDollar[1].valExprs, yyDollar[3].valExpr)
		}
	case 132:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:765
		{
			yyVAL.valExpr = yyDollar[1].valExpr
		}
	case 133:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:769
		{
			yyVAL.valExpr = yyDollar[1].colName
		}
	case 134:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:773
		{
			yyVAL.valExpr = yyDollar[1].valTuple
		}
	case 135:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:777
		{
			yyVAL.valExpr = yyDollar[1].subquery
		}
	case 136:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:781
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: BitAndStr, Right: yyDollar[3].valExpr}
		}
	case 137:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:785
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: BitOrStr, Right: yyDollar[3].valExpr}
		}
	case 138:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:789
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: BitXorStr, Right: yyDollar[3].valExpr}
		}
	case 139:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:793
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: PlusStr, Right: yyDollar[3].valExpr}
		}
	case 140:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:797
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: MinusStr, Right: yyDollar[3].valExpr}
		}
	case 141:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:801
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: MultStr, Right: yyDollar[3].valExpr}
		}
	case 142:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:805
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: DivStr, Right: yyDollar[3].valExpr}
		}
	case 143:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:809
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: ModStr, Right: yyDollar[3].valExpr}
		}
	case 144:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:813
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: ModStr, Right: yyDollar[3].valExpr}
		}
	case 145:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:817
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: ShiftLeftStr, Right: yyDollar[3].valExpr}
		}
	case 146:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:821
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: ShiftRightStr, Right: yyDollar[3].valExpr}
		}
	case 147:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:825
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].colName, Operator: JSONExtractOp, Right: yyDollar[3].valExpr}
		}
	case 148:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:829
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].colName, Operator: JSONUnquoteExtractOp, Right: yyDollar[3].valExpr}
		}
	case 149:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:833
		{
			if num, ok := yyDollar[2].valExpr.(NumVal); ok {
				yyVAL.valExpr = num
			} else {
				yyVAL.valExpr = &UnaryExpr{Operator: UPlusStr, Expr: yyDollar[2].valExpr}
			}
		}
	case 150:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:841
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
	case 151:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:854
		{
			yyVAL.valExpr = &UnaryExpr{Operator: TildaStr, Expr: yyDollar[2].valExpr}
		}
	case 152:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:858
		{
			// This rule prevents the usage of INTERVAL
			// as a function. If support is needed for that,
			// we'll need to revisit this. The solution
			// will be non-trivial because of grammar conflicts.
			yyVAL.valExpr = &IntervalExpr{Expr: yyDollar[2].valExpr, Unit: yyDollar[3].colIdent}
		}
	case 153:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:866
		{
			yyVAL.valExpr = &FuncExpr{Name: string(yyDollar[1].tableIdent)}
		}
	case 154:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:870
		{
			yyVAL.valExpr = &FuncExpr{Name: string(yyDollar[1].tableIdent), Exprs: yyDollar[3].selectExprs}
		}
	case 155:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:874
		{
			yyVAL.valExpr = &FuncExpr{Name: string(yyDollar[1].tableIdent), Distinct: true, Exprs: yyDollar[4].selectExprs}
		}
	case 156:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:878
		{
			yyVAL.valExpr = &FuncExpr{Name: yyDollar[1].str}
		}
	case 157:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:882
		{
			yyVAL.valExpr = &FuncExpr{Name: yyDollar[1].str, Exprs: yyDollar[3].selectExprs}
		}
	case 158:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:886
		{
			yyVAL.valExpr = yyDollar[1].caseExpr
		}
	case 159:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:892
		{
			yyVAL.str = "if"
		}
	case 160:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:896
		{
			yyVAL.str = "current_timestamp"
		}
	case 161:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:900
		{
			yyVAL.str = "database"
		}
	case 162:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:904
		{
			yyVAL.str = "mod"
		}
	case 163:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:910
		{
			yyVAL.caseExpr = &CaseExpr{Expr: yyDollar[2].valExpr, Whens: yyDollar[3].whens, Else: yyDollar[4].valExpr}
		}
	case 164:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:915
		{
			yyVAL.valExpr = nil
		}
	case 165:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:919
		{
			yyVAL.valExpr = yyDollar[1].valExpr
		}
	case 166:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:925
		{
			yyVAL.whens = []*When{yyDollar[1].when}
		}
	case 167:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:929
		{
			yyVAL.whens = append(yyDollar[1].whens, yyDollar[2].when)
		}
	case 168:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:935
		{
			yyVAL.when = &When{Cond: yyDollar[2].boolExpr, Val: yyDollar[4].valExpr}
		}
	case 169:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:940
		{
			yyVAL.valExpr = nil
		}
	case 170:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:944
		{
			yyVAL.valExpr = yyDollar[2].valExpr
		}
	case 171:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:950
		{
			yyVAL.colName = &ColName{Name: yyDollar[1].colIdent}
		}
	case 172:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:954
		{
			yyVAL.colName = &ColName{Qualifier: &TableName{Name: yyDollar[1].tableIdent}, Name: yyDollar[3].colIdent}
		}
	case 173:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:958
		{
			yyVAL.colName = &ColName{Qualifier: &TableName{Qualifier: yyDollar[1].tableIdent, Name: yyDollar[3].tableIdent}, Name: yyDollar[5].colIdent}
		}
	case 174:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:964
		{
			yyVAL.valExpr = StrVal(yyDollar[1].bytes)
		}
	case 175:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:968
		{
			yyVAL.valExpr = HexVal(yyDollar[1].bytes)
		}
	case 176:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:972
		{
			yyVAL.valExpr = NumVal(yyDollar[1].bytes)
		}
	case 177:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:976
		{
			yyVAL.valExpr = ValArg(yyDollar[1].bytes)
		}
	case 178:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:980
		{
			yyVAL.valExpr = &NullVal{}
		}
	case 179:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:986
		{
			// TODO(sougou): Deprecate this construct.
			if yyDollar[1].colIdent.Lowered() != "value" {
				yylex.Error("expecting value after next")
				return 1
			}
			yyVAL.valExpr = NumVal("1")
		}
	case 180:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:995
		{
			yyVAL.valExpr = NumVal(yyDollar[1].bytes)
		}
	case 181:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:999
		{
			yyVAL.valExpr = ValArg(yyDollar[1].bytes)
		}
	case 182:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1004
		{
			yyVAL.valExprs = nil
		}
	case 183:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1008
		{
			yyVAL.valExprs = yyDollar[3].valExprs
		}
	case 184:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1013
		{
			yyVAL.boolExpr = nil
		}
	case 185:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:1017
		{
			yyVAL.boolExpr = yyDollar[2].boolExpr
		}
	case 186:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1022
		{
			yyVAL.orderBy = nil
		}
	case 187:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1026
		{
			yyVAL.orderBy = yyDollar[3].orderBy
		}
	case 188:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1032
		{
			yyVAL.orderBy = OrderBy{yyDollar[1].order}
		}
	case 189:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1036
		{
			yyVAL.orderBy = append(yyDollar[1].orderBy, yyDollar[3].order)
		}
	case 190:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:1042
		{
			yyVAL.order = &Order{Expr: yyDollar[1].valExpr, Direction: yyDollar[2].str}
		}
	case 191:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1047
		{
			yyVAL.str = AscScr
		}
	case 192:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1051
		{
			yyVAL.str = AscScr
		}
	case 193:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1055
		{
			yyVAL.str = DescScr
		}
	case 194:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1060
		{
			yyVAL.limit = nil
		}
	case 195:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:1064
		{
			yyVAL.limit = &Limit{Rowcount: yyDollar[2].valExpr}
		}
	case 196:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:1068
		{
			yyVAL.limit = &Limit{Offset: yyDollar[2].valExpr, Rowcount: yyDollar[4].valExpr}
		}
	case 197:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1073
		{
			yyVAL.str = ""
		}
	case 198:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:1077
		{
			yyVAL.str = ForUpdateStr
		}
	case 199:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:1081
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
	case 200:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1094
		{
			yyVAL.columns = nil
		}
	case 201:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1098
		{
			yyVAL.columns = yyDollar[2].columns
		}
	case 202:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1104
		{
			yyVAL.columns = Columns{yyDollar[1].colIdent}
		}
	case 203:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1108
		{
			yyVAL.columns = append(yyVAL.columns, yyDollar[3].colIdent)
		}
	case 204:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1113
		{
			yyVAL.updateExprs = nil
		}
	case 205:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:1117
		{
			yyVAL.updateExprs = yyDollar[5].updateExprs
		}
	case 206:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:1123
		{
			yyVAL.insRows = yyDollar[2].values
		}
	case 207:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1127
		{
			yyVAL.insRows = yyDollar[1].selStmt
		}
	case 208:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1133
		{
			yyVAL.values = Values{yyDollar[1].valTuple}
		}
	case 209:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1137
		{
			yyVAL.values = append(yyDollar[1].values, yyDollar[3].valTuple)
		}
	case 210:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1143
		{
			yyVAL.valTuple = ValTuple(yyDollar[2].valExprs)
		}
	case 211:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1149
		{
			yyVAL.updateExprs = UpdateExprs{yyDollar[1].updateExpr}
		}
	case 212:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1153
		{
			yyVAL.updateExprs = append(yyDollar[1].updateExprs, yyDollar[3].updateExpr)
		}
	case 213:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1159
		{
			yyVAL.updateExpr = &UpdateExpr{Name: yyDollar[1].colIdent, Expr: yyDollar[3].valExpr}
		}
	case 216:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1168
		{
			yyVAL.byt = 0
		}
	case 217:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:1170
		{
			yyVAL.byt = 1
		}
	case 218:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1173
		{
			yyVAL.empty = struct{}{}
		}
	case 219:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1175
		{
			yyVAL.empty = struct{}{}
		}
	case 220:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1178
		{
			yyVAL.str = ""
		}
	case 221:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1180
		{
			yyVAL.str = IgnoreStr
		}
	case 222:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1184
		{
			yyVAL.empty = struct{}{}
		}
	case 223:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1186
		{
			yyVAL.empty = struct{}{}
		}
	case 224:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1188
		{
			yyVAL.empty = struct{}{}
		}
	case 225:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1190
		{
			yyVAL.empty = struct{}{}
		}
	case 226:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1192
		{
			yyVAL.empty = struct{}{}
		}
	case 227:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1194
		{
			yyVAL.empty = struct{}{}
		}
	case 228:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1197
		{
			yyVAL.empty = struct{}{}
		}
	case 229:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1199
		{
			yyVAL.empty = struct{}{}
		}
	case 230:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1202
		{
			yyVAL.empty = struct{}{}
		}
	case 231:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1204
		{
			yyVAL.empty = struct{}{}
		}
	case 232:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1207
		{
			yyVAL.empty = struct{}{}
		}
	case 233:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:1209
		{
			yyVAL.empty = struct{}{}
		}
	case 234:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1213
		{
			yyVAL.colIdent = NewColIdent(string(yyDollar[1].bytes))
		}
	case 235:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1219
		{
			yyVAL.tableIdent = TableIdent(yyDollar[1].bytes)
		}
	case 236:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1225
		{
			if incNesting(yylex) {
				yylex.Error("max nesting level reached")
				return 1
			}
		}
	case 237:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1234
		{
			decNesting(yylex)
		}
	case 238:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1239
		{
			forceEOF(yylex)
		}
	}
	goto yystack /* stack new state and value */
}
