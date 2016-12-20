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
const HEXNUM = 57391
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
const END = 57406
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
const MOD = 57417
const UNARY = 57418
const INTERVAL = 57419
const JSON_EXTRACT_OP = 57420
const JSON_UNQUOTE_EXTRACT_OP = 57421
const CREATE = 57422
const ALTER = 57423
const DROP = 57424
const RENAME = 57425
const ANALYZE = 57426
const TABLE = 57427
const INDEX = 57428
const VIEW = 57429
const TO = 57430
const IGNORE = 57431
const IF = 57432
const UNIQUE = 57433
const USING = 57434
const SHOW = 57435
const DESCRIBE = 57436
const EXPLAIN = 57437
const CURRENT_TIMESTAMP = 57438
const DATABASE = 57439
const UNUSED = 57440

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
	93, 240,
	-2, 239,
}

const yyNprod = 244
const yyPrivate = 57344

var yyTokenNames []string
var yyStates []string

const yyLast = 938

var yyAct = [...]int{

	121, 360, 282, 427, 62, 107, 305, 228, 314, 209,
	115, 324, 242, 259, 349, 240, 296, 208, 3, 172,
	102, 253, 147, 156, 113, 103, 244, 74, 35, 241,
	37, 47, 379, 381, 38, 64, 152, 67, 69, 41,
	40, 71, 41, 237, 97, 50, 410, 14, 15, 16,
	17, 116, 43, 44, 45, 81, 409, 48, 49, 58,
	162, 408, 68, 70, 46, 42, 211, 212, 331, 18,
	231, 176, 87, 160, 63, 189, 109, 101, 196, 197,
	190, 191, 192, 193, 194, 195, 189, 64, 88, 90,
	64, 145, 433, 266, 164, 380, 192, 193, 194, 195,
	189, 94, 179, 96, 178, 177, 174, 264, 265, 263,
	392, 390, 144, 248, 297, 112, 347, 205, 207, 297,
	179, 177, 84, 153, 92, 190, 191, 192, 193, 194,
	195, 189, 65, 167, 283, 89, 179, 19, 20, 22,
	21, 23, 60, 159, 161, 158, 178, 177, 112, 112,
	24, 25, 26, 109, 230, 178, 177, 234, 217, 218,
	163, 93, 179, 220, 149, 75, 223, 127, 227, 127,
	404, 179, 60, 60, 174, 327, 262, 249, 219, 285,
	65, 226, 175, 251, 252, 112, 399, 400, 206, 235,
	397, 238, 14, 247, 148, 239, 187, 196, 197, 190,
	191, 192, 193, 194, 195, 189, 112, 246, 270, 435,
	283, 284, 286, 127, 112, 112, 394, 289, 260, 312,
	290, 293, 245, 254, 256, 257, 64, 303, 255, 89,
	301, 127, 249, 261, 60, 304, 287, 288, 291, 294,
	169, 283, 300, 188, 187, 196, 197, 190, 191, 192,
	193, 194, 195, 189, 112, 112, 129, 128, 130, 131,
	132, 325, 330, 133, 332, 333, 334, 108, 65, 350,
	335, 82, 28, 83, 173, 285, 283, 336, 225, 150,
	312, 283, 79, 246, 356, 283, 350, 233, 89, 60,
	100, 327, 407, 73, 342, 127, 406, 344, 245, 170,
	260, 210, 65, 348, 175, 355, 213, 214, 215, 216,
	352, 357, 346, 343, 283, 261, 354, 316, 319, 320,
	321, 317, 371, 318, 322, 374, 367, 222, 369, 112,
	375, 377, 385, 384, 112, 169, 353, 382, 386, 76,
	362, 232, 366, 372, 368, 370, 389, 55, 373, 246,
	246, 246, 246, 376, 396, 320, 321, 395, 108, 143,
	54, 142, 86, 413, 245, 245, 245, 245, 39, 258,
	402, 401, 267, 268, 269, 403, 271, 272, 273, 274,
	275, 276, 277, 278, 279, 280, 281, 188, 187, 196,
	197, 190, 191, 192, 193, 194, 195, 189, 393, 416,
	57, 112, 425, 14, 85, 414, 108, 108, 417, 154,
	418, 419, 99, 329, 426, 306, 51, 52, 151, 365,
	307, 428, 428, 428, 64, 429, 430, 299, 431, 229,
	434, 353, 436, 437, 438, 311, 439, 141, 364, 440,
	148, 112, 112, 61, 140, 420, 421, 422, 432, 232,
	423, 59, 14, 337, 338, 339, 316, 319, 320, 321,
	317, 72, 318, 322, 127, 77, 405, 109, 129, 128,
	130, 131, 132, 28, 341, 133, 30, 1, 328, 323,
	59, 108, 138, 171, 155, 91, 36, 236, 157, 95,
	66, 139, 98, 302, 224, 358, 361, 106, 424, 398,
	359, 122, 117, 118, 59, 363, 146, 137, 310, 119,
	345, 120, 221, 295, 123, 114, 165, 351, 80, 166,
	298, 29, 180, 110, 387, 134, 378, 315, 313, 388,
	243, 135, 136, 292, 168, 126, 391, 31, 32, 33,
	34, 105, 232, 188, 187, 196, 197, 190, 191, 192,
	193, 194, 195, 189, 232, 78, 53, 27, 59, 127,
	56, 283, 109, 129, 128, 130, 131, 132, 13, 12,
	133, 124, 125, 11, 10, 111, 411, 138, 9, 8,
	7, 412, 6, 5, 4, 415, 361, 2, 106, 59,
	340, 0, 0, 0, 0, 250, 0, 117, 118, 104,
	0, 0, 137, 0, 119, 0, 120, 0, 0, 188,
	187, 196, 197, 190, 191, 192, 193, 194, 195, 189,
	134, 0, 0, 0, 0, 0, 135, 136, 0, 0,
	0, 0, 0, 0, 0, 0, 106, 106, 0, 0,
	0, 0, 0, 0, 0, 126, 0, 0, 0, 0,
	250, 0, 308, 0, 0, 309, 0, 65, 0, 0,
	0, 0, 0, 326, 0, 59, 0, 0, 0, 127,
	0, 283, 109, 129, 128, 130, 131, 132, 0, 0,
	133, 124, 125, 0, 0, 111, 0, 138, 188, 187,
	196, 197, 190, 191, 192, 193, 194, 195, 189, 0,
	0, 0, 0, 0, 0, 0, 0, 117, 118, 104,
	0, 106, 137, 0, 119, 0, 120, 0, 0, 0,
	0, 126, 0, 14, 0, 0, 0, 0, 0, 0,
	134, 59, 59, 59, 59, 0, 135, 136, 126, 0,
	0, 0, 0, 0, 326, 127, 0, 383, 109, 129,
	128, 130, 131, 132, 0, 0, 133, 124, 125, 0,
	0, 111, 127, 138, 0, 109, 129, 128, 130, 131,
	132, 0, 0, 133, 124, 125, 0, 0, 111, 0,
	138, 0, 0, 117, 118, 104, 0, 0, 137, 0,
	119, 0, 120, 0, 0, 0, 0, 0, 0, 0,
	117, 118, 0, 0, 126, 137, 134, 119, 14, 120,
	0, 0, 135, 136, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 134, 0, 0, 0, 0, 127, 135,
	136, 109, 129, 128, 130, 131, 132, 0, 0, 133,
	124, 125, 0, 0, 111, 0, 138, 127, 0, 0,
	109, 129, 128, 130, 131, 132, 0, 0, 133, 0,
	0, 0, 0, 0, 0, 138, 117, 118, 0, 0,
	0, 137, 0, 119, 0, 120, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 117, 118, 0, 0, 134,
	137, 0, 119, 0, 120, 135, 136, 188, 187, 196,
	197, 190, 191, 192, 193, 194, 195, 189, 134, 182,
	185, 0, 0, 0, 135, 136, 198, 199, 200, 201,
	202, 203, 204, 186, 183, 184, 181, 188, 187, 196,
	197, 190, 191, 192, 193, 194, 195, 189,
}
var yyPact = [...]int{

	41, -1000, -1000, 468, -1000, -1000, -1000, -1000, -1000, -1000,
	-1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, -73,
	-63, -36, -49, -37, -1000, -1000, -1000, 446, 398, 328,
	-1000, -66, 94, 433, 84, -69, -40, 84, -1000, -38,
	84, -1000, 94, -79, 117, -79, 94, -1000, -1000, -1000,
	-1000, -1000, -1000, 247, 220, -1000, 67, 380, 334, -21,
	-1000, 94, 89, -1000, 21, -1000, 94, 63, 113, -1000,
	94, -1000, -60, 94, 391, 246, 84, -1000, 700, -1000,
	427, -1000, 331, 329, -1000, 94, 84, 94, 429, 84,
	419, -1000, 388, -85, -1000, 46, -1000, 94, -1000, -1000,
	94, -1000, 289, -1000, -1000, 254, -22, 96, 848, -1000,
	-1000, 783, 717, -1000, -28, -1000, -1000, 419, 419, 419,
	419, 168, 168, -1000, -1000, -1000, 168, -1000, -1000, -1000,
	-1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, 419, 94,
	-1000, -1000, -1000, -1000, 250, 183, -1000, 415, 783, -1000,
	818, -23, 802, -1000, -1000, 243, 84, -1000, -61, -1000,
	-1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, 429, 700,
	124, -1000, -1000, 132, -1000, -1000, 28, 783, 783, 167,
	419, 122, 31, 419, 419, 419, 167, 419, 419, 419,
	419, 419, 419, 419, 419, 419, 419, 419, -1000, -1000,
	-1000, -1000, -1000, -1000, -1000, 27, 848, 87, 267, 229,
	848, 207, 207, -1000, -1000, -1000, 609, 514, 624, -1000,
	446, 55, 818, -1000, 397, 84, 84, 415, 399, 405,
	96, 105, 818, 94, -1000, -1000, 94, -1000, 423, -1000,
	173, 283, -1000, -1000, 241, 393, 186, -1000, -1000, -1000,
	-25, 27, 61, -1000, -1000, 208, -1000, -1000, 818, -1000,
	802, -1000, -1000, 122, 419, 419, 419, 818, 818, 530,
	-1000, -3, 116, -1000, 11, 11, -14, -14, -14, -14,
	42, 42, -1000, -1000, -1000, 419, -1000, -1000, -1000, -1000,
	-1000, 194, 700, -1000, 194, 50, -1000, 783, 225, 168,
	468, 242, 238, -1000, 399, -1000, 419, 419, -1000, -1000,
	425, 404, 124, 124, 124, 124, -1000, 311, 288, -1000,
	309, 291, 319, -10, -1000, 125, -1000, -1000, 94, -1000,
	234, 84, -1000, -1000, -1000, 229, -1000, 818, 818, 464,
	419, 818, -1000, 194, -1000, 44, -1000, 419, 45, -1000,
	373, 170, -1000, 419, -1000, -1000, 84, -1000, 308, 144,
	-1000, 164, -1000, 415, 783, 419, 283, 126, 422, -1000,
	-1000, -1000, -1000, 262, -1000, 258, -1000, -1000, -1000, -41,
	-46, -56, -1000, -1000, -1000, -1000, -1000, 419, 818, -1000,
	-1000, 818, 419, 337, 168, -1000, 419, 419, -1000, -1000,
	-1000, 399, 96, 133, 783, 783, -1000, -1000, 168, 168,
	168, 818, 818, 442, -1000, 818, -1000, 385, 96, 96,
	84, 84, 84, 84, -1000, 440, 14, 163, -1000, 163,
	163, 89, -1000, 84, -1000, 84, -1000, -1000, 84, -1000,
	-1000,
}
var yyPgo = [...]int{

	0, 587, 17, 584, 583, 582, 580, 579, 578, 574,
	573, 569, 568, 521, 560, 557, 556, 555, 20, 25,
	541, 534, 15, 29, 12, 530, 528, 8, 527, 26,
	526, 3, 22, 5, 523, 522, 520, 24, 188, 518,
	21, 13, 9, 517, 10, 51, 515, 514, 513, 16,
	512, 510, 508, 505, 501, 7, 500, 1, 499, 6,
	498, 494, 493, 14, 4, 74, 491, 368, 293, 490,
	488, 487, 486, 484, 0, 19, 483, 418, 11, 479,
	478, 31, 477, 476, 36, 2,
}
var yyR1 = [...]int{

	0, 82, 1, 1, 1, 1, 1, 1, 1, 1,
	1, 1, 1, 2, 2, 2, 3, 3, 4, 5,
	6, 7, 7, 7, 8, 8, 8, 9, 10, 10,
	10, 11, 12, 12, 12, 83, 13, 14, 14, 15,
	15, 15, 16, 16, 17, 17, 18, 18, 19, 19,
	19, 20, 20, 76, 76, 76, 75, 75, 21, 21,
	22, 22, 23, 23, 24, 24, 24, 25, 25, 25,
	25, 80, 80, 79, 79, 79, 78, 78, 26, 26,
	26, 26, 27, 27, 27, 27, 28, 28, 29, 29,
	30, 30, 30, 30, 31, 31, 32, 32, 33, 33,
	33, 33, 33, 33, 34, 34, 34, 34, 34, 34,
	34, 34, 34, 34, 34, 34, 34, 40, 40, 40,
	40, 40, 40, 35, 35, 35, 35, 35, 35, 35,
	41, 41, 41, 45, 42, 42, 38, 38, 38, 38,
	38, 38, 38, 38, 38, 38, 38, 38, 38, 38,
	38, 38, 38, 38, 38, 38, 38, 38, 38, 38,
	38, 38, 38, 54, 54, 54, 54, 47, 50, 50,
	48, 48, 49, 51, 51, 46, 46, 46, 37, 37,
	37, 37, 37, 37, 39, 39, 39, 52, 52, 53,
	53, 55, 55, 56, 56, 57, 58, 58, 58, 59,
	59, 59, 60, 60, 60, 61, 61, 62, 62, 63,
	63, 36, 36, 43, 43, 44, 64, 64, 65, 66,
	66, 68, 68, 69, 69, 67, 67, 70, 70, 70,
	70, 70, 70, 71, 71, 72, 72, 73, 73, 74,
	77, 84, 85, 81,
}
var yyR2 = [...]int{

	0, 1, 1, 1, 1, 1, 1, 1, 1, 1,
	1, 1, 1, 12, 6, 3, 8, 8, 8, 7,
	3, 5, 8, 4, 6, 7, 4, 5, 4, 5,
	5, 3, 2, 2, 2, 0, 2, 0, 2, 1,
	2, 2, 0, 1, 0, 1, 1, 3, 1, 2,
	3, 1, 1, 0, 1, 2, 1, 1, 0, 2,
	1, 3, 1, 1, 3, 3, 3, 3, 5, 5,
	3, 0, 1, 0, 1, 2, 1, 1, 1, 2,
	2, 1, 2, 3, 2, 3, 2, 2, 1, 3,
	0, 5, 5, 5, 1, 3, 0, 2, 1, 3,
	3, 2, 3, 3, 1, 1, 3, 3, 4, 3,
	4, 3, 4, 5, 6, 3, 2, 1, 2, 1,
	2, 1, 2, 1, 1, 1, 1, 1, 1, 1,
	3, 1, 1, 3, 1, 3, 1, 1, 1, 1,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 2, 2, 2, 3, 3, 4, 5,
	3, 4, 1, 1, 1, 1, 1, 5, 0, 1,
	1, 2, 4, 0, 2, 1, 3, 5, 1, 1,
	1, 1, 1, 1, 1, 2, 2, 0, 3, 0,
	2, 0, 3, 1, 3, 2, 0, 1, 1, 0,
	2, 4, 0, 2, 4, 0, 3, 1, 3, 0,
	5, 2, 1, 1, 3, 3, 1, 3, 3, 1,
	1, 0, 2, 0, 3, 0, 1, 1, 1, 1,
	1, 1, 1, 0, 1, 0, 1, 0, 2, 1,
	1, 1, 1, 0,
}
var yyChk = [...]int{

	-1000, -82, -1, -2, -3, -4, -5, -6, -7, -8,
	-9, -10, -11, -12, 6, 7, 8, 9, 28, 96,
	97, 99, 98, 100, 109, 110, 111, -15, 5, -13,
	-83, -13, -13, -13, -13, 101, -72, 103, 107, -67,
	103, 105, 101, 101, 102, 103, 101, -81, -81, -81,
	-2, 18, 19, -16, 32, 19, -14, -67, -29, -77,
	48, 10, -64, -65, -74, 48, -69, 106, 102, -74,
	101, -74, -77, -68, 106, 48, -68, -77, -17, 35,
	-39, -74, 51, 53, 55, 24, 28, 93, -29, 46,
	68, -77, 61, 48, -81, -77, -81, 104, -77, 21,
	44, -74, -18, -19, 85, -20, -77, -33, -38, 48,
	-34, 61, -84, -37, -46, -44, -45, 83, 84, 90,
	92, -74, -54, -47, 57, 58, 21, 45, 50, 49,
	51, 52, 53, 56, 106, 112, 113, 88, 63, -66,
	17, 10, 30, 30, -29, -64, -77, -32, 11, -65,
	-38, -77, -84, -81, 21, -73, 108, -70, 99, 97,
	27, 98, 14, 114, 48, -77, -77, -81, -21, 46,
	10, -76, -75, 20, -74, 50, 93, 60, 59, 75,
	-35, 78, 61, 76, 77, 62, 75, 80, 79, 89,
	83, 84, 85, 86, 87, 88, 81, 82, 68, 69,
	70, 71, 72, 73, 74, -33, -38, -33, -2, -42,
	-38, 94, 95, -38, -38, -38, -38, -84, -84, -45,
	-84, -50, -38, -29, -61, 28, -84, -32, -55, 14,
	-33, 93, -38, 44, -74, -81, -71, 104, -32, -19,
	-22, -23, -24, -25, -29, -45, -84, -75, 85, -74,
	-77, -33, -33, -40, 56, 61, 57, 58, -38, -41,
	-84, -45, 54, 78, 76, 77, 62, -38, -38, -38,
	-40, -38, -38, -38, -38, -38, -38, -38, -38, -38,
	-38, -38, -85, 47, -85, 46, -85, -37, -37, -74,
	-85, -18, 19, -85, -18, -48, -49, 64, -36, 30,
	-2, -64, -62, -74, -55, -59, 16, 15, -77, -77,
	-52, 12, 46, -26, -27, -28, 34, 38, 40, 35,
	36, 37, 41, -79, -78, 20, -77, 50, -80, 20,
	-22, 93, 56, 57, 58, -42, -41, -38, -38, -38,
	60, -38, -85, -18, -85, -51, -49, 66, -33, -63,
	44, -43, -44, -84, -63, -85, 46, -59, -38, -56,
	-57, -38, -81, -53, 13, 15, -23, -24, -23, -24,
	34, 34, 34, 39, 34, 39, 34, -27, -30, 42,
	105, 43, -78, -77, -85, -74, -85, 60, -38, -85,
	67, -38, 65, 25, 46, -74, 46, 46, -58, 22,
	23, -55, -33, -42, 44, 44, 34, 34, 102, 102,
	102, -38, -38, 26, -44, -38, -57, -59, -33, -33,
	-84, -84, -84, 8, -60, 17, 29, -31, -74, -31,
	-31, -64, 8, 78, -85, 46, -85, -85, -74, -74,
	-74,
}
var yyDef = [...]int{

	0, -2, 1, 2, 3, 4, 5, 6, 7, 8,
	9, 10, 11, 12, 35, 35, 35, 35, 35, 235,
	225, 0, 0, 0, 243, 243, 243, 0, 39, 42,
	37, 225, 0, 0, 0, 223, 0, 0, 236, 0,
	0, 226, 0, 221, 0, 221, 0, 32, 33, 34,
	15, 40, 41, 44, 0, 43, 36, 0, 0, 88,
	240, 0, 20, 216, 0, 239, 0, 0, 0, 243,
	0, 243, 0, 0, 0, 0, 0, 31, 0, 45,
	0, 184, 0, 0, 38, 0, 0, 0, 96, 0,
	0, 243, 0, 237, 23, 0, 26, 0, 28, 222,
	0, 243, 58, 46, 48, 53, 0, 51, 52, -2,
	98, 0, 0, 136, 137, 138, 139, 0, 0, 0,
	0, 175, 0, 162, 104, 105, 0, 241, 178, 179,
	180, 181, 182, 183, 163, 164, 165, 166, 168, 0,
	219, 220, 185, 186, 205, 96, 89, 191, 0, 217,
	218, 0, 0, 21, 224, 0, 0, 243, 233, 227,
	228, 229, 230, 231, 232, 27, 29, 30, 96, 0,
	0, 49, 54, 0, 56, 57, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 123, 124,
	125, 126, 127, 128, 129, 101, 0, 0, 0, 0,
	134, 0, 0, 153, 154, 155, 0, 0, 0, 116,
	0, 0, 169, 14, 0, 0, 0, 191, 199, 0,
	97, 0, 134, 0, 238, 24, 0, 234, 187, 47,
	59, 60, 62, 63, 73, 71, 0, 55, 50, 176,
	0, 99, 100, 103, 117, 0, 119, 121, 106, 107,
	0, 131, 132, 0, 0, 0, 0, 109, 111, 0,
	115, 140, 141, 142, 143, 144, 145, 146, 147, 148,
	149, 150, 102, 242, 133, 0, 215, 151, 152, 156,
	157, 0, 0, 160, 0, 173, 170, 0, 209, 0,
	212, 209, 0, 207, 199, 19, 0, 0, 243, 25,
	189, 0, 0, 0, 0, 0, 78, 0, 0, 81,
	0, 0, 0, 90, 74, 0, 76, 77, 0, 72,
	0, 0, 118, 120, 122, 0, 108, 110, 112, 0,
	0, 135, 158, 0, 161, 0, 171, 0, 0, 16,
	0, 211, 213, 0, 17, 206, 0, 18, 200, 192,
	193, 196, 22, 191, 0, 0, 61, 67, 0, 70,
	79, 80, 82, 0, 84, 0, 86, 87, 64, 0,
	0, 0, 75, 65, 66, 177, 130, 0, 113, 159,
	167, 174, 0, 0, 0, 208, 0, 0, 195, 197,
	198, 199, 190, 188, 0, 0, 83, 85, 0, 0,
	0, 114, 172, 0, 214, 201, 194, 202, 68, 69,
	0, 0, 0, 0, 13, 0, 0, 0, 94, 0,
	0, 210, 203, 0, 91, 0, 92, 93, 0, 95,
	204,
}
var yyTok1 = [...]int{

	1, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 87, 80, 3,
	45, 47, 85, 83, 46, 84, 93, 86, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	69, 68, 70, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 89, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 79, 3, 90,
}
var yyTok2 = [...]int{

	2, 3, 4, 5, 6, 7, 8, 9, 10, 11,
	12, 13, 14, 15, 16, 17, 18, 19, 20, 21,
	22, 23, 24, 25, 26, 27, 28, 29, 30, 31,
	32, 33, 34, 35, 36, 37, 38, 39, 40, 41,
	42, 43, 44, 48, 49, 50, 51, 52, 53, 54,
	55, 56, 57, 58, 59, 60, 61, 62, 63, 64,
	65, 66, 67, 71, 72, 73, 74, 75, 76, 77,
	78, 81, 82, 88, 91, 92, 94, 95, 96, 97,
	98, 99, 100, 101, 102, 103, 104, 105, 106, 107,
	108, 109, 110, 111, 112, 113, 114,
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
			yyVAL.statement = &DDL{Action: CreateStr, NewName: NewTableIdent(yyDollar[3].colIdent.Lowered())}
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
			yyVAL.statement = &DDL{Action: AlterStr, Table: NewTableIdent(yyDollar[3].colIdent.Lowered()), NewName: NewTableIdent(yyDollar[3].colIdent.Lowered())}
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
			yyVAL.statement = &DDL{Action: DropStr, Table: NewTableIdent(yyDollar[4].colIdent.Lowered()), IfExists: exists}
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
	case 57:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:427
		{
			yyVAL.colIdent = NewColIdent(string(yyDollar[1].bytes))
		}
	case 58:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:432
		{
			yyVAL.tableExprs = TableExprs{&AliasedTableExpr{Expr: &TableName{Name: NewTableIdent("dual")}}}
		}
	case 59:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:436
		{
			yyVAL.tableExprs = yyDollar[2].tableExprs
		}
	case 60:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:442
		{
			yyVAL.tableExprs = TableExprs{yyDollar[1].tableExpr}
		}
	case 61:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:446
		{
			yyVAL.tableExprs = append(yyVAL.tableExprs, yyDollar[3].tableExpr)
		}
	case 64:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:456
		{
			yyVAL.tableExpr = &AliasedTableExpr{Expr: yyDollar[1].tableName, As: yyDollar[2].tableIdent, Hints: yyDollar[3].indexHints}
		}
	case 65:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:460
		{
			yyVAL.tableExpr = &AliasedTableExpr{Expr: yyDollar[1].subquery, As: yyDollar[3].tableIdent}
		}
	case 66:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:464
		{
			yyVAL.tableExpr = &ParenTableExpr{Exprs: yyDollar[2].tableExprs}
		}
	case 67:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:477
		{
			yyVAL.tableExpr = &JoinTableExpr{LeftExpr: yyDollar[1].tableExpr, Join: yyDollar[2].str, RightExpr: yyDollar[3].tableExpr}
		}
	case 68:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:481
		{
			yyVAL.tableExpr = &JoinTableExpr{LeftExpr: yyDollar[1].tableExpr, Join: yyDollar[2].str, RightExpr: yyDollar[3].tableExpr, On: yyDollar[5].boolExpr}
		}
	case 69:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:485
		{
			yyVAL.tableExpr = &JoinTableExpr{LeftExpr: yyDollar[1].tableExpr, Join: yyDollar[2].str, RightExpr: yyDollar[3].tableExpr, On: yyDollar[5].boolExpr}
		}
	case 70:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:489
		{
			yyVAL.tableExpr = &JoinTableExpr{LeftExpr: yyDollar[1].tableExpr, Join: yyDollar[2].str, RightExpr: yyDollar[3].tableExpr}
		}
	case 71:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:494
		{
			yyVAL.empty = struct{}{}
		}
	case 72:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:496
		{
			yyVAL.empty = struct{}{}
		}
	case 73:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:499
		{
			yyVAL.tableIdent = NewTableIdent("")
		}
	case 74:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:503
		{
			yyVAL.tableIdent = yyDollar[1].tableIdent
		}
	case 75:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:507
		{
			yyVAL.tableIdent = yyDollar[2].tableIdent
		}
	case 77:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:514
		{
			yyVAL.tableIdent = NewTableIdent(string(yyDollar[1].bytes))
		}
	case 78:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:520
		{
			yyVAL.str = JoinStr
		}
	case 79:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:524
		{
			yyVAL.str = JoinStr
		}
	case 80:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:528
		{
			yyVAL.str = JoinStr
		}
	case 81:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:532
		{
			yyVAL.str = StraightJoinStr
		}
	case 82:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:538
		{
			yyVAL.str = LeftJoinStr
		}
	case 83:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:542
		{
			yyVAL.str = LeftJoinStr
		}
	case 84:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:546
		{
			yyVAL.str = RightJoinStr
		}
	case 85:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:550
		{
			yyVAL.str = RightJoinStr
		}
	case 86:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:556
		{
			yyVAL.str = NaturalJoinStr
		}
	case 87:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:560
		{
			if yyDollar[2].str == LeftJoinStr {
				yyVAL.str = NaturalLeftJoinStr
			} else {
				yyVAL.str = NaturalRightJoinStr
			}
		}
	case 88:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:570
		{
			yyVAL.tableName = &TableName{Name: yyDollar[1].tableIdent}
		}
	case 89:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:574
		{
			yyVAL.tableName = &TableName{Qualifier: yyDollar[1].tableIdent, Name: yyDollar[3].tableIdent}
		}
	case 90:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:579
		{
			yyVAL.indexHints = nil
		}
	case 91:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:583
		{
			yyVAL.indexHints = &IndexHints{Type: UseStr, Indexes: yyDollar[4].colIdents}
		}
	case 92:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:587
		{
			yyVAL.indexHints = &IndexHints{Type: IgnoreStr, Indexes: yyDollar[4].colIdents}
		}
	case 93:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:591
		{
			yyVAL.indexHints = &IndexHints{Type: ForceStr, Indexes: yyDollar[4].colIdents}
		}
	case 94:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:597
		{
			yyVAL.colIdents = []ColIdent{yyDollar[1].colIdent}
		}
	case 95:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:601
		{
			yyVAL.colIdents = append(yyDollar[1].colIdents, yyDollar[3].colIdent)
		}
	case 96:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:606
		{
			yyVAL.boolExpr = nil
		}
	case 97:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:610
		{
			yyVAL.boolExpr = yyDollar[2].boolExpr
		}
	case 99:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:617
		{
			yyVAL.boolExpr = &AndExpr{Left: yyDollar[1].boolExpr, Right: yyDollar[3].boolExpr}
		}
	case 100:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:621
		{
			yyVAL.boolExpr = &OrExpr{Left: yyDollar[1].boolExpr, Right: yyDollar[3].boolExpr}
		}
	case 101:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:625
		{
			yyVAL.boolExpr = &NotExpr{Expr: yyDollar[2].boolExpr}
		}
	case 102:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:629
		{
			yyVAL.boolExpr = &ParenBoolExpr{Expr: yyDollar[2].boolExpr}
		}
	case 103:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:633
		{
			yyVAL.boolExpr = &IsExpr{Operator: yyDollar[3].str, Expr: yyDollar[1].boolExpr}
		}
	case 104:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:639
		{
			yyVAL.boolExpr = BoolVal(true)
		}
	case 105:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:643
		{
			yyVAL.boolExpr = BoolVal(false)
		}
	case 106:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:647
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: yyDollar[2].str, Right: yyDollar[3].valExpr}
		}
	case 107:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:651
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: InStr, Right: yyDollar[3].colTuple}
		}
	case 108:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:655
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: NotInStr, Right: yyDollar[4].colTuple}
		}
	case 109:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:659
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: LikeStr, Right: yyDollar[3].valExpr}
		}
	case 110:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:663
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: NotLikeStr, Right: yyDollar[4].valExpr}
		}
	case 111:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:667
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: RegexpStr, Right: yyDollar[3].valExpr}
		}
	case 112:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:671
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: NotRegexpStr, Right: yyDollar[4].valExpr}
		}
	case 113:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:675
		{
			yyVAL.boolExpr = &RangeCond{Left: yyDollar[1].valExpr, Operator: BetweenStr, From: yyDollar[3].valExpr, To: yyDollar[5].valExpr}
		}
	case 114:
		yyDollar = yyS[yypt-6 : yypt+1]
		//line sql.y:679
		{
			yyVAL.boolExpr = &RangeCond{Left: yyDollar[1].valExpr, Operator: NotBetweenStr, From: yyDollar[4].valExpr, To: yyDollar[6].valExpr}
		}
	case 115:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:683
		{
			yyVAL.boolExpr = &IsExpr{Operator: yyDollar[3].str, Expr: yyDollar[1].valExpr}
		}
	case 116:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:687
		{
			yyVAL.boolExpr = &ExistsExpr{Subquery: yyDollar[2].subquery}
		}
	case 117:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:693
		{
			yyVAL.str = IsNullStr
		}
	case 118:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:697
		{
			yyVAL.str = IsNotNullStr
		}
	case 119:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:701
		{
			yyVAL.str = IsTrueStr
		}
	case 120:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:705
		{
			yyVAL.str = IsNotTrueStr
		}
	case 121:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:709
		{
			yyVAL.str = IsFalseStr
		}
	case 122:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:713
		{
			yyVAL.str = IsNotFalseStr
		}
	case 123:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:719
		{
			yyVAL.str = EqualStr
		}
	case 124:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:723
		{
			yyVAL.str = LessThanStr
		}
	case 125:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:727
		{
			yyVAL.str = GreaterThanStr
		}
	case 126:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:731
		{
			yyVAL.str = LessEqualStr
		}
	case 127:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:735
		{
			yyVAL.str = GreaterEqualStr
		}
	case 128:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:739
		{
			yyVAL.str = NotEqualStr
		}
	case 129:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:743
		{
			yyVAL.str = NullSafeEqualStr
		}
	case 130:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:749
		{
			yyVAL.colTuple = ValTuple(yyDollar[2].valExprs)
		}
	case 131:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:753
		{
			yyVAL.colTuple = yyDollar[1].subquery
		}
	case 132:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:757
		{
			yyVAL.colTuple = ListArg(yyDollar[1].bytes)
		}
	case 133:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:763
		{
			yyVAL.subquery = &Subquery{yyDollar[2].selStmt}
		}
	case 134:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:769
		{
			yyVAL.valExprs = ValExprs{yyDollar[1].valExpr}
		}
	case 135:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:773
		{
			yyVAL.valExprs = append(yyDollar[1].valExprs, yyDollar[3].valExpr)
		}
	case 136:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:779
		{
			yyVAL.valExpr = yyDollar[1].valExpr
		}
	case 137:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:783
		{
			yyVAL.valExpr = yyDollar[1].colName
		}
	case 138:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:787
		{
			yyVAL.valExpr = yyDollar[1].valTuple
		}
	case 139:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:791
		{
			yyVAL.valExpr = yyDollar[1].subquery
		}
	case 140:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:795
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: BitAndStr, Right: yyDollar[3].valExpr}
		}
	case 141:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:799
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: BitOrStr, Right: yyDollar[3].valExpr}
		}
	case 142:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:803
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: BitXorStr, Right: yyDollar[3].valExpr}
		}
	case 143:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:807
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: PlusStr, Right: yyDollar[3].valExpr}
		}
	case 144:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:811
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: MinusStr, Right: yyDollar[3].valExpr}
		}
	case 145:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:815
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: MultStr, Right: yyDollar[3].valExpr}
		}
	case 146:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:819
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: DivStr, Right: yyDollar[3].valExpr}
		}
	case 147:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:823
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: ModStr, Right: yyDollar[3].valExpr}
		}
	case 148:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:827
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: ModStr, Right: yyDollar[3].valExpr}
		}
	case 149:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:831
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: ShiftLeftStr, Right: yyDollar[3].valExpr}
		}
	case 150:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:835
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: ShiftRightStr, Right: yyDollar[3].valExpr}
		}
	case 151:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:839
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].colName, Operator: JSONExtractOp, Right: yyDollar[3].valExpr}
		}
	case 152:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:843
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].colName, Operator: JSONUnquoteExtractOp, Right: yyDollar[3].valExpr}
		}
	case 153:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:847
		{
			if num, ok := yyDollar[2].valExpr.(NumVal); ok {
				yyVAL.valExpr = num
			} else {
				yyVAL.valExpr = &UnaryExpr{Operator: UPlusStr, Expr: yyDollar[2].valExpr}
			}
		}
	case 154:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:855
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
	case 155:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:868
		{
			yyVAL.valExpr = &UnaryExpr{Operator: TildaStr, Expr: yyDollar[2].valExpr}
		}
	case 156:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:872
		{
			// This rule prevents the usage of INTERVAL
			// as a function. If support is needed for that,
			// we'll need to revisit this. The solution
			// will be non-trivial because of grammar conflicts.
			yyVAL.valExpr = &IntervalExpr{Expr: yyDollar[2].valExpr, Unit: yyDollar[3].colIdent}
		}
	case 157:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:880
		{
			yyVAL.valExpr = &FuncExpr{Name: yyDollar[1].colIdent}
		}
	case 158:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:884
		{
			yyVAL.valExpr = &FuncExpr{Name: yyDollar[1].colIdent, Exprs: yyDollar[3].selectExprs}
		}
	case 159:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:888
		{
			yyVAL.valExpr = &FuncExpr{Name: yyDollar[1].colIdent, Distinct: true, Exprs: yyDollar[4].selectExprs}
		}
	case 160:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:892
		{
			yyVAL.valExpr = &FuncExpr{Name: yyDollar[1].colIdent}
		}
	case 161:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:896
		{
			yyVAL.valExpr = &FuncExpr{Name: yyDollar[1].colIdent, Exprs: yyDollar[3].selectExprs}
		}
	case 162:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:900
		{
			yyVAL.valExpr = yyDollar[1].caseExpr
		}
	case 163:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:906
		{
			yyVAL.colIdent = NewColIdent("if")
		}
	case 164:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:910
		{
			yyVAL.colIdent = NewColIdent("current_timestamp")
		}
	case 165:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:914
		{
			yyVAL.colIdent = NewColIdent("database")
		}
	case 166:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:918
		{
			yyVAL.colIdent = NewColIdent("mod")
		}
	case 167:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:924
		{
			yyVAL.caseExpr = &CaseExpr{Expr: yyDollar[2].valExpr, Whens: yyDollar[3].whens, Else: yyDollar[4].valExpr}
		}
	case 168:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:929
		{
			yyVAL.valExpr = nil
		}
	case 169:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:933
		{
			yyVAL.valExpr = yyDollar[1].valExpr
		}
	case 170:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:939
		{
			yyVAL.whens = []*When{yyDollar[1].when}
		}
	case 171:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:943
		{
			yyVAL.whens = append(yyDollar[1].whens, yyDollar[2].when)
		}
	case 172:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:949
		{
			yyVAL.when = &When{Cond: yyDollar[2].boolExpr, Val: yyDollar[4].valExpr}
		}
	case 173:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:954
		{
			yyVAL.valExpr = nil
		}
	case 174:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:958
		{
			yyVAL.valExpr = yyDollar[2].valExpr
		}
	case 175:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:964
		{
			yyVAL.colName = &ColName{Name: yyDollar[1].colIdent}
		}
	case 176:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:968
		{
			yyVAL.colName = &ColName{Qualifier: &TableName{Name: yyDollar[1].tableIdent}, Name: yyDollar[3].colIdent}
		}
	case 177:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:972
		{
			yyVAL.colName = &ColName{Qualifier: &TableName{Qualifier: yyDollar[1].tableIdent, Name: yyDollar[3].tableIdent}, Name: yyDollar[5].colIdent}
		}
	case 178:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:978
		{
			yyVAL.valExpr = StrVal(yyDollar[1].bytes)
		}
	case 179:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:982
		{
			yyVAL.valExpr = HexVal(yyDollar[1].bytes)
		}
	case 180:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:986
		{
			yyVAL.valExpr = NumVal(yyDollar[1].bytes)
		}
	case 181:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:990
		{
			yyVAL.valExpr = HexNum(yyDollar[1].bytes)
		}
	case 182:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:994
		{
			yyVAL.valExpr = ValArg(yyDollar[1].bytes)
		}
	case 183:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:998
		{
			yyVAL.valExpr = &NullVal{}
		}
	case 184:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1004
		{
			// TODO(sougou): Deprecate this construct.
			if yyDollar[1].colIdent.Lowered() != "value" {
				yylex.Error("expecting value after next")
				return 1
			}
			yyVAL.valExpr = NumVal("1")
		}
	case 185:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:1013
		{
			yyVAL.valExpr = NumVal(yyDollar[1].bytes)
		}
	case 186:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:1017
		{
			yyVAL.valExpr = ValArg(yyDollar[1].bytes)
		}
	case 187:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1022
		{
			yyVAL.valExprs = nil
		}
	case 188:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1026
		{
			yyVAL.valExprs = yyDollar[3].valExprs
		}
	case 189:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1031
		{
			yyVAL.boolExpr = nil
		}
	case 190:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:1035
		{
			yyVAL.boolExpr = yyDollar[2].boolExpr
		}
	case 191:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1040
		{
			yyVAL.orderBy = nil
		}
	case 192:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1044
		{
			yyVAL.orderBy = yyDollar[3].orderBy
		}
	case 193:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1050
		{
			yyVAL.orderBy = OrderBy{yyDollar[1].order}
		}
	case 194:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1054
		{
			yyVAL.orderBy = append(yyDollar[1].orderBy, yyDollar[3].order)
		}
	case 195:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:1060
		{
			yyVAL.order = &Order{Expr: yyDollar[1].valExpr, Direction: yyDollar[2].str}
		}
	case 196:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1065
		{
			yyVAL.str = AscScr
		}
	case 197:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1069
		{
			yyVAL.str = AscScr
		}
	case 198:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1073
		{
			yyVAL.str = DescScr
		}
	case 199:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1078
		{
			yyVAL.limit = nil
		}
	case 200:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:1082
		{
			yyVAL.limit = &Limit{Rowcount: yyDollar[2].valExpr}
		}
	case 201:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:1086
		{
			yyVAL.limit = &Limit{Offset: yyDollar[2].valExpr, Rowcount: yyDollar[4].valExpr}
		}
	case 202:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1091
		{
			yyVAL.str = ""
		}
	case 203:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:1095
		{
			yyVAL.str = ForUpdateStr
		}
	case 204:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:1099
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
	case 205:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1112
		{
			yyVAL.columns = nil
		}
	case 206:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1116
		{
			yyVAL.columns = yyDollar[2].columns
		}
	case 207:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1122
		{
			yyVAL.columns = Columns{yyDollar[1].colIdent}
		}
	case 208:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1126
		{
			yyVAL.columns = append(yyVAL.columns, yyDollar[3].colIdent)
		}
	case 209:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1131
		{
			yyVAL.updateExprs = nil
		}
	case 210:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:1135
		{
			yyVAL.updateExprs = yyDollar[5].updateExprs
		}
	case 211:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:1141
		{
			yyVAL.insRows = yyDollar[2].values
		}
	case 212:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1145
		{
			yyVAL.insRows = yyDollar[1].selStmt
		}
	case 213:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1151
		{
			yyVAL.values = Values{yyDollar[1].valTuple}
		}
	case 214:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1155
		{
			yyVAL.values = append(yyDollar[1].values, yyDollar[3].valTuple)
		}
	case 215:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1161
		{
			yyVAL.valTuple = ValTuple(yyDollar[2].valExprs)
		}
	case 216:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1167
		{
			yyVAL.updateExprs = UpdateExprs{yyDollar[1].updateExpr}
		}
	case 217:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1171
		{
			yyVAL.updateExprs = append(yyDollar[1].updateExprs, yyDollar[3].updateExpr)
		}
	case 218:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1177
		{
			yyVAL.updateExpr = &UpdateExpr{Name: yyDollar[1].colIdent, Expr: yyDollar[3].valExpr}
		}
	case 221:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1186
		{
			yyVAL.byt = 0
		}
	case 222:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:1188
		{
			yyVAL.byt = 1
		}
	case 223:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1191
		{
			yyVAL.empty = struct{}{}
		}
	case 224:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1193
		{
			yyVAL.empty = struct{}{}
		}
	case 225:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1196
		{
			yyVAL.str = ""
		}
	case 226:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1198
		{
			yyVAL.str = IgnoreStr
		}
	case 227:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1202
		{
			yyVAL.empty = struct{}{}
		}
	case 228:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1204
		{
			yyVAL.empty = struct{}{}
		}
	case 229:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1206
		{
			yyVAL.empty = struct{}{}
		}
	case 230:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1208
		{
			yyVAL.empty = struct{}{}
		}
	case 231:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1210
		{
			yyVAL.empty = struct{}{}
		}
	case 232:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1212
		{
			yyVAL.empty = struct{}{}
		}
	case 233:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1215
		{
			yyVAL.empty = struct{}{}
		}
	case 234:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1217
		{
			yyVAL.empty = struct{}{}
		}
	case 235:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1220
		{
			yyVAL.empty = struct{}{}
		}
	case 236:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1222
		{
			yyVAL.empty = struct{}{}
		}
	case 237:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1225
		{
			yyVAL.empty = struct{}{}
		}
	case 238:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:1227
		{
			yyVAL.empty = struct{}{}
		}
	case 239:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1231
		{
			yyVAL.colIdent = NewColIdent(string(yyDollar[1].bytes))
		}
	case 240:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1237
		{
			yyVAL.tableIdent = NewTableIdent(string(yyDollar[1].bytes))
		}
	case 241:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1243
		{
			if incNesting(yylex) {
				yylex.Error("max nesting level reached")
				return 1
			}
		}
	case 242:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1252
		{
			decNesting(yylex)
		}
	case 243:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1257
		{
			forceEOF(yylex)
		}
	}
	goto yystack /* stack new state and value */
}
