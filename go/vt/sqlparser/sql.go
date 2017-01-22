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
	boolVal     BoolVal
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
const OFFSET = 57359
const FOR = 57360
const ALL = 57361
const DISTINCT = 57362
const AS = 57363
const EXISTS = 57364
const ASC = 57365
const DESC = 57366
const INTO = 57367
const DUPLICATE = 57368
const KEY = 57369
const DEFAULT = 57370
const SET = 57371
const LOCK = 57372
const VALUES = 57373
const LAST_INSERT_ID = 57374
const NEXT = 57375
const VALUE = 57376
const JOIN = 57377
const STRAIGHT_JOIN = 57378
const LEFT = 57379
const RIGHT = 57380
const INNER = 57381
const OUTER = 57382
const CROSS = 57383
const NATURAL = 57384
const USE = 57385
const FORCE = 57386
const ON = 57387
const ID = 57388
const HEX = 57389
const STRING = 57390
const INTEGRAL = 57391
const FLOAT = 57392
const HEXNUM = 57393
const VALUE_ARG = 57394
const LIST_ARG = 57395
const COMMENT = 57396
const NULL = 57397
const TRUE = 57398
const FALSE = 57399
const OR = 57400
const AND = 57401
const NOT = 57402
const BETWEEN = 57403
const CASE = 57404
const WHEN = 57405
const THEN = 57406
const ELSE = 57407
const END = 57408
const LE = 57409
const GE = 57410
const NE = 57411
const NULL_SAFE_EQUAL = 57412
const IS = 57413
const LIKE = 57414
const REGEXP = 57415
const IN = 57416
const SHIFT_LEFT = 57417
const SHIFT_RIGHT = 57418
const MOD = 57419
const UNARY = 57420
const COLLATE = 57421
const INTERVAL = 57422
const JSON_EXTRACT_OP = 57423
const JSON_UNQUOTE_EXTRACT_OP = 57424
const CREATE = 57425
const ALTER = 57426
const DROP = 57427
const RENAME = 57428
const ANALYZE = 57429
const TABLE = 57430
const INDEX = 57431
const VIEW = 57432
const TO = 57433
const IGNORE = 57434
const IF = 57435
const UNIQUE = 57436
const USING = 57437
const SHOW = 57438
const DESCRIBE = 57439
const EXPLAIN = 57440
const CURRENT_TIMESTAMP = 57441
const DATABASE = 57442
const UNUSED = 57443

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
	"OFFSET",
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
	"INTEGRAL",
	"FLOAT",
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
	"COLLATE",
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
	-1, 111,
	96, 248,
	-2, 247,
}

const yyNprod = 252
const yyPrivate = 57344

var yyTokenNames []string
var yyStates []string

const yyLast = 1059

var yyAct = [...]int{

	123, 320, 214, 371, 217, 440, 117, 340, 239, 295,
	62, 330, 253, 309, 252, 251, 271, 115, 311, 264,
	154, 126, 104, 216, 3, 255, 47, 163, 169, 179,
	35, 74, 37, 118, 67, 64, 38, 105, 69, 41,
	248, 71, 167, 99, 391, 393, 40, 423, 41, 422,
	421, 50, 48, 49, 68, 81, 70, 46, 58, 14,
	15, 16, 17, 171, 43, 44, 45, 42, 373, 199,
	200, 201, 202, 196, 412, 413, 205, 103, 219, 220,
	347, 110, 18, 87, 242, 183, 89, 90, 196, 64,
	205, 205, 64, 63, 92, 157, 96, 445, 98, 152,
	186, 403, 185, 184, 109, 310, 65, 363, 181, 392,
	111, 310, 151, 278, 166, 168, 165, 218, 186, 94,
	160, 84, 221, 222, 223, 224, 65, 276, 277, 275,
	174, 170, 195, 194, 203, 204, 197, 198, 199, 200,
	201, 202, 196, 230, 397, 205, 128, 128, 259, 60,
	64, 237, 19, 20, 22, 21, 23, 274, 86, 184,
	235, 227, 243, 60, 245, 24, 25, 26, 111, 231,
	185, 184, 234, 238, 186, 95, 405, 65, 91, 110,
	82, 181, 60, 83, 261, 156, 186, 265, 267, 268,
	270, 246, 266, 279, 280, 281, 249, 283, 284, 285,
	286, 287, 288, 289, 290, 291, 292, 293, 294, 269,
	258, 256, 75, 282, 250, 348, 349, 350, 213, 215,
	298, 60, 273, 343, 410, 302, 297, 299, 110, 110,
	65, 366, 182, 447, 296, 303, 306, 300, 301, 312,
	314, 91, 400, 261, 176, 296, 317, 319, 304, 307,
	298, 296, 328, 159, 316, 341, 180, 328, 296, 149,
	241, 195, 194, 203, 204, 197, 198, 199, 200, 201,
	202, 196, 14, 346, 205, 243, 128, 351, 128, 353,
	354, 355, 417, 60, 65, 343, 182, 318, 296, 262,
	263, 256, 352, 197, 198, 199, 200, 201, 202, 196,
	155, 357, 205, 28, 312, 244, 73, 102, 110, 273,
	177, 386, 128, 79, 358, 60, 387, 360, 243, 367,
	420, 368, 362, 409, 369, 372, 384, 388, 359, 336,
	337, 385, 114, 88, 419, 14, 91, 383, 382, 150,
	39, 296, 379, 378, 381, 380, 296, 176, 398, 394,
	389, 374, 76, 408, 185, 184, 396, 147, 55, 401,
	233, 399, 256, 256, 256, 256, 404, 114, 114, 402,
	186, 54, 57, 407, 398, 146, 406, 225, 226, 365,
	243, 228, 416, 161, 414, 101, 438, 195, 194, 203,
	204, 197, 198, 199, 200, 201, 202, 196, 439, 345,
	205, 51, 52, 424, 321, 377, 322, 240, 425, 114,
	376, 427, 428, 372, 429, 364, 430, 194, 203, 204,
	197, 198, 199, 200, 201, 202, 196, 64, 327, 205,
	114, 257, 155, 444, 441, 441, 441, 436, 114, 114,
	442, 443, 272, 61, 426, 14, 450, 145, 451, 28,
	446, 452, 448, 449, 30, 144, 1, 158, 332, 335,
	336, 337, 333, 128, 334, 338, 111, 130, 129, 131,
	132, 133, 134, 344, 339, 135, 141, 142, 178, 114,
	114, 415, 140, 162, 36, 247, 164, 315, 66, 143,
	59, 130, 129, 131, 132, 133, 134, 236, 148, 135,
	72, 437, 119, 120, 77, 411, 370, 139, 124, 121,
	375, 257, 122, 326, 361, 59, 229, 308, 125, 59,
	116, 313, 431, 432, 93, 80, 136, 232, 97, 272,
	187, 100, 137, 138, 29, 112, 108, 390, 85, 332,
	335, 336, 337, 333, 59, 334, 338, 153, 331, 418,
	31, 32, 33, 34, 329, 254, 175, 172, 107, 114,
	173, 78, 53, 27, 114, 56, 13, 356, 203, 204,
	197, 198, 199, 200, 201, 202, 196, 12, 305, 205,
	127, 11, 257, 257, 257, 257, 195, 194, 203, 204,
	197, 198, 199, 200, 201, 202, 196, 10, 9, 205,
	8, 59, 7, 6, 128, 5, 296, 111, 130, 129,
	131, 132, 133, 134, 4, 2, 135, 141, 142, 0,
	315, 113, 0, 140, 65, 0, 0, 0, 0, 0,
	114, 0, 0, 0, 108, 59, 0, 0, 0, 0,
	0, 260, 0, 119, 120, 106, 0, 0, 139, 0,
	121, 0, 0, 122, 0, 0, 195, 194, 203, 204,
	197, 198, 199, 200, 201, 202, 196, 136, 0, 205,
	0, 114, 114, 137, 138, 433, 434, 435, 0, 0,
	0, 0, 0, 108, 108, 195, 194, 203, 204, 197,
	198, 199, 200, 201, 202, 196, 0, 127, 205, 0,
	323, 0, 324, 0, 0, 325, 0, 0, 0, 0,
	0, 0, 0, 342, 0, 59, 0, 0, 0, 0,
	0, 128, 0, 296, 111, 130, 129, 131, 132, 133,
	134, 0, 0, 135, 141, 142, 0, 0, 113, 0,
	140, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	119, 120, 106, 108, 0, 139, 0, 121, 0, 0,
	122, 0, 0, 0, 0, 0, 127, 0, 14, 0,
	0, 0, 0, 0, 136, 0, 59, 59, 59, 59,
	137, 138, 0, 0, 127, 0, 0, 0, 0, 342,
	128, 0, 395, 111, 130, 129, 131, 132, 133, 134,
	0, 0, 135, 141, 142, 0, 0, 113, 128, 140,
	0, 111, 130, 129, 131, 132, 133, 134, 0, 0,
	135, 141, 142, 0, 0, 113, 0, 140, 0, 119,
	120, 106, 0, 0, 139, 0, 121, 0, 0, 122,
	0, 0, 0, 0, 0, 0, 0, 119, 120, 0,
	0, 0, 139, 136, 121, 127, 0, 122, 0, 137,
	138, 14, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 136, 0, 0, 0, 0, 0, 137, 138, 128,
	0, 0, 111, 130, 129, 131, 132, 133, 134, 0,
	0, 135, 141, 142, 0, 0, 113, 0, 140, 0,
	0, 128, 0, 0, 111, 130, 129, 131, 132, 133,
	134, 0, 0, 135, 0, 0, 0, 0, 119, 120,
	140, 0, 0, 139, 0, 121, 0, 0, 122, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	119, 120, 136, 0, 0, 139, 0, 121, 137, 138,
	122, 0, 0, 128, 0, 0, 111, 130, 129, 131,
	132, 133, 134, 0, 136, 135, 0, 0, 0, 0,
	137, 138, 140, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 119, 120, 0, 0, 0, 139, 0, 121,
	0, 0, 122, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 136, 189, 192, 0,
	0, 0, 137, 138, 206, 207, 208, 209, 210, 211,
	212, 193, 190, 191, 188, 195, 194, 203, 204, 197,
	198, 199, 200, 201, 202, 196, 0, 0, 205,
}
var yyPact = [...]int{

	53, -1000, -1000, 444, -1000, -1000, -1000, -1000, -1000, -1000,
	-1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, -74,
	-60, -37, -40, -47, -1000, -1000, -1000, 439, 382, 338,
	-1000, -69, 114, 433, 77, -75, -51, 77, -1000, -48,
	77, -1000, 114, -78, 163, -78, 114, -1000, -1000, -1000,
	-1000, -1000, -1000, 277, 128, -1000, 64, 133, 304, -10,
	-1000, 114, 131, -1000, 24, -1000, 114, 56, 126, -1000,
	114, -1000, -64, 114, 363, 262, 77, -1000, 754, -1000,
	437, -1000, 344, 326, -1000, 230, 114, -1000, 77, 114,
	421, 77, 917, -1000, 361, -84, -1000, 14, -1000, 114,
	-1000, -1000, 114, -1000, 300, -1000, -1000, 235, -11, 41,
	964, -1000, -1000, 843, 772, -1000, -19, -1000, -1000, 917,
	917, 917, 917, 232, 232, -1000, -1000, 232, -1000, -1000,
	-1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000,
	917, -1000, -1000, 114, -1000, -1000, -1000, -1000, 329, 77,
	77, -1000, 289, -1000, 393, 843, -1000, 604, -12, 865,
	-1000, -1000, 260, 77, -1000, -67, -1000, -1000, -1000, -1000,
	-1000, -1000, -1000, -1000, -1000, 421, 754, 100, -1000, -1000,
	181, -1000, -1000, 61, 843, 843, 129, 417, 101, 49,
	917, 917, 917, 129, 917, 917, 917, 917, 917, 917,
	917, 917, 917, 917, 917, 917, -1000, -1000, -1000, -1000,
	-1000, -1000, -1000, 23, 964, 293, 298, 203, 964, 441,
	441, -4, -4, -4, 575, 558, 675, -1000, 439, 45,
	604, -1000, 259, 232, 444, 194, 240, -1000, 393, 388,
	391, 41, 119, 604, 114, -1000, -1000, 114, -1000, 416,
	-1000, 205, 423, -1000, -1000, 234, 378, 266, -1000, -1000,
	-16, -1000, 23, 97, -1000, -1000, 157, -1000, -1000, -1000,
	604, -1000, 865, -1000, -1000, 101, 917, 917, 917, 604,
	604, 505, -1000, 485, 335, -4, -18, -18, -3, -3,
	-3, -3, 208, 208, -1000, -1000, -1000, -1000, 917, -1000,
	-1000, -1000, -1000, -1000, 197, 754, -1000, 197, 39, -1000,
	843, -1000, 353, 184, -1000, 917, -1000, -1000, 77, 388,
	-1000, 917, 917, -28, -1000, -1000, 397, 390, 100, 100,
	100, 100, -1000, 303, 302, -1000, 291, 276, 292, 1,
	-1000, 172, -1000, -1000, 114, -1000, 210, 57, -1000, -1000,
	-1000, 203, -1000, 604, 604, 180, 917, 604, -1000, 197,
	-1000, 32, -1000, 917, 109, 349, 232, -1000, -1000, 306,
	177, -1000, 51, 77, -1000, 393, 843, 917, 423, 237,
	504, -1000, -1000, -1000, -1000, 299, -1000, 285, -1000, -1000,
	-1000, -55, -56, -58, -1000, -1000, -1000, -1000, -1000, -1000,
	917, 604, -1000, -1000, 604, 917, 436, -1000, 917, 917,
	917, -1000, -1000, -1000, 388, 41, 173, 843, 843, -1000,
	-1000, 232, 232, 232, 604, 604, 77, 604, 604, -1000,
	368, 41, 41, 77, 77, 77, 131, -1000, 425, 17,
	186, -1000, 186, 186, -1000, 77, -1000, 77, -1000, -1000,
	77, -1000, -1000,
}
var yyPgo = [...]int{

	0, 615, 23, 614, 605, 603, 602, 600, 598, 597,
	581, 577, 566, 534, 565, 563, 562, 561, 22, 37,
	558, 556, 15, 14, 12, 555, 554, 11, 548, 25,
	538, 537, 5, 20, 104, 535, 21, 530, 527, 17,
	2, 525, 19, 16, 4, 521, 6, 33, 520, 518,
	517, 13, 516, 514, 513, 510, 508, 8, 506, 3,
	505, 1, 501, 498, 497, 18, 10, 93, 489, 340,
	306, 488, 486, 485, 484, 483, 0, 29, 478, 457,
	7, 474, 473, 26, 456, 454, 253, 9,
}
var yyR1 = [...]int{

	0, 84, 1, 1, 1, 1, 1, 1, 1, 1,
	1, 1, 1, 2, 2, 2, 3, 3, 4, 5,
	6, 7, 7, 7, 8, 8, 8, 9, 10, 10,
	10, 11, 12, 12, 12, 85, 13, 14, 14, 15,
	15, 15, 16, 16, 17, 17, 18, 18, 19, 19,
	19, 19, 20, 20, 78, 78, 78, 77, 77, 21,
	21, 22, 22, 23, 23, 24, 24, 24, 25, 25,
	25, 25, 82, 82, 81, 81, 81, 80, 80, 26,
	26, 26, 26, 27, 27, 27, 27, 28, 28, 30,
	30, 29, 29, 31, 31, 31, 31, 32, 32, 33,
	33, 34, 34, 34, 34, 34, 34, 36, 36, 35,
	35, 35, 35, 35, 35, 35, 35, 35, 35, 35,
	35, 35, 42, 42, 42, 42, 42, 42, 37, 37,
	37, 37, 37, 37, 37, 43, 43, 43, 47, 44,
	44, 40, 40, 40, 40, 40, 40, 40, 40, 40,
	40, 40, 40, 40, 40, 40, 40, 40, 40, 40,
	40, 40, 40, 40, 40, 40, 40, 40, 40, 56,
	56, 56, 56, 49, 52, 52, 50, 50, 51, 53,
	53, 48, 48, 48, 39, 39, 39, 39, 39, 39,
	39, 41, 41, 41, 54, 54, 55, 55, 57, 57,
	58, 58, 59, 60, 60, 60, 61, 61, 61, 61,
	62, 62, 62, 63, 63, 64, 64, 65, 65, 38,
	38, 45, 45, 46, 66, 66, 67, 68, 68, 70,
	70, 71, 71, 69, 69, 72, 72, 72, 72, 72,
	72, 73, 73, 74, 74, 75, 75, 76, 79, 86,
	87, 83,
}
var yyR2 = [...]int{

	0, 1, 1, 1, 1, 1, 1, 1, 1, 1,
	1, 1, 1, 12, 6, 3, 7, 7, 8, 7,
	3, 5, 8, 4, 6, 7, 4, 5, 4, 5,
	5, 3, 2, 2, 2, 0, 2, 0, 2, 1,
	2, 2, 0, 1, 0, 1, 1, 3, 1, 2,
	3, 5, 1, 1, 0, 1, 2, 1, 1, 0,
	2, 1, 3, 1, 1, 3, 3, 3, 3, 5,
	5, 3, 0, 1, 0, 1, 2, 1, 1, 1,
	2, 2, 1, 2, 3, 2, 3, 2, 2, 2,
	1, 1, 3, 0, 5, 5, 5, 1, 3, 0,
	2, 1, 3, 3, 2, 3, 3, 1, 1, 1,
	3, 3, 3, 4, 3, 4, 3, 4, 5, 6,
	3, 2, 1, 2, 1, 2, 1, 2, 1, 1,
	1, 1, 1, 1, 1, 3, 1, 1, 3, 1,
	3, 1, 1, 1, 1, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 2,
	2, 2, 3, 3, 4, 5, 3, 4, 1, 1,
	1, 1, 1, 5, 0, 1, 1, 2, 4, 0,
	2, 1, 3, 5, 1, 1, 1, 1, 1, 1,
	1, 1, 2, 2, 0, 3, 0, 2, 0, 3,
	1, 3, 2, 0, 1, 1, 0, 2, 4, 4,
	0, 2, 4, 0, 3, 1, 3, 0, 5, 2,
	1, 1, 3, 3, 1, 3, 3, 1, 1, 0,
	2, 0, 3, 0, 1, 1, 1, 1, 1, 1,
	1, 0, 1, 0, 1, 0, 2, 1, 1, 1,
	1, 0,
}
var yyChk = [...]int{

	-1000, -84, -1, -2, -3, -4, -5, -6, -7, -8,
	-9, -10, -11, -12, 6, 7, 8, 9, 29, 99,
	100, 102, 101, 103, 112, 113, 114, -15, 5, -13,
	-85, -13, -13, -13, -13, 104, -74, 106, 110, -69,
	106, 108, 104, 104, 105, 106, 104, -83, -83, -83,
	-2, 19, 20, -16, 33, 20, -14, -69, -29, -79,
	49, 10, -66, -67, -76, 49, -71, 109, 105, -76,
	104, -76, -79, -70, 109, 49, -70, -79, -17, 36,
	-41, -76, 52, 55, 57, -30, 25, -29, 29, 96,
	-29, 47, 70, -79, 63, 49, -83, -79, -83, 107,
	-79, 22, 45, -76, -18, -19, 87, -20, -79, -34,
	-40, 49, -35, 63, -86, -39, -48, -46, -47, 85,
	86, 92, 95, -76, -56, -49, -36, 22, 46, 51,
	50, 52, 53, 54, 55, 58, 109, 115, 116, 90,
	65, 59, 60, -68, 18, 10, 31, 31, -63, 29,
	-86, -29, -66, -79, -33, 11, -67, -40, -79, -86,
	-83, 22, -75, 111, -72, 102, 100, 28, 101, 14,
	117, 49, -79, -79, -83, -21, 47, 10, -78, -77,
	21, -76, 51, 96, 62, 61, 77, -37, 80, 63,
	78, 79, 64, 77, 82, 81, 91, 85, 86, 87,
	88, 89, 90, 83, 84, 94, 70, 71, 72, 73,
	74, 75, 76, -34, -40, -34, -2, -44, -40, 97,
	98, -40, -40, -40, -40, -86, -86, -47, -86, -52,
	-40, -29, -38, 31, -2, -66, -64, -76, -33, -57,
	14, -34, 96, -40, 45, -76, -83, -73, 107, -33,
	-19, -22, -23, -24, -25, -29, -47, -86, -77, 87,
	-79, -76, -34, -34, -42, 58, 63, 59, 60, -36,
	-40, -43, -86, -47, 56, 80, 78, 79, 64, -40,
	-40, -40, -42, -40, -40, -40, -40, -40, -40, -40,
	-40, -40, -40, -40, -40, -87, 48, -87, 47, -87,
	-39, -39, -76, -87, -18, 20, -87, -18, -50, -51,
	66, -65, 45, -45, -46, -86, -65, -87, 47, -57,
	-61, 16, 15, -79, -79, -79, -54, 12, 47, -26,
	-27, -28, 35, 39, 41, 36, 37, 38, 42, -81,
	-80, 21, -79, 51, -82, 21, -22, 96, 58, 59,
	60, -44, -43, -40, -40, -40, 62, -40, -87, -18,
	-87, -53, -51, 68, -34, 26, 47, -76, -61, -40,
	-58, -59, -40, 96, -83, -55, 13, 15, -23, -24,
	-23, -24, 35, 35, 35, 40, 35, 40, 35, -27,
	-31, 43, 108, 44, -80, -79, -87, 87, -76, -87,
	62, -40, -87, 69, -40, 67, 27, -46, 47, 17,
	47, -60, 23, 24, -57, -34, -44, 45, 45, 35,
	35, 105, 105, 105, -40, -40, 8, -40, -40, -59,
	-61, -34, -34, -86, -86, -86, -66, -62, 18, 30,
	-32, -76, -32, -32, 8, 80, -87, 47, -87, -87,
	-76, -76, -76,
}
var yyDef = [...]int{

	0, -2, 1, 2, 3, 4, 5, 6, 7, 8,
	9, 10, 11, 12, 35, 35, 35, 35, 35, 243,
	233, 0, 0, 0, 251, 251, 251, 0, 39, 42,
	37, 233, 0, 0, 0, 231, 0, 0, 244, 0,
	0, 234, 0, 229, 0, 229, 0, 32, 33, 34,
	15, 40, 41, 44, 0, 43, 36, 0, 0, 91,
	248, 0, 20, 224, 0, 247, 0, 0, 0, 251,
	0, 251, 0, 0, 0, 0, 0, 31, 0, 45,
	0, 191, 0, 0, 38, 213, 0, 90, 0, 0,
	99, 0, 0, 251, 0, 245, 23, 0, 26, 0,
	28, 230, 0, 251, 59, 46, 48, 54, 0, 52,
	53, -2, 101, 0, 0, 141, 142, 143, 144, 0,
	0, 0, 0, 181, 0, 168, 109, 0, 249, 184,
	185, 186, 187, 188, 189, 190, 169, 170, 171, 172,
	174, 107, 108, 0, 227, 228, 192, 193, 0, 0,
	0, 89, 99, 92, 198, 0, 225, 226, 0, 0,
	21, 232, 0, 0, 251, 241, 235, 236, 237, 238,
	239, 240, 27, 29, 30, 99, 0, 0, 49, 55,
	0, 57, 58, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 128, 129, 130, 131,
	132, 133, 134, 104, 0, 0, 0, 0, 139, 0,
	0, 159, 160, 161, 0, 0, 0, 121, 0, 0,
	175, 14, 217, 0, 220, 217, 0, 215, 198, 206,
	0, 100, 0, 139, 0, 246, 24, 0, 242, 194,
	47, 60, 61, 63, 64, 74, 72, 0, 56, 50,
	0, 182, 102, 103, 106, 122, 0, 124, 126, 110,
	111, 112, 0, 136, 137, 0, 0, 0, 0, 114,
	116, 0, 120, 145, 146, 147, 148, 149, 150, 151,
	152, 153, 154, 155, 158, 105, 250, 138, 0, 223,
	156, 157, 162, 163, 0, 0, 166, 0, 179, 176,
	0, 16, 0, 219, 221, 0, 17, 214, 0, 206,
	19, 0, 0, 0, 251, 25, 196, 0, 0, 0,
	0, 0, 79, 0, 0, 82, 0, 0, 0, 93,
	75, 0, 77, 78, 0, 73, 0, 0, 123, 125,
	127, 0, 113, 115, 117, 0, 0, 140, 164, 0,
	167, 0, 177, 0, 0, 0, 0, 216, 18, 207,
	199, 200, 203, 0, 22, 198, 0, 0, 62, 68,
	0, 71, 80, 81, 83, 0, 85, 0, 87, 88,
	65, 0, 0, 0, 76, 66, 67, 51, 183, 135,
	0, 118, 165, 173, 180, 0, 0, 222, 0, 0,
	0, 202, 204, 205, 206, 197, 195, 0, 0, 84,
	86, 0, 0, 0, 119, 178, 0, 208, 209, 201,
	210, 69, 70, 0, 0, 0, 218, 13, 0, 0,
	0, 97, 0, 0, 211, 0, 94, 0, 95, 96,
	0, 98, 212,
}
var yyTok1 = [...]int{

	1, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 89, 82, 3,
	46, 48, 87, 85, 47, 86, 96, 88, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	71, 70, 72, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 91, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 81, 3, 92,
}
var yyTok2 = [...]int{

	2, 3, 4, 5, 6, 7, 8, 9, 10, 11,
	12, 13, 14, 15, 16, 17, 18, 19, 20, 21,
	22, 23, 24, 25, 26, 27, 28, 29, 30, 31,
	32, 33, 34, 35, 36, 37, 38, 39, 40, 41,
	42, 43, 44, 45, 49, 50, 51, 52, 53, 54,
	55, 56, 57, 58, 59, 60, 61, 62, 63, 64,
	65, 66, 67, 68, 69, 73, 74, 75, 76, 77,
	78, 79, 80, 83, 84, 90, 93, 94, 95, 97,
	98, 99, 100, 101, 102, 103, 104, 105, 106, 107,
	108, 109, 110, 111, 112, 113, 114, 115, 116, 117,
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
		//line sql.y:182
		{
			setParseTree(yylex, yyDollar[1].statement)
		}
	case 2:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:188
		{
			yyVAL.statement = yyDollar[1].selStmt
		}
	case 13:
		yyDollar = yyS[yypt-12 : yypt+1]
		//line sql.y:204
		{
			yyVAL.selStmt = &Select{Comments: Comments(yyDollar[2].bytes2), Distinct: yyDollar[3].str, Hints: yyDollar[4].str, SelectExprs: yyDollar[5].selectExprs, From: yyDollar[6].tableExprs, Where: NewWhere(WhereStr, yyDollar[7].boolExpr), GroupBy: GroupBy(yyDollar[8].valExprs), Having: NewWhere(HavingStr, yyDollar[9].boolExpr), OrderBy: yyDollar[10].orderBy, Limit: yyDollar[11].limit, Lock: yyDollar[12].str}
		}
	case 14:
		yyDollar = yyS[yypt-6 : yypt+1]
		//line sql.y:208
		{
			yyVAL.selStmt = &Select{Comments: Comments(yyDollar[2].bytes2), SelectExprs: SelectExprs{Nextval{Expr: yyDollar[4].valExpr}}, From: TableExprs{&AliasedTableExpr{Expr: yyDollar[6].tableName}}}
		}
	case 15:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:212
		{
			yyVAL.selStmt = &Union{Type: yyDollar[2].str, Left: yyDollar[1].selStmt, Right: yyDollar[3].selStmt}
		}
	case 16:
		yyDollar = yyS[yypt-7 : yypt+1]
		//line sql.y:218
		{
			yyVAL.statement = &Insert{Comments: Comments(yyDollar[2].bytes2), Ignore: yyDollar[3].str, Table: yyDollar[4].tableName, Columns: yyDollar[5].columns, Rows: yyDollar[6].insRows, OnDup: OnDup(yyDollar[7].updateExprs)}
		}
	case 17:
		yyDollar = yyS[yypt-7 : yypt+1]
		//line sql.y:222
		{
			cols := make(Columns, 0, len(yyDollar[6].updateExprs))
			vals := make(ValTuple, 0, len(yyDollar[7].updateExprs))
			for _, updateList := range yyDollar[6].updateExprs {
				cols = append(cols, updateList.Name)
				vals = append(vals, updateList.Expr)
			}
			yyVAL.statement = &Insert{Comments: Comments(yyDollar[2].bytes2), Ignore: yyDollar[3].str, Table: yyDollar[4].tableName, Columns: cols, Rows: Values{vals}, OnDup: OnDup(yyDollar[7].updateExprs)}
		}
	case 18:
		yyDollar = yyS[yypt-8 : yypt+1]
		//line sql.y:234
		{
			yyVAL.statement = &Update{Comments: Comments(yyDollar[2].bytes2), Table: yyDollar[3].tableName, Exprs: yyDollar[5].updateExprs, Where: NewWhere(WhereStr, yyDollar[6].boolExpr), OrderBy: yyDollar[7].orderBy, Limit: yyDollar[8].limit}
		}
	case 19:
		yyDollar = yyS[yypt-7 : yypt+1]
		//line sql.y:240
		{
			yyVAL.statement = &Delete{Comments: Comments(yyDollar[2].bytes2), Table: yyDollar[4].tableName, Where: NewWhere(WhereStr, yyDollar[5].boolExpr), OrderBy: yyDollar[6].orderBy, Limit: yyDollar[7].limit}
		}
	case 20:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:246
		{
			yyVAL.statement = &Set{Comments: Comments(yyDollar[2].bytes2), Exprs: yyDollar[3].updateExprs}
		}
	case 21:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:252
		{
			yyVAL.statement = &DDL{Action: CreateStr, NewName: yyDollar[4].tableIdent}
		}
	case 22:
		yyDollar = yyS[yypt-8 : yypt+1]
		//line sql.y:256
		{
			// Change this to an alter statement
			yyVAL.statement = &DDL{Action: AlterStr, Table: yyDollar[7].tableIdent, NewName: yyDollar[7].tableIdent}
		}
	case 23:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:261
		{
			yyVAL.statement = &DDL{Action: CreateStr, NewName: NewTableIdent(yyDollar[3].colIdent.Lowered())}
		}
	case 24:
		yyDollar = yyS[yypt-6 : yypt+1]
		//line sql.y:267
		{
			yyVAL.statement = &DDL{Action: AlterStr, Table: yyDollar[4].tableIdent, NewName: yyDollar[4].tableIdent}
		}
	case 25:
		yyDollar = yyS[yypt-7 : yypt+1]
		//line sql.y:271
		{
			// Change this to a rename statement
			yyVAL.statement = &DDL{Action: RenameStr, Table: yyDollar[4].tableIdent, NewName: yyDollar[7].tableIdent}
		}
	case 26:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:276
		{
			yyVAL.statement = &DDL{Action: AlterStr, Table: NewTableIdent(yyDollar[3].colIdent.Lowered()), NewName: NewTableIdent(yyDollar[3].colIdent.Lowered())}
		}
	case 27:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:282
		{
			yyVAL.statement = &DDL{Action: RenameStr, Table: yyDollar[3].tableIdent, NewName: yyDollar[5].tableIdent}
		}
	case 28:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:288
		{
			var exists bool
			if yyDollar[3].byt != 0 {
				exists = true
			}
			yyVAL.statement = &DDL{Action: DropStr, Table: yyDollar[4].tableIdent, IfExists: exists}
		}
	case 29:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:296
		{
			// Change this to an alter statement
			yyVAL.statement = &DDL{Action: AlterStr, Table: yyDollar[5].tableIdent, NewName: yyDollar[5].tableIdent}
		}
	case 30:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:301
		{
			var exists bool
			if yyDollar[3].byt != 0 {
				exists = true
			}
			yyVAL.statement = &DDL{Action: DropStr, Table: NewTableIdent(yyDollar[4].colIdent.Lowered()), IfExists: exists}
		}
	case 31:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:311
		{
			yyVAL.statement = &DDL{Action: AlterStr, Table: yyDollar[3].tableIdent, NewName: yyDollar[3].tableIdent}
		}
	case 32:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:317
		{
			yyVAL.statement = &Other{}
		}
	case 33:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:321
		{
			yyVAL.statement = &Other{}
		}
	case 34:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:325
		{
			yyVAL.statement = &Other{}
		}
	case 35:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:330
		{
			setAllowComments(yylex, true)
		}
	case 36:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:334
		{
			yyVAL.bytes2 = yyDollar[2].bytes2
			setAllowComments(yylex, false)
		}
	case 37:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:340
		{
			yyVAL.bytes2 = nil
		}
	case 38:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:344
		{
			yyVAL.bytes2 = append(yyDollar[1].bytes2, yyDollar[2].bytes)
		}
	case 39:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:350
		{
			yyVAL.str = UnionStr
		}
	case 40:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:354
		{
			yyVAL.str = UnionAllStr
		}
	case 41:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:358
		{
			yyVAL.str = UnionDistinctStr
		}
	case 42:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:363
		{
			yyVAL.str = ""
		}
	case 43:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:367
		{
			yyVAL.str = DistinctStr
		}
	case 44:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:372
		{
			yyVAL.str = ""
		}
	case 45:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:376
		{
			yyVAL.str = StraightJoinHint
		}
	case 46:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:382
		{
			yyVAL.selectExprs = SelectExprs{yyDollar[1].selectExpr}
		}
	case 47:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:386
		{
			yyVAL.selectExprs = append(yyVAL.selectExprs, yyDollar[3].selectExpr)
		}
	case 48:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:392
		{
			yyVAL.selectExpr = &StarExpr{}
		}
	case 49:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:396
		{
			yyVAL.selectExpr = &NonStarExpr{Expr: yyDollar[1].expr, As: yyDollar[2].colIdent}
		}
	case 50:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:400
		{
			yyVAL.selectExpr = &StarExpr{TableName: &TableName{Name: yyDollar[1].tableIdent}}
		}
	case 51:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:404
		{
			yyVAL.selectExpr = &StarExpr{TableName: &TableName{Qualifier: yyDollar[1].tableIdent, Name: yyDollar[3].tableIdent}}
		}
	case 52:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:410
		{
			yyVAL.expr = yyDollar[1].boolExpr
		}
	case 53:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:414
		{
			yyVAL.expr = yyDollar[1].valExpr
		}
	case 54:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:419
		{
			yyVAL.colIdent = ColIdent{}
		}
	case 55:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:423
		{
			yyVAL.colIdent = yyDollar[1].colIdent
		}
	case 56:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:427
		{
			yyVAL.colIdent = yyDollar[2].colIdent
		}
	case 58:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:434
		{
			yyVAL.colIdent = NewColIdent(string(yyDollar[1].bytes))
		}
	case 59:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:439
		{
			yyVAL.tableExprs = TableExprs{&AliasedTableExpr{Expr: &TableName{Name: NewTableIdent("dual")}}}
		}
	case 60:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:443
		{
			yyVAL.tableExprs = yyDollar[2].tableExprs
		}
	case 61:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:449
		{
			yyVAL.tableExprs = TableExprs{yyDollar[1].tableExpr}
		}
	case 62:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:453
		{
			yyVAL.tableExprs = append(yyVAL.tableExprs, yyDollar[3].tableExpr)
		}
	case 65:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:463
		{
			yyVAL.tableExpr = &AliasedTableExpr{Expr: yyDollar[1].tableName, As: yyDollar[2].tableIdent, Hints: yyDollar[3].indexHints}
		}
	case 66:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:467
		{
			yyVAL.tableExpr = &AliasedTableExpr{Expr: yyDollar[1].subquery, As: yyDollar[3].tableIdent}
		}
	case 67:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:471
		{
			yyVAL.tableExpr = &ParenTableExpr{Exprs: yyDollar[2].tableExprs}
		}
	case 68:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:484
		{
			yyVAL.tableExpr = &JoinTableExpr{LeftExpr: yyDollar[1].tableExpr, Join: yyDollar[2].str, RightExpr: yyDollar[3].tableExpr}
		}
	case 69:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:488
		{
			yyVAL.tableExpr = &JoinTableExpr{LeftExpr: yyDollar[1].tableExpr, Join: yyDollar[2].str, RightExpr: yyDollar[3].tableExpr, On: yyDollar[5].boolExpr}
		}
	case 70:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:492
		{
			yyVAL.tableExpr = &JoinTableExpr{LeftExpr: yyDollar[1].tableExpr, Join: yyDollar[2].str, RightExpr: yyDollar[3].tableExpr, On: yyDollar[5].boolExpr}
		}
	case 71:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:496
		{
			yyVAL.tableExpr = &JoinTableExpr{LeftExpr: yyDollar[1].tableExpr, Join: yyDollar[2].str, RightExpr: yyDollar[3].tableExpr}
		}
	case 72:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:501
		{
			yyVAL.empty = struct{}{}
		}
	case 73:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:503
		{
			yyVAL.empty = struct{}{}
		}
	case 74:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:506
		{
			yyVAL.tableIdent = NewTableIdent("")
		}
	case 75:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:510
		{
			yyVAL.tableIdent = yyDollar[1].tableIdent
		}
	case 76:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:514
		{
			yyVAL.tableIdent = yyDollar[2].tableIdent
		}
	case 78:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:521
		{
			yyVAL.tableIdent = NewTableIdent(string(yyDollar[1].bytes))
		}
	case 79:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:527
		{
			yyVAL.str = JoinStr
		}
	case 80:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:531
		{
			yyVAL.str = JoinStr
		}
	case 81:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:535
		{
			yyVAL.str = JoinStr
		}
	case 82:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:539
		{
			yyVAL.str = StraightJoinStr
		}
	case 83:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:545
		{
			yyVAL.str = LeftJoinStr
		}
	case 84:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:549
		{
			yyVAL.str = LeftJoinStr
		}
	case 85:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:553
		{
			yyVAL.str = RightJoinStr
		}
	case 86:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:557
		{
			yyVAL.str = RightJoinStr
		}
	case 87:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:563
		{
			yyVAL.str = NaturalJoinStr
		}
	case 88:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:567
		{
			if yyDollar[2].str == LeftJoinStr {
				yyVAL.str = NaturalLeftJoinStr
			} else {
				yyVAL.str = NaturalRightJoinStr
			}
		}
	case 89:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:577
		{
			yyVAL.tableName = yyDollar[2].tableName
		}
	case 90:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:581
		{
			yyVAL.tableName = yyDollar[1].tableName
		}
	case 91:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:587
		{
			yyVAL.tableName = &TableName{Name: yyDollar[1].tableIdent}
		}
	case 92:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:591
		{
			yyVAL.tableName = &TableName{Qualifier: yyDollar[1].tableIdent, Name: yyDollar[3].tableIdent}
		}
	case 93:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:596
		{
			yyVAL.indexHints = nil
		}
	case 94:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:600
		{
			yyVAL.indexHints = &IndexHints{Type: UseStr, Indexes: yyDollar[4].colIdents}
		}
	case 95:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:604
		{
			yyVAL.indexHints = &IndexHints{Type: IgnoreStr, Indexes: yyDollar[4].colIdents}
		}
	case 96:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:608
		{
			yyVAL.indexHints = &IndexHints{Type: ForceStr, Indexes: yyDollar[4].colIdents}
		}
	case 97:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:614
		{
			yyVAL.colIdents = []ColIdent{yyDollar[1].colIdent}
		}
	case 98:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:618
		{
			yyVAL.colIdents = append(yyDollar[1].colIdents, yyDollar[3].colIdent)
		}
	case 99:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:623
		{
			yyVAL.boolExpr = nil
		}
	case 100:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:627
		{
			yyVAL.boolExpr = yyDollar[2].boolExpr
		}
	case 102:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:634
		{
			yyVAL.boolExpr = &AndExpr{Left: yyDollar[1].boolExpr, Right: yyDollar[3].boolExpr}
		}
	case 103:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:638
		{
			yyVAL.boolExpr = &OrExpr{Left: yyDollar[1].boolExpr, Right: yyDollar[3].boolExpr}
		}
	case 104:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:642
		{
			yyVAL.boolExpr = &NotExpr{Expr: yyDollar[2].boolExpr}
		}
	case 105:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:646
		{
			yyVAL.boolExpr = &ParenBoolExpr{Expr: yyDollar[2].boolExpr}
		}
	case 106:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:650
		{
			yyVAL.boolExpr = &IsExpr{Operator: yyDollar[3].str, Expr: yyDollar[1].boolExpr}
		}
	case 107:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:656
		{
			yyVAL.boolVal = BoolVal(true)
		}
	case 108:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:660
		{
			yyVAL.boolVal = BoolVal(false)
		}
	case 109:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:666
		{
			yyVAL.boolExpr = yyDollar[1].boolVal
		}
	case 110:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:670
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: yyDollar[2].str, Right: yyDollar[3].boolVal}
		}
	case 111:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:674
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: yyDollar[2].str, Right: yyDollar[3].valExpr}
		}
	case 112:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:678
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: InStr, Right: yyDollar[3].colTuple}
		}
	case 113:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:682
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: NotInStr, Right: yyDollar[4].colTuple}
		}
	case 114:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:686
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: LikeStr, Right: yyDollar[3].valExpr}
		}
	case 115:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:690
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: NotLikeStr, Right: yyDollar[4].valExpr}
		}
	case 116:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:694
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: RegexpStr, Right: yyDollar[3].valExpr}
		}
	case 117:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:698
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: NotRegexpStr, Right: yyDollar[4].valExpr}
		}
	case 118:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:702
		{
			yyVAL.boolExpr = &RangeCond{Left: yyDollar[1].valExpr, Operator: BetweenStr, From: yyDollar[3].valExpr, To: yyDollar[5].valExpr}
		}
	case 119:
		yyDollar = yyS[yypt-6 : yypt+1]
		//line sql.y:706
		{
			yyVAL.boolExpr = &RangeCond{Left: yyDollar[1].valExpr, Operator: NotBetweenStr, From: yyDollar[4].valExpr, To: yyDollar[6].valExpr}
		}
	case 120:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:710
		{
			yyVAL.boolExpr = &IsExpr{Operator: yyDollar[3].str, Expr: yyDollar[1].valExpr}
		}
	case 121:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:714
		{
			yyVAL.boolExpr = &ExistsExpr{Subquery: yyDollar[2].subquery}
		}
	case 122:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:720
		{
			yyVAL.str = IsNullStr
		}
	case 123:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:724
		{
			yyVAL.str = IsNotNullStr
		}
	case 124:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:728
		{
			yyVAL.str = IsTrueStr
		}
	case 125:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:732
		{
			yyVAL.str = IsNotTrueStr
		}
	case 126:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:736
		{
			yyVAL.str = IsFalseStr
		}
	case 127:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:740
		{
			yyVAL.str = IsNotFalseStr
		}
	case 128:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:746
		{
			yyVAL.str = EqualStr
		}
	case 129:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:750
		{
			yyVAL.str = LessThanStr
		}
	case 130:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:754
		{
			yyVAL.str = GreaterThanStr
		}
	case 131:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:758
		{
			yyVAL.str = LessEqualStr
		}
	case 132:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:762
		{
			yyVAL.str = GreaterEqualStr
		}
	case 133:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:766
		{
			yyVAL.str = NotEqualStr
		}
	case 134:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:770
		{
			yyVAL.str = NullSafeEqualStr
		}
	case 135:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:776
		{
			yyVAL.colTuple = ValTuple(yyDollar[2].valExprs)
		}
	case 136:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:780
		{
			yyVAL.colTuple = yyDollar[1].subquery
		}
	case 137:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:784
		{
			yyVAL.colTuple = ListArg(yyDollar[1].bytes)
		}
	case 138:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:790
		{
			yyVAL.subquery = &Subquery{yyDollar[2].selStmt}
		}
	case 139:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:796
		{
			yyVAL.valExprs = ValExprs{yyDollar[1].valExpr}
		}
	case 140:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:800
		{
			yyVAL.valExprs = append(yyDollar[1].valExprs, yyDollar[3].valExpr)
		}
	case 141:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:806
		{
			yyVAL.valExpr = yyDollar[1].valExpr
		}
	case 142:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:810
		{
			yyVAL.valExpr = yyDollar[1].colName
		}
	case 143:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:814
		{
			yyVAL.valExpr = yyDollar[1].valTuple
		}
	case 144:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:818
		{
			yyVAL.valExpr = yyDollar[1].subquery
		}
	case 145:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:822
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: BitAndStr, Right: yyDollar[3].valExpr}
		}
	case 146:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:826
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: BitOrStr, Right: yyDollar[3].valExpr}
		}
	case 147:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:830
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: BitXorStr, Right: yyDollar[3].valExpr}
		}
	case 148:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:834
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: PlusStr, Right: yyDollar[3].valExpr}
		}
	case 149:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:838
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: MinusStr, Right: yyDollar[3].valExpr}
		}
	case 150:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:842
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: MultStr, Right: yyDollar[3].valExpr}
		}
	case 151:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:846
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: DivStr, Right: yyDollar[3].valExpr}
		}
	case 152:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:850
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: ModStr, Right: yyDollar[3].valExpr}
		}
	case 153:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:854
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: ModStr, Right: yyDollar[3].valExpr}
		}
	case 154:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:858
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: ShiftLeftStr, Right: yyDollar[3].valExpr}
		}
	case 155:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:862
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: ShiftRightStr, Right: yyDollar[3].valExpr}
		}
	case 156:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:866
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].colName, Operator: JSONExtractOp, Right: yyDollar[3].valExpr}
		}
	case 157:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:870
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].colName, Operator: JSONUnquoteExtractOp, Right: yyDollar[3].valExpr}
		}
	case 158:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:874
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: CollateStr, Right: yyDollar[3].valExpr}
		}
	case 159:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:878
		{
			if num, ok := yyDollar[2].valExpr.(*SQLVal); ok && num.Type == IntVal {
				yyVAL.valExpr = num
			} else {
				yyVAL.valExpr = &UnaryExpr{Operator: UPlusStr, Expr: yyDollar[2].valExpr}
			}
		}
	case 160:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:886
		{
			if num, ok := yyDollar[2].valExpr.(*SQLVal); ok && num.Type == IntVal {
				// Handle double negative
				if num.Val[0] == '-' {
					num.Val = num.Val[1:]
					yyVAL.valExpr = num
				} else {
					yyVAL.valExpr = NewIntVal(append([]byte("-"), num.Val...))
				}
			} else {
				yyVAL.valExpr = &UnaryExpr{Operator: UMinusStr, Expr: yyDollar[2].valExpr}
			}
		}
	case 161:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:900
		{
			yyVAL.valExpr = &UnaryExpr{Operator: TildaStr, Expr: yyDollar[2].valExpr}
		}
	case 162:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:904
		{
			// This rule prevents the usage of INTERVAL
			// as a function. If support is needed for that,
			// we'll need to revisit this. The solution
			// will be non-trivial because of grammar conflicts.
			yyVAL.valExpr = &IntervalExpr{Expr: yyDollar[2].valExpr, Unit: yyDollar[3].colIdent}
		}
	case 163:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:912
		{
			yyVAL.valExpr = &FuncExpr{Name: yyDollar[1].colIdent}
		}
	case 164:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:916
		{
			yyVAL.valExpr = &FuncExpr{Name: yyDollar[1].colIdent, Exprs: yyDollar[3].selectExprs}
		}
	case 165:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:920
		{
			yyVAL.valExpr = &FuncExpr{Name: yyDollar[1].colIdent, Distinct: true, Exprs: yyDollar[4].selectExprs}
		}
	case 166:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:924
		{
			yyVAL.valExpr = &FuncExpr{Name: yyDollar[1].colIdent}
		}
	case 167:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:928
		{
			yyVAL.valExpr = &FuncExpr{Name: yyDollar[1].colIdent, Exprs: yyDollar[3].selectExprs}
		}
	case 168:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:932
		{
			yyVAL.valExpr = yyDollar[1].caseExpr
		}
	case 169:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:938
		{
			yyVAL.colIdent = NewColIdent("if")
		}
	case 170:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:942
		{
			yyVAL.colIdent = NewColIdent("current_timestamp")
		}
	case 171:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:946
		{
			yyVAL.colIdent = NewColIdent("database")
		}
	case 172:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:950
		{
			yyVAL.colIdent = NewColIdent("mod")
		}
	case 173:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:956
		{
			yyVAL.caseExpr = &CaseExpr{Expr: yyDollar[2].valExpr, Whens: yyDollar[3].whens, Else: yyDollar[4].valExpr}
		}
	case 174:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:961
		{
			yyVAL.valExpr = nil
		}
	case 175:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:965
		{
			yyVAL.valExpr = yyDollar[1].valExpr
		}
	case 176:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:971
		{
			yyVAL.whens = []*When{yyDollar[1].when}
		}
	case 177:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:975
		{
			yyVAL.whens = append(yyDollar[1].whens, yyDollar[2].when)
		}
	case 178:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:981
		{
			yyVAL.when = &When{Cond: yyDollar[2].boolExpr, Val: yyDollar[4].valExpr}
		}
	case 179:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:986
		{
			yyVAL.valExpr = nil
		}
	case 180:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:990
		{
			yyVAL.valExpr = yyDollar[2].valExpr
		}
	case 181:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:996
		{
			yyVAL.colName = &ColName{Name: yyDollar[1].colIdent}
		}
	case 182:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1000
		{
			yyVAL.colName = &ColName{Qualifier: &TableName{Name: yyDollar[1].tableIdent}, Name: yyDollar[3].colIdent}
		}
	case 183:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:1004
		{
			yyVAL.colName = &ColName{Qualifier: &TableName{Qualifier: yyDollar[1].tableIdent, Name: yyDollar[3].tableIdent}, Name: yyDollar[5].colIdent}
		}
	case 184:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1010
		{
			yyVAL.valExpr = NewStrVal(yyDollar[1].bytes)
		}
	case 185:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1014
		{
			yyVAL.valExpr = NewHexVal(yyDollar[1].bytes)
		}
	case 186:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1018
		{
			yyVAL.valExpr = NewIntVal(yyDollar[1].bytes)
		}
	case 187:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1022
		{
			yyVAL.valExpr = NewFloatVal(yyDollar[1].bytes)
		}
	case 188:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1026
		{
			yyVAL.valExpr = NewHexNum(yyDollar[1].bytes)
		}
	case 189:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1030
		{
			yyVAL.valExpr = NewValArg(yyDollar[1].bytes)
		}
	case 190:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1034
		{
			yyVAL.valExpr = &NullVal{}
		}
	case 191:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1040
		{
			// TODO(sougou): Deprecate this construct.
			if yyDollar[1].colIdent.Lowered() != "value" {
				yylex.Error("expecting value after next")
				return 1
			}
			yyVAL.valExpr = NewIntVal([]byte("1"))
		}
	case 192:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:1049
		{
			yyVAL.valExpr = NewIntVal(yyDollar[1].bytes)
		}
	case 193:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:1053
		{
			yyVAL.valExpr = NewValArg(yyDollar[1].bytes)
		}
	case 194:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1058
		{
			yyVAL.valExprs = nil
		}
	case 195:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1062
		{
			yyVAL.valExprs = yyDollar[3].valExprs
		}
	case 196:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1067
		{
			yyVAL.boolExpr = nil
		}
	case 197:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:1071
		{
			yyVAL.boolExpr = yyDollar[2].boolExpr
		}
	case 198:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1076
		{
			yyVAL.orderBy = nil
		}
	case 199:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1080
		{
			yyVAL.orderBy = yyDollar[3].orderBy
		}
	case 200:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1086
		{
			yyVAL.orderBy = OrderBy{yyDollar[1].order}
		}
	case 201:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1090
		{
			yyVAL.orderBy = append(yyDollar[1].orderBy, yyDollar[3].order)
		}
	case 202:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:1096
		{
			yyVAL.order = &Order{Expr: yyDollar[1].valExpr, Direction: yyDollar[2].str}
		}
	case 203:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1101
		{
			yyVAL.str = AscScr
		}
	case 204:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1105
		{
			yyVAL.str = AscScr
		}
	case 205:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1109
		{
			yyVAL.str = DescScr
		}
	case 206:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1114
		{
			yyVAL.limit = nil
		}
	case 207:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:1118
		{
			yyVAL.limit = &Limit{Rowcount: yyDollar[2].valExpr}
		}
	case 208:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:1122
		{
			yyVAL.limit = &Limit{Offset: yyDollar[2].valExpr, Rowcount: yyDollar[4].valExpr}
		}
	case 209:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:1126
		{
			yyVAL.limit = &Limit{Offset: yyDollar[4].valExpr, Rowcount: yyDollar[2].valExpr}
		}
	case 210:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1131
		{
			yyVAL.str = ""
		}
	case 211:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:1135
		{
			yyVAL.str = ForUpdateStr
		}
	case 212:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:1139
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
	case 213:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1152
		{
			yyVAL.columns = nil
		}
	case 214:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1156
		{
			yyVAL.columns = yyDollar[2].columns
		}
	case 215:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1162
		{
			yyVAL.columns = Columns{yyDollar[1].colIdent}
		}
	case 216:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1166
		{
			yyVAL.columns = append(yyVAL.columns, yyDollar[3].colIdent)
		}
	case 217:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1171
		{
			yyVAL.updateExprs = nil
		}
	case 218:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:1175
		{
			yyVAL.updateExprs = yyDollar[5].updateExprs
		}
	case 219:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:1181
		{
			yyVAL.insRows = yyDollar[2].values
		}
	case 220:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1185
		{
			yyVAL.insRows = yyDollar[1].selStmt
		}
	case 221:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1191
		{
			yyVAL.values = Values{yyDollar[1].valTuple}
		}
	case 222:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1195
		{
			yyVAL.values = append(yyDollar[1].values, yyDollar[3].valTuple)
		}
	case 223:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1201
		{
			yyVAL.valTuple = ValTuple(yyDollar[2].valExprs)
		}
	case 224:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1207
		{
			yyVAL.updateExprs = UpdateExprs{yyDollar[1].updateExpr}
		}
	case 225:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1211
		{
			yyVAL.updateExprs = append(yyDollar[1].updateExprs, yyDollar[3].updateExpr)
		}
	case 226:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1217
		{
			yyVAL.updateExpr = &UpdateExpr{Name: yyDollar[1].colIdent, Expr: yyDollar[3].valExpr}
		}
	case 229:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1226
		{
			yyVAL.byt = 0
		}
	case 230:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:1228
		{
			yyVAL.byt = 1
		}
	case 231:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1231
		{
			yyVAL.empty = struct{}{}
		}
	case 232:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1233
		{
			yyVAL.empty = struct{}{}
		}
	case 233:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1236
		{
			yyVAL.str = ""
		}
	case 234:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1238
		{
			yyVAL.str = IgnoreStr
		}
	case 235:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1242
		{
			yyVAL.empty = struct{}{}
		}
	case 236:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1244
		{
			yyVAL.empty = struct{}{}
		}
	case 237:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1246
		{
			yyVAL.empty = struct{}{}
		}
	case 238:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1248
		{
			yyVAL.empty = struct{}{}
		}
	case 239:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1250
		{
			yyVAL.empty = struct{}{}
		}
	case 240:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1252
		{
			yyVAL.empty = struct{}{}
		}
	case 241:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1255
		{
			yyVAL.empty = struct{}{}
		}
	case 242:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1257
		{
			yyVAL.empty = struct{}{}
		}
	case 243:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1260
		{
			yyVAL.empty = struct{}{}
		}
	case 244:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1262
		{
			yyVAL.empty = struct{}{}
		}
	case 245:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1265
		{
			yyVAL.empty = struct{}{}
		}
	case 246:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:1267
		{
			yyVAL.empty = struct{}{}
		}
	case 247:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1271
		{
			yyVAL.colIdent = NewColIdent(string(yyDollar[1].bytes))
		}
	case 248:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1277
		{
			yyVAL.tableIdent = NewTableIdent(string(yyDollar[1].bytes))
		}
	case 249:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1283
		{
			if incNesting(yylex) {
				yylex.Error("max nesting level reached")
				return 1
			}
		}
	case 250:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1292
		{
			decNesting(yylex)
		}
	case 251:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1297
		{
			forceEOF(yylex)
		}
	}
	goto yystack /* stack new state and value */
}
