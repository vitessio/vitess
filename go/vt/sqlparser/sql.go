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
const OR_PIPE = 57401
const AND = 57402
const NOT = 57403
const BETWEEN = 57404
const CASE = 57405
const WHEN = 57406
const THEN = 57407
const ELSE = 57408
const END = 57409
const LE = 57410
const GE = 57411
const NE = 57412
const NULL_SAFE_EQUAL = 57413
const IS = 57414
const LIKE = 57415
const REGEXP = 57416
const IN = 57417
const SHIFT_LEFT = 57418
const SHIFT_RIGHT = 57419
const MOD = 57420
const UNARY = 57421
const COLLATE = 57422
const INTERVAL = 57423
const JSON_EXTRACT_OP = 57424
const JSON_UNQUOTE_EXTRACT_OP = 57425
const CREATE = 57426
const ALTER = 57427
const DROP = 57428
const RENAME = 57429
const ANALYZE = 57430
const TABLE = 57431
const INDEX = 57432
const VIEW = 57433
const TO = 57434
const IGNORE = 57435
const IF = 57436
const UNIQUE = 57437
const USING = 57438
const SHOW = 57439
const DESCRIBE = 57440
const EXPLAIN = 57441
const CURRENT_TIMESTAMP = 57442
const DATABASE = 57443
const UNUSED = 57444

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
	"OR_PIPE",
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
	97, 249,
	-2, 248,
}

const yyNprod = 253
const yyPrivate = 57344

var yyTokenNames []string
var yyStates []string

const yyLast = 999

var yyAct = [...]int{

	123, 109, 442, 373, 218, 62, 117, 342, 322, 297,
	240, 118, 311, 273, 252, 332, 253, 313, 115, 266,
	126, 159, 179, 105, 104, 254, 154, 163, 74, 217,
	3, 393, 395, 169, 35, 64, 37, 67, 69, 47,
	38, 71, 40, 41, 41, 249, 99, 167, 43, 44,
	45, 425, 424, 423, 68, 81, 70, 50, 46, 402,
	42, 220, 221, 375, 349, 48, 49, 243, 171, 256,
	183, 89, 197, 206, 63, 206, 447, 103, 196, 195,
	204, 205, 198, 199, 200, 201, 202, 203, 197, 64,
	65, 206, 64, 111, 152, 184, 187, 394, 280, 92,
	114, 405, 58, 312, 312, 365, 94, 150, 181, 96,
	187, 98, 278, 279, 277, 214, 216, 185, 186, 184,
	166, 168, 165, 84, 407, 65, 60, 87, 345, 399,
	60, 90, 260, 160, 187, 114, 114, 170, 128, 228,
	350, 351, 352, 174, 128, 226, 227, 60, 276, 229,
	64, 238, 65, 111, 182, 236, 151, 242, 200, 201,
	202, 203, 197, 95, 246, 206, 156, 14, 15, 16,
	17, 75, 86, 91, 185, 186, 184, 114, 235, 239,
	298, 181, 449, 298, 262, 300, 263, 264, 265, 257,
	18, 187, 412, 185, 186, 184, 60, 368, 114, 258,
	251, 275, 250, 259, 247, 215, 114, 114, 114, 271,
	187, 274, 330, 232, 284, 198, 199, 200, 201, 202,
	203, 197, 176, 298, 206, 343, 304, 299, 301, 267,
	269, 270, 300, 298, 28, 268, 305, 308, 128, 302,
	303, 316, 155, 14, 262, 330, 298, 319, 114, 114,
	321, 306, 309, 60, 318, 345, 317, 180, 320, 298,
	177, 19, 20, 22, 21, 23, 65, 419, 314, 82,
	257, 73, 83, 348, 24, 25, 26, 298, 91, 353,
	258, 358, 79, 128, 110, 65, 60, 182, 314, 275,
	91, 354, 245, 102, 149, 422, 421, 176, 157, 274,
	196, 195, 204, 205, 198, 199, 200, 201, 202, 203,
	197, 128, 385, 206, 366, 384, 360, 76, 147, 362,
	219, 369, 146, 364, 88, 222, 223, 224, 225, 114,
	370, 388, 361, 386, 114, 55, 389, 390, 387, 338,
	339, 39, 257, 257, 257, 257, 231, 380, 54, 382,
	400, 396, 258, 258, 258, 258, 391, 381, 398, 383,
	14, 408, 440, 401, 367, 244, 376, 161, 101, 347,
	379, 404, 323, 57, 441, 409, 400, 51, 52, 145,
	417, 324, 110, 378, 418, 234, 241, 144, 416, 329,
	317, 155, 61, 446, 272, 428, 14, 281, 282, 283,
	114, 285, 286, 287, 288, 289, 290, 291, 292, 293,
	294, 295, 296, 28, 30, 1, 431, 346, 341, 178,
	162, 433, 434, 36, 248, 432, 164, 66, 143, 64,
	237, 148, 110, 110, 438, 439, 443, 443, 443, 444,
	445, 114, 114, 413, 372, 435, 436, 437, 452, 124,
	453, 377, 448, 454, 450, 451, 328, 158, 363, 230,
	310, 125, 128, 116, 315, 111, 130, 129, 131, 132,
	133, 134, 80, 233, 135, 141, 142, 188, 112, 29,
	244, 392, 140, 85, 355, 356, 357, 333, 331, 255,
	59, 175, 107, 78, 14, 31, 32, 33, 34, 53,
	72, 27, 119, 120, 77, 56, 359, 139, 13, 121,
	12, 11, 122, 110, 10, 59, 9, 8, 7, 59,
	6, 5, 4, 244, 93, 2, 136, 0, 97, 371,
	374, 100, 137, 138, 128, 0, 108, 111, 130, 129,
	131, 132, 133, 134, 59, 0, 135, 153, 130, 129,
	131, 132, 133, 134, 140, 0, 135, 172, 0, 0,
	173, 0, 0, 0, 403, 0, 334, 337, 338, 339,
	335, 406, 336, 340, 119, 120, 420, 0, 0, 139,
	0, 121, 0, 0, 122, 244, 414, 415, 0, 0,
	0, 0, 0, 307, 0, 127, 0, 0, 136, 0,
	0, 59, 0, 0, 137, 138, 0, 0, 426, 0,
	0, 0, 0, 427, 0, 0, 429, 430, 374, 128,
	0, 298, 111, 130, 129, 131, 132, 133, 134, 0,
	0, 135, 141, 142, 108, 59, 0, 113, 0, 140,
	0, 261, 0, 0, 127, 196, 195, 204, 205, 198,
	199, 200, 201, 202, 203, 197, 0, 0, 206, 119,
	120, 106, 0, 0, 139, 0, 121, 0, 128, 122,
	298, 111, 130, 129, 131, 132, 133, 134, 0, 0,
	135, 141, 142, 136, 108, 108, 113, 0, 140, 137,
	138, 0, 0, 334, 337, 338, 339, 335, 127, 336,
	340, 325, 0, 326, 0, 0, 327, 0, 119, 120,
	106, 65, 0, 139, 344, 121, 59, 0, 122, 0,
	0, 0, 128, 0, 0, 111, 130, 129, 131, 132,
	133, 134, 136, 0, 135, 141, 142, 0, 137, 138,
	113, 0, 140, 0, 196, 195, 204, 205, 198, 199,
	200, 201, 202, 203, 197, 0, 0, 206, 0, 0,
	0, 0, 119, 120, 106, 108, 14, 139, 0, 121,
	411, 0, 122, 204, 205, 198, 199, 200, 201, 202,
	203, 197, 127, 0, 206, 0, 136, 0, 59, 59,
	59, 59, 137, 138, 0, 0, 0, 0, 0, 0,
	410, 344, 0, 0, 397, 0, 128, 0, 0, 111,
	130, 129, 131, 132, 133, 134, 0, 0, 135, 141,
	142, 0, 0, 0, 113, 0, 140, 0, 0, 0,
	127, 0, 0, 0, 0, 196, 195, 204, 205, 198,
	199, 200, 201, 202, 203, 197, 119, 120, 206, 0,
	0, 139, 0, 121, 128, 0, 122, 111, 130, 129,
	131, 132, 133, 134, 0, 0, 135, 141, 142, 0,
	136, 0, 113, 0, 140, 0, 137, 138, 196, 195,
	204, 205, 198, 199, 200, 201, 202, 203, 197, 0,
	0, 206, 0, 0, 119, 120, 0, 0, 0, 139,
	0, 121, 128, 0, 122, 111, 130, 129, 131, 132,
	133, 134, 0, 0, 135, 0, 0, 0, 136, 0,
	0, 0, 140, 0, 137, 138, 195, 204, 205, 198,
	199, 200, 201, 202, 203, 197, 0, 0, 206, 0,
	0, 0, 119, 120, 0, 0, 0, 139, 0, 121,
	0, 0, 122, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 136, 190, 193, 0,
	0, 0, 137, 138, 207, 208, 209, 210, 211, 212,
	213, 194, 191, 192, 189, 196, 195, 204, 205, 198,
	199, 200, 201, 202, 203, 197, 0, 0, 206,
}
var yyPact = [...]int{

	161, -1000, -1000, 408, -1000, -1000, -1000, -1000, -1000, -1000,
	-1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, -71,
	-65, -45, -57, -47, -1000, -1000, -1000, 390, 358, 315,
	-1000, -66, 81, 382, 76, -73, -52, 76, -1000, -49,
	76, -1000, 81, -82, 122, -82, 81, -1000, -1000, -1000,
	-1000, -1000, -1000, 246, 217, -1000, 66, 147, 295, -26,
	-1000, 81, 126, -1000, 28, -1000, 81, 42, 114, -1000,
	81, -1000, -62, 81, 346, 248, 76, -1000, 676, -1000,
	369, -1000, 291, 287, -1000, 265, 81, -1000, 76, 81,
	380, 76, 856, -1000, 345, -85, -1000, 19, -1000, 81,
	-1000, -1000, 81, -1000, 250, -1000, -1000, 236, -27, 113,
	903, -1000, -1000, 808, 760, -1000, -37, -1000, -1000, 856,
	856, 856, 856, 192, 192, -1000, -1000, 192, -1000, -1000,
	-1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000,
	856, -1000, -1000, 81, -1000, -1000, -1000, -1000, 354, 76,
	76, -1000, 231, -1000, 372, 808, -1000, 796, -30, 488,
	-1000, -1000, 247, 76, -1000, -63, -1000, -1000, -1000, -1000,
	-1000, -1000, -1000, -1000, -1000, 380, 676, 98, -1000, -1000,
	103, -1000, -1000, 44, 808, 808, 808, 171, 416, 92,
	33, 856, 856, 856, 171, 856, 856, 856, 856, 856,
	856, 856, 856, 856, 856, 856, 856, -1000, -1000, -1000,
	-1000, -1000, -1000, -1000, 18, 903, 132, 229, 185, 903,
	498, 498, -22, -22, -22, 662, 573, 622, -1000, 390,
	37, 796, -1000, 223, 192, 408, 243, 211, -1000, 372,
	356, 366, 113, 104, 796, 81, -1000, -1000, 81, -1000,
	377, -1000, 165, 658, -1000, -1000, 204, 348, 237, -1000,
	-1000, -33, -1000, 18, 32, 32, -1000, -1000, 82, -1000,
	-1000, -1000, 796, -1000, 488, -1000, -1000, 92, 856, 856,
	856, 796, 796, 218, -1000, 689, 843, -22, 70, 70,
	-20, -20, -20, -20, 129, 129, -1000, -1000, -1000, -1000,
	856, -1000, -1000, -1000, -1000, -1000, 175, 676, -1000, 175,
	36, -1000, 808, -1000, 338, 150, -1000, 856, -1000, -1000,
	76, 356, -1000, 856, 856, -34, -1000, -1000, 370, 355,
	98, 98, 98, 98, -1000, 280, 277, -1000, 298, 296,
	302, -12, -1000, 77, -1000, -1000, 81, -1000, 198, 41,
	-1000, -1000, -1000, 185, -1000, 796, 796, -4, 856, 796,
	-1000, 175, -1000, 31, -1000, 856, 56, 334, 192, -1000,
	-1000, 753, 145, -1000, 563, 76, -1000, 372, 808, 856,
	658, 222, 531, -1000, -1000, -1000, -1000, 261, -1000, 260,
	-1000, -1000, -1000, -53, -54, -55, -1000, -1000, -1000, -1000,
	-1000, -1000, 856, 796, -1000, -1000, 796, 856, 387, -1000,
	856, 856, 856, -1000, -1000, -1000, 356, 113, 138, 808,
	808, -1000, -1000, 192, 192, 192, 796, 796, 76, 796,
	796, -1000, 344, 113, 113, 76, 76, 76, 126, -1000,
	385, -5, 135, -1000, 135, 135, -1000, 76, -1000, 76,
	-1000, -1000, 76, -1000, -1000,
}
var yyPgo = [...]int{

	0, 525, 29, 522, 521, 520, 518, 517, 516, 514,
	511, 510, 508, 479, 505, 501, 499, 493, 24, 23,
	492, 491, 14, 16, 25, 489, 488, 15, 487, 69,
	483, 481, 2, 26, 1, 478, 20, 477, 473, 18,
	205, 472, 19, 13, 4, 464, 6, 11, 463, 461,
	460, 12, 459, 458, 456, 451, 449, 10, 444, 3,
	443, 8, 435, 431, 430, 17, 5, 74, 428, 341,
	271, 427, 426, 424, 423, 420, 0, 22, 419, 457,
	7, 418, 417, 39, 415, 414, 21, 9,
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
	33, 34, 34, 34, 34, 34, 34, 34, 36, 36,
	35, 35, 35, 35, 35, 35, 35, 35, 35, 35,
	35, 35, 35, 42, 42, 42, 42, 42, 42, 37,
	37, 37, 37, 37, 37, 37, 43, 43, 43, 47,
	44, 44, 40, 40, 40, 40, 40, 40, 40, 40,
	40, 40, 40, 40, 40, 40, 40, 40, 40, 40,
	40, 40, 40, 40, 40, 40, 40, 40, 40, 40,
	56, 56, 56, 56, 49, 52, 52, 50, 50, 51,
	53, 53, 48, 48, 48, 39, 39, 39, 39, 39,
	39, 39, 41, 41, 41, 54, 54, 55, 55, 57,
	57, 58, 58, 59, 60, 60, 60, 61, 61, 61,
	61, 62, 62, 62, 63, 63, 64, 64, 65, 65,
	38, 38, 45, 45, 46, 66, 66, 67, 68, 68,
	70, 70, 71, 71, 69, 69, 72, 72, 72, 72,
	72, 72, 73, 73, 74, 74, 75, 75, 76, 79,
	86, 87, 83,
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
	2, 1, 3, 3, 3, 2, 3, 3, 1, 1,
	1, 3, 3, 3, 4, 3, 4, 3, 4, 5,
	6, 3, 2, 1, 2, 1, 2, 1, 2, 1,
	1, 1, 1, 1, 1, 1, 3, 1, 1, 3,
	1, 3, 1, 1, 1, 1, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	2, 2, 2, 3, 3, 4, 5, 3, 4, 1,
	1, 1, 1, 1, 5, 0, 1, 1, 2, 4,
	0, 2, 1, 3, 5, 1, 1, 1, 1, 1,
	1, 1, 1, 2, 2, 0, 3, 0, 2, 0,
	3, 1, 3, 2, 0, 1, 1, 0, 2, 4,
	4, 0, 2, 4, 0, 3, 1, 3, 0, 5,
	2, 1, 1, 3, 3, 1, 3, 3, 1, 1,
	0, 2, 0, 3, 0, 1, 1, 1, 1, 1,
	1, 1, 0, 1, 0, 1, 0, 2, 1, 1,
	1, 1, 0,
}
var yyChk = [...]int{

	-1000, -84, -1, -2, -3, -4, -5, -6, -7, -8,
	-9, -10, -11, -12, 6, 7, 8, 9, 29, 100,
	101, 103, 102, 104, 113, 114, 115, -15, 5, -13,
	-85, -13, -13, -13, -13, 105, -74, 107, 111, -69,
	107, 109, 105, 105, 106, 107, 105, -83, -83, -83,
	-2, 19, 20, -16, 33, 20, -14, -69, -29, -79,
	49, 10, -66, -67, -76, 49, -71, 110, 106, -76,
	105, -76, -79, -70, 110, 49, -70, -79, -17, 36,
	-41, -76, 52, 55, 57, -30, 25, -29, 29, 97,
	-29, 47, 71, -79, 64, 49, -83, -79, -83, 108,
	-79, 22, 45, -76, -18, -19, 88, -20, -79, -34,
	-40, 49, -35, 64, -86, -39, -48, -46, -47, 86,
	87, 93, 96, -76, -56, -49, -36, 22, 46, 51,
	50, 52, 53, 54, 55, 58, 110, 116, 117, 91,
	66, 59, 60, -68, 18, 10, 31, 31, -63, 29,
	-86, -29, -66, -79, -33, 11, -67, -40, -79, -86,
	-83, 22, -75, 112, -72, 103, 101, 28, 102, 14,
	118, 49, -79, -79, -83, -21, 47, 10, -78, -77,
	21, -76, 51, 97, 63, 61, 62, 78, -37, 81,
	64, 79, 80, 65, 78, 83, 82, 92, 86, 87,
	88, 89, 90, 91, 84, 85, 95, 71, 72, 73,
	74, 75, 76, 77, -34, -40, -34, -2, -44, -40,
	98, 99, -40, -40, -40, -40, -86, -86, -47, -86,
	-52, -40, -29, -38, 31, -2, -66, -64, -76, -33,
	-57, 14, -34, 97, -40, 45, -76, -83, -73, 108,
	-33, -19, -22, -23, -24, -25, -29, -47, -86, -77,
	88, -79, -76, -34, -34, -34, -42, 58, 64, 59,
	60, -36, -40, -43, -86, -47, 56, 81, 79, 80,
	65, -40, -40, -40, -42, -40, -40, -40, -40, -40,
	-40, -40, -40, -40, -40, -40, -40, -87, 48, -87,
	47, -87, -39, -39, -76, -87, -18, 20, -87, -18,
	-50, -51, 67, -65, 45, -45, -46, -86, -65, -87,
	47, -57, -61, 16, 15, -79, -79, -79, -54, 12,
	47, -26, -27, -28, 35, 39, 41, 36, 37, 38,
	42, -81, -80, 21, -79, 51, -82, 21, -22, 97,
	58, 59, 60, -44, -43, -40, -40, -40, 63, -40,
	-87, -18, -87, -53, -51, 69, -34, 26, 47, -76,
	-61, -40, -58, -59, -40, 97, -83, -55, 13, 15,
	-23, -24, -23, -24, 35, 35, 35, 40, 35, 40,
	35, -27, -31, 43, 109, 44, -80, -79, -87, 88,
	-76, -87, 63, -40, -87, 70, -40, 68, 27, -46,
	47, 17, 47, -60, 23, 24, -57, -34, -44, 45,
	45, 35, 35, 106, 106, 106, -40, -40, 8, -40,
	-40, -59, -61, -34, -34, -86, -86, -86, -66, -62,
	18, 30, -32, -76, -32, -32, 8, 81, -87, 47,
	-87, -87, -76, -76, -76,
}
var yyDef = [...]int{

	0, -2, 1, 2, 3, 4, 5, 6, 7, 8,
	9, 10, 11, 12, 35, 35, 35, 35, 35, 244,
	234, 0, 0, 0, 252, 252, 252, 0, 39, 42,
	37, 234, 0, 0, 0, 232, 0, 0, 245, 0,
	0, 235, 0, 230, 0, 230, 0, 32, 33, 34,
	15, 40, 41, 44, 0, 43, 36, 0, 0, 91,
	249, 0, 20, 225, 0, 248, 0, 0, 0, 252,
	0, 252, 0, 0, 0, 0, 0, 31, 0, 45,
	0, 192, 0, 0, 38, 214, 0, 90, 0, 0,
	99, 0, 0, 252, 0, 246, 23, 0, 26, 0,
	28, 231, 0, 252, 59, 46, 48, 54, 0, 52,
	53, -2, 101, 0, 0, 142, 143, 144, 145, 0,
	0, 0, 0, 182, 0, 169, 110, 0, 250, 185,
	186, 187, 188, 189, 190, 191, 170, 171, 172, 173,
	175, 108, 109, 0, 228, 229, 193, 194, 0, 0,
	0, 89, 99, 92, 199, 0, 226, 227, 0, 0,
	21, 233, 0, 0, 252, 242, 236, 237, 238, 239,
	240, 241, 27, 29, 30, 99, 0, 0, 49, 55,
	0, 57, 58, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 129, 130, 131,
	132, 133, 134, 135, 105, 0, 0, 0, 0, 140,
	0, 0, 160, 161, 162, 0, 0, 0, 122, 0,
	0, 176, 14, 218, 0, 221, 218, 0, 216, 199,
	207, 0, 100, 0, 140, 0, 247, 24, 0, 243,
	195, 47, 60, 61, 63, 64, 74, 72, 0, 56,
	50, 0, 183, 102, 103, 104, 107, 123, 0, 125,
	127, 111, 112, 113, 0, 137, 138, 0, 0, 0,
	0, 115, 117, 0, 121, 146, 147, 148, 149, 150,
	151, 152, 153, 154, 155, 156, 159, 106, 251, 139,
	0, 224, 157, 158, 163, 164, 0, 0, 167, 0,
	180, 177, 0, 16, 0, 220, 222, 0, 17, 215,
	0, 207, 19, 0, 0, 0, 252, 25, 197, 0,
	0, 0, 0, 0, 79, 0, 0, 82, 0, 0,
	0, 93, 75, 0, 77, 78, 0, 73, 0, 0,
	124, 126, 128, 0, 114, 116, 118, 0, 0, 141,
	165, 0, 168, 0, 178, 0, 0, 0, 0, 217,
	18, 208, 200, 201, 204, 0, 22, 199, 0, 0,
	62, 68, 0, 71, 80, 81, 83, 0, 85, 0,
	87, 88, 65, 0, 0, 0, 76, 66, 67, 51,
	184, 136, 0, 119, 166, 174, 181, 0, 0, 223,
	0, 0, 0, 203, 205, 206, 207, 198, 196, 0,
	0, 84, 86, 0, 0, 0, 120, 179, 0, 209,
	210, 202, 211, 69, 70, 0, 0, 0, 219, 13,
	0, 0, 0, 97, 0, 0, 212, 0, 94, 0,
	95, 96, 0, 98, 213,
}
var yyTok1 = [...]int{

	1, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 90, 83, 3,
	46, 48, 88, 86, 47, 87, 97, 89, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	72, 71, 73, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 92, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 82, 3, 93,
}
var yyTok2 = [...]int{

	2, 3, 4, 5, 6, 7, 8, 9, 10, 11,
	12, 13, 14, 15, 16, 17, 18, 19, 20, 21,
	22, 23, 24, 25, 26, 27, 28, 29, 30, 31,
	32, 33, 34, 35, 36, 37, 38, 39, 40, 41,
	42, 43, 44, 45, 49, 50, 51, 52, 53, 54,
	55, 56, 57, 58, 59, 60, 61, 62, 63, 64,
	65, 66, 67, 68, 69, 70, 74, 75, 76, 77,
	78, 79, 80, 81, 84, 85, 91, 94, 95, 96,
	98, 99, 100, 101, 102, 103, 104, 105, 106, 107,
	108, 109, 110, 111, 112, 113, 114, 115, 116, 117,
	118,
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
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:642
		{
			yyVAL.boolExpr = &OrExpr{Left: yyDollar[1].boolExpr, Right: yyDollar[3].boolExpr}
		}
	case 105:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:646
		{
			yyVAL.boolExpr = &NotExpr{Expr: yyDollar[2].boolExpr}
		}
	case 106:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:650
		{
			yyVAL.boolExpr = &ParenBoolExpr{Expr: yyDollar[2].boolExpr}
		}
	case 107:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:654
		{
			yyVAL.boolExpr = &IsExpr{Operator: yyDollar[3].str, Expr: yyDollar[1].boolExpr}
		}
	case 108:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:660
		{
			yyVAL.boolVal = BoolVal(true)
		}
	case 109:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:664
		{
			yyVAL.boolVal = BoolVal(false)
		}
	case 110:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:670
		{
			yyVAL.boolExpr = yyDollar[1].boolVal
		}
	case 111:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:674
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: yyDollar[2].str, Right: yyDollar[3].boolVal}
		}
	case 112:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:678
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: yyDollar[2].str, Right: yyDollar[3].valExpr}
		}
	case 113:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:682
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: InStr, Right: yyDollar[3].colTuple}
		}
	case 114:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:686
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: NotInStr, Right: yyDollar[4].colTuple}
		}
	case 115:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:690
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: LikeStr, Right: yyDollar[3].valExpr}
		}
	case 116:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:694
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: NotLikeStr, Right: yyDollar[4].valExpr}
		}
	case 117:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:698
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: RegexpStr, Right: yyDollar[3].valExpr}
		}
	case 118:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:702
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: NotRegexpStr, Right: yyDollar[4].valExpr}
		}
	case 119:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:706
		{
			yyVAL.boolExpr = &RangeCond{Left: yyDollar[1].valExpr, Operator: BetweenStr, From: yyDollar[3].valExpr, To: yyDollar[5].valExpr}
		}
	case 120:
		yyDollar = yyS[yypt-6 : yypt+1]
		//line sql.y:710
		{
			yyVAL.boolExpr = &RangeCond{Left: yyDollar[1].valExpr, Operator: NotBetweenStr, From: yyDollar[4].valExpr, To: yyDollar[6].valExpr}
		}
	case 121:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:714
		{
			yyVAL.boolExpr = &IsExpr{Operator: yyDollar[3].str, Expr: yyDollar[1].valExpr}
		}
	case 122:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:718
		{
			yyVAL.boolExpr = &ExistsExpr{Subquery: yyDollar[2].subquery}
		}
	case 123:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:724
		{
			yyVAL.str = IsNullStr
		}
	case 124:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:728
		{
			yyVAL.str = IsNotNullStr
		}
	case 125:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:732
		{
			yyVAL.str = IsTrueStr
		}
	case 126:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:736
		{
			yyVAL.str = IsNotTrueStr
		}
	case 127:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:740
		{
			yyVAL.str = IsFalseStr
		}
	case 128:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:744
		{
			yyVAL.str = IsNotFalseStr
		}
	case 129:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:750
		{
			yyVAL.str = EqualStr
		}
	case 130:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:754
		{
			yyVAL.str = LessThanStr
		}
	case 131:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:758
		{
			yyVAL.str = GreaterThanStr
		}
	case 132:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:762
		{
			yyVAL.str = LessEqualStr
		}
	case 133:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:766
		{
			yyVAL.str = GreaterEqualStr
		}
	case 134:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:770
		{
			yyVAL.str = NotEqualStr
		}
	case 135:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:774
		{
			yyVAL.str = NullSafeEqualStr
		}
	case 136:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:780
		{
			yyVAL.colTuple = ValTuple(yyDollar[2].valExprs)
		}
	case 137:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:784
		{
			yyVAL.colTuple = yyDollar[1].subquery
		}
	case 138:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:788
		{
			yyVAL.colTuple = ListArg(yyDollar[1].bytes)
		}
	case 139:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:794
		{
			yyVAL.subquery = &Subquery{yyDollar[2].selStmt}
		}
	case 140:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:800
		{
			yyVAL.valExprs = ValExprs{yyDollar[1].valExpr}
		}
	case 141:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:804
		{
			yyVAL.valExprs = append(yyDollar[1].valExprs, yyDollar[3].valExpr)
		}
	case 142:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:810
		{
			yyVAL.valExpr = yyDollar[1].valExpr
		}
	case 143:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:814
		{
			yyVAL.valExpr = yyDollar[1].colName
		}
	case 144:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:818
		{
			yyVAL.valExpr = yyDollar[1].valTuple
		}
	case 145:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:822
		{
			yyVAL.valExpr = yyDollar[1].subquery
		}
	case 146:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:826
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: BitAndStr, Right: yyDollar[3].valExpr}
		}
	case 147:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:830
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: BitOrStr, Right: yyDollar[3].valExpr}
		}
	case 148:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:834
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: BitXorStr, Right: yyDollar[3].valExpr}
		}
	case 149:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:838
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: PlusStr, Right: yyDollar[3].valExpr}
		}
	case 150:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:842
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: MinusStr, Right: yyDollar[3].valExpr}
		}
	case 151:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:846
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: MultStr, Right: yyDollar[3].valExpr}
		}
	case 152:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:850
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: DivStr, Right: yyDollar[3].valExpr}
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
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: ModStr, Right: yyDollar[3].valExpr}
		}
	case 155:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:862
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: ShiftLeftStr, Right: yyDollar[3].valExpr}
		}
	case 156:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:866
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: ShiftRightStr, Right: yyDollar[3].valExpr}
		}
	case 157:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:870
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].colName, Operator: JSONExtractOp, Right: yyDollar[3].valExpr}
		}
	case 158:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:874
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].colName, Operator: JSONUnquoteExtractOp, Right: yyDollar[3].valExpr}
		}
	case 159:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:878
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: CollateStr, Right: yyDollar[3].valExpr}
		}
	case 160:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:882
		{
			if num, ok := yyDollar[2].valExpr.(*SQLVal); ok && num.Type == IntVal {
				yyVAL.valExpr = num
			} else {
				yyVAL.valExpr = &UnaryExpr{Operator: UPlusStr, Expr: yyDollar[2].valExpr}
			}
		}
	case 161:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:890
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
	case 162:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:904
		{
			yyVAL.valExpr = &UnaryExpr{Operator: TildaStr, Expr: yyDollar[2].valExpr}
		}
	case 163:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:908
		{
			// This rule prevents the usage of INTERVAL
			// as a function. If support is needed for that,
			// we'll need to revisit this. The solution
			// will be non-trivial because of grammar conflicts.
			yyVAL.valExpr = &IntervalExpr{Expr: yyDollar[2].valExpr, Unit: yyDollar[3].colIdent}
		}
	case 164:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:916
		{
			yyVAL.valExpr = &FuncExpr{Name: yyDollar[1].colIdent}
		}
	case 165:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:920
		{
			yyVAL.valExpr = &FuncExpr{Name: yyDollar[1].colIdent, Exprs: yyDollar[3].selectExprs}
		}
	case 166:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:924
		{
			yyVAL.valExpr = &FuncExpr{Name: yyDollar[1].colIdent, Distinct: true, Exprs: yyDollar[4].selectExprs}
		}
	case 167:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:928
		{
			yyVAL.valExpr = &FuncExpr{Name: yyDollar[1].colIdent}
		}
	case 168:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:932
		{
			yyVAL.valExpr = &FuncExpr{Name: yyDollar[1].colIdent, Exprs: yyDollar[3].selectExprs}
		}
	case 169:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:936
		{
			yyVAL.valExpr = yyDollar[1].caseExpr
		}
	case 170:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:942
		{
			yyVAL.colIdent = NewColIdent("if")
		}
	case 171:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:946
		{
			yyVAL.colIdent = NewColIdent("current_timestamp")
		}
	case 172:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:950
		{
			yyVAL.colIdent = NewColIdent("database")
		}
	case 173:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:954
		{
			yyVAL.colIdent = NewColIdent("mod")
		}
	case 174:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:960
		{
			yyVAL.caseExpr = &CaseExpr{Expr: yyDollar[2].valExpr, Whens: yyDollar[3].whens, Else: yyDollar[4].valExpr}
		}
	case 175:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:965
		{
			yyVAL.valExpr = nil
		}
	case 176:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:969
		{
			yyVAL.valExpr = yyDollar[1].valExpr
		}
	case 177:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:975
		{
			yyVAL.whens = []*When{yyDollar[1].when}
		}
	case 178:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:979
		{
			yyVAL.whens = append(yyDollar[1].whens, yyDollar[2].when)
		}
	case 179:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:985
		{
			yyVAL.when = &When{Cond: yyDollar[2].boolExpr, Val: yyDollar[4].valExpr}
		}
	case 180:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:990
		{
			yyVAL.valExpr = nil
		}
	case 181:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:994
		{
			yyVAL.valExpr = yyDollar[2].valExpr
		}
	case 182:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1000
		{
			yyVAL.colName = &ColName{Name: yyDollar[1].colIdent}
		}
	case 183:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1004
		{
			yyVAL.colName = &ColName{Qualifier: &TableName{Name: yyDollar[1].tableIdent}, Name: yyDollar[3].colIdent}
		}
	case 184:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:1008
		{
			yyVAL.colName = &ColName{Qualifier: &TableName{Qualifier: yyDollar[1].tableIdent, Name: yyDollar[3].tableIdent}, Name: yyDollar[5].colIdent}
		}
	case 185:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1014
		{
			yyVAL.valExpr = NewStrVal(yyDollar[1].bytes)
		}
	case 186:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1018
		{
			yyVAL.valExpr = NewHexVal(yyDollar[1].bytes)
		}
	case 187:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1022
		{
			yyVAL.valExpr = NewIntVal(yyDollar[1].bytes)
		}
	case 188:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1026
		{
			yyVAL.valExpr = NewFloatVal(yyDollar[1].bytes)
		}
	case 189:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1030
		{
			yyVAL.valExpr = NewHexNum(yyDollar[1].bytes)
		}
	case 190:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1034
		{
			yyVAL.valExpr = NewValArg(yyDollar[1].bytes)
		}
	case 191:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1038
		{
			yyVAL.valExpr = &NullVal{}
		}
	case 192:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1044
		{
			// TODO(sougou): Deprecate this construct.
			if yyDollar[1].colIdent.Lowered() != "value" {
				yylex.Error("expecting value after next")
				return 1
			}
			yyVAL.valExpr = NewIntVal([]byte("1"))
		}
	case 193:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:1053
		{
			yyVAL.valExpr = NewIntVal(yyDollar[1].bytes)
		}
	case 194:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:1057
		{
			yyVAL.valExpr = NewValArg(yyDollar[1].bytes)
		}
	case 195:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1062
		{
			yyVAL.valExprs = nil
		}
	case 196:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1066
		{
			yyVAL.valExprs = yyDollar[3].valExprs
		}
	case 197:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1071
		{
			yyVAL.boolExpr = nil
		}
	case 198:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:1075
		{
			yyVAL.boolExpr = yyDollar[2].boolExpr
		}
	case 199:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1080
		{
			yyVAL.orderBy = nil
		}
	case 200:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1084
		{
			yyVAL.orderBy = yyDollar[3].orderBy
		}
	case 201:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1090
		{
			yyVAL.orderBy = OrderBy{yyDollar[1].order}
		}
	case 202:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1094
		{
			yyVAL.orderBy = append(yyDollar[1].orderBy, yyDollar[3].order)
		}
	case 203:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:1100
		{
			yyVAL.order = &Order{Expr: yyDollar[1].valExpr, Direction: yyDollar[2].str}
		}
	case 204:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1105
		{
			yyVAL.str = AscScr
		}
	case 205:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1109
		{
			yyVAL.str = AscScr
		}
	case 206:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1113
		{
			yyVAL.str = DescScr
		}
	case 207:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1118
		{
			yyVAL.limit = nil
		}
	case 208:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:1122
		{
			yyVAL.limit = &Limit{Rowcount: yyDollar[2].valExpr}
		}
	case 209:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:1126
		{
			yyVAL.limit = &Limit{Offset: yyDollar[2].valExpr, Rowcount: yyDollar[4].valExpr}
		}
	case 210:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:1130
		{
			yyVAL.limit = &Limit{Offset: yyDollar[4].valExpr, Rowcount: yyDollar[2].valExpr}
		}
	case 211:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1135
		{
			yyVAL.str = ""
		}
	case 212:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:1139
		{
			yyVAL.str = ForUpdateStr
		}
	case 213:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:1143
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
	case 214:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1156
		{
			yyVAL.columns = nil
		}
	case 215:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1160
		{
			yyVAL.columns = yyDollar[2].columns
		}
	case 216:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1166
		{
			yyVAL.columns = Columns{yyDollar[1].colIdent}
		}
	case 217:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1170
		{
			yyVAL.columns = append(yyVAL.columns, yyDollar[3].colIdent)
		}
	case 218:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1175
		{
			yyVAL.updateExprs = nil
		}
	case 219:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:1179
		{
			yyVAL.updateExprs = yyDollar[5].updateExprs
		}
	case 220:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:1185
		{
			yyVAL.insRows = yyDollar[2].values
		}
	case 221:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1189
		{
			yyVAL.insRows = yyDollar[1].selStmt
		}
	case 222:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1195
		{
			yyVAL.values = Values{yyDollar[1].valTuple}
		}
	case 223:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1199
		{
			yyVAL.values = append(yyDollar[1].values, yyDollar[3].valTuple)
		}
	case 224:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1205
		{
			yyVAL.valTuple = ValTuple(yyDollar[2].valExprs)
		}
	case 225:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1211
		{
			yyVAL.updateExprs = UpdateExprs{yyDollar[1].updateExpr}
		}
	case 226:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1215
		{
			yyVAL.updateExprs = append(yyDollar[1].updateExprs, yyDollar[3].updateExpr)
		}
	case 227:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1221
		{
			yyVAL.updateExpr = &UpdateExpr{Name: yyDollar[1].colIdent, Expr: yyDollar[3].valExpr}
		}
	case 230:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1230
		{
			yyVAL.byt = 0
		}
	case 231:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:1232
		{
			yyVAL.byt = 1
		}
	case 232:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1235
		{
			yyVAL.empty = struct{}{}
		}
	case 233:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1237
		{
			yyVAL.empty = struct{}{}
		}
	case 234:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1240
		{
			yyVAL.str = ""
		}
	case 235:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1242
		{
			yyVAL.str = IgnoreStr
		}
	case 236:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1246
		{
			yyVAL.empty = struct{}{}
		}
	case 237:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1248
		{
			yyVAL.empty = struct{}{}
		}
	case 238:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1250
		{
			yyVAL.empty = struct{}{}
		}
	case 239:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1252
		{
			yyVAL.empty = struct{}{}
		}
	case 240:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1254
		{
			yyVAL.empty = struct{}{}
		}
	case 241:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1256
		{
			yyVAL.empty = struct{}{}
		}
	case 242:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1259
		{
			yyVAL.empty = struct{}{}
		}
	case 243:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1261
		{
			yyVAL.empty = struct{}{}
		}
	case 244:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1264
		{
			yyVAL.empty = struct{}{}
		}
	case 245:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1266
		{
			yyVAL.empty = struct{}{}
		}
	case 246:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1269
		{
			yyVAL.empty = struct{}{}
		}
	case 247:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:1271
		{
			yyVAL.empty = struct{}{}
		}
	case 248:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1275
		{
			yyVAL.colIdent = NewColIdent(string(yyDollar[1].bytes))
		}
	case 249:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1281
		{
			yyVAL.tableIdent = NewTableIdent(string(yyDollar[1].bytes))
		}
	case 250:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1287
		{
			if incNesting(yylex) {
				yylex.Error("max nesting level reached")
				return 1
			}
		}
	case 251:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1296
		{
			decNesting(yylex)
		}
	case 252:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1301
		{
			forceEOF(yylex)
		}
	}
	goto yystack /* stack new state and value */
}
