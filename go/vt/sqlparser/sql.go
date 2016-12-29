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
const INTEGRAL = 57390
const FLOAT = 57391
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
	94, 242,
	-2, 241,
}

const yyNprod = 246
const yyPrivate = 57344

var yyTokenNames []string
var yyStates []string

const yyLast = 1050

var yyAct = [...]int{

	121, 107, 283, 431, 62, 362, 306, 115, 210, 229,
	116, 316, 351, 326, 243, 102, 242, 297, 260, 241,
	209, 3, 254, 113, 173, 157, 245, 148, 382, 384,
	103, 47, 74, 163, 35, 64, 37, 67, 69, 41,
	38, 71, 40, 70, 41, 238, 161, 97, 50, 14,
	15, 16, 17, 414, 413, 81, 412, 48, 49, 58,
	43, 44, 45, 68, 46, 42, 364, 165, 212, 213,
	333, 18, 232, 153, 177, 87, 63, 101, 197, 198,
	191, 192, 193, 194, 195, 196, 190, 64, 88, 190,
	64, 146, 383, 191, 192, 193, 194, 195, 196, 190,
	65, 94, 437, 96, 179, 178, 175, 193, 194, 195,
	196, 190, 145, 206, 208, 180, 267, 160, 162, 159,
	180, 109, 394, 154, 178, 90, 298, 298, 349, 92,
	265, 266, 264, 168, 164, 284, 127, 220, 388, 180,
	19, 20, 22, 21, 23, 84, 263, 127, 179, 178,
	60, 231, 112, 24, 25, 26, 179, 178, 235, 249,
	65, 60, 396, 329, 180, 60, 150, 224, 255, 257,
	258, 109, 180, 256, 228, 175, 93, 65, 251, 176,
	252, 253, 246, 439, 284, 112, 112, 334, 335, 336,
	236, 170, 284, 262, 75, 218, 219, 239, 89, 248,
	221, 240, 327, 129, 128, 130, 131, 132, 133, 14,
	271, 134, 285, 287, 65, 286, 284, 82, 290, 227,
	83, 291, 294, 112, 314, 284, 149, 64, 304, 174,
	60, 302, 329, 251, 292, 295, 288, 289, 305, 358,
	284, 352, 28, 89, 112, 247, 301, 286, 127, 171,
	401, 60, 112, 112, 398, 73, 261, 65, 246, 176,
	207, 89, 318, 321, 322, 323, 319, 332, 320, 324,
	337, 314, 409, 226, 127, 262, 408, 352, 234, 100,
	411, 377, 375, 338, 284, 170, 378, 376, 79, 410,
	127, 374, 112, 112, 379, 344, 322, 323, 346, 373,
	350, 76, 144, 143, 86, 39, 357, 417, 354, 345,
	397, 55, 359, 85, 348, 356, 318, 321, 322, 323,
	319, 247, 320, 324, 54, 246, 246, 246, 246, 14,
	370, 369, 372, 371, 389, 387, 380, 57, 261, 108,
	390, 385, 365, 429, 29, 155, 99, 331, 393, 51,
	52, 151, 307, 300, 368, 430, 308, 142, 230, 399,
	31, 32, 33, 34, 141, 389, 367, 112, 313, 406,
	149, 61, 112, 211, 355, 436, 405, 407, 214, 215,
	216, 217, 427, 14, 28, 30, 1, 330, 247, 247,
	247, 247, 325, 172, 156, 36, 237, 158, 66, 140,
	223, 303, 225, 428, 402, 361, 418, 420, 122, 366,
	422, 423, 421, 312, 233, 188, 197, 198, 191, 192,
	193, 194, 195, 196, 190, 432, 432, 432, 64, 433,
	434, 108, 435, 347, 438, 222, 440, 441, 442, 296,
	443, 112, 259, 444, 123, 268, 269, 270, 114, 272,
	273, 274, 275, 276, 277, 278, 279, 280, 281, 282,
	353, 80, 299, 152, 181, 110, 381, 317, 315, 244,
	169, 105, 355, 78, 53, 27, 56, 400, 13, 108,
	108, 12, 112, 112, 11, 10, 424, 425, 426, 9,
	8, 7, 6, 5, 4, 2, 59, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 72, 0, 0, 0,
	77, 189, 188, 197, 198, 191, 192, 193, 194, 195,
	196, 190, 233, 0, 0, 59, 339, 340, 341, 0,
	91, 0, 0, 0, 95, 0, 0, 98, 0, 0,
	0, 0, 106, 0, 0, 0, 0, 343, 293, 59,
	126, 147, 0, 0, 108, 0, 0, 0, 0, 0,
	0, 166, 0, 0, 167, 0, 0, 0, 360, 363,
	0, 0, 0, 0, 127, 0, 284, 109, 129, 128,
	130, 131, 132, 133, 0, 0, 134, 124, 125, 0,
	0, 111, 0, 139, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 392, 59, 403, 404, 0, 0, 0,
	395, 0, 0, 117, 118, 104, 233, 0, 138, 0,
	119, 0, 120, 0, 0, 0, 0, 0, 0, 233,
	0, 0, 0, 0, 106, 59, 135, 0, 0, 0,
	0, 250, 136, 137, 0, 0, 0, 0, 0, 0,
	0, 0, 415, 0, 0, 0, 0, 416, 0, 0,
	0, 419, 363, 189, 188, 197, 198, 191, 192, 193,
	194, 195, 196, 190, 0, 0, 0, 0, 0, 0,
	0, 0, 106, 106, 0, 0, 0, 0, 0, 0,
	126, 0, 0, 0, 0, 0, 309, 0, 310, 0,
	0, 311, 0, 0, 0, 0, 0, 0, 0, 328,
	0, 59, 0, 0, 127, 391, 284, 109, 129, 128,
	130, 131, 132, 133, 0, 0, 134, 124, 125, 0,
	0, 111, 0, 139, 189, 188, 197, 198, 191, 192,
	193, 194, 195, 196, 190, 0, 0, 0, 0, 0,
	0, 0, 0, 117, 118, 104, 0, 106, 138, 0,
	119, 0, 120, 0, 0, 0, 0, 0, 126, 0,
	0, 0, 0, 0, 0, 0, 135, 0, 59, 59,
	59, 59, 136, 137, 0, 0, 0, 0, 0, 0,
	0, 328, 127, 0, 386, 109, 129, 128, 130, 131,
	132, 133, 0, 0, 134, 124, 125, 0, 0, 111,
	0, 139, 0, 0, 0, 0, 0, 0, 0, 0,
	14, 0, 342, 0, 0, 0, 0, 0, 0, 0,
	0, 117, 118, 104, 0, 126, 138, 0, 119, 0,
	120, 189, 188, 197, 198, 191, 192, 193, 194, 195,
	196, 190, 0, 0, 135, 0, 0, 0, 0, 127,
	136, 137, 109, 129, 128, 130, 131, 132, 133, 0,
	0, 134, 124, 125, 0, 0, 111, 0, 139, 0,
	0, 0, 126, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 14, 0, 0, 0, 0, 117, 118,
	0, 0, 0, 138, 0, 119, 127, 120, 0, 109,
	129, 128, 130, 131, 132, 133, 0, 0, 134, 124,
	125, 135, 0, 111, 0, 139, 0, 136, 137, 0,
	0, 0, 127, 0, 0, 109, 129, 128, 130, 131,
	132, 133, 0, 0, 134, 117, 118, 0, 0, 65,
	138, 139, 119, 0, 120, 0, 0, 0, 127, 0,
	0, 109, 129, 128, 130, 131, 132, 133, 135, 0,
	134, 117, 118, 0, 136, 137, 138, 139, 119, 0,
	120, 189, 188, 197, 198, 191, 192, 193, 194, 195,
	196, 190, 0, 0, 135, 0, 0, 117, 118, 0,
	136, 137, 138, 0, 119, 0, 120, 189, 188, 197,
	198, 191, 192, 193, 194, 195, 196, 190, 0, 0,
	135, 183, 186, 0, 0, 0, 136, 137, 199, 200,
	201, 202, 203, 204, 205, 187, 184, 185, 182, 189,
	188, 197, 198, 191, 192, 193, 194, 195, 196, 190,
}
var yyPact = [...]int{

	43, -1000, -1000, 379, -1000, -1000, -1000, -1000, -1000, -1000,
	-1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, -68,
	-62, -37, -42, -38, -1000, -1000, -1000, 377, 331, 292,
	-1000, -67, 117, 361, 112, -70, -40, 112, -1000, -59,
	112, -1000, 117, -75, 146, -75, 117, -1000, -1000, -1000,
	-1000, -1000, -1000, 253, 166, -1000, 89, 289, 276, -19,
	-1000, 117, 152, -1000, 56, -1000, 117, 67, 128, -1000,
	117, -1000, -58, 117, 325, 235, 112, -1000, 747, -1000,
	347, -1000, 273, 272, -1000, 117, 112, 117, 359, 112,
	913, -1000, 324, -84, -1000, 19, -1000, 117, -1000, -1000,
	117, -1000, 239, -1000, -1000, 209, -20, 44, 959, -1000,
	-1000, 861, 814, -1000, -27, -1000, -1000, 913, 913, 913,
	913, 229, 229, -1000, -1000, -1000, 229, -1000, -1000, -1000,
	-1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, 913,
	117, -1000, -1000, -1000, -1000, 245, 215, -1000, 344, 861,
	-1000, 927, -22, 887, -1000, -1000, 234, 112, -1000, -60,
	-1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, 359,
	747, 102, -1000, -1000, 129, -1000, -1000, 73, 861, 861,
	111, 913, 91, 53, 913, 913, 913, 111, 913, 913,
	913, 913, 913, 913, 913, 913, 913, 913, 913, -1000,
	-1000, -1000, -1000, -1000, -1000, -1000, 39, 959, 88, 237,
	169, 959, 154, 154, -1000, -1000, -1000, 901, 529, 669,
	-1000, 377, 62, 927, -1000, 323, 112, 112, 344, 336,
	341, 44, 123, 927, 117, -1000, -1000, 117, -1000, 356,
	-1000, 225, 282, -1000, -1000, 182, 327, 203, -1000, -1000,
	-24, -1000, 39, 63, -1000, -1000, 130, -1000, -1000, 927,
	-1000, 887, -1000, -1000, 91, 913, 913, 913, 927, 927,
	761, -1000, -4, 334, -1000, 21, 21, -1, -1, -1,
	-1, 9, 9, -1000, -1000, -1000, 913, -1000, -1000, -1000,
	-1000, -1000, 145, 747, -1000, 145, 61, -1000, 861, 233,
	229, 379, 197, 193, -1000, 336, -1000, 913, 913, -28,
	-1000, -1000, 353, 339, 102, 102, 102, 102, -1000, 265,
	257, -1000, 248, 247, 260, -14, -1000, 113, -1000, -1000,
	117, -1000, 178, 52, -1000, -1000, -1000, 169, -1000, 927,
	927, 654, 913, 927, -1000, 145, -1000, 54, -1000, 913,
	96, -1000, 285, 208, -1000, 913, -1000, -1000, 112, -1000,
	431, 204, -1000, 583, 112, -1000, 344, 861, 913, 282,
	232, 228, -1000, -1000, -1000, -1000, 255, -1000, 246, -1000,
	-1000, -1000, -47, -49, -50, -1000, -1000, -1000, -1000, -1000,
	-1000, 913, 927, -1000, -1000, 927, 913, 281, 229, -1000,
	913, 913, -1000, -1000, -1000, 336, 44, 201, 861, 861,
	-1000, -1000, 229, 229, 229, 927, 927, 374, -1000, 927,
	-1000, 326, 44, 44, 112, 112, 112, 112, -1000, 367,
	23, 137, -1000, 137, 137, 152, -1000, 112, -1000, 112,
	-1000, -1000, 112, -1000, -1000,
}
var yyPgo = [...]int{

	0, 495, 20, 494, 493, 492, 491, 490, 489, 485,
	484, 481, 478, 344, 476, 475, 474, 473, 15, 30,
	471, 470, 19, 16, 14, 469, 468, 11, 467, 26,
	466, 3, 27, 1, 465, 464, 462, 23, 260, 461,
	22, 18, 8, 460, 7, 10, 448, 444, 439, 17,
	435, 433, 413, 409, 408, 9, 405, 5, 404, 6,
	403, 402, 401, 12, 4, 76, 399, 305, 255, 398,
	397, 396, 395, 394, 0, 24, 393, 463, 13, 392,
	387, 31, 386, 385, 73, 2,
}
var yyR1 = [...]int{

	0, 82, 1, 1, 1, 1, 1, 1, 1, 1,
	1, 1, 1, 2, 2, 2, 3, 3, 4, 5,
	6, 7, 7, 7, 8, 8, 8, 9, 10, 10,
	10, 11, 12, 12, 12, 83, 13, 14, 14, 15,
	15, 15, 16, 16, 17, 17, 18, 18, 19, 19,
	19, 19, 20, 20, 76, 76, 76, 75, 75, 21,
	21, 22, 22, 23, 23, 24, 24, 24, 25, 25,
	25, 25, 80, 80, 79, 79, 79, 78, 78, 26,
	26, 26, 26, 27, 27, 27, 27, 28, 28, 29,
	29, 30, 30, 30, 30, 31, 31, 32, 32, 33,
	33, 33, 33, 33, 33, 34, 34, 34, 34, 34,
	34, 34, 34, 34, 34, 34, 34, 34, 40, 40,
	40, 40, 40, 40, 35, 35, 35, 35, 35, 35,
	35, 41, 41, 41, 45, 42, 42, 38, 38, 38,
	38, 38, 38, 38, 38, 38, 38, 38, 38, 38,
	38, 38, 38, 38, 38, 38, 38, 38, 38, 38,
	38, 38, 38, 38, 54, 54, 54, 54, 47, 50,
	50, 48, 48, 49, 51, 51, 46, 46, 46, 37,
	37, 37, 37, 37, 37, 37, 39, 39, 39, 52,
	52, 53, 53, 55, 55, 56, 56, 57, 58, 58,
	58, 59, 59, 59, 60, 60, 60, 61, 61, 62,
	62, 63, 63, 36, 36, 43, 43, 44, 64, 64,
	65, 66, 66, 68, 68, 69, 69, 67, 67, 70,
	70, 70, 70, 70, 70, 71, 71, 72, 72, 73,
	73, 74, 77, 84, 85, 81,
}
var yyR2 = [...]int{

	0, 1, 1, 1, 1, 1, 1, 1, 1, 1,
	1, 1, 1, 12, 6, 3, 8, 8, 8, 7,
	3, 5, 8, 4, 6, 7, 4, 5, 4, 5,
	5, 3, 2, 2, 2, 0, 2, 0, 2, 1,
	2, 2, 0, 1, 0, 1, 1, 3, 1, 2,
	3, 5, 1, 1, 0, 1, 2, 1, 1, 0,
	2, 1, 3, 1, 1, 3, 3, 3, 3, 5,
	5, 3, 0, 1, 0, 1, 2, 1, 1, 1,
	2, 2, 1, 2, 3, 2, 3, 2, 2, 1,
	3, 0, 5, 5, 5, 1, 3, 0, 2, 1,
	3, 3, 2, 3, 3, 1, 1, 3, 3, 4,
	3, 4, 3, 4, 5, 6, 3, 2, 1, 2,
	1, 2, 1, 2, 1, 1, 1, 1, 1, 1,
	1, 3, 1, 1, 3, 1, 3, 1, 1, 1,
	1, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 2, 2, 2, 3, 3, 4,
	5, 3, 4, 1, 1, 1, 1, 1, 5, 0,
	1, 1, 2, 4, 0, 2, 1, 3, 5, 1,
	1, 1, 1, 1, 1, 1, 1, 2, 2, 0,
	3, 0, 2, 0, 3, 1, 3, 2, 0, 1,
	1, 0, 2, 4, 0, 2, 4, 0, 3, 1,
	3, 0, 5, 2, 1, 1, 3, 3, 1, 3,
	3, 1, 1, 0, 2, 0, 3, 0, 1, 1,
	1, 1, 1, 1, 1, 0, 1, 0, 1, 0,
	2, 1, 1, 1, 1, 0,
}
var yyChk = [...]int{

	-1000, -82, -1, -2, -3, -4, -5, -6, -7, -8,
	-9, -10, -11, -12, 6, 7, 8, 9, 28, 97,
	98, 100, 99, 101, 110, 111, 112, -15, 5, -13,
	-83, -13, -13, -13, -13, 102, -72, 104, 108, -67,
	104, 106, 102, 102, 103, 104, 102, -81, -81, -81,
	-2, 18, 19, -16, 32, 19, -14, -67, -29, -77,
	48, 10, -64, -65, -74, 48, -69, 107, 103, -74,
	102, -74, -77, -68, 107, 48, -68, -77, -17, 35,
	-39, -74, 51, 54, 56, 24, 28, 94, -29, 46,
	69, -77, 62, 48, -81, -77, -81, 105, -77, 21,
	44, -74, -18, -19, 86, -20, -77, -33, -38, 48,
	-34, 62, -84, -37, -46, -44, -45, 84, 85, 91,
	93, -74, -54, -47, 58, 59, 21, 45, 50, 49,
	51, 52, 53, 54, 57, 107, 113, 114, 89, 64,
	-66, 17, 10, 30, 30, -29, -64, -77, -32, 11,
	-65, -38, -77, -84, -81, 21, -73, 109, -70, 100,
	98, 27, 99, 14, 115, 48, -77, -77, -81, -21,
	46, 10, -76, -75, 20, -74, 50, 94, 61, 60,
	76, -35, 79, 62, 77, 78, 63, 76, 81, 80,
	90, 84, 85, 86, 87, 88, 89, 82, 83, 69,
	70, 71, 72, 73, 74, 75, -33, -38, -33, -2,
	-42, -38, 95, 96, -38, -38, -38, -38, -84, -84,
	-45, -84, -50, -38, -29, -61, 28, -84, -32, -55,
	14, -33, 94, -38, 44, -74, -81, -71, 105, -32,
	-19, -22, -23, -24, -25, -29, -45, -84, -75, 86,
	-77, -74, -33, -33, -40, 57, 62, 58, 59, -38,
	-41, -84, -45, 55, 79, 77, 78, 63, -38, -38,
	-38, -40, -38, -38, -38, -38, -38, -38, -38, -38,
	-38, -38, -38, -85, 47, -85, 46, -85, -37, -37,
	-74, -85, -18, 19, -85, -18, -48, -49, 65, -36,
	30, -2, -64, -62, -74, -55, -59, 16, 15, -77,
	-77, -77, -52, 12, 46, -26, -27, -28, 34, 38,
	40, 35, 36, 37, 41, -79, -78, 20, -77, 50,
	-80, 20, -22, 94, 57, 58, 59, -42, -41, -38,
	-38, -38, 61, -38, -85, -18, -85, -51, -49, 67,
	-33, -63, 44, -43, -44, -84, -63, -85, 46, -59,
	-38, -56, -57, -38, 94, -81, -53, 13, 15, -23,
	-24, -23, -24, 34, 34, 34, 39, 34, 39, 34,
	-27, -30, 42, 106, 43, -78, -77, -85, 86, -74,
	-85, 61, -38, -85, 68, -38, 66, 25, 46, -74,
	46, 46, -58, 22, 23, -55, -33, -42, 44, 44,
	34, 34, 103, 103, 103, -38, -38, 26, -44, -38,
	-57, -59, -33, -33, -84, -84, -84, 8, -60, 17,
	29, -31, -74, -31, -31, -64, 8, 79, -85, 46,
	-85, -85, -74, -74, -74,
}
var yyDef = [...]int{

	0, -2, 1, 2, 3, 4, 5, 6, 7, 8,
	9, 10, 11, 12, 35, 35, 35, 35, 35, 237,
	227, 0, 0, 0, 245, 245, 245, 0, 39, 42,
	37, 227, 0, 0, 0, 225, 0, 0, 238, 0,
	0, 228, 0, 223, 0, 223, 0, 32, 33, 34,
	15, 40, 41, 44, 0, 43, 36, 0, 0, 89,
	242, 0, 20, 218, 0, 241, 0, 0, 0, 245,
	0, 245, 0, 0, 0, 0, 0, 31, 0, 45,
	0, 186, 0, 0, 38, 0, 0, 0, 97, 0,
	0, 245, 0, 239, 23, 0, 26, 0, 28, 224,
	0, 245, 59, 46, 48, 54, 0, 52, 53, -2,
	99, 0, 0, 137, 138, 139, 140, 0, 0, 0,
	0, 176, 0, 163, 105, 106, 0, 243, 179, 180,
	181, 182, 183, 184, 185, 164, 165, 166, 167, 169,
	0, 221, 222, 187, 188, 207, 97, 90, 193, 0,
	219, 220, 0, 0, 21, 226, 0, 0, 245, 235,
	229, 230, 231, 232, 233, 234, 27, 29, 30, 97,
	0, 0, 49, 55, 0, 57, 58, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 124,
	125, 126, 127, 128, 129, 130, 102, 0, 0, 0,
	0, 135, 0, 0, 154, 155, 156, 0, 0, 0,
	117, 0, 0, 170, 14, 0, 0, 0, 193, 201,
	0, 98, 0, 135, 0, 240, 24, 0, 236, 189,
	47, 60, 61, 63, 64, 74, 72, 0, 56, 50,
	0, 177, 100, 101, 104, 118, 0, 120, 122, 107,
	108, 0, 132, 133, 0, 0, 0, 0, 110, 112,
	0, 116, 141, 142, 143, 144, 145, 146, 147, 148,
	149, 150, 151, 103, 244, 134, 0, 217, 152, 153,
	157, 158, 0, 0, 161, 0, 174, 171, 0, 211,
	0, 214, 211, 0, 209, 201, 19, 0, 0, 0,
	245, 25, 191, 0, 0, 0, 0, 0, 79, 0,
	0, 82, 0, 0, 0, 91, 75, 0, 77, 78,
	0, 73, 0, 0, 119, 121, 123, 0, 109, 111,
	113, 0, 0, 136, 159, 0, 162, 0, 172, 0,
	0, 16, 0, 213, 215, 0, 17, 208, 0, 18,
	202, 194, 195, 198, 0, 22, 193, 0, 0, 62,
	68, 0, 71, 80, 81, 83, 0, 85, 0, 87,
	88, 65, 0, 0, 0, 76, 66, 67, 51, 178,
	131, 0, 114, 160, 168, 175, 0, 0, 0, 210,
	0, 0, 197, 199, 200, 201, 192, 190, 0, 0,
	84, 86, 0, 0, 0, 115, 173, 0, 216, 203,
	196, 204, 69, 70, 0, 0, 0, 0, 13, 0,
	0, 0, 95, 0, 0, 212, 205, 0, 92, 0,
	93, 94, 0, 96, 206,
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
			yyVAL.selectExpr = &StarExpr{TableName: &TableName{Name: yyDollar[1].tableIdent}}
		}
	case 51:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:401
		{
			yyVAL.selectExpr = &StarExpr{TableName: &TableName{Qualifier: yyDollar[1].tableIdent, Name: yyDollar[3].tableIdent}}
		}
	case 52:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:407
		{
			yyVAL.expr = yyDollar[1].boolExpr
		}
	case 53:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:411
		{
			yyVAL.expr = yyDollar[1].valExpr
		}
	case 54:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:416
		{
			yyVAL.colIdent = ColIdent{}
		}
	case 55:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:420
		{
			yyVAL.colIdent = yyDollar[1].colIdent
		}
	case 56:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:424
		{
			yyVAL.colIdent = yyDollar[2].colIdent
		}
	case 58:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:431
		{
			yyVAL.colIdent = NewColIdent(string(yyDollar[1].bytes))
		}
	case 59:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:436
		{
			yyVAL.tableExprs = TableExprs{&AliasedTableExpr{Expr: &TableName{Name: NewTableIdent("dual")}}}
		}
	case 60:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:440
		{
			yyVAL.tableExprs = yyDollar[2].tableExprs
		}
	case 61:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:446
		{
			yyVAL.tableExprs = TableExprs{yyDollar[1].tableExpr}
		}
	case 62:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:450
		{
			yyVAL.tableExprs = append(yyVAL.tableExprs, yyDollar[3].tableExpr)
		}
	case 65:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:460
		{
			yyVAL.tableExpr = &AliasedTableExpr{Expr: yyDollar[1].tableName, As: yyDollar[2].tableIdent, Hints: yyDollar[3].indexHints}
		}
	case 66:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:464
		{
			yyVAL.tableExpr = &AliasedTableExpr{Expr: yyDollar[1].subquery, As: yyDollar[3].tableIdent}
		}
	case 67:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:468
		{
			yyVAL.tableExpr = &ParenTableExpr{Exprs: yyDollar[2].tableExprs}
		}
	case 68:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:481
		{
			yyVAL.tableExpr = &JoinTableExpr{LeftExpr: yyDollar[1].tableExpr, Join: yyDollar[2].str, RightExpr: yyDollar[3].tableExpr}
		}
	case 69:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:485
		{
			yyVAL.tableExpr = &JoinTableExpr{LeftExpr: yyDollar[1].tableExpr, Join: yyDollar[2].str, RightExpr: yyDollar[3].tableExpr, On: yyDollar[5].boolExpr}
		}
	case 70:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:489
		{
			yyVAL.tableExpr = &JoinTableExpr{LeftExpr: yyDollar[1].tableExpr, Join: yyDollar[2].str, RightExpr: yyDollar[3].tableExpr, On: yyDollar[5].boolExpr}
		}
	case 71:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:493
		{
			yyVAL.tableExpr = &JoinTableExpr{LeftExpr: yyDollar[1].tableExpr, Join: yyDollar[2].str, RightExpr: yyDollar[3].tableExpr}
		}
	case 72:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:498
		{
			yyVAL.empty = struct{}{}
		}
	case 73:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:500
		{
			yyVAL.empty = struct{}{}
		}
	case 74:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:503
		{
			yyVAL.tableIdent = NewTableIdent("")
		}
	case 75:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:507
		{
			yyVAL.tableIdent = yyDollar[1].tableIdent
		}
	case 76:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:511
		{
			yyVAL.tableIdent = yyDollar[2].tableIdent
		}
	case 78:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:518
		{
			yyVAL.tableIdent = NewTableIdent(string(yyDollar[1].bytes))
		}
	case 79:
		yyDollar = yyS[yypt-1 : yypt+1]
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
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:532
		{
			yyVAL.str = JoinStr
		}
	case 82:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:536
		{
			yyVAL.str = StraightJoinStr
		}
	case 83:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:542
		{
			yyVAL.str = LeftJoinStr
		}
	case 84:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:546
		{
			yyVAL.str = LeftJoinStr
		}
	case 85:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:550
		{
			yyVAL.str = RightJoinStr
		}
	case 86:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:554
		{
			yyVAL.str = RightJoinStr
		}
	case 87:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:560
		{
			yyVAL.str = NaturalJoinStr
		}
	case 88:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:564
		{
			if yyDollar[2].str == LeftJoinStr {
				yyVAL.str = NaturalLeftJoinStr
			} else {
				yyVAL.str = NaturalRightJoinStr
			}
		}
	case 89:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:574
		{
			yyVAL.tableName = &TableName{Name: yyDollar[1].tableIdent}
		}
	case 90:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:578
		{
			yyVAL.tableName = &TableName{Qualifier: yyDollar[1].tableIdent, Name: yyDollar[3].tableIdent}
		}
	case 91:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:583
		{
			yyVAL.indexHints = nil
		}
	case 92:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:587
		{
			yyVAL.indexHints = &IndexHints{Type: UseStr, Indexes: yyDollar[4].colIdents}
		}
	case 93:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:591
		{
			yyVAL.indexHints = &IndexHints{Type: IgnoreStr, Indexes: yyDollar[4].colIdents}
		}
	case 94:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:595
		{
			yyVAL.indexHints = &IndexHints{Type: ForceStr, Indexes: yyDollar[4].colIdents}
		}
	case 95:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:601
		{
			yyVAL.colIdents = []ColIdent{yyDollar[1].colIdent}
		}
	case 96:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:605
		{
			yyVAL.colIdents = append(yyDollar[1].colIdents, yyDollar[3].colIdent)
		}
	case 97:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:610
		{
			yyVAL.boolExpr = nil
		}
	case 98:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:614
		{
			yyVAL.boolExpr = yyDollar[2].boolExpr
		}
	case 100:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:621
		{
			yyVAL.boolExpr = &AndExpr{Left: yyDollar[1].boolExpr, Right: yyDollar[3].boolExpr}
		}
	case 101:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:625
		{
			yyVAL.boolExpr = &OrExpr{Left: yyDollar[1].boolExpr, Right: yyDollar[3].boolExpr}
		}
	case 102:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:629
		{
			yyVAL.boolExpr = &NotExpr{Expr: yyDollar[2].boolExpr}
		}
	case 103:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:633
		{
			yyVAL.boolExpr = &ParenBoolExpr{Expr: yyDollar[2].boolExpr}
		}
	case 104:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:637
		{
			yyVAL.boolExpr = &IsExpr{Operator: yyDollar[3].str, Expr: yyDollar[1].boolExpr}
		}
	case 105:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:643
		{
			yyVAL.boolExpr = BoolVal(true)
		}
	case 106:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:647
		{
			yyVAL.boolExpr = BoolVal(false)
		}
	case 107:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:651
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: yyDollar[2].str, Right: yyDollar[3].valExpr}
		}
	case 108:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:655
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: InStr, Right: yyDollar[3].colTuple}
		}
	case 109:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:659
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: NotInStr, Right: yyDollar[4].colTuple}
		}
	case 110:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:663
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: LikeStr, Right: yyDollar[3].valExpr}
		}
	case 111:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:667
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: NotLikeStr, Right: yyDollar[4].valExpr}
		}
	case 112:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:671
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: RegexpStr, Right: yyDollar[3].valExpr}
		}
	case 113:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:675
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: NotRegexpStr, Right: yyDollar[4].valExpr}
		}
	case 114:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:679
		{
			yyVAL.boolExpr = &RangeCond{Left: yyDollar[1].valExpr, Operator: BetweenStr, From: yyDollar[3].valExpr, To: yyDollar[5].valExpr}
		}
	case 115:
		yyDollar = yyS[yypt-6 : yypt+1]
		//line sql.y:683
		{
			yyVAL.boolExpr = &RangeCond{Left: yyDollar[1].valExpr, Operator: NotBetweenStr, From: yyDollar[4].valExpr, To: yyDollar[6].valExpr}
		}
	case 116:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:687
		{
			yyVAL.boolExpr = &IsExpr{Operator: yyDollar[3].str, Expr: yyDollar[1].valExpr}
		}
	case 117:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:691
		{
			yyVAL.boolExpr = &ExistsExpr{Subquery: yyDollar[2].subquery}
		}
	case 118:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:697
		{
			yyVAL.str = IsNullStr
		}
	case 119:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:701
		{
			yyVAL.str = IsNotNullStr
		}
	case 120:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:705
		{
			yyVAL.str = IsTrueStr
		}
	case 121:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:709
		{
			yyVAL.str = IsNotTrueStr
		}
	case 122:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:713
		{
			yyVAL.str = IsFalseStr
		}
	case 123:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:717
		{
			yyVAL.str = IsNotFalseStr
		}
	case 124:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:723
		{
			yyVAL.str = EqualStr
		}
	case 125:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:727
		{
			yyVAL.str = LessThanStr
		}
	case 126:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:731
		{
			yyVAL.str = GreaterThanStr
		}
	case 127:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:735
		{
			yyVAL.str = LessEqualStr
		}
	case 128:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:739
		{
			yyVAL.str = GreaterEqualStr
		}
	case 129:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:743
		{
			yyVAL.str = NotEqualStr
		}
	case 130:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:747
		{
			yyVAL.str = NullSafeEqualStr
		}
	case 131:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:753
		{
			yyVAL.colTuple = ValTuple(yyDollar[2].valExprs)
		}
	case 132:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:757
		{
			yyVAL.colTuple = yyDollar[1].subquery
		}
	case 133:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:761
		{
			yyVAL.colTuple = ListArg(yyDollar[1].bytes)
		}
	case 134:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:767
		{
			yyVAL.subquery = &Subquery{yyDollar[2].selStmt}
		}
	case 135:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:773
		{
			yyVAL.valExprs = ValExprs{yyDollar[1].valExpr}
		}
	case 136:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:777
		{
			yyVAL.valExprs = append(yyDollar[1].valExprs, yyDollar[3].valExpr)
		}
	case 137:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:783
		{
			yyVAL.valExpr = yyDollar[1].valExpr
		}
	case 138:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:787
		{
			yyVAL.valExpr = yyDollar[1].colName
		}
	case 139:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:791
		{
			yyVAL.valExpr = yyDollar[1].valTuple
		}
	case 140:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:795
		{
			yyVAL.valExpr = yyDollar[1].subquery
		}
	case 141:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:799
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: BitAndStr, Right: yyDollar[3].valExpr}
		}
	case 142:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:803
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: BitOrStr, Right: yyDollar[3].valExpr}
		}
	case 143:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:807
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: BitXorStr, Right: yyDollar[3].valExpr}
		}
	case 144:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:811
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: PlusStr, Right: yyDollar[3].valExpr}
		}
	case 145:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:815
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: MinusStr, Right: yyDollar[3].valExpr}
		}
	case 146:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:819
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: MultStr, Right: yyDollar[3].valExpr}
		}
	case 147:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:823
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: DivStr, Right: yyDollar[3].valExpr}
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
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: ModStr, Right: yyDollar[3].valExpr}
		}
	case 150:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:835
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: ShiftLeftStr, Right: yyDollar[3].valExpr}
		}
	case 151:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:839
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: ShiftRightStr, Right: yyDollar[3].valExpr}
		}
	case 152:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:843
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].colName, Operator: JSONExtractOp, Right: yyDollar[3].valExpr}
		}
	case 153:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:847
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].colName, Operator: JSONUnquoteExtractOp, Right: yyDollar[3].valExpr}
		}
	case 154:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:851
		{
			if num, ok := yyDollar[2].valExpr.(*SQLVal); ok && num.Type == IntVal {
				yyVAL.valExpr = num
			} else {
				yyVAL.valExpr = &UnaryExpr{Operator: UPlusStr, Expr: yyDollar[2].valExpr}
			}
		}
	case 155:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:859
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
	case 156:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:873
		{
			yyVAL.valExpr = &UnaryExpr{Operator: TildaStr, Expr: yyDollar[2].valExpr}
		}
	case 157:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:877
		{
			// This rule prevents the usage of INTERVAL
			// as a function. If support is needed for that,
			// we'll need to revisit this. The solution
			// will be non-trivial because of grammar conflicts.
			yyVAL.valExpr = &IntervalExpr{Expr: yyDollar[2].valExpr, Unit: yyDollar[3].colIdent}
		}
	case 158:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:885
		{
			yyVAL.valExpr = &FuncExpr{Name: yyDollar[1].colIdent}
		}
	case 159:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:889
		{
			yyVAL.valExpr = &FuncExpr{Name: yyDollar[1].colIdent, Exprs: yyDollar[3].selectExprs}
		}
	case 160:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:893
		{
			yyVAL.valExpr = &FuncExpr{Name: yyDollar[1].colIdent, Distinct: true, Exprs: yyDollar[4].selectExprs}
		}
	case 161:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:897
		{
			yyVAL.valExpr = &FuncExpr{Name: yyDollar[1].colIdent}
		}
	case 162:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:901
		{
			yyVAL.valExpr = &FuncExpr{Name: yyDollar[1].colIdent, Exprs: yyDollar[3].selectExprs}
		}
	case 163:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:905
		{
			yyVAL.valExpr = yyDollar[1].caseExpr
		}
	case 164:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:911
		{
			yyVAL.colIdent = NewColIdent("if")
		}
	case 165:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:915
		{
			yyVAL.colIdent = NewColIdent("current_timestamp")
		}
	case 166:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:919
		{
			yyVAL.colIdent = NewColIdent("database")
		}
	case 167:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:923
		{
			yyVAL.colIdent = NewColIdent("mod")
		}
	case 168:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:929
		{
			yyVAL.caseExpr = &CaseExpr{Expr: yyDollar[2].valExpr, Whens: yyDollar[3].whens, Else: yyDollar[4].valExpr}
		}
	case 169:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:934
		{
			yyVAL.valExpr = nil
		}
	case 170:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:938
		{
			yyVAL.valExpr = yyDollar[1].valExpr
		}
	case 171:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:944
		{
			yyVAL.whens = []*When{yyDollar[1].when}
		}
	case 172:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:948
		{
			yyVAL.whens = append(yyDollar[1].whens, yyDollar[2].when)
		}
	case 173:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:954
		{
			yyVAL.when = &When{Cond: yyDollar[2].boolExpr, Val: yyDollar[4].valExpr}
		}
	case 174:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:959
		{
			yyVAL.valExpr = nil
		}
	case 175:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:963
		{
			yyVAL.valExpr = yyDollar[2].valExpr
		}
	case 176:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:969
		{
			yyVAL.colName = &ColName{Name: yyDollar[1].colIdent}
		}
	case 177:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:973
		{
			yyVAL.colName = &ColName{Qualifier: &TableName{Name: yyDollar[1].tableIdent}, Name: yyDollar[3].colIdent}
		}
	case 178:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:977
		{
			yyVAL.colName = &ColName{Qualifier: &TableName{Qualifier: yyDollar[1].tableIdent, Name: yyDollar[3].tableIdent}, Name: yyDollar[5].colIdent}
		}
	case 179:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:983
		{
			yyVAL.valExpr = NewStrVal(yyDollar[1].bytes)
		}
	case 180:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:987
		{
			yyVAL.valExpr = NewHexVal(yyDollar[1].bytes)
		}
	case 181:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:991
		{
			yyVAL.valExpr = NewIntVal(yyDollar[1].bytes)
		}
	case 182:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:995
		{
			yyVAL.valExpr = NewFloatVal(yyDollar[1].bytes)
		}
	case 183:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:999
		{
			yyVAL.valExpr = NewHexNum(yyDollar[1].bytes)
		}
	case 184:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1003
		{
			yyVAL.valExpr = NewValArg(yyDollar[1].bytes)
		}
	case 185:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1007
		{
			yyVAL.valExpr = &NullVal{}
		}
	case 186:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1013
		{
			// TODO(sougou): Deprecate this construct.
			if yyDollar[1].colIdent.Lowered() != "value" {
				yylex.Error("expecting value after next")
				return 1
			}
			yyVAL.valExpr = NewIntVal([]byte("1"))
		}
	case 187:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:1022
		{
			yyVAL.valExpr = NewIntVal(yyDollar[1].bytes)
		}
	case 188:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:1026
		{
			yyVAL.valExpr = NewValArg(yyDollar[1].bytes)
		}
	case 189:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1031
		{
			yyVAL.valExprs = nil
		}
	case 190:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1035
		{
			yyVAL.valExprs = yyDollar[3].valExprs
		}
	case 191:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1040
		{
			yyVAL.boolExpr = nil
		}
	case 192:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:1044
		{
			yyVAL.boolExpr = yyDollar[2].boolExpr
		}
	case 193:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1049
		{
			yyVAL.orderBy = nil
		}
	case 194:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1053
		{
			yyVAL.orderBy = yyDollar[3].orderBy
		}
	case 195:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1059
		{
			yyVAL.orderBy = OrderBy{yyDollar[1].order}
		}
	case 196:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1063
		{
			yyVAL.orderBy = append(yyDollar[1].orderBy, yyDollar[3].order)
		}
	case 197:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:1069
		{
			yyVAL.order = &Order{Expr: yyDollar[1].valExpr, Direction: yyDollar[2].str}
		}
	case 198:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1074
		{
			yyVAL.str = AscScr
		}
	case 199:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1078
		{
			yyVAL.str = AscScr
		}
	case 200:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1082
		{
			yyVAL.str = DescScr
		}
	case 201:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1087
		{
			yyVAL.limit = nil
		}
	case 202:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:1091
		{
			yyVAL.limit = &Limit{Rowcount: yyDollar[2].valExpr}
		}
	case 203:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:1095
		{
			yyVAL.limit = &Limit{Offset: yyDollar[2].valExpr, Rowcount: yyDollar[4].valExpr}
		}
	case 204:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1100
		{
			yyVAL.str = ""
		}
	case 205:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:1104
		{
			yyVAL.str = ForUpdateStr
		}
	case 206:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:1108
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
	case 207:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1121
		{
			yyVAL.columns = nil
		}
	case 208:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1125
		{
			yyVAL.columns = yyDollar[2].columns
		}
	case 209:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1131
		{
			yyVAL.columns = Columns{yyDollar[1].colIdent}
		}
	case 210:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1135
		{
			yyVAL.columns = append(yyVAL.columns, yyDollar[3].colIdent)
		}
	case 211:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1140
		{
			yyVAL.updateExprs = nil
		}
	case 212:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:1144
		{
			yyVAL.updateExprs = yyDollar[5].updateExprs
		}
	case 213:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:1150
		{
			yyVAL.insRows = yyDollar[2].values
		}
	case 214:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1154
		{
			yyVAL.insRows = yyDollar[1].selStmt
		}
	case 215:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1160
		{
			yyVAL.values = Values{yyDollar[1].valTuple}
		}
	case 216:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1164
		{
			yyVAL.values = append(yyDollar[1].values, yyDollar[3].valTuple)
		}
	case 217:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1170
		{
			yyVAL.valTuple = ValTuple(yyDollar[2].valExprs)
		}
	case 218:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1176
		{
			yyVAL.updateExprs = UpdateExprs{yyDollar[1].updateExpr}
		}
	case 219:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1180
		{
			yyVAL.updateExprs = append(yyDollar[1].updateExprs, yyDollar[3].updateExpr)
		}
	case 220:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1186
		{
			yyVAL.updateExpr = &UpdateExpr{Name: yyDollar[1].colIdent, Expr: yyDollar[3].valExpr}
		}
	case 223:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1195
		{
			yyVAL.byt = 0
		}
	case 224:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:1197
		{
			yyVAL.byt = 1
		}
	case 225:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1200
		{
			yyVAL.empty = struct{}{}
		}
	case 226:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1202
		{
			yyVAL.empty = struct{}{}
		}
	case 227:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1205
		{
			yyVAL.str = ""
		}
	case 228:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1207
		{
			yyVAL.str = IgnoreStr
		}
	case 229:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1211
		{
			yyVAL.empty = struct{}{}
		}
	case 230:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1213
		{
			yyVAL.empty = struct{}{}
		}
	case 231:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1215
		{
			yyVAL.empty = struct{}{}
		}
	case 232:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1217
		{
			yyVAL.empty = struct{}{}
		}
	case 233:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1219
		{
			yyVAL.empty = struct{}{}
		}
	case 234:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1221
		{
			yyVAL.empty = struct{}{}
		}
	case 235:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1224
		{
			yyVAL.empty = struct{}{}
		}
	case 236:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1226
		{
			yyVAL.empty = struct{}{}
		}
	case 237:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1229
		{
			yyVAL.empty = struct{}{}
		}
	case 238:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1231
		{
			yyVAL.empty = struct{}{}
		}
	case 239:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1234
		{
			yyVAL.empty = struct{}{}
		}
	case 240:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:1236
		{
			yyVAL.empty = struct{}{}
		}
	case 241:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1240
		{
			yyVAL.colIdent = NewColIdent(string(yyDollar[1].bytes))
		}
	case 242:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1246
		{
			yyVAL.tableIdent = NewTableIdent(string(yyDollar[1].bytes))
		}
	case 243:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1252
		{
			if incNesting(yylex) {
				yylex.Error("max nesting level reached")
				return 1
			}
		}
	case 244:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1261
		{
			decNesting(yylex)
		}
	case 245:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1266
		{
			forceEOF(yylex)
		}
	}
	goto yystack /* stack new state and value */
}
