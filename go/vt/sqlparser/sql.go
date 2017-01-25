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
	-1, 112,
	96, 245,
	-2, 244,
}

const yyNprod = 249
const yyPrivate = 57344

var yyTokenNames []string
var yyStates []string

const yyLast = 868

var yyAct = [...]int{

	123, 291, 135, 432, 62, 312, 363, 222, 234, 247,
	117, 322, 332, 246, 111, 301, 245, 303, 265, 153,
	104, 115, 105, 249, 47, 221, 3, 161, 35, 74,
	37, 67, 242, 113, 38, 64, 383, 385, 69, 180,
	40, 71, 41, 41, 99, 167, 43, 44, 45, 118,
	48, 49, 415, 50, 414, 81, 58, 413, 68, 165,
	141, 70, 46, 112, 129, 128, 130, 131, 132, 133,
	42, 365, 134, 126, 127, 213, 214, 103, 342, 140,
	169, 87, 237, 184, 89, 90, 203, 194, 149, 64,
	203, 107, 64, 151, 96, 437, 98, 63, 179, 119,
	120, 384, 92, 395, 139, 177, 121, 65, 182, 122,
	150, 302, 197, 198, 199, 200, 194, 212, 158, 203,
	179, 84, 112, 136, 94, 65, 219, 220, 172, 137,
	138, 164, 166, 163, 215, 216, 217, 218, 195, 196,
	197, 198, 199, 200, 194, 389, 60, 203, 168, 64,
	232, 439, 294, 230, 272, 141, 14, 15, 16, 17,
	260, 302, 239, 355, 211, 268, 226, 112, 270, 271,
	269, 233, 95, 229, 339, 340, 341, 75, 251, 18,
	178, 177, 182, 91, 156, 262, 397, 240, 298, 155,
	266, 404, 405, 243, 174, 294, 179, 244, 255, 257,
	258, 264, 185, 256, 273, 274, 275, 276, 277, 278,
	279, 280, 281, 282, 283, 284, 285, 286, 287, 290,
	263, 259, 295, 297, 299, 250, 60, 223, 335, 178,
	177, 307, 225, 309, 402, 288, 289, 267, 262, 306,
	292, 296, 311, 181, 358, 179, 236, 320, 308, 19,
	20, 22, 21, 23, 251, 65, 154, 14, 82, 178,
	177, 83, 24, 25, 26, 333, 86, 141, 338, 252,
	253, 65, 266, 183, 343, 179, 65, 141, 183, 28,
	60, 298, 294, 178, 177, 345, 346, 347, 344, 73,
	60, 409, 91, 60, 349, 335, 79, 141, 351, 179,
	60, 250, 201, 202, 195, 196, 197, 198, 199, 200,
	194, 359, 175, 203, 350, 148, 354, 360, 304, 267,
	91, 157, 294, 251, 251, 251, 251, 320, 294, 310,
	294, 371, 141, 373, 370, 76, 372, 401, 304, 238,
	388, 366, 381, 390, 102, 391, 386, 412, 378, 174,
	376, 411, 394, 379, 59, 377, 375, 380, 223, 328,
	329, 307, 374, 393, 72, 55, 390, 400, 77, 399,
	250, 250, 250, 250, 430, 146, 406, 408, 54, 59,
	145, 178, 177, 59, 88, 14, 431, 398, 93, 357,
	352, 159, 97, 39, 356, 100, 101, 179, 337, 223,
	108, 51, 52, 313, 369, 361, 364, 416, 59, 421,
	228, 152, 422, 314, 144, 235, 425, 426, 427, 64,
	368, 170, 143, 428, 171, 57, 433, 433, 433, 319,
	434, 435, 154, 61, 438, 436, 440, 441, 442, 418,
	443, 14, 293, 444, 114, 28, 30, 396, 1, 193,
	192, 201, 202, 195, 196, 197, 198, 199, 200, 194,
	407, 223, 203, 336, 59, 331, 176, 160, 141, 36,
	294, 112, 129, 128, 130, 131, 132, 133, 241, 162,
	134, 126, 127, 66, 142, 110, 231, 140, 147, 417,
	429, 403, 419, 420, 364, 362, 108, 59, 124, 367,
	392, 423, 424, 318, 353, 224, 261, 119, 120, 106,
	300, 125, 139, 116, 121, 305, 254, 122, 80, 193,
	192, 201, 202, 195, 196, 197, 198, 199, 200, 194,
	227, 136, 203, 186, 109, 382, 85, 137, 138, 323,
	321, 108, 108, 192, 201, 202, 195, 196, 197, 198,
	199, 200, 194, 114, 248, 203, 173, 78, 53, 315,
	316, 27, 56, 317, 129, 128, 130, 131, 132, 133,
	13, 334, 134, 59, 12, 11, 10, 141, 348, 294,
	112, 129, 128, 130, 131, 132, 133, 9, 8, 134,
	126, 127, 7, 6, 110, 29, 140, 193, 192, 201,
	202, 195, 196, 197, 198, 199, 200, 194, 5, 4,
	203, 31, 32, 33, 34, 108, 119, 120, 106, 2,
	0, 139, 0, 121, 0, 0, 122, 0, 0, 0,
	0, 0, 114, 0, 14, 0, 0, 0, 0, 0,
	136, 0, 59, 59, 59, 59, 137, 138, 0, 0,
	114, 0, 0, 0, 0, 334, 141, 0, 387, 112,
	129, 128, 130, 131, 132, 133, 0, 0, 134, 126,
	127, 0, 0, 110, 141, 140, 0, 112, 129, 128,
	130, 131, 132, 133, 0, 0, 134, 126, 127, 0,
	0, 110, 0, 140, 0, 119, 120, 106, 0, 0,
	139, 0, 121, 0, 0, 122, 0, 0, 0, 0,
	0, 0, 0, 119, 120, 0, 0, 0, 139, 136,
	121, 114, 0, 122, 0, 137, 138, 141, 0, 0,
	112, 129, 128, 130, 131, 132, 133, 136, 0, 134,
	0, 0, 0, 137, 138, 141, 140, 0, 112, 129,
	128, 130, 131, 132, 133, 0, 0, 134, 126, 127,
	0, 0, 110, 0, 140, 0, 119, 120, 0, 0,
	0, 139, 0, 121, 0, 0, 122, 0, 0, 0,
	0, 0, 0, 0, 119, 120, 0, 0, 0, 139,
	136, 121, 0, 0, 122, 0, 137, 138, 324, 327,
	328, 329, 325, 0, 326, 330, 0, 0, 136, 188,
	191, 65, 0, 0, 137, 138, 204, 205, 206, 207,
	208, 209, 210, 0, 189, 190, 187, 193, 192, 201,
	202, 195, 196, 197, 198, 199, 200, 194, 0, 0,
	203, 0, 0, 193, 192, 201, 202, 195, 196, 197,
	198, 199, 200, 194, 0, 0, 203, 324, 327, 328,
	329, 325, 0, 326, 330, 0, 0, 410,
}
var yyPact = [...]int{

	150, -1000, -1000, 440, -1000, -1000, -1000, -1000, -1000, -1000,
	-1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, -76,
	-66, -34, -58, -42, -1000, -1000, -1000, 435, 382, 345,
	-1000, -65, 97, 423, 76, -78, -47, 76, -1000, -43,
	76, -1000, 97, -80, 128, -80, 97, -1000, -1000, -1000,
	-1000, -1000, -1000, 260, 206, -1000, 64, 241, 355, -12,
	-1000, 97, 136, -1000, 32, -1000, 97, 61, 123, -1000,
	97, -1000, -63, 97, 374, 299, 76, -1000, 610, -1000,
	404, -1000, 349, 344, -1000, 286, 97, -1000, 76, 97,
	421, 76, 699, -1000, 369, -84, -1000, 31, -1000, 97,
	-1000, -1000, 97, -1000, 302, -1000, -1000, 222, -13, -1000,
	699, 746, -1000, -1000, 221, -1000, -22, -1000, -1000, 681,
	681, 681, 681, 221, 221, -1000, -1000, -1000, -1000, -1000,
	-1000, -1000, -1000, -1000, -1000, 628, -1000, -1000, -1000, -1000,
	699, -1000, 97, -1000, -1000, -1000, -1000, 379, 76, 76,
	-1000, 245, -1000, 401, 699, -1000, 198, -14, -1000, -1000,
	294, 76, -1000, -75, -1000, -1000, -1000, -1000, -1000, -1000,
	-1000, -1000, -1000, 421, 610, 231, -1000, 699, 699, 140,
	-1000, 227, -1000, -1000, 73, 21, 14, 109, 90, 681,
	681, 681, 681, 681, 681, 681, 681, 681, 681, 681,
	681, 681, 681, 681, -1000, -1000, -1000, -1000, -1000, -1000,
	-1000, -1000, 435, 514, 514, -8, -8, -8, 762, 422,
	531, 274, 234, 198, 45, 198, -1000, 293, 221, 440,
	273, 282, -1000, 401, 387, 398, 198, 118, 97, -1000,
	-1000, 97, -1000, 417, -1000, 200, 763, -1000, -1000, 244,
	377, 251, 21, 43, -1000, -1000, 116, -1000, -1000, -1000,
	-1000, -18, -1000, -1000, 368, -1000, 628, -1000, -1000, 109,
	681, 681, 681, 368, 368, 516, 219, 461, -8, 25,
	25, -4, -4, -4, -4, 53, 53, -1000, -1000, -1000,
	-1000, -1000, 147, 610, -1000, -1000, 147, -1000, 699, -1000,
	95, -1000, 699, -1000, 363, 197, -1000, 699, -1000, -1000,
	76, 387, -1000, 699, 699, -25, -1000, -1000, 407, 389,
	231, 231, 231, 231, -1000, 327, 321, -1000, 315, 313,
	322, -7, -1000, 177, -1000, -1000, 97, -1000, 280, -1000,
	-1000, -1000, 58, 234, -1000, 368, 368, 438, 681, -1000,
	147, -1000, 198, 34, -1000, 699, 119, 360, 221, -1000,
	-1000, 320, 187, -1000, 168, 76, -1000, 401, 699, 699,
	763, 246, 822, -1000, -1000, -1000, -1000, 316, -1000, 312,
	-1000, -1000, -1000, -48, -51, -53, -1000, -1000, -1000, -1000,
	-1000, -1000, 681, 368, -1000, -1000, 198, 699, 431, -1000,
	699, 699, 699, -1000, -1000, -1000, 387, 198, 141, 699,
	699, -1000, -1000, 221, 221, 221, 368, 198, 76, 198,
	198, -1000, 356, 198, 198, 76, 76, 76, 136, -1000,
	427, 15, 104, -1000, 104, 104, -1000, 76, -1000, 76,
	-1000, -1000, 76, -1000, -1000,
}
var yyPgo = [...]int{

	0, 619, 25, 609, 608, 593, 592, 588, 587, 576,
	575, 574, 570, 595, 562, 561, 558, 557, 20, 22,
	91, 556, 16, 13, 9, 554, 540, 11, 539, 23,
	536, 535, 3, 19, 534, 33, 533, 530, 21, 14,
	518, 516, 18, 7, 515, 10, 49, 513, 511, 510,
	15, 505, 504, 503, 499, 498, 8, 495, 6, 491,
	5, 490, 488, 486, 17, 4, 97, 484, 393, 289,
	483, 479, 478, 469, 467, 0, 39, 466, 321, 12,
	465, 463, 24, 448, 446, 2, 1,
}
var yyR1 = [...]int{

	0, 83, 1, 1, 1, 1, 1, 1, 1, 1,
	1, 1, 1, 2, 2, 2, 3, 3, 4, 5,
	6, 7, 7, 7, 8, 8, 8, 9, 10, 10,
	10, 11, 12, 12, 12, 84, 13, 14, 14, 15,
	15, 15, 16, 16, 17, 17, 18, 18, 19, 19,
	19, 19, 77, 77, 77, 76, 76, 21, 21, 22,
	22, 23, 23, 24, 24, 24, 25, 25, 25, 25,
	81, 81, 80, 80, 80, 79, 79, 26, 26, 26,
	26, 27, 27, 27, 27, 28, 28, 30, 30, 29,
	29, 31, 31, 31, 31, 32, 32, 33, 33, 20,
	20, 20, 20, 20, 20, 35, 35, 34, 34, 34,
	34, 34, 34, 34, 34, 34, 34, 34, 34, 41,
	41, 41, 41, 41, 41, 36, 36, 36, 36, 36,
	36, 36, 42, 42, 42, 46, 43, 43, 39, 39,
	39, 39, 39, 39, 39, 39, 39, 39, 39, 39,
	39, 39, 39, 39, 39, 39, 39, 39, 39, 39,
	39, 39, 39, 39, 39, 39, 55, 55, 55, 55,
	48, 51, 51, 49, 49, 50, 52, 52, 47, 47,
	47, 38, 38, 38, 38, 38, 38, 38, 40, 40,
	40, 53, 53, 54, 54, 56, 56, 57, 57, 58,
	59, 59, 59, 60, 60, 60, 60, 61, 61, 61,
	62, 62, 63, 63, 64, 64, 37, 37, 44, 44,
	45, 65, 65, 66, 67, 67, 69, 69, 70, 70,
	68, 68, 71, 71, 71, 71, 71, 71, 72, 72,
	73, 73, 74, 74, 75, 78, 85, 86, 82,
}
var yyR2 = [...]int{

	0, 1, 1, 1, 1, 1, 1, 1, 1, 1,
	1, 1, 1, 12, 6, 3, 7, 7, 8, 7,
	3, 5, 8, 4, 6, 7, 4, 5, 4, 5,
	5, 3, 2, 2, 2, 0, 2, 0, 2, 1,
	2, 2, 0, 1, 0, 1, 1, 3, 1, 2,
	3, 5, 0, 1, 2, 1, 1, 0, 2, 1,
	3, 1, 1, 3, 3, 3, 3, 5, 5, 3,
	0, 1, 0, 1, 2, 1, 1, 1, 2, 2,
	1, 2, 3, 2, 3, 2, 2, 2, 1, 1,
	3, 0, 5, 5, 5, 1, 3, 0, 2, 1,
	3, 3, 2, 3, 1, 1, 1, 1, 3, 3,
	3, 4, 3, 4, 3, 4, 5, 6, 2, 1,
	2, 1, 2, 1, 2, 1, 1, 1, 1, 1,
	1, 1, 3, 1, 1, 3, 1, 3, 1, 1,
	1, 1, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 2, 2, 2, 3,
	3, 4, 5, 3, 4, 1, 1, 1, 1, 1,
	5, 0, 1, 1, 2, 4, 0, 2, 1, 3,
	5, 1, 1, 1, 1, 1, 1, 1, 1, 2,
	2, 0, 3, 0, 2, 0, 3, 1, 3, 2,
	0, 1, 1, 0, 2, 4, 4, 0, 2, 4,
	0, 3, 1, 3, 0, 5, 2, 1, 1, 3,
	3, 1, 3, 3, 1, 1, 0, 2, 0, 3,
	0, 1, 1, 1, 1, 1, 1, 1, 0, 1,
	0, 1, 0, 2, 1, 1, 1, 1, 0,
}
var yyChk = [...]int{

	-1000, -83, -1, -2, -3, -4, -5, -6, -7, -8,
	-9, -10, -11, -12, 6, 7, 8, 9, 29, 99,
	100, 102, 101, 103, 112, 113, 114, -15, 5, -13,
	-84, -13, -13, -13, -13, 104, -73, 106, 110, -68,
	106, 108, 104, 104, 105, 106, 104, -82, -82, -82,
	-2, 19, 20, -16, 33, 20, -14, -68, -29, -78,
	49, 10, -65, -66, -75, 49, -70, 109, 105, -75,
	104, -75, -78, -69, 109, 49, -69, -78, -17, 36,
	-40, -75, 52, 55, 57, -30, 25, -29, 29, 96,
	-29, 47, 70, -78, 63, 49, -82, -78, -82, 107,
	-78, 22, 45, -75, -18, -19, 87, -20, -78, -34,
	63, -39, 49, -35, 22, -38, -47, -45, -46, 85,
	86, 92, 95, -75, -55, -48, 59, 60, 51, 50,
	52, 53, 54, 55, 58, -85, 109, 115, 116, 90,
	65, 46, -67, 18, 10, 31, 31, -62, 29, -85,
	-29, -65, -78, -33, 11, -66, -20, -78, -82, 22,
	-74, 111, -71, 102, 100, 28, 101, 14, 117, 49,
	-78, -78, -82, -21, 47, 10, -77, 62, 61, 77,
	-76, 21, -75, 51, 96, -20, -36, 80, 63, 78,
	79, 64, 82, 81, 91, 85, 86, 87, 88, 89,
	90, 83, 84, 94, 70, 71, 72, 73, 74, 75,
	76, -46, -85, 97, 98, -39, -39, -39, -39, -85,
	-85, -2, -43, -20, -51, -20, -29, -37, 31, -2,
	-65, -63, -75, -33, -56, 14, -20, 96, 45, -75,
	-82, -72, 107, -33, -19, -22, -23, -24, -25, -29,
	-46, -85, -20, -20, -41, 58, 63, 59, 60, -76,
	87, -78, -75, -35, -39, -42, -85, -46, 56, 80,
	78, 79, 64, -39, -39, -39, -39, -39, -39, -39,
	-39, -39, -39, -39, -39, -39, -39, -39, -38, -38,
	-75, -86, -18, 20, 48, -86, -18, -86, 47, -86,
	-49, -50, 66, -64, 45, -44, -45, -85, -64, -86,
	47, -56, -60, 16, 15, -78, -78, -78, -53, 12,
	47, -26, -27, -28, 35, 39, 41, 36, 37, 38,
	42, -80, -79, 21, -78, 51, -81, 21, -22, 58,
	59, 60, 96, -43, -42, -39, -39, -39, 62, -86,
	-18, -86, -20, -52, -50, 68, -20, 26, 47, -75,
	-60, -20, -57, -58, -20, 96, -82, -54, 13, 15,
	-23, -24, -23, -24, 35, 35, 35, 40, 35, 40,
	35, -27, -31, 43, 108, 44, -79, -78, -86, 87,
	-75, -86, 62, -39, -86, 69, -20, 67, 27, -45,
	47, 17, 47, -59, 23, 24, -56, -20, -43, 45,
	45, 35, 35, 105, 105, 105, -39, -20, 8, -20,
	-20, -58, -60, -20, -20, -85, -85, -85, -65, -61,
	18, 30, -32, -75, -32, -32, 8, 80, -86, 47,
	-86, -86, -75, -75, -75,
}
var yyDef = [...]int{

	0, -2, 1, 2, 3, 4, 5, 6, 7, 8,
	9, 10, 11, 12, 35, 35, 35, 35, 35, 240,
	230, 0, 0, 0, 248, 248, 248, 0, 39, 42,
	37, 230, 0, 0, 0, 228, 0, 0, 241, 0,
	0, 231, 0, 226, 0, 226, 0, 32, 33, 34,
	15, 40, 41, 44, 0, 43, 36, 0, 0, 89,
	245, 0, 20, 221, 0, 244, 0, 0, 0, 248,
	0, 248, 0, 0, 0, 0, 0, 31, 0, 45,
	0, 188, 0, 0, 38, 210, 0, 88, 0, 0,
	97, 0, 0, 248, 0, 242, 23, 0, 26, 0,
	28, 227, 0, 248, 57, 46, 48, 52, 0, 99,
	0, 104, -2, 107, 0, 138, 139, 140, 141, 0,
	0, 0, 0, 178, 0, 165, 105, 106, 181, 182,
	183, 184, 185, 186, 187, 0, 166, 167, 168, 169,
	171, 246, 0, 224, 225, 189, 190, 0, 0, 0,
	87, 97, 90, 195, 0, 222, 223, 0, 21, 229,
	0, 0, 248, 238, 232, 233, 234, 235, 236, 237,
	27, 29, 30, 97, 0, 0, 49, 0, 0, 0,
	53, 0, 55, 56, 0, 102, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 125, 126, 127, 128, 129, 130,
	131, 118, 0, 0, 0, 156, 157, 158, 0, 0,
	0, 0, 0, 136, 0, 172, 14, 214, 0, 217,
	214, 0, 212, 195, 203, 0, 98, 0, 0, 243,
	24, 0, 239, 191, 47, 58, 59, 61, 62, 72,
	70, 0, 100, 101, 103, 119, 0, 121, 123, 54,
	50, 0, 179, 108, 109, 110, 0, 133, 134, 0,
	0, 0, 0, 112, 114, 0, 142, 143, 144, 145,
	146, 147, 148, 149, 150, 151, 152, 155, 153, 154,
	159, 160, 0, 0, 247, 163, 0, 135, 0, 220,
	176, 173, 0, 16, 0, 216, 218, 0, 17, 211,
	0, 203, 19, 0, 0, 0, 248, 25, 193, 0,
	0, 0, 0, 0, 77, 0, 0, 80, 0, 0,
	0, 91, 73, 0, 75, 76, 0, 71, 0, 120,
	122, 124, 0, 0, 111, 113, 115, 0, 0, 161,
	0, 164, 137, 0, 174, 0, 0, 0, 0, 213,
	18, 204, 196, 197, 200, 0, 22, 195, 0, 0,
	60, 66, 0, 69, 78, 79, 81, 0, 83, 0,
	85, 86, 63, 0, 0, 0, 74, 64, 65, 51,
	180, 132, 0, 116, 162, 170, 177, 0, 0, 219,
	0, 0, 0, 199, 201, 202, 203, 194, 192, 0,
	0, 82, 84, 0, 0, 0, 117, 175, 0, 205,
	206, 198, 207, 67, 68, 0, 0, 0, 215, 13,
	0, 0, 0, 95, 0, 0, 208, 0, 92, 0,
	93, 94, 0, 96, 209,
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
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:409
		{
			yyVAL.colIdent = ColIdent{}
		}
	case 53:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:413
		{
			yyVAL.colIdent = yyDollar[1].colIdent
		}
	case 54:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:417
		{
			yyVAL.colIdent = yyDollar[2].colIdent
		}
	case 56:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:424
		{
			yyVAL.colIdent = NewColIdent(string(yyDollar[1].bytes))
		}
	case 57:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:429
		{
			yyVAL.tableExprs = TableExprs{&AliasedTableExpr{Expr: &TableName{Name: NewTableIdent("dual")}}}
		}
	case 58:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:433
		{
			yyVAL.tableExprs = yyDollar[2].tableExprs
		}
	case 59:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:439
		{
			yyVAL.tableExprs = TableExprs{yyDollar[1].tableExpr}
		}
	case 60:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:443
		{
			yyVAL.tableExprs = append(yyVAL.tableExprs, yyDollar[3].tableExpr)
		}
	case 63:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:453
		{
			yyVAL.tableExpr = &AliasedTableExpr{Expr: yyDollar[1].tableName, As: yyDollar[2].tableIdent, Hints: yyDollar[3].indexHints}
		}
	case 64:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:457
		{
			yyVAL.tableExpr = &AliasedTableExpr{Expr: yyDollar[1].subquery, As: yyDollar[3].tableIdent}
		}
	case 65:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:461
		{
			yyVAL.tableExpr = &ParenTableExpr{Exprs: yyDollar[2].tableExprs}
		}
	case 66:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:474
		{
			yyVAL.tableExpr = &JoinTableExpr{LeftExpr: yyDollar[1].tableExpr, Join: yyDollar[2].str, RightExpr: yyDollar[3].tableExpr}
		}
	case 67:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:478
		{
			yyVAL.tableExpr = &JoinTableExpr{LeftExpr: yyDollar[1].tableExpr, Join: yyDollar[2].str, RightExpr: yyDollar[3].tableExpr, On: yyDollar[5].expr}
		}
	case 68:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:482
		{
			yyVAL.tableExpr = &JoinTableExpr{LeftExpr: yyDollar[1].tableExpr, Join: yyDollar[2].str, RightExpr: yyDollar[3].tableExpr, On: yyDollar[5].expr}
		}
	case 69:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:486
		{
			yyVAL.tableExpr = &JoinTableExpr{LeftExpr: yyDollar[1].tableExpr, Join: yyDollar[2].str, RightExpr: yyDollar[3].tableExpr}
		}
	case 70:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:491
		{
			yyVAL.empty = struct{}{}
		}
	case 71:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:493
		{
			yyVAL.empty = struct{}{}
		}
	case 72:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:496
		{
			yyVAL.tableIdent = NewTableIdent("")
		}
	case 73:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:500
		{
			yyVAL.tableIdent = yyDollar[1].tableIdent
		}
	case 74:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:504
		{
			yyVAL.tableIdent = yyDollar[2].tableIdent
		}
	case 76:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:511
		{
			yyVAL.tableIdent = NewTableIdent(string(yyDollar[1].bytes))
		}
	case 77:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:517
		{
			yyVAL.str = JoinStr
		}
	case 78:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:521
		{
			yyVAL.str = JoinStr
		}
	case 79:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:525
		{
			yyVAL.str = JoinStr
		}
	case 80:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:529
		{
			yyVAL.str = StraightJoinStr
		}
	case 81:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:535
		{
			yyVAL.str = LeftJoinStr
		}
	case 82:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:539
		{
			yyVAL.str = LeftJoinStr
		}
	case 83:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:543
		{
			yyVAL.str = RightJoinStr
		}
	case 84:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:547
		{
			yyVAL.str = RightJoinStr
		}
	case 85:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:553
		{
			yyVAL.str = NaturalJoinStr
		}
	case 86:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:557
		{
			if yyDollar[2].str == LeftJoinStr {
				yyVAL.str = NaturalLeftJoinStr
			} else {
				yyVAL.str = NaturalRightJoinStr
			}
		}
	case 87:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:567
		{
			yyVAL.tableName = yyDollar[2].tableName
		}
	case 88:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:571
		{
			yyVAL.tableName = yyDollar[1].tableName
		}
	case 89:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:577
		{
			yyVAL.tableName = &TableName{Name: yyDollar[1].tableIdent}
		}
	case 90:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:581
		{
			yyVAL.tableName = &TableName{Qualifier: yyDollar[1].tableIdent, Name: yyDollar[3].tableIdent}
		}
	case 91:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:586
		{
			yyVAL.indexHints = nil
		}
	case 92:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:590
		{
			yyVAL.indexHints = &IndexHints{Type: UseStr, Indexes: yyDollar[4].colIdents}
		}
	case 93:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:594
		{
			yyVAL.indexHints = &IndexHints{Type: IgnoreStr, Indexes: yyDollar[4].colIdents}
		}
	case 94:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:598
		{
			yyVAL.indexHints = &IndexHints{Type: ForceStr, Indexes: yyDollar[4].colIdents}
		}
	case 95:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:604
		{
			yyVAL.colIdents = []ColIdent{yyDollar[1].colIdent}
		}
	case 96:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:608
		{
			yyVAL.colIdents = append(yyDollar[1].colIdents, yyDollar[3].colIdent)
		}
	case 97:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:613
		{
			yyVAL.boolExpr = nil
		}
	case 98:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:617
		{
			yyVAL.boolExpr = yyDollar[2].expr
		}
	case 99:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:623
		{
			yyVAL.expr = nil
		}
	case 100:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:627
		{
			yyVAL.expr = &AndExpr{Left: yyDollar[1].expr, Right: yyDollar[3].expr}
		}
	case 101:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:631
		{
			yyVAL.expr = &OrExpr{Left: yyDollar[1].expr, Right: yyDollar[3].expr}
		}
	case 102:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:635
		{
			yyVAL.expr = &NotExpr{Expr: yyDollar[2].expr}
		}
	case 103:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:639
		{
			yyVAL.expr = &IsExpr{Operator: yyDollar[3].str, Expr: yyDollar[1].expr}
		}
	case 104:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:643
		{
			yyVAL.expr = nil
		}
	case 105:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:649
		{
			yyVAL.boolVal = BoolVal(true)
		}
	case 106:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:653
		{
			yyVAL.boolVal = BoolVal(false)
		}
	case 107:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:659
		{
			yyVAL.boolExpr = yyDollar[1].boolVal
		}
	case 108:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:663
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: yyDollar[2].str, Right: yyDollar[3].boolVal}
		}
	case 109:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:667
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: yyDollar[2].str, Right: yyDollar[3].valExpr}
		}
	case 110:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:671
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: InStr, Right: yyDollar[3].colTuple}
		}
	case 111:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:675
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: NotInStr, Right: yyDollar[4].colTuple}
		}
	case 112:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:679
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: LikeStr, Right: yyDollar[3].valExpr}
		}
	case 113:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:683
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: NotLikeStr, Right: yyDollar[4].valExpr}
		}
	case 114:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:687
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: RegexpStr, Right: yyDollar[3].valExpr}
		}
	case 115:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:691
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: NotRegexpStr, Right: yyDollar[4].valExpr}
		}
	case 116:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:695
		{
			yyVAL.boolExpr = &RangeCond{Left: yyDollar[1].valExpr, Operator: BetweenStr, From: yyDollar[3].valExpr, To: yyDollar[5].valExpr}
		}
	case 117:
		yyDollar = yyS[yypt-6 : yypt+1]
		//line sql.y:699
		{
			yyVAL.boolExpr = &RangeCond{Left: yyDollar[1].valExpr, Operator: NotBetweenStr, From: yyDollar[4].valExpr, To: yyDollar[6].valExpr}
		}
	case 118:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:703
		{
			yyVAL.boolExpr = &ExistsExpr{Subquery: yyDollar[2].subquery}
		}
	case 119:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:709
		{
			yyVAL.str = IsNullStr
		}
	case 120:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:713
		{
			yyVAL.str = IsNotNullStr
		}
	case 121:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:717
		{
			yyVAL.str = IsTrueStr
		}
	case 122:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:721
		{
			yyVAL.str = IsNotTrueStr
		}
	case 123:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:725
		{
			yyVAL.str = IsFalseStr
		}
	case 124:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:729
		{
			yyVAL.str = IsNotFalseStr
		}
	case 125:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:735
		{
			yyVAL.str = EqualStr
		}
	case 126:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:739
		{
			yyVAL.str = LessThanStr
		}
	case 127:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:743
		{
			yyVAL.str = GreaterThanStr
		}
	case 128:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:747
		{
			yyVAL.str = LessEqualStr
		}
	case 129:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:751
		{
			yyVAL.str = GreaterEqualStr
		}
	case 130:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:755
		{
			yyVAL.str = NotEqualStr
		}
	case 131:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:759
		{
			yyVAL.str = NullSafeEqualStr
		}
	case 132:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:765
		{
			yyVAL.colTuple = ValTuple(yyDollar[2].valExprs)
		}
	case 133:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:769
		{
			yyVAL.colTuple = yyDollar[1].subquery
		}
	case 134:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:773
		{
			yyVAL.colTuple = ListArg(yyDollar[1].bytes)
		}
	case 135:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:779
		{
			yyVAL.subquery = &Subquery{yyDollar[2].selStmt}
		}
	case 136:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:785
		{
			yyVAL.valExprs = ValExprs{yyDollar[1].expr}
		}
	case 137:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:789
		{
			yyVAL.valExprs = append(yyDollar[1].valExprs, yyDollar[3].expr)
		}
	case 138:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:795
		{
			yyVAL.valExpr = yyDollar[1].valExpr
		}
	case 139:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:799
		{
			yyVAL.valExpr = yyDollar[1].colName
		}
	case 140:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:803
		{
			yyVAL.valExpr = yyDollar[1].valTuple
		}
	case 141:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:807
		{
			yyVAL.valExpr = yyDollar[1].subquery
		}
	case 142:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:811
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: BitAndStr, Right: yyDollar[3].valExpr}
		}
	case 143:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:815
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: BitOrStr, Right: yyDollar[3].valExpr}
		}
	case 144:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:819
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: BitXorStr, Right: yyDollar[3].valExpr}
		}
	case 145:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:823
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: PlusStr, Right: yyDollar[3].valExpr}
		}
	case 146:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:827
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: MinusStr, Right: yyDollar[3].valExpr}
		}
	case 147:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:831
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: MultStr, Right: yyDollar[3].valExpr}
		}
	case 148:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:835
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: DivStr, Right: yyDollar[3].valExpr}
		}
	case 149:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:839
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: ModStr, Right: yyDollar[3].valExpr}
		}
	case 150:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:843
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: ModStr, Right: yyDollar[3].valExpr}
		}
	case 151:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:847
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: ShiftLeftStr, Right: yyDollar[3].valExpr}
		}
	case 152:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:851
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: ShiftRightStr, Right: yyDollar[3].valExpr}
		}
	case 153:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:855
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].colName, Operator: JSONExtractOp, Right: yyDollar[3].valExpr}
		}
	case 154:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:859
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].colName, Operator: JSONUnquoteExtractOp, Right: yyDollar[3].valExpr}
		}
	case 155:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:863
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: CollateStr, Right: yyDollar[3].valExpr}
		}
	case 156:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:867
		{
			if num, ok := yyDollar[2].valExpr.(*SQLVal); ok && num.Type == IntVal {
				yyVAL.valExpr = num
			} else {
				yyVAL.valExpr = &UnaryExpr{Operator: UPlusStr, Expr: yyDollar[2].valExpr}
			}
		}
	case 157:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:875
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
	case 158:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:889
		{
			yyVAL.valExpr = &UnaryExpr{Operator: TildaStr, Expr: yyDollar[2].valExpr}
		}
	case 159:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:893
		{
			// This rule prevents the usage of INTERVAL
			// as a function. If support is needed for that,
			// we'll need to revisit this. The solution
			// will be non-trivial because of grammar conflicts.
			yyVAL.valExpr = &IntervalExpr{Expr: yyDollar[2].valExpr, Unit: yyDollar[3].colIdent}
		}
	case 160:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:901
		{
			yyVAL.valExpr = &FuncExpr{Name: yyDollar[1].colIdent}
		}
	case 161:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:905
		{
			yyVAL.valExpr = &FuncExpr{Name: yyDollar[1].colIdent, Exprs: yyDollar[3].selectExprs}
		}
	case 162:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:909
		{
			yyVAL.valExpr = &FuncExpr{Name: yyDollar[1].colIdent, Distinct: true, Exprs: yyDollar[4].selectExprs}
		}
	case 163:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:913
		{
			yyVAL.valExpr = &FuncExpr{Name: yyDollar[1].colIdent}
		}
	case 164:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:917
		{
			yyVAL.valExpr = &FuncExpr{Name: yyDollar[1].colIdent, Exprs: yyDollar[3].selectExprs}
		}
	case 165:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:921
		{
			yyVAL.valExpr = yyDollar[1].caseExpr
		}
	case 166:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:927
		{
			yyVAL.colIdent = NewColIdent("if")
		}
	case 167:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:931
		{
			yyVAL.colIdent = NewColIdent("current_timestamp")
		}
	case 168:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:935
		{
			yyVAL.colIdent = NewColIdent("database")
		}
	case 169:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:939
		{
			yyVAL.colIdent = NewColIdent("mod")
		}
	case 170:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:945
		{
			yyVAL.caseExpr = &CaseExpr{Expr: yyDollar[2].valExpr, Whens: yyDollar[3].whens, Else: yyDollar[4].valExpr}
		}
	case 171:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:950
		{
			yyVAL.valExpr = nil
		}
	case 172:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:954
		{
			yyVAL.valExpr = yyDollar[1].expr
		}
	case 173:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:960
		{
			yyVAL.whens = []*When{yyDollar[1].when}
		}
	case 174:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:964
		{
			yyVAL.whens = append(yyDollar[1].whens, yyDollar[2].when)
		}
	case 175:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:970
		{
			yyVAL.when = &When{Cond: yyDollar[2].expr, Val: yyDollar[4].expr}
		}
	case 176:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:975
		{
			yyVAL.valExpr = nil
		}
	case 177:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:979
		{
			yyVAL.valExpr = yyDollar[2].expr
		}
	case 178:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:985
		{
			yyVAL.colName = &ColName{Name: yyDollar[1].colIdent}
		}
	case 179:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:989
		{
			yyVAL.colName = &ColName{Qualifier: &TableName{Name: yyDollar[1].tableIdent}, Name: yyDollar[3].colIdent}
		}
	case 180:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:993
		{
			yyVAL.colName = &ColName{Qualifier: &TableName{Qualifier: yyDollar[1].tableIdent, Name: yyDollar[3].tableIdent}, Name: yyDollar[5].colIdent}
		}
	case 181:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:999
		{
			yyVAL.valExpr = NewStrVal(yyDollar[1].bytes)
		}
	case 182:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1003
		{
			yyVAL.valExpr = NewHexVal(yyDollar[1].bytes)
		}
	case 183:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1007
		{
			yyVAL.valExpr = NewIntVal(yyDollar[1].bytes)
		}
	case 184:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1011
		{
			yyVAL.valExpr = NewFloatVal(yyDollar[1].bytes)
		}
	case 185:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1015
		{
			yyVAL.valExpr = NewHexNum(yyDollar[1].bytes)
		}
	case 186:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1019
		{
			yyVAL.valExpr = NewValArg(yyDollar[1].bytes)
		}
	case 187:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1023
		{
			yyVAL.valExpr = &NullVal{}
		}
	case 188:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1029
		{
			// TODO(sougou): Deprecate this construct.
			if yyDollar[1].colIdent.Lowered() != "value" {
				yylex.Error("expecting value after next")
				return 1
			}
			yyVAL.valExpr = NewIntVal([]byte("1"))
		}
	case 189:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:1038
		{
			yyVAL.valExpr = NewIntVal(yyDollar[1].bytes)
		}
	case 190:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:1042
		{
			yyVAL.valExpr = NewValArg(yyDollar[1].bytes)
		}
	case 191:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1047
		{
			yyVAL.valExprs = nil
		}
	case 192:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1051
		{
			yyVAL.valExprs = yyDollar[3].valExprs
		}
	case 193:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1056
		{
			yyVAL.boolExpr = nil
		}
	case 194:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:1060
		{
			yyVAL.boolExpr = yyDollar[2].expr
		}
	case 195:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1065
		{
			yyVAL.orderBy = nil
		}
	case 196:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1069
		{
			yyVAL.orderBy = yyDollar[3].orderBy
		}
	case 197:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1075
		{
			yyVAL.orderBy = OrderBy{yyDollar[1].order}
		}
	case 198:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1079
		{
			yyVAL.orderBy = append(yyDollar[1].orderBy, yyDollar[3].order)
		}
	case 199:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:1085
		{
			yyVAL.order = &Order{Expr: yyDollar[1].expr, Direction: yyDollar[2].str}
		}
	case 200:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1090
		{
			yyVAL.str = AscScr
		}
	case 201:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1094
		{
			yyVAL.str = AscScr
		}
	case 202:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1098
		{
			yyVAL.str = DescScr
		}
	case 203:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1103
		{
			yyVAL.limit = nil
		}
	case 204:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:1107
		{
			yyVAL.limit = &Limit{Rowcount: yyDollar[2].expr}
		}
	case 205:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:1111
		{
			yyVAL.limit = &Limit{Offset: yyDollar[2].expr, Rowcount: yyDollar[4].expr}
		}
	case 206:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:1115
		{
			yyVAL.limit = &Limit{Offset: yyDollar[4].expr, Rowcount: yyDollar[2].expr}
		}
	case 207:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1120
		{
			yyVAL.str = ""
		}
	case 208:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:1124
		{
			yyVAL.str = ForUpdateStr
		}
	case 209:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:1128
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
	case 210:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1141
		{
			yyVAL.columns = nil
		}
	case 211:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1145
		{
			yyVAL.columns = yyDollar[2].columns
		}
	case 212:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1151
		{
			yyVAL.columns = Columns{yyDollar[1].colIdent}
		}
	case 213:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1155
		{
			yyVAL.columns = append(yyVAL.columns, yyDollar[3].colIdent)
		}
	case 214:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1160
		{
			yyVAL.updateExprs = nil
		}
	case 215:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:1164
		{
			yyVAL.updateExprs = yyDollar[5].updateExprs
		}
	case 216:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:1170
		{
			yyVAL.insRows = yyDollar[2].values
		}
	case 217:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1174
		{
			yyVAL.insRows = yyDollar[1].selStmt
		}
	case 218:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1180
		{
			yyVAL.values = Values{yyDollar[1].valTuple}
		}
	case 219:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1184
		{
			yyVAL.values = append(yyDollar[1].values, yyDollar[3].valTuple)
		}
	case 220:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1190
		{
			yyVAL.valTuple = ValTuple(yyDollar[2].valExprs)
		}
	case 221:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1196
		{
			yyVAL.updateExprs = UpdateExprs{yyDollar[1].updateExpr}
		}
	case 222:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1200
		{
			yyVAL.updateExprs = append(yyDollar[1].updateExprs, yyDollar[3].updateExpr)
		}
	case 223:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1206
		{
			yyVAL.updateExpr = &UpdateExpr{Name: yyDollar[1].colIdent, Expr: yyDollar[3].expr}
		}
	case 226:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1215
		{
			yyVAL.byt = 0
		}
	case 227:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:1217
		{
			yyVAL.byt = 1
		}
	case 228:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1220
		{
			yyVAL.empty = struct{}{}
		}
	case 229:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1222
		{
			yyVAL.empty = struct{}{}
		}
	case 230:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1225
		{
			yyVAL.str = ""
		}
	case 231:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1227
		{
			yyVAL.str = IgnoreStr
		}
	case 232:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1231
		{
			yyVAL.empty = struct{}{}
		}
	case 233:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1233
		{
			yyVAL.empty = struct{}{}
		}
	case 234:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1235
		{
			yyVAL.empty = struct{}{}
		}
	case 235:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1237
		{
			yyVAL.empty = struct{}{}
		}
	case 236:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1239
		{
			yyVAL.empty = struct{}{}
		}
	case 237:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1241
		{
			yyVAL.empty = struct{}{}
		}
	case 238:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1244
		{
			yyVAL.empty = struct{}{}
		}
	case 239:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1246
		{
			yyVAL.empty = struct{}{}
		}
	case 240:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1249
		{
			yyVAL.empty = struct{}{}
		}
	case 241:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1251
		{
			yyVAL.empty = struct{}{}
		}
	case 242:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1254
		{
			yyVAL.empty = struct{}{}
		}
	case 243:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:1256
		{
			yyVAL.empty = struct{}{}
		}
	case 244:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1260
		{
			yyVAL.colIdent = NewColIdent(string(yyDollar[1].bytes))
		}
	case 245:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1266
		{
			yyVAL.tableIdent = NewTableIdent(string(yyDollar[1].bytes))
		}
	case 246:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1272
		{
			if incNesting(yylex) {
				yylex.Error("max nesting level reached")
				return 1
			}
		}
	case 247:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1281
		{
			decNesting(yylex)
		}
	case 248:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1286
		{
			forceEOF(yylex)
		}
	}
	goto yystack /* stack new state and value */
}
