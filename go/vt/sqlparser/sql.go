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
	96, 249,
	-2, 248,
}

const yyNprod = 253
const yyPrivate = 57344

var yyTokenNames []string
var yyStates []string

const yyLast = 986

var yyAct = [...]int{

	123, 109, 62, 217, 341, 331, 240, 321, 117, 310,
	372, 118, 295, 47, 441, 254, 252, 272, 265, 312,
	159, 179, 126, 163, 253, 74, 104, 154, 216, 3,
	105, 392, 394, 67, 169, 64, 41, 115, 69, 48,
	49, 71, 40, 35, 41, 37, 424, 249, 167, 38,
	99, 256, 43, 44, 45, 81, 50, 423, 422, 68,
	70, 46, 42, 374, 348, 14, 15, 16, 17, 171,
	221, 243, 196, 219, 220, 183, 89, 103, 199, 200,
	201, 202, 196, 96, 58, 98, 446, 63, 18, 64,
	184, 152, 64, 186, 185, 184, 393, 92, 65, 114,
	406, 404, 111, 311, 401, 186, 150, 160, 181, 87,
	186, 94, 311, 90, 364, 213, 215, 174, 84, 342,
	166, 168, 165, 195, 194, 203, 204, 197, 198, 199,
	200, 201, 202, 196, 114, 114, 398, 170, 151, 228,
	260, 296, 14, 65, 226, 227, 128, 60, 229, 344,
	64, 238, 236, 279, 185, 184, 275, 242, 19, 20,
	22, 21, 23, 60, 246, 344, 60, 277, 278, 276,
	186, 24, 25, 26, 185, 184, 114, 235, 247, 156,
	239, 181, 128, 180, 262, 60, 263, 264, 111, 257,
	186, 349, 350, 351, 95, 232, 357, 114, 258, 65,
	274, 182, 259, 250, 86, 114, 114, 251, 75, 273,
	270, 65, 283, 182, 214, 195, 194, 203, 204, 197,
	198, 199, 200, 201, 202, 196, 303, 91, 60, 297,
	299, 197, 198, 199, 200, 201, 202, 196, 128, 304,
	307, 60, 298, 315, 262, 411, 320, 114, 114, 28,
	318, 448, 296, 305, 308, 316, 317, 300, 301, 302,
	194, 203, 204, 197, 198, 199, 200, 201, 202, 196,
	257, 266, 268, 269, 367, 347, 267, 352, 313, 258,
	91, 130, 129, 131, 132, 133, 134, 155, 274, 135,
	176, 296, 296, 110, 353, 298, 296, 273, 203, 204,
	197, 198, 199, 200, 201, 202, 196, 157, 65, 329,
	296, 82, 329, 365, 83, 319, 296, 128, 359, 363,
	368, 361, 177, 91, 73, 418, 313, 114, 369, 218,
	245, 102, 114, 360, 222, 223, 224, 225, 79, 375,
	149, 257, 257, 257, 257, 390, 380, 395, 382, 399,
	258, 258, 258, 258, 379, 231, 381, 128, 387, 176,
	397, 385, 421, 388, 420, 400, 386, 384, 383, 389,
	76, 337, 338, 403, 244, 399, 408, 55, 147, 416,
	146, 88, 417, 415, 161, 101, 407, 366, 316, 346,
	54, 110, 39, 333, 336, 337, 338, 334, 114, 335,
	339, 14, 271, 419, 322, 280, 281, 282, 378, 284,
	285, 286, 287, 288, 289, 290, 291, 292, 293, 294,
	432, 433, 430, 431, 57, 145, 234, 241, 64, 158,
	437, 51, 52, 144, 439, 442, 442, 442, 323, 114,
	114, 110, 110, 434, 435, 436, 440, 451, 377, 452,
	443, 444, 453, 328, 447, 155, 449, 450, 61, 445,
	427, 29, 59, 333, 336, 337, 338, 334, 14, 335,
	339, 28, 72, 30, 1, 345, 77, 31, 32, 33,
	34, 340, 178, 162, 36, 248, 164, 59, 244, 66,
	143, 59, 354, 355, 356, 237, 93, 148, 438, 412,
	97, 371, 124, 100, 376, 327, 362, 230, 108, 309,
	125, 116, 314, 358, 80, 306, 59, 127, 233, 153,
	187, 110, 112, 391, 85, 332, 330, 255, 175, 172,
	107, 244, 173, 78, 53, 27, 56, 370, 373, 13,
	12, 128, 11, 296, 111, 130, 129, 131, 132, 133,
	134, 10, 9, 135, 141, 142, 8, 7, 113, 128,
	140, 6, 111, 130, 129, 131, 132, 133, 134, 5,
	4, 135, 402, 59, 2, 0, 0, 0, 140, 405,
	119, 120, 106, 0, 0, 139, 0, 121, 0, 0,
	122, 0, 0, 244, 0, 0, 0, 410, 119, 120,
	0, 0, 0, 139, 136, 121, 108, 59, 122, 0,
	137, 138, 0, 261, 0, 0, 425, 127, 0, 0,
	0, 426, 136, 0, 428, 429, 373, 409, 137, 138,
	195, 194, 203, 204, 197, 198, 199, 200, 201, 202,
	196, 128, 0, 296, 111, 130, 129, 131, 132, 133,
	134, 0, 0, 135, 141, 142, 108, 108, 113, 0,
	140, 195, 194, 203, 204, 197, 198, 199, 200, 201,
	202, 196, 0, 324, 0, 325, 0, 0, 326, 0,
	119, 120, 106, 0, 0, 139, 343, 121, 59, 0,
	122, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 127, 0, 0, 136, 0, 0, 0, 0, 0,
	137, 138, 0, 65, 0, 0, 0, 0, 413, 414,
	0, 0, 0, 0, 0, 128, 0, 0, 111, 130,
	129, 131, 132, 133, 134, 0, 108, 135, 141, 142,
	0, 0, 113, 0, 140, 195, 194, 203, 204, 197,
	198, 199, 200, 201, 202, 196, 0, 14, 0, 59,
	59, 59, 59, 0, 119, 120, 106, 0, 0, 139,
	0, 121, 343, 127, 122, 396, 195, 194, 203, 204,
	197, 198, 199, 200, 201, 202, 196, 0, 136, 0,
	0, 127, 0, 0, 137, 138, 0, 128, 0, 0,
	111, 130, 129, 131, 132, 133, 134, 0, 0, 135,
	141, 142, 0, 0, 113, 128, 140, 0, 111, 130,
	129, 131, 132, 133, 134, 0, 0, 135, 141, 142,
	0, 0, 113, 0, 140, 0, 119, 120, 0, 0,
	0, 139, 0, 121, 0, 0, 122, 0, 0, 0,
	14, 0, 0, 0, 119, 120, 0, 0, 0, 139,
	136, 121, 0, 0, 122, 0, 137, 138, 128, 0,
	0, 111, 130, 129, 131, 132, 133, 134, 136, 0,
	135, 141, 142, 0, 137, 138, 0, 140, 0, 0,
	128, 0, 0, 111, 130, 129, 131, 132, 133, 134,
	0, 0, 135, 0, 0, 0, 0, 119, 120, 140,
	0, 0, 139, 0, 121, 0, 0, 122, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 119,
	120, 136, 0, 0, 139, 0, 121, 137, 138, 122,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 136, 189, 192, 0, 0, 0, 137,
	138, 205, 206, 207, 208, 209, 210, 211, 193, 190,
	191, 188, 195, 194, 203, 204, 197, 198, 199, 200,
	201, 202, 196, 0, 0, 212,
}
var yyPact = [...]int{

	59, -1000, -1000, 466, -1000, -1000, -1000, -1000, -1000, -1000,
	-1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, -61,
	-64, -42, -52, -43, -1000, -1000, -1000, 462, 412, 357,
	-1000, -72, 117, 448, 94, -76, -46, 94, -1000, -44,
	94, -1000, 117, -84, 159, -84, 117, -1000, -1000, -1000,
	-1000, -1000, -1000, 302, 259, -1000, 61, 179, 352, -20,
	-1000, 117, 180, -1000, 27, -1000, 117, 48, 145, -1000,
	117, -1000, -57, 117, 363, 286, 94, -1000, 679, -1000,
	415, -1000, 349, 347, -1000, 311, 117, -1000, 94, 117,
	444, 94, 513, -1000, 362, -88, -1000, 20, -1000, 117,
	-1000, -1000, 117, -1000, 312, -1000, -1000, 162, -21, 113,
	891, -1000, -1000, 769, 751, -1000, -24, -1000, -1000, 513,
	513, 513, 513, 271, 271, -1000, -1000, 271, -1000, -1000,
	-1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000,
	513, -1000, -1000, 117, -1000, -1000, -1000, -1000, 395, 94,
	94, -1000, 276, -1000, 413, 769, -1000, 549, -25, 844,
	-1000, -1000, 285, 94, -1000, -60, -1000, -1000, -1000, -1000,
	-1000, -1000, -1000, -1000, -1000, 444, 679, 192, -1000, -1000,
	150, -1000, -1000, 53, 769, 769, 213, 822, 100, 89,
	513, 513, 513, 213, 513, 513, 513, 513, 513, 513,
	513, 513, 513, 513, 513, -1000, -1000, -1000, -1000, -1000,
	-1000, -1000, -1000, 16, 891, 93, 244, 248, 891, 231,
	231, 231, -1000, -1000, -1000, 664, 495, 595, -1000, 462,
	37, 549, -1000, 281, 271, 466, 233, 268, -1000, 413,
	388, 423, 113, 139, 549, 117, -1000, -1000, 117, -1000,
	441, -1000, 265, 428, -1000, -1000, 98, 368, 136, -1000,
	-1000, -32, -1000, 16, 28, -1000, -1000, 133, -1000, -1000,
	-1000, 549, -1000, 844, -1000, -1000, 100, 513, 513, 513,
	549, 549, 134, -1000, 215, 178, -1000, -9, -9, -19,
	-19, -19, -19, 146, 146, -1000, -1000, -1000, 513, -1000,
	-1000, -1000, -1000, -1000, -1000, 243, 679, -1000, 243, 46,
	-1000, 769, -1000, 361, 227, -1000, 513, -1000, -1000, 94,
	388, -1000, 513, 513, -33, -1000, -1000, 435, 393, 192,
	192, 192, 192, -1000, 333, 332, -1000, 326, 323, 334,
	-12, -1000, 114, -1000, -1000, 117, -1000, 262, 49, -1000,
	-1000, -1000, 248, -1000, 549, 549, 42, 513, 549, -1000,
	243, -1000, 32, -1000, 513, 33, 359, 271, -1000, -1000,
	580, 198, -1000, 695, 94, -1000, 413, 769, 513, 428,
	280, 358, -1000, -1000, -1000, -1000, 329, -1000, 327, -1000,
	-1000, -1000, -47, -48, -59, -1000, -1000, -1000, -1000, -1000,
	-1000, 513, 549, -1000, -1000, 549, 513, 452, -1000, 513,
	513, 513, -1000, -1000, -1000, 388, 113, 195, 769, 769,
	-1000, -1000, 271, 271, 271, 549, 549, 94, 549, 549,
	-1000, 416, 113, 113, 94, 94, 94, 180, -1000, 451,
	6, 204, -1000, 204, 204, -1000, 94, -1000, 94, -1000,
	-1000, 94, -1000, -1000,
}
var yyPgo = [...]int{

	0, 574, 28, 570, 569, 561, 557, 556, 552, 551,
	542, 540, 539, 461, 536, 535, 534, 533, 26, 30,
	530, 528, 16, 24, 15, 527, 526, 5, 525, 51,
	524, 523, 14, 27, 1, 522, 22, 520, 518, 37,
	214, 514, 18, 17, 3, 512, 8, 11, 511, 510,
	509, 9, 507, 506, 505, 504, 502, 6, 501, 10,
	499, 7, 498, 497, 495, 19, 2, 87, 490, 392,
	324, 489, 486, 485, 484, 483, 0, 21, 482, 429,
	4, 481, 475, 13, 474, 473, 20, 12,
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
	2, 1, 3, 3, 2, 3, 3, 1, 1, 1,
	3, 3, 3, 4, 3, 4, 3, 4, 5, 6,
	3, 2, 1, 2, 1, 2, 1, 2, 1, 1,
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
	88, 89, 90, 83, 84, 70, 71, 72, 73, 74,
	75, 76, 94, -34, -40, -34, -2, -44, -40, 97,
	98, 94, -40, -40, -40, -40, -86, -86, -47, -86,
	-52, -40, -29, -38, 31, -2, -66, -64, -76, -33,
	-57, 14, -34, 96, -40, 45, -76, -83, -73, 107,
	-33, -19, -22, -23, -24, -25, -29, -47, -86, -77,
	87, -79, -76, -34, -34, -42, 58, 63, 59, 60,
	-36, -40, -43, -86, -47, 56, 80, 78, 79, 64,
	-40, -40, -40, -42, -40, -40, -40, -40, -40, -40,
	-40, -40, -40, -40, -40, -87, 48, -87, 47, -87,
	-39, -39, -39, -76, -87, -18, 20, -87, -18, -50,
	-51, 66, -65, 45, -45, -46, -86, -65, -87, 47,
	-57, -61, 16, 15, -79, -79, -79, -54, 12, 47,
	-26, -27, -28, 35, 39, 41, 36, 37, 38, 42,
	-81, -80, 21, -79, 51, -82, 21, -22, 96, 58,
	59, 60, -44, -43, -40, -40, -40, 62, -40, -87,
	-18, -87, -53, -51, 68, -34, 26, 47, -76, -61,
	-40, -58, -59, -40, 96, -83, -55, 13, 15, -23,
	-24, -23, -24, 35, 35, 35, 40, 35, 40, 35,
	-27, -31, 43, 108, 44, -80, -79, -87, 87, -76,
	-87, 62, -40, -87, 69, -40, 67, 27, -46, 47,
	17, 47, -60, 23, 24, -57, -34, -44, 45, 45,
	35, 35, 105, 105, 105, -40, -40, 8, -40, -40,
	-59, -61, -34, -34, -86, -86, -86, -66, -62, 18,
	30, -32, -76, -32, -32, 8, 80, -87, 47, -87,
	-87, -76, -76, -76,
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
	0, 0, 0, 182, 0, 169, 109, 0, 250, 185,
	186, 187, 188, 189, 190, 191, 170, 171, 172, 173,
	175, 107, 108, 0, 228, 229, 193, 194, 0, 0,
	0, 89, 99, 92, 199, 0, 226, 227, 0, 0,
	21, 233, 0, 0, 252, 242, 236, 237, 238, 239,
	240, 241, 27, 29, 30, 99, 0, 0, 49, 55,
	0, 57, 58, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 128, 129, 130, 131, 132,
	133, 134, 135, 104, 0, 0, 0, 0, 140, 0,
	0, 0, 160, 161, 162, 0, 0, 0, 121, 0,
	0, 176, 14, 218, 0, 221, 218, 0, 216, 199,
	207, 0, 100, 0, 140, 0, 247, 24, 0, 243,
	195, 47, 60, 61, 63, 64, 74, 72, 0, 56,
	50, 0, 183, 102, 103, 106, 122, 0, 124, 126,
	110, 111, 112, 0, 137, 138, 0, 0, 0, 0,
	114, 116, 0, 120, 146, 147, 148, 149, 150, 151,
	152, 153, 154, 155, 156, 105, 251, 139, 0, 224,
	157, 158, 159, 163, 164, 0, 0, 167, 0, 180,
	177, 0, 16, 0, 220, 222, 0, 17, 215, 0,
	207, 19, 0, 0, 0, 252, 25, 197, 0, 0,
	0, 0, 0, 79, 0, 0, 82, 0, 0, 0,
	93, 75, 0, 77, 78, 0, 73, 0, 0, 123,
	125, 127, 0, 113, 115, 117, 0, 0, 141, 165,
	0, 168, 0, 178, 0, 0, 0, 0, 217, 18,
	208, 200, 201, 204, 0, 22, 199, 0, 0, 62,
	68, 0, 71, 80, 81, 83, 0, 85, 0, 87,
	88, 65, 0, 0, 0, 76, 66, 67, 51, 184,
	136, 0, 118, 166, 174, 181, 0, 0, 223, 0,
	0, 0, 203, 205, 206, 207, 198, 196, 0, 0,
	84, 86, 0, 0, 0, 119, 179, 0, 209, 210,
	202, 211, 69, 70, 0, 0, 0, 219, 13, 0,
	0, 0, 97, 0, 0, 212, 0, 94, 0, 95,
	96, 0, 98, 213,
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
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:774
		{
			yyVAL.str = CollateStr
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
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].colName, Operator: Collate, Right: yyDollar[3].valExpr}
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
