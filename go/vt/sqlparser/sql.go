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
	96, 250,
	-2, 249,
}

const yyNprod = 254
const yyPrivate = 57344

var yyTokenNames []string
var yyStates []string

const yyLast = 1047

var yyAct = [...]int{

	123, 109, 442, 322, 62, 373, 239, 332, 217, 297,
	117, 118, 342, 311, 104, 253, 271, 251, 313, 264,
	126, 159, 179, 216, 3, 154, 252, 163, 105, 115,
	74, 35, 67, 37, 41, 64, 255, 38, 69, 47,
	40, 71, 41, 248, 99, 393, 395, 43, 44, 45,
	425, 50, 424, 423, 68, 81, 70, 46, 42, 375,
	199, 200, 201, 202, 196, 48, 49, 205, 349, 58,
	197, 198, 199, 200, 201, 202, 196, 103, 242, 205,
	219, 220, 14, 183, 89, 196, 205, 63, 205, 64,
	278, 447, 64, 152, 87, 186, 185, 184, 90, 65,
	114, 92, 111, 184, 276, 277, 275, 150, 181, 96,
	394, 98, 186, 405, 312, 213, 215, 312, 186, 365,
	94, 84, 128, 151, 65, 111, 130, 129, 131, 132,
	133, 134, 86, 160, 135, 114, 114, 399, 60, 227,
	259, 140, 128, 174, 111, 225, 226, 185, 184, 228,
	64, 237, 274, 407, 235, 95, 60, 241, 350, 351,
	352, 119, 120, 186, 245, 75, 139, 65, 121, 28,
	82, 122, 234, 83, 169, 449, 298, 114, 238, 156,
	231, 181, 298, 91, 261, 136, 262, 263, 167, 256,
	343, 137, 138, 180, 128, 185, 184, 60, 114, 257,
	273, 249, 300, 258, 246, 250, 114, 114, 269, 171,
	272, 186, 298, 282, 265, 267, 268, 412, 60, 266,
	345, 65, 60, 182, 345, 304, 299, 301, 368, 130,
	129, 131, 132, 133, 134, 305, 308, 135, 176, 298,
	306, 309, 14, 261, 316, 321, 319, 114, 114, 302,
	303, 295, 330, 296, 318, 317, 65, 314, 182, 91,
	166, 168, 165, 149, 334, 337, 338, 339, 335, 256,
	336, 340, 214, 128, 420, 348, 155, 170, 177, 257,
	128, 353, 128, 419, 73, 60, 147, 273, 300, 298,
	330, 298, 354, 320, 298, 314, 244, 272, 334, 337,
	338, 339, 335, 102, 336, 340, 79, 422, 14, 15,
	16, 17, 91, 388, 366, 176, 360, 421, 389, 362,
	385, 369, 361, 386, 364, 370, 384, 55, 387, 114,
	76, 18, 39, 390, 114, 338, 339, 14, 88, 146,
	54, 408, 256, 256, 256, 256, 367, 381, 391, 383,
	400, 110, 257, 257, 257, 257, 396, 380, 398, 382,
	440, 161, 233, 401, 57, 157, 376, 101, 347, 51,
	52, 404, 441, 145, 323, 379, 400, 324, 240, 409,
	417, 144, 378, 329, 416, 155, 61, 218, 418, 446,
	317, 29, 221, 222, 223, 224, 428, 14, 28, 30,
	114, 19, 20, 22, 21, 23, 1, 31, 32, 33,
	34, 294, 346, 230, 24, 25, 26, 341, 431, 178,
	432, 433, 434, 162, 414, 415, 36, 247, 164, 64,
	66, 143, 243, 438, 236, 148, 443, 443, 443, 444,
	445, 114, 114, 439, 413, 435, 436, 437, 452, 110,
	453, 372, 448, 454, 450, 451, 124, 377, 328, 363,
	270, 229, 310, 279, 280, 281, 125, 283, 284, 285,
	286, 287, 288, 289, 290, 291, 292, 293, 116, 315,
	80, 158, 195, 194, 203, 204, 197, 198, 199, 200,
	201, 202, 196, 232, 187, 205, 112, 392, 110, 110,
	195, 194, 203, 204, 197, 198, 199, 200, 201, 202,
	196, 85, 333, 205, 59, 203, 204, 197, 198, 199,
	200, 201, 202, 196, 72, 331, 205, 254, 77, 194,
	203, 204, 197, 198, 199, 200, 201, 202, 196, 59,
	175, 205, 107, 59, 78, 243, 53, 27, 93, 355,
	356, 357, 97, 56, 13, 100, 12, 11, 10, 9,
	108, 8, 7, 6, 5, 4, 2, 0, 59, 0,
	0, 153, 0, 359, 307, 0, 127, 0, 0, 0,
	110, 172, 0, 0, 173, 0, 0, 0, 0, 0,
	243, 0, 0, 0, 0, 0, 371, 374, 0, 0,
	128, 0, 298, 111, 130, 129, 131, 132, 133, 134,
	0, 0, 135, 141, 142, 0, 0, 113, 0, 140,
	0, 0, 0, 0, 0, 59, 0, 0, 0, 0,
	0, 403, 0, 0, 0, 0, 0, 0, 406, 119,
	120, 106, 0, 0, 139, 0, 121, 0, 411, 122,
	0, 0, 243, 0, 0, 0, 0, 0, 108, 59,
	0, 0, 0, 136, 0, 260, 0, 0, 127, 137,
	138, 0, 0, 0, 0, 426, 0, 0, 410, 0,
	427, 0, 0, 429, 430, 374, 0, 0, 0, 0,
	0, 0, 128, 0, 298, 111, 130, 129, 131, 132,
	133, 134, 0, 0, 135, 141, 142, 108, 108, 113,
	0, 140, 195, 194, 203, 204, 197, 198, 199, 200,
	201, 202, 196, 0, 325, 205, 326, 0, 0, 327,
	0, 119, 120, 106, 0, 0, 139, 344, 121, 59,
	0, 122, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 127, 136, 0, 0, 0, 0,
	128, 137, 138, 111, 130, 129, 131, 132, 133, 134,
	0, 0, 135, 141, 142, 0, 0, 0, 128, 140,
	0, 111, 130, 129, 131, 132, 133, 134, 0, 108,
	135, 141, 142, 0, 0, 113, 0, 140, 0, 119,
	120, 0, 0, 0, 139, 0, 121, 0, 0, 122,
	14, 0, 59, 59, 59, 59, 0, 119, 120, 106,
	0, 0, 139, 136, 121, 344, 127, 122, 397, 137,
	138, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 136, 0, 0, 127, 0, 0, 137, 138, 0,
	128, 0, 0, 111, 130, 129, 131, 132, 133, 134,
	0, 0, 135, 141, 142, 0, 0, 113, 128, 140,
	0, 111, 130, 129, 131, 132, 133, 134, 0, 0,
	135, 141, 142, 0, 0, 113, 0, 140, 0, 119,
	120, 0, 0, 0, 139, 0, 121, 0, 0, 122,
	0, 0, 0, 0, 0, 0, 0, 119, 120, 0,
	0, 0, 139, 136, 121, 0, 0, 122, 0, 137,
	138, 128, 402, 0, 111, 130, 129, 131, 132, 133,
	134, 136, 0, 135, 0, 0, 0, 137, 138, 0,
	140, 195, 194, 203, 204, 197, 198, 199, 200, 201,
	202, 196, 0, 0, 205, 0, 0, 0, 0, 0,
	119, 120, 0, 0, 0, 139, 0, 121, 0, 0,
	122, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 136, 189, 192, 65, 0, 0,
	137, 138, 206, 207, 208, 209, 210, 211, 212, 193,
	190, 191, 188, 195, 194, 203, 204, 197, 198, 199,
	200, 201, 202, 196, 358, 0, 205, 0, 0, 195,
	194, 203, 204, 197, 198, 199, 200, 201, 202, 196,
	0, 0, 205, 195, 194, 203, 204, 197, 198, 199,
	200, 201, 202, 196, 0, 0, 205,
}
var yyPact = [...]int{

	302, -1000, -1000, 393, -1000, -1000, -1000, -1000, -1000, -1000,
	-1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, -73,
	-66, -46, -57, -47, -1000, -1000, -1000, 391, 350, 307,
	-1000, -74, 89, 376, 75, -77, -51, 75, -1000, -48,
	75, -1000, 89, -79, 116, -79, 89, -1000, -1000, -1000,
	-1000, -1000, -1000, 270, 118, -1000, 64, 107, 309, -12,
	-1000, 89, 136, -1000, 31, -1000, 89, 57, 106, -1000,
	89, -1000, -63, 89, 345, 258, 75, -1000, 732, -1000,
	363, -1000, 308, 255, -1000, 234, 89, -1000, 75, 89,
	374, 75, 875, -1000, 339, -84, -1000, 160, -1000, 89,
	-1000, -1000, 89, -1000, 268, -1000, -1000, 172, -13, 35,
	922, -1000, -1000, 822, 804, -1000, -17, -1000, -1000, 875,
	875, 875, 875, 227, 227, -1000, -1000, 227, -1000, -1000,
	-1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000,
	875, -1000, -1000, 89, -1000, -1000, -1000, -1000, 331, 75,
	75, -1000, 265, -1000, 364, 822, -1000, 419, -18, 76,
	-1000, -1000, 251, 75, -1000, -64, -1000, -1000, -1000, -1000,
	-1000, -1000, -1000, -1000, -1000, 374, 732, 148, -1000, -1000,
	207, -1000, -1000, 53, 822, 822, 156, 714, 96, 26,
	875, 875, 875, 156, 875, 875, 875, 875, 875, 875,
	875, 875, 875, 875, 875, 202, -1000, -1000, -1000, -1000,
	-1000, -1000, -1000, 18, 922, 134, 164, 241, 922, 179,
	179, -8, -8, -8, 938, 554, 646, -1000, 391, 48,
	419, -1000, 250, 227, 393, 212, 246, -1000, 364, 358,
	362, 35, 95, 419, 89, -1000, -1000, 89, -1000, 371,
	-1000, 205, 263, -1000, -1000, 169, 347, 236, -1000, -1000,
	-28, -1000, 18, 41, -1000, -1000, 100, -1000, -1000, -1000,
	419, -1000, 76, -1000, -1000, 96, 875, 875, 875, 419,
	419, 952, -1000, 432, 447, -8, -27, -27, -6, -6,
	-6, -6, -15, -15, -1000, -1000, -1000, -1000, -1000, -1000,
	875, -1000, -1000, -1000, -1000, -1000, 191, 732, -1000, 191,
	51, -1000, 822, -1000, 320, 181, -1000, 875, -1000, -1000,
	75, 358, -1000, 875, 875, -37, -1000, -1000, 369, 360,
	148, 148, 148, 148, -1000, 291, 285, -1000, 288, 278,
	298, 2, -1000, 173, -1000, -1000, 89, -1000, 243, 50,
	-1000, -1000, -1000, 241, -1000, 419, 419, 860, 875, 419,
	-1000, 191, -1000, 44, -1000, 875, 86, 314, 227, -1000,
	-1000, 631, 170, -1000, 401, 75, -1000, 364, 822, 875,
	263, 238, 229, -1000, -1000, -1000, -1000, 282, -1000, 272,
	-1000, -1000, -1000, -52, -53, -55, -1000, -1000, -1000, -1000,
	-1000, -1000, 875, 419, -1000, -1000, 419, 875, 388, -1000,
	875, 875, 875, -1000, -1000, -1000, 358, 35, 155, 822,
	822, -1000, -1000, 227, 227, 227, 419, 419, 75, 419,
	419, -1000, 342, 35, 35, 75, 75, 75, 136, -1000,
	381, 11, 128, -1000, 128, 128, -1000, 75, -1000, 75,
	-1000, -1000, 75, -1000, -1000,
}
var yyPgo = [...]int{

	0, 566, 23, 565, 564, 563, 562, 561, 559, 558,
	557, 556, 554, 391, 553, 547, 546, 544, 14, 28,
	542, 540, 17, 26, 15, 527, 525, 7, 512, 36,
	511, 497, 2, 25, 1, 496, 20, 494, 493, 29,
	272, 480, 19, 16, 8, 479, 10, 11, 478, 466,
	462, 13, 461, 459, 458, 457, 456, 6, 451, 5,
	444, 3, 443, 435, 434, 18, 4, 87, 431, 332,
	284, 430, 428, 427, 426, 423, 0, 22, 419, 481,
	12, 417, 412, 39, 411, 406, 399, 21, 9,
}
var yyR1 = [...]int{

	0, 85, 1, 1, 1, 1, 1, 1, 1, 1,
	1, 1, 1, 2, 2, 2, 3, 3, 4, 5,
	6, 7, 7, 7, 8, 8, 8, 9, 10, 10,
	10, 11, 12, 12, 12, 86, 13, 14, 14, 15,
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
	44, 84, 84, 40, 40, 40, 40, 40, 40, 40,
	40, 40, 40, 40, 40, 40, 40, 40, 40, 40,
	40, 40, 40, 40, 40, 40, 40, 40, 40, 40,
	40, 56, 56, 56, 56, 49, 52, 52, 50, 50,
	51, 53, 53, 48, 48, 48, 39, 39, 39, 39,
	39, 39, 39, 41, 41, 41, 54, 54, 55, 55,
	57, 57, 58, 58, 59, 60, 60, 60, 61, 61,
	61, 61, 62, 62, 62, 63, 63, 64, 64, 65,
	65, 38, 38, 45, 45, 46, 66, 66, 67, 68,
	68, 70, 70, 71, 71, 69, 69, 72, 72, 72,
	72, 72, 72, 73, 73, 74, 74, 75, 75, 76,
	79, 87, 88, 83,
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
	3, 1, 1, 1, 1, 1, 1, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 2, 2, 2, 3, 3, 4, 5, 3, 4,
	1, 1, 1, 1, 1, 5, 0, 1, 1, 2,
	4, 0, 2, 1, 3, 5, 1, 1, 1, 1,
	1, 1, 1, 1, 2, 2, 0, 3, 0, 2,
	0, 3, 1, 3, 2, 0, 1, 1, 0, 2,
	4, 4, 0, 2, 4, 0, 3, 1, 3, 0,
	5, 2, 1, 1, 3, 3, 1, 3, 3, 1,
	1, 0, 2, 0, 3, 0, 1, 1, 1, 1,
	1, 1, 1, 0, 1, 0, 1, 0, 2, 1,
	1, 1, 1, 0,
}
var yyChk = [...]int{

	-1000, -85, -1, -2, -3, -4, -5, -6, -7, -8,
	-9, -10, -11, -12, 6, 7, 8, 9, 29, 99,
	100, 102, 101, 103, 112, 113, 114, -15, 5, -13,
	-86, -13, -13, -13, -13, 104, -74, 106, 110, -69,
	106, 108, 104, 104, 105, 106, 104, -83, -83, -83,
	-2, 19, 20, -16, 33, 20, -14, -69, -29, -79,
	49, 10, -66, -67, -76, 49, -71, 109, 105, -76,
	104, -76, -79, -70, 109, 49, -70, -79, -17, 36,
	-41, -76, 52, 55, 57, -30, 25, -29, 29, 96,
	-29, 47, 70, -79, 63, 49, -83, -79, -83, 107,
	-79, 22, 45, -76, -18, -19, 87, -20, -79, -34,
	-40, 49, -35, 63, -87, -39, -48, -46, -47, 85,
	86, 92, 95, -76, -56, -49, -36, 22, 46, 51,
	50, 52, 53, 54, 55, 58, 109, 115, 116, 90,
	65, 59, 60, -68, 18, 10, 31, 31, -63, 29,
	-87, -29, -66, -79, -33, 11, -67, -40, -79, -87,
	-83, 22, -75, 111, -72, 102, 100, 28, 101, 14,
	117, 49, -79, -79, -83, -21, 47, 10, -78, -77,
	21, -76, 51, 96, 62, 61, 77, -37, 80, 63,
	78, 79, 64, 77, 82, 81, 91, 85, 86, 87,
	88, 89, 90, 83, 84, 94, 70, 71, 72, 73,
	74, 75, 76, -34, -40, -34, -2, -44, -40, 97,
	98, -40, -40, -40, -40, -87, -87, -47, -87, -52,
	-40, -29, -38, 31, -2, -66, -64, -76, -33, -57,
	14, -34, 96, -40, 45, -76, -83, -73, 107, -33,
	-19, -22, -23, -24, -25, -29, -47, -87, -77, 87,
	-79, -76, -34, -34, -42, 58, 63, 59, 60, -36,
	-40, -43, -87, -47, 56, 80, 78, 79, 64, -40,
	-40, -40, -42, -40, -40, -40, -40, -40, -40, -40,
	-40, -40, -40, -40, -84, 49, 51, -88, 48, -88,
	47, -88, -39, -39, -76, -88, -18, 20, -88, -18,
	-50, -51, 66, -65, 45, -45, -46, -87, -65, -88,
	47, -57, -61, 16, 15, -79, -79, -79, -54, 12,
	47, -26, -27, -28, 35, 39, 41, 36, 37, 38,
	42, -81, -80, 21, -79, 51, -82, 21, -22, 96,
	58, 59, 60, -44, -43, -40, -40, -40, 62, -40,
	-88, -18, -88, -53, -51, 68, -34, 26, 47, -76,
	-61, -40, -58, -59, -40, 96, -83, -55, 13, 15,
	-23, -24, -23, -24, 35, 35, 35, 40, 35, 40,
	35, -27, -31, 43, 108, 44, -80, -79, -88, 87,
	-76, -88, 62, -40, -88, 69, -40, 67, 27, -46,
	47, 17, 47, -60, 23, 24, -57, -34, -44, 45,
	45, 35, 35, 105, 105, 105, -40, -40, 8, -40,
	-40, -59, -61, -34, -34, -87, -87, -87, -66, -62,
	18, 30, -32, -76, -32, -32, 8, 80, -88, 47,
	-88, -88, -76, -76, -76,
}
var yyDef = [...]int{

	0, -2, 1, 2, 3, 4, 5, 6, 7, 8,
	9, 10, 11, 12, 35, 35, 35, 35, 35, 245,
	235, 0, 0, 0, 253, 253, 253, 0, 39, 42,
	37, 235, 0, 0, 0, 233, 0, 0, 246, 0,
	0, 236, 0, 231, 0, 231, 0, 32, 33, 34,
	15, 40, 41, 44, 0, 43, 36, 0, 0, 91,
	250, 0, 20, 226, 0, 249, 0, 0, 0, 253,
	0, 253, 0, 0, 0, 0, 0, 31, 0, 45,
	0, 193, 0, 0, 38, 215, 0, 90, 0, 0,
	99, 0, 0, 253, 0, 247, 23, 0, 26, 0,
	28, 232, 0, 253, 59, 46, 48, 54, 0, 52,
	53, -2, 101, 0, 0, 143, 144, 145, 146, 0,
	0, 0, 0, 183, 0, 170, 109, 0, 251, 186,
	187, 188, 189, 190, 191, 192, 171, 172, 173, 174,
	176, 107, 108, 0, 229, 230, 194, 195, 0, 0,
	0, 89, 99, 92, 200, 0, 227, 228, 0, 0,
	21, 234, 0, 0, 253, 243, 237, 238, 239, 240,
	241, 242, 27, 29, 30, 99, 0, 0, 49, 55,
	0, 57, 58, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 128, 129, 130, 131,
	132, 133, 134, 104, 0, 0, 0, 0, 139, 0,
	0, 161, 162, 163, 0, 0, 0, 121, 0, 0,
	177, 14, 219, 0, 222, 219, 0, 217, 200, 208,
	0, 100, 0, 139, 0, 248, 24, 0, 244, 196,
	47, 60, 61, 63, 64, 74, 72, 0, 56, 50,
	0, 184, 102, 103, 106, 122, 0, 124, 126, 110,
	111, 112, 0, 136, 137, 0, 0, 0, 0, 114,
	116, 0, 120, 147, 148, 149, 150, 151, 152, 153,
	154, 155, 156, 157, 160, 141, 142, 105, 252, 138,
	0, 225, 158, 159, 164, 165, 0, 0, 168, 0,
	181, 178, 0, 16, 0, 221, 223, 0, 17, 216,
	0, 208, 19, 0, 0, 0, 253, 25, 198, 0,
	0, 0, 0, 0, 79, 0, 0, 82, 0, 0,
	0, 93, 75, 0, 77, 78, 0, 73, 0, 0,
	123, 125, 127, 0, 113, 115, 117, 0, 0, 140,
	166, 0, 169, 0, 179, 0, 0, 0, 0, 218,
	18, 209, 201, 202, 205, 0, 22, 200, 0, 0,
	62, 68, 0, 71, 80, 81, 83, 0, 85, 0,
	87, 88, 65, 0, 0, 0, 76, 66, 67, 51,
	185, 135, 0, 118, 167, 175, 182, 0, 0, 224,
	0, 0, 0, 204, 206, 207, 208, 199, 197, 0,
	0, 84, 86, 0, 0, 0, 119, 180, 0, 210,
	211, 203, 212, 69, 70, 0, 0, 0, 220, 13,
	0, 0, 0, 97, 0, 0, 213, 0, 94, 0,
	95, 96, 0, 98, 214,
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
		//line sql.y:183
		{
			setParseTree(yylex, yyDollar[1].statement)
		}
	case 2:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:189
		{
			yyVAL.statement = yyDollar[1].selStmt
		}
	case 13:
		yyDollar = yyS[yypt-12 : yypt+1]
		//line sql.y:205
		{
			yyVAL.selStmt = &Select{Comments: Comments(yyDollar[2].bytes2), Distinct: yyDollar[3].str, Hints: yyDollar[4].str, SelectExprs: yyDollar[5].selectExprs, From: yyDollar[6].tableExprs, Where: NewWhere(WhereStr, yyDollar[7].boolExpr), GroupBy: GroupBy(yyDollar[8].valExprs), Having: NewWhere(HavingStr, yyDollar[9].boolExpr), OrderBy: yyDollar[10].orderBy, Limit: yyDollar[11].limit, Lock: yyDollar[12].str}
		}
	case 14:
		yyDollar = yyS[yypt-6 : yypt+1]
		//line sql.y:209
		{
			yyVAL.selStmt = &Select{Comments: Comments(yyDollar[2].bytes2), SelectExprs: SelectExprs{Nextval{Expr: yyDollar[4].valExpr}}, From: TableExprs{&AliasedTableExpr{Expr: yyDollar[6].tableName}}}
		}
	case 15:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:213
		{
			yyVAL.selStmt = &Union{Type: yyDollar[2].str, Left: yyDollar[1].selStmt, Right: yyDollar[3].selStmt}
		}
	case 16:
		yyDollar = yyS[yypt-7 : yypt+1]
		//line sql.y:219
		{
			yyVAL.statement = &Insert{Comments: Comments(yyDollar[2].bytes2), Ignore: yyDollar[3].str, Table: yyDollar[4].tableName, Columns: yyDollar[5].columns, Rows: yyDollar[6].insRows, OnDup: OnDup(yyDollar[7].updateExprs)}
		}
	case 17:
		yyDollar = yyS[yypt-7 : yypt+1]
		//line sql.y:223
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
		//line sql.y:235
		{
			yyVAL.statement = &Update{Comments: Comments(yyDollar[2].bytes2), Table: yyDollar[3].tableName, Exprs: yyDollar[5].updateExprs, Where: NewWhere(WhereStr, yyDollar[6].boolExpr), OrderBy: yyDollar[7].orderBy, Limit: yyDollar[8].limit}
		}
	case 19:
		yyDollar = yyS[yypt-7 : yypt+1]
		//line sql.y:241
		{
			yyVAL.statement = &Delete{Comments: Comments(yyDollar[2].bytes2), Table: yyDollar[4].tableName, Where: NewWhere(WhereStr, yyDollar[5].boolExpr), OrderBy: yyDollar[6].orderBy, Limit: yyDollar[7].limit}
		}
	case 20:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:247
		{
			yyVAL.statement = &Set{Comments: Comments(yyDollar[2].bytes2), Exprs: yyDollar[3].updateExprs}
		}
	case 21:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:253
		{
			yyVAL.statement = &DDL{Action: CreateStr, NewName: yyDollar[4].tableIdent}
		}
	case 22:
		yyDollar = yyS[yypt-8 : yypt+1]
		//line sql.y:257
		{
			// Change this to an alter statement
			yyVAL.statement = &DDL{Action: AlterStr, Table: yyDollar[7].tableIdent, NewName: yyDollar[7].tableIdent}
		}
	case 23:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:262
		{
			yyVAL.statement = &DDL{Action: CreateStr, NewName: NewTableIdent(yyDollar[3].colIdent.Lowered())}
		}
	case 24:
		yyDollar = yyS[yypt-6 : yypt+1]
		//line sql.y:268
		{
			yyVAL.statement = &DDL{Action: AlterStr, Table: yyDollar[4].tableIdent, NewName: yyDollar[4].tableIdent}
		}
	case 25:
		yyDollar = yyS[yypt-7 : yypt+1]
		//line sql.y:272
		{
			// Change this to a rename statement
			yyVAL.statement = &DDL{Action: RenameStr, Table: yyDollar[4].tableIdent, NewName: yyDollar[7].tableIdent}
		}
	case 26:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:277
		{
			yyVAL.statement = &DDL{Action: AlterStr, Table: NewTableIdent(yyDollar[3].colIdent.Lowered()), NewName: NewTableIdent(yyDollar[3].colIdent.Lowered())}
		}
	case 27:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:283
		{
			yyVAL.statement = &DDL{Action: RenameStr, Table: yyDollar[3].tableIdent, NewName: yyDollar[5].tableIdent}
		}
	case 28:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:289
		{
			var exists bool
			if yyDollar[3].byt != 0 {
				exists = true
			}
			yyVAL.statement = &DDL{Action: DropStr, Table: yyDollar[4].tableIdent, IfExists: exists}
		}
	case 29:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:297
		{
			// Change this to an alter statement
			yyVAL.statement = &DDL{Action: AlterStr, Table: yyDollar[5].tableIdent, NewName: yyDollar[5].tableIdent}
		}
	case 30:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:302
		{
			var exists bool
			if yyDollar[3].byt != 0 {
				exists = true
			}
			yyVAL.statement = &DDL{Action: DropStr, Table: NewTableIdent(yyDollar[4].colIdent.Lowered()), IfExists: exists}
		}
	case 31:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:312
		{
			yyVAL.statement = &DDL{Action: AlterStr, Table: yyDollar[3].tableIdent, NewName: yyDollar[3].tableIdent}
		}
	case 32:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:318
		{
			yyVAL.statement = &Other{}
		}
	case 33:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:322
		{
			yyVAL.statement = &Other{}
		}
	case 34:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:326
		{
			yyVAL.statement = &Other{}
		}
	case 35:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:331
		{
			setAllowComments(yylex, true)
		}
	case 36:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:335
		{
			yyVAL.bytes2 = yyDollar[2].bytes2
			setAllowComments(yylex, false)
		}
	case 37:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:341
		{
			yyVAL.bytes2 = nil
		}
	case 38:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:345
		{
			yyVAL.bytes2 = append(yyDollar[1].bytes2, yyDollar[2].bytes)
		}
	case 39:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:351
		{
			yyVAL.str = UnionStr
		}
	case 40:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:355
		{
			yyVAL.str = UnionAllStr
		}
	case 41:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:359
		{
			yyVAL.str = UnionDistinctStr
		}
	case 42:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:364
		{
			yyVAL.str = ""
		}
	case 43:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:368
		{
			yyVAL.str = DistinctStr
		}
	case 44:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:373
		{
			yyVAL.str = ""
		}
	case 45:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:377
		{
			yyVAL.str = StraightJoinHint
		}
	case 46:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:383
		{
			yyVAL.selectExprs = SelectExprs{yyDollar[1].selectExpr}
		}
	case 47:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:387
		{
			yyVAL.selectExprs = append(yyVAL.selectExprs, yyDollar[3].selectExpr)
		}
	case 48:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:393
		{
			yyVAL.selectExpr = &StarExpr{}
		}
	case 49:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:397
		{
			yyVAL.selectExpr = &NonStarExpr{Expr: yyDollar[1].expr, As: yyDollar[2].colIdent}
		}
	case 50:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:401
		{
			yyVAL.selectExpr = &StarExpr{TableName: &TableName{Name: yyDollar[1].tableIdent}}
		}
	case 51:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:405
		{
			yyVAL.selectExpr = &StarExpr{TableName: &TableName{Qualifier: yyDollar[1].tableIdent, Name: yyDollar[3].tableIdent}}
		}
	case 52:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:411
		{
			yyVAL.expr = yyDollar[1].boolExpr
		}
	case 53:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:415
		{
			yyVAL.expr = yyDollar[1].valExpr
		}
	case 54:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:420
		{
			yyVAL.colIdent = ColIdent{}
		}
	case 55:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:424
		{
			yyVAL.colIdent = yyDollar[1].colIdent
		}
	case 56:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:428
		{
			yyVAL.colIdent = yyDollar[2].colIdent
		}
	case 58:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:435
		{
			yyVAL.colIdent = NewColIdent(string(yyDollar[1].bytes))
		}
	case 59:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:440
		{
			yyVAL.tableExprs = TableExprs{&AliasedTableExpr{Expr: &TableName{Name: NewTableIdent("dual")}}}
		}
	case 60:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:444
		{
			yyVAL.tableExprs = yyDollar[2].tableExprs
		}
	case 61:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:450
		{
			yyVAL.tableExprs = TableExprs{yyDollar[1].tableExpr}
		}
	case 62:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:454
		{
			yyVAL.tableExprs = append(yyVAL.tableExprs, yyDollar[3].tableExpr)
		}
	case 65:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:464
		{
			yyVAL.tableExpr = &AliasedTableExpr{Expr: yyDollar[1].tableName, As: yyDollar[2].tableIdent, Hints: yyDollar[3].indexHints}
		}
	case 66:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:468
		{
			yyVAL.tableExpr = &AliasedTableExpr{Expr: yyDollar[1].subquery, As: yyDollar[3].tableIdent}
		}
	case 67:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:472
		{
			yyVAL.tableExpr = &ParenTableExpr{Exprs: yyDollar[2].tableExprs}
		}
	case 68:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:485
		{
			yyVAL.tableExpr = &JoinTableExpr{LeftExpr: yyDollar[1].tableExpr, Join: yyDollar[2].str, RightExpr: yyDollar[3].tableExpr}
		}
	case 69:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:489
		{
			yyVAL.tableExpr = &JoinTableExpr{LeftExpr: yyDollar[1].tableExpr, Join: yyDollar[2].str, RightExpr: yyDollar[3].tableExpr, On: yyDollar[5].boolExpr}
		}
	case 70:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:493
		{
			yyVAL.tableExpr = &JoinTableExpr{LeftExpr: yyDollar[1].tableExpr, Join: yyDollar[2].str, RightExpr: yyDollar[3].tableExpr, On: yyDollar[5].boolExpr}
		}
	case 71:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:497
		{
			yyVAL.tableExpr = &JoinTableExpr{LeftExpr: yyDollar[1].tableExpr, Join: yyDollar[2].str, RightExpr: yyDollar[3].tableExpr}
		}
	case 72:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:502
		{
			yyVAL.empty = struct{}{}
		}
	case 73:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:504
		{
			yyVAL.empty = struct{}{}
		}
	case 74:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:507
		{
			yyVAL.tableIdent = NewTableIdent("")
		}
	case 75:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:511
		{
			yyVAL.tableIdent = yyDollar[1].tableIdent
		}
	case 76:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:515
		{
			yyVAL.tableIdent = yyDollar[2].tableIdent
		}
	case 78:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:522
		{
			yyVAL.tableIdent = NewTableIdent(string(yyDollar[1].bytes))
		}
	case 79:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:528
		{
			yyVAL.str = JoinStr
		}
	case 80:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:532
		{
			yyVAL.str = JoinStr
		}
	case 81:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:536
		{
			yyVAL.str = JoinStr
		}
	case 82:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:540
		{
			yyVAL.str = StraightJoinStr
		}
	case 83:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:546
		{
			yyVAL.str = LeftJoinStr
		}
	case 84:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:550
		{
			yyVAL.str = LeftJoinStr
		}
	case 85:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:554
		{
			yyVAL.str = RightJoinStr
		}
	case 86:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:558
		{
			yyVAL.str = RightJoinStr
		}
	case 87:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:564
		{
			yyVAL.str = NaturalJoinStr
		}
	case 88:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:568
		{
			if yyDollar[2].str == LeftJoinStr {
				yyVAL.str = NaturalLeftJoinStr
			} else {
				yyVAL.str = NaturalRightJoinStr
			}
		}
	case 89:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:578
		{
			yyVAL.tableName = yyDollar[2].tableName
		}
	case 90:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:582
		{
			yyVAL.tableName = yyDollar[1].tableName
		}
	case 91:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:588
		{
			yyVAL.tableName = &TableName{Name: yyDollar[1].tableIdent}
		}
	case 92:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:592
		{
			yyVAL.tableName = &TableName{Qualifier: yyDollar[1].tableIdent, Name: yyDollar[3].tableIdent}
		}
	case 93:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:597
		{
			yyVAL.indexHints = nil
		}
	case 94:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:601
		{
			yyVAL.indexHints = &IndexHints{Type: UseStr, Indexes: yyDollar[4].colIdents}
		}
	case 95:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:605
		{
			yyVAL.indexHints = &IndexHints{Type: IgnoreStr, Indexes: yyDollar[4].colIdents}
		}
	case 96:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:609
		{
			yyVAL.indexHints = &IndexHints{Type: ForceStr, Indexes: yyDollar[4].colIdents}
		}
	case 97:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:615
		{
			yyVAL.colIdents = []ColIdent{yyDollar[1].colIdent}
		}
	case 98:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:619
		{
			yyVAL.colIdents = append(yyDollar[1].colIdents, yyDollar[3].colIdent)
		}
	case 99:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:624
		{
			yyVAL.boolExpr = nil
		}
	case 100:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:628
		{
			yyVAL.boolExpr = yyDollar[2].boolExpr
		}
	case 102:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:635
		{
			yyVAL.boolExpr = &AndExpr{Left: yyDollar[1].boolExpr, Right: yyDollar[3].boolExpr}
		}
	case 103:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:639
		{
			yyVAL.boolExpr = &OrExpr{Left: yyDollar[1].boolExpr, Right: yyDollar[3].boolExpr}
		}
	case 104:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:643
		{
			yyVAL.boolExpr = &NotExpr{Expr: yyDollar[2].boolExpr}
		}
	case 105:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:647
		{
			yyVAL.boolExpr = &ParenBoolExpr{Expr: yyDollar[2].boolExpr}
		}
	case 106:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:651
		{
			yyVAL.boolExpr = &IsExpr{Operator: yyDollar[3].str, Expr: yyDollar[1].boolExpr}
		}
	case 107:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:657
		{
			yyVAL.boolVal = BoolVal(true)
		}
	case 108:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:661
		{
			yyVAL.boolVal = BoolVal(false)
		}
	case 109:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:667
		{
			yyVAL.boolExpr = yyDollar[1].boolVal
		}
	case 110:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:671
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: yyDollar[2].str, Right: yyDollar[3].boolVal}
		}
	case 111:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:675
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: yyDollar[2].str, Right: yyDollar[3].valExpr}
		}
	case 112:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:679
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: InStr, Right: yyDollar[3].colTuple}
		}
	case 113:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:683
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: NotInStr, Right: yyDollar[4].colTuple}
		}
	case 114:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:687
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: LikeStr, Right: yyDollar[3].valExpr}
		}
	case 115:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:691
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: NotLikeStr, Right: yyDollar[4].valExpr}
		}
	case 116:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:695
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: RegexpStr, Right: yyDollar[3].valExpr}
		}
	case 117:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:699
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: NotRegexpStr, Right: yyDollar[4].valExpr}
		}
	case 118:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:703
		{
			yyVAL.boolExpr = &RangeCond{Left: yyDollar[1].valExpr, Operator: BetweenStr, From: yyDollar[3].valExpr, To: yyDollar[5].valExpr}
		}
	case 119:
		yyDollar = yyS[yypt-6 : yypt+1]
		//line sql.y:707
		{
			yyVAL.boolExpr = &RangeCond{Left: yyDollar[1].valExpr, Operator: NotBetweenStr, From: yyDollar[4].valExpr, To: yyDollar[6].valExpr}
		}
	case 120:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:711
		{
			yyVAL.boolExpr = &IsExpr{Operator: yyDollar[3].str, Expr: yyDollar[1].valExpr}
		}
	case 121:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:715
		{
			yyVAL.boolExpr = &ExistsExpr{Subquery: yyDollar[2].subquery}
		}
	case 122:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:721
		{
			yyVAL.str = IsNullStr
		}
	case 123:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:725
		{
			yyVAL.str = IsNotNullStr
		}
	case 124:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:729
		{
			yyVAL.str = IsTrueStr
		}
	case 125:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:733
		{
			yyVAL.str = IsNotTrueStr
		}
	case 126:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:737
		{
			yyVAL.str = IsFalseStr
		}
	case 127:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:741
		{
			yyVAL.str = IsNotFalseStr
		}
	case 128:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:747
		{
			yyVAL.str = EqualStr
		}
	case 129:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:751
		{
			yyVAL.str = LessThanStr
		}
	case 130:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:755
		{
			yyVAL.str = GreaterThanStr
		}
	case 131:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:759
		{
			yyVAL.str = LessEqualStr
		}
	case 132:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:763
		{
			yyVAL.str = GreaterEqualStr
		}
	case 133:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:767
		{
			yyVAL.str = NotEqualStr
		}
	case 134:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:771
		{
			yyVAL.str = NullSafeEqualStr
		}
	case 135:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:777
		{
			yyVAL.colTuple = ValTuple(yyDollar[2].valExprs)
		}
	case 136:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:781
		{
			yyVAL.colTuple = yyDollar[1].subquery
		}
	case 137:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:785
		{
			yyVAL.colTuple = ListArg(yyDollar[1].bytes)
		}
	case 138:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:791
		{
			yyVAL.subquery = &Subquery{yyDollar[2].selStmt}
		}
	case 139:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:797
		{
			yyVAL.valExprs = ValExprs{yyDollar[1].valExpr}
		}
	case 140:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:801
		{
			yyVAL.valExprs = append(yyDollar[1].valExprs, yyDollar[3].valExpr)
		}
	case 141:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:807
		{
			yyVAL.str = string(yyDollar[1].bytes)
		}
	case 142:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:811
		{
			yyVAL.str = string(yyDollar[1].bytes)
		}
	case 143:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:817
		{
			yyVAL.valExpr = yyDollar[1].valExpr
		}
	case 144:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:821
		{
			yyVAL.valExpr = yyDollar[1].colName
		}
	case 145:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:825
		{
			yyVAL.valExpr = yyDollar[1].valTuple
		}
	case 146:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:829
		{
			yyVAL.valExpr = yyDollar[1].subquery
		}
	case 147:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:833
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: BitAndStr, Right: yyDollar[3].valExpr}
		}
	case 148:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:837
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: BitOrStr, Right: yyDollar[3].valExpr}
		}
	case 149:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:841
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: BitXorStr, Right: yyDollar[3].valExpr}
		}
	case 150:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:845
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: PlusStr, Right: yyDollar[3].valExpr}
		}
	case 151:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:849
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: MinusStr, Right: yyDollar[3].valExpr}
		}
	case 152:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:853
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: MultStr, Right: yyDollar[3].valExpr}
		}
	case 153:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:857
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: DivStr, Right: yyDollar[3].valExpr}
		}
	case 154:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:861
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: ModStr, Right: yyDollar[3].valExpr}
		}
	case 155:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:865
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: ModStr, Right: yyDollar[3].valExpr}
		}
	case 156:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:869
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: ShiftLeftStr, Right: yyDollar[3].valExpr}
		}
	case 157:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:873
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: ShiftRightStr, Right: yyDollar[3].valExpr}
		}
	case 158:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:877
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].colName, Operator: JSONExtractOp, Right: yyDollar[3].valExpr}
		}
	case 159:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:881
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].colName, Operator: JSONUnquoteExtractOp, Right: yyDollar[3].valExpr}
		}
	case 160:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:885
		{
			yyVAL.valExpr = &CollateExpr{Expr: yyDollar[1].valExpr, Charset: yyDollar[3].str}
		}
	case 161:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:889
		{
			if num, ok := yyDollar[2].valExpr.(*SQLVal); ok && num.Type == IntVal {
				yyVAL.valExpr = num
			} else {
				yyVAL.valExpr = &UnaryExpr{Operator: UPlusStr, Expr: yyDollar[2].valExpr}
			}
		}
	case 162:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:897
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
	case 163:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:911
		{
			yyVAL.valExpr = &UnaryExpr{Operator: TildaStr, Expr: yyDollar[2].valExpr}
		}
	case 164:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:915
		{
			// This rule prevents the usage of INTERVAL
			// as a function. If support is needed for that,
			// we'll need to revisit this. The solution
			// will be non-trivial because of grammar conflicts.
			yyVAL.valExpr = &IntervalExpr{Expr: yyDollar[2].valExpr, Unit: yyDollar[3].colIdent}
		}
	case 165:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:923
		{
			yyVAL.valExpr = &FuncExpr{Name: yyDollar[1].colIdent}
		}
	case 166:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:927
		{
			yyVAL.valExpr = &FuncExpr{Name: yyDollar[1].colIdent, Exprs: yyDollar[3].selectExprs}
		}
	case 167:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:931
		{
			yyVAL.valExpr = &FuncExpr{Name: yyDollar[1].colIdent, Distinct: true, Exprs: yyDollar[4].selectExprs}
		}
	case 168:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:935
		{
			yyVAL.valExpr = &FuncExpr{Name: yyDollar[1].colIdent}
		}
	case 169:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:939
		{
			yyVAL.valExpr = &FuncExpr{Name: yyDollar[1].colIdent, Exprs: yyDollar[3].selectExprs}
		}
	case 170:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:943
		{
			yyVAL.valExpr = yyDollar[1].caseExpr
		}
	case 171:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:949
		{
			yyVAL.colIdent = NewColIdent("if")
		}
	case 172:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:953
		{
			yyVAL.colIdent = NewColIdent("current_timestamp")
		}
	case 173:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:957
		{
			yyVAL.colIdent = NewColIdent("database")
		}
	case 174:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:961
		{
			yyVAL.colIdent = NewColIdent("mod")
		}
	case 175:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:967
		{
			yyVAL.caseExpr = &CaseExpr{Expr: yyDollar[2].valExpr, Whens: yyDollar[3].whens, Else: yyDollar[4].valExpr}
		}
	case 176:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:972
		{
			yyVAL.valExpr = nil
		}
	case 177:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:976
		{
			yyVAL.valExpr = yyDollar[1].valExpr
		}
	case 178:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:982
		{
			yyVAL.whens = []*When{yyDollar[1].when}
		}
	case 179:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:986
		{
			yyVAL.whens = append(yyDollar[1].whens, yyDollar[2].when)
		}
	case 180:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:992
		{
			yyVAL.when = &When{Cond: yyDollar[2].boolExpr, Val: yyDollar[4].valExpr}
		}
	case 181:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:997
		{
			yyVAL.valExpr = nil
		}
	case 182:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:1001
		{
			yyVAL.valExpr = yyDollar[2].valExpr
		}
	case 183:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1007
		{
			yyVAL.colName = &ColName{Name: yyDollar[1].colIdent}
		}
	case 184:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1011
		{
			yyVAL.colName = &ColName{Qualifier: &TableName{Name: yyDollar[1].tableIdent}, Name: yyDollar[3].colIdent}
		}
	case 185:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:1015
		{
			yyVAL.colName = &ColName{Qualifier: &TableName{Qualifier: yyDollar[1].tableIdent, Name: yyDollar[3].tableIdent}, Name: yyDollar[5].colIdent}
		}
	case 186:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1021
		{
			yyVAL.valExpr = NewStrVal(yyDollar[1].bytes)
		}
	case 187:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1025
		{
			yyVAL.valExpr = NewHexVal(yyDollar[1].bytes)
		}
	case 188:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1029
		{
			yyVAL.valExpr = NewIntVal(yyDollar[1].bytes)
		}
	case 189:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1033
		{
			yyVAL.valExpr = NewFloatVal(yyDollar[1].bytes)
		}
	case 190:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1037
		{
			yyVAL.valExpr = NewHexNum(yyDollar[1].bytes)
		}
	case 191:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1041
		{
			yyVAL.valExpr = NewValArg(yyDollar[1].bytes)
		}
	case 192:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1045
		{
			yyVAL.valExpr = &NullVal{}
		}
	case 193:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1051
		{
			// TODO(sougou): Deprecate this construct.
			if yyDollar[1].colIdent.Lowered() != "value" {
				yylex.Error("expecting value after next")
				return 1
			}
			yyVAL.valExpr = NewIntVal([]byte("1"))
		}
	case 194:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:1060
		{
			yyVAL.valExpr = NewIntVal(yyDollar[1].bytes)
		}
	case 195:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:1064
		{
			yyVAL.valExpr = NewValArg(yyDollar[1].bytes)
		}
	case 196:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1069
		{
			yyVAL.valExprs = nil
		}
	case 197:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1073
		{
			yyVAL.valExprs = yyDollar[3].valExprs
		}
	case 198:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1078
		{
			yyVAL.boolExpr = nil
		}
	case 199:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:1082
		{
			yyVAL.boolExpr = yyDollar[2].boolExpr
		}
	case 200:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1087
		{
			yyVAL.orderBy = nil
		}
	case 201:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1091
		{
			yyVAL.orderBy = yyDollar[3].orderBy
		}
	case 202:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1097
		{
			yyVAL.orderBy = OrderBy{yyDollar[1].order}
		}
	case 203:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1101
		{
			yyVAL.orderBy = append(yyDollar[1].orderBy, yyDollar[3].order)
		}
	case 204:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:1107
		{
			yyVAL.order = &Order{Expr: yyDollar[1].valExpr, Direction: yyDollar[2].str}
		}
	case 205:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1112
		{
			yyVAL.str = AscScr
		}
	case 206:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1116
		{
			yyVAL.str = AscScr
		}
	case 207:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1120
		{
			yyVAL.str = DescScr
		}
	case 208:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1125
		{
			yyVAL.limit = nil
		}
	case 209:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:1129
		{
			yyVAL.limit = &Limit{Rowcount: yyDollar[2].valExpr}
		}
	case 210:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:1133
		{
			yyVAL.limit = &Limit{Offset: yyDollar[2].valExpr, Rowcount: yyDollar[4].valExpr}
		}
	case 211:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:1137
		{
			yyVAL.limit = &Limit{Offset: yyDollar[4].valExpr, Rowcount: yyDollar[2].valExpr}
		}
	case 212:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1142
		{
			yyVAL.str = ""
		}
	case 213:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:1146
		{
			yyVAL.str = ForUpdateStr
		}
	case 214:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:1150
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
	case 215:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1163
		{
			yyVAL.columns = nil
		}
	case 216:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1167
		{
			yyVAL.columns = yyDollar[2].columns
		}
	case 217:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1173
		{
			yyVAL.columns = Columns{yyDollar[1].colIdent}
		}
	case 218:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1177
		{
			yyVAL.columns = append(yyVAL.columns, yyDollar[3].colIdent)
		}
	case 219:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1182
		{
			yyVAL.updateExprs = nil
		}
	case 220:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:1186
		{
			yyVAL.updateExprs = yyDollar[5].updateExprs
		}
	case 221:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:1192
		{
			yyVAL.insRows = yyDollar[2].values
		}
	case 222:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1196
		{
			yyVAL.insRows = yyDollar[1].selStmt
		}
	case 223:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1202
		{
			yyVAL.values = Values{yyDollar[1].valTuple}
		}
	case 224:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1206
		{
			yyVAL.values = append(yyDollar[1].values, yyDollar[3].valTuple)
		}
	case 225:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1212
		{
			yyVAL.valTuple = ValTuple(yyDollar[2].valExprs)
		}
	case 226:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1218
		{
			yyVAL.updateExprs = UpdateExprs{yyDollar[1].updateExpr}
		}
	case 227:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1222
		{
			yyVAL.updateExprs = append(yyDollar[1].updateExprs, yyDollar[3].updateExpr)
		}
	case 228:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1228
		{
			yyVAL.updateExpr = &UpdateExpr{Name: yyDollar[1].colIdent, Expr: yyDollar[3].valExpr}
		}
	case 231:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1237
		{
			yyVAL.byt = 0
		}
	case 232:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:1239
		{
			yyVAL.byt = 1
		}
	case 233:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1242
		{
			yyVAL.empty = struct{}{}
		}
	case 234:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1244
		{
			yyVAL.empty = struct{}{}
		}
	case 235:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1247
		{
			yyVAL.str = ""
		}
	case 236:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1249
		{
			yyVAL.str = IgnoreStr
		}
	case 237:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1253
		{
			yyVAL.empty = struct{}{}
		}
	case 238:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1255
		{
			yyVAL.empty = struct{}{}
		}
	case 239:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1257
		{
			yyVAL.empty = struct{}{}
		}
	case 240:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1259
		{
			yyVAL.empty = struct{}{}
		}
	case 241:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1261
		{
			yyVAL.empty = struct{}{}
		}
	case 242:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1263
		{
			yyVAL.empty = struct{}{}
		}
	case 243:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1266
		{
			yyVAL.empty = struct{}{}
		}
	case 244:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1268
		{
			yyVAL.empty = struct{}{}
		}
	case 245:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1271
		{
			yyVAL.empty = struct{}{}
		}
	case 246:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1273
		{
			yyVAL.empty = struct{}{}
		}
	case 247:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1276
		{
			yyVAL.empty = struct{}{}
		}
	case 248:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:1278
		{
			yyVAL.empty = struct{}{}
		}
	case 249:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1282
		{
			yyVAL.colIdent = NewColIdent(string(yyDollar[1].bytes))
		}
	case 250:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1288
		{
			yyVAL.tableIdent = NewTableIdent(string(yyDollar[1].bytes))
		}
	case 251:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1294
		{
			if incNesting(yylex) {
				yylex.Error("max nesting level reached")
				return 1
			}
		}
	case 252:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1303
		{
			decNesting(yylex)
		}
	case 253:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1308
		{
			forceEOF(yylex)
		}
	}
	goto yystack /* stack new state and value */
}
