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
const LE = 57405
const GE = 57406
const NE = 57407
const NULL_SAFE_EQUAL = 57408
const IS = 57409
const LIKE = 57410
const REGEXP = 57411
const IN = 57412
const SHIFT_LEFT = 57413
const SHIFT_RIGHT = 57414
const UNARY = 57415
const INTERVAL = 57416
const END = 57417
const CREATE = 57418
const ALTER = 57419
const DROP = 57420
const RENAME = 57421
const ANALYZE = 57422
const TABLE = 57423
const INDEX = 57424
const VIEW = 57425
const TO = 57426
const IGNORE = 57427
const IF = 57428
const UNIQUE = 57429
const USING = 57430
const SHOW = 57431
const DESCRIBE = 57432
const EXPLAIN = 57433
const CURRENT_TIMESTAMP = 57434
const DATABASE = 57435
const MOD = 57436
const UNUSED = 57437

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
	"'^'",
	"'~'",
	"UNARY",
	"INTERVAL",
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
	"CURRENT_TIMESTAMP",
	"DATABASE",
	"MOD",
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
	45, 232,
	90, 232,
	-2, 231,
}

const yyNprod = 236
const yyPrivate = 57344

var yyTokenNames []string
var yyStates []string

const yyLast = 924

var yyAct = [...]int{

	131, 296, 245, 415, 62, 222, 151, 348, 206, 115,
	132, 257, 305, 236, 338, 234, 205, 3, 102, 287,
	251, 238, 155, 146, 161, 103, 35, 235, 37, 74,
	47, 41, 38, 367, 369, 64, 67, 159, 69, 231,
	40, 71, 41, 97, 50, 43, 44, 45, 398, 397,
	396, 68, 70, 107, 58, 81, 48, 49, 163, 46,
	42, 186, 185, 193, 194, 188, 189, 190, 191, 192,
	187, 378, 125, 320, 63, 125, 87, 101, 187, 109,
	421, 387, 388, 88, 177, 112, 288, 64, 336, 288,
	64, 144, 368, 188, 189, 190, 191, 192, 187, 90,
	94, 92, 96, 158, 160, 157, 171, 143, 190, 191,
	192, 187, 175, 174, 242, 264, 84, 225, 112, 112,
	173, 162, 152, 323, 324, 325, 177, 212, 262, 263,
	261, 214, 166, 176, 175, 213, 186, 185, 193, 194,
	188, 189, 190, 191, 192, 187, 176, 175, 177, 65,
	220, 248, 380, 60, 112, 125, 228, 174, 60, 125,
	217, 177, 176, 175, 148, 202, 204, 260, 221, 109,
	316, 14, 28, 241, 243, 112, 240, 177, 423, 248,
	239, 112, 112, 112, 93, 203, 258, 229, 172, 75,
	259, 232, 169, 246, 233, 252, 254, 255, 60, 65,
	253, 224, 82, 83, 89, 268, 384, 279, 280, 282,
	125, 125, 283, 60, 248, 284, 65, 281, 248, 112,
	64, 294, 168, 248, 292, 147, 243, 295, 168, 249,
	250, 285, 303, 248, 339, 291, 89, 186, 185, 193,
	194, 188, 189, 190, 191, 192, 187, 240, 281, 321,
	385, 239, 344, 248, 112, 382, 319, 303, 219, 376,
	89, 392, 73, 339, 108, 227, 322, 326, 258, 100,
	79, 395, 259, 327, 394, 125, 149, 186, 185, 193,
	194, 188, 189, 190, 191, 192, 187, 362, 333, 360,
	359, 358, 363, 142, 361, 112, 343, 345, 207, 141,
	341, 86, 208, 209, 210, 211, 335, 342, 76, 39,
	240, 240, 240, 240, 239, 239, 239, 239, 355, 14,
	357, 373, 372, 216, 413, 374, 365, 55, 401, 375,
	350, 354, 364, 356, 311, 312, 414, 226, 153, 381,
	54, 57, 337, 290, 85, 383, 193, 194, 188, 189,
	190, 191, 192, 187, 108, 99, 318, 389, 297, 112,
	108, 353, 391, 331, 256, 51, 52, 265, 266, 267,
	298, 269, 270, 271, 272, 273, 274, 275, 276, 277,
	278, 186, 185, 193, 194, 188, 189, 190, 191, 192,
	187, 405, 402, 404, 140, 223, 352, 302, 108, 112,
	112, 139, 147, 408, 409, 410, 390, 61, 150, 416,
	416, 416, 64, 417, 418, 420, 419, 14, 422, 14,
	424, 425, 426, 411, 427, 28, 30, 428, 307, 310,
	311, 312, 308, 108, 309, 313, 1, 317, 393, 314,
	170, 59, 154, 36, 226, 230, 406, 407, 328, 329,
	330, 72, 156, 66, 138, 77, 125, 293, 218, 109,
	127, 126, 128, 129, 412, 386, 130, 332, 347, 120,
	59, 351, 29, 137, 301, 91, 334, 215, 286, 95,
	121, 114, 98, 346, 349, 340, 80, 106, 31, 32,
	33, 34, 116, 117, 59, 113, 145, 247, 118, 124,
	119, 307, 310, 311, 312, 308, 164, 309, 313, 165,
	289, 178, 110, 133, 366, 306, 304, 377, 237, 134,
	135, 136, 379, 125, 167, 248, 109, 127, 126, 128,
	129, 105, 78, 130, 122, 123, 53, 27, 111, 226,
	137, 14, 15, 16, 17, 56, 13, 59, 185, 193,
	194, 188, 189, 190, 191, 192, 187, 12, 11, 116,
	117, 104, 399, 18, 65, 118, 400, 119, 10, 9,
	403, 349, 8, 7, 6, 5, 4, 106, 59, 2,
	133, 0, 244, 106, 124, 0, 134, 135, 136, 0,
	0, 0, 0, 186, 185, 193, 194, 188, 189, 190,
	191, 192, 187, 0, 0, 0, 0, 0, 125, 0,
	248, 109, 127, 126, 128, 129, 0, 0, 130, 122,
	123, 106, 0, 111, 0, 137, 0, 19, 20, 22,
	21, 23, 0, 0, 244, 0, 299, 0, 0, 300,
	24, 25, 26, 0, 116, 117, 104, 315, 0, 59,
	118, 0, 119, 0, 0, 0, 106, 0, 0, 0,
	0, 124, 0, 0, 0, 133, 0, 0, 0, 0,
	0, 134, 135, 136, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 125, 0, 0, 109, 127,
	126, 128, 129, 0, 0, 130, 122, 123, 0, 0,
	111, 0, 137, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 59, 59, 59, 59, 14, 0, 0, 0,
	0, 116, 117, 104, 0, 370, 371, 118, 0, 119,
	0, 124, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 133, 0, 0, 0, 0, 124, 134, 135,
	136, 0, 0, 0, 0, 125, 0, 0, 109, 127,
	126, 128, 129, 0, 0, 130, 122, 123, 0, 0,
	111, 125, 137, 0, 109, 127, 126, 128, 129, 0,
	0, 130, 122, 123, 0, 0, 111, 0, 137, 0,
	0, 116, 117, 0, 0, 0, 0, 118, 0, 119,
	0, 0, 0, 0, 0, 0, 0, 116, 117, 0,
	0, 0, 133, 118, 0, 119, 0, 0, 134, 135,
	136, 0, 0, 0, 0, 0, 0, 0, 133, 0,
	0, 0, 0, 0, 134, 135, 136, 125, 0, 0,
	109, 127, 126, 128, 129, 0, 0, 130, 0, 0,
	0, 0, 0, 0, 137, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 116, 117, 0, 0, 0, 0, 118,
	0, 119, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 133, 0, 0, 180, 183, 0,
	134, 135, 136, 195, 196, 197, 198, 199, 200, 201,
	184, 181, 182, 179, 186, 185, 193, 194, 188, 189,
	190, 191, 192, 187,
}
var yyPact = [...]int{

	535, -1000, -1000, 420, -1000, -1000, -1000, -1000, -1000, -1000,
	-1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, -71,
	-59, -37, -52, -38, -1000, -1000, -1000, 413, 347, 308,
	-1000, -70, 105, 397, 101, -66, -47, 101, -1000, -45,
	101, -1000, 105, -73, 141, -73, 105, -1000, -1000, -1000,
	-1000, -1000, -1000, 235, 151, -1000, 62, 320, 273, -14,
	-1000, 105, 158, -1000, 33, -1000, 105, 41, 136, -1000,
	105, -1000, -57, 105, 334, 225, 101, -1000, 640, -1000,
	384, -1000, 269, 263, -1000, 105, 101, 105, 391, 101,
	792, -1000, 317, -82, -1000, 10, -1000, 105, -1000, -1000,
	105, -1000, 182, -1000, -1000, 168, 30, 75, 837, -1000,
	-1000, 726, 710, -1000, -1000, -1000, 792, 792, 792, 792,
	166, -1000, -1000, -1000, 166, -1000, -1000, -1000, -1000, -1000,
	-1000, -1000, -1000, -1000, -1000, -1000, -1000, 792, 105, -1000,
	-1000, -1000, -1000, 230, 214, -1000, 381, 726, -1000, -16,
	27, 411, -1000, -1000, 221, 101, -1000, -61, -1000, -1000,
	-1000, -1000, -1000, -1000, -1000, -1000, -1000, 391, 640, 110,
	-1000, -1000, 101, 31, 478, 726, 726, 140, 792, 114,
	54, 792, 792, 792, 140, 792, 792, 792, 792, 792,
	792, 792, 792, 792, 792, -1000, -1000, -1000, -1000, -1000,
	-1000, -1000, 11, 837, 104, 167, 171, 837, -1000, -1000,
	-1000, 516, 563, -1000, 413, 26, -16, -1000, 313, 101,
	101, 381, 342, 355, 75, 121, -16, 105, -1000, -1000,
	105, -1000, 385, -1000, 211, 467, -1000, -1000, 150, 336,
	165, -1000, -1000, -1000, -17, -1000, 176, 640, -1000, 11,
	53, -1000, -1000, 68, -1000, -1000, -16, -1000, 411, -1000,
	-1000, 114, 792, 792, 792, -16, -16, 304, -1000, 267,
	470, -1000, 25, 25, -8, -8, -8, 12, 12, -1000,
	-1000, 792, -1000, -1000, -1000, 176, 23, -1000, 726, 219,
	166, 420, 190, 206, -1000, 342, -1000, 792, 792, -1000,
	-1000, 383, 346, 110, 110, 110, 110, -1000, 257, 256,
	-1000, 255, 253, 298, -9, -1000, 105, 105, -1000, 186,
	101, -1000, 176, -1000, -1000, -1000, 171, -1000, -16, -16,
	200, 792, -16, -1000, -20, -1000, 792, 88, -1000, 314,
	209, -1000, -1000, -1000, 101, -1000, 160, 204, -1000, 59,
	-1000, 381, 726, 792, 467, 217, 394, -1000, -1000, -1000,
	-1000, 240, -1000, 237, -1000, -1000, -1000, -48, -49, -50,
	-1000, -1000, -1000, -1000, -1000, -1000, 792, -16, -1000, -16,
	792, 302, 166, -1000, 792, 792, -1000, -1000, -1000, 342,
	75, 202, 726, 726, -1000, -1000, 166, 166, 166, -16,
	-16, 415, -1000, -16, -1000, 307, 75, 75, 101, 101,
	101, 101, -1000, 407, 4, 132, -1000, 132, 132, 158,
	-1000, 101, -1000, 101, -1000, -1000, 101, -1000, -1000,
}
var yyPgo = [...]int{

	0, 579, 16, 576, 575, 574, 573, 572, 569, 568,
	558, 557, 546, 472, 545, 537, 536, 532, 18, 25,
	531, 524, 15, 27, 13, 518, 516, 12, 515, 21,
	514, 3, 23, 53, 512, 511, 510, 495, 185, 486,
	20, 11, 8, 485, 9, 10, 481, 480, 478, 19,
	477, 476, 474, 471, 469, 5, 468, 7, 465, 1,
	464, 458, 457, 14, 4, 74, 454, 309, 262, 453,
	452, 445, 443, 442, 0, 440, 408, 439, 437, 30,
	436, 426, 6, 2,
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
	38, 38, 38, 38, 38, 54, 54, 54, 54, 47,
	50, 50, 48, 48, 49, 51, 51, 46, 46, 46,
	37, 37, 37, 37, 37, 39, 39, 39, 52, 52,
	53, 53, 55, 55, 56, 56, 57, 58, 58, 58,
	59, 59, 59, 60, 60, 60, 61, 61, 62, 62,
	63, 63, 36, 36, 43, 43, 44, 44, 64, 64,
	65, 66, 66, 68, 68, 69, 69, 67, 67, 70,
	70, 70, 70, 70, 70, 71, 71, 72, 72, 73,
	73, 74, 76, 82, 83, 79,
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
	1, 3, 1, 1, 1, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 2, 2, 2, 3, 3,
	4, 5, 3, 4, 1, 1, 1, 1, 1, 5,
	0, 1, 1, 2, 4, 0, 2, 1, 3, 5,
	1, 1, 1, 1, 1, 1, 2, 2, 0, 3,
	0, 2, 0, 3, 1, 3, 2, 0, 1, 1,
	0, 2, 4, 0, 2, 4, 0, 3, 1, 3,
	0, 5, 2, 1, 1, 3, 3, 1, 1, 3,
	3, 1, 1, 0, 2, 0, 3, 0, 1, 1,
	1, 1, 1, 1, 1, 0, 1, 0, 1, 0,
	2, 1, 1, 1, 1, 0,
}
var yyChk = [...]int{

	-1000, -80, -1, -2, -3, -4, -5, -6, -7, -8,
	-9, -10, -11, -12, 6, 7, 8, 9, 28, 92,
	93, 95, 94, 96, 105, 106, 107, -15, 5, -13,
	-81, -13, -13, -13, -13, 97, -72, 99, 103, -67,
	99, 101, 97, 97, 98, 99, 97, -79, -79, -79,
	-2, 18, 19, -16, 32, 19, -14, -67, -29, -76,
	48, 10, -64, -65, -74, 48, -69, 102, 98, -74,
	97, -74, -76, -68, 102, 48, -68, -76, -17, 35,
	-39, -74, 51, 52, 54, 24, 28, 90, -29, 46,
	66, -76, 60, 48, -79, -76, -79, 100, -76, 21,
	44, -74, -18, -19, 83, -20, -76, -33, -38, 48,
	-34, 60, -82, -37, -46, -44, 81, 82, 87, 89,
	-54, -47, 56, 57, 21, 45, 50, 49, 51, 52,
	55, -74, -45, 102, 108, 109, 110, 62, -66, 17,
	10, 30, 30, -29, -64, -76, -32, 11, -65, -38,
	-76, -82, -79, 21, -73, 104, -70, 95, 93, 27,
	94, 14, 111, 48, -76, -76, -79, -21, 46, 10,
	-75, -74, 20, 90, -82, 59, 58, 73, -35, 76,
	60, 74, 75, 61, 73, 78, 77, 86, 81, 82,
	83, 84, 85, 79, 80, 66, 67, 68, 69, 70,
	71, 72, -33, -38, -33, -2, -42, -38, -38, -38,
	-38, -38, -82, -45, -82, -50, -38, -29, -61, 28,
	-82, -32, -55, 14, -33, 90, -38, 44, -74, -79,
	-71, 100, -32, -19, -22, -23, -24, -25, -29, -45,
	-82, -74, 83, -74, -76, -83, -18, 19, 47, -33,
	-33, -40, 55, 60, 56, 57, -38, -41, -82, -45,
	53, 76, 74, 75, 61, -38, -38, -38, -40, -38,
	-38, -38, -38, -38, -38, -38, -38, -38, -38, -83,
	-83, 46, -83, -74, -83, -18, -48, -49, 63, -36,
	30, -2, -64, -62, -74, -55, -59, 16, 15, -76,
	-76, -52, 12, 46, -26, -27, -28, 34, 38, 40,
	35, 36, 37, 41, -77, -76, 20, -78, 20, -22,
	90, -83, -18, 55, 56, 57, -42, -41, -38, -38,
	-38, 59, -38, -83, -51, -49, 65, -33, -63, 44,
	-43, -44, -63, -83, 46, -59, -38, -56, -57, -38,
	-79, -53, 13, 15, -23, -24, -23, -24, 34, 34,
	34, 39, 34, 39, 34, -27, -30, 42, 101, 43,
	-76, -76, -83, -74, -83, -83, 59, -38, 91, -38,
	64, 25, 46, -74, 46, 46, -58, 22, 23, -55,
	-33, -42, 44, 44, 34, 34, 98, 98, 98, -38,
	-38, 26, -44, -38, -57, -59, -33, -33, -82, -82,
	-82, 8, -60, 17, 29, -31, -74, -31, -31, -64,
	8, 76, -83, 46, -83, -83, -74, -74, -74,
}
var yyDef = [...]int{

	0, -2, 1, 2, 3, 4, 5, 6, 7, 8,
	9, 10, 11, 12, 35, 35, 35, 35, 35, 227,
	217, 0, 0, 0, 235, 235, 235, 0, 39, 42,
	37, 217, 0, 0, 0, 215, 0, 0, 228, 0,
	0, 218, 0, 213, 0, 213, 0, 32, 33, 34,
	15, 40, 41, 44, 0, 43, 36, 0, 0, 84,
	232, 0, 20, 208, 0, 231, 0, 0, 0, 235,
	0, 235, 0, 0, 0, 0, 0, 31, 0, 45,
	0, 175, 0, 0, 38, 0, 0, 0, 92, 0,
	0, 235, 0, 229, 23, 0, 26, 0, 28, 214,
	0, 235, 56, 46, 48, 53, 0, 51, 52, -2,
	94, 0, 0, 132, 133, 134, 0, 0, 0, 0,
	0, 154, 100, 101, 0, 233, 170, 171, 172, 173,
	174, 167, 207, 155, 156, 157, 158, 160, 0, 211,
	212, 176, 177, 196, 92, 85, 182, 0, 209, 210,
	0, 0, 21, 216, 0, 0, 235, 225, 219, 220,
	221, 222, 223, 224, 27, 29, 30, 92, 0, 0,
	49, 54, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 119, 120, 121, 122, 123,
	124, 125, 97, 0, 0, 0, 0, 130, 145, 146,
	147, 0, 0, 112, 0, 0, 161, 14, 0, 0,
	0, 182, 190, 0, 93, 0, 130, 0, 230, 24,
	0, 226, 178, 47, 57, 58, 60, 61, 71, 69,
	0, 55, 50, 168, 0, 149, 0, 0, 234, 95,
	96, 99, 113, 0, 115, 117, 102, 103, 0, 127,
	128, 0, 0, 0, 0, 105, 107, 0, 111, 135,
	136, 137, 138, 139, 140, 141, 142, 143, 144, 98,
	129, 0, 206, 148, 152, 0, 165, 162, 0, 200,
	0, 203, 200, 0, 198, 190, 19, 0, 0, 235,
	25, 180, 0, 0, 0, 0, 0, 74, 0, 0,
	77, 0, 0, 0, 86, 72, 0, 0, 70, 0,
	0, 150, 0, 114, 116, 118, 0, 104, 106, 108,
	0, 0, 131, 153, 0, 163, 0, 0, 16, 0,
	202, 204, 17, 197, 0, 18, 191, 183, 184, 187,
	22, 182, 0, 0, 59, 65, 0, 68, 75, 76,
	78, 0, 80, 0, 82, 83, 62, 0, 0, 0,
	73, 63, 64, 169, 151, 126, 0, 109, 159, 166,
	0, 0, 0, 199, 0, 0, 186, 188, 189, 190,
	181, 179, 0, 0, 79, 81, 0, 0, 0, 110,
	164, 0, 205, 192, 185, 193, 66, 67, 0, 0,
	0, 0, 13, 0, 0, 0, 90, 0, 0, 201,
	194, 0, 87, 0, 88, 89, 0, 91, 195,
}
var yyTok1 = [...]int{

	1, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 85, 78, 3,
	45, 47, 83, 81, 46, 82, 90, 84, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	67, 66, 68, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 86, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 77, 3, 87,
}
var yyTok2 = [...]int{

	2, 3, 4, 5, 6, 7, 8, 9, 10, 11,
	12, 13, 14, 15, 16, 17, 18, 19, 20, 21,
	22, 23, 24, 25, 26, 27, 28, 29, 30, 31,
	32, 33, 34, 35, 36, 37, 38, 39, 40, 41,
	42, 43, 44, 48, 49, 50, 51, 52, 53, 54,
	55, 56, 57, 58, 59, 60, 61, 62, 63, 64,
	65, 69, 70, 71, 72, 73, 74, 75, 76, 79,
	80, 88, 89, 91, 92, 93, 94, 95, 96, 97,
	98, 99, 100, 101, 102, 103, 104, 105, 106, 107,
	108, 109, 110, 111,
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
		//line sql.y:175
		{
			setParseTree(yylex, yyDollar[1].statement)
		}
	case 2:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:181
		{
			yyVAL.statement = yyDollar[1].selStmt
		}
	case 13:
		yyDollar = yyS[yypt-12 : yypt+1]
		//line sql.y:197
		{
			yyVAL.selStmt = &Select{Comments: Comments(yyDollar[2].bytes2), Distinct: yyDollar[3].str, Hints: yyDollar[4].str, SelectExprs: yyDollar[5].selectExprs, From: yyDollar[6].tableExprs, Where: NewWhere(WhereStr, yyDollar[7].boolExpr), GroupBy: GroupBy(yyDollar[8].valExprs), Having: NewWhere(HavingStr, yyDollar[9].boolExpr), OrderBy: yyDollar[10].orderBy, Limit: yyDollar[11].limit, Lock: yyDollar[12].str}
		}
	case 14:
		yyDollar = yyS[yypt-6 : yypt+1]
		//line sql.y:201
		{
			yyVAL.selStmt = &Select{Comments: Comments(yyDollar[2].bytes2), SelectExprs: SelectExprs{Nextval{Expr: yyDollar[4].valExpr}}, From: TableExprs{&AliasedTableExpr{Expr: yyDollar[6].tableName}}}
		}
	case 15:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:205
		{
			yyVAL.selStmt = &Union{Type: yyDollar[2].str, Left: yyDollar[1].selStmt, Right: yyDollar[3].selStmt}
		}
	case 16:
		yyDollar = yyS[yypt-8 : yypt+1]
		//line sql.y:211
		{
			yyVAL.statement = &Insert{Comments: Comments(yyDollar[2].bytes2), Ignore: yyDollar[3].str, Table: yyDollar[5].tableName, Columns: yyDollar[6].columns, Rows: yyDollar[7].insRows, OnDup: OnDup(yyDollar[8].updateExprs)}
		}
	case 17:
		yyDollar = yyS[yypt-8 : yypt+1]
		//line sql.y:215
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
		//line sql.y:227
		{
			yyVAL.statement = &Update{Comments: Comments(yyDollar[2].bytes2), Table: yyDollar[3].tableName, Exprs: yyDollar[5].updateExprs, Where: NewWhere(WhereStr, yyDollar[6].boolExpr), OrderBy: yyDollar[7].orderBy, Limit: yyDollar[8].limit}
		}
	case 19:
		yyDollar = yyS[yypt-7 : yypt+1]
		//line sql.y:233
		{
			yyVAL.statement = &Delete{Comments: Comments(yyDollar[2].bytes2), Table: yyDollar[4].tableName, Where: NewWhere(WhereStr, yyDollar[5].boolExpr), OrderBy: yyDollar[6].orderBy, Limit: yyDollar[7].limit}
		}
	case 20:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:239
		{
			yyVAL.statement = &Set{Comments: Comments(yyDollar[2].bytes2), Exprs: yyDollar[3].updateExprs}
		}
	case 21:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:245
		{
			yyVAL.statement = &DDL{Action: CreateStr, NewName: yyDollar[4].tableIdent}
		}
	case 22:
		yyDollar = yyS[yypt-8 : yypt+1]
		//line sql.y:249
		{
			// Change this to an alter statement
			yyVAL.statement = &DDL{Action: AlterStr, Table: yyDollar[7].tableIdent, NewName: yyDollar[7].tableIdent}
		}
	case 23:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:254
		{
			yyVAL.statement = &DDL{Action: CreateStr, NewName: TableIdent(yyDollar[3].colIdent.Lowered())}
		}
	case 24:
		yyDollar = yyS[yypt-6 : yypt+1]
		//line sql.y:260
		{
			yyVAL.statement = &DDL{Action: AlterStr, Table: yyDollar[4].tableIdent, NewName: yyDollar[4].tableIdent}
		}
	case 25:
		yyDollar = yyS[yypt-7 : yypt+1]
		//line sql.y:264
		{
			// Change this to a rename statement
			yyVAL.statement = &DDL{Action: RenameStr, Table: yyDollar[4].tableIdent, NewName: yyDollar[7].tableIdent}
		}
	case 26:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:269
		{
			yyVAL.statement = &DDL{Action: AlterStr, Table: TableIdent(yyDollar[3].colIdent.Lowered()), NewName: TableIdent(yyDollar[3].colIdent.Lowered())}
		}
	case 27:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:275
		{
			yyVAL.statement = &DDL{Action: RenameStr, Table: yyDollar[3].tableIdent, NewName: yyDollar[5].tableIdent}
		}
	case 28:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:281
		{
			var exists bool
			if yyDollar[3].byt != 0 {
				exists = true
			}
			yyVAL.statement = &DDL{Action: DropStr, Table: yyDollar[4].tableIdent, IfExists: exists}
		}
	case 29:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:289
		{
			// Change this to an alter statement
			yyVAL.statement = &DDL{Action: AlterStr, Table: yyDollar[5].tableIdent, NewName: yyDollar[5].tableIdent}
		}
	case 30:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:294
		{
			var exists bool
			if yyDollar[3].byt != 0 {
				exists = true
			}
			yyVAL.statement = &DDL{Action: DropStr, Table: TableIdent(yyDollar[4].colIdent.Lowered()), IfExists: exists}
		}
	case 31:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:304
		{
			yyVAL.statement = &DDL{Action: AlterStr, Table: yyDollar[3].tableIdent, NewName: yyDollar[3].tableIdent}
		}
	case 32:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:310
		{
			yyVAL.statement = &Other{}
		}
	case 33:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:314
		{
			yyVAL.statement = &Other{}
		}
	case 34:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:318
		{
			yyVAL.statement = &Other{}
		}
	case 35:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:323
		{
			setAllowComments(yylex, true)
		}
	case 36:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:327
		{
			yyVAL.bytes2 = yyDollar[2].bytes2
			setAllowComments(yylex, false)
		}
	case 37:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:333
		{
			yyVAL.bytes2 = nil
		}
	case 38:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:337
		{
			yyVAL.bytes2 = append(yyDollar[1].bytes2, yyDollar[2].bytes)
		}
	case 39:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:343
		{
			yyVAL.str = UnionStr
		}
	case 40:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:347
		{
			yyVAL.str = UnionAllStr
		}
	case 41:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:351
		{
			yyVAL.str = UnionDistinctStr
		}
	case 42:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:356
		{
			yyVAL.str = ""
		}
	case 43:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:360
		{
			yyVAL.str = DistinctStr
		}
	case 44:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:365
		{
			yyVAL.str = ""
		}
	case 45:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:369
		{
			yyVAL.str = StraightJoinHint
		}
	case 46:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:375
		{
			yyVAL.selectExprs = SelectExprs{yyDollar[1].selectExpr}
		}
	case 47:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:379
		{
			yyVAL.selectExprs = append(yyVAL.selectExprs, yyDollar[3].selectExpr)
		}
	case 48:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:385
		{
			yyVAL.selectExpr = &StarExpr{}
		}
	case 49:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:389
		{
			yyVAL.selectExpr = &NonStarExpr{Expr: yyDollar[1].expr, As: yyDollar[2].colIdent}
		}
	case 50:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:393
		{
			yyVAL.selectExpr = &StarExpr{TableName: yyDollar[1].tableIdent}
		}
	case 51:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:399
		{
			yyVAL.expr = yyDollar[1].boolExpr
		}
	case 52:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:403
		{
			yyVAL.expr = yyDollar[1].valExpr
		}
	case 53:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:408
		{
			yyVAL.colIdent = ColIdent{}
		}
	case 54:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:412
		{
			yyVAL.colIdent = yyDollar[1].colIdent
		}
	case 55:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:416
		{
			yyVAL.colIdent = yyDollar[2].colIdent
		}
	case 56:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:421
		{
			yyVAL.tableExprs = TableExprs{&AliasedTableExpr{Expr: &TableName{Name: "dual"}}}
		}
	case 57:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:425
		{
			yyVAL.tableExprs = yyDollar[2].tableExprs
		}
	case 58:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:431
		{
			yyVAL.tableExprs = TableExprs{yyDollar[1].tableExpr}
		}
	case 59:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:435
		{
			yyVAL.tableExprs = append(yyVAL.tableExprs, yyDollar[3].tableExpr)
		}
	case 62:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:445
		{
			yyVAL.tableExpr = &AliasedTableExpr{Expr: yyDollar[1].tableName, As: yyDollar[2].tableIdent, Hints: yyDollar[3].indexHints}
		}
	case 63:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:449
		{
			yyVAL.tableExpr = &AliasedTableExpr{Expr: yyDollar[1].subquery, As: yyDollar[3].tableIdent}
		}
	case 64:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:453
		{
			yyVAL.tableExpr = &ParenTableExpr{Exprs: yyDollar[2].tableExprs}
		}
	case 65:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:466
		{
			yyVAL.tableExpr = &JoinTableExpr{LeftExpr: yyDollar[1].tableExpr, Join: yyDollar[2].str, RightExpr: yyDollar[3].tableExpr}
		}
	case 66:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:470
		{
			yyVAL.tableExpr = &JoinTableExpr{LeftExpr: yyDollar[1].tableExpr, Join: yyDollar[2].str, RightExpr: yyDollar[3].tableExpr, On: yyDollar[5].boolExpr}
		}
	case 67:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:474
		{
			yyVAL.tableExpr = &JoinTableExpr{LeftExpr: yyDollar[1].tableExpr, Join: yyDollar[2].str, RightExpr: yyDollar[3].tableExpr, On: yyDollar[5].boolExpr}
		}
	case 68:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:478
		{
			yyVAL.tableExpr = &JoinTableExpr{LeftExpr: yyDollar[1].tableExpr, Join: yyDollar[2].str, RightExpr: yyDollar[3].tableExpr}
		}
	case 69:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:483
		{
			yyVAL.empty = struct{}{}
		}
	case 70:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:485
		{
			yyVAL.empty = struct{}{}
		}
	case 71:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:488
		{
			yyVAL.tableIdent = ""
		}
	case 72:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:492
		{
			yyVAL.tableIdent = yyDollar[1].tableIdent
		}
	case 73:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:496
		{
			yyVAL.tableIdent = yyDollar[2].tableIdent
		}
	case 74:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:502
		{
			yyVAL.str = JoinStr
		}
	case 75:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:506
		{
			yyVAL.str = JoinStr
		}
	case 76:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:510
		{
			yyVAL.str = JoinStr
		}
	case 77:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:514
		{
			yyVAL.str = StraightJoinStr
		}
	case 78:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:520
		{
			yyVAL.str = LeftJoinStr
		}
	case 79:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:524
		{
			yyVAL.str = LeftJoinStr
		}
	case 80:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:528
		{
			yyVAL.str = RightJoinStr
		}
	case 81:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:532
		{
			yyVAL.str = RightJoinStr
		}
	case 82:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:538
		{
			yyVAL.str = NaturalJoinStr
		}
	case 83:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:542
		{
			if yyDollar[2].str == LeftJoinStr {
				yyVAL.str = NaturalLeftJoinStr
			} else {
				yyVAL.str = NaturalRightJoinStr
			}
		}
	case 84:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:552
		{
			yyVAL.tableName = &TableName{Name: yyDollar[1].tableIdent}
		}
	case 85:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:556
		{
			yyVAL.tableName = &TableName{Qualifier: yyDollar[1].tableIdent, Name: yyDollar[3].tableIdent}
		}
	case 86:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:561
		{
			yyVAL.indexHints = nil
		}
	case 87:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:565
		{
			yyVAL.indexHints = &IndexHints{Type: UseStr, Indexes: yyDollar[4].colIdents}
		}
	case 88:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:569
		{
			yyVAL.indexHints = &IndexHints{Type: IgnoreStr, Indexes: yyDollar[4].colIdents}
		}
	case 89:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:573
		{
			yyVAL.indexHints = &IndexHints{Type: ForceStr, Indexes: yyDollar[4].colIdents}
		}
	case 90:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:579
		{
			yyVAL.colIdents = []ColIdent{yyDollar[1].colIdent}
		}
	case 91:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:583
		{
			yyVAL.colIdents = append(yyDollar[1].colIdents, yyDollar[3].colIdent)
		}
	case 92:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:588
		{
			yyVAL.boolExpr = nil
		}
	case 93:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:592
		{
			yyVAL.boolExpr = yyDollar[2].boolExpr
		}
	case 95:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:599
		{
			yyVAL.boolExpr = &AndExpr{Left: yyDollar[1].boolExpr, Right: yyDollar[3].boolExpr}
		}
	case 96:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:603
		{
			yyVAL.boolExpr = &OrExpr{Left: yyDollar[1].boolExpr, Right: yyDollar[3].boolExpr}
		}
	case 97:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:607
		{
			yyVAL.boolExpr = &NotExpr{Expr: yyDollar[2].boolExpr}
		}
	case 98:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:611
		{
			yyVAL.boolExpr = &ParenBoolExpr{Expr: yyDollar[2].boolExpr}
		}
	case 99:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:615
		{
			yyVAL.boolExpr = &IsExpr{Operator: yyDollar[3].str, Expr: yyDollar[1].boolExpr}
		}
	case 100:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:621
		{
			yyVAL.boolExpr = BoolVal(true)
		}
	case 101:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:625
		{
			yyVAL.boolExpr = BoolVal(false)
		}
	case 102:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:629
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: yyDollar[2].str, Right: yyDollar[3].valExpr}
		}
	case 103:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:633
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: InStr, Right: yyDollar[3].colTuple}
		}
	case 104:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:637
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: NotInStr, Right: yyDollar[4].colTuple}
		}
	case 105:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:641
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: LikeStr, Right: yyDollar[3].valExpr}
		}
	case 106:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:645
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: NotLikeStr, Right: yyDollar[4].valExpr}
		}
	case 107:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:649
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: RegexpStr, Right: yyDollar[3].valExpr}
		}
	case 108:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:653
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: NotRegexpStr, Right: yyDollar[4].valExpr}
		}
	case 109:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:657
		{
			yyVAL.boolExpr = &RangeCond{Left: yyDollar[1].valExpr, Operator: BetweenStr, From: yyDollar[3].valExpr, To: yyDollar[5].valExpr}
		}
	case 110:
		yyDollar = yyS[yypt-6 : yypt+1]
		//line sql.y:661
		{
			yyVAL.boolExpr = &RangeCond{Left: yyDollar[1].valExpr, Operator: NotBetweenStr, From: yyDollar[4].valExpr, To: yyDollar[6].valExpr}
		}
	case 111:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:665
		{
			yyVAL.boolExpr = &IsExpr{Operator: yyDollar[3].str, Expr: yyDollar[1].valExpr}
		}
	case 112:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:669
		{
			yyVAL.boolExpr = &ExistsExpr{Subquery: yyDollar[2].subquery}
		}
	case 113:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:675
		{
			yyVAL.str = IsNullStr
		}
	case 114:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:679
		{
			yyVAL.str = IsNotNullStr
		}
	case 115:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:683
		{
			yyVAL.str = IsTrueStr
		}
	case 116:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:687
		{
			yyVAL.str = IsNotTrueStr
		}
	case 117:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:691
		{
			yyVAL.str = IsFalseStr
		}
	case 118:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:695
		{
			yyVAL.str = IsNotFalseStr
		}
	case 119:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:701
		{
			yyVAL.str = EqualStr
		}
	case 120:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:705
		{
			yyVAL.str = LessThanStr
		}
	case 121:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:709
		{
			yyVAL.str = GreaterThanStr
		}
	case 122:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:713
		{
			yyVAL.str = LessEqualStr
		}
	case 123:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:717
		{
			yyVAL.str = GreaterEqualStr
		}
	case 124:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:721
		{
			yyVAL.str = NotEqualStr
		}
	case 125:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:725
		{
			yyVAL.str = NullSafeEqualStr
		}
	case 126:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:731
		{
			yyVAL.colTuple = ValTuple(yyDollar[2].valExprs)
		}
	case 127:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:735
		{
			yyVAL.colTuple = yyDollar[1].subquery
		}
	case 128:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:739
		{
			yyVAL.colTuple = ListArg(yyDollar[1].bytes)
		}
	case 129:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:745
		{
			yyVAL.subquery = &Subquery{yyDollar[2].selStmt}
		}
	case 130:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:751
		{
			yyVAL.valExprs = ValExprs{yyDollar[1].valExpr}
		}
	case 131:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:755
		{
			yyVAL.valExprs = append(yyDollar[1].valExprs, yyDollar[3].valExpr)
		}
	case 132:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:761
		{
			yyVAL.valExpr = yyDollar[1].valExpr
		}
	case 133:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:765
		{
			yyVAL.valExpr = yyDollar[1].colName
		}
	case 134:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:769
		{
			yyVAL.valExpr = yyDollar[1].rowTuple
		}
	case 135:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:773
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: BitAndStr, Right: yyDollar[3].valExpr}
		}
	case 136:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:777
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: BitOrStr, Right: yyDollar[3].valExpr}
		}
	case 137:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:781
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: BitXorStr, Right: yyDollar[3].valExpr}
		}
	case 138:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:785
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: PlusStr, Right: yyDollar[3].valExpr}
		}
	case 139:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:789
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: MinusStr, Right: yyDollar[3].valExpr}
		}
	case 140:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:793
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: MultStr, Right: yyDollar[3].valExpr}
		}
	case 141:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:797
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: DivStr, Right: yyDollar[3].valExpr}
		}
	case 142:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:801
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: ModStr, Right: yyDollar[3].valExpr}
		}
	case 143:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:805
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: ShiftLeftStr, Right: yyDollar[3].valExpr}
		}
	case 144:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:809
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: ShiftRightStr, Right: yyDollar[3].valExpr}
		}
	case 145:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:813
		{
			if num, ok := yyDollar[2].valExpr.(NumVal); ok {
				yyVAL.valExpr = num
			} else {
				yyVAL.valExpr = &UnaryExpr{Operator: UPlusStr, Expr: yyDollar[2].valExpr}
			}
		}
	case 146:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:821
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
	case 147:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:834
		{
			yyVAL.valExpr = &UnaryExpr{Operator: TildaStr, Expr: yyDollar[2].valExpr}
		}
	case 148:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:838
		{
			// This rule prevents the usage of INTERVAL
			// as a function. If support is needed for that,
			// we'll need to revisit this. The solution
			// will be non-trivial because of grammar conflicts.
			yyVAL.valExpr = &IntervalExpr{Expr: yyDollar[2].valExpr, Unit: yyDollar[3].colIdent}
		}
	case 149:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:846
		{
			yyVAL.valExpr = &FuncExpr{Name: string(yyDollar[1].tableIdent)}
		}
	case 150:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:850
		{
			yyVAL.valExpr = &FuncExpr{Name: string(yyDollar[1].tableIdent), Exprs: yyDollar[3].selectExprs}
		}
	case 151:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:854
		{
			yyVAL.valExpr = &FuncExpr{Name: string(yyDollar[1].tableIdent), Distinct: true, Exprs: yyDollar[4].selectExprs}
		}
	case 152:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:858
		{
			yyVAL.valExpr = &FuncExpr{Name: yyDollar[1].str}
		}
	case 153:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:862
		{
			yyVAL.valExpr = &FuncExpr{Name: yyDollar[1].str, Exprs: yyDollar[3].selectExprs}
		}
	case 154:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:866
		{
			yyVAL.valExpr = yyDollar[1].caseExpr
		}
	case 155:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:872
		{
			yyVAL.str = "if"
		}
	case 156:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:876
		{
			yyVAL.str = "current_timestamp"
		}
	case 157:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:880
		{
			yyVAL.str = "database"
		}
	case 158:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:884
		{
			yyVAL.str = "mod"
		}
	case 159:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:890
		{
			yyVAL.caseExpr = &CaseExpr{Expr: yyDollar[2].valExpr, Whens: yyDollar[3].whens, Else: yyDollar[4].valExpr}
		}
	case 160:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:895
		{
			yyVAL.valExpr = nil
		}
	case 161:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:899
		{
			yyVAL.valExpr = yyDollar[1].valExpr
		}
	case 162:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:905
		{
			yyVAL.whens = []*When{yyDollar[1].when}
		}
	case 163:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:909
		{
			yyVAL.whens = append(yyDollar[1].whens, yyDollar[2].when)
		}
	case 164:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:915
		{
			yyVAL.when = &When{Cond: yyDollar[2].boolExpr, Val: yyDollar[4].valExpr}
		}
	case 165:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:920
		{
			yyVAL.valExpr = nil
		}
	case 166:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:924
		{
			yyVAL.valExpr = yyDollar[2].valExpr
		}
	case 167:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:930
		{
			yyVAL.colName = &ColName{Name: yyDollar[1].colIdent}
		}
	case 168:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:934
		{
			yyVAL.colName = &ColName{Qualifier: &TableName{Name: yyDollar[1].tableIdent}, Name: yyDollar[3].colIdent}
		}
	case 169:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:938
		{
			yyVAL.colName = &ColName{Qualifier: &TableName{Qualifier: yyDollar[1].tableIdent, Name: yyDollar[3].tableIdent}, Name: yyDollar[5].colIdent}
		}
	case 170:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:944
		{
			yyVAL.valExpr = StrVal(yyDollar[1].bytes)
		}
	case 171:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:948
		{
			yyVAL.valExpr = HexVal(yyDollar[1].bytes)
		}
	case 172:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:952
		{
			yyVAL.valExpr = NumVal(yyDollar[1].bytes)
		}
	case 173:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:956
		{
			yyVAL.valExpr = ValArg(yyDollar[1].bytes)
		}
	case 174:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:960
		{
			yyVAL.valExpr = &NullVal{}
		}
	case 175:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:966
		{
			// TODO(sougou): Deprecate this construct.
			if yyDollar[1].colIdent.Lowered() != "value" {
				yylex.Error("expecting value after next")
				return 1
			}
			yyVAL.valExpr = NumVal("1")
		}
	case 176:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:975
		{
			yyVAL.valExpr = NumVal(yyDollar[1].bytes)
		}
	case 177:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:979
		{
			yyVAL.valExpr = ValArg(yyDollar[1].bytes)
		}
	case 178:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:984
		{
			yyVAL.valExprs = nil
		}
	case 179:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:988
		{
			yyVAL.valExprs = yyDollar[3].valExprs
		}
	case 180:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:993
		{
			yyVAL.boolExpr = nil
		}
	case 181:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:997
		{
			yyVAL.boolExpr = yyDollar[2].boolExpr
		}
	case 182:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1002
		{
			yyVAL.orderBy = nil
		}
	case 183:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1006
		{
			yyVAL.orderBy = yyDollar[3].orderBy
		}
	case 184:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1012
		{
			yyVAL.orderBy = OrderBy{yyDollar[1].order}
		}
	case 185:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1016
		{
			yyVAL.orderBy = append(yyDollar[1].orderBy, yyDollar[3].order)
		}
	case 186:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:1022
		{
			yyVAL.order = &Order{Expr: yyDollar[1].valExpr, Direction: yyDollar[2].str}
		}
	case 187:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1027
		{
			yyVAL.str = AscScr
		}
	case 188:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1031
		{
			yyVAL.str = AscScr
		}
	case 189:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1035
		{
			yyVAL.str = DescScr
		}
	case 190:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1040
		{
			yyVAL.limit = nil
		}
	case 191:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:1044
		{
			yyVAL.limit = &Limit{Rowcount: yyDollar[2].valExpr}
		}
	case 192:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:1048
		{
			yyVAL.limit = &Limit{Offset: yyDollar[2].valExpr, Rowcount: yyDollar[4].valExpr}
		}
	case 193:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1053
		{
			yyVAL.str = ""
		}
	case 194:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:1057
		{
			yyVAL.str = ForUpdateStr
		}
	case 195:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:1061
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
	case 196:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1074
		{
			yyVAL.columns = nil
		}
	case 197:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1078
		{
			yyVAL.columns = yyDollar[2].columns
		}
	case 198:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1084
		{
			yyVAL.columns = Columns{yyDollar[1].colIdent}
		}
	case 199:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1088
		{
			yyVAL.columns = append(yyVAL.columns, yyDollar[3].colIdent)
		}
	case 200:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1093
		{
			yyVAL.updateExprs = nil
		}
	case 201:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:1097
		{
			yyVAL.updateExprs = yyDollar[5].updateExprs
		}
	case 202:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:1103
		{
			yyVAL.insRows = yyDollar[2].values
		}
	case 203:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1107
		{
			yyVAL.insRows = yyDollar[1].selStmt
		}
	case 204:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1113
		{
			yyVAL.values = Values{yyDollar[1].rowTuple}
		}
	case 205:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1117
		{
			yyVAL.values = append(yyDollar[1].values, yyDollar[3].rowTuple)
		}
	case 206:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1123
		{
			yyVAL.rowTuple = ValTuple(yyDollar[2].valExprs)
		}
	case 207:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1127
		{
			yyVAL.rowTuple = yyDollar[1].subquery
		}
	case 208:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1133
		{
			yyVAL.updateExprs = UpdateExprs{yyDollar[1].updateExpr}
		}
	case 209:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1137
		{
			yyVAL.updateExprs = append(yyDollar[1].updateExprs, yyDollar[3].updateExpr)
		}
	case 210:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1143
		{
			yyVAL.updateExpr = &UpdateExpr{Name: yyDollar[1].colIdent, Expr: yyDollar[3].valExpr}
		}
	case 213:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1152
		{
			yyVAL.byt = 0
		}
	case 214:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:1154
		{
			yyVAL.byt = 1
		}
	case 215:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1157
		{
			yyVAL.empty = struct{}{}
		}
	case 216:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1159
		{
			yyVAL.empty = struct{}{}
		}
	case 217:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1162
		{
			yyVAL.str = ""
		}
	case 218:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1164
		{
			yyVAL.str = IgnoreStr
		}
	case 219:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1168
		{
			yyVAL.empty = struct{}{}
		}
	case 220:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1170
		{
			yyVAL.empty = struct{}{}
		}
	case 221:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1172
		{
			yyVAL.empty = struct{}{}
		}
	case 222:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1174
		{
			yyVAL.empty = struct{}{}
		}
	case 223:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1176
		{
			yyVAL.empty = struct{}{}
		}
	case 224:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1178
		{
			yyVAL.empty = struct{}{}
		}
	case 225:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1181
		{
			yyVAL.empty = struct{}{}
		}
	case 226:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1183
		{
			yyVAL.empty = struct{}{}
		}
	case 227:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1186
		{
			yyVAL.empty = struct{}{}
		}
	case 228:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1188
		{
			yyVAL.empty = struct{}{}
		}
	case 229:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1191
		{
			yyVAL.empty = struct{}{}
		}
	case 230:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:1193
		{
			yyVAL.empty = struct{}{}
		}
	case 231:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1197
		{
			yyVAL.colIdent = NewColIdent(string(yyDollar[1].bytes))
		}
	case 232:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1203
		{
			yyVAL.tableIdent = TableIdent(yyDollar[1].bytes)
		}
	case 233:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1209
		{
			if incNesting(yylex) {
				yylex.Error("max nesting level reached")
				return 1
			}
		}
	case 234:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1218
		{
			decNesting(yylex)
		}
	case 235:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1223
		{
			forceEOF(yylex)
		}
	}
	goto yystack /* stack new state and value */
}
