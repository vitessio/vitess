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
	45, 236,
	93, 236,
	-2, 235,
}

const yyNprod = 240
const yyPrivate = 57344

var yyTokenNames []string
var yyStates []string

const yyLast = 956

var yyAct = [...]int{

	133, 356, 249, 423, 62, 303, 152, 115, 208, 226,
	312, 240, 345, 107, 261, 238, 294, 102, 207, 3,
	113, 242, 239, 147, 103, 156, 74, 162, 35, 67,
	37, 47, 255, 41, 38, 64, 375, 377, 69, 235,
	160, 71, 40, 70, 41, 97, 50, 43, 44, 45,
	406, 405, 404, 68, 58, 81, 46, 48, 49, 42,
	126, 164, 195, 196, 189, 190, 191, 192, 193, 194,
	188, 116, 210, 211, 126, 327, 87, 101, 395, 396,
	63, 188, 268, 88, 109, 112, 429, 64, 386, 178,
	64, 145, 90, 92, 205, 295, 266, 267, 265, 376,
	84, 94, 176, 96, 89, 65, 172, 144, 229, 286,
	159, 161, 158, 175, 177, 176, 60, 178, 112, 112,
	388, 246, 174, 153, 109, 204, 206, 163, 216, 295,
	178, 343, 218, 167, 93, 187, 186, 195, 196, 189,
	190, 191, 192, 193, 194, 188, 191, 192, 193, 194,
	188, 224, 252, 126, 126, 112, 60, 232, 175, 75,
	323, 221, 228, 264, 177, 176, 256, 258, 259, 225,
	149, 257, 393, 108, 245, 247, 112, 244, 431, 252,
	178, 390, 112, 112, 112, 150, 126, 262, 60, 233,
	253, 254, 236, 250, 237, 286, 252, 217, 189, 190,
	191, 192, 193, 194, 188, 177, 176, 209, 28, 284,
	285, 287, 212, 213, 214, 215, 290, 65, 272, 291,
	82, 178, 83, 112, 64, 301, 14, 173, 299, 310,
	247, 288, 289, 220, 292, 302, 128, 127, 129, 130,
	131, 298, 243, 132, 330, 331, 332, 230, 169, 252,
	252, 244, 263, 328, 73, 65, 310, 252, 112, 148,
	326, 352, 252, 170, 108, 126, 400, 346, 60, 329,
	108, 333, 262, 346, 260, 89, 231, 269, 270, 271,
	334, 273, 274, 275, 276, 277, 278, 279, 280, 281,
	282, 283, 370, 100, 89, 340, 223, 371, 79, 169,
	76, 368, 112, 351, 349, 348, 369, 403, 353, 344,
	342, 108, 350, 126, 402, 367, 243, 244, 244, 244,
	244, 366, 39, 363, 372, 365, 318, 319, 381, 380,
	143, 373, 382, 362, 14, 364, 383, 263, 358, 55,
	142, 314, 317, 318, 319, 315, 108, 316, 320, 86,
	409, 401, 54, 391, 57, 389, 421, 230, 297, 85,
	29, 335, 336, 337, 154, 99, 325, 112, 422, 397,
	399, 51, 52, 141, 398, 304, 31, 32, 33, 34,
	140, 339, 243, 243, 243, 243, 314, 317, 318, 319,
	315, 361, 316, 320, 305, 412, 227, 349, 410, 354,
	357, 360, 309, 413, 148, 61, 151, 112, 112, 428,
	419, 416, 417, 418, 414, 415, 14, 424, 424, 424,
	64, 425, 426, 28, 427, 30, 430, 1, 432, 433,
	434, 324, 435, 385, 321, 436, 171, 155, 387, 59,
	36, 234, 157, 66, 230, 139, 300, 222, 420, 72,
	394, 355, 121, 77, 359, 308, 230, 187, 186, 195,
	196, 189, 190, 191, 192, 193, 194, 188, 59, 341,
	219, 293, 122, 91, 114, 347, 80, 95, 296, 407,
	98, 179, 110, 408, 374, 106, 313, 411, 357, 311,
	241, 251, 59, 125, 146, 168, 105, 78, 53, 27,
	56, 13, 12, 11, 165, 10, 9, 166, 8, 7,
	6, 5, 4, 2, 0, 0, 0, 126, 0, 252,
	109, 128, 127, 129, 130, 131, 0, 0, 132, 123,
	124, 0, 392, 111, 0, 138, 186, 195, 196, 189,
	190, 191, 192, 193, 194, 188, 59, 0, 0, 0,
	0, 0, 0, 0, 0, 117, 118, 104, 0, 0,
	137, 0, 119, 125, 120, 187, 186, 195, 196, 189,
	190, 191, 192, 193, 194, 188, 106, 59, 134, 0,
	0, 248, 106, 0, 135, 136, 0, 126, 384, 252,
	109, 128, 127, 129, 130, 131, 0, 0, 132, 123,
	124, 0, 0, 111, 0, 138, 0, 187, 186, 195,
	196, 189, 190, 191, 192, 193, 194, 188, 0, 0,
	0, 0, 0, 106, 0, 117, 118, 104, 0, 0,
	137, 0, 119, 0, 120, 0, 248, 125, 306, 0,
	0, 307, 0, 0, 0, 0, 0, 0, 134, 322,
	0, 59, 0, 0, 135, 136, 0, 0, 106, 0,
	0, 126, 338, 0, 109, 128, 127, 129, 130, 131,
	0, 0, 132, 123, 124, 0, 0, 111, 0, 138,
	0, 187, 186, 195, 196, 189, 190, 191, 192, 193,
	194, 188, 0, 0, 14, 0, 0, 0, 0, 117,
	118, 104, 0, 0, 137, 0, 119, 0, 120, 125,
	0, 0, 0, 0, 0, 0, 0, 59, 59, 59,
	59, 0, 134, 0, 0, 0, 125, 0, 135, 136,
	378, 379, 0, 126, 0, 0, 109, 128, 127, 129,
	130, 131, 0, 0, 132, 123, 124, 0, 0, 111,
	126, 138, 0, 109, 128, 127, 129, 130, 131, 0,
	0, 132, 123, 124, 0, 0, 111, 0, 138, 0,
	0, 117, 118, 0, 0, 0, 137, 14, 119, 0,
	120, 0, 0, 0, 0, 0, 0, 0, 117, 118,
	0, 0, 0, 137, 134, 119, 0, 120, 0, 0,
	135, 136, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 134, 0, 0, 0, 0, 126, 135, 136, 109,
	128, 127, 129, 130, 131, 0, 0, 132, 14, 15,
	16, 17, 0, 126, 138, 0, 109, 128, 127, 129,
	130, 131, 0, 0, 132, 0, 0, 0, 0, 0,
	18, 138, 0, 0, 117, 118, 0, 0, 0, 137,
	0, 119, 0, 120, 0, 0, 0, 0, 0, 0,
	0, 117, 118, 65, 0, 0, 137, 134, 119, 0,
	120, 0, 0, 135, 136, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 134, 0, 0, 0, 0, 0,
	135, 136, 0, 0, 187, 186, 195, 196, 189, 190,
	191, 192, 193, 194, 188, 0, 0, 0, 19, 20,
	22, 21, 23, 0, 0, 0, 0, 181, 184, 0,
	0, 24, 25, 26, 197, 198, 199, 200, 201, 202,
	203, 185, 182, 183, 180, 187, 186, 195, 196, 189,
	190, 191, 192, 193, 194, 188,
}
var yyPact = [...]int{

	822, -1000, -1000, 418, -1000, -1000, -1000, -1000, -1000, -1000,
	-1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, -73,
	-61, -42, -54, -45, -1000, -1000, -1000, 410, 353, 320,
	-1000, -72, 68, 395, 57, -77, -49, 57, -1000, -58,
	57, -1000, 68, -80, 111, -80, 68, -1000, -1000, -1000,
	-1000, -1000, -1000, 263, 169, -1000, 45, 335, 321, -17,
	-1000, 68, 58, -1000, 24, -1000, 68, 32, 86, -1000,
	68, -1000, -59, 68, 344, 249, 57, -1000, 616, -1000,
	363, -1000, 310, 300, -1000, 68, 57, 68, 393, 57,
	788, -1000, 343, -83, -1000, 13, -1000, 68, -1000, -1000,
	68, -1000, 253, -1000, -1000, 207, 29, 146, 866, -1000,
	-1000, 705, 688, -1000, -22, -1000, -1000, 788, 788, 788,
	788, 141, -1000, -1000, -1000, 141, -1000, -1000, -1000, -1000,
	-1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, 788, 68,
	-1000, -1000, -1000, -1000, 268, 248, -1000, 382, 705, -1000,
	378, 15, 771, -1000, -1000, 232, 57, -1000, -65, -1000,
	-1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, 393, 616,
	108, -1000, -1000, 57, 36, 472, 705, 705, 110, 788,
	109, 20, 788, 788, 788, 110, 788, 788, 788, 788,
	788, 788, 788, 788, 788, 788, 788, -1000, -1000, -1000,
	-1000, -1000, -1000, -1000, 14, 866, 105, 203, 149, 866,
	187, 187, -1000, -1000, -1000, 825, 542, -1000, 410, 31,
	378, -1000, 328, 57, 57, 382, 359, 379, 146, 76,
	378, 68, -1000, -1000, 68, -1000, 390, -1000, 183, 352,
	-1000, -1000, 140, 346, 220, -1000, -1000, -1000, -18, -1000,
	202, 616, -1000, 14, 42, -1000, -1000, 188, -1000, -1000,
	378, -1000, 771, -1000, -1000, 109, 788, 788, 788, 378,
	378, 602, -1000, -19, 456, -1000, 61, 61, -8, -8,
	-8, -8, 115, 115, -1000, -1000, 788, -1000, -1000, -1000,
	-1000, -1000, 202, 65, -1000, 705, 223, 141, 418, 229,
	215, -1000, 359, -1000, 788, 788, -1000, -1000, 388, 376,
	108, 108, 108, 108, -1000, 287, 281, -1000, 267, 258,
	290, -6, -1000, 68, 68, -1000, 210, 57, -1000, 202,
	-1000, -1000, -1000, 149, -1000, 378, 378, 528, 788, 378,
	-1000, 21, -1000, 788, 55, -1000, 330, 135, -1000, 788,
	-1000, -1000, 57, -1000, 486, 126, -1000, 56, -1000, 382,
	705, 788, 352, 222, 307, -1000, -1000, -1000, -1000, 280,
	-1000, 273, -1000, -1000, -1000, -50, -51, -52, -1000, -1000,
	-1000, -1000, -1000, -1000, 788, 378, -1000, 378, 788, 324,
	141, -1000, 788, 788, -1000, -1000, -1000, 359, 146, 63,
	705, 705, -1000, -1000, 141, 141, 141, 378, 378, 402,
	-1000, 378, -1000, 339, 146, 146, 57, 57, 57, 57,
	-1000, 401, 8, 132, -1000, 132, 132, 58, -1000, 57,
	-1000, 57, -1000, -1000, 57, -1000, -1000,
}
var yyPgo = [...]int{

	0, 513, 18, 512, 511, 510, 509, 508, 506, 505,
	503, 502, 501, 360, 500, 499, 498, 497, 17, 24,
	496, 495, 15, 22, 11, 490, 489, 10, 486, 21,
	484, 3, 23, 13, 482, 481, 478, 20, 94, 476,
	32, 14, 8, 475, 7, 71, 474, 472, 471, 16,
	470, 469, 455, 454, 452, 9, 451, 1, 450, 5,
	448, 447, 446, 12, 4, 80, 445, 322, 254, 443,
	442, 441, 440, 437, 0, 436, 406, 434, 431, 31,
	427, 425, 6, 2,
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
	51, 46, 46, 46, 37, 37, 37, 37, 37, 37,
	39, 39, 39, 52, 52, 53, 53, 55, 55, 56,
	56, 57, 58, 58, 58, 59, 59, 59, 60, 60,
	60, 61, 61, 62, 62, 63, 63, 36, 36, 43,
	43, 44, 64, 64, 65, 66, 66, 68, 68, 69,
	69, 67, 67, 70, 70, 70, 70, 70, 70, 71,
	71, 72, 72, 73, 73, 74, 76, 82, 83, 79,
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
	1, 2, 2, 0, 3, 0, 2, 0, 3, 1,
	3, 2, 0, 1, 1, 0, 2, 4, 0, 2,
	4, 0, 3, 1, 3, 0, 5, 2, 1, 1,
	3, 3, 1, 3, 3, 1, 1, 0, 2, 0,
	3, 0, 1, 1, 1, 1, 1, 1, 1, 0,
	1, 0, 1, 0, 2, 1, 1, 1, 1, 0,
}
var yyChk = [...]int{

	-1000, -80, -1, -2, -3, -4, -5, -6, -7, -8,
	-9, -10, -11, -12, 6, 7, 8, 9, 28, 96,
	97, 99, 98, 100, 109, 110, 111, -15, 5, -13,
	-81, -13, -13, -13, -13, 101, -72, 103, 107, -67,
	103, 105, 101, 101, 102, 103, 101, -79, -79, -79,
	-2, 18, 19, -16, 32, 19, -14, -67, -29, -76,
	48, 10, -64, -65, -74, 48, -69, 106, 102, -74,
	101, -74, -76, -68, 106, 48, -68, -76, -17, 35,
	-39, -74, 51, 53, 55, 24, 28, 93, -29, 46,
	68, -76, 61, 48, -79, -76, -79, 104, -76, 21,
	44, -74, -18, -19, 85, -20, -76, -33, -38, 48,
	-34, 61, -82, -37, -46, -44, -45, 83, 84, 90,
	92, -54, -47, 57, 58, 21, 45, 50, 49, 51,
	52, 53, 56, -74, 106, 112, 113, 88, 63, -66,
	17, 10, 30, 30, -29, -64, -76, -32, 11, -65,
	-38, -76, -82, -79, 21, -73, 108, -70, 99, 97,
	27, 98, 14, 114, 48, -76, -76, -79, -21, 46,
	10, -75, -74, 20, 93, -82, 60, 59, 75, -35,
	78, 61, 76, 77, 62, 75, 80, 79, 89, 83,
	84, 85, 86, 87, 88, 81, 82, 68, 69, 70,
	71, 72, 73, 74, -33, -38, -33, -2, -42, -38,
	94, 95, -38, -38, -38, -38, -82, -45, -82, -50,
	-38, -29, -61, 28, -82, -32, -55, 14, -33, 93,
	-38, 44, -74, -79, -71, 104, -32, -19, -22, -23,
	-24, -25, -29, -45, -82, -74, 85, -74, -76, -83,
	-18, 19, 47, -33, -33, -40, 56, 61, 57, 58,
	-38, -41, -82, -45, 54, 78, 76, 77, 62, -38,
	-38, -38, -40, -38, -38, -38, -38, -38, -38, -38,
	-38, -38, -38, -38, -83, -83, 46, -83, -37, -37,
	-74, -83, -18, -48, -49, 64, -36, 30, -2, -64,
	-62, -74, -55, -59, 16, 15, -76, -76, -52, 12,
	46, -26, -27, -28, 34, 38, 40, 35, 36, 37,
	41, -77, -76, 20, -78, 20, -22, 93, -83, -18,
	56, 57, 58, -42, -41, -38, -38, -38, 60, -38,
	-83, -51, -49, 66, -33, -63, 44, -43, -44, -82,
	-63, -83, 46, -59, -38, -56, -57, -38, -79, -53,
	13, 15, -23, -24, -23, -24, 34, 34, 34, 39,
	34, 39, 34, -27, -30, 42, 105, 43, -76, -76,
	-83, -74, -83, -83, 60, -38, 67, -38, 65, 25,
	46, -74, 46, 46, -58, 22, 23, -55, -33, -42,
	44, 44, 34, 34, 102, 102, 102, -38, -38, 26,
	-44, -38, -57, -59, -33, -33, -82, -82, -82, 8,
	-60, 17, 29, -31, -74, -31, -31, -64, 8, 78,
	-83, 46, -83, -83, -74, -74, -74,
}
var yyDef = [...]int{

	0, -2, 1, 2, 3, 4, 5, 6, 7, 8,
	9, 10, 11, 12, 35, 35, 35, 35, 35, 231,
	221, 0, 0, 0, 239, 239, 239, 0, 39, 42,
	37, 221, 0, 0, 0, 219, 0, 0, 232, 0,
	0, 222, 0, 217, 0, 217, 0, 32, 33, 34,
	15, 40, 41, 44, 0, 43, 36, 0, 0, 84,
	236, 0, 20, 212, 0, 235, 0, 0, 0, 239,
	0, 239, 0, 0, 0, 0, 0, 31, 0, 45,
	0, 180, 0, 0, 38, 0, 0, 0, 92, 0,
	0, 239, 0, 233, 23, 0, 26, 0, 28, 218,
	0, 239, 56, 46, 48, 53, 0, 51, 52, -2,
	94, 0, 0, 132, 133, 134, 135, 0, 0, 0,
	0, 0, 158, 100, 101, 0, 237, 174, 175, 176,
	177, 178, 179, 171, 159, 160, 161, 162, 164, 0,
	215, 216, 181, 182, 201, 92, 85, 187, 0, 213,
	214, 0, 0, 21, 220, 0, 0, 239, 229, 223,
	224, 225, 226, 227, 228, 27, 29, 30, 92, 0,
	0, 49, 54, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 119, 120, 121,
	122, 123, 124, 125, 97, 0, 0, 0, 0, 130,
	0, 0, 149, 150, 151, 0, 0, 112, 0, 0,
	165, 14, 0, 0, 0, 187, 195, 0, 93, 0,
	130, 0, 234, 24, 0, 230, 183, 47, 57, 58,
	60, 61, 71, 69, 0, 55, 50, 172, 0, 153,
	0, 0, 238, 95, 96, 99, 113, 0, 115, 117,
	102, 103, 0, 127, 128, 0, 0, 0, 0, 105,
	107, 0, 111, 136, 137, 138, 139, 140, 141, 142,
	143, 144, 145, 146, 98, 129, 0, 211, 147, 148,
	152, 156, 0, 169, 166, 0, 205, 0, 208, 205,
	0, 203, 195, 19, 0, 0, 239, 25, 185, 0,
	0, 0, 0, 0, 74, 0, 0, 77, 0, 0,
	0, 86, 72, 0, 0, 70, 0, 0, 154, 0,
	114, 116, 118, 0, 104, 106, 108, 0, 0, 131,
	157, 0, 167, 0, 0, 16, 0, 207, 209, 0,
	17, 202, 0, 18, 196, 188, 189, 192, 22, 187,
	0, 0, 59, 65, 0, 68, 75, 76, 78, 0,
	80, 0, 82, 83, 62, 0, 0, 0, 73, 63,
	64, 173, 155, 126, 0, 109, 163, 170, 0, 0,
	0, 204, 0, 0, 191, 193, 194, 195, 186, 184,
	0, 0, 79, 81, 0, 0, 0, 110, 168, 0,
	210, 197, 190, 198, 66, 67, 0, 0, 0, 0,
	13, 0, 0, 0, 90, 0, 0, 206, 199, 0,
	87, 0, 88, 89, 0, 91, 200,
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
			yyVAL.valExpr = HexNum(yyDollar[1].bytes)
		}
	case 178:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:980
		{
			yyVAL.valExpr = ValArg(yyDollar[1].bytes)
		}
	case 179:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:984
		{
			yyVAL.valExpr = &NullVal{}
		}
	case 180:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:990
		{
			// TODO(sougou): Deprecate this construct.
			if yyDollar[1].colIdent.Lowered() != "value" {
				yylex.Error("expecting value after next")
				return 1
			}
			yyVAL.valExpr = NumVal("1")
		}
	case 181:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:999
		{
			yyVAL.valExpr = NumVal(yyDollar[1].bytes)
		}
	case 182:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:1003
		{
			yyVAL.valExpr = ValArg(yyDollar[1].bytes)
		}
	case 183:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1008
		{
			yyVAL.valExprs = nil
		}
	case 184:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1012
		{
			yyVAL.valExprs = yyDollar[3].valExprs
		}
	case 185:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1017
		{
			yyVAL.boolExpr = nil
		}
	case 186:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:1021
		{
			yyVAL.boolExpr = yyDollar[2].boolExpr
		}
	case 187:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1026
		{
			yyVAL.orderBy = nil
		}
	case 188:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1030
		{
			yyVAL.orderBy = yyDollar[3].orderBy
		}
	case 189:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1036
		{
			yyVAL.orderBy = OrderBy{yyDollar[1].order}
		}
	case 190:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1040
		{
			yyVAL.orderBy = append(yyDollar[1].orderBy, yyDollar[3].order)
		}
	case 191:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:1046
		{
			yyVAL.order = &Order{Expr: yyDollar[1].valExpr, Direction: yyDollar[2].str}
		}
	case 192:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1051
		{
			yyVAL.str = AscScr
		}
	case 193:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1055
		{
			yyVAL.str = AscScr
		}
	case 194:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1059
		{
			yyVAL.str = DescScr
		}
	case 195:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1064
		{
			yyVAL.limit = nil
		}
	case 196:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:1068
		{
			yyVAL.limit = &Limit{Rowcount: yyDollar[2].valExpr}
		}
	case 197:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:1072
		{
			yyVAL.limit = &Limit{Offset: yyDollar[2].valExpr, Rowcount: yyDollar[4].valExpr}
		}
	case 198:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1077
		{
			yyVAL.str = ""
		}
	case 199:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:1081
		{
			yyVAL.str = ForUpdateStr
		}
	case 200:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:1085
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
	case 201:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1098
		{
			yyVAL.columns = nil
		}
	case 202:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1102
		{
			yyVAL.columns = yyDollar[2].columns
		}
	case 203:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1108
		{
			yyVAL.columns = Columns{yyDollar[1].colIdent}
		}
	case 204:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1112
		{
			yyVAL.columns = append(yyVAL.columns, yyDollar[3].colIdent)
		}
	case 205:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1117
		{
			yyVAL.updateExprs = nil
		}
	case 206:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:1121
		{
			yyVAL.updateExprs = yyDollar[5].updateExprs
		}
	case 207:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:1127
		{
			yyVAL.insRows = yyDollar[2].values
		}
	case 208:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1131
		{
			yyVAL.insRows = yyDollar[1].selStmt
		}
	case 209:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1137
		{
			yyVAL.values = Values{yyDollar[1].valTuple}
		}
	case 210:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1141
		{
			yyVAL.values = append(yyDollar[1].values, yyDollar[3].valTuple)
		}
	case 211:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1147
		{
			yyVAL.valTuple = ValTuple(yyDollar[2].valExprs)
		}
	case 212:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1153
		{
			yyVAL.updateExprs = UpdateExprs{yyDollar[1].updateExpr}
		}
	case 213:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1157
		{
			yyVAL.updateExprs = append(yyDollar[1].updateExprs, yyDollar[3].updateExpr)
		}
	case 214:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1163
		{
			yyVAL.updateExpr = &UpdateExpr{Name: yyDollar[1].colIdent, Expr: yyDollar[3].valExpr}
		}
	case 217:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1172
		{
			yyVAL.byt = 0
		}
	case 218:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:1174
		{
			yyVAL.byt = 1
		}
	case 219:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1177
		{
			yyVAL.empty = struct{}{}
		}
	case 220:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1179
		{
			yyVAL.empty = struct{}{}
		}
	case 221:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1182
		{
			yyVAL.str = ""
		}
	case 222:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1184
		{
			yyVAL.str = IgnoreStr
		}
	case 223:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1188
		{
			yyVAL.empty = struct{}{}
		}
	case 224:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1190
		{
			yyVAL.empty = struct{}{}
		}
	case 225:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1192
		{
			yyVAL.empty = struct{}{}
		}
	case 226:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1194
		{
			yyVAL.empty = struct{}{}
		}
	case 227:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1196
		{
			yyVAL.empty = struct{}{}
		}
	case 228:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1198
		{
			yyVAL.empty = struct{}{}
		}
	case 229:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1201
		{
			yyVAL.empty = struct{}{}
		}
	case 230:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1203
		{
			yyVAL.empty = struct{}{}
		}
	case 231:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1206
		{
			yyVAL.empty = struct{}{}
		}
	case 232:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1208
		{
			yyVAL.empty = struct{}{}
		}
	case 233:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1211
		{
			yyVAL.empty = struct{}{}
		}
	case 234:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:1213
		{
			yyVAL.empty = struct{}{}
		}
	case 235:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1217
		{
			yyVAL.colIdent = NewColIdent(string(yyDollar[1].bytes))
		}
	case 236:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1223
		{
			yyVAL.tableIdent = TableIdent(yyDollar[1].bytes)
		}
	case 237:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1229
		{
			if incNesting(yylex) {
				yylex.Error("max nesting level reached")
				return 1
			}
		}
	case 238:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1238
		{
			decNesting(yylex)
		}
	case 239:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1243
		{
			forceEOF(yylex)
		}
	}
	goto yystack /* stack new state and value */
}
