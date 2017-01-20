//line ./go/vt/sqlparser/sql.y:6
package sqlparser

import __yyfmt__ "fmt"

//line ./go/vt/sqlparser/sql.y:6
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

//line ./go/vt/sqlparser/sql.y:34
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
const CURRENT_DATE = 57443
const UNIX_TIMESTAMP = 57444
const CURRENT_TIME = 57445
const LOCALTIME = 57446
const LOCALTIMESTAMP = 57447
const UTC_DATE = 57448
const UTC_TIME = 57449
const UTC_TIMESTAMP = 57450
const REPLACE = 57451
const UNUSED = 57452

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
	"CURRENT_DATE",
	"UNIX_TIMESTAMP",
	"CURRENT_TIME",
	"LOCALTIME",
	"LOCALTIMESTAMP",
	"UTC_DATE",
	"UTC_TIME",
	"UTC_TIMESTAMP",
	"REPLACE",
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
	-1, 120,
	96, 265,
	-2, 264,
	-1, 134,
	46, 183,
	-2, 173,
}

const yyNprod = 269
const yyPrivate = 57344

var yyTokenNames []string
var yyStates []string

const yyLast = 1320

var yyAct = [...]int{

	132, 182, 343, 461, 68, 393, 260, 363, 240, 278,
	296, 353, 264, 127, 118, 335, 113, 126, 276, 289,
	337, 136, 202, 169, 114, 186, 277, 124, 177, 239,
	3, 38, 80, 40, 73, 413, 415, 41, 70, 44,
	50, 75, 192, 280, 77, 43, 444, 44, 46, 47,
	48, 15, 16, 18, 19, 273, 190, 108, 87, 53,
	443, 442, 74, 94, 76, 49, 45, 395, 51, 52,
	242, 243, 370, 69, 20, 267, 206, 194, 63, 66,
	97, 219, 228, 112, 228, 71, 123, 466, 120, 92,
	209, 208, 207, 94, 70, 174, 101, 427, 172, 70,
	414, 70, 425, 176, 63, 303, 96, 209, 336, 336,
	386, 99, 103, 371, 372, 373, 105, 204, 107, 301,
	302, 300, 171, 419, 123, 123, 284, 262, 189, 191,
	188, 290, 292, 293, 248, 249, 291, 236, 238, 251,
	208, 207, 61, 183, 21, 22, 24, 23, 25, 90,
	65, 250, 366, 197, 193, 71, 209, 26, 27, 28,
	226, 227, 220, 221, 222, 223, 224, 225, 219, 70,
	17, 228, 259, 256, 179, 222, 223, 224, 225, 219,
	123, 167, 228, 208, 207, 207, 95, 270, 258, 364,
	203, 255, 71, 266, 15, 88, 299, 171, 89, 209,
	209, 123, 282, 91, 204, 263, 254, 286, 95, 123,
	123, 65, 65, 297, 281, 62, 120, 65, 71, 366,
	205, 104, 287, 288, 275, 298, 283, 274, 271, 320,
	30, 321, 294, 71, 95, 205, 307, 65, 81, 65,
	468, 262, 199, 262, 178, 322, 323, 325, 328, 100,
	123, 123, 324, 71, 79, 329, 332, 433, 434, 324,
	262, 431, 341, 351, 262, 330, 333, 338, 286, 100,
	326, 327, 200, 262, 351, 237, 342, 339, 261, 262,
	100, 438, 340, 168, 282, 218, 217, 226, 227, 220,
	221, 222, 223, 224, 225, 219, 281, 95, 228, 441,
	95, 369, 297, 82, 338, 269, 374, 408, 111, 199,
	93, 375, 409, 85, 298, 218, 217, 226, 227, 220,
	221, 222, 223, 224, 225, 219, 406, 95, 228, 440,
	405, 407, 410, 123, 359, 360, 404, 381, 123, 166,
	383, 42, 259, 165, 58, 390, 98, 459, 382, 15,
	385, 387, 430, 282, 282, 282, 282, 57, 389, 460,
	119, 428, 401, 388, 403, 281, 281, 281, 281, 184,
	110, 420, 416, 411, 170, 60, 418, 180, 400, 368,
	402, 421, 429, 54, 55, 344, 399, 345, 396, 424,
	139, 138, 140, 141, 142, 143, 420, 265, 144, 241,
	123, 164, 398, 178, 244, 245, 246, 247, 437, 163,
	435, 350, 67, 436, 465, 447, 218, 217, 226, 227,
	220, 221, 222, 223, 224, 225, 219, 15, 30, 228,
	32, 1, 319, 367, 362, 253, 201, 450, 451, 185,
	123, 123, 39, 272, 454, 455, 456, 187, 70, 72,
	162, 173, 457, 452, 453, 462, 462, 462, 268, 463,
	464, 458, 432, 392, 134, 133, 397, 471, 467, 472,
	469, 470, 473, 181, 349, 119, 217, 226, 227, 220,
	221, 222, 223, 224, 225, 219, 295, 384, 228, 304,
	305, 306, 252, 308, 309, 310, 311, 312, 313, 314,
	315, 316, 317, 318, 334, 135, 125, 257, 64, 64,
	86, 220, 221, 222, 223, 224, 225, 219, 210, 78,
	228, 121, 412, 83, 119, 119, 355, 358, 359, 360,
	356, 354, 357, 361, 64, 268, 64, 352, 279, 198,
	422, 64, 116, 84, 56, 29, 102, 59, 14, 13,
	106, 12, 11, 109, 10, 9, 8, 7, 117, 218,
	217, 226, 227, 220, 221, 222, 223, 224, 225, 219,
	379, 175, 228, 268, 6, 5, 4, 376, 377, 378,
	2, 0, 195, 0, 0, 196, 0, 0, 0, 218,
	217, 226, 227, 220, 221, 222, 223, 224, 225, 219,
	380, 0, 228, 0, 0, 0, 0, 119, 0, 218,
	217, 226, 227, 220, 221, 222, 223, 224, 225, 219,
	391, 394, 228, 355, 358, 359, 360, 356, 31, 357,
	361, 0, 0, 439, 0, 0, 64, 0, 0, 0,
	0, 0, 0, 0, 0, 33, 34, 35, 36, 37,
	0, 0, 0, 0, 0, 423, 0, 0, 0, 0,
	0, 0, 426, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 117, 64, 268, 0, 0, 0, 0,
	285, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 445, 331,
	0, 137, 0, 446, 0, 448, 449, 394, 0, 0,
	0, 0, 0, 0, 0, 0, 150, 0, 0, 0,
	0, 0, 117, 117, 0, 95, 0, 262, 120, 139,
	138, 140, 141, 142, 143, 0, 0, 144, 160, 161,
	0, 346, 122, 347, 159, 0, 348, 0, 0, 0,
	0, 0, 0, 0, 365, 0, 64, 0, 0, 0,
	0, 0, 0, 0, 128, 129, 115, 0, 0, 148,
	0, 130, 0, 0, 131, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 145, 0,
	0, 0, 0, 0, 151, 146, 152, 147, 153, 157,
	158, 156, 155, 154, 149, 117, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 137, 0, 0, 0,
	0, 0, 0, 0, 0, 64, 64, 64, 64, 0,
	0, 150, 0, 0, 0, 0, 0, 0, 365, 0,
	95, 417, 262, 120, 139, 138, 140, 141, 142, 143,
	0, 0, 144, 160, 161, 0, 0, 122, 0, 159,
	0, 0, 0, 137, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 150, 128,
	129, 115, 0, 0, 148, 0, 130, 95, 0, 131,
	120, 139, 138, 140, 141, 142, 143, 0, 0, 144,
	160, 161, 0, 145, 122, 0, 159, 0, 0, 151,
	146, 152, 147, 153, 157, 158, 156, 155, 154, 149,
	0, 0, 0, 0, 0, 0, 128, 129, 115, 0,
	0, 148, 0, 130, 0, 0, 131, 15, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	145, 0, 0, 137, 0, 0, 151, 146, 152, 147,
	153, 157, 158, 156, 155, 154, 149, 0, 150, 0,
	0, 0, 0, 0, 0, 0, 0, 95, 0, 0,
	120, 139, 138, 140, 141, 142, 143, 0, 0, 144,
	160, 161, 0, 0, 122, 0, 159, 0, 0, 0,
	137, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 150, 128, 129, 0, 0,
	0, 148, 0, 130, 95, 0, 131, 120, 139, 138,
	140, 141, 142, 143, 0, 0, 144, 160, 161, 0,
	145, 122, 0, 159, 0, 0, 151, 146, 152, 147,
	153, 157, 158, 156, 155, 154, 149, 0, 0, 0,
	0, 0, 150, 128, 129, 0, 0, 0, 148, 0,
	130, 95, 0, 131, 120, 139, 138, 140, 141, 142,
	143, 0, 0, 144, 160, 161, 0, 145, 0, 0,
	159, 0, 0, 151, 146, 152, 147, 153, 157, 158,
	156, 155, 154, 149, 0, 0, 0, 0, 0, 0,
	128, 129, 0, 0, 0, 148, 0, 130, 0, 0,
	131, 15, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 145, 0, 0, 0, 0, 0,
	151, 146, 152, 147, 153, 157, 158, 156, 155, 154,
	149, 0, 150, 0, 0, 0, 0, 0, 0, 0,
	0, 95, 0, 0, 120, 139, 138, 140, 141, 142,
	143, 0, 0, 144, 0, 0, 0, 0, 0, 0,
	159, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 150,
	128, 129, 0, 0, 0, 148, 0, 130, 95, 0,
	131, 120, 139, 138, 140, 141, 142, 143, 0, 0,
	144, 0, 0, 0, 145, 0, 0, 159, 0, 0,
	151, 146, 152, 147, 153, 157, 158, 156, 155, 154,
	149, 0, 0, 0, 0, 0, 0, 128, 129, 0,
	0, 0, 148, 0, 130, 0, 0, 131, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 145, 0, 0, 0, 0, 0, 151, 146, 152,
	147, 153, 157, 158, 156, 155, 154, 149, 212, 215,
	0, 0, 0, 0, 0, 229, 230, 231, 232, 233,
	234, 235, 216, 213, 214, 211, 218, 217, 226, 227,
	220, 221, 222, 223, 224, 225, 219, 0, 0, 228,
}
var yyPact = [...]int{

	45, -1000, -1000, 423, -1000, -1000, -1000, -1000, -1000, -1000,
	-1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000,
	-1000, -73, -61, -38, -56, -39, -1000, -1000, -1000, 421,
	364, 324, -1000, -69, 190, 163, 402, 106, -75, -43,
	106, -1000, -40, 106, -1000, 163, -77, 189, -77, 163,
	-1000, -1000, -1000, -1000, -1000, -1000, 277, 143, -1000, 92,
	190, 281, 163, -1000, -16, -1000, 317, 163, 202, -1000,
	26, -1000, 163, 49, 172, -1000, 163, -1000, -50, 163,
	348, 263, 106, -1000, 841, -1000, 391, -1000, 312, 308,
	-1000, 254, 343, 106, 106, -1000, -1000, 163, 106, 392,
	106, 1162, -1000, 347, -86, -1000, 28, -1000, 163, -1000,
	-1000, 163, -1000, 262, -1000, -1000, 169, -20, 122, 1225,
	-1000, -1000, 978, 931, -1000, -27, -1000, -1000, 1162, 1162,
	1162, 1162, 251, 251, -1000, -1000, -1000, 251, -1000, -1000,
	-1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000,
	-1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, 1162,
	-1000, -1000, 163, -1000, -1000, -1000, -1000, 343, 106, -1000,
	251, 423, 202, 231, -1000, -1000, 233, 383, 978, -1000,
	528, -21, 1115, -1000, -1000, 260, 106, -1000, -52, -1000,
	-1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, 392, 841,
	162, -1000, -1000, 184, -1000, -1000, 39, 978, 978, 73,
	1025, 140, 41, 1162, 1162, 1162, 73, 1162, 1162, 1162,
	1162, 1162, 1162, 1162, 1162, 1162, 1162, 1162, 180, -1000,
	-1000, -1000, -1000, -1000, -1000, -1000, 13, 1225, 79, 225,
	212, 1225, 340, 340, -12, -12, -12, 204, 679, 794,
	-1000, 421, 43, 528, -1000, 259, 222, 235, -1000, 1162,
	-1000, 106, -1000, 383, 369, 372, 122, 167, 528, 163,
	-1000, -1000, 163, -1000, 399, -1000, 227, 491, -1000, -1000,
	168, 358, 188, -1000, -1000, -24, -1000, 13, 123, -1000,
	-1000, 55, -1000, -1000, -1000, 528, -1000, 1115, -1000, -1000,
	140, 1162, 1162, 1162, 528, 528, 508, -1000, 77, 394,
	-12, 88, 88, -10, -10, -10, -10, 426, 426, -1000,
	-1000, -1000, -1000, -1000, 1162, -1000, -1000, -1000, -1000, -1000,
	195, 841, -1000, 195, 42, -1000, 978, -1000, 337, -1000,
	251, -1000, 369, -1000, 1162, 1162, -29, -1000, -1000, 389,
	371, 162, 162, 162, 162, -1000, 301, 295, -1000, 291,
	272, 297, -8, -1000, 101, -1000, -1000, 163, -1000, 216,
	36, -1000, -1000, -1000, 212, -1000, 528, 528, 478, 1162,
	528, -1000, 195, -1000, 33, -1000, 1162, 30, 334, -1000,
	-1000, 335, 214, -1000, 234, 106, -1000, 383, 978, 1162,
	491, 236, 588, -1000, -1000, -1000, -1000, 294, -1000, 264,
	-1000, -1000, -1000, -44, -45, -59, -1000, -1000, -1000, -1000,
	-1000, -1000, 1162, 528, -1000, -1000, 528, 1162, 407, 1162,
	1162, 1162, -1000, -1000, -1000, 369, 122, 205, 978, 978,
	-1000, -1000, 251, 251, 251, 528, 528, 106, 528, 528,
	-1000, 329, 122, 122, 106, 106, 106, 202, -1000, 406,
	7, 193, -1000, 193, 193, -1000, 106, -1000, 106, -1000,
	-1000, 106, -1000, -1000,
}
var yyPgo = [...]int{

	0, 580, 29, 576, 575, 574, 557, 556, 555, 554,
	552, 551, 549, 548, 628, 547, 545, 544, 543, 16,
	24, 542, 539, 18, 26, 9, 538, 537, 11, 531,
	43, 142, 522, 3, 28, 14, 521, 21, 518, 23,
	27, 275, 510, 19, 10, 8, 507, 17, 13, 506,
	505, 504, 15, 492, 487, 474, 466, 465, 464, 12,
	463, 5, 462, 2, 461, 89, 451, 20, 4, 73,
	450, 341, 254, 449, 447, 443, 442, 439, 0, 22,
	436, 473, 7, 434, 433, 40, 432, 431, 430, 1,
	6,
}
var yyR1 = [...]int{

	0, 87, 1, 1, 1, 1, 1, 1, 1, 1,
	1, 1, 1, 1, 2, 2, 2, 3, 3, 4,
	4, 5, 6, 7, 8, 8, 8, 9, 9, 9,
	10, 11, 11, 11, 12, 13, 13, 13, 88, 14,
	15, 15, 16, 16, 16, 17, 17, 18, 18, 19,
	19, 20, 20, 20, 20, 21, 21, 80, 80, 80,
	79, 79, 22, 22, 23, 23, 24, 24, 25, 25,
	25, 26, 26, 26, 26, 84, 84, 83, 83, 83,
	82, 82, 27, 27, 27, 27, 28, 28, 28, 28,
	29, 29, 31, 31, 30, 30, 32, 32, 32, 32,
	33, 33, 34, 34, 35, 35, 35, 35, 35, 35,
	37, 37, 36, 36, 36, 36, 36, 36, 36, 36,
	36, 36, 36, 36, 36, 43, 43, 43, 43, 43,
	43, 38, 38, 38, 38, 38, 38, 38, 44, 44,
	44, 48, 45, 45, 86, 86, 41, 41, 41, 41,
	41, 41, 41, 41, 41, 41, 41, 41, 41, 41,
	41, 41, 41, 41, 41, 41, 41, 41, 41, 41,
	41, 41, 41, 41, 41, 58, 58, 58, 58, 58,
	58, 58, 58, 57, 57, 57, 57, 57, 57, 57,
	50, 53, 53, 51, 51, 52, 54, 54, 49, 49,
	49, 40, 40, 40, 40, 40, 40, 40, 42, 42,
	42, 55, 55, 56, 56, 59, 59, 60, 60, 61,
	62, 62, 62, 63, 63, 63, 63, 64, 64, 64,
	65, 65, 66, 66, 67, 67, 39, 39, 46, 46,
	47, 68, 68, 69, 70, 70, 72, 72, 73, 73,
	71, 71, 74, 74, 74, 74, 74, 74, 75, 75,
	76, 76, 77, 77, 78, 81, 89, 90, 85,
}
var yyR2 = [...]int{

	0, 1, 1, 1, 1, 1, 1, 1, 1, 1,
	1, 1, 1, 1, 12, 6, 3, 7, 7, 5,
	5, 8, 7, 3, 5, 8, 4, 6, 7, 4,
	5, 4, 5, 5, 3, 2, 2, 2, 0, 2,
	0, 2, 1, 2, 2, 0, 1, 0, 1, 1,
	3, 1, 2, 3, 5, 1, 1, 0, 1, 2,
	1, 1, 0, 2, 1, 3, 1, 1, 3, 3,
	3, 3, 5, 5, 3, 0, 1, 0, 1, 2,
	1, 1, 1, 2, 2, 1, 2, 3, 2, 3,
	2, 2, 2, 1, 1, 3, 0, 5, 5, 5,
	1, 3, 0, 2, 1, 3, 3, 2, 3, 3,
	1, 1, 1, 3, 3, 3, 4, 3, 4, 3,
	4, 5, 6, 3, 2, 1, 2, 1, 2, 1,
	2, 1, 1, 1, 1, 1, 1, 1, 3, 1,
	1, 3, 1, 3, 1, 1, 1, 1, 1, 1,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 2, 2, 2, 3, 3, 4,
	5, 3, 4, 1, 1, 1, 1, 1, 1, 1,
	1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
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

	-1000, -87, -1, -2, -3, -4, -5, -6, -7, -8,
	-9, -10, -11, -12, -13, 6, 7, 125, 8, 9,
	29, 99, 100, 102, 101, 103, 112, 113, 114, -16,
	5, -14, -88, -14, -14, -14, -14, -14, 104, -76,
	106, 110, -71, 106, 108, 104, 104, 105, 106, 104,
	-85, -85, -85, -2, 19, 20, -17, 33, 20, -15,
	-71, -31, 25, -30, -81, 49, -30, 10, -68, -69,
	-78, 49, -73, 109, 105, -78, 104, -78, -81, -72,
	109, 49, -72, -81, -18, 36, -42, -78, 52, 55,
	57, -31, -65, 29, -89, 46, -30, 96, 29, -30,
	47, 70, -81, 63, 49, -85, -81, -85, 107, -81,
	22, 45, -78, -19, -20, 87, -21, -81, -35, -41,
	49, -36, 63, -89, -40, -49, -47, -48, 85, 86,
	92, 95, -78, -57, -58, -50, -37, 22, 51, 50,
	52, 53, 54, 55, 58, 109, 116, 118, 90, 125,
	37, 115, 117, 119, 124, 123, 122, 120, 121, 65,
	59, 60, -70, 18, 10, 31, 31, -65, 29, -39,
	31, -2, -68, -66, -78, -81, -68, -34, 11, -69,
	-41, -81, -89, -85, 22, -77, 111, -74, 102, 100,
	28, 101, 14, 126, 49, -81, -81, -85, -22, 47,
	10, -80, -79, 21, -78, 51, 96, 62, 61, 77,
	-38, 80, 63, 78, 79, 64, 77, 82, 81, 91,
	85, 86, 87, 88, 89, 90, 83, 84, 94, 70,
	71, 72, 73, 74, 75, 76, -35, -41, -35, -2,
	-45, -41, 97, 98, -41, -41, -41, -41, -89, -89,
	-48, -89, -53, -41, -30, -39, -68, -46, -47, -89,
	-90, 47, 48, -34, -59, 14, -35, 96, -41, 45,
	-78, -85, -75, 107, -34, -20, -23, -24, -25, -26,
	-30, -48, -89, -79, 87, -81, -78, -35, -35, -43,
	58, 63, 59, 60, -37, -41, -44, -89, -48, 56,
	80, 78, 79, 64, -41, -41, -41, -43, -41, -41,
	-41, -41, -41, -41, -41, -41, -41, -41, -41, -86,
	49, 51, -90, -90, 47, -90, -40, -40, -78, -90,
	-19, 20, -90, -19, -51, -52, 66, -67, 45, -67,
	47, -78, -59, -63, 16, 15, -81, -81, -81, -55,
	12, 47, -27, -28, -29, 35, 39, 41, 36, 37,
	38, 42, -83, -82, 21, -81, 51, -84, 21, -23,
	96, 58, 59, 60, -45, -44, -41, -41, -41, 62,
	-41, -90, -19, -90, -54, -52, 68, -35, 26, -47,
	-63, -41, -60, -61, -41, 96, -85, -56, 13, 15,
	-24, -25, -24, -25, 35, 35, 35, 40, 35, 40,
	35, -28, -32, 43, 108, 44, -82, -81, -90, 87,
	-78, -90, 62, -41, -90, 69, -41, 67, 27, 47,
	17, 47, -62, 23, 24, -59, -35, -45, 45, 45,
	35, 35, 105, 105, 105, -41, -41, 8, -41, -41,
	-61, -63, -35, -35, -89, -89, -89, -68, -64, 18,
	30, -33, -78, -33, -33, 8, 80, -90, 47, -90,
	-90, -78, -78, -78,
}
var yyDef = [...]int{

	0, -2, 1, 2, 3, 4, 5, 6, 7, 8,
	9, 10, 11, 12, 13, 38, 38, 38, 38, 38,
	38, 260, 250, 0, 0, 0, 268, 268, 268, 0,
	42, 45, 40, 250, 0, 0, 0, 0, 248, 0,
	0, 261, 0, 0, 251, 0, 246, 0, 246, 0,
	35, 36, 37, 16, 43, 44, 47, 0, 46, 39,
	0, 230, 0, 93, 94, 265, 0, 0, 23, 241,
	0, 264, 0, 0, 0, 268, 0, 268, 0, 0,
	0, 0, 0, 34, 0, 48, 0, 208, 0, 0,
	41, 230, 0, 0, 0, 266, 92, 0, 0, 102,
	0, 0, 268, 0, 262, 26, 0, 29, 0, 31,
	247, 0, 268, 62, 49, 51, 57, 0, 55, 56,
	-2, 104, 0, 0, 146, 147, 148, 149, 0, 0,
	0, 0, 198, 0, -2, 174, 112, 0, 201, 202,
	203, 204, 205, 206, 207, 184, 185, 186, 187, 188,
	189, 175, 176, 177, 178, 179, 180, 181, 182, 191,
	110, 111, 0, 244, 245, 209, 210, 0, 0, 19,
	0, 237, 20, 0, 232, 95, 102, 215, 0, 242,
	243, 0, 0, 24, 249, 0, 0, 268, 258, 252,
	253, 254, 255, 256, 257, 30, 32, 33, 102, 0,
	0, 52, 58, 0, 60, 61, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 131,
	132, 133, 134, 135, 136, 137, 107, 0, 0, 0,
	0, 142, 0, 0, 164, 165, 166, 0, 0, 0,
	124, 0, 0, 192, 15, 234, 234, 236, 238, 0,
	231, 0, 267, 215, 223, 0, 103, 0, 142, 0,
	263, 27, 0, 259, 211, 50, 63, 64, 66, 67,
	77, 75, 0, 59, 53, 0, 199, 105, 106, 109,
	125, 0, 127, 129, 113, 114, 115, 0, 139, 140,
	0, 0, 0, 0, 117, 119, 0, 123, 150, 151,
	152, 153, 154, 155, 156, 157, 158, 159, 160, 163,
	144, 145, 108, 141, 0, 240, 161, 162, 167, 168,
	0, 0, 171, 0, 196, 193, 0, 17, 0, 18,
	0, 233, 223, 22, 0, 0, 0, 268, 28, 213,
	0, 0, 0, 0, 0, 82, 0, 0, 85, 0,
	0, 0, 96, 78, 0, 80, 81, 0, 76, 0,
	0, 126, 128, 130, 0, 116, 118, 120, 0, 0,
	143, 169, 0, 172, 0, 194, 0, 0, 0, 239,
	21, 224, 216, 217, 220, 0, 25, 215, 0, 0,
	65, 71, 0, 74, 83, 84, 86, 0, 88, 0,
	90, 91, 68, 0, 0, 0, 79, 69, 70, 54,
	200, 138, 0, 121, 170, 190, 197, 0, 0, 0,
	0, 0, 219, 221, 222, 223, 214, 212, 0, 0,
	87, 89, 0, 0, 0, 122, 195, 0, 225, 226,
	218, 227, 72, 73, 0, 0, 0, 235, 14, 0,
	0, 0, 100, 0, 0, 228, 0, 97, 0, 98,
	99, 0, 101, 229,
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
	118, 119, 120, 121, 122, 123, 124, 125, 126,
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
		//line ./go/vt/sqlparser/sql.y:186
		{
			setParseTree(yylex, yyDollar[1].statement)
		}
	case 2:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:192
		{
			yyVAL.statement = yyDollar[1].selStmt
		}
	case 14:
		yyDollar = yyS[yypt-12 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:209
		{
			yyVAL.selStmt = &Select{Comments: Comments(yyDollar[2].bytes2), Distinct: yyDollar[3].str, Hints: yyDollar[4].str, SelectExprs: yyDollar[5].selectExprs, From: yyDollar[6].tableExprs, Where: NewWhere(WhereStr, yyDollar[7].boolExpr), GroupBy: GroupBy(yyDollar[8].valExprs), Having: NewWhere(HavingStr, yyDollar[9].boolExpr), OrderBy: yyDollar[10].orderBy, Limit: yyDollar[11].limit, Lock: yyDollar[12].str}
		}
	case 15:
		yyDollar = yyS[yypt-6 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:213
		{
			yyVAL.selStmt = &Select{Comments: Comments(yyDollar[2].bytes2), SelectExprs: SelectExprs{Nextval{Expr: yyDollar[4].valExpr}}, From: TableExprs{&AliasedTableExpr{Expr: yyDollar[6].tableName}}}
		}
	case 16:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:217
		{
			yyVAL.selStmt = &Union{Type: yyDollar[2].str, Left: yyDollar[1].selStmt, Right: yyDollar[3].selStmt}
		}
	case 17:
		yyDollar = yyS[yypt-7 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:223
		{
			yyVAL.statement = &Insert{Comments: Comments(yyDollar[2].bytes2), Ignore: yyDollar[3].str, Table: yyDollar[4].tableName, Columns: yyDollar[5].columns, Rows: yyDollar[6].insRows, OnDup: OnDup(yyDollar[7].updateExprs)}
		}
	case 18:
		yyDollar = yyS[yypt-7 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:227
		{
			cols := make(Columns, 0, len(yyDollar[6].updateExprs))
			vals := make(ValTuple, 0, len(yyDollar[7].updateExprs))
			for _, updateList := range yyDollar[6].updateExprs {
				cols = append(cols, updateList.Name)
				vals = append(vals, updateList.Expr)
			}
			yyVAL.statement = &Insert{Comments: Comments(yyDollar[2].bytes2), Ignore: yyDollar[3].str, Table: yyDollar[4].tableName, Columns: cols, Rows: Values{vals}, OnDup: OnDup(yyDollar[7].updateExprs)}
		}
	case 19:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:239
		{
			yyVAL.statement = &Insert{Comments: Comments(yyDollar[2].bytes2), Table: yyDollar[3].tableName, Columns: yyDollar[4].columns, Rows: yyDollar[5].insRows, Replace: true}
		}
	case 20:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:243
		{
			cols := make(Columns, 0, len(yyDollar[5].updateExprs))
			vals := make(ValTuple, 0, len(yyDollar[5].updateExprs))
			for _, updateList := range yyDollar[5].updateExprs {
				cols = append(cols, updateList.Name)
				vals = append(vals, updateList.Expr)
			}
			yyVAL.statement = &Insert{Comments: Comments(yyDollar[2].bytes2), Table: yyDollar[3].tableName, Columns: cols, Rows: Values{vals}, Replace: true}
		}
	case 21:
		yyDollar = yyS[yypt-8 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:255
		{
			yyVAL.statement = &Update{Comments: Comments(yyDollar[2].bytes2), Table: yyDollar[3].tableName, Exprs: yyDollar[5].updateExprs, Where: NewWhere(WhereStr, yyDollar[6].boolExpr), OrderBy: yyDollar[7].orderBy, Limit: yyDollar[8].limit}
		}
	case 22:
		yyDollar = yyS[yypt-7 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:261
		{
			yyVAL.statement = &Delete{Comments: Comments(yyDollar[2].bytes2), Table: yyDollar[4].tableName, Where: NewWhere(WhereStr, yyDollar[5].boolExpr), OrderBy: yyDollar[6].orderBy, Limit: yyDollar[7].limit}
		}
	case 23:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:267
		{
			yyVAL.statement = &Set{Comments: Comments(yyDollar[2].bytes2), Exprs: yyDollar[3].updateExprs}
		}
	case 24:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:273
		{
			yyVAL.statement = &DDL{Action: CreateStr, NewName: yyDollar[4].tableIdent}
		}
	case 25:
		yyDollar = yyS[yypt-8 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:277
		{
			// Change this to an alter statement
			yyVAL.statement = &DDL{Action: AlterStr, Table: yyDollar[7].tableIdent, NewName: yyDollar[7].tableIdent}
		}
	case 26:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:282
		{
			yyVAL.statement = &DDL{Action: CreateStr, NewName: NewTableIdent(yyDollar[3].colIdent.Lowered())}
		}
	case 27:
		yyDollar = yyS[yypt-6 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:288
		{
			yyVAL.statement = &DDL{Action: AlterStr, Table: yyDollar[4].tableIdent, NewName: yyDollar[4].tableIdent}
		}
	case 28:
		yyDollar = yyS[yypt-7 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:292
		{
			// Change this to a rename statement
			yyVAL.statement = &DDL{Action: RenameStr, Table: yyDollar[4].tableIdent, NewName: yyDollar[7].tableIdent}
		}
	case 29:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:297
		{
			yyVAL.statement = &DDL{Action: AlterStr, Table: NewTableIdent(yyDollar[3].colIdent.Lowered()), NewName: NewTableIdent(yyDollar[3].colIdent.Lowered())}
		}
	case 30:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:303
		{
			yyVAL.statement = &DDL{Action: RenameStr, Table: yyDollar[3].tableIdent, NewName: yyDollar[5].tableIdent}
		}
	case 31:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:309
		{
			var exists bool
			if yyDollar[3].byt != 0 {
				exists = true
			}
			yyVAL.statement = &DDL{Action: DropStr, Table: yyDollar[4].tableIdent, IfExists: exists}
		}
	case 32:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:317
		{
			// Change this to an alter statement
			yyVAL.statement = &DDL{Action: AlterStr, Table: yyDollar[5].tableIdent, NewName: yyDollar[5].tableIdent}
		}
	case 33:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:322
		{
			var exists bool
			if yyDollar[3].byt != 0 {
				exists = true
			}
			yyVAL.statement = &DDL{Action: DropStr, Table: NewTableIdent(yyDollar[4].colIdent.Lowered()), IfExists: exists}
		}
	case 34:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:332
		{
			yyVAL.statement = &DDL{Action: AlterStr, Table: yyDollar[3].tableIdent, NewName: yyDollar[3].tableIdent}
		}
	case 35:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:338
		{
			yyVAL.statement = &Other{}
		}
	case 36:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:342
		{
			yyVAL.statement = &Other{}
		}
	case 37:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:346
		{
			yyVAL.statement = &Other{}
		}
	case 38:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:351
		{
			setAllowComments(yylex, true)
		}
	case 39:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:355
		{
			yyVAL.bytes2 = yyDollar[2].bytes2
			setAllowComments(yylex, false)
		}
	case 40:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:361
		{
			yyVAL.bytes2 = nil
		}
	case 41:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:365
		{
			yyVAL.bytes2 = append(yyDollar[1].bytes2, yyDollar[2].bytes)
		}
	case 42:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:371
		{
			yyVAL.str = UnionStr
		}
	case 43:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:375
		{
			yyVAL.str = UnionAllStr
		}
	case 44:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:379
		{
			yyVAL.str = UnionDistinctStr
		}
	case 45:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:384
		{
			yyVAL.str = ""
		}
	case 46:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:388
		{
			yyVAL.str = DistinctStr
		}
	case 47:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:393
		{
			yyVAL.str = ""
		}
	case 48:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:397
		{
			yyVAL.str = StraightJoinHint
		}
	case 49:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:403
		{
			yyVAL.selectExprs = SelectExprs{yyDollar[1].selectExpr}
		}
	case 50:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:407
		{
			yyVAL.selectExprs = append(yyVAL.selectExprs, yyDollar[3].selectExpr)
		}
	case 51:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:413
		{
			yyVAL.selectExpr = &StarExpr{}
		}
	case 52:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:417
		{
			yyVAL.selectExpr = &NonStarExpr{Expr: yyDollar[1].expr, As: yyDollar[2].colIdent}
		}
	case 53:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:421
		{
			yyVAL.selectExpr = &StarExpr{TableName: &TableName{Name: yyDollar[1].tableIdent}}
		}
	case 54:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:425
		{
			yyVAL.selectExpr = &StarExpr{TableName: &TableName{Qualifier: yyDollar[1].tableIdent, Name: yyDollar[3].tableIdent}}
		}
	case 55:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:431
		{
			yyVAL.expr = yyDollar[1].boolExpr
		}
	case 56:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:435
		{
			yyVAL.expr = yyDollar[1].valExpr
		}
	case 57:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:440
		{
			yyVAL.colIdent = ColIdent{}
		}
	case 58:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:444
		{
			yyVAL.colIdent = yyDollar[1].colIdent
		}
	case 59:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:448
		{
			yyVAL.colIdent = yyDollar[2].colIdent
		}
	case 61:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:455
		{
			yyVAL.colIdent = NewColIdent(string(yyDollar[1].bytes))
		}
	case 62:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:460
		{
			yyVAL.tableExprs = TableExprs{&AliasedTableExpr{Expr: &TableName{Name: NewTableIdent("dual")}}}
		}
	case 63:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:464
		{
			yyVAL.tableExprs = yyDollar[2].tableExprs
		}
	case 64:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:470
		{
			yyVAL.tableExprs = TableExprs{yyDollar[1].tableExpr}
		}
	case 65:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:474
		{
			yyVAL.tableExprs = append(yyVAL.tableExprs, yyDollar[3].tableExpr)
		}
	case 68:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:484
		{
			yyVAL.tableExpr = &AliasedTableExpr{Expr: yyDollar[1].tableName, As: yyDollar[2].tableIdent, Hints: yyDollar[3].indexHints}
		}
	case 69:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:488
		{
			yyVAL.tableExpr = &AliasedTableExpr{Expr: yyDollar[1].subquery, As: yyDollar[3].tableIdent}
		}
	case 70:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:492
		{
			yyVAL.tableExpr = &ParenTableExpr{Exprs: yyDollar[2].tableExprs}
		}
	case 71:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:505
		{
			yyVAL.tableExpr = &JoinTableExpr{LeftExpr: yyDollar[1].tableExpr, Join: yyDollar[2].str, RightExpr: yyDollar[3].tableExpr}
		}
	case 72:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:509
		{
			yyVAL.tableExpr = &JoinTableExpr{LeftExpr: yyDollar[1].tableExpr, Join: yyDollar[2].str, RightExpr: yyDollar[3].tableExpr, On: yyDollar[5].boolExpr}
		}
	case 73:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:513
		{
			yyVAL.tableExpr = &JoinTableExpr{LeftExpr: yyDollar[1].tableExpr, Join: yyDollar[2].str, RightExpr: yyDollar[3].tableExpr, On: yyDollar[5].boolExpr}
		}
	case 74:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:517
		{
			yyVAL.tableExpr = &JoinTableExpr{LeftExpr: yyDollar[1].tableExpr, Join: yyDollar[2].str, RightExpr: yyDollar[3].tableExpr}
		}
	case 75:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:522
		{
			yyVAL.empty = struct{}{}
		}
	case 76:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:524
		{
			yyVAL.empty = struct{}{}
		}
	case 77:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:527
		{
			yyVAL.tableIdent = NewTableIdent("")
		}
	case 78:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:531
		{
			yyVAL.tableIdent = yyDollar[1].tableIdent
		}
	case 79:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:535
		{
			yyVAL.tableIdent = yyDollar[2].tableIdent
		}
	case 81:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:542
		{
			yyVAL.tableIdent = NewTableIdent(string(yyDollar[1].bytes))
		}
	case 82:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:548
		{
			yyVAL.str = JoinStr
		}
	case 83:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:552
		{
			yyVAL.str = JoinStr
		}
	case 84:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:556
		{
			yyVAL.str = JoinStr
		}
	case 85:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:560
		{
			yyVAL.str = StraightJoinStr
		}
	case 86:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:566
		{
			yyVAL.str = LeftJoinStr
		}
	case 87:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:570
		{
			yyVAL.str = LeftJoinStr
		}
	case 88:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:574
		{
			yyVAL.str = RightJoinStr
		}
	case 89:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:578
		{
			yyVAL.str = RightJoinStr
		}
	case 90:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:584
		{
			yyVAL.str = NaturalJoinStr
		}
	case 91:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:588
		{
			if yyDollar[2].str == LeftJoinStr {
				yyVAL.str = NaturalLeftJoinStr
			} else {
				yyVAL.str = NaturalRightJoinStr
			}
		}
	case 92:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:598
		{
			yyVAL.tableName = yyDollar[2].tableName
		}
	case 93:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:602
		{
			yyVAL.tableName = yyDollar[1].tableName
		}
	case 94:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:608
		{
			yyVAL.tableName = &TableName{Name: yyDollar[1].tableIdent}
		}
	case 95:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:612
		{
			yyVAL.tableName = &TableName{Qualifier: yyDollar[1].tableIdent, Name: yyDollar[3].tableIdent}
		}
	case 96:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:617
		{
			yyVAL.indexHints = nil
		}
	case 97:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:621
		{
			yyVAL.indexHints = &IndexHints{Type: UseStr, Indexes: yyDollar[4].colIdents}
		}
	case 98:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:625
		{
			yyVAL.indexHints = &IndexHints{Type: IgnoreStr, Indexes: yyDollar[4].colIdents}
		}
	case 99:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:629
		{
			yyVAL.indexHints = &IndexHints{Type: ForceStr, Indexes: yyDollar[4].colIdents}
		}
	case 100:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:635
		{
			yyVAL.colIdents = []ColIdent{yyDollar[1].colIdent}
		}
	case 101:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:639
		{
			yyVAL.colIdents = append(yyDollar[1].colIdents, yyDollar[3].colIdent)
		}
	case 102:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:644
		{
			yyVAL.boolExpr = nil
		}
	case 103:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:648
		{
			yyVAL.boolExpr = yyDollar[2].boolExpr
		}
	case 105:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:655
		{
			yyVAL.boolExpr = &AndExpr{Left: yyDollar[1].boolExpr, Right: yyDollar[3].boolExpr}
		}
	case 106:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:659
		{
			yyVAL.boolExpr = &OrExpr{Left: yyDollar[1].boolExpr, Right: yyDollar[3].boolExpr}
		}
	case 107:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:663
		{
			yyVAL.boolExpr = &NotExpr{Expr: yyDollar[2].boolExpr}
		}
	case 108:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:667
		{
			yyVAL.boolExpr = &ParenBoolExpr{Expr: yyDollar[2].boolExpr}
		}
	case 109:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:671
		{
			yyVAL.boolExpr = &IsExpr{Operator: yyDollar[3].str, Expr: yyDollar[1].boolExpr}
		}
	case 110:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:677
		{
			yyVAL.boolVal = BoolVal(true)
		}
	case 111:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:681
		{
			yyVAL.boolVal = BoolVal(false)
		}
	case 112:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:687
		{
			yyVAL.boolExpr = yyDollar[1].boolVal
		}
	case 113:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:691
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: yyDollar[2].str, Right: yyDollar[3].boolVal}
		}
	case 114:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:695
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: yyDollar[2].str, Right: yyDollar[3].valExpr}
		}
	case 115:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:699
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: InStr, Right: yyDollar[3].colTuple}
		}
	case 116:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:703
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: NotInStr, Right: yyDollar[4].colTuple}
		}
	case 117:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:707
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: LikeStr, Right: yyDollar[3].valExpr}
		}
	case 118:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:711
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: NotLikeStr, Right: yyDollar[4].valExpr}
		}
	case 119:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:715
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: RegexpStr, Right: yyDollar[3].valExpr}
		}
	case 120:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:719
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: NotRegexpStr, Right: yyDollar[4].valExpr}
		}
	case 121:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:723
		{
			yyVAL.boolExpr = &RangeCond{Left: yyDollar[1].valExpr, Operator: BetweenStr, From: yyDollar[3].valExpr, To: yyDollar[5].valExpr}
		}
	case 122:
		yyDollar = yyS[yypt-6 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:727
		{
			yyVAL.boolExpr = &RangeCond{Left: yyDollar[1].valExpr, Operator: NotBetweenStr, From: yyDollar[4].valExpr, To: yyDollar[6].valExpr}
		}
	case 123:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:731
		{
			yyVAL.boolExpr = &IsExpr{Operator: yyDollar[3].str, Expr: yyDollar[1].valExpr}
		}
	case 124:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:735
		{
			yyVAL.boolExpr = &ExistsExpr{Subquery: yyDollar[2].subquery}
		}
	case 125:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:741
		{
			yyVAL.str = IsNullStr
		}
	case 126:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:745
		{
			yyVAL.str = IsNotNullStr
		}
	case 127:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:749
		{
			yyVAL.str = IsTrueStr
		}
	case 128:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:753
		{
			yyVAL.str = IsNotTrueStr
		}
	case 129:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:757
		{
			yyVAL.str = IsFalseStr
		}
	case 130:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:761
		{
			yyVAL.str = IsNotFalseStr
		}
	case 131:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:767
		{
			yyVAL.str = EqualStr
		}
	case 132:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:771
		{
			yyVAL.str = LessThanStr
		}
	case 133:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:775
		{
			yyVAL.str = GreaterThanStr
		}
	case 134:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:779
		{
			yyVAL.str = LessEqualStr
		}
	case 135:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:783
		{
			yyVAL.str = GreaterEqualStr
		}
	case 136:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:787
		{
			yyVAL.str = NotEqualStr
		}
	case 137:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:791
		{
			yyVAL.str = NullSafeEqualStr
		}
	case 138:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:797
		{
			yyVAL.colTuple = ValTuple(yyDollar[2].valExprs)
		}
	case 139:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:801
		{
			yyVAL.colTuple = yyDollar[1].subquery
		}
	case 140:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:805
		{
			yyVAL.colTuple = ListArg(yyDollar[1].bytes)
		}
	case 141:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:811
		{
			yyVAL.subquery = &Subquery{yyDollar[2].selStmt}
		}
	case 142:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:817
		{
			yyVAL.valExprs = ValExprs{yyDollar[1].valExpr}
		}
	case 143:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:821
		{
			yyVAL.valExprs = append(yyDollar[1].valExprs, yyDollar[3].valExpr)
		}
	case 144:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:827
		{
			yyVAL.str = string(yyDollar[1].bytes)
		}
	case 145:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:831
		{
			yyVAL.str = string(yyDollar[1].bytes)
		}
	case 146:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:837
		{
			yyVAL.valExpr = yyDollar[1].valExpr
		}
	case 147:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:841
		{
			yyVAL.valExpr = yyDollar[1].colName
		}
	case 148:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:845
		{
			yyVAL.valExpr = yyDollar[1].valTuple
		}
	case 149:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:849
		{
			yyVAL.valExpr = yyDollar[1].subquery
		}
	case 150:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:853
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: BitAndStr, Right: yyDollar[3].valExpr}
		}
	case 151:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:857
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: BitOrStr, Right: yyDollar[3].valExpr}
		}
	case 152:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:861
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: BitXorStr, Right: yyDollar[3].valExpr}
		}
	case 153:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:865
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: PlusStr, Right: yyDollar[3].valExpr}
		}
	case 154:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:869
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: MinusStr, Right: yyDollar[3].valExpr}
		}
	case 155:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:873
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: MultStr, Right: yyDollar[3].valExpr}
		}
	case 156:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:877
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: DivStr, Right: yyDollar[3].valExpr}
		}
	case 157:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:881
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: ModStr, Right: yyDollar[3].valExpr}
		}
	case 158:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:885
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: ModStr, Right: yyDollar[3].valExpr}
		}
	case 159:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:889
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: ShiftLeftStr, Right: yyDollar[3].valExpr}
		}
	case 160:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:893
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: ShiftRightStr, Right: yyDollar[3].valExpr}
		}
	case 161:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:897
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].colName, Operator: JSONExtractOp, Right: yyDollar[3].valExpr}
		}
	case 162:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:901
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].colName, Operator: JSONUnquoteExtractOp, Right: yyDollar[3].valExpr}
		}
	case 163:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:905
		{
			yyVAL.valExpr = &CollateExpr{Expr: yyDollar[1].valExpr, Charset: yyDollar[3].str}
		}
	case 164:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:909
		{
			if num, ok := yyDollar[2].valExpr.(*SQLVal); ok && num.Type == IntVal {
				yyVAL.valExpr = num
			} else {
				yyVAL.valExpr = &UnaryExpr{Operator: UPlusStr, Expr: yyDollar[2].valExpr}
			}
		}
	case 165:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:917
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
	case 166:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:931
		{
			yyVAL.valExpr = &UnaryExpr{Operator: TildaStr, Expr: yyDollar[2].valExpr}
		}
	case 167:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:935
		{
			// This rule prevents the usage of INTERVAL
			// as a function. If support is needed for that,
			// we'll need to revisit this. The solution
			// will be non-trivial because of grammar conflicts.
			yyVAL.valExpr = &IntervalExpr{Expr: yyDollar[2].valExpr, Unit: yyDollar[3].colIdent}
		}
	case 168:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:943
		{
			yyVAL.valExpr = &FuncExpr{Name: yyDollar[1].colIdent}
		}
	case 169:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:947
		{
			yyVAL.valExpr = &FuncExpr{Name: yyDollar[1].colIdent, Exprs: yyDollar[3].selectExprs}
		}
	case 170:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:951
		{
			yyVAL.valExpr = &FuncExpr{Name: yyDollar[1].colIdent, Distinct: true, Exprs: yyDollar[4].selectExprs}
		}
	case 171:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:955
		{
			yyVAL.valExpr = &FuncExpr{Name: yyDollar[1].colIdent}
		}
	case 172:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:959
		{
			yyVAL.valExpr = &FuncExpr{Name: yyDollar[1].colIdent, Exprs: yyDollar[3].selectExprs}
		}
	case 173:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:963
		{
			yyVAL.valExpr = &FuncExpr{Name: yyDollar[1].colIdent}
		}
	case 174:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:967
		{
			yyVAL.valExpr = yyDollar[1].caseExpr
		}
	case 175:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:975
		{
			yyVAL.colIdent = NewColIdent("current_timestamp")
		}
	case 176:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:979
		{
			yyVAL.colIdent = NewColIdent("current_date")
		}
	case 177:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:983
		{
			yyVAL.colIdent = NewColIdent("current_time")
		}
	case 178:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:987
		{
			yyVAL.colIdent = NewColIdent("utc_timestamp")
		}
	case 179:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:991
		{
			yyVAL.colIdent = NewColIdent("utc_time")
		}
	case 180:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:995
		{
			yyVAL.colIdent = NewColIdent("utc_date")
		}
	case 181:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:999
		{
			yyVAL.colIdent = NewColIdent("localtime")
		}
	case 182:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1003
		{
			yyVAL.colIdent = NewColIdent("localtimestamp")
		}
	case 184:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1012
		{
			yyVAL.colIdent = NewColIdent("if")
		}
	case 185:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1016
		{
			yyVAL.colIdent = NewColIdent("database")
		}
	case 186:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1020
		{
			yyVAL.colIdent = NewColIdent("unix_timestamp")
		}
	case 187:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1024
		{
			yyVAL.colIdent = NewColIdent("mod")
		}
	case 188:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1028
		{
			yyVAL.colIdent = NewColIdent("replace")
		}
	case 189:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1032
		{
			yyVAL.colIdent = NewColIdent("left")
		}
	case 190:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1038
		{
			yyVAL.caseExpr = &CaseExpr{Expr: yyDollar[2].valExpr, Whens: yyDollar[3].whens, Else: yyDollar[4].valExpr}
		}
	case 191:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1043
		{
			yyVAL.valExpr = nil
		}
	case 192:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1047
		{
			yyVAL.valExpr = yyDollar[1].valExpr
		}
	case 193:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1053
		{
			yyVAL.whens = []*When{yyDollar[1].when}
		}
	case 194:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1057
		{
			yyVAL.whens = append(yyDollar[1].whens, yyDollar[2].when)
		}
	case 195:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1063
		{
			yyVAL.when = &When{Cond: yyDollar[2].boolExpr, Val: yyDollar[4].valExpr}
		}
	case 196:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1068
		{
			yyVAL.valExpr = nil
		}
	case 197:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1072
		{
			yyVAL.valExpr = yyDollar[2].valExpr
		}
	case 198:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1078
		{
			yyVAL.colName = &ColName{Name: yyDollar[1].colIdent}
		}
	case 199:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1082
		{
			yyVAL.colName = &ColName{Qualifier: &TableName{Name: yyDollar[1].tableIdent}, Name: yyDollar[3].colIdent}
		}
	case 200:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1086
		{
			yyVAL.colName = &ColName{Qualifier: &TableName{Qualifier: yyDollar[1].tableIdent, Name: yyDollar[3].tableIdent}, Name: yyDollar[5].colIdent}
		}
	case 201:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1092
		{
			yyVAL.valExpr = NewStrVal(yyDollar[1].bytes)
		}
	case 202:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1096
		{
			yyVAL.valExpr = NewHexVal(yyDollar[1].bytes)
		}
	case 203:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1100
		{
			yyVAL.valExpr = NewIntVal(yyDollar[1].bytes)
		}
	case 204:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1104
		{
			yyVAL.valExpr = NewFloatVal(yyDollar[1].bytes)
		}
	case 205:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1108
		{
			yyVAL.valExpr = NewHexNum(yyDollar[1].bytes)
		}
	case 206:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1112
		{
			yyVAL.valExpr = NewValArg(yyDollar[1].bytes)
		}
	case 207:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1116
		{
			yyVAL.valExpr = &NullVal{}
		}
	case 208:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1122
		{
			// TODO(sougou): Deprecate this construct.
			if yyDollar[1].colIdent.Lowered() != "value" {
				yylex.Error("expecting value after next")
				return 1
			}
			yyVAL.valExpr = NewIntVal([]byte("1"))
		}
	case 209:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1131
		{
			yyVAL.valExpr = NewIntVal(yyDollar[1].bytes)
		}
	case 210:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1135
		{
			yyVAL.valExpr = NewValArg(yyDollar[1].bytes)
		}
	case 211:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1140
		{
			yyVAL.valExprs = nil
		}
	case 212:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1144
		{
			yyVAL.valExprs = yyDollar[3].valExprs
		}
	case 213:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1149
		{
			yyVAL.boolExpr = nil
		}
	case 214:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1153
		{
			yyVAL.boolExpr = yyDollar[2].boolExpr
		}
	case 215:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1158
		{
			yyVAL.orderBy = nil
		}
	case 216:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1162
		{
			yyVAL.orderBy = yyDollar[3].orderBy
		}
	case 217:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1168
		{
			yyVAL.orderBy = OrderBy{yyDollar[1].order}
		}
	case 218:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1172
		{
			yyVAL.orderBy = append(yyDollar[1].orderBy, yyDollar[3].order)
		}
	case 219:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1178
		{
			yyVAL.order = &Order{Expr: yyDollar[1].valExpr, Direction: yyDollar[2].str}
		}
	case 220:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1183
		{
			yyVAL.str = AscScr
		}
	case 221:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1187
		{
			yyVAL.str = AscScr
		}
	case 222:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1191
		{
			yyVAL.str = DescScr
		}
	case 223:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1196
		{
			yyVAL.limit = nil
		}
	case 224:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1200
		{
			yyVAL.limit = &Limit{Rowcount: yyDollar[2].valExpr}
		}
	case 225:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1204
		{
			yyVAL.limit = &Limit{Offset: yyDollar[2].valExpr, Rowcount: yyDollar[4].valExpr}
		}
	case 226:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1208
		{
			yyVAL.limit = &Limit{Offset: yyDollar[4].valExpr, Rowcount: yyDollar[2].valExpr}
		}
	case 227:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1213
		{
			yyVAL.str = ""
		}
	case 228:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1217
		{
			yyVAL.str = ForUpdateStr
		}
	case 229:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1221
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
	case 230:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1234
		{
			yyVAL.columns = nil
		}
	case 231:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1238
		{
			yyVAL.columns = yyDollar[2].columns
		}
	case 232:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1244
		{
			yyVAL.columns = Columns{yyDollar[1].colIdent}
		}
	case 233:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1248
		{
			yyVAL.columns = append(yyVAL.columns, yyDollar[3].colIdent)
		}
	case 234:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1253
		{
			yyVAL.updateExprs = nil
		}
	case 235:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1257
		{
			yyVAL.updateExprs = yyDollar[5].updateExprs
		}
	case 236:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1263
		{
			yyVAL.insRows = yyDollar[2].values
		}
	case 237:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1267
		{
			yyVAL.insRows = yyDollar[1].selStmt
		}
	case 238:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1273
		{
			yyVAL.values = Values{yyDollar[1].valTuple}
		}
	case 239:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1277
		{
			yyVAL.values = append(yyDollar[1].values, yyDollar[3].valTuple)
		}
	case 240:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1283
		{
			yyVAL.valTuple = ValTuple(yyDollar[2].valExprs)
		}
	case 241:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1289
		{
			yyVAL.updateExprs = UpdateExprs{yyDollar[1].updateExpr}
		}
	case 242:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1293
		{
			yyVAL.updateExprs = append(yyDollar[1].updateExprs, yyDollar[3].updateExpr)
		}
	case 243:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1299
		{
			yyVAL.updateExpr = &UpdateExpr{Name: yyDollar[1].colIdent, Expr: yyDollar[3].valExpr}
		}
	case 246:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1308
		{
			yyVAL.byt = 0
		}
	case 247:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1310
		{
			yyVAL.byt = 1
		}
	case 248:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1313
		{
			yyVAL.empty = struct{}{}
		}
	case 249:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1315
		{
			yyVAL.empty = struct{}{}
		}
	case 250:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1318
		{
			yyVAL.str = ""
		}
	case 251:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1320
		{
			yyVAL.str = IgnoreStr
		}
	case 252:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1324
		{
			yyVAL.empty = struct{}{}
		}
	case 253:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1326
		{
			yyVAL.empty = struct{}{}
		}
	case 254:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1328
		{
			yyVAL.empty = struct{}{}
		}
	case 255:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1330
		{
			yyVAL.empty = struct{}{}
		}
	case 256:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1332
		{
			yyVAL.empty = struct{}{}
		}
	case 257:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1334
		{
			yyVAL.empty = struct{}{}
		}
	case 258:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1337
		{
			yyVAL.empty = struct{}{}
		}
	case 259:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1339
		{
			yyVAL.empty = struct{}{}
		}
	case 260:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1342
		{
			yyVAL.empty = struct{}{}
		}
	case 261:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1344
		{
			yyVAL.empty = struct{}{}
		}
	case 262:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1347
		{
			yyVAL.empty = struct{}{}
		}
	case 263:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1349
		{
			yyVAL.empty = struct{}{}
		}
	case 264:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1353
		{
			yyVAL.colIdent = NewColIdent(string(yyDollar[1].bytes))
		}
	case 265:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1359
		{
			yyVAL.tableIdent = NewTableIdent(string(yyDollar[1].bytes))
		}
	case 266:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1365
		{
			if incNesting(yylex) {
				yylex.Error("max nesting level reached")
				return 1
			}
		}
	case 267:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1374
		{
			decNesting(yylex)
		}
	case 268:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line ./go/vt/sqlparser/sql.y:1379
		{
			forceEOF(yylex)
		}
	}
	goto yystack /* stack new state and value */
}
