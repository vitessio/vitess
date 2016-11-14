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
const UNUSED = 57434

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
	45, 225,
	90, 225,
	-2, 224,
}

const yyNprod = 229
const yyPrivate = 57344

var yyTokenNames []string
var yyStates []string

const yyLast = 824

var yyAct = [...]int{

	131, 198, 289, 239, 62, 340, 132, 280, 201, 217,
	297, 115, 229, 330, 406, 251, 228, 227, 142, 200,
	3, 245, 102, 231, 151, 74, 125, 67, 103, 109,
	127, 126, 128, 129, 41, 64, 130, 40, 69, 41,
	35, 71, 37, 133, 157, 47, 38, 50, 358, 360,
	226, 107, 43, 44, 45, 81, 58, 155, 97, 389,
	388, 387, 116, 117, 68, 70, 46, 42, 118, 369,
	119, 48, 49, 312, 87, 367, 182, 101, 159, 419,
	108, 125, 125, 120, 63, 88, 172, 64, 90, 281,
	64, 140, 145, 181, 180, 188, 189, 183, 184, 185,
	186, 187, 182, 281, 92, 328, 166, 359, 84, 139,
	185, 186, 187, 182, 202, 94, 109, 96, 203, 204,
	205, 206, 242, 154, 156, 153, 220, 168, 315, 316,
	317, 208, 65, 171, 170, 211, 170, 148, 158, 188,
	189, 183, 184, 185, 186, 187, 182, 162, 172, 221,
	172, 236, 223, 415, 242, 246, 248, 249, 212, 216,
	247, 171, 170, 197, 199, 125, 108, 371, 235, 237,
	232, 108, 60, 254, 144, 250, 172, 109, 259, 260,
	261, 253, 263, 264, 265, 266, 267, 268, 269, 270,
	271, 272, 240, 234, 93, 219, 75, 258, 224, 171,
	170, 262, 89, 273, 274, 276, 14, 277, 275, 108,
	256, 257, 255, 308, 172, 64, 287, 125, 125, 285,
	60, 237, 243, 244, 65, 167, 288, 82, 83, 376,
	278, 275, 242, 284, 183, 184, 185, 186, 187, 182,
	232, 60, 28, 108, 313, 125, 294, 373, 60, 164,
	242, 311, 163, 65, 221, 295, 242, 383, 320, 321,
	322, 318, 253, 143, 314, 336, 242, 147, 331, 214,
	89, 319, 299, 302, 303, 304, 300, 324, 301, 305,
	73, 79, 325, 331, 242, 143, 125, 327, 164, 222,
	335, 337, 338, 341, 100, 333, 353, 386, 295, 334,
	385, 354, 232, 232, 232, 232, 350, 349, 55, 346,
	14, 348, 345, 364, 347, 363, 356, 138, 365, 351,
	89, 54, 366, 412, 352, 368, 76, 137, 86, 355,
	370, 303, 304, 329, 283, 413, 39, 374, 342, 392,
	372, 175, 178, 85, 149, 99, 112, 190, 191, 192,
	193, 194, 195, 196, 179, 176, 177, 174, 181, 180,
	188, 189, 183, 184, 185, 186, 187, 182, 57, 390,
	310, 51, 52, 391, 169, 290, 382, 394, 341, 112,
	112, 218, 395, 291, 221, 393, 136, 381, 207, 344,
	396, 398, 209, 135, 143, 61, 418, 404, 14, 405,
	28, 30, 407, 407, 407, 64, 1, 215, 309, 410,
	414, 112, 416, 417, 169, 306, 420, 408, 409, 165,
	421, 150, 422, 36, 225, 323, 152, 241, 66, 124,
	134, 233, 112, 397, 286, 399, 400, 112, 112, 112,
	146, 213, 252, 181, 180, 188, 189, 183, 184, 185,
	186, 187, 182, 125, 411, 242, 109, 127, 126, 128,
	129, 377, 339, 130, 122, 123, 380, 343, 111, 326,
	133, 210, 279, 59, 121, 112, 114, 332, 299, 302,
	303, 304, 300, 72, 301, 305, 80, 77, 384, 116,
	117, 104, 113, 282, 173, 118, 110, 119, 357, 298,
	296, 233, 59, 230, 105, 78, 53, 91, 27, 112,
	120, 95, 56, 13, 98, 12, 124, 11, 10, 106,
	9, 8, 7, 252, 6, 5, 59, 4, 141, 2,
	0, 14, 15, 16, 17, 0, 0, 0, 160, 0,
	125, 161, 29, 109, 127, 126, 128, 129, 0, 112,
	130, 122, 123, 18, 375, 111, 0, 133, 31, 32,
	33, 34, 0, 233, 233, 233, 233, 0, 0, 0,
	0, 0, 0, 0, 0, 59, 116, 117, 104, 378,
	379, 0, 118, 0, 119, 181, 180, 188, 189, 183,
	184, 185, 186, 187, 182, 0, 0, 120, 0, 0,
	0, 0, 0, 0, 59, 106, 0, 0, 0, 238,
	106, 0, 0, 0, 0, 0, 0, 19, 20, 22,
	21, 23, 0, 0, 0, 0, 0, 0, 0, 0,
	24, 25, 26, 0, 181, 180, 188, 189, 183, 184,
	185, 186, 187, 182, 14, 0, 0, 0, 106, 112,
	0, 112, 112, 0, 0, 401, 402, 403, 0, 124,
	0, 238, 0, 292, 0, 0, 293, 0, 0, 0,
	0, 0, 307, 65, 59, 0, 0, 0, 0, 0,
	0, 0, 106, 125, 0, 0, 109, 127, 126, 128,
	129, 0, 0, 130, 122, 123, 0, 0, 111, 0,
	133, 0, 181, 180, 188, 189, 183, 184, 185, 186,
	187, 182, 0, 0, 0, 0, 0, 124, 14, 116,
	117, 0, 0, 0, 0, 118, 0, 119, 0, 0,
	0, 0, 0, 0, 0, 0, 59, 59, 59, 59,
	120, 125, 0, 0, 109, 127, 126, 128, 129, 361,
	362, 130, 122, 123, 0, 0, 111, 125, 133, 0,
	109, 127, 126, 128, 129, 0, 0, 130, 0, 0,
	0, 0, 0, 0, 133, 0, 0, 116, 117, 0,
	0, 0, 0, 118, 0, 119, 0, 0, 0, 0,
	0, 0, 0, 116, 117, 0, 0, 0, 120, 118,
	0, 119, 181, 180, 188, 189, 183, 184, 185, 186,
	187, 182, 0, 0, 120, 180, 188, 189, 183, 184,
	185, 186, 187, 182,
}
var yyPact = [...]int{

	525, -1000, -1000, 395, -1000, -1000, -1000, -1000, -1000, -1000,
	-1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, -57,
	-62, -30, -45, -31, -1000, -1000, -1000, 392, 353, 289,
	-1000, -67, 124, 385, 84, -75, -34, 84, -1000, -32,
	84, -1000, 124, -77, 148, -77, 124, -1000, -1000, -1000,
	-1000, -1000, -1000, 246, 176, -1000, 54, 319, 300, -16,
	-1000, 124, 156, -1000, 22, -1000, 124, 44, 146, -1000,
	124, -1000, -42, 124, 324, 250, 84, -1000, 495, -1000,
	376, -1000, 297, 287, -1000, 124, 84, 124, 383, 84,
	-19, -1000, 323, -80, -1000, 30, -1000, 124, -1000, -1000,
	124, -1000, 242, -1000, -1000, 205, 37, 141, 281, -1000,
	-1000, 696, 638, -1000, -1000, -1000, -19, -19, -19, -19,
	173, -1000, -1000, -1000, 173, -1000, -1000, -1000, -1000, -1000,
	-1000, -1000, -1000, -19, 124, -1000, -1000, -1000, -1000, 241,
	274, -1000, 367, 696, -1000, 725, 36, 712, -1000, -1000,
	245, 84, -1000, -50, -1000, -1000, -1000, -1000, -1000, -1000,
	-1000, -1000, -1000, 172, 495, -1000, -1000, 84, 68, 408,
	696, 696, 100, -19, 120, 136, -19, -19, -19, 100,
	-19, -19, -19, -19, -19, -19, -19, -19, -19, -19,
	-1000, -1000, -1000, -1000, -1000, -1000, -1000, 13, 281, 75,
	237, 185, 281, -1000, -1000, -1000, 625, 495, -1000, 392,
	26, 725, -1000, 304, 84, 84, 367, 359, 368, 141,
	129, 725, 124, -1000, -1000, 124, -1000, 252, 238, -1000,
	-1000, 193, 350, 200, -1000, -1000, -1000, -1000, -17, -1000,
	203, 495, -1000, 13, 77, -1000, -1000, 73, -1000, -1000,
	725, -1000, 712, -1000, -1000, 120, -19, -19, -19, 725,
	725, 366, -1000, 60, 737, -1000, 27, 27, -10, -10,
	-10, 153, 153, -1000, -1000, -19, -1000, -1000, 203, 40,
	-1000, 696, 239, 173, 395, 224, 219, -1000, 359, -1000,
	-19, -19, -1000, -1000, 377, 172, 172, 172, 172, -1000,
	273, 272, -1000, 285, 262, 295, 6, -1000, 124, 124,
	-1000, 209, 84, -1000, 203, -1000, -1000, -1000, 185, -1000,
	725, 725, 16, -19, 725, -1000, -22, -1000, -19, 103,
	-1000, 315, 201, -1000, -1000, -1000, 84, -1000, 508, 183,
	-1000, 557, -1000, 374, 361, 238, 213, 444, -1000, -1000,
	-1000, -1000, 266, -1000, 263, -1000, -1000, -1000, -37, -38,
	-39, -1000, -1000, -1000, -1000, -1000, -1000, -19, 725, -1000,
	725, -19, 313, 173, -1000, -19, -19, -1000, -1000, -1000,
	367, 696, -19, 696, 696, -1000, -1000, 173, 173, 173,
	725, 725, 389, -1000, 725, -1000, 359, 141, 162, 141,
	141, 84, 84, 84, 84, 306, 107, -1000, 107, 107,
	156, -1000, 388, 3, -1000, 84, -1000, -1000, -1000, 84,
	-1000, 84, -1000,
}
var yyPgo = [...]int{

	0, 529, 19, 527, 525, 524, 522, 521, 520, 518,
	517, 515, 513, 542, 512, 508, 506, 505, 22, 28,
	504, 17, 16, 12, 503, 500, 10, 499, 23, 498,
	14, 18, 51, 496, 494, 493, 492, 1, 486, 21,
	15, 8, 477, 11, 6, 476, 474, 472, 7, 471,
	469, 467, 466, 9, 462, 5, 461, 2, 454, 441,
	434, 13, 4, 84, 430, 336, 280, 428, 426, 424,
	423, 421, 0, 419, 440, 415, 408, 45, 406, 401,
	267, 3,
}
var yyR1 = [...]int{

	0, 78, 1, 1, 1, 1, 1, 1, 1, 1,
	1, 1, 1, 2, 2, 2, 3, 3, 4, 5,
	6, 7, 7, 7, 8, 8, 8, 9, 10, 10,
	10, 11, 12, 12, 12, 79, 13, 14, 14, 15,
	15, 15, 16, 16, 17, 17, 18, 18, 19, 19,
	19, 20, 20, 73, 73, 73, 21, 21, 22, 22,
	23, 23, 23, 24, 24, 24, 24, 76, 76, 75,
	75, 75, 25, 25, 25, 25, 26, 26, 26, 26,
	27, 27, 28, 28, 29, 29, 29, 29, 30, 30,
	31, 31, 32, 32, 32, 32, 32, 32, 33, 33,
	33, 33, 33, 33, 33, 33, 33, 33, 33, 33,
	33, 39, 39, 39, 39, 39, 39, 34, 34, 34,
	34, 34, 34, 34, 40, 40, 40, 44, 41, 41,
	37, 37, 37, 37, 37, 37, 37, 37, 37, 37,
	37, 37, 37, 37, 37, 37, 37, 37, 37, 37,
	37, 37, 46, 49, 49, 47, 47, 48, 50, 50,
	45, 45, 45, 36, 36, 36, 36, 36, 38, 38,
	38, 51, 51, 52, 52, 53, 53, 54, 54, 55,
	56, 56, 56, 57, 57, 57, 58, 58, 58, 59,
	59, 60, 60, 61, 61, 35, 35, 42, 42, 43,
	43, 62, 62, 63, 64, 64, 66, 66, 67, 67,
	65, 65, 68, 68, 68, 68, 68, 68, 69, 69,
	70, 70, 71, 71, 72, 74, 80, 81, 77,
}
var yyR2 = [...]int{

	0, 1, 1, 1, 1, 1, 1, 1, 1, 1,
	1, 1, 1, 13, 6, 3, 8, 8, 8, 7,
	3, 5, 8, 4, 6, 7, 4, 5, 4, 5,
	5, 3, 2, 2, 2, 0, 2, 0, 2, 1,
	2, 2, 0, 1, 0, 1, 1, 3, 1, 2,
	3, 1, 1, 0, 1, 2, 1, 3, 1, 1,
	3, 3, 3, 3, 5, 5, 3, 0, 1, 0,
	1, 2, 1, 2, 2, 1, 2, 3, 2, 3,
	2, 2, 1, 3, 0, 5, 5, 5, 1, 3,
	0, 2, 1, 3, 3, 2, 3, 3, 1, 1,
	3, 3, 4, 3, 4, 3, 4, 5, 6, 3,
	2, 1, 2, 1, 2, 1, 2, 1, 1, 1,
	1, 1, 1, 1, 3, 1, 1, 3, 1, 3,
	1, 1, 1, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 2, 2, 2, 3, 3, 4, 5,
	4, 1, 5, 0, 1, 1, 2, 4, 0, 2,
	1, 3, 5, 1, 1, 1, 1, 1, 1, 2,
	2, 0, 3, 0, 2, 0, 3, 1, 3, 2,
	0, 1, 1, 0, 2, 4, 0, 2, 4, 0,
	3, 1, 3, 0, 5, 2, 1, 1, 3, 3,
	1, 1, 3, 3, 1, 1, 0, 2, 0, 3,
	0, 1, 1, 1, 1, 1, 1, 1, 0, 1,
	0, 1, 0, 2, 1, 1, 1, 1, 0,
}
var yyChk = [...]int{

	-1000, -78, -1, -2, -3, -4, -5, -6, -7, -8,
	-9, -10, -11, -12, 6, 7, 8, 9, 28, 92,
	93, 95, 94, 96, 105, 106, 107, -15, 5, -13,
	-79, -13, -13, -13, -13, 97, -70, 99, 103, -65,
	99, 101, 97, 97, 98, 99, 97, -77, -77, -77,
	-2, 18, 19, -16, 32, 19, -14, -65, -28, -74,
	48, 10, -62, -63, -72, 48, -67, 102, 98, -72,
	97, -72, -74, -66, 102, 48, -66, -74, -17, 35,
	-38, -72, 51, 52, 54, 24, 28, 90, -28, 46,
	66, -74, 60, 48, -77, -74, -77, 100, -74, 21,
	44, -72, -18, -19, 83, -20, -74, -32, -37, 48,
	-33, 60, -80, -36, -45, -43, 81, 82, 87, 89,
	102, -46, 56, 57, 21, 45, 50, 49, 51, 52,
	55, -72, -44, 62, -64, 17, 10, 30, 30, -28,
	-62, -74, -31, 11, -63, -37, -74, -80, -77, 21,
	-71, 104, -68, 95, 93, 27, 94, 14, 108, 48,
	-74, -74, -77, 10, 46, -73, -72, 20, 90, -80,
	59, 58, 73, -34, 76, 60, 74, 75, 61, 73,
	78, 77, 86, 81, 82, 83, 84, 85, 79, 80,
	66, 67, 68, 69, 70, 71, 72, -32, -37, -32,
	-2, -41, -37, -37, -37, -37, -37, -80, -44, -80,
	-49, -37, -28, -59, 28, -80, -31, -53, 14, -32,
	90, -37, 44, -72, -77, -69, 100, -21, -22, -23,
	-24, -28, -44, -80, -19, -72, 83, -72, -74, -81,
	-18, 19, 47, -32, -32, -39, 55, 60, 56, 57,
	-37, -40, -80, -44, 53, 76, 74, 75, 61, -37,
	-37, -37, -39, -37, -37, -37, -37, -37, -37, -37,
	-37, -37, -37, -81, -81, 46, -81, -72, -18, -47,
	-48, 63, -35, 30, -2, -62, -60, -72, -53, -57,
	16, 15, -74, -74, -31, 46, -25, -26, -27, 34,
	38, 40, 35, 36, 37, 41, -75, -74, 20, -76,
	20, -21, 90, -81, -18, 55, 56, 57, -41, -40,
	-37, -37, -37, 59, -37, -81, -50, -48, 65, -32,
	-61, 44, -42, -43, -61, -81, 46, -57, -37, -54,
	-55, -37, -77, -51, 12, -22, -23, -22, -23, 34,
	34, 34, 39, 34, 39, 34, -26, -29, 42, 101,
	43, -74, -74, -81, -72, -81, -81, 59, -37, 91,
	-37, 64, 25, 46, -72, 46, 46, -56, 22, 23,
	-52, 13, 15, 44, 44, 34, 34, 98, 98, 98,
	-37, -37, 26, -43, -37, -55, -53, -32, -41, -32,
	-32, -80, -80, -80, 8, -57, -30, -72, -30, -30,
	-62, -58, 17, 29, -81, 46, -81, -81, 8, 76,
	-72, -72, -72,
}
var yyDef = [...]int{

	0, -2, 1, 2, 3, 4, 5, 6, 7, 8,
	9, 10, 11, 12, 35, 35, 35, 35, 35, 220,
	210, 0, 0, 0, 228, 228, 228, 0, 39, 42,
	37, 210, 0, 0, 0, 208, 0, 0, 221, 0,
	0, 211, 0, 206, 0, 206, 0, 32, 33, 34,
	15, 40, 41, 44, 0, 43, 36, 0, 0, 82,
	225, 0, 20, 201, 0, 224, 0, 0, 0, 228,
	0, 228, 0, 0, 0, 0, 0, 31, 0, 45,
	0, 168, 0, 0, 38, 0, 0, 0, 90, 0,
	0, 228, 0, 222, 23, 0, 26, 0, 28, 207,
	0, 228, 0, 46, 48, 53, 0, 51, 52, -2,
	92, 0, 0, 130, 131, 132, 0, 0, 0, 0,
	0, 151, 98, 99, 0, 226, 163, 164, 165, 166,
	167, 160, 200, 153, 0, 204, 205, 169, 170, 189,
	90, 83, 175, 0, 202, 203, 0, 0, 21, 209,
	0, 0, 228, 218, 212, 213, 214, 215, 216, 217,
	27, 29, 30, 0, 0, 49, 54, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	117, 118, 119, 120, 121, 122, 123, 95, 0, 0,
	0, 0, 128, 143, 144, 145, 0, 0, 110, 0,
	0, 154, 14, 0, 0, 0, 175, 183, 0, 91,
	0, 128, 0, 223, 24, 0, 219, 90, 56, 58,
	59, 69, 67, 0, 47, 55, 50, 161, 0, 147,
	0, 0, 227, 93, 94, 97, 111, 0, 113, 115,
	100, 101, 0, 125, 126, 0, 0, 0, 0, 103,
	105, 0, 109, 133, 134, 135, 136, 137, 138, 139,
	140, 141, 142, 96, 127, 0, 199, 146, 0, 158,
	155, 0, 193, 0, 196, 193, 0, 191, 183, 19,
	0, 0, 228, 25, 171, 0, 0, 0, 0, 72,
	0, 0, 75, 0, 0, 0, 84, 70, 0, 0,
	68, 0, 0, 148, 0, 112, 114, 116, 0, 102,
	104, 106, 0, 0, 129, 150, 0, 156, 0, 0,
	16, 0, 195, 197, 17, 190, 0, 18, 184, 176,
	177, 180, 22, 173, 0, 57, 63, 0, 66, 73,
	74, 76, 0, 78, 0, 80, 81, 60, 0, 0,
	0, 71, 61, 62, 162, 149, 124, 0, 107, 152,
	159, 0, 0, 0, 192, 0, 0, 179, 181, 182,
	175, 0, 0, 0, 0, 77, 79, 0, 0, 0,
	108, 157, 0, 198, 185, 178, 183, 174, 172, 64,
	65, 0, 0, 0, 0, 186, 0, 88, 0, 0,
	194, 13, 0, 0, 85, 0, 86, 87, 187, 0,
	89, 0, 188,
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
	108,
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
		//line sql.y:171
		{
			setParseTree(yylex, yyDollar[1].statement)
		}
	case 2:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:177
		{
			yyVAL.statement = yyDollar[1].selStmt
		}
	case 13:
		yyDollar = yyS[yypt-13 : yypt+1]
		//line sql.y:193
		{
			yyVAL.selStmt = &Select{Comments: Comments(yyDollar[2].bytes2), Distinct: yyDollar[3].str, Hints: yyDollar[4].str, SelectExprs: yyDollar[5].selectExprs, From: yyDollar[7].tableExprs, Where: NewWhere(WhereStr, yyDollar[8].boolExpr), GroupBy: GroupBy(yyDollar[9].valExprs), Having: NewWhere(HavingStr, yyDollar[10].boolExpr), OrderBy: yyDollar[11].orderBy, Limit: yyDollar[12].limit, Lock: yyDollar[13].str}
		}
	case 14:
		yyDollar = yyS[yypt-6 : yypt+1]
		//line sql.y:197
		{
			yyVAL.selStmt = &Select{Comments: Comments(yyDollar[2].bytes2), SelectExprs: SelectExprs{Nextval{Expr: yyDollar[4].valExpr}}, From: TableExprs{&AliasedTableExpr{Expr: yyDollar[6].tableName}}}
		}
	case 15:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:201
		{
			yyVAL.selStmt = &Union{Type: yyDollar[2].str, Left: yyDollar[1].selStmt, Right: yyDollar[3].selStmt}
		}
	case 16:
		yyDollar = yyS[yypt-8 : yypt+1]
		//line sql.y:207
		{
			yyVAL.statement = &Insert{Comments: Comments(yyDollar[2].bytes2), Ignore: yyDollar[3].str, Table: yyDollar[5].tableName, Columns: yyDollar[6].columns, Rows: yyDollar[7].insRows, OnDup: OnDup(yyDollar[8].updateExprs)}
		}
	case 17:
		yyDollar = yyS[yypt-8 : yypt+1]
		//line sql.y:211
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
		//line sql.y:223
		{
			yyVAL.statement = &Update{Comments: Comments(yyDollar[2].bytes2), Table: yyDollar[3].tableName, Exprs: yyDollar[5].updateExprs, Where: NewWhere(WhereStr, yyDollar[6].boolExpr), OrderBy: yyDollar[7].orderBy, Limit: yyDollar[8].limit}
		}
	case 19:
		yyDollar = yyS[yypt-7 : yypt+1]
		//line sql.y:229
		{
			yyVAL.statement = &Delete{Comments: Comments(yyDollar[2].bytes2), Table: yyDollar[4].tableName, Where: NewWhere(WhereStr, yyDollar[5].boolExpr), OrderBy: yyDollar[6].orderBy, Limit: yyDollar[7].limit}
		}
	case 20:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:235
		{
			yyVAL.statement = &Set{Comments: Comments(yyDollar[2].bytes2), Exprs: yyDollar[3].updateExprs}
		}
	case 21:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:241
		{
			yyVAL.statement = &DDL{Action: CreateStr, NewName: yyDollar[4].tableIdent}
		}
	case 22:
		yyDollar = yyS[yypt-8 : yypt+1]
		//line sql.y:245
		{
			// Change this to an alter statement
			yyVAL.statement = &DDL{Action: AlterStr, Table: yyDollar[7].tableIdent, NewName: yyDollar[7].tableIdent}
		}
	case 23:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:250
		{
			yyVAL.statement = &DDL{Action: CreateStr, NewName: TableIdent(yyDollar[3].colIdent.Lowered())}
		}
	case 24:
		yyDollar = yyS[yypt-6 : yypt+1]
		//line sql.y:256
		{
			yyVAL.statement = &DDL{Action: AlterStr, Table: yyDollar[4].tableIdent, NewName: yyDollar[4].tableIdent}
		}
	case 25:
		yyDollar = yyS[yypt-7 : yypt+1]
		//line sql.y:260
		{
			// Change this to a rename statement
			yyVAL.statement = &DDL{Action: RenameStr, Table: yyDollar[4].tableIdent, NewName: yyDollar[7].tableIdent}
		}
	case 26:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:265
		{
			yyVAL.statement = &DDL{Action: AlterStr, Table: TableIdent(yyDollar[3].colIdent.Lowered()), NewName: TableIdent(yyDollar[3].colIdent.Lowered())}
		}
	case 27:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:271
		{
			yyVAL.statement = &DDL{Action: RenameStr, Table: yyDollar[3].tableIdent, NewName: yyDollar[5].tableIdent}
		}
	case 28:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:277
		{
			var exists bool
			if yyDollar[3].byt != 0 {
				exists = true
			}
			yyVAL.statement = &DDL{Action: DropStr, Table: yyDollar[4].tableIdent, IfExists: exists}
		}
	case 29:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:285
		{
			// Change this to an alter statement
			yyVAL.statement = &DDL{Action: AlterStr, Table: yyDollar[5].tableIdent, NewName: yyDollar[5].tableIdent}
		}
	case 30:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:290
		{
			var exists bool
			if yyDollar[3].byt != 0 {
				exists = true
			}
			yyVAL.statement = &DDL{Action: DropStr, Table: TableIdent(yyDollar[4].colIdent.Lowered()), IfExists: exists}
		}
	case 31:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:300
		{
			yyVAL.statement = &DDL{Action: AlterStr, Table: yyDollar[3].tableIdent, NewName: yyDollar[3].tableIdent}
		}
	case 32:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:306
		{
			yyVAL.statement = &Other{}
		}
	case 33:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:310
		{
			yyVAL.statement = &Other{}
		}
	case 34:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:314
		{
			yyVAL.statement = &Other{}
		}
	case 35:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:319
		{
			setAllowComments(yylex, true)
		}
	case 36:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:323
		{
			yyVAL.bytes2 = yyDollar[2].bytes2
			setAllowComments(yylex, false)
		}
	case 37:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:329
		{
			yyVAL.bytes2 = nil
		}
	case 38:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:333
		{
			yyVAL.bytes2 = append(yyDollar[1].bytes2, yyDollar[2].bytes)
		}
	case 39:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:339
		{
			yyVAL.str = UnionStr
		}
	case 40:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:343
		{
			yyVAL.str = UnionAllStr
		}
	case 41:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:347
		{
			yyVAL.str = UnionDistinctStr
		}
	case 42:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:352
		{
			yyVAL.str = ""
		}
	case 43:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:356
		{
			yyVAL.str = DistinctStr
		}
	case 44:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:361
		{
			yyVAL.str = ""
		}
	case 45:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:365
		{
			yyVAL.str = StraightJoinHint
		}
	case 46:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:371
		{
			yyVAL.selectExprs = SelectExprs{yyDollar[1].selectExpr}
		}
	case 47:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:375
		{
			yyVAL.selectExprs = append(yyVAL.selectExprs, yyDollar[3].selectExpr)
		}
	case 48:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:381
		{
			yyVAL.selectExpr = &StarExpr{}
		}
	case 49:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:385
		{
			yyVAL.selectExpr = &NonStarExpr{Expr: yyDollar[1].expr, As: yyDollar[2].colIdent}
		}
	case 50:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:389
		{
			yyVAL.selectExpr = &StarExpr{TableName: yyDollar[1].tableIdent}
		}
	case 51:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:395
		{
			yyVAL.expr = yyDollar[1].boolExpr
		}
	case 52:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:399
		{
			yyVAL.expr = yyDollar[1].valExpr
		}
	case 53:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:404
		{
			yyVAL.colIdent = ColIdent{}
		}
	case 54:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:408
		{
			yyVAL.colIdent = yyDollar[1].colIdent
		}
	case 55:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:412
		{
			yyVAL.colIdent = yyDollar[2].colIdent
		}
	case 56:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:418
		{
			yyVAL.tableExprs = TableExprs{yyDollar[1].tableExpr}
		}
	case 57:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:422
		{
			yyVAL.tableExprs = append(yyVAL.tableExprs, yyDollar[3].tableExpr)
		}
	case 60:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:432
		{
			yyVAL.tableExpr = &AliasedTableExpr{Expr: yyDollar[1].tableName, As: yyDollar[2].tableIdent, Hints: yyDollar[3].indexHints}
		}
	case 61:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:436
		{
			yyVAL.tableExpr = &AliasedTableExpr{Expr: yyDollar[1].subquery, As: yyDollar[3].tableIdent}
		}
	case 62:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:440
		{
			yyVAL.tableExpr = &ParenTableExpr{Exprs: yyDollar[2].tableExprs}
		}
	case 63:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:453
		{
			yyVAL.tableExpr = &JoinTableExpr{LeftExpr: yyDollar[1].tableExpr, Join: yyDollar[2].str, RightExpr: yyDollar[3].tableExpr}
		}
	case 64:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:457
		{
			yyVAL.tableExpr = &JoinTableExpr{LeftExpr: yyDollar[1].tableExpr, Join: yyDollar[2].str, RightExpr: yyDollar[3].tableExpr, On: yyDollar[5].boolExpr}
		}
	case 65:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:461
		{
			yyVAL.tableExpr = &JoinTableExpr{LeftExpr: yyDollar[1].tableExpr, Join: yyDollar[2].str, RightExpr: yyDollar[3].tableExpr, On: yyDollar[5].boolExpr}
		}
	case 66:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:465
		{
			yyVAL.tableExpr = &JoinTableExpr{LeftExpr: yyDollar[1].tableExpr, Join: yyDollar[2].str, RightExpr: yyDollar[3].tableExpr}
		}
	case 67:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:470
		{
			yyVAL.empty = struct{}{}
		}
	case 68:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:472
		{
			yyVAL.empty = struct{}{}
		}
	case 69:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:475
		{
			yyVAL.tableIdent = ""
		}
	case 70:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:479
		{
			yyVAL.tableIdent = yyDollar[1].tableIdent
		}
	case 71:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:483
		{
			yyVAL.tableIdent = yyDollar[2].tableIdent
		}
	case 72:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:489
		{
			yyVAL.str = JoinStr
		}
	case 73:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:493
		{
			yyVAL.str = JoinStr
		}
	case 74:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:497
		{
			yyVAL.str = JoinStr
		}
	case 75:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:501
		{
			yyVAL.str = StraightJoinStr
		}
	case 76:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:507
		{
			yyVAL.str = LeftJoinStr
		}
	case 77:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:511
		{
			yyVAL.str = LeftJoinStr
		}
	case 78:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:515
		{
			yyVAL.str = RightJoinStr
		}
	case 79:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:519
		{
			yyVAL.str = RightJoinStr
		}
	case 80:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:525
		{
			yyVAL.str = NaturalJoinStr
		}
	case 81:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:529
		{
			if yyDollar[2].str == LeftJoinStr {
				yyVAL.str = NaturalLeftJoinStr
			} else {
				yyVAL.str = NaturalRightJoinStr
			}
		}
	case 82:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:539
		{
			yyVAL.tableName = &TableName{Name: yyDollar[1].tableIdent}
		}
	case 83:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:543
		{
			yyVAL.tableName = &TableName{Qualifier: yyDollar[1].tableIdent, Name: yyDollar[3].tableIdent}
		}
	case 84:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:548
		{
			yyVAL.indexHints = nil
		}
	case 85:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:552
		{
			yyVAL.indexHints = &IndexHints{Type: UseStr, Indexes: yyDollar[4].colIdents}
		}
	case 86:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:556
		{
			yyVAL.indexHints = &IndexHints{Type: IgnoreStr, Indexes: yyDollar[4].colIdents}
		}
	case 87:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:560
		{
			yyVAL.indexHints = &IndexHints{Type: ForceStr, Indexes: yyDollar[4].colIdents}
		}
	case 88:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:566
		{
			yyVAL.colIdents = []ColIdent{yyDollar[1].colIdent}
		}
	case 89:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:570
		{
			yyVAL.colIdents = append(yyDollar[1].colIdents, yyDollar[3].colIdent)
		}
	case 90:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:575
		{
			yyVAL.boolExpr = nil
		}
	case 91:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:579
		{
			yyVAL.boolExpr = yyDollar[2].boolExpr
		}
	case 93:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:586
		{
			yyVAL.boolExpr = &AndExpr{Left: yyDollar[1].boolExpr, Right: yyDollar[3].boolExpr}
		}
	case 94:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:590
		{
			yyVAL.boolExpr = &OrExpr{Left: yyDollar[1].boolExpr, Right: yyDollar[3].boolExpr}
		}
	case 95:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:594
		{
			yyVAL.boolExpr = &NotExpr{Expr: yyDollar[2].boolExpr}
		}
	case 96:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:598
		{
			yyVAL.boolExpr = &ParenBoolExpr{Expr: yyDollar[2].boolExpr}
		}
	case 97:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:602
		{
			yyVAL.boolExpr = &IsExpr{Operator: yyDollar[3].str, Expr: yyDollar[1].boolExpr}
		}
	case 98:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:608
		{
			yyVAL.boolExpr = BoolVal(true)
		}
	case 99:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:612
		{
			yyVAL.boolExpr = BoolVal(false)
		}
	case 100:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:616
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: yyDollar[2].str, Right: yyDollar[3].valExpr}
		}
	case 101:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:620
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: InStr, Right: yyDollar[3].colTuple}
		}
	case 102:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:624
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: NotInStr, Right: yyDollar[4].colTuple}
		}
	case 103:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:628
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: LikeStr, Right: yyDollar[3].valExpr}
		}
	case 104:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:632
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: NotLikeStr, Right: yyDollar[4].valExpr}
		}
	case 105:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:636
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: RegexpStr, Right: yyDollar[3].valExpr}
		}
	case 106:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:640
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: NotRegexpStr, Right: yyDollar[4].valExpr}
		}
	case 107:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:644
		{
			yyVAL.boolExpr = &RangeCond{Left: yyDollar[1].valExpr, Operator: BetweenStr, From: yyDollar[3].valExpr, To: yyDollar[5].valExpr}
		}
	case 108:
		yyDollar = yyS[yypt-6 : yypt+1]
		//line sql.y:648
		{
			yyVAL.boolExpr = &RangeCond{Left: yyDollar[1].valExpr, Operator: NotBetweenStr, From: yyDollar[4].valExpr, To: yyDollar[6].valExpr}
		}
	case 109:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:652
		{
			yyVAL.boolExpr = &IsExpr{Operator: yyDollar[3].str, Expr: yyDollar[1].valExpr}
		}
	case 110:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:656
		{
			yyVAL.boolExpr = &ExistsExpr{Subquery: yyDollar[2].subquery}
		}
	case 111:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:662
		{
			yyVAL.str = IsNullStr
		}
	case 112:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:666
		{
			yyVAL.str = IsNotNullStr
		}
	case 113:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:670
		{
			yyVAL.str = IsTrueStr
		}
	case 114:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:674
		{
			yyVAL.str = IsNotTrueStr
		}
	case 115:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:678
		{
			yyVAL.str = IsFalseStr
		}
	case 116:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:682
		{
			yyVAL.str = IsNotFalseStr
		}
	case 117:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:688
		{
			yyVAL.str = EqualStr
		}
	case 118:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:692
		{
			yyVAL.str = LessThanStr
		}
	case 119:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:696
		{
			yyVAL.str = GreaterThanStr
		}
	case 120:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:700
		{
			yyVAL.str = LessEqualStr
		}
	case 121:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:704
		{
			yyVAL.str = GreaterEqualStr
		}
	case 122:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:708
		{
			yyVAL.str = NotEqualStr
		}
	case 123:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:712
		{
			yyVAL.str = NullSafeEqualStr
		}
	case 124:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:718
		{
			yyVAL.colTuple = ValTuple(yyDollar[2].valExprs)
		}
	case 125:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:722
		{
			yyVAL.colTuple = yyDollar[1].subquery
		}
	case 126:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:726
		{
			yyVAL.colTuple = ListArg(yyDollar[1].bytes)
		}
	case 127:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:732
		{
			yyVAL.subquery = &Subquery{yyDollar[2].selStmt}
		}
	case 128:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:738
		{
			yyVAL.valExprs = ValExprs{yyDollar[1].valExpr}
		}
	case 129:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:742
		{
			yyVAL.valExprs = append(yyDollar[1].valExprs, yyDollar[3].valExpr)
		}
	case 130:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:748
		{
			yyVAL.valExpr = yyDollar[1].valExpr
		}
	case 131:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:752
		{
			yyVAL.valExpr = yyDollar[1].colName
		}
	case 132:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:756
		{
			yyVAL.valExpr = yyDollar[1].rowTuple
		}
	case 133:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:760
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: BitAndStr, Right: yyDollar[3].valExpr}
		}
	case 134:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:764
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: BitOrStr, Right: yyDollar[3].valExpr}
		}
	case 135:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:768
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: BitXorStr, Right: yyDollar[3].valExpr}
		}
	case 136:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:772
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: PlusStr, Right: yyDollar[3].valExpr}
		}
	case 137:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:776
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: MinusStr, Right: yyDollar[3].valExpr}
		}
	case 138:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:780
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: MultStr, Right: yyDollar[3].valExpr}
		}
	case 139:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:784
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: DivStr, Right: yyDollar[3].valExpr}
		}
	case 140:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:788
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: ModStr, Right: yyDollar[3].valExpr}
		}
	case 141:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:792
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: ShiftLeftStr, Right: yyDollar[3].valExpr}
		}
	case 142:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:796
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: ShiftRightStr, Right: yyDollar[3].valExpr}
		}
	case 143:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:800
		{
			if num, ok := yyDollar[2].valExpr.(NumVal); ok {
				yyVAL.valExpr = num
			} else {
				yyVAL.valExpr = &UnaryExpr{Operator: UPlusStr, Expr: yyDollar[2].valExpr}
			}
		}
	case 144:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:808
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
	case 145:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:821
		{
			yyVAL.valExpr = &UnaryExpr{Operator: TildaStr, Expr: yyDollar[2].valExpr}
		}
	case 146:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:825
		{
			// This rule prevents the usage of INTERVAL
			// as a function. If support is needed for that,
			// we'll need to revisit this. The solution
			// will be non-trivial because of grammar conflicts.
			yyVAL.valExpr = &IntervalExpr{Expr: yyDollar[2].valExpr, Unit: yyDollar[3].colIdent}
		}
	case 147:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:833
		{
			yyVAL.valExpr = &FuncExpr{Name: string(yyDollar[1].tableIdent)}
		}
	case 148:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:837
		{
			yyVAL.valExpr = &FuncExpr{Name: string(yyDollar[1].tableIdent), Exprs: yyDollar[3].selectExprs}
		}
	case 149:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:841
		{
			yyVAL.valExpr = &FuncExpr{Name: string(yyDollar[1].tableIdent), Distinct: true, Exprs: yyDollar[4].selectExprs}
		}
	case 150:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:845
		{
			yyVAL.valExpr = &FuncExpr{Name: "if", Exprs: yyDollar[3].selectExprs}
		}
	case 151:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:849
		{
			yyVAL.valExpr = yyDollar[1].caseExpr
		}
	case 152:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:855
		{
			yyVAL.caseExpr = &CaseExpr{Expr: yyDollar[2].valExpr, Whens: yyDollar[3].whens, Else: yyDollar[4].valExpr}
		}
	case 153:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:860
		{
			yyVAL.valExpr = nil
		}
	case 154:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:864
		{
			yyVAL.valExpr = yyDollar[1].valExpr
		}
	case 155:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:870
		{
			yyVAL.whens = []*When{yyDollar[1].when}
		}
	case 156:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:874
		{
			yyVAL.whens = append(yyDollar[1].whens, yyDollar[2].when)
		}
	case 157:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:880
		{
			yyVAL.when = &When{Cond: yyDollar[2].boolExpr, Val: yyDollar[4].valExpr}
		}
	case 158:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:885
		{
			yyVAL.valExpr = nil
		}
	case 159:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:889
		{
			yyVAL.valExpr = yyDollar[2].valExpr
		}
	case 160:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:895
		{
			yyVAL.colName = &ColName{Name: yyDollar[1].colIdent}
		}
	case 161:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:899
		{
			yyVAL.colName = &ColName{Qualifier: &TableName{Name: yyDollar[1].tableIdent}, Name: yyDollar[3].colIdent}
		}
	case 162:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:903
		{
			yyVAL.colName = &ColName{Qualifier: &TableName{Qualifier: yyDollar[1].tableIdent, Name: yyDollar[3].tableIdent}, Name: yyDollar[5].colIdent}
		}
	case 163:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:909
		{
			yyVAL.valExpr = StrVal(yyDollar[1].bytes)
		}
	case 164:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:913
		{
			yyVAL.valExpr = HexVal(yyDollar[1].bytes)
		}
	case 165:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:917
		{
			yyVAL.valExpr = NumVal(yyDollar[1].bytes)
		}
	case 166:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:921
		{
			yyVAL.valExpr = ValArg(yyDollar[1].bytes)
		}
	case 167:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:925
		{
			yyVAL.valExpr = &NullVal{}
		}
	case 168:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:931
		{
			if yyDollar[1].colIdent.Lowered() != "value" {
				yylex.Error("expecting value after next")
				return 1
			}
			yyVAL.valExpr = NumVal("1")
		}
	case 169:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:939
		{
			yyVAL.valExpr = NumVal(yyDollar[1].bytes)
		}
	case 170:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:943
		{
			yyVAL.valExpr = ValArg(yyDollar[1].bytes)
		}
	case 171:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:948
		{
			yyVAL.valExprs = nil
		}
	case 172:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:952
		{
			yyVAL.valExprs = yyDollar[3].valExprs
		}
	case 173:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:957
		{
			yyVAL.boolExpr = nil
		}
	case 174:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:961
		{
			yyVAL.boolExpr = yyDollar[2].boolExpr
		}
	case 175:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:966
		{
			yyVAL.orderBy = nil
		}
	case 176:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:970
		{
			yyVAL.orderBy = yyDollar[3].orderBy
		}
	case 177:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:976
		{
			yyVAL.orderBy = OrderBy{yyDollar[1].order}
		}
	case 178:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:980
		{
			yyVAL.orderBy = append(yyDollar[1].orderBy, yyDollar[3].order)
		}
	case 179:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:986
		{
			yyVAL.order = &Order{Expr: yyDollar[1].valExpr, Direction: yyDollar[2].str}
		}
	case 180:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:991
		{
			yyVAL.str = AscScr
		}
	case 181:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:995
		{
			yyVAL.str = AscScr
		}
	case 182:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:999
		{
			yyVAL.str = DescScr
		}
	case 183:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1004
		{
			yyVAL.limit = nil
		}
	case 184:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:1008
		{
			yyVAL.limit = &Limit{Rowcount: yyDollar[2].valExpr}
		}
	case 185:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:1012
		{
			yyVAL.limit = &Limit{Offset: yyDollar[2].valExpr, Rowcount: yyDollar[4].valExpr}
		}
	case 186:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1017
		{
			yyVAL.str = ""
		}
	case 187:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:1021
		{
			yyVAL.str = ForUpdateStr
		}
	case 188:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:1025
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
	case 189:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1038
		{
			yyVAL.columns = nil
		}
	case 190:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1042
		{
			yyVAL.columns = yyDollar[2].columns
		}
	case 191:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1048
		{
			yyVAL.columns = Columns{yyDollar[1].colIdent}
		}
	case 192:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1052
		{
			yyVAL.columns = append(yyVAL.columns, yyDollar[3].colIdent)
		}
	case 193:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1057
		{
			yyVAL.updateExprs = nil
		}
	case 194:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:1061
		{
			yyVAL.updateExprs = yyDollar[5].updateExprs
		}
	case 195:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:1067
		{
			yyVAL.insRows = yyDollar[2].values
		}
	case 196:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1071
		{
			yyVAL.insRows = yyDollar[1].selStmt
		}
	case 197:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1077
		{
			yyVAL.values = Values{yyDollar[1].rowTuple}
		}
	case 198:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1081
		{
			yyVAL.values = append(yyDollar[1].values, yyDollar[3].rowTuple)
		}
	case 199:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1087
		{
			yyVAL.rowTuple = ValTuple(yyDollar[2].valExprs)
		}
	case 200:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1091
		{
			yyVAL.rowTuple = yyDollar[1].subquery
		}
	case 201:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1097
		{
			yyVAL.updateExprs = UpdateExprs{yyDollar[1].updateExpr}
		}
	case 202:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1101
		{
			yyVAL.updateExprs = append(yyDollar[1].updateExprs, yyDollar[3].updateExpr)
		}
	case 203:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1107
		{
			yyVAL.updateExpr = &UpdateExpr{Name: yyDollar[1].colIdent, Expr: yyDollar[3].valExpr}
		}
	case 206:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1116
		{
			yyVAL.byt = 0
		}
	case 207:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:1118
		{
			yyVAL.byt = 1
		}
	case 208:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1121
		{
			yyVAL.empty = struct{}{}
		}
	case 209:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1123
		{
			yyVAL.empty = struct{}{}
		}
	case 210:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1126
		{
			yyVAL.str = ""
		}
	case 211:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1128
		{
			yyVAL.str = IgnoreStr
		}
	case 212:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1132
		{
			yyVAL.empty = struct{}{}
		}
	case 213:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1134
		{
			yyVAL.empty = struct{}{}
		}
	case 214:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1136
		{
			yyVAL.empty = struct{}{}
		}
	case 215:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1138
		{
			yyVAL.empty = struct{}{}
		}
	case 216:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1140
		{
			yyVAL.empty = struct{}{}
		}
	case 217:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1142
		{
			yyVAL.empty = struct{}{}
		}
	case 218:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1145
		{
			yyVAL.empty = struct{}{}
		}
	case 219:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1147
		{
			yyVAL.empty = struct{}{}
		}
	case 220:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1150
		{
			yyVAL.empty = struct{}{}
		}
	case 221:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1152
		{
			yyVAL.empty = struct{}{}
		}
	case 222:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1155
		{
			yyVAL.empty = struct{}{}
		}
	case 223:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:1157
		{
			yyVAL.empty = struct{}{}
		}
	case 224:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1161
		{
			yyVAL.colIdent = NewColIdent(string(yyDollar[1].bytes))
		}
	case 225:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1167
		{
			yyVAL.tableIdent = TableIdent(yyDollar[1].bytes)
		}
	case 226:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1173
		{
			if incNesting(yylex) {
				yylex.Error("max nesting level reached")
				return 1
			}
		}
	case 227:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1182
		{
			decNesting(yylex)
		}
	case 228:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1187
		{
			forceEOF(yylex)
		}
	}
	goto yystack /* stack new state and value */
}
