//line sql.y:6
package sqlparser

import __yyfmt__ "fmt"

//line sql.y:6
import "strings"

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

//line sql.y:36
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
	smTableExpr SimpleTableExpr
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
	sqlID       SQLName
	sqlIDs      []SQLName
}

const LEX_ERROR = 57346
const UNION = 57347
const MINUS = 57348
const EXCEPT = 57349
const INTERSECT = 57350
const SELECT = 57351
const INSERT = 57352
const UPDATE = 57353
const DELETE = 57354
const FROM = 57355
const WHERE = 57356
const GROUP = 57357
const HAVING = 57358
const ORDER = 57359
const BY = 57360
const LIMIT = 57361
const FOR = 57362
const ALL = 57363
const DISTINCT = 57364
const AS = 57365
const EXISTS = 57366
const ASC = 57367
const DESC = 57368
const INTO = 57369
const DUPLICATE = 57370
const KEY = 57371
const DEFAULT = 57372
const SET = 57373
const LOCK = 57374
const KEYRANGE = 57375
const VALUES = 57376
const LAST_INSERT_ID = 57377
const JOIN = 57378
const STRAIGHT_JOIN = 57379
const LEFT = 57380
const RIGHT = 57381
const INNER = 57382
const OUTER = 57383
const CROSS = 57384
const NATURAL = 57385
const USE = 57386
const FORCE = 57387
const ON = 57388
const ID = 57389
const STRING = 57390
const NUMBER = 57391
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
const LE = 57406
const GE = 57407
const NE = 57408
const NULL_SAFE_EQUAL = 57409
const IS = 57410
const LIKE = 57411
const REGEXP = 57412
const IN = 57413
const SHIFT_LEFT = 57414
const SHIFT_RIGHT = 57415
const UNARY = 57416
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

var yyToknames = [...]string{
	"$end",
	"error",
	"$unk",
	"LEX_ERROR",
	"UNION",
	"MINUS",
	"EXCEPT",
	"INTERSECT",
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
	"KEYRANGE",
	"VALUES",
	"LAST_INSERT_ID",
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
}
var yyStatenames = [...]string{}

const yyEofCode = 1
const yyErrCode = 2
const yyMaxDepth = 200

//line yacctab:1
var yyExca = [...]int{
	-1, 1,
	1, -1,
	-2, 0,
	-1, 68,
	90, 219,
	-2, 218,
}

const yyNprod = 223
const yyPrivate = 57344

var yyTokenNames []string
var yyStates []string

const yyLast = 740

var yyAct = [...]int{

	98, 169, 166, 397, 93, 360, 350, 94, 63, 206,
	263, 315, 274, 257, 205, 168, 3, 204, 92, 245,
	217, 223, 64, 82, 185, 59, 38, 193, 40, 110,
	78, 83, 41, 14, 335, 337, 70, 43, 66, 44,
	44, 72, 65, 53, 75, 46, 47, 48, 74, 270,
	127, 372, 50, 371, 370, 71, 49, 45, 88, 149,
	148, 156, 157, 151, 152, 153, 154, 155, 150, 347,
	289, 105, 87, 137, 68, 106, 107, 108, 51, 52,
	109, 131, 153, 154, 155, 150, 135, 112, 117, 120,
	116, 336, 150, 73, 170, 230, 411, 138, 171, 173,
	174, 140, 119, 139, 138, 172, 95, 96, 228, 229,
	227, 140, 97, 258, 122, 181, 66, 113, 140, 66,
	65, 189, 188, 65, 183, 124, 111, 214, 126, 258,
	73, 306, 105, 177, 68, 246, 88, 213, 189, 226,
	182, 187, 14, 105, 222, 61, 61, 231, 232, 233,
	285, 235, 236, 237, 238, 239, 240, 241, 242, 243,
	244, 91, 209, 165, 167, 212, 136, 123, 234, 79,
	139, 138, 225, 186, 190, 250, 349, 61, 88, 88,
	105, 246, 118, 61, 203, 140, 379, 248, 247, 249,
	380, 139, 138, 73, 267, 251, 91, 91, 255, 252,
	254, 351, 199, 118, 175, 176, 140, 272, 262, 178,
	179, 215, 216, 406, 246, 197, 149, 148, 156, 157,
	151, 152, 153, 154, 155, 150, 293, 250, 288, 271,
	377, 295, 296, 297, 303, 200, 356, 246, 210, 91,
	209, 106, 107, 108, 91, 91, 109, 268, 224, 294,
	186, 299, 105, 218, 220, 221, 88, 225, 219, 265,
	366, 66, 66, 133, 246, 65, 313, 248, 246, 311,
	305, 132, 300, 314, 302, 310, 77, 301, 196, 198,
	195, 91, 91, 323, 118, 325, 369, 322, 261, 324,
	272, 246, 91, 351, 260, 333, 290, 291, 292, 266,
	130, 344, 209, 209, 209, 209, 133, 330, 340, 348,
	105, 368, 331, 342, 327, 326, 210, 353, 354, 358,
	361, 345, 346, 14, 328, 80, 357, 42, 115, 329,
	392, 307, 355, 224, 105, 403, 376, 68, 106, 107,
	108, 191, 332, 109, 280, 281, 373, 404, 309, 114,
	112, 129, 375, 28, 29, 30, 31, 66, 287, 91,
	56, 378, 58, 54, 91, 316, 374, 386, 250, 95,
	96, 362, 67, 365, 384, 97, 317, 264, 210, 210,
	210, 210, 394, 361, 364, 393, 395, 321, 186, 111,
	398, 398, 398, 62, 399, 400, 396, 246, 410, 401,
	14, 33, 66, 1, 286, 283, 65, 412, 60, 134,
	409, 192, 413, 39, 414, 269, 194, 405, 76, 407,
	408, 69, 81, 28, 29, 30, 31, 32, 86, 312,
	253, 259, 103, 402, 381, 60, 359, 385, 363, 387,
	388, 104, 121, 34, 35, 36, 37, 125, 320, 304,
	128, 180, 256, 100, 99, 105, 352, 246, 68, 106,
	107, 108, 308, 141, 109, 101, 102, 89, 334, 90,
	91, 112, 91, 91, 208, 275, 389, 390, 391, 14,
	15, 16, 17, 273, 207, 85, 55, 60, 27, 184,
	95, 96, 84, 103, 57, 13, 97, 12, 11, 10,
	201, 18, 104, 202, 9, 211, 86, 8, 7, 6,
	111, 5, 4, 2, 0, 343, 105, 0, 0, 68,
	106, 107, 108, 0, 0, 109, 101, 102, 0, 0,
	90, 0, 112, 149, 148, 156, 157, 151, 152, 153,
	154, 155, 150, 0, 14, 0, 382, 383, 86, 86,
	0, 95, 96, 84, 0, 0, 0, 97, 0, 103,
	0, 0, 19, 20, 22, 21, 23, 0, 104, 0,
	0, 111, 0, 0, 0, 24, 25, 26, 0, 0,
	0, 284, 105, 211, 0, 68, 106, 107, 108, 0,
	0, 109, 101, 102, 0, 0, 90, 0, 112, 149,
	148, 156, 157, 151, 152, 153, 154, 155, 150, 298,
	151, 152, 153, 154, 155, 150, 0, 95, 96, 0,
	0, 0, 0, 97, 0, 0, 86, 149, 148, 156,
	157, 151, 152, 153, 154, 155, 150, 111, 0, 318,
	103, 0, 319, 0, 0, 211, 211, 211, 211, 104,
	156, 157, 151, 152, 153, 154, 155, 150, 338, 339,
	0, 0, 341, 105, 0, 0, 68, 106, 107, 108,
	0, 0, 109, 101, 102, 0, 0, 90, 0, 112,
	148, 156, 157, 151, 152, 153, 154, 155, 150, 0,
	276, 279, 280, 281, 277, 0, 278, 282, 95, 96,
	367, 0, 0, 0, 97, 276, 279, 280, 281, 277,
	0, 278, 282, 143, 146, 0, 0, 0, 111, 158,
	159, 160, 161, 162, 163, 164, 147, 144, 145, 142,
	149, 148, 156, 157, 151, 152, 153, 154, 155, 150,
}
var yyPact = [...]int{

	470, -1000, -1000, 418, -1000, -1000, -1000, -1000, -1000, -1000,
	-1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, -71,
	-62, -40, -52, -41, -1000, -1000, -1000, 391, 342, -1000,
	-1000, -1000, 338, -1000, -61, 95, 380, 84, -66, -43,
	80, -1000, -49, 80, -1000, 95, -72, 119, -72, 95,
	-1000, -1000, -1000, -1000, -1000, 469, -1000, 62, 322, 297,
	0, -1000, 95, 134, -1000, 35, -1000, -1, -1000, 95,
	53, 117, -1000, -1000, 95, -1000, -50, 95, 327, 254,
	80, -1000, 258, -1000, -1000, 143, -17, 44, 652, -1000,
	616, 535, -1000, -1000, -1000, 287, 287, 287, 205, 205,
	-1000, -1000, -1000, 205, 205, -1000, -1000, -1000, -1000, -1000,
	-1000, -1000, 287, -1000, 95, 84, 95, 374, 84, 287,
	80, -1000, 317, -77, -1000, 185, -1000, 95, -1000, -1000,
	95, -1000, 96, 469, -1000, -1000, 80, 43, 616, 616,
	197, 287, 85, 33, 287, 287, 287, 197, 287, 287,
	287, 287, 287, 287, 287, 287, 287, 287, -1000, -1000,
	-1000, -1000, -1000, -1000, -1000, 27, 652, 132, 348, 219,
	652, -1000, 24, -1000, -1000, 408, 469, -1000, 391, 190,
	49, -19, 263, 236, -1000, 360, 616, -1000, -19, -1000,
	-1000, -1000, 253, 80, -1000, -51, -1000, -1000, -1000, -1000,
	-1000, -1000, -1000, -1000, 159, 669, -1000, -1000, 127, 335,
	133, -20, -1000, -1000, -1000, 27, 37, -1000, -1000, 240,
	-1000, -1000, -19, -1000, 24, -1000, -1000, 85, 287, 287,
	287, -19, -19, 549, -1000, 570, 601, -1000, -2, -2,
	5, 5, 5, 528, 528, -1000, -1000, -1000, 287, -1000,
	-19, -1000, 215, 469, 215, 186, 65, -1000, 616, 314,
	84, 84, 360, 346, 358, 44, 95, -1000, -1000, 95,
	-1000, 372, 96, 96, 96, 96, -1000, 279, 278, -1000,
	288, 271, 306, -10, -1000, 95, 95, -1000, 242, 95,
	-1000, -1000, -1000, 219, -1000, -19, -19, 455, 287, -19,
	-1000, 215, -1000, 190, -22, -1000, 287, 111, 247, 205,
	418, 155, 188, -1000, 346, -1000, 287, 287, -1000, -1000,
	368, 355, 669, 214, 654, -1000, -1000, -1000, -1000, 275,
	-1000, 250, -1000, -1000, -1000, -44, -45, -47, -1000, -1000,
	-1000, -1000, -1000, 287, -19, -1000, 86, -1000, -19, 287,
	-1000, 308, 182, -1000, -1000, -1000, 84, -1000, 138, 142,
	-1000, 521, -1000, 360, 616, 287, 616, 616, -1000, -1000,
	205, 205, 205, -19, -1000, -19, 301, 205, -1000, 287,
	287, -1000, -1000, -1000, 346, 44, 139, 44, 44, 80,
	80, 80, 388, -1000, -19, -1000, 315, 165, -1000, 165,
	165, 84, -1000, 387, 19, -1000, 80, -1000, -1000, 134,
	-1000, 80, -1000, 80, -1000,
}
var yyPgo = [...]int{

	0, 513, 15, 512, 511, 509, 508, 507, 504, 499,
	498, 497, 495, 427, 494, 488, 486, 23, 31, 485,
	17, 14, 9, 484, 483, 12, 475, 474, 25, 468,
	3, 24, 72, 467, 463, 462, 18, 2, 20, 21,
	1, 456, 7, 454, 29, 4, 453, 452, 13, 451,
	449, 448, 438, 10, 436, 5, 434, 11, 433, 431,
	429, 6, 8, 22, 327, 276, 421, 416, 415, 413,
	411, 0, 409, 372, 405, 404, 52, 403, 401, 105,
	19,
}
var yyR1 = [...]int{

	0, 77, 1, 1, 1, 1, 1, 1, 1, 1,
	1, 1, 1, 2, 2, 3, 3, 4, 5, 6,
	7, 7, 7, 8, 8, 8, 9, 10, 10, 10,
	11, 12, 12, 12, 78, 13, 14, 14, 15, 15,
	15, 15, 15, 16, 16, 17, 17, 18, 18, 18,
	19, 19, 72, 72, 72, 20, 20, 21, 21, 22,
	22, 22, 23, 23, 23, 23, 75, 75, 74, 74,
	74, 24, 24, 24, 24, 25, 25, 25, 25, 26,
	26, 27, 27, 28, 28, 29, 29, 29, 29, 30,
	30, 31, 31, 32, 32, 32, 32, 32, 32, 33,
	33, 33, 33, 33, 33, 33, 33, 33, 33, 33,
	33, 33, 33, 38, 38, 38, 38, 38, 38, 34,
	34, 34, 34, 34, 34, 34, 39, 39, 39, 44,
	40, 40, 37, 37, 37, 37, 37, 37, 37, 37,
	37, 37, 37, 37, 37, 37, 37, 37, 37, 37,
	37, 37, 37, 43, 46, 49, 49, 47, 47, 48,
	50, 50, 45, 45, 36, 36, 36, 36, 51, 51,
	52, 52, 53, 53, 54, 54, 55, 56, 56, 56,
	57, 57, 57, 58, 58, 58, 59, 59, 60, 60,
	61, 61, 35, 35, 41, 41, 42, 42, 62, 62,
	63, 65, 65, 66, 66, 64, 64, 67, 67, 67,
	67, 67, 68, 68, 69, 69, 70, 70, 71, 73,
	79, 80, 76,
}
var yyR2 = [...]int{

	0, 1, 1, 1, 1, 1, 1, 1, 1, 1,
	1, 1, 1, 12, 3, 8, 8, 8, 7, 3,
	5, 8, 4, 6, 7, 4, 5, 4, 5, 5,
	3, 2, 2, 2, 0, 2, 0, 2, 1, 2,
	1, 1, 1, 0, 1, 1, 3, 1, 2, 3,
	1, 1, 0, 1, 2, 1, 3, 1, 1, 3,
	3, 3, 3, 5, 5, 3, 0, 1, 0, 1,
	2, 1, 2, 2, 1, 2, 3, 2, 3, 2,
	2, 1, 3, 1, 3, 0, 5, 5, 5, 1,
	3, 0, 2, 1, 3, 3, 2, 3, 3, 1,
	1, 3, 3, 4, 3, 4, 3, 4, 5, 6,
	3, 2, 6, 1, 2, 1, 2, 1, 2, 1,
	1, 1, 1, 1, 1, 1, 3, 1, 1, 3,
	1, 3, 1, 1, 1, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 2, 2, 2, 3, 4,
	5, 4, 1, 1, 5, 0, 1, 1, 2, 4,
	0, 2, 1, 3, 1, 1, 1, 1, 0, 3,
	0, 2, 0, 3, 1, 3, 2, 0, 1, 1,
	0, 2, 4, 0, 2, 4, 0, 3, 1, 3,
	0, 5, 2, 1, 1, 3, 3, 1, 1, 3,
	3, 0, 2, 0, 3, 0, 1, 1, 1, 1,
	1, 1, 0, 1, 0, 1, 0, 2, 1, 1,
	1, 1, 0,
}
var yyChk = [...]int{

	-1000, -77, -1, -2, -3, -4, -5, -6, -7, -8,
	-9, -10, -11, -12, 9, 10, 11, 12, 31, 92,
	93, 95, 94, 96, 105, 106, 107, -15, 5, 6,
	7, 8, -13, -78, -13, -13, -13, -13, 97, -69,
	99, 103, -64, 99, 101, 97, 97, 98, 99, 97,
	-76, -76, -76, -2, 21, -16, 22, -14, -64, -28,
	-73, 50, 13, -62, -63, -45, -71, -73, 50, -66,
	102, 98, -71, 50, 97, -71, -73, -65, 102, 50,
	-65, -73, -17, -18, 84, -19, -73, -32, -37, -33,
	61, -79, -36, -45, -42, 82, 83, 88, -71, -43,
	-46, 57, 58, 24, 33, 47, 51, 52, 53, 56,
	-44, 102, 63, 55, 27, 31, 90, -28, 48, 67,
	90, -73, 61, 50, -76, -73, -76, 100, -73, 24,
	46, -71, 13, 48, -72, -71, 23, 90, 60, 59,
	74, -34, 77, 61, 75, 76, 62, 74, 79, 78,
	87, 82, 83, 84, 85, 86, 80, 81, 67, 68,
	69, 70, 71, 72, 73, -32, -37, -32, -2, -40,
	-37, -37, -79, -37, -37, -79, -79, -44, -79, -79,
	-49, -37, -28, -62, -73, -31, 14, -63, -37, -71,
	-76, 24, -70, 104, -67, 95, 93, 30, 94, 17,
	50, -73, -73, -76, -20, -21, -22, -23, -27, -44,
	-79, -73, -18, -71, 84, -32, -32, -38, 56, 61,
	57, 58, -37, -39, -79, -44, 54, 77, 75, 76,
	62, -37, -37, -37, -38, -37, -37, -37, -37, -37,
	-37, -37, -37, -37, -37, -80, 49, -80, 48, -80,
	-37, -80, -17, 22, -17, -36, -47, -48, 64, -59,
	31, -79, -31, -53, 17, -32, 46, -71, -76, -68,
	100, -31, 48, -24, -25, -26, 36, 40, 42, 37,
	38, 39, 43, -74, -73, 23, -75, 23, -20, 90,
	56, 57, 58, -40, -39, -37, -37, -37, 60, -37,
	-80, -17, -80, 48, -50, -48, 66, -32, -35, 34,
	-2, -62, -60, -45, -53, -57, 19, 18, -73, -73,
	-51, 15, -21, -22, -21, -22, 36, 36, 36, 41,
	36, 41, 36, -25, -29, 44, 101, 45, -73, -73,
	-80, -73, -80, 60, -37, -80, -36, 91, -37, 65,
	-61, 46, -41, -42, -61, -80, 48, -57, -37, -54,
	-55, -37, -76, -52, 16, 18, 46, 46, 36, 36,
	98, 98, 98, -37, -80, -37, 28, 48, -45, 48,
	48, -56, 25, 26, -53, -32, -40, -32, -32, -79,
	-79, -79, 29, -42, -37, -55, -57, -30, -71, -30,
	-30, 11, -58, 20, 32, -80, 48, -80, -80, -62,
	11, 77, -71, -71, -71,
}
var yyDef = [...]int{

	0, -2, 1, 2, 3, 4, 5, 6, 7, 8,
	9, 10, 11, 12, 34, 34, 34, 34, 34, 214,
	205, 0, 0, 0, 222, 222, 222, 0, 38, 40,
	41, 42, 43, 36, 205, 0, 0, 0, 203, 0,
	0, 215, 0, 0, 206, 0, 201, 0, 201, 0,
	31, 32, 33, 14, 39, 0, 44, 35, 0, 0,
	83, 219, 0, 19, 198, 0, 162, 0, -2, 0,
	0, 0, 222, 218, 0, 222, 0, 0, 0, 0,
	0, 30, 0, 45, 47, 52, 0, 50, 51, 93,
	0, 0, 132, 133, 134, 0, 0, 0, 162, 0,
	152, 99, 100, 0, 0, 220, 164, 165, 166, 167,
	197, 153, 155, 37, 0, 0, 0, 91, 0, 0,
	0, 222, 0, 216, 22, 0, 25, 0, 27, 202,
	0, 222, 0, 0, 48, 53, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 119, 120,
	121, 122, 123, 124, 125, 96, 0, 0, 0, 0,
	130, 145, 0, 146, 147, 0, 0, 111, 0, 0,
	0, 156, 186, 91, 84, 172, 0, 199, 200, 163,
	20, 204, 0, 0, 222, 212, 207, 208, 209, 210,
	211, 26, 28, 29, 91, 55, 57, 58, 68, 66,
	0, 81, 46, 54, 49, 94, 95, 98, 113, 0,
	115, 117, 101, 102, 0, 127, 128, 0, 0, 0,
	0, 104, 106, 0, 110, 135, 136, 137, 138, 139,
	140, 141, 142, 143, 144, 97, 221, 129, 0, 196,
	130, 148, 0, 0, 0, 0, 160, 157, 0, 0,
	0, 0, 172, 180, 0, 92, 0, 217, 23, 0,
	213, 168, 0, 0, 0, 0, 71, 0, 0, 74,
	0, 0, 0, 85, 69, 0, 0, 67, 0, 0,
	114, 116, 118, 0, 103, 105, 107, 0, 0, 131,
	149, 0, 151, 0, 0, 158, 0, 0, 190, 0,
	193, 190, 0, 188, 180, 18, 0, 0, 222, 24,
	170, 0, 56, 62, 0, 65, 72, 73, 75, 0,
	77, 0, 79, 80, 59, 0, 0, 0, 70, 60,
	61, 82, 126, 0, 108, 150, 0, 154, 161, 0,
	15, 0, 192, 194, 16, 187, 0, 17, 181, 173,
	174, 177, 21, 172, 0, 0, 0, 0, 76, 78,
	0, 0, 0, 109, 112, 159, 0, 0, 189, 0,
	0, 176, 178, 179, 180, 171, 169, 63, 64, 0,
	0, 0, 0, 195, 182, 175, 183, 0, 89, 0,
	0, 0, 13, 0, 0, 86, 0, 87, 88, 191,
	184, 0, 90, 0, 185,
}
var yyTok1 = [...]int{

	1, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 86, 79, 3,
	47, 49, 84, 82, 48, 83, 90, 85, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	68, 67, 69, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 87, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 78, 3, 88,
}
var yyTok2 = [...]int{

	2, 3, 4, 5, 6, 7, 8, 9, 10, 11,
	12, 13, 14, 15, 16, 17, 18, 19, 20, 21,
	22, 23, 24, 25, 26, 27, 28, 29, 30, 31,
	32, 33, 34, 35, 36, 37, 38, 39, 40, 41,
	42, 43, 44, 45, 46, 50, 51, 52, 53, 54,
	55, 56, 57, 58, 59, 60, 61, 62, 63, 64,
	65, 66, 70, 71, 72, 73, 74, 75, 76, 77,
	80, 81, 89, 91, 92, 93, 94, 95, 96, 97,
	98, 99, 100, 101, 102, 103, 104, 105, 106, 107,
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
	lookahead func() int
}

func (p *yyParserImpl) Lookahead() int {
	return p.lookahead()
}

func yyNewParser() yyParser {
	p := &yyParserImpl{
		lookahead: func() int { return -1 },
	}
	return p
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
	var yylval yySymType
	var yyVAL yySymType
	var yyDollar []yySymType
	yyS := make([]yySymType, yyMaxDepth)

	Nerrs := 0   /* number of errors */
	Errflag := 0 /* error recovery flag */
	yystate := 0
	yychar := -1
	yytoken := -1 // yychar translated into internal numbering
	yyrcvr.lookahead = func() int { return yychar }
	defer func() {
		// Make sure we report no lookahead when not parsing.
		yystate = -1
		yychar = -1
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
	if yychar < 0 {
		yychar, yytoken = yylex1(yylex, &yylval)
	}
	yyn += yytoken
	if yyn < 0 || yyn >= yyLast {
		goto yydefault
	}
	yyn = yyAct[yyn]
	if yyChk[yyn] == yytoken { /* valid shift */
		yychar = -1
		yytoken = -1
		yyVAL = yylval
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
		if yychar < 0 {
			yychar, yytoken = yylex1(yylex, &yylval)
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
			yychar = -1
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
		//line sql.y:168
		{
			setParseTree(yylex, yyDollar[1].statement)
		}
	case 2:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:174
		{
			yyVAL.statement = yyDollar[1].selStmt
		}
	case 13:
		yyDollar = yyS[yypt-12 : yypt+1]
		//line sql.y:190
		{
			yyVAL.selStmt = &Select{Comments: Comments(yyDollar[2].bytes2), Distinct: yyDollar[3].str, SelectExprs: yyDollar[4].selectExprs, From: yyDollar[6].tableExprs, Where: NewWhere(WhereStr, yyDollar[7].boolExpr), GroupBy: GroupBy(yyDollar[8].valExprs), Having: NewWhere(HavingStr, yyDollar[9].boolExpr), OrderBy: yyDollar[10].orderBy, Limit: yyDollar[11].limit, Lock: yyDollar[12].str}
		}
	case 14:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:194
		{
			yyVAL.selStmt = &Union{Type: yyDollar[2].str, Left: yyDollar[1].selStmt, Right: yyDollar[3].selStmt}
		}
	case 15:
		yyDollar = yyS[yypt-8 : yypt+1]
		//line sql.y:200
		{
			yyVAL.statement = &Insert{Comments: Comments(yyDollar[2].bytes2), Ignore: yyDollar[3].str, Table: yyDollar[5].tableName, Columns: yyDollar[6].columns, Rows: yyDollar[7].insRows, OnDup: OnDup(yyDollar[8].updateExprs)}
		}
	case 16:
		yyDollar = yyS[yypt-8 : yypt+1]
		//line sql.y:204
		{
			cols := make(Columns, 0, len(yyDollar[7].updateExprs))
			vals := make(ValTuple, 0, len(yyDollar[7].updateExprs))
			for _, col := range yyDollar[7].updateExprs {
				cols = append(cols, &NonStarExpr{Expr: col.Name})
				vals = append(vals, col.Expr)
			}
			yyVAL.statement = &Insert{Comments: Comments(yyDollar[2].bytes2), Ignore: yyDollar[3].str, Table: yyDollar[5].tableName, Columns: cols, Rows: Values{vals}, OnDup: OnDup(yyDollar[8].updateExprs)}
		}
	case 17:
		yyDollar = yyS[yypt-8 : yypt+1]
		//line sql.y:216
		{
			yyVAL.statement = &Update{Comments: Comments(yyDollar[2].bytes2), Table: yyDollar[3].tableName, Exprs: yyDollar[5].updateExprs, Where: NewWhere(WhereStr, yyDollar[6].boolExpr), OrderBy: yyDollar[7].orderBy, Limit: yyDollar[8].limit}
		}
	case 18:
		yyDollar = yyS[yypt-7 : yypt+1]
		//line sql.y:222
		{
			yyVAL.statement = &Delete{Comments: Comments(yyDollar[2].bytes2), Table: yyDollar[4].tableName, Where: NewWhere(WhereStr, yyDollar[5].boolExpr), OrderBy: yyDollar[6].orderBy, Limit: yyDollar[7].limit}
		}
	case 19:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:228
		{
			yyVAL.statement = &Set{Comments: Comments(yyDollar[2].bytes2), Exprs: yyDollar[3].updateExprs}
		}
	case 20:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:234
		{
			yyVAL.statement = &DDL{Action: CreateStr, NewName: yyDollar[4].sqlID}
		}
	case 21:
		yyDollar = yyS[yypt-8 : yypt+1]
		//line sql.y:238
		{
			// Change this to an alter statement
			yyVAL.statement = &DDL{Action: AlterStr, Table: yyDollar[7].sqlID, NewName: yyDollar[7].sqlID}
		}
	case 22:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:243
		{
			yyVAL.statement = &DDL{Action: CreateStr, NewName: SQLName(yyDollar[3].sqlID)}
		}
	case 23:
		yyDollar = yyS[yypt-6 : yypt+1]
		//line sql.y:249
		{
			yyVAL.statement = &DDL{Action: AlterStr, Table: yyDollar[4].sqlID, NewName: yyDollar[4].sqlID}
		}
	case 24:
		yyDollar = yyS[yypt-7 : yypt+1]
		//line sql.y:253
		{
			// Change this to a rename statement
			yyVAL.statement = &DDL{Action: RenameStr, Table: yyDollar[4].sqlID, NewName: yyDollar[7].sqlID}
		}
	case 25:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:258
		{
			yyVAL.statement = &DDL{Action: AlterStr, Table: SQLName(yyDollar[3].sqlID), NewName: SQLName(yyDollar[3].sqlID)}
		}
	case 26:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:264
		{
			yyVAL.statement = &DDL{Action: RenameStr, Table: yyDollar[3].sqlID, NewName: yyDollar[5].sqlID}
		}
	case 27:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:270
		{
			yyVAL.statement = &DDL{Action: DropStr, Table: yyDollar[4].sqlID}
		}
	case 28:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:274
		{
			// Change this to an alter statement
			yyVAL.statement = &DDL{Action: AlterStr, Table: yyDollar[5].sqlID, NewName: yyDollar[5].sqlID}
		}
	case 29:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:279
		{
			yyVAL.statement = &DDL{Action: DropStr, Table: SQLName(yyDollar[4].sqlID)}
		}
	case 30:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:285
		{
			yyVAL.statement = &DDL{Action: AlterStr, Table: yyDollar[3].sqlID, NewName: yyDollar[3].sqlID}
		}
	case 31:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:291
		{
			yyVAL.statement = &Other{}
		}
	case 32:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:295
		{
			yyVAL.statement = &Other{}
		}
	case 33:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:299
		{
			yyVAL.statement = &Other{}
		}
	case 34:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:304
		{
			setAllowComments(yylex, true)
		}
	case 35:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:308
		{
			yyVAL.bytes2 = yyDollar[2].bytes2
			setAllowComments(yylex, false)
		}
	case 36:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:314
		{
			yyVAL.bytes2 = nil
		}
	case 37:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:318
		{
			yyVAL.bytes2 = append(yyDollar[1].bytes2, yyDollar[2].bytes)
		}
	case 38:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:324
		{
			yyVAL.str = UnionStr
		}
	case 39:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:328
		{
			yyVAL.str = UnionAllStr
		}
	case 40:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:332
		{
			yyVAL.str = SetMinusStr
		}
	case 41:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:336
		{
			yyVAL.str = ExceptStr
		}
	case 42:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:340
		{
			yyVAL.str = IntersectStr
		}
	case 43:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:345
		{
			yyVAL.str = ""
		}
	case 44:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:349
		{
			yyVAL.str = DistinctStr
		}
	case 45:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:355
		{
			yyVAL.selectExprs = SelectExprs{yyDollar[1].selectExpr}
		}
	case 46:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:359
		{
			yyVAL.selectExprs = append(yyVAL.selectExprs, yyDollar[3].selectExpr)
		}
	case 47:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:365
		{
			yyVAL.selectExpr = &StarExpr{}
		}
	case 48:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:369
		{
			yyVAL.selectExpr = &NonStarExpr{Expr: yyDollar[1].expr, As: yyDollar[2].sqlID}
		}
	case 49:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:373
		{
			yyVAL.selectExpr = &StarExpr{TableName: yyDollar[1].sqlID}
		}
	case 50:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:379
		{
			yyVAL.expr = yyDollar[1].boolExpr
		}
	case 51:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:383
		{
			yyVAL.expr = yyDollar[1].valExpr
		}
	case 52:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:388
		{
			yyVAL.sqlID = ""
		}
	case 53:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:392
		{
			yyVAL.sqlID = yyDollar[1].sqlID
		}
	case 54:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:396
		{
			yyVAL.sqlID = yyDollar[2].sqlID
		}
	case 55:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:402
		{
			yyVAL.tableExprs = TableExprs{yyDollar[1].tableExpr}
		}
	case 56:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:406
		{
			yyVAL.tableExprs = append(yyVAL.tableExprs, yyDollar[3].tableExpr)
		}
	case 59:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:416
		{
			yyVAL.tableExpr = &AliasedTableExpr{Expr: yyDollar[1].smTableExpr, As: yyDollar[2].sqlID, Hints: yyDollar[3].indexHints}
		}
	case 60:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:420
		{
			yyVAL.tableExpr = &AliasedTableExpr{Expr: yyDollar[1].subquery, As: yyDollar[3].sqlID}
		}
	case 61:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:424
		{
			yyVAL.tableExpr = &ParenTableExpr{Exprs: yyDollar[2].tableExprs}
		}
	case 62:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:437
		{
			yyVAL.tableExpr = &JoinTableExpr{LeftExpr: yyDollar[1].tableExpr, Join: yyDollar[2].str, RightExpr: yyDollar[3].tableExpr}
		}
	case 63:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:441
		{
			yyVAL.tableExpr = &JoinTableExpr{LeftExpr: yyDollar[1].tableExpr, Join: yyDollar[2].str, RightExpr: yyDollar[3].tableExpr, On: yyDollar[5].boolExpr}
		}
	case 64:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:445
		{
			yyVAL.tableExpr = &JoinTableExpr{LeftExpr: yyDollar[1].tableExpr, Join: yyDollar[2].str, RightExpr: yyDollar[3].tableExpr, On: yyDollar[5].boolExpr}
		}
	case 65:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:449
		{
			yyVAL.tableExpr = &JoinTableExpr{LeftExpr: yyDollar[1].tableExpr, Join: yyDollar[2].str, RightExpr: yyDollar[3].tableExpr}
		}
	case 66:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:454
		{
			yyVAL.empty = struct{}{}
		}
	case 67:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:456
		{
			yyVAL.empty = struct{}{}
		}
	case 68:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:459
		{
			yyVAL.sqlID = ""
		}
	case 69:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:463
		{
			yyVAL.sqlID = yyDollar[1].sqlID
		}
	case 70:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:467
		{
			yyVAL.sqlID = yyDollar[2].sqlID
		}
	case 71:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:473
		{
			yyVAL.str = JoinStr
		}
	case 72:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:477
		{
			yyVAL.str = JoinStr
		}
	case 73:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:481
		{
			yyVAL.str = JoinStr
		}
	case 74:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:485
		{
			yyVAL.str = StraightJoinStr
		}
	case 75:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:491
		{
			yyVAL.str = LeftJoinStr
		}
	case 76:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:495
		{
			yyVAL.str = LeftJoinStr
		}
	case 77:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:499
		{
			yyVAL.str = RightJoinStr
		}
	case 78:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:503
		{
			yyVAL.str = RightJoinStr
		}
	case 79:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:509
		{
			yyVAL.str = NaturalJoinStr
		}
	case 80:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:513
		{
			if yyDollar[2].str == LeftJoinStr {
				yyVAL.str = NaturalLeftJoinStr
			} else {
				yyVAL.str = NaturalRightJoinStr
			}
		}
	case 81:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:523
		{
			yyVAL.smTableExpr = &TableName{Name: yyDollar[1].sqlID}
		}
	case 82:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:527
		{
			yyVAL.smTableExpr = &TableName{Qualifier: yyDollar[1].sqlID, Name: yyDollar[3].sqlID}
		}
	case 83:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:533
		{
			yyVAL.tableName = &TableName{Name: yyDollar[1].sqlID}
		}
	case 84:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:537
		{
			yyVAL.tableName = &TableName{Qualifier: yyDollar[1].sqlID, Name: yyDollar[3].sqlID}
		}
	case 85:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:542
		{
			yyVAL.indexHints = nil
		}
	case 86:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:546
		{
			yyVAL.indexHints = &IndexHints{Type: UseStr, Indexes: yyDollar[4].sqlIDs}
		}
	case 87:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:550
		{
			yyVAL.indexHints = &IndexHints{Type: IgnoreStr, Indexes: yyDollar[4].sqlIDs}
		}
	case 88:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:554
		{
			yyVAL.indexHints = &IndexHints{Type: ForceStr, Indexes: yyDollar[4].sqlIDs}
		}
	case 89:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:560
		{
			yyVAL.sqlIDs = []SQLName{yyDollar[1].sqlID}
		}
	case 90:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:564
		{
			yyVAL.sqlIDs = append(yyDollar[1].sqlIDs, yyDollar[3].sqlID)
		}
	case 91:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:569
		{
			yyVAL.boolExpr = nil
		}
	case 92:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:573
		{
			yyVAL.boolExpr = yyDollar[2].boolExpr
		}
	case 94:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:580
		{
			yyVAL.boolExpr = &AndExpr{Left: yyDollar[1].boolExpr, Right: yyDollar[3].boolExpr}
		}
	case 95:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:584
		{
			yyVAL.boolExpr = &OrExpr{Left: yyDollar[1].boolExpr, Right: yyDollar[3].boolExpr}
		}
	case 96:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:588
		{
			yyVAL.boolExpr = &NotExpr{Expr: yyDollar[2].boolExpr}
		}
	case 97:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:592
		{
			yyVAL.boolExpr = &ParenBoolExpr{Expr: yyDollar[2].boolExpr}
		}
	case 98:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:596
		{
			yyVAL.boolExpr = &IsExpr{Operator: yyDollar[3].str, Expr: yyDollar[1].boolExpr}
		}
	case 99:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:602
		{
			yyVAL.boolExpr = BoolVal(true)
		}
	case 100:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:606
		{
			yyVAL.boolExpr = BoolVal(false)
		}
	case 101:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:610
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: yyDollar[2].str, Right: yyDollar[3].valExpr}
		}
	case 102:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:614
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: InStr, Right: yyDollar[3].colTuple}
		}
	case 103:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:618
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: NotInStr, Right: yyDollar[4].colTuple}
		}
	case 104:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:622
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: LikeStr, Right: yyDollar[3].valExpr}
		}
	case 105:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:626
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: NotLikeStr, Right: yyDollar[4].valExpr}
		}
	case 106:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:630
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: RegexpStr, Right: yyDollar[3].valExpr}
		}
	case 107:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:634
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: NotRegexpStr, Right: yyDollar[4].valExpr}
		}
	case 108:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:638
		{
			yyVAL.boolExpr = &RangeCond{Left: yyDollar[1].valExpr, Operator: BetweenStr, From: yyDollar[3].valExpr, To: yyDollar[5].valExpr}
		}
	case 109:
		yyDollar = yyS[yypt-6 : yypt+1]
		//line sql.y:642
		{
			yyVAL.boolExpr = &RangeCond{Left: yyDollar[1].valExpr, Operator: NotBetweenStr, From: yyDollar[4].valExpr, To: yyDollar[6].valExpr}
		}
	case 110:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:646
		{
			yyVAL.boolExpr = &IsExpr{Operator: yyDollar[3].str, Expr: yyDollar[1].valExpr}
		}
	case 111:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:650
		{
			yyVAL.boolExpr = &ExistsExpr{Subquery: yyDollar[2].subquery}
		}
	case 112:
		yyDollar = yyS[yypt-6 : yypt+1]
		//line sql.y:654
		{
			yyVAL.boolExpr = &KeyrangeExpr{Start: yyDollar[3].valExpr, End: yyDollar[5].valExpr}
		}
	case 113:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:660
		{
			yyVAL.str = IsNullStr
		}
	case 114:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:664
		{
			yyVAL.str = IsNotNullStr
		}
	case 115:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:668
		{
			yyVAL.str = IsTrueStr
		}
	case 116:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:672
		{
			yyVAL.str = IsNotTrueStr
		}
	case 117:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:676
		{
			yyVAL.str = IsFalseStr
		}
	case 118:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:680
		{
			yyVAL.str = IsNotFalseStr
		}
	case 119:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:686
		{
			yyVAL.str = EqualStr
		}
	case 120:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:690
		{
			yyVAL.str = LessThanStr
		}
	case 121:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:694
		{
			yyVAL.str = GreaterThanStr
		}
	case 122:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:698
		{
			yyVAL.str = LessEqualStr
		}
	case 123:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:702
		{
			yyVAL.str = GreaterEqualStr
		}
	case 124:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:706
		{
			yyVAL.str = NotEqualStr
		}
	case 125:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:710
		{
			yyVAL.str = NullSafeEqualStr
		}
	case 126:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:716
		{
			yyVAL.colTuple = ValTuple(yyDollar[2].valExprs)
		}
	case 127:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:720
		{
			yyVAL.colTuple = yyDollar[1].subquery
		}
	case 128:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:724
		{
			yyVAL.colTuple = ListArg(yyDollar[1].bytes)
		}
	case 129:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:730
		{
			yyVAL.subquery = &Subquery{yyDollar[2].selStmt}
		}
	case 130:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:736
		{
			yyVAL.valExprs = ValExprs{yyDollar[1].valExpr}
		}
	case 131:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:740
		{
			yyVAL.valExprs = append(yyDollar[1].valExprs, yyDollar[3].valExpr)
		}
	case 132:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:746
		{
			yyVAL.valExpr = yyDollar[1].valExpr
		}
	case 133:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:750
		{
			yyVAL.valExpr = yyDollar[1].colName
		}
	case 134:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:754
		{
			yyVAL.valExpr = yyDollar[1].rowTuple
		}
	case 135:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:758
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: BitAndStr, Right: yyDollar[3].valExpr}
		}
	case 136:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:762
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: BitOrStr, Right: yyDollar[3].valExpr}
		}
	case 137:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:766
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: BitXorStr, Right: yyDollar[3].valExpr}
		}
	case 138:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:770
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: PlusStr, Right: yyDollar[3].valExpr}
		}
	case 139:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:774
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: MinusStr, Right: yyDollar[3].valExpr}
		}
	case 140:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:778
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: MultStr, Right: yyDollar[3].valExpr}
		}
	case 141:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:782
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: DivStr, Right: yyDollar[3].valExpr}
		}
	case 142:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:786
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: ModStr, Right: yyDollar[3].valExpr}
		}
	case 143:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:790
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: ShiftLeftStr, Right: yyDollar[3].valExpr}
		}
	case 144:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:794
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: ShiftRightStr, Right: yyDollar[3].valExpr}
		}
	case 145:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:798
		{
			if num, ok := yyDollar[2].valExpr.(NumVal); ok {
				yyVAL.valExpr = num
			} else {
				yyVAL.valExpr = &UnaryExpr{Operator: UPlusStr, Expr: yyDollar[2].valExpr}
			}
		}
	case 146:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:806
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
		//line sql.y:819
		{
			yyVAL.valExpr = &UnaryExpr{Operator: TildaStr, Expr: yyDollar[2].valExpr}
		}
	case 148:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:823
		{
			yyVAL.valExpr = &FuncExpr{Name: string(yyDollar[1].sqlID)}
		}
	case 149:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:827
		{
			yyVAL.valExpr = &FuncExpr{Name: string(yyDollar[1].sqlID), Exprs: yyDollar[3].selectExprs}
		}
	case 150:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:831
		{
			yyVAL.valExpr = &FuncExpr{Name: string(yyDollar[1].sqlID), Distinct: true, Exprs: yyDollar[4].selectExprs}
		}
	case 151:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:835
		{
			yyVAL.valExpr = &FuncExpr{Name: yyDollar[1].str, Exprs: yyDollar[3].selectExprs}
		}
	case 152:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:839
		{
			yyVAL.valExpr = yyDollar[1].caseExpr
		}
	case 153:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:845
		{
			yyVAL.str = "if"
		}
	case 154:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:851
		{
			yyVAL.caseExpr = &CaseExpr{Expr: yyDollar[2].valExpr, Whens: yyDollar[3].whens, Else: yyDollar[4].valExpr}
		}
	case 155:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:856
		{
			yyVAL.valExpr = nil
		}
	case 156:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:860
		{
			yyVAL.valExpr = yyDollar[1].valExpr
		}
	case 157:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:866
		{
			yyVAL.whens = []*When{yyDollar[1].when}
		}
	case 158:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:870
		{
			yyVAL.whens = append(yyDollar[1].whens, yyDollar[2].when)
		}
	case 159:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:876
		{
			yyVAL.when = &When{Cond: yyDollar[2].boolExpr, Val: yyDollar[4].valExpr}
		}
	case 160:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:881
		{
			yyVAL.valExpr = nil
		}
	case 161:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:885
		{
			yyVAL.valExpr = yyDollar[2].valExpr
		}
	case 162:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:891
		{
			yyVAL.colName = &ColName{Name: yyDollar[1].sqlID}
		}
	case 163:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:895
		{
			yyVAL.colName = &ColName{Qualifier: yyDollar[1].sqlID, Name: yyDollar[3].sqlID}
		}
	case 164:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:901
		{
			yyVAL.valExpr = StrVal(yyDollar[1].bytes)
		}
	case 165:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:905
		{
			yyVAL.valExpr = NumVal(yyDollar[1].bytes)
		}
	case 166:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:909
		{
			yyVAL.valExpr = ValArg(yyDollar[1].bytes)
		}
	case 167:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:913
		{
			yyVAL.valExpr = &NullVal{}
		}
	case 168:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:918
		{
			yyVAL.valExprs = nil
		}
	case 169:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:922
		{
			yyVAL.valExprs = yyDollar[3].valExprs
		}
	case 170:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:927
		{
			yyVAL.boolExpr = nil
		}
	case 171:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:931
		{
			yyVAL.boolExpr = yyDollar[2].boolExpr
		}
	case 172:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:936
		{
			yyVAL.orderBy = nil
		}
	case 173:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:940
		{
			yyVAL.orderBy = yyDollar[3].orderBy
		}
	case 174:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:946
		{
			yyVAL.orderBy = OrderBy{yyDollar[1].order}
		}
	case 175:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:950
		{
			yyVAL.orderBy = append(yyDollar[1].orderBy, yyDollar[3].order)
		}
	case 176:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:956
		{
			yyVAL.order = &Order{Expr: yyDollar[1].valExpr, Direction: yyDollar[2].str}
		}
	case 177:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:961
		{
			yyVAL.str = AscScr
		}
	case 178:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:965
		{
			yyVAL.str = AscScr
		}
	case 179:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:969
		{
			yyVAL.str = DescScr
		}
	case 180:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:974
		{
			yyVAL.limit = nil
		}
	case 181:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:978
		{
			yyVAL.limit = &Limit{Rowcount: yyDollar[2].valExpr}
		}
	case 182:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:982
		{
			yyVAL.limit = &Limit{Offset: yyDollar[2].valExpr, Rowcount: yyDollar[4].valExpr}
		}
	case 183:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:987
		{
			yyVAL.str = ""
		}
	case 184:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:991
		{
			yyVAL.str = ForUpdateStr
		}
	case 185:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:995
		{
			if yyDollar[3].sqlID != "share" {
				yylex.Error("expecting share")
				return 1
			}
			if yyDollar[4].sqlID != "mode" {
				yylex.Error("expecting mode")
				return 1
			}
			yyVAL.str = ShareModeStr
		}
	case 186:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1008
		{
			yyVAL.columns = nil
		}
	case 187:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1012
		{
			yyVAL.columns = yyDollar[2].columns
		}
	case 188:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1018
		{
			yyVAL.columns = Columns{&NonStarExpr{Expr: yyDollar[1].colName}}
		}
	case 189:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1022
		{
			yyVAL.columns = append(yyVAL.columns, &NonStarExpr{Expr: yyDollar[3].colName})
		}
	case 190:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1027
		{
			yyVAL.updateExprs = nil
		}
	case 191:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:1031
		{
			yyVAL.updateExprs = yyDollar[5].updateExprs
		}
	case 192:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:1037
		{
			yyVAL.insRows = yyDollar[2].values
		}
	case 193:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1041
		{
			yyVAL.insRows = yyDollar[1].selStmt
		}
	case 194:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1047
		{
			yyVAL.values = Values{yyDollar[1].rowTuple}
		}
	case 195:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1051
		{
			yyVAL.values = append(yyDollar[1].values, yyDollar[3].rowTuple)
		}
	case 196:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1057
		{
			yyVAL.rowTuple = ValTuple(yyDollar[2].valExprs)
		}
	case 197:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1061
		{
			yyVAL.rowTuple = yyDollar[1].subquery
		}
	case 198:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1067
		{
			yyVAL.updateExprs = UpdateExprs{yyDollar[1].updateExpr}
		}
	case 199:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1071
		{
			yyVAL.updateExprs = append(yyDollar[1].updateExprs, yyDollar[3].updateExpr)
		}
	case 200:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1077
		{
			yyVAL.updateExpr = &UpdateExpr{Name: yyDollar[1].colName, Expr: yyDollar[3].valExpr}
		}
	case 201:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1082
		{
			yyVAL.empty = struct{}{}
		}
	case 202:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:1084
		{
			yyVAL.empty = struct{}{}
		}
	case 203:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1087
		{
			yyVAL.empty = struct{}{}
		}
	case 204:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1089
		{
			yyVAL.empty = struct{}{}
		}
	case 205:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1092
		{
			yyVAL.str = ""
		}
	case 206:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1094
		{
			yyVAL.str = IgnoreStr
		}
	case 207:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1098
		{
			yyVAL.empty = struct{}{}
		}
	case 208:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1100
		{
			yyVAL.empty = struct{}{}
		}
	case 209:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1102
		{
			yyVAL.empty = struct{}{}
		}
	case 210:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1104
		{
			yyVAL.empty = struct{}{}
		}
	case 211:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1106
		{
			yyVAL.empty = struct{}{}
		}
	case 212:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1109
		{
			yyVAL.empty = struct{}{}
		}
	case 213:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1111
		{
			yyVAL.empty = struct{}{}
		}
	case 214:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1114
		{
			yyVAL.empty = struct{}{}
		}
	case 215:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1116
		{
			yyVAL.empty = struct{}{}
		}
	case 216:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1119
		{
			yyVAL.empty = struct{}{}
		}
	case 217:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:1121
		{
			yyVAL.empty = struct{}{}
		}
	case 218:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1125
		{
			yyVAL.sqlID = SQLName(strings.ToLower(string(yyDollar[1].bytes)))
		}
	case 219:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1131
		{
			yyVAL.sqlID = SQLName(yyDollar[1].bytes)
		}
	case 220:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1137
		{
			if incNesting(yylex) {
				yylex.Error("max nesting level reached")
				return 1
			}
		}
	case 221:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1146
		{
			decNesting(yylex)
		}
	case 222:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1151
		{
			forceEOF(yylex)
		}
	}
	goto yystack /* stack new state and value */
}
