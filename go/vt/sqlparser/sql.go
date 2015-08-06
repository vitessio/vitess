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
const SELECT = 57347
const INSERT = 57348
const UPDATE = 57349
const DELETE = 57350
const FROM = 57351
const WHERE = 57352
const GROUP = 57353
const HAVING = 57354
const ORDER = 57355
const BY = 57356
const LIMIT = 57357
const FOR = 57358
const ALL = 57359
const DISTINCT = 57360
const AS = 57361
const EXISTS = 57362
const IN = 57363
const IS = 57364
const LIKE = 57365
const BETWEEN = 57366
const NULL = 57367
const ASC = 57368
const DESC = 57369
const VALUES = 57370
const INTO = 57371
const DUPLICATE = 57372
const KEY = 57373
const DEFAULT = 57374
const SET = 57375
const LOCK = 57376
const KEYRANGE = 57377
const ID = 57378
const STRING = 57379
const NUMBER = 57380
const VALUE_ARG = 57381
const LIST_ARG = 57382
const COMMENT = 57383
const LE = 57384
const GE = 57385
const NE = 57386
const NULL_SAFE_EQUAL = 57387
const UNION = 57388
const MINUS = 57389
const EXCEPT = 57390
const INTERSECT = 57391
const JOIN = 57392
const STRAIGHT_JOIN = 57393
const LEFT = 57394
const RIGHT = 57395
const INNER = 57396
const OUTER = 57397
const CROSS = 57398
const NATURAL = 57399
const USE = 57400
const FORCE = 57401
const ON = 57402
const OR = 57403
const AND = 57404
const NOT = 57405
const SHIFT_LEFT = 57406
const SHIFT_RIGHT = 57407
const UNARY = 57408
const CASE = 57409
const WHEN = 57410
const THEN = 57411
const ELSE = 57412
const END = 57413
const CREATE = 57414
const ALTER = 57415
const DROP = 57416
const RENAME = 57417
const ANALYZE = 57418
const TABLE = 57419
const INDEX = 57420
const VIEW = 57421
const TO = 57422
const IGNORE = 57423
const IF = 57424
const UNIQUE = 57425
const USING = 57426
const SHOW = 57427
const DESCRIBE = 57428
const EXPLAIN = 57429

var yyToknames = [...]string{
	"$end",
	"error",
	"$unk",
	"LEX_ERROR",
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
	"IN",
	"IS",
	"LIKE",
	"BETWEEN",
	"NULL",
	"ASC",
	"DESC",
	"VALUES",
	"INTO",
	"DUPLICATE",
	"KEY",
	"DEFAULT",
	"SET",
	"LOCK",
	"KEYRANGE",
	"ID",
	"STRING",
	"NUMBER",
	"VALUE_ARG",
	"LIST_ARG",
	"COMMENT",
	"LE",
	"GE",
	"NE",
	"NULL_SAFE_EQUAL",
	"'('",
	"'='",
	"'<'",
	"'>'",
	"UNION",
	"MINUS",
	"EXCEPT",
	"INTERSECT",
	"','",
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
	"OR",
	"AND",
	"NOT",
	"'|'",
	"'&'",
	"'+'",
	"'-'",
	"'*'",
	"'/'",
	"'%'",
	"SHIFT_LEFT",
	"SHIFT_RIGHT",
	"'^'",
	"'~'",
	"UNARY",
	"'.'",
	"CASE",
	"WHEN",
	"THEN",
	"ELSE",
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
	"')'",
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
	81, 203,
	-2, 202,
}

const yyNprod = 207
const yyPrivate = 57344

var yyTokenNames []string
var yyStates []string

const yyLast = 691

var yyAct = [...]int{

	98, 166, 163, 340, 236, 373, 257, 93, 303, 63,
	94, 215, 295, 204, 92, 165, 3, 382, 184, 132,
	64, 248, 237, 138, 137, 82, 239, 83, 301, 192,
	78, 101, 108, 70, 264, 43, 107, 44, 66, 110,
	126, 72, 351, 53, 75, 65, 102, 68, 104, 105,
	106, 46, 47, 48, 101, 320, 322, 103, 88, 107,
	237, 350, 110, 349, 71, 74, 237, 87, 237, 102,
	68, 104, 105, 106, 49, 237, 45, 237, 249, 90,
	103, 130, 95, 96, 84, 38, 134, 40, 321, 331,
	97, 41, 147, 111, 167, 249, 279, 293, 168, 170,
	171, 136, 90, 119, 115, 95, 96, 121, 109, 28,
	29, 30, 31, 97, 178, 66, 111, 137, 66, 346,
	188, 187, 65, 296, 182, 65, 268, 269, 270, 271,
	272, 109, 273, 274, 174, 88, 210, 188, 186, 260,
	50, 73, 214, 129, 224, 222, 223, 348, 226, 227,
	228, 229, 230, 231, 232, 233, 234, 235, 162, 164,
	209, 198, 237, 347, 208, 169, 51, 52, 138, 137,
	238, 240, 241, 217, 237, 88, 88, 242, 211, 318,
	196, 66, 66, 117, 199, 117, 333, 225, 65, 255,
	253, 246, 59, 261, 296, 252, 138, 137, 243, 245,
	317, 256, 153, 154, 147, 212, 213, 150, 151, 152,
	153, 154, 147, 123, 316, 131, 125, 239, 280, 241,
	278, 91, 265, 282, 283, 77, 268, 269, 270, 271,
	272, 281, 273, 274, 185, 314, 195, 197, 194, 208,
	315, 312, 286, 358, 335, 290, 313, 88, 287, 118,
	289, 113, 217, 259, 103, 116, 91, 91, 112, 300,
	132, 189, 298, 302, 172, 173, 299, 175, 176, 292,
	288, 202, 185, 180, 80, 360, 361, 277, 266, 181,
	310, 311, 61, 324, 73, 326, 103, 328, 28, 29,
	30, 31, 103, 329, 61, 68, 332, 206, 91, 208,
	208, 14, 66, 91, 91, 330, 216, 338, 341, 336,
	61, 337, 14, 15, 16, 17, 117, 294, 146, 145,
	148, 149, 150, 151, 152, 153, 154, 147, 135, 122,
	352, 218, 61, 79, 262, 353, 354, 103, 91, 91,
	18, 114, 103, 355, 334, 73, 356, 364, 241, 58,
	362, 91, 148, 149, 150, 151, 152, 153, 154, 147,
	370, 341, 371, 14, 379, 285, 386, 374, 374, 374,
	66, 372, 206, 375, 376, 67, 190, 65, 381, 377,
	383, 384, 380, 387, 128, 216, 251, 388, 107, 389,
	219, 56, 220, 221, 19, 20, 22, 21, 23, 54,
	104, 105, 106, 304, 345, 305, 258, 24, 25, 26,
	91, 60, 363, 344, 365, 91, 309, 185, 62, 385,
	369, 76, 14, 33, 275, 81, 133, 191, 39, 263,
	193, 86, 206, 206, 60, 42, 69, 254, 60, 244,
	179, 101, 378, 359, 339, 120, 107, 342, 343, 110,
	124, 308, 291, 127, 357, 177, 102, 68, 104, 105,
	106, 247, 100, 99, 297, 14, 250, 103, 139, 146,
	145, 148, 149, 150, 151, 152, 153, 154, 147, 89,
	101, 319, 205, 267, 203, 107, 85, 55, 110, 90,
	27, 183, 95, 96, 84, 102, 68, 104, 105, 106,
	97, 57, 200, 111, 32, 201, 103, 207, 86, 13,
	91, 12, 91, 14, 11, 366, 367, 368, 109, 10,
	34, 35, 36, 37, 237, 9, 8, 7, 90, 6,
	5, 95, 96, 107, 4, 2, 110, 1, 0, 97,
	0, 0, 111, 0, 68, 104, 105, 106, 86, 86,
	107, 0, 0, 110, 103, 0, 0, 109, 0, 0,
	0, 68, 104, 105, 106, 0, 0, 0, 0, 0,
	0, 103, 0, 0, 0, 0, 0, 0, 0, 95,
	96, 276, 207, 0, 0, 0, 0, 97, 0, 0,
	111, 0, 0, 0, 0, 0, 95, 96, 0, 0,
	0, 0, 0, 0, 97, 109, 327, 111, 146, 145,
	148, 149, 150, 151, 152, 153, 154, 147, 0, 0,
	86, 0, 109, 140, 144, 142, 143, 145, 148, 149,
	150, 151, 152, 153, 154, 147, 306, 0, 0, 307,
	0, 0, 207, 207, 158, 159, 160, 161, 0, 155,
	156, 157, 0, 323, 284, 325, 146, 145, 148, 149,
	150, 151, 152, 153, 154, 147, 0, 0, 0, 0,
	141, 146, 145, 148, 149, 150, 151, 152, 153, 154,
	147, 146, 145, 148, 149, 150, 151, 152, 153, 154,
	147,
}
var yyPact = [...]int{

	307, -1000, -1000, 238, -1000, -1000, -1000, -1000, -1000, -1000,
	-1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, -7,
	-59, -16, -41, -18, -1000, -1000, -1000, 417, 382, -1000,
	-1000, -1000, 373, -1000, 320, 274, 409, 259, -64, -29,
	248, -1000, -27, 248, -1000, 274, -67, 297, -67, 274,
	-1000, -1000, -1000, -1000, -1000, 11, -1000, 217, 274, 308,
	23, -1000, 274, 131, -1000, 202, -1000, 22, -1000, 274,
	39, 293, -1000, -1000, 274, -1000, -55, 274, 364, 78,
	248, -1000, 206, -1000, -1000, 309, 20, 130, 602, -1000,
	34, 460, -1000, -1000, -1000, 525, 525, 525, 208, 208,
	-1000, 208, 208, -1000, -1000, -1000, -1000, -1000, -1000, -1000,
	-1000, 525, -1000, 240, 259, 274, 407, 259, 525, 248,
	-1000, 356, -70, -1000, 148, -1000, 274, -1000, -1000, 274,
	-1000, 246, 11, -1000, -1000, 248, 105, 34, 34, 525,
	291, 369, 525, 525, 119, 525, 525, 525, 525, 525,
	525, 525, 525, 525, 525, -1000, -1000, -1000, -1000, -1000,
	-1000, -1000, -1000, 602, -43, 59, -28, 602, -1000, 508,
	-1000, -1000, 421, 11, -1000, 417, 363, -5, 612, 358,
	259, 259, 262, -1000, 393, 34, -1000, 612, -1000, -1000,
	-1000, 74, 248, -1000, -61, -1000, -1000, -1000, -1000, -1000,
	-1000, -1000, -1000, 224, 171, 258, 296, 15, -1000, -1000,
	-1000, -1000, -1000, 50, 612, -1000, 508, -1000, -1000, 291,
	525, 525, 612, 587, -1000, 340, 281, 557, -1000, 134,
	134, 126, 126, 126, 14, 14, -1000, -1000, -1000, 525,
	-1000, 612, -1000, -35, 11, -35, 191, 12, -1000, 34,
	58, 208, 238, 129, -26, -1000, 393, 388, 391, 130,
	274, -1000, -1000, 274, -1000, 405, 246, 246, -1000, -1000,
	186, 180, 159, 145, 124, -8, -1000, 274, 71, 274,
	-28, -1000, 612, 539, 525, -1000, 612, -1000, -35, -1000,
	363, 3, -1000, 525, 102, -1000, 314, 190, -1000, -1000,
	-1000, 259, 388, -1000, 525, 525, -1000, -1000, 401, 390,
	171, 54, -1000, 108, -1000, 92, -1000, -1000, -1000, -1000,
	-30, -32, -51, -1000, -1000, -1000, -1000, 525, 612, -1000,
	-81, -1000, 612, 525, 312, 208, -1000, -1000, 400, 189,
	-1000, 249, -1000, 393, 34, 525, 34, -1000, -1000, 208,
	208, 208, 612, -1000, 612, 413, -1000, 525, 525, -1000,
	-1000, -1000, 388, 130, 163, 130, 248, 248, 248, 259,
	612, -1000, 348, -37, -1000, -37, -37, 131, -1000, 412,
	345, -1000, 248, -1000, -1000, -1000, 248, -1000, 248, -1000,
}
var yyPgo = [...]int{

	0, 537, 535, 15, 534, 530, 529, 527, 526, 525,
	519, 514, 511, 509, 504, 501, 490, 487, 25, 27,
	486, 484, 13, 483, 482, 192, 481, 5, 18, 67,
	479, 468, 466, 14, 2, 11, 1, 464, 10, 463,
	32, 7, 462, 461, 21, 455, 452, 451, 448, 6,
	444, 3, 443, 8, 442, 440, 437, 12, 9, 20,
	225, 436, 435, 430, 429, 428, 427, 0, 426, 375,
	424, 140, 423, 165, 4,
}
var yyR1 = [...]int{

	0, 1, 2, 2, 2, 2, 2, 2, 2, 2,
	2, 2, 2, 3, 3, 4, 4, 5, 6, 7,
	8, 8, 8, 9, 9, 9, 10, 11, 11, 11,
	12, 13, 13, 13, 72, 14, 15, 15, 16, 16,
	16, 16, 16, 17, 17, 18, 18, 19, 19, 19,
	20, 20, 68, 68, 68, 21, 21, 22, 22, 22,
	22, 70, 70, 70, 23, 23, 23, 23, 23, 23,
	23, 23, 23, 24, 24, 24, 25, 25, 26, 26,
	26, 26, 27, 27, 28, 28, 29, 29, 29, 29,
	29, 30, 30, 30, 30, 30, 30, 30, 30, 30,
	30, 30, 31, 31, 31, 31, 31, 31, 31, 35,
	35, 35, 40, 36, 36, 34, 34, 34, 34, 34,
	34, 34, 34, 34, 34, 34, 34, 34, 34, 34,
	34, 34, 34, 34, 34, 34, 39, 39, 42, 45,
	45, 43, 43, 44, 46, 46, 41, 41, 33, 33,
	33, 33, 47, 47, 48, 48, 49, 49, 50, 50,
	51, 52, 52, 52, 53, 53, 53, 54, 54, 54,
	55, 55, 56, 56, 57, 57, 32, 32, 37, 37,
	38, 38, 58, 58, 59, 60, 60, 61, 61, 62,
	62, 63, 63, 63, 63, 63, 64, 64, 65, 65,
	66, 66, 67, 69, 73, 74, 71,
}
var yyR2 = [...]int{

	0, 1, 1, 1, 1, 1, 1, 1, 1, 1,
	1, 1, 1, 12, 3, 7, 7, 8, 7, 3,
	5, 8, 4, 6, 7, 4, 5, 4, 5, 5,
	3, 2, 2, 2, 0, 2, 0, 2, 1, 2,
	1, 1, 1, 0, 1, 1, 3, 1, 2, 3,
	1, 1, 0, 1, 2, 1, 3, 3, 3, 3,
	5, 0, 1, 2, 1, 1, 2, 3, 2, 3,
	2, 2, 2, 1, 3, 1, 1, 3, 0, 5,
	5, 5, 1, 3, 0, 2, 1, 3, 3, 2,
	3, 3, 3, 4, 3, 4, 5, 6, 3, 4,
	2, 6, 1, 1, 1, 1, 1, 1, 1, 3,
	1, 1, 3, 1, 3, 1, 1, 1, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 2, 2,
	2, 3, 4, 5, 4, 1, 1, 1, 5, 0,
	1, 1, 2, 4, 0, 2, 1, 3, 1, 1,
	1, 1, 0, 3, 0, 2, 0, 3, 1, 3,
	2, 0, 1, 1, 0, 2, 4, 0, 2, 4,
	0, 3, 1, 3, 0, 5, 2, 1, 1, 3,
	3, 1, 1, 3, 3, 0, 2, 0, 3, 0,
	1, 1, 1, 1, 1, 1, 0, 1, 0, 1,
	0, 2, 1, 1, 1, 1, 0,
}
var yyChk = [...]int{

	-1000, -1, -2, -3, -4, -5, -6, -7, -8, -9,
	-10, -11, -12, -13, 5, 6, 7, 8, 33, 87,
	88, 90, 89, 91, 100, 101, 102, -16, 50, 51,
	52, 53, -14, -72, -14, -14, -14, -14, 92, -65,
	94, 98, -62, 94, 96, 92, 92, 93, 94, 92,
	-71, -71, -71, -3, 17, -17, 18, -15, 29, -25,
	-69, 36, 9, -58, -59, -41, -67, -69, 36, -61,
	97, 93, -67, 36, 92, -67, -69, -60, 97, 36,
	-60, -69, -18, -19, 73, -20, -69, -29, -34, -30,
	68, -73, -33, -41, -38, 71, 72, 79, -67, -39,
	-42, 20, 35, 46, 37, 38, 39, 25, -40, 97,
	28, 82, 41, -25, 33, 81, -25, 54, 47, 81,
	-69, 68, 36, -71, -69, -71, 95, -69, 20, 65,
	-67, 9, 54, -68, -67, 19, 81, 67, 66, -31,
	21, 68, 23, 24, 22, 70, 69, 78, 71, 72,
	73, 74, 75, 76, 77, 47, 48, 49, 42, 43,
	44, 45, -29, -34, -29, -3, -36, -34, -34, -73,
	-34, -34, -73, -73, -40, -73, -73, -45, -34, -55,
	33, -73, -58, -69, -28, 10, -59, -34, -67, -71,
	20, -66, 99, -63, 90, 88, 32, 89, 13, 36,
	-69, -69, -71, -21, -22, -24, -73, -69, -40, -19,
	-67, 73, -29, -29, -34, -35, -73, -40, 40, 21,
	23, 24, -34, -34, 25, 68, -34, -34, -34, -34,
	-34, -34, -34, -34, -34, -34, -74, 103, -74, 54,
	-74, -34, -74, -18, 18, -18, -33, -43, -44, 83,
	-32, 28, -3, -58, -56, -41, -28, -49, 13, -29,
	65, -67, -71, -64, 95, -28, 54, -23, 55, 56,
	57, 58, 59, 61, 62, -70, -69, 19, -22, 81,
	-36, -35, -34, -34, 67, 25, -34, -74, -18, -74,
	54, -46, -44, 85, -29, -57, 65, -37, -38, -57,
	-74, 54, -49, -53, 15, 14, -69, -69, -47, 11,
	-22, -22, 55, 60, 55, 60, 55, 55, 55, -26,
	63, 96, 64, -69, -74, -69, -74, 67, -34, -74,
	-33, 86, -34, 84, 30, 54, -41, -53, -34, -50,
	-51, -34, -71, -48, 12, 14, 65, 55, 55, 93,
	93, 93, -34, -74, -34, 31, -38, 54, 54, -52,
	26, 27, -49, -29, -36, -29, -73, -73, -73, 7,
	-34, -51, -53, -27, -67, -27, -27, -58, -54, 16,
	34, -74, 54, -74, -74, 7, 21, -67, -67, -67,
}
var yyDef = [...]int{

	0, -2, 1, 2, 3, 4, 5, 6, 7, 8,
	9, 10, 11, 12, 34, 34, 34, 34, 34, 198,
	189, 0, 0, 0, 206, 206, 206, 0, 38, 40,
	41, 42, 43, 36, 0, 0, 0, 0, 187, 0,
	0, 199, 0, 0, 190, 0, 185, 0, 185, 0,
	31, 32, 33, 14, 39, 0, 44, 35, 0, 0,
	76, 203, 0, 19, 182, 0, 146, 0, -2, 0,
	0, 0, 206, 202, 0, 206, 0, 0, 0, 0,
	0, 30, 0, 45, 47, 52, 0, 50, 51, 86,
	0, 0, 115, 116, 117, 0, 0, 0, 146, 0,
	135, 0, 0, 204, 148, 149, 150, 151, 181, 136,
	137, 139, 37, 170, 0, 0, 84, 0, 0, 0,
	206, 0, 200, 22, 0, 25, 0, 27, 186, 0,
	206, 0, 0, 48, 53, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 102, 103, 104, 105, 106,
	107, 108, 89, 0, 0, 0, 0, 113, 128, 0,
	129, 130, 0, 0, 100, 0, 0, 0, 140, 0,
	0, 0, 84, 77, 156, 0, 183, 184, 147, 20,
	188, 0, 0, 206, 196, 191, 192, 193, 194, 195,
	26, 28, 29, 84, 55, 61, 0, 73, 75, 46,
	54, 49, 87, 88, 91, 92, 0, 110, 111, 0,
	0, 0, 94, 0, 98, 0, 118, 119, 120, 121,
	122, 123, 124, 125, 126, 127, 90, 205, 112, 0,
	180, 113, 131, 0, 0, 0, 0, 144, 141, 0,
	174, 0, 177, 174, 0, 172, 156, 164, 0, 85,
	0, 201, 23, 0, 197, 152, 0, 0, 64, 65,
	0, 0, 0, 0, 0, 78, 62, 0, 0, 0,
	0, 93, 95, 0, 0, 99, 114, 132, 0, 134,
	0, 0, 142, 0, 0, 15, 0, 176, 178, 16,
	171, 0, 164, 18, 0, 0, 206, 24, 154, 0,
	56, 59, 66, 0, 68, 0, 70, 71, 72, 57,
	0, 0, 0, 63, 58, 74, 109, 0, 96, 133,
	0, 138, 145, 0, 0, 0, 173, 17, 165, 157,
	158, 161, 21, 156, 0, 0, 0, 67, 69, 0,
	0, 0, 97, 101, 143, 0, 179, 0, 0, 160,
	162, 163, 164, 155, 153, 60, 0, 0, 0, 0,
	166, 159, 167, 0, 82, 0, 0, 175, 13, 0,
	0, 79, 0, 80, 81, 168, 0, 83, 0, 169,
}
var yyTok1 = [...]int{

	1, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 75, 70, 3,
	46, 103, 73, 71, 54, 72, 81, 74, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	48, 47, 49, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 78, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 69, 3, 79,
}
var yyTok2 = [...]int{

	2, 3, 4, 5, 6, 7, 8, 9, 10, 11,
	12, 13, 14, 15, 16, 17, 18, 19, 20, 21,
	22, 23, 24, 25, 26, 27, 28, 29, 30, 31,
	32, 33, 34, 35, 36, 37, 38, 39, 40, 41,
	42, 43, 44, 45, 50, 51, 52, 53, 55, 56,
	57, 58, 59, 60, 61, 62, 63, 64, 65, 66,
	67, 68, 76, 77, 80, 82, 83, 84, 85, 86,
	87, 88, 89, 90, 91, 92, 93, 94, 95, 96,
	97, 98, 99, 100, 101, 102,
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
		//line sql.y:160
		{
			setParseTree(yylex, yyDollar[1].statement)
		}
	case 2:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:166
		{
			yyVAL.statement = yyDollar[1].selStmt
		}
	case 13:
		yyDollar = yyS[yypt-12 : yypt+1]
		//line sql.y:182
		{
			yyVAL.selStmt = &Select{Comments: Comments(yyDollar[2].bytes2), Distinct: yyDollar[3].str, SelectExprs: yyDollar[4].selectExprs, From: yyDollar[6].tableExprs, Where: NewWhere(AST_WHERE, yyDollar[7].boolExpr), GroupBy: GroupBy(yyDollar[8].valExprs), Having: NewWhere(AST_HAVING, yyDollar[9].boolExpr), OrderBy: yyDollar[10].orderBy, Limit: yyDollar[11].limit, Lock: yyDollar[12].str}
		}
	case 14:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:186
		{
			yyVAL.selStmt = &Union{Type: yyDollar[2].str, Left: yyDollar[1].selStmt, Right: yyDollar[3].selStmt}
		}
	case 15:
		yyDollar = yyS[yypt-7 : yypt+1]
		//line sql.y:192
		{
			yyVAL.statement = &Insert{Comments: Comments(yyDollar[2].bytes2), Table: yyDollar[4].tableName, Columns: yyDollar[5].columns, Rows: yyDollar[6].insRows, OnDup: OnDup(yyDollar[7].updateExprs)}
		}
	case 16:
		yyDollar = yyS[yypt-7 : yypt+1]
		//line sql.y:196
		{
			cols := make(Columns, 0, len(yyDollar[6].updateExprs))
			vals := make(ValTuple, 0, len(yyDollar[6].updateExprs))
			for _, col := range yyDollar[6].updateExprs {
				cols = append(cols, &NonStarExpr{Expr: col.Name})
				vals = append(vals, col.Expr)
			}
			yyVAL.statement = &Insert{Comments: Comments(yyDollar[2].bytes2), Table: yyDollar[4].tableName, Columns: cols, Rows: Values{vals}, OnDup: OnDup(yyDollar[7].updateExprs)}
		}
	case 17:
		yyDollar = yyS[yypt-8 : yypt+1]
		//line sql.y:208
		{
			yyVAL.statement = &Update{Comments: Comments(yyDollar[2].bytes2), Table: yyDollar[3].tableName, Exprs: yyDollar[5].updateExprs, Where: NewWhere(AST_WHERE, yyDollar[6].boolExpr), OrderBy: yyDollar[7].orderBy, Limit: yyDollar[8].limit}
		}
	case 18:
		yyDollar = yyS[yypt-7 : yypt+1]
		//line sql.y:214
		{
			yyVAL.statement = &Delete{Comments: Comments(yyDollar[2].bytes2), Table: yyDollar[4].tableName, Where: NewWhere(AST_WHERE, yyDollar[5].boolExpr), OrderBy: yyDollar[6].orderBy, Limit: yyDollar[7].limit}
		}
	case 19:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:220
		{
			yyVAL.statement = &Set{Comments: Comments(yyDollar[2].bytes2), Exprs: yyDollar[3].updateExprs}
		}
	case 20:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:226
		{
			yyVAL.statement = &DDL{Action: AST_CREATE, NewName: yyDollar[4].sqlID}
		}
	case 21:
		yyDollar = yyS[yypt-8 : yypt+1]
		//line sql.y:230
		{
			// Change this to an alter statement
			yyVAL.statement = &DDL{Action: AST_ALTER, Table: yyDollar[7].sqlID, NewName: yyDollar[7].sqlID}
		}
	case 22:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:235
		{
			yyVAL.statement = &DDL{Action: AST_CREATE, NewName: SQLName(yyDollar[3].sqlID)}
		}
	case 23:
		yyDollar = yyS[yypt-6 : yypt+1]
		//line sql.y:241
		{
			yyVAL.statement = &DDL{Action: AST_ALTER, Table: yyDollar[4].sqlID, NewName: yyDollar[4].sqlID}
		}
	case 24:
		yyDollar = yyS[yypt-7 : yypt+1]
		//line sql.y:245
		{
			// Change this to a rename statement
			yyVAL.statement = &DDL{Action: AST_RENAME, Table: yyDollar[4].sqlID, NewName: yyDollar[7].sqlID}
		}
	case 25:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:250
		{
			yyVAL.statement = &DDL{Action: AST_ALTER, Table: SQLName(yyDollar[3].sqlID), NewName: SQLName(yyDollar[3].sqlID)}
		}
	case 26:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:256
		{
			yyVAL.statement = &DDL{Action: AST_RENAME, Table: yyDollar[3].sqlID, NewName: yyDollar[5].sqlID}
		}
	case 27:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:262
		{
			yyVAL.statement = &DDL{Action: AST_DROP, Table: yyDollar[4].sqlID}
		}
	case 28:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:266
		{
			// Change this to an alter statement
			yyVAL.statement = &DDL{Action: AST_ALTER, Table: yyDollar[5].sqlID, NewName: yyDollar[5].sqlID}
		}
	case 29:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:271
		{
			yyVAL.statement = &DDL{Action: AST_DROP, Table: SQLName(yyDollar[4].sqlID)}
		}
	case 30:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:277
		{
			yyVAL.statement = &DDL{Action: AST_ALTER, Table: yyDollar[3].sqlID, NewName: yyDollar[3].sqlID}
		}
	case 31:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:283
		{
			yyVAL.statement = &Other{}
		}
	case 32:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:287
		{
			yyVAL.statement = &Other{}
		}
	case 33:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:291
		{
			yyVAL.statement = &Other{}
		}
	case 34:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:296
		{
			setAllowComments(yylex, true)
		}
	case 35:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:300
		{
			yyVAL.bytes2 = yyDollar[2].bytes2
			setAllowComments(yylex, false)
		}
	case 36:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:306
		{
			yyVAL.bytes2 = nil
		}
	case 37:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:310
		{
			yyVAL.bytes2 = append(yyDollar[1].bytes2, yyDollar[2].bytes)
		}
	case 38:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:316
		{
			yyVAL.str = AST_UNION
		}
	case 39:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:320
		{
			yyVAL.str = AST_UNION_ALL
		}
	case 40:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:324
		{
			yyVAL.str = AST_SET_MINUS
		}
	case 41:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:328
		{
			yyVAL.str = AST_EXCEPT
		}
	case 42:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:332
		{
			yyVAL.str = AST_INTERSECT
		}
	case 43:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:337
		{
			yyVAL.str = ""
		}
	case 44:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:341
		{
			yyVAL.str = AST_DISTINCT
		}
	case 45:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:347
		{
			yyVAL.selectExprs = SelectExprs{yyDollar[1].selectExpr}
		}
	case 46:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:351
		{
			yyVAL.selectExprs = append(yyVAL.selectExprs, yyDollar[3].selectExpr)
		}
	case 47:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:357
		{
			yyVAL.selectExpr = &StarExpr{}
		}
	case 48:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:361
		{
			yyVAL.selectExpr = &NonStarExpr{Expr: yyDollar[1].expr, As: yyDollar[2].sqlID}
		}
	case 49:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:365
		{
			yyVAL.selectExpr = &StarExpr{TableName: yyDollar[1].sqlID}
		}
	case 50:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:371
		{
			yyVAL.expr = yyDollar[1].boolExpr
		}
	case 51:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:375
		{
			yyVAL.expr = yyDollar[1].valExpr
		}
	case 52:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:380
		{
			yyVAL.sqlID = ""
		}
	case 53:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:384
		{
			yyVAL.sqlID = yyDollar[1].sqlID
		}
	case 54:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:388
		{
			yyVAL.sqlID = yyDollar[2].sqlID
		}
	case 55:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:394
		{
			yyVAL.tableExprs = TableExprs{yyDollar[1].tableExpr}
		}
	case 56:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:398
		{
			yyVAL.tableExprs = append(yyVAL.tableExprs, yyDollar[3].tableExpr)
		}
	case 57:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:404
		{
			yyVAL.tableExpr = &AliasedTableExpr{Expr: yyDollar[1].smTableExpr, As: yyDollar[2].sqlID, Hints: yyDollar[3].indexHints}
		}
	case 58:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:408
		{
			yyVAL.tableExpr = &ParenTableExpr{Expr: yyDollar[2].tableExpr}
		}
	case 59:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:412
		{
			yyVAL.tableExpr = &JoinTableExpr{LeftExpr: yyDollar[1].tableExpr, Join: yyDollar[2].str, RightExpr: yyDollar[3].tableExpr}
		}
	case 60:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:416
		{
			yyVAL.tableExpr = &JoinTableExpr{LeftExpr: yyDollar[1].tableExpr, Join: yyDollar[2].str, RightExpr: yyDollar[3].tableExpr, On: yyDollar[5].boolExpr}
		}
	case 61:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:421
		{
			yyVAL.sqlID = ""
		}
	case 62:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:425
		{
			yyVAL.sqlID = yyDollar[1].sqlID
		}
	case 63:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:429
		{
			yyVAL.sqlID = yyDollar[2].sqlID
		}
	case 64:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:435
		{
			yyVAL.str = AST_JOIN
		}
	case 65:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:439
		{
			yyVAL.str = AST_STRAIGHT_JOIN
		}
	case 66:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:443
		{
			yyVAL.str = AST_LEFT_JOIN
		}
	case 67:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:447
		{
			yyVAL.str = AST_LEFT_JOIN
		}
	case 68:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:451
		{
			yyVAL.str = AST_RIGHT_JOIN
		}
	case 69:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:455
		{
			yyVAL.str = AST_RIGHT_JOIN
		}
	case 70:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:459
		{
			yyVAL.str = AST_JOIN
		}
	case 71:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:463
		{
			yyVAL.str = AST_CROSS_JOIN
		}
	case 72:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:467
		{
			yyVAL.str = AST_NATURAL_JOIN
		}
	case 73:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:473
		{
			yyVAL.smTableExpr = &TableName{Name: yyDollar[1].sqlID}
		}
	case 74:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:477
		{
			yyVAL.smTableExpr = &TableName{Qualifier: yyDollar[1].sqlID, Name: yyDollar[3].sqlID}
		}
	case 75:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:481
		{
			yyVAL.smTableExpr = yyDollar[1].subquery
		}
	case 76:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:487
		{
			yyVAL.tableName = &TableName{Name: yyDollar[1].sqlID}
		}
	case 77:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:491
		{
			yyVAL.tableName = &TableName{Qualifier: yyDollar[1].sqlID, Name: yyDollar[3].sqlID}
		}
	case 78:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:496
		{
			yyVAL.indexHints = nil
		}
	case 79:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:500
		{
			yyVAL.indexHints = &IndexHints{Type: AST_USE, Indexes: yyDollar[4].sqlIDs}
		}
	case 80:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:504
		{
			yyVAL.indexHints = &IndexHints{Type: AST_IGNORE, Indexes: yyDollar[4].sqlIDs}
		}
	case 81:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:508
		{
			yyVAL.indexHints = &IndexHints{Type: AST_FORCE, Indexes: yyDollar[4].sqlIDs}
		}
	case 82:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:514
		{
			yyVAL.sqlIDs = []SQLName{yyDollar[1].sqlID}
		}
	case 83:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:518
		{
			yyVAL.sqlIDs = append(yyDollar[1].sqlIDs, yyDollar[3].sqlID)
		}
	case 84:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:523
		{
			yyVAL.boolExpr = nil
		}
	case 85:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:527
		{
			yyVAL.boolExpr = yyDollar[2].boolExpr
		}
	case 87:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:534
		{
			yyVAL.boolExpr = &AndExpr{Left: yyDollar[1].boolExpr, Right: yyDollar[3].boolExpr}
		}
	case 88:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:538
		{
			yyVAL.boolExpr = &OrExpr{Left: yyDollar[1].boolExpr, Right: yyDollar[3].boolExpr}
		}
	case 89:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:542
		{
			yyVAL.boolExpr = &NotExpr{Expr: yyDollar[2].boolExpr}
		}
	case 90:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:546
		{
			yyVAL.boolExpr = &ParenBoolExpr{Expr: yyDollar[2].boolExpr}
		}
	case 91:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:552
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: yyDollar[2].str, Right: yyDollar[3].valExpr}
		}
	case 92:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:556
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: AST_IN, Right: yyDollar[3].colTuple}
		}
	case 93:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:560
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: AST_NOT_IN, Right: yyDollar[4].colTuple}
		}
	case 94:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:564
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: AST_LIKE, Right: yyDollar[3].valExpr}
		}
	case 95:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:568
		{
			yyVAL.boolExpr = &ComparisonExpr{Left: yyDollar[1].valExpr, Operator: AST_NOT_LIKE, Right: yyDollar[4].valExpr}
		}
	case 96:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:572
		{
			yyVAL.boolExpr = &RangeCond{Left: yyDollar[1].valExpr, Operator: AST_BETWEEN, From: yyDollar[3].valExpr, To: yyDollar[5].valExpr}
		}
	case 97:
		yyDollar = yyS[yypt-6 : yypt+1]
		//line sql.y:576
		{
			yyVAL.boolExpr = &RangeCond{Left: yyDollar[1].valExpr, Operator: AST_NOT_BETWEEN, From: yyDollar[4].valExpr, To: yyDollar[6].valExpr}
		}
	case 98:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:580
		{
			yyVAL.boolExpr = &NullCheck{Operator: AST_IS_NULL, Expr: yyDollar[1].valExpr}
		}
	case 99:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:584
		{
			yyVAL.boolExpr = &NullCheck{Operator: AST_IS_NOT_NULL, Expr: yyDollar[1].valExpr}
		}
	case 100:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:588
		{
			yyVAL.boolExpr = &ExistsExpr{Subquery: yyDollar[2].subquery}
		}
	case 101:
		yyDollar = yyS[yypt-6 : yypt+1]
		//line sql.y:592
		{
			yyVAL.boolExpr = &KeyrangeExpr{Start: yyDollar[3].valExpr, End: yyDollar[5].valExpr}
		}
	case 102:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:598
		{
			yyVAL.str = AST_EQ
		}
	case 103:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:602
		{
			yyVAL.str = AST_LT
		}
	case 104:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:606
		{
			yyVAL.str = AST_GT
		}
	case 105:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:610
		{
			yyVAL.str = AST_LE
		}
	case 106:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:614
		{
			yyVAL.str = AST_GE
		}
	case 107:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:618
		{
			yyVAL.str = AST_NE
		}
	case 108:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:622
		{
			yyVAL.str = AST_NSE
		}
	case 109:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:628
		{
			yyVAL.colTuple = ValTuple(yyDollar[2].valExprs)
		}
	case 110:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:632
		{
			yyVAL.colTuple = yyDollar[1].subquery
		}
	case 111:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:636
		{
			yyVAL.colTuple = ListArg(yyDollar[1].bytes)
		}
	case 112:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:642
		{
			yyVAL.subquery = &Subquery{yyDollar[2].selStmt}
		}
	case 113:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:648
		{
			yyVAL.valExprs = ValExprs{yyDollar[1].valExpr}
		}
	case 114:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:652
		{
			yyVAL.valExprs = append(yyDollar[1].valExprs, yyDollar[3].valExpr)
		}
	case 115:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:658
		{
			yyVAL.valExpr = yyDollar[1].valExpr
		}
	case 116:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:662
		{
			yyVAL.valExpr = yyDollar[1].colName
		}
	case 117:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:666
		{
			yyVAL.valExpr = yyDollar[1].rowTuple
		}
	case 118:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:670
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: AST_BITAND, Right: yyDollar[3].valExpr}
		}
	case 119:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:674
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: AST_BITOR, Right: yyDollar[3].valExpr}
		}
	case 120:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:678
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: AST_BITXOR, Right: yyDollar[3].valExpr}
		}
	case 121:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:682
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: AST_PLUS, Right: yyDollar[3].valExpr}
		}
	case 122:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:686
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: AST_MINUS, Right: yyDollar[3].valExpr}
		}
	case 123:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:690
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: AST_MULT, Right: yyDollar[3].valExpr}
		}
	case 124:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:694
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: AST_DIV, Right: yyDollar[3].valExpr}
		}
	case 125:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:698
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: AST_MOD, Right: yyDollar[3].valExpr}
		}
	case 126:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:702
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: AST_SHIFT_LEFT, Right: yyDollar[3].valExpr}
		}
	case 127:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:706
		{
			yyVAL.valExpr = &BinaryExpr{Left: yyDollar[1].valExpr, Operator: AST_SHIFT_RIGHT, Right: yyDollar[3].valExpr}
		}
	case 128:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:710
		{
			if num, ok := yyDollar[2].valExpr.(NumVal); ok {
				yyVAL.valExpr = num
			} else {
				yyVAL.valExpr = &UnaryExpr{Operator: AST_UPLUS, Expr: yyDollar[2].valExpr}
			}
		}
	case 129:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:718
		{
			if num, ok := yyDollar[2].valExpr.(NumVal); ok {
				// Handle double negative
				if num[0] == '-' {
					yyVAL.valExpr = num[1:]
				} else {
					yyVAL.valExpr = append(NumVal("-"), num...)
				}
			} else {
				yyVAL.valExpr = &UnaryExpr{Operator: AST_UMINUS, Expr: yyDollar[2].valExpr}
			}
		}
	case 130:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:731
		{
			yyVAL.valExpr = &UnaryExpr{Operator: AST_TILDA, Expr: yyDollar[2].valExpr}
		}
	case 131:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:735
		{
			yyVAL.valExpr = &FuncExpr{Name: string(yyDollar[1].sqlID)}
		}
	case 132:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:739
		{
			yyVAL.valExpr = &FuncExpr{Name: string(yyDollar[1].sqlID), Exprs: yyDollar[3].selectExprs}
		}
	case 133:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:743
		{
			yyVAL.valExpr = &FuncExpr{Name: string(yyDollar[1].sqlID), Distinct: true, Exprs: yyDollar[4].selectExprs}
		}
	case 134:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:747
		{
			yyVAL.valExpr = &FuncExpr{Name: yyDollar[1].str, Exprs: yyDollar[3].selectExprs}
		}
	case 135:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:751
		{
			yyVAL.valExpr = yyDollar[1].caseExpr
		}
	case 136:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:757
		{
			yyVAL.str = "if"
		}
	case 137:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:761
		{
			yyVAL.str = "values"
		}
	case 138:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:767
		{
			yyVAL.caseExpr = &CaseExpr{Expr: yyDollar[2].valExpr, Whens: yyDollar[3].whens, Else: yyDollar[4].valExpr}
		}
	case 139:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:772
		{
			yyVAL.valExpr = nil
		}
	case 140:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:776
		{
			yyVAL.valExpr = yyDollar[1].valExpr
		}
	case 141:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:782
		{
			yyVAL.whens = []*When{yyDollar[1].when}
		}
	case 142:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:786
		{
			yyVAL.whens = append(yyDollar[1].whens, yyDollar[2].when)
		}
	case 143:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:792
		{
			yyVAL.when = &When{Cond: yyDollar[2].boolExpr, Val: yyDollar[4].valExpr}
		}
	case 144:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:797
		{
			yyVAL.valExpr = nil
		}
	case 145:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:801
		{
			yyVAL.valExpr = yyDollar[2].valExpr
		}
	case 146:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:807
		{
			yyVAL.colName = &ColName{Name: yyDollar[1].sqlID}
		}
	case 147:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:811
		{
			yyVAL.colName = &ColName{Qualifier: yyDollar[1].sqlID, Name: yyDollar[3].sqlID}
		}
	case 148:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:817
		{
			yyVAL.valExpr = StrVal(yyDollar[1].bytes)
		}
	case 149:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:821
		{
			yyVAL.valExpr = NumVal(yyDollar[1].bytes)
		}
	case 150:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:825
		{
			yyVAL.valExpr = ValArg(yyDollar[1].bytes)
		}
	case 151:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:829
		{
			yyVAL.valExpr = &NullVal{}
		}
	case 152:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:834
		{
			yyVAL.valExprs = nil
		}
	case 153:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:838
		{
			yyVAL.valExprs = yyDollar[3].valExprs
		}
	case 154:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:843
		{
			yyVAL.boolExpr = nil
		}
	case 155:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:847
		{
			yyVAL.boolExpr = yyDollar[2].boolExpr
		}
	case 156:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:852
		{
			yyVAL.orderBy = nil
		}
	case 157:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:856
		{
			yyVAL.orderBy = yyDollar[3].orderBy
		}
	case 158:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:862
		{
			yyVAL.orderBy = OrderBy{yyDollar[1].order}
		}
	case 159:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:866
		{
			yyVAL.orderBy = append(yyDollar[1].orderBy, yyDollar[3].order)
		}
	case 160:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:872
		{
			yyVAL.order = &Order{Expr: yyDollar[1].valExpr, Direction: yyDollar[2].str}
		}
	case 161:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:877
		{
			yyVAL.str = AST_ASC
		}
	case 162:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:881
		{
			yyVAL.str = AST_ASC
		}
	case 163:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:885
		{
			yyVAL.str = AST_DESC
		}
	case 164:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:890
		{
			yyVAL.limit = nil
		}
	case 165:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:894
		{
			yyVAL.limit = &Limit{Rowcount: yyDollar[2].valExpr}
		}
	case 166:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:898
		{
			yyVAL.limit = &Limit{Offset: yyDollar[2].valExpr, Rowcount: yyDollar[4].valExpr}
		}
	case 167:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:903
		{
			yyVAL.str = ""
		}
	case 168:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:907
		{
			yyVAL.str = AST_FOR_UPDATE
		}
	case 169:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:911
		{
			if yyDollar[3].sqlID != "share" {
				yylex.Error("expecting share")
				return 1
			}
			if yyDollar[4].sqlID != "mode" {
				yylex.Error("expecting mode")
				return 1
			}
			yyVAL.str = AST_SHARE_MODE
		}
	case 170:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:924
		{
			yyVAL.columns = nil
		}
	case 171:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:928
		{
			yyVAL.columns = yyDollar[2].columns
		}
	case 172:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:934
		{
			yyVAL.columns = Columns{&NonStarExpr{Expr: yyDollar[1].colName}}
		}
	case 173:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:938
		{
			yyVAL.columns = append(yyVAL.columns, &NonStarExpr{Expr: yyDollar[3].colName})
		}
	case 174:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:943
		{
			yyVAL.updateExprs = nil
		}
	case 175:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:947
		{
			yyVAL.updateExprs = yyDollar[5].updateExprs
		}
	case 176:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:953
		{
			yyVAL.insRows = yyDollar[2].values
		}
	case 177:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:957
		{
			yyVAL.insRows = yyDollar[1].selStmt
		}
	case 178:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:963
		{
			yyVAL.values = Values{yyDollar[1].rowTuple}
		}
	case 179:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:967
		{
			yyVAL.values = append(yyDollar[1].values, yyDollar[3].rowTuple)
		}
	case 180:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:973
		{
			yyVAL.rowTuple = ValTuple(yyDollar[2].valExprs)
		}
	case 181:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:977
		{
			yyVAL.rowTuple = yyDollar[1].subquery
		}
	case 182:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:983
		{
			yyVAL.updateExprs = UpdateExprs{yyDollar[1].updateExpr}
		}
	case 183:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:987
		{
			yyVAL.updateExprs = append(yyDollar[1].updateExprs, yyDollar[3].updateExpr)
		}
	case 184:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:993
		{
			yyVAL.updateExpr = &UpdateExpr{Name: yyDollar[1].colName, Expr: yyDollar[3].valExpr}
		}
	case 185:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:998
		{
			yyVAL.empty = struct{}{}
		}
	case 186:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:1000
		{
			yyVAL.empty = struct{}{}
		}
	case 187:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1003
		{
			yyVAL.empty = struct{}{}
		}
	case 188:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1005
		{
			yyVAL.empty = struct{}{}
		}
	case 189:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1008
		{
			yyVAL.empty = struct{}{}
		}
	case 190:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1010
		{
			yyVAL.empty = struct{}{}
		}
	case 191:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1014
		{
			yyVAL.empty = struct{}{}
		}
	case 192:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1016
		{
			yyVAL.empty = struct{}{}
		}
	case 193:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1018
		{
			yyVAL.empty = struct{}{}
		}
	case 194:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1020
		{
			yyVAL.empty = struct{}{}
		}
	case 195:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1022
		{
			yyVAL.empty = struct{}{}
		}
	case 196:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1025
		{
			yyVAL.empty = struct{}{}
		}
	case 197:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1027
		{
			yyVAL.empty = struct{}{}
		}
	case 198:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1030
		{
			yyVAL.empty = struct{}{}
		}
	case 199:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1032
		{
			yyVAL.empty = struct{}{}
		}
	case 200:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1035
		{
			yyVAL.empty = struct{}{}
		}
	case 201:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:1037
		{
			yyVAL.empty = struct{}{}
		}
	case 202:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1041
		{
			yyVAL.sqlID = SQLName(strings.ToLower(string(yyDollar[1].bytes)))
		}
	case 203:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1047
		{
			yyVAL.sqlID = SQLName(yyDollar[1].bytes)
		}
	case 204:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1053
		{
			if incNesting(yylex) {
				yylex.Error("max nesting level reached")
				return 1
			}
		}
	case 205:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1062
		{
			decNesting(yylex)
		}
	case 206:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1067
		{
			forceEOF(yylex)
		}
	}
	goto yystack /* stack new state and value */
}
