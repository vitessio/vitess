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
	93, 241,
	-2, 240,
}

const yyNprod = 245
const yyPrivate = 57344

var yyTokenNames []string
var yyStates []string

const yyLast = 987

var yyAct = [...]int{

	121, 107, 282, 430, 62, 361, 305, 115, 209, 228,
	350, 325, 116, 47, 296, 242, 315, 259, 240, 253,
	102, 208, 3, 113, 172, 156, 244, 241, 74, 67,
	147, 103, 381, 383, 162, 64, 41, 237, 69, 48,
	49, 71, 35, 40, 37, 41, 97, 160, 38, 50,
	14, 15, 16, 17, 413, 81, 43, 44, 45, 58,
	412, 411, 68, 70, 46, 42, 211, 212, 164, 363,
	332, 231, 18, 152, 176, 87, 189, 101, 192, 193,
	194, 195, 189, 94, 65, 96, 436, 64, 88, 177,
	64, 145, 63, 109, 292, 382, 126, 190, 191, 192,
	193, 194, 195, 189, 179, 153, 174, 179, 90, 393,
	297, 92, 144, 205, 207, 167, 84, 159, 161, 158,
	127, 387, 283, 109, 129, 128, 130, 131, 132, 65,
	248, 133, 124, 125, 163, 297, 111, 348, 138, 219,
	19, 20, 22, 21, 23, 178, 177, 60, 178, 177,
	230, 395, 112, 24, 25, 26, 283, 234, 117, 118,
	104, 179, 109, 137, 179, 119, 223, 120, 178, 177,
	60, 235, 328, 93, 174, 266, 227, 250, 75, 251,
	252, 134, 149, 245, 179, 112, 112, 135, 136, 264,
	265, 263, 28, 170, 261, 217, 218, 89, 247, 238,
	220, 239, 254, 256, 257, 127, 270, 255, 333, 334,
	335, 284, 286, 65, 262, 175, 285, 289, 226, 326,
	290, 293, 112, 438, 283, 400, 64, 303, 65, 169,
	301, 82, 250, 83, 283, 287, 288, 304, 291, 294,
	173, 397, 127, 112, 246, 60, 300, 60, 351, 328,
	89, 112, 112, 313, 14, 260, 169, 283, 225, 245,
	129, 128, 130, 131, 132, 331, 148, 133, 65, 336,
	175, 285, 283, 313, 283, 127, 261, 357, 283, 127,
	407, 337, 196, 197, 190, 191, 192, 193, 194, 195,
	189, 112, 112, 127, 343, 351, 60, 345, 73, 349,
	233, 89, 100, 376, 374, 356, 79, 353, 377, 375,
	347, 358, 355, 344, 410, 317, 320, 321, 322, 318,
	246, 319, 323, 364, 409, 206, 245, 245, 245, 245,
	369, 373, 371, 388, 386, 402, 403, 260, 384, 389,
	379, 368, 372, 370, 76, 39, 14, 392, 187, 196,
	197, 190, 191, 192, 193, 194, 195, 189, 398, 378,
	428, 321, 322, 143, 388, 55, 112, 142, 405, 86,
	299, 112, 429, 354, 416, 404, 406, 57, 54, 396,
	85, 154, 99, 330, 51, 52, 306, 246, 246, 246,
	246, 367, 188, 187, 196, 197, 190, 191, 192, 193,
	194, 195, 189, 29, 108, 417, 419, 307, 141, 421,
	422, 420, 312, 229, 399, 140, 150, 366, 148, 31,
	32, 33, 34, 61, 431, 431, 431, 64, 432, 433,
	435, 434, 426, 437, 14, 439, 440, 441, 210, 442,
	112, 28, 443, 213, 214, 215, 216, 188, 187, 196,
	197, 190, 191, 192, 193, 194, 195, 189, 30, 317,
	320, 321, 322, 318, 222, 319, 323, 1, 329, 408,
	324, 354, 171, 155, 36, 236, 157, 66, 232, 139,
	302, 112, 112, 224, 427, 423, 424, 425, 401, 360,
	122, 365, 311, 346, 221, 108, 295, 123, 114, 151,
	352, 80, 298, 180, 110, 380, 258, 316, 314, 267,
	268, 269, 243, 271, 272, 273, 274, 275, 276, 277,
	278, 279, 280, 281, 168, 105, 78, 53, 390, 27,
	56, 13, 59, 12, 11, 10, 9, 8, 7, 6,
	5, 4, 72, 108, 108, 2, 77, 188, 187, 196,
	197, 190, 191, 192, 193, 194, 195, 189, 0, 0,
	0, 59, 0, 0, 0, 0, 91, 0, 0, 0,
	95, 0, 0, 98, 0, 0, 0, 0, 106, 0,
	0, 0, 0, 0, 0, 59, 232, 146, 0, 0,
	338, 339, 340, 0, 0, 0, 0, 165, 0, 0,
	166, 341, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 342, 0, 0, 0, 0, 0, 0, 108, 126,
	188, 187, 196, 197, 190, 191, 192, 193, 194, 195,
	189, 0, 359, 362, 0, 0, 0, 0, 0, 59,
	0, 0, 0, 127, 0, 283, 109, 129, 128, 130,
	131, 132, 0, 0, 133, 124, 125, 0, 0, 111,
	0, 138, 0, 0, 0, 0, 0, 391, 0, 106,
	59, 0, 0, 0, 394, 0, 249, 0, 0, 0,
	232, 117, 118, 104, 0, 0, 137, 0, 119, 0,
	120, 0, 0, 232, 0, 14, 0, 0, 0, 0,
	0, 0, 0, 0, 134, 0, 0, 0, 0, 0,
	135, 136, 0, 0, 0, 0, 414, 106, 106, 0,
	0, 415, 0, 0, 0, 418, 362, 126, 0, 0,
	0, 308, 0, 309, 127, 0, 310, 109, 129, 128,
	130, 131, 132, 0, 327, 133, 59, 0, 0, 0,
	0, 127, 138, 0, 109, 129, 128, 130, 131, 132,
	0, 0, 133, 124, 125, 0, 0, 111, 0, 138,
	0, 0, 117, 118, 0, 0, 0, 137, 0, 119,
	0, 120, 0, 0, 0, 0, 0, 0, 14, 117,
	118, 104, 106, 0, 137, 134, 119, 0, 120, 0,
	0, 135, 136, 126, 0, 0, 0, 0, 0, 0,
	0, 0, 134, 59, 59, 59, 59, 0, 135, 136,
	126, 0, 0, 0, 0, 0, 327, 127, 0, 385,
	109, 129, 128, 130, 131, 132, 0, 0, 133, 124,
	125, 0, 0, 111, 127, 138, 0, 109, 129, 128,
	130, 131, 132, 0, 0, 133, 124, 125, 0, 0,
	111, 0, 138, 0, 0, 117, 118, 0, 0, 0,
	137, 0, 119, 0, 120, 0, 0, 0, 0, 0,
	0, 0, 117, 118, 65, 0, 0, 137, 134, 119,
	0, 120, 0, 0, 135, 136, 127, 0, 0, 109,
	129, 128, 130, 131, 132, 134, 0, 133, 0, 0,
	0, 135, 136, 0, 138, 188, 187, 196, 197, 190,
	191, 192, 193, 194, 195, 189, 0, 0, 0, 0,
	0, 0, 0, 0, 117, 118, 0, 0, 0, 137,
	0, 119, 0, 120, 188, 187, 196, 197, 190, 191,
	192, 193, 194, 195, 189, 0, 0, 134, 182, 185,
	0, 0, 0, 135, 136, 198, 199, 200, 201, 202,
	203, 204, 186, 183, 184, 181, 188, 187, 196, 197,
	190, 191, 192, 193, 194, 195, 189,
}
var yyPact = [...]int{

	44, -1000, -1000, 436, -1000, -1000, -1000, -1000, -1000, -1000,
	-1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, -59,
	-60, -36, -45, -37, -1000, -1000, -1000, 428, 366, 346,
	-1000, -69, 99, 413, 81, -77, -40, 81, -1000, -38,
	81, -1000, 99, -78, 130, -78, 99, -1000, -1000, -1000,
	-1000, -1000, -1000, 271, 180, -1000, 61, 356, 341, -18,
	-1000, 99, 151, -1000, 40, -1000, 99, 50, 125, -1000,
	99, -1000, -58, 99, 361, 258, 81, -1000, 706, -1000,
	398, -1000, 337, 333, -1000, 99, 81, 99, 407, 81,
	851, -1000, 360, -83, -1000, 20, -1000, 99, -1000, -1000,
	99, -1000, 183, -1000, -1000, 220, -19, 89, 897, -1000,
	-1000, 799, 782, -1000, -28, -1000, -1000, 851, 851, 851,
	851, 234, 234, -1000, -1000, -1000, 234, -1000, -1000, -1000,
	-1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, 851, 99,
	-1000, -1000, -1000, -1000, 230, 255, -1000, 399, 799, -1000,
	865, -22, 689, -1000, -1000, 256, 81, -1000, -67, -1000,
	-1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, 407, 706,
	197, -1000, -1000, 165, -1000, -1000, 45, 799, 799, 146,
	851, 160, 113, 851, 851, 851, 146, 851, 851, 851,
	851, 851, 851, 851, 851, 851, 851, 851, -1000, -1000,
	-1000, -1000, -1000, -1000, -1000, 32, 897, 109, 187, 225,
	897, 211, 211, -1000, -1000, -1000, 836, 75, 598, -1000,
	428, 46, 865, -1000, 340, 81, 81, 399, 370, 392,
	89, 114, 865, 99, -1000, -1000, 99, -1000, 400, -1000,
	207, 281, -1000, -1000, 199, 363, 248, -1000, -1000, -23,
	-1000, 32, 29, -1000, -1000, 152, -1000, -1000, 865, -1000,
	689, -1000, -1000, 160, 851, 851, 851, 865, 865, 541,
	-1000, 201, 268, -1000, -7, -7, -13, -13, -13, -13,
	14, 14, -1000, -1000, -1000, 851, -1000, -1000, -1000, -1000,
	-1000, 210, 706, -1000, 210, 71, -1000, 799, 251, 234,
	436, 204, 231, -1000, 370, -1000, 851, 851, -24, -1000,
	-1000, 404, 376, 197, 197, 197, 197, -1000, 308, 297,
	-1000, 270, 269, 325, -10, -1000, 122, -1000, -1000, 99,
	-1000, 227, 36, -1000, -1000, -1000, 225, -1000, 865, 865,
	468, 851, 865, -1000, 210, -1000, 42, -1000, 851, 86,
	-1000, 354, 195, -1000, 851, -1000, -1000, 81, -1000, 368,
	179, -1000, 313, 81, -1000, 399, 799, 851, 281, 236,
	425, -1000, -1000, -1000, -1000, 290, -1000, 280, -1000, -1000,
	-1000, -41, -42, -48, -1000, -1000, -1000, -1000, -1000, -1000,
	851, 865, -1000, -1000, 865, 851, 348, 234, -1000, 851,
	851, -1000, -1000, -1000, 370, 89, 170, 799, 799, -1000,
	-1000, 234, 234, 234, 865, 865, 424, -1000, 865, -1000,
	343, 89, 89, 81, 81, 81, 81, -1000, 422, 8,
	177, -1000, 177, 177, 151, -1000, 81, -1000, 81, -1000,
	-1000, 81, -1000, -1000,
}
var yyPgo = [...]int{

	0, 545, 21, 541, 540, 539, 538, 537, 536, 535,
	534, 533, 531, 403, 530, 529, 527, 526, 20, 31,
	525, 524, 18, 27, 15, 512, 508, 16, 507, 26,
	505, 3, 30, 1, 504, 503, 502, 23, 325, 501,
	19, 17, 8, 500, 7, 12, 498, 497, 496, 14,
	494, 493, 492, 491, 490, 9, 489, 5, 488, 6,
	484, 483, 480, 10, 4, 92, 479, 345, 298, 477,
	476, 475, 474, 473, 0, 24, 472, 499, 11, 470,
	468, 13, 467, 458, 73, 2,
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
	37, 37, 37, 37, 37, 39, 39, 39, 52, 52,
	53, 53, 55, 55, 56, 56, 57, 58, 58, 58,
	59, 59, 59, 60, 60, 60, 61, 61, 62, 62,
	63, 63, 36, 36, 43, 43, 44, 64, 64, 65,
	66, 66, 68, 68, 69, 69, 67, 67, 70, 70,
	70, 70, 70, 70, 71, 71, 72, 72, 73, 73,
	74, 77, 84, 85, 81,
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
	1, 1, 1, 1, 1, 1, 2, 2, 0, 3,
	0, 2, 0, 3, 1, 3, 2, 0, 1, 1,
	0, 2, 4, 0, 2, 4, 0, 3, 1, 3,
	0, 5, 2, 1, 1, 3, 3, 1, 3, 3,
	1, 1, 0, 2, 0, 3, 0, 1, 1, 1,
	1, 1, 1, 1, 0, 1, 0, 1, 0, 2,
	1, 1, 1, 1, 0,
}
var yyChk = [...]int{

	-1000, -82, -1, -2, -3, -4, -5, -6, -7, -8,
	-9, -10, -11, -12, 6, 7, 8, 9, 28, 96,
	97, 99, 98, 100, 109, 110, 111, -15, 5, -13,
	-83, -13, -13, -13, -13, 101, -72, 103, 107, -67,
	103, 105, 101, 101, 102, 103, 101, -81, -81, -81,
	-2, 18, 19, -16, 32, 19, -14, -67, -29, -77,
	48, 10, -64, -65, -74, 48, -69, 106, 102, -74,
	101, -74, -77, -68, 106, 48, -68, -77, -17, 35,
	-39, -74, 51, 53, 55, 24, 28, 93, -29, 46,
	68, -77, 61, 48, -81, -77, -81, 104, -77, 21,
	44, -74, -18, -19, 85, -20, -77, -33, -38, 48,
	-34, 61, -84, -37, -46, -44, -45, 83, 84, 90,
	92, -74, -54, -47, 57, 58, 21, 45, 50, 49,
	51, 52, 53, 56, 106, 112, 113, 88, 63, -66,
	17, 10, 30, 30, -29, -64, -77, -32, 11, -65,
	-38, -77, -84, -81, 21, -73, 108, -70, 99, 97,
	27, 98, 14, 114, 48, -77, -77, -81, -21, 46,
	10, -76, -75, 20, -74, 50, 93, 60, 59, 75,
	-35, 78, 61, 76, 77, 62, 75, 80, 79, 89,
	83, 84, 85, 86, 87, 88, 81, 82, 68, 69,
	70, 71, 72, 73, 74, -33, -38, -33, -2, -42,
	-38, 94, 95, -38, -38, -38, -38, -84, -84, -45,
	-84, -50, -38, -29, -61, 28, -84, -32, -55, 14,
	-33, 93, -38, 44, -74, -81, -71, 104, -32, -19,
	-22, -23, -24, -25, -29, -45, -84, -75, 85, -77,
	-74, -33, -33, -40, 56, 61, 57, 58, -38, -41,
	-84, -45, 54, 78, 76, 77, 62, -38, -38, -38,
	-40, -38, -38, -38, -38, -38, -38, -38, -38, -38,
	-38, -38, -85, 47, -85, 46, -85, -37, -37, -74,
	-85, -18, 19, -85, -18, -48, -49, 64, -36, 30,
	-2, -64, -62, -74, -55, -59, 16, 15, -77, -77,
	-77, -52, 12, 46, -26, -27, -28, 34, 38, 40,
	35, 36, 37, 41, -79, -78, 20, -77, 50, -80,
	20, -22, 93, 56, 57, 58, -42, -41, -38, -38,
	-38, 60, -38, -85, -18, -85, -51, -49, 66, -33,
	-63, 44, -43, -44, -84, -63, -85, 46, -59, -38,
	-56, -57, -38, 93, -81, -53, 13, 15, -23, -24,
	-23, -24, 34, 34, 34, 39, 34, 39, 34, -27,
	-30, 42, 105, 43, -78, -77, -85, 85, -74, -85,
	60, -38, -85, 67, -38, 65, 25, 46, -74, 46,
	46, -58, 22, 23, -55, -33, -42, 44, 44, 34,
	34, 102, 102, 102, -38, -38, 26, -44, -38, -57,
	-59, -33, -33, -84, -84, -84, 8, -60, 17, 29,
	-31, -74, -31, -31, -64, 8, 78, -85, 46, -85,
	-85, -74, -74, -74,
}
var yyDef = [...]int{

	0, -2, 1, 2, 3, 4, 5, 6, 7, 8,
	9, 10, 11, 12, 35, 35, 35, 35, 35, 236,
	226, 0, 0, 0, 244, 244, 244, 0, 39, 42,
	37, 226, 0, 0, 0, 224, 0, 0, 237, 0,
	0, 227, 0, 222, 0, 222, 0, 32, 33, 34,
	15, 40, 41, 44, 0, 43, 36, 0, 0, 89,
	241, 0, 20, 217, 0, 240, 0, 0, 0, 244,
	0, 244, 0, 0, 0, 0, 0, 31, 0, 45,
	0, 185, 0, 0, 38, 0, 0, 0, 97, 0,
	0, 244, 0, 238, 23, 0, 26, 0, 28, 223,
	0, 244, 59, 46, 48, 54, 0, 52, 53, -2,
	99, 0, 0, 137, 138, 139, 140, 0, 0, 0,
	0, 176, 0, 163, 105, 106, 0, 242, 179, 180,
	181, 182, 183, 184, 164, 165, 166, 167, 169, 0,
	220, 221, 186, 187, 206, 97, 90, 192, 0, 218,
	219, 0, 0, 21, 225, 0, 0, 244, 234, 228,
	229, 230, 231, 232, 233, 27, 29, 30, 97, 0,
	0, 49, 55, 0, 57, 58, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 124, 125,
	126, 127, 128, 129, 130, 102, 0, 0, 0, 0,
	135, 0, 0, 154, 155, 156, 0, 0, 0, 117,
	0, 0, 170, 14, 0, 0, 0, 192, 200, 0,
	98, 0, 135, 0, 239, 24, 0, 235, 188, 47,
	60, 61, 63, 64, 74, 72, 0, 56, 50, 0,
	177, 100, 101, 104, 118, 0, 120, 122, 107, 108,
	0, 132, 133, 0, 0, 0, 0, 110, 112, 0,
	116, 141, 142, 143, 144, 145, 146, 147, 148, 149,
	150, 151, 103, 243, 134, 0, 216, 152, 153, 157,
	158, 0, 0, 161, 0, 174, 171, 0, 210, 0,
	213, 210, 0, 208, 200, 19, 0, 0, 0, 244,
	25, 190, 0, 0, 0, 0, 0, 79, 0, 0,
	82, 0, 0, 0, 91, 75, 0, 77, 78, 0,
	73, 0, 0, 119, 121, 123, 0, 109, 111, 113,
	0, 0, 136, 159, 0, 162, 0, 172, 0, 0,
	16, 0, 212, 214, 0, 17, 207, 0, 18, 201,
	193, 194, 197, 0, 22, 192, 0, 0, 62, 68,
	0, 71, 80, 81, 83, 0, 85, 0, 87, 88,
	65, 0, 0, 0, 76, 66, 67, 51, 178, 131,
	0, 114, 160, 168, 175, 0, 0, 0, 209, 0,
	0, 196, 198, 199, 200, 191, 189, 0, 0, 84,
	86, 0, 0, 0, 115, 173, 0, 215, 202, 195,
	203, 69, 70, 0, 0, 0, 0, 13, 0, 0,
	0, 95, 0, 0, 211, 204, 0, 92, 0, 93,
	94, 0, 96, 205,
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
			if num, ok := yyDollar[2].valExpr.(NumVal); ok {
				yyVAL.valExpr = num
			} else {
				yyVAL.valExpr = &UnaryExpr{Operator: UPlusStr, Expr: yyDollar[2].valExpr}
			}
		}
	case 155:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:859
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
	case 156:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:872
		{
			yyVAL.valExpr = &UnaryExpr{Operator: TildaStr, Expr: yyDollar[2].valExpr}
		}
	case 157:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:876
		{
			// This rule prevents the usage of INTERVAL
			// as a function. If support is needed for that,
			// we'll need to revisit this. The solution
			// will be non-trivial because of grammar conflicts.
			yyVAL.valExpr = &IntervalExpr{Expr: yyDollar[2].valExpr, Unit: yyDollar[3].colIdent}
		}
	case 158:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:884
		{
			yyVAL.valExpr = &FuncExpr{Name: yyDollar[1].colIdent}
		}
	case 159:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:888
		{
			yyVAL.valExpr = &FuncExpr{Name: yyDollar[1].colIdent, Exprs: yyDollar[3].selectExprs}
		}
	case 160:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:892
		{
			yyVAL.valExpr = &FuncExpr{Name: yyDollar[1].colIdent, Distinct: true, Exprs: yyDollar[4].selectExprs}
		}
	case 161:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:896
		{
			yyVAL.valExpr = &FuncExpr{Name: yyDollar[1].colIdent}
		}
	case 162:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:900
		{
			yyVAL.valExpr = &FuncExpr{Name: yyDollar[1].colIdent, Exprs: yyDollar[3].selectExprs}
		}
	case 163:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:904
		{
			yyVAL.valExpr = yyDollar[1].caseExpr
		}
	case 164:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:910
		{
			yyVAL.colIdent = NewColIdent("if")
		}
	case 165:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:914
		{
			yyVAL.colIdent = NewColIdent("current_timestamp")
		}
	case 166:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:918
		{
			yyVAL.colIdent = NewColIdent("database")
		}
	case 167:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:922
		{
			yyVAL.colIdent = NewColIdent("mod")
		}
	case 168:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:928
		{
			yyVAL.caseExpr = &CaseExpr{Expr: yyDollar[2].valExpr, Whens: yyDollar[3].whens, Else: yyDollar[4].valExpr}
		}
	case 169:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:933
		{
			yyVAL.valExpr = nil
		}
	case 170:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:937
		{
			yyVAL.valExpr = yyDollar[1].valExpr
		}
	case 171:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:943
		{
			yyVAL.whens = []*When{yyDollar[1].when}
		}
	case 172:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:947
		{
			yyVAL.whens = append(yyDollar[1].whens, yyDollar[2].when)
		}
	case 173:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:953
		{
			yyVAL.when = &When{Cond: yyDollar[2].boolExpr, Val: yyDollar[4].valExpr}
		}
	case 174:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:958
		{
			yyVAL.valExpr = nil
		}
	case 175:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:962
		{
			yyVAL.valExpr = yyDollar[2].valExpr
		}
	case 176:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:968
		{
			yyVAL.colName = &ColName{Name: yyDollar[1].colIdent}
		}
	case 177:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:972
		{
			yyVAL.colName = &ColName{Qualifier: &TableName{Name: yyDollar[1].tableIdent}, Name: yyDollar[3].colIdent}
		}
	case 178:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:976
		{
			yyVAL.colName = &ColName{Qualifier: &TableName{Qualifier: yyDollar[1].tableIdent, Name: yyDollar[3].tableIdent}, Name: yyDollar[5].colIdent}
		}
	case 179:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:982
		{
			yyVAL.valExpr = StrVal(yyDollar[1].bytes)
		}
	case 180:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:986
		{
			yyVAL.valExpr = HexVal(yyDollar[1].bytes)
		}
	case 181:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:990
		{
			yyVAL.valExpr = NumVal(yyDollar[1].bytes)
		}
	case 182:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:994
		{
			yyVAL.valExpr = HexNum(yyDollar[1].bytes)
		}
	case 183:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:998
		{
			yyVAL.valExpr = ValArg(yyDollar[1].bytes)
		}
	case 184:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1002
		{
			yyVAL.valExpr = &NullVal{}
		}
	case 185:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1008
		{
			// TODO(sougou): Deprecate this construct.
			if yyDollar[1].colIdent.Lowered() != "value" {
				yylex.Error("expecting value after next")
				return 1
			}
			yyVAL.valExpr = NumVal("1")
		}
	case 186:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:1017
		{
			yyVAL.valExpr = NumVal(yyDollar[1].bytes)
		}
	case 187:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:1021
		{
			yyVAL.valExpr = ValArg(yyDollar[1].bytes)
		}
	case 188:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1026
		{
			yyVAL.valExprs = nil
		}
	case 189:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1030
		{
			yyVAL.valExprs = yyDollar[3].valExprs
		}
	case 190:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1035
		{
			yyVAL.boolExpr = nil
		}
	case 191:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:1039
		{
			yyVAL.boolExpr = yyDollar[2].boolExpr
		}
	case 192:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1044
		{
			yyVAL.orderBy = nil
		}
	case 193:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1048
		{
			yyVAL.orderBy = yyDollar[3].orderBy
		}
	case 194:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1054
		{
			yyVAL.orderBy = OrderBy{yyDollar[1].order}
		}
	case 195:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1058
		{
			yyVAL.orderBy = append(yyDollar[1].orderBy, yyDollar[3].order)
		}
	case 196:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:1064
		{
			yyVAL.order = &Order{Expr: yyDollar[1].valExpr, Direction: yyDollar[2].str}
		}
	case 197:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1069
		{
			yyVAL.str = AscScr
		}
	case 198:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1073
		{
			yyVAL.str = AscScr
		}
	case 199:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1077
		{
			yyVAL.str = DescScr
		}
	case 200:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1082
		{
			yyVAL.limit = nil
		}
	case 201:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:1086
		{
			yyVAL.limit = &Limit{Rowcount: yyDollar[2].valExpr}
		}
	case 202:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:1090
		{
			yyVAL.limit = &Limit{Offset: yyDollar[2].valExpr, Rowcount: yyDollar[4].valExpr}
		}
	case 203:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1095
		{
			yyVAL.str = ""
		}
	case 204:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:1099
		{
			yyVAL.str = ForUpdateStr
		}
	case 205:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:1103
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
	case 206:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1116
		{
			yyVAL.columns = nil
		}
	case 207:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1120
		{
			yyVAL.columns = yyDollar[2].columns
		}
	case 208:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1126
		{
			yyVAL.columns = Columns{yyDollar[1].colIdent}
		}
	case 209:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1130
		{
			yyVAL.columns = append(yyVAL.columns, yyDollar[3].colIdent)
		}
	case 210:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1135
		{
			yyVAL.updateExprs = nil
		}
	case 211:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:1139
		{
			yyVAL.updateExprs = yyDollar[5].updateExprs
		}
	case 212:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:1145
		{
			yyVAL.insRows = yyDollar[2].values
		}
	case 213:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1149
		{
			yyVAL.insRows = yyDollar[1].selStmt
		}
	case 214:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1155
		{
			yyVAL.values = Values{yyDollar[1].valTuple}
		}
	case 215:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1159
		{
			yyVAL.values = append(yyDollar[1].values, yyDollar[3].valTuple)
		}
	case 216:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1165
		{
			yyVAL.valTuple = ValTuple(yyDollar[2].valExprs)
		}
	case 217:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1171
		{
			yyVAL.updateExprs = UpdateExprs{yyDollar[1].updateExpr}
		}
	case 218:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1175
		{
			yyVAL.updateExprs = append(yyDollar[1].updateExprs, yyDollar[3].updateExpr)
		}
	case 219:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1181
		{
			yyVAL.updateExpr = &UpdateExpr{Name: yyDollar[1].colIdent, Expr: yyDollar[3].valExpr}
		}
	case 222:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1190
		{
			yyVAL.byt = 0
		}
	case 223:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:1192
		{
			yyVAL.byt = 1
		}
	case 224:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1195
		{
			yyVAL.empty = struct{}{}
		}
	case 225:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1197
		{
			yyVAL.empty = struct{}{}
		}
	case 226:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1200
		{
			yyVAL.str = ""
		}
	case 227:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1202
		{
			yyVAL.str = IgnoreStr
		}
	case 228:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1206
		{
			yyVAL.empty = struct{}{}
		}
	case 229:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1208
		{
			yyVAL.empty = struct{}{}
		}
	case 230:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1210
		{
			yyVAL.empty = struct{}{}
		}
	case 231:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1212
		{
			yyVAL.empty = struct{}{}
		}
	case 232:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1214
		{
			yyVAL.empty = struct{}{}
		}
	case 233:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1216
		{
			yyVAL.empty = struct{}{}
		}
	case 234:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1219
		{
			yyVAL.empty = struct{}{}
		}
	case 235:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1221
		{
			yyVAL.empty = struct{}{}
		}
	case 236:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1224
		{
			yyVAL.empty = struct{}{}
		}
	case 237:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1226
		{
			yyVAL.empty = struct{}{}
		}
	case 238:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1229
		{
			yyVAL.empty = struct{}{}
		}
	case 239:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:1231
		{
			yyVAL.empty = struct{}{}
		}
	case 240:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1235
		{
			yyVAL.colIdent = NewColIdent(string(yyDollar[1].bytes))
		}
	case 241:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1241
		{
			yyVAL.tableIdent = NewTableIdent(string(yyDollar[1].bytes))
		}
	case 242:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1247
		{
			if incNesting(yylex) {
				yylex.Error("max nesting level reached")
				return 1
			}
		}
	case 243:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1256
		{
			decNesting(yylex)
		}
	case 244:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1261
		{
			forceEOF(yylex)
		}
	}
	goto yystack /* stack new state and value */
}
