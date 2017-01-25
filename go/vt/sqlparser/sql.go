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
	boolVal     BoolVal
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
	-1, 123,
	96, 242,
	-2, 241,
	-1, 381,
	61, 80,
	62, 80,
	-2, 92,
	-1, 406,
	61, 80,
	62, 80,
	-2, 93,
}

const yyNprod = 246
const yyPrivate = 57344

var yyTokenNames []string
var yyStates []string

const yyLast = 1162

var yyAct = [...]int{

	117, 353, 62, 302, 422, 225, 213, 312, 111, 281,
	322, 291, 238, 263, 236, 151, 196, 109, 105, 165,
	212, 3, 237, 240, 35, 47, 37, 159, 293, 41,
	38, 74, 131, 163, 67, 64, 373, 375, 69, 233,
	40, 71, 41, 43, 44, 45, 405, 99, 50, 404,
	403, 48, 49, 104, 167, 81, 58, 68, 70, 46,
	107, 176, 175, 184, 185, 178, 179, 180, 181, 182,
	183, 177, 42, 355, 186, 201, 202, 103, 338, 228,
	200, 87, 89, 177, 186, 90, 186, 63, 65, 64,
	123, 149, 64, 427, 92, 96, 385, 98, 292, 292,
	345, 374, 65, 94, 84, 162, 164, 161, 198, 270,
	148, 139, 112, 180, 181, 182, 183, 177, 147, 156,
	186, 266, 166, 268, 269, 267, 382, 60, 275, 170,
	175, 184, 185, 178, 179, 180, 181, 182, 183, 177,
	429, 284, 186, 329, 330, 331, 123, 64, 223, 221,
	207, 208, 60, 154, 325, 211, 258, 260, 261, 14,
	230, 259, 172, 284, 217, 224, 220, 65, 28, 199,
	14, 15, 16, 17, 203, 204, 205, 206, 95, 153,
	323, 209, 65, 288, 284, 82, 231, 234, 83, 310,
	284, 235, 214, 18, 300, 284, 91, 216, 198, 139,
	139, 277, 60, 60, 75, 288, 242, 280, 60, 86,
	325, 284, 392, 227, 274, 294, 173, 91, 285, 278,
	279, 348, 287, 289, 264, 310, 139, 73, 296, 277,
	301, 399, 299, 60, 294, 210, 243, 244, 245, 246,
	247, 248, 249, 250, 251, 252, 253, 254, 255, 256,
	298, 262, 297, 172, 271, 272, 273, 328, 229, 152,
	102, 282, 286, 19, 20, 22, 21, 23, 146, 368,
	402, 332, 79, 76, 369, 242, 24, 25, 26, 366,
	370, 333, 318, 319, 367, 139, 241, 314, 317, 318,
	319, 315, 339, 316, 320, 91, 341, 400, 401, 365,
	264, 349, 344, 364, 265, 350, 184, 185, 178, 179,
	180, 181, 182, 183, 177, 14, 39, 186, 144, 143,
	88, 388, 347, 55, 361, 214, 363, 157, 371, 334,
	335, 336, 356, 360, 376, 362, 54, 340, 378, 383,
	219, 101, 379, 242, 242, 242, 242, 420, 57, 342,
	384, 327, 303, 346, 142, 241, 383, 389, 214, 421,
	51, 52, 141, 396, 351, 354, 398, 178, 179, 180,
	181, 182, 183, 177, 359, 304, 186, 226, 358, 309,
	265, 297, 125, 124, 126, 127, 128, 129, 152, 61,
	130, 426, 408, 14, 411, 28, 155, 30, 381, 1,
	412, 314, 317, 318, 319, 315, 386, 316, 320, 64,
	326, 418, 321, 174, 158, 36, 423, 423, 423, 397,
	214, 424, 425, 241, 241, 241, 241, 232, 432, 59,
	433, 160, 428, 434, 430, 431, 415, 416, 417, 72,
	66, 406, 140, 77, 222, 145, 419, 393, 407, 352,
	118, 409, 410, 354, 59, 29, 357, 308, 59, 343,
	413, 414, 215, 93, 290, 119, 110, 97, 295, 257,
	100, 31, 32, 33, 34, 108, 283, 80, 122, 218,
	121, 372, 85, 59, 313, 311, 150, 239, 171, 78,
	53, 27, 56, 13, 12, 11, 168, 10, 9, 169,
	8, 7, 139, 6, 284, 123, 125, 124, 126, 127,
	128, 129, 5, 4, 130, 137, 138, 2, 0, 120,
	0, 136, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 59, 0, 0,
	0, 113, 114, 106, 0, 0, 135, 0, 115, 122,
	0, 116, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 132, 0, 0, 0, 108,
	59, 133, 134, 139, 0, 284, 123, 125, 124, 126,
	127, 128, 129, 0, 0, 130, 137, 138, 0, 0,
	120, 0, 136, 0, 0, 0, 0, 276, 0, 0,
	0, 0, 0, 0, 108, 108, 0, 0, 0, 0,
	0, 0, 113, 114, 106, 0, 0, 135, 0, 115,
	0, 0, 116, 0, 122, 305, 306, 0, 0, 307,
	0, 0, 0, 0, 0, 0, 132, 324, 0, 59,
	0, 0, 133, 134, 0, 0, 0, 0, 139, 0,
	0, 123, 125, 124, 126, 127, 128, 129, 0, 0,
	130, 137, 138, 0, 0, 120, 0, 136, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	108, 0, 14, 0, 0, 0, 0, 113, 114, 106,
	0, 0, 135, 0, 115, 0, 0, 116, 122, 0,
	0, 0, 0, 0, 0, 0, 0, 59, 59, 59,
	59, 132, 0, 0, 0, 0, 122, 133, 134, 0,
	324, 0, 139, 377, 0, 123, 125, 124, 126, 127,
	128, 129, 0, 0, 130, 137, 138, 0, 0, 120,
	139, 136, 0, 123, 125, 124, 126, 127, 128, 129,
	0, 0, 130, 137, 138, 0, 0, 120, 0, 136,
	0, 113, 114, 0, 0, 197, 135, 0, 115, 0,
	0, 116, 0, 0, 0, 0, 0, 0, 0, 113,
	114, 0, 0, 0, 135, 132, 115, 0, 0, 116,
	0, 133, 134, 65, 0, 199, 0, 0, 0, 0,
	0, 394, 395, 132, 0, 188, 187, 192, 195, 133,
	134, 0, 0, 0, 190, 0, 0, 0, 0, 0,
	0, 189, 193, 194, 191, 176, 175, 184, 185, 178,
	179, 180, 181, 182, 183, 177, 0, 0, 186, 188,
	187, 192, 195, 391, 0, 0, 0, 0, 190, 0,
	0, 0, 0, 0, 0, 189, 193, 194, 191, 176,
	175, 184, 185, 178, 179, 180, 181, 182, 183, 177,
	0, 0, 186, 390, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 188, 187, 192,
	195, 0, 0, 0, 0, 0, 190, 0, 0, 0,
	0, 0, 0, 189, 193, 194, 191, 176, 175, 184,
	185, 178, 179, 180, 181, 182, 183, 177, 0, 0,
	186, 188, 187, 192, 195, 0, 0, 387, 0, 0,
	190, 0, 0, 0, 0, 0, 0, 189, 193, 194,
	191, 176, 175, 184, 185, 178, 179, 180, 181, 182,
	183, 177, 65, 0, 186, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 188, 187, 192, 195, 0, 0,
	0, 0, 0, 190, 0, 0, 0, 0, 0, 0,
	189, 193, 194, 191, 176, 175, 184, 185, 178, 179,
	180, 181, 182, 183, 177, 0, 0, 186, 188, 187,
	192, 195, 0, 0, 0, 0, 0, 190, 0, 0,
	0, 0, 0, 0, 189, 193, 194, 191, 176, 175,
	184, 185, 178, 179, 180, 181, 182, 183, 177, 0,
	0, 186, 188, 380, 192, 195, 0, 0, 0, 0,
	0, 190, 0, 0, 0, 0, 0, 0, 189, 193,
	194, 191, 176, 175, 184, 185, 178, 179, 180, 181,
	182, 183, 177, 0, 0, 186, 188, 337, 192, 195,
	0, 0, 0, 0, 0, 190, 0, 0, 0, 0,
	0, 0, 189, 193, 194, 191, 176, 175, 184, 185,
	178, 179, 180, 181, 182, 183, 177, 0, 0, 186,
	187, 192, 195, 0, 0, 0, 0, 0, 190, 0,
	0, 0, 0, 0, 0, 189, 193, 194, 191, 176,
	175, 184, 185, 178, 179, 180, 181, 182, 183, 177,
	192, 195, 186, 0, 0, 0, 0, 190, 0, 0,
	0, 0, 0, 0, 189, 193, 194, 191, 176, 175,
	184, 185, 178, 179, 180, 181, 182, 183, 177, 0,
	0, 186,
}
var yyPact = [...]int{

	164, -1000, -1000, 390, -1000, -1000, -1000, -1000, -1000, -1000,
	-1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, -80,
	-66, -32, -61, -45, -1000, -1000, -1000, 387, 341, 303,
	-1000, -79, 78, 379, 53, -75, -48, 53, -1000, -46,
	53, -1000, 78, -78, 155, -78, 78, -1000, -1000, -1000,
	-1000, -1000, -1000, 236, 133, -1000, 47, 184, 291, -14,
	-1000, 78, 149, -1000, 24, -1000, 78, 40, 129, -1000,
	78, -1000, -60, 78, 319, 215, 53, -1000, 602, -1000,
	344, -1000, 288, 287, -1000, 239, 78, -1000, 53, 78,
	377, 53, 694, -1000, 305, -84, -1000, 5, -1000, 78,
	-1000, -1000, 78, -1000, 206, -1000, -1000, 744, -16, -1000,
	-22, -1000, -1000, 694, 694, 694, 694, 180, 180, -1000,
	694, -1000, 180, -1000, -1000, -1000, -1000, -1000, -1000, -1000,
	-1000, 676, -1000, -1000, -1000, -1000, 694, -1000, -1000, -1000,
	78, -1000, -1000, -1000, -1000, 309, 53, 53, -1000, 248,
	-1000, 363, 694, -1000, 937, -17, -1000, -1000, 213, 53,
	-1000, -68, -1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000,
	-1000, 377, 602, 154, -1000, 694, 694, 694, 694, 694,
	694, 694, 694, 694, 694, 694, 694, 694, 694, 98,
	694, 65, 45, 694, 694, 694, -1000, 118, -1000, -1000,
	41, 332, 332, -10, -10, -10, 903, 456, 527, 1067,
	-1000, 387, 163, 136, 937, 33, 937, -1000, 189, 180,
	390, 170, 147, -1000, 363, 336, 360, 937, 97, 78,
	-1000, -1000, 78, -1000, 367, -1000, 178, 366, -1000, -1000,
	159, 330, 153, 223, 48, -10, 26, 26, -8, -8,
	-8, -8, 282, 282, -1000, 1067, 1038, -1000, -1000, 85,
	-1000, -1000, -20, -1000, 676, -1000, -1000, 65, 694, 694,
	694, -20, -20, 1005, -1000, -1000, -18, -1000, -1000, -1000,
	-1000, -1000, 115, 602, -1000, -1000, 115, -1000, 694, -1000,
	32, -1000, 694, -1000, 296, 174, -1000, 694, -1000, -1000,
	53, 336, -1000, 694, 694, -23, -1000, -1000, 365, 359,
	154, 154, 154, 154, -1000, 268, 264, -1000, 244, 234,
	245, -7, -1000, 103, -1000, -1000, 78, -1000, 142, -1000,
	-1000, -1000, 136, -1000, -20, -20, 971, 694, 39, -1000,
	115, -1000, 937, 27, -1000, 694, 860, 294, 180, -1000,
	-1000, 826, 165, -1000, 778, 53, -1000, 363, 694, 694,
	366, 186, 252, -1000, -1000, -1000, -1000, 263, -1000, 235,
	-1000, -1000, -1000, -55, -56, -59, -1000, -1000, -1000, -1000,
	694, 1067, -1000, -1000, -1000, -1000, 937, 694, 384, -1000,
	694, 694, 694, -1000, -1000, -1000, 336, 937, 158, 694,
	694, -1000, -1000, 180, 180, 180, 1067, 937, 53, 937,
	937, -1000, 329, 937, 937, 53, 53, 53, 149, -1000,
	383, 13, 93, -1000, 93, 93, -1000, 53, -1000, 53,
	-1000, -1000, 53, -1000, -1000,
}
var yyPgo = [...]int{

	0, 517, 20, 513, 512, 503, 501, 500, 498, 497,
	495, 494, 493, 455, 492, 491, 490, 489, 53, 18,
	60, 488, 14, 22, 12, 487, 485, 7, 484, 23,
	482, 481, 4, 15, 480, 480, 479, 17, 477, 469,
	13, 6, 468, 8, 112, 466, 465, 464, 11, 462,
	459, 457, 456, 450, 5, 449, 1, 447, 3, 446,
	445, 444, 28, 2, 87, 442, 316, 227, 440, 431,
	427, 415, 414, 0, 16, 413, 396, 10, 412, 410,
	25, 399, 397, 32, 9,
}
var yyR1 = [...]int{

	0, 81, 1, 1, 1, 1, 1, 1, 1, 1,
	1, 1, 1, 2, 2, 2, 3, 3, 4, 5,
	6, 7, 7, 7, 8, 8, 8, 9, 10, 10,
	10, 11, 12, 12, 12, 82, 13, 14, 14, 15,
	15, 15, 16, 16, 17, 17, 18, 18, 19, 19,
	19, 19, 20, 20, 20, 20, 20, 20, 20, 20,
	20, 20, 20, 20, 20, 20, 20, 20, 20, 20,
	20, 20, 20, 20, 20, 20, 20, 20, 20, 20,
	20, 20, 20, 20, 20, 20, 20, 20, 20, 20,
	20, 20, 20, 20, 20, 75, 75, 75, 74, 74,
	21, 21, 22, 22, 23, 23, 24, 24, 24, 25,
	25, 25, 25, 79, 79, 78, 78, 78, 77, 77,
	26, 26, 26, 26, 27, 27, 27, 27, 28, 28,
	30, 30, 29, 29, 31, 31, 31, 31, 32, 32,
	33, 33, 34, 34, 39, 39, 39, 39, 39, 39,
	35, 35, 35, 35, 35, 35, 35, 40, 40, 40,
	44, 41, 41, 53, 53, 53, 53, 46, 49, 49,
	47, 47, 48, 50, 50, 45, 45, 45, 37, 37,
	37, 37, 37, 37, 37, 38, 38, 38, 51, 51,
	52, 52, 54, 54, 55, 55, 56, 57, 57, 57,
	58, 58, 58, 58, 59, 59, 59, 60, 60, 61,
	61, 62, 62, 36, 36, 42, 42, 43, 63, 63,
	64, 65, 65, 67, 67, 68, 68, 66, 66, 69,
	69, 69, 69, 69, 69, 70, 70, 71, 71, 72,
	72, 73, 76, 83, 84, 80,
}
var yyR2 = [...]int{

	0, 1, 1, 1, 1, 1, 1, 1, 1, 1,
	1, 1, 1, 12, 6, 3, 7, 7, 8, 7,
	3, 5, 8, 4, 6, 7, 4, 5, 4, 5,
	5, 3, 2, 2, 2, 0, 2, 0, 2, 1,
	2, 2, 0, 1, 0, 1, 1, 3, 1, 2,
	3, 5, 1, 1, 1, 1, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	2, 2, 2, 3, 3, 4, 5, 3, 4, 1,
	3, 3, 2, 3, 1, 3, 3, 4, 3, 4,
	3, 4, 5, 6, 2, 0, 1, 2, 1, 1,
	0, 2, 1, 3, 1, 1, 3, 3, 3, 3,
	5, 5, 3, 0, 1, 0, 1, 2, 1, 1,
	1, 2, 2, 1, 2, 3, 2, 3, 2, 2,
	2, 1, 1, 3, 0, 5, 5, 5, 1, 3,
	0, 2, 1, 1, 1, 2, 1, 2, 1, 2,
	1, 1, 1, 1, 1, 1, 1, 3, 1, 1,
	3, 1, 3, 1, 1, 1, 1, 5, 0, 1,
	1, 2, 4, 0, 2, 1, 3, 5, 1, 1,
	1, 1, 1, 1, 1, 1, 2, 2, 0, 3,
	0, 2, 0, 3, 1, 3, 2, 0, 1, 1,
	0, 2, 4, 4, 0, 2, 4, 0, 3, 1,
	3, 0, 5, 2, 1, 1, 3, 3, 1, 3,
	3, 1, 1, 0, 2, 0, 3, 0, 1, 1,
	1, 1, 1, 1, 1, 0, 1, 0, 1, 0,
	2, 1, 1, 1, 1, 0,
}
var yyChk = [...]int{

	-1000, -81, -1, -2, -3, -4, -5, -6, -7, -8,
	-9, -10, -11, -12, 6, 7, 8, 9, 29, 99,
	100, 102, 101, 103, 112, 113, 114, -15, 5, -13,
	-82, -13, -13, -13, -13, 104, -71, 106, 110, -66,
	106, 108, 104, 104, 105, 106, 104, -80, -80, -80,
	-2, 19, 20, -16, 33, 20, -14, -66, -29, -76,
	49, 10, -63, -64, -73, 49, -68, 109, 105, -73,
	104, -73, -76, -67, 109, 49, -67, -76, -17, 36,
	-38, -73, 52, 55, 57, -30, 25, -29, 29, 96,
	-29, 47, 70, -76, 63, 49, -80, -76, -80, 107,
	-76, 22, 45, -73, -18, -19, 87, -20, -76, -37,
	-45, -43, -44, 85, 86, 92, 95, -73, -53, -46,
	63, -34, 22, 49, 51, 50, 52, 53, 54, 55,
	58, -83, 109, 115, 116, 90, 65, 59, 60, 46,
	-65, 18, 10, 31, 31, -60, 29, -83, -29, -63,
	-76, -33, 11, -64, -20, -76, -80, 22, -72, 111,
	-69, 102, 100, 28, 101, 14, 117, 49, -76, -76,
	-80, -21, 47, 10, -75, 82, 81, 91, 85, 86,
	87, 88, 89, 90, 83, 84, 94, 62, 61, 77,
	70, 80, 63, 78, 79, 64, -74, 21, -73, 51,
	96, 97, 98, -20, -20, -20, -20, -83, -83, -20,
	-44, -83, -2, -41, -20, -49, -20, -29, -36, 31,
	-2, -63, -61, -73, -33, -54, 14, -20, 96, 45,
	-73, -80, -70, 107, -33, -19, -22, -23, -24, -25,
	-29, -44, -83, -20, -20, -20, -20, -20, -20, -20,
	-20, -20, -20, -20, -20, -20, -20, -39, 58, 63,
	59, 60, -20, -40, -83, -44, 56, 80, 78, 79,
	64, -20, -20, -20, -74, 87, -76, -73, -37, -37,
	-73, -84, -18, 20, 48, -84, -18, -84, 47, -84,
	-47, -48, 66, -62, 45, -42, -43, -83, -62, -84,
	47, -54, -58, 16, 15, -76, -76, -76, -51, 12,
	47, -26, -27, -28, 35, 39, 41, 36, 37, 38,
	42, -78, -77, 21, -76, 51, -79, 21, -22, 58,
	59, 60, -41, -40, -20, -20, -20, 62, 96, -84,
	-18, -84, -20, -50, -48, 68, -20, 26, 47, -73,
	-58, -20, -55, -56, -20, 96, -80, -52, 13, 15,
	-23, -24, -23, -24, 35, 35, 35, 40, 35, 40,
	35, -27, -31, 43, 108, 44, -77, -76, -84, -84,
	62, -20, 87, -73, -84, 69, -20, 67, 27, -43,
	47, 17, 47, -57, 23, 24, -54, -20, -41, 45,
	45, 35, 35, 105, 105, 105, -20, -20, 8, -20,
	-20, -56, -58, -20, -20, -83, -83, -83, -63, -59,
	18, 30, -32, -73, -32, -32, 8, 80, -84, 47,
	-84, -84, -73, -73, -73,
}
var yyDef = [...]int{

	0, -2, 1, 2, 3, 4, 5, 6, 7, 8,
	9, 10, 11, 12, 35, 35, 35, 35, 35, 237,
	227, 0, 0, 0, 245, 245, 245, 0, 39, 42,
	37, 227, 0, 0, 0, 225, 0, 0, 238, 0,
	0, 228, 0, 223, 0, 223, 0, 32, 33, 34,
	15, 40, 41, 44, 0, 43, 36, 0, 0, 132,
	242, 0, 20, 218, 0, 241, 0, 0, 0, 245,
	0, 245, 0, 0, 0, 0, 0, 31, 0, 45,
	0, 185, 0, 0, 38, 207, 0, 131, 0, 0,
	140, 0, 0, 245, 0, 239, 23, 0, 26, 0,
	28, 224, 0, 245, 100, 46, 48, 95, 0, 52,
	53, 54, 55, 0, 0, 0, 0, 175, 0, 79,
	0, 84, 0, -2, 178, 179, 180, 181, 182, 183,
	184, 0, 163, 164, 165, 166, 168, 142, 143, 243,
	0, 221, 222, 186, 187, 0, 0, 0, 130, 140,
	133, 192, 0, 219, 220, 0, 21, 226, 0, 0,
	245, 235, 229, 230, 231, 232, 233, 234, 27, 29,
	30, 140, 0, 0, 49, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 96, 0, 98, 99,
	0, 0, 0, 70, 71, 72, 0, 0, 0, 82,
	94, 0, 0, 0, 161, 0, 169, 14, 211, 0,
	214, 211, 0, 209, 192, 200, 0, 141, 0, 0,
	240, 24, 0, 236, 188, 47, 101, 102, 104, 105,
	115, 113, 0, 56, 57, 58, 59, 60, 61, 62,
	63, 64, 65, 66, 69, 80, 81, 83, 144, 0,
	146, 148, 85, 86, 0, 158, 159, 0, 0, 0,
	0, 88, 90, 0, 97, 50, 0, 176, 67, 68,
	73, 74, 0, 0, 244, 77, 0, 160, 0, 217,
	173, 170, 0, 16, 0, 213, 215, 0, 17, 208,
	0, 200, 19, 0, 0, 0, 245, 25, 190, 0,
	0, 0, 0, 0, 120, 0, 0, 123, 0, 0,
	0, 134, 116, 0, 118, 119, 0, 114, 0, 145,
	147, 149, 0, 87, 89, 91, 0, 0, 0, 75,
	0, 78, 162, 0, 171, 0, 0, 0, 0, 210,
	18, 201, 193, 194, 197, 0, 22, 192, 0, 0,
	103, 109, 0, 112, 121, 122, 124, 0, 126, 0,
	128, 129, 106, 0, 0, 0, 117, 107, 108, 157,
	0, -2, 51, 177, 76, 167, 174, 0, 0, 216,
	0, 0, 0, 196, 198, 199, 200, 191, 189, 0,
	0, 125, 127, 0, 0, 0, -2, 172, 0, 202,
	203, 195, 204, 110, 111, 0, 0, 0, 212, 13,
	0, 0, 0, 138, 0, 0, 205, 0, 135, 0,
	136, 137, 0, 139, 206,
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
		//line sql.y:180
		{
			setParseTree(yylex, yyDollar[1].statement)
		}
	case 2:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:186
		{
			yyVAL.statement = yyDollar[1].selStmt
		}
	case 13:
		yyDollar = yyS[yypt-12 : yypt+1]
		//line sql.y:202
		{
			yyVAL.selStmt = &Select{Comments: Comments(yyDollar[2].bytes2), Distinct: yyDollar[3].str, Hints: yyDollar[4].str, SelectExprs: yyDollar[5].selectExprs, From: yyDollar[6].tableExprs, Where: NewWhere(WhereStr, yyDollar[7].expr), GroupBy: GroupBy(yyDollar[8].valExprs), Having: NewWhere(HavingStr, yyDollar[9].expr), OrderBy: yyDollar[10].orderBy, Limit: yyDollar[11].limit, Lock: yyDollar[12].str}
		}
	case 14:
		yyDollar = yyS[yypt-6 : yypt+1]
		//line sql.y:206
		{
			yyVAL.selStmt = &Select{Comments: Comments(yyDollar[2].bytes2), SelectExprs: SelectExprs{Nextval{Expr: yyDollar[4].expr}}, From: TableExprs{&AliasedTableExpr{Expr: yyDollar[6].tableName}}}
		}
	case 15:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:210
		{
			yyVAL.selStmt = &Union{Type: yyDollar[2].str, Left: yyDollar[1].selStmt, Right: yyDollar[3].selStmt}
		}
	case 16:
		yyDollar = yyS[yypt-7 : yypt+1]
		//line sql.y:216
		{
			yyVAL.statement = &Insert{Comments: Comments(yyDollar[2].bytes2), Ignore: yyDollar[3].str, Table: yyDollar[4].tableName, Columns: yyDollar[5].columns, Rows: yyDollar[6].insRows, OnDup: OnDup(yyDollar[7].updateExprs)}
		}
	case 17:
		yyDollar = yyS[yypt-7 : yypt+1]
		//line sql.y:220
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
		//line sql.y:232
		{
			yyVAL.statement = &Update{Comments: Comments(yyDollar[2].bytes2), Table: yyDollar[3].tableName, Exprs: yyDollar[5].updateExprs, Where: NewWhere(WhereStr, yyDollar[6].expr), OrderBy: yyDollar[7].orderBy, Limit: yyDollar[8].limit}
		}
	case 19:
		yyDollar = yyS[yypt-7 : yypt+1]
		//line sql.y:238
		{
			yyVAL.statement = &Delete{Comments: Comments(yyDollar[2].bytes2), Table: yyDollar[4].tableName, Where: NewWhere(WhereStr, yyDollar[5].expr), OrderBy: yyDollar[6].orderBy, Limit: yyDollar[7].limit}
		}
	case 20:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:244
		{
			yyVAL.statement = &Set{Comments: Comments(yyDollar[2].bytes2), Exprs: yyDollar[3].updateExprs}
		}
	case 21:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:250
		{
			yyVAL.statement = &DDL{Action: CreateStr, NewName: yyDollar[4].tableIdent}
		}
	case 22:
		yyDollar = yyS[yypt-8 : yypt+1]
		//line sql.y:254
		{
			// Change this to an alter statement
			yyVAL.statement = &DDL{Action: AlterStr, Table: yyDollar[7].tableIdent, NewName: yyDollar[7].tableIdent}
		}
	case 23:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:259
		{
			yyVAL.statement = &DDL{Action: CreateStr, NewName: NewTableIdent(yyDollar[3].colIdent.Lowered())}
		}
	case 24:
		yyDollar = yyS[yypt-6 : yypt+1]
		//line sql.y:265
		{
			yyVAL.statement = &DDL{Action: AlterStr, Table: yyDollar[4].tableIdent, NewName: yyDollar[4].tableIdent}
		}
	case 25:
		yyDollar = yyS[yypt-7 : yypt+1]
		//line sql.y:269
		{
			// Change this to a rename statement
			yyVAL.statement = &DDL{Action: RenameStr, Table: yyDollar[4].tableIdent, NewName: yyDollar[7].tableIdent}
		}
	case 26:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:274
		{
			yyVAL.statement = &DDL{Action: AlterStr, Table: NewTableIdent(yyDollar[3].colIdent.Lowered()), NewName: NewTableIdent(yyDollar[3].colIdent.Lowered())}
		}
	case 27:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:280
		{
			yyVAL.statement = &DDL{Action: RenameStr, Table: yyDollar[3].tableIdent, NewName: yyDollar[5].tableIdent}
		}
	case 28:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:286
		{
			var exists bool
			if yyDollar[3].byt != 0 {
				exists = true
			}
			yyVAL.statement = &DDL{Action: DropStr, Table: yyDollar[4].tableIdent, IfExists: exists}
		}
	case 29:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:294
		{
			// Change this to an alter statement
			yyVAL.statement = &DDL{Action: AlterStr, Table: yyDollar[5].tableIdent, NewName: yyDollar[5].tableIdent}
		}
	case 30:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:299
		{
			var exists bool
			if yyDollar[3].byt != 0 {
				exists = true
			}
			yyVAL.statement = &DDL{Action: DropStr, Table: NewTableIdent(yyDollar[4].colIdent.Lowered()), IfExists: exists}
		}
	case 31:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:309
		{
			yyVAL.statement = &DDL{Action: AlterStr, Table: yyDollar[3].tableIdent, NewName: yyDollar[3].tableIdent}
		}
	case 32:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:315
		{
			yyVAL.statement = &Other{}
		}
	case 33:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:319
		{
			yyVAL.statement = &Other{}
		}
	case 34:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:323
		{
			yyVAL.statement = &Other{}
		}
	case 35:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:328
		{
			setAllowComments(yylex, true)
		}
	case 36:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:332
		{
			yyVAL.bytes2 = yyDollar[2].bytes2
			setAllowComments(yylex, false)
		}
	case 37:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:338
		{
			yyVAL.bytes2 = nil
		}
	case 38:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:342
		{
			yyVAL.bytes2 = append(yyDollar[1].bytes2, yyDollar[2].bytes)
		}
	case 39:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:348
		{
			yyVAL.str = UnionStr
		}
	case 40:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:352
		{
			yyVAL.str = UnionAllStr
		}
	case 41:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:356
		{
			yyVAL.str = UnionDistinctStr
		}
	case 42:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:361
		{
			yyVAL.str = ""
		}
	case 43:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:365
		{
			yyVAL.str = DistinctStr
		}
	case 44:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:370
		{
			yyVAL.str = ""
		}
	case 45:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:374
		{
			yyVAL.str = StraightJoinHint
		}
	case 46:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:380
		{
			yyVAL.selectExprs = SelectExprs{yyDollar[1].selectExpr}
		}
	case 47:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:384
		{
			yyVAL.selectExprs = append(yyVAL.selectExprs, yyDollar[3].selectExpr)
		}
	case 48:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:390
		{
			yyVAL.selectExpr = &StarExpr{}
		}
	case 49:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:394
		{
			yyVAL.selectExpr = &NonStarExpr{Expr: yyDollar[1].expr, As: yyDollar[2].colIdent}
		}
	case 50:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:398
		{
			yyVAL.selectExpr = &StarExpr{TableName: &TableName{Name: yyDollar[1].tableIdent}}
		}
	case 51:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:402
		{
			yyVAL.selectExpr = &StarExpr{TableName: &TableName{Qualifier: yyDollar[1].tableIdent, Name: yyDollar[3].tableIdent}}
		}
	case 52:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:408
		{
			yyVAL.expr = yyDollar[1].expr
		}
	case 53:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:412
		{
			yyVAL.expr = yyDollar[1].colName
		}
	case 54:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:416
		{
			yyVAL.expr = yyDollar[1].valTuple
		}
	case 55:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:420
		{
			yyVAL.expr = yyDollar[1].subquery
		}
	case 56:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:424
		{
			yyVAL.expr = &BinaryExpr{Left: yyDollar[1].expr, Operator: BitAndStr, Right: yyDollar[3].expr}
		}
	case 57:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:428
		{
			yyVAL.expr = &BinaryExpr{Left: yyDollar[1].expr, Operator: BitOrStr, Right: yyDollar[3].expr}
		}
	case 58:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:432
		{
			yyVAL.expr = &BinaryExpr{Left: yyDollar[1].expr, Operator: BitXorStr, Right: yyDollar[3].expr}
		}
	case 59:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:436
		{
			yyVAL.expr = &BinaryExpr{Left: yyDollar[1].expr, Operator: PlusStr, Right: yyDollar[3].expr}
		}
	case 60:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:440
		{
			yyVAL.expr = &BinaryExpr{Left: yyDollar[1].expr, Operator: MinusStr, Right: yyDollar[3].expr}
		}
	case 61:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:444
		{
			yyVAL.expr = &BinaryExpr{Left: yyDollar[1].expr, Operator: MultStr, Right: yyDollar[3].expr}
		}
	case 62:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:448
		{
			yyVAL.expr = &BinaryExpr{Left: yyDollar[1].expr, Operator: DivStr, Right: yyDollar[3].expr}
		}
	case 63:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:452
		{
			yyVAL.expr = &BinaryExpr{Left: yyDollar[1].expr, Operator: ModStr, Right: yyDollar[3].expr}
		}
	case 64:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:456
		{
			yyVAL.expr = &BinaryExpr{Left: yyDollar[1].expr, Operator: ModStr, Right: yyDollar[3].expr}
		}
	case 65:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:460
		{
			yyVAL.expr = &BinaryExpr{Left: yyDollar[1].expr, Operator: ShiftLeftStr, Right: yyDollar[3].expr}
		}
	case 66:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:464
		{
			yyVAL.expr = &BinaryExpr{Left: yyDollar[1].expr, Operator: ShiftRightStr, Right: yyDollar[3].expr}
		}
	case 67:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:468
		{
			yyVAL.expr = &BinaryExpr{Left: yyDollar[1].colName, Operator: JSONExtractOp, Right: yyDollar[3].expr}
		}
	case 68:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:472
		{
			yyVAL.expr = &BinaryExpr{Left: yyDollar[1].colName, Operator: JSONUnquoteExtractOp, Right: yyDollar[3].expr}
		}
	case 69:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:476
		{
			yyVAL.expr = &BinaryExpr{Left: yyDollar[1].expr, Operator: CollateStr, Right: yyDollar[3].expr}
		}
	case 70:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:480
		{
			if num, ok := yyDollar[2].expr.(*SQLVal); ok && num.Type == IntVal {
				yyVAL.expr = num
			} else {
				yyVAL.expr = &UnaryExpr{Operator: UPlusStr, Expr: yyDollar[2].expr}
			}
		}
	case 71:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:488
		{
			if num, ok := yyDollar[2].expr.(*SQLVal); ok && num.Type == IntVal {
				// Handle double negative
				if num.Val[0] == '-' {
					num.Val = num.Val[1:]
					yyVAL.expr = num
				} else {
					yyVAL.expr = NewIntVal(append([]byte("-"), num.Val...))
				}
			} else {
				yyVAL.expr = &UnaryExpr{Operator: UMinusStr, Expr: yyDollar[2].expr}
			}
		}
	case 72:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:502
		{
			yyVAL.expr = &UnaryExpr{Operator: TildaStr, Expr: yyDollar[2].expr}
		}
	case 73:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:506
		{
			// This rule prevents the usage of INTERVAL
			// as a function. If support is needed for that,
			// we'll need to revisit this. The solution
			// will be non-trivial because of grammar conflicts.
			yyVAL.expr = &IntervalExpr{Expr: yyDollar[2].expr, Unit: yyDollar[3].colIdent}
		}
	case 74:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:514
		{
			yyVAL.expr = &FuncExpr{Name: yyDollar[1].colIdent}
		}
	case 75:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:518
		{
			yyVAL.expr = &FuncExpr{Name: yyDollar[1].colIdent, Exprs: yyDollar[3].selectExprs}
		}
	case 76:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:522
		{
			yyVAL.expr = &FuncExpr{Name: yyDollar[1].colIdent, Distinct: true, Exprs: yyDollar[4].selectExprs}
		}
	case 77:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:526
		{
			yyVAL.expr = &FuncExpr{Name: yyDollar[1].colIdent}
		}
	case 78:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:530
		{
			yyVAL.expr = &FuncExpr{Name: yyDollar[1].colIdent, Exprs: yyDollar[3].selectExprs}
		}
	case 79:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:534
		{
			yyVAL.expr = yyDollar[1].expr
		}
	case 80:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:538
		{
			yyVAL.expr = &AndExpr{Left: yyDollar[1].expr, Right: yyDollar[3].expr}
		}
	case 81:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:542
		{
			yyVAL.expr = &OrExpr{Left: yyDollar[1].expr, Right: yyDollar[3].expr}
		}
	case 82:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:546
		{
			yyVAL.expr = &NotExpr{Expr: yyDollar[2].expr}
		}
	case 83:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:550
		{
			yyVAL.expr = &IsExpr{Operator: yyDollar[3].str, Expr: yyDollar[1].expr}
		}
	case 84:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:554
		{
			yyVAL.expr = yyDollar[1].boolVal
		}
	case 85:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:558
		{
			yyVAL.expr = &ComparisonExpr{Left: yyDollar[1].expr, Operator: yyDollar[2].empty, Right: yyDollar[3].expr}
		}
	case 86:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:562
		{
			yyVAL.expr = &ComparisonExpr{Left: yyDollar[1].expr, Operator: InStr, Right: yyDollar[3].colTuple}
		}
	case 87:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:566
		{
			yyVAL.expr = &ComparisonExpr{Left: yyDollar[1].expr, Operator: NotInStr, Right: yyDollar[4].colTuple}
		}
	case 88:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:570
		{
			yyVAL.expr = &ComparisonExpr{Left: yyDollar[1].expr, Operator: LikeStr, Right: yyDollar[3].expr}
		}
	case 89:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:574
		{
			yyVAL.expr = &ComparisonExpr{Left: yyDollar[1].expr, Operator: NotLikeStr, Right: yyDollar[4].expr}
		}
	case 90:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:578
		{
			yyVAL.expr = &ComparisonExpr{Left: yyDollar[1].expr, Operator: RegexpStr, Right: yyDollar[3].expr}
		}
	case 91:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:582
		{
			yyVAL.expr = &ComparisonExpr{Left: yyDollar[1].expr, Operator: NotRegexpStr, Right: yyDollar[4].expr}
		}
	case 92:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:586
		{
			yyVAL.expr = &RangeCond{Left: yyDollar[1].expr, Operator: BetweenStr, From: yyDollar[3].expr, To: yyDollar[5].expr}
		}
	case 93:
		yyDollar = yyS[yypt-6 : yypt+1]
		//line sql.y:590
		{
			yyVAL.expr = &RangeCond{Left: yyDollar[1].expr, Operator: NotBetweenStr, From: yyDollar[4].expr, To: yyDollar[6].expr}
		}
	case 94:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:594
		{
			yyVAL.expr = &ExistsExpr{Subquery: yyDollar[2].subquery}
		}
	case 95:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:599
		{
			yyVAL.colIdent = ColIdent{}
		}
	case 96:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:603
		{
			yyVAL.colIdent = yyDollar[1].colIdent
		}
	case 97:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:607
		{
			yyVAL.colIdent = yyDollar[2].colIdent
		}
	case 99:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:614
		{
			yyVAL.colIdent = NewColIdent(string(yyDollar[1].bytes))
		}
	case 100:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:619
		{
			yyVAL.tableExprs = TableExprs{&AliasedTableExpr{Expr: &TableName{Name: NewTableIdent("dual")}}}
		}
	case 101:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:623
		{
			yyVAL.tableExprs = yyDollar[2].tableExprs
		}
	case 102:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:629
		{
			yyVAL.tableExprs = TableExprs{yyDollar[1].tableExpr}
		}
	case 103:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:633
		{
			yyVAL.tableExprs = append(yyVAL.tableExprs, yyDollar[3].tableExpr)
		}
	case 106:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:643
		{
			yyVAL.tableExpr = &AliasedTableExpr{Expr: yyDollar[1].tableName, As: yyDollar[2].tableIdent, Hints: yyDollar[3].indexHints}
		}
	case 107:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:647
		{
			yyVAL.tableExpr = &AliasedTableExpr{Expr: yyDollar[1].subquery, As: yyDollar[3].tableIdent}
		}
	case 108:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:651
		{
			yyVAL.tableExpr = &ParenTableExpr{Exprs: yyDollar[2].tableExprs}
		}
	case 109:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:664
		{
			yyVAL.tableExpr = &JoinTableExpr{LeftExpr: yyDollar[1].tableExpr, Join: yyDollar[2].str, RightExpr: yyDollar[3].tableExpr}
		}
	case 110:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:668
		{
			yyVAL.tableExpr = &JoinTableExpr{LeftExpr: yyDollar[1].tableExpr, Join: yyDollar[2].str, RightExpr: yyDollar[3].tableExpr, On: yyDollar[5].expr}
		}
	case 111:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:672
		{
			yyVAL.tableExpr = &JoinTableExpr{LeftExpr: yyDollar[1].tableExpr, Join: yyDollar[2].str, RightExpr: yyDollar[3].tableExpr, On: yyDollar[5].expr}
		}
	case 112:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:676
		{
			yyVAL.tableExpr = &JoinTableExpr{LeftExpr: yyDollar[1].tableExpr, Join: yyDollar[2].str, RightExpr: yyDollar[3].tableExpr}
		}
	case 113:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:681
		{
			yyVAL.empty = struct{}{}
		}
	case 114:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:683
		{
			yyVAL.empty = struct{}{}
		}
	case 115:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:686
		{
			yyVAL.tableIdent = NewTableIdent("")
		}
	case 116:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:690
		{
			yyVAL.tableIdent = yyDollar[1].tableIdent
		}
	case 117:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:694
		{
			yyVAL.tableIdent = yyDollar[2].tableIdent
		}
	case 119:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:701
		{
			yyVAL.tableIdent = NewTableIdent(string(yyDollar[1].bytes))
		}
	case 120:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:707
		{
			yyVAL.str = JoinStr
		}
	case 121:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:711
		{
			yyVAL.str = JoinStr
		}
	case 122:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:715
		{
			yyVAL.str = JoinStr
		}
	case 123:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:719
		{
			yyVAL.str = StraightJoinStr
		}
	case 124:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:725
		{
			yyVAL.str = LeftJoinStr
		}
	case 125:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:729
		{
			yyVAL.str = LeftJoinStr
		}
	case 126:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:733
		{
			yyVAL.str = RightJoinStr
		}
	case 127:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:737
		{
			yyVAL.str = RightJoinStr
		}
	case 128:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:743
		{
			yyVAL.str = NaturalJoinStr
		}
	case 129:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:747
		{
			if yyDollar[2].str == LeftJoinStr {
				yyVAL.str = NaturalLeftJoinStr
			} else {
				yyVAL.str = NaturalRightJoinStr
			}
		}
	case 130:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:757
		{
			yyVAL.tableName = yyDollar[2].tableName
		}
	case 131:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:761
		{
			yyVAL.tableName = yyDollar[1].tableName
		}
	case 132:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:767
		{
			yyVAL.tableName = &TableName{Name: yyDollar[1].tableIdent}
		}
	case 133:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:771
		{
			yyVAL.tableName = &TableName{Qualifier: yyDollar[1].tableIdent, Name: yyDollar[3].tableIdent}
		}
	case 134:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:776
		{
			yyVAL.indexHints = nil
		}
	case 135:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:780
		{
			yyVAL.indexHints = &IndexHints{Type: UseStr, Indexes: yyDollar[4].colIdents}
		}
	case 136:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:784
		{
			yyVAL.indexHints = &IndexHints{Type: IgnoreStr, Indexes: yyDollar[4].colIdents}
		}
	case 137:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:788
		{
			yyVAL.indexHints = &IndexHints{Type: ForceStr, Indexes: yyDollar[4].colIdents}
		}
	case 138:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:794
		{
			yyVAL.colIdents = []ColIdent{yyDollar[1].colIdent}
		}
	case 139:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:798
		{
			yyVAL.colIdents = append(yyDollar[1].colIdents, yyDollar[3].colIdent)
		}
	case 140:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:803
		{
			yyVAL.expr = nil
		}
	case 141:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:807
		{
			yyVAL.expr = yyDollar[2].expr
		}
	case 142:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:813
		{
			yyVAL.boolVal = BoolVal(true)
		}
	case 143:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:817
		{
			yyVAL.boolVal = BoolVal(false)
		}
	case 144:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:823
		{
			yyVAL.str = IsNullStr
		}
	case 145:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:827
		{
			yyVAL.str = IsNotNullStr
		}
	case 146:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:831
		{
			yyVAL.str = IsTrueStr
		}
	case 147:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:835
		{
			yyVAL.str = IsNotTrueStr
		}
	case 148:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:839
		{
			yyVAL.str = IsFalseStr
		}
	case 149:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:843
		{
			yyVAL.str = IsNotFalseStr
		}
	case 150:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:849
		{
			yyVAL.str = EqualStr
		}
	case 151:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:853
		{
			yyVAL.str = LessThanStr
		}
	case 152:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:857
		{
			yyVAL.str = GreaterThanStr
		}
	case 153:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:861
		{
			yyVAL.str = LessEqualStr
		}
	case 154:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:865
		{
			yyVAL.str = GreaterEqualStr
		}
	case 155:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:869
		{
			yyVAL.str = NotEqualStr
		}
	case 156:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:873
		{
			yyVAL.str = NullSafeEqualStr
		}
	case 157:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:879
		{
			yyVAL.colTuple = ValTuple(yyDollar[2].valExprs)
		}
	case 158:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:883
		{
			yyVAL.colTuple = yyDollar[1].subquery
		}
	case 159:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:887
		{
			yyVAL.colTuple = ListArg(yyDollar[1].bytes)
		}
	case 160:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:893
		{
			yyVAL.subquery = &Subquery{yyDollar[2].selStmt}
		}
	case 161:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:899
		{
			yyVAL.valExprs = ValExprs{yyDollar[1].expr}
		}
	case 162:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:903
		{
			yyVAL.valExprs = append(yyDollar[1].valExprs, yyDollar[3].expr)
		}
	case 163:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:909
		{
			yyVAL.colIdent = NewColIdent("if")
		}
	case 164:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:913
		{
			yyVAL.colIdent = NewColIdent("current_timestamp")
		}
	case 165:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:917
		{
			yyVAL.colIdent = NewColIdent("database")
		}
	case 166:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:921
		{
			yyVAL.colIdent = NewColIdent("mod")
		}
	case 167:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:927
		{
			yyVAL.expr = &CaseExpr{Expr: yyDollar[2].expr, Whens: yyDollar[3].whens, Else: yyDollar[4].expr}
		}
	case 168:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:932
		{
			yyVAL.expr = nil
		}
	case 169:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:936
		{
			yyVAL.expr = yyDollar[1].expr
		}
	case 170:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:942
		{
			yyVAL.whens = []*When{yyDollar[1].when}
		}
	case 171:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:946
		{
			yyVAL.whens = append(yyDollar[1].whens, yyDollar[2].when)
		}
	case 172:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:952
		{
			yyVAL.when = &When{Cond: yyDollar[2].expr, Val: yyDollar[4].expr}
		}
	case 173:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:957
		{
			yyVAL.expr = nil
		}
	case 174:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:961
		{
			yyVAL.expr = yyDollar[2].expr
		}
	case 175:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:967
		{
			yyVAL.colName = &ColName{Name: yyDollar[1].colIdent}
		}
	case 176:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:971
		{
			yyVAL.colName = &ColName{Qualifier: &TableName{Name: yyDollar[1].tableIdent}, Name: yyDollar[3].colIdent}
		}
	case 177:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:975
		{
			yyVAL.colName = &ColName{Qualifier: &TableName{Qualifier: yyDollar[1].tableIdent, Name: yyDollar[3].tableIdent}, Name: yyDollar[5].colIdent}
		}
	case 178:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:981
		{
			yyVAL.expr = NewStrVal(yyDollar[1].bytes)
		}
	case 179:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:985
		{
			yyVAL.expr = NewHexVal(yyDollar[1].bytes)
		}
	case 180:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:989
		{
			yyVAL.expr = NewIntVal(yyDollar[1].bytes)
		}
	case 181:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:993
		{
			yyVAL.expr = NewFloatVal(yyDollar[1].bytes)
		}
	case 182:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:997
		{
			yyVAL.expr = NewHexNum(yyDollar[1].bytes)
		}
	case 183:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1001
		{
			yyVAL.expr = NewValArg(yyDollar[1].bytes)
		}
	case 184:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1005
		{
			yyVAL.expr = &NullVal{}
		}
	case 185:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1011
		{
			// TODO(sougou): Deprecate this construct.
			if yyDollar[1].colIdent.Lowered() != "value" {
				yylex.Error("expecting value after next")
				return 1
			}
			yyVAL.expr = NewIntVal([]byte("1"))
		}
	case 186:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:1020
		{
			yyVAL.expr = NewIntVal(yyDollar[1].bytes)
		}
	case 187:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:1024
		{
			yyVAL.expr = NewValArg(yyDollar[1].bytes)
		}
	case 188:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1029
		{
			yyVAL.valExprs = nil
		}
	case 189:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1033
		{
			yyVAL.valExprs = yyDollar[3].valExprs
		}
	case 190:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1038
		{
			yyVAL.expr = nil
		}
	case 191:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:1042
		{
			yyVAL.expr = yyDollar[2].expr
		}
	case 192:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1047
		{
			yyVAL.orderBy = nil
		}
	case 193:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1051
		{
			yyVAL.orderBy = yyDollar[3].orderBy
		}
	case 194:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1057
		{
			yyVAL.orderBy = OrderBy{yyDollar[1].order}
		}
	case 195:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1061
		{
			yyVAL.orderBy = append(yyDollar[1].orderBy, yyDollar[3].order)
		}
	case 196:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:1067
		{
			yyVAL.order = &Order{Expr: yyDollar[1].expr, Direction: yyDollar[2].str}
		}
	case 197:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1072
		{
			yyVAL.str = AscScr
		}
	case 198:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1076
		{
			yyVAL.str = AscScr
		}
	case 199:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1080
		{
			yyVAL.str = DescScr
		}
	case 200:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1085
		{
			yyVAL.limit = nil
		}
	case 201:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:1089
		{
			yyVAL.limit = &Limit{Rowcount: yyDollar[2].expr}
		}
	case 202:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:1093
		{
			yyVAL.limit = &Limit{Offset: yyDollar[2].expr, Rowcount: yyDollar[4].expr}
		}
	case 203:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:1097
		{
			yyVAL.limit = &Limit{Offset: yyDollar[4].expr, Rowcount: yyDollar[2].expr}
		}
	case 204:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1102
		{
			yyVAL.str = ""
		}
	case 205:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:1106
		{
			yyVAL.str = ForUpdateStr
		}
	case 206:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line sql.y:1110
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
	case 207:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1123
		{
			yyVAL.columns = nil
		}
	case 208:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1127
		{
			yyVAL.columns = yyDollar[2].columns
		}
	case 209:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1133
		{
			yyVAL.columns = Columns{yyDollar[1].colIdent}
		}
	case 210:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1137
		{
			yyVAL.columns = append(yyVAL.columns, yyDollar[3].colIdent)
		}
	case 211:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1142
		{
			yyVAL.updateExprs = nil
		}
	case 212:
		yyDollar = yyS[yypt-5 : yypt+1]
		//line sql.y:1146
		{
			yyVAL.updateExprs = yyDollar[5].updateExprs
		}
	case 213:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:1152
		{
			yyVAL.insRows = yyDollar[2].values
		}
	case 214:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1156
		{
			yyVAL.insRows = yyDollar[1].selStmt
		}
	case 215:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1162
		{
			yyVAL.values = Values{yyDollar[1].valTuple}
		}
	case 216:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1166
		{
			yyVAL.values = append(yyDollar[1].values, yyDollar[3].valTuple)
		}
	case 217:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1172
		{
			yyVAL.valTuple = ValTuple(yyDollar[2].valExprs)
		}
	case 218:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1178
		{
			yyVAL.updateExprs = UpdateExprs{yyDollar[1].updateExpr}
		}
	case 219:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1182
		{
			yyVAL.updateExprs = append(yyDollar[1].updateExprs, yyDollar[3].updateExpr)
		}
	case 220:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1188
		{
			yyVAL.updateExpr = &UpdateExpr{Name: yyDollar[1].colIdent, Expr: yyDollar[3].expr}
		}
	case 223:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1197
		{
			yyVAL.byt = 0
		}
	case 224:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:1199
		{
			yyVAL.byt = 1
		}
	case 225:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1202
		{
			yyVAL.empty = struct{}{}
		}
	case 226:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line sql.y:1204
		{
			yyVAL.empty = struct{}{}
		}
	case 227:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1207
		{
			yyVAL.str = ""
		}
	case 228:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1209
		{
			yyVAL.str = IgnoreStr
		}
	case 229:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1213
		{
			yyVAL.empty = struct{}{}
		}
	case 230:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1215
		{
			yyVAL.empty = struct{}{}
		}
	case 231:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1217
		{
			yyVAL.empty = struct{}{}
		}
	case 232:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1219
		{
			yyVAL.empty = struct{}{}
		}
	case 233:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1221
		{
			yyVAL.empty = struct{}{}
		}
	case 234:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1223
		{
			yyVAL.empty = struct{}{}
		}
	case 235:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1226
		{
			yyVAL.empty = struct{}{}
		}
	case 236:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1228
		{
			yyVAL.empty = struct{}{}
		}
	case 237:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1231
		{
			yyVAL.empty = struct{}{}
		}
	case 238:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1233
		{
			yyVAL.empty = struct{}{}
		}
	case 239:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1236
		{
			yyVAL.empty = struct{}{}
		}
	case 240:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line sql.y:1238
		{
			yyVAL.empty = struct{}{}
		}
	case 241:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1242
		{
			yyVAL.colIdent = NewColIdent(string(yyDollar[1].bytes))
		}
	case 242:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1248
		{
			yyVAL.tableIdent = NewTableIdent(string(yyDollar[1].bytes))
		}
	case 243:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1254
		{
			if incNesting(yylex) {
				yylex.Error("max nesting level reached")
				return 1
			}
		}
	case 244:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line sql.y:1263
		{
			decNesting(yylex)
		}
	case 245:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line sql.y:1268
		{
			forceEOF(yylex)
		}
	}
	goto yystack /* stack new state and value */
}
