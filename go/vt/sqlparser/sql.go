/* A Bison parser, made by GNU Bison 2.3.  */

/* Skeleton implementation for Bison's Yacc-like parsers in C

   Copyright (C) 1984, 1989, 1990, 2000, 2001, 2002, 2003, 2004, 2005, 2006
   Free Software Foundation, Inc.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; either version 2, or (at your option)
   any later version.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin Street, Fifth Floor,
   Boston, MA 02110-1301, USA.  */

/* As a special exception, you may create a larger work that contains
   part or all of the Bison parser skeleton and distribute that work
   under terms of your choice, so long as that work isn't itself a
   parser generator using the skeleton or a modified version thereof
   as a parser skeleton.  Alternatively, if you modify or redistribute
   the parser skeleton itself, you may (at your option) remove this
   special exception, which will cause the skeleton and the resulting
   Bison output files to be licensed under the GNU General Public
   License without this special exception.

   This special exception was added by the Free Software Foundation in
   version 2.2 of Bison.  */

/* C LALR(1) parser skeleton written by Richard Stallman, by
   simplifying the original so-called "semantic" parser.  */

/* All symbols defined below should begin with yy or YY, to avoid
   infringing on user name space.  This should be done even for local
   variables, as they might otherwise be expanded by user macros.
   There are some unavoidable exceptions within include files to
   define necessary library symbols; they are noted "INFRINGES ON
   USER NAME SPACE" below.  */

/* Identify Bison output.  */
#define YYBISON 1

/* Bison version.  */
#define YYBISON_VERSION "2.3"

/* Skeleton name.  */
#define YYSKELETON_NAME "yacc.c"

/* Pure parsers.  */
#define YYPURE 0

/* Using locations.  */
#define YYLSP_NEEDED 0



/* Tokens.  */
#ifndef YYTOKENTYPE
# define YYTOKENTYPE
   /* Put the tokens into the symbol table, so that GDB and other debuggers
      know about them.  */
   enum yytokentype {
     LEX_ERROR = 258,
     UNION = 259,
     SELECT = 260,
     STREAM = 261,
     INSERT = 262,
     UPDATE = 263,
     DELETE = 264,
     FROM = 265,
     WHERE = 266,
     GROUP = 267,
     HAVING = 268,
     ORDER = 269,
     BY = 270,
     LIMIT = 271,
     OFFSET = 272,
     FOR = 273,
     CALL = 274,
     ALL = 275,
     DISTINCT = 276,
     AS = 277,
     EXISTS = 278,
     ASC = 279,
     DESC = 280,
     INTO = 281,
     DUPLICATE = 282,
     DEFAULT = 283,
     SET = 284,
     LOCK = 285,
     UNLOCK = 286,
     KEYS = 287,
     OF = 288,
     OUTFILE = 289,
     DATA = 290,
     LOAD = 291,
     LINES = 292,
     TERMINATED = 293,
     ESCAPED = 294,
     ENCLOSED = 295,
     OPTIONALLY = 296,
     STARTING = 297,
     KEY = 298,
     UNIQUE = 299,
     SYSTEM_TIME = 300,
     VALUES = 301,
     LAST_INSERT_ID = 302,
     NEXT = 303,
     VALUE = 304,
     SHARE = 305,
     MODE = 306,
     SQL_NO_CACHE = 307,
     SQL_CACHE = 308,
     FORCE = 309,
     USE = 310,
     NATURAL = 311,
     CROSS = 312,
     OUTER = 313,
     INNER = 314,
     RIGHT = 315,
     LEFT = 316,
     STRAIGHT_JOIN = 317,
     JOIN = 318,
     USING = 319,
     ON = 320,
     ID = 321,
     HEX = 322,
     STRING = 323,
     INTEGRAL = 324,
     FLOAT = 325,
     HEXNUM = 326,
     VALUE_ARG = 327,
     LIST_ARG = 328,
     COMMENT = 329,
     COMMENT_KEYWORD = 330,
     BIT_LITERAL = 331,
     NULL = 332,
     TRUE = 333,
     FALSE = 334,
     OFF = 335,
     OR = 336,
     AND = 337,
     NOT = 338,
     END = 339,
     ELSEIF = 340,
     ELSE = 341,
     THEN = 342,
     WHEN = 343,
     CASE = 344,
     BETWEEN = 345,
     IN = 346,
     REGEXP = 347,
     LIKE = 348,
     IS = 349,
     NULL_SAFE_EQUAL = 350,
     NE = 351,
     GE = 352,
     LE = 353,
     SHIFT_RIGHT = 354,
     SHIFT_LEFT = 355,
     MOD = 356,
     DIV = 357,
     UNARY = 358,
     COLLATE = 359,
     UNDERSCORE_UTF8MB4 = 360,
     UNDERSCORE_BINARY = 361,
     BINARY = 362,
     INTERVAL = 363,
     JSON_EXTRACT_OP = 364,
     JSON_UNQUOTE_EXTRACT_OP = 365,
     CREATE = 366,
     ALTER = 367,
     DROP = 368,
     RENAME = 369,
     ANALYZE = 370,
     ADD = 371,
     FLUSH = 372,
     MODIFY = 373,
     CHANGE = 374,
     SCHEMA = 375,
     TABLE = 376,
     INDEX = 377,
     INDEXES = 378,
     VIEW = 379,
     TO = 380,
     IGNORE = 381,
     IF = 382,
     PRIMARY = 383,
     COLUMN = 384,
     SPATIAL = 385,
     FULLTEXT = 386,
     KEY_BLOCK_SIZE = 387,
     CHECK = 388,
     ACTION = 389,
     CASCADE = 390,
     CONSTRAINT = 391,
     FOREIGN = 392,
     NO = 393,
     REFERENCES = 394,
     RESTRICT = 395,
     FIRST = 396,
     AFTER = 397,
     SHOW = 398,
     DESCRIBE = 399,
     EXPLAIN = 400,
     DATE = 401,
     ESCAPE = 402,
     REPAIR = 403,
     OPTIMIZE = 404,
     TRUNCATE = 405,
     FORMAT = 406,
     MAXVALUE = 407,
     PARTITION = 408,
     REORGANIZE = 409,
     LESS = 410,
     THAN = 411,
     PROCEDURE = 412,
     TRIGGER = 413,
     TRIGGERS = 414,
     FUNCTION = 415,
     VINDEX = 416,
     VINDEXES = 417,
     STATUS = 418,
     VARIABLES = 419,
     WARNINGS = 420,
     SEQUENCE = 421,
     EACH = 422,
     ROW = 423,
     BEFORE = 424,
     FOLLOWS = 425,
     PRECEDES = 426,
     DEFINER = 427,
     INVOKER = 428,
     INOUT = 429,
     OUT = 430,
     DETERMINISTIC = 431,
     CONTAINS = 432,
     READS = 433,
     MODIFIES = 434,
     SQL = 435,
     SECURITY = 436,
     CLASS_ORIGIN = 437,
     SUBCLASS_ORIGIN = 438,
     MESSAGE_TEXT = 439,
     MYSQL_ERRNO = 440,
     CONSTRAINT_CATALOG = 441,
     CONSTRAINT_SCHEMA = 442,
     CONSTRAINT_NAME = 443,
     CATALOG_NAME = 444,
     SCHEMA_NAME = 445,
     TABLE_NAME = 446,
     COLUMN_NAME = 447,
     CURSOR_NAME = 448,
     SIGNAL = 449,
     SQLSTATE = 450,
     BEGIN = 451,
     START = 452,
     TRANSACTION = 453,
     COMMIT = 454,
     ROLLBACK = 455,
     BIT = 456,
     TINYINT = 457,
     SMALLINT = 458,
     MEDIUMINT = 459,
     INT = 460,
     INTEGER = 461,
     BIGINT = 462,
     INTNUM = 463,
     REAL = 464,
     DOUBLE = 465,
     FLOAT_TYPE = 466,
     DECIMAL = 467,
     NUMERIC = 468,
     DEC = 469,
     FIXED = 470,
     PRECISION = 471,
     TIME = 472,
     TIMESTAMP = 473,
     DATETIME = 474,
     YEAR = 475,
     CHAR = 476,
     VARCHAR = 477,
     BOOL = 478,
     CHARACTER = 479,
     VARBINARY = 480,
     NCHAR = 481,
     NVARCHAR = 482,
     NATIONAL = 483,
     VARYING = 484,
     TEXT = 485,
     TINYTEXT = 486,
     MEDIUMTEXT = 487,
     LONGTEXT = 488,
     LONG = 489,
     BLOB = 490,
     TINYBLOB = 491,
     MEDIUMBLOB = 492,
     LONGBLOB = 493,
     JSON = 494,
     ENUM = 495,
     GEOMETRY = 496,
     POINT = 497,
     LINESTRING = 498,
     POLYGON = 499,
     GEOMETRYCOLLECTION = 500,
     MULTIPOINT = 501,
     MULTILINESTRING = 502,
     MULTIPOLYGON = 503,
     NULLX = 504,
     AUTO_INCREMENT = 505,
     APPROXNUM = 506,
     SIGNED = 507,
     UNSIGNED = 508,
     ZEROFILL = 509,
     LOCAL = 510,
     COLLATION = 511,
     DATABASES = 512,
     SCHEMAS = 513,
     TABLES = 514,
     VITESS_METADATA = 515,
     VSCHEMA = 516,
     FULL = 517,
     PROCESSLIST = 518,
     COLUMNS = 519,
     FIELDS = 520,
     ENGINES = 521,
     PLUGINS = 522,
     NAMES = 523,
     CHARSET = 524,
     GLOBAL = 525,
     SESSION = 526,
     ISOLATION = 527,
     LEVEL = 528,
     READ = 529,
     WRITE = 530,
     ONLY = 531,
     REPEATABLE = 532,
     COMMITTED = 533,
     UNCOMMITTED = 534,
     SERIALIZABLE = 535,
     CURRENT_TIMESTAMP = 536,
     DATABASE = 537,
     CURRENT_DATE = 538,
     CURRENT_USER = 539,
     CURRENT_TIME = 540,
     LOCALTIME = 541,
     LOCALTIMESTAMP = 542,
     UTC_DATE = 543,
     UTC_TIME = 544,
     UTC_TIMESTAMP = 545,
     REPLACE = 546,
     CONVERT = 547,
     CAST = 548,
     SUBSTR = 549,
     SUBSTRING = 550,
     GROUP_CONCAT = 551,
     SEPARATOR = 552,
     TIMESTAMPADD = 553,
     TIMESTAMPDIFF = 554,
     OVER = 555,
     WINDOW = 556,
     GROUPING = 557,
     GROUPS = 558,
     AVG = 559,
     BIT_AND = 560,
     BIT_OR = 561,
     BIT_XOR = 562,
     COUNT = 563,
     JSON_ARRAYAGG = 564,
     JSON_OBJECTAGG = 565,
     MAX = 566,
     MIN = 567,
     STDDEV_POP = 568,
     STDDEV = 569,
     STD = 570,
     STDDEV_SAMP = 571,
     SUM = 572,
     VAR_POP = 573,
     VARIANCE = 574,
     VAR_SAMP = 575,
     CUME_DIST = 576,
     DENSE_RANK = 577,
     FIRST_VALUE = 578,
     LAG = 579,
     LAST_VALUE = 580,
     LEAD = 581,
     NTH_VALUE = 582,
     NTILE = 583,
     ROW_NUMBER = 584,
     PERCENT_RANK = 585,
     RANK = 586,
     MATCH = 587,
     AGAINST = 588,
     BOOLEAN = 589,
     LANGUAGE = 590,
     WITH = 591,
     QUERY = 592,
     EXPANSION = 593,
     UNUSED = 594,
     ARRAY = 595,
     DESCRIPTION = 596,
     EMPTY = 597,
     EXCEPT = 598,
     JSON_TABLE = 599,
     LATERAL = 600,
     MEMBER = 601,
     RECURSIVE = 602,
     ACTIVE = 603,
     ADMIN = 604,
     BUCKETS = 605,
     CLONE = 606,
     COMPONENT = 607,
     DEFINITION = 608,
     ENFORCED = 609,
     EXCLUDE = 610,
     FOLLOWING = 611,
     GEOMCOLLECTION = 612,
     GET_MASTER_PUBLIC_KEY = 613,
     HISTOGRAM = 614,
     HISTORY = 615,
     INACTIVE = 616,
     INVISIBLE = 617,
     LOCKED = 618,
     MASTER_COMPRESSION_ALGORITHMS = 619,
     MASTER_PUBLIC_KEY_PATH = 620,
     MASTER_TLS_CIPHERSUITES = 621,
     MASTER_ZSTD_COMPRESSION_LEVEL = 622,
     NESTED = 623,
     NETWORK_NAMESPACE = 624,
     NOWAIT = 625,
     NULLS = 626,
     OJ = 627,
     OLD = 628,
     OPTIONAL = 629,
     ORDINALITY = 630,
     ORGANIZATION = 631,
     OTHERS = 632,
     PATH = 633,
     PERSIST = 634,
     PERSIST_ONLY = 635,
     PRECEDING = 636,
     PRIVILEGE_CHECKS_USER = 637,
     PROCESS = 638,
     RANDOM = 639,
     REFERENCE = 640,
     REQUIRE_ROW_FORMAT = 641,
     RESOURCE = 642,
     RESPECT = 643,
     RESTART = 644,
     RETAIN = 645,
     REUSE = 646,
     ROLE = 647,
     SECONDARY = 648,
     SECONDARY_ENGINE = 649,
     SECONDARY_LOAD = 650,
     SECONDARY_UNLOAD = 651,
     SKIP = 652,
     SRID = 653,
     THREAD_PRIORITY = 654,
     TIES = 655,
     UNBOUNDED = 656,
     VCPU = 657,
     VISIBLE = 658,
     SYSTEM = 659,
     INFILE = 660
   };
#endif
/* Tokens.  */
#define LEX_ERROR 258
#define UNION 259
#define SELECT 260
#define STREAM 261
#define INSERT 262
#define UPDATE 263
#define DELETE 264
#define FROM 265
#define WHERE 266
#define GROUP 267
#define HAVING 268
#define ORDER 269
#define BY 270
#define LIMIT 271
#define OFFSET 272
#define FOR 273
#define CALL 274
#define ALL 275
#define DISTINCT 276
#define AS 277
#define EXISTS 278
#define ASC 279
#define DESC 280
#define INTO 281
#define DUPLICATE 282
#define DEFAULT 283
#define SET 284
#define LOCK 285
#define UNLOCK 286
#define KEYS 287
#define OF 288
#define OUTFILE 289
#define DATA 290
#define LOAD 291
#define LINES 292
#define TERMINATED 293
#define ESCAPED 294
#define ENCLOSED 295
#define OPTIONALLY 296
#define STARTING 297
#define KEY 298
#define UNIQUE 299
#define SYSTEM_TIME 300
#define VALUES 301
#define LAST_INSERT_ID 302
#define NEXT 303
#define VALUE 304
#define SHARE 305
#define MODE 306
#define SQL_NO_CACHE 307
#define SQL_CACHE 308
#define FORCE 309
#define USE 310
#define NATURAL 311
#define CROSS 312
#define OUTER 313
#define INNER 314
#define RIGHT 315
#define LEFT 316
#define STRAIGHT_JOIN 317
#define JOIN 318
#define USING 319
#define ON 320
#define ID 321
#define HEX 322
#define STRING 323
#define INTEGRAL 324
#define FLOAT 325
#define HEXNUM 326
#define VALUE_ARG 327
#define LIST_ARG 328
#define COMMENT 329
#define COMMENT_KEYWORD 330
#define BIT_LITERAL 331
#define NULL 332
#define TRUE 333
#define FALSE 334
#define OFF 335
#define OR 336
#define AND 337
#define NOT 338
#define END 339
#define ELSEIF 340
#define ELSE 341
#define THEN 342
#define WHEN 343
#define CASE 344
#define BETWEEN 345
#define IN 346
#define REGEXP 347
#define LIKE 348
#define IS 349
#define NULL_SAFE_EQUAL 350
#define NE 351
#define GE 352
#define LE 353
#define SHIFT_RIGHT 354
#define SHIFT_LEFT 355
#define MOD 356
#define DIV 357
#define UNARY 358
#define COLLATE 359
#define UNDERSCORE_UTF8MB4 360
#define UNDERSCORE_BINARY 361
#define BINARY 362
#define INTERVAL 363
#define JSON_EXTRACT_OP 364
#define JSON_UNQUOTE_EXTRACT_OP 365
#define CREATE 366
#define ALTER 367
#define DROP 368
#define RENAME 369
#define ANALYZE 370
#define ADD 371
#define FLUSH 372
#define MODIFY 373
#define CHANGE 374
#define SCHEMA 375
#define TABLE 376
#define INDEX 377
#define INDEXES 378
#define VIEW 379
#define TO 380
#define IGNORE 381
#define IF 382
#define PRIMARY 383
#define COLUMN 384
#define SPATIAL 385
#define FULLTEXT 386
#define KEY_BLOCK_SIZE 387
#define CHECK 388
#define ACTION 389
#define CASCADE 390
#define CONSTRAINT 391
#define FOREIGN 392
#define NO 393
#define REFERENCES 394
#define RESTRICT 395
#define FIRST 396
#define AFTER 397
#define SHOW 398
#define DESCRIBE 399
#define EXPLAIN 400
#define DATE 401
#define ESCAPE 402
#define REPAIR 403
#define OPTIMIZE 404
#define TRUNCATE 405
#define FORMAT 406
#define MAXVALUE 407
#define PARTITION 408
#define REORGANIZE 409
#define LESS 410
#define THAN 411
#define PROCEDURE 412
#define TRIGGER 413
#define TRIGGERS 414
#define FUNCTION 415
#define VINDEX 416
#define VINDEXES 417
#define STATUS 418
#define VARIABLES 419
#define WARNINGS 420
#define SEQUENCE 421
#define EACH 422
#define ROW 423
#define BEFORE 424
#define FOLLOWS 425
#define PRECEDES 426
#define DEFINER 427
#define INVOKER 428
#define INOUT 429
#define OUT 430
#define DETERMINISTIC 431
#define CONTAINS 432
#define READS 433
#define MODIFIES 434
#define SQL 435
#define SECURITY 436
#define CLASS_ORIGIN 437
#define SUBCLASS_ORIGIN 438
#define MESSAGE_TEXT 439
#define MYSQL_ERRNO 440
#define CONSTRAINT_CATALOG 441
#define CONSTRAINT_SCHEMA 442
#define CONSTRAINT_NAME 443
#define CATALOG_NAME 444
#define SCHEMA_NAME 445
#define TABLE_NAME 446
#define COLUMN_NAME 447
#define CURSOR_NAME 448
#define SIGNAL 449
#define SQLSTATE 450
#define BEGIN 451
#define START 452
#define TRANSACTION 453
#define COMMIT 454
#define ROLLBACK 455
#define BIT 456
#define TINYINT 457
#define SMALLINT 458
#define MEDIUMINT 459
#define INT 460
#define INTEGER 461
#define BIGINT 462
#define INTNUM 463
#define REAL 464
#define DOUBLE 465
#define FLOAT_TYPE 466
#define DECIMAL 467
#define NUMERIC 468
#define DEC 469
#define FIXED 470
#define PRECISION 471
#define TIME 472
#define TIMESTAMP 473
#define DATETIME 474
#define YEAR 475
#define CHAR 476
#define VARCHAR 477
#define BOOL 478
#define CHARACTER 479
#define VARBINARY 480
#define NCHAR 481
#define NVARCHAR 482
#define NATIONAL 483
#define VARYING 484
#define TEXT 485
#define TINYTEXT 486
#define MEDIUMTEXT 487
#define LONGTEXT 488
#define LONG 489
#define BLOB 490
#define TINYBLOB 491
#define MEDIUMBLOB 492
#define LONGBLOB 493
#define JSON 494
#define ENUM 495
#define GEOMETRY 496
#define POINT 497
#define LINESTRING 498
#define POLYGON 499
#define GEOMETRYCOLLECTION 500
#define MULTIPOINT 501
#define MULTILINESTRING 502
#define MULTIPOLYGON 503
#define NULLX 504
#define AUTO_INCREMENT 505
#define APPROXNUM 506
#define SIGNED 507
#define UNSIGNED 508
#define ZEROFILL 509
#define LOCAL 510
#define COLLATION 511
#define DATABASES 512
#define SCHEMAS 513
#define TABLES 514
#define VITESS_METADATA 515
#define VSCHEMA 516
#define FULL 517
#define PROCESSLIST 518
#define COLUMNS 519
#define FIELDS 520
#define ENGINES 521
#define PLUGINS 522
#define NAMES 523
#define CHARSET 524
#define GLOBAL 525
#define SESSION 526
#define ISOLATION 527
#define LEVEL 528
#define READ 529
#define WRITE 530
#define ONLY 531
#define REPEATABLE 532
#define COMMITTED 533
#define UNCOMMITTED 534
#define SERIALIZABLE 535
#define CURRENT_TIMESTAMP 536
#define DATABASE 537
#define CURRENT_DATE 538
#define CURRENT_USER 539
#define CURRENT_TIME 540
#define LOCALTIME 541
#define LOCALTIMESTAMP 542
#define UTC_DATE 543
#define UTC_TIME 544
#define UTC_TIMESTAMP 545
#define REPLACE 546
#define CONVERT 547
#define CAST 548
#define SUBSTR 549
#define SUBSTRING 550
#define GROUP_CONCAT 551
#define SEPARATOR 552
#define TIMESTAMPADD 553
#define TIMESTAMPDIFF 554
#define OVER 555
#define WINDOW 556
#define GROUPING 557
#define GROUPS 558
#define AVG 559
#define BIT_AND 560
#define BIT_OR 561
#define BIT_XOR 562
#define COUNT 563
#define JSON_ARRAYAGG 564
#define JSON_OBJECTAGG 565
#define MAX 566
#define MIN 567
#define STDDEV_POP 568
#define STDDEV 569
#define STD 570
#define STDDEV_SAMP 571
#define SUM 572
#define VAR_POP 573
#define VARIANCE 574
#define VAR_SAMP 575
#define CUME_DIST 576
#define DENSE_RANK 577
#define FIRST_VALUE 578
#define LAG 579
#define LAST_VALUE 580
#define LEAD 581
#define NTH_VALUE 582
#define NTILE 583
#define ROW_NUMBER 584
#define PERCENT_RANK 585
#define RANK 586
#define MATCH 587
#define AGAINST 588
#define BOOLEAN 589
#define LANGUAGE 590
#define WITH 591
#define QUERY 592
#define EXPANSION 593
#define UNUSED 594
#define ARRAY 595
#define DESCRIPTION 596
#define EMPTY 597
#define EXCEPT 598
#define JSON_TABLE 599
#define LATERAL 600
#define MEMBER 601
#define RECURSIVE 602
#define ACTIVE 603
#define ADMIN 604
#define BUCKETS 605
#define CLONE 606
#define COMPONENT 607
#define DEFINITION 608
#define ENFORCED 609
#define EXCLUDE 610
#define FOLLOWING 611
#define GEOMCOLLECTION 612
#define GET_MASTER_PUBLIC_KEY 613
#define HISTOGRAM 614
#define HISTORY 615
#define INACTIVE 616
#define INVISIBLE 617
#define LOCKED 618
#define MASTER_COMPRESSION_ALGORITHMS 619
#define MASTER_PUBLIC_KEY_PATH 620
#define MASTER_TLS_CIPHERSUITES 621
#define MASTER_ZSTD_COMPRESSION_LEVEL 622
#define NESTED 623
#define NETWORK_NAMESPACE 624
#define NOWAIT 625
#define NULLS 626
#define OJ 627
#define OLD 628
#define OPTIONAL 629
#define ORDINALITY 630
#define ORGANIZATION 631
#define OTHERS 632
#define PATH 633
#define PERSIST 634
#define PERSIST_ONLY 635
#define PRECEDING 636
#define PRIVILEGE_CHECKS_USER 637
#define PROCESS 638
#define RANDOM 639
#define REFERENCE 640
#define REQUIRE_ROW_FORMAT 641
#define RESOURCE 642
#define RESPECT 643
#define RESTART 644
#define RETAIN 645
#define REUSE 646
#define ROLE 647
#define SECONDARY 648
#define SECONDARY_ENGINE 649
#define SECONDARY_LOAD 650
#define SECONDARY_UNLOAD 651
#define SKIP 652
#define SRID 653
#define THREAD_PRIORITY 654
#define TIES 655
#define UNBOUNDED 656
#define VCPU 657
#define VISIBLE 658
#define SYSTEM 659
#define INFILE 660




/* Copy the first part of user declarations.  */
#line 17 "go/vt/sqlparser/sql.y"

package sqlparser

func setParseTree(yylex interface{}, stmt Statement) {
  yylex.(*Tokenizer).ParseTree = stmt
}

func setAllowComments(yylex interface{}, allow bool) {
  yylex.(*Tokenizer).AllowComments = allow
}

func setDDL(yylex interface{}, ddl *DDL) {
  yylex.(*Tokenizer).partialDDL = ddl
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

func yyPosition(yylex interface{}) int {
  return yylex.(*Tokenizer).Position
}

// skipToEnd forces the lexer to end prematurely. Not all SQL statements
// are supported by the Parser, thus calling skipToEnd will make the lexer
// return EOF early.
func skipToEnd(yylex interface{}) {
  yylex.(*Tokenizer).SkipToEnd = true
}



/* Enabling traces.  */
#ifndef YYDEBUG
# define YYDEBUG 0
#endif

/* Enabling verbose error messages.  */
#ifdef YYERROR_VERBOSE
# undef YYERROR_VERBOSE
# define YYERROR_VERBOSE 1
#else
# define YYERROR_VERBOSE 0
#endif

/* Enabling the token table.  */
#ifndef YYTOKEN_TABLE
# define YYTOKEN_TABLE 0
#endif

#if ! defined YYSTYPE && ! defined YYSTYPE_IS_DECLARED
typedef union YYSTYPE
#line 57 "go/vt/sqlparser/sql.y"
{
  empty         struct{}
  statement     Statement
  selStmt       SelectStatement
  ddl           *DDL
  ins           *Insert
  byt           byte
  bytes         []byte
  bytes2        [][]byte
  str           string
  int           int
  strs          []string
  selectExprs   SelectExprs
  selectExpr    SelectExpr
  columns       Columns
  statements    Statements
  partitions    Partitions
  colName       *ColName
  tableExprs    TableExprs
  tableExpr     TableExpr
  joinCondition JoinCondition
  tableName     TableName
  tableNames    TableNames
  indexHints    *IndexHints
  asOf          *AsOf
  expr          Expr
  exprs         Exprs
  boolVal       BoolVal
  sqlVal        *SQLVal
  colTuple      ColTuple
  values        Values
  valTuple      ValTuple
  subquery      *Subquery
  whens         []*When
  when          *When
  orderBy       OrderBy
  order         *Order
  limit         *Limit
  setExprs      SetExprs
  setExpr       *SetExpr
  colIdent      ColIdent
  tableIdent    TableIdent
  convertType   *ConvertType
  aliasedTableName *AliasedTableExpr
  TableSpec  *TableSpec
  columnType    ColumnType
  columnOrder   *ColumnOrder
  triggerOrder  *TriggerOrder
  colKeyOpt     ColumnKeyOption
  optVal        Expr
  LengthScaleOption LengthScaleOption
  columnDefinition *ColumnDefinition
  indexDefinition *IndexDefinition
  indexInfo     *IndexInfo
  indexOption   *IndexOption
  indexOptions  []*IndexOption
  indexColumn   *IndexColumn
  indexColumns  []*IndexColumn
  constraintDefinition *ConstraintDefinition
  constraintInfo ConstraintInfo
  ReferenceAction ReferenceAction
  partDefs      []*PartitionDefinition
  partDef       *PartitionDefinition
  partSpec      *PartitionSpec
  vindexParam   VindexParam
  vindexParams  []VindexParam
  showFilter    *ShowFilter
  over          *Over
  caseStatementCases []CaseStatementCase
  caseStatementCase CaseStatementCase
  ifStatementConditions []IfStatementCondition
  ifStatementCondition IfStatementCondition
  signalInfo SignalInfo
  signalInfos []SignalInfo
  procedureParam ProcedureParam
  procedureParams []ProcedureParam
  characteristic Characteristic
  characteristics []Characteristic
  Fields *Fields
  Lines	*Lines
  EnclosedBy *EnclosedBy
}
/* Line 193 of yacc.c.  */
#line 1029 "go/vt/sqlparser/sql.go"
	YYSTYPE;
# define yystype YYSTYPE /* obsolescent; will be withdrawn */
# define YYSTYPE_IS_DECLARED 1
# define YYSTYPE_IS_TRIVIAL 1
#endif



/* Copy the second part of user declarations.  */


/* Line 216 of yacc.c.  */
#line 1042 "go/vt/sqlparser/sql.go"

#ifdef short
# undef short
#endif

#ifdef YYTYPE_UINT8
typedef YYTYPE_UINT8 yytype_uint8;
#else
typedef unsigned char yytype_uint8;
#endif

#ifdef YYTYPE_INT8
typedef YYTYPE_INT8 yytype_int8;
#elif (defined __STDC__ || defined __C99__FUNC__ \
     || defined __cplusplus || defined _MSC_VER)
typedef signed char yytype_int8;
#else
typedef short int yytype_int8;
#endif

#ifdef YYTYPE_UINT16
typedef YYTYPE_UINT16 yytype_uint16;
#else
typedef unsigned short int yytype_uint16;
#endif

#ifdef YYTYPE_INT16
typedef YYTYPE_INT16 yytype_int16;
#else
typedef short int yytype_int16;
#endif

#ifndef YYSIZE_T
# ifdef __SIZE_TYPE__
#  define YYSIZE_T __SIZE_TYPE__
# elif defined size_t
#  define YYSIZE_T size_t
# elif ! defined YYSIZE_T && (defined __STDC__ || defined __C99__FUNC__ \
     || defined __cplusplus || defined _MSC_VER)
#  include <stddef.h> /* INFRINGES ON USER NAME SPACE */
#  define YYSIZE_T size_t
# else
#  define YYSIZE_T unsigned int
# endif
#endif

#define YYSIZE_MAXIMUM ((YYSIZE_T) -1)

#ifndef YY_
# if defined YYENABLE_NLS && YYENABLE_NLS
#  if ENABLE_NLS
#   include <libintl.h> /* INFRINGES ON USER NAME SPACE */
#   define YY_(msgid) dgettext ("bison-runtime", msgid)
#  endif
# endif
# ifndef YY_
#  define YY_(msgid) msgid
# endif
#endif

/* Suppress unused-variable warnings by "using" E.  */
#if ! defined lint || defined __GNUC__
# define YYUSE(e) ((void) (e))
#else
# define YYUSE(e) /* empty */
#endif

/* Identity function, used to suppress warnings about constant conditions.  */
#ifndef lint
# define YYID(n) (n)
#else
#if (defined __STDC__ || defined __C99__FUNC__ \
     || defined __cplusplus || defined _MSC_VER)
static int
YYID (int i)
#else
static int
YYID (i)
    int i;
#endif
{
  return i;
}
#endif

#if ! defined yyoverflow || YYERROR_VERBOSE

/* The parser invokes alloca or malloc; define the necessary symbols.  */

# ifdef YYSTACK_USE_ALLOCA
#  if YYSTACK_USE_ALLOCA
#   ifdef __GNUC__
#    define YYSTACK_ALLOC __builtin_alloca
#   elif defined __BUILTIN_VA_ARG_INCR
#    include <alloca.h> /* INFRINGES ON USER NAME SPACE */
#   elif defined _AIX
#    define YYSTACK_ALLOC __alloca
#   elif defined _MSC_VER
#    include <malloc.h> /* INFRINGES ON USER NAME SPACE */
#    define alloca _alloca
#   else
#    define YYSTACK_ALLOC alloca
#    if ! defined _ALLOCA_H && ! defined _STDLIB_H && (defined __STDC__ || defined __C99__FUNC__ \
     || defined __cplusplus || defined _MSC_VER)
#     include <stdlib.h> /* INFRINGES ON USER NAME SPACE */
#     ifndef _STDLIB_H
#      define _STDLIB_H 1
#     endif
#    endif
#   endif
#  endif
# endif

# ifdef YYSTACK_ALLOC
   /* Pacify GCC's `empty if-body' warning.  */
#  define YYSTACK_FREE(Ptr) do { /* empty */; } while (YYID (0))
#  ifndef YYSTACK_ALLOC_MAXIMUM
    /* The OS might guarantee only one guard page at the bottom of the stack,
       and a page size can be as small as 4096 bytes.  So we cannot safely
       invoke alloca (N) if N exceeds 4096.  Use a slightly smaller number
       to allow for a few compiler-allocated temporary stack slots.  */
#   define YYSTACK_ALLOC_MAXIMUM 4032 /* reasonable circa 2006 */
#  endif
# else
#  define YYSTACK_ALLOC YYMALLOC
#  define YYSTACK_FREE YYFREE
#  ifndef YYSTACK_ALLOC_MAXIMUM
#   define YYSTACK_ALLOC_MAXIMUM YYSIZE_MAXIMUM
#  endif
#  if (defined __cplusplus && ! defined _STDLIB_H \
       && ! ((defined YYMALLOC || defined malloc) \
	     && (defined YYFREE || defined free)))
#   include <stdlib.h> /* INFRINGES ON USER NAME SPACE */
#   ifndef _STDLIB_H
#    define _STDLIB_H 1
#   endif
#  endif
#  ifndef YYMALLOC
#   define YYMALLOC malloc
#   if ! defined malloc && ! defined _STDLIB_H && (defined __STDC__ || defined __C99__FUNC__ \
     || defined __cplusplus || defined _MSC_VER)
void *malloc (YYSIZE_T); /* INFRINGES ON USER NAME SPACE */
#   endif
#  endif
#  ifndef YYFREE
#   define YYFREE free
#   if ! defined free && ! defined _STDLIB_H && (defined __STDC__ || defined __C99__FUNC__ \
     || defined __cplusplus || defined _MSC_VER)
void free (void *); /* INFRINGES ON USER NAME SPACE */
#   endif
#  endif
# endif
#endif /* ! defined yyoverflow || YYERROR_VERBOSE */


#if (! defined yyoverflow \
     && (! defined __cplusplus \
	 || (defined YYSTYPE_IS_TRIVIAL && YYSTYPE_IS_TRIVIAL)))

/* A type that is properly aligned for any stack member.  */
union yyalloc
{
  yytype_int16 yyss;
  YYSTYPE yyvs;
  };

/* The size of the maximum gap between one aligned stack and the next.  */
# define YYSTACK_GAP_MAXIMUM (sizeof (union yyalloc) - 1)

/* The size of an array large to enough to hold all stacks, each with
   N elements.  */
# define YYSTACK_BYTES(N) \
     ((N) * (sizeof (yytype_int16) + sizeof (YYSTYPE)) \
      + YYSTACK_GAP_MAXIMUM)

/* Copy COUNT objects from FROM to TO.  The source and destination do
   not overlap.  */
# ifndef YYCOPY
#  if defined __GNUC__ && 1 < __GNUC__
#   define YYCOPY(To, From, Count) \
      __builtin_memcpy (To, From, (Count) * sizeof (*(From)))
#  else
#   define YYCOPY(To, From, Count)		\
      do					\
	{					\
	  YYSIZE_T yyi;				\
	  for (yyi = 0; yyi < (Count); yyi++)	\
	    (To)[yyi] = (From)[yyi];		\
	}					\
      while (YYID (0))
#  endif
# endif

/* Relocate STACK from its old location to the new one.  The
   local variables YYSIZE and YYSTACKSIZE give the old and new number of
   elements in the stack, and YYPTR gives the new location of the
   stack.  Advance YYPTR to a properly aligned location for the next
   stack.  */
# define YYSTACK_RELOCATE(Stack)					\
    do									\
      {									\
	YYSIZE_T yynewbytes;						\
	YYCOPY (&yyptr->Stack, Stack, yysize);				\
	Stack = &yyptr->Stack;						\
	yynewbytes = yystacksize * sizeof (*Stack) + YYSTACK_GAP_MAXIMUM; \
	yyptr += yynewbytes / sizeof (*yyptr);				\
      }									\
    while (YYID (0))

#endif

/* YYFINAL -- State number of the termination state.  */
#define YYFINAL  351
/* YYLAST -- Last index in YYTABLE.  */
#define YYLAST   22594

/* YYNTOKENS -- Number of terminals.  */
#define YYNTOKENS  424
/* YYNNTS -- Number of nonterminals.  */
#define YYNNTS  271
/* YYNRULES -- Number of rules.  */
#define YYNRULES  1276
/* YYNRULES -- Number of states.  */
#define YYNSTATES  2217

/* YYTRANSLATE(YYLEX) -- Bison symbol number corresponding to YYLEX.  */
#define YYUNDEFTOK  2
#define YYMAXUTOK   660

#define YYTRANSLATE(YYX)						\
  ((unsigned int) (YYX) <= YYMAXUTOK ? yytranslate[YYX] : YYUNDEFTOK)

/* YYTRANSLATE[YYLEX] -- Bison symbol number corresponding to YYLEX.  */
static const yytype_uint16 yytranslate[] =
{
       0,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,    86,     2,     2,     2,   114,   107,     2,
      66,    68,   112,   110,    67,   111,   125,   113,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,   423,
      96,    95,    97,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,   117,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,   106,     2,   118,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     1,     2,     3,     4,
       5,     6,     7,     8,     9,    10,    11,    12,    13,    14,
      15,    16,    17,    18,    19,    20,    21,    22,    23,    24,
      25,    26,    27,    28,    29,    30,    31,    32,    33,    34,
      35,    36,    37,    38,    39,    40,    41,    42,    43,    44,
      45,    46,    47,    48,    49,    50,    51,    52,    53,    54,
      55,    56,    57,    58,    59,    60,    61,    62,    63,    64,
      65,    69,    70,    71,    72,    73,    74,    75,    76,    77,
      78,    79,    80,    81,    82,    83,    84,    85,    87,    88,
      89,    90,    91,    92,    93,    94,    98,    99,   100,   101,
     102,   103,   104,   105,   108,   109,   115,   116,   119,   120,
     121,   122,   123,   124,   126,   127,   128,   129,   130,   131,
     132,   133,   134,   135,   136,   137,   138,   139,   140,   141,
     142,   143,   144,   145,   146,   147,   148,   149,   150,   151,
     152,   153,   154,   155,   156,   157,   158,   159,   160,   161,
     162,   163,   164,   165,   166,   167,   168,   169,   170,   171,
     172,   173,   174,   175,   176,   177,   178,   179,   180,   181,
     182,   183,   184,   185,   186,   187,   188,   189,   190,   191,
     192,   193,   194,   195,   196,   197,   198,   199,   200,   201,
     202,   203,   204,   205,   206,   207,   208,   209,   210,   211,
     212,   213,   214,   215,   216,   217,   218,   219,   220,   221,
     222,   223,   224,   225,   226,   227,   228,   229,   230,   231,
     232,   233,   234,   235,   236,   237,   238,   239,   240,   241,
     242,   243,   244,   245,   246,   247,   248,   249,   250,   251,
     252,   253,   254,   255,   256,   257,   258,   259,   260,   261,
     262,   263,   264,   265,   266,   267,   268,   269,   270,   271,
     272,   273,   274,   275,   276,   277,   278,   279,   280,   281,
     282,   283,   284,   285,   286,   287,   288,   289,   290,   291,
     292,   293,   294,   295,   296,   297,   298,   299,   300,   301,
     302,   303,   304,   305,   306,   307,   308,   309,   310,   311,
     312,   313,   314,   315,   316,   317,   318,   319,   320,   321,
     322,   323,   324,   325,   326,   327,   328,   329,   330,   331,
     332,   333,   334,   335,   336,   337,   338,   339,   340,   341,
     342,   343,   344,   345,   346,   347,   348,   349,   350,   351,
     352,   353,   354,   355,   356,   357,   358,   359,   360,   361,
     362,   363,   364,   365,   366,   367,   368,   369,   370,   371,
     372,   373,   374,   375,   376,   377,   378,   379,   380,   381,
     382,   383,   384,   385,   386,   387,   388,   389,   390,   391,
     392,   393,   394,   395,   396,   397,   398,   399,   400,   401,
     402,   403,   404,   405,   406,   407,   408,   409,   410,   411,
     412,   413,   414,   415,   416,   417,   418,   419,   420,   421,
     422
};

#if YYDEBUG
/* YYPRHS[YYN] -- Index of the first RHS symbol of rule number YYN in
   YYRHS.  */
static const yytype_uint16 yyprhs[] =
{
       0,     0,     3,     6,     7,     9,    11,    13,    15,    17,
      19,    21,    23,    25,    27,    29,    31,    33,    35,    37,
      39,    41,    43,    45,    47,    49,    51,    53,    55,    57,
      58,    70,    75,    82,    90,    96,   107,   109,   113,   115,
     119,   127,   136,   138,   140,   150,   159,   167,   174,   181,
     183,   185,   187,   191,   193,   197,   199,   203,   204,   209,
     213,   218,   224,   229,   231,   235,   239,   242,   245,   248,
     251,   254,   256,   258,   260,   261,   264,   268,   280,   288,
     298,   304,   310,   326,   338,   339,   341,   343,   347,   350,
     354,   358,   362,   363,   365,   367,   370,   373,   376,   379,
     381,   384,   387,   391,   395,   399,   403,   405,   407,   409,
     411,   413,   415,   417,   419,   421,   423,   425,   427,   429,
     431,   433,   435,   437,   439,   441,   443,   445,   447,   452,
     454,   458,   459,   463,   465,   467,   469,   471,   473,   474,
     477,   480,   482,   484,   486,   488,   490,   495,   501,   510,
     512,   515,   521,   529,   540,   549,   561,   563,   566,   572,
     575,   580,   583,   587,   589,   593,   597,   599,   601,   603,
     605,   607,   609,   611,   613,   615,   617,   619,   621,   625,
     626,   629,   633,   635,   639,   641,   643,   645,   647,   649,
     651,   653,   655,   657,   658,   661,   663,   664,   667,   669,
     673,   677,   682,   687,   689,   693,   697,   701,   705,   709,
     710,   713,   716,   719,   722,   725,   728,   732,   734,   736,
     738,   741,   743,   745,   747,   749,   751,   753,   755,   757,
     759,   761,   764,   767,   771,   774,   777,   780,   783,   786,
     788,   791,   794,   797,   799,   804,   809,   813,   817,   820,
     825,   831,   834,   838,   843,   846,   849,   853,   857,   861,
     865,   869,   874,   876,   878,   880,   882,   884,   891,   898,
     900,   902,   904,   906,   908,   910,   912,   914,   916,   920,
     921,   925,   926,   932,   933,   937,   943,   944,   946,   947,
     949,   951,   954,   957,   961,   963,   964,   968,   972,   973,
     976,   979,   982,   984,   987,   989,   992,   998,  1003,  1004,
    1006,  1008,  1011,  1014,  1018,  1021,  1022,  1024,  1027,  1031,
    1035,  1038,  1041,  1043,  1045,  1047,  1049,  1051,  1052,  1054,
    1055,  1057,  1059,  1063,  1067,  1071,  1073,  1084,  1096,  1108,
    1121,  1134,  1136,  1138,  1139,  1142,  1145,  1149,  1153,  1155,
    1157,  1160,  1163,  1166,  1167,  1169,  1173,  1175,  1178,  1182,
    1184,  1186,  1188,  1189,  1192,  1194,  1196,  1198,  1205,  1216,
    1226,  1234,  1242,  1250,  1260,  1268,  1281,  1296,  1304,  1312,
    1322,  1332,  1343,  1349,  1356,  1365,  1373,  1374,  1376,  1379,
    1384,  1392,  1398,  1404,  1410,  1423,  1431,  1437,  1447,  1448,
    1450,  1452,  1454,  1456,  1458,  1460,  1468,  1470,  1474,  1483,
    1492,  1496,  1500,  1506,  1511,  1518,  1523,  1528,  1533,  1538,
    1543,  1547,  1550,  1554,  1559,  1564,  1568,  1575,  1582,  1587,
    1592,  1597,  1602,  1607,  1612,  1616,  1620,  1623,  1630,  1633,
    1638,  1643,  1648,  1652,  1660,  1667,  1673,  1678,  1681,  1686,
    1690,  1695,  1699,  1703,  1709,  1712,  1716,  1720,  1722,  1724,
    1725,  1727,  1729,  1731,  1732,  1735,  1738,  1739,  1742,  1745,
    1746,  1749,  1750,  1752,  1754,  1757,  1759,  1761,  1763,  1766,
    1768,  1770,  1772,  1774,  1778,  1782,  1784,  1786,  1788,  1790,
    1791,  1795,  1797,  1799,  1802,  1805,  1808,  1811,  1815,  1819,
    1822,  1823,  1826,  1827,  1830,  1832,  1835,  1838,  1839,  1841,
    1843,  1844,  1846,  1848,  1849,  1851,  1852,  1854,  1856,  1860,
    1862,  1865,  1869,  1875,  1878,  1883,  1891,  1892,  1894,  1895,
    1897,  1900,  1902,  1904,  1906,  1908,  1909,  1912,  1914,  1918,
    1920,  1922,  1924,  1928,  1930,  1934,  1937,  1944,  1946,  1951,
    1958,  1962,  1965,  1966,  1970,  1971,  1975,  1977,  1981,  1983,
    1987,  1992,  1997,  2002,  2006,  2009,  2014,  2015,  2017,  2018,
    2021,  2022,  2024,  2026,  2028,  2030,  2032,  2035,  2038,  2040,
    2043,  2047,  2050,  2054,  2057,  2060,  2064,  2067,  2069,  2071,
    2075,  2079,  2080,  2086,  2092,  2098,  2099,  2102,  2104,  2108,
    2112,  2115,  2119,  2121,  2124,  2125,  2129,  2131,  2133,  2137,
    2141,  2146,  2151,  2157,  2161,  2166,  2172,  2179,  2182,  2184,
    2187,  2189,  2192,  2194,  2197,  2199,  2201,  2203,  2205,  2207,
    2209,  2211,  2212,  2215,  2217,  2219,  2221,  2225,  2227,  2231,
    2233,  2235,  2237,  2239,  2241,  2245,  2249,  2253,  2257,  2261,
    2265,  2269,  2273,  2277,  2281,  2285,  2289,  2293,  2297,  2301,
    2304,  2307,  2310,  2313,  2316,  2319,  2322,  2326,  2328,  2330,
    2332,  2334,  2336,  2338,  2344,  2351,  2358,  2365,  2371,  2377,
    2383,  2390,  2396,  2402,  2409,  2415,  2421,  2427,  2433,  2440,
    2446,  2452,  2458,  2463,  2468,  2474,  2480,  2486,  2492,  2498,
    2503,  2508,  2513,  2518,  2523,  2528,  2532,  2539,  2546,  2553,
    2562,  2571,  2580,  2589,  2599,  2604,  2612,  2618,  2623,  2626,
    2629,  2632,  2635,  2638,  2641,  2644,  2647,  2650,  2653,  2656,
    2659,  2662,  2665,  2668,  2677,  2686,  2687,  2690,  2694,  2699,
    2704,  2709,  2714,  2719,  2724,  2725,  2729,  2734,  2742,  2746,
    2748,  2750,  2753,  2757,  2761,  2763,  2766,  2769,  2771,  2774,
    2776,  2779,  2782,  2784,  2787,  2788,  2790,  2791,  2794,  2796,
    2799,  2804,  2805,  2808,  2810,  2814,  2820,  2822,  2824,  2826,
    2828,  2830,  2832,  2834,  2836,  2838,  2841,  2844,  2845,  2849,
    2851,  2855,  2857,  2859,  2860,  2863,  2865,  2867,  2868,  2872,
    2874,  2878,  2881,  2884,  2885,  2887,  2889,  2890,  2893,  2898,
    2903,  2904,  2907,  2912,  2915,  2917,  2921,  2927,  2932,  2939,
    2941,  2945,  2949,  2955,  2956,  2962,  2964,  2968,  2970,  2973,
    2977,  2979,  2981,  2985,  2989,  2993,  2997,  3001,  3003,  3006,
    3008,  3010,  3012,  3014,  3016,  3018,  3019,  3022,  3023,  3027,
    3028,  3030,  3031,  3035,  3037,  3039,  3041,  3043,  3045,  3047,
    3049,  3051,  3053,  3055,  3057,  3058,  3060,  3062,  3064,  3066,
    3068,  3069,  3071,  3072,  3075,  3077,  3079,  3081,  3083,  3085,
    3087,  3089,  3091,  3092,  3095,  3096,  3098,  3099,  3104,  3105,
    3107,  3108,  3112,  3113,  3117,  3118,  3123,  3124,  3128,  3129,
    3133,  3135,  3137,  3139,  3141,  3143,  3145,  3147,  3149,  3151,
    3153,  3155,  3157,  3159,  3161,  3163,  3165,  3167,  3169,  3171,
    3173,  3175,  3177,  3179,  3181,  3183,  3185,  3187,  3189,  3191,
    3193,  3195,  3197,  3199,  3201,  3203,  3205,  3207,  3209,  3211,
    3213,  3215,  3217,  3219,  3221,  3223,  3225,  3227,  3229,  3231,
    3233,  3235,  3237,  3239,  3241,  3243,  3245,  3247,  3249,  3251,
    3253,  3255,  3257,  3259,  3261,  3263,  3265,  3267,  3269,  3271,
    3273,  3275,  3277,  3279,  3281,  3283,  3285,  3287,  3289,  3291,
    3293,  3295,  3297,  3299,  3301,  3303,  3305,  3307,  3309,  3311,
    3313,  3315,  3317,  3319,  3321,  3323,  3325,  3327,  3329,  3331,
    3333,  3335,  3337,  3339,  3341,  3343,  3345,  3347,  3349,  3351,
    3353,  3355,  3357,  3359,  3361,  3363,  3365,  3367,  3369,  3371,
    3373,  3375,  3377,  3379,  3381,  3383,  3385,  3387,  3389,  3391,
    3393,  3395,  3397,  3399,  3401,  3403,  3405,  3407,  3409,  3411,
    3413,  3415,  3417,  3419,  3421,  3423,  3425,  3427,  3429,  3431,
    3433,  3435,  3437,  3439,  3441,  3443,  3445,  3447,  3449,  3451,
    3453,  3455,  3457,  3459,  3461,  3463,  3465,  3467,  3469,  3471,
    3473,  3475,  3477,  3479,  3481,  3483,  3485,  3487,  3489,  3491,
    3493,  3495,  3497,  3499,  3501,  3503,  3505,  3507,  3509,  3511,
    3513,  3515,  3517,  3519,  3521,  3523,  3525,  3527,  3529,  3531,
    3533,  3535,  3537,  3539,  3541,  3543,  3545,  3547,  3549,  3551,
    3553,  3555,  3557,  3559,  3561,  3563,  3565,  3567,  3569,  3571,
    3573,  3575,  3577,  3579,  3581,  3583,  3585,  3587,  3589,  3591,
    3593,  3595,  3597,  3599,  3601,  3603,  3605,  3607,  3609,  3611,
    3613,  3615,  3617,  3619,  3621,  3623,  3625,  3627,  3629,  3631,
    3633,  3635,  3637,  3639,  3641,  3643,  3645,  3647,  3649,  3651,
    3653,  3655,  3657,  3659,  3661,  3663,  3665,  3667,  3669,  3671,
    3673,  3675,  3677,  3679,  3681,  3683,  3685,  3687,  3689,  3691,
    3693,  3695,  3697,  3699,  3701,  3703,  3705,  3707,  3709,  3711,
    3713,  3715,  3717,  3719,  3721,  3723,  3725,  3727,  3729,  3731,
    3733,  3735,  3737,  3739,  3741,  3743,  3745,  3747,  3749,  3751,
    3753,  3755,  3757,  3759,  3761,  3763,  3765,  3767,  3769,  3771,
    3773,  3775,  3777,  3779,  3781,  3783,  3785,  3787,  3789,  3791,
    3793,  3795,  3797,  3799,  3801,  3803,  3805,  3807,  3809,  3811,
    3813,  3815,  3817,  3819,  3821,  3823,  3825,  3827,  3829,  3831,
    3833,  3835,  3837,  3839,  3841,  3843,  3845,  3847,  3849,  3851,
    3853,  3855,  3857,  3859,  3861,  3863,  3865,  3867,  3869,  3871,
    3873,  3875,  3877,  3879,  3880,  3881,  3883
};

/* YYRHS -- A `-1'-separated list of the rules' RHS.  */
static const yytype_int16 yyrhs[] =
{
     425,     0,    -1,   427,   426,    -1,    -1,   423,    -1,   429,
      -1,   430,    -1,   434,    -1,   436,    -1,   437,    -1,   443,
      -1,   449,    -1,   535,    -1,   545,    -1,   547,    -1,   548,
      -1,   549,    -1,   550,    -1,   559,    -1,   560,    -1,   562,
      -1,   563,    -1,   565,    -1,   569,    -1,   570,    -1,   571,
      -1,   471,    -1,   476,    -1,   428,    -1,    -1,    36,    35,
     680,   679,   606,   442,   508,   685,   686,   668,   593,    -1,
     431,   647,   651,   652,    -1,   432,   575,   433,   647,   651,
     652,    -1,     5,   572,   576,    48,   641,   664,   608,    -1,
       6,   572,   581,    10,   608,    -1,     5,   572,   576,   577,
     578,   580,   586,   611,   642,   645,    -1,   429,    -1,   691,
     429,   692,    -1,   431,    -1,   691,   429,   692,    -1,   435,
     572,   667,   607,   442,   653,   655,    -1,   435,   572,   667,
     607,   442,    29,   660,   655,    -1,     7,    -1,   308,    -1,
       8,   572,   667,   587,    29,   660,   611,   647,   651,    -1,
       9,   572,    10,   608,   442,   611,   647,   651,    -1,     9,
     572,    10,   440,    64,   587,   611,    -1,     9,   572,   440,
     438,   587,   611,    -1,     9,   572,   441,   438,   587,   611,
      -1,    10,    -1,    64,    -1,   608,    -1,   439,    67,   608,
      -1,   608,    -1,   440,    67,   608,    -1,   609,    -1,   441,
      67,   609,    -1,    -1,   170,   691,   595,   692,    -1,    29,
     572,   660,    -1,    29,   572,   447,   660,    -1,    29,   572,
     447,   215,   444,    -1,    29,   572,   215,   444,    -1,   445,
      -1,   444,    67,   445,    -1,   289,   290,   446,    -1,   291,
     292,    -1,   291,   293,    -1,   294,   291,    -1,   291,   295,
      -1,   291,   296,    -1,   297,    -1,   288,    -1,   287,    -1,
      -1,   485,   486,    -1,   485,   100,   608,    -1,   128,   673,
     139,   675,   674,    65,   608,    66,   522,    68,   513,    -1,
     128,   141,   608,    22,   448,   429,   448,    -1,   128,    84,
     308,   141,   608,    22,   448,   429,   448,    -1,   128,   299,
     666,    69,   694,    -1,   128,   137,   666,    69,   694,    -1,
     128,   459,   175,    69,   460,   461,    65,   608,    18,   184,
     185,   462,   448,   463,   448,    -1,   128,   459,   174,    69,
      66,   450,    68,   453,   448,   456,   448,    -1,    -1,   451,
      -1,   452,    -1,   451,    67,   452,    -1,    69,   491,    -1,
      98,    69,   491,    -1,   191,    69,   491,    -1,   192,    69,
     491,    -1,    -1,   454,    -1,   455,    -1,   454,   455,    -1,
      78,    71,    -1,   352,   197,    -1,    87,   193,    -1,   193,
      -1,   194,   197,    -1,   155,   197,    -1,   195,   197,    35,
      -1,   196,   197,    35,    -1,   197,   198,   189,    -1,   197,
     198,   190,    -1,   429,    -1,   434,    -1,   436,    -1,   437,
      -1,   443,    -1,   449,    -1,   535,    -1,   545,    -1,   547,
      -1,   465,    -1,   468,    -1,   548,    -1,   549,    -1,   550,
      -1,   561,    -1,   562,    -1,   563,    -1,   565,    -1,   569,
      -1,   471,    -1,   476,    -1,   457,    -1,   213,   458,   423,
      88,    -1,   456,    -1,   458,   423,   456,    -1,    -1,   189,
      95,    69,    -1,   186,    -1,   159,    -1,     7,    -1,     8,
      -1,     9,    -1,    -1,   187,    69,    -1,   188,    69,    -1,
     464,    -1,   443,    -1,   434,    -1,   436,    -1,   437,    -1,
     213,   478,   423,    88,    -1,    93,   612,   466,    88,    93,
      -1,    93,   612,   466,    90,   478,   423,    88,    93,    -1,
     467,    -1,   466,   467,    -1,    92,   612,    91,   478,   423,
      -1,   144,   612,    91,   478,   423,    88,   144,    -1,   144,
     612,    91,   478,   423,    90,   478,   423,    88,   144,    -1,
     144,   612,    91,   478,   423,   469,    88,   144,    -1,   144,
     612,    91,   478,   423,   469,    90,   478,   423,    88,   144,
      -1,   470,    -1,   469,   470,    -1,    89,   612,    91,   478,
     423,    -1,   211,   472,    -1,   211,   472,    29,   473,    -1,
     212,    71,    -1,   212,    49,    71,    -1,   474,    -1,   473,
      67,   474,    -1,   475,    95,   640,    -1,   199,    -1,   200,
      -1,   201,    -1,   202,    -1,   203,    -1,   204,    -1,   205,
      -1,   206,    -1,   207,    -1,   208,    -1,   209,    -1,   210,
      -1,    19,    69,   477,    -1,    -1,    66,    68,    -1,    66,
     621,    68,    -1,   479,    -1,   478,   423,   479,    -1,   429,
      -1,   434,    -1,   436,    -1,   437,    -1,   443,    -1,   465,
      -1,   468,    -1,   471,    -1,   464,    -1,    -1,    64,   481,
      -1,    69,    -1,    -1,   353,   483,    -1,   484,    -1,   483,
      67,   484,    -1,   676,    95,   533,    -1,   128,   138,   666,
     608,    -1,    66,   487,    68,   531,    -1,   489,    -1,   487,
      67,   489,    -1,   487,    67,   512,    -1,   487,    67,   524,
      -1,    69,   491,   490,    -1,   676,   491,   490,    -1,    -1,
     490,   504,    -1,   490,   505,    -1,   490,   506,    -1,   490,
     507,    -1,   490,   510,    -1,   490,   511,    -1,   492,   502,
     503,    -1,   496,    -1,   495,    -1,   497,    -1,   493,   499,
      -1,   494,    -1,   218,    -1,   240,    -1,   351,    -1,   219,
      -1,   220,    -1,   221,    -1,   222,    -1,   223,    -1,   224,
      -1,   226,   500,    -1,   227,   500,    -1,   227,   233,   500,
      -1,   228,   500,    -1,   229,   501,    -1,   230,   501,    -1,
     231,   501,    -1,   232,   501,    -1,   163,    -1,   234,   499,
      -1,   235,   499,    -1,   236,   499,    -1,   237,    -1,   238,
     499,   508,   509,    -1,   241,   499,   508,   509,    -1,   245,
     238,   499,    -1,   245,   241,   499,    -1,   243,   499,    -1,
     239,   499,   508,   509,    -1,   241,   246,   499,   508,   509,
      -1,   244,   499,    -1,   245,   239,   499,    -1,   245,   241,
     246,   499,    -1,   123,   499,    -1,   242,   499,    -1,   247,
     508,   509,    -1,   248,   508,   509,    -1,   249,   508,   509,
      -1,   250,   508,   509,    -1,   251,   508,   509,    -1,   251,
     239,   508,   509,    -1,   252,    -1,   253,    -1,   254,    -1,
     255,    -1,   256,    -1,   257,    66,   498,    68,   508,   509,
      -1,    29,    66,   498,    68,   508,   509,    -1,   258,    -1,
     259,    -1,   260,    -1,   261,    -1,   262,    -1,   263,    -1,
     264,    -1,   265,    -1,    71,    -1,   498,    67,    71,    -1,
      -1,    66,    72,    68,    -1,    -1,    66,    72,    67,    72,
      68,    -1,    -1,    66,    72,    68,    -1,    66,    72,    67,
      72,    68,    -1,    -1,   270,    -1,    -1,   271,    -1,    80,
      -1,    87,    80,    -1,    28,   622,    -1,    65,     8,   627,
      -1,   267,    -1,    -1,   241,    29,    69,    -1,   241,    29,
     123,    -1,    -1,   120,    69,    -1,   120,    71,    -1,   145,
      43,    -1,    43,    -1,    44,    43,    -1,    44,    -1,    78,
      71,    -1,   517,    66,   522,    68,   514,    -1,   517,    66,
     522,    68,    -1,    -1,   514,    -1,   515,    -1,   514,   515,
      -1,    64,    69,    -1,   149,   516,    72,    -1,    78,    71,
      -1,    -1,    95,    -1,   145,    43,    -1,   147,   519,   521,
      -1,    44,   519,   521,    -1,    44,   521,    -1,   519,   521,
      -1,   139,    -1,   140,    -1,    32,    -1,   139,    -1,    43,
      -1,    -1,   519,    -1,    -1,    69,    -1,   523,    -1,   522,
      67,   523,    -1,   675,   499,   650,    -1,   153,    69,   525,
      -1,   525,    -1,   154,    43,    66,   594,    68,   156,   608,
      66,   594,    68,    -1,   154,    43,    66,   594,    68,   156,
     608,    66,   594,    68,   528,    -1,   154,    43,    66,   594,
      68,   156,   608,    66,   594,    68,   529,    -1,   154,    43,
      66,   594,    68,   156,   608,    66,   594,    68,   528,   529,
      -1,   154,    43,    66,   594,    68,   156,   608,    66,   594,
      68,   529,   528,    -1,    10,    -1,    98,    -1,    -1,    10,
      69,    -1,    98,    69,    -1,    65,     9,   530,    -1,    65,
       8,   530,    -1,   157,    -1,   152,    -1,   155,   151,    -1,
      29,    28,    -1,    29,    80,    -1,    -1,   532,    -1,   531,
      67,   532,    -1,   533,    -1,   532,   533,    -1,   532,    95,
     533,    -1,   676,    -1,    71,    -1,    72,    -1,    -1,   153,
      69,    -1,   536,    -1,   538,    -1,   539,    -1,   129,   667,
     138,   608,   669,   693,    -1,   129,   667,   138,   608,   133,
     540,    66,   488,    68,   693,    -1,   129,   667,   138,   608,
     133,   540,   488,   537,   693,    -1,   129,   667,   138,   608,
     133,   541,   693,    -1,   129,   667,   138,   608,   130,   540,
      69,    -1,   129,   667,   138,   608,   130,   541,   693,    -1,
     129,   667,   138,   608,   131,   146,    69,   670,    69,    -1,
     129,   667,   138,   608,   131,   671,   608,    -1,   129,   667,
     138,   608,   133,   519,   521,   674,    66,   522,    68,   513,
      -1,   129,   667,   138,   608,   133,   534,   672,   520,   521,
     674,    66,   522,    68,   513,    -1,   129,   667,   138,   608,
     130,   153,    69,    -1,   129,   667,   138,   608,   130,   519,
     675,    -1,   129,   667,   138,   608,   131,   519,   675,   142,
     675,    -1,   129,   667,   138,   608,   135,   540,   488,   537,
     693,    -1,   129,   667,   138,   608,   136,   540,    69,   488,
     537,   693,    -1,   129,   667,   138,   608,   542,    -1,   129,
     667,   138,   608,   133,   524,    -1,   129,   667,   138,   608,
     130,   154,    43,    69,    -1,   129,   667,   138,   608,   267,
     516,   612,    -1,    -1,   158,    -1,   159,    69,    -1,   129,
     141,   608,   694,    -1,   129,   278,   128,   178,   608,   480,
     482,    -1,   129,   278,   130,   178,   608,    -1,   129,   278,
     133,   138,   608,    -1,   129,   278,   130,   138,   608,    -1,
     129,   278,    65,   608,   133,   178,   675,    66,   594,    68,
     480,   482,    -1,   129,   278,    65,   608,   130,   178,   675,
      -1,   129,   278,   133,   183,   608,    -1,   129,   278,    65,
     608,   133,   267,   675,    64,   608,    -1,    -1,   146,    -1,
     150,    -1,   153,    -1,   154,    -1,   145,    -1,   170,    -1,
     171,   170,   675,    26,   691,   543,   692,    -1,   544,    -1,
     543,    67,   544,    -1,   170,   675,    46,   172,   173,   691,
     622,   692,    -1,   170,   675,    46,   172,   173,   691,   169,
     692,    -1,   131,   138,   546,    -1,   608,   142,   608,    -1,
     546,    67,   608,   142,   608,    -1,   130,   138,   665,   440,
      -1,   130,   139,   675,    65,   608,   694,    -1,   130,   141,
     665,   439,    -1,   130,   299,   665,    69,    -1,   130,   137,
     665,    69,    -1,   130,   175,   665,    69,    -1,   130,   174,
     665,    69,    -1,   167,   138,   608,    -1,   167,   608,    -1,
     132,   138,   608,    -1,   160,   123,    69,   694,    -1,   160,
     241,    29,   694,    -1,   160,   286,   694,    -1,   160,   128,
     299,   666,    69,   694,    -1,   160,   128,   137,   666,    69,
     694,    -1,   160,   128,    69,   694,    -1,   160,   128,   174,
     694,    -1,   160,   128,   177,   694,    -1,   160,   128,   138,
     608,    -1,   160,   128,   175,   608,    -1,   160,   128,   141,
     608,    -1,   160,   274,   694,    -1,   160,   275,   694,    -1,
     160,   283,    -1,   160,   518,   526,   608,   527,   611,    -1,
     160,   284,    -1,   160,   174,   180,   556,    -1,   160,   177,
     180,   556,    -1,   160,   558,   180,   694,    -1,   160,   138,
     694,    -1,   160,   553,   554,    10,   608,   555,   556,    -1,
     160,   553,   552,   555,   592,   556,    -1,   160,   553,   176,
     555,   556,    -1,   160,   558,   181,   694,    -1,   160,   273,
      -1,   160,   273,    11,   612,    -1,   160,   273,   551,    -1,
     160,   277,   181,   557,    -1,   160,   278,   276,    -1,   160,
     278,   179,    -1,   160,   278,   179,    65,   608,    -1,   160,
     182,    -1,   160,    69,   694,    -1,   100,   622,   618,    -1,
     276,    -1,   280,    -1,    -1,   279,    -1,   281,    -1,   282,
      -1,    -1,    10,   677,    -1,    98,   677,    -1,    -1,   100,
      71,    -1,    11,   612,    -1,    -1,   100,    71,    -1,    -1,
     288,    -1,   287,    -1,    55,   677,    -1,    55,    -1,   213,
      -1,   561,    -1,   214,   215,    -1,   216,    -1,   217,    -1,
     161,    -1,    25,    -1,   568,   567,   566,    -1,   568,   132,
     429,    -1,   429,    -1,   437,    -1,   434,    -1,   436,    -1,
      -1,   168,    95,    69,    -1,   564,    -1,   162,    -1,   564,
     608,    -1,   564,   693,    -1,   165,   693,    -1,   166,   693,
      -1,    30,   276,   693,    -1,    31,   276,   693,    -1,   134,
     693,    -1,    -1,   573,   574,    -1,    -1,   574,    77,    -1,
       4,    -1,     4,    20,    -1,     4,    21,    -1,    -1,    52,
      -1,    53,    -1,    -1,    20,    -1,    21,    -1,    -1,    62,
      -1,    -1,   580,    -1,   581,    -1,   580,    67,   581,    -1,
     112,    -1,   612,   584,    -1,   677,   125,   112,    -1,   677,
     125,   678,   125,   112,    -1,   317,   675,    -1,   317,   691,
     647,   692,    -1,   317,   691,   170,    15,   621,   647,   692,
      -1,    -1,   582,    -1,    -1,   585,    -1,    22,   585,    -1,
      69,    -1,   689,    -1,   690,    -1,    71,    -1,    -1,    10,
     587,    -1,   588,    -1,   587,    67,   588,    -1,   589,    -1,
     596,    -1,   590,    -1,   620,   600,   601,    -1,   620,    -1,
     691,   587,   692,    -1,   608,   591,    -1,   608,   170,   691,
     595,   692,   591,    -1,   610,    -1,    22,    33,   622,   610,
      -1,    22,    33,   622,   600,   601,   610,    -1,    22,   601,
     610,    -1,   601,   610,    -1,    -1,    22,    33,   622,    -1,
      -1,    66,   594,    68,    -1,   675,    -1,   594,    67,   675,
      -1,   675,    -1,   595,    67,   675,    -1,   588,   602,   589,
     598,    -1,   588,   603,   589,   599,    -1,   588,   604,   588,
     597,    -1,   588,   605,   589,    -1,    65,   612,    -1,    64,
      66,   594,    68,    -1,    -1,   597,    -1,    -1,    65,   612,
      -1,    -1,    22,    -1,   677,    -1,   690,    -1,    71,    -1,
      63,    -1,    59,    63,    -1,    57,    63,    -1,    62,    -1,
      61,    63,    -1,    61,    58,    63,    -1,    60,    63,    -1,
      60,    58,    63,    -1,    56,    63,    -1,    56,   604,    -1,
      26,   138,   608,    -1,    26,   608,    -1,   608,    -1,   677,
      -1,   677,   125,   678,    -1,   677,   125,   112,    -1,    -1,
      55,   139,   691,   594,   692,    -1,   143,   139,   691,   594,
     692,    -1,    54,   139,   691,   594,   692,    -1,    -1,    11,
     612,    -1,   615,    -1,   612,    85,   612,    -1,   612,    84,
     612,    -1,    87,   612,    -1,   612,   101,   616,    -1,   622,
      -1,    28,   613,    -1,    -1,   691,    69,   692,    -1,    81,
      -1,    82,    -1,   622,   617,   622,    -1,   622,    98,   619,
      -1,   622,    87,    98,   619,    -1,   622,   100,   622,   618,
      -1,   622,    87,   100,   622,   618,    -1,   622,    99,   622,
      -1,   622,    87,    99,   622,    -1,   622,    94,   622,    85,
     622,    -1,   622,    87,    94,   622,    85,   622,    -1,    23,
     620,    -1,    80,    -1,    87,    80,    -1,    81,    -1,    87,
      81,    -1,    82,    -1,    87,    82,    -1,    95,    -1,    96,
      -1,    97,    -1,   105,    -1,   104,    -1,   103,    -1,   102,
      -1,    -1,   164,   622,    -1,   658,    -1,   620,    -1,    76,
      -1,   691,   429,   692,    -1,   612,    -1,   621,    67,   612,
      -1,   640,    -1,   614,    -1,   639,    -1,   659,    -1,   620,
      -1,   622,   107,   622,    -1,   622,   106,   622,    -1,   622,
     117,   622,    -1,   622,   110,   622,    -1,   622,   111,   622,
      -1,   622,   112,   622,    -1,   622,   113,   622,    -1,   622,
     116,   622,    -1,   622,   114,   622,    -1,   622,   115,   622,
      -1,   622,   109,   622,    -1,   622,   108,   622,    -1,   639,
     126,   640,    -1,   639,   127,   640,    -1,   622,   120,   632,
      -1,   123,   622,    -1,   122,   622,    -1,   121,   622,    -1,
     110,   622,    -1,   111,   622,    -1,   118,   622,    -1,    86,
     622,    -1,   124,   622,   675,    -1,   623,    -1,   626,    -1,
     627,    -1,   630,    -1,   625,    -1,   624,    -1,   675,   691,
     577,   579,   692,    -1,   677,   125,   676,   691,   579,   692,
      -1,   328,   691,   577,   580,   692,   583,    -1,   321,   691,
     577,   580,   692,   583,    -1,   322,   691,   580,   692,   583,
      -1,   323,   691,   580,   692,   583,    -1,   324,   691,   580,
     692,   583,    -1,   325,   691,   577,   580,   692,   583,    -1,
     326,   691,   580,   692,   583,    -1,   327,   691,   580,   692,
     583,    -1,   329,   691,   577,   580,   692,   583,    -1,   330,
     691,   580,   692,   583,    -1,   331,   691,   580,   692,   583,
      -1,   332,   691,   580,   692,   583,    -1,   333,   691,   580,
     692,   583,    -1,   334,   691,   577,   580,   692,   583,    -1,
     335,   691,   580,   692,   583,    -1,   336,   691,   580,   692,
     583,    -1,   337,   691,   580,   692,   583,    -1,   338,   691,
     692,   582,    -1,   339,   691,   692,   582,    -1,   340,   691,
     581,   692,   582,    -1,   341,   691,   580,   692,   582,    -1,
     342,   691,   581,   692,   582,    -1,   343,   691,   580,   692,
     582,    -1,   344,   691,   580,   692,   582,    -1,   345,   691,
     692,   582,    -1,   347,   691,   692,   582,    -1,   348,   691,
     692,   582,    -1,   346,   691,   692,   582,    -1,    61,   691,
     580,   692,    -1,    60,   691,   580,   692,    -1,   137,   691,
     692,    -1,   309,   691,   612,    67,   633,   692,    -1,   310,
     691,   612,    22,   633,   692,    -1,   309,   691,   612,    64,
     632,   692,    -1,   311,   691,   639,    10,   622,    18,   622,
     692,    -1,   312,   691,   639,    10,   622,    18,   622,   692,
      -1,   311,   691,    71,    10,   622,    18,   622,   692,    -1,
     312,   691,    71,    10,   622,    18,   622,   692,    -1,   349,
     691,   580,   692,   350,   691,   622,   631,   692,    -1,   158,
     691,   580,   692,    -1,   313,   691,   577,   580,   647,   635,
     692,    -1,    93,   634,   636,   638,    88,    -1,    46,   691,
     639,   692,    -1,   298,   628,    -1,   307,   628,    -1,   306,
     628,    -1,   305,   628,    -1,   303,   628,    -1,   304,   628,
      -1,   300,   628,    -1,   302,   628,    -1,   301,   628,    -1,
     298,   629,    -1,   307,   629,    -1,   306,   629,    -1,   303,
     629,    -1,   304,   629,    -1,   302,   629,    -1,   315,   691,
     675,    67,   622,    67,   622,   692,    -1,   316,   691,   675,
      67,   622,    67,   622,   692,    -1,    -1,   691,   692,    -1,
     691,   622,   692,    -1,   144,   691,   580,   692,    -1,   299,
     691,   579,   692,    -1,   115,   691,   580,   692,    -1,   308,
     691,   580,   692,    -1,   311,   691,   580,   692,    -1,   312,
     691,   580,   692,    -1,    -1,    98,   351,    51,    -1,    98,
      56,   352,    51,    -1,    98,    56,   352,    51,   353,   354,
     355,    -1,   353,   354,   355,    -1,    69,    -1,    71,    -1,
     123,   499,    -1,   238,   499,   508,    -1,   238,   499,    69,
      -1,   163,    -1,   236,   499,    -1,   229,   501,    -1,   256,
      -1,   243,   499,    -1,   269,    -1,   269,   223,    -1,   234,
     499,    -1,   270,    -1,   270,   223,    -1,    -1,   612,    -1,
      -1,   314,    71,    -1,   637,    -1,   636,   637,    -1,    92,
     612,    91,   612,    -1,    -1,    90,   612,    -1,   675,    -1,
     677,   125,   676,    -1,   677,   125,   678,   125,   676,    -1,
      71,    -1,    70,    -1,    79,    -1,    72,    -1,    73,    -1,
      74,    -1,    75,    -1,    80,    -1,    49,    -1,    72,    46,
      -1,    75,    46,    -1,    -1,    12,    15,   643,    -1,   644,
      -1,   643,    67,   644,    -1,   612,    -1,   690,    -1,    -1,
      13,   646,    -1,   612,    -1,   690,    -1,    -1,    14,    15,
     648,    -1,   649,    -1,   648,    67,   649,    -1,   612,   650,
      -1,   690,   650,    -1,    -1,    24,    -1,    25,    -1,    -1,
      16,   612,    -1,    16,   612,    67,   612,    -1,    16,   612,
      17,   612,    -1,    -1,    18,     8,    -1,    30,    98,    50,
      51,    -1,    46,   656,    -1,   429,    -1,   691,   429,   692,
      -1,   691,   654,   692,    46,   656,    -1,   691,   654,   692,
     429,    -1,   691,   654,   692,   691,   429,   692,    -1,   675,
      -1,   675,   125,   675,    -1,   654,    67,   675,    -1,   654,
      67,   675,   125,   675,    -1,    -1,    65,    27,    43,     8,
     660,    -1,   657,    -1,   656,    67,   657,    -1,   658,    -1,
     691,   692,    -1,   691,   621,   692,    -1,   658,    -1,   661,
      -1,   660,    67,   661,    -1,   639,    95,    65,    -1,   639,
      95,    83,    -1,   639,    95,   612,    -1,   662,   663,   509,
      -1,   286,    -1,   241,    29,    -1,   285,    -1,   675,    -1,
      71,    -1,    28,    -1,    18,    -1,    10,    -1,    -1,   144,
      23,    -1,    -1,   144,    87,    23,    -1,    -1,   143,    -1,
      -1,   143,    72,    37,    -1,   129,    -1,   241,    -1,    78,
      -1,    28,    -1,    14,    -1,   309,    -1,   170,    -1,   356,
      -1,    69,    -1,   142,    -1,    22,    -1,    -1,   142,    -1,
      22,    -1,    44,    -1,   148,    -1,   147,    -1,    -1,   672,
      -1,    -1,    64,   675,    -1,    69,    -1,   689,    -1,   675,
      -1,   688,    -1,    69,    -1,   689,    -1,   677,    -1,   688,
      -1,    -1,   422,    71,    -1,    -1,   272,    -1,    -1,   682,
      40,    15,    71,    -1,    -1,    41,    -1,    -1,    38,    15,
      71,    -1,    -1,    39,    15,    71,    -1,    -1,   554,   683,
     681,   684,    -1,    -1,    37,   687,   683,    -1,    -1,    42,
      15,    71,    -1,   133,    -1,   159,    -1,    85,    -1,   357,
      -1,    22,    -1,    24,    -1,   267,    -1,   321,    -1,    94,
      -1,   123,    -1,   322,    -1,   323,    -1,   324,    -1,    15,
      -1,    19,    -1,    93,    -1,   120,    -1,   309,    -1,   325,
      -1,   128,    -1,    57,    -1,   300,    -1,   302,    -1,   298,
      -1,   299,    -1,   274,    -1,    28,    -1,     9,    -1,    25,
      -1,   161,    -1,   193,    -1,    21,    -1,   116,    -1,   130,
      -1,    90,    -1,    89,    -1,    88,    -1,   164,    -1,    23,
      -1,   162,    -1,    82,    -1,   158,    -1,    18,    -1,    54,
      -1,    10,    -1,    12,    -1,   319,    -1,   320,    -1,    13,
      -1,   144,    -1,   143,    -1,    98,    -1,   191,    -1,   139,
      -1,    59,    -1,     7,    -1,   124,    -1,    26,    -1,   101,
      -1,    63,    -1,   326,    -1,   327,    -1,   361,    -1,    43,
      -1,   362,    -1,    61,    -1,   100,    -1,    16,    -1,   303,
      -1,   304,    -1,    30,    -1,   349,    -1,   328,    -1,   169,
      -1,   363,    -1,   329,    -1,   115,    -1,   196,    -1,    56,
      -1,    48,    -1,    87,    -1,    80,    -1,    33,    -1,    83,
      -1,    65,    -1,    84,    -1,    14,    -1,   192,    -1,    58,
      -1,   317,    -1,   195,    -1,   364,    -1,    99,    -1,   131,
      -1,   308,    -1,    60,    -1,   137,    -1,     5,    -1,   314,
      -1,    29,    -1,   160,    -1,   332,    -1,   331,    -1,   330,
      -1,   333,    -1,   197,    -1,    62,    -1,   311,    -1,   312,
      -1,   334,    -1,   421,    -1,   138,    -1,    91,    -1,   315,
      -1,   316,    -1,   142,    -1,    81,    -1,     4,    -1,    44,
      -1,    31,    -1,     8,    -1,    55,    -1,    64,    -1,   305,
      -1,   306,    -1,   307,    -1,    46,    -1,   336,    -1,   335,
      -1,   337,    -1,    92,    -1,    11,    -1,   318,    -1,   350,
      -1,   151,    -1,   365,    -1,   366,    -1,   129,    -1,   186,
      -1,   213,    -1,   224,    -1,   218,    -1,   252,    -1,   240,
      -1,   351,    -1,   367,    -1,   152,    -1,   206,    -1,   136,
      -1,   238,    -1,   241,    -1,   286,    -1,   150,    -1,   199,
      -1,   368,    -1,   273,    -1,   281,    -1,   209,    -1,    78,
      -1,   216,    -1,   295,    -1,   369,    -1,   153,    -1,   203,
      -1,   205,    -1,   204,    -1,   194,    -1,   210,    -1,    35,
      -1,   163,    -1,   236,    -1,   229,    -1,   189,    -1,   370,
      -1,   358,    -1,   227,    -1,    27,    -1,   184,    -1,   371,
      -1,   283,    -1,   257,    -1,   372,    -1,   355,    -1,   282,
      -1,   232,    -1,   228,    -1,   134,    -1,   373,    -1,   154,
      -1,   148,    -1,   177,    -1,   374,    -1,   258,    -1,   262,
      -1,   375,    -1,   287,    -1,   376,    -1,   377,    -1,   378,
      -1,   140,    -1,   222,    -1,   223,    -1,   379,    -1,   190,
      -1,   289,    -1,   256,    -1,    32,    -1,   149,    -1,   352,
      -1,    47,    -1,   172,    -1,   290,    -1,    37,    -1,   260,
      -1,    36,    -1,   272,    -1,   380,    -1,   255,    -1,   250,
      -1,   381,    -1,   382,    -1,   383,    -1,   384,    -1,   254,
      -1,   221,    -1,   249,    -1,   201,    -1,    51,    -1,   135,
      -1,   264,    -1,   263,    -1,   265,    -1,   202,    -1,   285,
      -1,   245,    -1,   243,    -1,   385,    -1,   386,    -1,   155,
      -1,   387,    -1,   388,    -1,   230,    -1,    17,    -1,   389,
      -1,   390,    -1,   293,    -1,   166,    -1,   391,    -1,    41,
      -1,   392,    -1,   393,    -1,   394,    -1,   170,    -1,   395,
      -1,   396,    -1,   397,    -1,   284,    -1,   259,    -1,   261,
      -1,   188,    -1,   398,    -1,   233,    -1,   145,    -1,   399,
      -1,   174,    -1,   400,    -1,   354,    -1,   401,    -1,   291,
      -1,   226,    -1,   402,    -1,   156,    -1,   171,    -1,   165,
      -1,   294,    -1,   403,    -1,   404,    -1,   405,    -1,   406,
      -1,   157,    -1,   407,    -1,   408,    -1,   409,    -1,   217,
      -1,   275,    -1,   207,    -1,   410,    -1,   411,    -1,   412,
      -1,   413,    -1,   198,    -1,   183,    -1,   297,    -1,   288,
      -1,    50,    -1,   211,    -1,   269,    -1,   414,    -1,   220,
      -1,   147,    -1,   212,    -1,   415,    -1,   214,    -1,    42,
      -1,   180,    -1,     6,    -1,   200,    -1,   276,    -1,   208,
      -1,   247,    -1,   173,    -1,   416,    -1,   417,    -1,   234,
      -1,   235,    -1,   253,    -1,   219,    -1,   248,    -1,   215,
      -1,   175,    -1,   176,    -1,   167,    -1,   418,    -1,   296,
      -1,   270,    -1,   356,    -1,    49,    -1,   242,    -1,   239,
      -1,   181,    -1,   246,    -1,   419,    -1,   141,    -1,   178,
      -1,   179,    -1,   420,    -1,   277,    -1,   278,    -1,   182,
      -1,   353,    -1,   292,    -1,   237,    -1,   271,    -1,   321,
      -1,   322,    -1,   323,    -1,   324,    -1,   325,    -1,   338,
      -1,   339,    -1,   340,    -1,   326,    -1,   327,    -1,   341,
      -1,   342,    -1,   343,    -1,   328,    -1,   329,    -1,   344,
      -1,   345,    -1,   347,    -1,   348,    -1,   346,    -1,   332,
      -1,   331,    -1,   330,    -1,   333,    -1,   334,    -1,   336,
      -1,   335,    -1,   337,    -1,    66,    -1,    68,    -1,    -1,
      -1,   691,    -1,   676,    -1
};

/* YYRLINE[YYN] -- source line where rule number YYN was defined.  */
static const yytype_uint16 yyrline[] =
{
       0,   384,   384,   390,   391,   394,   398,   399,   400,   401,
     402,   403,   404,   405,   406,   407,   408,   409,   410,   411,
     412,   413,   414,   415,   416,   417,   418,   419,   420,   422,
     427,   433,   441,   445,   451,   458,   464,   468,   474,   478,
     485,   497,   509,   513,   519,   525,   529,   533,   537,   543,
     544,   547,   551,   557,   561,   567,   571,   577,   580,   586,
     590,   594,   598,   604,   608,   614,   618,   622,   628,   632,
     636,   640,   646,   650,   656,   661,   669,   674,   678,   682,
     686,   694,   702,   706,   712,   715,   721,   725,   731,   735,
     739,   743,   749,   752,   758,   762,   768,   772,   776,   780,
     784,   788,   792,   796,   800,   804,   810,   814,   815,   816,
     817,   818,   819,   820,   821,   822,   823,   824,   825,   826,
     827,   828,   829,   830,   831,   832,   833,   834,   837,   843,
     847,   853,   856,   862,   866,   872,   876,   880,   886,   889,
     893,   899,   903,   904,   905,   906,   909,   915,   919,   925,
     929,   935,   941,   946,   951,   957,   965,   969,   975,   981,
     985,   991,   995,  1001,  1005,  1011,  1017,  1021,  1022,  1023,
    1024,  1025,  1026,  1027,  1028,  1029,  1030,  1031,  1034,  1040,
    1043,  1047,  1053,  1057,  1063,  1067,  1068,  1069,  1070,  1071,
    1072,  1073,  1074,  1077,  1080,  1086,  1092,  1096,  1102,  1107,
    1113,  1119,  1130,  1137,  1142,  1146,  1150,  1156,  1166,  1176,
    1179,  1188,  1197,  1206,  1215,  1224,  1235,  1241,  1242,  1243,
    1246,  1251,  1257,  1261,  1265,  1269,  1273,  1277,  1281,  1285,
    1289,  1295,  1301,  1307,  1313,  1319,  1325,  1331,  1337,  1345,
    1349,  1353,  1357,  1361,  1367,  1371,  1375,  1379,  1383,  1387,
    1391,  1395,  1399,  1403,  1407,  1411,  1415,  1419,  1423,  1427,
    1431,  1435,  1439,  1443,  1447,  1451,  1455,  1459,  1464,  1470,
    1474,  1478,  1482,  1486,  1490,  1494,  1498,  1504,  1509,  1515,
    1518,  1524,  1527,  1536,  1539,  1545,  1554,  1557,  1563,  1566,
    1573,  1577,  1583,  1589,  1595,  1601,  1604,  1608,  1614,  1617,
    1621,  1627,  1631,  1635,  1639,  1645,  1651,  1655,  1661,  1664,
    1670,  1674,  1680,  1684,  1689,  1696,  1699,  1705,  1709,  1713,
    1717,  1721,  1727,  1731,  1735,  1741,  1745,  1751,  1754,  1760,
    1763,  1769,  1773,  1779,  1785,  1789,  1796,  1800,  1804,  1808,
    1812,  1818,  1822,  1828,  1831,  1835,  1841,  1847,  1853,  1857,
    1861,  1865,  1869,  1875,  1878,  1882,  1890,  1894,  1898,  1904,
    1908,  1912,  1918,  1921,  1927,  1928,  1929,  1932,  1936,  1943,
    1950,  1954,  1958,  1962,  1966,  1971,  1975,  1979,  1984,  1988,
    1992,  1999,  2005,  2009,  2015,  2020,  2026,  2029,  2033,  2039,
    2045,  2057,  2067,  2071,  2075,  2088,  2098,  2102,  2116,  2117,
    2121,  2122,  2123,  2124,  2125,  2128,  2134,  2138,  2144,  2148,
    2154,  2160,  2164,  2172,  2180,  2184,  2192,  2200,  2208,  2216,
    2226,  2230,  2235,  2241,  2246,  2250,  2254,  2258,  2263,  2267,
    2271,  2275,  2279,  2283,  2287,  2291,  2295,  2299,  2303,  2307,
    2311,  2315,  2319,  2323,  2328,  2338,  2342,  2346,  2350,  2356,
    2364,  2369,  2373,  2377,  2381,  2395,  2401,  2407,  2411,  2418,
    2421,  2427,  2431,  2438,  2441,  2445,  2452,  2455,  2459,  2466,
    2469,  2476,  2479,  2483,  2489,  2493,  2499,  2503,  2509,  2515,
    2521,  2527,  2528,  2531,  2535,  2541,  2545,  2546,  2547,  2550,
    2553,  2559,  2560,  2563,  2568,  2574,  2578,  2582,  2586,  2592,
    2597,  2597,  2607,  2610,  2616,  2620,  2624,  2630,  2633,  2637,
    2643,  2646,  2650,  2656,  2659,  2665,  2668,  2674,  2678,  2684,
    2688,  2692,  2696,  2703,  2707,  2711,  2717,  2720,  2726,  2729,
    2733,  2739,  2743,  2747,  2751,  2757,  2760,  2766,  2770,  2776,
    2777,  2780,  2784,  2788,  2794,  2800,  2805,  2816,  2820,  2824,
    2828,  2832,  2844,  2847,  2853,  2856,  2862,  2866,  2872,  2876,
    2889,  2893,  2897,  2901,  2907,  2909,  2913,  2915,  2919,  2921,
    2925,  2926,  2930,  2931,  2935,  2941,  2945,  2949,  2955,  2961,
    2965,  2969,  2973,  2979,  2983,  2993,  2999,  3003,  3009,  3013,
    3019,  3025,  3028,  3032,  3036,  3042,  3045,  3051,  3055,  3059,
    3063,  3067,  3071,  3075,  3082,  3085,  3091,  3095,  3101,  3105,
    3109,  3113,  3117,  3121,  3125,  3129,  3133,  3137,  3143,  3147,
    3151,  3155,  3159,  3163,  3169,  3173,  3177,  3181,  3185,  3189,
    3193,  3199,  3202,  3208,  3212,  3216,  3222,  3228,  3232,  3238,
    3242,  3246,  3250,  3254,  3258,  3262,  3266,  3270,  3274,  3278,
    3282,  3286,  3290,  3294,  3298,  3302,  3306,  3310,  3314,  3318,
    3322,  3326,  3330,  3338,  3352,  3356,  3360,  3368,  3369,  3370,
    3371,  3372,  3373,  3380,  3384,  3394,  3398,  3402,  3406,  3410,
    3414,  3418,  3422,  3426,  3430,  3434,  3438,  3442,  3446,  3450,
    3454,  3458,  3467,  3471,  3475,  3479,  3483,  3487,  3491,  3495,
    3499,  3503,  3507,  3517,  3521,  3525,  3529,  3533,  3537,  3541,
    3545,  3549,  3553,  3557,  3561,  3565,  3569,  3573,  3583,  3587,
    3591,  3596,  3601,  3606,  3612,  3617,  3621,  3626,  3630,  3634,
    3639,  3644,  3649,  3653,  3657,  3662,  3664,  3667,  3677,  3681,
    3685,  3689,  3693,  3697,  3704,  3707,  3711,  3715,  3719,  3725,
    3729,  3735,  3739,  3743,  3747,  3751,  3755,  3761,  3765,  3769,
    3773,  3777,  3781,  3785,  3791,  3794,  3800,  3803,  3809,  3813,
    3819,  3825,  3828,  3834,  3838,  3842,  3848,  3852,  3856,  3860,
    3864,  3868,  3872,  3876,  3882,  3886,  3890,  3896,  3899,  3905,
    3909,  3915,  3919,  3925,  3928,  3934,  3938,  3944,  3947,  3953,
    3957,  3963,  3967,  3973,  3976,  3980,  3986,  3989,  3993,  3997,
    4003,  4006,  4010,  4023,  4027,  4031,  4036,  4040,  4044,  4051,
    4055,  4059,  4063,  4069,  4072,  4078,  4082,  4088,  4092,  4098,
    4104,  4114,  4118,  4124,  4128,  4132,  4136,  4142,  4143,  4147,
    4150,  4154,  4158,  4164,  4165,  4168,  4169,  4173,  4174,  4178,
    4179,  4183,  4184,  4188,  4190,  4192,  4194,  4196,  4198,  4200,
    4202,  4204,  4208,  4210,  4214,  4215,  4217,  4221,  4223,  4225,
    4229,  4230,  4234,  4235,  4239,  4243,  4249,  4250,  4256,  4260,
    4266,  4267,  4273,  4274,  4278,  4279,  4283,  4286,  4292,  4295,
    4301,  4304,  4310,  4313,  4319,  4322,  4328,  4331,  4337,  4340,
    4355,  4356,  4357,  4358,  4359,  4360,  4361,  4362,  4363,  4364,
    4365,  4366,  4367,  4368,  4369,  4370,  4371,  4372,  4373,  4374,
    4375,  4376,  4377,  4378,  4379,  4380,  4381,  4382,  4383,  4384,
    4385,  4386,  4387,  4388,  4389,  4390,  4391,  4392,  4393,  4394,
    4395,  4396,  4397,  4398,  4399,  4400,  4401,  4402,  4403,  4404,
    4405,  4406,  4407,  4408,  4409,  4410,  4411,  4412,  4413,  4414,
    4415,  4416,  4417,  4418,  4419,  4420,  4421,  4422,  4423,  4424,
    4425,  4426,  4427,  4428,  4429,  4430,  4431,  4432,  4433,  4434,
    4435,  4436,  4437,  4438,  4439,  4440,  4441,  4442,  4443,  4444,
    4445,  4446,  4447,  4448,  4449,  4450,  4451,  4452,  4453,  4454,
    4455,  4456,  4457,  4458,  4459,  4460,  4461,  4462,  4463,  4464,
    4465,  4466,  4467,  4468,  4469,  4470,  4471,  4472,  4473,  4474,
    4475,  4476,  4477,  4478,  4479,  4480,  4481,  4482,  4483,  4484,
    4485,  4486,  4487,  4497,  4498,  4499,  4500,  4501,  4502,  4503,
    4504,  4505,  4506,  4507,  4508,  4509,  4510,  4511,  4512,  4513,
    4514,  4515,  4516,  4517,  4518,  4519,  4520,  4521,  4522,  4523,
    4524,  4525,  4526,  4527,  4528,  4529,  4530,  4531,  4532,  4533,
    4534,  4535,  4536,  4537,  4538,  4539,  4540,  4541,  4542,  4543,
    4544,  4545,  4546,  4547,  4548,  4549,  4550,  4551,  4552,  4553,
    4554,  4555,  4556,  4557,  4558,  4559,  4560,  4561,  4562,  4563,
    4564,  4565,  4566,  4567,  4568,  4569,  4570,  4571,  4572,  4573,
    4574,  4575,  4576,  4577,  4578,  4579,  4580,  4581,  4582,  4583,
    4584,  4585,  4586,  4587,  4588,  4589,  4590,  4591,  4592,  4593,
    4594,  4595,  4596,  4597,  4598,  4599,  4600,  4601,  4602,  4603,
    4604,  4605,  4606,  4607,  4608,  4609,  4610,  4611,  4612,  4613,
    4614,  4615,  4616,  4617,  4618,  4619,  4620,  4621,  4622,  4623,
    4624,  4625,  4626,  4627,  4628,  4629,  4630,  4631,  4632,  4633,
    4634,  4635,  4636,  4637,  4638,  4639,  4640,  4641,  4642,  4643,
    4644,  4645,  4646,  4647,  4648,  4649,  4650,  4651,  4652,  4653,
    4654,  4655,  4656,  4657,  4658,  4659,  4660,  4661,  4662,  4663,
    4664,  4665,  4666,  4667,  4668,  4669,  4670,  4671,  4672,  4673,
    4674,  4675,  4676,  4677,  4678,  4679,  4680,  4681,  4682,  4683,
    4684,  4685,  4686,  4687,  4688,  4689,  4690,  4691,  4692,  4693,
    4694,  4695,  4696,  4697,  4698,  4699,  4700,  4701,  4702,  4703,
    4704,  4705,  4706,  4711,  4712,  4713,  4714,  4715,  4716,  4717,
    4718,  4719,  4720,  4721,  4722,  4723,  4724,  4725,  4726,  4727,
    4728,  4729,  4730,  4731,  4732,  4733,  4734,  4735,  4736,  4737,
    4738,  4741,  4750,  4756,  4761,  4764,  4768
};
#endif

#if YYDEBUG || YYERROR_VERBOSE || YYTOKEN_TABLE
/* YYTNAME[SYMBOL-NUM] -- String name of the symbol SYMBOL-NUM.
   First, the terminals, then, starting at YYNTOKENS, nonterminals.  */
static const char *const yytname[] =
{
  "$end", "error", "$undefined", "LEX_ERROR", "UNION", "SELECT", "STREAM",
  "INSERT", "UPDATE", "DELETE", "FROM", "WHERE", "GROUP", "HAVING",
  "ORDER", "BY", "LIMIT", "OFFSET", "FOR", "CALL", "ALL", "DISTINCT", "AS",
  "EXISTS", "ASC", "DESC", "INTO", "DUPLICATE", "DEFAULT", "SET", "LOCK",
  "UNLOCK", "KEYS", "OF", "OUTFILE", "DATA", "LOAD", "LINES", "TERMINATED",
  "ESCAPED", "ENCLOSED", "OPTIONALLY", "STARTING", "KEY", "UNIQUE",
  "SYSTEM_TIME", "VALUES", "LAST_INSERT_ID", "NEXT", "VALUE", "SHARE",
  "MODE", "SQL_NO_CACHE", "SQL_CACHE", "FORCE", "USE", "NATURAL", "CROSS",
  "OUTER", "INNER", "RIGHT", "LEFT", "STRAIGHT_JOIN", "JOIN", "USING",
  "ON", "'('", "','", "')'", "ID", "HEX", "STRING", "INTEGRAL", "FLOAT",
  "HEXNUM", "VALUE_ARG", "LIST_ARG", "COMMENT", "COMMENT_KEYWORD",
  "BIT_LITERAL", "NULL", "TRUE", "FALSE", "OFF", "OR", "AND", "'!'", "NOT",
  "END", "ELSEIF", "ELSE", "THEN", "WHEN", "CASE", "BETWEEN", "'='", "'<'",
  "'>'", "IN", "REGEXP", "LIKE", "IS", "NULL_SAFE_EQUAL", "NE", "GE", "LE",
  "'|'", "'&'", "SHIFT_RIGHT", "SHIFT_LEFT", "'+'", "'-'", "'*'", "'/'",
  "'%'", "MOD", "DIV", "'^'", "'~'", "UNARY", "COLLATE",
  "UNDERSCORE_UTF8MB4", "UNDERSCORE_BINARY", "BINARY", "INTERVAL", "'.'",
  "JSON_EXTRACT_OP", "JSON_UNQUOTE_EXTRACT_OP", "CREATE", "ALTER", "DROP",
  "RENAME", "ANALYZE", "ADD", "FLUSH", "MODIFY", "CHANGE", "SCHEMA",
  "TABLE", "INDEX", "INDEXES", "VIEW", "TO", "IGNORE", "IF", "PRIMARY",
  "COLUMN", "SPATIAL", "FULLTEXT", "KEY_BLOCK_SIZE", "CHECK", "ACTION",
  "CASCADE", "CONSTRAINT", "FOREIGN", "NO", "REFERENCES", "RESTRICT",
  "FIRST", "AFTER", "SHOW", "DESCRIBE", "EXPLAIN", "DATE", "ESCAPE",
  "REPAIR", "OPTIMIZE", "TRUNCATE", "FORMAT", "MAXVALUE", "PARTITION",
  "REORGANIZE", "LESS", "THAN", "PROCEDURE", "TRIGGER", "TRIGGERS",
  "FUNCTION", "VINDEX", "VINDEXES", "STATUS", "VARIABLES", "WARNINGS",
  "SEQUENCE", "EACH", "ROW", "BEFORE", "FOLLOWS", "PRECEDES", "DEFINER",
  "INVOKER", "INOUT", "OUT", "DETERMINISTIC", "CONTAINS", "READS",
  "MODIFIES", "SQL", "SECURITY", "CLASS_ORIGIN", "SUBCLASS_ORIGIN",
  "MESSAGE_TEXT", "MYSQL_ERRNO", "CONSTRAINT_CATALOG", "CONSTRAINT_SCHEMA",
  "CONSTRAINT_NAME", "CATALOG_NAME", "SCHEMA_NAME", "TABLE_NAME",
  "COLUMN_NAME", "CURSOR_NAME", "SIGNAL", "SQLSTATE", "BEGIN", "START",
  "TRANSACTION", "COMMIT", "ROLLBACK", "BIT", "TINYINT", "SMALLINT",
  "MEDIUMINT", "INT", "INTEGER", "BIGINT", "INTNUM", "REAL", "DOUBLE",
  "FLOAT_TYPE", "DECIMAL", "NUMERIC", "DEC", "FIXED", "PRECISION", "TIME",
  "TIMESTAMP", "DATETIME", "YEAR", "CHAR", "VARCHAR", "BOOL", "CHARACTER",
  "VARBINARY", "NCHAR", "NVARCHAR", "NATIONAL", "VARYING", "TEXT",
  "TINYTEXT", "MEDIUMTEXT", "LONGTEXT", "LONG", "BLOB", "TINYBLOB",
  "MEDIUMBLOB", "LONGBLOB", "JSON", "ENUM", "GEOMETRY", "POINT",
  "LINESTRING", "POLYGON", "GEOMETRYCOLLECTION", "MULTIPOINT",
  "MULTILINESTRING", "MULTIPOLYGON", "NULLX", "AUTO_INCREMENT",
  "APPROXNUM", "SIGNED", "UNSIGNED", "ZEROFILL", "LOCAL", "COLLATION",
  "DATABASES", "SCHEMAS", "TABLES", "VITESS_METADATA", "VSCHEMA", "FULL",
  "PROCESSLIST", "COLUMNS", "FIELDS", "ENGINES", "PLUGINS", "NAMES",
  "CHARSET", "GLOBAL", "SESSION", "ISOLATION", "LEVEL", "READ", "WRITE",
  "ONLY", "REPEATABLE", "COMMITTED", "UNCOMMITTED", "SERIALIZABLE",
  "CURRENT_TIMESTAMP", "DATABASE", "CURRENT_DATE", "CURRENT_USER",
  "CURRENT_TIME", "LOCALTIME", "LOCALTIMESTAMP", "UTC_DATE", "UTC_TIME",
  "UTC_TIMESTAMP", "REPLACE", "CONVERT", "CAST", "SUBSTR", "SUBSTRING",
  "GROUP_CONCAT", "SEPARATOR", "TIMESTAMPADD", "TIMESTAMPDIFF", "OVER",
  "WINDOW", "GROUPING", "GROUPS", "AVG", "BIT_AND", "BIT_OR", "BIT_XOR",
  "COUNT", "JSON_ARRAYAGG", "JSON_OBJECTAGG", "MAX", "MIN", "STDDEV_POP",
  "STDDEV", "STD", "STDDEV_SAMP", "SUM", "VAR_POP", "VARIANCE", "VAR_SAMP",
  "CUME_DIST", "DENSE_RANK", "FIRST_VALUE", "LAG", "LAST_VALUE", "LEAD",
  "NTH_VALUE", "NTILE", "ROW_NUMBER", "PERCENT_RANK", "RANK", "MATCH",
  "AGAINST", "BOOLEAN", "LANGUAGE", "WITH", "QUERY", "EXPANSION", "UNUSED",
  "ARRAY", "DESCRIPTION", "EMPTY", "EXCEPT", "JSON_TABLE", "LATERAL",
  "MEMBER", "RECURSIVE", "ACTIVE", "ADMIN", "BUCKETS", "CLONE",
  "COMPONENT", "DEFINITION", "ENFORCED", "EXCLUDE", "FOLLOWING",
  "GEOMCOLLECTION", "GET_MASTER_PUBLIC_KEY", "HISTOGRAM", "HISTORY",
  "INACTIVE", "INVISIBLE", "LOCKED", "MASTER_COMPRESSION_ALGORITHMS",
  "MASTER_PUBLIC_KEY_PATH", "MASTER_TLS_CIPHERSUITES",
  "MASTER_ZSTD_COMPRESSION_LEVEL", "NESTED", "NETWORK_NAMESPACE", "NOWAIT",
  "NULLS", "OJ", "OLD", "OPTIONAL", "ORDINALITY", "ORGANIZATION", "OTHERS",
  "PATH", "PERSIST", "PERSIST_ONLY", "PRECEDING", "PRIVILEGE_CHECKS_USER",
  "PROCESS", "RANDOM", "REFERENCE", "REQUIRE_ROW_FORMAT", "RESOURCE",
  "RESPECT", "RESTART", "RETAIN", "REUSE", "ROLE", "SECONDARY",
  "SECONDARY_ENGINE", "SECONDARY_LOAD", "SECONDARY_UNLOAD", "SKIP", "SRID",
  "THREAD_PRIORITY", "TIES", "UNBOUNDED", "VCPU", "VISIBLE", "SYSTEM",
  "INFILE", "';'", "$accept", "any_command", "semicolon_opt", "command",
  "load_statement", "select_statement", "stream_statement", "base_select",
  "union_lhs", "union_rhs", "insert_statement", "insert_or_replace",
  "update_statement", "delete_statement", "from_or_using",
  "view_name_list", "table_name_list", "delete_table_list",
  "opt_partition_clause", "set_statement", "transaction_chars",
  "transaction_char", "isolation_level", "set_session_or_global",
  "lexer_position", "create_statement", "proc_param_list_opt",
  "proc_param_list", "proc_param", "characteristic_list_opt",
  "characteristic_list", "characteristic", "proc_allowed_statement",
  "proc_begin_end_block", "proc_allowed_statement_list", "definer_opt",
  "trigger_time", "trigger_event", "trigger_order_opt", "trigger_body",
  "trigger_begin_end_block", "case_statement", "case_statement_case_list",
  "case_statement_case", "if_statement", "elseif_list", "elseif_list_item",
  "signal_statement", "signal_condition_value",
  "signal_information_item_list", "signal_information_item",
  "signal_information_name", "call_statement", "call_param_list_opt",
  "statement_list", "statement_list_statement", "vindex_type_opt",
  "vindex_type", "vindex_params_opt", "vindex_param_list", "vindex_param",
  "create_table_prefix", "table_spec", "table_column_list",
  "column_definition", "column_definition_for_create",
  "column_type_options", "column_type", "numeric_type", "int_type",
  "decimal_type", "time_type", "char_type", "spatial_type", "enum_values",
  "length_opt", "float_length_opt", "decimal_length_opt", "unsigned_opt",
  "zero_fill_opt", "null_or_not_null", "column_default", "on_update",
  "auto_increment", "charset_opt", "collate_opt", "column_key",
  "column_comment", "index_definition", "index_option_list_opt",
  "index_option_list", "index_option", "equal_opt", "index_info",
  "indexes_or_keys", "index_or_key", "index_or_key_opt", "name_opt",
  "index_column_list", "index_column", "constraint_definition",
  "constraint_info", "from_or_in", "show_database_opt", "fk_on_delete",
  "fk_on_update", "fk_reference_action", "table_option_list",
  "table_option", "table_opt_value", "constraint_symbol_opt",
  "alter_statement", "alter_table_statement", "column_order_opt",
  "alter_view_statement", "alter_vschema_statement", "column_opt",
  "ignored_alter_object_type", "partition_operation",
  "partition_definitions", "partition_definition", "rename_statement",
  "rename_list", "drop_statement", "truncate_statement",
  "analyze_statement", "show_statement", "naked_like",
  "tables_or_processlist", "full_opt", "columns_or_fields",
  "from_database_opt", "like_or_where_opt", "like_opt",
  "show_session_or_global", "use_statement", "begin_statement",
  "start_transaction_statement", "commit_statement", "rollback_statement",
  "describe", "explain_statement", "explainable_statement", "format_opt",
  "explain_verb", "describe_statement", "other_statement",
  "flush_statement", "comment_opt", "@1", "comment_list", "union_op",
  "cache_opt", "distinct_opt", "straight_join_opt",
  "select_expression_list_opt", "select_expression_list",
  "select_expression", "over", "over_opt", "as_ci_opt", "col_alias",
  "from_opt", "table_references", "table_reference", "table_factor",
  "aliased_table_name", "aliased_table_options", "as_of_opt",
  "column_list_opt", "column_list", "partition_list", "join_table",
  "join_condition", "join_condition_opt", "on_expression_opt", "as_opt",
  "table_alias", "inner_join", "straight_join", "outer_join",
  "natural_join", "load_into_table_name", "into_table_name", "table_name",
  "delete_table_name", "index_hint_list", "where_expression_opt",
  "expression", "default_opt", "boolean_value", "condition", "is_suffix",
  "compare", "like_escape_opt", "col_tuple", "subquery", "expression_list",
  "value_expression", "function_call_generic",
  "function_call_aggregate_with_window", "function_call_window",
  "function_call_keyword", "function_call_nonkeyword", "func_datetime_opt",
  "func_datetime_precision", "function_call_conflict", "match_option",
  "charset", "convert_type", "expression_opt", "separator_opt",
  "when_expression_list", "when_expression", "else_expression_opt",
  "column_name", "value", "num_val", "group_by_opt", "group_by_list",
  "group_by", "having_opt", "having", "order_by_opt", "order_list",
  "order", "asc_desc_opt", "limit_opt", "lock_opt", "insert_data",
  "ins_column_list", "on_dup_opt", "tuple_list", "tuple_or_empty",
  "row_tuple", "tuple_expression", "set_list", "set_expression",
  "charset_or_character_set", "charset_value", "for_from", "exists_opt",
  "not_exists_opt", "ignore_opt", "ignore_number_opt",
  "non_add_drop_or_rename_operation", "to_or_as", "to_opt", "key_type",
  "key_type_opt", "using_opt", "sql_id", "reserved_sql_id", "table_id",
  "reserved_table_id", "infile_opt", "local_opt", "enclosed_by_opt",
  "optionally_opt", "terminated_by_opt", "escaped_by_opt", "fields_opt",
  "lines_opt", "starting_by_opt", "reserved_keyword",
  "non_reserved_keyword", "column_name_safe_reserved_keyword", "openb",
  "closeb", "skip_to_end", "ddl_skip_to_end", 0
};
#endif

# ifdef YYPRINT
/* YYTOKNUM[YYLEX-NUM] -- Internal token number corresponding to
   token YYLEX-NUM.  */
static const yytype_uint16 yytoknum[] =
{
       0,   256,   257,   258,   259,   260,   261,   262,   263,   264,
     265,   266,   267,   268,   269,   270,   271,   272,   273,   274,
     275,   276,   277,   278,   279,   280,   281,   282,   283,   284,
     285,   286,   287,   288,   289,   290,   291,   292,   293,   294,
     295,   296,   297,   298,   299,   300,   301,   302,   303,   304,
     305,   306,   307,   308,   309,   310,   311,   312,   313,   314,
     315,   316,   317,   318,   319,   320,    40,    44,    41,   321,
     322,   323,   324,   325,   326,   327,   328,   329,   330,   331,
     332,   333,   334,   335,   336,   337,    33,   338,   339,   340,
     341,   342,   343,   344,   345,    61,    60,    62,   346,   347,
     348,   349,   350,   351,   352,   353,   124,    38,   354,   355,
      43,    45,    42,    47,    37,   356,   357,    94,   126,   358,
     359,   360,   361,   362,   363,    46,   364,   365,   366,   367,
     368,   369,   370,   371,   372,   373,   374,   375,   376,   377,
     378,   379,   380,   381,   382,   383,   384,   385,   386,   387,
     388,   389,   390,   391,   392,   393,   394,   395,   396,   397,
     398,   399,   400,   401,   402,   403,   404,   405,   406,   407,
     408,   409,   410,   411,   412,   413,   414,   415,   416,   417,
     418,   419,   420,   421,   422,   423,   424,   425,   426,   427,
     428,   429,   430,   431,   432,   433,   434,   435,   436,   437,
     438,   439,   440,   441,   442,   443,   444,   445,   446,   447,
     448,   449,   450,   451,   452,   453,   454,   455,   456,   457,
     458,   459,   460,   461,   462,   463,   464,   465,   466,   467,
     468,   469,   470,   471,   472,   473,   474,   475,   476,   477,
     478,   479,   480,   481,   482,   483,   484,   485,   486,   487,
     488,   489,   490,   491,   492,   493,   494,   495,   496,   497,
     498,   499,   500,   501,   502,   503,   504,   505,   506,   507,
     508,   509,   510,   511,   512,   513,   514,   515,   516,   517,
     518,   519,   520,   521,   522,   523,   524,   525,   526,   527,
     528,   529,   530,   531,   532,   533,   534,   535,   536,   537,
     538,   539,   540,   541,   542,   543,   544,   545,   546,   547,
     548,   549,   550,   551,   552,   553,   554,   555,   556,   557,
     558,   559,   560,   561,   562,   563,   564,   565,   566,   567,
     568,   569,   570,   571,   572,   573,   574,   575,   576,   577,
     578,   579,   580,   581,   582,   583,   584,   585,   586,   587,
     588,   589,   590,   591,   592,   593,   594,   595,   596,   597,
     598,   599,   600,   601,   602,   603,   604,   605,   606,   607,
     608,   609,   610,   611,   612,   613,   614,   615,   616,   617,
     618,   619,   620,   621,   622,   623,   624,   625,   626,   627,
     628,   629,   630,   631,   632,   633,   634,   635,   636,   637,
     638,   639,   640,   641,   642,   643,   644,   645,   646,   647,
     648,   649,   650,   651,   652,   653,   654,   655,   656,   657,
     658,   659,   660,    59
};
# endif

/* YYR1[YYN] -- Symbol number of symbol that rule YYN derives.  */
static const yytype_uint16 yyr1[] =
{
       0,   424,   425,   426,   426,   427,   427,   427,   427,   427,
     427,   427,   427,   427,   427,   427,   427,   427,   427,   427,
     427,   427,   427,   427,   427,   427,   427,   427,   427,   427,
     428,   429,   429,   429,   430,   431,   432,   432,   433,   433,
     434,   434,   435,   435,   436,   437,   437,   437,   437,   438,
     438,   439,   439,   440,   440,   441,   441,   442,   442,   443,
     443,   443,   443,   444,   444,   445,   445,   445,   446,   446,
     446,   446,   447,   447,   448,   449,   449,   449,   449,   449,
     449,   449,   449,   449,   450,   450,   451,   451,   452,   452,
     452,   452,   453,   453,   454,   454,   455,   455,   455,   455,
     455,   455,   455,   455,   455,   455,   456,   456,   456,   456,
     456,   456,   456,   456,   456,   456,   456,   456,   456,   456,
     456,   456,   456,   456,   456,   456,   456,   456,   457,   458,
     458,   459,   459,   460,   460,   461,   461,   461,   462,   462,
     462,   463,   463,   463,   463,   463,   464,   465,   465,   466,
     466,   467,   468,   468,   468,   468,   469,   469,   470,   471,
     471,   472,   472,   473,   473,   474,   475,   475,   475,   475,
     475,   475,   475,   475,   475,   475,   475,   475,   476,   477,
     477,   477,   478,   478,   479,   479,   479,   479,   479,   479,
     479,   479,   479,   480,   480,   481,   482,   482,   483,   483,
     484,   485,   486,   487,   487,   487,   487,   488,   489,   490,
     490,   490,   490,   490,   490,   490,   491,   491,   491,   491,
     492,   492,   493,   493,   493,   493,   493,   493,   493,   493,
     493,   494,   494,   494,   494,   494,   494,   494,   494,   495,
     495,   495,   495,   495,   496,   496,   496,   496,   496,   496,
     496,   496,   496,   496,   496,   496,   496,   496,   496,   496,
     496,   496,   496,   496,   496,   496,   496,   496,   496,   497,
     497,   497,   497,   497,   497,   497,   497,   498,   498,   499,
     499,   500,   500,   501,   501,   501,   502,   502,   503,   503,
     504,   504,   505,   506,   507,   508,   508,   508,   509,   509,
     509,   510,   510,   510,   510,   511,   512,   512,   513,   513,
     514,   514,   515,   515,   515,   516,   516,   517,   517,   517,
     517,   517,   518,   518,   518,   519,   519,   520,   520,   521,
     521,   522,   522,   523,   524,   524,   525,   525,   525,   525,
     525,   526,   526,   527,   527,   527,   528,   529,   530,   530,
     530,   530,   530,   531,   531,   531,   532,   532,   532,   533,
     533,   533,   534,   534,   535,   535,   535,   536,   536,   536,
     536,   536,   536,   536,   536,   536,   536,   536,   536,   536,
     536,   536,   536,   536,   536,   536,   537,   537,   537,   538,
     539,   539,   539,   539,   539,   539,   539,   539,   540,   540,
     541,   541,   541,   541,   541,   542,   543,   543,   544,   544,
     545,   546,   546,   547,   547,   547,   547,   547,   547,   547,
     548,   548,   549,   550,   550,   550,   550,   550,   550,   550,
     550,   550,   550,   550,   550,   550,   550,   550,   550,   550,
     550,   550,   550,   550,   550,   550,   550,   550,   550,   550,
     550,   550,   550,   550,   550,   550,   551,   552,   552,   553,
     553,   554,   554,   555,   555,   555,   556,   556,   556,   557,
     557,   558,   558,   558,   559,   559,   560,   560,   561,   562,
     563,   564,   564,   565,   565,   566,   566,   566,   566,   567,
     567,   568,   568,   569,   569,   570,   570,   570,   570,   571,
     573,   572,   574,   574,   575,   575,   575,   576,   576,   576,
     577,   577,   577,   578,   578,   579,   579,   580,   580,   581,
     581,   581,   581,   582,   582,   582,   583,   583,   584,   584,
     584,   585,   585,   585,   585,   586,   586,   587,   587,   588,
     588,   589,   589,   589,   589,   590,   590,   591,   591,   591,
     591,   591,   592,   592,   593,   593,   594,   594,   595,   595,
     596,   596,   596,   596,   597,   597,   598,   598,   599,   599,
     600,   600,   601,   601,   601,   602,   602,   602,   603,   604,
     604,   604,   604,   605,   605,   606,   607,   607,   608,   608,
     609,   610,   610,   610,   610,   611,   611,   612,   612,   612,
     612,   612,   612,   612,   613,   613,   614,   614,   615,   615,
     615,   615,   615,   615,   615,   615,   615,   615,   616,   616,
     616,   616,   616,   616,   617,   617,   617,   617,   617,   617,
     617,   618,   618,   619,   619,   619,   620,   621,   621,   622,
     622,   622,   622,   622,   622,   622,   622,   622,   622,   622,
     622,   622,   622,   622,   622,   622,   622,   622,   622,   622,
     622,   622,   622,   622,   622,   622,   622,   622,   622,   622,
     622,   622,   622,   623,   623,   624,   624,   624,   624,   624,
     624,   624,   624,   624,   624,   624,   624,   624,   624,   624,
     624,   624,   625,   625,   625,   625,   625,   625,   625,   625,
     625,   625,   625,   626,   626,   626,   626,   626,   626,   626,
     626,   626,   626,   626,   626,   626,   626,   626,   627,   627,
     627,   627,   627,   627,   627,   627,   627,   627,   627,   627,
     627,   627,   627,   627,   627,   628,   628,   629,   630,   630,
     630,   630,   630,   630,   631,   631,   631,   631,   631,   632,
     632,   633,   633,   633,   633,   633,   633,   633,   633,   633,
     633,   633,   633,   633,   634,   634,   635,   635,   636,   636,
     637,   638,   638,   639,   639,   639,   640,   640,   640,   640,
     640,   640,   640,   640,   641,   641,   641,   642,   642,   643,
     643,   644,   644,   645,   645,   646,   646,   647,   647,   648,
     648,   649,   649,   650,   650,   650,   651,   651,   651,   651,
     652,   652,   652,   653,   653,   653,   653,   653,   653,   654,
     654,   654,   654,   655,   655,   656,   656,   657,   657,   658,
     659,   660,   660,   661,   661,   661,   661,   662,   662,   662,
     663,   663,   663,   664,   664,   665,   665,   666,   666,   667,
     667,   668,   668,   669,   669,   669,   669,   669,   669,   669,
     669,   669,   670,   670,   671,   671,   671,   672,   672,   672,
     673,   673,   674,   674,   675,   675,   676,   676,   677,   677,
     678,   678,   679,   679,   680,   680,   681,   681,   682,   682,
     683,   683,   684,   684,   685,   685,   686,   686,   687,   687,
     688,   688,   688,   688,   688,   688,   688,   688,   688,   688,
     688,   688,   688,   688,   688,   688,   688,   688,   688,   688,
     688,   688,   688,   688,   688,   688,   688,   688,   688,   688,
     688,   688,   688,   688,   688,   688,   688,   688,   688,   688,
     688,   688,   688,   688,   688,   688,   688,   688,   688,   688,
     688,   688,   688,   688,   688,   688,   688,   688,   688,   688,
     688,   688,   688,   688,   688,   688,   688,   688,   688,   688,
     688,   688,   688,   688,   688,   688,   688,   688,   688,   688,
     688,   688,   688,   688,   688,   688,   688,   688,   688,   688,
     688,   688,   688,   688,   688,   688,   688,   688,   688,   688,
     688,   688,   688,   688,   688,   688,   688,   688,   688,   688,
     688,   688,   688,   688,   688,   688,   688,   688,   688,   688,
     688,   688,   688,   688,   688,   688,   688,   688,   688,   688,
     688,   688,   688,   689,   689,   689,   689,   689,   689,   689,
     689,   689,   689,   689,   689,   689,   689,   689,   689,   689,
     689,   689,   689,   689,   689,   689,   689,   689,   689,   689,
     689,   689,   689,   689,   689,   689,   689,   689,   689,   689,
     689,   689,   689,   689,   689,   689,   689,   689,   689,   689,
     689,   689,   689,   689,   689,   689,   689,   689,   689,   689,
     689,   689,   689,   689,   689,   689,   689,   689,   689,   689,
     689,   689,   689,   689,   689,   689,   689,   689,   689,   689,
     689,   689,   689,   689,   689,   689,   689,   689,   689,   689,
     689,   689,   689,   689,   689,   689,   689,   689,   689,   689,
     689,   689,   689,   689,   689,   689,   689,   689,   689,   689,
     689,   689,   689,   689,   689,   689,   689,   689,   689,   689,
     689,   689,   689,   689,   689,   689,   689,   689,   689,   689,
     689,   689,   689,   689,   689,   689,   689,   689,   689,   689,
     689,   689,   689,   689,   689,   689,   689,   689,   689,   689,
     689,   689,   689,   689,   689,   689,   689,   689,   689,   689,
     689,   689,   689,   689,   689,   689,   689,   689,   689,   689,
     689,   689,   689,   689,   689,   689,   689,   689,   689,   689,
     689,   689,   689,   689,   689,   689,   689,   689,   689,   689,
     689,   689,   689,   689,   689,   689,   689,   689,   689,   689,
     689,   689,   689,   689,   689,   689,   689,   689,   689,   689,
     689,   689,   689,   690,   690,   690,   690,   690,   690,   690,
     690,   690,   690,   690,   690,   690,   690,   690,   690,   690,
     690,   690,   690,   690,   690,   690,   690,   690,   690,   690,
     690,   691,   692,   693,   694,   694,   694
};

/* YYR2[YYN] -- Number of symbols composing right hand side of rule YYN.  */
static const yytype_uint8 yyr2[] =
{
       0,     2,     2,     0,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     0,
      11,     4,     6,     7,     5,    10,     1,     3,     1,     3,
       7,     8,     1,     1,     9,     8,     7,     6,     6,     1,
       1,     1,     3,     1,     3,     1,     3,     0,     4,     3,
       4,     5,     4,     1,     3,     3,     2,     2,     2,     2,
       2,     1,     1,     1,     0,     2,     3,    11,     7,     9,
       5,     5,    15,    11,     0,     1,     1,     3,     2,     3,
       3,     3,     0,     1,     1,     2,     2,     2,     2,     1,
       2,     2,     3,     3,     3,     3,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     4,     1,
       3,     0,     3,     1,     1,     1,     1,     1,     0,     2,
       2,     1,     1,     1,     1,     1,     4,     5,     8,     1,
       2,     5,     7,    10,     8,    11,     1,     2,     5,     2,
       4,     2,     3,     1,     3,     3,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     3,     0,
       2,     3,     1,     3,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     0,     2,     1,     0,     2,     1,     3,
       3,     4,     4,     1,     3,     3,     3,     3,     3,     0,
       2,     2,     2,     2,     2,     2,     3,     1,     1,     1,
       2,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     2,     2,     3,     2,     2,     2,     2,     2,     1,
       2,     2,     2,     1,     4,     4,     3,     3,     2,     4,
       5,     2,     3,     4,     2,     2,     3,     3,     3,     3,
       3,     4,     1,     1,     1,     1,     1,     6,     6,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     3,     0,
       3,     0,     5,     0,     3,     5,     0,     1,     0,     1,
       1,     2,     2,     3,     1,     0,     3,     3,     0,     2,
       2,     2,     1,     2,     1,     2,     5,     4,     0,     1,
       1,     2,     2,     3,     2,     0,     1,     2,     3,     3,
       2,     2,     1,     1,     1,     1,     1,     0,     1,     0,
       1,     1,     3,     3,     3,     1,    10,    11,    11,    12,
      12,     1,     1,     0,     2,     2,     3,     3,     1,     1,
       2,     2,     2,     0,     1,     3,     1,     2,     3,     1,
       1,     1,     0,     2,     1,     1,     1,     6,    10,     9,
       7,     7,     7,     9,     7,    12,    14,     7,     7,     9,
       9,    10,     5,     6,     8,     7,     0,     1,     2,     4,
       7,     5,     5,     5,    12,     7,     5,     9,     0,     1,
       1,     1,     1,     1,     1,     7,     1,     3,     8,     8,
       3,     3,     5,     4,     6,     4,     4,     4,     4,     4,
       3,     2,     3,     4,     4,     3,     6,     6,     4,     4,
       4,     4,     4,     4,     3,     3,     2,     6,     2,     4,
       4,     4,     3,     7,     6,     5,     4,     2,     4,     3,
       4,     3,     3,     5,     2,     3,     3,     1,     1,     0,
       1,     1,     1,     0,     2,     2,     0,     2,     2,     0,
       2,     0,     1,     1,     2,     1,     1,     1,     2,     1,
       1,     1,     1,     3,     3,     1,     1,     1,     1,     0,
       3,     1,     1,     2,     2,     2,     2,     3,     3,     2,
       0,     2,     0,     2,     1,     2,     2,     0,     1,     1,
       0,     1,     1,     0,     1,     0,     1,     1,     3,     1,
       2,     3,     5,     2,     4,     7,     0,     1,     0,     1,
       2,     1,     1,     1,     1,     0,     2,     1,     3,     1,
       1,     1,     3,     1,     3,     2,     6,     1,     4,     6,
       3,     2,     0,     3,     0,     3,     1,     3,     1,     3,
       4,     4,     4,     3,     2,     4,     0,     1,     0,     2,
       0,     1,     1,     1,     1,     1,     2,     2,     1,     2,
       3,     2,     3,     2,     2,     3,     2,     1,     1,     3,
       3,     0,     5,     5,     5,     0,     2,     1,     3,     3,
       2,     3,     1,     2,     0,     3,     1,     1,     3,     3,
       4,     4,     5,     3,     4,     5,     6,     2,     1,     2,
       1,     2,     1,     2,     1,     1,     1,     1,     1,     1,
       1,     0,     2,     1,     1,     1,     3,     1,     3,     1,
       1,     1,     1,     1,     3,     3,     3,     3,     3,     3,
       3,     3,     3,     3,     3,     3,     3,     3,     3,     2,
       2,     2,     2,     2,     2,     2,     3,     1,     1,     1,
       1,     1,     1,     5,     6,     6,     6,     5,     5,     5,
       6,     5,     5,     6,     5,     5,     5,     5,     6,     5,
       5,     5,     4,     4,     5,     5,     5,     5,     5,     4,
       4,     4,     4,     4,     4,     3,     6,     6,     6,     8,
       8,     8,     8,     9,     4,     7,     5,     4,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     8,     8,     0,     2,     3,     4,     4,
       4,     4,     4,     4,     0,     3,     4,     7,     3,     1,
       1,     2,     3,     3,     1,     2,     2,     1,     2,     1,
       2,     2,     1,     2,     0,     1,     0,     2,     1,     2,
       4,     0,     2,     1,     3,     5,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     2,     2,     0,     3,     1,
       3,     1,     1,     0,     2,     1,     1,     0,     3,     1,
       3,     2,     2,     0,     1,     1,     0,     2,     4,     4,
       0,     2,     4,     2,     1,     3,     5,     4,     6,     1,
       3,     3,     5,     0,     5,     1,     3,     1,     2,     3,
       1,     1,     3,     3,     3,     3,     3,     1,     2,     1,
       1,     1,     1,     1,     1,     0,     2,     0,     3,     0,
       1,     0,     3,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     0,     1,     1,     1,     1,     1,
       0,     1,     0,     2,     1,     1,     1,     1,     1,     1,
       1,     1,     0,     2,     0,     1,     0,     4,     0,     1,
       0,     3,     0,     3,     0,     4,     0,     3,     0,     3,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     0,     0,     1,     1
};

/* YYDEFACT[STATE-NAME] -- Default rule to reduce with in state
   STATE-NUM when YYTABLE doesn't specify something else to do.  Zero
   means the default is an error.  */
static const yytype_uint16 yydefact[] =
{
      29,   500,   500,    42,   500,   500,     0,   482,   500,     0,
       0,     0,   475,  1271,   131,   849,     0,     0,     0,  1273,
     459,   481,   492,  1273,  1273,     0,     0,   476,     0,   479,
     480,    43,     0,     3,    28,     5,     6,   797,     0,     7,
     500,     8,     9,    10,    11,    26,    27,     0,    12,   364,
     365,   366,    13,    14,    15,    16,    17,    18,    19,   477,
      20,    21,   491,    22,   489,    23,    24,    25,     0,   507,
     502,     0,   849,     0,   179,     0,  1273,  1273,   884,  1205,
    1142,  1076,  1106,  1068,  1114,  1112,  1148,  1203,  1109,  1226,
    1194,  1127,   878,  1058,  1037,  1086,  1128,  1048,  1099,  1232,
    1162,  1199,  1089,  1107,  1052,  1034,  1046,  1062,  1088,  1138,
    1171,  1179,  1069,  1173,  1146,  1221,  1152,  1172,  1110,  1210,
    1164,  1219,  1220,  1090,  1233,  1234,  1204,  1229,  1238,  1191,
    1077,  1038,  1159,  1072,  1103,  1066,  1190,  1053,  1206,  1126,
    1132,  1063,  1065,  1064,  1047,  1185,  1208,  1057,  1067,  1195,
    1200,  1039,  1202,  1218,  1059,  1183,  1041,  1216,  1198,  1124,
    1100,  1101,  1040,  1169,  1075,  1085,  1071,  1141,  1084,  1161,
    1213,  1214,  1070,  1241,  1049,  1228,  1043,  1050,  1227,  1135,
    1134,  1230,  1209,  1217,  1125,  1118,  1042,  1215,  1123,  1117,
    1105,  1080,  1092,  1157,  1113,  1158,  1093,  1130,  1129,  1131,
    1196,  1224,  1242,  1115,  1055,  1184,  1207,  1236,  1237,  1056,
    1083,  1079,  1156,  1133,  1051,  1095,  1193,  1104,  1111,  1168,
    1240,  1145,  1174,  1060,  1223,  1192,  1033,  1044,  1108,  1239,
    1166,  1082,  1225,  1074,  1035,  1036,  1045,  1054,  1061,  1073,
    1078,  1081,  1087,  1091,  1094,  1096,  1097,  1098,  1102,  1116,
    1119,  1120,  1121,  1122,  1136,  1137,  1139,  1140,  1143,  1144,
    1147,  1149,  1150,  1151,  1153,  1154,  1155,  1160,  1163,  1165,
    1167,  1170,  1175,  1176,  1177,  1178,  1180,  1181,  1182,  1186,
    1187,  1188,  1189,  1197,  1201,  1211,  1212,  1222,  1231,  1235,
     474,   879,   867,     0,   847,   847,     0,   869,   868,     0,
     847,     0,   871,     0,     0,   850,     0,     0,   845,   845,
       0,   845,   845,   845,   845,     0,     0,   499,   324,  1274,
       0,     0,  1274,   322,   323,     0,     0,   454,     0,   447,
    1274,  1274,     0,     0,   460,   436,   438,  1274,   473,   472,
       0,     0,     0,   495,   496,     0,   421,   588,     0,   159,
     478,     1,     4,     2,     0,   806,   504,     0,   849,     0,
       0,    75,   493,   494,     0,     0,     0,    36,   508,   509,
     510,   501,     0,   604,     0,     0,     0,   874,   777,   776,
     779,   780,   781,   782,   778,   783,   606,   607,     0,     0,
     764,     0,     0,   519,     0,     0,     0,     0,     0,     0,
       0,     0,     0,   735,     0,   735,   735,   735,   735,   735,
     735,   735,   735,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,   528,   640,   597,   643,   602,   667,   672,   671,   668,
     669,   670,   641,   639,   830,   642,   773,     0,   875,     0,
       0,     0,     0,     0,    53,    55,   588,     0,   178,  1218,
    1050,   839,   837,    73,    72,     0,     0,    59,   831,     0,
     773,     0,   497,   498,   885,   882,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,  1274,     0,     0,     0,
       0,     0,     0,     0,     0,   874,     0,   875,     0,     0,
       0,     0,   410,     0,   422,  1017,   997,   955,  1020,   927,
     944,  1031,   945,   948,   986,   913,   967,   942,   914,   931,
     904,   938,   905,   928,   957,   926,   999,   970,  1019,   982,
     963,  1018,  1026,   979,   943,  1021,   978,   920,   988,   954,
     995,   965,  1006,   959,  1022,   984,   981,  1016,   940,   983,
     985,   902,   980,   936,   935,   934,  1012,  1030,   915,   908,
     951,   992,   966,   958,   976,   932,   916,   909,   956,   919,
     933,   993,   900,   996,  1011,   953,  1015,   950,   949,   941,
     901,  1000,   929,   939,   937,   973,   952,   987,   930,   990,
     977,  1005,   906,   925,   923,   924,   921,   922,   968,   969,
    1023,  1024,  1025,   994,   917,  1007,  1008,   998,  1013,  1014,
     989,  1032,   946,   947,   907,   910,   911,   912,   918,   960,
     961,   972,   975,  1003,  1002,  1001,  1004,  1009,  1028,  1027,
    1029,   971,   903,   962,   964,   974,   991,  1010,   876,  1276,
     877,  1275,   455,  1274,  1274,   847,     0,     0,  1274,     0,
    1274,   847,   442,   466,   466,  1274,     0,     0,   449,   434,
     435,   469,   452,   451,   425,   341,   342,     0,   463,   457,
     458,   461,   462,   463,     0,  1274,  1274,   420,     0,     0,
     161,     0,     0,     0,   810,   505,   506,   500,    38,   797,
       0,     0,     0,   203,     0,    76,   484,     0,   485,   487,
     488,   486,   483,  1272,    37,   511,   512,     0,   513,   503,
     617,     0,   603,     0,     0,     0,     0,   665,     0,   600,
     765,     0,   662,   663,     0,   664,   661,   660,   659,     0,
       0,     0,     0,   718,   727,     0,   515,   724,     0,   726,
     725,   732,   722,   730,   723,   731,   721,   720,   729,   719,
     728,     0,     0,     0,     0,     0,   510,     0,     0,   510,
       0,     0,     0,   510,     0,     0,   510,   510,     0,     0,
       0,     0,   510,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
     531,   534,     0,     0,     0,  1243,  1244,  1245,  1246,  1247,
    1251,  1252,  1256,  1257,  1265,  1264,  1263,  1266,  1267,  1269,
    1268,  1270,  1248,  1249,  1250,  1253,  1254,  1255,  1258,  1259,
    1262,  1260,  1261,   520,   529,   532,   533,     0,     0,   624,
     625,   626,     0,     0,     0,   630,   629,   628,   627,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,   510,     0,    36,   637,     0,
       0,     0,   537,   539,   541,   540,   591,   570,     0,     0,
      57,    49,    50,     0,     0,     0,     0,     0,   180,     0,
       0,     0,    62,    63,   838,  1218,    60,     0,     0,   842,
     841,   298,   840,     0,     0,     0,     0,     0,  1274,   201,
      74,   132,  1274,     0,     0,   872,   389,     0,     0,     0,
       0,     0,     0,     0,   846,   417,   413,     0,   415,    51,
     419,   418,   416,     0,     0,   423,   428,     0,   431,   433,
     429,   432,   430,     0,     0,     0,   439,   440,   424,   448,
     631,     0,   450,     0,   343,     0,     0,   466,   552,     0,
     441,   446,   880,   589,   881,   162,   166,   167,   168,   169,
     170,   171,   172,   173,   174,   175,   176,   177,   160,   163,
       0,  1243,  1244,  1245,  1246,  1247,  1251,  1252,  1256,  1257,
    1265,  1264,  1263,  1266,  1267,  1269,  1268,  1270,  1248,  1249,
    1250,  1253,  1254,  1255,  1258,  1259,  1262,  1260,  1261,   803,
     798,   799,   803,   807,     0,     0,    31,   507,   806,    36,
       0,    57,   587,     0,   353,     0,   279,   239,   222,   225,
     226,   227,   228,   229,   230,   281,   281,   281,   283,   283,
     283,   283,   279,   279,   279,   243,   279,   279,   223,   279,
     279,   279,   279,     0,   295,   295,   295,   295,   295,   262,
     263,   264,   265,   266,     0,   269,   270,   271,   272,   273,
     274,   275,   276,   224,   209,   286,   279,   221,   218,   217,
     219,   490,   784,     0,     0,     0,   514,     0,     0,     0,
       0,   517,     0,     0,     0,   771,   768,     0,   666,   705,
       0,     0,     0,   736,     0,   516,     0,     0,     0,   776,
       0,   641,   776,     0,   641,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,    34,   530,   599,
     598,   618,   620,   622,     0,   601,     0,     0,     0,     0,
       0,   635,   609,   634,   633,   613,   631,   645,   644,   655,
     654,   647,   648,   649,   650,   652,   653,   651,   646,   749,
     750,   658,   608,   656,   657,   515,   521,   774,     0,   877,
     636,     0,   829,    36,     0,     0,     0,     0,     0,     0,
       0,   578,   575,     0,     0,     0,     0,     0,     0,     0,
     574,     0,  1152,   545,   591,   547,   572,   573,   571,     0,
       0,     0,     0,     0,   595,    54,   595,    56,     0,   595,
     590,   181,     0,    66,    67,     0,    61,   833,   834,   835,
     832,     0,   836,   774,     0,   883,     0,    57,     0,   848,
      81,     0,    80,    84,   134,   133,     0,     0,     0,     0,
       0,   193,   393,   391,   392,   396,   857,   856,   861,   855,
     853,   398,   864,   362,   398,   398,   859,     0,   854,   315,
     858,   860,   382,  1273,  1274,     0,     0,   411,  1274,  1274,
     468,   467,     0,   456,   470,   453,     0,     0,   595,   464,
     465,   445,     0,   466,   463,     0,     0,   804,   805,   801,
       0,   802,     0,     0,   811,     0,   510,   810,    39,   586,
       0,   963,  1018,   953,  1162,  1199,  1062,  1088,   204,   205,
       0,   329,   206,   335,   360,   361,   202,   354,   356,   359,
       0,     0,   254,     0,   231,   281,   232,   234,     0,   235,
     236,   237,   238,   240,   241,   242,   295,   295,   279,   295,
     255,   248,   251,   279,   279,   279,     0,   298,   298,   298,
     298,   295,   298,     0,   208,   287,   288,   220,   785,   786,
     844,   843,     0,   535,   605,   717,     0,   704,   703,     0,
       0,   769,     0,   740,   738,   714,   737,   739,   741,     0,
       0,     0,     0,   742,     0,     0,   743,     0,   797,     0,
       0,     0,   526,   526,   526,     0,   526,   526,     0,     0,
     526,   526,   526,   526,     0,   526,   526,   526,     0,   692,
     693,     0,     0,     0,     0,     0,   699,   702,   700,   701,
       0,   619,   621,   623,     0,   610,   614,   631,     0,   611,
       0,   515,     0,   638,   636,   595,   538,   583,   584,   577,
     576,     0,   581,     0,   579,   566,   568,     0,   563,     0,
     591,     0,     0,     0,     0,   551,   542,   544,   595,     0,
       0,   797,    47,     0,    48,     0,     0,    71,    65,    64,
     299,   300,     0,     0,   295,    74,    74,     0,     0,     0,
       0,     0,    85,    86,   135,   136,   137,     0,   873,     0,
       0,     0,     0,     0,   196,   326,   325,   403,   399,   400,
     401,   402,   404,     0,     0,  1273,   866,   865,     0,     0,
       0,   401,   402,   329,   383,     0,     0,  1273,     0,     0,
       0,   316,     0,   367,   414,    52,     0,   427,   426,   632,
     344,   345,   437,     0,   444,   466,   164,   165,   800,   809,
     808,     0,    32,     0,     0,   814,   823,     0,   330,   329,
     320,   317,   329,     0,     0,     0,   321,     0,     0,   357,
     277,     0,     0,     0,   233,     0,   298,   298,   295,   298,
     246,   252,   279,   247,     0,   256,   257,   258,   259,   298,
     260,     0,     0,   302,   304,     0,     0,   290,     0,     0,
     294,   210,   211,   212,   213,   214,   215,   289,   216,    33,
       0,   595,   518,     0,   772,   716,     0,   279,   754,   283,
     279,   279,   279,   279,   757,   759,   762,     0,     0,     0,
       0,     0,     0,   766,     0,     0,   526,   527,   677,   678,
     679,   526,   681,   682,   526,   526,   684,   685,   686,   687,
     526,   689,   690,   691,   523,   797,   694,   695,   696,   697,
     698,     0,     0,   612,   615,   673,     0,   522,   775,   797,
     582,   580,     0,     0,   567,   560,     0,   561,   562,   570,
     550,     0,     0,     0,     0,   558,    46,     0,   596,   806,
      69,    70,    68,   585,   894,     0,    78,    88,     0,     0,
       0,    92,     0,     0,     0,   395,     0,     0,   195,   194,
       0,   390,   377,     0,   378,   371,   372,     0,     0,   374,
     363,   872,   327,     0,     0,   386,   370,   386,     0,     0,
     385,   412,   553,   443,   812,   823,   813,   825,   827,     0,
       0,    40,    36,     0,   819,   319,   318,     0,   334,     0,
       0,   331,   279,   355,   358,     0,   295,   280,     0,     0,
     284,   244,   249,   298,   245,   253,   296,   297,   261,   295,
     292,   303,     0,   305,   291,   301,   536,   787,   770,   708,
     751,   756,   761,   755,   295,   758,   760,   763,   706,   707,
       0,     0,     0,     0,     0,     0,     0,     0,   676,   680,
     675,   683,   688,     0,     0,     0,   616,   674,   806,     0,
     564,   569,     0,   548,     0,   556,     0,     0,     0,   591,
      58,    45,   890,   896,    74,    89,    90,    91,     0,     0,
       0,    99,     0,     0,     0,     0,     0,    74,    93,    94,
      87,     0,     0,     0,     0,   197,   198,     0,   384,   863,
     862,     0,     0,     0,   328,   329,     0,   209,   387,     0,
    1273,  1273,   386,     0,    41,     0,   828,     0,   815,     0,
       0,     0,     0,     0,   307,   803,   278,   298,     0,     0,
     250,   298,   293,     0,   793,   753,   752,     0,     0,     0,
       0,   767,   715,     0,     0,     0,   524,   744,    44,     0,
     591,     0,   594,   592,   593,   559,   546,     0,   886,   898,
     851,    79,    96,    98,   101,   100,     0,     0,     0,    97,
       0,    95,     0,     0,     0,   397,     0,     0,   373,   379,
       0,   872,  1273,   207,   388,   369,   380,  1273,     0,   826,
       0,   821,     0,   817,     0,   820,     0,   332,     0,     0,
     315,   306,   310,   333,   268,   282,   285,   267,     0,     0,
      35,   711,   709,   712,   710,   733,   734,   797,     0,     0,
       0,   565,   549,   557,     0,   889,   892,     0,     0,   890,
       0,   554,   102,   103,   104,   105,     0,     0,     0,   106,
     107,   108,   109,   110,   111,    74,   127,   115,   116,   125,
     126,   112,   113,   114,   117,   118,   119,   120,   121,   122,
     123,   124,     0,   308,   193,   199,   200,     0,     0,   368,
     381,     0,     0,   406,     0,     0,   816,    36,     0,   312,
     314,     0,   311,   791,   788,   789,   792,   795,   794,   796,
       0,     0,     0,     0,   713,   891,     0,   895,     0,     0,
     897,     0,     0,    30,     0,     0,   129,     0,    83,   138,
      77,   309,   196,   308,     0,     0,     0,   405,   824,   822,
     818,     0,   313,     0,   525,     0,   745,   748,     0,     0,
     899,   852,     0,     0,     0,   149,     0,     0,     0,     0,
      74,   394,   375,     0,     0,   407,     0,   790,   746,   893,
     887,   555,     0,     0,     0,   150,     0,    36,   185,   186,
     187,   188,   192,   189,   190,   191,     0,   182,   128,   130,
     139,   140,     0,   308,     0,     0,     0,     0,   147,     0,
       0,     0,   143,   144,   145,   142,    74,   141,   376,     0,
     336,     0,     0,     0,     0,     0,     0,     0,     0,   156,
     183,    82,     0,     0,   337,   338,   747,   151,     0,   146,
     152,     0,     0,     0,     0,   157,     0,     0,     0,     0,
       0,   339,     0,   340,   148,     0,     0,   154,     0,   409,
     408,     0,   349,     0,   348,   347,   346,     0,     0,     0,
     351,   352,   350,   158,   153,     0,   155
};

/* YYDEFGOTO[NTERM-NUM].  */
static const yytype_int16 yydefgoto[] =
{
      -1,    32,   353,    33,    34,  2127,    36,    37,    38,   709,
    2128,    40,  2129,  2130,   894,   938,   472,   473,  1234,  2131,
     902,   903,  1498,   485,  1261,  2014,  1511,  1512,  1513,  1857,
    1858,  1859,  2015,  2016,  2077,   301,  1266,  1517,  2110,  2156,
    2132,  2133,  2104,  2105,  2134,  2168,  2169,  2135,   349,   988,
     989,   990,  2020,   478,  2136,  2137,  1524,  1729,  1731,  1865,
    1866,    47,   361,   712,  1745,   713,  1384,  1084,  1085,  1086,
    1087,  1088,  1089,  1090,  1591,  1352,  1354,  1359,  1386,  1628,
    1621,  1622,  1623,  1624,  1377,  1252,  1625,  1626,  1339,  2080,
    2081,  1972,  1552,  1340,   340,  1341,  1875,  1580,  1770,  1771,
    1342,  1343,   687,  1308,  2174,  2175,  2205,  1346,  1347,  1348,
    1545,  2021,    49,  1880,    50,    51,  1534,  1535,  1292,  2042,
    2043,  2022,   522,  2023,  2024,  2025,  2026,   678,   693,   341,
     694,   967,   956,   962,   342,    57,    58,  2027,  2028,  2029,
      62,  2030,   722,   366,    64,  2031,    66,    67,    69,    70,
     371,   357,   370,   728,  1097,  1114,  1115,  1101,  1657,  1658,
     843,   844,  1631,  1230,   882,   883,   884,  1223,  1313,  2073,
    1834,  1704,   885,  1694,  1695,  1697,  1229,  1224,  1213,  1214,
    1215,  1216,  1257,  1031,   886,   475,  1225,  1491,   451,   732,
     452,   453,  1165,   872,  1303,  1172,   454,   879,   455,   456,
     457,   458,   459,   460,   753,   754,   461,  1990,  1191,  1647,
     741,  1815,  1105,  1106,  1402,   462,   463,  1095,  1904,  2054,
    2055,  1980,  2058,   355,  1020,  1021,  1319,   704,  1026,  1576,
    1763,  1761,  1756,  1757,   464,   465,   487,   488,   489,   911,
    1392,   513,   498,   307,  2001,  1293,  1871,  1540,   302,   303,
    1268,   466,   659,   738,   973,   915,   495,  1996,  1997,  1928,
    2067,  1843,  1930,  1999,   660,   468,  1227,   469,  1113,   317,
     662
};

/* YYPACT[STATE-NUM] -- Index in YYTABLE of the portion describing
   STATE-NUM.  */
#define YYPACT_NINF -1882
static const yytype_int16 yypact[] =
{
    2251, -1882, -1882, -1882, -1882, -1882,    25, -1882, -1882,  -124,
      14,   299, 20099, -1882,   466,   -23,    93,   227,   347, -1882,
    1259, -1882, -1882, -1882, -1882, 18024,   166, -1882,   252, -1882,
   -1882, -1882,   537,   134, -1882,   577, -1882,   613,   587, -1882,
   -1882, -1882, -1882, -1882, -1882, -1882, -1882,    43, -1882, -1882,
   -1882, -1882, -1882, -1882, -1882, -1882, -1882, -1882, -1882, -1882,
   -1882, -1882,  4293, -1882,   217, -1882, -1882, -1882,   105,   330,
   -1882, 10554,   502, 18439,   608, 20514, -1882, -1882,   394, -1882,
   -1882, -1882, -1882, -1882, -1882, -1882, -1882, -1882, -1882, -1882,
   -1882, -1882, -1882, -1882, -1882, -1882, -1882, -1882, -1882, -1882,
   -1882, -1882, -1882, -1882, -1882, -1882, -1882, -1882, -1882, -1882,
   -1882, -1882, -1882, -1882, -1882, -1882, -1882, -1882, -1882, -1882,
   -1882, -1882, -1882, -1882, -1882, -1882, -1882, -1882, -1882, -1882,
   -1882, -1882, -1882, -1882, -1882, -1882, -1882, -1882, -1882, -1882,
   -1882, -1882, -1882, -1882, -1882, -1882, -1882, -1882, -1882, -1882,
   -1882, -1882, -1882, -1882, -1882, -1882, -1882, -1882, -1882, -1882,
   -1882, -1882, -1882, -1882, -1882, -1882, -1882, -1882, -1882, -1882,
   -1882, -1882, -1882, -1882, -1882, -1882, -1882, -1882, -1882, -1882,
   -1882, -1882, -1882, -1882, -1882, -1882, -1882, -1882, -1882, -1882,
   -1882, -1882, -1882, -1882, -1882, -1882, -1882, -1882, -1882, -1882,
   -1882, -1882, -1882, -1882, -1882, -1882, -1882, -1882, -1882, -1882,
   -1882, -1882, -1882, -1882, -1882, -1882, -1882, -1882, -1882, -1882,
   -1882, -1882, -1882, -1882, -1882, -1882, -1882, -1882, -1882, -1882,
   -1882, -1882, -1882, -1882, -1882, -1882, -1882, -1882, -1882, -1882,
   -1882, -1882, -1882, -1882, -1882, -1882, -1882, -1882, -1882, -1882,
   -1882, -1882, -1882, -1882, -1882, -1882, -1882, -1882, -1882, -1882,
   -1882, -1882, -1882, -1882, -1882, -1882, -1882, -1882, -1882, -1882,
   -1882, -1882, -1882, -1882, -1882, -1882, -1882, -1882, -1882, -1882,
   -1882, -1882, -1882, -1882, -1882, -1882, -1882, -1882, -1882, -1882,
   -1882, -1882, -1882,   378,   546,   546, 20099, -1882, -1882,   605,
     546,   457, -1882,   555, 20099, -1882,   528,   570,   584,   584,
   20929,   584,   584,   584,   584, 20099, 20099, -1882, -1882,  5549,
     677,   173,  5549, -1882, -1882,   576,   586, -1882,   735,   123,
    5549,  5549,   606,   -57, -1882, -1882, -1882,  5549, -1882, -1882,
      96,   298,   417, -1882, -1882, 20099, -1882,   643,   408,   742,
   -1882, -1882, -1882, -1882,   785,   806,   717,   111,   502,  7221,
   20099, -1882, -1882, -1882,   105,   707,    79,   756, -1882, -1882,
     574,   753,   766,   766,   766,   766,   766,   711, -1882, -1882,
   -1882, -1882, -1882, -1882, -1882, -1882, -1882, -1882, 14289, 12629,
   12629, 14289, 14289, -1882,   766, 14289, 14289, 14289, 14289, 14289,
     766,   766,   766,   766,   766,   766,   766,   766,   766,   766,
     766,   766,   766,   766,   766,   766,   766,   766,   766,   766,
     766,   766,   766,   766,   766,   766,   766,   766,   766,   766,
     766,   766,   766,   766,   766,   766,   766,   766,   766,   766,
     766,   766,   766,   766,   766,   766,   766,   766,   766,   766,
     828, 14704, -1882, -1882, -1882,  3098, -1882, -1882, -1882, -1882,
   -1882, -1882,   494, -1882, -1882, -1882,   766,   723,   725,  8892,
   18854, 20099,   288,   523, -1882, -1882,   731, 10969, -1882,   254,
     829,   440,   459,   476,   477, 21344,   767,   804, -1882, 17609,
   -1882,   743, -1882, -1882, -1882,   456,   746,   803,   852, 20099,
     873,   857,   859,   896,   898, 20929,  5549, 20099,   719,   -45,
     178, 20099,   946,   902, 20099, -1882,   908, -1882, 20099,   906,
     961,   965,   971,   904, -1882, -1882, -1882, -1882, -1882, -1882,
   -1882, -1882, -1882, -1882, -1882, -1882, -1882, -1882, -1882, -1882,
   -1882, -1882, -1882, -1882, -1882, -1882, -1882, -1882, -1882, -1882,
   -1882, -1882, -1882, -1882, -1882, -1882, -1882, -1882, -1882, -1882,
   -1882, -1882, -1882, -1882, -1882, -1882, -1882, -1882, -1882, -1882,
   -1882, -1882, -1882, -1882, -1882, -1882, -1882, -1882, -1882, -1882,
   -1882, -1882, -1882, -1882, -1882, -1882, -1882, -1882, -1882, -1882,
   -1882, -1882, -1882, -1882, -1882, -1882, -1882, -1882, -1882, -1882,
   -1882, -1882, -1882, -1882, -1882, -1882, -1882, -1882, -1882, -1882,
   -1882, -1882, -1882, -1882, -1882, -1882, -1882, -1882, -1882, -1882,
   -1882, -1882, -1882, -1882, -1882, -1882, -1882, -1882, -1882, -1882,
   -1882, -1882, -1882, -1882, -1882, -1882, -1882, -1882, -1882, -1882,
   -1882, -1882, -1882, -1882, -1882, -1882, -1882, -1882, -1882, -1882,
   -1882, -1882, -1882, -1882, -1882, -1882, -1882, -1882, -1882, -1882,
   -1882, -1882, -1882,  5549,  5549,   546, 20099, 20099,  5549, 20099,
    5549,   546, -1882,   128,   128,  5549, 12629, 14289, -1882, -1882,
   -1882,   944,   986, -1882, -1882, -1882, -1882, 20099,   140, -1882,
   -1882, -1882, -1882,   140,  1058,  5549,  5549, -1882,  7639,  1021,
   -1882,  1929, 13044, 12629,   443, -1882, -1882, -1882, -1882,   613,
     105, 19269,   692, -1882,  2573, -1882,   577,  1006,   577, -1882,
   -1882, -1882, -1882, -1882, -1882, -1882, -1882,   627,  1036, -1882,
   -1882,   105, -1882,  1026, 21759, 10554, 10554,   990,   996,  1014,
     558,  1034,   990,   990, 10554,   990,   990,   990,   990, 17194,
     756, 10554, 10554, -1882, -1882, 13459, 10554, -1882,   756, -1882,
   -1882, -1882, -1882, -1882, -1882, -1882, -1882, -1882, -1882, -1882,
   -1882, 10554, 12629, 12629, 11384, 11799,   786, 20929, 20929,   786,
   10554, 10554, 10554,   786, 10554, 10554,   786,   786, 10554, 10554,
   10554, 10554,   786, 10554, 10554, 10554,   756,   756, 10554, 10554,
   10554, 10554, 10554,   756,   756,   756,   756, 10554, 20099, 16364,
   -1882, -1882, 12629, 12629,   634, -1882, -1882, -1882, -1882, -1882,
   -1882, -1882, -1882, -1882, -1882, -1882, -1882, -1882, -1882, -1882,
   -1882, -1882, -1882, -1882, -1882, -1882, -1882, -1882, -1882, -1882,
   -1882, -1882, -1882, -1882, -1882, -1882, -1882,   675, 14289, -1882,
   -1882, -1882,   482, 14289, 14289, -1882, -1882, -1882, -1882, 14289,
   14289, 14289, 14289, 14289, 14289, 14289, 14289, 14289, 14289, 14289,
   14289,   492, 14289,   868,   868,   786,  5967,   756,   558,   797,
    8892,   458,  1131, -1882, -1882, -1882, 15119,   124,  9308,    62,
      28, -1882, -1882, 20099, 18854, 20099, 18854,  6385, -1882,   817,
     839,   607,  1051, -1882, -1882,   254,   804, 10139, 22174, -1882,
   -1882,  1015, -1882,  8057,  1066,  1119, 20099,  1134,  5549, -1882,
   -1882, -1882,  5549,  1093,   430,  1114, -1882,   335, 20099, 20099,
   20099, 20099, 20099,   548, -1882, -1882,  1129, 20099,  1135, -1882,
   -1882, -1882, -1882, 20099, 20099, -1882, -1882,  1143, -1882, -1882,
   -1882, -1882, -1882,  1148, 12629,  1137, -1882, -1882, -1882,   558,
    1294,  1147, -1882, 20099,   191, 20099, 20099,   128,  1182, 20099,
   -1882, -1882, -1882, -1882, -1882, -1882, -1882, -1882, -1882, -1882,
   -1882, -1882, -1882, -1882, -1882, -1882, -1882, -1882,  1152, -1882,
    1125,   766,   766,   766,   766,   766,   766,   766,   766,   766,
     766,   766,   766,   766,   766,   766,   766,   766,   766,   766,
     766,   766,   766,   766,   766,   766,   766,   766,   766,   258,
    1154, -1882,   878,   566,  1216,  1127, -1882,   330,   806,   756,
   20099,  1056, -1882,  8475,  5131,  1161,  1162, -1882, -1882, -1882,
   -1882, -1882, -1882, -1882, -1882,  1169,    31,  1169,  1170,  1170,
    1170,  1170,  1162,  1162,  1162, -1882,  1162,  1162, -1882,    16,
    1162,  1162,  1162,   158,   998,   998,   998,   998,   371, -1882,
   -1882, -1882, -1882, -1882,  1174, -1882, -1882, -1882, -1882, -1882,
   -1882, -1882, -1882, -1882, -1882,   972,  1162, -1882, -1882, -1882,
   -1882, -1882, -1882,  1195,  1197,   344, -1882, 10554,   756,   756,
     849, -1882,   849,  8057, 12629,   590, -1882,   849, -1882, -1882,
     849,   849,  1859, -1882,   756,  1178,   849,   621,   224,  1236,
     849,   122,  1239,   849,   160, 10554,  1183,  1184, 10554,   849,
     849,   849, 10554,   849,   849, 10554, 10554,   849,   849,   849,
     849, 10554,   849,   849,   849,   939,   939,   756,   849,   756,
     849,   849,   939,   939,   939,   939,   849, -1882, -1882,    40,
    1014, -1882, -1882, -1882,   730, -1882, 14289,   482, 14289, 14289,
    1799, -1882, -1882, -1882, -1882,  1928,  1294,   912,  1961,   867,
     867,   967,   967,   432,   432,   432,   432,   432,   990, -1882,
   -1882, -1882,  1928, -1882, -1882, 10554, -1882,   766,  1138,  1140,
   -1882, 12629, -1882,   756, 22174, 18854,   672,  1196,  1201,   260,
     423, -1882, -1882, 18854, 18854, 18854, 18854, 15949,  1121,  1128,
   -1882,  1130,   766, -1882,   186, -1882, -1882, -1882, -1882, 16779,
     856,  9308, 18854,   766,  1247, -1882,    92, -1882,  1141,    92,
   -1882, -1882,   407, -1882, -1882,   254,  1051, -1882, -1882,   558,
   -1882,   684, -1882, -1882,  1153, -1882,  1144,  1056,  1251, -1882,
   -1882,   105, -1882,    80, -1882, -1882,   770, 20929,  1212,  1101,
     -34,  1219, -1882, -1882, -1882, -1882, -1882, -1882, -1882, -1882,
   -1882,   484,   338,  1084,  1146,  1146, -1882,  1115, -1882,  1189,
   -1882, -1882, -1882, -1882,  5549, 20099,  1151, -1882,  5549,  5549,
     558, -1882, 14289, -1882, -1882, -1882,  1226,  1227,  1247, -1882,
   -1882, -1882,  1264,   128,   140,  1929,   868, -1882, -1882, -1882,
   13044, -1882, 12629, 12629, -1882,  1254,   786,   443, -1882, -1882,
     446,   575,   320,   591,  1258,   104,  1240,  1262, -1882, -1882,
    1244,  1242, -1882, -1882, -1882, -1882,  1246,  4713, -1882, -1882,
    1243,  1248, -1882,  1249, -1882,  1169, -1882, -1882,  1250, -1882,
   -1882, -1882, -1882, -1882, -1882, -1882,   998,   998,  1162,   998,
   -1882, -1882, -1882,  1162,  1162,    24,  1287,  1015,  1015,  1015,
    1015,   998,  1015,  1243,   495, -1882,  1047, -1882, -1882, -1882,
   -1882, -1882, 20099,   317, -1882, -1882, 10554, -1882, -1882,   697,
   12629, -1882,  1235, -1882, -1882, -1882, -1882, -1882, -1882,   492,
    1019,  1019, 14289, -1882, 14289, 14289, -1882, 14289,   107, 14289,
   14289,   849,   939,   939,   939,   849,   939,   939,   849,   849,
     939,   939,   939,   939,   849,   939,   939,   939, 19684, -1882,
   -1882,   939,   939,   939,   939,   939, -1882, -1882, -1882, -1882,
     975, -1882, -1882, -1882,  1946, -1882,  1928,  1294, 14289, -1882,
     756, 10554,  6803,   558,  1320,   321,  1131, -1882, -1882, -1882,
   -1882,  1266, -1882,  1268, -1882,   881,  1269,  1329, -1882, 14289,
     186,   766,   766,   766, 20929, -1882, -1882, -1882,    92, 20929,
   12629,   613, -1882,  1215, -1882,   654,  1042, -1882, -1882, -1882,
   -1882, -1882,  7221, 20099,   998, -1882,   577,  2573,  1267,  1270,
    1271,  1273,  1275, -1882, -1882, -1882, -1882,  1272, -1882, 20099,
   20929, 20929, 20929,  1276,   991, -1882, -1882, -1882, -1882, -1882,
    1279,  1306, -1882, 20929,  1281, -1882, -1882, -1882,  1282, 20929,
   20099,  1283,  1262,  1242, -1882,   149,   596, -1882,  1290,  1291,
   20929, -1882, 12629, -1882, -1882, -1882, 20099, -1882, -1882,  1928,
   -1882, -1882, -1882, 14289, -1882,   128, -1882, -1882, -1882,   558,
     558,  1305, -1882, 22174,   766,   577,  1300,  9724, -1882,  1242,
   -1882, -1882,  1242,  1217,  1303, 20929, -1882,  5131,  5131, -1882,
   -1882,   918,  1302,  1307, -1882,   968,  1015,  1015,   998,  1015,
   -1882, -1882,  1162, -1882,   329, -1882, -1882, -1882, -1882,  1015,
   -1882,   974, 14289, -1882,  1333,  1369,  1308, -1882,  1298,  1337,
   -1882, -1882, -1882, -1882, -1882, -1882, -1882, -1882, -1882, -1882,
   18854,  1247, -1882, 12629,   558, -1882,   756,  1162, -1882,  1170,
    1162,  1162,  1162,  1162, -1882,  1158,  1160,   756,   756,   950,
    1509,  1675,  1702,  1070,  1730,  1753,   939, -1882, -1882, -1882,
   -1882,   939, -1882, -1882,   939,   939, -1882, -1882, -1882, -1882,
     939, -1882, -1882, -1882, -1882,    77, -1882, -1882, -1882, -1882,
   -1882,   766, 14289, -1882,  1928, -1882,   756, -1882, -1882,   613,
   -1882, -1882,  1330, 12629, -1882, -1882, 12629, -1882, -1882,   101,
   -1882, 20929, 20929, 20929,  1004, -1882, -1882,  1004,   558,   806,
   -1882, -1882, -1882, -1882,   792,   105, -1882, -1882,  2573,  2573,
    2573,   736,    80, 20099,  1358, -1882,  1359,  1331, -1882, -1882,
    7221, -1882, -1882,  1357, -1882, -1882, -1882,    78,  1285, -1882,
    1217,  1114,   104,  1290,  2573,   927, -1882,   927,  1290,  1402,
     558, -1882,  1928, -1882, -1882,   676,  1362, -1882, -1882, 12214,
    1403, -1882,   756,  1022,  1309, -1882, -1882,  1262, -1882, 20929,
    1033, -1882,  1162,  4713, -1882,  1360,   998, -1882,  1363,  1365,
   -1882, -1882, -1882,  1015, -1882, -1882, -1882, -1882, -1882,   998,
    1928, -1882,  1255, -1882, -1882, -1882,  1375,  1420,   558, -1882,
   -1882, -1882, -1882, -1882,    -4, -1882, -1882, -1882, -1882, -1882,
   14289, 14289, 14289, 14289,  1372,   756, 14289, 14289, -1882, -1882,
   -1882, -1882, -1882,  1429,   756, 14289,  1928, -1882,   806, 20929,
     558,   558, 16779, -1882,  1045, -1882,  1045,  1045, 20929, 15534,
   -1882, -1882,  1412,  1416,   577, -1882, -1882, -1882,  1383,  1263,
    1260, -1882,  1265,  1277,  1278,  1257,  1280, -1882,   736, -1882,
   -1882,  1441, 20929, 20929, 20099,  1394, -1882,  1368, -1882, -1882,
   -1882,  1404, 20929,  1406, -1882,  1242,  1410, -1882, -1882,  1415,
   -1882, -1882,   927,   766, -1882,   766, -1882,  1436,  1320, 20929,
     290, 20929,  1057, 20929,   242,   878, -1882,  1015,  1417,  1418,
   -1882,  1015, -1882,  1472,  1476, -1882, -1882,  1859,  1859,  1859,
    1859, -1882, -1882,  1859,  1859, 12629, -1882,   846, -1882,  1081,
     186, 20929, -1882, -1882, -1882, -1882, -1882,  1475,  1111,  1449,
    1349, -1882, -1882, -1882, -1882, -1882,  1458,  1459,   973, -1882,
    2857, -1882,  1311,  1097,  1099, -1882,  7221,  5131, -1882, -1882,
   20929,  1114, -1882,   495, -1882, -1882, -1882, -1882,  1328, -1882,
    1494,  1378,   766,   577,   105, -1882,  1348, -1882,  1437,  1438,
    1189,   242, -1882, -1882, -1882, -1882, -1882, -1882, 13044, 13044,
   -1882, -1882, -1882, -1882, -1882, -1882, -1882,   479,    -5,  1156,
     756, -1882, -1882, -1882,  1440, -1882,  1469,  1473,  1499,  1412,
    1443,  1450, -1882, -1882, -1882, -1882, 12629, 12629,  2857,   577,
   -1882, -1882, -1882, -1882, -1882, -1882, -1882, -1882, -1882, -1882,
   -1882, -1882, -1882, -1882, -1882, -1882, -1882, -1882, -1882, -1882,
   -1882, -1882,  1332,   242,  1219, -1882, -1882,  1106,  1452, -1882,
   -1882, 20929,  1108, -1882, 22174, 20929,  1362,   756, 20099, -1882,
   -1882,  1447, -1882,   558,  1453, -1882, -1882,   558, -1882, -1882,
     756,  1171,  1474,  1173, -1882, -1882,  1511, -1882,  1515,  1460,
   -1882,  1487, 20929, -1882,   628,   708, -1882,  1126, -1882,   993,
   -1882,   242,   991,   242, 20929,  1498,  1328, -1882,   804, -1882,
    1320,  1485, -1882, 13044, -1882,  1501, -1882, -1882,  1492,  1493,
   -1882, -1882,  1118, 12629,   599, -1882,   251,  2539,  1497,  1500,
   -1882, -1882, -1882,  1133,  1395, -1882, 20929, -1882,  1220, -1882,
   -1882, -1882,   775,  1479,   251, -1882,   251,  1145, -1882, -1882,
   -1882, -1882, -1882, -1882, -1882, -1882,  1157, -1882, -1882, -1882,
   -1882, -1882,   245,   242,  1408,  1139,  1221,   251, -1882,  1159,
    1163,   559, -1882, -1882, -1882, -1882, -1882, -1882, -1882,   766,
    1518,  1229,  1165,   664,   820,  1432, 12629,   251,   822, -1882,
   -1882, -1882, 13874,  1202,  1520,  1524, -1882,   251,  1506, -1882,
   -1882,   788,  1168,  1456,   251, -1882,   756,  1859,    70,    70,
    1585, -1882,  1593, -1882, -1882,   251,   826, -1882,  1180, -1882,
   -1882,   223, -1882,  1454, -1882, -1882, -1882,  1181,  1463,   832,
   -1882, -1882, -1882,   251, -1882,  1464, -1882
};

/* YYPGOTO[NTERM-NUM].  */
static const yytype_int16 yypgoto[] =
{
   -1882, -1882, -1882, -1882, -1882,    13, -1882,  1237, -1882, -1882,
       1, -1882,     2,     3,  1155, -1882,  -156, -1882,  -964,     4,
     706,   367, -1882, -1882, -1464,  1627, -1882, -1882,   -91, -1882,
   -1882,  -224, -1881, -1882, -1882, -1882, -1882, -1882, -1882, -1882,
    -507, -1857, -1882,  -463, -1829, -1882,  -530,     5, -1882, -1882,
     333, -1882,  1645, -1882, -1048, -1055,  -385, -1882,  -424, -1882,
    -287, -1882, -1882, -1882, -1482,   629,  -217, -1443, -1882, -1882,
   -1882, -1882, -1882, -1882,   282,  -857,  -984, -1004, -1882, -1882,
   -1882, -1882, -1882, -1882, -1028, -1305, -1882, -1882, -1882, -1798,
    -228, -1875,  -303, -1882, -1882, -1228, -1882, -1298, -1760,  -225,
     387, -1505, -1882, -1882,  -504,  -502,  -516, -1882,    87, -1319,
   -1882,  1676, -1882, -1658, -1882, -1882,  -349,   392, -1882, -1882,
    -409,  1678, -1882,  1682,  1683,  1685,  1688, -1882, -1882, -1882,
     -24,  -678,  -660, -1882, -1882, -1882, -1882,  1692,  1699,  1700,
   -1882,  1701, -1882, -1882, -1882,  1703, -1882, -1882,    17, -1882,
   -1882, -1882,   678,  -168, -1882, -1135,   862,   -64,  -624,   -69,
   -1882,   893, -1882,  -441,  -606,  -419, -1882,  -133, -1882, -1882,
   -1530,   218, -1882,   231, -1882, -1882,    10, -1193, -1882, -1882,
     504, -1882, -1882, -1882,   240,   818, -1180, -1160,   600, -1882,
   -1882, -1882, -1882, -1882, -1117,   547,  -362,  -466,  -365, -1882,
   -1882, -1882, -1882,   -76,  1010,   695, -1882, -1882,   309,   310,
   -1882, -1882, -1882,   617, -1882,   -25,  -825, -1882, -1882, -1882,
    -369, -1882, -1882,  -701, -1882,   405, -1013, -1012,   399, -1882,
   -1882,   -28,  -227,  -151,  -832, -1882,  -479,   831, -1882, -1882,
   -1882,   533,  -215,   -37, -1882, -1882, -1882, -1882,   192, -1882,
   -1672,  1016,  -307,  2382,  -815, -1882, -1882, -1882, -1882,  -263,
   -1882, -1882, -1882, -1882,  -640,   801,  -439,     0,  2230,    -6,
    -155
};

/* YYTABLE[YYPACT[STATE-NUM]].  What to do in state STATE-NUM.  If
   positive, shift that token.  If negative, reduce the rule which
   number is the opposite.  If zero, do what YYDEFACT says.
   If YYTABLE_NINF, syntax error.  */
#define YYTABLE_NINF -1274
static const yytype_int16 yytable[] =
{
      68,    39,    41,    42,    43,    45,   906,   450,  1028,  1321,
     730,   899,   846,    35,   957,   968,  1327,   343,   344,    71,
    1174,    72,    73,   737,  1480,    75,   742,   743,  1589,   881,
     745,   746,   747,   748,   749,   470,  1486,  1378,  1379,  1380,
    1382,  1715,  1716,  1586,  1485,  1360,  1361,  1362,  1193,  1194,
     486,  2061,   714,  1533,  1539,  1543,   363,   358,   974,  1459,
    1460,  1198,  1356,  1357,  1717,  1905,  1747,  1330,    68,  1873,
     492,   493,  1605,  1606,  1607,  1608,  1492,  1610,  1768,  1494,
     499,   367,  1351,  2017,     1,   502,     3,     4,     5,  1881,
    1351,   354,   -53,   929,    74,   -53,  2052,  1353,  1254,  2201,
    1869,  -591,  1943,  1490,  1579,  -591,   685,  1582,   887,   359,
       1,  2018,  -591,  -591,  -591,  -591,   707,  -591,   304,  -591,
     305,   354,   682,  1228,  -543,   813,  1232,  2076,  -543,   893,
    -591,  -591,  1414,   930,   676,  -543,  -543,  -543,  -543,   954,
    -543,   814,  -543,   360,  1521,    13,  1228,  1525,  1562,  1507,
     965,  2017,    76,  -543,  -543,  1218,  1219,  -591,  -591,  1205,
    -591,  -591,  -591,  -591,  -591,  -591,  -591,   672,  -591,  -591,
    1417,    13,  1836,  1837,  1396,   679,   680,    13,  1508,  2018,
    -543,  -543,   684,  -543,  -543,  -543,  -543,  -543,  -543,  -543,
    2037,  -543,  -543,   292,   686,  1363,  1364,  1365,  1233,  1366,
    1367,  1306,  1369,  1370,  1371,  1372,  2052,   859,   860,   861,
     862,   863,   864,   865,   866,   867,   868,   869,   870,   683,
    1870,   871,  2202,   677,  1957,  2203,  2139,  2204,   955,  1387,
     308,   309,   310,  1522,   311,  1768,  1199,  1376,   966,  1892,
    1218,  1219,   664,  1526,  1221,  1741,  1411,  1823,   873,   874,
    2017,  2210,     3,     4,     5,   306,     1,   974,     3,     4,
       5,  1876,  1368,  1022,  1355,   346,  1882,   312,   313,  1774,
    1602,  1509,  1510,  1199,     8,  1845,  1846,  1847,  2018,  2038,
       8,  1765,  1317,  1318,  1766,  2112,   873,   874,  1254,  1307,
      77,  1781,  1782,  1504,  1784,     1,   297,   298,   891,  1919,
    1700,  1877,   362,  2211,  1788,  1689,  1968,  1311,   812,   813,
     665,   666,   960,   474,   667,   889,   931,    13,  1471,   661,
    1969,   711,   661,  1472,  2113,   814,  1686,  1630,  1706,  1221,
     661,   661,  1490,  1944,    78,  1174,  1962,   661,  1596,  1597,
    1683,  1599,   812,   813,  2006,  2158,  2062,   668,   669,   364,
     670,   926,   892,  1609,  1390,   893,    13,   710,   936,   814,
    1536,   932,  1391,  1525,    68,   315,    68,   719,   720,   721,
     846,  1594,   731,   733,   734,   735,   736,   716,   348,   718,
    1931,  1525,   368,   369,  1396,   365,  -329,    31,   908,  1578,
    1112,  1970,   314,  1940,   744,  2007,  1373,  1374,  1786,  1375,
     750,   751,   752,   755,   756,   758,   758,   755,   755,   755,
     758,   755,   755,   771,   772,   773,   774,   775,   776,   777,
     778,   779,   780,   781,   782,   783,   784,   785,   786,   787,
     788,   789,   790,   791,   792,   793,   794,   795,   796,   797,
     798,   799,   800,   801,   802,   803,   804,   805,   806,   807,
     947,     1,  1787,  1236,  1589,  1239,   953,   699,  2126,  1526,
     486,  1024,    26,  1199,  2126,  1269,   875,   350,  1270,   880,
     888,  1797,   671,  1025,   688,  1573,  1714,  1526,  1900,   700,
    1537,  1473,   877,  1170,  1538,   316,  1474,  1204,  1175,  1176,
    1173,  1567,  1574,   354,  1177,  1178,  1179,  1180,  1181,  1182,
    1183,  1184,  1185,  1186,  1187,  1188,   661,  1192,   945,   946,
     292,  1598,    13,   950,  1874,   952,  1600,  1601,  1603,  1833,
     958,  1439,  1440,  1612,  -591,  1205,   887,  1525,  1446,  1447,
    1448,  1449,   887,   891,   887, -1133,   500,   351,  1613,  1614,
     970,   971,  2102,   900,   506,   901,  1201,  -543,    13,   870,
     293,  2078,   871,    31, -1051,   523,   524,   352,  1171,    31,
    1615,  1189,  1276,  1190,     1, -1133,     3,     4,     5,  1197,
    1783, -1095, -1193,  1616,   689,  1617,  1277,  1951,   690,   691,
     692,   -36,  1618,  1322, -1051,   697,  2145,   892,     8,  1264,
     895,   356,  1974,   507,   725,   726,  1977,   695,   696,  1466,
     715, -1095, -1193,   294,   295,  -870,  1253,   296,  1125,  1477,
    1381,  1128,  1376,   297,   298,  1132,  1265,  1278,  1135,  1136,
     873,   874,   727,  1526,  1141,    13,  1279,   354,  2036,  1527,
    1528,   503,   504,  1323,  1529,  1801,  1565,  1530,  1531,  1920,
    1619,  -326,   812,   813,  -326,   305,  2142,  2165,  2166,  2167,
     812,   813,  2006,  1564,  1532,   299,   508,  -325,   509,   814,
    -325,   510,  1743,   661,   661,  1744,   494,   814,   661,     1,
     661,     3,     4,     5,   477,   661,  1092,  1280,  1281,  1282,
    1400,  1283,  1104,  1284,  1285,  1409,   496,  2123,  1410,  2124,
     497,  2103,  2171,     8,   505,   661,   661,  1841,  1495,  1093,
     501,  1496,  1094,  2007,  1497,   812,   813,  1195,   511,  1099,
      68,   890,   812,   813,  1161,  1162,  1163,  1653,  1286,  1287,
    2103,  1164,   814,  1029,  1027,  1465,   714,  1349,   512,   814,
      13,    68,  1209,  1210,  1147,  1467,  1149,   705,   706,   919,
    1992,  1760,  1758,   908,   877,  1785,   663,   927,  1897,  1121,
    1124,   933,  2178,  1500,   474,  1501,   673,  2006,   939,  1033,
    1034,  1901,  1620,  1260,   675,   300,   674,  1262,   698,  1166,
      26,   701,  2126,  1167,  1168,  1169,  1906,  1514,  1515,  1516,
    1800,   812,   813,  1802,  1803,  1804,  1805,   681,  1633,  1288,
    1709,  1488,   812,   813,  1475,  1476,  1197,  1478,   814,  2106,
     702,  1454,   717,  1456,  1457,  1173,   725,   726,  2007,   814,
    1451,  1452,  1453,   291,  1848,  1289,  1918,  1676,  1677,  1678,
    1679,  1680,   703,  1849,   723,     1,   291,     3,     4,     5,
     729,     1,    13,     3,     4,     5,  -878,     1,   808,     3,
       4,     5,   514,   887,   518,   519,   520,   521,   876,     8,
    -879,   887,   887,   887,   887,     8,   897,  1290,   904,   812,
     813,     8,   907,   291,  1201,   723,  2147,    31,   913,   887,
     887,   908,   812,   813,   291,    26,   814,  2126,   914,  2195,
     880,  1022,  1973,   486,  1201,  1241,    13,   916,  1231,   814,
     917,  1850,    13,  1203,   888,   920,   888,   928,    13,  1243,
    1244,   877,  1317,  1318,  1291,  1753,   948,   949,  2179,   951,
    2183,  2166,  2184,  2006,  2208,  1895,  1396,   723,   661,  2006,
    2215,   918,   661,  1205,   723,  2006,   921,   964,   922,  1851,
    1852,  1853,  1854,  1855,  1546,  1548,  1549,  1559,   378,   379,
     380,   381,   382,   383,  1988,  1692,  1693,   384,   385,  1710,
    1711,  1032,   859,   860,   861,   862,   863,   864,   865,   866,
     867,   868,   869,   870,  2007,   923,   871,   924,  1810,   934,
    2007,   935,    31,   937,  1824,   940,  2007,   863,   864,   865,
     866,   867,   868,   869,   870,  1775,  1776,   871,  1828,   739,
     740,   779,   780,   781,   782,   783,   784,   785,   786,   787,
     788,   789,   790,   791,   792,   793,   794,   795,   796,   797,
     798,   799,   800,   801,   802,   803,   804,   805,   806,   860,
     861,   862,   863,   864,   865,   866,   867,   868,   869,   870,
     941,    26,   871,  2126,   942,  1779,  1780,    26,   943,  2126,
    1349,  1775,  1789,    26,   961,  2126,   944,  1649,  1157,  1650,
    1651,   963,  1652,  1758,  1654,  1655,   859,   860,   861,   862,
     863,   864,   865,   866,   867,   868,   869,   870,   969,   878,
     871,  1838,   723,   691,   692,  1091,  2149,   878,  2150,   865,
     866,   867,   868,   869,   870,  1878,  1879,   871,  1856,  1889,
     723,   490,   975,  1684,  1755,  1098,  2170,   291,  1096,  2162,
    1893,  1894,   761,   763,   765,   291,   768,   770,  2170,  2170,
     871,   517,  1921,   723,  1699,   814,   291,   291,  1245,  2182,
     517,  1103,  2170,   517,  1921,  1966,  1104,  1525,    31,  1242,
    1758,   517,   517,  1235,    31,  1251,  2198,  1255,   517,  1554,
      31,  2170,  1637,  1557,  1558,  1256,   291,  2207,  1921,  1991,
    -398,  -888,  1995,  -398,  2170,  1688,  1258,  1259,  2170,  1263,
     517,   291,  2004,  2005,  1893,  2033,  1921,  2034,  1271,  1272,
    1273,  1274,  1275,  1893,  2083,  2086,   723,  1294,  1267,   486,
    2108,  2109,  1638,  1296,  1297,  1921,  2121,  1206,  1207,  1796,
    1208,  1209,  1210,  1211,  1212,  1688,   893,  1461,  1752,  1989,
    1893,  2143,  1295,  1305,  1312,   888,  1921,  2160,  1301,  1314,
    2188,  2189,  1298,   888,   888,   888,   888,  1299,  1304,  1315,
    1316,  1320,  1484,  1526,  1324,  1325,  1233,  1350,  1351,  1527,
    1528,  1231,   888,  1489,  1529,  1353,  1358,  1541,  1542,  1376,
    1383,  1388,  1385,  1389,  1203,  1396,  1412,  1790,  1639,  1415,
    1419,  1420,   845,  1640,  1532,  1641,  1438,  1642,  1490,  1469,
    1481,    68,  1643,  1462,  1470,  -881,  1493,  1482,   887,  1483,
    1329,   291,   291,  1505,  1506,  1644,   959,  1519,  1502,  1520,
    1349,  1349,  1503,  1523,  1551,  1550,  2060,  1553,  1645,  1646,
     517,   318,  1528,  1556,   661,  1560,  1561,  1563,   661,   661,
     291,  1581,  1019,  1023,  1571,  1584,   517,   517,   291,  1583,
    1585,  1578,   291,  1587,  1590,   291,  1604,  1826,  1627,   291,
    1592,  1593,  1595,  1635,   -37,  1681,   516,  1240,   319,  1690,
    1577,  1691,  1632,  1712,  1696,   658,  1718,  1723,   658,  1719,
    1720,  1721,  1722,  1575,  1730,  1728,   658,   658,  1732,  1733,
    1735,  1737,  1740,   658,  1659,  1660,  1754,  1662,  1663,  1744,
    1748,  1666,  1667,  1668,  1669,  1760,  1671,  1672,  1673,  1769,
    1777,  1767,  1117,  1118,  1778,   658,  1791,  1792,  1794,  1793,
    1795,  1806,   320,  1807,  1814,  1206,  1207,   321,  1208,  1209,
    1210,  1211,  1212,  1692,  1693,  1864,  1829,   322,   323,   324,
     859,   860,   861,   862,   863,   864,   865,   866,   867,   868,
     869,   870,  1159,  1160,   871,   757,   759,   760,   762,   764,
     766,   767,   769,  1867,  1862,  1863,  1868,  1872,  1883,  1885,
    1887,  1896,  1903,   325,  1891,  1898,   326,  1899,  1675,  -471,
    -471,   327,  1205,  1911,  1915,  1907,  1908,  1909,  1910,  1987,
    1927,  1913,  1914,  1929,  1932,  1938,  1933,  1934,  1302,  1942,
    1917,  1946,  1935,  1947,   517,   517,  1349,   291,   291,   517,
     291,   517,  1950,  1948,  1936,  1937,   517,  1939,  1952,  1960,
     878,  1701,  1702,  1703,  1954,  1975,  1976,  1978,   291,  1979,
    1994,  1998,  2000,  2002,  2003,  2032,   517,   517,  2041,   291,
     328,   490,  2044,  2045,  2048,   912,  2049,  1249,  2066,  2050,
    2063,  2065,   291,  2068,  2069,  2071,  2072,  2079,  2084,  2092,
    2093,   925,   658,  2095,  2101,  2096,  2098,  1811,  2097,  1736,
    2099,  2100,   329,   330,   331,  1555,   332,   333,   334,  2056,
    2059,  1746,   335,   336,  2114,   337,   338,   339,   486,  2107,
     517,  2116,  2118,   403,  1300,   405,   406,   407,   408,   409,
     410,   411,   412,  2119,  2120,  2088,  2140,  2144,  -184,  2141,
     419,   420,  2148,  2146,  1759,  2161,  2180,    68,   517,   517,
    2151,  2159,  2163,  2173,  2176,  2190,  2164,  1818,  2177,  2192,
    1762,  2196,  1819,  2188,   708,  1820,  1821,  1100,  1102,  2194,
    2197,  1822,  2189,  2209,  2213,  2212,  1107,  2214,  2216,   291,
     845,  1246,  1499,  1110,  1111,   859,   860,   861,   862,   863,
     864,   865,   866,   867,   868,   869,   870,    44,   896,   871,
     888,  1860,  1629,  1116,  1941,  2157,  1120,  1123,  2185,  1867,
    1349,  2125,  1129,  1130,  1131,    46,  1133,  1134,  1566,  2082,
    1137,  1138,  1139,  1140,  2056,  1142,  1143,  1144,  2111,  2035,
    1953,  1148,  1338,  1150,  1151,  1611,  1971,  2051,  1967,  1156,
    1544,  2193,  2191,  2206,  1773,  1547,    48,  2115,    52,   658,
     658,  1825,    53,    54,   658,    55,   658,   291,    56,   291,
    1842,   658,    59,  1812,   291,   291,   291,   291,   291,    60,
      61,    63,  1158,    65,  1399,  1326,  1926,  1707,  1698,  1832,
    1468,   658,   658,  1237,  1455,    68,  1902,   291,  1636,   517,
    1813,  1648,  1401,   517,  2117,  1568,  1572,  1884,  1844,   291,
     291,   291,   291,   291,  1959,  2046,  2070,  1742,   291,  1250,
       0,     0,     0,  1713,   291,   291,     0,     0,     0,     0,
     490,     0,     0,     0,     0,     0,     0,     0,     0,  1724,
       0,     0,     0,     0,   291,  1108,   291,   291,     0,     0,
     291,     0,     0,     0,     0,     0,     0,     0,     0,     0,
    1739,   859,   860,   861,   862,   863,   864,   865,   866,   867,
     868,   869,   870,  1126,  1127,   871,  1751,  1816,     0,     0,
       0,  1463,     0,     0,     0,     0,     0,  2187,   859,   860,
     861,   862,   863,   864,   865,   866,   867,   868,   869,   870,
    1817,     0,   871,     0,     0,     0,     0,     0,     0,     0,
       0,   291,     0,     0,   517,   517,   859,   860,   861,   862,
     863,   864,   865,   866,   867,   868,   869,   870,     0,     0,
     871,     0,     0,     0,     0,     0,     0,     0,     0,   859,
     860,   861,   862,   863,   864,   865,   866,   867,   868,   869,
     870,     0,     0,   871,  1955,  1956,     0,     0,     0,     0,
       0,     0,     0,  1958,  1458,  1759,     0,     0,     0,     0,
    1964,     0,   658,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,  1963,     0,   859,   860,   861,   862,   863,
     864,   865,   866,   867,   868,   869,   870,     0,     0,   871,
    1019,     0,  1569,  1570,   490,     0,     0,   723,     0,   658,
       0,     0,     0,     0,   658,     0,     0,     0,   658,     0,
      68,  2010,  2011,  2012,  2013,  2019,  2039,     0,     0,     0,
       0,  2040,     0,  2009,     0,     0,     0,     0,     0,  1393,
       0,     0,  1759,  1861,    68,   859,   860,   861,   862,   863,
     864,   865,   866,   867,   868,   869,   870,  2047,     0,   871,
       0,     0,     0,     0,     0,     0,     0,  1418,     0,     0,
    1421,     0,     0,     0,  1425,     0,     0,  1428,  1429,     0,
    1634,     0,     0,  1434,     0,     0,   291,     0,    68,  2010,
    2011,  2012,  2013,  2019,   291,   291,   291,   291,   291,   486,
       0,  2009,     0,     0,     0,     0,     0,     0,     0,     0,
     291,  1682,   291,   291,   859,   860,   861,   862,   863,   864,
     865,   866,   867,   868,   869,   870,     0,     0,   871,   658,
     658,     0,   859,   860,   861,   862,   863,   864,   865,   866,
     867,   868,   869,   870,     0,     0,   871,     0,   517,   861,
     862,   863,   864,   865,   866,   867,   868,   869,   870,     0,
       0,   871,     0,     0,     0,     0,     0,     0,     0,     0,
    1708,     0,     0,     0,     0,   517,   291,     0,     0,   517,
     517,     0,     0,     0,  1945,     0,    68,    68,  2010,  2011,
    2012,  2013,  2019,     0,     0,     0,     0,     0,     0,   658,
    2009,     0,     0,     0,    68,     0,    68,     0,   976,   977,
     978,   979,   980,   981,   982,   983,   984,   985,   986,   987,
       0,     0,     0,  2152,  2153,  2154,  2155,    68,   517,     0,
       0,    68,  1750,     0,     0,     0,     0,     0,     0,  2172,
       0,     0,     0,    68,    68,     0,     0,    68,     0,     0,
       0,     0,     0,     0,     0,     0,     0,    68,     0,     0,
       0,     0,     0,     0,    68,     0,     0,     0,     0,     0,
       0,     0,     0,   291,     0,    68,    68,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,    68,
       0,     0,     0,    68,     0,     0,     0,     0,     0,     0,
     490,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,  1798,     0,     0,     0,     0,     0,   517,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     1,     2,     3,     4,
       5,     0,     0,   517,     0,     0,     0,     0,     0,     0,
       6,     0,     0,     0,     0,     0,     7,     0,     0,     0,
       8,     9,    10,  1518,     0,   517,     0,    11,  2091,     0,
     517,     0,     0,  1830,     0,     0,  1831,     0,     0,     0,
       0,     0,     0,   517,   291,     0,    12,     0,     0,     0,
     658,     0,     0,     0,   658,   658,     0,    13,     0,     0,
     291,   517,   517,   517,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,   517,     0,     0,     0,     0,     0,
     517,   291,     0,     0,     0,     0,     0,     0,     0,     0,
       0,   517,     0,     0,     0,     0,     0,   291,     0,   878,
       0,     0,     0,   658,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,   517,    14,
      15,    16,    17,    18,     0,    19,   517,     0,   517,   517,
       0,     0,     0,     0,   290,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,   347,     0,     0,
       0,    20,    21,    22,     0,     0,    23,    24,    25,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,   291,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,   347,     0,     0,     0,     0,     0,
       0,     0,     0,   467,  1674,   476,     0,   491,     0,     0,
       0,     0,    26,     0,    27,    28,     0,    29,    30,     0,
       0,     0,     0,     0,     0,     0,     0,     0,   658,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
    1705,     0,   517,   517,   517,  1705,     0,     0,     0,     0,
       0,     0,     0,     0,     0,   878,     0,     0,   658,     0,
       0,     0,     0,     0,   291,     0,     0,     0,     0,     0,
       0,   517,     0,     0,     0,     0,  1725,  1726,  1727,     0,
       0,     0,     0,     0,     1,     0,     3,     4,     5,  1734,
       0,     0,     0,     0,     0,  1738,     0,     0,     6,    31,
       0,     0,     0,     0,     7,     0,  1749,     0,     8,     0,
     517,     0,     0,     0,   517,     0,     0,     0,  2053,  2057,
       0,     0,     0,     0,     0,     0,     0,     0,     0,   490,
       0,     0,     0,  1764,     0,     0,     0,   724,     0,     0,
       0,  1772,  1035,   658,   658,    13,  2074,  2075,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,  2138,     0,     0,
     517,     0,  2006,   291,     0,     0,     0,     0,     0,   517,
     291,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,   517,   517,   291,     0,    14,    15,    16,
      17,    18,     0,   517,     0,     0,     0,     0,   347,     0,
       0,     0,     0,  2007,     0,     0,   347,     0,     0,     0,
     517,     0,   517,  2053,   517,     0,  1036,   347,   347,    20,
      21,    22,     0,  2122,     0,     0,    25,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,  1835,  1835,  1835,
       0,     0,   517,     0,     0,     0,     0,   347,     0,     0,
       0,     0,     0,     0,     0,     0,  1037,     0,     0,     0,
       0,     0,   347,     0,     0,     0,   658,   517,   517,     0,
      26,   517,  2008,    28,     0,    29,    30,     0,     0,     0,
       0,     0,     0,     0,     0,     0,  2181,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,  1835,     0,     0,     0,   658,
       0,  1038,  1039,  1040,  1041,  1042,  1043,  1044,     0,  1045,
    1046,  1047,  1048,  1049,  1050,  1051,     0,  1052,  1053,  1054,
    1055,  1056,  1057,  1058,  1059,  1060,  1061,  1062,  1063,     0,
    1064,  1065,  1066,  1067,  1068,  1069,  1070,  1071,  1072,  1073,
    1074,  1075,  1076,  1077,  1078,  1079,  1080,  1081,  1082,     0,
       0,     0,   517,     0,     0,  1835,   517,    31,     0,   291,
       0,     0,   347,   347,  1925,     0,     0,     0,     0,     0,
       0,     0,     1,     0,     3,     4,     5,   491,     0,     0,
       0,     0,     0,   517,     0,     0,     6,     0,  1772,  1835,
       0,   347,     7,     0,     0,   517,     8,     0,  1949,   347,
       0,     0,     0,   347,     0,     0,   347,     0,     0,     0,
     347,     0,     0,     0,     0,  1961,     0,  1965,     0,  1772,
       0,     0,     0,     0,     0,     0,     0,   517,     0,     0,
       0,     0,     0,    13,  1083,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,  1993,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
    2006,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,   658,   658,     0,     0,  1772,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
    1109,     0,     0,     0,     0,    14,    15,    16,    17,    18,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,  2007,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,    20,    21,    22,
       0,     0,     0,     0,    25,     0,  1145,  1146,     0,     0,
       0,     0,     0,  1152,  1153,  1154,  1155,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,   347,   347,
       0,   347,     0,     0,     0,     0,     0,  2085,     0,     0,
     490,  2089,     0,     0,     0,     0,     0,     0,    26,   347,
    2008,    28,     0,    29,    30,     0,     0,     0,     0,     0,
     972,     0,     0,     0,     0,     0,     0,     0,  1835,     0,
       0,     0,     0,   347,     0,     0,     0,     0,     0,     0,
    1772,     0,     0,     0,     0,     0,     0,  1200,     0,  1202,
       0,     0,     0,     0,     0,     0,   491,   467,   467,     0,
       0,     0,     0,     0,     0,     0,   467,     0,     0,     0,
       0,     0,  1835,   467,   467,     0,     0,     0,   467,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,   467,     0,     0,   467,   467,     0,     0,
       0,     0,   467,   467,   467,    31,   467,   467,     0,     0,
     467,   467,   467,   467,     0,   467,   467,   467,     0,     0,
     467,   467,   467,   467,   467,   847,     0,     0,     0,   467,
     347,     0,   848,   849,   850,   851,   852,   853,   854,     0,
     855,   856,   857,   858,   859,   860,   861,   862,   863,   864,
     865,   866,   867,   868,   869,   870,     0,     0,   871,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,   972,  1328,
       0,     0,     0,     0,     0,     0,     0,     0,  1226,     0,
     347,     0,     0,     0,     0,   347,   347,  1238,   347,   972,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
     491,     0,     0,     0,     0,   972,     0,     0,   347,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
     347,   347,   347,   347,   347,     0,     0,     0,     0,   347,
       0,     0,     0,     0,     0,   347,   347,     0,  1394,  1395,
    1397,     0,  1398,     0,     0,     0,     0,  1403,     0,     0,
    1404,  1405,  1406,     0,  1407,   347,  1408,  1309,  1310,     0,
    1413,   347,     0,  1416,     0,     0,     0,     0,     0,  1422,
    1423,  1424,     0,  1426,  1427,     0,     0,  1430,  1431,  1432,
    1433,     0,  1435,  1436,  1437,     0,     0,  1441,  1442,  1443,
    1444,  1445,     0,     0,     0,     0,  1450,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,   347,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,  1464,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
    1487,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,   467,
       0,     0,     0,     0,     0,   972,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,   467,     0,     0,
     467,     0,     0,     0,   467,     0,     0,   467,   467,     0,
       0,     0,     0,   467,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,   467,     0,     0,
       0,     0,     0,     0,     0,     0,   491,   347,     0,     0,
       0,     0,     0,     0,     0,   347,   347,   347,   347,  1226,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,  1226,     0,   347,   347,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,  1656,     0,     0,     0,  1661,     0,     0,  1664,  1665,
       0,     0,     0,     0,  1670,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,   347,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
    1685,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,   347,     0,     0,     0,   467,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,   467,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,  1799,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,  1808,  1809,     0,
       0,     0,     0,     0,     0,   347,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,   347,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,  1827,     0,     0,     0,
       0,     0,   347,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,  1839,     0,     0,  1840,   347,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,   491,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,  1886,
       0,     0,  1888,  1890,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,   347,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,  1912,     0,     0,     0,     0,
       0,     0,     0,     0,  1916,     0,     0,     0,     0,     0,
       0,     0,     0,     0,  1922,     0,  1923,  1924,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,   347,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,  1981,  1982,  1983,
    1984,     0,     0,  1985,  1986,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,  1226,     0,     0,     0,     0,     0,
    2064,  1226,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,   347,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,  2087,     0,     0,     0,     0,  2090,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
    2094,     0,     0, -1273,     0,     0,     0,     0,     0,    79,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
      80,     0,     0,     0,     0,     0,     0,     0,     0,     0,
      81,     0,     0,     0,     0,    82,     0,     0,    83,    84,
      85,     0,     0,     0,    86,    87,     0,     0,     0,     0,
      88,     0,    89,    90,    91,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,    92,     0,     0,     0,     0,     0,     0,     0,
       0,    93,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,  2199,  2200,     0,     0,
       0,     0,    94,     0,     0,     0,   491,    95,    96,    97,
     347,     0,     0,    98,    99,     0,     0,     0,   100,     0,
     101,   102,   103,   104,   105,   106,   107,   108,   109,   110,
     111,     0,     0,     0,     0,     0,   112,     0,   113,   114,
     115,     0,     0,   116,   117,   118,   119,   120,   121,   122,
     123,   124,   125,   126,   127,   128,   129,   130,     0,   131,
       0,   132,   133,   134,     0,     0,     0,   135,     0,     0,
       0,   136,   137,   138,   139,   140,   141,   142,   143,   144,
     145,   146,   147,   148,   149,   150,   151,   152,   153,   154,
     155,   156,   157,   158,   159,   160,   161,   162,     0,   163,
     164,   165,   166,   167,     0,   168,   169,   170,   171,   172,
     173,   174,   175,   176,   177,   178,   179,     0,   180,   181,
     182,   183,   184,   185,     0,   186,   187,   188,   189,   190,
     191,   192,   193,   194,   195,   196,   197,   198,   199,     0,
       0,     0,   200,   201,   202,   203,   204,     0,   205,   206,
     207,   208,     0,     0,   209,   210,   211,   212,   213,   214,
     215,   216,   217,   218,   219,   220,   221,   222,   223,   224,
     225,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,   226,   227,   228,   229,   230,   231,   232,
       0,   233,     0,     0,     0,     0,     0,     0,   234,   235,
     236,   237,   238,   239,   240,   241,   242,   243,   244,   245,
     246,   247,   248,   249,   250,   251,   252,   253,   254,   255,
     256,   257,   258,   259,   260,   261,   262,   263,   264,   265,
     266,   267,   268,   269,   270,   271,   272,   273,   274,   275,
     276,   277,   278,   279,   280,   281,   282,   283,   284,   285,
     286,   287,   288,   289,     0,     0, -1273,   525,   526,    79,
     527,   528,   529,   530,   531,   532,   533,   534,   535,   536,
      80,   537,   538,     0,   539,   540,   541,   542,   543,   544,
      81,   545,   546,   547,   548,    82,   549,     0,    83,    84,
      85,     0,     0,     0,    86,    87,   550,   551,     0,   552,
      88,   553,    89,    90,    91,     0,     0,   554,   555,   556,
     557,   558,   559,   560,   561,   562,   563,   564,   565,     0,
       0,     0,   515,     0,  1344,  1345,     0,     0,     0,     0,
       0,    93,     0,   566,   567,   568,   569,   570,   571,     0,
     572,   573,   574,   575,   576,   577,   578,   579,  1588,     0,
       0,   580,   581,   582,   583,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,   584,   585,
       0,     0,     0,   586,     0,     0,   587,   588,     0,     0,
       0,   589,    94,   590,   591,     0,   592,    95,    96,    97,
     593,   594,   595,    98,    99,   596,   597,   598,   100,     0,
     101,   102,   103,   104,   105,   106,   107,   108,   109,   110,
     111,   599,   600,   601,   602,   603,   112,   604,   113,   114,
     115,     0,   605,   116,   117,   118,   119,   120,   121,   122,
     123,   124,   125,   126,   127,   128,   129,   130,     0,   131,
       0,   132,   133,   134,   606,   607,   608,   135,   609,   610,
     611,   136,   137,   138,   139,   140,   141,   142,   143,   144,
     145,   146,   147,   148,   149,   150,   151,   152,   153,   154,
     155,   156,   157,   158,   159,   160,   161,   162,     0,   163,
     164,   165,   166,   167,     0,   168,   169,   170,   171,   172,
     173,   174,   175,   176,   177,   178,   179,     0,   180,   181,
     182,   183,   184,   185,     0,   186,   187,   188,   189,   190,
     191,   192,   193,   194,   195,   196,   197,   198,   199,     0,
     612,     0,   200,   201,   202,   203,   204,   613,   205,   206,
     207,   208,     0,     0,   209,   210,   211,   212,   213,   214,
     215,   216,   217,   218,   219,   220,   221,   222,   223,   224,
     225,   614,   615,   616,     0,   617,   618,   619,   620,   621,
     622,   623,   624,     0,   625,   626,     0,   627,   628,   629,
     630,   631,   632,   633,   634,   635,   636,   637,   638,   639,
     640,   641,   642,   643,   644,   645,   646,   647,   648,   649,
     650,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,   651,   226,   227,   228,   229,   230,   231,   232,
     652,   233,     0,     0,   653,   654,   655,   656,   234,   235,
     236,   237,   238,   239,   240,   241,   242,   243,   244,   245,
     246,   247,   248,   249,   250,   251,   252,   253,   254,   255,
     256,   257,   258,   259,   260,   261,   262,   263,   264,   265,
     266,   267,   268,   269,   270,   271,   272,   273,   274,   275,
     276,   277,   278,   279,   280,   281,   282,   283,   284,   285,
     286,   287,   288,   289,   657,   525,   526,    79,   527,   528,
     529,   530,   531,   532,   533,   534,   535,   536,    80,   537,
     538,     0,   539,   540,   541,   542,   543,   544,    81,   545,
     546,   547,   548,    82,   549,     0,    83,    84,    85,     0,
       0,     0,    86,    87,   550,   551,     0,   552,    88,   553,
      89,    90,    91,     0,     0,   554,   555,   556,   557,   558,
     559,   560,   561,   562,   563,   564,   565,     0,     0,     0,
     515,     0,  1344,  1345,     0,     0,     0,     0,     0,    93,
       0,   566,   567,   568,   569,   570,   571,     0,   572,   573,
     574,   575,   576,   577,   578,   579,     0,     0,     0,   580,
     581,   582,   583,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,   584,   585,     0,     0,
       0,   586,     0,     0,   587,   588,     0,     0,     0,   589,
      94,   590,   591,     0,   592,    95,    96,    97,   593,   594,
     595,    98,    99,   596,   597,   598,   100,     0,   101,   102,
     103,   104,   105,   106,   107,   108,   109,   110,   111,   599,
     600,   601,   602,   603,   112,   604,   113,   114,   115,     0,
     605,   116,   117,   118,   119,   120,   121,   122,   123,   124,
     125,   126,   127,   128,   129,   130,     0,   131,     0,   132,
     133,   134,   606,   607,   608,   135,   609,   610,   611,   136,
     137,   138,   139,   140,   141,   142,   143,   144,   145,   146,
     147,   148,   149,   150,   151,   152,   153,   154,   155,   156,
     157,   158,   159,   160,   161,   162,     0,   163,   164,   165,
     166,   167,     0,   168,   169,   170,   171,   172,   173,   174,
     175,   176,   177,   178,   179,     0,   180,   181,   182,   183,
     184,   185,     0,   186,   187,   188,   189,   190,   191,   192,
     193,   194,   195,   196,   197,   198,   199,     0,   612,     0,
     200,   201,   202,   203,   204,   613,   205,   206,   207,   208,
       0,     0,   209,   210,   211,   212,   213,   214,   215,   216,
     217,   218,   219,   220,   221,   222,   223,   224,   225,   614,
     615,   616,     0,   617,   618,   619,   620,   621,   622,   623,
     624,     0,   625,   626,     0,   627,   628,   629,   630,   631,
     632,   633,   634,   635,   636,   637,   638,   639,   640,   641,
     642,   643,   644,   645,   646,   647,   648,   649,   650,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
     651,   226,   227,   228,   229,   230,   231,   232,   652,   233,
       0,     0,   653,   654,   655,   656,   234,   235,   236,   237,
     238,   239,   240,   241,   242,   243,   244,   245,   246,   247,
     248,   249,   250,   251,   252,   253,   254,   255,   256,   257,
     258,   259,   260,   261,   262,   263,   264,   265,   266,   267,
     268,   269,   270,   271,   272,   273,   274,   275,   276,   277,
     278,   279,   280,   281,   282,   283,   284,   285,   286,   287,
     288,   289,   657,   525,   526,    79,   527,   528,   529,   530,
     531,   532,   533,   534,   535,   536,    80,   537,   538,     0,
     539,   540,   541,   542,   543,   544,    81,   545,   546,   547,
     548,    82,   549,     0,    83,    84,    85,     0,     0,     0,
      86,    87,   550,   551,     0,   552,    88,   553,    89,    90,
      91,     0,     0,   554,   555,   556,   557,   558,   559,   560,
     561,   562,   563,   564,   565,    13,     0,     0,   515,     0,
       0,     0,     0,     0,     0,     0,     0,    93,     0,   566,
     567,   568,   569,   570,   571,     0,   572,   573,   574,   575,
     576,   577,   578,   579,     0,     0,     0,   580,   581,   582,
     583,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,   584,   585,     0,     0,     0,   586,
       0,     0,   587,   588,     0,     0,     0,   589,    94,   590,
     591,     0,   592,    95,    96,    97,   593,   594,   595,    98,
      99,   596,   597,   598,   100,     0,   101,   102,   103,   104,
     105,   106,   107,   108,   109,   110,   111,   599,   600,   601,
     602,   603,   112,   604,   113,   114,   115,     0,   605,   116,
     117,   118,   119,   120,   121,   122,   123,   124,   125,   126,
     127,   128,   129,   130,     0,   131,     0,   132,   133,   134,
     606,   607,   608,   135,   609,   610,   611,   136,   137,   138,
     139,   140,   141,   142,   143,   144,   145,   146,   147,   148,
     149,   150,   151,   152,   153,   154,   155,   156,   157,   158,
     159,   160,   161,   162,     0,   163,   164,   165,   166,   167,
       0,   168,   169,   170,   171,   172,   173,   174,   175,   176,
     177,   178,   179,     0,   180,   181,   182,   183,   184,   185,
       0,   186,   187,   188,   189,   190,   191,   192,   193,   194,
     195,   196,   197,   198,   199,     0,   612,     0,   200,   201,
     202,   203,   204,   613,   205,   206,   207,   208,     0,     0,
     209,   210,   211,   212,   213,   214,   215,   216,   217,   218,
     219,   220,   221,   222,   223,   224,   225,   614,   615,   616,
       0,   617,   618,   619,   620,   621,   622,   623,   624,     0,
     625,   626,     0,   627,   628,   629,   630,   631,   632,   633,
     634,   635,   636,   637,   638,   639,   640,   641,   642,   643,
     644,   645,   646,   647,   648,   649,   650,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,   651,   226,
     227,   228,   229,   230,   231,   232,   652,   233,     0,     0,
     653,   654,   655,   656,   234,   235,   236,   237,   238,   239,
     240,   241,   242,   243,   244,   245,   246,   247,   248,   249,
     250,   251,   252,   253,   254,   255,   256,   257,   258,   259,
     260,   261,   262,   263,   264,   265,   266,   267,   268,   269,
     270,   271,   272,   273,   274,   275,   276,   277,   278,   279,
     280,   281,   282,   283,   284,   285,   286,   287,   288,   289,
     657,   525,   526,    79,   527,   528,   529,   530,   531,   532,
     533,   534,   535,   536,    80,   537,   538,     0,   539,   540,
     541,   542,   543,   544,    81,   545,   546,   547,   548,    82,
     549,     0,    83,    84,    85,     0,     0,     0,    86,    87,
     550,   551,     0,   552,    88,   553,    89,    90,    91,     0,
       0,   554,   555,   556,   557,   558,   559,   560,   561,   562,
     563,   564,   565,     0,     0,     0,   377,     0,     0,     0,
       0,     0,     0,     0,     0,    93,     0,   566,   567,   568,
     569,   570,   571,     0,   572,   573,   574,   575,   576,   577,
     578,   579,     0,     0,     0,   580,   581,   582,   583,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,  1196,
       0,     0,   584,   585,     0,     0,     0,   586,     0,     0,
     587,   588,     0,     0,     0,   589,    94,   590,   591,     0,
     592,    95,    96,    97,   593,   594,   595,    98,    99,   596,
     597,   598,   100,     0,   101,   102,   103,   104,   105,   106,
     107,   108,   109,   110,   111,   599,   600,   601,   602,   603,
     112,   604,   113,   114,   115,     0,   605,   116,   117,   118,
     119,   120,   121,   122,   123,   124,   125,   126,   127,   128,
     129,   130,     0,   131,     0,   132,   133,   134,   606,   607,
     608,   135,   609,   610,   611,   136,   137,   138,   139,   140,
     141,   142,   143,   144,   145,   146,   147,   148,   149,   150,
     151,   152,   153,   154,   155,   156,   157,   158,   159,   160,
     161,   162,     0,   163,   164,   165,   166,   167,     0,   168,
     169,   170,   171,   172,   173,   174,   175,   176,   177,   178,
     179,     0,   180,   181,   182,   183,   184,   185,     0,   186,
     187,   188,   189,   190,   191,   192,   193,   194,   195,   196,
     197,   198,   199,     0,   612,     0,   200,   201,   202,   203,
     204,   613,   205,   206,   207,   208,     0,     0,   209,   210,
     211,   212,   213,   214,   215,   216,   217,   218,   219,   220,
     221,   222,   223,   224,   225,   614,   615,   616,     0,   617,
     618,   619,   620,   621,   622,   623,   624,     0,   625,   626,
       0,   627,   628,   629,   630,   631,   632,   633,   634,   635,
     636,   637,   638,   639,   640,   641,   642,   643,   644,   645,
     646,   647,   648,   649,   650,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,   651,   226,   227,   228,
     229,   230,   231,   232,   652,   233,     0,     0,   653,   654,
     655,   656,   234,   235,   236,   237,   238,   239,   240,   241,
     242,   243,   244,   245,   246,   247,   248,   249,   250,   251,
     252,   253,   254,   255,   256,   257,   258,   259,   260,   261,
     262,   263,   264,   265,   266,   267,   268,   269,   270,   271,
     272,   273,   274,   275,   276,   277,   278,   279,   280,   281,
     282,   283,   284,   285,   286,   287,   288,   289,   657,   525,
     526,    79,   527,   528,   529,   530,   531,   532,   533,   534,
     535,   536,    80,   537,   538,     0,   539,   540,   541,   542,
     543,   544,    81,   545,   546,   547,   548,    82,   549,     0,
      83,    84,    85,     0,     0,     0,    86,    87,   550,   551,
       0,   552,    88,   553,    89,    90,    91,     0,     0,   554,
     555,   556,   557,   558,   559,   560,   561,   562,   563,   564,
     565,     0,     0,     0,    92,     0,     0,     0,     0,     0,
       0,     0,     0,    93,     0,   566,   567,   568,   569,   570,
     571,     0,   572,   573,   574,   575,   576,   577,   578,   579,
       0,     0,     0,   580,   581,   582,   583,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,  1240,     0,     0,
     584,   585,     0,     0,     0,   586,     0,     0,   587,   588,
       0,     0,     0,   589,    94,   590,   591,     0,   592,    95,
      96,    97,   593,   594,   595,    98,    99,   596,   597,   598,
     100,     0,   101,   102,   103,   104,   105,   106,   107,   108,
     109,   110,   111,   599,   600,   601,   602,   603,   112,   604,
     113,   114,   115,     0,   605,   116,   117,   118,   119,   120,
     121,   122,   123,   124,   125,   126,   127,   128,   129,   130,
       0,   131,     0,   132,   133,   134,   606,   607,   608,   135,
     609,   610,   611,   136,   137,   138,   139,   140,   141,   142,
     143,   144,   145,   146,   147,   148,   149,   150,   151,   152,
     153,   154,   155,   156,   157,   158,   159,   160,   161,   162,
       0,   163,   164,   165,   166,   167,     0,   168,   169,   170,
     171,   172,   173,   174,   175,   176,   177,   178,   179,     0,
     180,   181,   182,   183,   184,   185,     0,   186,   187,   188,
     189,   190,   191,   192,   193,   194,   195,   196,   197,   198,
     199,     0,   612,     0,   200,   201,   202,   203,   204,   613,
     205,   206,   207,   208,     0,     0,   209,   210,   211,   212,
     213,   214,   215,   216,   217,   218,   219,   220,   221,   222,
     223,   224,   225,   614,   615,   616,     0,   617,   618,   619,
     620,   621,   622,   623,   624,     0,   625,   626,     0,   627,
     628,   629,   630,   631,   632,   633,   634,   635,   636,   637,
     638,   639,   640,   641,   642,   643,   644,   645,   646,   647,
     648,   649,   650,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,   651,   226,   227,   228,   229,   230,
     231,   232,   652,   233,     0,     0,   653,   654,   655,   656,
     234,   235,   236,   237,   238,   239,   240,   241,   242,   243,
     244,   245,   246,   247,   248,   249,   250,   251,   252,   253,
     254,   255,   256,   257,   258,   259,   260,   261,   262,   263,
     264,   265,   266,   267,   268,   269,   270,   271,   272,   273,
     274,   275,   276,   277,   278,   279,   280,   281,   282,   283,
     284,   285,   286,   287,   288,   289,   657,   525,   526,    79,
     527,   528,   529,   530,   531,   532,   533,   534,   535,   536,
      80,   537,   538,     0,   539,   540,   541,   542,   543,   544,
      81,   545,   546,   547,   548,    82,   549,     0,    83,    84,
      85,     0,     0,     0,    86,    87,   550,   551,     0,   552,
      88,   553,    89,    90,    91,     0,     0,   554,   555,   556,
     557,   558,   559,   560,   561,   562,   563,   564,   565,     0,
       0,     0,   515,     0,     0,     0,     0,     0,     0,     0,
       0,    93,     0,   566,   567,   568,   569,   570,   571,     0,
     572,   573,   574,   575,   576,   577,   578,   579,     0,     0,
       0,   580,   581,   582,   583,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,  1687,     0,     0,   584,   585,
       0,     0,     0,   586,     0,     0,   587,   588,     0,     0,
       0,   589,    94,   590,   591,     0,   592,    95,    96,    97,
     593,   594,   595,    98,    99,   596,   597,   598,   100,     0,
     101,   102,   103,   104,   105,   106,   107,   108,   109,   110,
     111,   599,   600,   601,   602,   603,   112,   604,   113,   114,
     115,     0,   605,   116,   117,   118,   119,   120,   121,   122,
     123,   124,   125,   126,   127,   128,   129,   130,     0,   131,
       0,   132,   133,   134,   606,   607,   608,   135,   609,   610,
     611,   136,   137,   138,   139,   140,   141,   142,   143,   144,
     145,   146,   147,   148,   149,   150,   151,   152,   153,   154,
     155,   156,   157,   158,   159,   160,   161,   162,     0,   163,
     164,   165,   166,   167,     0,   168,   169,   170,   171,   172,
     173,   174,   175,   176,   177,   178,   179,     0,   180,   181,
     182,   183,   184,   185,     0,   186,   187,   188,   189,   190,
     191,   192,   193,   194,   195,   196,   197,   198,   199,     0,
     612,     0,   200,   201,   202,   203,   204,   613,   205,   206,
     207,   208,     0,     0,   209,   210,   211,   212,   213,   214,
     215,   216,   217,   218,   219,   220,   221,   222,   223,   224,
     225,   614,   615,   616,     0,   617,   618,   619,   620,   621,
     622,   623,   624,     0,   625,   626,     0,   627,   628,   629,
     630,   631,   632,   633,   634,   635,   636,   637,   638,   639,
     640,   641,   642,   643,   644,   645,   646,   647,   648,   649,
     650,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,   651,   226,   227,   228,   229,   230,   231,   232,
     652,   233,     0,     0,   653,   654,   655,   656,   234,   235,
     236,   237,   238,   239,   240,   241,   242,   243,   244,   245,
     246,   247,   248,   249,   250,   251,   252,   253,   254,   255,
     256,   257,   258,   259,   260,   261,   262,   263,   264,   265,
     266,   267,   268,   269,   270,   271,   272,   273,   274,   275,
     276,   277,   278,   279,   280,   281,   282,   283,   284,   285,
     286,   287,   288,   289,   657,   525,   526,    79,   527,   528,
     529,   530,   531,   532,   533,   534,   535,   536,    80,   537,
     538,     0,   539,   540,   541,   542,   543,   544,    81,   545,
     546,   547,   548,    82,   549,     0,    83,    84,    85,     0,
       0,     0,    86,    87,   550,   551,     0,   552,    88,   553,
      89,    90,    91,     0,     0,   554,   555,   556,   557,   558,
     559,   560,   561,   562,   563,   564,   565,     0,     0,     0,
     515,     0,     0,     0,     0,     0,     0,     0,     0,    93,
       0,   566,   567,   568,   569,   570,   571,     0,   572,   573,
     574,   575,   576,   577,   578,   579,     0,     0,     0,   580,
     581,   582,   583,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,   584,   585,     0,     0,
       0,   586,     0,     0,   587,   588,     0,     0,     0,   589,
      94,   590,   591,     0,   592,    95,    96,    97,   593,   594,
     595,    98,    99,   596,   597,   598,   100,     0,   101,   102,
     103,   104,   105,   106,   107,   108,   109,   110,   111,   599,
     600,   601,   602,   603,   112,   604,   113,   114,   115,     0,
     605,   116,   117,   118,   119,   120,   121,   122,   123,   124,
     125,   126,   127,   128,   129,   130,     0,   131,     0,   132,
     133,   134,   606,   607,   608,   135,   609,   610,   611,   136,
     137,   138,   139,   140,   141,   142,   143,   144,   145,   146,
     147,   148,   149,   150,   151,   152,   153,   154,   155,   156,
     157,   158,   159,   160,   161,   162,     0,   163,   164,   165,
     166,   167,     0,   168,   169,   170,   171,   172,   173,   174,
     175,   176,   177,   178,   179,     0,   180,   181,   182,   183,
     184,   185,     0,   186,   187,   188,   189,   190,   191,   192,
     193,   194,   195,   196,   197,   198,   199,     0,   612,     0,
     200,   201,   202,   203,   204,   613,   205,   206,   207,   208,
       0,     0,   209,   210,   211,   212,   213,   214,   215,   216,
     217,   218,   219,   220,   221,   222,   223,   224,   225,   614,
     615,   616,     0,   617,   618,   619,   620,   621,   622,   623,
     624,     0,   625,   626,     0,   627,   628,   629,   630,   631,
     632,   633,   634,   635,   636,   637,   638,   639,   640,   641,
     642,   643,   644,   645,   646,   647,   648,   649,   650,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
     651,   226,   227,   228,   229,   230,   231,   232,   652,   233,
       0,     0,   653,   654,   655,   656,   234,   235,   236,   237,
     238,   239,   240,   241,   242,   243,   244,   245,   246,   247,
     248,   249,   250,   251,   252,   253,   254,   255,   256,   257,
     258,   259,   260,   261,   262,   263,   264,   265,   266,   267,
     268,   269,   270,   271,   272,   273,   274,   275,   276,   277,
     278,   279,   280,   281,   282,   283,   284,   285,   286,   287,
     288,   289,   657,   525,   526,    79,   527,   528,   529,   530,
     531,   532,   533,   534,   535,   536,    80,   537,   538,     0,
     539,   540,   541,   542,   543,   544,    81,   545,   546,   547,
     548,    82,   549,     0,    83,    84,    85,     0,     0,     0,
      86,    87,   550,   551,     0,   552,    88,   553,    89,    90,
      91,     0,     0,   554,   555,   556,   557,   558,   559,   560,
     561,   562,   563,   564,   565,     0,     0,     0,    92,     0,
       0,     0,     0,     0,     0,     0,     0,    93,     0,   566,
     567,   568,   569,   570,   571,     0,   572,   573,   574,   575,
     576,   577,   578,   579,     0,     0,     0,   580,   581,   582,
     583,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,   584,   585,     0,     0,     0,   586,
       0,     0,   587,   588,     0,     0,     0,   589,    94,   590,
     591,     0,   592,    95,    96,    97,   593,   594,   595,    98,
      99,   596,   597,   598,   100,     0,   101,   102,   103,   104,
     105,   106,   107,   108,   109,   110,   111,   599,   600,   601,
     602,   603,   112,   604,   113,   114,   115,     0,   605,   116,
     117,   118,   119,   120,   121,   122,   123,   124,   125,   126,
     127,   128,   129,   130,     0,   131,     0,   132,   133,   134,
     606,   607,   608,   135,   609,   610,   611,   136,   137,   138,
     139,   140,   141,   142,   143,   144,   145,   146,   147,   148,
     149,   150,   151,   152,   153,   154,   155,   156,   157,   158,
     159,   160,   161,   162,     0,   163,   164,   165,   166,   167,
       0,   168,   169,   170,   171,   172,   173,   174,   175,   176,
     177,   178,   179,     0,   180,   181,   182,   183,   184,   185,
       0,   186,   187,   188,   189,   190,   191,   192,   193,   194,
     195,   196,   197,   198,   199,     0,   612,     0,   200,   201,
     202,   203,   204,   613,   205,   206,   207,   208,     0,     0,
     209,   210,   211,   212,   213,   214,   215,   216,   217,   218,
     219,   220,   221,   222,   223,   224,   225,   614,   615,   616,
       0,   617,   618,   619,   620,   621,   622,   623,   624,     0,
     625,   626,     0,   627,   628,   629,   630,   631,   632,   633,
     634,   635,   636,   637,   638,   639,   640,   641,   642,   643,
     644,   645,   646,   647,   648,   649,   650,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,   651,   226,
     227,   228,   229,   230,   231,   232,   652,   233,     0,     0,
     653,   654,   655,   656,   234,   235,   236,   237,   238,   239,
     240,   241,   242,   243,   244,   245,   246,   247,   248,   249,
     250,   251,   252,   253,   254,   255,   256,   257,   258,   259,
     260,   261,   262,   263,   264,   265,   266,   267,   268,   269,
     270,   271,   272,   273,   274,   275,   276,   277,   278,   279,
     280,   281,   282,   283,   284,   285,   286,   287,   288,   289,
     657,   525,   526,    79,   527,   528,   529,   530,   531,   532,
     533,   534,   535,   536,    80,   537,   538,     0,   539,   540,
     541,   542,   543,   544,    81,   545,   546,   547,   548,    82,
     549,     0,    83,    84,    85,     0,     0,     0,    86,    87,
     550,   551,     0,   552,    88,   553,    89,    90,    91,     0,
       0,   554,   555,   556,   557,   558,   559,   560,   561,   562,
     563,   564,   565,     0,     0,     0,   377,     0,     0,     0,
       0,     0,     0,     0,     0,    93,     0,   566,   567,   568,
     569,   570,   571,     0,   572,   573,   574,   575,   576,   577,
     578,   579,     0,     0,     0,   580,   581,   582,   583,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,   584,   585,     0,     0,     0,   586,     0,     0,
     587,   588,     0,     0,     0,   589,    94,   590,   591,     0,
     592,    95,    96,    97,   593,   594,   595,    98,    99,   596,
     597,   598,   100,     0,   101,   102,   103,   104,   105,   106,
     107,   108,   109,   110,   111,   599,   600,   601,   602,   603,
     112,   604,   113,   114,   115,     0,   605,   116,   117,   118,
     119,   120,   121,   122,   123,   124,   125,   126,   127,   128,
     129,   130,     0,   131,     0,   132,   133,   134,   606,   607,
     608,   135,   609,   610,   611,   136,   137,   138,   139,   140,
     141,   142,   143,   144,   145,   146,   147,   148,   149,   150,
     151,   152,   153,   154,   155,   156,   157,   158,   159,   160,
     161,   162,     0,   163,   164,   165,   166,   167,     0,   168,
     169,   170,   171,   172,   173,   174,   175,   176,   177,   178,
     179,     0,   180,   181,   182,   183,   184,   185,     0,   186,
     187,   188,   189,   190,   191,   192,   193,   194,   195,   196,
     197,   198,   199,     0,   612,     0,   200,   201,   202,   203,
     204,   613,   205,   206,   207,   208,     0,     0,   209,   210,
     211,   212,   213,   214,   215,   216,   217,   218,   219,   220,
     221,   222,   223,   224,   225,   614,   615,   616,     0,   617,
     618,   619,   620,   621,   622,   623,   624,     0,   625,   626,
       0,   627,   628,   629,   630,   631,   632,   633,   634,   635,
     636,   637,   638,   639,   640,   641,   642,   643,   644,   645,
     646,   647,   648,   649,   650,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,   651,   226,   227,   228,
     229,   230,   231,   232,   652,   233,     0,     0,   653,   654,
     655,   656,   234,   235,   236,   237,   238,   239,   240,   241,
     242,   243,   244,   245,   246,   247,   248,   249,   250,   251,
     252,   253,   254,   255,   256,   257,   258,   259,   260,   261,
     262,   263,   264,   265,   266,   267,   268,   269,   270,   271,
     272,   273,   274,   275,   276,   277,   278,   279,   280,   281,
     282,   283,   284,   285,   286,   287,   288,   289,   657,   525,
     526,    79,   527,   528,   529,   530,   531,   532,   533,   534,
     535,   536,    80,   537,   538,     0,   539,   540,   541,   542,
     543,   544,    81,   545,   546,   547,   548,    82,   549,     0,
      83,    84,    85,     0,     0,     0,    86,    87,  1331,  1332,
       0,   552,    88,   553,    89,    90,    91,     0,     0,   554,
     555,   556,   557,   558,   559,   560,   561,   562,   563,   564,
     565,     0,     0,     0,   515,     0,     0,     0,     0,     0,
       0,     0,     0,    93,     0,   566,   567,   568,   569,   570,
     571,     0,   572,   573,   574,   575,   576,   577,   578,   579,
       0,     0,     0,   580,   581,   582,   583,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
     584,   585,     0,     0,     0,   586,     0,     0,   587,   588,
       0,     0,     0,   589,    94,   590,   591,     0,   592,    95,
      96,    97,   593,   594,  1333,    98,    99,   596,   597,   598,
    1334,     0,  1335,   102,   103,   104,   105,   106,  1336,  1337,
     109,   110,   111,   599,   600,   601,   602,   603,   112,   604,
     113,   114,   115,     0,   605,   116,   117,   118,   119,   120,
     121,   122,   123,   124,   125,   126,   127,   128,   129,   130,
       0,   131,     0,   132,   133,   134,   606,   607,   608,   135,
     609,   610,   611,   136,   137,   138,   139,   140,   141,   142,
     143,   144,   145,   146,   147,   148,   149,   150,   151,   152,
     153,   154,   155,   156,   157,   158,   159,   160,   161,   162,
       0,   163,   164,   165,   166,   167,     0,   168,   169,   170,
     171,   172,   173,   174,   175,   176,   177,   178,   179,     0,
     180,   181,   182,   183,   184,   185,     0,   186,   187,   188,
     189,   190,   191,   192,   193,   194,   195,   196,   197,   198,
     199,     0,   612,     0,   200,   201,   202,   203,   204,   613,
     205,   206,   207,   208,     0,     0,   209,   210,   211,   212,
     213,   214,   215,   216,   217,   218,   219,   220,   221,   222,
     223,   224,   225,   614,   615,   616,     0,   617,   618,   619,
     620,   621,   622,   623,   624,     0,   625,   626,     0,   627,
     628,   629,   630,   631,   632,   633,   634,   635,   636,   637,
     638,   639,   640,   641,   642,   643,   644,   645,   646,   647,
     648,   649,   650,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,   651,   226,   227,   228,   229,   230,
     231,   232,   652,   233,     0,     0,   653,   654,   655,   656,
     234,   235,   236,   237,   238,   239,   240,   241,   242,   243,
     244,   245,   246,   247,   248,   249,   250,   251,   252,   253,
     254,   255,   256,   257,   258,   259,   260,   261,   262,   263,
     264,   265,   266,   267,   268,   269,   270,   271,   272,   273,
     274,   275,   276,   277,   278,   279,   280,   281,   282,   283,
     284,   285,   286,   287,   288,   289,   657,     1,    79,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,    80,
       0,     0,     0,     0,     0,   372,     0,     0,     0,    81,
     373,     0,     0,     0,    82,     0,     0,    83,    84,    85,
       0,     0,     0,    86,    87,     0,     0,     0,   374,    88,
       0,    89,    90,    91,     0,     0,     0,     0,     0,     0,
       0,     0,   375,   376,     0,     0,     0,     0,    13,     0,
       0,   377,   378,   379,   380,   381,   382,   383,     0,     0,
      93,   384,   385,   386,   387,     0,     0,     0,   388,   389,
       0,     0,     0,     0,     0,   390,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,   391,   392,     0,     0,     0,   394,     0,     0,
     395,     0,     0,   396,   397,   398,   399,     0,     0,     0,
       0,    94,     0,     0,     0,     0,    95,    96,    97,   400,
       0,     0,    98,    99,     0,     0,   401,   100,     0,   101,
     102,   103,   104,   105,   106,   107,   108,   109,   110,   111,
     402,     0,     0,     0,     0,   112,     0,   113,   114,   115,
       0,     0,   116,   117,   118,   119,   120,   121,   122,   123,
     124,   125,   126,   127,   128,   129,   130,     0,   131,     0,
     132,   133,   134,     0,     0,     0,   135,     0,     0,     0,
     136,   137,   138,   139,   140,   141,   142,   143,   144,   145,
     146,   147,   148,   149,   150,   151,   152,   153,   154,   155,
     156,   157,   158,   159,   160,   161,   162,     0,   163,   164,
     165,   166,   167,     0,   168,   169,   170,   171,   172,   173,
     174,   175,   176,   177,   178,   179,     0,   180,   181,   182,
     183,   184,   185,     0,   186,   187,   188,   189,   190,   191,
     192,   193,   194,   195,   196,   197,   198,   199,     0,     0,
       0,   200,   201,   202,   203,   204,     0,   205,   206,   207,
     208,     0,     0,   209,   210,   211,   212,   213,   214,   215,
     216,   217,   218,   219,   220,   221,   222,   223,   224,   225,
     403,   404,   405,   406,   407,   408,   409,   410,   411,   412,
     413,   414,   415,   416,   417,   418,     0,   419,   420,     0,
       0,     0,     0,   421,   422,   423,   424,   425,   426,   427,
     428,   429,   430,   431,   432,   433,   434,   435,   436,   437,
     438,   439,   440,   441,   442,   443,   444,   445,   446,   447,
     448,   449,   226,   227,   228,   229,   230,   231,   232,     0,
     233,     0,     0,     0,     0,     0,     0,   234,   235,   236,
     237,   238,   239,   240,   241,   242,   243,   244,   245,   246,
     247,   248,   249,   250,   251,   252,   253,   254,   255,   256,
     257,   258,   259,   260,   261,   262,   263,   264,   265,   266,
     267,   268,   269,   270,   271,   272,   273,   274,   275,   276,
     277,   278,   279,   280,   281,   282,   283,   284,   285,   286,
     287,   288,   289,     1,    79,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,    80,     0,     0,     0,     0,
       0,     0,     0,     0,     0,    81,     0,     0,     0,     0,
      82,     0,     0,    83,    84,    85,     0,     0,     0,    86,
      87,     0,     0,     0,     0,    88,     0,    89,    90,    91,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,    13,     0,     0,    92,     0,     0,
       0,     0,     0,     0,     0,     0,    93,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,    94,     0,     0,
       0,     0,    95,    96,    97,     0,     0,     0,    98,    99,
       0,     0,     0,   100,     0,   101,   102,   103,   104,   105,
     106,   107,   108,   109,   110,   111,     0,     0,     0,     0,
       0,   112,     0,   113,   114,   115,     0,     0,   116,   117,
     118,   119,   120,   121,   122,   123,   124,   125,   126,   127,
     128,   129,   130,     0,   131,     0,   132,   133,   134,     0,
       0,     0,   135,     0,     0,     0,   136,   137,   138,   139,
     140,   141,   142,   143,   144,   145,   146,   147,   148,   149,
     150,   151,   152,   153,   154,   155,   156,   157,   158,   159,
     160,   161,   162,     0,   163,   164,   165,   166,   167,     0,
     168,   169,   170,   171,   172,   173,   174,   175,   176,   177,
     178,   179,     0,   180,   181,   182,   183,   184,   185,     0,
     186,   187,   188,   189,   190,   191,   192,   193,   194,   195,
     196,   197,   198,   199,     0,     0,     0,   200,   201,   202,
     203,   204,     0,   205,   206,   207,   208,     0,     0,   209,
     210,   211,   212,   213,   214,   215,   216,   217,   218,   219,
     220,   221,   222,   223,   224,   225,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,   226,   227,
     228,   229,   230,   231,   232,     0,   233,     0,     0,     0,
       0,     0,     0,   234,   235,   236,   237,   238,   239,   240,
     241,   242,   243,   244,   245,   246,   247,   248,   249,   250,
     251,   252,   253,   254,   255,   256,   257,   258,   259,   260,
     261,   262,   263,   264,   265,   266,   267,   268,   269,   270,
     271,   272,   273,   274,   275,   276,   277,   278,   279,   280,
     281,   282,   283,   284,   285,   286,   287,   288,   289,     1,
      79,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,    80,     0,     0,     0,     0,     0,     0,     0,     0,
       0,    81,     0,     0,     0,     0,    82,     0,     0,    83,
      84,    85,     0,     0,     0,    86,    87,     0,     0,     0,
       0,    88,     0,    89,    90,    91,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
      13,     0,     0,   515,     0,     0,     0,     0,     0,     0,
       0,     0,    93,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,    94,     0,     0,     0,     0,    95,    96,
      97,     0,     0,     0,    98,    99,     0,     0,     0,   100,
       0,   101,   102,   103,   104,   105,   106,   107,   108,   109,
     110,   111,     0,     0,     0,     0,     0,   112,     0,   113,
     114,   115,     0,     0,   116,   117,   118,   119,   120,   121,
     122,   123,   124,   125,   126,   127,   128,   129,   130,     0,
     131,     0,   132,   133,   134,     0,     0,     0,   135,     0,
       0,     0,   136,   137,   138,   139,   140,   141,   142,   143,
     144,   145,   146,   147,   148,   149,   150,   151,   152,   153,
     154,   155,   156,   157,   158,   159,   160,   161,   162,     0,
     163,   164,   165,   166,   167,     0,   168,   169,   170,   171,
     172,   173,   174,   175,   176,   177,   178,   179,     0,   180,
     181,   182,   183,   184,   185,     0,   186,   187,   188,   189,
     190,   191,   192,   193,   194,   195,   196,   197,   198,   199,
       0,     0,     0,   200,   201,   202,   203,   204,     0,   205,
     206,   207,   208,     0,     0,   209,   210,   211,   212,   213,
     214,   215,   216,   217,   218,   219,   220,   221,   222,   223,
     224,   225,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,   226,   227,   228,   229,   230,   231,
     232,     0,   233,     0,     0,     0,     0,     0,     0,   234,
     235,   236,   237,   238,   239,   240,   241,   242,   243,   244,
     245,   246,   247,   248,   249,   250,   251,   252,   253,   254,
     255,   256,   257,   258,   259,   260,   261,   262,   263,   264,
     265,   266,   267,   268,   269,   270,   271,   272,   273,   274,
     275,   276,   277,   278,   279,   280,   281,   282,   283,   284,
     285,   286,   287,   288,   289,    79,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,    80,     0,     0,     0,
       0,     0,   372,     0,     0,     0,    81,   373,     0,     0,
       0,    82,     0,     0,    83,    84,    85,     0,     0,     0,
      86,    87,     0,     0,     0,   374,    88,     0,    89,    90,
      91,     0,     0,     0,     0,     0,     0,     0,     0,   375,
     376,     0,     0,     0,  1247,    13,     0,     0,   377,   378,
     379,   380,   381,   382,   383,     0,     0,    93,   384,   385,
     386,   387,  1248,     0,     0,   388,   389,     0,     0,     0,
       0,     0,   390,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,   391,
     392,     0,     0,     0,   394,     0,     0,   395,     0,     0,
     396,   397,   398,   399,     0,     0,     0,     0,    94,     0,
       0,     0,     0,    95,    96,    97,   400,     0,     0,    98,
      99,     0,     0,   401,   100,     0,   101,   102,   103,   104,
     105,   106,   107,   108,   109,   110,   111,   402,     0,     0,
       0,     0,   112,     0,   113,   114,   115,     0,     0,   116,
     117,   118,   119,   120,   121,   122,   123,   124,   125,   126,
     127,   128,   129,   130,     0,   131,     0,   132,   133,   134,
       0,     0,     0,   135,     0,     0,     0,   136,   137,   138,
     139,   140,   141,   142,   143,   144,   145,   146,   147,   148,
     149,   150,   151,   152,   153,   154,   155,   156,   157,   158,
     159,   160,   161,   162,     0,   163,   164,   165,   166,   167,
       0,   168,   169,   170,   171,   172,   173,   174,   175,   176,
     177,   178,   179,     0,   180,   181,   182,   183,   184,   185,
       0,   186,   187,   188,   189,   190,   191,   192,   193,   194,
     195,   196,   197,   198,   199,     0,     0,     0,   200,   201,
     202,   203,   204,     0,   205,   206,   207,   208,     0,     0,
     209,   210,   211,   212,   213,   214,   215,   216,   217,   218,
     219,   220,   221,   222,   223,   224,   225,   403,   404,   405,
     406,   407,   408,   409,   410,   411,   412,   413,   414,   415,
     416,   417,   418,     0,   419,   420,     0,     0,     0,     0,
     421,   422,   423,   424,   425,   426,   427,   428,   429,   430,
     431,   432,   433,   434,   435,   436,   437,   438,   439,   440,
     441,   442,   443,   444,   445,   446,   447,   448,   449,   226,
     227,   228,   229,   230,   231,   232,     0,   233,     0,     0,
       0,     0,     0,     0,   234,   235,   236,   237,   238,   239,
     240,   241,   242,   243,   244,   245,   246,   247,   248,   249,
     250,   251,   252,   253,   254,   255,   256,   257,   258,   259,
     260,   261,   262,   263,   264,   265,   266,   267,   268,   269,
     270,   271,   272,   273,   274,   275,   276,   277,   278,   279,
     280,   281,   282,   283,   284,   285,   286,   287,   288,   289,
      79,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,    80,     0,     0,     0,     0,     0,   372,     0,     0,
       0,    81,   373,     0,     0,     0,    82,     0,     0,    83,
      84,    85,     0,     0,     0,    86,    87,     0,     0,     0,
     374,    88,     0,    89,    90,    91,     0,     0,     0,     0,
       0,     0,     0,     0,   375,   376,     0,     0,     0,     0,
      13,     0,     0,   377,   378,   379,   380,   381,   382,   383,
       0,     0,    93,   384,   385,   386,   387,     0,     0,     0,
     388,   389,     0,     0,     0,     0,     0,   390,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,   391,   392,   393,     0,     0,   394,
       0,     0,   395,     0,     0,   396,   397,   398,   399,     0,
       0,     0,     0,    94,     0,     0,     0,     0,    95,    96,
      97,   400,     0,     0,    98,    99,     0,     0,   401,   100,
       0,   101,   102,   103,   104,   105,   106,   107,   108,   109,
     110,   111,   402,     0,     0,     0,     0,   112,     0,   113,
     114,   115,     0,     0,   116,   117,   118,   119,   120,   121,
     122,   123,   124,   125,   126,   127,   128,   129,   130,     0,
     131,     0,   132,   133,   134,     0,     0,     0,   135,     0,
       0,     0,   136,   137,   138,   139,   140,   141,   142,   143,
     144,   145,   146,   147,   148,   149,   150,   151,   152,   153,
     154,   155,   156,   157,   158,   159,   160,   161,   162,     0,
     163,   164,   165,   166,   167,     0,   168,   169,   170,   171,
     172,   173,   174,   175,   176,   177,   178,   179,     0,   180,
     181,   182,   183,   184,   185,     0,   186,   187,   188,   189,
     190,   191,   192,   193,   194,   195,   196,   197,   198,   199,
       0,     0,     0,   200,   201,   202,   203,   204,     0,   205,
     206,   207,   208,     0,     0,   209,   210,   211,   212,   213,
     214,   215,   216,   217,   218,   219,   220,   221,   222,   223,
     224,   225,   403,   404,   405,   406,   407,   408,   409,   410,
     411,   412,   413,   414,   415,   416,   417,   418,     0,   419,
     420,     0,     0,     0,     0,   421,   422,   423,   424,   425,
     426,   427,   428,   429,   430,   431,   432,   433,   434,   435,
     436,   437,   438,   439,   440,   441,   442,   443,   444,   445,
     446,   447,   448,   449,   226,   227,   228,   229,   230,   231,
     232,     0,   233,     0,     0,     0,     0,     0,     0,   234,
     235,   236,   237,   238,   239,   240,   241,   242,   243,   244,
     245,   246,   247,   248,   249,   250,   251,   252,   253,   254,
     255,   256,   257,   258,   259,   260,   261,   262,   263,   264,
     265,   266,   267,   268,   269,   270,   271,   272,   273,   274,
     275,   276,   277,   278,   279,   280,   281,   282,   283,   284,
     285,   286,   287,   288,   289,    79,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,    80,     0,     0,     0,
       0,     0,   372,     0,     0,     0,    81,   373,     0,     0,
       0,    82,     0,     0,    83,    84,    85,     0,     0,     0,
      86,    87,     0,     0,     0,   374,    88,     0,    89,    90,
      91,     0,     0,     0,     0,     0,     0,     0,     0,   375,
     376,     0,     0,     0,     0,    13,     0,   898,   377,   378,
     379,   380,   381,   382,   383,     0,     0,    93,   384,   385,
     386,   387,     0,     0,     0,   388,   389,     0,     0,     0,
       0,     0,   390,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,   391,
     392,     0,     0,     0,   394,     0,     0,   395,     0,     0,
     396,   397,   398,   399,     0,     0,     0,     0,    94,     0,
       0,     0,     0,    95,    96,    97,   400,     0,     0,    98,
      99,     0,     0,   401,   100,     0,   101,   102,   103,   104,
     105,   106,   107,   108,   109,   110,   111,   402,     0,     0,
       0,     0,   112,     0,   113,   114,   115,     0,     0,   116,
     117,   118,   119,   120,   121,   122,   123,   124,   125,   126,
     127,   128,   129,   130,     0,   131,     0,   132,   133,   134,
       0,     0,     0,   135,     0,     0,     0,   136,   137,   138,
     139,   140,   141,   142,   143,   144,   145,   146,   147,   148,
     149,   150,   151,   152,   153,   154,   155,   156,   157,   158,
     159,   160,   161,   162,     0,   163,   164,   165,   166,   167,
       0,   168,   169,   170,   171,   172,   173,   174,   175,   176,
     177,   178,   179,     0,   180,   181,   182,   183,   184,   185,
       0,   186,   187,   188,   189,   190,   191,   192,   193,   194,
     195,   196,   197,   198,   199,     0,     0,     0,   200,   201,
     202,   203,   204,     0,   205,   206,   207,   208,     0,     0,
     209,   210,   211,   212,   213,   214,   215,   216,   217,   218,
     219,   220,   221,   222,   223,   224,   225,   403,   404,   405,
     406,   407,   408,   409,   410,   411,   412,   413,   414,   415,
     416,   417,   418,     0,   419,   420,     0,     0,     0,     0,
     421,   422,   423,   424,   425,   426,   427,   428,   429,   430,
     431,   432,   433,   434,   435,   436,   437,   438,   439,   440,
     441,   442,   443,   444,   445,   446,   447,   448,   449,   226,
     227,   228,   229,   230,   231,   232,     0,   233,     0,     0,
       0,     0,     0,     0,   234,   235,   236,   237,   238,   239,
     240,   241,   242,   243,   244,   245,   246,   247,   248,   249,
     250,   251,   252,   253,   254,   255,   256,   257,   258,   259,
     260,   261,   262,   263,   264,   265,   266,   267,   268,   269,
     270,   271,   272,   273,   274,   275,   276,   277,   278,   279,
     280,   281,   282,   283,   284,   285,   286,   287,   288,   289,
      79,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,    80,     0,     0,     0,     0,     0,   372,     0,     0,
       0,    81,   373,     0,     0,     0,    82,     0,     0,    83,
      84,    85,     0,     0,     0,    86,    87,     0,     0,     0,
     374,    88,     0,    89,    90,    91,     0,     0,     0,     0,
       0,     0,     0,     0,   375,   376,     0,     0,     0,     0,
      13,     0,     0,   377,   378,  1119,   380,   381,   382,   383,
       0,     0,    93,   384,   385,   386,   387,     0,     0,     0,
     388,   389,     0,     0,     0,     0,     0,   390,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,   391,   392,   393,     0,     0,   394,
       0,     0,   395,     0,     0,   396,   397,   398,   399,     0,
       0,     0,     0,    94,     0,     0,     0,     0,    95,    96,
      97,   400,     0,     0,    98,    99,     0,     0,   401,   100,
       0,   101,   102,   103,   104,   105,   106,   107,   108,   109,
     110,   111,   402,     0,     0,     0,     0,   112,     0,   113,
     114,   115,     0,     0,   116,   117,   118,   119,   120,   121,
     122,   123,   124,   125,   126,   127,   128,   129,   130,     0,
     131,     0,   132,   133,   134,     0,     0,     0,   135,     0,
       0,     0,   136,   137,   138,   139,   140,   141,   142,   143,
     144,   145,   146,   147,   148,   149,   150,   151,   152,   153,
     154,   155,   156,   157,   158,   159,   160,   161,   162,     0,
     163,   164,   165,   166,   167,     0,   168,   169,   170,   171,
     172,   173,   174,   175,   176,   177,   178,   179,     0,   180,
     181,   182,   183,   184,   185,     0,   186,   187,   188,   189,
     190,   191,   192,   193,   194,   195,   196,   197,   198,   199,
       0,     0,     0,   200,   201,   202,   203,   204,     0,   205,
     206,   207,   208,     0,     0,   209,   210,   211,   212,   213,
     214,   215,   216,   217,   218,   219,   220,   221,   222,   223,
     224,   225,   403,   404,   405,   406,   407,   408,   409,   410,
     411,   412,   413,   414,   415,   416,   417,   418,     0,   419,
     420,     0,     0,     0,     0,   421,   422,   423,   424,   425,
     426,   427,   428,   429,   430,   431,   432,   433,   434,   435,
     436,   437,   438,   439,   440,   441,   442,   443,   444,   445,
     446,   447,   448,   449,   226,   227,   228,   229,   230,   231,
     232,     0,   233,     0,     0,     0,     0,     0,     0,   234,
     235,   236,   237,   238,   239,   240,   241,   242,   243,   244,
     245,   246,   247,   248,   249,   250,   251,   252,   253,   254,
     255,   256,   257,   258,   259,   260,   261,   262,   263,   264,
     265,   266,   267,   268,   269,   270,   271,   272,   273,   274,
     275,   276,   277,   278,   279,   280,   281,   282,   283,   284,
     285,   286,   287,   288,   289,    79,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,    80,     0,     0,     0,
       0,     0,   372,     0,     0,     0,    81,   373,     0,     0,
       0,    82,     0,     0,    83,    84,    85,     0,     0,     0,
      86,    87,     0,     0,     0,   374,    88,     0,    89,    90,
      91,     0,     0,     0,     0,     0,     0,     0,     0,   375,
     376,     0,     0,     0,     0,    13,     0,     0,   377,   378,
    1122,   380,   381,   382,   383,     0,     0,    93,   384,   385,
     386,   387,     0,     0,     0,   388,   389,     0,     0,     0,
       0,     0,   390,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,   391,
     392,   393,     0,     0,   394,     0,     0,   395,     0,     0,
     396,   397,   398,   399,     0,     0,     0,     0,    94,     0,
       0,     0,     0,    95,    96,    97,   400,     0,     0,    98,
      99,     0,     0,   401,   100,     0,   101,   102,   103,   104,
     105,   106,   107,   108,   109,   110,   111,   402,     0,     0,
       0,     0,   112,     0,   113,   114,   115,     0,     0,   116,
     117,   118,   119,   120,   121,   122,   123,   124,   125,   126,
     127,   128,   129,   130,     0,   131,     0,   132,   133,   134,
       0,     0,     0,   135,     0,     0,     0,   136,   137,   138,
     139,   140,   141,   142,   143,   144,   145,   146,   147,   148,
     149,   150,   151,   152,   153,   154,   155,   156,   157,   158,
     159,   160,   161,   162,     0,   163,   164,   165,   166,   167,
       0,   168,   169,   170,   171,   172,   173,   174,   175,   176,
     177,   178,   179,     0,   180,   181,   182,   183,   184,   185,
       0,   186,   187,   188,   189,   190,   191,   192,   193,   194,
     195,   196,   197,   198,   199,     0,     0,     0,   200,   201,
     202,   203,   204,     0,   205,   206,   207,   208,     0,     0,
     209,   210,   211,   212,   213,   214,   215,   216,   217,   218,
     219,   220,   221,   222,   223,   224,   225,   403,   404,   405,
     406,   407,   408,   409,   410,   411,   412,   413,   414,   415,
     416,   417,   418,     0,   419,   420,     0,     0,     0,     0,
     421,   422,   423,   424,   425,   426,   427,   428,   429,   430,
     431,   432,   433,   434,   435,   436,   437,   438,   439,   440,
     441,   442,   443,   444,   445,   446,   447,   448,   449,   226,
     227,   228,   229,   230,   231,   232,     0,   233,     0,     0,
       0,     0,     0,     0,   234,   235,   236,   237,   238,   239,
     240,   241,   242,   243,   244,   245,   246,   247,   248,   249,
     250,   251,   252,   253,   254,   255,   256,   257,   258,   259,
     260,   261,   262,   263,   264,   265,   266,   267,   268,   269,
     270,   271,   272,   273,   274,   275,   276,   277,   278,   279,
     280,   281,   282,   283,   284,   285,   286,   287,   288,   289,
      79,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,    80,     0,     0,     0,     0,     0,   372,     0,     0,
       0,    81,   373,     0,     0,     0,    82,     0,     0,    83,
      84,    85,     0,     0,     0,    86,    87,     0,     0,     0,
     374,    88,     0,    89,    90,    91,     0,     0,     0,     0,
       0,     0,     0,     0,   375,   376,     0,     0,     0,     0,
      13,     0,   723,   377,   378,   379,   380,   381,   382,   383,
       0,     0,    93,   384,   385,   386,   387,     0,     0,     0,
     388,   389,     0,     0,     0,     0,     0,   390,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,   391,   392,     0,     0,     0,   394,
       0,     0,   395,     0,     0,   396,   397,   398,   399,     0,
       0,     0,     0,    94,     0,     0,     0,     0,    95,    96,
      97,   400,     0,     0,    98,    99,     0,     0,   401,   100,
       0,   101,   102,   103,   104,   105,   106,   107,   108,   109,
     110,   111,   402,     0,     0,     0,     0,   112,     0,   113,
     114,   115,     0,     0,   116,   117,   118,   119,   120,   121,
     122,   123,   124,   125,   126,   127,   128,   129,   130,     0,
     131,     0,   132,   133,   134,     0,     0,     0,   135,     0,
       0,     0,   136,   137,   138,   139,   140,   141,   142,   143,
     144,   145,   146,   147,   148,   149,   150,   151,   152,   153,
     154,   155,   156,   157,   158,   159,   160,   161,   162,     0,
     163,   164,   165,   166,   167,     0,   168,   169,   170,   171,
     172,   173,   174,   175,   176,   177,   178,   179,     0,   180,
     181,   182,   183,   184,   185,     0,   186,   187,   188,   189,
     190,   191,   192,   193,   194,   195,   196,   197,   198,   199,
       0,     0,     0,   200,   201,   202,   203,   204,     0,   205,
     206,   207,   208,     0,     0,   209,   210,   211,   212,   213,
     214,   215,   216,   217,   218,   219,   220,   221,   222,   223,
     224,   225,   403,   404,   405,   406,   407,   408,   409,   410,
     411,   412,   413,   414,   415,   416,   417,   418,     0,   419,
     420,     0,     0,     0,     0,   421,   422,   423,   424,   425,
     426,   427,   428,   429,   430,   431,   432,   433,   434,   435,
     436,   437,   438,   439,   440,   441,   442,   443,   444,   445,
     446,   447,   448,   449,   226,   227,   228,   229,   230,   231,
     232,     0,   233,     0,     0,     0,     0,     0,     0,   234,
     235,   236,   237,   238,   239,   240,   241,   242,   243,   244,
     245,   246,   247,   248,   249,   250,   251,   252,   253,   254,
     255,   256,   257,   258,   259,   260,   261,   262,   263,   264,
     265,   266,   267,   268,   269,   270,   271,   272,   273,   274,
     275,   276,   277,   278,   279,   280,   281,   282,   283,   284,
     285,   286,   287,   288,   289,    79,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,    80,     0,     0,     0,
       0,     0,   372,     0,     0,     0,    81,   373,     0,     0,
       0,    82,     0,     0,    83,    84,    85,     0,     0,     0,
      86,    87,     0,     0,     0,   374,    88,     0,    89,    90,
      91,     0,     0,     0,     0,     0,     0,     0,     0,   375,
     376,     0,     0,     0,     0,    13,     0,     0,   377,   378,
     379,   380,   381,   382,   383,     0,     0,    93,   384,   385,
     386,   387,     0,     0,     0,   388,   389,     0,     0,     0,
       0,     0,   390,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,   391,
     392,     0,     0,     0,   394,     0,     0,   395,     0,     0,
     396,   397,   398,   399,     0,     0,     0,     0,    94,     0,
       0,     0,     0,    95,    96,    97,   400,     0,     0,    98,
      99,     0,     0,   401,   100,     0,   101,   102,   103,   104,
     105,   106,   107,   108,   109,   110,   111,   402,     0,     0,
       0,     0,   112,     0,   113,   114,   115,     0,     0,   116,
     117,   118,   119,   120,   121,   122,   123,   124,   125,   126,
     127,   128,   129,   130,     0,   131,     0,   132,   133,   134,
       0,     0,     0,   135,     0,     0,     0,   136,   137,   138,
     139,   140,   141,   142,   143,   144,   145,   146,   147,   148,
     149,   150,   151,   152,   153,   154,   155,   156,   157,   158,
     159,   160,   161,   162,     0,   163,   164,   165,   166,   167,
       0,   168,   169,   170,   171,   172,   173,   174,   175,   176,
     177,   178,   179,     0,   180,   181,   182,   183,   184,   185,
       0,   186,   187,   188,   189,   190,   191,   192,   193,   194,
     195,   196,   197,   198,   199,     0,     0,     0,   200,   201,
     202,   203,   204,     0,   205,   206,   207,   208,     0,     0,
     209,   210,   211,   212,   213,   214,   215,   216,   217,   218,
     219,   220,   221,   222,   223,   224,   225,   403,   404,   405,
     406,   407,   408,   409,   410,   411,   412,   413,   414,   415,
     416,   417,   418,     0,   419,   420,     0,     0,     0,     0,
     421,   422,   423,   424,   425,   426,   427,   428,   429,   430,
     431,   432,   433,   434,   435,   436,   437,   438,   439,   440,
     441,   442,   443,   444,   445,   446,   447,   448,   449,   226,
     227,   228,   229,   230,   231,   232,     0,   233,     0,     0,
       0,     0,     0,     0,   234,   235,   236,   237,   238,   239,
     240,   241,   242,   243,   244,   245,   246,   247,   248,   249,
     250,   251,   252,   253,   254,   255,   256,   257,   258,   259,
     260,   261,   262,   263,   264,   265,   266,   267,   268,   269,
     270,   271,   272,   273,   274,   275,   276,   277,   278,   279,
     280,   281,   282,   283,   284,   285,   286,   287,   288,   289,
      79,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,    80,     0,     0,     0,     0,     0,   372,     0,     0,
       0,    81,   373,     0,     0,     0,    82,     0,     0,    83,
      84,    85,     0,     0,     0,    86,    87,     0,     0,     0,
     374,    88,     0,    89,    90,    91,     0,     0,     0,     0,
       0,     0,     0,     0,   375,   376,     0,     0,     0,     0,
      13,     0,     0,   377,   378,   379,   380,   381,   382,   383,
       0,     0,    93,   384,   385,   386,   387,     0,     0,     0,
     388,   389,     0,     0,     0,     0,     0,   390,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,   391,   392,     0,     0,     0,   394,
       0,     0,   395,     0,     0,   396,   397,   398,   399,     0,
       0,     0,     0,    94,     0,     0,     0,     0,    95,    96,
      97,   400,     0,     0,    98,    99,     0,     0,   401,   100,
       0,   101,   102,   103,   104,   105,   106,   107,   108,   109,
     110,   111,   402,     0,     0,     0,     0,   112,     0,   113,
     114,   115,     0,     0,   116,   117,   118,   119,   120,   121,
     122,   123,   124,   125,   126,   127,   128,   129,   130,     0,
     131,     0,   132,   133,   134,     0,     0,     0,   135,     0,
       0,     0,   136,   137,   138,   139,   140,   141,   142,   143,
     144,   145,   146,   147,   148,   149,   150,   151,   152,   153,
     154,   155,   156,   157,   158,   159,   160,   161,   162,     0,
     163,   164,   165,   166,   167,     0,   168,   169,   170,   171,
     172,   173,   174,   175,   176,   177,   178,   179,     0,   180,
     181,   182,   183,   184,   185,     0,   186,   187,   188,   189,
     190,   191,   192,   193,   194,   195,   196,   197,   198,   199,
       0,     0,     0,   200,   201,   202,   203,   204,     0,   205,
     206,   207,   208,     0,     0,   209,   210,   211,   212,   213,
     214,   215,   216,   217,   218,   219,   220,   221,   222,   223,
     224,   225,   403,   404,   405,   406,   407,   408,   409,   410,
     411,   412,   413,   414,   415,   416,   417,   418,     0,   419,
     420,     0,     0,     0,     0,   991,   992,   993,   994,   995,
     996,   997,   998,   999,  1000,  1001,  1002,  1003,  1004,  1005,
    1006,  1007,  1008,  1009,  1010,  1011,  1012,  1013,  1014,  1015,
    1016,  1017,  1018,   449,   226,   227,   228,   229,   230,   231,
     232,     0,   233,     0,     0,     0,     0,     0,     0,   234,
     235,   236,   237,   238,   239,   240,   241,   242,   243,   244,
     245,   246,   247,   248,   249,   250,   251,   252,   253,   254,
     255,   256,   257,   258,   259,   260,   261,   262,   263,   264,
     265,   266,   267,   268,   269,   270,   271,   272,   273,   274,
     275,   276,   277,   278,   279,   280,   281,   282,   283,   284,
     285,   286,   287,   288,   289,    79,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,    80,     0,     0,     0,
       0,     0,     0,     0,     0,     0,    81,     0,     0,     0,
       0,    82,     0,     0,    83,    84,    85,     0,     0,     0,
      86,    87,     0,     0,     0,   374,    88,     0,    89,    90,
      91,     0,     0,     0,     0,     0,     0,     0,     0,   375,
     376,     0,     0,     0,     0,    13,     0,   723,   377,   378,
     379,   380,   381,   382,   383,     0,     0,    93,   384,   385,
     386,   387,     0,     0,     0,   388,     0,     0,     0,     0,
       0,     0,   390,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,   391,
     392,     0,     0,     0,   394,     0,     0,   395,     0,     0,
     396,   397,   398,   399,     0,     0,     0,     0,    94,     0,
       0,     0,     0,    95,    96,    97,   400,     0,     0,    98,
      99,     0,     0,   401,   100,     0,   101,   102,   103,   104,
     105,   106,   107,   108,   109,   110,   111,   402,     0,     0,
       0,     0,   112,     0,   113,   114,   115,     0,     0,   116,
     117,   118,   119,   120,   121,   122,   123,   124,   125,   126,
     127,   128,   129,   130,     0,   131,     0,   132,   133,   134,
       0,     0,     0,   135,     0,     0,     0,   136,   137,   138,
     139,   140,   141,   142,   143,   144,   145,   146,   147,   148,
     149,   150,   151,   152,   153,   154,   155,   156,   157,   158,
     159,   160,   161,   162,     0,   163,   164,   165,   166,   167,
       0,   168,   169,   170,   171,   172,   173,   174,   175,   176,
     177,   178,   179,     0,   180,   181,   182,   183,   184,   185,
       0,   186,   187,   188,   189,   190,   191,   192,   193,   194,
     195,   196,   197,   198,   199,     0,     0,     0,   200,   201,
     202,   203,   204,     0,   205,   206,   207,   208,     0,     0,
     209,   210,   211,   212,   213,   214,   215,   216,   217,   218,
     219,   220,   221,   222,   223,   224,   225,   403,   404,   405,
     406,   407,   408,   409,   410,   411,   412,   413,   414,   415,
     416,   417,   418,     0,   419,   420,     0,     0,     0,     0,
     421,   422,   423,   424,   425,   426,   427,   428,   429,   430,
     431,   432,   433,   434,   435,   436,   437,   438,   439,   440,
     441,   442,   443,   444,   445,   446,   447,   448,   449,   226,
     227,   228,   229,   230,   231,   232,     0,   233,     0,     0,
       0,     0,     0,     0,   234,   235,   236,   237,   238,   239,
     240,   241,   242,   243,   244,   245,   246,   247,   248,   249,
     250,   251,   252,   253,   254,   255,   256,   257,   258,   259,
     260,   261,   262,   263,   264,   265,   266,   267,   268,   269,
     270,   271,   272,   273,   274,   275,   276,   277,   278,   279,
     280,   281,   282,   283,   284,   285,   286,   287,   288,   289,
      79,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,    80,     0,     0,     0,     0,     0,     0,     0,     0,
       0,    81,     0,     0,     0,     0,    82,     0,     0,    83,
      84,    85,     0,     0,     0,    86,    87,     0,     0,     0,
     374,    88,     0,    89,    90,    91,     0,     0,     0,     0,
       0,     0,     0,     0,   375,   376,     0,     0,     0,     0,
      13,     0,     0,   377,   378,   379,   380,   381,   382,   383,
       0,     0,    93,   384,   385,   386,   387,     0,     0,     0,
     388,     0,     0,     0,     0,     0,     0,   390,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,   391,   392,     0,     0,     0,   394,
       0,     0,   395,     0,     0,   396,   397,   398,   399,     0,
       0,     0,     0,    94,     0,     0,     0,     0,    95,    96,
      97,   400,     0,     0,    98,    99,     0,     0,   401,   100,
       0,   101,   102,   103,   104,   105,   106,   107,   108,   109,
     110,   111,   402,     0,     0,     0,     0,   112,     0,   113,
     114,   115,     0,  2186,   116,   117,   118,   119,   120,   121,
     122,   123,   124,   125,   126,   127,   128,   129,   130,     0,
     131,     0,   132,   133,   134,     0,     0,     0,   135,     0,
       0,     0,   136,   137,   138,   139,   140,   141,   142,   143,
     144,   145,   146,   147,   148,   149,   150,   151,   152,   153,
     154,   155,   156,   157,   158,   159,   160,   161,   162,     0,
     163,   164,   165,   166,   167,     0,   168,   169,   170,   171,
     172,   173,   174,   175,   176,   177,   178,   179,     0,   180,
     181,   182,   183,   184,   185,     0,   186,   187,   188,   189,
     190,   191,   192,   193,   194,   195,   196,   197,   198,   199,
       0,     0,     0,   200,   201,   202,   203,   204,     0,   205,
     206,   207,   208,     0,     0,   209,   210,   211,   212,   213,
     214,   215,   216,   217,   218,   219,   220,   221,   222,   223,
     224,   225,   403,   404,   405,   406,   407,   408,   409,   410,
     411,   412,   413,   414,   415,   416,   417,   418,     0,   419,
     420,     0,     0,     0,     0,   421,   422,   423,   424,   425,
     426,   427,   428,   429,   430,   431,   432,   433,   434,   435,
     436,   437,   438,   439,   440,   441,   442,   443,   444,   445,
     446,   447,   448,   449,   226,   227,   228,   229,   230,   231,
     232,     0,   233,     0,     0,     0,     0,     0,     0,   234,
     235,   236,   237,   238,   239,   240,   241,   242,   243,   244,
     245,   246,   247,   248,   249,   250,   251,   252,   253,   254,
     255,   256,   257,   258,   259,   260,   261,   262,   263,   264,
     265,   266,   267,   268,   269,   270,   271,   272,   273,   274,
     275,   276,   277,   278,   279,   280,   281,   282,   283,   284,
     285,   286,   287,   288,   289,    79,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,    80,     0,     0,     0,
       0,     0,     0,     0,     0,     0,    81,     0,     0,     0,
       0,    82,     0,     0,    83,    84,    85,     0,     0,     0,
      86,    87,     0,     0,     0,   374,    88,     0,    89,    90,
      91,     0,     0,     0,     0,     0,     0,     0,     0,   375,
     376,     0,     0,     0,     0,    13,     0,     0,   377,   378,
     379,   380,   381,   382,   383,     0,     0,    93,   384,   385,
     386,   387,     0,     0,     0,   388,     0,     0,     0,     0,
       0,     0,   390,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,   391,
     392,     0,     0,     0,   394,     0,     0,   395,     0,     0,
     396,   397,   398,   399,     0,     0,     0,     0,    94,     0,
       0,     0,     0,    95,    96,    97,   400,     0,     0,    98,
      99,     0,     0,   401,   100,     0,   101,   102,   103,   104,
     105,   106,   107,   108,   109,   110,   111,   402,     0,     0,
       0,     0,   112,     0,   113,   114,   115,     0,     0,   116,
     117,   118,   119,   120,   121,   122,   123,   124,   125,   126,
     127,   128,   129,   130,     0,   131,     0,   132,   133,   134,
       0,     0,     0,   135,     0,     0,     0,   136,   137,   138,
     139,   140,   141,   142,   143,   144,   145,   146,   147,   148,
     149,   150,   151,   152,   153,   154,   155,   156,   157,   158,
     159,   160,   161,   162,     0,   163,   164,   165,   166,   167,
       0,   168,   169,   170,   171,   172,   173,   174,   175,   176,
     177,   178,   179,     0,   180,   181,   182,   183,   184,   185,
       0,   186,   187,   188,   189,   190,   191,   192,   193,   194,
     195,   196,   197,   198,   199,     0,     0,     0,   200,   201,
     202,   203,   204,     0,   205,   206,   207,   208,     0,     0,
     209,   210,   211,   212,   213,   214,   215,   216,   217,   218,
     219,   220,   221,   222,   223,   224,   225,   403,   404,   405,
     406,   407,   408,   409,   410,   411,   412,   413,   414,   415,
     416,   417,   418,     0,   419,   420,     0,     0,     0,     0,
     421,   422,   423,   424,   425,   426,   427,   428,   429,   430,
     431,   432,   433,   434,   435,   436,   437,   438,   439,   440,
     441,   442,   443,   444,   445,   446,   447,   448,   449,   226,
     227,   228,   229,   230,   231,   232,     0,   233,     0,     0,
       0,     0,     0,     0,   234,   235,   236,   237,   238,   239,
     240,   241,   242,   243,   244,   245,   246,   247,   248,   249,
     250,   251,   252,   253,   254,   255,   256,   257,   258,   259,
     260,   261,   262,   263,   264,   265,   266,   267,   268,   269,
     270,   271,   272,   273,   274,   275,   276,   277,   278,   279,
     280,   281,   282,   283,   284,   285,   286,   287,   288,   289,
      79,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,    80,     0,     0,     0,     0,   809,     0,     0,     0,
       0,    81,     0,     0,     0,     0,    82,     0,     0,    83,
      84,    85,     0,     0,     0,    86,    87,     0,     0,     0,
       0,    88,     0,    89,    90,    91,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,   810,     0,   811,     0,     0,     0,     0,
       0,     0,    93,     0,     0,     0,     0,     0,   812,   813,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,   814,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,    94,     0,     0,     0,     0,    95,    96,
      97,     0,     0,     0,    98,    99,     0,     0,     0,   100,
       0,   101,   102,   103,   104,   105,   106,   107,   108,   109,
     110,   111,     0,     0,     0,     0,     0,   112,     0,   113,
     114,   115,     0,     0,   116,   117,   118,   119,   120,   121,
     122,   123,   124,   125,   126,   127,   128,   129,   130,     0,
     131,     0,   132,   133,   134,     0,     0,     0,   135,     0,
       0,     0,   136,   137,   138,   139,   140,   141,   142,   143,
     144,   145,   146,   147,   148,   149,   150,   151,   152,   153,
     154,   155,   156,   157,   158,   159,   160,   161,   162,     0,
     163,   164,   165,   166,   167,     0,   168,   169,   170,   171,
     172,   173,   174,   175,   176,   177,   178,   179,     0,   180,
     181,   182,   183,   184,   185,     0,   186,   187,   188,   189,
     190,   191,   192,   193,   194,   195,   196,   197,   198,   199,
       0,     0,     0,   200,   201,   202,   203,   204,     0,   205,
     206,   207,   208,     0,     0,   209,   210,   211,   212,   213,
     214,   215,   216,   217,   218,   219,   220,   221,   222,   223,
     224,   225,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,   815,   816,   817,   818,   819,
     820,   821,   822,   823,   824,   825,   826,   827,   828,   829,
     830,   831,   832,   833,   834,   835,   836,   837,   838,   839,
     840,   841,   842,     0,   226,   227,   228,   229,   230,   231,
     232,     0,   233,     0,     0,     0,     0,     0,     0,   234,
     235,   236,   237,   238,   239,   240,   241,   242,   243,   244,
     245,   246,   247,   248,   249,   250,   251,   252,   253,   254,
     255,   256,   257,   258,   259,   260,   261,   262,   263,   264,
     265,   266,   267,   268,   269,   270,   271,   272,   273,   274,
     275,   276,   277,   278,   279,   280,   281,   282,   283,   284,
     285,   286,   287,   288,   289,    79,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,    80,     0,     0,     0,
       0,  1217,     0,     0,     0,     0,    81,     0,     0,     0,
       0,    82,     0,     0,    83,    84,    85,     0,     0,     0,
      86,    87,     0,     0,     0,     0,    88,     0,    89,    90,
      91,     0,     0,  1218,  1219,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,    92,     0,
    1220,     0,     0,     0,     0,     0,     0,    93,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,    94,     0,
       0,     0,     0,    95,    96,    97,     0,     0,     0,    98,
      99,     0,  1221,     0,   100,     0,   101,   102,   103,   104,
     105,   106,   107,   108,   109,   110,   111,     0,     0,     0,
       0,     0,   112,     0,   113,   114,   115,     0,     0,  1222,
     117,   118,   119,   120,   121,   122,   123,   124,   125,   126,
     127,   128,   129,   130,     0,   131,     0,   132,   133,   134,
       0,     0,     0,   135,     0,     0,     0,   136,   137,   138,
     139,   140,   141,   142,   143,   144,   145,   146,   147,   148,
     149,   150,   151,   152,   153,   154,   155,   156,   157,   158,
     159,   160,   161,   162,     0,   163,   164,   165,   166,   167,
       0,   168,   169,   170,   171,   172,   173,   174,   175,   176,
     177,   178,   179,     0,   180,   181,   182,   183,   184,   185,
       0,   186,   187,   188,   189,   190,   191,   192,   193,   194,
     195,   196,   197,   198,   199,     0,     0,     0,   200,   201,
     202,   203,   204,     0,   205,   206,   207,   208,     0,     0,
     209,   210,   211,   212,   213,   214,   215,   216,   217,   218,
     219,   220,   221,   222,   223,   224,   225,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
     815,   816,   817,   818,   819,   820,   821,   822,   823,   824,
     825,   826,   827,   828,   829,   830,   831,   832,   833,   834,
     835,   836,   837,   838,   839,   840,   841,   842,     0,   226,
     227,   228,   229,   230,   231,   232,     0,   233,     0,     0,
       0,     0,     0,     0,   234,   235,   236,   237,   238,   239,
     240,   241,   242,   243,   244,   245,   246,   247,   248,   249,
     250,   251,   252,   253,   254,   255,   256,   257,   258,   259,
     260,   261,   262,   263,   264,   265,   266,   267,   268,   269,
     270,   271,   272,   273,   274,   275,   276,   277,   278,   279,
     280,   281,   282,   283,   284,   285,   286,   287,   288,   289,
      79,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,    80,     0,     0,     0,     0,  1217,     0,     0,     0,
       0,    81,     0,     0,     0,     0,    82,     0,     0,    83,
      84,    85,     0,     0,     0,    86,    87,     0,     0,     0,
       0,    88,     0,    89,    90,    91,     0,     0,  1218,  1219,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,    92,     0,  1220,     0,     0,     0,     0,
       0,     0,    93,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,    94,     0,     0,     0,     0,    95,    96,
      97,     0,     0,     0,    98,    99,     0,  1221,     0,   100,
       0,   101,   102,   103,   104,   105,   106,   107,   108,   109,
     110,   111,     0,     0,     0,     0,     0,   112,     0,   113,
     114,   115,     0,     0,   116,   117,   118,   119,   120,   121,
     122,   123,   124,   125,   126,   127,   128,   129,   130,     0,
     131,     0,   132,   133,   134,     0,     0,     0,   135,     0,
       0,     0,   136,   137,   138,   139,   140,   141,   142,   143,
     144,   145,   146,   147,   148,   149,   150,   151,   152,   153,
     154,   155,   156,   157,   158,   159,   160,   161,   162,     0,
     163,   164,   165,   166,   167,     0,   168,   169,   170,   171,
     172,   173,   174,   175,   176,   177,   178,   179,     0,   180,
     181,   182,   183,   184,   185,     0,   186,   187,   188,   189,
     190,   191,   192,   193,   194,   195,   196,   197,   198,   199,
       0,     0,     0,   200,   201,   202,   203,   204,     0,   205,
     206,   207,   208,     0,     0,   209,   210,   211,   212,   213,
     214,   215,   216,   217,   218,   219,   220,   221,   222,   223,
     224,   225,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,   815,   816,   817,   818,   819,
     820,   821,   822,   823,   824,   825,   826,   827,   828,   829,
     830,   831,   832,   833,   834,   835,   836,   837,   838,   839,
     840,   841,   842,     0,   226,   227,   228,   229,   230,   231,
     232,     0,   233,     0,     0,     0,     0,     0,     0,   234,
     235,   236,   237,   238,   239,   240,   241,   242,   243,   244,
     245,   246,   247,   248,   249,   250,   251,   252,   253,   254,
     255,   256,   257,   258,   259,   260,   261,   262,   263,   264,
     265,   266,   267,   268,   269,   270,   271,   272,   273,   274,
     275,   276,   277,   278,   279,   280,   281,   282,   283,   284,
     285,   286,   287,   288,   289,    79,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,    80,     0,     0,     0,
       0,     0,     0,     0,     0,     0,    81,     0,     0,     0,
       0,    82,  1479,     0,    83,    84,    85,     0,     0,     0,
      86,    87,     0,     0,     0,     0,    88,     0,    89,    90,
      91,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,    92,     0,
    1220,     0,     0,     0,     0,     0,     0,    93,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,    94,     0,
       0,     0,     0,    95,    96,    97,     0,     0,     0,    98,
      99,     0,     0,     0,   100,     0,   101,   102,   103,   104,
     105,   106,   107,   108,   109,   110,   111,     0,     0,     0,
       0,     0,   112,     0,   113,   114,   115,     0,     0,   116,
     117,   118,   119,   120,   121,   122,   123,   124,   125,   126,
     127,   128,   129,   130,     0,   131,     0,   132,   133,   134,
       0,     0,     0,   135,     0,     0,     0,   136,   137,   138,
     139,   140,   141,   142,   143,   144,   145,   146,   147,   148,
     149,   150,   151,   152,   153,   154,   155,   156,   157,   158,
     159,   160,   161,   162,     0,   163,   164,   165,   166,   167,
       0,   168,   169,   170,   171,   172,   173,   174,   175,   176,
     177,   178,   179,     0,   180,   181,   182,   183,   184,   185,
       0,   186,   187,   188,   189,   190,   191,   192,   193,   194,
     195,   196,   197,   198,   199,     0,     0,     0,   200,   201,
     202,   203,   204,     0,   205,   206,   207,   208,     0,     0,
     209,   210,   211,   212,   213,   214,   215,   216,   217,   218,
     219,   220,   221,   222,   223,   224,   225,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
     815,   816,   817,   818,   819,   820,   821,   822,   823,   824,
     825,   826,   827,   828,   829,   830,   831,   832,   833,   834,
     835,   836,   837,   838,   839,   840,   841,   842,     0,   226,
     227,   228,   229,   230,   231,   232,     0,   233,     0,     0,
       0,     0,     0,     0,   234,   235,   236,   237,   238,   239,
     240,   241,   242,   243,   244,   245,   246,   247,   248,   249,
     250,   251,   252,   253,   254,   255,   256,   257,   258,   259,
     260,   261,   262,   263,   264,   265,   266,   267,   268,   269,
     270,   271,   272,   273,   274,   275,   276,   277,   278,   279,
     280,   281,   282,   283,   284,   285,   286,   287,   288,   289,
      79,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,    80,     0,     0,     0,     0,     0,     0,     0,     0,
       0,    81,     0,     0,     0,     0,    82,     0,     0,    83,
      84,    85,     0,     0,     0,    86,    87,     0,     0,     0,
       0,    88,     0,    89,    90,    91,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,   810,     0,   811,     0,     0,     0,     0,
       0,     0,    93,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,    94,     0,     0,     0,     0,    95,    96,
      97,     0,     0,     0,    98,    99,     0,     0,     0,   100,
       0,   101,   102,   103,   104,   105,   106,   107,   108,   109,
     110,   111,     0,     0,     0,     0,     0,   112,     0,   113,
     114,   115,     0,     0,   116,   117,   118,   119,   120,   121,
     122,   123,   124,   125,   126,   127,   128,   129,   130,     0,
     131,     0,   132,   133,   134,     0,     0,     0,   135,     0,
       0,     0,   136,   137,   138,   139,   140,   141,   142,   143,
     144,   145,   146,   147,   148,   149,   150,   151,   152,   153,
     154,   155,   156,   157,   158,   159,   160,   161,   162,     0,
     163,   164,   165,   166,   167,     0,   168,   169,   170,   171,
     172,   173,   174,   175,   176,   177,   178,   179,     0,   180,
     181,   182,   183,   184,   185,     0,   186,   187,   188,   189,
     190,   191,   192,   193,   194,   195,   196,   197,   198,   199,
       0,     0,     0,   200,   201,   202,   203,   204,     0,   205,
     206,   207,   208,     0,     0,   209,   210,   211,   212,   213,
     214,   215,   216,   217,   218,   219,   220,   221,   222,   223,
     224,   225,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,   815,   816,   817,   818,   819,
     820,   821,   822,   823,   824,   825,   826,   827,   828,   829,
     830,   831,   832,   833,   834,   835,   836,   837,   838,   839,
     840,   841,   842,     0,   226,   227,   228,   229,   230,   231,
     232,     0,   233,     0,     0,     0,     0,     0,     0,   234,
     235,   236,   237,   238,   239,   240,   241,   242,   243,   244,
     245,   246,   247,   248,   249,   250,   251,   252,   253,   254,
     255,   256,   257,   258,   259,   260,   261,   262,   263,   264,
     265,   266,   267,   268,   269,   270,   271,   272,   273,   274,
     275,   276,   277,   278,   279,   280,   281,   282,   283,   284,
     285,   286,   287,   288,   289,    79,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,    80,     0,     0,     0,
       0,     0,     0,     0,     0,     0,    81,     0,     0,     0,
       0,    82,     0,     0,    83,    84,    85,     0,     0,     0,
      86,    87,     0,     0,     0,     0,    88,     0,    89,    90,
      91,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,    92,     0,
    1220,     0,     0,     0,     0,     0,     0,    93,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,    94,     0,
       0,     0,     0,    95,    96,    97,     0,     0,     0,    98,
      99,     0,     0,     0,   100,     0,   101,   102,   103,   104,
     105,   106,   107,   108,   109,   110,   111,     0,     0,     0,
       0,     0,   112,     0,   113,   114,   115,     0,     0,   116,
     117,   118,   119,   120,   121,   122,   123,   124,   125,   126,
     127,   128,   129,   130,     0,   131,     0,   132,   133,   134,
       0,     0,     0,   135,     0,     0,     0,   136,   137,   138,
     139,   140,   141,   142,   143,   144,   145,   146,   147,   148,
     149,   150,   151,   152,   153,   154,   155,   156,   157,   158,
     159,   160,   161,   162,     0,   163,   164,   165,   166,   167,
       0,   168,   169,   170,   171,   172,   173,   174,   175,   176,
     177,   178,   179,     0,   180,   181,   182,   183,   184,   185,
       0,   186,   187,   188,   189,   190,   191,   192,   193,   194,
     195,   196,   197,   198,   199,     0,     0,     0,   200,   201,
     202,   203,   204,     0,   205,   206,   207,   208,     0,     0,
     209,   210,   211,   212,   213,   214,   215,   216,   217,   218,
     219,   220,   221,   222,   223,   224,   225,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
     815,   816,   817,   818,   819,   820,   821,   822,   823,   824,
     825,   826,   827,   828,   829,   830,   831,   832,   833,   834,
     835,   836,   837,   838,   839,   840,   841,   842,     0,   226,
     227,   228,   229,   230,   231,   232,     0,   233,     0,     0,
       0,     0,     0,     0,   234,   235,   236,   237,   238,   239,
     240,   241,   242,   243,   244,   245,   246,   247,   248,   249,
     250,   251,   252,   253,   254,   255,   256,   257,   258,   259,
     260,   261,   262,   263,   264,   265,   266,   267,   268,   269,
     270,   271,   272,   273,   274,   275,   276,   277,   278,   279,
     280,   281,   282,   283,   284,   285,   286,   287,   288,   289,
      79,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,    80,     0,     0,     0,     0,     0,     0,     0,     0,
       0,    81,     0,     0,     0,     0,    82,     0,     0,    83,
      84,    85,     0,     0,     0,    86,    87,     0,     0,     0,
       0,    88,     0,    89,    90,    91,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,   515,     0,     0,     0,     0,     0,     0,
       0,     0,    93,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
     859,   860,   861,   862,   863,   864,   865,   866,   867,   868,
     869,   870,     0,     0,   871,     0,     0,     0,     0,     0,
       0,     0,     0,    94,     0,     0,     0,     0,    95,    96,
      97,     0,     0,     0,    98,    99,     0,     0,     0,   100,
       0,   101,   102,   103,   104,   105,   106,   107,   108,   109,
     110,   111,     0,     0,     0,     0,     0,   112,     0,   113,
     114,   115,     0,     0,   116,   117,   118,   119,   120,   121,
     122,   123,   124,   125,   126,   127,   128,   129,   130,     0,
     131,     0,   132,   133,   134,     0,     0,     0,   135,     0,
       0,     0,   136,   137,   138,   139,   140,   141,   142,   143,
     144,   145,   146,   147,   148,   149,   150,   151,   152,   153,
     154,   155,   156,   157,   158,   159,   160,   161,   162,     0,
     163,   164,   165,   166,   167,     0,   168,   169,   170,   171,
     172,   173,   174,   175,   176,   177,   178,   179,     0,   180,
     181,   182,   183,   184,   185,     0,   186,   187,   188,   189,
     190,   191,   192,   193,   194,   195,   196,   197,   198,   199,
       0,     0,     0,   200,   201,   202,   203,   204,     0,   205,
     206,   207,   208,     0,     0,   209,   210,   211,   212,   213,
     214,   215,   216,   217,   218,   219,   220,   221,   222,   223,
     224,   225,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,   226,   227,   228,   229,   230,   231,
     232,     0,   233,     0,     0,     0,     0,     0,     0,   234,
     235,   236,   237,   238,   239,   240,   241,   242,   243,   244,
     245,   246,   247,   248,   249,   250,   251,   252,   253,   254,
     255,   256,   257,   258,   259,   260,   261,   262,   263,   264,
     265,   266,   267,   268,   269,   270,   271,   272,   273,   274,
     275,   276,   277,   278,   279,   280,   281,   282,   283,   284,
     285,   286,   287,   288,   289,    79,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,    80,     0,     0,     0,
       0,     0,     0,     0,     0,     0,    81,   909,     0,     0,
       0,    82,     0,     0,    83,    84,    85,     0,     0,     0,
      86,    87,     0,     0,     0,     0,    88,     0,    89,    90,
      91,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,   515,     0,
     910,     0,     0,     0,     0,     0,     0,    93,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,    94,     0,
       0,     0,     0,    95,    96,    97,     0,     0,     0,    98,
      99,     0,     0,     0,   100,     0,   101,   102,   103,   104,
     105,   106,   107,   108,   109,   110,   111,     0,     0,     0,
       0,     0,   112,     0,   113,   114,   115,     0,     0,   116,
     117,   118,   119,   120,   121,   122,   123,   124,   125,   126,
     127,   128,   129,   130,     0,   131,     0,   132,   133,   134,
       0,     0,     0,   135,     0,     0,     0,   136,   137,   138,
     139,   140,   141,   142,   143,   144,   145,   146,   147,   148,
     149,   150,   151,   152,   153,   154,   155,   156,   157,   158,
     159,   160,   161,   162,     0,   163,   164,   165,   166,   167,
       0,   168,   169,   170,   171,   172,   173,   174,   175,   176,
     177,   178,   179,     0,   180,   181,   182,   183,   184,   185,
       0,   186,   187,   188,   189,   190,   191,   192,   193,   194,
     195,   196,   197,   198,   199,     0,     0,     0,   200,   201,
     202,   203,   204,     0,   205,   206,   207,   208,     0,     0,
     209,   210,   211,   212,   213,   214,   215,   216,   217,   218,
     219,   220,   221,   222,   223,   224,   225,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,   226,
     227,   228,   229,   230,   231,   232,     0,   233,     0,     0,
       0,     0,     0,     0,   234,   235,   236,   237,   238,   239,
     240,   241,   242,   243,   244,   245,   246,   247,   248,   249,
     250,   251,   252,   253,   254,   255,   256,   257,   258,   259,
     260,   261,   262,   263,   264,   265,   266,   267,   268,   269,
     270,   271,   272,   273,   274,   275,   276,   277,   278,   279,
     280,   281,   282,   283,   284,   285,   286,   287,   288,   289,
      79,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,    80,     0,     0,     0,     0,     0,     0,     0,     0,
       0,    81,     0,     0,     0,     0,    82,     0,     0,    83,
      84,    85,     0,     0,     0,    86,    87,     0,     0,     0,
       0,    88,     0,    89,    90,    91,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,    92,     0,     0,     0,     0,     0,     0,
       0,     0,    93,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,    94,     0,     0,     0,     0,    95,    96,
      97,     0,   345,     0,    98,    99,     0,     0,     0,   100,
       0,   101,   102,   103,   104,   105,   106,   107,   108,   109,
     110,   111,     0,     0,     0,     0,     0,   112,     0,   113,
     114,   115,     0,     0,   116,   117,   118,   119,   120,   121,
     122,   123,   124,   125,   126,   127,   128,   129,   130,     0,
     131,     0,   132,   133,   134,     0,     0,     0,   135,     0,
       0,     0,   136,   137,   138,   139,   140,   141,   142,   143,
     144,   145,   146,   147,   148,   149,   150,   151,   152,   153,
     154,   155,   156,   157,   158,   159,   160,   161,   162,     0,
     163,   164,   165,   166,   167,     0,   168,   169,   170,   171,
     172,   173,   174,   175,   176,   177,   178,   179,     0,   180,
     181,   182,   183,   184,   185,     0,   186,   187,   188,   189,
     190,   191,   192,   193,   194,   195,   196,   197,   198,   199,
       0,     0,     0,   200,   201,   202,   203,   204,     0,   205,
     206,   207,   208,     0,     0,   209,   210,   211,   212,   213,
     214,   215,   216,   217,   218,   219,   220,   221,   222,   223,
     224,   225,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,   226,   227,   228,   229,   230,   231,
     232,     0,   233,     0,     0,     0,     0,     0,     0,   234,
     235,   236,   237,   238,   239,   240,   241,   242,   243,   244,
     245,   246,   247,   248,   249,   250,   251,   252,   253,   254,
     255,   256,   257,   258,   259,   260,   261,   262,   263,   264,
     265,   266,   267,   268,   269,   270,   271,   272,   273,   274,
     275,   276,   277,   278,   279,   280,   281,   282,   283,   284,
     285,   286,   287,   288,   289,    79,     0,     0,     0,   471,
       0,     0,     0,     0,     0,     0,    80,     0,     0,     0,
       0,     0,     0,     0,     0,     0,    81,     0,     0,     0,
       0,    82,     0,     0,    83,    84,    85,     0,     0,     0,
      86,    87,     0,     0,     0,     0,    88,     0,    89,    90,
      91,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,    92,     0,
       0,     0,     0,     0,     0,     0,     0,    93,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,    94,     0,
       0,     0,     0,    95,    96,    97,     0,     0,     0,    98,
      99,     0,     0,     0,   100,     0,   101,   102,   103,   104,
     105,   106,   107,   108,   109,   110,   111,     0,     0,     0,
       0,     0,   112,     0,   113,   114,   115,     0,     0,   116,
     117,   118,   119,   120,   121,   122,   123,   124,   125,   126,
     127,   128,   129,   130,     0,   131,     0,   132,   133,   134,
       0,     0,     0,   135,     0,     0,     0,   136,   137,   138,
     139,   140,   141,   142,   143,   144,   145,   146,   147,   148,
     149,   150,   151,   152,   153,   154,   155,   156,   157,   158,
     159,   160,   161,   162,     0,   163,   164,   165,   166,   167,
       0,   168,   169,   170,   171,   172,   173,   174,   175,   176,
     177,   178,   179,     0,   180,   181,   182,   183,   184,   185,
       0,   186,   187,   188,   189,   190,   191,   192,   193,   194,
     195,   196,   197,   198,   199,     0,     0,     0,   200,   201,
     202,   203,   204,     0,   205,   206,   207,   208,     0,     0,
     209,   210,   211,   212,   213,   214,   215,   216,   217,   218,
     219,   220,   221,   222,   223,   224,   225,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,   226,
     227,   228,   229,   230,   231,   232,     0,   233,     0,     0,
       0,     0,     0,     0,   234,   235,   236,   237,   238,   239,
     240,   241,   242,   243,   244,   245,   246,   247,   248,   249,
     250,   251,   252,   253,   254,   255,   256,   257,   258,   259,
     260,   261,   262,   263,   264,   265,   266,   267,   268,   269,
     270,   271,   272,   273,   274,   275,   276,   277,   278,   279,
     280,   281,   282,   283,   284,   285,   286,   287,   288,   289,
      79,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,    80,     0,     0,     0,     0,     0,     0,     0,     0,
       0,    81,     0,     0,     0,     0,    82,     0,     0,    83,
      84,    85,     0,     0,     0,    86,    87,     0,     0,     0,
       0,    88,     0,    89,    90,    91,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
      13,     0,     0,    92,     0,     0,     0,     0,     0,     0,
       0,     0,    93,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,    94,     0,     0,     0,     0,    95,    96,
      97,     0,     0,     0,    98,    99,     0,     0,     0,   100,
       0,   101,   102,   103,   104,   105,   106,   107,   108,   109,
     110,   111,     0,     0,     0,     0,     0,   112,     0,   113,
     114,   115,     0,     0,   116,   117,   118,   119,   120,   121,
     122,   123,   124,   125,   126,   127,   128,   129,   130,     0,
     131,     0,   132,   133,   134,     0,     0,     0,   135,     0,
       0,     0,   136,   137,   138,   139,   140,   141,   142,   143,
     144,   145,   146,   147,   148,   149,   150,   151,   152,   153,
     154,   155,   156,   157,   158,   159,   160,   161,   162,     0,
     163,   164,   165,   166,   167,     0,   168,   169,   170,   171,
     172,   173,   174,   175,   176,   177,   178,   179,     0,   180,
     181,   182,   183,   184,   185,     0,   186,   187,   188,   189,
     190,   191,   192,   193,   194,   195,   196,   197,   198,   199,
       0,     0,     0,   200,   201,   202,   203,   204,     0,   205,
     206,   207,   208,     0,     0,   209,   210,   211,   212,   213,
     214,   215,   216,   217,   218,   219,   220,   221,   222,   223,
     224,   225,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,   226,   227,   228,   229,   230,   231,
     232,     0,   233,     0,     0,     0,     0,     0,     0,   234,
     235,   236,   237,   238,   239,   240,   241,   242,   243,   244,
     245,   246,   247,   248,   249,   250,   251,   252,   253,   254,
     255,   256,   257,   258,   259,   260,   261,   262,   263,   264,
     265,   266,   267,   268,   269,   270,   271,   272,   273,   274,
     275,   276,   277,   278,   279,   280,   281,   282,   283,   284,
     285,   286,   287,   288,   289,    79,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,    80,     0,     0,     0,
       0,     0,     0,     0,     0,  1030,    81,     0,     0,     0,
       0,    82,     0,     0,    83,    84,    85,     0,     0,     0,
      86,    87,     0,     0,     0,     0,    88,     0,    89,    90,
      91,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,    92,     0,
       0,     0,     0,     0,     0,     0,     0,    93,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,    94,     0,
       0,     0,     0,    95,    96,    97,     0,     0,     0,    98,
      99,     0,     0,     0,   100,     0,   101,   102,   103,   104,
     105,   106,   107,   108,   109,   110,   111,     0,     0,     0,
       0,     0,   112,     0,   113,   114,   115,     0,     0,   116,
     117,   118,   119,   120,   121,   122,   123,   124,   125,   126,
     127,   128,   129,   130,     0,   131,     0,   132,   133,   134,
       0,     0,     0,   135,     0,     0,     0,   136,   137,   138,
     139,   140,   141,   142,   143,   144,   145,   146,   147,   148,
     149,   150,   151,   152,   153,   154,   155,   156,   157,   158,
     159,   160,   161,   162,     0,   163,   164,   165,   166,   167,
       0,   168,   169,   170,   171,   172,   173,   174,   175,   176,
     177,   178,   179,     0,   180,   181,   182,   183,   184,   185,
       0,   186,   187,   188,   189,   190,   191,   192,   193,   194,
     195,   196,   197,   198,   199,     0,     0,     0,   200,   201,
     202,   203,   204,     0,   205,   206,   207,   208,     0,     0,
     209,   210,   211,   212,   213,   214,   215,   216,   217,   218,
     219,   220,   221,   222,   223,   224,   225,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,   226,
     227,   228,   229,   230,   231,   232,     0,   233,     0,     0,
       0,     0,     0,     0,   234,   235,   236,   237,   238,   239,
     240,   241,   242,   243,   244,   245,   246,   247,   248,   249,
     250,   251,   252,   253,   254,   255,   256,   257,   258,   259,
     260,   261,   262,   263,   264,   265,   266,   267,   268,   269,
     270,   271,   272,   273,   274,   275,   276,   277,   278,   279,
     280,   281,   282,   283,   284,   285,   286,   287,   288,   289,
      79,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,    80,     0,     0,     0,     0,     0,     0,     0,     0,
       0,    81,     0,     0,     0,     0,    82,     0,     0,    83,
      84,    85,     0,     0,     0,    86,    87,     0,     0,     0,
       0,    88,     0,    89,    90,    91,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
      13,     0,     0,   515,     0,     0,     0,     0,     0,     0,
       0,     0,    93,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,    94,     0,     0,     0,     0,    95,    96,
      97,     0,     0,     0,    98,    99,     0,     0,     0,   100,
       0,   101,   102,   103,   104,   105,   106,   107,   108,   109,
     110,   111,     0,     0,     0,     0,     0,   112,     0,   113,
     114,   115,     0,     0,   116,   117,   118,   119,   120,   121,
     122,   123,   124,   125,   126,   127,   128,   129,   130,     0,
     131,     0,   132,   133,   134,     0,     0,     0,   135,     0,
       0,     0,   136,   137,   138,   139,   140,   141,   142,   143,
     144,   145,   146,   147,   148,   149,   150,   151,   152,   153,
     154,   155,   156,   157,   158,   159,   160,   161,   162,     0,
     163,   164,   165,   166,   167,     0,   168,   169,   170,   171,
     172,   173,   174,   175,   176,   177,   178,   179,     0,   180,
     181,   182,   183,   184,   185,     0,   186,   187,   188,   189,
     190,   191,   192,   193,   194,   195,   196,   197,   198,   199,
       0,     0,     0,   200,   201,   202,   203,   204,     0,   205,
     206,   207,   208,     0,     0,   209,   210,   211,   212,   213,
     214,   215,   216,   217,   218,   219,   220,   221,   222,   223,
     224,   225,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,   226,   227,   228,   229,   230,   231,
     232,     0,   233,     0,     0,     0,     0,     0,     0,   234,
     235,   236,   237,   238,   239,   240,   241,   242,   243,   244,
     245,   246,   247,   248,   249,   250,   251,   252,   253,   254,
     255,   256,   257,   258,   259,   260,   261,   262,   263,   264,
     265,   266,   267,   268,   269,   270,   271,   272,   273,   274,
     275,   276,   277,   278,   279,   280,   281,   282,   283,   284,
     285,   286,   287,   288,   289,    79,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,    80,     0,     0,     0,
       0,     0,     0,     0,     0,     0,    81,     0,     0,     0,
       0,    82,     0,     0,    83,    84,    85,     0,     0,     0,
      86,    87,     0,     0,     0,     0,    88,     0,    89,    90,
      91,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,    92,     0,
       0,     0,     0,     0,     0,     0,     0,    93,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,    94,     0,
       0,     0,     0,    95,    96,    97,     0,     0,     0,    98,
      99,     0,     0,     0,   100,     0,   101,   102,   103,   104,
     105,   106,   107,   108,   109,   110,   111,     0,     0,     0,
       0,     0,   112,     0,   113,   114,   115,     0,     0,   116,
     117,   118,   119,   120,   121,   122,   123,   124,   125,   126,
     127,   128,   129,   130,     0,   131,     0,   132,   133,   134,
       0,     0,     0,   135,     0,     0,     0,   136,   137,   138,
     139,   140,   141,   142,   143,   144,   145,   146,   147,   148,
     149,   150,   151,   152,   153,   154,   155,   156,   157,   158,
     159,   160,   161,   162,     0,   163,   164,   165,   166,   167,
       0,   168,   169,   170,   171,   172,   173,   174,   175,   176,
     177,   178,   179,     0,   180,   181,   182,   183,   184,   185,
       0,   186,   187,   188,   189,   190,   191,   192,   193,   194,
     195,   196,   197,   198,   199,     0,     0,     0,   200,   201,
     202,   203,   204,     0,   205,   206,   207,   208,     0,     0,
     209,   210,   211,   212,   213,   214,   215,   216,   217,   218,
     219,   220,   221,   222,   223,   224,   225,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,   226,
     227,   228,   229,   230,   231,   232,     0,   233,     0,     0,
       0,     0,     0,     0,   234,   235,   236,   237,   238,   239,
     240,   241,   242,   243,   244,   245,   246,   247,   248,   249,
     250,   251,   252,   253,   254,   255,   256,   257,   258,   259,
     260,   261,   262,   263,   264,   265,   266,   267,   268,   269,
     270,   271,   272,   273,   274,   275,   276,   277,   278,   279,
     280,   281,   282,   283,   284,   285,   286,   287,   288,   289,
      79,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,    80,     0,     0,     0,     0,     0,     0,     0,     0,
       0,    81,     0,     0,     0,     0,    82,     0,     0,    83,
      84,    85,     0,     0,     0,    86,    87,     0,     0,     0,
       0,    88,     0,    89,    90,    91,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,   377,     0,     0,     0,     0,     0,     0,
       0,     0,    93,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,    94,     0,     0,     0,     0,    95,    96,
      97,     0,     0,     0,    98,    99,     0,     0,     0,   100,
       0,   101,   102,   103,   104,   105,   106,   107,   108,   109,
     110,   111,     0,     0,     0,     0,     0,   112,     0,   113,
     114,   115,     0,     0,   116,   117,   118,   119,   120,   121,
     122,   123,   124,   125,   126,   127,   128,   129,   130,     0,
     131,     0,   132,   133,   134,     0,     0,     0,   135,     0,
       0,     0,   136,   137,   138,   139,   140,   141,   142,   143,
     144,   145,   146,   147,   148,   149,   150,   151,   152,   479,
     154,   155,   156,   157,   158,   159,   160,   161,   162,     0,
     163,   164,   165,   166,   167,     0,   168,   169,   170,   171,
     172,   173,   174,   175,   176,   480,   178,   179,     0,   180,
     181,   182,   183,   184,   185,     0,   186,   187,   188,   189,
     190,   191,   192,   193,   194,   195,   196,   197,   198,   199,
       0,     0,     0,   200,   201,   202,   203,   204,     0,   205,
     206,   207,   208,     0,     0,   209,   210,   211,   212,   481,
     482,   483,   484,   217,   218,   219,   220,   221,   222,   223,
     224,   225,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,   226,   227,   228,   229,   230,   231,
     232,     0,   233,     0,     0,     0,     0,     0,     0,   234,
     235,   236,   237,   238,   239,   240,   241,   242,   243,   244,
     245,   246,   247,   248,   249,   250,   251,   252,   253,   254,
     255,   256,   257,   258,   259,   260,   261,   262,   263,   264,
     265,   266,   267,   268,   269,   270,   271,   272,   273,   274,
     275,   276,   277,   278,   279,   280,   281,   282,   283,   284,
     285,   286,   287,   288,   289,    79,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,    80,     0,     0,     0,
       0,     0,     0,     0,     0,     0,    81,     0,     0,     0,
       0,    82,     0,     0,    83,    84,    85,     0,     0,     0,
      86,    87,     0,     0,     0,     0,    88,     0,    89,    90,
      91,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,   515,     0,
       0,     0,     0,     0,     0,     0,     0,    93,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,    94,     0,
       0,     0,     0,    95,    96,    97,     0,     0,     0,    98,
      99,     0,     0,     0,   100,     0,   101,   102,   103,   104,
     105,   106,   107,   108,   109,   110,   111,     0,     0,     0,
       0,     0,   112,     0,   113,   114,   115,     0,     0,   116,
     117,   118,   119,   120,   121,   122,   123,   124,   125,   126,
     127,   128,   129,   130,     0,   131,     0,   132,   133,   134,
       0,     0,     0,   135,     0,     0,     0,   136,   137,   138,
     139,   140,   141,   142,   143,   144,   145,   146,   147,   148,
     149,   150,   151,   152,   153,   154,   155,   156,   157,   158,
     159,   160,   161,   162,     0,   163,   164,   165,   166,   167,
       0,   168,   169,   170,   171,   172,   173,   174,   175,   176,
     177,   178,   179,     0,   180,   181,   182,   183,   184,   185,
       0,   186,   187,   188,   189,   190,   191,   192,   193,   194,
     195,   196,   197,   198,   199,     0,     0,     0,   200,   201,
     202,   203,   204,     0,   205,   206,   207,   208,     0,     0,
     209,   210,   211,   212,   213,   214,   215,   216,   217,   218,
     219,   220,   221,   222,   223,   224,   225,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,   226,
     227,   228,   229,   230,   231,   232,     0,   233,     0,     0,
       0,     0,     0,     0,   234,   235,   236,   237,   238,   239,
     240,   241,   242,   243,   244,   245,   246,   247,   248,   249,
     250,   251,   252,   253,   254,   255,   256,   257,   258,   259,
     260,   261,   262,   263,   264,   265,   266,   267,   268,   269,
     270,   271,   272,   273,   274,   275,   276,   277,   278,   279,
     280,   281,   282,   283,   284,   285,   286,   287,   288,   289,
      79,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,    80,     0,     0,     0,     0,     0,     0,     0,     0,
       0,    81,     0,     0,     0,     0,    82,     0,     0,    83,
      84,    85,     0,     0,     0,    86,    87,     0,     0,     0,
       0,    88,     0,    89,    90,    91,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,   377,     0,     0,     0,     0,     0,     0,
       0,     0,    93,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,    94,     0,     0,     0,     0,    95,    96,
      97,     0,     0,     0,    98,    99,     0,     0,     0,   100,
       0,   101,   102,   103,   104,   105,   106,   107,   108,   109,
     110,   111,     0,     0,     0,     0,     0,   112,     0,   113,
     114,   115,     0,     0,   116,   117,   118,   119,   120,   121,
     122,   123,   124,   125,   126,   127,   128,   129,   130,     0,
     131,     0,   132,   133,   134,     0,     0,     0,   135,     0,
       0,     0,   136,   137,   138,   139,   140,   141,   142,   143,
     144,   145,   146,   147,   148,   149,   150,   151,   152,   905,
     154,   155,   156,   157,   158,   159,   160,   161,   162,     0,
     163,   164,   165,   166,   167,     0,   168,   169,   170,   171,
     172,   173,   174,   175,   176,   480,   178,   179,     0,   180,
     181,   182,   183,   184,   185,     0,   186,   187,   188,   189,
     190,   191,   192,   193,   194,   195,   196,   197,   198,   199,
       0,     0,     0,   200,   201,   202,   203,   204,     0,   205,
     206,   207,   208,     0,     0,   209,   210,   211,   212,   481,
     482,   215,   216,   217,   218,   219,   220,   221,   222,   223,
     224,   225,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,   226,   227,   228,   229,   230,   231,
     232,     0,   233,     0,     0,     0,     0,     0,     0,   234,
     235,   236,   237,   238,   239,   240,   241,   242,   243,   244,
     245,   246,   247,   248,   249,   250,   251,   252,   253,   254,
     255,   256,   257,   258,   259,   260,   261,   262,   263,   264,
     265,   266,   267,   268,   269,   270,   271,   272,   273,   274,
     275,   276,   277,   278,   279,   280,   281,   282,   283,   284,
     285,   286,   287,   288,   289,    79,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,    80,     0,     0,     0,
       0,     0,     0,     0,     0,     0,    81,     0,     0,     0,
       0,    82,     0,     0,    83,    84,    85,     0,     0,     0,
      86,    87,     0,     0,     0,     0,    88,     0,    89,    90,
      91,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,   377,     0,
       0,     0,     0,     0,     0,     0,     0,    93,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,    94,     0,
       0,     0,     0,    95,    96,    97,     0,     0,     0,    98,
      99,     0,     0,     0,   100,     0,   101,   102,   103,   104,
     105,   106,   107,   108,   109,   110,   111,     0,     0,     0,
       0,     0,   112,     0,   113,   114,   115,     0,     0,   116,
     117,   118,   119,   120,   121,   122,   123,   124,   125,   126,
     127,   128,   129,   130,     0,   131,     0,   132,   133,   134,
       0,     0,     0,   135,     0,     0,     0,   136,   137,   138,
     139,   140,   141,   142,   143,   144,   145,   146,   147,   148,
     149,   150,   151,   152,   153,   154,   155,   156,   157,   158,
     159,   160,   161,   162,     0,   163,   164,   165,   166,   167,
       0,   168,   169,   170,   171,   172,   173,   174,   175,   176,
     177,   178,   179,     0,   180,   181,   182,   183,   184,   185,
       0,   186,   187,   188,   189,   190,   191,   192,   193,   194,
     195,   196,   197,   198,   199,     0,     0,     0,   200,   201,
     202,   203,   204,     0,   205,   206,   207,   208,     0,     0,
     209,   210,   211,   212,   213,   214,   215,   216,   217,   218,
     219,   220,   221,   222,   223,   224,   225,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,   226,
     227,   228,   229,   230,   231,   232,     0,   233,     0,     0,
       0,     0,     0,     0,   234,   235,   236,   237,   238,   239,
     240,   241,   242,   243,   244,   245,   246,   247,   248,   249,
     250,   251,   252,   253,   254,   255,   256,   257,   258,   259,
     260,   261,   262,   263,   264,   265,   266,   267,   268,   269,
     270,   271,   272,   273,   274,   275,   276,   277,   278,   279,
     280,   281,   282,   283,   284,   285,   286,   287,   288,   289,
      79,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,    80,     0,     0,     0,     0,     0,     0,     0,     0,
       0,    81,     0,     0,     0,     0,    82,     0,     0,    83,
      84,    85,     0,     0,     0,    86,    87,     0,     0,     0,
       0,    88,     0,    89,    90,    91,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,   377,     0,     0,     0,     0,     0,     0,
       0,     0,    93,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,    94,     0,     0,     0,     0,    95,    96,
      97,     0,     0,     0,    98,    99,     0,     0,     0,   100,
       0,   101,   102,   103,   104,   105,   106,   107,   108,   109,
     110,   111,     0,     0,     0,     0,     0,   112,     0,   113,
     114,   115,     0,     0,   116,   117,   118,   119,   120,   121,
     122,   123,   124,   125,   126,   127,   128,   129,   130,     0,
     131,     0,   132,   133,   134,     0,     0,     0,   135,     0,
       0,     0,   136,   137,   138,   139,   140,   141,   142,   143,
     144,   145,   146,   147,   148,   149,   150,   151,   152,   153,
     154,   155,   156,   157,   158,   159,   160,   161,   162,     0,
     163,   164,   165,   166,   167,     0,   168,   169,   170,   171,
     172,   173,   174,   175,   176,   480,   178,   179,     0,   180,
     181,   182,   183,   184,   185,     0,   186,   187,   188,   189,
     190,   191,   192,   193,   194,   195,   196,   197,   198,   199,
       0,     0,     0,   200,   201,   202,   203,   204,     0,   205,
     206,   207,   208,     0,     0,   209,   210,   211,   212,   481,
     482,   215,   216,   217,   218,   219,   220,   221,   222,   223,
     224,   225,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,   226,   227,   228,   229,   230,   231,
     232,     0,   233,     0,     0,     0,     0,     0,     0,   234,
     235,   236,   237,   238,   239,   240,   241,   242,   243,   244,
     245,   246,   247,   248,   249,   250,   251,   252,   253,   254,
     255,   256,   257,   258,   259,   260,   261,   262,   263,   264,
     265,   266,   267,   268,   269,   270,   271,   272,   273,   274,
     275,   276,   277,   278,   279,   280,   281,   282,   283,   284,
     285,   286,   287,   288,   289
};

static const yytype_int16 yycheck[] =
{
       0,     0,     0,     0,     0,     0,   485,    71,   709,  1022,
     372,   477,   451,     0,   674,   693,  1028,    23,    24,     2,
     852,     4,     5,   388,  1217,     8,   391,   392,  1347,   470,
     395,   396,   397,   398,   399,    72,  1229,  1065,  1066,  1067,
    1068,  1505,  1506,  1341,  1224,  1049,  1050,  1051,   873,   874,
      75,    56,   359,  1281,  1282,  1283,    62,    40,   698,  1176,
    1195,   876,  1046,  1047,  1507,    69,  1548,  1031,    68,  1741,
      76,    77,  1377,  1378,  1379,  1380,  1236,  1382,  1583,  1239,
     295,    68,    66,  1940,     5,   300,     7,     8,     9,  1747,
      66,    14,    64,   138,    69,    67,  1971,    66,   913,    29,
      22,     0,  1862,    11,  1332,     4,    10,  1335,   470,    66,
       5,  1940,    11,    12,    13,    14,     5,    16,   141,    18,
     143,    14,   179,    22,     0,    85,    64,  2008,     4,    67,
      29,    30,    10,   178,    11,    11,    12,    13,    14,    11,
      16,   101,    18,   100,   178,    66,    22,    43,  1308,    69,
      10,  2008,   276,    29,    30,    54,    55,    56,    57,    67,
      59,    60,    61,    62,    63,    64,    65,   322,    67,    68,
      10,    66,  1702,  1703,    67,   330,   331,    66,    98,  2008,
      56,    57,   337,    59,    60,    61,    62,    63,    64,    65,
    1950,    67,    68,    44,    98,  1052,  1053,  1054,   170,  1056,
    1057,    10,  1059,  1060,  1061,  1062,  2081,   106,   107,   108,
     109,   110,   111,   112,   113,   114,   115,   116,   117,   276,
     142,   120,   152,   100,  1882,   155,  2107,   157,   100,  1086,
     137,   138,   139,   267,   141,  1740,   876,   241,    98,  1769,
      54,    55,    69,   139,   143,  1543,    22,   170,   126,   127,
    2107,    28,     7,     8,     9,   278,     5,   897,     7,     8,
       9,  1743,   246,   702,   233,    25,  1748,   174,   175,  1588,
     246,   191,   192,   913,    29,  1718,  1719,  1720,  2107,  1951,
      29,  1579,    24,    25,  1582,  2083,   126,   127,  1103,    98,
     276,  1596,  1597,  1257,  1599,     5,   147,   148,    10,  1829,
    1480,  1744,    62,    80,  1609,  1465,    64,   967,    84,    85,
     137,   138,   677,    73,   141,   471,   138,    66,    58,   319,
      78,   358,   322,    63,  2084,   101,  1461,    10,  1488,   143,
     330,   331,    11,  1863,    35,  1167,    46,   337,  1366,  1367,
    1457,  1369,    84,    85,    93,  2143,   351,   174,   175,   132,
     177,   506,    64,  1381,    10,    67,    66,   357,   514,   101,
      22,   183,    18,    43,   364,   138,   366,   366,   366,   366,
     809,  1355,   372,   373,   374,   375,   376,   364,   212,   366,
    1844,    43,    52,    53,    67,   168,    66,   308,    67,    69,
     755,   149,   299,  1857,   394,   144,   238,   239,    69,   241,
     400,   401,   402,   403,   404,   405,   406,   407,   408,   409,
     410,   411,   412,   413,   414,   415,   416,   417,   418,   419,
     420,   421,   422,   423,   424,   425,   426,   427,   428,   429,
     430,   431,   432,   433,   434,   435,   436,   437,   438,   439,
     440,   441,   442,   443,   444,   445,   446,   447,   448,   449,
     665,     5,   123,   894,  1773,   896,   671,    49,   213,   139,
     485,    18,   211,  1103,   213,   130,   466,   215,   133,   469,
     470,  1631,   299,    30,   176,    29,  1504,   139,  1783,    71,
     142,    58,   469,   848,   146,   138,    63,    29,   853,   854,
     852,  1316,    46,    14,   859,   860,   861,   862,   863,   864,
     865,   866,   867,   868,   869,   870,   506,   872,   663,   664,
      44,  1368,    66,   668,  1742,   670,  1373,  1374,  1375,  1699,
     675,  1145,  1146,    28,   423,    67,   888,    43,  1152,  1153,
    1154,  1155,   894,    10,   896,    95,   296,     0,    43,    44,
     695,   696,  2072,   289,   304,   291,    67,   423,    66,   117,
      84,  2015,   120,   308,    95,   315,   316,   423,    76,   308,
      65,    69,    14,    71,     5,   125,     7,     8,     9,   876,
    1598,    95,    95,    78,   276,    80,    28,  1875,   280,   281,
     282,     4,    87,    17,   125,   345,  2116,    64,    29,   159,
      67,     4,  1897,    65,    20,    21,  1901,   180,   181,  1205,
     360,   125,   125,   137,   138,   139,   913,   141,   776,  1215,
     239,   779,   241,   147,   148,   783,   186,    69,   786,   787,
     126,   127,    48,   139,   792,    66,    78,    14,  1947,   145,
     146,   174,   175,    67,   150,  1639,  1314,   153,   154,  1832,
     145,    66,    84,    85,    69,   143,  2110,    88,    89,    90,
      84,    85,    93,  1313,   170,   189,   128,    66,   130,   101,
      69,   133,    66,   663,   664,    69,   272,   101,   668,     5,
     670,     7,     8,     9,    66,   675,    49,   129,   130,   131,
      90,   133,    92,   135,   136,    64,   308,    88,    67,    90,
     144,    92,  2156,    29,   139,   695,   696,  1709,   291,    72,
      95,   294,    75,   144,   297,    84,    85,   875,   138,   734,
     710,   471,    84,    85,    80,    81,    82,  1418,   170,   171,
      92,    87,   101,   710,   707,  1204,  1033,  1034,   144,   101,
      66,   731,    60,    61,   798,    63,   800,    20,    21,   499,
    1920,    65,  1574,    67,   731,  1602,    69,   507,  1776,   774,
     775,   511,    88,    69,   514,    71,   180,    93,   518,    67,
      68,  1789,   267,   918,    29,   299,   180,   922,   125,    94,
     211,    29,   213,    98,    99,   100,  1804,     7,     8,     9,
    1637,    84,    85,  1640,  1641,  1642,  1643,   181,    91,   241,
    1491,  1232,    84,    85,  1213,  1214,  1103,  1216,   101,    91,
      15,  1166,    95,  1168,  1169,  1167,    20,    21,   144,   101,
      80,    81,    82,    12,    78,   267,  1828,  1441,  1442,  1443,
    1444,  1445,    16,    87,    68,     5,    25,     7,     8,     9,
      77,     5,    66,     7,     8,     9,   125,     5,    10,     7,
       8,     9,   309,  1205,   311,   312,   313,   314,   125,    29,
     125,  1213,  1214,  1215,  1216,    29,   125,   309,    29,    84,
      85,    29,    95,    62,    67,    68,    91,   308,   125,  1231,
    1232,    67,    84,    85,    73,   211,   101,   213,   422,    91,
     880,  1320,  1895,   908,    67,    68,    66,   141,   888,   101,
      87,   155,    66,   880,   894,    22,   896,   178,    66,   292,
     293,   888,    24,    25,   356,  1565,   666,   667,    88,   669,
      88,    89,    90,    93,    88,  1772,    67,    68,   918,    93,
      88,    69,   922,    67,    68,    93,    69,   687,    69,   193,
     194,   195,   196,   197,  1283,  1284,  1285,  1302,    70,    71,
      72,    73,    74,    75,    98,    64,    65,    79,    80,   295,
     296,   711,   106,   107,   108,   109,   110,   111,   112,   113,
     114,   115,   116,   117,   144,    69,   120,    69,    18,    23,
     144,    69,   308,    65,  1675,    69,   144,   110,   111,   112,
     113,   114,   115,   116,   117,    67,    68,   120,  1689,   389,
     390,   991,   992,   993,   994,   995,   996,   997,   998,   999,
    1000,  1001,  1002,  1003,  1004,  1005,  1006,  1007,  1008,  1009,
    1010,  1011,  1012,  1013,  1014,  1015,  1016,  1017,  1018,   107,
     108,   109,   110,   111,   112,   113,   114,   115,   116,   117,
      69,   211,   120,   213,    69,    67,    68,   211,    67,   213,
    1347,    67,    68,   211,   100,   213,   142,  1412,   808,  1414,
    1415,    65,  1417,  1885,  1419,  1420,   106,   107,   108,   109,
     110,   111,   112,   113,   114,   115,   116,   117,    10,   469,
     120,    67,    68,   281,   282,    69,  2124,   477,  2126,   112,
     113,   114,   115,   116,   117,   158,   159,   120,   352,    67,
      68,    75,    71,  1458,  1573,    69,  2151,   296,    62,  2147,
      67,    68,   407,   408,   409,   304,   411,   412,  2163,  2164,
     120,   310,    67,    68,  1479,   101,   315,   316,    67,  2167,
     319,   125,  2177,   322,    67,    68,    92,    43,   308,   290,
    1962,   330,   331,   893,   308,   120,  2184,    71,   337,  1294,
     308,  2196,   123,  1298,  1299,    26,   345,  2195,    67,    68,
      66,    40,    41,    69,  2209,  1462,   916,    23,  2213,    66,
     359,   360,   189,   190,    67,    68,    67,    68,   928,   929,
     930,   931,   932,    67,    68,    67,    68,   937,    64,  1204,
     187,   188,   163,   943,   944,    67,    68,    56,    57,  1630,
      59,    60,    61,    62,    63,  1502,    67,  1197,  1563,   353,
      67,    68,    67,   963,    22,  1205,    67,    68,    71,   969,
       8,     9,    69,  1213,  1214,  1215,  1216,    69,    71,    67,
      95,    67,  1222,   139,     8,    98,   170,    66,    66,   145,
     146,  1231,  1232,  1233,   150,    66,    66,   153,   154,   241,
      66,    46,   270,    46,  1231,    67,    10,  1612,   229,    10,
      67,    67,   451,   234,   170,   236,   317,   238,    11,    63,
     139,  1261,   243,   125,    63,   125,   125,   139,  1630,   139,
    1030,   470,   471,    22,  1261,   256,   676,    65,   125,   178,
    1587,  1588,   138,    64,    95,   170,  1987,  1293,   269,   270,
     489,    32,   146,   142,  1294,    69,    69,    33,  1298,  1299,
     499,    43,   702,   703,    50,    43,   505,   506,   507,    69,
      66,    69,   511,    67,    71,   514,    29,  1682,   271,   518,
      72,    72,    72,    88,     4,   350,   310,   112,    69,    63,
    1330,    63,  1396,   291,    65,   319,    69,    65,   322,    69,
      69,    68,    67,  1330,   353,    69,   330,   331,    69,    43,
      69,    69,    69,   337,  1423,  1424,    51,  1426,  1427,    69,
      69,  1430,  1431,  1432,  1433,    65,  1435,  1436,  1437,    66,
      68,   154,   772,   773,    67,   359,    43,     8,    80,    71,
      43,   223,   123,   223,   314,    56,    57,   128,    59,    60,
      61,    62,    63,    64,    65,    64,    66,   138,   139,   140,
     106,   107,   108,   109,   110,   111,   112,   113,   114,   115,
     116,   117,   812,   813,   120,   405,   406,   407,   408,   409,
     410,   411,   412,  1730,    66,    66,    69,   142,    26,    67,
      27,    71,    12,   174,   125,    72,   177,    72,  1438,   180,
     181,   182,    67,    71,    15,  1810,  1811,  1812,  1813,  1915,
      38,  1816,  1817,    37,    71,   198,   193,   197,   164,    18,
    1825,    67,   197,    95,   663,   664,  1773,   666,   667,   668,
     669,   670,    66,    69,   197,   197,   675,   197,    68,    43,
     880,  1481,  1482,  1483,    69,    68,    68,    15,   687,    13,
      15,    42,   143,    35,    35,   184,   695,   696,   170,   698,
     241,   485,     8,   125,   156,   489,    69,   907,    39,    71,
     354,    71,   711,    40,    15,    72,    66,   185,    66,    72,
      67,   505,   506,   352,    37,    51,    15,    18,   355,  1535,
      15,    71,   273,   274,   275,  1295,   277,   278,   279,  1978,
    1979,  1547,   283,   284,    46,   286,   287,   288,  1573,   423,
     749,    66,    51,   298,   954,   300,   301,   302,   303,   304,
     305,   306,   307,    71,    71,  2044,    69,   172,   423,    69,
     315,   316,    93,   353,  1574,   354,   144,  1577,   777,   778,
     423,   173,   423,    65,   355,    65,   423,  1656,   423,    65,
    1577,   423,  1661,     8,   357,  1664,  1665,   735,   736,    93,
     144,  1670,     9,   423,   423,   151,   744,   144,   144,   808,
     809,   905,  1245,   751,   752,   106,   107,   108,   109,   110,
     111,   112,   113,   114,   115,   116,   117,     0,   473,   120,
    1630,  1722,  1392,   771,  1858,  2142,   774,   775,  2168,  1946,
    1947,  2104,   780,   781,   782,     0,   784,   785,  1315,  2034,
     788,   789,   790,   791,  2093,   793,   794,   795,  2082,  1946,
    1877,   799,  1033,   801,   802,  1383,  1894,  1970,  1893,   807,
    1283,  2175,  2174,  2189,  1587,  1283,     0,  2086,     0,   663,
     664,  1681,     0,     0,   668,     0,   670,   886,     0,   888,
    1714,   675,     0,    18,   893,   894,   895,   896,   897,     0,
       0,     0,   809,     0,  1104,  1027,  1839,  1489,  1477,  1699,
    1206,   695,   696,   895,  1167,  1715,  1792,   916,  1409,   918,
      18,  1411,  1105,   922,  2093,  1320,  1327,  1755,  1715,   928,
     929,   930,   931,   932,  1885,  1962,  1999,  1545,   937,   908,
      -1,    -1,    -1,  1503,   943,   944,    -1,    -1,    -1,    -1,
     734,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,  1519,
      -1,    -1,    -1,    -1,   963,   749,   965,   966,    -1,    -1,
     969,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
    1540,   106,   107,   108,   109,   110,   111,   112,   113,   114,
     115,   116,   117,   777,   778,   120,  1556,    67,    -1,    -1,
      -1,  1201,    -1,    -1,    -1,    -1,    -1,  2172,   106,   107,
     108,   109,   110,   111,   112,   113,   114,   115,   116,   117,
      67,    -1,   120,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,  1030,    -1,    -1,  1033,  1034,   106,   107,   108,   109,
     110,   111,   112,   113,   114,   115,   116,   117,    -1,    -1,
     120,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   106,
     107,   108,   109,   110,   111,   112,   113,   114,   115,   116,
     117,    -1,    -1,   120,  1880,  1881,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,  1883,    85,  1885,    -1,    -1,    -1,    -1,
    1890,    -1,   876,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,  1890,    -1,   106,   107,   108,   109,   110,
     111,   112,   113,   114,   115,   116,   117,    -1,    -1,   120,
    1320,    -1,  1322,  1323,   908,    -1,    -1,    68,    -1,   913,
      -1,    -1,    -1,    -1,   918,    -1,    -1,    -1,   922,    -1,
    1940,  1940,  1940,  1940,  1940,  1940,  1952,    -1,    -1,    -1,
      -1,  1957,    -1,  1940,    -1,    -1,    -1,    -1,    -1,  1097,
      -1,    -1,  1962,  1723,  1964,   106,   107,   108,   109,   110,
     111,   112,   113,   114,   115,   116,   117,  1964,    -1,   120,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,  1125,    -1,    -1,
    1128,    -1,    -1,    -1,  1132,    -1,    -1,  1135,  1136,    -1,
    1400,    -1,    -1,  1141,    -1,    -1,  1205,    -1,  2008,  2008,
    2008,  2008,  2008,  2008,  1213,  1214,  1215,  1216,  1217,  2044,
      -1,  2008,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
    1229,    85,  1231,  1232,   106,   107,   108,   109,   110,   111,
     112,   113,   114,   115,   116,   117,    -1,    -1,   120,  1033,
    1034,    -1,   106,   107,   108,   109,   110,   111,   112,   113,
     114,   115,   116,   117,    -1,    -1,   120,    -1,  1267,   108,
     109,   110,   111,   112,   113,   114,   115,   116,   117,    -1,
      -1,   120,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
    1490,    -1,    -1,    -1,    -1,  1294,  1295,    -1,    -1,  1298,
    1299,    -1,    -1,    -1,  1864,    -1,  2106,  2107,  2107,  2107,
    2107,  2107,  2107,    -1,    -1,    -1,    -1,    -1,    -1,  1103,
    2107,    -1,    -1,    -1,  2124,    -1,  2126,    -1,   199,   200,
     201,   202,   203,   204,   205,   206,   207,   208,   209,   210,
      -1,    -1,    -1,  2142,  2142,  2142,  2142,  2147,  1347,    -1,
      -1,  2151,  1552,    -1,    -1,    -1,    -1,    -1,    -1,  2159,
      -1,    -1,    -1,  2163,  2164,    -1,    -1,  2167,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,  2177,    -1,    -1,
      -1,    -1,    -1,    -1,  2184,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,  1392,    -1,  2195,  2196,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,  2209,
      -1,    -1,    -1,  2213,    -1,    -1,    -1,    -1,    -1,    -1,
    1204,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,  1633,    -1,    -1,    -1,    -1,    -1,  1438,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,     5,     6,     7,     8,
       9,    -1,    -1,  1462,    -1,    -1,    -1,    -1,    -1,    -1,
      19,    -1,    -1,    -1,    -1,    -1,    25,    -1,    -1,    -1,
      29,    30,    31,  1267,    -1,  1484,    -1,    36,  2048,    -1,
    1489,    -1,    -1,  1693,    -1,    -1,  1696,    -1,    -1,    -1,
      -1,    -1,    -1,  1502,  1503,    -1,    55,    -1,    -1,    -1,
    1294,    -1,    -1,    -1,  1298,  1299,    -1,    66,    -1,    -1,
    1519,  1520,  1521,  1522,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,  1533,    -1,    -1,    -1,    -1,    -1,
    1539,  1540,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,  1550,    -1,    -1,    -1,    -1,    -1,  1556,    -1,  1759,
      -1,    -1,    -1,  1347,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,  1577,   128,
     129,   130,   131,   132,    -1,   134,  1585,    -1,  1587,  1588,
      -1,    -1,    -1,    -1,    12,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    25,    -1,    -1,
      -1,   160,   161,   162,    -1,    -1,   165,   166,   167,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,  1630,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    62,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    71,  1438,    73,    -1,    75,    -1,    -1,
      -1,    -1,   211,    -1,   213,   214,    -1,   216,   217,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,  1462,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
    1484,    -1,  1701,  1702,  1703,  1489,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,  1915,    -1,    -1,  1502,    -1,
      -1,    -1,    -1,    -1,  1723,    -1,    -1,    -1,    -1,    -1,
      -1,  1730,    -1,    -1,    -1,    -1,  1520,  1521,  1522,    -1,
      -1,    -1,    -1,    -1,     5,    -1,     7,     8,     9,  1533,
      -1,    -1,    -1,    -1,    -1,  1539,    -1,    -1,    19,   308,
      -1,    -1,    -1,    -1,    25,    -1,  1550,    -1,    29,    -1,
    1769,    -1,    -1,    -1,  1773,    -1,    -1,    -1,  1978,  1979,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,  1573,
      -1,    -1,    -1,  1577,    -1,    -1,    -1,   367,    -1,    -1,
      -1,  1585,    29,  1587,  1588,    66,  2006,  2007,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    88,    -1,    -1,
    1829,    -1,    93,  1832,    -1,    -1,    -1,    -1,    -1,  1838,
    1839,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,  1862,  1863,  1864,    -1,   128,   129,   130,
     131,   132,    -1,  1872,    -1,    -1,    -1,    -1,   296,    -1,
      -1,    -1,    -1,   144,    -1,    -1,   304,    -1,    -1,    -1,
    1889,    -1,  1891,  2093,  1893,    -1,   123,   315,   316,   160,
     161,   162,    -1,  2103,    -1,    -1,   167,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,  1701,  1702,  1703,
      -1,    -1,  1921,    -1,    -1,    -1,    -1,   345,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,   163,    -1,    -1,    -1,
      -1,    -1,   360,    -1,    -1,    -1,  1730,  1946,  1947,    -1,
     211,  1950,   213,   214,    -1,   216,   217,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,  2166,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,  1769,    -1,    -1,    -1,  1773,
      -1,   218,   219,   220,   221,   222,   223,   224,    -1,   226,
     227,   228,   229,   230,   231,   232,    -1,   234,   235,   236,
     237,   238,   239,   240,   241,   242,   243,   244,   245,    -1,
     247,   248,   249,   250,   251,   252,   253,   254,   255,   256,
     257,   258,   259,   260,   261,   262,   263,   264,   265,    -1,
      -1,    -1,  2041,    -1,    -1,  1829,  2045,   308,    -1,  2048,
      -1,    -1,   470,   471,  1838,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,     5,    -1,     7,     8,     9,   485,    -1,    -1,
      -1,    -1,    -1,  2072,    -1,    -1,    19,    -1,  1862,  1863,
      -1,   499,    25,    -1,    -1,  2084,    29,    -1,  1872,   507,
      -1,    -1,    -1,   511,    -1,    -1,   514,    -1,    -1,    -1,
     518,    -1,    -1,    -1,    -1,  1889,    -1,  1891,    -1,  1893,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,  2116,    -1,    -1,
      -1,    -1,    -1,    66,   351,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,  1921,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      93,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,  1946,  1947,    -1,    -1,  1950,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
     750,    -1,    -1,    -1,    -1,   128,   129,   130,   131,   132,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,   144,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,   160,   161,   162,
      -1,    -1,    -1,    -1,   167,    -1,   796,   797,    -1,    -1,
      -1,    -1,    -1,   803,   804,   805,   806,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   666,   667,
      -1,   669,    -1,    -1,    -1,    -1,    -1,  2041,    -1,    -1,
    2044,  2045,    -1,    -1,    -1,    -1,    -1,    -1,   211,   687,
     213,   214,    -1,   216,   217,    -1,    -1,    -1,    -1,    -1,
     698,    -1,    -1,    -1,    -1,    -1,    -1,    -1,  2072,    -1,
      -1,    -1,    -1,   711,    -1,    -1,    -1,    -1,    -1,    -1,
    2084,    -1,    -1,    -1,    -1,    -1,    -1,   877,    -1,   879,
      -1,    -1,    -1,    -1,    -1,    -1,   734,   735,   736,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,   744,    -1,    -1,    -1,
      -1,    -1,  2116,   751,   752,    -1,    -1,    -1,   756,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,   771,    -1,    -1,   774,   775,    -1,    -1,
      -1,    -1,   780,   781,   782,   308,   784,   785,    -1,    -1,
     788,   789,   790,   791,    -1,   793,   794,   795,    -1,    -1,
     798,   799,   800,   801,   802,    87,    -1,    -1,    -1,   807,
     808,    -1,    94,    95,    96,    97,    98,    99,   100,    -1,
     102,   103,   104,   105,   106,   107,   108,   109,   110,   111,
     112,   113,   114,   115,   116,   117,    -1,    -1,   120,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   876,  1029,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   886,    -1,
     888,    -1,    -1,    -1,    -1,   893,   894,   895,   896,   897,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
     908,    -1,    -1,    -1,    -1,   913,    -1,    -1,   916,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
     928,   929,   930,   931,   932,    -1,    -1,    -1,    -1,   937,
      -1,    -1,    -1,    -1,    -1,   943,   944,    -1,  1098,  1099,
    1100,    -1,  1102,    -1,    -1,    -1,    -1,  1107,    -1,    -1,
    1110,  1111,  1112,    -1,  1114,   963,  1116,   965,   966,    -1,
    1120,   969,    -1,  1123,    -1,    -1,    -1,    -1,    -1,  1129,
    1130,  1131,    -1,  1133,  1134,    -1,    -1,  1137,  1138,  1139,
    1140,    -1,  1142,  1143,  1144,    -1,    -1,  1147,  1148,  1149,
    1150,  1151,    -1,    -1,    -1,    -1,  1156,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,  1030,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,  1203,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
    1230,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,  1097,
      -1,    -1,    -1,    -1,    -1,  1103,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,  1125,    -1,    -1,
    1128,    -1,    -1,    -1,  1132,    -1,    -1,  1135,  1136,    -1,
      -1,    -1,    -1,  1141,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,  1195,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,  1204,  1205,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,  1213,  1214,  1215,  1216,  1217,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,  1229,    -1,  1231,  1232,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,  1421,    -1,    -1,    -1,  1425,    -1,    -1,  1428,  1429,
      -1,    -1,    -1,    -1,  1434,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,  1295,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
    1460,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,  1392,    -1,    -1,    -1,  1396,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,  1461,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,  1636,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,  1647,  1648,    -1,
      -1,    -1,    -1,    -1,    -1,  1503,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,  1519,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,  1686,    -1,    -1,    -1,
      -1,    -1,  1540,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,  1704,    -1,    -1,  1707,  1556,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,  1573,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,  1759,
      -1,    -1,  1762,  1763,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,  1630,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,  1815,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,  1824,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,  1834,    -1,  1836,  1837,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,  1723,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,  1907,  1908,  1909,
    1910,    -1,    -1,  1913,  1914,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,  1832,    -1,    -1,    -1,    -1,    -1,
    1990,  1839,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,  1864,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,  2042,    -1,    -1,    -1,    -1,  2047,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
    2060,    -1,    -1,     0,    -1,    -1,    -1,    -1,    -1,     6,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      17,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      27,    -1,    -1,    -1,    -1,    32,    -1,    -1,    35,    36,
      37,    -1,    -1,    -1,    41,    42,    -1,    -1,    -1,    -1,
      47,    -1,    49,    50,    51,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    69,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    78,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,  2186,  2187,    -1,    -1,
      -1,    -1,   129,    -1,    -1,    -1,  2044,   134,   135,   136,
    2048,    -1,    -1,   140,   141,    -1,    -1,    -1,   145,    -1,
     147,   148,   149,   150,   151,   152,   153,   154,   155,   156,
     157,    -1,    -1,    -1,    -1,    -1,   163,    -1,   165,   166,
     167,    -1,    -1,   170,   171,   172,   173,   174,   175,   176,
     177,   178,   179,   180,   181,   182,   183,   184,    -1,   186,
      -1,   188,   189,   190,    -1,    -1,    -1,   194,    -1,    -1,
      -1,   198,   199,   200,   201,   202,   203,   204,   205,   206,
     207,   208,   209,   210,   211,   212,   213,   214,   215,   216,
     217,   218,   219,   220,   221,   222,   223,   224,    -1,   226,
     227,   228,   229,   230,    -1,   232,   233,   234,   235,   236,
     237,   238,   239,   240,   241,   242,   243,    -1,   245,   246,
     247,   248,   249,   250,    -1,   252,   253,   254,   255,   256,
     257,   258,   259,   260,   261,   262,   263,   264,   265,    -1,
      -1,    -1,   269,   270,   271,   272,   273,    -1,   275,   276,
     277,   278,    -1,    -1,   281,   282,   283,   284,   285,   286,
     287,   288,   289,   290,   291,   292,   293,   294,   295,   296,
     297,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,   350,   351,   352,   353,   354,   355,   356,
      -1,   358,    -1,    -1,    -1,    -1,    -1,    -1,   365,   366,
     367,   368,   369,   370,   371,   372,   373,   374,   375,   376,
     377,   378,   379,   380,   381,   382,   383,   384,   385,   386,
     387,   388,   389,   390,   391,   392,   393,   394,   395,   396,
     397,   398,   399,   400,   401,   402,   403,   404,   405,   406,
     407,   408,   409,   410,   411,   412,   413,   414,   415,   416,
     417,   418,   419,   420,    -1,    -1,   423,     4,     5,     6,
       7,     8,     9,    10,    11,    12,    13,    14,    15,    16,
      17,    18,    19,    -1,    21,    22,    23,    24,    25,    26,
      27,    28,    29,    30,    31,    32,    33,    -1,    35,    36,
      37,    -1,    -1,    -1,    41,    42,    43,    44,    -1,    46,
      47,    48,    49,    50,    51,    -1,    -1,    54,    55,    56,
      57,    58,    59,    60,    61,    62,    63,    64,    65,    -1,
      -1,    -1,    69,    -1,    71,    72,    -1,    -1,    -1,    -1,
      -1,    78,    -1,    80,    81,    82,    83,    84,    85,    -1,
      87,    88,    89,    90,    91,    92,    93,    94,    95,    -1,
      -1,    98,    99,   100,   101,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   115,   116,
      -1,    -1,    -1,   120,    -1,    -1,   123,   124,    -1,    -1,
      -1,   128,   129,   130,   131,    -1,   133,   134,   135,   136,
     137,   138,   139,   140,   141,   142,   143,   144,   145,    -1,
     147,   148,   149,   150,   151,   152,   153,   154,   155,   156,
     157,   158,   159,   160,   161,   162,   163,   164,   165,   166,
     167,    -1,   169,   170,   171,   172,   173,   174,   175,   176,
     177,   178,   179,   180,   181,   182,   183,   184,    -1,   186,
      -1,   188,   189,   190,   191,   192,   193,   194,   195,   196,
     197,   198,   199,   200,   201,   202,   203,   204,   205,   206,
     207,   208,   209,   210,   211,   212,   213,   214,   215,   216,
     217,   218,   219,   220,   221,   222,   223,   224,    -1,   226,
     227,   228,   229,   230,    -1,   232,   233,   234,   235,   236,
     237,   238,   239,   240,   241,   242,   243,    -1,   245,   246,
     247,   248,   249,   250,    -1,   252,   253,   254,   255,   256,
     257,   258,   259,   260,   261,   262,   263,   264,   265,    -1,
     267,    -1,   269,   270,   271,   272,   273,   274,   275,   276,
     277,   278,    -1,    -1,   281,   282,   283,   284,   285,   286,
     287,   288,   289,   290,   291,   292,   293,   294,   295,   296,
     297,   298,   299,   300,    -1,   302,   303,   304,   305,   306,
     307,   308,   309,    -1,   311,   312,    -1,   314,   315,   316,
     317,   318,   319,   320,   321,   322,   323,   324,   325,   326,
     327,   328,   329,   330,   331,   332,   333,   334,   335,   336,
     337,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,   349,   350,   351,   352,   353,   354,   355,   356,
     357,   358,    -1,    -1,   361,   362,   363,   364,   365,   366,
     367,   368,   369,   370,   371,   372,   373,   374,   375,   376,
     377,   378,   379,   380,   381,   382,   383,   384,   385,   386,
     387,   388,   389,   390,   391,   392,   393,   394,   395,   396,
     397,   398,   399,   400,   401,   402,   403,   404,   405,   406,
     407,   408,   409,   410,   411,   412,   413,   414,   415,   416,
     417,   418,   419,   420,   421,     4,     5,     6,     7,     8,
       9,    10,    11,    12,    13,    14,    15,    16,    17,    18,
      19,    -1,    21,    22,    23,    24,    25,    26,    27,    28,
      29,    30,    31,    32,    33,    -1,    35,    36,    37,    -1,
      -1,    -1,    41,    42,    43,    44,    -1,    46,    47,    48,
      49,    50,    51,    -1,    -1,    54,    55,    56,    57,    58,
      59,    60,    61,    62,    63,    64,    65,    -1,    -1,    -1,
      69,    -1,    71,    72,    -1,    -1,    -1,    -1,    -1,    78,
      -1,    80,    81,    82,    83,    84,    85,    -1,    87,    88,
      89,    90,    91,    92,    93,    94,    -1,    -1,    -1,    98,
      99,   100,   101,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,   115,   116,    -1,    -1,
      -1,   120,    -1,    -1,   123,   124,    -1,    -1,    -1,   128,
     129,   130,   131,    -1,   133,   134,   135,   136,   137,   138,
     139,   140,   141,   142,   143,   144,   145,    -1,   147,   148,
     149,   150,   151,   152,   153,   154,   155,   156,   157,   158,
     159,   160,   161,   162,   163,   164,   165,   166,   167,    -1,
     169,   170,   171,   172,   173,   174,   175,   176,   177,   178,
     179,   180,   181,   182,   183,   184,    -1,   186,    -1,   188,
     189,   190,   191,   192,   193,   194,   195,   196,   197,   198,
     199,   200,   201,   202,   203,   204,   205,   206,   207,   208,
     209,   210,   211,   212,   213,   214,   215,   216,   217,   218,
     219,   220,   221,   222,   223,   224,    -1,   226,   227,   228,
     229,   230,    -1,   232,   233,   234,   235,   236,   237,   238,
     239,   240,   241,   242,   243,    -1,   245,   246,   247,   248,
     249,   250,    -1,   252,   253,   254,   255,   256,   257,   258,
     259,   260,   261,   262,   263,   264,   265,    -1,   267,    -1,
     269,   270,   271,   272,   273,   274,   275,   276,   277,   278,
      -1,    -1,   281,   282,   283,   284,   285,   286,   287,   288,
     289,   290,   291,   292,   293,   294,   295,   296,   297,   298,
     299,   300,    -1,   302,   303,   304,   305,   306,   307,   308,
     309,    -1,   311,   312,    -1,   314,   315,   316,   317,   318,
     319,   320,   321,   322,   323,   324,   325,   326,   327,   328,
     329,   330,   331,   332,   333,   334,   335,   336,   337,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
     349,   350,   351,   352,   353,   354,   355,   356,   357,   358,
      -1,    -1,   361,   362,   363,   364,   365,   366,   367,   368,
     369,   370,   371,   372,   373,   374,   375,   376,   377,   378,
     379,   380,   381,   382,   383,   384,   385,   386,   387,   388,
     389,   390,   391,   392,   393,   394,   395,   396,   397,   398,
     399,   400,   401,   402,   403,   404,   405,   406,   407,   408,
     409,   410,   411,   412,   413,   414,   415,   416,   417,   418,
     419,   420,   421,     4,     5,     6,     7,     8,     9,    10,
      11,    12,    13,    14,    15,    16,    17,    18,    19,    -1,
      21,    22,    23,    24,    25,    26,    27,    28,    29,    30,
      31,    32,    33,    -1,    35,    36,    37,    -1,    -1,    -1,
      41,    42,    43,    44,    -1,    46,    47,    48,    49,    50,
      51,    -1,    -1,    54,    55,    56,    57,    58,    59,    60,
      61,    62,    63,    64,    65,    66,    -1,    -1,    69,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    78,    -1,    80,
      81,    82,    83,    84,    85,    -1,    87,    88,    89,    90,
      91,    92,    93,    94,    -1,    -1,    -1,    98,    99,   100,
     101,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,   115,   116,    -1,    -1,    -1,   120,
      -1,    -1,   123,   124,    -1,    -1,    -1,   128,   129,   130,
     131,    -1,   133,   134,   135,   136,   137,   138,   139,   140,
     141,   142,   143,   144,   145,    -1,   147,   148,   149,   150,
     151,   152,   153,   154,   155,   156,   157,   158,   159,   160,
     161,   162,   163,   164,   165,   166,   167,    -1,   169,   170,
     171,   172,   173,   174,   175,   176,   177,   178,   179,   180,
     181,   182,   183,   184,    -1,   186,    -1,   188,   189,   190,
     191,   192,   193,   194,   195,   196,   197,   198,   199,   200,
     201,   202,   203,   204,   205,   206,   207,   208,   209,   210,
     211,   212,   213,   214,   215,   216,   217,   218,   219,   220,
     221,   222,   223,   224,    -1,   226,   227,   228,   229,   230,
      -1,   232,   233,   234,   235,   236,   237,   238,   239,   240,
     241,   242,   243,    -1,   245,   246,   247,   248,   249,   250,
      -1,   252,   253,   254,   255,   256,   257,   258,   259,   260,
     261,   262,   263,   264,   265,    -1,   267,    -1,   269,   270,
     271,   272,   273,   274,   275,   276,   277,   278,    -1,    -1,
     281,   282,   283,   284,   285,   286,   287,   288,   289,   290,
     291,   292,   293,   294,   295,   296,   297,   298,   299,   300,
      -1,   302,   303,   304,   305,   306,   307,   308,   309,    -1,
     311,   312,    -1,   314,   315,   316,   317,   318,   319,   320,
     321,   322,   323,   324,   325,   326,   327,   328,   329,   330,
     331,   332,   333,   334,   335,   336,   337,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   349,   350,
     351,   352,   353,   354,   355,   356,   357,   358,    -1,    -1,
     361,   362,   363,   364,   365,   366,   367,   368,   369,   370,
     371,   372,   373,   374,   375,   376,   377,   378,   379,   380,
     381,   382,   383,   384,   385,   386,   387,   388,   389,   390,
     391,   392,   393,   394,   395,   396,   397,   398,   399,   400,
     401,   402,   403,   404,   405,   406,   407,   408,   409,   410,
     411,   412,   413,   414,   415,   416,   417,   418,   419,   420,
     421,     4,     5,     6,     7,     8,     9,    10,    11,    12,
      13,    14,    15,    16,    17,    18,    19,    -1,    21,    22,
      23,    24,    25,    26,    27,    28,    29,    30,    31,    32,
      33,    -1,    35,    36,    37,    -1,    -1,    -1,    41,    42,
      43,    44,    -1,    46,    47,    48,    49,    50,    51,    -1,
      -1,    54,    55,    56,    57,    58,    59,    60,    61,    62,
      63,    64,    65,    -1,    -1,    -1,    69,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    78,    -1,    80,    81,    82,
      83,    84,    85,    -1,    87,    88,    89,    90,    91,    92,
      93,    94,    -1,    -1,    -1,    98,    99,   100,   101,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   112,
      -1,    -1,   115,   116,    -1,    -1,    -1,   120,    -1,    -1,
     123,   124,    -1,    -1,    -1,   128,   129,   130,   131,    -1,
     133,   134,   135,   136,   137,   138,   139,   140,   141,   142,
     143,   144,   145,    -1,   147,   148,   149,   150,   151,   152,
     153,   154,   155,   156,   157,   158,   159,   160,   161,   162,
     163,   164,   165,   166,   167,    -1,   169,   170,   171,   172,
     173,   174,   175,   176,   177,   178,   179,   180,   181,   182,
     183,   184,    -1,   186,    -1,   188,   189,   190,   191,   192,
     193,   194,   195,   196,   197,   198,   199,   200,   201,   202,
     203,   204,   205,   206,   207,   208,   209,   210,   211,   212,
     213,   214,   215,   216,   217,   218,   219,   220,   221,   222,
     223,   224,    -1,   226,   227,   228,   229,   230,    -1,   232,
     233,   234,   235,   236,   237,   238,   239,   240,   241,   242,
     243,    -1,   245,   246,   247,   248,   249,   250,    -1,   252,
     253,   254,   255,   256,   257,   258,   259,   260,   261,   262,
     263,   264,   265,    -1,   267,    -1,   269,   270,   271,   272,
     273,   274,   275,   276,   277,   278,    -1,    -1,   281,   282,
     283,   284,   285,   286,   287,   288,   289,   290,   291,   292,
     293,   294,   295,   296,   297,   298,   299,   300,    -1,   302,
     303,   304,   305,   306,   307,   308,   309,    -1,   311,   312,
      -1,   314,   315,   316,   317,   318,   319,   320,   321,   322,
     323,   324,   325,   326,   327,   328,   329,   330,   331,   332,
     333,   334,   335,   336,   337,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,   349,   350,   351,   352,
     353,   354,   355,   356,   357,   358,    -1,    -1,   361,   362,
     363,   364,   365,   366,   367,   368,   369,   370,   371,   372,
     373,   374,   375,   376,   377,   378,   379,   380,   381,   382,
     383,   384,   385,   386,   387,   388,   389,   390,   391,   392,
     393,   394,   395,   396,   397,   398,   399,   400,   401,   402,
     403,   404,   405,   406,   407,   408,   409,   410,   411,   412,
     413,   414,   415,   416,   417,   418,   419,   420,   421,     4,
       5,     6,     7,     8,     9,    10,    11,    12,    13,    14,
      15,    16,    17,    18,    19,    -1,    21,    22,    23,    24,
      25,    26,    27,    28,    29,    30,    31,    32,    33,    -1,
      35,    36,    37,    -1,    -1,    -1,    41,    42,    43,    44,
      -1,    46,    47,    48,    49,    50,    51,    -1,    -1,    54,
      55,    56,    57,    58,    59,    60,    61,    62,    63,    64,
      65,    -1,    -1,    -1,    69,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    78,    -1,    80,    81,    82,    83,    84,
      85,    -1,    87,    88,    89,    90,    91,    92,    93,    94,
      -1,    -1,    -1,    98,    99,   100,   101,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,   112,    -1,    -1,
     115,   116,    -1,    -1,    -1,   120,    -1,    -1,   123,   124,
      -1,    -1,    -1,   128,   129,   130,   131,    -1,   133,   134,
     135,   136,   137,   138,   139,   140,   141,   142,   143,   144,
     145,    -1,   147,   148,   149,   150,   151,   152,   153,   154,
     155,   156,   157,   158,   159,   160,   161,   162,   163,   164,
     165,   166,   167,    -1,   169,   170,   171,   172,   173,   174,
     175,   176,   177,   178,   179,   180,   181,   182,   183,   184,
      -1,   186,    -1,   188,   189,   190,   191,   192,   193,   194,
     195,   196,   197,   198,   199,   200,   201,   202,   203,   204,
     205,   206,   207,   208,   209,   210,   211,   212,   213,   214,
     215,   216,   217,   218,   219,   220,   221,   222,   223,   224,
      -1,   226,   227,   228,   229,   230,    -1,   232,   233,   234,
     235,   236,   237,   238,   239,   240,   241,   242,   243,    -1,
     245,   246,   247,   248,   249,   250,    -1,   252,   253,   254,
     255,   256,   257,   258,   259,   260,   261,   262,   263,   264,
     265,    -1,   267,    -1,   269,   270,   271,   272,   273,   274,
     275,   276,   277,   278,    -1,    -1,   281,   282,   283,   284,
     285,   286,   287,   288,   289,   290,   291,   292,   293,   294,
     295,   296,   297,   298,   299,   300,    -1,   302,   303,   304,
     305,   306,   307,   308,   309,    -1,   311,   312,    -1,   314,
     315,   316,   317,   318,   319,   320,   321,   322,   323,   324,
     325,   326,   327,   328,   329,   330,   331,   332,   333,   334,
     335,   336,   337,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,   349,   350,   351,   352,   353,   354,
     355,   356,   357,   358,    -1,    -1,   361,   362,   363,   364,
     365,   366,   367,   368,   369,   370,   371,   372,   373,   374,
     375,   376,   377,   378,   379,   380,   381,   382,   383,   384,
     385,   386,   387,   388,   389,   390,   391,   392,   393,   394,
     395,   396,   397,   398,   399,   400,   401,   402,   403,   404,
     405,   406,   407,   408,   409,   410,   411,   412,   413,   414,
     415,   416,   417,   418,   419,   420,   421,     4,     5,     6,
       7,     8,     9,    10,    11,    12,    13,    14,    15,    16,
      17,    18,    19,    -1,    21,    22,    23,    24,    25,    26,
      27,    28,    29,    30,    31,    32,    33,    -1,    35,    36,
      37,    -1,    -1,    -1,    41,    42,    43,    44,    -1,    46,
      47,    48,    49,    50,    51,    -1,    -1,    54,    55,    56,
      57,    58,    59,    60,    61,    62,    63,    64,    65,    -1,
      -1,    -1,    69,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    78,    -1,    80,    81,    82,    83,    84,    85,    -1,
      87,    88,    89,    90,    91,    92,    93,    94,    -1,    -1,
      -1,    98,    99,   100,   101,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,   112,    -1,    -1,   115,   116,
      -1,    -1,    -1,   120,    -1,    -1,   123,   124,    -1,    -1,
      -1,   128,   129,   130,   131,    -1,   133,   134,   135,   136,
     137,   138,   139,   140,   141,   142,   143,   144,   145,    -1,
     147,   148,   149,   150,   151,   152,   153,   154,   155,   156,
     157,   158,   159,   160,   161,   162,   163,   164,   165,   166,
     167,    -1,   169,   170,   171,   172,   173,   174,   175,   176,
     177,   178,   179,   180,   181,   182,   183,   184,    -1,   186,
      -1,   188,   189,   190,   191,   192,   193,   194,   195,   196,
     197,   198,   199,   200,   201,   202,   203,   204,   205,   206,
     207,   208,   209,   210,   211,   212,   213,   214,   215,   216,
     217,   218,   219,   220,   221,   222,   223,   224,    -1,   226,
     227,   228,   229,   230,    -1,   232,   233,   234,   235,   236,
     237,   238,   239,   240,   241,   242,   243,    -1,   245,   246,
     247,   248,   249,   250,    -1,   252,   253,   254,   255,   256,
     257,   258,   259,   260,   261,   262,   263,   264,   265,    -1,
     267,    -1,   269,   270,   271,   272,   273,   274,   275,   276,
     277,   278,    -1,    -1,   281,   282,   283,   284,   285,   286,
     287,   288,   289,   290,   291,   292,   293,   294,   295,   296,
     297,   298,   299,   300,    -1,   302,   303,   304,   305,   306,
     307,   308,   309,    -1,   311,   312,    -1,   314,   315,   316,
     317,   318,   319,   320,   321,   322,   323,   324,   325,   326,
     327,   328,   329,   330,   331,   332,   333,   334,   335,   336,
     337,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,   349,   350,   351,   352,   353,   354,   355,   356,
     357,   358,    -1,    -1,   361,   362,   363,   364,   365,   366,
     367,   368,   369,   370,   371,   372,   373,   374,   375,   376,
     377,   378,   379,   380,   381,   382,   383,   384,   385,   386,
     387,   388,   389,   390,   391,   392,   393,   394,   395,   396,
     397,   398,   399,   400,   401,   402,   403,   404,   405,   406,
     407,   408,   409,   410,   411,   412,   413,   414,   415,   416,
     417,   418,   419,   420,   421,     4,     5,     6,     7,     8,
       9,    10,    11,    12,    13,    14,    15,    16,    17,    18,
      19,    -1,    21,    22,    23,    24,    25,    26,    27,    28,
      29,    30,    31,    32,    33,    -1,    35,    36,    37,    -1,
      -1,    -1,    41,    42,    43,    44,    -1,    46,    47,    48,
      49,    50,    51,    -1,    -1,    54,    55,    56,    57,    58,
      59,    60,    61,    62,    63,    64,    65,    -1,    -1,    -1,
      69,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    78,
      -1,    80,    81,    82,    83,    84,    85,    -1,    87,    88,
      89,    90,    91,    92,    93,    94,    -1,    -1,    -1,    98,
      99,   100,   101,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,   115,   116,    -1,    -1,
      -1,   120,    -1,    -1,   123,   124,    -1,    -1,    -1,   128,
     129,   130,   131,    -1,   133,   134,   135,   136,   137,   138,
     139,   140,   141,   142,   143,   144,   145,    -1,   147,   148,
     149,   150,   151,   152,   153,   154,   155,   156,   157,   158,
     159,   160,   161,   162,   163,   164,   165,   166,   167,    -1,
     169,   170,   171,   172,   173,   174,   175,   176,   177,   178,
     179,   180,   181,   182,   183,   184,    -1,   186,    -1,   188,
     189,   190,   191,   192,   193,   194,   195,   196,   197,   198,
     199,   200,   201,   202,   203,   204,   205,   206,   207,   208,
     209,   210,   211,   212,   213,   214,   215,   216,   217,   218,
     219,   220,   221,   222,   223,   224,    -1,   226,   227,   228,
     229,   230,    -1,   232,   233,   234,   235,   236,   237,   238,
     239,   240,   241,   242,   243,    -1,   245,   246,   247,   248,
     249,   250,    -1,   252,   253,   254,   255,   256,   257,   258,
     259,   260,   261,   262,   263,   264,   265,    -1,   267,    -1,
     269,   270,   271,   272,   273,   274,   275,   276,   277,   278,
      -1,    -1,   281,   282,   283,   284,   285,   286,   287,   288,
     289,   290,   291,   292,   293,   294,   295,   296,   297,   298,
     299,   300,    -1,   302,   303,   304,   305,   306,   307,   308,
     309,    -1,   311,   312,    -1,   314,   315,   316,   317,   318,
     319,   320,   321,   322,   323,   324,   325,   326,   327,   328,
     329,   330,   331,   332,   333,   334,   335,   336,   337,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
     349,   350,   351,   352,   353,   354,   355,   356,   357,   358,
      -1,    -1,   361,   362,   363,   364,   365,   366,   367,   368,
     369,   370,   371,   372,   373,   374,   375,   376,   377,   378,
     379,   380,   381,   382,   383,   384,   385,   386,   387,   388,
     389,   390,   391,   392,   393,   394,   395,   396,   397,   398,
     399,   400,   401,   402,   403,   404,   405,   406,   407,   408,
     409,   410,   411,   412,   413,   414,   415,   416,   417,   418,
     419,   420,   421,     4,     5,     6,     7,     8,     9,    10,
      11,    12,    13,    14,    15,    16,    17,    18,    19,    -1,
      21,    22,    23,    24,    25,    26,    27,    28,    29,    30,
      31,    32,    33,    -1,    35,    36,    37,    -1,    -1,    -1,
      41,    42,    43,    44,    -1,    46,    47,    48,    49,    50,
      51,    -1,    -1,    54,    55,    56,    57,    58,    59,    60,
      61,    62,    63,    64,    65,    -1,    -1,    -1,    69,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    78,    -1,    80,
      81,    82,    83,    84,    85,    -1,    87,    88,    89,    90,
      91,    92,    93,    94,    -1,    -1,    -1,    98,    99,   100,
     101,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,   115,   116,    -1,    -1,    -1,   120,
      -1,    -1,   123,   124,    -1,    -1,    -1,   128,   129,   130,
     131,    -1,   133,   134,   135,   136,   137,   138,   139,   140,
     141,   142,   143,   144,   145,    -1,   147,   148,   149,   150,
     151,   152,   153,   154,   155,   156,   157,   158,   159,   160,
     161,   162,   163,   164,   165,   166,   167,    -1,   169,   170,
     171,   172,   173,   174,   175,   176,   177,   178,   179,   180,
     181,   182,   183,   184,    -1,   186,    -1,   188,   189,   190,
     191,   192,   193,   194,   195,   196,   197,   198,   199,   200,
     201,   202,   203,   204,   205,   206,   207,   208,   209,   210,
     211,   212,   213,   214,   215,   216,   217,   218,   219,   220,
     221,   222,   223,   224,    -1,   226,   227,   228,   229,   230,
      -1,   232,   233,   234,   235,   236,   237,   238,   239,   240,
     241,   242,   243,    -1,   245,   246,   247,   248,   249,   250,
      -1,   252,   253,   254,   255,   256,   257,   258,   259,   260,
     261,   262,   263,   264,   265,    -1,   267,    -1,   269,   270,
     271,   272,   273,   274,   275,   276,   277,   278,    -1,    -1,
     281,   282,   283,   284,   285,   286,   287,   288,   289,   290,
     291,   292,   293,   294,   295,   296,   297,   298,   299,   300,
      -1,   302,   303,   304,   305,   306,   307,   308,   309,    -1,
     311,   312,    -1,   314,   315,   316,   317,   318,   319,   320,
     321,   322,   323,   324,   325,   326,   327,   328,   329,   330,
     331,   332,   333,   334,   335,   336,   337,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   349,   350,
     351,   352,   353,   354,   355,   356,   357,   358,    -1,    -1,
     361,   362,   363,   364,   365,   366,   367,   368,   369,   370,
     371,   372,   373,   374,   375,   376,   377,   378,   379,   380,
     381,   382,   383,   384,   385,   386,   387,   388,   389,   390,
     391,   392,   393,   394,   395,   396,   397,   398,   399,   400,
     401,   402,   403,   404,   405,   406,   407,   408,   409,   410,
     411,   412,   413,   414,   415,   416,   417,   418,   419,   420,
     421,     4,     5,     6,     7,     8,     9,    10,    11,    12,
      13,    14,    15,    16,    17,    18,    19,    -1,    21,    22,
      23,    24,    25,    26,    27,    28,    29,    30,    31,    32,
      33,    -1,    35,    36,    37,    -1,    -1,    -1,    41,    42,
      43,    44,    -1,    46,    47,    48,    49,    50,    51,    -1,
      -1,    54,    55,    56,    57,    58,    59,    60,    61,    62,
      63,    64,    65,    -1,    -1,    -1,    69,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    78,    -1,    80,    81,    82,
      83,    84,    85,    -1,    87,    88,    89,    90,    91,    92,
      93,    94,    -1,    -1,    -1,    98,    99,   100,   101,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,   115,   116,    -1,    -1,    -1,   120,    -1,    -1,
     123,   124,    -1,    -1,    -1,   128,   129,   130,   131,    -1,
     133,   134,   135,   136,   137,   138,   139,   140,   141,   142,
     143,   144,   145,    -1,   147,   148,   149,   150,   151,   152,
     153,   154,   155,   156,   157,   158,   159,   160,   161,   162,
     163,   164,   165,   166,   167,    -1,   169,   170,   171,   172,
     173,   174,   175,   176,   177,   178,   179,   180,   181,   182,
     183,   184,    -1,   186,    -1,   188,   189,   190,   191,   192,
     193,   194,   195,   196,   197,   198,   199,   200,   201,   202,
     203,   204,   205,   206,   207,   208,   209,   210,   211,   212,
     213,   214,   215,   216,   217,   218,   219,   220,   221,   222,
     223,   224,    -1,   226,   227,   228,   229,   230,    -1,   232,
     233,   234,   235,   236,   237,   238,   239,   240,   241,   242,
     243,    -1,   245,   246,   247,   248,   249,   250,    -1,   252,
     253,   254,   255,   256,   257,   258,   259,   260,   261,   262,
     263,   264,   265,    -1,   267,    -1,   269,   270,   271,   272,
     273,   274,   275,   276,   277,   278,    -1,    -1,   281,   282,
     283,   284,   285,   286,   287,   288,   289,   290,   291,   292,
     293,   294,   295,   296,   297,   298,   299,   300,    -1,   302,
     303,   304,   305,   306,   307,   308,   309,    -1,   311,   312,
      -1,   314,   315,   316,   317,   318,   319,   320,   321,   322,
     323,   324,   325,   326,   327,   328,   329,   330,   331,   332,
     333,   334,   335,   336,   337,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,   349,   350,   351,   352,
     353,   354,   355,   356,   357,   358,    -1,    -1,   361,   362,
     363,   364,   365,   366,   367,   368,   369,   370,   371,   372,
     373,   374,   375,   376,   377,   378,   379,   380,   381,   382,
     383,   384,   385,   386,   387,   388,   389,   390,   391,   392,
     393,   394,   395,   396,   397,   398,   399,   400,   401,   402,
     403,   404,   405,   406,   407,   408,   409,   410,   411,   412,
     413,   414,   415,   416,   417,   418,   419,   420,   421,     4,
       5,     6,     7,     8,     9,    10,    11,    12,    13,    14,
      15,    16,    17,    18,    19,    -1,    21,    22,    23,    24,
      25,    26,    27,    28,    29,    30,    31,    32,    33,    -1,
      35,    36,    37,    -1,    -1,    -1,    41,    42,    43,    44,
      -1,    46,    47,    48,    49,    50,    51,    -1,    -1,    54,
      55,    56,    57,    58,    59,    60,    61,    62,    63,    64,
      65,    -1,    -1,    -1,    69,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    78,    -1,    80,    81,    82,    83,    84,
      85,    -1,    87,    88,    89,    90,    91,    92,    93,    94,
      -1,    -1,    -1,    98,    99,   100,   101,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
     115,   116,    -1,    -1,    -1,   120,    -1,    -1,   123,   124,
      -1,    -1,    -1,   128,   129,   130,   131,    -1,   133,   134,
     135,   136,   137,   138,   139,   140,   141,   142,   143,   144,
     145,    -1,   147,   148,   149,   150,   151,   152,   153,   154,
     155,   156,   157,   158,   159,   160,   161,   162,   163,   164,
     165,   166,   167,    -1,   169,   170,   171,   172,   173,   174,
     175,   176,   177,   178,   179,   180,   181,   182,   183,   184,
      -1,   186,    -1,   188,   189,   190,   191,   192,   193,   194,
     195,   196,   197,   198,   199,   200,   201,   202,   203,   204,
     205,   206,   207,   208,   209,   210,   211,   212,   213,   214,
     215,   216,   217,   218,   219,   220,   221,   222,   223,   224,
      -1,   226,   227,   228,   229,   230,    -1,   232,   233,   234,
     235,   236,   237,   238,   239,   240,   241,   242,   243,    -1,
     245,   246,   247,   248,   249,   250,    -1,   252,   253,   254,
     255,   256,   257,   258,   259,   260,   261,   262,   263,   264,
     265,    -1,   267,    -1,   269,   270,   271,   272,   273,   274,
     275,   276,   277,   278,    -1,    -1,   281,   282,   283,   284,
     285,   286,   287,   288,   289,   290,   291,   292,   293,   294,
     295,   296,   297,   298,   299,   300,    -1,   302,   303,   304,
     305,   306,   307,   308,   309,    -1,   311,   312,    -1,   314,
     315,   316,   317,   318,   319,   320,   321,   322,   323,   324,
     325,   326,   327,   328,   329,   330,   331,   332,   333,   334,
     335,   336,   337,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,   349,   350,   351,   352,   353,   354,
     355,   356,   357,   358,    -1,    -1,   361,   362,   363,   364,
     365,   366,   367,   368,   369,   370,   371,   372,   373,   374,
     375,   376,   377,   378,   379,   380,   381,   382,   383,   384,
     385,   386,   387,   388,   389,   390,   391,   392,   393,   394,
     395,   396,   397,   398,   399,   400,   401,   402,   403,   404,
     405,   406,   407,   408,   409,   410,   411,   412,   413,   414,
     415,   416,   417,   418,   419,   420,   421,     5,     6,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    17,
      -1,    -1,    -1,    -1,    -1,    23,    -1,    -1,    -1,    27,
      28,    -1,    -1,    -1,    32,    -1,    -1,    35,    36,    37,
      -1,    -1,    -1,    41,    42,    -1,    -1,    -1,    46,    47,
      -1,    49,    50,    51,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    60,    61,    -1,    -1,    -1,    -1,    66,    -1,
      -1,    69,    70,    71,    72,    73,    74,    75,    -1,    -1,
      78,    79,    80,    81,    82,    -1,    -1,    -1,    86,    87,
      -1,    -1,    -1,    -1,    -1,    93,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,   110,   111,    -1,    -1,    -1,   115,    -1,    -1,
     118,    -1,    -1,   121,   122,   123,   124,    -1,    -1,    -1,
      -1,   129,    -1,    -1,    -1,    -1,   134,   135,   136,   137,
      -1,    -1,   140,   141,    -1,    -1,   144,   145,    -1,   147,
     148,   149,   150,   151,   152,   153,   154,   155,   156,   157,
     158,    -1,    -1,    -1,    -1,   163,    -1,   165,   166,   167,
      -1,    -1,   170,   171,   172,   173,   174,   175,   176,   177,
     178,   179,   180,   181,   182,   183,   184,    -1,   186,    -1,
     188,   189,   190,    -1,    -1,    -1,   194,    -1,    -1,    -1,
     198,   199,   200,   201,   202,   203,   204,   205,   206,   207,
     208,   209,   210,   211,   212,   213,   214,   215,   216,   217,
     218,   219,   220,   221,   222,   223,   224,    -1,   226,   227,
     228,   229,   230,    -1,   232,   233,   234,   235,   236,   237,
     238,   239,   240,   241,   242,   243,    -1,   245,   246,   247,
     248,   249,   250,    -1,   252,   253,   254,   255,   256,   257,
     258,   259,   260,   261,   262,   263,   264,   265,    -1,    -1,
      -1,   269,   270,   271,   272,   273,    -1,   275,   276,   277,
     278,    -1,    -1,   281,   282,   283,   284,   285,   286,   287,
     288,   289,   290,   291,   292,   293,   294,   295,   296,   297,
     298,   299,   300,   301,   302,   303,   304,   305,   306,   307,
     308,   309,   310,   311,   312,   313,    -1,   315,   316,    -1,
      -1,    -1,    -1,   321,   322,   323,   324,   325,   326,   327,
     328,   329,   330,   331,   332,   333,   334,   335,   336,   337,
     338,   339,   340,   341,   342,   343,   344,   345,   346,   347,
     348,   349,   350,   351,   352,   353,   354,   355,   356,    -1,
     358,    -1,    -1,    -1,    -1,    -1,    -1,   365,   366,   367,
     368,   369,   370,   371,   372,   373,   374,   375,   376,   377,
     378,   379,   380,   381,   382,   383,   384,   385,   386,   387,
     388,   389,   390,   391,   392,   393,   394,   395,   396,   397,
     398,   399,   400,   401,   402,   403,   404,   405,   406,   407,
     408,   409,   410,   411,   412,   413,   414,   415,   416,   417,
     418,   419,   420,     5,     6,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    17,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    27,    -1,    -1,    -1,    -1,
      32,    -1,    -1,    35,    36,    37,    -1,    -1,    -1,    41,
      42,    -1,    -1,    -1,    -1,    47,    -1,    49,    50,    51,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    66,    -1,    -1,    69,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    78,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,   129,    -1,    -1,
      -1,    -1,   134,   135,   136,    -1,    -1,    -1,   140,   141,
      -1,    -1,    -1,   145,    -1,   147,   148,   149,   150,   151,
     152,   153,   154,   155,   156,   157,    -1,    -1,    -1,    -1,
      -1,   163,    -1,   165,   166,   167,    -1,    -1,   170,   171,
     172,   173,   174,   175,   176,   177,   178,   179,   180,   181,
     182,   183,   184,    -1,   186,    -1,   188,   189,   190,    -1,
      -1,    -1,   194,    -1,    -1,    -1,   198,   199,   200,   201,
     202,   203,   204,   205,   206,   207,   208,   209,   210,   211,
     212,   213,   214,   215,   216,   217,   218,   219,   220,   221,
     222,   223,   224,    -1,   226,   227,   228,   229,   230,    -1,
     232,   233,   234,   235,   236,   237,   238,   239,   240,   241,
     242,   243,    -1,   245,   246,   247,   248,   249,   250,    -1,
     252,   253,   254,   255,   256,   257,   258,   259,   260,   261,
     262,   263,   264,   265,    -1,    -1,    -1,   269,   270,   271,
     272,   273,    -1,   275,   276,   277,   278,    -1,    -1,   281,
     282,   283,   284,   285,   286,   287,   288,   289,   290,   291,
     292,   293,   294,   295,   296,   297,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   350,   351,
     352,   353,   354,   355,   356,    -1,   358,    -1,    -1,    -1,
      -1,    -1,    -1,   365,   366,   367,   368,   369,   370,   371,
     372,   373,   374,   375,   376,   377,   378,   379,   380,   381,
     382,   383,   384,   385,   386,   387,   388,   389,   390,   391,
     392,   393,   394,   395,   396,   397,   398,   399,   400,   401,
     402,   403,   404,   405,   406,   407,   408,   409,   410,   411,
     412,   413,   414,   415,   416,   417,   418,   419,   420,     5,
       6,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    17,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    27,    -1,    -1,    -1,    -1,    32,    -1,    -1,    35,
      36,    37,    -1,    -1,    -1,    41,    42,    -1,    -1,    -1,
      -1,    47,    -1,    49,    50,    51,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      66,    -1,    -1,    69,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    78,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,   129,    -1,    -1,    -1,    -1,   134,   135,
     136,    -1,    -1,    -1,   140,   141,    -1,    -1,    -1,   145,
      -1,   147,   148,   149,   150,   151,   152,   153,   154,   155,
     156,   157,    -1,    -1,    -1,    -1,    -1,   163,    -1,   165,
     166,   167,    -1,    -1,   170,   171,   172,   173,   174,   175,
     176,   177,   178,   179,   180,   181,   182,   183,   184,    -1,
     186,    -1,   188,   189,   190,    -1,    -1,    -1,   194,    -1,
      -1,    -1,   198,   199,   200,   201,   202,   203,   204,   205,
     206,   207,   208,   209,   210,   211,   212,   213,   214,   215,
     216,   217,   218,   219,   220,   221,   222,   223,   224,    -1,
     226,   227,   228,   229,   230,    -1,   232,   233,   234,   235,
     236,   237,   238,   239,   240,   241,   242,   243,    -1,   245,
     246,   247,   248,   249,   250,    -1,   252,   253,   254,   255,
     256,   257,   258,   259,   260,   261,   262,   263,   264,   265,
      -1,    -1,    -1,   269,   270,   271,   272,   273,    -1,   275,
     276,   277,   278,    -1,    -1,   281,   282,   283,   284,   285,
     286,   287,   288,   289,   290,   291,   292,   293,   294,   295,
     296,   297,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,   350,   351,   352,   353,   354,   355,
     356,    -1,   358,    -1,    -1,    -1,    -1,    -1,    -1,   365,
     366,   367,   368,   369,   370,   371,   372,   373,   374,   375,
     376,   377,   378,   379,   380,   381,   382,   383,   384,   385,
     386,   387,   388,   389,   390,   391,   392,   393,   394,   395,
     396,   397,   398,   399,   400,   401,   402,   403,   404,   405,
     406,   407,   408,   409,   410,   411,   412,   413,   414,   415,
     416,   417,   418,   419,   420,     6,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    17,    -1,    -1,    -1,
      -1,    -1,    23,    -1,    -1,    -1,    27,    28,    -1,    -1,
      -1,    32,    -1,    -1,    35,    36,    37,    -1,    -1,    -1,
      41,    42,    -1,    -1,    -1,    46,    47,    -1,    49,    50,
      51,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    60,
      61,    -1,    -1,    -1,    65,    66,    -1,    -1,    69,    70,
      71,    72,    73,    74,    75,    -1,    -1,    78,    79,    80,
      81,    82,    83,    -1,    -1,    86,    87,    -1,    -1,    -1,
      -1,    -1,    93,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   110,
     111,    -1,    -1,    -1,   115,    -1,    -1,   118,    -1,    -1,
     121,   122,   123,   124,    -1,    -1,    -1,    -1,   129,    -1,
      -1,    -1,    -1,   134,   135,   136,   137,    -1,    -1,   140,
     141,    -1,    -1,   144,   145,    -1,   147,   148,   149,   150,
     151,   152,   153,   154,   155,   156,   157,   158,    -1,    -1,
      -1,    -1,   163,    -1,   165,   166,   167,    -1,    -1,   170,
     171,   172,   173,   174,   175,   176,   177,   178,   179,   180,
     181,   182,   183,   184,    -1,   186,    -1,   188,   189,   190,
      -1,    -1,    -1,   194,    -1,    -1,    -1,   198,   199,   200,
     201,   202,   203,   204,   205,   206,   207,   208,   209,   210,
     211,   212,   213,   214,   215,   216,   217,   218,   219,   220,
     221,   222,   223,   224,    -1,   226,   227,   228,   229,   230,
      -1,   232,   233,   234,   235,   236,   237,   238,   239,   240,
     241,   242,   243,    -1,   245,   246,   247,   248,   249,   250,
      -1,   252,   253,   254,   255,   256,   257,   258,   259,   260,
     261,   262,   263,   264,   265,    -1,    -1,    -1,   269,   270,
     271,   272,   273,    -1,   275,   276,   277,   278,    -1,    -1,
     281,   282,   283,   284,   285,   286,   287,   288,   289,   290,
     291,   292,   293,   294,   295,   296,   297,   298,   299,   300,
     301,   302,   303,   304,   305,   306,   307,   308,   309,   310,
     311,   312,   313,    -1,   315,   316,    -1,    -1,    -1,    -1,
     321,   322,   323,   324,   325,   326,   327,   328,   329,   330,
     331,   332,   333,   334,   335,   336,   337,   338,   339,   340,
     341,   342,   343,   344,   345,   346,   347,   348,   349,   350,
     351,   352,   353,   354,   355,   356,    -1,   358,    -1,    -1,
      -1,    -1,    -1,    -1,   365,   366,   367,   368,   369,   370,
     371,   372,   373,   374,   375,   376,   377,   378,   379,   380,
     381,   382,   383,   384,   385,   386,   387,   388,   389,   390,
     391,   392,   393,   394,   395,   396,   397,   398,   399,   400,
     401,   402,   403,   404,   405,   406,   407,   408,   409,   410,
     411,   412,   413,   414,   415,   416,   417,   418,   419,   420,
       6,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    17,    -1,    -1,    -1,    -1,    -1,    23,    -1,    -1,
      -1,    27,    28,    -1,    -1,    -1,    32,    -1,    -1,    35,
      36,    37,    -1,    -1,    -1,    41,    42,    -1,    -1,    -1,
      46,    47,    -1,    49,    50,    51,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    60,    61,    -1,    -1,    -1,    -1,
      66,    -1,    -1,    69,    70,    71,    72,    73,    74,    75,
      -1,    -1,    78,    79,    80,    81,    82,    -1,    -1,    -1,
      86,    87,    -1,    -1,    -1,    -1,    -1,    93,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,   110,   111,   112,    -1,    -1,   115,
      -1,    -1,   118,    -1,    -1,   121,   122,   123,   124,    -1,
      -1,    -1,    -1,   129,    -1,    -1,    -1,    -1,   134,   135,
     136,   137,    -1,    -1,   140,   141,    -1,    -1,   144,   145,
      -1,   147,   148,   149,   150,   151,   152,   153,   154,   155,
     156,   157,   158,    -1,    -1,    -1,    -1,   163,    -1,   165,
     166,   167,    -1,    -1,   170,   171,   172,   173,   174,   175,
     176,   177,   178,   179,   180,   181,   182,   183,   184,    -1,
     186,    -1,   188,   189,   190,    -1,    -1,    -1,   194,    -1,
      -1,    -1,   198,   199,   200,   201,   202,   203,   204,   205,
     206,   207,   208,   209,   210,   211,   212,   213,   214,   215,
     216,   217,   218,   219,   220,   221,   222,   223,   224,    -1,
     226,   227,   228,   229,   230,    -1,   232,   233,   234,   235,
     236,   237,   238,   239,   240,   241,   242,   243,    -1,   245,
     246,   247,   248,   249,   250,    -1,   252,   253,   254,   255,
     256,   257,   258,   259,   260,   261,   262,   263,   264,   265,
      -1,    -1,    -1,   269,   270,   271,   272,   273,    -1,   275,
     276,   277,   278,    -1,    -1,   281,   282,   283,   284,   285,
     286,   287,   288,   289,   290,   291,   292,   293,   294,   295,
     296,   297,   298,   299,   300,   301,   302,   303,   304,   305,
     306,   307,   308,   309,   310,   311,   312,   313,    -1,   315,
     316,    -1,    -1,    -1,    -1,   321,   322,   323,   324,   325,
     326,   327,   328,   329,   330,   331,   332,   333,   334,   335,
     336,   337,   338,   339,   340,   341,   342,   343,   344,   345,
     346,   347,   348,   349,   350,   351,   352,   353,   354,   355,
     356,    -1,   358,    -1,    -1,    -1,    -1,    -1,    -1,   365,
     366,   367,   368,   369,   370,   371,   372,   373,   374,   375,
     376,   377,   378,   379,   380,   381,   382,   383,   384,   385,
     386,   387,   388,   389,   390,   391,   392,   393,   394,   395,
     396,   397,   398,   399,   400,   401,   402,   403,   404,   405,
     406,   407,   408,   409,   410,   411,   412,   413,   414,   415,
     416,   417,   418,   419,   420,     6,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    17,    -1,    -1,    -1,
      -1,    -1,    23,    -1,    -1,    -1,    27,    28,    -1,    -1,
      -1,    32,    -1,    -1,    35,    36,    37,    -1,    -1,    -1,
      41,    42,    -1,    -1,    -1,    46,    47,    -1,    49,    50,
      51,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    60,
      61,    -1,    -1,    -1,    -1,    66,    -1,    68,    69,    70,
      71,    72,    73,    74,    75,    -1,    -1,    78,    79,    80,
      81,    82,    -1,    -1,    -1,    86,    87,    -1,    -1,    -1,
      -1,    -1,    93,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   110,
     111,    -1,    -1,    -1,   115,    -1,    -1,   118,    -1,    -1,
     121,   122,   123,   124,    -1,    -1,    -1,    -1,   129,    -1,
      -1,    -1,    -1,   134,   135,   136,   137,    -1,    -1,   140,
     141,    -1,    -1,   144,   145,    -1,   147,   148,   149,   150,
     151,   152,   153,   154,   155,   156,   157,   158,    -1,    -1,
      -1,    -1,   163,    -1,   165,   166,   167,    -1,    -1,   170,
     171,   172,   173,   174,   175,   176,   177,   178,   179,   180,
     181,   182,   183,   184,    -1,   186,    -1,   188,   189,   190,
      -1,    -1,    -1,   194,    -1,    -1,    -1,   198,   199,   200,
     201,   202,   203,   204,   205,   206,   207,   208,   209,   210,
     211,   212,   213,   214,   215,   216,   217,   218,   219,   220,
     221,   222,   223,   224,    -1,   226,   227,   228,   229,   230,
      -1,   232,   233,   234,   235,   236,   237,   238,   239,   240,
     241,   242,   243,    -1,   245,   246,   247,   248,   249,   250,
      -1,   252,   253,   254,   255,   256,   257,   258,   259,   260,
     261,   262,   263,   264,   265,    -1,    -1,    -1,   269,   270,
     271,   272,   273,    -1,   275,   276,   277,   278,    -1,    -1,
     281,   282,   283,   284,   285,   286,   287,   288,   289,   290,
     291,   292,   293,   294,   295,   296,   297,   298,   299,   300,
     301,   302,   303,   304,   305,   306,   307,   308,   309,   310,
     311,   312,   313,    -1,   315,   316,    -1,    -1,    -1,    -1,
     321,   322,   323,   324,   325,   326,   327,   328,   329,   330,
     331,   332,   333,   334,   335,   336,   337,   338,   339,   340,
     341,   342,   343,   344,   345,   346,   347,   348,   349,   350,
     351,   352,   353,   354,   355,   356,    -1,   358,    -1,    -1,
      -1,    -1,    -1,    -1,   365,   366,   367,   368,   369,   370,
     371,   372,   373,   374,   375,   376,   377,   378,   379,   380,
     381,   382,   383,   384,   385,   386,   387,   388,   389,   390,
     391,   392,   393,   394,   395,   396,   397,   398,   399,   400,
     401,   402,   403,   404,   405,   406,   407,   408,   409,   410,
     411,   412,   413,   414,   415,   416,   417,   418,   419,   420,
       6,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    17,    -1,    -1,    -1,    -1,    -1,    23,    -1,    -1,
      -1,    27,    28,    -1,    -1,    -1,    32,    -1,    -1,    35,
      36,    37,    -1,    -1,    -1,    41,    42,    -1,    -1,    -1,
      46,    47,    -1,    49,    50,    51,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    60,    61,    -1,    -1,    -1,    -1,
      66,    -1,    -1,    69,    70,    71,    72,    73,    74,    75,
      -1,    -1,    78,    79,    80,    81,    82,    -1,    -1,    -1,
      86,    87,    -1,    -1,    -1,    -1,    -1,    93,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,   110,   111,   112,    -1,    -1,   115,
      -1,    -1,   118,    -1,    -1,   121,   122,   123,   124,    -1,
      -1,    -1,    -1,   129,    -1,    -1,    -1,    -1,   134,   135,
     136,   137,    -1,    -1,   140,   141,    -1,    -1,   144,   145,
      -1,   147,   148,   149,   150,   151,   152,   153,   154,   155,
     156,   157,   158,    -1,    -1,    -1,    -1,   163,    -1,   165,
     166,   167,    -1,    -1,   170,   171,   172,   173,   174,   175,
     176,   177,   178,   179,   180,   181,   182,   183,   184,    -1,
     186,    -1,   188,   189,   190,    -1,    -1,    -1,   194,    -1,
      -1,    -1,   198,   199,   200,   201,   202,   203,   204,   205,
     206,   207,   208,   209,   210,   211,   212,   213,   214,   215,
     216,   217,   218,   219,   220,   221,   222,   223,   224,    -1,
     226,   227,   228,   229,   230,    -1,   232,   233,   234,   235,
     236,   237,   238,   239,   240,   241,   242,   243,    -1,   245,
     246,   247,   248,   249,   250,    -1,   252,   253,   254,   255,
     256,   257,   258,   259,   260,   261,   262,   263,   264,   265,
      -1,    -1,    -1,   269,   270,   271,   272,   273,    -1,   275,
     276,   277,   278,    -1,    -1,   281,   282,   283,   284,   285,
     286,   287,   288,   289,   290,   291,   292,   293,   294,   295,
     296,   297,   298,   299,   300,   301,   302,   303,   304,   305,
     306,   307,   308,   309,   310,   311,   312,   313,    -1,   315,
     316,    -1,    -1,    -1,    -1,   321,   322,   323,   324,   325,
     326,   327,   328,   329,   330,   331,   332,   333,   334,   335,
     336,   337,   338,   339,   340,   341,   342,   343,   344,   345,
     346,   347,   348,   349,   350,   351,   352,   353,   354,   355,
     356,    -1,   358,    -1,    -1,    -1,    -1,    -1,    -1,   365,
     366,   367,   368,   369,   370,   371,   372,   373,   374,   375,
     376,   377,   378,   379,   380,   381,   382,   383,   384,   385,
     386,   387,   388,   389,   390,   391,   392,   393,   394,   395,
     396,   397,   398,   399,   400,   401,   402,   403,   404,   405,
     406,   407,   408,   409,   410,   411,   412,   413,   414,   415,
     416,   417,   418,   419,   420,     6,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    17,    -1,    -1,    -1,
      -1,    -1,    23,    -1,    -1,    -1,    27,    28,    -1,    -1,
      -1,    32,    -1,    -1,    35,    36,    37,    -1,    -1,    -1,
      41,    42,    -1,    -1,    -1,    46,    47,    -1,    49,    50,
      51,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    60,
      61,    -1,    -1,    -1,    -1,    66,    -1,    -1,    69,    70,
      71,    72,    73,    74,    75,    -1,    -1,    78,    79,    80,
      81,    82,    -1,    -1,    -1,    86,    87,    -1,    -1,    -1,
      -1,    -1,    93,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   110,
     111,   112,    -1,    -1,   115,    -1,    -1,   118,    -1,    -1,
     121,   122,   123,   124,    -1,    -1,    -1,    -1,   129,    -1,
      -1,    -1,    -1,   134,   135,   136,   137,    -1,    -1,   140,
     141,    -1,    -1,   144,   145,    -1,   147,   148,   149,   150,
     151,   152,   153,   154,   155,   156,   157,   158,    -1,    -1,
      -1,    -1,   163,    -1,   165,   166,   167,    -1,    -1,   170,
     171,   172,   173,   174,   175,   176,   177,   178,   179,   180,
     181,   182,   183,   184,    -1,   186,    -1,   188,   189,   190,
      -1,    -1,    -1,   194,    -1,    -1,    -1,   198,   199,   200,
     201,   202,   203,   204,   205,   206,   207,   208,   209,   210,
     211,   212,   213,   214,   215,   216,   217,   218,   219,   220,
     221,   222,   223,   224,    -1,   226,   227,   228,   229,   230,
      -1,   232,   233,   234,   235,   236,   237,   238,   239,   240,
     241,   242,   243,    -1,   245,   246,   247,   248,   249,   250,
      -1,   252,   253,   254,   255,   256,   257,   258,   259,   260,
     261,   262,   263,   264,   265,    -1,    -1,    -1,   269,   270,
     271,   272,   273,    -1,   275,   276,   277,   278,    -1,    -1,
     281,   282,   283,   284,   285,   286,   287,   288,   289,   290,
     291,   292,   293,   294,   295,   296,   297,   298,   299,   300,
     301,   302,   303,   304,   305,   306,   307,   308,   309,   310,
     311,   312,   313,    -1,   315,   316,    -1,    -1,    -1,    -1,
     321,   322,   323,   324,   325,   326,   327,   328,   329,   330,
     331,   332,   333,   334,   335,   336,   337,   338,   339,   340,
     341,   342,   343,   344,   345,   346,   347,   348,   349,   350,
     351,   352,   353,   354,   355,   356,    -1,   358,    -1,    -1,
      -1,    -1,    -1,    -1,   365,   366,   367,   368,   369,   370,
     371,   372,   373,   374,   375,   376,   377,   378,   379,   380,
     381,   382,   383,   384,   385,   386,   387,   388,   389,   390,
     391,   392,   393,   394,   395,   396,   397,   398,   399,   400,
     401,   402,   403,   404,   405,   406,   407,   408,   409,   410,
     411,   412,   413,   414,   415,   416,   417,   418,   419,   420,
       6,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    17,    -1,    -1,    -1,    -1,    -1,    23,    -1,    -1,
      -1,    27,    28,    -1,    -1,    -1,    32,    -1,    -1,    35,
      36,    37,    -1,    -1,    -1,    41,    42,    -1,    -1,    -1,
      46,    47,    -1,    49,    50,    51,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    60,    61,    -1,    -1,    -1,    -1,
      66,    -1,    68,    69,    70,    71,    72,    73,    74,    75,
      -1,    -1,    78,    79,    80,    81,    82,    -1,    -1,    -1,
      86,    87,    -1,    -1,    -1,    -1,    -1,    93,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,   110,   111,    -1,    -1,    -1,   115,
      -1,    -1,   118,    -1,    -1,   121,   122,   123,   124,    -1,
      -1,    -1,    -1,   129,    -1,    -1,    -1,    -1,   134,   135,
     136,   137,    -1,    -1,   140,   141,    -1,    -1,   144,   145,
      -1,   147,   148,   149,   150,   151,   152,   153,   154,   155,
     156,   157,   158,    -1,    -1,    -1,    -1,   163,    -1,   165,
     166,   167,    -1,    -1,   170,   171,   172,   173,   174,   175,
     176,   177,   178,   179,   180,   181,   182,   183,   184,    -1,
     186,    -1,   188,   189,   190,    -1,    -1,    -1,   194,    -1,
      -1,    -1,   198,   199,   200,   201,   202,   203,   204,   205,
     206,   207,   208,   209,   210,   211,   212,   213,   214,   215,
     216,   217,   218,   219,   220,   221,   222,   223,   224,    -1,
     226,   227,   228,   229,   230,    -1,   232,   233,   234,   235,
     236,   237,   238,   239,   240,   241,   242,   243,    -1,   245,
     246,   247,   248,   249,   250,    -1,   252,   253,   254,   255,
     256,   257,   258,   259,   260,   261,   262,   263,   264,   265,
      -1,    -1,    -1,   269,   270,   271,   272,   273,    -1,   275,
     276,   277,   278,    -1,    -1,   281,   282,   283,   284,   285,
     286,   287,   288,   289,   290,   291,   292,   293,   294,   295,
     296,   297,   298,   299,   300,   301,   302,   303,   304,   305,
     306,   307,   308,   309,   310,   311,   312,   313,    -1,   315,
     316,    -1,    -1,    -1,    -1,   321,   322,   323,   324,   325,
     326,   327,   328,   329,   330,   331,   332,   333,   334,   335,
     336,   337,   338,   339,   340,   341,   342,   343,   344,   345,
     346,   347,   348,   349,   350,   351,   352,   353,   354,   355,
     356,    -1,   358,    -1,    -1,    -1,    -1,    -1,    -1,   365,
     366,   367,   368,   369,   370,   371,   372,   373,   374,   375,
     376,   377,   378,   379,   380,   381,   382,   383,   384,   385,
     386,   387,   388,   389,   390,   391,   392,   393,   394,   395,
     396,   397,   398,   399,   400,   401,   402,   403,   404,   405,
     406,   407,   408,   409,   410,   411,   412,   413,   414,   415,
     416,   417,   418,   419,   420,     6,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    17,    -1,    -1,    -1,
      -1,    -1,    23,    -1,    -1,    -1,    27,    28,    -1,    -1,
      -1,    32,    -1,    -1,    35,    36,    37,    -1,    -1,    -1,
      41,    42,    -1,    -1,    -1,    46,    47,    -1,    49,    50,
      51,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    60,
      61,    -1,    -1,    -1,    -1,    66,    -1,    -1,    69,    70,
      71,    72,    73,    74,    75,    -1,    -1,    78,    79,    80,
      81,    82,    -1,    -1,    -1,    86,    87,    -1,    -1,    -1,
      -1,    -1,    93,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   110,
     111,    -1,    -1,    -1,   115,    -1,    -1,   118,    -1,    -1,
     121,   122,   123,   124,    -1,    -1,    -1,    -1,   129,    -1,
      -1,    -1,    -1,   134,   135,   136,   137,    -1,    -1,   140,
     141,    -1,    -1,   144,   145,    -1,   147,   148,   149,   150,
     151,   152,   153,   154,   155,   156,   157,   158,    -1,    -1,
      -1,    -1,   163,    -1,   165,   166,   167,    -1,    -1,   170,
     171,   172,   173,   174,   175,   176,   177,   178,   179,   180,
     181,   182,   183,   184,    -1,   186,    -1,   188,   189,   190,
      -1,    -1,    -1,   194,    -1,    -1,    -1,   198,   199,   200,
     201,   202,   203,   204,   205,   206,   207,   208,   209,   210,
     211,   212,   213,   214,   215,   216,   217,   218,   219,   220,
     221,   222,   223,   224,    -1,   226,   227,   228,   229,   230,
      -1,   232,   233,   234,   235,   236,   237,   238,   239,   240,
     241,   242,   243,    -1,   245,   246,   247,   248,   249,   250,
      -1,   252,   253,   254,   255,   256,   257,   258,   259,   260,
     261,   262,   263,   264,   265,    -1,    -1,    -1,   269,   270,
     271,   272,   273,    -1,   275,   276,   277,   278,    -1,    -1,
     281,   282,   283,   284,   285,   286,   287,   288,   289,   290,
     291,   292,   293,   294,   295,   296,   297,   298,   299,   300,
     301,   302,   303,   304,   305,   306,   307,   308,   309,   310,
     311,   312,   313,    -1,   315,   316,    -1,    -1,    -1,    -1,
     321,   322,   323,   324,   325,   326,   327,   328,   329,   330,
     331,   332,   333,   334,   335,   336,   337,   338,   339,   340,
     341,   342,   343,   344,   345,   346,   347,   348,   349,   350,
     351,   352,   353,   354,   355,   356,    -1,   358,    -1,    -1,
      -1,    -1,    -1,    -1,   365,   366,   367,   368,   369,   370,
     371,   372,   373,   374,   375,   376,   377,   378,   379,   380,
     381,   382,   383,   384,   385,   386,   387,   388,   389,   390,
     391,   392,   393,   394,   395,   396,   397,   398,   399,   400,
     401,   402,   403,   404,   405,   406,   407,   408,   409,   410,
     411,   412,   413,   414,   415,   416,   417,   418,   419,   420,
       6,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    17,    -1,    -1,    -1,    -1,    -1,    23,    -1,    -1,
      -1,    27,    28,    -1,    -1,    -1,    32,    -1,    -1,    35,
      36,    37,    -1,    -1,    -1,    41,    42,    -1,    -1,    -1,
      46,    47,    -1,    49,    50,    51,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    60,    61,    -1,    -1,    -1,    -1,
      66,    -1,    -1,    69,    70,    71,    72,    73,    74,    75,
      -1,    -1,    78,    79,    80,    81,    82,    -1,    -1,    -1,
      86,    87,    -1,    -1,    -1,    -1,    -1,    93,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,   110,   111,    -1,    -1,    -1,   115,
      -1,    -1,   118,    -1,    -1,   121,   122,   123,   124,    -1,
      -1,    -1,    -1,   129,    -1,    -1,    -1,    -1,   134,   135,
     136,   137,    -1,    -1,   140,   141,    -1,    -1,   144,   145,
      -1,   147,   148,   149,   150,   151,   152,   153,   154,   155,
     156,   157,   158,    -1,    -1,    -1,    -1,   163,    -1,   165,
     166,   167,    -1,    -1,   170,   171,   172,   173,   174,   175,
     176,   177,   178,   179,   180,   181,   182,   183,   184,    -1,
     186,    -1,   188,   189,   190,    -1,    -1,    -1,   194,    -1,
      -1,    -1,   198,   199,   200,   201,   202,   203,   204,   205,
     206,   207,   208,   209,   210,   211,   212,   213,   214,   215,
     216,   217,   218,   219,   220,   221,   222,   223,   224,    -1,
     226,   227,   228,   229,   230,    -1,   232,   233,   234,   235,
     236,   237,   238,   239,   240,   241,   242,   243,    -1,   245,
     246,   247,   248,   249,   250,    -1,   252,   253,   254,   255,
     256,   257,   258,   259,   260,   261,   262,   263,   264,   265,
      -1,    -1,    -1,   269,   270,   271,   272,   273,    -1,   275,
     276,   277,   278,    -1,    -1,   281,   282,   283,   284,   285,
     286,   287,   288,   289,   290,   291,   292,   293,   294,   295,
     296,   297,   298,   299,   300,   301,   302,   303,   304,   305,
     306,   307,   308,   309,   310,   311,   312,   313,    -1,   315,
     316,    -1,    -1,    -1,    -1,   321,   322,   323,   324,   325,
     326,   327,   328,   329,   330,   331,   332,   333,   334,   335,
     336,   337,   338,   339,   340,   341,   342,   343,   344,   345,
     346,   347,   348,   349,   350,   351,   352,   353,   354,   355,
     356,    -1,   358,    -1,    -1,    -1,    -1,    -1,    -1,   365,
     366,   367,   368,   369,   370,   371,   372,   373,   374,   375,
     376,   377,   378,   379,   380,   381,   382,   383,   384,   385,
     386,   387,   388,   389,   390,   391,   392,   393,   394,   395,
     396,   397,   398,   399,   400,   401,   402,   403,   404,   405,
     406,   407,   408,   409,   410,   411,   412,   413,   414,   415,
     416,   417,   418,   419,   420,     6,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    17,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    27,    -1,    -1,    -1,
      -1,    32,    -1,    -1,    35,    36,    37,    -1,    -1,    -1,
      41,    42,    -1,    -1,    -1,    46,    47,    -1,    49,    50,
      51,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    60,
      61,    -1,    -1,    -1,    -1,    66,    -1,    68,    69,    70,
      71,    72,    73,    74,    75,    -1,    -1,    78,    79,    80,
      81,    82,    -1,    -1,    -1,    86,    -1,    -1,    -1,    -1,
      -1,    -1,    93,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   110,
     111,    -1,    -1,    -1,   115,    -1,    -1,   118,    -1,    -1,
     121,   122,   123,   124,    -1,    -1,    -1,    -1,   129,    -1,
      -1,    -1,    -1,   134,   135,   136,   137,    -1,    -1,   140,
     141,    -1,    -1,   144,   145,    -1,   147,   148,   149,   150,
     151,   152,   153,   154,   155,   156,   157,   158,    -1,    -1,
      -1,    -1,   163,    -1,   165,   166,   167,    -1,    -1,   170,
     171,   172,   173,   174,   175,   176,   177,   178,   179,   180,
     181,   182,   183,   184,    -1,   186,    -1,   188,   189,   190,
      -1,    -1,    -1,   194,    -1,    -1,    -1,   198,   199,   200,
     201,   202,   203,   204,   205,   206,   207,   208,   209,   210,
     211,   212,   213,   214,   215,   216,   217,   218,   219,   220,
     221,   222,   223,   224,    -1,   226,   227,   228,   229,   230,
      -1,   232,   233,   234,   235,   236,   237,   238,   239,   240,
     241,   242,   243,    -1,   245,   246,   247,   248,   249,   250,
      -1,   252,   253,   254,   255,   256,   257,   258,   259,   260,
     261,   262,   263,   264,   265,    -1,    -1,    -1,   269,   270,
     271,   272,   273,    -1,   275,   276,   277,   278,    -1,    -1,
     281,   282,   283,   284,   285,   286,   287,   288,   289,   290,
     291,   292,   293,   294,   295,   296,   297,   298,   299,   300,
     301,   302,   303,   304,   305,   306,   307,   308,   309,   310,
     311,   312,   313,    -1,   315,   316,    -1,    -1,    -1,    -1,
     321,   322,   323,   324,   325,   326,   327,   328,   329,   330,
     331,   332,   333,   334,   335,   336,   337,   338,   339,   340,
     341,   342,   343,   344,   345,   346,   347,   348,   349,   350,
     351,   352,   353,   354,   355,   356,    -1,   358,    -1,    -1,
      -1,    -1,    -1,    -1,   365,   366,   367,   368,   369,   370,
     371,   372,   373,   374,   375,   376,   377,   378,   379,   380,
     381,   382,   383,   384,   385,   386,   387,   388,   389,   390,
     391,   392,   393,   394,   395,   396,   397,   398,   399,   400,
     401,   402,   403,   404,   405,   406,   407,   408,   409,   410,
     411,   412,   413,   414,   415,   416,   417,   418,   419,   420,
       6,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    17,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    27,    -1,    -1,    -1,    -1,    32,    -1,    -1,    35,
      36,    37,    -1,    -1,    -1,    41,    42,    -1,    -1,    -1,
      46,    47,    -1,    49,    50,    51,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    60,    61,    -1,    -1,    -1,    -1,
      66,    -1,    -1,    69,    70,    71,    72,    73,    74,    75,
      -1,    -1,    78,    79,    80,    81,    82,    -1,    -1,    -1,
      86,    -1,    -1,    -1,    -1,    -1,    -1,    93,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,   110,   111,    -1,    -1,    -1,   115,
      -1,    -1,   118,    -1,    -1,   121,   122,   123,   124,    -1,
      -1,    -1,    -1,   129,    -1,    -1,    -1,    -1,   134,   135,
     136,   137,    -1,    -1,   140,   141,    -1,    -1,   144,   145,
      -1,   147,   148,   149,   150,   151,   152,   153,   154,   155,
     156,   157,   158,    -1,    -1,    -1,    -1,   163,    -1,   165,
     166,   167,    -1,   169,   170,   171,   172,   173,   174,   175,
     176,   177,   178,   179,   180,   181,   182,   183,   184,    -1,
     186,    -1,   188,   189,   190,    -1,    -1,    -1,   194,    -1,
      -1,    -1,   198,   199,   200,   201,   202,   203,   204,   205,
     206,   207,   208,   209,   210,   211,   212,   213,   214,   215,
     216,   217,   218,   219,   220,   221,   222,   223,   224,    -1,
     226,   227,   228,   229,   230,    -1,   232,   233,   234,   235,
     236,   237,   238,   239,   240,   241,   242,   243,    -1,   245,
     246,   247,   248,   249,   250,    -1,   252,   253,   254,   255,
     256,   257,   258,   259,   260,   261,   262,   263,   264,   265,
      -1,    -1,    -1,   269,   270,   271,   272,   273,    -1,   275,
     276,   277,   278,    -1,    -1,   281,   282,   283,   284,   285,
     286,   287,   288,   289,   290,   291,   292,   293,   294,   295,
     296,   297,   298,   299,   300,   301,   302,   303,   304,   305,
     306,   307,   308,   309,   310,   311,   312,   313,    -1,   315,
     316,    -1,    -1,    -1,    -1,   321,   322,   323,   324,   325,
     326,   327,   328,   329,   330,   331,   332,   333,   334,   335,
     336,   337,   338,   339,   340,   341,   342,   343,   344,   345,
     346,   347,   348,   349,   350,   351,   352,   353,   354,   355,
     356,    -1,   358,    -1,    -1,    -1,    -1,    -1,    -1,   365,
     366,   367,   368,   369,   370,   371,   372,   373,   374,   375,
     376,   377,   378,   379,   380,   381,   382,   383,   384,   385,
     386,   387,   388,   389,   390,   391,   392,   393,   394,   395,
     396,   397,   398,   399,   400,   401,   402,   403,   404,   405,
     406,   407,   408,   409,   410,   411,   412,   413,   414,   415,
     416,   417,   418,   419,   420,     6,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    17,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    27,    -1,    -1,    -1,
      -1,    32,    -1,    -1,    35,    36,    37,    -1,    -1,    -1,
      41,    42,    -1,    -1,    -1,    46,    47,    -1,    49,    50,
      51,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    60,
      61,    -1,    -1,    -1,    -1,    66,    -1,    -1,    69,    70,
      71,    72,    73,    74,    75,    -1,    -1,    78,    79,    80,
      81,    82,    -1,    -1,    -1,    86,    -1,    -1,    -1,    -1,
      -1,    -1,    93,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   110,
     111,    -1,    -1,    -1,   115,    -1,    -1,   118,    -1,    -1,
     121,   122,   123,   124,    -1,    -1,    -1,    -1,   129,    -1,
      -1,    -1,    -1,   134,   135,   136,   137,    -1,    -1,   140,
     141,    -1,    -1,   144,   145,    -1,   147,   148,   149,   150,
     151,   152,   153,   154,   155,   156,   157,   158,    -1,    -1,
      -1,    -1,   163,    -1,   165,   166,   167,    -1,    -1,   170,
     171,   172,   173,   174,   175,   176,   177,   178,   179,   180,
     181,   182,   183,   184,    -1,   186,    -1,   188,   189,   190,
      -1,    -1,    -1,   194,    -1,    -1,    -1,   198,   199,   200,
     201,   202,   203,   204,   205,   206,   207,   208,   209,   210,
     211,   212,   213,   214,   215,   216,   217,   218,   219,   220,
     221,   222,   223,   224,    -1,   226,   227,   228,   229,   230,
      -1,   232,   233,   234,   235,   236,   237,   238,   239,   240,
     241,   242,   243,    -1,   245,   246,   247,   248,   249,   250,
      -1,   252,   253,   254,   255,   256,   257,   258,   259,   260,
     261,   262,   263,   264,   265,    -1,    -1,    -1,   269,   270,
     271,   272,   273,    -1,   275,   276,   277,   278,    -1,    -1,
     281,   282,   283,   284,   285,   286,   287,   288,   289,   290,
     291,   292,   293,   294,   295,   296,   297,   298,   299,   300,
     301,   302,   303,   304,   305,   306,   307,   308,   309,   310,
     311,   312,   313,    -1,   315,   316,    -1,    -1,    -1,    -1,
     321,   322,   323,   324,   325,   326,   327,   328,   329,   330,
     331,   332,   333,   334,   335,   336,   337,   338,   339,   340,
     341,   342,   343,   344,   345,   346,   347,   348,   349,   350,
     351,   352,   353,   354,   355,   356,    -1,   358,    -1,    -1,
      -1,    -1,    -1,    -1,   365,   366,   367,   368,   369,   370,
     371,   372,   373,   374,   375,   376,   377,   378,   379,   380,
     381,   382,   383,   384,   385,   386,   387,   388,   389,   390,
     391,   392,   393,   394,   395,   396,   397,   398,   399,   400,
     401,   402,   403,   404,   405,   406,   407,   408,   409,   410,
     411,   412,   413,   414,   415,   416,   417,   418,   419,   420,
       6,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    17,    -1,    -1,    -1,    -1,    22,    -1,    -1,    -1,
      -1,    27,    -1,    -1,    -1,    -1,    32,    -1,    -1,    35,
      36,    37,    -1,    -1,    -1,    41,    42,    -1,    -1,    -1,
      -1,    47,    -1,    49,    50,    51,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    69,    -1,    71,    -1,    -1,    -1,    -1,
      -1,    -1,    78,    -1,    -1,    -1,    -1,    -1,    84,    85,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,   101,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,   129,    -1,    -1,    -1,    -1,   134,   135,
     136,    -1,    -1,    -1,   140,   141,    -1,    -1,    -1,   145,
      -1,   147,   148,   149,   150,   151,   152,   153,   154,   155,
     156,   157,    -1,    -1,    -1,    -1,    -1,   163,    -1,   165,
     166,   167,    -1,    -1,   170,   171,   172,   173,   174,   175,
     176,   177,   178,   179,   180,   181,   182,   183,   184,    -1,
     186,    -1,   188,   189,   190,    -1,    -1,    -1,   194,    -1,
      -1,    -1,   198,   199,   200,   201,   202,   203,   204,   205,
     206,   207,   208,   209,   210,   211,   212,   213,   214,   215,
     216,   217,   218,   219,   220,   221,   222,   223,   224,    -1,
     226,   227,   228,   229,   230,    -1,   232,   233,   234,   235,
     236,   237,   238,   239,   240,   241,   242,   243,    -1,   245,
     246,   247,   248,   249,   250,    -1,   252,   253,   254,   255,
     256,   257,   258,   259,   260,   261,   262,   263,   264,   265,
      -1,    -1,    -1,   269,   270,   271,   272,   273,    -1,   275,
     276,   277,   278,    -1,    -1,   281,   282,   283,   284,   285,
     286,   287,   288,   289,   290,   291,   292,   293,   294,   295,
     296,   297,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,   321,   322,   323,   324,   325,
     326,   327,   328,   329,   330,   331,   332,   333,   334,   335,
     336,   337,   338,   339,   340,   341,   342,   343,   344,   345,
     346,   347,   348,    -1,   350,   351,   352,   353,   354,   355,
     356,    -1,   358,    -1,    -1,    -1,    -1,    -1,    -1,   365,
     366,   367,   368,   369,   370,   371,   372,   373,   374,   375,
     376,   377,   378,   379,   380,   381,   382,   383,   384,   385,
     386,   387,   388,   389,   390,   391,   392,   393,   394,   395,
     396,   397,   398,   399,   400,   401,   402,   403,   404,   405,
     406,   407,   408,   409,   410,   411,   412,   413,   414,   415,
     416,   417,   418,   419,   420,     6,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    17,    -1,    -1,    -1,
      -1,    22,    -1,    -1,    -1,    -1,    27,    -1,    -1,    -1,
      -1,    32,    -1,    -1,    35,    36,    37,    -1,    -1,    -1,
      41,    42,    -1,    -1,    -1,    -1,    47,    -1,    49,    50,
      51,    -1,    -1,    54,    55,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    69,    -1,
      71,    -1,    -1,    -1,    -1,    -1,    -1,    78,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   129,    -1,
      -1,    -1,    -1,   134,   135,   136,    -1,    -1,    -1,   140,
     141,    -1,   143,    -1,   145,    -1,   147,   148,   149,   150,
     151,   152,   153,   154,   155,   156,   157,    -1,    -1,    -1,
      -1,    -1,   163,    -1,   165,   166,   167,    -1,    -1,   170,
     171,   172,   173,   174,   175,   176,   177,   178,   179,   180,
     181,   182,   183,   184,    -1,   186,    -1,   188,   189,   190,
      -1,    -1,    -1,   194,    -1,    -1,    -1,   198,   199,   200,
     201,   202,   203,   204,   205,   206,   207,   208,   209,   210,
     211,   212,   213,   214,   215,   216,   217,   218,   219,   220,
     221,   222,   223,   224,    -1,   226,   227,   228,   229,   230,
      -1,   232,   233,   234,   235,   236,   237,   238,   239,   240,
     241,   242,   243,    -1,   245,   246,   247,   248,   249,   250,
      -1,   252,   253,   254,   255,   256,   257,   258,   259,   260,
     261,   262,   263,   264,   265,    -1,    -1,    -1,   269,   270,
     271,   272,   273,    -1,   275,   276,   277,   278,    -1,    -1,
     281,   282,   283,   284,   285,   286,   287,   288,   289,   290,
     291,   292,   293,   294,   295,   296,   297,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
     321,   322,   323,   324,   325,   326,   327,   328,   329,   330,
     331,   332,   333,   334,   335,   336,   337,   338,   339,   340,
     341,   342,   343,   344,   345,   346,   347,   348,    -1,   350,
     351,   352,   353,   354,   355,   356,    -1,   358,    -1,    -1,
      -1,    -1,    -1,    -1,   365,   366,   367,   368,   369,   370,
     371,   372,   373,   374,   375,   376,   377,   378,   379,   380,
     381,   382,   383,   384,   385,   386,   387,   388,   389,   390,
     391,   392,   393,   394,   395,   396,   397,   398,   399,   400,
     401,   402,   403,   404,   405,   406,   407,   408,   409,   410,
     411,   412,   413,   414,   415,   416,   417,   418,   419,   420,
       6,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    17,    -1,    -1,    -1,    -1,    22,    -1,    -1,    -1,
      -1,    27,    -1,    -1,    -1,    -1,    32,    -1,    -1,    35,
      36,    37,    -1,    -1,    -1,    41,    42,    -1,    -1,    -1,
      -1,    47,    -1,    49,    50,    51,    -1,    -1,    54,    55,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    69,    -1,    71,    -1,    -1,    -1,    -1,
      -1,    -1,    78,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,   129,    -1,    -1,    -1,    -1,   134,   135,
     136,    -1,    -1,    -1,   140,   141,    -1,   143,    -1,   145,
      -1,   147,   148,   149,   150,   151,   152,   153,   154,   155,
     156,   157,    -1,    -1,    -1,    -1,    -1,   163,    -1,   165,
     166,   167,    -1,    -1,   170,   171,   172,   173,   174,   175,
     176,   177,   178,   179,   180,   181,   182,   183,   184,    -1,
     186,    -1,   188,   189,   190,    -1,    -1,    -1,   194,    -1,
      -1,    -1,   198,   199,   200,   201,   202,   203,   204,   205,
     206,   207,   208,   209,   210,   211,   212,   213,   214,   215,
     216,   217,   218,   219,   220,   221,   222,   223,   224,    -1,
     226,   227,   228,   229,   230,    -1,   232,   233,   234,   235,
     236,   237,   238,   239,   240,   241,   242,   243,    -1,   245,
     246,   247,   248,   249,   250,    -1,   252,   253,   254,   255,
     256,   257,   258,   259,   260,   261,   262,   263,   264,   265,
      -1,    -1,    -1,   269,   270,   271,   272,   273,    -1,   275,
     276,   277,   278,    -1,    -1,   281,   282,   283,   284,   285,
     286,   287,   288,   289,   290,   291,   292,   293,   294,   295,
     296,   297,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,   321,   322,   323,   324,   325,
     326,   327,   328,   329,   330,   331,   332,   333,   334,   335,
     336,   337,   338,   339,   340,   341,   342,   343,   344,   345,
     346,   347,   348,    -1,   350,   351,   352,   353,   354,   355,
     356,    -1,   358,    -1,    -1,    -1,    -1,    -1,    -1,   365,
     366,   367,   368,   369,   370,   371,   372,   373,   374,   375,
     376,   377,   378,   379,   380,   381,   382,   383,   384,   385,
     386,   387,   388,   389,   390,   391,   392,   393,   394,   395,
     396,   397,   398,   399,   400,   401,   402,   403,   404,   405,
     406,   407,   408,   409,   410,   411,   412,   413,   414,   415,
     416,   417,   418,   419,   420,     6,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    17,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    27,    -1,    -1,    -1,
      -1,    32,    33,    -1,    35,    36,    37,    -1,    -1,    -1,
      41,    42,    -1,    -1,    -1,    -1,    47,    -1,    49,    50,
      51,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    69,    -1,
      71,    -1,    -1,    -1,    -1,    -1,    -1,    78,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   129,    -1,
      -1,    -1,    -1,   134,   135,   136,    -1,    -1,    -1,   140,
     141,    -1,    -1,    -1,   145,    -1,   147,   148,   149,   150,
     151,   152,   153,   154,   155,   156,   157,    -1,    -1,    -1,
      -1,    -1,   163,    -1,   165,   166,   167,    -1,    -1,   170,
     171,   172,   173,   174,   175,   176,   177,   178,   179,   180,
     181,   182,   183,   184,    -1,   186,    -1,   188,   189,   190,
      -1,    -1,    -1,   194,    -1,    -1,    -1,   198,   199,   200,
     201,   202,   203,   204,   205,   206,   207,   208,   209,   210,
     211,   212,   213,   214,   215,   216,   217,   218,   219,   220,
     221,   222,   223,   224,    -1,   226,   227,   228,   229,   230,
      -1,   232,   233,   234,   235,   236,   237,   238,   239,   240,
     241,   242,   243,    -1,   245,   246,   247,   248,   249,   250,
      -1,   252,   253,   254,   255,   256,   257,   258,   259,   260,
     261,   262,   263,   264,   265,    -1,    -1,    -1,   269,   270,
     271,   272,   273,    -1,   275,   276,   277,   278,    -1,    -1,
     281,   282,   283,   284,   285,   286,   287,   288,   289,   290,
     291,   292,   293,   294,   295,   296,   297,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
     321,   322,   323,   324,   325,   326,   327,   328,   329,   330,
     331,   332,   333,   334,   335,   336,   337,   338,   339,   340,
     341,   342,   343,   344,   345,   346,   347,   348,    -1,   350,
     351,   352,   353,   354,   355,   356,    -1,   358,    -1,    -1,
      -1,    -1,    -1,    -1,   365,   366,   367,   368,   369,   370,
     371,   372,   373,   374,   375,   376,   377,   378,   379,   380,
     381,   382,   383,   384,   385,   386,   387,   388,   389,   390,
     391,   392,   393,   394,   395,   396,   397,   398,   399,   400,
     401,   402,   403,   404,   405,   406,   407,   408,   409,   410,
     411,   412,   413,   414,   415,   416,   417,   418,   419,   420,
       6,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    17,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    27,    -1,    -1,    -1,    -1,    32,    -1,    -1,    35,
      36,    37,    -1,    -1,    -1,    41,    42,    -1,    -1,    -1,
      -1,    47,    -1,    49,    50,    51,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    69,    -1,    71,    -1,    -1,    -1,    -1,
      -1,    -1,    78,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,   129,    -1,    -1,    -1,    -1,   134,   135,
     136,    -1,    -1,    -1,   140,   141,    -1,    -1,    -1,   145,
      -1,   147,   148,   149,   150,   151,   152,   153,   154,   155,
     156,   157,    -1,    -1,    -1,    -1,    -1,   163,    -1,   165,
     166,   167,    -1,    -1,   170,   171,   172,   173,   174,   175,
     176,   177,   178,   179,   180,   181,   182,   183,   184,    -1,
     186,    -1,   188,   189,   190,    -1,    -1,    -1,   194,    -1,
      -1,    -1,   198,   199,   200,   201,   202,   203,   204,   205,
     206,   207,   208,   209,   210,   211,   212,   213,   214,   215,
     216,   217,   218,   219,   220,   221,   222,   223,   224,    -1,
     226,   227,   228,   229,   230,    -1,   232,   233,   234,   235,
     236,   237,   238,   239,   240,   241,   242,   243,    -1,   245,
     246,   247,   248,   249,   250,    -1,   252,   253,   254,   255,
     256,   257,   258,   259,   260,   261,   262,   263,   264,   265,
      -1,    -1,    -1,   269,   270,   271,   272,   273,    -1,   275,
     276,   277,   278,    -1,    -1,   281,   282,   283,   284,   285,
     286,   287,   288,   289,   290,   291,   292,   293,   294,   295,
     296,   297,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,   321,   322,   323,   324,   325,
     326,   327,   328,   329,   330,   331,   332,   333,   334,   335,
     336,   337,   338,   339,   340,   341,   342,   343,   344,   345,
     346,   347,   348,    -1,   350,   351,   352,   353,   354,   355,
     356,    -1,   358,    -1,    -1,    -1,    -1,    -1,    -1,   365,
     366,   367,   368,   369,   370,   371,   372,   373,   374,   375,
     376,   377,   378,   379,   380,   381,   382,   383,   384,   385,
     386,   387,   388,   389,   390,   391,   392,   393,   394,   395,
     396,   397,   398,   399,   400,   401,   402,   403,   404,   405,
     406,   407,   408,   409,   410,   411,   412,   413,   414,   415,
     416,   417,   418,   419,   420,     6,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    17,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    27,    -1,    -1,    -1,
      -1,    32,    -1,    -1,    35,    36,    37,    -1,    -1,    -1,
      41,    42,    -1,    -1,    -1,    -1,    47,    -1,    49,    50,
      51,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    69,    -1,
      71,    -1,    -1,    -1,    -1,    -1,    -1,    78,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   129,    -1,
      -1,    -1,    -1,   134,   135,   136,    -1,    -1,    -1,   140,
     141,    -1,    -1,    -1,   145,    -1,   147,   148,   149,   150,
     151,   152,   153,   154,   155,   156,   157,    -1,    -1,    -1,
      -1,    -1,   163,    -1,   165,   166,   167,    -1,    -1,   170,
     171,   172,   173,   174,   175,   176,   177,   178,   179,   180,
     181,   182,   183,   184,    -1,   186,    -1,   188,   189,   190,
      -1,    -1,    -1,   194,    -1,    -1,    -1,   198,   199,   200,
     201,   202,   203,   204,   205,   206,   207,   208,   209,   210,
     211,   212,   213,   214,   215,   216,   217,   218,   219,   220,
     221,   222,   223,   224,    -1,   226,   227,   228,   229,   230,
      -1,   232,   233,   234,   235,   236,   237,   238,   239,   240,
     241,   242,   243,    -1,   245,   246,   247,   248,   249,   250,
      -1,   252,   253,   254,   255,   256,   257,   258,   259,   260,
     261,   262,   263,   264,   265,    -1,    -1,    -1,   269,   270,
     271,   272,   273,    -1,   275,   276,   277,   278,    -1,    -1,
     281,   282,   283,   284,   285,   286,   287,   288,   289,   290,
     291,   292,   293,   294,   295,   296,   297,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
     321,   322,   323,   324,   325,   326,   327,   328,   329,   330,
     331,   332,   333,   334,   335,   336,   337,   338,   339,   340,
     341,   342,   343,   344,   345,   346,   347,   348,    -1,   350,
     351,   352,   353,   354,   355,   356,    -1,   358,    -1,    -1,
      -1,    -1,    -1,    -1,   365,   366,   367,   368,   369,   370,
     371,   372,   373,   374,   375,   376,   377,   378,   379,   380,
     381,   382,   383,   384,   385,   386,   387,   388,   389,   390,
     391,   392,   393,   394,   395,   396,   397,   398,   399,   400,
     401,   402,   403,   404,   405,   406,   407,   408,   409,   410,
     411,   412,   413,   414,   415,   416,   417,   418,   419,   420,
       6,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    17,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    27,    -1,    -1,    -1,    -1,    32,    -1,    -1,    35,
      36,    37,    -1,    -1,    -1,    41,    42,    -1,    -1,    -1,
      -1,    47,    -1,    49,    50,    51,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    69,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    78,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
     106,   107,   108,   109,   110,   111,   112,   113,   114,   115,
     116,   117,    -1,    -1,   120,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,   129,    -1,    -1,    -1,    -1,   134,   135,
     136,    -1,    -1,    -1,   140,   141,    -1,    -1,    -1,   145,
      -1,   147,   148,   149,   150,   151,   152,   153,   154,   155,
     156,   157,    -1,    -1,    -1,    -1,    -1,   163,    -1,   165,
     166,   167,    -1,    -1,   170,   171,   172,   173,   174,   175,
     176,   177,   178,   179,   180,   181,   182,   183,   184,    -1,
     186,    -1,   188,   189,   190,    -1,    -1,    -1,   194,    -1,
      -1,    -1,   198,   199,   200,   201,   202,   203,   204,   205,
     206,   207,   208,   209,   210,   211,   212,   213,   214,   215,
     216,   217,   218,   219,   220,   221,   222,   223,   224,    -1,
     226,   227,   228,   229,   230,    -1,   232,   233,   234,   235,
     236,   237,   238,   239,   240,   241,   242,   243,    -1,   245,
     246,   247,   248,   249,   250,    -1,   252,   253,   254,   255,
     256,   257,   258,   259,   260,   261,   262,   263,   264,   265,
      -1,    -1,    -1,   269,   270,   271,   272,   273,    -1,   275,
     276,   277,   278,    -1,    -1,   281,   282,   283,   284,   285,
     286,   287,   288,   289,   290,   291,   292,   293,   294,   295,
     296,   297,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,   350,   351,   352,   353,   354,   355,
     356,    -1,   358,    -1,    -1,    -1,    -1,    -1,    -1,   365,
     366,   367,   368,   369,   370,   371,   372,   373,   374,   375,
     376,   377,   378,   379,   380,   381,   382,   383,   384,   385,
     386,   387,   388,   389,   390,   391,   392,   393,   394,   395,
     396,   397,   398,   399,   400,   401,   402,   403,   404,   405,
     406,   407,   408,   409,   410,   411,   412,   413,   414,   415,
     416,   417,   418,   419,   420,     6,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    17,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    27,    28,    -1,    -1,
      -1,    32,    -1,    -1,    35,    36,    37,    -1,    -1,    -1,
      41,    42,    -1,    -1,    -1,    -1,    47,    -1,    49,    50,
      51,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    69,    -1,
      71,    -1,    -1,    -1,    -1,    -1,    -1,    78,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   129,    -1,
      -1,    -1,    -1,   134,   135,   136,    -1,    -1,    -1,   140,
     141,    -1,    -1,    -1,   145,    -1,   147,   148,   149,   150,
     151,   152,   153,   154,   155,   156,   157,    -1,    -1,    -1,
      -1,    -1,   163,    -1,   165,   166,   167,    -1,    -1,   170,
     171,   172,   173,   174,   175,   176,   177,   178,   179,   180,
     181,   182,   183,   184,    -1,   186,    -1,   188,   189,   190,
      -1,    -1,    -1,   194,    -1,    -1,    -1,   198,   199,   200,
     201,   202,   203,   204,   205,   206,   207,   208,   209,   210,
     211,   212,   213,   214,   215,   216,   217,   218,   219,   220,
     221,   222,   223,   224,    -1,   226,   227,   228,   229,   230,
      -1,   232,   233,   234,   235,   236,   237,   238,   239,   240,
     241,   242,   243,    -1,   245,   246,   247,   248,   249,   250,
      -1,   252,   253,   254,   255,   256,   257,   258,   259,   260,
     261,   262,   263,   264,   265,    -1,    -1,    -1,   269,   270,
     271,   272,   273,    -1,   275,   276,   277,   278,    -1,    -1,
     281,   282,   283,   284,   285,   286,   287,   288,   289,   290,
     291,   292,   293,   294,   295,   296,   297,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   350,
     351,   352,   353,   354,   355,   356,    -1,   358,    -1,    -1,
      -1,    -1,    -1,    -1,   365,   366,   367,   368,   369,   370,
     371,   372,   373,   374,   375,   376,   377,   378,   379,   380,
     381,   382,   383,   384,   385,   386,   387,   388,   389,   390,
     391,   392,   393,   394,   395,   396,   397,   398,   399,   400,
     401,   402,   403,   404,   405,   406,   407,   408,   409,   410,
     411,   412,   413,   414,   415,   416,   417,   418,   419,   420,
       6,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    17,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    27,    -1,    -1,    -1,    -1,    32,    -1,    -1,    35,
      36,    37,    -1,    -1,    -1,    41,    42,    -1,    -1,    -1,
      -1,    47,    -1,    49,    50,    51,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    69,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    78,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,   129,    -1,    -1,    -1,    -1,   134,   135,
     136,    -1,   138,    -1,   140,   141,    -1,    -1,    -1,   145,
      -1,   147,   148,   149,   150,   151,   152,   153,   154,   155,
     156,   157,    -1,    -1,    -1,    -1,    -1,   163,    -1,   165,
     166,   167,    -1,    -1,   170,   171,   172,   173,   174,   175,
     176,   177,   178,   179,   180,   181,   182,   183,   184,    -1,
     186,    -1,   188,   189,   190,    -1,    -1,    -1,   194,    -1,
      -1,    -1,   198,   199,   200,   201,   202,   203,   204,   205,
     206,   207,   208,   209,   210,   211,   212,   213,   214,   215,
     216,   217,   218,   219,   220,   221,   222,   223,   224,    -1,
     226,   227,   228,   229,   230,    -1,   232,   233,   234,   235,
     236,   237,   238,   239,   240,   241,   242,   243,    -1,   245,
     246,   247,   248,   249,   250,    -1,   252,   253,   254,   255,
     256,   257,   258,   259,   260,   261,   262,   263,   264,   265,
      -1,    -1,    -1,   269,   270,   271,   272,   273,    -1,   275,
     276,   277,   278,    -1,    -1,   281,   282,   283,   284,   285,
     286,   287,   288,   289,   290,   291,   292,   293,   294,   295,
     296,   297,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,   350,   351,   352,   353,   354,   355,
     356,    -1,   358,    -1,    -1,    -1,    -1,    -1,    -1,   365,
     366,   367,   368,   369,   370,   371,   372,   373,   374,   375,
     376,   377,   378,   379,   380,   381,   382,   383,   384,   385,
     386,   387,   388,   389,   390,   391,   392,   393,   394,   395,
     396,   397,   398,   399,   400,   401,   402,   403,   404,   405,
     406,   407,   408,   409,   410,   411,   412,   413,   414,   415,
     416,   417,   418,   419,   420,     6,    -1,    -1,    -1,    10,
      -1,    -1,    -1,    -1,    -1,    -1,    17,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    27,    -1,    -1,    -1,
      -1,    32,    -1,    -1,    35,    36,    37,    -1,    -1,    -1,
      41,    42,    -1,    -1,    -1,    -1,    47,    -1,    49,    50,
      51,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    69,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    78,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   129,    -1,
      -1,    -1,    -1,   134,   135,   136,    -1,    -1,    -1,   140,
     141,    -1,    -1,    -1,   145,    -1,   147,   148,   149,   150,
     151,   152,   153,   154,   155,   156,   157,    -1,    -1,    -1,
      -1,    -1,   163,    -1,   165,   166,   167,    -1,    -1,   170,
     171,   172,   173,   174,   175,   176,   177,   178,   179,   180,
     181,   182,   183,   184,    -1,   186,    -1,   188,   189,   190,
      -1,    -1,    -1,   194,    -1,    -1,    -1,   198,   199,   200,
     201,   202,   203,   204,   205,   206,   207,   208,   209,   210,
     211,   212,   213,   214,   215,   216,   217,   218,   219,   220,
     221,   222,   223,   224,    -1,   226,   227,   228,   229,   230,
      -1,   232,   233,   234,   235,   236,   237,   238,   239,   240,
     241,   242,   243,    -1,   245,   246,   247,   248,   249,   250,
      -1,   252,   253,   254,   255,   256,   257,   258,   259,   260,
     261,   262,   263,   264,   265,    -1,    -1,    -1,   269,   270,
     271,   272,   273,    -1,   275,   276,   277,   278,    -1,    -1,
     281,   282,   283,   284,   285,   286,   287,   288,   289,   290,
     291,   292,   293,   294,   295,   296,   297,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   350,
     351,   352,   353,   354,   355,   356,    -1,   358,    -1,    -1,
      -1,    -1,    -1,    -1,   365,   366,   367,   368,   369,   370,
     371,   372,   373,   374,   375,   376,   377,   378,   379,   380,
     381,   382,   383,   384,   385,   386,   387,   388,   389,   390,
     391,   392,   393,   394,   395,   396,   397,   398,   399,   400,
     401,   402,   403,   404,   405,   406,   407,   408,   409,   410,
     411,   412,   413,   414,   415,   416,   417,   418,   419,   420,
       6,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    17,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    27,    -1,    -1,    -1,    -1,    32,    -1,    -1,    35,
      36,    37,    -1,    -1,    -1,    41,    42,    -1,    -1,    -1,
      -1,    47,    -1,    49,    50,    51,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      66,    -1,    -1,    69,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    78,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,   129,    -1,    -1,    -1,    -1,   134,   135,
     136,    -1,    -1,    -1,   140,   141,    -1,    -1,    -1,   145,
      -1,   147,   148,   149,   150,   151,   152,   153,   154,   155,
     156,   157,    -1,    -1,    -1,    -1,    -1,   163,    -1,   165,
     166,   167,    -1,    -1,   170,   171,   172,   173,   174,   175,
     176,   177,   178,   179,   180,   181,   182,   183,   184,    -1,
     186,    -1,   188,   189,   190,    -1,    -1,    -1,   194,    -1,
      -1,    -1,   198,   199,   200,   201,   202,   203,   204,   205,
     206,   207,   208,   209,   210,   211,   212,   213,   214,   215,
     216,   217,   218,   219,   220,   221,   222,   223,   224,    -1,
     226,   227,   228,   229,   230,    -1,   232,   233,   234,   235,
     236,   237,   238,   239,   240,   241,   242,   243,    -1,   245,
     246,   247,   248,   249,   250,    -1,   252,   253,   254,   255,
     256,   257,   258,   259,   260,   261,   262,   263,   264,   265,
      -1,    -1,    -1,   269,   270,   271,   272,   273,    -1,   275,
     276,   277,   278,    -1,    -1,   281,   282,   283,   284,   285,
     286,   287,   288,   289,   290,   291,   292,   293,   294,   295,
     296,   297,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,   350,   351,   352,   353,   354,   355,
     356,    -1,   358,    -1,    -1,    -1,    -1,    -1,    -1,   365,
     366,   367,   368,   369,   370,   371,   372,   373,   374,   375,
     376,   377,   378,   379,   380,   381,   382,   383,   384,   385,
     386,   387,   388,   389,   390,   391,   392,   393,   394,   395,
     396,   397,   398,   399,   400,   401,   402,   403,   404,   405,
     406,   407,   408,   409,   410,   411,   412,   413,   414,   415,
     416,   417,   418,   419,   420,     6,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    17,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    26,    27,    -1,    -1,    -1,
      -1,    32,    -1,    -1,    35,    36,    37,    -1,    -1,    -1,
      41,    42,    -1,    -1,    -1,    -1,    47,    -1,    49,    50,
      51,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    69,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    78,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   129,    -1,
      -1,    -1,    -1,   134,   135,   136,    -1,    -1,    -1,   140,
     141,    -1,    -1,    -1,   145,    -1,   147,   148,   149,   150,
     151,   152,   153,   154,   155,   156,   157,    -1,    -1,    -1,
      -1,    -1,   163,    -1,   165,   166,   167,    -1,    -1,   170,
     171,   172,   173,   174,   175,   176,   177,   178,   179,   180,
     181,   182,   183,   184,    -1,   186,    -1,   188,   189,   190,
      -1,    -1,    -1,   194,    -1,    -1,    -1,   198,   199,   200,
     201,   202,   203,   204,   205,   206,   207,   208,   209,   210,
     211,   212,   213,   214,   215,   216,   217,   218,   219,   220,
     221,   222,   223,   224,    -1,   226,   227,   228,   229,   230,
      -1,   232,   233,   234,   235,   236,   237,   238,   239,   240,
     241,   242,   243,    -1,   245,   246,   247,   248,   249,   250,
      -1,   252,   253,   254,   255,   256,   257,   258,   259,   260,
     261,   262,   263,   264,   265,    -1,    -1,    -1,   269,   270,
     271,   272,   273,    -1,   275,   276,   277,   278,    -1,    -1,
     281,   282,   283,   284,   285,   286,   287,   288,   289,   290,
     291,   292,   293,   294,   295,   296,   297,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   350,
     351,   352,   353,   354,   355,   356,    -1,   358,    -1,    -1,
      -1,    -1,    -1,    -1,   365,   366,   367,   368,   369,   370,
     371,   372,   373,   374,   375,   376,   377,   378,   379,   380,
     381,   382,   383,   384,   385,   386,   387,   388,   389,   390,
     391,   392,   393,   394,   395,   396,   397,   398,   399,   400,
     401,   402,   403,   404,   405,   406,   407,   408,   409,   410,
     411,   412,   413,   414,   415,   416,   417,   418,   419,   420,
       6,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    17,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    27,    -1,    -1,    -1,    -1,    32,    -1,    -1,    35,
      36,    37,    -1,    -1,    -1,    41,    42,    -1,    -1,    -1,
      -1,    47,    -1,    49,    50,    51,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      66,    -1,    -1,    69,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    78,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,   129,    -1,    -1,    -1,    -1,   134,   135,
     136,    -1,    -1,    -1,   140,   141,    -1,    -1,    -1,   145,
      -1,   147,   148,   149,   150,   151,   152,   153,   154,   155,
     156,   157,    -1,    -1,    -1,    -1,    -1,   163,    -1,   165,
     166,   167,    -1,    -1,   170,   171,   172,   173,   174,   175,
     176,   177,   178,   179,   180,   181,   182,   183,   184,    -1,
     186,    -1,   188,   189,   190,    -1,    -1,    -1,   194,    -1,
      -1,    -1,   198,   199,   200,   201,   202,   203,   204,   205,
     206,   207,   208,   209,   210,   211,   212,   213,   214,   215,
     216,   217,   218,   219,   220,   221,   222,   223,   224,    -1,
     226,   227,   228,   229,   230,    -1,   232,   233,   234,   235,
     236,   237,   238,   239,   240,   241,   242,   243,    -1,   245,
     246,   247,   248,   249,   250,    -1,   252,   253,   254,   255,
     256,   257,   258,   259,   260,   261,   262,   263,   264,   265,
      -1,    -1,    -1,   269,   270,   271,   272,   273,    -1,   275,
     276,   277,   278,    -1,    -1,   281,   282,   283,   284,   285,
     286,   287,   288,   289,   290,   291,   292,   293,   294,   295,
     296,   297,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,   350,   351,   352,   353,   354,   355,
     356,    -1,   358,    -1,    -1,    -1,    -1,    -1,    -1,   365,
     366,   367,   368,   369,   370,   371,   372,   373,   374,   375,
     376,   377,   378,   379,   380,   381,   382,   383,   384,   385,
     386,   387,   388,   389,   390,   391,   392,   393,   394,   395,
     396,   397,   398,   399,   400,   401,   402,   403,   404,   405,
     406,   407,   408,   409,   410,   411,   412,   413,   414,   415,
     416,   417,   418,   419,   420,     6,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    17,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    27,    -1,    -1,    -1,
      -1,    32,    -1,    -1,    35,    36,    37,    -1,    -1,    -1,
      41,    42,    -1,    -1,    -1,    -1,    47,    -1,    49,    50,
      51,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    69,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    78,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   129,    -1,
      -1,    -1,    -1,   134,   135,   136,    -1,    -1,    -1,   140,
     141,    -1,    -1,    -1,   145,    -1,   147,   148,   149,   150,
     151,   152,   153,   154,   155,   156,   157,    -1,    -1,    -1,
      -1,    -1,   163,    -1,   165,   166,   167,    -1,    -1,   170,
     171,   172,   173,   174,   175,   176,   177,   178,   179,   180,
     181,   182,   183,   184,    -1,   186,    -1,   188,   189,   190,
      -1,    -1,    -1,   194,    -1,    -1,    -1,   198,   199,   200,
     201,   202,   203,   204,   205,   206,   207,   208,   209,   210,
     211,   212,   213,   214,   215,   216,   217,   218,   219,   220,
     221,   222,   223,   224,    -1,   226,   227,   228,   229,   230,
      -1,   232,   233,   234,   235,   236,   237,   238,   239,   240,
     241,   242,   243,    -1,   245,   246,   247,   248,   249,   250,
      -1,   252,   253,   254,   255,   256,   257,   258,   259,   260,
     261,   262,   263,   264,   265,    -1,    -1,    -1,   269,   270,
     271,   272,   273,    -1,   275,   276,   277,   278,    -1,    -1,
     281,   282,   283,   284,   285,   286,   287,   288,   289,   290,
     291,   292,   293,   294,   295,   296,   297,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   350,
     351,   352,   353,   354,   355,   356,    -1,   358,    -1,    -1,
      -1,    -1,    -1,    -1,   365,   366,   367,   368,   369,   370,
     371,   372,   373,   374,   375,   376,   377,   378,   379,   380,
     381,   382,   383,   384,   385,   386,   387,   388,   389,   390,
     391,   392,   393,   394,   395,   396,   397,   398,   399,   400,
     401,   402,   403,   404,   405,   406,   407,   408,   409,   410,
     411,   412,   413,   414,   415,   416,   417,   418,   419,   420,
       6,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    17,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    27,    -1,    -1,    -1,    -1,    32,    -1,    -1,    35,
      36,    37,    -1,    -1,    -1,    41,    42,    -1,    -1,    -1,
      -1,    47,    -1,    49,    50,    51,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    69,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    78,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,   129,    -1,    -1,    -1,    -1,   134,   135,
     136,    -1,    -1,    -1,   140,   141,    -1,    -1,    -1,   145,
      -1,   147,   148,   149,   150,   151,   152,   153,   154,   155,
     156,   157,    -1,    -1,    -1,    -1,    -1,   163,    -1,   165,
     166,   167,    -1,    -1,   170,   171,   172,   173,   174,   175,
     176,   177,   178,   179,   180,   181,   182,   183,   184,    -1,
     186,    -1,   188,   189,   190,    -1,    -1,    -1,   194,    -1,
      -1,    -1,   198,   199,   200,   201,   202,   203,   204,   205,
     206,   207,   208,   209,   210,   211,   212,   213,   214,   215,
     216,   217,   218,   219,   220,   221,   222,   223,   224,    -1,
     226,   227,   228,   229,   230,    -1,   232,   233,   234,   235,
     236,   237,   238,   239,   240,   241,   242,   243,    -1,   245,
     246,   247,   248,   249,   250,    -1,   252,   253,   254,   255,
     256,   257,   258,   259,   260,   261,   262,   263,   264,   265,
      -1,    -1,    -1,   269,   270,   271,   272,   273,    -1,   275,
     276,   277,   278,    -1,    -1,   281,   282,   283,   284,   285,
     286,   287,   288,   289,   290,   291,   292,   293,   294,   295,
     296,   297,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,   350,   351,   352,   353,   354,   355,
     356,    -1,   358,    -1,    -1,    -1,    -1,    -1,    -1,   365,
     366,   367,   368,   369,   370,   371,   372,   373,   374,   375,
     376,   377,   378,   379,   380,   381,   382,   383,   384,   385,
     386,   387,   388,   389,   390,   391,   392,   393,   394,   395,
     396,   397,   398,   399,   400,   401,   402,   403,   404,   405,
     406,   407,   408,   409,   410,   411,   412,   413,   414,   415,
     416,   417,   418,   419,   420,     6,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    17,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    27,    -1,    -1,    -1,
      -1,    32,    -1,    -1,    35,    36,    37,    -1,    -1,    -1,
      41,    42,    -1,    -1,    -1,    -1,    47,    -1,    49,    50,
      51,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    69,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    78,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   129,    -1,
      -1,    -1,    -1,   134,   135,   136,    -1,    -1,    -1,   140,
     141,    -1,    -1,    -1,   145,    -1,   147,   148,   149,   150,
     151,   152,   153,   154,   155,   156,   157,    -1,    -1,    -1,
      -1,    -1,   163,    -1,   165,   166,   167,    -1,    -1,   170,
     171,   172,   173,   174,   175,   176,   177,   178,   179,   180,
     181,   182,   183,   184,    -1,   186,    -1,   188,   189,   190,
      -1,    -1,    -1,   194,    -1,    -1,    -1,   198,   199,   200,
     201,   202,   203,   204,   205,   206,   207,   208,   209,   210,
     211,   212,   213,   214,   215,   216,   217,   218,   219,   220,
     221,   222,   223,   224,    -1,   226,   227,   228,   229,   230,
      -1,   232,   233,   234,   235,   236,   237,   238,   239,   240,
     241,   242,   243,    -1,   245,   246,   247,   248,   249,   250,
      -1,   252,   253,   254,   255,   256,   257,   258,   259,   260,
     261,   262,   263,   264,   265,    -1,    -1,    -1,   269,   270,
     271,   272,   273,    -1,   275,   276,   277,   278,    -1,    -1,
     281,   282,   283,   284,   285,   286,   287,   288,   289,   290,
     291,   292,   293,   294,   295,   296,   297,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   350,
     351,   352,   353,   354,   355,   356,    -1,   358,    -1,    -1,
      -1,    -1,    -1,    -1,   365,   366,   367,   368,   369,   370,
     371,   372,   373,   374,   375,   376,   377,   378,   379,   380,
     381,   382,   383,   384,   385,   386,   387,   388,   389,   390,
     391,   392,   393,   394,   395,   396,   397,   398,   399,   400,
     401,   402,   403,   404,   405,   406,   407,   408,   409,   410,
     411,   412,   413,   414,   415,   416,   417,   418,   419,   420,
       6,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    17,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    27,    -1,    -1,    -1,    -1,    32,    -1,    -1,    35,
      36,    37,    -1,    -1,    -1,    41,    42,    -1,    -1,    -1,
      -1,    47,    -1,    49,    50,    51,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    69,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    78,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,   129,    -1,    -1,    -1,    -1,   134,   135,
     136,    -1,    -1,    -1,   140,   141,    -1,    -1,    -1,   145,
      -1,   147,   148,   149,   150,   151,   152,   153,   154,   155,
     156,   157,    -1,    -1,    -1,    -1,    -1,   163,    -1,   165,
     166,   167,    -1,    -1,   170,   171,   172,   173,   174,   175,
     176,   177,   178,   179,   180,   181,   182,   183,   184,    -1,
     186,    -1,   188,   189,   190,    -1,    -1,    -1,   194,    -1,
      -1,    -1,   198,   199,   200,   201,   202,   203,   204,   205,
     206,   207,   208,   209,   210,   211,   212,   213,   214,   215,
     216,   217,   218,   219,   220,   221,   222,   223,   224,    -1,
     226,   227,   228,   229,   230,    -1,   232,   233,   234,   235,
     236,   237,   238,   239,   240,   241,   242,   243,    -1,   245,
     246,   247,   248,   249,   250,    -1,   252,   253,   254,   255,
     256,   257,   258,   259,   260,   261,   262,   263,   264,   265,
      -1,    -1,    -1,   269,   270,   271,   272,   273,    -1,   275,
     276,   277,   278,    -1,    -1,   281,   282,   283,   284,   285,
     286,   287,   288,   289,   290,   291,   292,   293,   294,   295,
     296,   297,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,   350,   351,   352,   353,   354,   355,
     356,    -1,   358,    -1,    -1,    -1,    -1,    -1,    -1,   365,
     366,   367,   368,   369,   370,   371,   372,   373,   374,   375,
     376,   377,   378,   379,   380,   381,   382,   383,   384,   385,
     386,   387,   388,   389,   390,   391,   392,   393,   394,   395,
     396,   397,   398,   399,   400,   401,   402,   403,   404,   405,
     406,   407,   408,   409,   410,   411,   412,   413,   414,   415,
     416,   417,   418,   419,   420,     6,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    17,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    27,    -1,    -1,    -1,
      -1,    32,    -1,    -1,    35,    36,    37,    -1,    -1,    -1,
      41,    42,    -1,    -1,    -1,    -1,    47,    -1,    49,    50,
      51,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    69,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    78,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   129,    -1,
      -1,    -1,    -1,   134,   135,   136,    -1,    -1,    -1,   140,
     141,    -1,    -1,    -1,   145,    -1,   147,   148,   149,   150,
     151,   152,   153,   154,   155,   156,   157,    -1,    -1,    -1,
      -1,    -1,   163,    -1,   165,   166,   167,    -1,    -1,   170,
     171,   172,   173,   174,   175,   176,   177,   178,   179,   180,
     181,   182,   183,   184,    -1,   186,    -1,   188,   189,   190,
      -1,    -1,    -1,   194,    -1,    -1,    -1,   198,   199,   200,
     201,   202,   203,   204,   205,   206,   207,   208,   209,   210,
     211,   212,   213,   214,   215,   216,   217,   218,   219,   220,
     221,   222,   223,   224,    -1,   226,   227,   228,   229,   230,
      -1,   232,   233,   234,   235,   236,   237,   238,   239,   240,
     241,   242,   243,    -1,   245,   246,   247,   248,   249,   250,
      -1,   252,   253,   254,   255,   256,   257,   258,   259,   260,
     261,   262,   263,   264,   265,    -1,    -1,    -1,   269,   270,
     271,   272,   273,    -1,   275,   276,   277,   278,    -1,    -1,
     281,   282,   283,   284,   285,   286,   287,   288,   289,   290,
     291,   292,   293,   294,   295,   296,   297,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   350,
     351,   352,   353,   354,   355,   356,    -1,   358,    -1,    -1,
      -1,    -1,    -1,    -1,   365,   366,   367,   368,   369,   370,
     371,   372,   373,   374,   375,   376,   377,   378,   379,   380,
     381,   382,   383,   384,   385,   386,   387,   388,   389,   390,
     391,   392,   393,   394,   395,   396,   397,   398,   399,   400,
     401,   402,   403,   404,   405,   406,   407,   408,   409,   410,
     411,   412,   413,   414,   415,   416,   417,   418,   419,   420,
       6,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    17,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    27,    -1,    -1,    -1,    -1,    32,    -1,    -1,    35,
      36,    37,    -1,    -1,    -1,    41,    42,    -1,    -1,    -1,
      -1,    47,    -1,    49,    50,    51,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    69,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    78,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,   129,    -1,    -1,    -1,    -1,   134,   135,
     136,    -1,    -1,    -1,   140,   141,    -1,    -1,    -1,   145,
      -1,   147,   148,   149,   150,   151,   152,   153,   154,   155,
     156,   157,    -1,    -1,    -1,    -1,    -1,   163,    -1,   165,
     166,   167,    -1,    -1,   170,   171,   172,   173,   174,   175,
     176,   177,   178,   179,   180,   181,   182,   183,   184,    -1,
     186,    -1,   188,   189,   190,    -1,    -1,    -1,   194,    -1,
      -1,    -1,   198,   199,   200,   201,   202,   203,   204,   205,
     206,   207,   208,   209,   210,   211,   212,   213,   214,   215,
     216,   217,   218,   219,   220,   221,   222,   223,   224,    -1,
     226,   227,   228,   229,   230,    -1,   232,   233,   234,   235,
     236,   237,   238,   239,   240,   241,   242,   243,    -1,   245,
     246,   247,   248,   249,   250,    -1,   252,   253,   254,   255,
     256,   257,   258,   259,   260,   261,   262,   263,   264,   265,
      -1,    -1,    -1,   269,   270,   271,   272,   273,    -1,   275,
     276,   277,   278,    -1,    -1,   281,   282,   283,   284,   285,
     286,   287,   288,   289,   290,   291,   292,   293,   294,   295,
     296,   297,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,   350,   351,   352,   353,   354,   355,
     356,    -1,   358,    -1,    -1,    -1,    -1,    -1,    -1,   365,
     366,   367,   368,   369,   370,   371,   372,   373,   374,   375,
     376,   377,   378,   379,   380,   381,   382,   383,   384,   385,
     386,   387,   388,   389,   390,   391,   392,   393,   394,   395,
     396,   397,   398,   399,   400,   401,   402,   403,   404,   405,
     406,   407,   408,   409,   410,   411,   412,   413,   414,   415,
     416,   417,   418,   419,   420
};

/* YYSTOS[STATE-NUM] -- The (internal number of the) accessing
   symbol of state STATE-NUM.  */
static const yytype_uint16 yystos[] =
{
       0,     5,     6,     7,     8,     9,    19,    25,    29,    30,
      31,    36,    55,    66,   128,   129,   130,   131,   132,   134,
     160,   161,   162,   165,   166,   167,   211,   213,   214,   216,
     217,   308,   425,   427,   428,   429,   430,   431,   432,   434,
     435,   436,   437,   443,   449,   471,   476,   485,   535,   536,
     538,   539,   545,   547,   548,   549,   550,   559,   560,   561,
     562,   563,   564,   565,   568,   569,   570,   571,   691,   572,
     573,   572,   572,   572,    69,   572,   276,   276,    35,     6,
      17,    27,    32,    35,    36,    37,    41,    42,    47,    49,
      50,    51,    69,    78,   129,   134,   135,   136,   140,   141,
     145,   147,   148,   149,   150,   151,   152,   153,   154,   155,
     156,   157,   163,   165,   166,   167,   170,   171,   172,   173,
     174,   175,   176,   177,   178,   179,   180,   181,   182,   183,
     184,   186,   188,   189,   190,   194,   198,   199,   200,   201,
     202,   203,   204,   205,   206,   207,   208,   209,   210,   211,
     212,   213,   214,   215,   216,   217,   218,   219,   220,   221,
     222,   223,   224,   226,   227,   228,   229,   230,   232,   233,
     234,   235,   236,   237,   238,   239,   240,   241,   242,   243,
     245,   246,   247,   248,   249,   250,   252,   253,   254,   255,
     256,   257,   258,   259,   260,   261,   262,   263,   264,   265,
     269,   270,   271,   272,   273,   275,   276,   277,   278,   281,
     282,   283,   284,   285,   286,   287,   288,   289,   290,   291,
     292,   293,   294,   295,   296,   297,   350,   351,   352,   353,
     354,   355,   356,   358,   365,   366,   367,   368,   369,   370,
     371,   372,   373,   374,   375,   376,   377,   378,   379,   380,
     381,   382,   383,   384,   385,   386,   387,   388,   389,   390,
     391,   392,   393,   394,   395,   396,   397,   398,   399,   400,
     401,   402,   403,   404,   405,   406,   407,   408,   409,   410,
     411,   412,   413,   414,   415,   416,   417,   418,   419,   420,
     677,   689,    44,    84,   137,   138,   141,   147,   148,   189,
     299,   459,   672,   673,   141,   143,   278,   667,   137,   138,
     139,   141,   174,   175,   299,   138,   138,   693,    32,    69,
     123,   128,   138,   139,   140,   174,   177,   182,   241,   273,
     274,   275,   277,   278,   279,   283,   284,   286,   287,   288,
     518,   553,   558,   693,   693,   138,   608,   677,   212,   472,
     215,     0,   423,   426,    14,   647,     4,   575,   572,    66,
     100,   486,   608,   693,   132,   168,   567,   429,    52,    53,
     576,   574,    23,    28,    46,    60,    61,    69,    70,    71,
      72,    73,    74,    75,    79,    80,    81,    82,    86,    87,
      93,   110,   111,   112,   115,   118,   121,   122,   123,   124,
     137,   144,   158,   298,   299,   300,   301,   302,   303,   304,
     305,   306,   307,   308,   309,   310,   311,   312,   313,   315,
     316,   321,   322,   323,   324,   325,   326,   327,   328,   329,
     330,   331,   332,   333,   334,   335,   336,   337,   338,   339,
     340,   341,   342,   343,   344,   345,   346,   347,   348,   349,
     581,   612,   614,   615,   620,   622,   623,   624,   625,   626,
     627,   630,   639,   640,   658,   659,   675,   677,   689,   691,
     667,    10,   440,   441,   608,   609,   677,    66,   477,   215,
     241,   285,   286,   287,   288,   447,   639,   660,   661,   662,
     675,   677,   693,   693,   272,   680,   308,   144,   666,   666,
     608,    95,   666,   174,   175,   139,   608,    65,   128,   130,
     133,   138,   144,   665,   665,    69,   675,   689,   665,   665,
     665,   665,   546,   608,   608,     4,     5,     7,     8,     9,
      10,    11,    12,    13,    14,    15,    16,    18,    19,    21,
      22,    23,    24,    25,    26,    28,    29,    30,    31,    33,
      43,    44,    46,    48,    54,    55,    56,    57,    58,    59,
      60,    61,    62,    63,    64,    65,    80,    81,    82,    83,
      84,    85,    87,    88,    89,    90,    91,    92,    93,    94,
      98,    99,   100,   101,   115,   116,   120,   123,   124,   128,
     130,   131,   133,   137,   138,   139,   142,   143,   144,   158,
     159,   160,   161,   162,   164,   169,   191,   192,   193,   195,
     196,   197,   267,   274,   298,   299,   300,   302,   303,   304,
     305,   306,   307,   308,   309,   311,   312,   314,   315,   316,
     317,   318,   319,   320,   321,   322,   323,   324,   325,   326,
     327,   328,   329,   330,   331,   332,   333,   334,   335,   336,
     337,   349,   357,   361,   362,   363,   364,   421,   675,   676,
     688,   691,   694,    69,    69,   137,   138,   141,   174,   175,
     177,   299,   694,   180,   180,    29,    11,   100,   551,   694,
     694,   181,   179,   276,   694,    10,    98,   526,   176,   276,
     280,   281,   282,   552,   554,   180,   181,   608,   125,    49,
      71,    29,    15,    16,   651,    20,    21,     5,   431,   433,
     691,   667,   487,   489,   676,   608,   429,    95,   429,   434,
     436,   437,   566,    68,   692,    20,    21,    48,   577,    77,
     620,   691,   613,   691,   691,   691,   691,   622,   677,   612,
     612,   634,   622,   622,   691,   622,   622,   622,   622,   622,
     691,   691,   691,   628,   629,   691,   691,   628,   691,   628,
     628,   629,   628,   629,   628,   629,   628,   628,   629,   628,
     629,   691,   691,   691,   691,   691,   691,   691,   691,   691,
     691,   691,   691,   691,   691,   691,   691,   691,   691,   691,
     691,   691,   691,   691,   691,   691,   691,   691,   691,   691,
     691,   691,   691,   691,   691,   691,   691,   691,    10,    22,
      69,    71,    84,    85,   101,   321,   322,   323,   324,   325,
     326,   327,   328,   329,   330,   331,   332,   333,   334,   335,
     336,   337,   338,   339,   340,   341,   342,   343,   344,   345,
     346,   347,   348,   584,   585,   689,   690,    87,    94,    95,
      96,    97,    98,    99,   100,   102,   103,   104,   105,   106,
     107,   108,   109,   110,   111,   112,   113,   114,   115,   116,
     117,   120,   617,   126,   127,   691,   125,   429,   612,   621,
     691,   587,   588,   589,   590,   596,   608,   620,   691,   440,
     608,    10,    64,    67,   438,    67,   438,   125,    68,   621,
     289,   291,   444,   445,    29,   215,   660,    95,    67,    28,
      71,   663,   675,   125,   422,   679,   141,    87,    69,   608,
      22,    69,    69,    69,    69,   675,   694,   608,   178,   138,
     178,   138,   183,   608,    23,    69,   440,    65,   439,   608,
      69,    69,    69,    67,   142,   694,   694,   666,   608,   608,
     694,   608,   694,   666,    11,   100,   556,   556,   694,   612,
     622,   100,   557,    65,   608,    10,    98,   555,   555,    10,
     694,   694,   677,   678,   688,    71,   199,   200,   201,   202,
     203,   204,   205,   206,   207,   208,   209,   210,   473,   474,
     475,   321,   322,   323,   324,   325,   326,   327,   328,   329,
     330,   331,   332,   333,   334,   335,   336,   337,   338,   339,
     340,   341,   342,   343,   344,   345,   346,   347,   348,   612,
     648,   649,   690,   612,    18,    30,   652,   572,   647,   429,
      26,   607,   608,    67,    68,    29,   123,   163,   218,   219,
     220,   221,   222,   223,   224,   226,   227,   228,   229,   230,
     231,   232,   234,   235,   236,   237,   238,   239,   240,   241,
     242,   243,   244,   245,   247,   248,   249,   250,   251,   252,
     253,   254,   255,   256,   257,   258,   259,   260,   261,   262,
     263,   264,   265,   351,   491,   492,   493,   494,   495,   496,
     497,    69,    49,    72,    75,   641,    62,   578,    69,   639,
     580,   581,   580,   125,    92,   636,   637,   580,   675,   692,
     580,   580,   622,   692,   579,   580,   580,   612,   612,    71,
     580,   639,    71,   580,   639,   577,   675,   675,   577,   580,
     580,   580,   577,   580,   580,   577,   577,   580,   580,   580,
     580,   577,   580,   580,   580,   692,   692,   581,   580,   581,
     580,   580,   692,   692,   692,   692,   580,   608,   585,   612,
     612,    80,    81,    82,    87,   616,    94,    98,    99,   100,
     622,    76,   619,   620,   658,   622,   622,   622,   622,   622,
     622,   622,   622,   622,   622,   622,   622,   622,   622,    69,
      71,   632,   622,   640,   640,   577,   112,   676,   678,   688,
     692,    67,   692,   429,    29,    67,    56,    57,    59,    60,
      61,    62,    63,   602,   603,   604,   605,    22,    54,    55,
      71,   143,   170,   591,   601,   610,   677,   690,    22,   600,
     587,   691,    64,   170,   442,   608,   587,   609,   677,   587,
     112,    68,   290,   292,   293,    67,   444,    65,    83,   612,
     661,   120,   509,   676,   678,    71,    26,   606,   608,    23,
     694,   448,   694,    66,   159,   186,   460,    64,   674,   130,
     133,   608,   608,   608,   608,   608,    14,    28,    69,    78,
     129,   130,   131,   133,   135,   136,   170,   171,   241,   267,
     309,   356,   542,   669,   608,    67,   608,   608,    69,    69,
     612,    71,   164,   618,    71,   608,    10,    98,   527,   677,
     677,   556,    22,   592,   608,    67,    95,    24,    25,   650,
      67,   650,    17,    67,     8,    98,   576,   651,   692,   608,
     442,    43,    44,   139,   145,   147,   153,   154,   489,   512,
     517,   519,   524,   525,    71,    72,   531,   532,   533,   676,
      66,    66,   499,    66,   500,   233,   500,   500,    66,   501,
     501,   501,   501,   499,   499,   499,   499,   499,   246,   499,
     499,   499,   499,   238,   239,   241,   241,   508,   508,   508,
     508,   239,   508,    66,   490,   270,   502,   499,    46,    46,
      10,    18,   664,   580,   692,   692,    67,   692,   692,   612,
      90,   637,   638,   692,   692,   692,   692,   692,   692,    64,
      67,    22,    10,   692,    10,    10,   692,    10,   580,    67,
      67,   580,   692,   692,   692,   580,   692,   692,   580,   580,
     692,   692,   692,   692,   580,   692,   692,   692,   317,   582,
     582,   692,   692,   692,   692,   692,   582,   582,   582,   582,
     692,    80,    81,    82,   622,   619,   622,   622,    85,   618,
     579,   691,   125,   612,   692,   660,   588,    63,   604,    63,
      63,    58,    63,    58,    63,   589,   589,   588,   589,    33,
     601,   139,   139,   139,   691,   610,   601,   692,   587,   691,
      11,   611,   611,   125,   611,   291,   294,   297,   446,   445,
      69,    71,   125,   138,   442,    22,   429,    69,    98,   191,
     192,   450,   451,   452,     7,     8,     9,   461,   675,    65,
     178,   178,   267,    64,   480,    43,   139,   145,   146,   150,
     153,   154,   170,   519,   540,   541,    22,   142,   146,   519,
     671,   153,   154,   519,   524,   534,   540,   541,   540,   540,
     170,    95,   516,   693,   694,   608,   142,   694,   694,   622,
      69,    69,   611,    33,   556,   555,   474,   640,   649,   612,
     612,    50,   652,    29,    46,   429,   653,   691,    69,   519,
     521,    43,   519,    69,    43,    66,   521,    67,    95,   533,
      71,   498,    72,    72,   500,    72,   508,   508,   499,   508,
     499,   499,   246,   499,    29,   509,   509,   509,   509,   508,
     509,   498,    28,    43,    44,    65,    78,    80,    87,   145,
     267,   504,   505,   506,   507,   510,   511,   271,   503,   608,
      10,   586,   581,    91,   612,    88,   632,   123,   163,   229,
     234,   236,   238,   243,   256,   269,   270,   633,   633,   622,
     622,   622,   622,   647,   622,   622,   692,   582,   583,   583,
     583,   692,   583,   583,   692,   692,   583,   583,   583,   583,
     692,   583,   583,   583,   675,   691,   582,   582,   582,   582,
     582,   350,    85,   618,   622,   692,   579,   112,   676,   611,
      63,    63,    64,    65,   597,   598,    65,   599,   597,   622,
     610,   691,   691,   691,   595,   675,   611,   595,   612,   647,
     295,   296,   291,   608,   508,   448,   448,   491,    69,    69,
      69,    68,    67,    65,   608,   675,   675,   675,    69,   481,
     353,   482,    69,    43,   675,    69,   693,    69,   675,   608,
      69,   521,   672,    66,    69,   488,   693,   488,    69,   675,
     612,   608,   622,   556,    51,   660,   656,   657,   658,   691,
      65,   655,   429,   654,   675,   521,   521,   154,   525,    66,
     522,   523,   675,   532,   533,    67,    68,    68,    67,    67,
      68,   509,   509,   508,   509,   499,    69,   123,   509,    68,
     622,    43,     8,    71,    80,    43,   587,   611,   612,   692,
     499,   501,   499,   499,   499,   499,   223,   223,   692,   692,
      18,    18,    18,    18,   314,   635,    67,    67,   583,   583,
     583,   583,   583,   170,   647,   691,   622,   692,   647,    66,
     612,   612,   600,   610,   594,   675,   594,   594,    67,   692,
     692,   651,   554,   685,   429,   491,   491,   491,    78,    87,
     155,   193,   194,   195,   196,   197,   352,   453,   454,   455,
     452,   608,    66,    66,    64,   483,   484,   676,    69,    22,
     142,   670,   142,   674,   519,   520,   488,   491,   158,   159,
     537,   537,   488,    26,   655,    67,   692,    27,   692,    67,
     692,   125,   594,    67,    68,   499,    71,   508,    72,    72,
     509,   508,   627,    12,   642,    69,   508,   622,   622,   622,
     622,    71,   692,   622,   622,    15,   692,   622,   651,   594,
     601,    67,   692,   692,   692,   675,   591,    38,   683,    37,
     686,   448,    71,   193,   197,   197,   197,   197,   198,   197,
     448,   455,    18,   522,   594,   608,    67,    95,    69,   675,
      66,   521,    68,   490,    69,   693,   693,   537,   691,   657,
      43,   675,    46,   429,   691,   675,    68,   523,    64,    78,
     149,   514,   515,   650,   509,    68,    68,   509,    15,    13,
     645,   692,   692,   692,   692,   692,   692,   621,    98,   353,
     631,    68,   610,   675,    15,    41,   681,   682,    42,   687,
     143,   668,    35,    35,   189,   190,    93,   144,   213,   429,
     434,   436,   437,   443,   449,   456,   457,   465,   468,   471,
     476,   535,   545,   547,   548,   549,   550,   561,   562,   563,
     565,   569,   184,    68,    68,   484,   533,   522,   674,   693,
     693,   170,   543,   544,     8,   125,   656,   429,   156,    69,
      71,   516,   515,   612,   643,   644,   690,   612,   646,   690,
     647,    56,   351,   354,   692,    71,    39,   684,    40,    15,
     683,    72,    66,   593,   612,   612,   456,   458,   448,   185,
     513,   514,   480,    68,    66,   675,    67,   692,   660,   675,
     692,   608,    72,    67,   692,   352,    51,   355,    15,    15,
      71,    37,   594,    92,   466,   467,    91,   423,   187,   188,
     462,   482,   513,   522,    46,   544,    66,   644,    51,    71,
      71,    68,   612,    88,    90,   467,   213,   429,   434,   436,
     437,   443,   464,   465,   468,   471,   478,   479,    88,   456,
      69,    69,   448,    68,   172,   594,   353,    91,    93,   478,
     478,   423,   434,   436,   437,   443,   463,   464,   513,   173,
      68,   354,   478,   423,   423,    88,    89,    90,   469,   470,
     479,   448,   691,    65,   528,   529,   355,   423,    88,    88,
     144,   612,   478,    88,    90,   470,   169,   622,     8,     9,
      65,   529,    65,   528,    93,    91,   423,   144,   478,   692,
     692,    29,   152,   155,   157,   530,   530,   478,    88,   423,
      28,    80,   151,   423,   144,    88,   144
};

#define yyerrok		(yyerrstatus = 0)
#define yyclearin	(yychar = YYEMPTY)
#define YYEMPTY		(-2)
#define YYEOF		0

#define YYACCEPT	goto yyacceptlab
#define YYABORT		goto yyabortlab
#define YYERROR		goto yyerrorlab


/* Like YYERROR except do call yyerror.  This remains here temporarily
   to ease the transition to the new meaning of YYERROR, for GCC.
   Once GCC version 2 has supplanted version 1, this can go.  */

#define YYFAIL		goto yyerrlab

#define YYRECOVERING()  (!!yyerrstatus)

#define YYBACKUP(Token, Value)					\
do								\
  if (yychar == YYEMPTY && yylen == 1)				\
    {								\
      yychar = (Token);						\
      yylval = (Value);						\
      yytoken = YYTRANSLATE (yychar);				\
      YYPOPSTACK (1);						\
      goto yybackup;						\
    }								\
  else								\
    {								\
      yyerror (YY_("syntax error: cannot back up")); \
      YYERROR;							\
    }								\
while (YYID (0))


#define YYTERROR	1
#define YYERRCODE	256


/* YYLLOC_DEFAULT -- Set CURRENT to span from RHS[1] to RHS[N].
   If N is 0, then set CURRENT to the empty location which ends
   the previous symbol: RHS[0] (always defined).  */

#define YYRHSLOC(Rhs, K) ((Rhs)[K])
#ifndef YYLLOC_DEFAULT
# define YYLLOC_DEFAULT(Current, Rhs, N)				\
    do									\
      if (YYID (N))                                                    \
	{								\
	  (Current).first_line   = YYRHSLOC (Rhs, 1).first_line;	\
	  (Current).first_column = YYRHSLOC (Rhs, 1).first_column;	\
	  (Current).last_line    = YYRHSLOC (Rhs, N).last_line;		\
	  (Current).last_column  = YYRHSLOC (Rhs, N).last_column;	\
	}								\
      else								\
	{								\
	  (Current).first_line   = (Current).last_line   =		\
	    YYRHSLOC (Rhs, 0).last_line;				\
	  (Current).first_column = (Current).last_column =		\
	    YYRHSLOC (Rhs, 0).last_column;				\
	}								\
    while (YYID (0))
#endif


/* YY_LOCATION_PRINT -- Print the location on the stream.
   This macro was not mandated originally: define only if we know
   we won't break user code: when these are the locations we know.  */

#ifndef YY_LOCATION_PRINT
# if defined YYLTYPE_IS_TRIVIAL && YYLTYPE_IS_TRIVIAL
#  define YY_LOCATION_PRINT(File, Loc)			\
     fprintf (File, "%d.%d-%d.%d",			\
	      (Loc).first_line, (Loc).first_column,	\
	      (Loc).last_line,  (Loc).last_column)
# else
#  define YY_LOCATION_PRINT(File, Loc) ((void) 0)
# endif
#endif


/* YYLEX -- calling `yylex' with the right arguments.  */

#ifdef YYLEX_PARAM
# define YYLEX yylex (YYLEX_PARAM)
#else
# define YYLEX yylex ()
#endif

/* Enable debugging if requested.  */
#if YYDEBUG

# ifndef YYFPRINTF
#  include <stdio.h> /* INFRINGES ON USER NAME SPACE */
#  define YYFPRINTF fprintf
# endif

# define YYDPRINTF(Args)			\
do {						\
  if (yydebug)					\
    YYFPRINTF Args;				\
} while (YYID (0))

# define YY_SYMBOL_PRINT(Title, Type, Value, Location)			  \
do {									  \
  if (yydebug)								  \
    {									  \
      YYFPRINTF (stderr, "%s ", Title);					  \
      yy_symbol_print (stderr,						  \
		  Type, Value); \
      YYFPRINTF (stderr, "\n");						  \
    }									  \
} while (YYID (0))


/*--------------------------------.
| Print this symbol on YYOUTPUT.  |
`--------------------------------*/

/*ARGSUSED*/
#if (defined __STDC__ || defined __C99__FUNC__ \
     || defined __cplusplus || defined _MSC_VER)
static void
yy_symbol_value_print (FILE *yyoutput, int yytype, YYSTYPE const * const yyvaluep)
#else
static void
yy_symbol_value_print (yyoutput, yytype, yyvaluep)
    FILE *yyoutput;
    int yytype;
    YYSTYPE const * const yyvaluep;
#endif
{
  if (!yyvaluep)
    return;
# ifdef YYPRINT
  if (yytype < YYNTOKENS)
    YYPRINT (yyoutput, yytoknum[yytype], *yyvaluep);
# else
  YYUSE (yyoutput);
# endif
  switch (yytype)
    {
      default:
	break;
    }
}


/*--------------------------------.
| Print this symbol on YYOUTPUT.  |
`--------------------------------*/

#if (defined __STDC__ || defined __C99__FUNC__ \
     || defined __cplusplus || defined _MSC_VER)
static void
yy_symbol_print (FILE *yyoutput, int yytype, YYSTYPE const * const yyvaluep)
#else
static void
yy_symbol_print (yyoutput, yytype, yyvaluep)
    FILE *yyoutput;
    int yytype;
    YYSTYPE const * const yyvaluep;
#endif
{
  if (yytype < YYNTOKENS)
    YYFPRINTF (yyoutput, "token %s (", yytname[yytype]);
  else
    YYFPRINTF (yyoutput, "nterm %s (", yytname[yytype]);

  yy_symbol_value_print (yyoutput, yytype, yyvaluep);
  YYFPRINTF (yyoutput, ")");
}

/*------------------------------------------------------------------.
| yy_stack_print -- Print the state stack from its BOTTOM up to its |
| TOP (included).                                                   |
`------------------------------------------------------------------*/

#if (defined __STDC__ || defined __C99__FUNC__ \
     || defined __cplusplus || defined _MSC_VER)
static void
yy_stack_print (yytype_int16 *bottom, yytype_int16 *top)
#else
static void
yy_stack_print (bottom, top)
    yytype_int16 *bottom;
    yytype_int16 *top;
#endif
{
  YYFPRINTF (stderr, "Stack now");
  for (; bottom <= top; ++bottom)
    YYFPRINTF (stderr, " %d", *bottom);
  YYFPRINTF (stderr, "\n");
}

# define YY_STACK_PRINT(Bottom, Top)				\
do {								\
  if (yydebug)							\
    yy_stack_print ((Bottom), (Top));				\
} while (YYID (0))


/*------------------------------------------------.
| Report that the YYRULE is going to be reduced.  |
`------------------------------------------------*/

#if (defined __STDC__ || defined __C99__FUNC__ \
     || defined __cplusplus || defined _MSC_VER)
static void
yy_reduce_print (YYSTYPE *yyvsp, int yyrule)
#else
static void
yy_reduce_print (yyvsp, yyrule)
    YYSTYPE *yyvsp;
    int yyrule;
#endif
{
  int yynrhs = yyr2[yyrule];
  int yyi;
  unsigned long int yylno = yyrline[yyrule];
  YYFPRINTF (stderr, "Reducing stack by rule %d (line %lu):\n",
	     yyrule - 1, yylno);
  /* The symbols being reduced.  */
  for (yyi = 0; yyi < yynrhs; yyi++)
    {
      fprintf (stderr, "   $%d = ", yyi + 1);
      yy_symbol_print (stderr, yyrhs[yyprhs[yyrule] + yyi],
		       &(yyvsp[(yyi + 1) - (yynrhs)])
		       		       );
      fprintf (stderr, "\n");
    }
}

# define YY_REDUCE_PRINT(Rule)		\
do {					\
  if (yydebug)				\
    yy_reduce_print (yyvsp, Rule); \
} while (YYID (0))

/* Nonzero means print parse trace.  It is left uninitialized so that
   multiple parsers can coexist.  */
int yydebug;
#else /* !YYDEBUG */
# define YYDPRINTF(Args)
# define YY_SYMBOL_PRINT(Title, Type, Value, Location)
# define YY_STACK_PRINT(Bottom, Top)
# define YY_REDUCE_PRINT(Rule)
#endif /* !YYDEBUG */


/* YYINITDEPTH -- initial size of the parser's stacks.  */
#ifndef	YYINITDEPTH
# define YYINITDEPTH 200
#endif

/* YYMAXDEPTH -- maximum size the stacks can grow to (effective only
   if the built-in stack extension method is used).

   Do not make this value too large; the results are undefined if
   YYSTACK_ALLOC_MAXIMUM < YYSTACK_BYTES (YYMAXDEPTH)
   evaluated with infinite-precision integer arithmetic.  */

#ifndef YYMAXDEPTH
# define YYMAXDEPTH 10000
#endif



#if YYERROR_VERBOSE

# ifndef yystrlen
#  if defined __GLIBC__ && defined _STRING_H
#   define yystrlen strlen
#  else
/* Return the length of YYSTR.  */
#if (defined __STDC__ || defined __C99__FUNC__ \
     || defined __cplusplus || defined _MSC_VER)
static YYSIZE_T
yystrlen (const char *yystr)
#else
static YYSIZE_T
yystrlen (yystr)
    const char *yystr;
#endif
{
  YYSIZE_T yylen;
  for (yylen = 0; yystr[yylen]; yylen++)
    continue;
  return yylen;
}
#  endif
# endif

# ifndef yystpcpy
#  if defined __GLIBC__ && defined _STRING_H && defined _GNU_SOURCE
#   define yystpcpy stpcpy
#  else
/* Copy YYSRC to YYDEST, returning the address of the terminating '\0' in
   YYDEST.  */
#if (defined __STDC__ || defined __C99__FUNC__ \
     || defined __cplusplus || defined _MSC_VER)
static char *
yystpcpy (char *yydest, const char *yysrc)
#else
static char *
yystpcpy (yydest, yysrc)
    char *yydest;
    const char *yysrc;
#endif
{
  char *yyd = yydest;
  const char *yys = yysrc;

  while ((*yyd++ = *yys++) != '\0')
    continue;

  return yyd - 1;
}
#  endif
# endif

# ifndef yytnamerr
/* Copy to YYRES the contents of YYSTR after stripping away unnecessary
   quotes and backslashes, so that it's suitable for yyerror.  The
   heuristic is that double-quoting is unnecessary unless the string
   contains an apostrophe, a comma, or backslash (other than
   backslash-backslash).  YYSTR is taken from yytname.  If YYRES is
   null, do not copy; instead, return the length of what the result
   would have been.  */
static YYSIZE_T
yytnamerr (char *yyres, const char *yystr)
{
  if (*yystr == '"')
    {
      YYSIZE_T yyn = 0;
      char const *yyp = yystr;

      for (;;)
	switch (*++yyp)
	  {
	  case '\'':
	  case ',':
	    goto do_not_strip_quotes;

	  case '\\':
	    if (*++yyp != '\\')
	      goto do_not_strip_quotes;
	    /* Fall through.  */
	  default:
	    if (yyres)
	      yyres[yyn] = *yyp;
	    yyn++;
	    break;

	  case '"':
	    if (yyres)
	      yyres[yyn] = '\0';
	    return yyn;
	  }
    do_not_strip_quotes: ;
    }

  if (! yyres)
    return yystrlen (yystr);

  return yystpcpy (yyres, yystr) - yyres;
}
# endif

/* Copy into YYRESULT an error message about the unexpected token
   YYCHAR while in state YYSTATE.  Return the number of bytes copied,
   including the terminating null byte.  If YYRESULT is null, do not
   copy anything; just return the number of bytes that would be
   copied.  As a special case, return 0 if an ordinary "syntax error"
   message will do.  Return YYSIZE_MAXIMUM if overflow occurs during
   size calculation.  */
static YYSIZE_T
yysyntax_error (char *yyresult, int yystate, int yychar)
{
  int yyn = yypact[yystate];

  if (! (YYPACT_NINF < yyn && yyn <= YYLAST))
    return 0;
  else
    {
      int yytype = YYTRANSLATE (yychar);
      YYSIZE_T yysize0 = yytnamerr (0, yytname[yytype]);
      YYSIZE_T yysize = yysize0;
      YYSIZE_T yysize1;
      int yysize_overflow = 0;
      enum { YYERROR_VERBOSE_ARGS_MAXIMUM = 5 };
      char const *yyarg[YYERROR_VERBOSE_ARGS_MAXIMUM];
      int yyx;

# if 0
      /* This is so xgettext sees the translatable formats that are
	 constructed on the fly.  */
      YY_("syntax error, unexpected %s");
      YY_("syntax error, unexpected %s, expecting %s");
      YY_("syntax error, unexpected %s, expecting %s or %s");
      YY_("syntax error, unexpected %s, expecting %s or %s or %s");
      YY_("syntax error, unexpected %s, expecting %s or %s or %s or %s");
# endif
      char *yyfmt;
      char const *yyf;
      static char const yyunexpected[] = "syntax error, unexpected %s";
      static char const yyexpecting[] = ", expecting %s";
      static char const yyor[] = " or %s";
      char yyformat[sizeof yyunexpected
		    + sizeof yyexpecting - 1
		    + ((YYERROR_VERBOSE_ARGS_MAXIMUM - 2)
		       * (sizeof yyor - 1))];
      char const *yyprefix = yyexpecting;

      /* Start YYX at -YYN if negative to avoid negative indexes in
	 YYCHECK.  */
      int yyxbegin = yyn < 0 ? -yyn : 0;

      /* Stay within bounds of both yycheck and yytname.  */
      int yychecklim = YYLAST - yyn + 1;
      int yyxend = yychecklim < YYNTOKENS ? yychecklim : YYNTOKENS;
      int yycount = 1;

      yyarg[0] = yytname[yytype];
      yyfmt = yystpcpy (yyformat, yyunexpected);

      for (yyx = yyxbegin; yyx < yyxend; ++yyx)
	if (yycheck[yyx + yyn] == yyx && yyx != YYTERROR)
	  {
	    if (yycount == YYERROR_VERBOSE_ARGS_MAXIMUM)
	      {
		yycount = 1;
		yysize = yysize0;
		yyformat[sizeof yyunexpected - 1] = '\0';
		break;
	      }
	    yyarg[yycount++] = yytname[yyx];
	    yysize1 = yysize + yytnamerr (0, yytname[yyx]);
	    yysize_overflow |= (yysize1 < yysize);
	    yysize = yysize1;
	    yyfmt = yystpcpy (yyfmt, yyprefix);
	    yyprefix = yyor;
	  }

      yyf = YY_(yyformat);
      yysize1 = yysize + yystrlen (yyf);
      yysize_overflow |= (yysize1 < yysize);
      yysize = yysize1;

      if (yysize_overflow)
	return YYSIZE_MAXIMUM;

      if (yyresult)
	{
	  /* Avoid sprintf, as that infringes on the user's name space.
	     Don't have undefined behavior even if the translation
	     produced a string with the wrong number of "%s"s.  */
	  char *yyp = yyresult;
	  int yyi = 0;
	  while ((*yyp = *yyf) != '\0')
	    {
	      if (*yyp == '%' && yyf[1] == 's' && yyi < yycount)
		{
		  yyp += yytnamerr (yyp, yyarg[yyi++]);
		  yyf += 2;
		}
	      else
		{
		  yyp++;
		  yyf++;
		}
	    }
	}
      return yysize;
    }
}
#endif /* YYERROR_VERBOSE */


/*-----------------------------------------------.
| Release the memory associated to this symbol.  |
`-----------------------------------------------*/

/*ARGSUSED*/
#if (defined __STDC__ || defined __C99__FUNC__ \
     || defined __cplusplus || defined _MSC_VER)
static void
yydestruct (const char *yymsg, int yytype, YYSTYPE *yyvaluep)
#else
static void
yydestruct (yymsg, yytype, yyvaluep)
    const char *yymsg;
    int yytype;
    YYSTYPE *yyvaluep;
#endif
{
  YYUSE (yyvaluep);

  if (!yymsg)
    yymsg = "Deleting";
  YY_SYMBOL_PRINT (yymsg, yytype, yyvaluep, yylocationp);

  switch (yytype)
    {

      default:
	break;
    }
}


/* Prevent warnings from -Wmissing-prototypes.  */

#ifdef YYPARSE_PARAM
#if defined __STDC__ || defined __cplusplus
int yyparse (void *YYPARSE_PARAM);
#else
int yyparse ();
#endif
#else /* ! YYPARSE_PARAM */
#if defined __STDC__ || defined __cplusplus
int yyparse (void);
#else
int yyparse ();
#endif
#endif /* ! YYPARSE_PARAM */



/* The look-ahead symbol.  */
int yychar;

/* The semantic value of the look-ahead symbol.  */
YYSTYPE yylval;

/* Number of syntax errors so far.  */
int yynerrs;



/*----------.
| yyparse.  |
`----------*/

#ifdef YYPARSE_PARAM
#if (defined __STDC__ || defined __C99__FUNC__ \
     || defined __cplusplus || defined _MSC_VER)
int
yyparse (void *YYPARSE_PARAM)
#else
int
yyparse (YYPARSE_PARAM)
    void *YYPARSE_PARAM;
#endif
#else /* ! YYPARSE_PARAM */
#if (defined __STDC__ || defined __C99__FUNC__ \
     || defined __cplusplus || defined _MSC_VER)
int
yyparse (void)
#else
int
yyparse ()

#endif
#endif
{
  
  int yystate;
  int yyn;
  int yyresult;
  /* Number of tokens to shift before error messages enabled.  */
  int yyerrstatus;
  /* Look-ahead token as an internal (translated) token number.  */
  int yytoken = 0;
#if YYERROR_VERBOSE
  /* Buffer for error messages, and its allocated size.  */
  char yymsgbuf[128];
  char *yymsg = yymsgbuf;
  YYSIZE_T yymsg_alloc = sizeof yymsgbuf;
#endif

  /* Three stacks and their tools:
     `yyss': related to states,
     `yyvs': related to semantic values,
     `yyls': related to locations.

     Refer to the stacks thru separate pointers, to allow yyoverflow
     to reallocate them elsewhere.  */

  /* The state stack.  */
  yytype_int16 yyssa[YYINITDEPTH];
  yytype_int16 *yyss = yyssa;
  yytype_int16 *yyssp;

  /* The semantic value stack.  */
  YYSTYPE yyvsa[YYINITDEPTH];
  YYSTYPE *yyvs = yyvsa;
  YYSTYPE *yyvsp;



#define YYPOPSTACK(N)   (yyvsp -= (N), yyssp -= (N))

  YYSIZE_T yystacksize = YYINITDEPTH;

  /* The variables used to return semantic value and location from the
     action routines.  */
  YYSTYPE yyval;


  /* The number of symbols on the RHS of the reduced rule.
     Keep to zero when no symbol should be popped.  */
  int yylen = 0;

  YYDPRINTF ((stderr, "Starting parse\n"));

  yystate = 0;
  yyerrstatus = 0;
  yynerrs = 0;
  yychar = YYEMPTY;		/* Cause a token to be read.  */

  /* Initialize stack pointers.
     Waste one element of value and location stack
     so that they stay on the same level as the state stack.
     The wasted elements are never initialized.  */

  yyssp = yyss;
  yyvsp = yyvs;

  goto yysetstate;

/*------------------------------------------------------------.
| yynewstate -- Push a new state, which is found in yystate.  |
`------------------------------------------------------------*/
 yynewstate:
  /* In all cases, when you get here, the value and location stacks
     have just been pushed.  So pushing a state here evens the stacks.  */
  yyssp++;

 yysetstate:
  *yyssp = yystate;

  if (yyss + yystacksize - 1 <= yyssp)
    {
      /* Get the current used size of the three stacks, in elements.  */
      YYSIZE_T yysize = yyssp - yyss + 1;

#ifdef yyoverflow
      {
	/* Give user a chance to reallocate the stack.  Use copies of
	   these so that the &'s don't force the real ones into
	   memory.  */
	YYSTYPE *yyvs1 = yyvs;
	yytype_int16 *yyss1 = yyss;


	/* Each stack pointer address is followed by the size of the
	   data in use in that stack, in bytes.  This used to be a
	   conditional around just the two extra args, but that might
	   be undefined if yyoverflow is a macro.  */
	yyoverflow (YY_("memory exhausted"),
		    &yyss1, yysize * sizeof (*yyssp),
		    &yyvs1, yysize * sizeof (*yyvsp),

		    &yystacksize);

	yyss = yyss1;
	yyvs = yyvs1;
      }
#else /* no yyoverflow */
# ifndef YYSTACK_RELOCATE
      goto yyexhaustedlab;
# else
      /* Extend the stack our own way.  */
      if (YYMAXDEPTH <= yystacksize)
	goto yyexhaustedlab;
      yystacksize *= 2;
      if (YYMAXDEPTH < yystacksize)
	yystacksize = YYMAXDEPTH;

      {
	yytype_int16 *yyss1 = yyss;
	union yyalloc *yyptr =
	  (union yyalloc *) YYSTACK_ALLOC (YYSTACK_BYTES (yystacksize));
	if (! yyptr)
	  goto yyexhaustedlab;
	YYSTACK_RELOCATE (yyss);
	YYSTACK_RELOCATE (yyvs);

#  undef YYSTACK_RELOCATE
	if (yyss1 != yyssa)
	  YYSTACK_FREE (yyss1);
      }
# endif
#endif /* no yyoverflow */

      yyssp = yyss + yysize - 1;
      yyvsp = yyvs + yysize - 1;


      YYDPRINTF ((stderr, "Stack size increased to %lu\n",
		  (unsigned long int) yystacksize));

      if (yyss + yystacksize - 1 <= yyssp)
	YYABORT;
    }

  YYDPRINTF ((stderr, "Entering state %d\n", yystate));

  goto yybackup;

/*-----------.
| yybackup.  |
`-----------*/
yybackup:

  /* Do appropriate processing given the current state.  Read a
     look-ahead token if we need one and don't already have one.  */

  /* First try to decide what to do without reference to look-ahead token.  */
  yyn = yypact[yystate];
  if (yyn == YYPACT_NINF)
    goto yydefault;

  /* Not known => get a look-ahead token if don't already have one.  */

  /* YYCHAR is either YYEMPTY or YYEOF or a valid look-ahead symbol.  */
  if (yychar == YYEMPTY)
    {
      YYDPRINTF ((stderr, "Reading a token: "));
      yychar = YYLEX;
    }

  if (yychar <= YYEOF)
    {
      yychar = yytoken = YYEOF;
      YYDPRINTF ((stderr, "Now at end of input.\n"));
    }
  else
    {
      yytoken = YYTRANSLATE (yychar);
      YY_SYMBOL_PRINT ("Next token is", yytoken, &yylval, &yylloc);
    }

  /* If the proper action on seeing token YYTOKEN is to reduce or to
     detect an error, take that action.  */
  yyn += yytoken;
  if (yyn < 0 || YYLAST < yyn || yycheck[yyn] != yytoken)
    goto yydefault;
  yyn = yytable[yyn];
  if (yyn <= 0)
    {
      if (yyn == 0 || yyn == YYTABLE_NINF)
	goto yyerrlab;
      yyn = -yyn;
      goto yyreduce;
    }

  if (yyn == YYFINAL)
    YYACCEPT;

  /* Count tokens shifted since error; after three, turn off error
     status.  */
  if (yyerrstatus)
    yyerrstatus--;

  /* Shift the look-ahead token.  */
  YY_SYMBOL_PRINT ("Shifting", yytoken, &yylval, &yylloc);

  /* Discard the shifted token unless it is eof.  */
  if (yychar != YYEOF)
    yychar = YYEMPTY;

  yystate = yyn;
  *++yyvsp = yylval;

  goto yynewstate;


/*-----------------------------------------------------------.
| yydefault -- do the default action for the current state.  |
`-----------------------------------------------------------*/
yydefault:
  yyn = yydefact[yystate];
  if (yyn == 0)
    goto yyerrlab;
  goto yyreduce;


/*-----------------------------.
| yyreduce -- Do a reduction.  |
`-----------------------------*/
yyreduce:
  /* yyn is the number of a rule to reduce with.  */
  yylen = yyr2[yyn];

  /* If YYLEN is nonzero, implement the default value of the action:
     `$$ = $1'.

     Otherwise, the following line sets YYVAL to garbage.
     This behavior is undocumented and Bison
     users should not rely upon it.  Assigning to YYVAL
     unconditionally makes the parser a bit smaller, and it avoids a
     GCC warning that YYVAL may be used uninitialized.  */
  yyval = yyvsp[1-yylen];


  YY_REDUCE_PRINT (yyn);
  switch (yyn)
    {
        case 2:
#line 385 "go/vt/sqlparser/sql.y"
    {
    setParseTree(yylex, (yyvsp[(1) - (2)].statement))
  }
    break;

  case 3:
#line 390 "go/vt/sqlparser/sql.y"
    {}
    break;

  case 4:
#line 391 "go/vt/sqlparser/sql.y"
    {}
    break;

  case 5:
#line 395 "go/vt/sqlparser/sql.y"
    {
    (yyval.statement) = (yyvsp[(1) - (1)].selStmt)
  }
    break;

  case 29:
#line 422 "go/vt/sqlparser/sql.y"
    {
  setParseTree(yylex, nil)
}
    break;

  case 30:
#line 428 "go/vt/sqlparser/sql.y"
    {
    (yyval.statement) = &Load{Local: (yyvsp[(3) - (11)].boolVal), Infile: (yyvsp[(4) - (11)].str), Table: (yyvsp[(5) - (11)].tableName), Partition: (yyvsp[(6) - (11)].partitions), Charset: (yyvsp[(7) - (11)].str), Fields: (yyvsp[(8) - (11)].Fields), Lines: (yyvsp[(9) - (11)].Lines), IgnoreNum: (yyvsp[(10) - (11)].sqlVal), Columns: (yyvsp[(11) - (11)].columns)}
  }
    break;

  case 31:
#line 434 "go/vt/sqlparser/sql.y"
    {
    sel := (yyvsp[(1) - (4)].selStmt).(*Select)
    sel.OrderBy = (yyvsp[(2) - (4)].orderBy)
    sel.Limit = (yyvsp[(3) - (4)].limit)
    sel.Lock = (yyvsp[(4) - (4)].str)
    (yyval.selStmt) = sel
  }
    break;

  case 32:
#line 442 "go/vt/sqlparser/sql.y"
    {
    (yyval.selStmt) = &Union{Type: (yyvsp[(2) - (6)].str), Left: (yyvsp[(1) - (6)].selStmt), Right: (yyvsp[(3) - (6)].selStmt), OrderBy: (yyvsp[(4) - (6)].orderBy), Limit: (yyvsp[(5) - (6)].limit), Lock: (yyvsp[(6) - (6)].str)}
  }
    break;

  case 33:
#line 446 "go/vt/sqlparser/sql.y"
    {
    (yyval.selStmt) = &Select{Comments: Comments((yyvsp[(2) - (7)].bytes2)), Cache: (yyvsp[(3) - (7)].str), SelectExprs: SelectExprs{Nextval{Expr: (yyvsp[(5) - (7)].expr)}}, From: TableExprs{&AliasedTableExpr{Expr: (yyvsp[(7) - (7)].tableName)}}}
  }
    break;

  case 34:
#line 452 "go/vt/sqlparser/sql.y"
    {
    (yyval.statement) = &Stream{Comments: Comments((yyvsp[(2) - (5)].bytes2)), SelectExpr: (yyvsp[(3) - (5)].selectExpr), Table: (yyvsp[(5) - (5)].tableName)}
  }
    break;

  case 35:
#line 459 "go/vt/sqlparser/sql.y"
    {
    (yyval.selStmt) = &Select{Comments: Comments((yyvsp[(2) - (10)].bytes2)), Cache: (yyvsp[(3) - (10)].str), Distinct: (yyvsp[(4) - (10)].str), Hints: (yyvsp[(5) - (10)].str), SelectExprs: (yyvsp[(6) - (10)].selectExprs), From: (yyvsp[(7) - (10)].tableExprs), Where: NewWhere(WhereStr, (yyvsp[(8) - (10)].expr)), GroupBy: GroupBy((yyvsp[(9) - (10)].exprs)), Having: NewWhere(HavingStr, (yyvsp[(10) - (10)].expr))}
  }
    break;

  case 36:
#line 465 "go/vt/sqlparser/sql.y"
    {
    (yyval.selStmt) = (yyvsp[(1) - (1)].selStmt)
  }
    break;

  case 37:
#line 469 "go/vt/sqlparser/sql.y"
    {
    (yyval.selStmt) = &ParenSelect{Select: (yyvsp[(2) - (3)].selStmt)}
  }
    break;

  case 38:
#line 475 "go/vt/sqlparser/sql.y"
    {
    (yyval.selStmt) = (yyvsp[(1) - (1)].selStmt)
  }
    break;

  case 39:
#line 479 "go/vt/sqlparser/sql.y"
    {
    (yyval.selStmt) = &ParenSelect{Select: (yyvsp[(2) - (3)].selStmt)}
  }
    break;

  case 40:
#line 486 "go/vt/sqlparser/sql.y"
    {
    // insert_data returns a *Insert pre-filled with Columns & Values
    ins := (yyvsp[(6) - (7)].ins)
    ins.Action = (yyvsp[(1) - (7)].str)
    ins.Comments = (yyvsp[(2) - (7)].bytes2)
    ins.Ignore = (yyvsp[(3) - (7)].str)
    ins.Table = (yyvsp[(4) - (7)].tableName)
    ins.Partitions = (yyvsp[(5) - (7)].partitions)
    ins.OnDup = OnDup((yyvsp[(7) - (7)].setExprs))
    (yyval.statement) = ins
  }
    break;

  case 41:
#line 498 "go/vt/sqlparser/sql.y"
    {
    cols := make(Columns, 0, len((yyvsp[(7) - (8)].setExprs)))
    vals := make(ValTuple, 0, len((yyvsp[(8) - (8)].setExprs)))
    for _, updateList := range (yyvsp[(7) - (8)].setExprs) {
      cols = append(cols, updateList.Name.Name)
      vals = append(vals, updateList.Expr)
    }
    (yyval.statement) = &Insert{Action: (yyvsp[(1) - (8)].str), Comments: Comments((yyvsp[(2) - (8)].bytes2)), Ignore: (yyvsp[(3) - (8)].str), Table: (yyvsp[(4) - (8)].tableName), Partitions: (yyvsp[(5) - (8)].partitions), Columns: cols, Rows: Values{vals}, OnDup: OnDup((yyvsp[(8) - (8)].setExprs))}
  }
    break;

  case 42:
#line 510 "go/vt/sqlparser/sql.y"
    {
    (yyval.str) = InsertStr
  }
    break;

  case 43:
#line 514 "go/vt/sqlparser/sql.y"
    {
    (yyval.str) = ReplaceStr
  }
    break;

  case 44:
#line 520 "go/vt/sqlparser/sql.y"
    {
    (yyval.statement) = &Update{Comments: Comments((yyvsp[(2) - (9)].bytes2)), Ignore: (yyvsp[(3) - (9)].str), TableExprs: (yyvsp[(4) - (9)].tableExprs), Exprs: (yyvsp[(6) - (9)].setExprs), Where: NewWhere(WhereStr, (yyvsp[(7) - (9)].expr)), OrderBy: (yyvsp[(8) - (9)].orderBy), Limit: (yyvsp[(9) - (9)].limit)}
  }
    break;

  case 45:
#line 526 "go/vt/sqlparser/sql.y"
    {
    (yyval.statement) = &Delete{Comments: Comments((yyvsp[(2) - (8)].bytes2)), TableExprs:  TableExprs{&AliasedTableExpr{Expr:(yyvsp[(4) - (8)].tableName)}}, Partitions: (yyvsp[(5) - (8)].partitions), Where: NewWhere(WhereStr, (yyvsp[(6) - (8)].expr)), OrderBy: (yyvsp[(7) - (8)].orderBy), Limit: (yyvsp[(8) - (8)].limit)}
  }
    break;

  case 46:
#line 530 "go/vt/sqlparser/sql.y"
    {
    (yyval.statement) = &Delete{Comments: Comments((yyvsp[(2) - (7)].bytes2)), Targets: (yyvsp[(4) - (7)].tableNames), TableExprs: (yyvsp[(6) - (7)].tableExprs), Where: NewWhere(WhereStr, (yyvsp[(7) - (7)].expr))}
  }
    break;

  case 47:
#line 534 "go/vt/sqlparser/sql.y"
    {
    (yyval.statement) = &Delete{Comments: Comments((yyvsp[(2) - (6)].bytes2)), Targets: (yyvsp[(3) - (6)].tableNames), TableExprs: (yyvsp[(5) - (6)].tableExprs), Where: NewWhere(WhereStr, (yyvsp[(6) - (6)].expr))}
  }
    break;

  case 48:
#line 538 "go/vt/sqlparser/sql.y"
    {
    (yyval.statement) = &Delete{Comments: Comments((yyvsp[(2) - (6)].bytes2)), Targets: (yyvsp[(3) - (6)].tableNames), TableExprs: (yyvsp[(5) - (6)].tableExprs), Where: NewWhere(WhereStr, (yyvsp[(6) - (6)].expr))}
  }
    break;

  case 49:
#line 543 "go/vt/sqlparser/sql.y"
    {}
    break;

  case 50:
#line 544 "go/vt/sqlparser/sql.y"
    {}
    break;

  case 51:
#line 548 "go/vt/sqlparser/sql.y"
    {
    (yyval.tableNames) = TableNames{(yyvsp[(1) - (1)].tableName).ToViewName()}
  }
    break;

  case 52:
#line 552 "go/vt/sqlparser/sql.y"
    {
    (yyval.tableNames) = append((yyval.tableNames), (yyvsp[(3) - (3)].tableName).ToViewName())
  }
    break;

  case 53:
#line 558 "go/vt/sqlparser/sql.y"
    {
    (yyval.tableNames) = TableNames{(yyvsp[(1) - (1)].tableName)}
  }
    break;

  case 54:
#line 562 "go/vt/sqlparser/sql.y"
    {
    (yyval.tableNames) = append((yyval.tableNames), (yyvsp[(3) - (3)].tableName))
  }
    break;

  case 55:
#line 568 "go/vt/sqlparser/sql.y"
    {
    (yyval.tableNames) = TableNames{(yyvsp[(1) - (1)].tableName)}
  }
    break;

  case 56:
#line 572 "go/vt/sqlparser/sql.y"
    {
    (yyval.tableNames) = append((yyval.tableNames), (yyvsp[(3) - (3)].tableName))
  }
    break;

  case 57:
#line 577 "go/vt/sqlparser/sql.y"
    {
    (yyval.partitions) = nil
  }
    break;

  case 58:
#line 581 "go/vt/sqlparser/sql.y"
    {
  (yyval.partitions) = (yyvsp[(3) - (4)].partitions)
  }
    break;

  case 59:
#line 587 "go/vt/sqlparser/sql.y"
    {
    (yyval.statement) = &Set{Comments: Comments((yyvsp[(2) - (3)].bytes2)), Exprs: (yyvsp[(3) - (3)].setExprs)}
  }
    break;

  case 60:
#line 591 "go/vt/sqlparser/sql.y"
    {
    (yyval.statement) = &Set{Comments: Comments((yyvsp[(2) - (4)].bytes2)), Scope: (yyvsp[(3) - (4)].str), Exprs: (yyvsp[(4) - (4)].setExprs)}
  }
    break;

  case 61:
#line 595 "go/vt/sqlparser/sql.y"
    {
    (yyval.statement) = &Set{Comments: Comments((yyvsp[(2) - (5)].bytes2)), Scope: (yyvsp[(3) - (5)].str), Exprs: (yyvsp[(5) - (5)].setExprs)}
  }
    break;

  case 62:
#line 599 "go/vt/sqlparser/sql.y"
    {
    (yyval.statement) = &Set{Comments: Comments((yyvsp[(2) - (4)].bytes2)), Exprs: (yyvsp[(4) - (4)].setExprs)}
  }
    break;

  case 63:
#line 605 "go/vt/sqlparser/sql.y"
    {
    (yyval.setExprs) = SetExprs{(yyvsp[(1) - (1)].setExpr)}
  }
    break;

  case 64:
#line 609 "go/vt/sqlparser/sql.y"
    {
    (yyval.setExprs) = append((yyval.setExprs), (yyvsp[(3) - (3)].setExpr))
  }
    break;

  case 65:
#line 615 "go/vt/sqlparser/sql.y"
    {
    (yyval.setExpr) = &SetExpr{Name: NewColName(TransactionStr), Expr: NewStrVal([]byte((yyvsp[(3) - (3)].str)))}
  }
    break;

  case 66:
#line 619 "go/vt/sqlparser/sql.y"
    {
    (yyval.setExpr) = &SetExpr{Name: NewColName(TransactionStr), Expr: NewStrVal([]byte(TxReadWrite))}
  }
    break;

  case 67:
#line 623 "go/vt/sqlparser/sql.y"
    {
    (yyval.setExpr) = &SetExpr{Name: NewColName(TransactionStr), Expr: NewStrVal([]byte(TxReadOnly))}
  }
    break;

  case 68:
#line 629 "go/vt/sqlparser/sql.y"
    {
    (yyval.str) = IsolationLevelRepeatableRead
  }
    break;

  case 69:
#line 633 "go/vt/sqlparser/sql.y"
    {
    (yyval.str) = IsolationLevelReadCommitted
  }
    break;

  case 70:
#line 637 "go/vt/sqlparser/sql.y"
    {
    (yyval.str) = IsolationLevelReadUncommitted
  }
    break;

  case 71:
#line 641 "go/vt/sqlparser/sql.y"
    {
    (yyval.str) = IsolationLevelSerializable
  }
    break;

  case 72:
#line 647 "go/vt/sqlparser/sql.y"
    {
    (yyval.str) = SessionStr
  }
    break;

  case 73:
#line 651 "go/vt/sqlparser/sql.y"
    {
    (yyval.str) = GlobalStr
  }
    break;

  case 74:
#line 656 "go/vt/sqlparser/sql.y"
    {
    (yyval.int) = yyPosition(yylex)
  }
    break;

  case 75:
#line 662 "go/vt/sqlparser/sql.y"
    {
    (yyvsp[(1) - (2)].ddl).TableSpec = (yyvsp[(2) - (2)].TableSpec)
    if len((yyvsp[(1) - (2)].ddl).TableSpec.Constraints) > 0 {
      (yyvsp[(1) - (2)].ddl).ConstraintAction = AddStr
    }
    (yyval.statement) = (yyvsp[(1) - (2)].ddl)
  }
    break;

  case 76:
#line 670 "go/vt/sqlparser/sql.y"
    {
    (yyvsp[(1) - (3)].ddl).OptLike = &OptLike{LikeTable: (yyvsp[(3) - (3)].tableName)}
    (yyval.statement) = (yyvsp[(1) - (3)].ddl)
  }
    break;

  case 77:
#line 675 "go/vt/sqlparser/sql.y"
    {
    (yyval.statement) = &DDL{Action: AlterStr, Table: (yyvsp[(7) - (11)].tableName), IndexSpec: &IndexSpec{Action: CreateStr, ToName: (yyvsp[(4) - (11)].colIdent), Using: (yyvsp[(5) - (11)].colIdent), Type: (yyvsp[(2) - (11)].str), Columns: (yyvsp[(9) - (11)].indexColumns), Options: (yyvsp[(11) - (11)].indexOptions)}}
  }
    break;

  case 78:
#line 679 "go/vt/sqlparser/sql.y"
    {
    (yyval.statement) = &DDL{Action: CreateStr, View: (yyvsp[(3) - (7)].tableName).ToViewName(), ViewExpr: (yyvsp[(6) - (7)].selStmt), SubStatementPositionStart: (yyvsp[(5) - (7)].int), SubStatementPositionEnd: (yyvsp[(7) - (7)].int) - 1}
  }
    break;

  case 79:
#line 683 "go/vt/sqlparser/sql.y"
    {
    (yyval.statement) = &DDL{Action: CreateStr, View: (yyvsp[(5) - (9)].tableName).ToViewName(), ViewExpr: (yyvsp[(8) - (9)].selStmt), SubStatementPositionStart: (yyvsp[(7) - (9)].int), SubStatementPositionEnd: (yyvsp[(9) - (9)].int) - 1, OrReplace: true}
  }
    break;

  case 80:
#line 687 "go/vt/sqlparser/sql.y"
    {
    var ne bool
    if (yyvsp[(3) - (5)].byt) != 0 {
      ne = true
    }
   (yyval.statement) = &DBDDL{Action: CreateStr, DBName: string((yyvsp[(4) - (5)].bytes)), IfNotExists: ne}
  }
    break;

  case 81:
#line 695 "go/vt/sqlparser/sql.y"
    {
    var ne bool
    if (yyvsp[(3) - (5)].byt) != 0 {
      ne = true
    }
    (yyval.statement) = &DBDDL{Action: CreateStr, DBName: string((yyvsp[(4) - (5)].bytes)), IfNotExists: ne}
  }
    break;

  case 82:
#line 703 "go/vt/sqlparser/sql.y"
    {
    (yyval.statement) = &DDL{Action: CreateStr, Table: (yyvsp[(8) - (15)].tableName), TriggerSpec: &TriggerSpec{Name: string((yyvsp[(4) - (15)].bytes)), Time: (yyvsp[(5) - (15)].str), Event: (yyvsp[(6) - (15)].str), Order: (yyvsp[(12) - (15)].triggerOrder), Body: (yyvsp[(14) - (15)].statement)}, SubStatementPositionStart: (yyvsp[(13) - (15)].int), SubStatementPositionEnd: (yyvsp[(15) - (15)].int) - 1}
  }
    break;

  case 83:
#line 707 "go/vt/sqlparser/sql.y"
    {
    (yyval.statement) = &DDL{Action: CreateStr, ProcedureSpec: &ProcedureSpec{Name: string((yyvsp[(4) - (11)].bytes)), Definer: (yyvsp[(2) - (11)].str), Params: (yyvsp[(6) - (11)].procedureParams), Characteristics: (yyvsp[(8) - (11)].characteristics), Body: (yyvsp[(10) - (11)].statement)}, SubStatementPositionStart: (yyvsp[(9) - (11)].int), SubStatementPositionEnd: (yyvsp[(11) - (11)].int) - 1}
  }
    break;

  case 84:
#line 712 "go/vt/sqlparser/sql.y"
    {
    (yyval.procedureParams) = nil
  }
    break;

  case 85:
#line 716 "go/vt/sqlparser/sql.y"
    {
    (yyval.procedureParams) = (yyvsp[(1) - (1)].procedureParams)
  }
    break;

  case 86:
#line 722 "go/vt/sqlparser/sql.y"
    {
    (yyval.procedureParams) = []ProcedureParam{(yyvsp[(1) - (1)].procedureParam)}
  }
    break;

  case 87:
#line 726 "go/vt/sqlparser/sql.y"
    {
    (yyval.procedureParams) = append((yyval.procedureParams), (yyvsp[(3) - (3)].procedureParam))
  }
    break;

  case 88:
#line 732 "go/vt/sqlparser/sql.y"
    {
    (yyval.procedureParam) = ProcedureParam{Direction: ProcedureParamDirection_In, Name: string((yyvsp[(1) - (2)].bytes)), Type: (yyvsp[(2) - (2)].columnType)}
  }
    break;

  case 89:
#line 736 "go/vt/sqlparser/sql.y"
    {
    (yyval.procedureParam) = ProcedureParam{Direction: ProcedureParamDirection_In, Name: string((yyvsp[(2) - (3)].bytes)), Type: (yyvsp[(3) - (3)].columnType)}
  }
    break;

  case 90:
#line 740 "go/vt/sqlparser/sql.y"
    {
    (yyval.procedureParam) = ProcedureParam{Direction: ProcedureParamDirection_Inout, Name: string((yyvsp[(2) - (3)].bytes)), Type: (yyvsp[(3) - (3)].columnType)}
  }
    break;

  case 91:
#line 744 "go/vt/sqlparser/sql.y"
    {
    (yyval.procedureParam) = ProcedureParam{Direction: ProcedureParamDirection_Out, Name: string((yyvsp[(2) - (3)].bytes)), Type: (yyvsp[(3) - (3)].columnType)}
  }
    break;

  case 92:
#line 749 "go/vt/sqlparser/sql.y"
    {
    (yyval.characteristics) = nil
  }
    break;

  case 93:
#line 753 "go/vt/sqlparser/sql.y"
    {
    (yyval.characteristics) = (yyvsp[(1) - (1)].characteristics)
  }
    break;

  case 94:
#line 759 "go/vt/sqlparser/sql.y"
    {
    (yyval.characteristics) = []Characteristic{(yyvsp[(1) - (1)].characteristic)}
  }
    break;

  case 95:
#line 763 "go/vt/sqlparser/sql.y"
    {
    (yyval.characteristics) = append((yyval.characteristics), (yyvsp[(2) - (2)].characteristic))
  }
    break;

  case 96:
#line 769 "go/vt/sqlparser/sql.y"
    {
    (yyval.characteristic) = Characteristic{Type: CharacteristicValue_Comment, Comment: string((yyvsp[(2) - (2)].bytes))}
  }
    break;

  case 97:
#line 773 "go/vt/sqlparser/sql.y"
    {
    (yyval.characteristic) = Characteristic{Type: CharacteristicValue_LanguageSql}
  }
    break;

  case 98:
#line 777 "go/vt/sqlparser/sql.y"
    {
    (yyval.characteristic) = Characteristic{Type: CharacteristicValue_NotDeterministic}
  }
    break;

  case 99:
#line 781 "go/vt/sqlparser/sql.y"
    {
    (yyval.characteristic) = Characteristic{Type: CharacteristicValue_Deterministic}
  }
    break;

  case 100:
#line 785 "go/vt/sqlparser/sql.y"
    {
    (yyval.characteristic) = Characteristic{Type: CharacteristicValue_ContainsSql}
  }
    break;

  case 101:
#line 789 "go/vt/sqlparser/sql.y"
    {
    (yyval.characteristic) = Characteristic{Type: CharacteristicValue_NoSql}
  }
    break;

  case 102:
#line 793 "go/vt/sqlparser/sql.y"
    {
    (yyval.characteristic) = Characteristic{Type: CharacteristicValue_ReadsSqlData}
  }
    break;

  case 103:
#line 797 "go/vt/sqlparser/sql.y"
    {
    (yyval.characteristic) = Characteristic{Type: CharacteristicValue_ModifiesSqlData}
  }
    break;

  case 104:
#line 801 "go/vt/sqlparser/sql.y"
    {
    (yyval.characteristic) = Characteristic{Type: CharacteristicValue_SqlSecurityDefiner}
  }
    break;

  case 105:
#line 805 "go/vt/sqlparser/sql.y"
    {
    (yyval.characteristic) = Characteristic{Type: CharacteristicValue_SqlSecurityInvoker}
  }
    break;

  case 106:
#line 811 "go/vt/sqlparser/sql.y"
    {
    (yyval.statement) = (yyvsp[(1) - (1)].selStmt)
  }
    break;

  case 128:
#line 838 "go/vt/sqlparser/sql.y"
    {
    (yyval.statement) = &BeginEndBlock{Statements: (yyvsp[(2) - (4)].statements)}
  }
    break;

  case 129:
#line 844 "go/vt/sqlparser/sql.y"
    {
    (yyval.statements) = Statements{(yyvsp[(1) - (1)].statement)}
  }
    break;

  case 130:
#line 848 "go/vt/sqlparser/sql.y"
    {
    (yyval.statements) = append((yyval.statements), (yyvsp[(3) - (3)].statement))
  }
    break;

  case 131:
#line 853 "go/vt/sqlparser/sql.y"
    {
    (yyval.str) = ""
  }
    break;

  case 132:
#line 857 "go/vt/sqlparser/sql.y"
    {
    (yyval.str) = string((yyvsp[(3) - (3)].bytes))
  }
    break;

  case 133:
#line 863 "go/vt/sqlparser/sql.y"
    {
    (yyval.str) = BeforeStr
  }
    break;

  case 134:
#line 867 "go/vt/sqlparser/sql.y"
    {
    (yyval.str) = AfterStr
  }
    break;

  case 135:
#line 873 "go/vt/sqlparser/sql.y"
    {
    (yyval.str) = InsertStr
  }
    break;

  case 136:
#line 877 "go/vt/sqlparser/sql.y"
    {
    (yyval.str) = UpdateStr
  }
    break;

  case 137:
#line 881 "go/vt/sqlparser/sql.y"
    {
    (yyval.str) = DeleteStr
  }
    break;

  case 138:
#line 886 "go/vt/sqlparser/sql.y"
    {
    (yyval.triggerOrder) = nil
  }
    break;

  case 139:
#line 890 "go/vt/sqlparser/sql.y"
    {
    (yyval.triggerOrder) = &TriggerOrder{PrecedesOrFollows: FollowsStr, OtherTriggerName: string((yyvsp[(2) - (2)].bytes))}
  }
    break;

  case 140:
#line 894 "go/vt/sqlparser/sql.y"
    {
    (yyval.triggerOrder) = &TriggerOrder{PrecedesOrFollows: PrecedesStr, OtherTriggerName: string((yyvsp[(2) - (2)].bytes))}
  }
    break;

  case 141:
#line 900 "go/vt/sqlparser/sql.y"
    {
    (yyval.statement) = (yyvsp[(1) - (1)].statement)
  }
    break;

  case 146:
#line 910 "go/vt/sqlparser/sql.y"
    {
    (yyval.statement) = &BeginEndBlock{Statements: (yyvsp[(2) - (4)].statements)}
  }
    break;

  case 147:
#line 916 "go/vt/sqlparser/sql.y"
    {
    (yyval.statement) = &CaseStatement{Expr: (yyvsp[(2) - (5)].expr), Cases: (yyvsp[(3) - (5)].caseStatementCases)}
  }
    break;

  case 148:
#line 920 "go/vt/sqlparser/sql.y"
    {
    (yyval.statement) = &CaseStatement{Expr: (yyvsp[(2) - (8)].expr), Cases: (yyvsp[(3) - (8)].caseStatementCases), Else: (yyvsp[(5) - (8)].statements)}
  }
    break;

  case 149:
#line 926 "go/vt/sqlparser/sql.y"
    {
    (yyval.caseStatementCases) = []CaseStatementCase{(yyvsp[(1) - (1)].caseStatementCase)}
  }
    break;

  case 150:
#line 930 "go/vt/sqlparser/sql.y"
    {
    (yyval.caseStatementCases) = append((yyval.caseStatementCases), (yyvsp[(2) - (2)].caseStatementCase))
  }
    break;

  case 151:
#line 936 "go/vt/sqlparser/sql.y"
    {
    (yyval.caseStatementCase) = CaseStatementCase{Case: (yyvsp[(2) - (5)].expr), Statements: (yyvsp[(4) - (5)].statements)}
  }
    break;

  case 152:
#line 942 "go/vt/sqlparser/sql.y"
    {
    conds := []IfStatementCondition{IfStatementCondition{Expr: (yyvsp[(2) - (7)].expr), Statements: (yyvsp[(4) - (7)].statements)}}
    (yyval.statement) = &IfStatement{Conditions: conds}
  }
    break;

  case 153:
#line 947 "go/vt/sqlparser/sql.y"
    {
    conds := []IfStatementCondition{IfStatementCondition{Expr: (yyvsp[(2) - (10)].expr), Statements: (yyvsp[(4) - (10)].statements)}}
    (yyval.statement) = &IfStatement{Conditions: conds, Else: (yyvsp[(7) - (10)].statements)}
  }
    break;

  case 154:
#line 952 "go/vt/sqlparser/sql.y"
    {
    conds := (yyvsp[(6) - (8)].ifStatementConditions)
    conds = append([]IfStatementCondition{IfStatementCondition{Expr: (yyvsp[(2) - (8)].expr), Statements: (yyvsp[(4) - (8)].statements)}}, conds...)
    (yyval.statement) = &IfStatement{Conditions: conds}
  }
    break;

  case 155:
#line 958 "go/vt/sqlparser/sql.y"
    {
    conds := (yyvsp[(6) - (11)].ifStatementConditions)
    conds = append([]IfStatementCondition{IfStatementCondition{Expr: (yyvsp[(2) - (11)].expr), Statements: (yyvsp[(4) - (11)].statements)}}, conds...)
    (yyval.statement) = &IfStatement{Conditions: conds, Else: (yyvsp[(8) - (11)].statements)}
  }
    break;

  case 156:
#line 966 "go/vt/sqlparser/sql.y"
    {
    (yyval.ifStatementConditions) = []IfStatementCondition{(yyvsp[(1) - (1)].ifStatementCondition)}
  }
    break;

  case 157:
#line 970 "go/vt/sqlparser/sql.y"
    {
    (yyval.ifStatementConditions) = append((yyval.ifStatementConditions), (yyvsp[(2) - (2)].ifStatementCondition))
  }
    break;

  case 158:
#line 976 "go/vt/sqlparser/sql.y"
    {
    (yyval.ifStatementCondition) = IfStatementCondition{Expr: (yyvsp[(2) - (5)].expr), Statements: (yyvsp[(4) - (5)].statements)}
  }
    break;

  case 159:
#line 982 "go/vt/sqlparser/sql.y"
    {
    (yyval.statement) = &Signal{SqlStateValue: string((yyvsp[(2) - (2)].bytes))}
  }
    break;

  case 160:
#line 986 "go/vt/sqlparser/sql.y"
    {
    (yyval.statement) = &Signal{SqlStateValue: string((yyvsp[(2) - (4)].bytes)), Info: (yyvsp[(4) - (4)].signalInfos)}
  }
    break;

  case 161:
#line 992 "go/vt/sqlparser/sql.y"
    {
    (yyval.bytes) = (yyvsp[(2) - (2)].bytes)
  }
    break;

  case 162:
#line 996 "go/vt/sqlparser/sql.y"
    {
    (yyval.bytes) = (yyvsp[(3) - (3)].bytes)
  }
    break;

  case 163:
#line 1002 "go/vt/sqlparser/sql.y"
    {
    (yyval.signalInfos) = []SignalInfo{(yyvsp[(1) - (1)].signalInfo)}
  }
    break;

  case 164:
#line 1006 "go/vt/sqlparser/sql.y"
    {
    (yyval.signalInfos) = append((yyval.signalInfos), (yyvsp[(3) - (3)].signalInfo))
  }
    break;

  case 165:
#line 1012 "go/vt/sqlparser/sql.y"
    {
    (yyval.signalInfo) = SignalInfo{Name: string((yyvsp[(1) - (3)].bytes)), Value: (yyvsp[(3) - (3)].expr).(*SQLVal)}
  }
    break;

  case 166:
#line 1018 "go/vt/sqlparser/sql.y"
    {
    (yyval.bytes) = (yyvsp[(1) - (1)].bytes)
  }
    break;

  case 178:
#line 1035 "go/vt/sqlparser/sql.y"
    {
    (yyval.statement) = &Call{FuncName: string((yyvsp[(2) - (3)].bytes)), Params: (yyvsp[(3) - (3)].exprs)}
  }
    break;

  case 179:
#line 1040 "go/vt/sqlparser/sql.y"
    {
    (yyval.exprs) = nil
  }
    break;

  case 180:
#line 1044 "go/vt/sqlparser/sql.y"
    {
    (yyval.exprs) = nil
  }
    break;

  case 181:
#line 1048 "go/vt/sqlparser/sql.y"
    {
    (yyval.exprs) = (yyvsp[(2) - (3)].exprs)
  }
    break;

  case 182:
#line 1054 "go/vt/sqlparser/sql.y"
    {
    (yyval.statements) = Statements{(yyvsp[(1) - (1)].statement)}
  }
    break;

  case 183:
#line 1058 "go/vt/sqlparser/sql.y"
    {
    (yyval.statements) = append((yyval.statements), (yyvsp[(3) - (3)].statement))
  }
    break;

  case 184:
#line 1064 "go/vt/sqlparser/sql.y"
    {
    (yyval.statement) = (yyvsp[(1) - (1)].selStmt)
  }
    break;

  case 193:
#line 1077 "go/vt/sqlparser/sql.y"
    {
    (yyval.colIdent) = NewColIdent("")
  }
    break;

  case 194:
#line 1081 "go/vt/sqlparser/sql.y"
    {
    (yyval.colIdent) = (yyvsp[(2) - (2)].colIdent)
  }
    break;

  case 195:
#line 1087 "go/vt/sqlparser/sql.y"
    {
    (yyval.colIdent) = NewColIdent(string((yyvsp[(1) - (1)].bytes)))
  }
    break;

  case 196:
#line 1092 "go/vt/sqlparser/sql.y"
    {
    var v []VindexParam
    (yyval.vindexParams) = v
  }
    break;

  case 197:
#line 1097 "go/vt/sqlparser/sql.y"
    {
    (yyval.vindexParams) = (yyvsp[(2) - (2)].vindexParams)
  }
    break;

  case 198:
#line 1103 "go/vt/sqlparser/sql.y"
    {
    (yyval.vindexParams) = make([]VindexParam, 0, 4)
    (yyval.vindexParams) = append((yyval.vindexParams), (yyvsp[(1) - (1)].vindexParam))
  }
    break;

  case 199:
#line 1108 "go/vt/sqlparser/sql.y"
    {
    (yyval.vindexParams) = append((yyval.vindexParams), (yyvsp[(3) - (3)].vindexParam))
  }
    break;

  case 200:
#line 1114 "go/vt/sqlparser/sql.y"
    {
    (yyval.vindexParam) = VindexParam{Key: (yyvsp[(1) - (3)].colIdent), Val: (yyvsp[(3) - (3)].str)}
  }
    break;

  case 201:
#line 1120 "go/vt/sqlparser/sql.y"
    {
    var ne bool
    if (yyvsp[(3) - (4)].byt) != 0 {
      ne = true
    }
    (yyval.ddl) = &DDL{Action: CreateStr, Table: (yyvsp[(4) - (4)].tableName), IfNotExists: ne}
    setDDL(yylex, (yyval.ddl))
  }
    break;

  case 202:
#line 1131 "go/vt/sqlparser/sql.y"
    {
    (yyval.TableSpec) = (yyvsp[(2) - (4)].TableSpec)
    (yyval.TableSpec).Options = (yyvsp[(4) - (4)].str)
  }
    break;

  case 203:
#line 1138 "go/vt/sqlparser/sql.y"
    {
    (yyval.TableSpec) = &TableSpec{}
    (yyval.TableSpec).AddColumn((yyvsp[(1) - (1)].columnDefinition))
  }
    break;

  case 204:
#line 1143 "go/vt/sqlparser/sql.y"
    {
    (yyval.TableSpec).AddColumn((yyvsp[(3) - (3)].columnDefinition))
  }
    break;

  case 205:
#line 1147 "go/vt/sqlparser/sql.y"
    {
    (yyval.TableSpec).AddIndex((yyvsp[(3) - (3)].indexDefinition))
  }
    break;

  case 206:
#line 1151 "go/vt/sqlparser/sql.y"
    {
    (yyval.TableSpec).AddConstraint((yyvsp[(3) - (3)].constraintDefinition))
  }
    break;

  case 207:
#line 1157 "go/vt/sqlparser/sql.y"
    {
    if err := (yyvsp[(2) - (3)].columnType).merge((yyvsp[(3) - (3)].columnType)); err != nil {
      yylex.Error(err.Error())
      return 1
    }
    (yyval.columnDefinition) = &ColumnDefinition{Name: NewColIdent(string((yyvsp[(1) - (3)].bytes))), Type: (yyvsp[(2) - (3)].columnType)}
  }
    break;

  case 208:
#line 1167 "go/vt/sqlparser/sql.y"
    {
    if err := (yyvsp[(2) - (3)].columnType).merge((yyvsp[(3) - (3)].columnType)); err != nil {
      yylex.Error(err.Error())
      return 1
    }
    (yyval.columnDefinition) = &ColumnDefinition{Name: (yyvsp[(1) - (3)].colIdent), Type: (yyvsp[(2) - (3)].columnType)}
  }
    break;

  case 209:
#line 1176 "go/vt/sqlparser/sql.y"
    {
    (yyval.columnType) = ColumnType{}
  }
    break;

  case 210:
#line 1180 "go/vt/sqlparser/sql.y"
    {
    opt := ColumnType{NotNull: (yyvsp[(2) - (2)].boolVal), sawnull: true}
    if err := (yyvsp[(1) - (2)].columnType).merge(opt); err != nil {
    	yylex.Error(err.Error())
    	return 1
    }
    (yyval.columnType) = (yyvsp[(1) - (2)].columnType)
  }
    break;

  case 211:
#line 1189 "go/vt/sqlparser/sql.y"
    {
    opt := ColumnType{Default: (yyvsp[(2) - (2)].optVal)}
    if err := (yyvsp[(1) - (2)].columnType).merge(opt); err != nil {
    	yylex.Error(err.Error())
    	return 1
    }
    (yyval.columnType) = (yyvsp[(1) - (2)].columnType)
  }
    break;

  case 212:
#line 1198 "go/vt/sqlparser/sql.y"
    {
    opt := ColumnType{OnUpdate: (yyvsp[(2) - (2)].optVal)}
    if err := (yyvsp[(1) - (2)].columnType).merge(opt); err != nil {
    	yylex.Error(err.Error())
    	return 1
    }
    (yyval.columnType) = (yyvsp[(1) - (2)].columnType)
  }
    break;

  case 213:
#line 1207 "go/vt/sqlparser/sql.y"
    {
    opt := ColumnType{Autoincrement: (yyvsp[(2) - (2)].boolVal), sawai: true}
    if err := (yyvsp[(1) - (2)].columnType).merge(opt); err != nil {
    	yylex.Error(err.Error())
    	return 1
    }
    (yyval.columnType) = (yyvsp[(1) - (2)].columnType)
  }
    break;

  case 214:
#line 1216 "go/vt/sqlparser/sql.y"
    {
    opt := ColumnType{KeyOpt: (yyvsp[(2) - (2)].colKeyOpt)}
    if err := (yyvsp[(1) - (2)].columnType).merge(opt); err != nil {
    	yylex.Error(err.Error())
    	return 1
    }
    (yyval.columnType) = (yyvsp[(1) - (2)].columnType)
  }
    break;

  case 215:
#line 1225 "go/vt/sqlparser/sql.y"
    {
    opt := ColumnType{Comment: (yyvsp[(2) - (2)].sqlVal)}
    if err := (yyvsp[(1) - (2)].columnType).merge(opt); err != nil {
    	yylex.Error(err.Error())
    	return 1
    }
    (yyval.columnType) = (yyvsp[(1) - (2)].columnType)
  }
    break;

  case 216:
#line 1236 "go/vt/sqlparser/sql.y"
    {
    (yyval.columnType) = (yyvsp[(1) - (3)].columnType)
    (yyval.columnType).Unsigned = (yyvsp[(2) - (3)].boolVal)
    (yyval.columnType).Zerofill = (yyvsp[(3) - (3)].boolVal)
  }
    break;

  case 220:
#line 1247 "go/vt/sqlparser/sql.y"
    {
    (yyval.columnType) = (yyvsp[(1) - (2)].columnType)
    (yyval.columnType).Length = (yyvsp[(2) - (2)].sqlVal)
  }
    break;

  case 221:
#line 1252 "go/vt/sqlparser/sql.y"
    {
    (yyval.columnType) = (yyvsp[(1) - (1)].columnType)
  }
    break;

  case 222:
#line 1258 "go/vt/sqlparser/sql.y"
    {
    (yyval.columnType) = ColumnType{Type: string((yyvsp[(1) - (1)].bytes))}
  }
    break;

  case 223:
#line 1262 "go/vt/sqlparser/sql.y"
    {
    (yyval.columnType) = ColumnType{Type: string((yyvsp[(1) - (1)].bytes))}
  }
    break;

  case 224:
#line 1266 "go/vt/sqlparser/sql.y"
    {
    (yyval.columnType) = ColumnType{Type: string((yyvsp[(1) - (1)].bytes))}
  }
    break;

  case 225:
#line 1270 "go/vt/sqlparser/sql.y"
    {
    (yyval.columnType) = ColumnType{Type: string((yyvsp[(1) - (1)].bytes))}
  }
    break;

  case 226:
#line 1274 "go/vt/sqlparser/sql.y"
    {
    (yyval.columnType) = ColumnType{Type: string((yyvsp[(1) - (1)].bytes))}
  }
    break;

  case 227:
#line 1278 "go/vt/sqlparser/sql.y"
    {
    (yyval.columnType) = ColumnType{Type: string((yyvsp[(1) - (1)].bytes))}
  }
    break;

  case 228:
#line 1282 "go/vt/sqlparser/sql.y"
    {
    (yyval.columnType) = ColumnType{Type: string((yyvsp[(1) - (1)].bytes))}
  }
    break;

  case 229:
#line 1286 "go/vt/sqlparser/sql.y"
    {
    (yyval.columnType) = ColumnType{Type: string((yyvsp[(1) - (1)].bytes))}
  }
    break;

  case 230:
#line 1290 "go/vt/sqlparser/sql.y"
    {
    (yyval.columnType) = ColumnType{Type: string((yyvsp[(1) - (1)].bytes))}
  }
    break;

  case 231:
#line 1296 "go/vt/sqlparser/sql.y"
    {
    (yyval.columnType) = ColumnType{Type: string((yyvsp[(1) - (2)].bytes))}
    (yyval.columnType).Length = (yyvsp[(2) - (2)].LengthScaleOption).Length
    (yyval.columnType).Scale = (yyvsp[(2) - (2)].LengthScaleOption).Scale
  }
    break;

  case 232:
#line 1302 "go/vt/sqlparser/sql.y"
    {
    (yyval.columnType) = ColumnType{Type: string((yyvsp[(1) - (2)].bytes))}
    (yyval.columnType).Length = (yyvsp[(2) - (2)].LengthScaleOption).Length
    (yyval.columnType).Scale = (yyvsp[(2) - (2)].LengthScaleOption).Scale
  }
    break;

  case 233:
#line 1308 "go/vt/sqlparser/sql.y"
    {
    (yyval.columnType) = ColumnType{Type: string((yyvsp[(1) - (3)].bytes)) + " " + string((yyvsp[(2) - (3)].bytes))}
    (yyval.columnType).Length = (yyvsp[(3) - (3)].LengthScaleOption).Length
    (yyval.columnType).Scale = (yyvsp[(3) - (3)].LengthScaleOption).Scale
  }
    break;

  case 234:
#line 1314 "go/vt/sqlparser/sql.y"
    {
    (yyval.columnType) = ColumnType{Type: string((yyvsp[(1) - (2)].bytes))}
    (yyval.columnType).Length = (yyvsp[(2) - (2)].LengthScaleOption).Length
    (yyval.columnType).Scale = (yyvsp[(2) - (2)].LengthScaleOption).Scale
  }
    break;

  case 235:
#line 1320 "go/vt/sqlparser/sql.y"
    {
    (yyval.columnType) = ColumnType{Type: string((yyvsp[(1) - (2)].bytes))}
    (yyval.columnType).Length = (yyvsp[(2) - (2)].LengthScaleOption).Length
    (yyval.columnType).Scale = (yyvsp[(2) - (2)].LengthScaleOption).Scale
  }
    break;

  case 236:
#line 1326 "go/vt/sqlparser/sql.y"
    {
    (yyval.columnType) = ColumnType{Type: string((yyvsp[(1) - (2)].bytes))}
    (yyval.columnType).Length = (yyvsp[(2) - (2)].LengthScaleOption).Length
    (yyval.columnType).Scale = (yyvsp[(2) - (2)].LengthScaleOption).Scale
  }
    break;

  case 237:
#line 1332 "go/vt/sqlparser/sql.y"
    {
    (yyval.columnType) = ColumnType{Type: string((yyvsp[(1) - (2)].bytes))}
    (yyval.columnType).Length = (yyvsp[(2) - (2)].LengthScaleOption).Length
    (yyval.columnType).Scale = (yyvsp[(2) - (2)].LengthScaleOption).Scale
  }
    break;

  case 238:
#line 1338 "go/vt/sqlparser/sql.y"
    {
    (yyval.columnType) = ColumnType{Type: string((yyvsp[(1) - (2)].bytes))}
    (yyval.columnType).Length = (yyvsp[(2) - (2)].LengthScaleOption).Length
    (yyval.columnType).Scale = (yyvsp[(2) - (2)].LengthScaleOption).Scale
  }
    break;

  case 239:
#line 1346 "go/vt/sqlparser/sql.y"
    {
    (yyval.columnType) = ColumnType{Type: string((yyvsp[(1) - (1)].bytes))}
  }
    break;

  case 240:
#line 1350 "go/vt/sqlparser/sql.y"
    {
    (yyval.columnType) = ColumnType{Type: string((yyvsp[(1) - (2)].bytes)), Length: (yyvsp[(2) - (2)].sqlVal)}
  }
    break;

  case 241:
#line 1354 "go/vt/sqlparser/sql.y"
    {
    (yyval.columnType) = ColumnType{Type: string((yyvsp[(1) - (2)].bytes)), Length: (yyvsp[(2) - (2)].sqlVal)}
  }
    break;

  case 242:
#line 1358 "go/vt/sqlparser/sql.y"
    {
    (yyval.columnType) = ColumnType{Type: string((yyvsp[(1) - (2)].bytes)), Length: (yyvsp[(2) - (2)].sqlVal)}
  }
    break;

  case 243:
#line 1362 "go/vt/sqlparser/sql.y"
    {
    (yyval.columnType) = ColumnType{Type: string((yyvsp[(1) - (1)].bytes))}
  }
    break;

  case 244:
#line 1368 "go/vt/sqlparser/sql.y"
    {
    (yyval.columnType) = ColumnType{Type: string((yyvsp[(1) - (4)].bytes)), Length: (yyvsp[(2) - (4)].sqlVal), Charset: (yyvsp[(3) - (4)].str), Collate: (yyvsp[(4) - (4)].str)}
  }
    break;

  case 245:
#line 1372 "go/vt/sqlparser/sql.y"
    {
    (yyval.columnType) = ColumnType{Type: string((yyvsp[(1) - (4)].bytes)), Length: (yyvsp[(2) - (4)].sqlVal), Charset: (yyvsp[(3) - (4)].str), Collate: (yyvsp[(4) - (4)].str)}
  }
    break;

  case 246:
#line 1376 "go/vt/sqlparser/sql.y"
    {
    (yyval.columnType) = ColumnType{Type: string((yyvsp[(1) - (3)].bytes)) + " " + string((yyvsp[(2) - (3)].bytes)), Length: (yyvsp[(3) - (3)].sqlVal)}
  }
    break;

  case 247:
#line 1380 "go/vt/sqlparser/sql.y"
    {
    (yyval.columnType) = ColumnType{Type: string((yyvsp[(1) - (3)].bytes)) + " " + string((yyvsp[(2) - (3)].bytes)), Length: (yyvsp[(3) - (3)].sqlVal)}
  }
    break;

  case 248:
#line 1384 "go/vt/sqlparser/sql.y"
    {
    (yyval.columnType) = ColumnType{Type: string((yyvsp[(1) - (2)].bytes)), Length: (yyvsp[(2) - (2)].sqlVal)}
  }
    break;

  case 249:
#line 1388 "go/vt/sqlparser/sql.y"
    {
    (yyval.columnType) = ColumnType{Type: string((yyvsp[(1) - (4)].bytes)), Length: (yyvsp[(2) - (4)].sqlVal), Charset: (yyvsp[(3) - (4)].str), Collate: (yyvsp[(4) - (4)].str)}
  }
    break;

  case 250:
#line 1392 "go/vt/sqlparser/sql.y"
    {
    (yyval.columnType) = ColumnType{Type: string((yyvsp[(1) - (5)].bytes)) + " " + string((yyvsp[(2) - (5)].bytes)), Length: (yyvsp[(3) - (5)].sqlVal), Charset: (yyvsp[(4) - (5)].str), Collate: (yyvsp[(5) - (5)].str)}
  }
    break;

  case 251:
#line 1396 "go/vt/sqlparser/sql.y"
    {
    (yyval.columnType) = ColumnType{Type: string((yyvsp[(1) - (2)].bytes)), Length: (yyvsp[(2) - (2)].sqlVal)}
  }
    break;

  case 252:
#line 1400 "go/vt/sqlparser/sql.y"
    {
    (yyval.columnType) = ColumnType{Type: string((yyvsp[(1) - (3)].bytes)) + " " + string((yyvsp[(2) - (3)].bytes)), Length: (yyvsp[(3) - (3)].sqlVal)}
  }
    break;

  case 253:
#line 1404 "go/vt/sqlparser/sql.y"
    {
    (yyval.columnType) = ColumnType{Type: string((yyvsp[(1) - (4)].bytes)) + " " + string((yyvsp[(2) - (4)].bytes)) + " " + string((yyvsp[(3) - (4)].bytes)), Length: (yyvsp[(4) - (4)].sqlVal)}
  }
    break;

  case 254:
#line 1408 "go/vt/sqlparser/sql.y"
    {
    (yyval.columnType) = ColumnType{Type: string((yyvsp[(1) - (2)].bytes)), Length: (yyvsp[(2) - (2)].sqlVal)}
  }
    break;

  case 255:
#line 1412 "go/vt/sqlparser/sql.y"
    {
    (yyval.columnType) = ColumnType{Type: string((yyvsp[(1) - (2)].bytes)), Length: (yyvsp[(2) - (2)].sqlVal)}
  }
    break;

  case 256:
#line 1416 "go/vt/sqlparser/sql.y"
    {
    (yyval.columnType) = ColumnType{Type: string((yyvsp[(1) - (3)].bytes)), Charset: (yyvsp[(2) - (3)].str), Collate: (yyvsp[(3) - (3)].str)}
  }
    break;

  case 257:
#line 1420 "go/vt/sqlparser/sql.y"
    {
    (yyval.columnType) = ColumnType{Type: string((yyvsp[(1) - (3)].bytes)), Charset: (yyvsp[(2) - (3)].str), Collate: (yyvsp[(3) - (3)].str)}
  }
    break;

  case 258:
#line 1424 "go/vt/sqlparser/sql.y"
    {
    (yyval.columnType) = ColumnType{Type: string((yyvsp[(1) - (3)].bytes)), Charset: (yyvsp[(2) - (3)].str), Collate: (yyvsp[(3) - (3)].str)}
  }
    break;

  case 259:
#line 1428 "go/vt/sqlparser/sql.y"
    {
    (yyval.columnType) = ColumnType{Type: string((yyvsp[(1) - (3)].bytes)), Charset: (yyvsp[(2) - (3)].str), Collate: (yyvsp[(3) - (3)].str)}
  }
    break;

  case 260:
#line 1432 "go/vt/sqlparser/sql.y"
    {
    (yyval.columnType) = ColumnType{Type: string((yyvsp[(1) - (3)].bytes)), Charset: (yyvsp[(2) - (3)].str), Collate: (yyvsp[(3) - (3)].str)}
  }
    break;

  case 261:
#line 1436 "go/vt/sqlparser/sql.y"
    {
    (yyval.columnType) = ColumnType{Type: string((yyvsp[(1) - (4)].bytes)) + " " + string((yyvsp[(2) - (4)].bytes)), Charset: (yyvsp[(3) - (4)].str), Collate: (yyvsp[(4) - (4)].str)}
  }
    break;

  case 262:
#line 1440 "go/vt/sqlparser/sql.y"
    {
    (yyval.columnType) = ColumnType{Type: string((yyvsp[(1) - (1)].bytes))}
  }
    break;

  case 263:
#line 1444 "go/vt/sqlparser/sql.y"
    {
    (yyval.columnType) = ColumnType{Type: string((yyvsp[(1) - (1)].bytes))}
  }
    break;

  case 264:
#line 1448 "go/vt/sqlparser/sql.y"
    {
    (yyval.columnType) = ColumnType{Type: string((yyvsp[(1) - (1)].bytes))}
  }
    break;

  case 265:
#line 1452 "go/vt/sqlparser/sql.y"
    {
    (yyval.columnType) = ColumnType{Type: string((yyvsp[(1) - (1)].bytes))}
  }
    break;

  case 266:
#line 1456 "go/vt/sqlparser/sql.y"
    {
    (yyval.columnType) = ColumnType{Type: string((yyvsp[(1) - (1)].bytes))}
  }
    break;

  case 267:
#line 1460 "go/vt/sqlparser/sql.y"
    {
    (yyval.columnType) = ColumnType{Type: string((yyvsp[(1) - (6)].bytes)), EnumValues: (yyvsp[(3) - (6)].strs), Charset: (yyvsp[(5) - (6)].str), Collate: (yyvsp[(6) - (6)].str)}
  }
    break;

  case 268:
#line 1465 "go/vt/sqlparser/sql.y"
    {
    (yyval.columnType) = ColumnType{Type: string((yyvsp[(1) - (6)].bytes)), EnumValues: (yyvsp[(3) - (6)].strs), Charset: (yyvsp[(5) - (6)].str), Collate: (yyvsp[(6) - (6)].str)}
  }
    break;

  case 269:
#line 1471 "go/vt/sqlparser/sql.y"
    {
    (yyval.columnType) = ColumnType{Type: string((yyvsp[(1) - (1)].bytes))}
  }
    break;

  case 270:
#line 1475 "go/vt/sqlparser/sql.y"
    {
    (yyval.columnType) = ColumnType{Type: string((yyvsp[(1) - (1)].bytes))}
  }
    break;

  case 271:
#line 1479 "go/vt/sqlparser/sql.y"
    {
    (yyval.columnType) = ColumnType{Type: string((yyvsp[(1) - (1)].bytes))}
  }
    break;

  case 272:
#line 1483 "go/vt/sqlparser/sql.y"
    {
    (yyval.columnType) = ColumnType{Type: string((yyvsp[(1) - (1)].bytes))}
  }
    break;

  case 273:
#line 1487 "go/vt/sqlparser/sql.y"
    {
    (yyval.columnType) = ColumnType{Type: string((yyvsp[(1) - (1)].bytes))}
  }
    break;

  case 274:
#line 1491 "go/vt/sqlparser/sql.y"
    {
    (yyval.columnType) = ColumnType{Type: string((yyvsp[(1) - (1)].bytes))}
  }
    break;

  case 275:
#line 1495 "go/vt/sqlparser/sql.y"
    {
    (yyval.columnType) = ColumnType{Type: string((yyvsp[(1) - (1)].bytes))}
  }
    break;

  case 276:
#line 1499 "go/vt/sqlparser/sql.y"
    {
    (yyval.columnType) = ColumnType{Type: string((yyvsp[(1) - (1)].bytes))}
  }
    break;

  case 277:
#line 1505 "go/vt/sqlparser/sql.y"
    {
    (yyval.strs) = make([]string, 0, 4)
    (yyval.strs) = append((yyval.strs), string((yyvsp[(1) - (1)].bytes)))
  }
    break;

  case 278:
#line 1510 "go/vt/sqlparser/sql.y"
    {
    (yyval.strs) = append((yyvsp[(1) - (3)].strs), string((yyvsp[(3) - (3)].bytes)))
  }
    break;

  case 279:
#line 1515 "go/vt/sqlparser/sql.y"
    {
    (yyval.sqlVal) = nil
  }
    break;

  case 280:
#line 1519 "go/vt/sqlparser/sql.y"
    {
    (yyval.sqlVal) = NewIntVal((yyvsp[(2) - (3)].bytes))
  }
    break;

  case 281:
#line 1524 "go/vt/sqlparser/sql.y"
    {
    (yyval.LengthScaleOption) = LengthScaleOption{}
  }
    break;

  case 282:
#line 1528 "go/vt/sqlparser/sql.y"
    {
    (yyval.LengthScaleOption) = LengthScaleOption{
        Length: NewIntVal((yyvsp[(2) - (5)].bytes)),
        Scale: NewIntVal((yyvsp[(4) - (5)].bytes)),
    }
  }
    break;

  case 283:
#line 1536 "go/vt/sqlparser/sql.y"
    {
    (yyval.LengthScaleOption) = LengthScaleOption{}
  }
    break;

  case 284:
#line 1540 "go/vt/sqlparser/sql.y"
    {
    (yyval.LengthScaleOption) = LengthScaleOption{
        Length: NewIntVal((yyvsp[(2) - (3)].bytes)),
    }
  }
    break;

  case 285:
#line 1546 "go/vt/sqlparser/sql.y"
    {
    (yyval.LengthScaleOption) = LengthScaleOption{
        Length: NewIntVal((yyvsp[(2) - (5)].bytes)),
        Scale: NewIntVal((yyvsp[(4) - (5)].bytes)),
    }
  }
    break;

  case 286:
#line 1554 "go/vt/sqlparser/sql.y"
    {
    (yyval.boolVal) = BoolVal(false)
  }
    break;

  case 287:
#line 1558 "go/vt/sqlparser/sql.y"
    {
    (yyval.boolVal) = BoolVal(true)
  }
    break;

  case 288:
#line 1563 "go/vt/sqlparser/sql.y"
    {
    (yyval.boolVal) = BoolVal(false)
  }
    break;

  case 289:
#line 1567 "go/vt/sqlparser/sql.y"
    {
    (yyval.boolVal) = BoolVal(true)
  }
    break;

  case 290:
#line 1574 "go/vt/sqlparser/sql.y"
    {
    (yyval.boolVal) = BoolVal(false)
  }
    break;

  case 291:
#line 1578 "go/vt/sqlparser/sql.y"
    {
    (yyval.boolVal) = BoolVal(true)
  }
    break;

  case 292:
#line 1584 "go/vt/sqlparser/sql.y"
    {
    (yyval.optVal) = (yyvsp[(2) - (2)].expr)
  }
    break;

  case 293:
#line 1590 "go/vt/sqlparser/sql.y"
    {
    (yyval.optVal) = (yyvsp[(3) - (3)].expr)
  }
    break;

  case 294:
#line 1596 "go/vt/sqlparser/sql.y"
    {
    (yyval.boolVal) = BoolVal(true)
  }
    break;

  case 295:
#line 1601 "go/vt/sqlparser/sql.y"
    {
    (yyval.str) = ""
  }
    break;

  case 296:
#line 1605 "go/vt/sqlparser/sql.y"
    {
    (yyval.str) = string((yyvsp[(3) - (3)].bytes))
  }
    break;

  case 297:
#line 1609 "go/vt/sqlparser/sql.y"
    {
    (yyval.str) = string((yyvsp[(3) - (3)].bytes))
  }
    break;

  case 298:
#line 1614 "go/vt/sqlparser/sql.y"
    {
    (yyval.str) = ""
  }
    break;

  case 299:
#line 1618 "go/vt/sqlparser/sql.y"
    {
    (yyval.str) = string((yyvsp[(2) - (2)].bytes))
  }
    break;

  case 300:
#line 1622 "go/vt/sqlparser/sql.y"
    {
    (yyval.str) = string((yyvsp[(2) - (2)].bytes))
  }
    break;

  case 301:
#line 1628 "go/vt/sqlparser/sql.y"
    {
    (yyval.colKeyOpt) = colKeyPrimary
  }
    break;

  case 302:
#line 1632 "go/vt/sqlparser/sql.y"
    {
    (yyval.colKeyOpt) = colKey
  }
    break;

  case 303:
#line 1636 "go/vt/sqlparser/sql.y"
    {
    (yyval.colKeyOpt) = colKeyUniqueKey
  }
    break;

  case 304:
#line 1640 "go/vt/sqlparser/sql.y"
    {
    (yyval.colKeyOpt) = colKeyUnique
  }
    break;

  case 305:
#line 1646 "go/vt/sqlparser/sql.y"
    {
    (yyval.sqlVal) = NewStrVal((yyvsp[(2) - (2)].bytes))
  }
    break;

  case 306:
#line 1652 "go/vt/sqlparser/sql.y"
    {
    (yyval.indexDefinition) = &IndexDefinition{Info: (yyvsp[(1) - (5)].indexInfo), Columns: (yyvsp[(3) - (5)].indexColumns), Options: (yyvsp[(5) - (5)].indexOptions)}
  }
    break;

  case 307:
#line 1656 "go/vt/sqlparser/sql.y"
    {
    (yyval.indexDefinition) = &IndexDefinition{Info: (yyvsp[(1) - (4)].indexInfo), Columns: (yyvsp[(3) - (4)].indexColumns)}
  }
    break;

  case 308:
#line 1661 "go/vt/sqlparser/sql.y"
    {
    (yyval.indexOptions) = nil
  }
    break;

  case 309:
#line 1665 "go/vt/sqlparser/sql.y"
    {
    (yyval.indexOptions) = (yyvsp[(1) - (1)].indexOptions)
  }
    break;

  case 310:
#line 1671 "go/vt/sqlparser/sql.y"
    {
    (yyval.indexOptions) = []*IndexOption{(yyvsp[(1) - (1)].indexOption)}
  }
    break;

  case 311:
#line 1675 "go/vt/sqlparser/sql.y"
    {
    (yyval.indexOptions) = append((yyval.indexOptions), (yyvsp[(2) - (2)].indexOption))
  }
    break;

  case 312:
#line 1681 "go/vt/sqlparser/sql.y"
    {
    (yyval.indexOption) = &IndexOption{Name: string((yyvsp[(1) - (2)].bytes)), Using: string((yyvsp[(2) - (2)].bytes))}
  }
    break;

  case 313:
#line 1685 "go/vt/sqlparser/sql.y"
    {
    // should not be string
    (yyval.indexOption) = &IndexOption{Name: string((yyvsp[(1) - (3)].bytes)), Value: NewIntVal((yyvsp[(3) - (3)].bytes))}
  }
    break;

  case 314:
#line 1690 "go/vt/sqlparser/sql.y"
    {
    (yyval.indexOption) = &IndexOption{Name: string((yyvsp[(1) - (2)].bytes)), Value: NewStrVal((yyvsp[(2) - (2)].bytes))}
  }
    break;

  case 315:
#line 1696 "go/vt/sqlparser/sql.y"
    {
    (yyval.str) = ""
  }
    break;

  case 316:
#line 1700 "go/vt/sqlparser/sql.y"
    {
    (yyval.str) = string((yyvsp[(1) - (1)].bytes))
  }
    break;

  case 317:
#line 1706 "go/vt/sqlparser/sql.y"
    {
    (yyval.indexInfo) = &IndexInfo{Type: string((yyvsp[(1) - (2)].bytes)) + " " + string((yyvsp[(2) - (2)].bytes)), Name: NewColIdent("PRIMARY"), Primary: true, Unique: true}
  }
    break;

  case 318:
#line 1710 "go/vt/sqlparser/sql.y"
    {
    (yyval.indexInfo) = &IndexInfo{Type: string((yyvsp[(1) - (3)].bytes)) + " " + string((yyvsp[(2) - (3)].str)), Name: NewColIdent((yyvsp[(3) - (3)].str)), Spatial: true, Unique: false}
  }
    break;

  case 319:
#line 1714 "go/vt/sqlparser/sql.y"
    {
    (yyval.indexInfo) = &IndexInfo{Type: string((yyvsp[(1) - (3)].bytes)) + " " + string((yyvsp[(2) - (3)].str)), Name: NewColIdent((yyvsp[(3) - (3)].str)), Unique: true}
  }
    break;

  case 320:
#line 1718 "go/vt/sqlparser/sql.y"
    {
    (yyval.indexInfo) = &IndexInfo{Type: string((yyvsp[(1) - (2)].bytes)), Name: NewColIdent((yyvsp[(2) - (2)].str)), Unique: true}
  }
    break;

  case 321:
#line 1722 "go/vt/sqlparser/sql.y"
    {
    (yyval.indexInfo) = &IndexInfo{Type: string((yyvsp[(1) - (2)].str)), Name: NewColIdent((yyvsp[(2) - (2)].str)), Unique: false}
  }
    break;

  case 322:
#line 1728 "go/vt/sqlparser/sql.y"
    {
    (yyval.str) = string((yyvsp[(1) - (1)].bytes))
  }
    break;

  case 323:
#line 1732 "go/vt/sqlparser/sql.y"
    {
    (yyval.str) = string((yyvsp[(1) - (1)].bytes))
  }
    break;

  case 324:
#line 1736 "go/vt/sqlparser/sql.y"
    {
    (yyval.str) = string((yyvsp[(1) - (1)].bytes))
  }
    break;

  case 325:
#line 1742 "go/vt/sqlparser/sql.y"
    {
    (yyval.str) = string((yyvsp[(1) - (1)].bytes))
  }
    break;

  case 326:
#line 1746 "go/vt/sqlparser/sql.y"
    {
    (yyval.str) = string((yyvsp[(1) - (1)].bytes))
  }
    break;

  case 327:
#line 1751 "go/vt/sqlparser/sql.y"
    {
    (yyval.str) = ""
  }
    break;

  case 328:
#line 1755 "go/vt/sqlparser/sql.y"
    {
    (yyval.str) = (yyvsp[(1) - (1)].str)
  }
    break;

  case 329:
#line 1760 "go/vt/sqlparser/sql.y"
    {
    (yyval.str) = ""
  }
    break;

  case 330:
#line 1764 "go/vt/sqlparser/sql.y"
    {
    (yyval.str) = string((yyvsp[(1) - (1)].bytes))
  }
    break;

  case 331:
#line 1770 "go/vt/sqlparser/sql.y"
    {
    (yyval.indexColumns) = []*IndexColumn{(yyvsp[(1) - (1)].indexColumn)}
  }
    break;

  case 332:
#line 1774 "go/vt/sqlparser/sql.y"
    {
    (yyval.indexColumns) = append((yyval.indexColumns), (yyvsp[(3) - (3)].indexColumn))
  }
    break;

  case 333:
#line 1780 "go/vt/sqlparser/sql.y"
    {
      (yyval.indexColumn) = &IndexColumn{Column: (yyvsp[(1) - (3)].colIdent), Length: (yyvsp[(2) - (3)].sqlVal), Order: (yyvsp[(3) - (3)].str)}
  }
    break;

  case 334:
#line 1786 "go/vt/sqlparser/sql.y"
    {
    (yyval.constraintDefinition) = &ConstraintDefinition{Name: string((yyvsp[(2) - (3)].bytes)), Details: (yyvsp[(3) - (3)].constraintInfo)}
  }
    break;

  case 335:
#line 1790 "go/vt/sqlparser/sql.y"
    {
    (yyval.constraintDefinition) = &ConstraintDefinition{Details: (yyvsp[(1) - (1)].constraintInfo)}
  }
    break;

  case 336:
#line 1797 "go/vt/sqlparser/sql.y"
    {
    (yyval.constraintInfo) = &ForeignKeyDefinition{Source: (yyvsp[(4) - (10)].columns), ReferencedTable: (yyvsp[(7) - (10)].tableName), ReferencedColumns: (yyvsp[(9) - (10)].columns)}
  }
    break;

  case 337:
#line 1801 "go/vt/sqlparser/sql.y"
    {
    (yyval.constraintInfo) = &ForeignKeyDefinition{Source: (yyvsp[(4) - (11)].columns), ReferencedTable: (yyvsp[(7) - (11)].tableName), ReferencedColumns: (yyvsp[(9) - (11)].columns), OnDelete: (yyvsp[(11) - (11)].ReferenceAction)}
  }
    break;

  case 338:
#line 1805 "go/vt/sqlparser/sql.y"
    {
    (yyval.constraintInfo) = &ForeignKeyDefinition{Source: (yyvsp[(4) - (11)].columns), ReferencedTable: (yyvsp[(7) - (11)].tableName), ReferencedColumns: (yyvsp[(9) - (11)].columns), OnUpdate: (yyvsp[(11) - (11)].ReferenceAction)}
  }
    break;

  case 339:
#line 1809 "go/vt/sqlparser/sql.y"
    {
    (yyval.constraintInfo) = &ForeignKeyDefinition{Source: (yyvsp[(4) - (12)].columns), ReferencedTable: (yyvsp[(7) - (12)].tableName), ReferencedColumns: (yyvsp[(9) - (12)].columns), OnDelete: (yyvsp[(11) - (12)].ReferenceAction), OnUpdate: (yyvsp[(12) - (12)].ReferenceAction)}
  }
    break;

  case 340:
#line 1813 "go/vt/sqlparser/sql.y"
    {
    (yyval.constraintInfo) = &ForeignKeyDefinition{Source: (yyvsp[(4) - (12)].columns), ReferencedTable: (yyvsp[(7) - (12)].tableName), ReferencedColumns: (yyvsp[(9) - (12)].columns), OnDelete: (yyvsp[(12) - (12)].ReferenceAction), OnUpdate: (yyvsp[(11) - (12)].ReferenceAction)}
  }
    break;

  case 341:
#line 1819 "go/vt/sqlparser/sql.y"
    {
    (yyval.str) = string((yyvsp[(1) - (1)].bytes))
  }
    break;

  case 342:
#line 1823 "go/vt/sqlparser/sql.y"
    {
    (yyval.str) = string((yyvsp[(1) - (1)].bytes))
  }
    break;

  case 343:
#line 1828 "go/vt/sqlparser/sql.y"
    {
    (yyval.str) = ""
  }
    break;

  case 344:
#line 1832 "go/vt/sqlparser/sql.y"
    {
    (yyval.str) = string((yyvsp[(2) - (2)].bytes))
  }
    break;

  case 345:
#line 1836 "go/vt/sqlparser/sql.y"
    {
    (yyval.str) = string((yyvsp[(2) - (2)].bytes))
  }
    break;

  case 346:
#line 1842 "go/vt/sqlparser/sql.y"
    {
    (yyval.ReferenceAction) = (yyvsp[(3) - (3)].ReferenceAction)
  }
    break;

  case 347:
#line 1848 "go/vt/sqlparser/sql.y"
    {
    (yyval.ReferenceAction) = (yyvsp[(3) - (3)].ReferenceAction)
  }
    break;

  case 348:
#line 1854 "go/vt/sqlparser/sql.y"
    {
    (yyval.ReferenceAction) = Restrict
  }
    break;

  case 349:
#line 1858 "go/vt/sqlparser/sql.y"
    {
    (yyval.ReferenceAction) = Cascade
  }
    break;

  case 350:
#line 1862 "go/vt/sqlparser/sql.y"
    {
    (yyval.ReferenceAction) = NoAction
  }
    break;

  case 351:
#line 1866 "go/vt/sqlparser/sql.y"
    {
    (yyval.ReferenceAction) = SetDefault
  }
    break;

  case 352:
#line 1870 "go/vt/sqlparser/sql.y"
    {
    (yyval.ReferenceAction) = SetNull
  }
    break;

  case 353:
#line 1875 "go/vt/sqlparser/sql.y"
    {
    (yyval.str) = ""
  }
    break;

  case 354:
#line 1879 "go/vt/sqlparser/sql.y"
    {
    (yyval.str) = " " + string((yyvsp[(1) - (1)].str))
  }
    break;

  case 355:
#line 1883 "go/vt/sqlparser/sql.y"
    {
    (yyval.str) = string((yyvsp[(1) - (3)].str)) + ", " + string((yyvsp[(3) - (3)].str))
  }
    break;

  case 356:
#line 1891 "go/vt/sqlparser/sql.y"
    {
    (yyval.str) = (yyvsp[(1) - (1)].str)
  }
    break;

  case 357:
#line 1895 "go/vt/sqlparser/sql.y"
    {
    (yyval.str) = (yyvsp[(1) - (2)].str) + " " + (yyvsp[(2) - (2)].str)
  }
    break;

  case 358:
#line 1899 "go/vt/sqlparser/sql.y"
    {
    (yyval.str) = (yyvsp[(1) - (3)].str) + "=" + (yyvsp[(3) - (3)].str)
  }
    break;

  case 359:
#line 1905 "go/vt/sqlparser/sql.y"
    {
    (yyval.str) = (yyvsp[(1) - (1)].colIdent).String()
  }
    break;

  case 360:
#line 1909 "go/vt/sqlparser/sql.y"
    {
    (yyval.str) = "'" + string((yyvsp[(1) - (1)].bytes)) + "'"
  }
    break;

  case 361:
#line 1913 "go/vt/sqlparser/sql.y"
    {
    (yyval.str) = string((yyvsp[(1) - (1)].bytes))
  }
    break;

  case 362:
#line 1918 "go/vt/sqlparser/sql.y"
    {
    (yyval.str) = ""
  }
    break;

  case 363:
#line 1922 "go/vt/sqlparser/sql.y"
    {
    (yyval.str) = string((yyvsp[(2) - (2)].bytes))
  }
    break;

  case 367:
#line 1933 "go/vt/sqlparser/sql.y"
    {
    (yyval.statement) = &DDL{Action: AlterStr, Table: (yyvsp[(4) - (6)].tableName)}
  }
    break;

  case 368:
#line 1937 "go/vt/sqlparser/sql.y"
    {
    ddl := &DDL{Action: AlterStr, ColumnAction: AddStr, Table: (yyvsp[(4) - (10)].tableName), TableSpec: &TableSpec{}}
    ddl.TableSpec.AddColumn((yyvsp[(8) - (10)].columnDefinition))
    ddl.Column = (yyvsp[(8) - (10)].columnDefinition).Name
    (yyval.statement) = ddl
  }
    break;

  case 369:
#line 1944 "go/vt/sqlparser/sql.y"
    {
    ddl := &DDL{Action: AlterStr, ColumnAction: AddStr, Table: (yyvsp[(4) - (9)].tableName), TableSpec: &TableSpec{}, ColumnOrder: (yyvsp[(8) - (9)].columnOrder)}
    ddl.TableSpec.AddColumn((yyvsp[(7) - (9)].columnDefinition))
    ddl.Column = (yyvsp[(7) - (9)].columnDefinition).Name
    (yyval.statement) = ddl
  }
    break;

  case 370:
#line 1951 "go/vt/sqlparser/sql.y"
    {
    (yyval.statement) = &DDL{Action: AlterStr, Table: (yyvsp[(4) - (7)].tableName)}
  }
    break;

  case 371:
#line 1955 "go/vt/sqlparser/sql.y"
    {
    (yyval.statement) = &DDL{Action: AlterStr, ColumnAction: DropStr, Column: NewColIdent(string((yyvsp[(7) - (7)].bytes))), Table: (yyvsp[(4) - (7)].tableName)}
  }
    break;

  case 372:
#line 1959 "go/vt/sqlparser/sql.y"
    {
    (yyval.statement) = &DDL{Action: AlterStr, Table: (yyvsp[(4) - (7)].tableName)}
  }
    break;

  case 373:
#line 1963 "go/vt/sqlparser/sql.y"
    {
    (yyval.statement) = &DDL{Action: AlterStr, ColumnAction: RenameStr, Table: (yyvsp[(4) - (9)].tableName), Column: NewColIdent(string((yyvsp[(7) - (9)].bytes))), ToColumn: NewColIdent(string((yyvsp[(9) - (9)].bytes)))}
  }
    break;

  case 374:
#line 1967 "go/vt/sqlparser/sql.y"
    {
    // Change this to a rename statement
    (yyval.statement) = &DDL{Action: RenameStr, FromTables: TableNames{(yyvsp[(4) - (7)].tableName)}, ToTables: TableNames{(yyvsp[(7) - (7)].tableName)}}
  }
    break;

  case 375:
#line 1972 "go/vt/sqlparser/sql.y"
    {
    (yyval.statement) = &DDL{Action: AlterStr, Table: (yyvsp[(4) - (12)].tableName), IndexSpec: &IndexSpec{Action: CreateStr, ToName: NewColIdent((yyvsp[(7) - (12)].str)),  Using: (yyvsp[(8) - (12)].colIdent), Columns: (yyvsp[(10) - (12)].indexColumns), Options: (yyvsp[(12) - (12)].indexOptions)}}
  }
    break;

  case 376:
#line 1976 "go/vt/sqlparser/sql.y"
    {
    (yyval.statement) = &DDL{Action: AlterStr, Table: (yyvsp[(4) - (14)].tableName), IndexSpec: &IndexSpec{Action: CreateStr, ToName: NewColIdent((yyvsp[(9) - (14)].str)), Type: (yyvsp[(7) - (14)].str), Using: (yyvsp[(10) - (14)].colIdent), Columns: (yyvsp[(12) - (14)].indexColumns), Options: (yyvsp[(14) - (14)].indexOptions)}}
  }
    break;

  case 377:
#line 1980 "go/vt/sqlparser/sql.y"
    {
    (yyval.statement) = &DDL{Action: AlterStr, ConstraintAction: DropStr, Table: (yyvsp[(4) - (7)].tableName), TableSpec: &TableSpec{Constraints:
        []*ConstraintDefinition{&ConstraintDefinition{Name: string((yyvsp[(7) - (7)].bytes))}}}}
  }
    break;

  case 378:
#line 1985 "go/vt/sqlparser/sql.y"
    {
    (yyval.statement) = &DDL{Action: AlterStr, Table: (yyvsp[(4) - (7)].tableName), IndexSpec: &IndexSpec{Action: DropStr, ToName: (yyvsp[(7) - (7)].colIdent)}}
  }
    break;

  case 379:
#line 1989 "go/vt/sqlparser/sql.y"
    {
    (yyval.statement) = &DDL{Action: AlterStr, Table: (yyvsp[(4) - (9)].tableName), IndexSpec: &IndexSpec{Action: RenameStr, FromName: (yyvsp[(7) - (9)].colIdent), ToName: (yyvsp[(9) - (9)].colIdent)}}
  }
    break;

  case 380:
#line 1993 "go/vt/sqlparser/sql.y"
    {
    ddl := &DDL{Action: AlterStr, ColumnAction: ModifyStr, Table: (yyvsp[(4) - (9)].tableName), TableSpec: &TableSpec{}, ColumnOrder: (yyvsp[(8) - (9)].columnOrder)}
    ddl.TableSpec.AddColumn((yyvsp[(7) - (9)].columnDefinition))
    ddl.Column = (yyvsp[(7) - (9)].columnDefinition).Name
    (yyval.statement) = ddl
  }
    break;

  case 381:
#line 2000 "go/vt/sqlparser/sql.y"
    {
    ddl := &DDL{Action: AlterStr, ColumnAction: ChangeStr, Table: (yyvsp[(4) - (10)].tableName), TableSpec: &TableSpec{}, Column: NewColIdent(string((yyvsp[(7) - (10)].bytes))), ColumnOrder: (yyvsp[(9) - (10)].columnOrder)}
    ddl.TableSpec.AddColumn((yyvsp[(8) - (10)].columnDefinition))
    (yyval.statement) = ddl
  }
    break;

  case 382:
#line 2006 "go/vt/sqlparser/sql.y"
    {
    (yyval.statement) = &DDL{Action: AlterStr, Table: (yyvsp[(4) - (5)].tableName), PartitionSpec: (yyvsp[(5) - (5)].partSpec)}
  }
    break;

  case 383:
#line 2010 "go/vt/sqlparser/sql.y"
    {
    ddl := &DDL{Action: AlterStr, ConstraintAction: AddStr, Table: (yyvsp[(4) - (6)].tableName), TableSpec: &TableSpec{}}
    ddl.TableSpec.AddConstraint((yyvsp[(6) - (6)].constraintDefinition))
    (yyval.statement) = ddl
  }
    break;

  case 384:
#line 2016 "go/vt/sqlparser/sql.y"
    {
    (yyval.statement) = &DDL{Action: AlterStr, ConstraintAction: DropStr, Table: (yyvsp[(4) - (8)].tableName), TableSpec: &TableSpec{Constraints:
        []*ConstraintDefinition{&ConstraintDefinition{Name: string((yyvsp[(8) - (8)].bytes)), Details: &ForeignKeyDefinition{}}}}}
  }
    break;

  case 385:
#line 2021 "go/vt/sqlparser/sql.y"
    {
    (yyval.statement) = &DDL{Action: AlterStr, Table: (yyvsp[(4) - (7)].tableName), AutoIncSpec: &AutoIncSpec{Value: (yyvsp[(7) - (7)].expr)}}
  }
    break;

  case 386:
#line 2026 "go/vt/sqlparser/sql.y"
    {
    (yyval.columnOrder) = nil
  }
    break;

  case 387:
#line 2030 "go/vt/sqlparser/sql.y"
    {
    (yyval.columnOrder) = &ColumnOrder{First: true}
  }
    break;

  case 388:
#line 2034 "go/vt/sqlparser/sql.y"
    {
    (yyval.columnOrder) = &ColumnOrder{AfterColumn: NewColIdent(string((yyvsp[(2) - (2)].bytes)))}
  }
    break;

  case 389:
#line 2040 "go/vt/sqlparser/sql.y"
    {
    (yyval.statement) = &DDL{Action: AlterStr, Table: (yyvsp[(3) - (4)].tableName).ToViewName()}
  }
    break;

  case 390:
#line 2046 "go/vt/sqlparser/sql.y"
    {
    (yyval.statement) = &DDL{
        Action: CreateVindexStr,
        Table: (yyvsp[(5) - (7)].tableName),
        VindexSpec: &VindexSpec{
          Name: NewColIdent((yyvsp[(5) - (7)].tableName).Name.String()),
          Type: (yyvsp[(6) - (7)].colIdent),
          Params: (yyvsp[(7) - (7)].vindexParams),
        },
      }
  }
    break;

  case 391:
#line 2058 "go/vt/sqlparser/sql.y"
    {
    (yyval.statement) = &DDL{
        Action: DropVindexStr,
        Table: (yyvsp[(5) - (5)].tableName),
        VindexSpec: &VindexSpec{
          Name: NewColIdent((yyvsp[(5) - (5)].tableName).Name.String()),
        },
      }
  }
    break;

  case 392:
#line 2068 "go/vt/sqlparser/sql.y"
    {
    (yyval.statement) = &DDL{Action: AddVschemaTableStr, Table: (yyvsp[(5) - (5)].tableName)}
  }
    break;

  case 393:
#line 2072 "go/vt/sqlparser/sql.y"
    {
    (yyval.statement) = &DDL{Action: DropVschemaTableStr, Table: (yyvsp[(5) - (5)].tableName)}
  }
    break;

  case 394:
#line 2076 "go/vt/sqlparser/sql.y"
    {
    (yyval.statement) = &DDL{
        Action: AddColVindexStr,
        Table: (yyvsp[(4) - (12)].tableName),
        VindexSpec: &VindexSpec{
            Name: (yyvsp[(7) - (12)].colIdent),
            Type: (yyvsp[(11) - (12)].colIdent),
            Params: (yyvsp[(12) - (12)].vindexParams),
        },
        VindexCols: (yyvsp[(9) - (12)].columns),
      }
  }
    break;

  case 395:
#line 2089 "go/vt/sqlparser/sql.y"
    {
    (yyval.statement) = &DDL{
        Action: DropColVindexStr,
        Table: (yyvsp[(4) - (7)].tableName),
        VindexSpec: &VindexSpec{
            Name: (yyvsp[(7) - (7)].colIdent),
        },
      }
  }
    break;

  case 396:
#line 2099 "go/vt/sqlparser/sql.y"
    {
    (yyval.statement) = &DDL{Action: AddSequenceStr, Table: (yyvsp[(5) - (5)].tableName)}
  }
    break;

  case 397:
#line 2103 "go/vt/sqlparser/sql.y"
    {
    (yyval.statement) = &DDL{
        Action: AddAutoIncStr,
        Table: (yyvsp[(4) - (9)].tableName),
        AutoIncSpec: &AutoIncSpec{
            Column: (yyvsp[(7) - (9)].colIdent),
            Sequence: (yyvsp[(9) - (9)].tableName),
        },
    }
  }
    break;

  case 398:
#line 2116 "go/vt/sqlparser/sql.y"
    { }
    break;

  case 399:
#line 2118 "go/vt/sqlparser/sql.y"
    { }
    break;

  case 405:
#line 2129 "go/vt/sqlparser/sql.y"
    {
    (yyval.partSpec) = &PartitionSpec{Action: ReorganizeStr, Name: (yyvsp[(3) - (7)].colIdent), Definitions: (yyvsp[(6) - (7)].partDefs)}
  }
    break;

  case 406:
#line 2135 "go/vt/sqlparser/sql.y"
    {
    (yyval.partDefs) = []*PartitionDefinition{(yyvsp[(1) - (1)].partDef)}
  }
    break;

  case 407:
#line 2139 "go/vt/sqlparser/sql.y"
    {
    (yyval.partDefs) = append((yyvsp[(1) - (3)].partDefs), (yyvsp[(3) - (3)].partDef))
  }
    break;

  case 408:
#line 2145 "go/vt/sqlparser/sql.y"
    {
    (yyval.partDef) = &PartitionDefinition{Name: (yyvsp[(2) - (8)].colIdent), Limit: (yyvsp[(7) - (8)].expr)}
  }
    break;

  case 409:
#line 2149 "go/vt/sqlparser/sql.y"
    {
    (yyval.partDef) = &PartitionDefinition{Name: (yyvsp[(2) - (8)].colIdent), Maxvalue: true}
  }
    break;

  case 410:
#line 2155 "go/vt/sqlparser/sql.y"
    {
    (yyval.statement) = (yyvsp[(3) - (3)].ddl)
  }
    break;

  case 411:
#line 2161 "go/vt/sqlparser/sql.y"
    {
    (yyval.ddl) = &DDL{Action: RenameStr, FromTables: TableNames{(yyvsp[(1) - (3)].tableName)}, ToTables: TableNames{(yyvsp[(3) - (3)].tableName)}}
  }
    break;

  case 412:
#line 2165 "go/vt/sqlparser/sql.y"
    {
    (yyval.ddl) = (yyvsp[(1) - (5)].ddl)
    (yyval.ddl).FromTables = append((yyval.ddl).FromTables, (yyvsp[(3) - (5)].tableName))
    (yyval.ddl).ToTables = append((yyval.ddl).ToTables, (yyvsp[(5) - (5)].tableName))
  }
    break;

  case 413:
#line 2173 "go/vt/sqlparser/sql.y"
    {
    var exists bool
    if (yyvsp[(3) - (4)].byt) != 0 {
      exists = true
    }
    (yyval.statement) = &DDL{Action: DropStr, FromTables: (yyvsp[(4) - (4)].tableNames), IfExists: exists}
  }
    break;

  case 414:
#line 2181 "go/vt/sqlparser/sql.y"
    {
    (yyval.statement) = &DDL{Action: AlterStr, Table: (yyvsp[(5) - (6)].tableName), IndexSpec: &IndexSpec{Action: DropStr, ToName: (yyvsp[(3) - (6)].colIdent)}}
  }
    break;

  case 415:
#line 2185 "go/vt/sqlparser/sql.y"
    {
    var exists bool
    if (yyvsp[(3) - (4)].byt) != 0 {
      exists = true
    }
    (yyval.statement) = &DDL{Action: DropStr, FromViews: (yyvsp[(4) - (4)].tableNames), IfExists: exists}
  }
    break;

  case 416:
#line 2193 "go/vt/sqlparser/sql.y"
    {
    var exists bool
    if (yyvsp[(3) - (4)].byt) != 0 {
      exists = true
    }
    (yyval.statement) = &DBDDL{Action: DropStr, DBName: string((yyvsp[(4) - (4)].bytes)), IfExists: exists}
  }
    break;

  case 417:
#line 2201 "go/vt/sqlparser/sql.y"
    {
    var exists bool
    if (yyvsp[(3) - (4)].byt) != 0 {
      exists = true
    }
    (yyval.statement) = &DBDDL{Action: DropStr, DBName: string((yyvsp[(4) - (4)].bytes)), IfExists: exists}
  }
    break;

  case 418:
#line 2209 "go/vt/sqlparser/sql.y"
    {
    var exists bool
    if (yyvsp[(3) - (4)].byt) != 0 {
      exists = true
    }
    (yyval.statement) = &DDL{Action: DropStr, TriggerSpec: &TriggerSpec{Name: string((yyvsp[(4) - (4)].bytes))}, IfExists: exists}
  }
    break;

  case 419:
#line 2217 "go/vt/sqlparser/sql.y"
    {
    var exists bool
    if (yyvsp[(3) - (4)].byt) != 0 {
      exists = true
    }
    (yyval.statement) = &DDL{Action: DropStr, ProcedureSpec: &ProcedureSpec{Name: string((yyvsp[(4) - (4)].bytes))}, IfExists: exists}
  }
    break;

  case 420:
#line 2227 "go/vt/sqlparser/sql.y"
    {
    (yyval.statement) = &DDL{Action: TruncateStr, Table: (yyvsp[(3) - (3)].tableName)}
  }
    break;

  case 421:
#line 2231 "go/vt/sqlparser/sql.y"
    {
    (yyval.statement) = &DDL{Action: TruncateStr, Table: (yyvsp[(2) - (2)].tableName)}
  }
    break;

  case 422:
#line 2236 "go/vt/sqlparser/sql.y"
    {
    (yyval.statement) = &DDL{Action: AlterStr, Table: (yyvsp[(3) - (3)].tableName)}
  }
    break;

  case 423:
#line 2242 "go/vt/sqlparser/sql.y"
    {
    (yyval.statement) = &Show{Type: string((yyvsp[(2) - (4)].bytes)) + " " + string((yyvsp[(3) - (4)].bytes))}
  }
    break;

  case 424:
#line 2247 "go/vt/sqlparser/sql.y"
    {
    (yyval.statement) = &Show{Type: CharsetStr}
  }
    break;

  case 425:
#line 2251 "go/vt/sqlparser/sql.y"
    {
    (yyval.statement) = &Show{Type: string((yyvsp[(2) - (3)].bytes))}
  }
    break;

  case 426:
#line 2255 "go/vt/sqlparser/sql.y"
    {
    (yyval.statement) = &Show{Type: string((yyvsp[(2) - (6)].bytes)) + " " + string((yyvsp[(3) - (6)].bytes)), IfNotExists: (yyvsp[(4) - (6)].byt) == 1, Database: string((yyvsp[(5) - (6)].bytes))}
  }
    break;

  case 427:
#line 2259 "go/vt/sqlparser/sql.y"
    {
    (yyval.statement) = &Show{Type: string((yyvsp[(2) - (6)].bytes)) + " " + string((yyvsp[(3) - (6)].bytes)), IfNotExists: (yyvsp[(4) - (6)].byt) == 1, Database: string((yyvsp[(5) - (6)].bytes))}
  }
    break;

  case 428:
#line 2264 "go/vt/sqlparser/sql.y"
    {
    (yyval.statement) = &Show{Type: string((yyvsp[(2) - (4)].bytes)) + " " + string((yyvsp[(3) - (4)].bytes))}
  }
    break;

  case 429:
#line 2268 "go/vt/sqlparser/sql.y"
    {
    (yyval.statement) = &Show{Type: string((yyvsp[(2) - (4)].bytes)) + " " + string((yyvsp[(3) - (4)].bytes))}
  }
    break;

  case 430:
#line 2272 "go/vt/sqlparser/sql.y"
    {
    (yyval.statement) = &Show{Type: string((yyvsp[(2) - (4)].bytes)) + " " + string((yyvsp[(3) - (4)].bytes))}
  }
    break;

  case 431:
#line 2276 "go/vt/sqlparser/sql.y"
    {
    (yyval.statement) = &Show{Type: string((yyvsp[(2) - (4)].bytes)) + " " + string((yyvsp[(3) - (4)].bytes)), Table: (yyvsp[(4) - (4)].tableName)}
  }
    break;

  case 432:
#line 2280 "go/vt/sqlparser/sql.y"
    {
    (yyval.statement) = &Show{Type: CreateTriggerStr, Table: (yyvsp[(4) - (4)].tableName)}
  }
    break;

  case 433:
#line 2284 "go/vt/sqlparser/sql.y"
    {
    (yyval.statement) = &Show{Type: string((yyvsp[(2) - (4)].bytes)) + " " + string((yyvsp[(3) - (4)].bytes)), Table: (yyvsp[(4) - (4)].tableName)}
  }
    break;

  case 434:
#line 2288 "go/vt/sqlparser/sql.y"
    {
    (yyval.statement) = &Show{Type: string((yyvsp[(2) - (3)].bytes))}
  }
    break;

  case 435:
#line 2292 "go/vt/sqlparser/sql.y"
    {
    (yyval.statement) = &Show{Type: string((yyvsp[(2) - (3)].bytes))}
  }
    break;

  case 436:
#line 2296 "go/vt/sqlparser/sql.y"
    {
    (yyval.statement) = &Show{Type: string((yyvsp[(2) - (2)].bytes))}
  }
    break;

  case 437:
#line 2300 "go/vt/sqlparser/sql.y"
    {
    (yyval.statement) = &Show{Type: IndexStr, Table: (yyvsp[(4) - (6)].tableName), Database: (yyvsp[(5) - (6)].str), ShowIndexFilterOpt: (yyvsp[(6) - (6)].expr)}
  }
    break;

  case 438:
#line 2304 "go/vt/sqlparser/sql.y"
    {
    (yyval.statement) = &Show{Type: string((yyvsp[(2) - (2)].bytes))}
  }
    break;

  case 439:
#line 2308 "go/vt/sqlparser/sql.y"
    {
    (yyval.statement) = &Show{Type: string((yyvsp[(2) - (4)].bytes)) + " " + string((yyvsp[(3) - (4)].bytes)), ProcFuncFilter: (yyvsp[(4) - (4)].showFilter)}
  }
    break;

  case 440:
#line 2312 "go/vt/sqlparser/sql.y"
    {
    (yyval.statement) = &Show{Type: string((yyvsp[(2) - (4)].bytes)) + " " + string((yyvsp[(3) - (4)].bytes)), ProcFuncFilter: (yyvsp[(4) - (4)].showFilter)}
  }
    break;

  case 441:
#line 2316 "go/vt/sqlparser/sql.y"
    {
    (yyval.statement) = &Show{Scope: (yyvsp[(2) - (4)].str), Type: string((yyvsp[(3) - (4)].bytes))}
  }
    break;

  case 442:
#line 2320 "go/vt/sqlparser/sql.y"
    {
    (yyval.statement) = &Show{Type: string((yyvsp[(2) - (3)].bytes))}
  }
    break;

  case 443:
#line 2324 "go/vt/sqlparser/sql.y"
    {
    showTablesOpt := &ShowTablesOpt{Full:(yyvsp[(2) - (7)].str), DbName:(yyvsp[(6) - (7)].str), Filter:(yyvsp[(7) - (7)].showFilter)}
    (yyval.statement) = &Show{Type: string((yyvsp[(3) - (7)].str)), ShowTablesOpt: showTablesOpt, OnTable: (yyvsp[(5) - (7)].tableName)}
  }
    break;

  case 444:
#line 2329 "go/vt/sqlparser/sql.y"
    {
    // this is ugly, but I couldn't find a better way for now
    if (yyvsp[(3) - (6)].str) == "processlist" {
      (yyval.statement) = &Show{Type: (yyvsp[(3) - (6)].str)}
    } else {
    showTablesOpt := &ShowTablesOpt{Full:(yyvsp[(2) - (6)].str), DbName:(yyvsp[(4) - (6)].str), Filter:(yyvsp[(6) - (6)].showFilter), AsOf:(yyvsp[(5) - (6)].expr)}
      (yyval.statement) = &Show{Type: (yyvsp[(3) - (6)].str), ShowTablesOpt: showTablesOpt}
    }
  }
    break;

  case 445:
#line 2339 "go/vt/sqlparser/sql.y"
    {
    (yyval.statement) = &Show{Type: string((yyvsp[(3) - (5)].bytes)), ShowTablesOpt: &ShowTablesOpt{DbName: (yyvsp[(4) - (5)].str), Filter: (yyvsp[(5) - (5)].showFilter)}}
  }
    break;

  case 446:
#line 2343 "go/vt/sqlparser/sql.y"
    {
    (yyval.statement) = &Show{Scope: (yyvsp[(2) - (4)].str), Type: string((yyvsp[(3) - (4)].bytes))}
  }
    break;

  case 447:
#line 2347 "go/vt/sqlparser/sql.y"
    {
    (yyval.statement) = &Show{Type: string((yyvsp[(2) - (2)].bytes))}
  }
    break;

  case 448:
#line 2351 "go/vt/sqlparser/sql.y"
    {
    // Cannot dereference $4 directly, or else the parser stackcannot be pooled. See yyParsePooled
    showCollationFilterOpt := (yyvsp[(4) - (4)].expr)
    (yyval.statement) = &Show{Type: string((yyvsp[(2) - (4)].bytes)), ShowCollationFilterOpt: &showCollationFilterOpt}
  }
    break;

  case 449:
#line 2357 "go/vt/sqlparser/sql.y"
    {
    // Cannot dereference $3 directly, or else the parser stackcannot be pooled. See yyParsePooled
    cmp := (yyvsp[(3) - (3)].expr).(*ComparisonExpr)
    cmp.Left = &ColName{Name: NewColIdent("collation")}
    var ex Expr = cmp
    (yyval.statement) = &Show{Type: string((yyvsp[(2) - (3)].bytes)), ShowCollationFilterOpt: &ex}
  }
    break;

  case 450:
#line 2365 "go/vt/sqlparser/sql.y"
    {
    showTablesOpt := &ShowTablesOpt{Filter: (yyvsp[(4) - (4)].showFilter)}
    (yyval.statement) = &Show{Scope: string((yyvsp[(2) - (4)].bytes)), Type: string((yyvsp[(3) - (4)].bytes)), ShowTablesOpt: showTablesOpt}
  }
    break;

  case 451:
#line 2370 "go/vt/sqlparser/sql.y"
    {
    (yyval.statement) = &Show{Type: string((yyvsp[(2) - (3)].bytes)) + " " + string((yyvsp[(3) - (3)].bytes))}
  }
    break;

  case 452:
#line 2374 "go/vt/sqlparser/sql.y"
    {
    (yyval.statement) = &Show{Type: string((yyvsp[(2) - (3)].bytes)) + " " + string((yyvsp[(3) - (3)].bytes))}
  }
    break;

  case 453:
#line 2378 "go/vt/sqlparser/sql.y"
    {
    (yyval.statement) = &Show{Type: string((yyvsp[(2) - (5)].bytes)) + " " + string((yyvsp[(3) - (5)].bytes)), OnTable: (yyvsp[(5) - (5)].tableName)}
  }
    break;

  case 454:
#line 2382 "go/vt/sqlparser/sql.y"
    {
    (yyval.statement) = &Show{Type: string((yyvsp[(2) - (2)].bytes))}
  }
    break;

  case 455:
#line 2396 "go/vt/sqlparser/sql.y"
    {
    (yyval.statement) = &Show{Type: string((yyvsp[(2) - (3)].bytes))}
  }
    break;

  case 456:
#line 2402 "go/vt/sqlparser/sql.y"
    {
    (yyval.expr) = &ComparisonExpr{Operator: LikeStr, Right: (yyvsp[(2) - (3)].expr), Escape: (yyvsp[(3) - (3)].expr)}
  }
    break;

  case 457:
#line 2408 "go/vt/sqlparser/sql.y"
    {
    (yyval.str) = string((yyvsp[(1) - (1)].bytes))
  }
    break;

  case 458:
#line 2412 "go/vt/sqlparser/sql.y"
    {
    (yyval.str) = string((yyvsp[(1) - (1)].bytes))
  }
    break;

  case 459:
#line 2418 "go/vt/sqlparser/sql.y"
    {
    (yyval.str) = ""
  }
    break;

  case 460:
#line 2422 "go/vt/sqlparser/sql.y"
    {
    (yyval.str) = "full "
  }
    break;

  case 461:
#line 2428 "go/vt/sqlparser/sql.y"
    {
      (yyval.str) = string((yyvsp[(1) - (1)].bytes))
  }
    break;

  case 462:
#line 2432 "go/vt/sqlparser/sql.y"
    {
      (yyval.str) = string((yyvsp[(1) - (1)].bytes))
  }
    break;

  case 463:
#line 2438 "go/vt/sqlparser/sql.y"
    {
    (yyval.str) = ""
  }
    break;

  case 464:
#line 2442 "go/vt/sqlparser/sql.y"
    {
    (yyval.str) = (yyvsp[(2) - (2)].tableIdent).v
  }
    break;

  case 465:
#line 2446 "go/vt/sqlparser/sql.y"
    {
    (yyval.str) = (yyvsp[(2) - (2)].tableIdent).v
  }
    break;

  case 466:
#line 2452 "go/vt/sqlparser/sql.y"
    {
    (yyval.showFilter) = nil
  }
    break;

  case 467:
#line 2456 "go/vt/sqlparser/sql.y"
    {
    (yyval.showFilter) = &ShowFilter{Like:string((yyvsp[(2) - (2)].bytes))}
  }
    break;

  case 468:
#line 2460 "go/vt/sqlparser/sql.y"
    {
    (yyval.showFilter) = &ShowFilter{Filter:(yyvsp[(2) - (2)].expr)}
  }
    break;

  case 469:
#line 2466 "go/vt/sqlparser/sql.y"
    {
      (yyval.showFilter) = nil
    }
    break;

  case 470:
#line 2470 "go/vt/sqlparser/sql.y"
    {
      (yyval.showFilter) = &ShowFilter{Like:string((yyvsp[(2) - (2)].bytes))}
    }
    break;

  case 471:
#line 2476 "go/vt/sqlparser/sql.y"
    {
    (yyval.str) = ""
  }
    break;

  case 472:
#line 2480 "go/vt/sqlparser/sql.y"
    {
    (yyval.str) = SessionStr
  }
    break;

  case 473:
#line 2484 "go/vt/sqlparser/sql.y"
    {
    (yyval.str) = GlobalStr
  }
    break;

  case 474:
#line 2490 "go/vt/sqlparser/sql.y"
    {
    (yyval.statement) = &Use{DBName: (yyvsp[(2) - (2)].tableIdent)}
  }
    break;

  case 475:
#line 2494 "go/vt/sqlparser/sql.y"
    {
    (yyval.statement) = &Use{DBName:TableIdent{v:""}}
  }
    break;

  case 476:
#line 2500 "go/vt/sqlparser/sql.y"
    {
    (yyval.statement) = &Begin{}
  }
    break;

  case 477:
#line 2504 "go/vt/sqlparser/sql.y"
    {
    (yyval.statement) = (yyvsp[(1) - (1)].statement)
  }
    break;

  case 478:
#line 2510 "go/vt/sqlparser/sql.y"
    {
    (yyval.statement) = &Begin{}
  }
    break;

  case 479:
#line 2516 "go/vt/sqlparser/sql.y"
    {
    (yyval.statement) = &Commit{}
  }
    break;

  case 480:
#line 2522 "go/vt/sqlparser/sql.y"
    {
    (yyval.statement) = &Rollback{}
  }
    break;

  case 481:
#line 2527 "go/vt/sqlparser/sql.y"
    { }
    break;

  case 482:
#line 2528 "go/vt/sqlparser/sql.y"
    { }
    break;

  case 483:
#line 2532 "go/vt/sqlparser/sql.y"
    {
    (yyval.statement) = &Explain{ExplainFormat: (yyvsp[(2) - (3)].str), Statement: (yyvsp[(3) - (3)].statement)}
  }
    break;

  case 484:
#line 2536 "go/vt/sqlparser/sql.y"
    {
    (yyval.statement) = &Explain{Analyze: true, ExplainFormat: TreeStr, Statement: (yyvsp[(3) - (3)].selStmt)}
  }
    break;

  case 485:
#line 2542 "go/vt/sqlparser/sql.y"
    {
    (yyval.statement) = (yyvsp[(1) - (1)].selStmt)
  }
    break;

  case 489:
#line 2550 "go/vt/sqlparser/sql.y"
    {
    (yyval.str) = ""
  }
    break;

  case 490:
#line 2554 "go/vt/sqlparser/sql.y"
    {
    (yyval.str) = string((yyvsp[(3) - (3)].bytes))
  }
    break;

  case 493:
#line 2565 "go/vt/sqlparser/sql.y"
    {
    (yyval.statement) = &Show{Type: "columns", OnTable: (yyvsp[(2) - (2)].tableName)}
  }
    break;

  case 494:
#line 2569 "go/vt/sqlparser/sql.y"
    {
    (yyval.statement) = &OtherRead{}
  }
    break;

  case 495:
#line 2575 "go/vt/sqlparser/sql.y"
    {
    (yyval.statement) = &OtherAdmin{}
  }
    break;

  case 496:
#line 2579 "go/vt/sqlparser/sql.y"
    {
    (yyval.statement) = &OtherAdmin{}
  }
    break;

  case 497:
#line 2583 "go/vt/sqlparser/sql.y"
    {
    (yyval.statement) = &OtherAdmin{}
  }
    break;

  case 498:
#line 2587 "go/vt/sqlparser/sql.y"
    {
    (yyval.statement) = &OtherAdmin{}
  }
    break;

  case 499:
#line 2593 "go/vt/sqlparser/sql.y"
    {
    (yyval.statement) = &DDL{Action: FlushStr}
  }
    break;

  case 500:
#line 2597 "go/vt/sqlparser/sql.y"
    {
    setAllowComments(yylex, true)
  }
    break;

  case 501:
#line 2601 "go/vt/sqlparser/sql.y"
    {
    (yyval.bytes2) = (yyvsp[(2) - (2)].bytes2)
    setAllowComments(yylex, false)
  }
    break;

  case 502:
#line 2607 "go/vt/sqlparser/sql.y"
    {
    (yyval.bytes2) = nil
  }
    break;

  case 503:
#line 2611 "go/vt/sqlparser/sql.y"
    {
    (yyval.bytes2) = append((yyvsp[(1) - (2)].bytes2), (yyvsp[(2) - (2)].bytes))
  }
    break;

  case 504:
#line 2617 "go/vt/sqlparser/sql.y"
    {
    (yyval.str) = UnionStr
  }
    break;

  case 505:
#line 2621 "go/vt/sqlparser/sql.y"
    {
    (yyval.str) = UnionAllStr
  }
    break;

  case 506:
#line 2625 "go/vt/sqlparser/sql.y"
    {
    (yyval.str) = UnionDistinctStr
  }
    break;

  case 507:
#line 2630 "go/vt/sqlparser/sql.y"
    {
  (yyval.str) = ""
}
    break;

  case 508:
#line 2634 "go/vt/sqlparser/sql.y"
    {
  (yyval.str) = SQLNoCacheStr
}
    break;

  case 509:
#line 2638 "go/vt/sqlparser/sql.y"
    {
  (yyval.str) = SQLCacheStr
}
    break;

  case 510:
#line 2643 "go/vt/sqlparser/sql.y"
    {
    (yyval.str) = ""
  }
    break;

  case 511:
#line 2647 "go/vt/sqlparser/sql.y"
    {
    (yyval.str) = ""
  }
    break;

  case 512:
#line 2651 "go/vt/sqlparser/sql.y"
    {
    (yyval.str) = DistinctStr
  }
    break;

  case 513:
#line 2656 "go/vt/sqlparser/sql.y"
    {
    (yyval.str) = ""
  }
    break;

  case 514:
#line 2660 "go/vt/sqlparser/sql.y"
    {
    (yyval.str) = StraightJoinHint
  }
    break;

  case 515:
#line 2665 "go/vt/sqlparser/sql.y"
    {
    (yyval.selectExprs) = nil
  }
    break;

  case 516:
#line 2669 "go/vt/sqlparser/sql.y"
    {
    (yyval.selectExprs) = (yyvsp[(1) - (1)].selectExprs)
  }
    break;

  case 517:
#line 2675 "go/vt/sqlparser/sql.y"
    {
    (yyval.selectExprs) = SelectExprs{(yyvsp[(1) - (1)].selectExpr)}
  }
    break;

  case 518:
#line 2679 "go/vt/sqlparser/sql.y"
    {
    (yyval.selectExprs) = append((yyval.selectExprs), (yyvsp[(3) - (3)].selectExpr))
  }
    break;

  case 519:
#line 2685 "go/vt/sqlparser/sql.y"
    {
    (yyval.selectExpr) = &StarExpr{}
  }
    break;

  case 520:
#line 2689 "go/vt/sqlparser/sql.y"
    {
    (yyval.selectExpr) = &AliasedExpr{Expr: (yyvsp[(1) - (2)].expr), As: (yyvsp[(2) - (2)].colIdent)}
  }
    break;

  case 521:
#line 2693 "go/vt/sqlparser/sql.y"
    {
    (yyval.selectExpr) = &StarExpr{TableName: TableName{Name: (yyvsp[(1) - (3)].tableIdent)}}
  }
    break;

  case 522:
#line 2697 "go/vt/sqlparser/sql.y"
    {
    (yyval.selectExpr) = &StarExpr{TableName: TableName{Qualifier: (yyvsp[(1) - (5)].tableIdent), Name: (yyvsp[(3) - (5)].tableIdent)}}
  }
    break;

  case 523:
#line 2704 "go/vt/sqlparser/sql.y"
    {
    (yyval.over) = &Over{WindowName: (yyvsp[(2) - (2)].colIdent)}
  }
    break;

  case 524:
#line 2708 "go/vt/sqlparser/sql.y"
    {
    (yyval.over) = &Over{OrderBy: (yyvsp[(3) - (4)].orderBy)}
  }
    break;

  case 525:
#line 2712 "go/vt/sqlparser/sql.y"
    {
    (yyval.over) = &Over{PartitionBy: (yyvsp[(5) - (7)].exprs), OrderBy: (yyvsp[(6) - (7)].orderBy)}
  }
    break;

  case 526:
#line 2717 "go/vt/sqlparser/sql.y"
    {
    (yyval.over) = nil
  }
    break;

  case 527:
#line 2721 "go/vt/sqlparser/sql.y"
    {
    (yyval.over) = (yyvsp[(1) - (1)].over)
  }
    break;

  case 528:
#line 2726 "go/vt/sqlparser/sql.y"
    {
    (yyval.colIdent) = ColIdent{}
  }
    break;

  case 529:
#line 2730 "go/vt/sqlparser/sql.y"
    {
    (yyval.colIdent) = (yyvsp[(1) - (1)].colIdent)
  }
    break;

  case 530:
#line 2734 "go/vt/sqlparser/sql.y"
    {
    (yyval.colIdent) = (yyvsp[(2) - (2)].colIdent)
  }
    break;

  case 531:
#line 2740 "go/vt/sqlparser/sql.y"
    {
    (yyval.colIdent) = NewColIdent(string((yyvsp[(1) - (1)].bytes)))
  }
    break;

  case 532:
#line 2744 "go/vt/sqlparser/sql.y"
    {
    (yyval.colIdent) = NewColIdent(string((yyvsp[(1) - (1)].bytes)))
  }
    break;

  case 533:
#line 2748 "go/vt/sqlparser/sql.y"
    {
    (yyval.colIdent) = NewColIdent(string((yyvsp[(1) - (1)].bytes)))
  }
    break;

  case 534:
#line 2752 "go/vt/sqlparser/sql.y"
    {
    (yyval.colIdent) = NewColIdent(string((yyvsp[(1) - (1)].bytes)))
  }
    break;

  case 535:
#line 2757 "go/vt/sqlparser/sql.y"
    {
    (yyval.tableExprs) = TableExprs{&AliasedTableExpr{Expr:TableName{Name: NewTableIdent("dual")}}}
  }
    break;

  case 536:
#line 2761 "go/vt/sqlparser/sql.y"
    {
    (yyval.tableExprs) = (yyvsp[(2) - (2)].tableExprs)
  }
    break;

  case 537:
#line 2767 "go/vt/sqlparser/sql.y"
    {
    (yyval.tableExprs) = TableExprs{(yyvsp[(1) - (1)].tableExpr)}
  }
    break;

  case 538:
#line 2771 "go/vt/sqlparser/sql.y"
    {
    (yyval.tableExprs) = append((yyval.tableExprs), (yyvsp[(3) - (3)].tableExpr))
  }
    break;

  case 541:
#line 2781 "go/vt/sqlparser/sql.y"
    {
    (yyval.tableExpr) = (yyvsp[(1) - (1)].aliasedTableName)
  }
    break;

  case 542:
#line 2785 "go/vt/sqlparser/sql.y"
    {
    (yyval.tableExpr) = &AliasedTableExpr{Expr:(yyvsp[(1) - (3)].subquery), As: (yyvsp[(3) - (3)].tableIdent)}
  }
    break;

  case 543:
#line 2789 "go/vt/sqlparser/sql.y"
    {
    // missed alias for subquery
    yylex.Error("Every derived table must have its own alias")
    return 1
  }
    break;

  case 544:
#line 2795 "go/vt/sqlparser/sql.y"
    {
    (yyval.tableExpr) = &ParenTableExpr{Exprs: (yyvsp[(2) - (3)].tableExprs)}
  }
    break;

  case 545:
#line 2801 "go/vt/sqlparser/sql.y"
    {
    (yyval.aliasedTableName) = (yyvsp[(2) - (2)].aliasedTableName)
    (yyval.aliasedTableName).Expr = (yyvsp[(1) - (2)].tableName)
  }
    break;

  case 546:
#line 2806 "go/vt/sqlparser/sql.y"
    {
    (yyval.aliasedTableName) = (yyvsp[(6) - (6)].aliasedTableName)
    (yyval.aliasedTableName).Expr = (yyvsp[(1) - (6)].tableName)
    (yyval.aliasedTableName).Partitions = (yyvsp[(4) - (6)].partitions)
  }
    break;

  case 547:
#line 2817 "go/vt/sqlparser/sql.y"
    {
    (yyval.aliasedTableName) = &AliasedTableExpr{Hints: (yyvsp[(1) - (1)].indexHints)}
  }
    break;

  case 548:
#line 2821 "go/vt/sqlparser/sql.y"
    {
    (yyval.aliasedTableName) = &AliasedTableExpr{AsOf: &AsOf{Time: (yyvsp[(3) - (4)].expr)}, Hints: (yyvsp[(4) - (4)].indexHints)}
  }
    break;

  case 549:
#line 2825 "go/vt/sqlparser/sql.y"
    {
    (yyval.aliasedTableName) = &AliasedTableExpr{AsOf: &AsOf{Time: (yyvsp[(3) - (6)].expr)}, As: (yyvsp[(5) - (6)].tableIdent), Hints: (yyvsp[(6) - (6)].indexHints)}
  }
    break;

  case 550:
#line 2829 "go/vt/sqlparser/sql.y"
    {
    (yyval.aliasedTableName) = &AliasedTableExpr{As: (yyvsp[(2) - (3)].tableIdent), Hints: (yyvsp[(3) - (3)].indexHints)}
  }
    break;

  case 551:
#line 2833 "go/vt/sqlparser/sql.y"
    {
    (yyval.aliasedTableName) = &AliasedTableExpr{As: (yyvsp[(1) - (2)].tableIdent), Hints: (yyvsp[(2) - (2)].indexHints)}
  }
    break;

  case 552:
#line 2844 "go/vt/sqlparser/sql.y"
    {
    (yyval.expr) = nil
  }
    break;

  case 553:
#line 2848 "go/vt/sqlparser/sql.y"
    {
    (yyval.expr) = (yyvsp[(3) - (3)].expr)
  }
    break;

  case 554:
#line 2853 "go/vt/sqlparser/sql.y"
    {
    (yyval.columns) = nil
  }
    break;

  case 555:
#line 2857 "go/vt/sqlparser/sql.y"
    {
    (yyval.columns) = (yyvsp[(2) - (3)].columns)
  }
    break;

  case 556:
#line 2863 "go/vt/sqlparser/sql.y"
    {
    (yyval.columns) = Columns{(yyvsp[(1) - (1)].colIdent)}
  }
    break;

  case 557:
#line 2867 "go/vt/sqlparser/sql.y"
    {
    (yyval.columns) = append((yyval.columns), (yyvsp[(3) - (3)].colIdent))
  }
    break;

  case 558:
#line 2873 "go/vt/sqlparser/sql.y"
    {
    (yyval.partitions) = Partitions{(yyvsp[(1) - (1)].colIdent)}
  }
    break;

  case 559:
#line 2877 "go/vt/sqlparser/sql.y"
    {
    (yyval.partitions) = append((yyval.partitions), (yyvsp[(3) - (3)].colIdent))
  }
    break;

  case 560:
#line 2890 "go/vt/sqlparser/sql.y"
    {
    (yyval.tableExpr) = &JoinTableExpr{LeftExpr: (yyvsp[(1) - (4)].tableExpr), Join: (yyvsp[(2) - (4)].str), RightExpr: (yyvsp[(3) - (4)].tableExpr), Condition: (yyvsp[(4) - (4)].joinCondition)}
  }
    break;

  case 561:
#line 2894 "go/vt/sqlparser/sql.y"
    {
    (yyval.tableExpr) = &JoinTableExpr{LeftExpr: (yyvsp[(1) - (4)].tableExpr), Join: (yyvsp[(2) - (4)].str), RightExpr: (yyvsp[(3) - (4)].tableExpr), Condition: (yyvsp[(4) - (4)].joinCondition)}
  }
    break;

  case 562:
#line 2898 "go/vt/sqlparser/sql.y"
    {
    (yyval.tableExpr) = &JoinTableExpr{LeftExpr: (yyvsp[(1) - (4)].tableExpr), Join: (yyvsp[(2) - (4)].str), RightExpr: (yyvsp[(3) - (4)].tableExpr), Condition: (yyvsp[(4) - (4)].joinCondition)}
  }
    break;

  case 563:
#line 2902 "go/vt/sqlparser/sql.y"
    {
    (yyval.tableExpr) = &JoinTableExpr{LeftExpr: (yyvsp[(1) - (3)].tableExpr), Join: (yyvsp[(2) - (3)].str), RightExpr: (yyvsp[(3) - (3)].tableExpr)}
  }
    break;

  case 564:
#line 2908 "go/vt/sqlparser/sql.y"
    { (yyval.joinCondition) = JoinCondition{On: (yyvsp[(2) - (2)].expr)} }
    break;

  case 565:
#line 2910 "go/vt/sqlparser/sql.y"
    { (yyval.joinCondition) = JoinCondition{Using: (yyvsp[(3) - (4)].columns)} }
    break;

  case 566:
#line 2914 "go/vt/sqlparser/sql.y"
    { (yyval.joinCondition) = JoinCondition{} }
    break;

  case 567:
#line 2916 "go/vt/sqlparser/sql.y"
    { (yyval.joinCondition) = (yyvsp[(1) - (1)].joinCondition) }
    break;

  case 568:
#line 2920 "go/vt/sqlparser/sql.y"
    { (yyval.joinCondition) = JoinCondition{} }
    break;

  case 569:
#line 2922 "go/vt/sqlparser/sql.y"
    { (yyval.joinCondition) = JoinCondition{On: (yyvsp[(2) - (2)].expr)} }
    break;

  case 570:
#line 2925 "go/vt/sqlparser/sql.y"
    { (yyval.empty) = struct{}{} }
    break;

  case 571:
#line 2927 "go/vt/sqlparser/sql.y"
    { (yyval.empty) = struct{}{} }
    break;

  case 573:
#line 2932 "go/vt/sqlparser/sql.y"
    {
    (yyval.tableIdent) = NewTableIdent(string((yyvsp[(1) - (1)].bytes)))
  }
    break;

  case 574:
#line 2936 "go/vt/sqlparser/sql.y"
    {
    (yyval.tableIdent) = NewTableIdent(string((yyvsp[(1) - (1)].bytes)))
  }
    break;

  case 575:
#line 2942 "go/vt/sqlparser/sql.y"
    {
    (yyval.str) = JoinStr
  }
    break;

  case 576:
#line 2946 "go/vt/sqlparser/sql.y"
    {
    (yyval.str) = JoinStr
  }
    break;

  case 577:
#line 2950 "go/vt/sqlparser/sql.y"
    {
    (yyval.str) = JoinStr
  }
    break;

  case 578:
#line 2956 "go/vt/sqlparser/sql.y"
    {
    (yyval.str) = StraightJoinStr
  }
    break;

  case 579:
#line 2962 "go/vt/sqlparser/sql.y"
    {
    (yyval.str) = LeftJoinStr
  }
    break;

  case 580:
#line 2966 "go/vt/sqlparser/sql.y"
    {
    (yyval.str) = LeftJoinStr
  }
    break;

  case 581:
#line 2970 "go/vt/sqlparser/sql.y"
    {
    (yyval.str) = RightJoinStr
  }
    break;

  case 582:
#line 2974 "go/vt/sqlparser/sql.y"
    {
    (yyval.str) = RightJoinStr
  }
    break;

  case 583:
#line 2980 "go/vt/sqlparser/sql.y"
    {
    (yyval.str) = NaturalJoinStr
  }
    break;

  case 584:
#line 2984 "go/vt/sqlparser/sql.y"
    {
    if (yyvsp[(2) - (2)].str) == LeftJoinStr {
      (yyval.str) = NaturalLeftJoinStr
    } else {
      (yyval.str) = NaturalRightJoinStr
    }
  }
    break;

  case 585:
#line 2994 "go/vt/sqlparser/sql.y"
    {
    (yyval.tableName) = (yyvsp[(3) - (3)].tableName)
  }
    break;

  case 586:
#line 3000 "go/vt/sqlparser/sql.y"
    {
    (yyval.tableName) = (yyvsp[(2) - (2)].tableName)
  }
    break;

  case 587:
#line 3004 "go/vt/sqlparser/sql.y"
    {
    (yyval.tableName) = (yyvsp[(1) - (1)].tableName)
  }
    break;

  case 588:
#line 3010 "go/vt/sqlparser/sql.y"
    {
    (yyval.tableName) = TableName{Name: (yyvsp[(1) - (1)].tableIdent)}
  }
    break;

  case 589:
#line 3014 "go/vt/sqlparser/sql.y"
    {
    (yyval.tableName) = TableName{Qualifier: (yyvsp[(1) - (3)].tableIdent), Name: (yyvsp[(3) - (3)].tableIdent)}
  }
    break;

  case 590:
#line 3020 "go/vt/sqlparser/sql.y"
    {
    (yyval.tableName) = TableName{Name: (yyvsp[(1) - (3)].tableIdent)}
  }
    break;

  case 591:
#line 3025 "go/vt/sqlparser/sql.y"
    {
    (yyval.indexHints) = nil
  }
    break;

  case 592:
#line 3029 "go/vt/sqlparser/sql.y"
    {
    (yyval.indexHints) = &IndexHints{Type: UseStr, Indexes: (yyvsp[(4) - (5)].columns)}
  }
    break;

  case 593:
#line 3033 "go/vt/sqlparser/sql.y"
    {
    (yyval.indexHints) = &IndexHints{Type: IgnoreStr, Indexes: (yyvsp[(4) - (5)].columns)}
  }
    break;

  case 594:
#line 3037 "go/vt/sqlparser/sql.y"
    {
    (yyval.indexHints) = &IndexHints{Type: ForceStr, Indexes: (yyvsp[(4) - (5)].columns)}
  }
    break;

  case 595:
#line 3042 "go/vt/sqlparser/sql.y"
    {
    (yyval.expr) = nil
  }
    break;

  case 596:
#line 3046 "go/vt/sqlparser/sql.y"
    {
    (yyval.expr) = (yyvsp[(2) - (2)].expr)
  }
    break;

  case 597:
#line 3052 "go/vt/sqlparser/sql.y"
    {
    (yyval.expr) = (yyvsp[(1) - (1)].expr)
  }
    break;

  case 598:
#line 3056 "go/vt/sqlparser/sql.y"
    {
    (yyval.expr) = &AndExpr{Left: (yyvsp[(1) - (3)].expr), Right: (yyvsp[(3) - (3)].expr)}
  }
    break;

  case 599:
#line 3060 "go/vt/sqlparser/sql.y"
    {
    (yyval.expr) = &OrExpr{Left: (yyvsp[(1) - (3)].expr), Right: (yyvsp[(3) - (3)].expr)}
  }
    break;

  case 600:
#line 3064 "go/vt/sqlparser/sql.y"
    {
    (yyval.expr) = &NotExpr{Expr: (yyvsp[(2) - (2)].expr)}
  }
    break;

  case 601:
#line 3068 "go/vt/sqlparser/sql.y"
    {
    (yyval.expr) = &IsExpr{Operator: (yyvsp[(3) - (3)].str), Expr: (yyvsp[(1) - (3)].expr)}
  }
    break;

  case 602:
#line 3072 "go/vt/sqlparser/sql.y"
    {
    (yyval.expr) = (yyvsp[(1) - (1)].expr)
  }
    break;

  case 603:
#line 3076 "go/vt/sqlparser/sql.y"
    {
    (yyval.expr) = &Default{ColName: (yyvsp[(2) - (2)].str)}
  }
    break;

  case 604:
#line 3082 "go/vt/sqlparser/sql.y"
    {
    (yyval.str) = ""
  }
    break;

  case 605:
#line 3086 "go/vt/sqlparser/sql.y"
    {
    (yyval.str) = string((yyvsp[(2) - (3)].bytes))
  }
    break;

  case 606:
#line 3092 "go/vt/sqlparser/sql.y"
    {
    (yyval.boolVal) = BoolVal(true)
  }
    break;

  case 607:
#line 3096 "go/vt/sqlparser/sql.y"
    {
    (yyval.boolVal) = BoolVal(false)
  }
    break;

  case 608:
#line 3102 "go/vt/sqlparser/sql.y"
    {
    (yyval.expr) = &ComparisonExpr{Left: (yyvsp[(1) - (3)].expr), Operator: (yyvsp[(2) - (3)].str), Right: (yyvsp[(3) - (3)].expr)}
  }
    break;

  case 609:
#line 3106 "go/vt/sqlparser/sql.y"
    {
    (yyval.expr) = &ComparisonExpr{Left: (yyvsp[(1) - (3)].expr), Operator: InStr, Right: (yyvsp[(3) - (3)].colTuple)}
  }
    break;

  case 610:
#line 3110 "go/vt/sqlparser/sql.y"
    {
    (yyval.expr) = &ComparisonExpr{Left: (yyvsp[(1) - (4)].expr), Operator: NotInStr, Right: (yyvsp[(4) - (4)].colTuple)}
  }
    break;

  case 611:
#line 3114 "go/vt/sqlparser/sql.y"
    {
    (yyval.expr) = &ComparisonExpr{Left: (yyvsp[(1) - (4)].expr), Operator: LikeStr, Right: (yyvsp[(3) - (4)].expr), Escape: (yyvsp[(4) - (4)].expr)}
  }
    break;

  case 612:
#line 3118 "go/vt/sqlparser/sql.y"
    {
    (yyval.expr) = &ComparisonExpr{Left: (yyvsp[(1) - (5)].expr), Operator: NotLikeStr, Right: (yyvsp[(4) - (5)].expr), Escape: (yyvsp[(5) - (5)].expr)}
  }
    break;

  case 613:
#line 3122 "go/vt/sqlparser/sql.y"
    {
    (yyval.expr) = &ComparisonExpr{Left: (yyvsp[(1) - (3)].expr), Operator: RegexpStr, Right: (yyvsp[(3) - (3)].expr)}
  }
    break;

  case 614:
#line 3126 "go/vt/sqlparser/sql.y"
    {
    (yyval.expr) = &ComparisonExpr{Left: (yyvsp[(1) - (4)].expr), Operator: NotRegexpStr, Right: (yyvsp[(4) - (4)].expr)}
  }
    break;

  case 615:
#line 3130 "go/vt/sqlparser/sql.y"
    {
    (yyval.expr) = &RangeCond{Left: (yyvsp[(1) - (5)].expr), Operator: BetweenStr, From: (yyvsp[(3) - (5)].expr), To: (yyvsp[(5) - (5)].expr)}
  }
    break;

  case 616:
#line 3134 "go/vt/sqlparser/sql.y"
    {
    (yyval.expr) = &RangeCond{Left: (yyvsp[(1) - (6)].expr), Operator: NotBetweenStr, From: (yyvsp[(4) - (6)].expr), To: (yyvsp[(6) - (6)].expr)}
  }
    break;

  case 617:
#line 3138 "go/vt/sqlparser/sql.y"
    {
    (yyval.expr) = &ExistsExpr{Subquery: (yyvsp[(2) - (2)].subquery)}
  }
    break;

  case 618:
#line 3144 "go/vt/sqlparser/sql.y"
    {
    (yyval.str) = IsNullStr
  }
    break;

  case 619:
#line 3148 "go/vt/sqlparser/sql.y"
    {
    (yyval.str) = IsNotNullStr
  }
    break;

  case 620:
#line 3152 "go/vt/sqlparser/sql.y"
    {
    (yyval.str) = IsTrueStr
  }
    break;

  case 621:
#line 3156 "go/vt/sqlparser/sql.y"
    {
    (yyval.str) = IsNotTrueStr
  }
    break;

  case 622:
#line 3160 "go/vt/sqlparser/sql.y"
    {
    (yyval.str) = IsFalseStr
  }
    break;

  case 623:
#line 3164 "go/vt/sqlparser/sql.y"
    {
    (yyval.str) = IsNotFalseStr
  }
    break;

  case 624:
#line 3170 "go/vt/sqlparser/sql.y"
    {
    (yyval.str) = EqualStr
  }
    break;

  case 625:
#line 3174 "go/vt/sqlparser/sql.y"
    {
    (yyval.str) = LessThanStr
  }
    break;

  case 626:
#line 3178 "go/vt/sqlparser/sql.y"
    {
    (yyval.str) = GreaterThanStr
  }
    break;

  case 627:
#line 3182 "go/vt/sqlparser/sql.y"
    {
    (yyval.str) = LessEqualStr
  }
    break;

  case 628:
#line 3186 "go/vt/sqlparser/sql.y"
    {
    (yyval.str) = GreaterEqualStr
  }
    break;

  case 629:
#line 3190 "go/vt/sqlparser/sql.y"
    {
    (yyval.str) = NotEqualStr
  }
    break;

  case 630:
#line 3194 "go/vt/sqlparser/sql.y"
    {
    (yyval.str) = NullSafeEqualStr
  }
    break;

  case 631:
#line 3199 "go/vt/sqlparser/sql.y"
    {
    (yyval.expr) = nil
  }
    break;

  case 632:
#line 3203 "go/vt/sqlparser/sql.y"
    {
    (yyval.expr) = (yyvsp[(2) - (2)].expr)
  }
    break;

  case 633:
#line 3209 "go/vt/sqlparser/sql.y"
    {
    (yyval.colTuple) = (yyvsp[(1) - (1)].valTuple)
  }
    break;

  case 634:
#line 3213 "go/vt/sqlparser/sql.y"
    {
    (yyval.colTuple) = (yyvsp[(1) - (1)].subquery)
  }
    break;

  case 635:
#line 3217 "go/vt/sqlparser/sql.y"
    {
    (yyval.colTuple) = ListArg((yyvsp[(1) - (1)].bytes))
  }
    break;

  case 636:
#line 3223 "go/vt/sqlparser/sql.y"
    {
    (yyval.subquery) = &Subquery{(yyvsp[(2) - (3)].selStmt)}
  }
    break;

  case 637:
#line 3229 "go/vt/sqlparser/sql.y"
    {
    (yyval.exprs) = Exprs{(yyvsp[(1) - (1)].expr)}
  }
    break;

  case 638:
#line 3233 "go/vt/sqlparser/sql.y"
    {
    (yyval.exprs) = append((yyvsp[(1) - (3)].exprs), (yyvsp[(3) - (3)].expr))
  }
    break;

  case 639:
#line 3239 "go/vt/sqlparser/sql.y"
    {
    (yyval.expr) = (yyvsp[(1) - (1)].expr)
  }
    break;

  case 640:
#line 3243 "go/vt/sqlparser/sql.y"
    {
    (yyval.expr) = (yyvsp[(1) - (1)].boolVal)
  }
    break;

  case 641:
#line 3247 "go/vt/sqlparser/sql.y"
    {
    (yyval.expr) = (yyvsp[(1) - (1)].colName)
  }
    break;

  case 642:
#line 3251 "go/vt/sqlparser/sql.y"
    {
    (yyval.expr) = (yyvsp[(1) - (1)].expr)
  }
    break;

  case 643:
#line 3255 "go/vt/sqlparser/sql.y"
    {
    (yyval.expr) = (yyvsp[(1) - (1)].subquery)
  }
    break;

  case 644:
#line 3259 "go/vt/sqlparser/sql.y"
    {
    (yyval.expr) = &BinaryExpr{Left: (yyvsp[(1) - (3)].expr), Operator: BitAndStr, Right: (yyvsp[(3) - (3)].expr)}
  }
    break;

  case 645:
#line 3263 "go/vt/sqlparser/sql.y"
    {
    (yyval.expr) = &BinaryExpr{Left: (yyvsp[(1) - (3)].expr), Operator: BitOrStr, Right: (yyvsp[(3) - (3)].expr)}
  }
    break;

  case 646:
#line 3267 "go/vt/sqlparser/sql.y"
    {
    (yyval.expr) = &BinaryExpr{Left: (yyvsp[(1) - (3)].expr), Operator: BitXorStr, Right: (yyvsp[(3) - (3)].expr)}
  }
    break;

  case 647:
#line 3271 "go/vt/sqlparser/sql.y"
    {
    (yyval.expr) = &BinaryExpr{Left: (yyvsp[(1) - (3)].expr), Operator: PlusStr, Right: (yyvsp[(3) - (3)].expr)}
  }
    break;

  case 648:
#line 3275 "go/vt/sqlparser/sql.y"
    {
    (yyval.expr) = &BinaryExpr{Left: (yyvsp[(1) - (3)].expr), Operator: MinusStr, Right: (yyvsp[(3) - (3)].expr)}
  }
    break;

  case 649:
#line 3279 "go/vt/sqlparser/sql.y"
    {
    (yyval.expr) = &BinaryExpr{Left: (yyvsp[(1) - (3)].expr), Operator: MultStr, Right: (yyvsp[(3) - (3)].expr)}
  }
    break;

  case 650:
#line 3283 "go/vt/sqlparser/sql.y"
    {
    (yyval.expr) = &BinaryExpr{Left: (yyvsp[(1) - (3)].expr), Operator: DivStr, Right: (yyvsp[(3) - (3)].expr)}
  }
    break;

  case 651:
#line 3287 "go/vt/sqlparser/sql.y"
    {
    (yyval.expr) = &BinaryExpr{Left: (yyvsp[(1) - (3)].expr), Operator: IntDivStr, Right: (yyvsp[(3) - (3)].expr)}
  }
    break;

  case 652:
#line 3291 "go/vt/sqlparser/sql.y"
    {
    (yyval.expr) = &BinaryExpr{Left: (yyvsp[(1) - (3)].expr), Operator: ModStr, Right: (yyvsp[(3) - (3)].expr)}
  }
    break;

  case 653:
#line 3295 "go/vt/sqlparser/sql.y"
    {
    (yyval.expr) = &BinaryExpr{Left: (yyvsp[(1) - (3)].expr), Operator: ModStr, Right: (yyvsp[(3) - (3)].expr)}
  }
    break;

  case 654:
#line 3299 "go/vt/sqlparser/sql.y"
    {
    (yyval.expr) = &BinaryExpr{Left: (yyvsp[(1) - (3)].expr), Operator: ShiftLeftStr, Right: (yyvsp[(3) - (3)].expr)}
  }
    break;

  case 655:
#line 3303 "go/vt/sqlparser/sql.y"
    {
    (yyval.expr) = &BinaryExpr{Left: (yyvsp[(1) - (3)].expr), Operator: ShiftRightStr, Right: (yyvsp[(3) - (3)].expr)}
  }
    break;

  case 656:
#line 3307 "go/vt/sqlparser/sql.y"
    {
    (yyval.expr) = &BinaryExpr{Left: (yyvsp[(1) - (3)].colName), Operator: JSONExtractOp, Right: (yyvsp[(3) - (3)].expr)}
  }
    break;

  case 657:
#line 3311 "go/vt/sqlparser/sql.y"
    {
    (yyval.expr) = &BinaryExpr{Left: (yyvsp[(1) - (3)].colName), Operator: JSONUnquoteExtractOp, Right: (yyvsp[(3) - (3)].expr)}
  }
    break;

  case 658:
#line 3315 "go/vt/sqlparser/sql.y"
    {
    (yyval.expr) = &CollateExpr{Expr: (yyvsp[(1) - (3)].expr), Charset: (yyvsp[(3) - (3)].str)}
  }
    break;

  case 659:
#line 3319 "go/vt/sqlparser/sql.y"
    {
    (yyval.expr) = &UnaryExpr{Operator: BinaryStr, Expr: (yyvsp[(2) - (2)].expr)}
  }
    break;

  case 660:
#line 3323 "go/vt/sqlparser/sql.y"
    {
    (yyval.expr) = &UnaryExpr{Operator: UBinaryStr, Expr: (yyvsp[(2) - (2)].expr)}
  }
    break;

  case 661:
#line 3327 "go/vt/sqlparser/sql.y"
    {
    (yyval.expr) = &UnaryExpr{Operator: Utf8mb4Str, Expr: (yyvsp[(2) - (2)].expr)}
  }
    break;

  case 662:
#line 3331 "go/vt/sqlparser/sql.y"
    {
    if num, ok := (yyvsp[(2) - (2)].expr).(*SQLVal); ok && num.Type == IntVal {
      (yyval.expr) = num
    } else {
      (yyval.expr) = &UnaryExpr{Operator: UPlusStr, Expr: (yyvsp[(2) - (2)].expr)}
    }
  }
    break;

  case 663:
#line 3339 "go/vt/sqlparser/sql.y"
    {
    if num, ok := (yyvsp[(2) - (2)].expr).(*SQLVal); ok && num.Type == IntVal {
      // Handle double negative
      if num.Val[0] == '-' {
        num.Val = num.Val[1:]
        (yyval.expr) = num
      } else {
        (yyval.expr) = NewIntVal(append([]byte("-"), num.Val...))
      }
    } else {
      (yyval.expr) = &UnaryExpr{Operator: UMinusStr, Expr: (yyvsp[(2) - (2)].expr)}
    }
  }
    break;

  case 664:
#line 3353 "go/vt/sqlparser/sql.y"
    {
    (yyval.expr) = &UnaryExpr{Operator: TildaStr, Expr: (yyvsp[(2) - (2)].expr)}
  }
    break;

  case 665:
#line 3357 "go/vt/sqlparser/sql.y"
    {
    (yyval.expr) = &UnaryExpr{Operator: BangStr, Expr: (yyvsp[(2) - (2)].expr)}
  }
    break;

  case 666:
#line 3361 "go/vt/sqlparser/sql.y"
    {
    // This rule prevents the usage of INTERVAL
    // as a function. If support is needed for that,
    // we'll need to revisit this. The solution
    // will be non-trivial because of grammar conflicts.
    (yyval.expr) = &IntervalExpr{Expr: (yyvsp[(2) - (3)].expr), Unit: (yyvsp[(3) - (3)].colIdent).String()}
  }
    break;

  case 673:
#line 3381 "go/vt/sqlparser/sql.y"
    {
    (yyval.expr) = &FuncExpr{Name: (yyvsp[(1) - (5)].colIdent), Distinct: (yyvsp[(3) - (5)].str) == DistinctStr, Exprs: (yyvsp[(4) - (5)].selectExprs)}
  }
    break;

  case 674:
#line 3385 "go/vt/sqlparser/sql.y"
    {
    (yyval.expr) = &FuncExpr{Qualifier: (yyvsp[(1) - (6)].tableIdent), Name: (yyvsp[(3) - (6)].colIdent), Exprs: (yyvsp[(5) - (6)].selectExprs)}
  }
    break;

  case 675:
#line 3395 "go/vt/sqlparser/sql.y"
    {
    (yyval.expr) = &FuncExpr{Name: NewColIdent(string((yyvsp[(1) - (6)].bytes))), Exprs: (yyvsp[(4) - (6)].selectExprs), Distinct: (yyvsp[(3) - (6)].str) == DistinctStr, Over: (yyvsp[(6) - (6)].over)}
  }
    break;

  case 676:
#line 3399 "go/vt/sqlparser/sql.y"
    {
    (yyval.expr) = &FuncExpr{Name: NewColIdent(string((yyvsp[(1) - (6)].bytes))), Exprs: (yyvsp[(4) - (6)].selectExprs), Distinct: (yyvsp[(3) - (6)].str) == DistinctStr, Over: (yyvsp[(6) - (6)].over)}
  }
    break;

  case 677:
#line 3403 "go/vt/sqlparser/sql.y"
    {
    (yyval.expr) = &FuncExpr{Name: NewColIdent(string((yyvsp[(1) - (5)].bytes))), Exprs: (yyvsp[(3) - (5)].selectExprs), Over: (yyvsp[(5) - (5)].over)}
  }
    break;

  case 678:
#line 3407 "go/vt/sqlparser/sql.y"
    {
    (yyval.expr) = &FuncExpr{Name: NewColIdent(string((yyvsp[(1) - (5)].bytes))), Exprs: (yyvsp[(3) - (5)].selectExprs), Over: (yyvsp[(5) - (5)].over)}
  }
    break;

  case 679:
#line 3411 "go/vt/sqlparser/sql.y"
    {
    (yyval.expr) = &FuncExpr{Name: NewColIdent(string((yyvsp[(1) - (5)].bytes))), Exprs: (yyvsp[(3) - (5)].selectExprs), Over: (yyvsp[(5) - (5)].over)}
  }
    break;

  case 680:
#line 3415 "go/vt/sqlparser/sql.y"
    {
    (yyval.expr) = &FuncExpr{Name: NewColIdent(string((yyvsp[(1) - (6)].bytes))), Exprs: (yyvsp[(4) - (6)].selectExprs), Distinct: (yyvsp[(3) - (6)].str) == DistinctStr, Over: (yyvsp[(6) - (6)].over)}
  }
    break;

  case 681:
#line 3419 "go/vt/sqlparser/sql.y"
    {
    (yyval.expr) = &FuncExpr{Name: NewColIdent(string((yyvsp[(1) - (5)].bytes))), Exprs: (yyvsp[(3) - (5)].selectExprs), Over: (yyvsp[(5) - (5)].over)}
  }
    break;

  case 682:
#line 3423 "go/vt/sqlparser/sql.y"
    {
    (yyval.expr) = &FuncExpr{Name: NewColIdent(string((yyvsp[(1) - (5)].bytes))), Exprs: (yyvsp[(3) - (5)].selectExprs), Over: (yyvsp[(5) - (5)].over)}
  }
    break;

  case 683:
#line 3427 "go/vt/sqlparser/sql.y"
    {
    (yyval.expr) = &FuncExpr{Name: NewColIdent(string((yyvsp[(1) - (6)].bytes))), Exprs: (yyvsp[(4) - (6)].selectExprs), Distinct: (yyvsp[(3) - (6)].str) == DistinctStr, Over: (yyvsp[(6) - (6)].over)}
  }
    break;

  case 684:
#line 3431 "go/vt/sqlparser/sql.y"
    {
    (yyval.expr) = &FuncExpr{Name: NewColIdent(string((yyvsp[(1) - (5)].bytes))), Exprs: (yyvsp[(3) - (5)].selectExprs), Over: (yyvsp[(5) - (5)].over)}
  }
    break;

  case 685:
#line 3435 "go/vt/sqlparser/sql.y"
    {
    (yyval.expr) = &FuncExpr{Name: NewColIdent(string((yyvsp[(1) - (5)].bytes))), Exprs: (yyvsp[(3) - (5)].selectExprs), Over: (yyvsp[(5) - (5)].over)}
  }
    break;

  case 686:
#line 3439 "go/vt/sqlparser/sql.y"
    {
    (yyval.expr) = &FuncExpr{Name: NewColIdent(string((yyvsp[(1) - (5)].bytes))), Exprs: (yyvsp[(3) - (5)].selectExprs), Over: (yyvsp[(5) - (5)].over)}
  }
    break;

  case 687:
#line 3443 "go/vt/sqlparser/sql.y"
    {
    (yyval.expr) = &FuncExpr{Name: NewColIdent(string((yyvsp[(1) - (5)].bytes))), Exprs: (yyvsp[(3) - (5)].selectExprs), Over: (yyvsp[(5) - (5)].over)}
  }
    break;

  case 688:
#line 3447 "go/vt/sqlparser/sql.y"
    {
    (yyval.expr) = &FuncExpr{Name: NewColIdent(string((yyvsp[(1) - (6)].bytes))), Exprs: (yyvsp[(4) - (6)].selectExprs), Distinct: (yyvsp[(3) - (6)].str) == DistinctStr, Over: (yyvsp[(6) - (6)].over)}
  }
    break;

  case 689:
#line 3451 "go/vt/sqlparser/sql.y"
    {
    (yyval.expr) = &FuncExpr{Name: NewColIdent(string((yyvsp[(1) - (5)].bytes))), Exprs: (yyvsp[(3) - (5)].selectExprs), Over: (yyvsp[(5) - (5)].over)}
  }
    break;

  case 690:
#line 3455 "go/vt/sqlparser/sql.y"
    {
    (yyval.expr) = &FuncExpr{Name: NewColIdent(string((yyvsp[(1) - (5)].bytes))), Exprs: (yyvsp[(3) - (5)].selectExprs), Over: (yyvsp[(5) - (5)].over)}
  }
    break;

  case 691:
#line 3459 "go/vt/sqlparser/sql.y"
    {
    (yyval.expr) = &FuncExpr{Name: NewColIdent(string((yyvsp[(1) - (5)].bytes))), Exprs: (yyvsp[(3) - (5)].selectExprs), Over: (yyvsp[(5) - (5)].over)}
  }
    break;

  case 692:
#line 3468 "go/vt/sqlparser/sql.y"
    {
    (yyval.expr) = &FuncExpr{Name: NewColIdent(string((yyvsp[(1) - (4)].bytes))), Over: (yyvsp[(4) - (4)].over)}
  }
    break;

  case 693:
#line 3472 "go/vt/sqlparser/sql.y"
    {
    (yyval.expr) = &FuncExpr{Name: NewColIdent(string((yyvsp[(1) - (4)].bytes))), Over: (yyvsp[(4) - (4)].over)}
  }
    break;

  case 694:
#line 3476 "go/vt/sqlparser/sql.y"
    {
    (yyval.expr) = &FuncExpr{Name: NewColIdent(string((yyvsp[(1) - (5)].bytes))), Exprs: SelectExprs{(yyvsp[(3) - (5)].selectExpr)}, Over: (yyvsp[(5) - (5)].over)}
  }
    break;

  case 695:
#line 3480 "go/vt/sqlparser/sql.y"
    {
    (yyval.expr) = &FuncExpr{Name: NewColIdent(string((yyvsp[(1) - (5)].bytes))), Exprs: (yyvsp[(3) - (5)].selectExprs), Over: (yyvsp[(5) - (5)].over)}
  }
    break;

  case 696:
#line 3484 "go/vt/sqlparser/sql.y"
    {
    (yyval.expr) = &FuncExpr{Name: NewColIdent(string((yyvsp[(1) - (5)].bytes))), Exprs: SelectExprs{(yyvsp[(3) - (5)].selectExpr)}, Over: (yyvsp[(5) - (5)].over)}
  }
    break;

  case 697:
#line 3488 "go/vt/sqlparser/sql.y"
    {
    (yyval.expr) = &FuncExpr{Name: NewColIdent(string((yyvsp[(1) - (5)].bytes))), Exprs: (yyvsp[(3) - (5)].selectExprs), Over: (yyvsp[(5) - (5)].over)}
  }
    break;

  case 698:
#line 3492 "go/vt/sqlparser/sql.y"
    {
    (yyval.expr) = &FuncExpr{Name: NewColIdent(string((yyvsp[(1) - (5)].bytes))), Exprs: (yyvsp[(3) - (5)].selectExprs), Over: (yyvsp[(5) - (5)].over)}
  }
    break;

  case 699:
#line 3496 "go/vt/sqlparser/sql.y"
    {
    (yyval.expr) = &FuncExpr{Name: NewColIdent(string((yyvsp[(1) - (4)].bytes))), Over: (yyvsp[(4) - (4)].over)}
  }
    break;

  case 700:
#line 3500 "go/vt/sqlparser/sql.y"
    {
    (yyval.expr) = &FuncExpr{Name: NewColIdent(string((yyvsp[(1) - (4)].bytes))), Over: (yyvsp[(4) - (4)].over)}
  }
    break;

  case 701:
#line 3504 "go/vt/sqlparser/sql.y"
    {
    (yyval.expr) = &FuncExpr{Name: NewColIdent(string((yyvsp[(1) - (4)].bytes))), Over: (yyvsp[(4) - (4)].over)}
  }
    break;

  case 702:
#line 3508 "go/vt/sqlparser/sql.y"
    {
    (yyval.expr) = &FuncExpr{Name: NewColIdent(string((yyvsp[(1) - (4)].bytes))), Over: (yyvsp[(4) - (4)].over)}
  }
    break;

  case 703:
#line 3518 "go/vt/sqlparser/sql.y"
    {
    (yyval.expr) = &FuncExpr{Name: NewColIdent("left"), Exprs: (yyvsp[(3) - (4)].selectExprs)}
  }
    break;

  case 704:
#line 3522 "go/vt/sqlparser/sql.y"
    {
    (yyval.expr) = &FuncExpr{Name: NewColIdent("right"), Exprs: (yyvsp[(3) - (4)].selectExprs)}
  }
    break;

  case 705:
#line 3526 "go/vt/sqlparser/sql.y"
    {
    (yyval.expr) = &FuncExpr{Name: NewColIdent("schema")}
  }
    break;

  case 706:
#line 3530 "go/vt/sqlparser/sql.y"
    {
    (yyval.expr) = &ConvertExpr{Expr: (yyvsp[(3) - (6)].expr), Type: (yyvsp[(5) - (6)].convertType)}
  }
    break;

  case 707:
#line 3534 "go/vt/sqlparser/sql.y"
    {
    (yyval.expr) = &ConvertExpr{Expr: (yyvsp[(3) - (6)].expr), Type: (yyvsp[(5) - (6)].convertType)}
  }
    break;

  case 708:
#line 3538 "go/vt/sqlparser/sql.y"
    {
    (yyval.expr) = &ConvertUsingExpr{Expr: (yyvsp[(3) - (6)].expr), Type: (yyvsp[(5) - (6)].str)}
  }
    break;

  case 709:
#line 3542 "go/vt/sqlparser/sql.y"
    {
    (yyval.expr) = &SubstrExpr{Name: (yyvsp[(3) - (8)].colName), From: (yyvsp[(5) - (8)].expr), To: (yyvsp[(7) - (8)].expr)}
  }
    break;

  case 710:
#line 3546 "go/vt/sqlparser/sql.y"
    {
    (yyval.expr) = &SubstrExpr{Name: (yyvsp[(3) - (8)].colName), From: (yyvsp[(5) - (8)].expr), To: (yyvsp[(7) - (8)].expr)}
  }
    break;

  case 711:
#line 3550 "go/vt/sqlparser/sql.y"
    {
    (yyval.expr) = &SubstrExpr{StrVal: NewStrVal((yyvsp[(3) - (8)].bytes)), From: (yyvsp[(5) - (8)].expr), To: (yyvsp[(7) - (8)].expr)}
  }
    break;

  case 712:
#line 3554 "go/vt/sqlparser/sql.y"
    {
    (yyval.expr) = &SubstrExpr{StrVal: NewStrVal((yyvsp[(3) - (8)].bytes)), From: (yyvsp[(5) - (8)].expr), To: (yyvsp[(7) - (8)].expr)}
  }
    break;

  case 713:
#line 3558 "go/vt/sqlparser/sql.y"
    {
  (yyval.expr) = &MatchExpr{Columns: (yyvsp[(3) - (9)].selectExprs), Expr: (yyvsp[(7) - (9)].expr), Option: (yyvsp[(8) - (9)].str)}
  }
    break;

  case 714:
#line 3562 "go/vt/sqlparser/sql.y"
    {
    (yyval.expr) = &FuncExpr{Name: NewColIdent("first"), Exprs: (yyvsp[(3) - (4)].selectExprs)}
  }
    break;

  case 715:
#line 3566 "go/vt/sqlparser/sql.y"
    {
    (yyval.expr) = &GroupConcatExpr{Distinct: (yyvsp[(3) - (7)].str), Exprs: (yyvsp[(4) - (7)].selectExprs), OrderBy: (yyvsp[(5) - (7)].orderBy), Separator: (yyvsp[(6) - (7)].str)}
  }
    break;

  case 716:
#line 3570 "go/vt/sqlparser/sql.y"
    {
    (yyval.expr) = &CaseExpr{Expr: (yyvsp[(2) - (5)].expr), Whens: (yyvsp[(3) - (5)].whens), Else: (yyvsp[(4) - (5)].expr)}
  }
    break;

  case 717:
#line 3574 "go/vt/sqlparser/sql.y"
    {
    (yyval.expr) = &ValuesFuncExpr{Name: (yyvsp[(3) - (4)].colName)}
  }
    break;

  case 718:
#line 3584 "go/vt/sqlparser/sql.y"
    {
    (yyval.expr) = &FuncExpr{Name:NewColIdent("current_timestamp")}
  }
    break;

  case 719:
#line 3588 "go/vt/sqlparser/sql.y"
    {
    (yyval.expr) = &FuncExpr{Name:NewColIdent("utc_timestamp")}
  }
    break;

  case 720:
#line 3592 "go/vt/sqlparser/sql.y"
    {
    (yyval.expr) = &FuncExpr{Name:NewColIdent("utc_time")}
  }
    break;

  case 721:
#line 3597 "go/vt/sqlparser/sql.y"
    {
    (yyval.expr) = &FuncExpr{Name:NewColIdent("utc_date")}
  }
    break;

  case 722:
#line 3602 "go/vt/sqlparser/sql.y"
    {
    (yyval.expr) = &FuncExpr{Name:NewColIdent("localtime")}
  }
    break;

  case 723:
#line 3607 "go/vt/sqlparser/sql.y"
    {
    (yyval.expr) = &FuncExpr{Name:NewColIdent("localtimestamp")}
  }
    break;

  case 724:
#line 3613 "go/vt/sqlparser/sql.y"
    {
    (yyval.expr) = &FuncExpr{Name:NewColIdent("current_date")}
  }
    break;

  case 725:
#line 3618 "go/vt/sqlparser/sql.y"
    {
    (yyval.expr) = &FuncExpr{Name:NewColIdent("current_time")}
  }
    break;

  case 726:
#line 3622 "go/vt/sqlparser/sql.y"
    {
    (yyval.expr) = &FuncExpr{Name:NewColIdent("current_user")}
  }
    break;

  case 727:
#line 3627 "go/vt/sqlparser/sql.y"
    {
    (yyval.expr) = &CurTimeFuncExpr{Name:NewColIdent("current_timestamp"), Fsp:(yyvsp[(2) - (2)].expr)}
  }
    break;

  case 728:
#line 3631 "go/vt/sqlparser/sql.y"
    {
    (yyval.expr) = &CurTimeFuncExpr{Name:NewColIdent("utc_timestamp"), Fsp:(yyvsp[(2) - (2)].expr)}
  }
    break;

  case 729:
#line 3635 "go/vt/sqlparser/sql.y"
    {
    (yyval.expr) = &CurTimeFuncExpr{Name:NewColIdent("utc_time"), Fsp:(yyvsp[(2) - (2)].expr)}
  }
    break;

  case 730:
#line 3640 "go/vt/sqlparser/sql.y"
    {
    (yyval.expr) = &CurTimeFuncExpr{Name:NewColIdent("localtime"), Fsp:(yyvsp[(2) - (2)].expr)}
  }
    break;

  case 731:
#line 3645 "go/vt/sqlparser/sql.y"
    {
    (yyval.expr) = &CurTimeFuncExpr{Name:NewColIdent("localtimestamp"), Fsp:(yyvsp[(2) - (2)].expr)}
  }
    break;

  case 732:
#line 3650 "go/vt/sqlparser/sql.y"
    {
    (yyval.expr) = &CurTimeFuncExpr{Name:NewColIdent("current_time"), Fsp:(yyvsp[(2) - (2)].expr)}
  }
    break;

  case 733:
#line 3654 "go/vt/sqlparser/sql.y"
    {
    (yyval.expr) = &TimestampFuncExpr{Name:string("timestampadd"), Unit:(yyvsp[(3) - (8)].colIdent).String(), Expr1:(yyvsp[(5) - (8)].expr), Expr2:(yyvsp[(7) - (8)].expr)}
  }
    break;

  case 734:
#line 3658 "go/vt/sqlparser/sql.y"
    {
    (yyval.expr) = &TimestampFuncExpr{Name:string("timestampdiff"), Unit:(yyvsp[(3) - (8)].colIdent).String(), Expr1:(yyvsp[(5) - (8)].expr), Expr2:(yyvsp[(7) - (8)].expr)}
  }
    break;

  case 737:
#line 3668 "go/vt/sqlparser/sql.y"
    {
    (yyval.expr) = (yyvsp[(2) - (3)].expr)
  }
    break;

  case 738:
#line 3678 "go/vt/sqlparser/sql.y"
    {
    (yyval.expr) = &FuncExpr{Name: NewColIdent("if"), Exprs: (yyvsp[(3) - (4)].selectExprs)}
  }
    break;

  case 739:
#line 3682 "go/vt/sqlparser/sql.y"
    {
    (yyval.expr) = &FuncExpr{Name: NewColIdent("database"), Exprs: (yyvsp[(3) - (4)].selectExprs)}
  }
    break;

  case 740:
#line 3686 "go/vt/sqlparser/sql.y"
    {
    (yyval.expr) = &FuncExpr{Name: NewColIdent("mod"), Exprs: (yyvsp[(3) - (4)].selectExprs)}
  }
    break;

  case 741:
#line 3690 "go/vt/sqlparser/sql.y"
    {
    (yyval.expr) = &FuncExpr{Name: NewColIdent("replace"), Exprs: (yyvsp[(3) - (4)].selectExprs)}
  }
    break;

  case 742:
#line 3694 "go/vt/sqlparser/sql.y"
    {
    (yyval.expr) = &FuncExpr{Name: NewColIdent("substr"), Exprs: (yyvsp[(3) - (4)].selectExprs)}
  }
    break;

  case 743:
#line 3698 "go/vt/sqlparser/sql.y"
    {
    (yyval.expr) = &FuncExpr{Name: NewColIdent("substr"), Exprs: (yyvsp[(3) - (4)].selectExprs)}
  }
    break;

  case 744:
#line 3704 "go/vt/sqlparser/sql.y"
    {
    (yyval.str) = ""
  }
    break;

  case 745:
#line 3708 "go/vt/sqlparser/sql.y"
    {
    (yyval.str) = BooleanModeStr
  }
    break;

  case 746:
#line 3712 "go/vt/sqlparser/sql.y"
    {
    (yyval.str) = NaturalLanguageModeStr
 }
    break;

  case 747:
#line 3716 "go/vt/sqlparser/sql.y"
    {
    (yyval.str) = NaturalLanguageModeWithQueryExpansionStr
 }
    break;

  case 748:
#line 3720 "go/vt/sqlparser/sql.y"
    {
    (yyval.str) = QueryExpansionStr
 }
    break;

  case 749:
#line 3726 "go/vt/sqlparser/sql.y"
    {
    (yyval.str) = string((yyvsp[(1) - (1)].bytes))
}
    break;

  case 750:
#line 3730 "go/vt/sqlparser/sql.y"
    {
    (yyval.str) = string((yyvsp[(1) - (1)].bytes))
}
    break;

  case 751:
#line 3736 "go/vt/sqlparser/sql.y"
    {
    (yyval.convertType) = &ConvertType{Type: string((yyvsp[(1) - (2)].bytes)), Length: (yyvsp[(2) - (2)].sqlVal)}
  }
    break;

  case 752:
#line 3740 "go/vt/sqlparser/sql.y"
    {
    (yyval.convertType) = &ConvertType{Type: string((yyvsp[(1) - (3)].bytes)), Length: (yyvsp[(2) - (3)].sqlVal), Charset: (yyvsp[(3) - (3)].str), Operator: CharacterSetStr}
  }
    break;

  case 753:
#line 3744 "go/vt/sqlparser/sql.y"
    {
    (yyval.convertType) = &ConvertType{Type: string((yyvsp[(1) - (3)].bytes)), Length: (yyvsp[(2) - (3)].sqlVal), Charset: string((yyvsp[(3) - (3)].bytes))}
  }
    break;

  case 754:
#line 3748 "go/vt/sqlparser/sql.y"
    {
    (yyval.convertType) = &ConvertType{Type: string((yyvsp[(1) - (1)].bytes))}
  }
    break;

  case 755:
#line 3752 "go/vt/sqlparser/sql.y"
    {
    (yyval.convertType) = &ConvertType{Type: string((yyvsp[(1) - (2)].bytes)), Length: (yyvsp[(2) - (2)].sqlVal)}
  }
    break;

  case 756:
#line 3756 "go/vt/sqlparser/sql.y"
    {
    (yyval.convertType) = &ConvertType{Type: string((yyvsp[(1) - (2)].bytes))}
    (yyval.convertType).Length = (yyvsp[(2) - (2)].LengthScaleOption).Length
    (yyval.convertType).Scale = (yyvsp[(2) - (2)].LengthScaleOption).Scale
  }
    break;

  case 757:
#line 3762 "go/vt/sqlparser/sql.y"
    {
    (yyval.convertType) = &ConvertType{Type: string((yyvsp[(1) - (1)].bytes))}
  }
    break;

  case 758:
#line 3766 "go/vt/sqlparser/sql.y"
    {
    (yyval.convertType) = &ConvertType{Type: string((yyvsp[(1) - (2)].bytes)), Length: (yyvsp[(2) - (2)].sqlVal)}
  }
    break;

  case 759:
#line 3770 "go/vt/sqlparser/sql.y"
    {
    (yyval.convertType) = &ConvertType{Type: string((yyvsp[(1) - (1)].bytes))}
  }
    break;

  case 760:
#line 3774 "go/vt/sqlparser/sql.y"
    {
    (yyval.convertType) = &ConvertType{Type: string((yyvsp[(1) - (2)].bytes))}
  }
    break;

  case 761:
#line 3778 "go/vt/sqlparser/sql.y"
    {
    (yyval.convertType) = &ConvertType{Type: string((yyvsp[(1) - (2)].bytes)), Length: (yyvsp[(2) - (2)].sqlVal)}
  }
    break;

  case 762:
#line 3782 "go/vt/sqlparser/sql.y"
    {
    (yyval.convertType) = &ConvertType{Type: string((yyvsp[(1) - (1)].bytes))}
  }
    break;

  case 763:
#line 3786 "go/vt/sqlparser/sql.y"
    {
    (yyval.convertType) = &ConvertType{Type: string((yyvsp[(1) - (2)].bytes))}
  }
    break;

  case 764:
#line 3791 "go/vt/sqlparser/sql.y"
    {
    (yyval.expr) = nil
  }
    break;

  case 765:
#line 3795 "go/vt/sqlparser/sql.y"
    {
    (yyval.expr) = (yyvsp[(1) - (1)].expr)
  }
    break;

  case 766:
#line 3800 "go/vt/sqlparser/sql.y"
    {
    (yyval.str) = string("")
  }
    break;

  case 767:
#line 3804 "go/vt/sqlparser/sql.y"
    {
    (yyval.str) = " separator '"+string((yyvsp[(2) - (2)].bytes))+"'"
  }
    break;

  case 768:
#line 3810 "go/vt/sqlparser/sql.y"
    {
    (yyval.whens) = []*When{(yyvsp[(1) - (1)].when)}
  }
    break;

  case 769:
#line 3814 "go/vt/sqlparser/sql.y"
    {
    (yyval.whens) = append((yyvsp[(1) - (2)].whens), (yyvsp[(2) - (2)].when))
  }
    break;

  case 770:
#line 3820 "go/vt/sqlparser/sql.y"
    {
    (yyval.when) = &When{Cond: (yyvsp[(2) - (4)].expr), Val: (yyvsp[(4) - (4)].expr)}
  }
    break;

  case 771:
#line 3825 "go/vt/sqlparser/sql.y"
    {
    (yyval.expr) = nil
  }
    break;

  case 772:
#line 3829 "go/vt/sqlparser/sql.y"
    {
    (yyval.expr) = (yyvsp[(2) - (2)].expr)
  }
    break;

  case 773:
#line 3835 "go/vt/sqlparser/sql.y"
    {
    (yyval.colName) = &ColName{Name: (yyvsp[(1) - (1)].colIdent)}
  }
    break;

  case 774:
#line 3839 "go/vt/sqlparser/sql.y"
    {
    (yyval.colName) = &ColName{Qualifier: TableName{Name: (yyvsp[(1) - (3)].tableIdent)}, Name: (yyvsp[(3) - (3)].colIdent)}
  }
    break;

  case 775:
#line 3843 "go/vt/sqlparser/sql.y"
    {
    (yyval.colName) = &ColName{Qualifier: TableName{Qualifier: (yyvsp[(1) - (5)].tableIdent), Name: (yyvsp[(3) - (5)].tableIdent)}, Name: (yyvsp[(5) - (5)].colIdent)}
  }
    break;

  case 776:
#line 3849 "go/vt/sqlparser/sql.y"
    {
    (yyval.expr) = NewStrVal((yyvsp[(1) - (1)].bytes))
  }
    break;

  case 777:
#line 3853 "go/vt/sqlparser/sql.y"
    {
    (yyval.expr) = NewHexVal((yyvsp[(1) - (1)].bytes))
  }
    break;

  case 778:
#line 3857 "go/vt/sqlparser/sql.y"
    {
    (yyval.expr) = NewBitVal((yyvsp[(1) - (1)].bytes))
  }
    break;

  case 779:
#line 3861 "go/vt/sqlparser/sql.y"
    {
    (yyval.expr) = NewIntVal((yyvsp[(1) - (1)].bytes))
  }
    break;

  case 780:
#line 3865 "go/vt/sqlparser/sql.y"
    {
    (yyval.expr) = NewFloatVal((yyvsp[(1) - (1)].bytes))
  }
    break;

  case 781:
#line 3869 "go/vt/sqlparser/sql.y"
    {
    (yyval.expr) = NewHexNum((yyvsp[(1) - (1)].bytes))
  }
    break;

  case 782:
#line 3873 "go/vt/sqlparser/sql.y"
    {
    (yyval.expr) = NewValArg((yyvsp[(1) - (1)].bytes))
  }
    break;

  case 783:
#line 3877 "go/vt/sqlparser/sql.y"
    {
    (yyval.expr) = &NullVal{}
  }
    break;

  case 784:
#line 3883 "go/vt/sqlparser/sql.y"
    {
    (yyval.expr) = NewIntVal([]byte("1"))
  }
    break;

  case 785:
#line 3887 "go/vt/sqlparser/sql.y"
    {
    (yyval.expr) = NewIntVal((yyvsp[(1) - (2)].bytes))
  }
    break;

  case 786:
#line 3891 "go/vt/sqlparser/sql.y"
    {
    (yyval.expr) = NewValArg((yyvsp[(1) - (2)].bytes))
  }
    break;

  case 787:
#line 3896 "go/vt/sqlparser/sql.y"
    {
    (yyval.exprs) = nil
  }
    break;

  case 788:
#line 3900 "go/vt/sqlparser/sql.y"
    {
    (yyval.exprs) = (yyvsp[(3) - (3)].exprs)
  }
    break;

  case 789:
#line 3906 "go/vt/sqlparser/sql.y"
    {
    (yyval.exprs) = Exprs{(yyvsp[(1) - (1)].expr)}
  }
    break;

  case 790:
#line 3910 "go/vt/sqlparser/sql.y"
    {
    (yyval.exprs) = append((yyvsp[(1) - (3)].exprs), (yyvsp[(3) - (3)].expr))
  }
    break;

  case 791:
#line 3916 "go/vt/sqlparser/sql.y"
    {
    (yyval.expr) = (yyvsp[(1) - (1)].expr)
  }
    break;

  case 792:
#line 3920 "go/vt/sqlparser/sql.y"
    {
    (yyval.expr) = &ColName{Name: NewColIdent(string((yyvsp[(1) - (1)].bytes)))}
  }
    break;

  case 793:
#line 3925 "go/vt/sqlparser/sql.y"
    {
    (yyval.expr) = nil
  }
    break;

  case 794:
#line 3929 "go/vt/sqlparser/sql.y"
    {
    (yyval.expr) = (yyvsp[(2) - (2)].expr)
  }
    break;

  case 795:
#line 3935 "go/vt/sqlparser/sql.y"
    {
    (yyval.expr) = (yyvsp[(1) - (1)].expr)
  }
    break;

  case 796:
#line 3939 "go/vt/sqlparser/sql.y"
    {
    (yyval.expr) = &ColName{Name: NewColIdent(string((yyvsp[(1) - (1)].bytes)))}
  }
    break;

  case 797:
#line 3944 "go/vt/sqlparser/sql.y"
    {
    (yyval.orderBy) = nil
  }
    break;

  case 798:
#line 3948 "go/vt/sqlparser/sql.y"
    {
    (yyval.orderBy) = (yyvsp[(3) - (3)].orderBy)
  }
    break;

  case 799:
#line 3954 "go/vt/sqlparser/sql.y"
    {
    (yyval.orderBy) = OrderBy{(yyvsp[(1) - (1)].order)}
  }
    break;

  case 800:
#line 3958 "go/vt/sqlparser/sql.y"
    {
    (yyval.orderBy) = append((yyvsp[(1) - (3)].orderBy), (yyvsp[(3) - (3)].order))
  }
    break;

  case 801:
#line 3964 "go/vt/sqlparser/sql.y"
    {
    (yyval.order) = &Order{Expr: (yyvsp[(1) - (2)].expr), Direction: (yyvsp[(2) - (2)].str)}
  }
    break;

  case 802:
#line 3968 "go/vt/sqlparser/sql.y"
    {
    (yyval.order) = &Order{Expr: &ColName{Name: NewColIdent(string((yyvsp[(1) - (2)].bytes)))}, Direction: (yyvsp[(2) - (2)].str)}
  }
    break;

  case 803:
#line 3973 "go/vt/sqlparser/sql.y"
    {
    (yyval.str) = AscScr
  }
    break;

  case 804:
#line 3977 "go/vt/sqlparser/sql.y"
    {
    (yyval.str) = AscScr
  }
    break;

  case 805:
#line 3981 "go/vt/sqlparser/sql.y"
    {
    (yyval.str) = DescScr
  }
    break;

  case 806:
#line 3986 "go/vt/sqlparser/sql.y"
    {
    (yyval.limit) = nil
  }
    break;

  case 807:
#line 3990 "go/vt/sqlparser/sql.y"
    {
    (yyval.limit) = &Limit{Rowcount: (yyvsp[(2) - (2)].expr)}
  }
    break;

  case 808:
#line 3994 "go/vt/sqlparser/sql.y"
    {
    (yyval.limit) = &Limit{Offset: (yyvsp[(2) - (4)].expr), Rowcount: (yyvsp[(4) - (4)].expr)}
  }
    break;

  case 809:
#line 3998 "go/vt/sqlparser/sql.y"
    {
    (yyval.limit) = &Limit{Offset: (yyvsp[(4) - (4)].expr), Rowcount: (yyvsp[(2) - (4)].expr)}
  }
    break;

  case 810:
#line 4003 "go/vt/sqlparser/sql.y"
    {
    (yyval.str) = ""
  }
    break;

  case 811:
#line 4007 "go/vt/sqlparser/sql.y"
    {
    (yyval.str) = ForUpdateStr
  }
    break;

  case 812:
#line 4011 "go/vt/sqlparser/sql.y"
    {
    (yyval.str) = ShareModeStr
  }
    break;

  case 813:
#line 4024 "go/vt/sqlparser/sql.y"
    {
    (yyval.ins) = &Insert{Rows: (yyvsp[(2) - (2)].values)}
  }
    break;

  case 814:
#line 4028 "go/vt/sqlparser/sql.y"
    {
    (yyval.ins) = &Insert{Rows: (yyvsp[(1) - (1)].selStmt)}
  }
    break;

  case 815:
#line 4032 "go/vt/sqlparser/sql.y"
    {
    // Drop the redundant parenthesis.
    (yyval.ins) = &Insert{Rows: (yyvsp[(2) - (3)].selStmt)}
  }
    break;

  case 816:
#line 4037 "go/vt/sqlparser/sql.y"
    {
    (yyval.ins) = &Insert{Columns: (yyvsp[(2) - (5)].columns), Rows: (yyvsp[(5) - (5)].values)}
  }
    break;

  case 817:
#line 4041 "go/vt/sqlparser/sql.y"
    {
    (yyval.ins) = &Insert{Columns: (yyvsp[(2) - (4)].columns), Rows: (yyvsp[(4) - (4)].selStmt)}
  }
    break;

  case 818:
#line 4045 "go/vt/sqlparser/sql.y"
    {
    // Drop the redundant parenthesis.
    (yyval.ins) = &Insert{Columns: (yyvsp[(2) - (6)].columns), Rows: (yyvsp[(5) - (6)].selStmt)}
  }
    break;

  case 819:
#line 4052 "go/vt/sqlparser/sql.y"
    {
    (yyval.columns) = Columns{(yyvsp[(1) - (1)].colIdent)}
  }
    break;

  case 820:
#line 4056 "go/vt/sqlparser/sql.y"
    {
    (yyval.columns) = Columns{(yyvsp[(3) - (3)].colIdent)}
  }
    break;

  case 821:
#line 4060 "go/vt/sqlparser/sql.y"
    {
    (yyval.columns) = append((yyval.columns), (yyvsp[(3) - (3)].colIdent))
  }
    break;

  case 822:
#line 4064 "go/vt/sqlparser/sql.y"
    {
    (yyval.columns) = append((yyval.columns), (yyvsp[(5) - (5)].colIdent))
  }
    break;

  case 823:
#line 4069 "go/vt/sqlparser/sql.y"
    {
    (yyval.setExprs) = nil
  }
    break;

  case 824:
#line 4073 "go/vt/sqlparser/sql.y"
    {
    (yyval.setExprs) = (yyvsp[(5) - (5)].setExprs)
  }
    break;

  case 825:
#line 4079 "go/vt/sqlparser/sql.y"
    {
    (yyval.values) = Values{(yyvsp[(1) - (1)].valTuple)}
  }
    break;

  case 826:
#line 4083 "go/vt/sqlparser/sql.y"
    {
    (yyval.values) = append((yyvsp[(1) - (3)].values), (yyvsp[(3) - (3)].valTuple))
  }
    break;

  case 827:
#line 4089 "go/vt/sqlparser/sql.y"
    {
    (yyval.valTuple) = (yyvsp[(1) - (1)].valTuple)
  }
    break;

  case 828:
#line 4093 "go/vt/sqlparser/sql.y"
    {
    (yyval.valTuple) = ValTuple{}
  }
    break;

  case 829:
#line 4099 "go/vt/sqlparser/sql.y"
    {
    (yyval.valTuple) = ValTuple((yyvsp[(2) - (3)].exprs))
  }
    break;

  case 830:
#line 4105 "go/vt/sqlparser/sql.y"
    {
    if len((yyvsp[(1) - (1)].valTuple)) == 1 {
      (yyval.expr) = &ParenExpr{(yyvsp[(1) - (1)].valTuple)[0]}
    } else {
      (yyval.expr) = (yyvsp[(1) - (1)].valTuple)
    }
  }
    break;

  case 831:
#line 4115 "go/vt/sqlparser/sql.y"
    {
    (yyval.setExprs) = SetExprs{(yyvsp[(1) - (1)].setExpr)}
  }
    break;

  case 832:
#line 4119 "go/vt/sqlparser/sql.y"
    {
    (yyval.setExprs) = append((yyvsp[(1) - (3)].setExprs), (yyvsp[(3) - (3)].setExpr))
  }
    break;

  case 833:
#line 4125 "go/vt/sqlparser/sql.y"
    {
    (yyval.setExpr) = &SetExpr{Name: (yyvsp[(1) - (3)].colName), Expr: NewStrVal([]byte("on"))}
  }
    break;

  case 834:
#line 4129 "go/vt/sqlparser/sql.y"
    {
    (yyval.setExpr) = &SetExpr{Name: (yyvsp[(1) - (3)].colName), Expr: NewStrVal([]byte("off"))}
  }
    break;

  case 835:
#line 4133 "go/vt/sqlparser/sql.y"
    {
    (yyval.setExpr) = &SetExpr{Name: (yyvsp[(1) - (3)].colName), Expr: (yyvsp[(3) - (3)].expr)}
  }
    break;

  case 836:
#line 4137 "go/vt/sqlparser/sql.y"
    {
    (yyval.setExpr) = &SetExpr{Name: NewColName(string((yyvsp[(1) - (3)].bytes))), Expr: (yyvsp[(2) - (3)].expr)}
  }
    break;

  case 838:
#line 4144 "go/vt/sqlparser/sql.y"
    {
    (yyval.bytes) = []byte("charset")
  }
    break;

  case 840:
#line 4151 "go/vt/sqlparser/sql.y"
    {
    (yyval.expr) = NewStrVal([]byte((yyvsp[(1) - (1)].colIdent).String()))
  }
    break;

  case 841:
#line 4155 "go/vt/sqlparser/sql.y"
    {
    (yyval.expr) = NewStrVal((yyvsp[(1) - (1)].bytes))
  }
    break;

  case 842:
#line 4159 "go/vt/sqlparser/sql.y"
    {
    (yyval.expr) = &Default{}
  }
    break;

  case 845:
#line 4168 "go/vt/sqlparser/sql.y"
    { (yyval.byt) = 0 }
    break;

  case 846:
#line 4170 "go/vt/sqlparser/sql.y"
    { (yyval.byt) = 1 }
    break;

  case 847:
#line 4173 "go/vt/sqlparser/sql.y"
    { (yyval.byt) = 0 }
    break;

  case 848:
#line 4175 "go/vt/sqlparser/sql.y"
    { (yyval.byt) = 1 }
    break;

  case 849:
#line 4178 "go/vt/sqlparser/sql.y"
    { (yyval.str) = "" }
    break;

  case 850:
#line 4180 "go/vt/sqlparser/sql.y"
    { (yyval.str) = IgnoreStr }
    break;

  case 851:
#line 4183 "go/vt/sqlparser/sql.y"
    { (yyval.sqlVal) = nil }
    break;

  case 852:
#line 4185 "go/vt/sqlparser/sql.y"
    { (yyval.sqlVal) = NewIntVal((yyvsp[(2) - (3)].bytes)) }
    break;

  case 853:
#line 4189 "go/vt/sqlparser/sql.y"
    { (yyval.empty) = struct{}{} }
    break;

  case 854:
#line 4191 "go/vt/sqlparser/sql.y"
    { (yyval.empty) = struct{}{} }
    break;

  case 855:
#line 4193 "go/vt/sqlparser/sql.y"
    { (yyval.empty) = struct{}{} }
    break;

  case 856:
#line 4195 "go/vt/sqlparser/sql.y"
    { (yyval.empty) = struct{}{} }
    break;

  case 857:
#line 4197 "go/vt/sqlparser/sql.y"
    { (yyval.empty) = struct{}{} }
    break;

  case 858:
#line 4199 "go/vt/sqlparser/sql.y"
    { (yyval.empty) = struct{}{} }
    break;

  case 859:
#line 4201 "go/vt/sqlparser/sql.y"
    { (yyval.empty) = struct{}{} }
    break;

  case 860:
#line 4203 "go/vt/sqlparser/sql.y"
    { (yyval.empty) = struct{}{} }
    break;

  case 861:
#line 4205 "go/vt/sqlparser/sql.y"
    { (yyval.empty) = struct{}{} }
    break;

  case 862:
#line 4209 "go/vt/sqlparser/sql.y"
    { (yyval.empty) = struct{}{} }
    break;

  case 863:
#line 4211 "go/vt/sqlparser/sql.y"
    { (yyval.empty) = struct{}{} }
    break;

  case 864:
#line 4214 "go/vt/sqlparser/sql.y"
    { (yyval.empty) = struct{}{} }
    break;

  case 865:
#line 4216 "go/vt/sqlparser/sql.y"
    { (yyval.empty) = struct{}{} }
    break;

  case 866:
#line 4218 "go/vt/sqlparser/sql.y"
    { (yyval.empty) = struct{}{} }
    break;

  case 867:
#line 4222 "go/vt/sqlparser/sql.y"
    { (yyval.str) = UniqueStr }
    break;

  case 868:
#line 4224 "go/vt/sqlparser/sql.y"
    { (yyval.str) = FulltextStr }
    break;

  case 869:
#line 4226 "go/vt/sqlparser/sql.y"
    { (yyval.str) = SpatialStr }
    break;

  case 870:
#line 4229 "go/vt/sqlparser/sql.y"
    { (yyval.str) = "" }
    break;

  case 871:
#line 4231 "go/vt/sqlparser/sql.y"
    { (yyval.str) = (yyvsp[(1) - (1)].str) }
    break;

  case 872:
#line 4234 "go/vt/sqlparser/sql.y"
    { (yyval.colIdent) = ColIdent{} }
    break;

  case 873:
#line 4236 "go/vt/sqlparser/sql.y"
    { (yyval.colIdent) = (yyvsp[(2) - (2)].colIdent) }
    break;

  case 874:
#line 4240 "go/vt/sqlparser/sql.y"
    {
    (yyval.colIdent) = NewColIdent(string((yyvsp[(1) - (1)].bytes)))
  }
    break;

  case 875:
#line 4244 "go/vt/sqlparser/sql.y"
    {
    (yyval.colIdent) = NewColIdent(string((yyvsp[(1) - (1)].bytes)))
  }
    break;

  case 877:
#line 4251 "go/vt/sqlparser/sql.y"
    {
    (yyval.colIdent) = NewColIdent(string((yyvsp[(1) - (1)].bytes)))
  }
    break;

  case 878:
#line 4257 "go/vt/sqlparser/sql.y"
    {
    (yyval.tableIdent) = NewTableIdent(string((yyvsp[(1) - (1)].bytes)))
  }
    break;

  case 879:
#line 4261 "go/vt/sqlparser/sql.y"
    {
    (yyval.tableIdent) = NewTableIdent(string((yyvsp[(1) - (1)].bytes)))
  }
    break;

  case 881:
#line 4268 "go/vt/sqlparser/sql.y"
    {
    (yyval.tableIdent) = NewTableIdent(string((yyvsp[(1) - (1)].bytes)))
  }
    break;

  case 882:
#line 4273 "go/vt/sqlparser/sql.y"
    { (yyval.str) = string("") }
    break;

  case 883:
#line 4275 "go/vt/sqlparser/sql.y"
    { (yyval.str) = string((yyvsp[(2) - (2)].bytes))}
    break;

  case 884:
#line 4278 "go/vt/sqlparser/sql.y"
    { (yyval.boolVal) = BoolVal(false) }
    break;

  case 885:
#line 4280 "go/vt/sqlparser/sql.y"
    { (yyval.boolVal) = BoolVal(true) }
    break;

  case 886:
#line 4283 "go/vt/sqlparser/sql.y"
    {
    (yyval.EnclosedBy) = nil
  }
    break;

  case 887:
#line 4287 "go/vt/sqlparser/sql.y"
    {
    (yyval.EnclosedBy) = &EnclosedBy{Optionally: (yyvsp[(1) - (4)].boolVal), Delim: "'" + string((yyvsp[(4) - (4)].bytes)) + "'"}
  }
    break;

  case 888:
#line 4292 "go/vt/sqlparser/sql.y"
    {
    (yyval.boolVal) = BoolVal(false)
  }
    break;

  case 889:
#line 4296 "go/vt/sqlparser/sql.y"
    {
    (yyval.boolVal) = BoolVal(true)
  }
    break;

  case 890:
#line 4301 "go/vt/sqlparser/sql.y"
    {
    (yyval.str) = ""
  }
    break;

  case 891:
#line 4305 "go/vt/sqlparser/sql.y"
    {
    (yyval.str) =  "'" + string((yyvsp[(3) - (3)].bytes)) +  "'"
  }
    break;

  case 892:
#line 4310 "go/vt/sqlparser/sql.y"
    {
    (yyval.str) = ""
  }
    break;

  case 893:
#line 4314 "go/vt/sqlparser/sql.y"
    {
    (yyval.str) =  "'" + string((yyvsp[(3) - (3)].bytes)) + "'"
  }
    break;

  case 894:
#line 4319 "go/vt/sqlparser/sql.y"
    {
    (yyval.Fields) = nil
  }
    break;

  case 895:
#line 4323 "go/vt/sqlparser/sql.y"
    {
    (yyval.Fields) = &Fields{TerminatedBy: (yyvsp[(2) - (4)].str), EnclosedBy: (yyvsp[(3) - (4)].EnclosedBy), EscapedBy: (yyvsp[(4) - (4)].str)}
  }
    break;

  case 896:
#line 4328 "go/vt/sqlparser/sql.y"
    {
    (yyval.Lines) = nil
  }
    break;

  case 897:
#line 4332 "go/vt/sqlparser/sql.y"
    {
    (yyval.Lines) = &Lines{StartingBy: (yyvsp[(2) - (3)].str), TerminatedBy: (yyvsp[(3) - (3)].str)}
  }
    break;

  case 898:
#line 4337 "go/vt/sqlparser/sql.y"
    {
    (yyval.str) = ""
  }
    break;

  case 899:
#line 4341 "go/vt/sqlparser/sql.y"
    {
    (yyval.str) = "'" + string((yyvsp[(3) - (3)].bytes)) + "'"
  }
    break;

  case 1271:
#line 4742 "go/vt/sqlparser/sql.y"
    {
    if incNesting(yylex) {
      yylex.Error("max nesting level reached")
      return 1
    }
  }
    break;

  case 1272:
#line 4751 "go/vt/sqlparser/sql.y"
    {
    decNesting(yylex)
  }
    break;

  case 1273:
#line 4756 "go/vt/sqlparser/sql.y"
    {
  skipToEnd(yylex)
}
    break;

  case 1274:
#line 4761 "go/vt/sqlparser/sql.y"
    {
    skipToEnd(yylex)
  }
    break;

  case 1275:
#line 4765 "go/vt/sqlparser/sql.y"
    {
    skipToEnd(yylex)
  }
    break;

  case 1276:
#line 4769 "go/vt/sqlparser/sql.y"
    {
    skipToEnd(yylex)
  }
    break;


/* Line 1267 of yacc.c.  */
#line 14354 "go/vt/sqlparser/sql.go"
      default: break;
    }
  YY_SYMBOL_PRINT ("-> $$ =", yyr1[yyn], &yyval, &yyloc);

  YYPOPSTACK (yylen);
  yylen = 0;
  YY_STACK_PRINT (yyss, yyssp);

  *++yyvsp = yyval;


  /* Now `shift' the result of the reduction.  Determine what state
     that goes to, based on the state we popped back to and the rule
     number reduced by.  */

  yyn = yyr1[yyn];

  yystate = yypgoto[yyn - YYNTOKENS] + *yyssp;
  if (0 <= yystate && yystate <= YYLAST && yycheck[yystate] == *yyssp)
    yystate = yytable[yystate];
  else
    yystate = yydefgoto[yyn - YYNTOKENS];

  goto yynewstate;


/*------------------------------------.
| yyerrlab -- here on detecting error |
`------------------------------------*/
yyerrlab:
  /* If not already recovering from an error, report this error.  */
  if (!yyerrstatus)
    {
      ++yynerrs;
#if ! YYERROR_VERBOSE
      yyerror (YY_("syntax error"));
#else
      {
	YYSIZE_T yysize = yysyntax_error (0, yystate, yychar);
	if (yymsg_alloc < yysize && yymsg_alloc < YYSTACK_ALLOC_MAXIMUM)
	  {
	    YYSIZE_T yyalloc = 2 * yysize;
	    if (! (yysize <= yyalloc && yyalloc <= YYSTACK_ALLOC_MAXIMUM))
	      yyalloc = YYSTACK_ALLOC_MAXIMUM;
	    if (yymsg != yymsgbuf)
	      YYSTACK_FREE (yymsg);
	    yymsg = (char *) YYSTACK_ALLOC (yyalloc);
	    if (yymsg)
	      yymsg_alloc = yyalloc;
	    else
	      {
		yymsg = yymsgbuf;
		yymsg_alloc = sizeof yymsgbuf;
	      }
	  }

	if (0 < yysize && yysize <= yymsg_alloc)
	  {
	    (void) yysyntax_error (yymsg, yystate, yychar);
	    yyerror (yymsg);
	  }
	else
	  {
	    yyerror (YY_("syntax error"));
	    if (yysize != 0)
	      goto yyexhaustedlab;
	  }
      }
#endif
    }



  if (yyerrstatus == 3)
    {
      /* If just tried and failed to reuse look-ahead token after an
	 error, discard it.  */

      if (yychar <= YYEOF)
	{
	  /* Return failure if at end of input.  */
	  if (yychar == YYEOF)
	    YYABORT;
	}
      else
	{
	  yydestruct ("Error: discarding",
		      yytoken, &yylval);
	  yychar = YYEMPTY;
	}
    }

  /* Else will try to reuse look-ahead token after shifting the error
     token.  */
  goto yyerrlab1;


/*---------------------------------------------------.
| yyerrorlab -- error raised explicitly by YYERROR.  |
`---------------------------------------------------*/
yyerrorlab:

  /* Pacify compilers like GCC when the user code never invokes
     YYERROR and the label yyerrorlab therefore never appears in user
     code.  */
  if (/*CONSTCOND*/ 0)
     goto yyerrorlab;

  /* Do not reclaim the symbols of the rule which action triggered
     this YYERROR.  */
  YYPOPSTACK (yylen);
  yylen = 0;
  YY_STACK_PRINT (yyss, yyssp);
  yystate = *yyssp;
  goto yyerrlab1;


/*-------------------------------------------------------------.
| yyerrlab1 -- common code for both syntax error and YYERROR.  |
`-------------------------------------------------------------*/
yyerrlab1:
  yyerrstatus = 3;	/* Each real token shifted decrements this.  */

  for (;;)
    {
      yyn = yypact[yystate];
      if (yyn != YYPACT_NINF)
	{
	  yyn += YYTERROR;
	  if (0 <= yyn && yyn <= YYLAST && yycheck[yyn] == YYTERROR)
	    {
	      yyn = yytable[yyn];
	      if (0 < yyn)
		break;
	    }
	}

      /* Pop the current state because it cannot handle the error token.  */
      if (yyssp == yyss)
	YYABORT;


      yydestruct ("Error: popping",
		  yystos[yystate], yyvsp);
      YYPOPSTACK (1);
      yystate = *yyssp;
      YY_STACK_PRINT (yyss, yyssp);
    }

  if (yyn == YYFINAL)
    YYACCEPT;

  *++yyvsp = yylval;


  /* Shift the error token.  */
  YY_SYMBOL_PRINT ("Shifting", yystos[yyn], yyvsp, yylsp);

  yystate = yyn;
  goto yynewstate;


/*-------------------------------------.
| yyacceptlab -- YYACCEPT comes here.  |
`-------------------------------------*/
yyacceptlab:
  yyresult = 0;
  goto yyreturn;

/*-----------------------------------.
| yyabortlab -- YYABORT comes here.  |
`-----------------------------------*/
yyabortlab:
  yyresult = 1;
  goto yyreturn;

#ifndef yyoverflow
/*-------------------------------------------------.
| yyexhaustedlab -- memory exhaustion comes here.  |
`-------------------------------------------------*/
yyexhaustedlab:
  yyerror (YY_("memory exhausted"));
  yyresult = 2;
  /* Fall through.  */
#endif

yyreturn:
  if (yychar != YYEOF && yychar != YYEMPTY)
     yydestruct ("Cleanup: discarding lookahead",
		 yytoken, &yylval);
  /* Do not reclaim the symbols of the rule which action triggered
     this YYABORT or YYACCEPT.  */
  YYPOPSTACK (yylen);
  YY_STACK_PRINT (yyss, yyssp);
  while (yyssp != yyss)
    {
      yydestruct ("Cleanup: popping",
		  yystos[*yyssp], yyvsp);
      YYPOPSTACK (1);
    }
#ifndef yyoverflow
  if (yyss != yyssa)
    YYSTACK_FREE (yyss);
#endif
#if YYERROR_VERBOSE
  if (yymsg != yymsgbuf)
    YYSTACK_FREE (yymsg);
#endif
  /* Make sure YYID is used.  */
  return YYID (yyresult);
}



