/*
Copyright 2019 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package sqlparser

// String constants to be used in ast.
const (
	// Select.Distinct
	AllStr              = "all "
	DistinctStr         = "distinct "
	StraightJoinHint    = "straight_join "
	SQLCalcFoundRowsStr = "sql_calc_found_rows "

	// Select.Lock
	NoLockStr    = ""
	ForUpdateStr = " for update"
	ShareModeStr = " lock in share mode"

	// Select.Cache
	SQLCacheStr   = "sql_cache "
	SQLNoCacheStr = "sql_no_cache "

	// Union.Type
	UnionStr         = "union"
	UnionAllStr      = "union all"
	UnionDistinctStr = "union distinct"

	// DDL strings.
	InsertStr  = "insert"
	ReplaceStr = "replace"

	// Set.Scope or Show.Scope
	SessionStr        = "session"
	GlobalStr         = "global"
	VitessMetadataStr = "vitess_metadata"
	VariableStr       = "variable"
	LocalStr          = "local"
	ImplicitStr       = ""

	// DDL strings.
	CreateStr           = "create"
	AlterStr            = "alter"
	DropStr             = "drop"
	RenameStr           = "rename"
	TruncateStr         = "truncate"
	FlushStr            = "flush"
	CreateVindexStr     = "create vindex"
	DropVindexStr       = "drop vindex"
	AddVschemaTableStr  = "add vschema table"
	DropVschemaTableStr = "drop vschema table"
	AddColVindexStr     = "on table add vindex"
	DropColVindexStr    = "on table drop vindex"
	AddSequenceStr      = "add sequence"
	AddAutoIncStr       = "add auto_increment"

	// Online DDL hint
	OnlineStr = "online"

	// Vindex DDL param to specify the owner of a vindex
	VindexOwnerStr = "owner"

	// Partition strings
	ReorganizeStr        = "reorganize partition"
	AddStr               = "add partition"
	DiscardStr           = "discard partition"
	DropPartitionStr     = "drop partition"
	ImportStr            = "import partition"
	TruncatePartitionStr = "truncate partition"
	CoalesceStr          = "coalesce partition"
	ExchangeStr          = "exchange partition"
	AnalyzePartitionStr  = "analyze partition"
	CheckStr             = "check partition"
	OptimizeStr          = "optimize partition"
	RebuildStr           = "rebuild partition"
	RepairStr            = "repair partition"
	RemoveStr            = "remove partitioning"
	UpgradeStr           = "upgrade partitioning"

	// JoinTableExpr.Join
	JoinStr             = "join"
	StraightJoinStr     = "straight_join"
	LeftJoinStr         = "left join"
	RightJoinStr        = "right join"
	NaturalJoinStr      = "natural join"
	NaturalLeftJoinStr  = "natural left join"
	NaturalRightJoinStr = "natural right join"

	// Index hints.
	UseStr    = "use "
	IgnoreStr = "ignore "
	ForceStr  = "force "

	// Where.Type
	WhereStr  = "where"
	HavingStr = "having"

	// ComparisonExpr.Operator
	EqualStr         = "="
	LessThanStr      = "<"
	GreaterThanStr   = ">"
	LessEqualStr     = "<="
	GreaterEqualStr  = ">="
	NotEqualStr      = "!="
	NullSafeEqualStr = "<=>"
	InStr            = "in"
	NotInStr         = "not in"
	LikeStr          = "like"
	NotLikeStr       = "not like"
	RegexpStr        = "regexp"
	NotRegexpStr     = "not regexp"

	// IsExpr.Operator
	IsNullStr     = "is null"
	IsNotNullStr  = "is not null"
	IsTrueStr     = "is true"
	IsNotTrueStr  = "is not true"
	IsFalseStr    = "is false"
	IsNotFalseStr = "is not false"

	// BinaryExpr.Operator
	BitAndStr               = "&"
	BitOrStr                = "|"
	BitXorStr               = "^"
	PlusStr                 = "+"
	MinusStr                = "-"
	MultStr                 = "*"
	DivStr                  = "/"
	IntDivStr               = "div"
	ModStr                  = "%"
	ShiftLeftStr            = "<<"
	ShiftRightStr           = ">>"
	JSONExtractOpStr        = "->"
	JSONUnquoteExtractOpStr = "->>"

	// UnaryExpr.Operator
	UPlusStr    = "+"
	UMinusStr   = "-"
	TildaStr    = "~"
	BangStr     = "!"
	BinaryStr   = "binary "
	Armscii8Str = "_armscii8"
	ASCIIStr    = "_ascii"
	Big5Str     = "_big5"
	UBinaryStr  = "_binary"
	Cp1250Str   = "_cp1250"
	Cp1251Str   = "_cp1251"
	Cp1256Str   = "_cp1256"
	Cp1257Str   = "_cp1257"
	Cp850Str    = "_cp850"
	Cp852Str    = "_cp852"
	Cp866Str    = "_cp866"
	Cp932Str    = "_cp932"
	Dec8Str     = "_dec8"
	EucjpmsStr  = "_eucjpms"
	EuckrStr    = "_euckr"
	Gb18030Str  = "_gb18030"
	Gb2312Str   = "_gb2312"
	GbkStr      = "_gbk"
	Geostd8Str  = "_geostd8"
	GreekStr    = "_greek"
	HebrewStr   = "_hebrew"
	Hp8Str      = "_hp8"
	Keybcs2Str  = "_keybcs2"
	Koi8rStr    = "_koi8r"
	Koi8uStr    = "_koi8u"
	Latin1Str   = "_latin1"
	Latin2Str   = "_latin2"
	Latin5Str   = "_latin5"
	Latin7Str   = "_latin7"
	MacceStr    = "_macce"
	MacromanStr = "_macroman"
	SjisStr     = "_sjis"
	Swe7Str     = "_swe7"
	Tis620Str   = "_tis620"
	Ucs2Str     = "_ucs2"
	UjisStr     = "_ujis"
	Utf16Str    = "_utf16"
	Utf16leStr  = "_utf16le"
	Utf32Str    = "_utf32"
	Utf8Str     = "_utf8"
	Utf8mb4Str  = "_utf8mb4"
	NStringStr  = "N"

	// ConvertType.Operator
	CharacterSetStr = " character set"
	NoOperatorStr   = ""

	// CollateAndCharset.Type
	CollateStr = " collate"

	// MatchExpr.Option
	NoOptionStr                              = ""
	BooleanModeStr                           = " in boolean mode"
	NaturalLanguageModeStr                   = " in natural language mode"
	NaturalLanguageModeWithQueryExpansionStr = " in natural language mode with query expansion"
	QueryExpansionStr                        = " with query expansion"

	// INTO OUTFILE
	IntoOutfileStr   = " into outfile "
	IntoOutfileS3Str = " into outfile s3 "
	IntoDumpfileStr  = " into dumpfile "

	// Order.Direction
	AscScr  = "asc"
	DescScr = "desc"

	// SetExpr.Expr, for SET TRANSACTION ... or START TRANSACTION
	// TransactionStr is the Name for a SET TRANSACTION statement
	TransactionStr = "transaction"

	// Transaction isolation levels
	ReadUncommittedStr = "read uncommitted"
	ReadCommittedStr   = "read committed"
	RepeatableReadStr  = "repeatable read"
	SerializableStr    = "serializable"

	TxReadOnly  = "read only"
	TxReadWrite = "read write"

	// Explain formats
	EmptyStr       = ""
	TreeStr        = "tree"
	JSONStr        = "json"
	VitessStr      = "vitess"
	TraditionalStr = "traditional"
	AnalyzeStr     = "analyze"

	// Lock Types
	ReadStr             = "read"
	ReadLocalStr        = "read local"
	WriteStr            = "write"
	LowPriorityWriteStr = "low_priority write"

	// ShowCommand Types
	CharsetStr          = " charset"
	CollationStr        = " collation"
	ColumnStr           = " columns"
	CreateDbStr         = " create database"
	CreateEStr          = " create event"
	CreateFStr          = " create function"
	CreateProcStr       = " create procedure"
	CreateTblStr        = " create table"
	CreateTrStr         = " create trigger"
	CreateVStr          = " create view"
	DatabaseStr         = " databases"
	FunctionCStr        = " function code"
	FunctionStr         = " function status"
	GtidExecGlobalStr   = " global gtid_executed"
	IndexStr            = " indexes"
	OpenTableStr        = " open tables"
	PrivilegeStr        = " privileges"
	ProcedureCStr       = " procedure code"
	ProcedureStr        = " procedure status"
	StatusGlobalStr     = " global status"
	StatusSessionStr    = " status"
	TableStr            = " tables"
	TableStatusStr      = " table status"
	TriggerStr          = " triggers"
	VariableGlobalStr   = " global variables"
	VariableSessionStr  = " variables"
	VGtidExecGlobalStr  = " global vgtid_executed"
	KeyspaceStr         = " keyspaces"
	VitessMigrationsStr = " vitess_migrations"
	WarningsStr         = " warnings"

	// DropKeyType strings
	PrimaryKeyTypeStr = "primary key"
	ForeignKeyTypeStr = "foreign key"
	NormalKeyTypeStr  = "key"

	// LockOptionType strings
	NoneTypeStr      = "none"
	SharedTypeStr    = "shared"
	DefaultTypeStr   = "default"
	ExclusiveTypeStr = "exclusive"

	// IntervalTypes strings
	DayStr               = "day"
	WeekStr              = "week"
	MonthStr             = "month"
	YearStr              = "year"
	DayHourStr           = "day_hour"
	DayMicrosecondStr    = "day_microsecond"
	DayMinuteStr         = "day_minute"
	DaySecondStr         = "day_second"
	HourStr              = "hour"
	HourMicrosecondStr   = "hour_microsecond"
	HourMinuteStr        = "hour_minute"
	HourSecondStr        = "hour_second"
	MicrosecondStr       = "microsecond"
	MinuteStr            = "minute"
	MinuteMicrosecondStr = "minute_microsecond"
	MinuteSecondStr      = "minute_second"
	QuarterStr           = "quarter"
	SecondStr            = "second"
	SecondMicrosecondStr = "second_microsecond"
	YearMonthStr         = "year_month"
)

// Constants for Enum type - AccessMode
const (
	ReadOnly AccessMode = iota
	ReadWrite
)

//Constants for Enum type - IsolationLevel
const (
	ReadUncommitted IsolationLevel = iota
	ReadCommitted
	RepeatableRead
	Serializable
)

// Constants for Enum Type - Insert.Action
const (
	InsertAct InsertAction = iota
	ReplaceAct
)

// Constants for Enum Type - DDL.Action
const (
	CreateDDLAction DDLAction = iota
	AlterDDLAction
	DropDDLAction
	RenameDDLAction
	TruncateDDLAction
	CreateVindexDDLAction
	DropVindexDDLAction
	AddVschemaTableDDLAction
	DropVschemaTableDDLAction
	AddColVindexDDLAction
	DropColVindexDDLAction
	AddSequenceDDLAction
	AddAutoIncDDLAction
	RevertDDLAction
)

// Constants for Enum Type - Scope
const (
	ImplicitScope Scope = iota
	SessionScope
	GlobalScope
	VitessMetadataScope
	VariableScope
	LocalScope
)

// Constants for Enum Type - Lock
const (
	NoLock Lock = iota
	ForUpdateLock
	ShareModeLock
)

// Constants for Enum Type - WhereType
const (
	WhereClause WhereType = iota
	HavingClause
)

// Constants for Enum Type - JoinType
const (
	NormalJoinType JoinType = iota
	StraightJoinType
	LeftJoinType
	RightJoinType
	NaturalJoinType
	NaturalLeftJoinType
	NaturalRightJoinType
)

// Constants for Enum Type - ComparisonExprOperator
const (
	EqualOp ComparisonExprOperator = iota
	LessThanOp
	GreaterThanOp
	LessEqualOp
	GreaterEqualOp
	NotEqualOp
	NullSafeEqualOp
	InOp
	NotInOp
	LikeOp
	NotLikeOp
	RegexpOp
	NotRegexpOp
)

// Constant for Enum Type - IsExprOperator
const (
	IsNullOp IsExprOperator = iota
	IsNotNullOp
	IsTrueOp
	IsNotTrueOp
	IsFalseOp
	IsNotFalseOp
)

// Constant for Enum Type - BinaryExprOperator
const (
	BitAndOp BinaryExprOperator = iota
	BitOrOp
	BitXorOp
	PlusOp
	MinusOp
	MultOp
	DivOp
	IntDivOp
	ModOp
	ShiftLeftOp
	ShiftRightOp
	JSONExtractOp
	JSONUnquoteExtractOp
)

// Constant for Enum Type - UnaryExprOperator
const (
	UPlusOp UnaryExprOperator = iota
	UMinusOp
	TildaOp
	BangOp
	BinaryOp
	NStringOp
)

// Constant for Enum Type - MatchExprOption
const (
	NoOption MatchExprOption = iota
	BooleanModeOpt
	NaturalLanguageModeOpt
	NaturalLanguageModeWithQueryExpansionOpt
	QueryExpansionOpt
)

// Constant for Enum Type - OrderDirection
const (
	AscOrder OrderDirection = iota
	DescOrder
)

// Constant for Enum Type - ConvertTypeOperator
const (
	NoOperator ConvertTypeOperator = iota
	CharacterSetOp
)

// Constant for Enum Type - IndexHintsType
const (
	UseOp IndexHintsType = iota
	IgnoreOp
	ForceOp
)

// Constant for Enum Type - PartitionSpecAction
const (
	ReorganizeAction PartitionSpecAction = iota
	AddAction
	DiscardAction
	DropAction
	ImportAction
	TruncateAction
	CoalesceAction
	ExchangeAction
	AnalyzeAction
	CheckAction
	OptimizeAction
	RebuildAction
	RepairAction
	RemoveAction
	UpgradeAction
)

// Constant for Enum Type - ExplainType
const (
	EmptyType ExplainType = iota
	TreeType
	JSONType
	VitessType
	TraditionalType
	AnalyzeType
)

// Constant for Enum Type - SelectIntoType
const (
	IntoOutfile SelectIntoType = iota
	IntoOutfileS3
	IntoDumpfile
)

// Constant for Enum Type - CollateAndCharsetType
const (
	CollateType CollateAndCharsetType = iota
	CharacterSetType
)

// LockType constants
const (
	UnknownLockType LockType = iota
	Read
	ReadLocal
	Write
	LowPriorityWrite
)

// ShowCommandType constants
const (
	UnknownCommandType ShowCommandType = iota
	Charset
	Collation
	Column
	CreateDb
	CreateE
	CreateF
	CreateProc
	CreateTbl
	CreateTr
	CreateV
	Database
	FunctionC
	Function
	GtidExecGlobal
	Index
	OpenTable
	Privilege
	ProcedureC
	Procedure
	StatusGlobal
	StatusSession
	Table
	TableStatus
	Trigger
	VariableGlobal
	VariableSession
	VGtidExecGlobal
	VitessMigrations
	Warnings
	Keyspace
)

// DropKeyType constants
const (
	PrimaryKeyType DropKeyType = iota
	ForeignKeyType
	NormalKeyType
)

// LockOptionType constants
const (
	DefaultType LockOptionType = iota
	NoneType
	SharedType
	ExclusiveType
)

// AlterMigrationType constants
const (
	RetryMigrationType AlterMigrationType = iota
	CompleteMigrationType
	CancelMigrationType
	CancelAllMigrationType
	CleanupMigrationType
)

// ColumnStorage constants
const (
	VirtualStorage ColumnStorage = iota
	StoredStorage
)

// IntervalTypes constants
const (
	IntervalYear IntervalTypes = iota
	IntervalQuarter
	IntervalMonth
	IntervalWeek
	IntervalDay
	IntervalHour
	IntervalMinute
	IntervalSecond
	IntervalMicrosecond
	IntervalYearMonth
	IntervalDayHour
	IntervalDayMinute
	IntervalDaySecond
	IntervalHourMinute
	IntervalHourSecond
	IntervalMinuteSecond
	IntervalDayMicrosecond
	IntervalHourMicrosecond
	IntervalMinuteMicrosecond
	IntervalSecondMicrosecond
)
