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

	// DDL strings.
	CreateStr           = "create"
	AlterStr            = "alter"
	DeallocateStr       = "deallocate"
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

	// ALTER TABLE ALGORITHM string.
	DefaultStr = "default"
	CopyStr    = "copy"
	InplaceStr = "inplace"
	InstantStr = "instant"

	// Partition and subpartition type strings
	HashTypeStr  = "hash"
	KeyTypeStr   = "key"
	RangeTypeStr = "range"
	ListTypeStr  = "list"

	// Partition value range type strings
	LessThanTypeStr = "less than"
	InTypeStr       = "in"

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

	// Index hints For types.
	JoinForStr    = "join"
	GroupByForStr = "group by"
	OrderByForStr = "order by"

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

	// DatabaseOption.Type
	CharacterSetStr = " character set"
	CollateStr      = " collate"
	EncryptionStr   = " encryption"

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

	// SetExpr.Expr transaction variables
	TransactionIsolationStr = "transaction_isolation"
	TransactionReadOnlyStr  = "transaction_read_only"

	// Transaction isolation levels
	ReadUncommittedStr = "read-uncommitted"
	ReadCommittedStr   = "read-committed"
	RepeatableReadStr  = "repeatable-read"
	SerializableStr    = "serializable"

	// Transaction access mode
	WithConsistentSnapshotStr = "with consistent snapshot"
	ReadWriteStr              = "read write"
	ReadOnlyStr               = "read only"

	// Explain formats
	EmptyStr       = ""
	TreeStr        = "tree"
	JSONStr        = "json"
	VitessStr      = "vitess"
	TraditionalStr = "traditional"
	AnalyzeStr     = "analyze"
	VTExplainStr   = "vtexplain"
	QueriesStr     = "queries"
	AllVExplainStr = "all"
	PlanStr        = "plan"

	// Lock Types
	ReadStr             = "read"
	ReadLocalStr        = "read local"
	WriteStr            = "write"
	LowPriorityWriteStr = "low_priority write"

	// ShowCommand Types
	CharsetStr                 = " charset"
	CollationStr               = " collation"
	ColumnStr                  = " columns"
	CreateDbStr                = " create database"
	CreateEStr                 = " create event"
	CreateFStr                 = " create function"
	CreateProcStr              = " create procedure"
	CreateTblStr               = " create table"
	CreateTrStr                = " create trigger"
	CreateVStr                 = " create view"
	DatabaseStr                = " databases"
	EnginesStr                 = " engines"
	FunctionCStr               = " function code"
	FunctionStr                = " function status"
	GtidExecGlobalStr          = " global gtid_executed"
	IndexStr                   = " indexes"
	OpenTableStr               = " open tables"
	PluginsStr                 = " plugins"
	PrivilegeStr               = " privileges"
	ProcedureCStr              = " procedure code"
	ProcedureStr               = " procedure status"
	StatusGlobalStr            = " global status"
	StatusSessionStr           = " status"
	TablesStr                  = " tables"
	TableStatusStr             = " table status"
	TriggerStr                 = " triggers"
	VariableGlobalStr          = " global variables"
	VariableSessionStr         = " variables"
	VGtidExecGlobalStr         = " global vgtid_executed"
	KeyspaceStr                = " keyspaces"
	VitessMigrationsStr        = " vitess_migrations"
	VitessReplicationStatusStr = " vitess_replication_status"
	VitessShardsStr            = " vitess_shards"
	VitessTabletsStr           = " vitess_tablets"
	VitessTargetStr            = " vitess_target"
	VitessVariablesStr         = " vitess_metadata variables"
	VschemaTablesStr           = " vschema tables"
	VschemaVindexesStr         = " vschema vindexes"
	WarningsStr                = " warnings"

	// DropKeyType strings
	PrimaryKeyTypeStr = "primary key"
	ForeignKeyTypeStr = "foreign key"
	NormalKeyTypeStr  = "key"
	CheckKeyTypeStr   = "check"

	// TrimType strings
	BothTrimStr     = "both"
	LeadingTrimStr  = "leading"
	TrailingTrimStr = "trailing"

	// FrameUnitType strings
	FrameRowsStr  = "rows"
	FrameRangeStr = "range"

	// FramePointType strings
	CurrentRowStr         = "current row"
	UnboundedPrecedingStr = "unbounded preceding"
	UnboundedFollowingStr = "unbounded following"
	ExprPrecedingStr      = "preceding"
	ExprFollowingStr      = "following"

	// ArgumentLessWindowExprType strings
	CumeDistExprStr    = "cume_dist"
	DenseRankExprStr   = "dense_rank"
	PercentRankExprStr = "percent_rank"
	RankExprStr        = "rank"
	RowNumberExprStr   = "row_number"

	// NullTreatmentType strings
	RespectNullsStr = "respect nulls"
	IgnoreNullsStr  = "ignore nulls"

	// FromFirstLastType strings
	FromFirstStr = "respect nulls"
	FromLastStr  = "ignore nulls"

	// FirstOrLastValueExprType strings
	FirstValueExprStr = "first_value"
	LastValueExprStr  = "last_value"

	// FirstOrLastValueExprType strings
	LagExprStr  = "lag"
	LeadExprStr = "lead"

	// TrimFuncType strings
	NormalTrimStr = "trim"
	LTrimStr      = "ltrim"
	RTrimStr      = "rtrim"

	// JSONAttributeType strings
	DepthAttributeStr  = "json_depth"
	ValidAttributeStr  = "json_valid"
	TypeAttributeStr   = "json_type"
	LengthAttributeStr = "json_length"

	// JSONValueModifierType strings
	JSONArrayAppendStr = "json_array_append"
	JSONArrayInsertStr = "json_array_insert"
	JSONInsertStr      = "json_insert"
	JSONReplaceStr     = "json_replace"
	JSONSetStr         = "json_set"

	// JSONValueMergeType strings
	JSONMergeStr         = "json_merge"
	JSONMergePatchStr    = "json_merge_patch"
	JSONMergePreserveStr = "json_merge_preserve"

	// LockingFuncType strings
	GetLockStr         = "get_lock"
	IsFreeLockStr      = "is_free_lock"
	IsUsedLockStr      = "is_used_lock"
	ReleaseAllLocksStr = "release_all_locks"
	ReleaseLockStr     = "release_lock"

	// PerformanceSchemaType strings
	FormatBytesStr       = "format_bytes"
	FormatPicoTimeStr    = "format_pico_time"
	PsCurrentThreadIDStr = "ps_current_thread_id"
	PsThreadIDStr        = "ps_thread_id"

	// GTIDType strings
	GTIDSubsetStr                   = "gtid_subset"
	GTIDSubtractStr                 = "gtid_subtract"
	WaitForExecutedGTIDSetStr       = "wait_for_executed_gtid_set"
	WaitUntilSQLThreadAfterGTIDSStr = "wait_until_sql_thread_after_gtids"

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

	// GeomeFromWktType strings
	GeometryFromTextStr           = "st_geometryfromtext"
	GeometryCollectionFromTextStr = "st_geometrycollectionfromtext"
	PointFromTextStr              = "st_pointfromtext"
	MultiPointFromTextStr         = "st_multipointfromtext"
	LineStringFromTextStr         = "st_linestringfromtext"
	MultiLinestringFromTextStr    = "st_multilinestringfromtext"
	PolygonFromTextStr            = "st_polygonfromtext"
	MultiPolygonFromTextStr       = "st_multipolygonfromtext"

	// GeomeFromWktType strings
	GeometryFromWKBStr           = "st_geometryfromwkb"
	GeometryCollectionFromWKBStr = "st_geometrycollectionfromwkb"
	PointFromWKBStr              = "st_pointfromwkb"
	MultiPointFromWKBStr         = "st_multipointfromwkb"
	LineStringFromWKBStr         = "st_linestringfromwkb"
	MultiLinestringFromWKBStr    = "st_multilinestringfromwkb"
	PolygonFromWKBStr            = "st_polygonfromwkb"
	MultiPolygonFromWKBStr       = "st_multipolygonfromwkb"

	// GeomFormatExpr strings
	TextFormatStr   = "st_astext"
	BinaryFormatStr = "st_asbinary"

	// GeomPropertyType strings
	IsSimpleStr     = "st_issimple"
	IsEmptyStr      = "st_isempty"
	EnvelopeStr     = "st_envelope"
	DimensionStr    = "st_dimension"
	GeometryTypeStr = "st_geometrytype"

	// PointPropertyType strings
	XCordinateStr = "st_x"
	YCordinateStr = "st_y"
	LatitudeStr   = "st_latitude"
	LongitudeStr  = "st_longitude"

	// LinestringPropertyType strings
	EndPointStr   = "st_endpoint"
	IsClosedStr   = "st_isclosed"
	LengthStr     = "st_length"
	NumPointsStr  = "st_numpoints"
	PointNStr     = "st_pointn"
	StartPointStr = "st_startpoint"

	// PolygonPropertyType strings
	AreaStr             = "st_area"
	CentroidStr         = "st_centroid"
	ExteriorRingStr     = "st_exteriorring"
	InteriorRingNStr    = "st_interiorringN"
	NumInteriorRingsStr = "st_numinteriorrings"

	// GeomCollPropType strings
	NumGeometriesStr = "st_numgeometries"
	GeometryNStr     = "st_geometryn"

	// GeomFromGeoHash strings
	LatitudeFromHashStr  = "st_latfromgeohash"
	LongitudeFromHashStr = "st_longfromgeohash"
	PointFromHashStr     = "st_pointfromgeohash"
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

// Constants for scope of variables
// See https://dev.mysql.com/doc/refman/8.0/en/set-variable.html
const (
	NoScope             Scope = iota
	SessionScope              // [SESSION | @@SESSION.| @@LOCAL. | @@] This is the default if no scope is given
	GlobalScope               // {GLOBAL | @@GLOBAL.} system_var_name
	VitessMetadataScope       // @@vitess_metadata.system_var_name
	PersistSysScope           // {PERSIST_ONLY | @@PERSIST_ONLY.} system_var_name
	PersistOnlySysScope       // {PERSIST_ONLY | @@PERSIST_ONLY.} system_var_name
	VariableScope             // @var_name   This is used for user defined variables.
	NextTxScope               // This is used for transaction related variables like transaction_isolation, transaction_read_write and set transaction statement.
)

// Constants for Enum Type - Lock
const (
	NoLock Lock = iota
	ForUpdateLock
	ShareModeLock
)

// Constants for Enum Type - TrimType
const (
	NoTrimType TrimType = iota
	BothTrimType
	LeadingTrimType
	TrailingTrimType
)

// Constants for Enum Type - TrimFuncType
const (
	NormalTrimType TrimFuncType = iota
	LTrimType
	RTrimType
)

// Constants for Enum Type - FrameUnitType
const (
	FrameRowsType FrameUnitType = iota
	FrameRangeType
)

// Constants for Enum Type - FramePointType
const (
	CurrentRowType FramePointType = iota
	UnboundedPrecedingType
	UnboundedFollowingType
	ExprPrecedingType
	ExprFollowingType
)

// Constants for Enum Type - ArgumentLessWindowExprType
const (
	CumeDistExprType ArgumentLessWindowExprType = iota
	DenseRankExprType
	PercentRankExprType
	RankExprType
	RowNumberExprType
)

// Constants for Enum Type - NullTreatmentType
const (
	RespectNullsType NullTreatmentType = iota
	IgnoreNullsType
)

// Constants for Enum Type - FromFirstLastType
const (
	FromFirstType FromFirstLastType = iota
	FromLastType
)

// Constants for Enum Type - FirstOrLastValueExprType
const (
	FirstValueExprType FirstOrLastValueExprType = iota
	LastValueExprType
)

// Constants for Enum Type - FirstOrLastValueExprType
const (
	LagExprType LagLeadExprType = iota
	LeadExprType
)

// Constants for Enum Type - JSONAttributeType
const (
	DepthAttributeType JSONAttributeType = iota
	ValidAttributeType
	TypeAttributeType
	LengthAttributeType
)

// Constants for Enum Type - JSONValueModifierType
const (
	JSONArrayAppendType JSONValueModifierType = iota
	JSONArrayInsertType
	JSONInsertType
	JSONReplaceType
	JSONSetType
)

// Constants for Enum Type - JSONValueMergeType
const (
	JSONMergeType JSONValueMergeType = iota
	JSONMergePatchType
	JSONMergePreserveType
)

// Constants for Enum Type - LockingFuncType
const (
	GetLock LockingFuncType = iota
	IsFreeLock
	IsUsedLock
	ReleaseAllLocks
	ReleaseLock
)

// Constants for Enum Type - PerformanceSchemaType
const (
	FormatBytesType PerformanceSchemaType = iota
	FormatPicoTimeType
	PsCurrentThreadIDType
	PsThreadIDType
)

// Constants for Enum Type - GTIDType
const (
	GTIDSubsetType GTIDType = iota
	GTIDSubtractType
	WaitForExecutedGTIDSetType
	WaitUntilSQLThreadAfterGTIDSType
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

// Constant for Enum Type - IndexHintType
const (
	UseOp IndexHintType = iota
	IgnoreOp
	ForceOp
)

// Constant for Enum Type - IndexHintForType
const (
	NoForType IndexHintForType = iota
	JoinForType
	GroupByForType
	OrderByForType
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

// Constant for Enum Type - PartitionByType
const (
	HashType PartitionByType = iota
	KeyType
	RangeType
	ListType
)

// Constant for Enum Type - PartitionValueRangeType
const (
	LessThanType PartitionValueRangeType = iota
	InType
)

// Constant for Enum Type - ExplainType
const (
	EmptyType ExplainType = iota
	TreeType
	JSONType
	VitessType
	VTExplainType
	TraditionalType
	AnalyzeType
)

// Constant for Enum Type - VExplainType
const (
	QueriesVExplainType VExplainType = iota
	PlanVExplainType
	AllVExplainType
)

// Constant for Enum Type - SelectIntoType
const (
	IntoOutfile SelectIntoType = iota
	IntoOutfileS3
	IntoDumpfile
)

// Constant for Enum Type - JtOnResponseType
const (
	ErrorJSONType JtOnResponseType = iota
	NullJSONType
	DefaultJSONType
)

// Constant for Enum Type - DatabaseOptionType
const (
	CollateType DatabaseOptionType = iota
	CharacterSetType
	EncryptionType
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
	Engines
	FunctionC
	Function
	GtidExecGlobal
	Index
	OpenTable
	Plugins
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
	VitessReplicationStatus
	VitessShards
	VitessTablets
	VitessTarget
	VitessVariables
	VschemaTables
	VschemaVindexes
	Warnings
	Keyspace
)

// DropKeyType constants
const (
	PrimaryKeyType DropKeyType = iota
	ForeignKeyType
	NormalKeyType
	CheckKeyType
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
	LaunchMigrationType
	LaunchAllMigrationType
	CompleteMigrationType
	CompleteAllMigrationType
	CancelMigrationType
	CancelAllMigrationType
	CleanupMigrationType
	ThrottleMigrationType
	ThrottleAllMigrationType
	UnthrottleMigrationType
	UnthrottleAllMigrationType
)

// ColumnStorage constants
const (
	VirtualStorage ColumnStorage = iota
	StoredStorage
)

// ColumnFormat constants
const (
	UnspecifiedFormat ColumnFormat = iota
	FixedFormat
	DynamicFormat
	DefaultFormat
)

// Constants for Enum Type - DateAddExprType
const (
	AdddateType DateAddExprType = iota
	DateAddType
	PlusIntervalLeftType
	PlusIntervalRightType
)

// Constants for Enum Type - DateAddExprType
const (
	SubdateType DateSubExprType = iota
	DateSubType
	MinusIntervalRightType
)

// IntervalTypes constants
const (
	IntervalUnknown IntervalTypes = iota
	IntervalYear
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

// Transaction access mode
const (
	WithConsistentSnapshot TxAccessMode = iota
	ReadWrite
	ReadOnly
)

// Enum Types of WKT functions
const (
	GeometryFromText GeomFromWktType = iota
	GeometryCollectionFromText
	PointFromText
	LineStringFromText
	PolygonFromText
	MultiPointFromText
	MultiPolygonFromText
	MultiLinestringFromText
)

// Enum Types of WKT functions
const (
	GeometryFromWKB GeomFromWkbType = iota
	GeometryCollectionFromWKB
	PointFromWKB
	LineStringFromWKB
	PolygonFromWKB
	MultiPointFromWKB
	MultiPolygonFromWKB
	MultiLinestringFromWKB
)

// Enum Types of spatial format functions
const (
	TextFormat GeomFormatType = iota
	BinaryFormat
)

// Enum Types of spatial property functions
const (
	IsSimple GeomPropertyType = iota
	IsEmpty
	Dimension
	GeometryType
	Envelope
)

// Enum Types of point property functions
const (
	XCordinate PointPropertyType = iota
	YCordinate
	Latitude
	Longitude
)

// Enum Types of linestring property functions
const (
	EndPoint LinestrPropType = iota
	IsClosed
	Length
	NumPoints
	PointN
	StartPoint
)

// Enum Types of linestring property functions
const (
	Area PolygonPropType = iota
	Centroid
	ExteriorRing
	InteriorRingN
	NumInteriorRings
)

// Enum Types of geom collection property functions
const (
	GeometryN GeomCollPropType = iota
	NumGeometries
)

// Enum Types of geom from geohash functions
const (
	LatitudeFromHash GeomFromHashType = iota
	LongitudeFromHash
	PointFromHash
)
