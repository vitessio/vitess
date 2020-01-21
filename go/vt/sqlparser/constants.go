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

const (
	// Select.Distinct
	DistinctStr      = "distinct "
	StraightJoinHint = "straight_join "

	// Select.Lock
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

	// Vindex DDL param to specify the owner of a vindex
	VindexOwnerStr = "owner"

	// Partition strings
	ReorganizeStr = "reorganize partition"

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
	EqualStr             = "="
	LessThanStr          = "<"
	GreaterThanStr       = ">"
	LessEqualStr         = "<="
	GreaterEqualStr      = ">="
	NotEqualStr          = "!="
	NullSafeEqualStr     = "<=>"
	InStr                = "in"
	NotInStr             = "not in"
	LikeStr              = "like"
	NotLikeStr           = "not like"
	RegexpStr            = "regexp"
	NotRegexpStr         = "not regexp"
	JSONExtractOp        = "->"
	JSONUnquoteExtractOp = "->>"

	// RangeCond.Operator
	BetweenStr    = "between"
	NotBetweenStr = "not between"

	// IsExpr.Operator
	IsNullStr     = "is null"
	IsNotNullStr  = "is not null"
	IsTrueStr     = "is true"
	IsNotTrueStr  = "is not true"
	IsFalseStr    = "is false"
	IsNotFalseStr = "is not false"

	// BinaryExpr.Operator
	BitAndStr     = "&"
	BitOrStr      = "|"
	BitXorStr     = "^"
	PlusStr       = "+"
	MinusStr      = "-"
	MultStr       = "*"
	DivStr        = "/"
	IntDivStr     = "div"
	ModStr        = "%"
	ShiftLeftStr  = "<<"
	ShiftRightStr = ">>"

	// UnaryExpr.Operator
	UPlusStr   = "+"
	UMinusStr  = "-"
	TildaStr   = "~"
	BangStr    = "!"
	BinaryStr  = "binary "
	UBinaryStr = "_binary "
	Utf8mb4Str = "_utf8mb4 "

	// this string is "character set" and this comment is required
	CharacterSetStr = " character set"
	CharsetStr      = "charset"

	// MatchExpr.Option
	BooleanModeStr                           = " in boolean mode"
	NaturalLanguageModeStr                   = " in natural language mode"
	NaturalLanguageModeWithQueryExpansionStr = " in natural language mode with query expansion"
	QueryExpansionStr                        = " with query expansion"

	// Order.Direction
	AscScr  = "asc"
	DescScr = "desc"

	// SetExpr.Expr, for SET TRANSACTION ... or START TRANSACTION
	// TransactionStr is the Name for a SET TRANSACTION statement
	TransactionStr = "transaction"

	IsolationLevelReadUncommitted = "isolation level read uncommitted"
	IsolationLevelReadCommitted   = "isolation level read committed"
	IsolationLevelRepeatableRead  = "isolation level repeatable read"
	IsolationLevelSerializable    = "isolation level serializable"

	TxReadOnly  = "read only"
	TxReadWrite = "read write"
)
