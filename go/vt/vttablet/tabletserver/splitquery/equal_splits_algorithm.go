package splitquery

import (
	"fmt"
	"math/big"
	"strconv"

	log "github.com/golang/glog"

	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/sqlparser"
	"github.com/youtube/vitess/go/vt/vterrors"
	"github.com/youtube/vitess/go/vt/vttablet/tabletserver/schema"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
	vtrpcpb "github.com/youtube/vitess/go/vt/proto/vtrpc"
)

// EqualSplitsAlgorithm implements the SplitAlgorithmInterface and represents the equal-splits
// algorithm for generating the boundary tuples. If this algorithm is used then
// SplitParams.split_columns must contain only one split_column. Additionally, the split_column
// must have numeric type (integral or floating point).
//
// The algorithm works by issuing a query to the database to find the minimum and maximum
// elements of the split column in the table referenced by the given SQL query. Denote these
// by min and max, respecitvely. The algorithm then "splits" the interval [min, max] into
// SplitParams.split_count sub-intervals of equal length:
// [a_1, a_2], [a_2, a_3],..., [a_{split_count}, a_{split_count+1}],
// where min=a_1 < a_2 < a_3 < ... < a_split_count < a_{split_count+1}=max.
// The boundary points returned by this algorithm are then: a_2, a_3, ..., a_{split_count}
// (an empty list of boundary points is returned if split_count <= 1). If the type of the
// split column is integral, the boundary points are truncated to the integer part.
type EqualSplitsAlgorithm struct {
	splitParams *SplitParams
	sqlExecuter SQLExecuter

	minMaxQuery string
}

// NewEqualSplitsAlgorithm constructs a new equal splits algorithm.
// It requires an SQLExecuter since it needs to execute a query to figure out the
// minimum and maximum elements in the table.
func NewEqualSplitsAlgorithm(splitParams *SplitParams, sqlExecuter SQLExecuter) (
	*EqualSplitsAlgorithm, error) {

	if len(splitParams.splitColumns) == 0 {
		panic(fmt.Sprintf("len(splitParams.splitColumns) == 0." +
			" SplitParams should have defaulted the split columns to the primary key columns."))
	}
	// This algorithm only uses the first splitColumn.
	// Note that we do not force the user to specify only one split column, since a common
	// use-case is not to specify split columns at all, which will make them default to the table
	// primary key columns, and there can be more than one primary key column for a table.
	if !sqltypes.IsFloat(splitParams.splitColumns[0].Type) &&
		!sqltypes.IsIntegral(splitParams.splitColumns[0].Type) {
		return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT,
			"using the EQUAL_SPLITS algorithm in SplitQuery requires having"+
				" a numeric (integral or float) split-column. Got type: %v", splitParams.splitColumns[0])
	}
	if splitParams.splitCount <= 0 {
		return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT,
			"using the EQUAL_SPLITS algorithm in SplitQuery requires a positive"+
				" splitParams.splitCount. Got: %v", splitParams.splitCount)
	}
	result := &EqualSplitsAlgorithm{
		splitParams: splitParams,
		sqlExecuter: sqlExecuter,

		minMaxQuery: buildMinMaxQuery(splitParams),
	}
	return result, nil
}

// getSplitColumns is part of the SplitAlgorithmInterface interface
func (a *EqualSplitsAlgorithm) getSplitColumns() []*schema.TableColumn {
	return a.splitParams.splitColumns[0:1]
}

func (a *EqualSplitsAlgorithm) generateBoundaries() ([]tuple, error) {
	// generateBoundaries should work for a split_column whose type is integral
	// (both signed and unsigned) as well as for floating point values.
	// We perform the calculation of the boundaries using precise big.Rat arithmetic and only
	// truncate the result in the end if necessary.
	// We do this since using float64 arithmetic does not have enough precision:
	// for example, if max=math.MaxUint64 and min=math.MaxUint64-1000 then float64(min)==float64(max).
	// On the other hand, using integer arithmetic for the case where the split_column is integral
	// (i.e., rounding (max-min)/split_count to an integer) may cause very dissimilar interval
	// lengths or a large deviation between split_count and the number of query-parts actually
	// returned (consider min=0, max=9.5*10^6, and split_count=10^6).
	// Note(erez): We can probably get away with using big.Float with ~64 bits of precision which
	// will likely be more efficient. However, we defer optimizing this code until we see if this
	// is a bottle-neck.
	minValue, maxValue, err := a.executeMinMaxQuery()
	if err != nil {
		return nil, err
	}
	// If the table is empty, minValue and maxValue will be NULL.
	if (minValue.IsNull() && !maxValue.IsNull()) ||
		!minValue.IsNull() && maxValue.IsNull() {
		panic(fmt.Sprintf("minValue and maxValue must both be NULL or both be non-NULL."+
			" minValue: %v, maxValue: %v, splitParams.sql: %v",
			minValue, maxValue, a.splitParams.sql))
	}
	if minValue.IsNull() {
		log.Infof("Splitting an empty table. splitParams.sql: %v. Query will not be split.",
			a.splitParams.sql)
		return []tuple{}, nil
	}
	min, err := valueToBigRat(minValue, a.splitParams.splitColumns[0].Type)
	if err != nil {
		panic(fmt.Sprintf("Failed to convert min to a big.Rat: %v, min: %+v", err, min))
	}
	max, err := valueToBigRat(maxValue, a.splitParams.splitColumns[0].Type)
	if err != nil {
		panic(fmt.Sprintf("Failed to convert max to a big.Rat: %v, max: %+v", err, max))
	}
	minCmpMax := min.Cmp(max)
	if minCmpMax > 0 {
		panic(fmt.Sprintf("max(splitColumn) < min(splitColumn): max:%v, min:%v", max, min))
	}
	if minCmpMax == 0 {
		log.Infof("max(%v)=min(%v)=%v. splitParams.sql: %v. Query will not be split.",
			a.splitParams.splitColumns[0].Name,
			a.splitParams.splitColumns[0].Name,
			min,
			a.splitParams.sql)
		return []tuple{}, nil
	}

	// subIntervalSize = (max - min) / splitCount
	maxMinDiff := new(big.Rat)
	maxMinDiff.Sub(max, min)
	subIntervalSize := new(big.Rat)
	subIntervalSize.Quo(maxMinDiff, new(big.Rat).SetInt64(a.splitParams.splitCount))
	// If the split-column type is integral then it's wasteful to have a sub-intervale-size smaller
	// than 1, as it'll result with some query-parts being trivially empty. We set the
	// sub-interval size to 1 in this case.
	one := new(big.Rat).SetInt64(1)
	if sqltypes.IsIntegral(a.splitParams.splitColumns[0].Type) &&
		subIntervalSize.Cmp(one) < 0 {
		subIntervalSize = one
	}
	boundary := new(big.Rat).Add(min, subIntervalSize)
	result := []tuple{}
	for ; boundary.Cmp(max) < 0; boundary.Add(boundary, subIntervalSize) {
		boundaryValue := bigRatToValue(boundary, a.splitParams.splitColumns[0].Type)
		result = append(result, tuple{boundaryValue})
	}
	return result, nil
}

func (a *EqualSplitsAlgorithm) executeMinMaxQuery() (minValue, maxValue sqltypes.Value, err error) {
	sqlResults, err := a.sqlExecuter.SQLExecute(a.minMaxQuery, nil /* Bind Variables */)
	if err != nil {
		return sqltypes.Value{}, sqltypes.Value{}, err
	}
	if len(sqlResults.Rows) != 1 {
		panic(fmt.Sprintf("MinMaxQuery should return exactly 1 row from query. MinMaxQuery: %v"+
			" Results: %v", a.minMaxQuery, sqlResults))
	}
	if len(sqlResults.Rows[0]) != 2 {
		panic(fmt.Sprintf("MinMaxQuery should return exactly 2 columns. MinMaxQuery: %v, Results:%v",
			a.minMaxQuery, sqlResults))
	}
	return sqlResults.Rows[0][0], sqlResults.Rows[0][1], nil
}

// buildMinMaxQuery returns the query to execute to get the minimum and maximum of the splitColumn.
// The query returned is:
//   SELECT MIN(<splitColumn>), MAX(<splitColumn>) FROM <table>;
// where <table> is the table referenced in the original query (held in splitParams.sql).
func buildMinMaxQuery(splitParams *SplitParams) string {
	// The SplitParams constructor should have already checked that the FROM clause of the query
	// is a simple table expression, so this type-assertion should succeed.
	tableName := sqlparser.GetTableName(
		splitParams.selectAST.From[0].(*sqlparser.AliasedTableExpr).Expr)
	if tableName.IsEmpty() {
		panic(fmt.Sprintf("Can't get tableName from query %v", splitParams.sql))
	}
	return fmt.Sprintf("select min(%v), max(%v) from %v",
		splitParams.splitColumns[0].Name,
		splitParams.splitColumns[0].Name,
		tableName)
}

// bigRatToValue converts 'number' to an SQL value with SQL type: valueType.
// If valueType is integral it truncates 'number' to the integer part according to the
// semantics of the big.Rat.Int method.
func bigRatToValue(number *big.Rat, valueType querypb.Type) sqltypes.Value {
	var numberAsBytes []byte
	switch {
	case sqltypes.IsIntegral(valueType):
		// 'number.Num()' returns a reference to the numerator of 'number'.
		// We copy it here to avoid changing 'number'.
		truncatedNumber := new(big.Int).Set(number.Num())
		truncatedNumber.Quo(truncatedNumber, number.Denom())
		numberAsBytes = bigIntToSliceOfBytes(truncatedNumber)
	case sqltypes.IsFloat(valueType):
		// Truncate to the closest 'float'.
		// There's not much we can do if there isn't an exact representation.
		numberAsFloat64, _ := number.Float64()
		numberAsBytes = strconv.AppendFloat([]byte{}, numberAsFloat64, 'f', -1, 64)
	default:
		panic(fmt.Sprintf("Unsupported type: %v", valueType))
	}
	result, err := sqltypes.ValueFromBytes(valueType, numberAsBytes)
	if err != nil {
		panic(fmt.Sprintf("sqltypes.ValueFromBytes failed with: %v", err))
	}
	return result
}

// Converts a big.Int into a slice of bytes.
func bigIntToSliceOfBytes(bigInt *big.Int) []byte {
	// Go1.6 introduced the method bigInt.Append() which makes this conversion
	// a lot easier.
	// TODO(erez): Use bigInt.Append() once we switch to GO-1.6.
	result := strconv.AppendQuoteToASCII([]byte{}, bigInt.String())
	// AppendQuoteToASCII adds a double-quoted string. We need to remove them.
	return result[1 : len(result)-1]
}

// valueToBigRat converts a numeric 'value' regarded as having type 'valueType' into a
// big.Rat object.
// Note:
// We use an explicit valueType rather than depend on the type stored in 'value' to force
// the type of MAX(column) or MIN(column) to correspond to the type of 'column'.
// (We've had issues where the type of MAX(column) returned by Vitess was signed even if the
// type of column was unsigned).
func valueToBigRat(value sqltypes.Value, valueType querypb.Type) (*big.Rat, error) {
	switch {
	case sqltypes.IsUnsigned(valueType):
		nativeValue, err := value.ParseUint64()
		if err != nil {
			return nil, err
		}
		return uint64ToBigRat(nativeValue), nil
	case sqltypes.IsSigned(valueType):
		nativeValue, err := value.ParseInt64()
		if err != nil {
			return nil, err
		}
		return int64ToBigRat(nativeValue), nil
	case sqltypes.IsFloat(valueType):
		nativeValue, err := value.ParseFloat64()
		if err != nil {
			return nil, err
		}
		return float64ToBigRat(nativeValue), nil
	default:
		panic(fmt.Sprintf("got value with a non numeric type: %v", value))
	}
}

func int64ToBigRat(value int64) *big.Rat {
	return new(big.Rat).SetInt64(value)
}

func uint64ToBigRat(value uint64) *big.Rat {
	// big.Rat does not have a 'setUint64()' so we have to use an intermediate 'big.Int'.
	return new(big.Rat).SetInt(big.NewInt(0).SetUint64(value))
}

func float64ToBigRat(value float64) *big.Rat {
	return new(big.Rat).SetFloat64(value)
}
