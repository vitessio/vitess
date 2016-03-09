package splitquery

import (
	"fmt"
	"math/big"
	"strconv"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/sqltypes"
	querypb "github.com/youtube/vitess/go/vt/proto/query"
	"github.com/youtube/vitess/go/vt/sqlparser"
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
// It requies a schema parameter since it needs to know the type of the split-column
// given. It requires an SQLExecuter since it needs to execute a query to figure out the
// minimum and maximum elements in the table.
func NewEqualSplitsAlgorithm(
	splitParams *SplitParams,
	sqlExecuter SQLExecuter) (*EqualSplitsAlgorithm, error) {
	assertEqual(len(splitParams.splitColumns), len(splitParams.splitColumnTypes))
	if len(splitParams.splitColumns) != 1 {
		return nil, fmt.Errorf("using the EQUAL_SPLITS algorithm in SplitQuery requires having"+
			" exactly one split-column. Got split-columns: %v", splitParams.splitColumns)
	}
	if !sqltypes.IsFloat(splitParams.splitColumnTypes[0]) &&
		!sqltypes.IsIntegral(splitParams.splitColumnTypes[0]) {
		return nil, fmt.Errorf("using the EQUAL_SPLITS algorithm in SplitQuery requires having"+
			" a numeric (integral or float) split-column. Got type: %v", splitParams.splitColumnTypes[0])
	}
	if splitParams.splitCount <= 0 {
		return nil, fmt.Errorf("using the EQUAL_SPLITS algorithm in SplitQuery requires a positive"+
			" splitParams.splitCount. Got: %v", splitParams.splitCount)
	}
	result := &EqualSplitsAlgorithm{
		splitParams: splitParams,
		sqlExecuter: sqlExecuter,

		minMaxQuery: buildMinMaxQuery(splitParams),
	}
	return result, nil
}

func (algorithm *EqualSplitsAlgorithm) generateBoundaries() ([]tuple, error) {
	// generateBoundaries should work for a split_column whose type is integral
	// (both signed and unsigned) as well as for floating point values.
	// We perform the calculation of the boundaries using precise big.Rat arithmetic and only
	// truncate the result in the end if necessary.
	// Using float64 arithemetic does not have enough precision: for example, if min=math.MaxUint64
	// and max=math.MaxUint64-1000 then float64(min)==float64(max).
	// Using integer arithmetic for the case where the split_column is integral (i.e., rounding
	// (max-min)/split_count to an integer) may cause very dissimilar interval lengths or a
	// large deviation between split_count and the number of query-parts actually returned (e.g.
	// min=0, max=9.5*10^6, and split_count=10^6).
	// Note(erez): We can probably get away with using big.Float with ~64 bits of precision. This
	// will likely be more efficient.
	minValue, maxValue, err := algorithm.executeMinMaxQuery()
	if err != nil {
		return nil, err
	}
	// If the table is empty minValue and maxValue will be NULL.
	if (minValue.IsNull() && !maxValue.IsNull()) ||
		!minValue.IsNull() && maxValue.IsNull() {
		panic(fmt.Sprintf("minValue and maxValue must both be NULL or both be non-NULL."+
			" minValue: %v, maxValue: %v, splitParams.sql: %v",
			minValue, maxValue, algorithm.splitParams.sql))
	}
	if minValue.IsNull() {
		log.Infof("Splitting an empty table. splitParams.sql: %v. Query will not be split.",
			algorithm.splitParams.sql)
		return []tuple{}, nil
	}
	min := valueToBigRat(minValue)
	max := valueToBigRat(maxValue)
	minCmpMax := min.Cmp(max)
	if minCmpMax > 0 {
		panic(fmt.Sprintf("max(splitColumn) < min(splitColumn): max:%v, min:%v", max, min))
	}
	if minCmpMax == 0 {
		log.Infof("max(%v)=min(%v)=%v. splitParams.sql: %v. Query will not be split.",
			algorithm.splitParams.splitColumns[0],
			algorithm.splitParams.splitColumns[0],
			min,
			algorithm.splitParams.sql)
		return []tuple{}, nil
	}
	// subIntervalSize = (max - min) / algorithm.splitParams.splitCount
	subIntervalSize := newBigRat()
	subIntervalSize.Sub(max, min)
	subIntervalSize.Quo(subIntervalSize, newBigRat().SetInt64(algorithm.splitParams.splitCount))
	result := []tuple{}
	for i := int64(1); i < algorithm.splitParams.splitCount; i++ {
		// boundary = min + i*subIntervalSize
		boundary := newBigRat()
		boundary.Mul(newBigRat().SetInt64(i), subIntervalSize)
		boundary.Add(boundary, min)
		boundaryValue := bigRatToValue(boundary, algorithm.splitParams.splitColumnTypes[0])
		result = append(result, tuple{boundaryValue})
	}
	return result, nil
}

func (algorithm *EqualSplitsAlgorithm) executeMinMaxQuery() (
	minValue, maxValue sqltypes.Value, err error) {
	sqlResults, err := algorithm.sqlExecuter.SQLExecute(
		algorithm.minMaxQuery, nil /* Bind Variables */)
	if err != nil {
		return sqltypes.Value{}, sqltypes.Value{}, err
	}
	if len(sqlResults.Rows) != 1 {
		panic(fmt.Sprintf("MinMaxQuery should return exactly 1 row from query. MinMaxQuery: %v"+
			" Results: %v", algorithm.minMaxQuery, sqlResults))
	}
	if len(sqlResults.Rows[0]) != 2 {
		panic(fmt.Sprintf("MinMaxQuery should return exactly 2 columns. MinMaxQuery: %v, Results:%v",
			algorithm.minMaxQuery, sqlResults))
	}
	return sqlResults.Rows[0][0], sqlResults.Rows[0][1], nil
}

// buildMinMaxQuery returns the query to execute to get the minimum and maximum of the splitColumn.
// The query returned is:
//   SELECT MIN(<splitColumn>), MAX(<splitColumn>) FROM <table>;
// where <table> is the table referenced in the original query (held in splitParams.sql).
func buildMinMaxQuery(splitParams *SplitParams) string {
	// The SplitParams constructor should have already checked that the FROM clause of the query
	// is a simple table expression, so this type assertion should succeed.
	tableName := sqlparser.GetTableName(
		splitParams.selectAST.From[0].(*sqlparser.AliasedTableExpr).Expr)
	if tableName == "" {
		panic(fmt.Sprintf("Can't get tableName from query %v", splitParams.sql))
	}
	return fmt.Sprintf("select min(%v), max(%v) from %v",
		splitParams.splitColumns[0],
		splitParams.splitColumns[0],
		tableName)
}

// bigRatToValue converts 'number' to an SQL value with SQL type: valueType.
// If valueType is integral it truncates 'number' to the integer part according to the
// semantics of the big.Rat.Int method.
func bigRatToValue(number *big.Rat, valueType querypb.Type) sqltypes.Value {
	var numberAsBytes []byte
	switch {
	case sqltypes.IsIntegral(valueType):
		truncatedNumber := *number.Num()
		truncatedNumber.Quo(&truncatedNumber, number.Denom())
		numberAsBytes = truncatedNumber.Append([]byte{}, 10)
	case sqltypes.IsFloat(valueType):
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

func valueToBigRat(value sqltypes.Value) *big.Rat {
	switch {
	case value.IsUnsigned():
		nativeValue, err := value.ParseUint64()
		if err != nil {
			panic(fmt.Sprintf("can't parse numeric value: %v", value))
		}
		return uint64ToBigRat(nativeValue)
	case value.IsSigned():
		nativeValue, err := value.ParseInt64()
		if err != nil {
			panic(fmt.Sprintf("can't parse numeric value: %v", value))
		}
		return int64ToBigRat(nativeValue)
	case value.IsFloat():
		nativeValue, err := value.ParseFloat64()
		if err != nil {
			panic(fmt.Sprintf("can't parse numeric value: %v", value))
		}
		return float64ToBigRat(nativeValue)
	default:
		panic(fmt.Sprintf("got value with a non numeric type: %v", value))
	}
}

func uint64ToBigRat(value uint64) *big.Rat {
	result := newBigRat()
	result.SetInt(big.NewInt(0).SetUint64(value))
	return result
}

func int64ToBigRat(value int64) *big.Rat {
	result := newBigRat()
	result.SetInt(big.NewInt(0).SetInt64(value))
	return result
}

func float64ToBigRat(value float64) *big.Rat {
	result := newBigRat()
	result.SetFloat64(value)
	return result
}

func newBigRat() *big.Rat {
	var result big.Rat
	return &result
}
