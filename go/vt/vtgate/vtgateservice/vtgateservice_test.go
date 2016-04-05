package vtgateservice

import (
	"testing"

	"github.com/golang/mock/gomock"
	"golang.org/x/net/context"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
	"github.com/youtube/vitess/go/vt/vtgate/vtgateservice/vtgateservice_testing"
)

var (
	keyspace                  = "It's a keyspace!"
	sql                       = "It's an SQL statement!"
	bindVariables             = map[string]interface{}{}
	splitCount          int64 = 123
	numRowsPerQueryPart int64 = 456
	algorithm                 = querypb.SplitQueryRequest_EQUAL_SPLITS
)

func TestCallCorrectSplitQueryCallV1NoSplitColumn(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	mockVTGateService := vtgateservice_testing.NewMockVTGateService(mockCtrl)

	mockVTGateService.EXPECT().SplitQuery(
		context.Background(),
		keyspace,
		sql,
		bindVariables,
		"",
		splitCount)
	CallCorrectSplitQuery(
		mockVTGateService,
		false, /* useSplitQueryV2 */
		context.Background(),
		keyspace,
		sql,
		bindVariables,
		[]string{}, /* SplitColumns */
		splitCount,
		numRowsPerQueryPart,
		algorithm)
}

func TestCallCorrectSplitQueryCallV1SplitColumnEmpty(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	mockVTGateService := vtgateservice_testing.NewMockVTGateService(mockCtrl)

	mockVTGateService.EXPECT().SplitQuery(
		context.Background(),
		keyspace,
		sql,
		bindVariables,
		"",
		splitCount)
	CallCorrectSplitQuery(
		mockVTGateService,
		false, /* useSplitQueryV2 */
		context.Background(),
		keyspace,
		sql,
		bindVariables,
		[]string{""}, /* SplitColumns */
		splitCount,
		numRowsPerQueryPart,
		algorithm)
}

func TestCallCorrectSplitQueryCallV1WithSplitColumn(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	mockVTGateService := vtgateservice_testing.NewMockVTGateService(mockCtrl)

	mockVTGateService.EXPECT().SplitQuery(
		context.Background(),
		keyspace,
		sql,
		bindVariables,
		"First Split Column",
		splitCount)
	CallCorrectSplitQuery(
		mockVTGateService,
		false, /* useSplitQueryV2 */
		context.Background(),
		keyspace,
		sql,
		bindVariables,
		[]string{"First Split Column"}, /* SplitColumns */
		splitCount,
		numRowsPerQueryPart,
		algorithm)
}

func TestCallCorrectSplitQueryCallV2(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	mockVTGateService := vtgateservice_testing.NewMockVTGateService(mockCtrl)
	splitColumns := []string{"col1", "col2"}
	mockVTGateService.EXPECT().SplitQueryV2(
		context.Background(),
		keyspace,
		sql,
		bindVariables,
		splitColumns,
		splitCount,
		numRowsPerQueryPart,
		algorithm)
	CallCorrectSplitQuery(
		mockVTGateService,
		true, /* useSplitQueryV2 */
		context.Background(),
		keyspace,
		sql,
		bindVariables,
		splitColumns,
		splitCount,
		numRowsPerQueryPart,
		algorithm)
}
