package queryservice

import (
	"testing"

	"github.com/golang/mock/gomock"
	"golang.org/x/net/context"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
	"github.com/youtube/vitess/go/vt/tabletserver/queryservice/queryservice_testing"
)

var (
	target              querypb.Target
	sql                       = "It's an SQL statement!"
	bindVariables             = map[string]interface{}{}
	splitCount          int64 = 123
	numRowsPerQueryPart int64 = 456
	sessionID           int64 = 789
	algorithm                 = querypb.SplitQueryRequest_EQUAL_SPLITS
)

func TestCallCorrectSplitQueryCallV1NoSplitColumn(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	mockQueryService := queryservice_testing.NewMockQueryService(mockCtrl)

	mockQueryService.EXPECT().SplitQuery(
		context.Background(),
		&target,
		sql,
		bindVariables,
		"",
		splitCount,
		sessionID)
	CallCorrectSplitQuery(
		mockQueryService,
		false, /* useSplitQueryV2 */
		context.Background(),
		&target,
		sql,
		bindVariables,
		[]string{}, /* SplitColumns */
		splitCount,
		numRowsPerQueryPart,
		algorithm,
		sessionID)
}

func TestCallCorrectSplitQueryCallV1WithSplitColumn(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	mockQueryService := queryservice_testing.NewMockQueryService(mockCtrl)

	mockQueryService.EXPECT().SplitQuery(
		context.Background(),
		&target,
		sql,
		bindVariables,
		"First Split Column",
		splitCount,
		sessionID)
	CallCorrectSplitQuery(
		mockQueryService,
		false, /* useSplitQueryV2 */
		context.Background(),
		&target,
		sql,
		bindVariables,
		[]string{"First Split Column"}, /* SplitColumns */
		splitCount,
		numRowsPerQueryPart,
		algorithm,
		sessionID)
}

func TestCallCorrectSplitQueryCallV2(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	mockQueryService := queryservice_testing.NewMockQueryService(mockCtrl)
	splitColumns := []string{"col1", "col2"}
	mockQueryService.EXPECT().SplitQueryV2(
		context.Background(),
		&target,
		sql,
		bindVariables,
		splitColumns,
		splitCount,
		numRowsPerQueryPart,
		algorithm,
		sessionID)
	CallCorrectSplitQuery(
		mockQueryService,
		true, /* useSplitQueryV2 */
		context.Background(),
		&target,
		sql,
		bindVariables,
		splitColumns,
		splitCount,
		numRowsPerQueryPart,
		algorithm,
		sessionID)
}
