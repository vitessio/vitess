package splitquery

import (
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/youtube/vitess/go/vt/tabletserver/splitquery/mock_splitquery"
)

func TestStandardExecution(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	splitParams, err := NewSplitParamsWithNumRowsPerQueryPart(
		"SELECT * FROM table WHERE int_col > 5",
		nil, /* bindVariables */
		[]string{"user_id", "video_id"}, /* splitColumns */
		1000)
	if err != nil {
		t.Fatalf("NewSplitParamsWithNumRowsPerQueryPart failed with: %v", err)
	}
	mockSQLExecuter := mock_splitquery.NewMockSQLExecuter(mockCtrl)
	mockSQLExecuter.EXPECT().SQLExecute("bla", nil).Return(nil, nil)
	algorithm := NewFullScanAlgorithm(&splitParams, mockSQLExecuter)
	boundaries, err := algorithm.generateBoundaries()
	if err != nil {
		t.Fatalf("FullScanAlgorithm.generateBoundaries() failed with: %v", err)
	}

}
