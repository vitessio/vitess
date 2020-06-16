package endtoend

import (
	"testing"

	"github.com/stretchr/testify/require"
	"vitess.io/vitess/go/vt/vttablet/endtoend/framework"
)

func TestReserve(t *testing.T) {
	client1 := framework.NewClient()
	client2 := framework.NewClient()

	//vstart := framework.DebugVars()

	query := "select connection_id()"

	qrc1_1, err := client1.ReserveExecute(query, nil, nil)
	require.NoError(t, err)
	defer client1.Release()
	qrc2_1, err := client2.ReserveExecute(query, nil, nil)
	require.NoError(t, err)
	defer client2.Release()
	require.NotEqual(t, qrc1_1.Rows, qrc2_1.Rows)

	qrc1_2, err := client1.Execute(query, nil)
	require.NoError(t, err)
	qrc2_2, err := client2.Execute(query, nil)
	require.NoError(t, err)
	require.Equal(t, qrc1_1.Rows, qrc1_2.Rows)
	require.Equal(t, qrc2_1.Rows, qrc2_2.Rows)

	//TODO: Add Counter tests.
	//expectedDiffs := []struct {
	//	tag  string
	//	diff int
	//}{{
	//	tag:  "Release/TotalCount",
	//	diff: 2,
	//}, {
	//	tag:  "Transactions/Histograms/commit/Count",
	//	diff: 2,
	//}, {
	//	tag:  "Queries/TotalCount",
	//	diff: 4,
	//}, {
	//	tag:  "Queries/Histograms/BEGIN/Count",
	//	diff: 0,
	//}, {
	//	tag:  "Queries/Histograms/COMMIT/Count",
	//	diff: 0,
	//}, {
	//	tag:  "Queries/Histograms/Insert/Count",
	//	diff: 1,
	//}, {
	//	tag:  "Queries/Histograms/DeleteLimit/Count",
	//	diff: 1,
	//}, {
	//	tag:  "Queries/Histograms/Select/Count",
	//	diff: 2,
	//}}
	//vend := framework.DebugVars()
	//for _, expected := range expectedDiffs {
	//	got := framework.FetchInt(vend, expected.tag)
	//	want := framework.FetchInt(vstart, expected.tag) + expected.diff
	//	// It's possible that other house-keeping transactions (like messaging)
	//	// can happen during this test. So, don't perform equality comparisons.
	//	if got < want {
	//		t.Errorf("%s: %d, must be at least %d", expected.tag, got, want)
	//	}
	//}
}
