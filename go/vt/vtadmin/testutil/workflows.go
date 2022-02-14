package testutil

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	vtadminpb "vitess.io/vitess/go/vt/proto/vtadmin"
)

// AssertClusterWorkflowsEqual is a test helper for asserting that two
// ClusterWorkflows objects are equal.
func AssertClusterWorkflowsEqual(t *testing.T, expected *vtadminpb.ClusterWorkflows, actual *vtadminpb.ClusterWorkflows, msgAndArgs ...interface{}) {
	t.Helper()

	if expected == nil && actual == nil {
		return
	}

	require.NotNil(t, expected, msgAndArgs...)
	require.NotNil(t, actual, msgAndArgs...)

	if expected.Warnings != nil && actual.Warnings != nil {
		assert.Equal(t, len(expected.Warnings), len(actual.Warnings), msgAndArgs...)
	}

	assert.ElementsMatch(t, expected.Workflows, actual.Workflows, msgAndArgs...)
}

// AssertGetWorkflowsResponsesEqual is a test helper for asserting that two
// GetWorkflowsResponse objects are equal.
func AssertGetWorkflowsResponsesEqual(t *testing.T, expected *vtadminpb.GetWorkflowsResponse, actual *vtadminpb.GetWorkflowsResponse, msgAndArgs ...interface{}) {
	t.Helper()

	if expected == nil && actual == nil {
		return
	}

	require.NotNil(t, expected, msgAndArgs...)
	require.NotNil(t, actual, msgAndArgs...)

	keysLeft := make([]string, 0, len(expected.WorkflowsByCluster))
	keysRight := make([]string, 0, len(actual.WorkflowsByCluster))

	for k := range expected.WorkflowsByCluster {
		keysLeft = append(keysLeft, k)
	}

	for k := range actual.WorkflowsByCluster {
		keysRight = append(keysRight, k)
	}

	require.ElementsMatch(t, keysLeft, keysRight, msgAndArgs...)

	for _, k := range keysLeft {
		AssertClusterWorkflowsEqual(t, expected.WorkflowsByCluster[k], actual.WorkflowsByCluster[k], msgAndArgs...)
	}
}
