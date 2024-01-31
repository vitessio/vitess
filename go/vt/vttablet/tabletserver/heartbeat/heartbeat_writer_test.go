package heartbeat

import (
	"testing"
)

// MockHeartbeatWriter is a mock implementation of the HeartbeatWriter interface for testing purposes.
type MockHeartbeatWriter struct {
	RequestedHeartbeats bool
}

// RequestHeartbeats implements the RequestHeartbeats method of the HeartbeatWriter interface.
func (m *MockHeartbeatWriter) RequestHeartbeats() {
	m.RequestedHeartbeats = true
}

func TestHeartbeatWriter_RequestHeartbeats(t *testing.T) {
	// Create an instance of the MockHeartbeatWriter
	mockWriter := &MockHeartbeatWriter{}

	// Call the RequestHeartbeats method
	mockWriter.RequestHeartbeats()

	// Verify that the RequestedHeartbeats flag is set to true
	if !mockWriter.RequestedHeartbeats {
		t.Error("Expected RequestedHeartbeats to be true, got false")
	}
}


