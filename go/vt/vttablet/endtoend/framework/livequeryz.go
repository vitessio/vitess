package framework

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// LiveQuery contains the streaming query info.
type LiveQuery struct {
	Type              string
	Query             string
	ContextHTML       string
	Start             time.Time
	Duration          int64
	ConnID            int
	State             string
	ShowTerminateLink bool
}

// OLAPQueryz returns the contents of /livequeryz?format=json.
// as a []LiveQuery. The function returns an empty list on error.
func LiveQueryz() []LiveQuery {
	var out []LiveQuery
	response, err := http.Get(fmt.Sprintf("%s/livequeryz?format=json", ServerAddress))
	if err != nil {
		return out
	}
	defer response.Body.Close()
	_ = json.NewDecoder(response.Body).Decode(&out)
	return out
}

// StreamTerminate terminates the specified streaming query.
func StreamTerminate(connID int) error {
	response, err := http.Get(fmt.Sprintf("%s/livequeryz/terminate?format=json&connID=%d", ServerAddress, connID))
	if err != nil {
		return err
	}
	response.Body.Close()
	return nil
}
