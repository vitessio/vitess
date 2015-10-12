// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package framework

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// StreamQuery contains the streaming query info.
type StreamQuery struct {
	Query             string
	ContextHTML       string
	Start             time.Time
	Duration          int64
	ConnID            int
	State             string
	ShowTerminateLink bool
}

// StreamQueryz returns the contents of /streamqueryz?format=json.
// as a []StreamQuery. The function returns an empty list on error.
func StreamQueryz() []StreamQuery {
	var out []StreamQuery
	response, err := http.Get(fmt.Sprintf("%s/streamqueryz?format=json", ServerAddress))
	if err != nil {
		return out
	}
	defer response.Body.Close()
	_ = json.NewDecoder(response.Body).Decode(&out)
	return out
}

// StreamTerminate terminates the specified streaming query.
func StreamTerminate(connID int) error {
	response, err := http.Get(fmt.Sprintf("%s/streamqueryz/terminate?format=json&connID=%d", ServerAddress, connID))
	if err != nil {
		return err
	}
	response.Body.Close()
	return nil
}
