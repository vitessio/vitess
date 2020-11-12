/*
Copyright 2019 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

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
