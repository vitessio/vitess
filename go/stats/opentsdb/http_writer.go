/*
Copyright 2023 The Vitess Authors.

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

package opentsdb

import (
	"bytes"
	"encoding/json"
	"net/http"
)

type httpWriter struct {
	client *http.Client
	uri    string
}

func newHTTPWriter(client *http.Client, uri string) *httpWriter {
	return &httpWriter{
		client: client,
		uri:    uri,
	}
}

func (hw *httpWriter) Write(data []*DataPoint) error {
	jsonb, err := json.Marshal(data)
	if err != nil {
		return err
	}

	resp, err := hw.client.Post(hw.uri, "application/json", bytes.NewReader(jsonb))
	if err != nil {
		return err
	}

	resp.Body.Close()

	return nil
}
