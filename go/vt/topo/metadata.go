/*
Copyright 2017 Google Inc.

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

package topo

import (
	"context"
	"path"
	"vitess.io/vitess/go/vt/sqlparser"

	"vitess.io/vitess/go/event"
	"vitess.io/vitess/go/vt/topo/events"
)

// UpsertMetadata sets the key/value in the metadata if it doesn't exist, otherwise it updates the content
func (ts *Server) UpsertMetadata(ctx context.Context, key string, val string) error {
	keyPath := path.Join(MetadataPath, key)

	_, _, err := ts.globalCell.Get(ctx, keyPath)
	status := "updated"

	if err != nil {
		if !IsErrType(err, NoNode) {
			return err
		}

		status = "created"
	}

	// nil version means that it will insert if keyPath does not exist
	if _, err := ts.globalCell.Update(ctx, keyPath, []byte(val), nil); err != nil {
		return err
	}

	dispatchEvent(keyPath, status)
	return nil
}

// GetMetadata retrieves all metadata value that matches the given key regular expression. If empty all values are returned.
func (ts *Server) GetMetadata(ctx context.Context, keyFilter string) (map[string]string, error) {
	keys, err := ts.globalCell.ListDir(ctx, MetadataPath, false)
	if err != nil {
		return nil, err
	}

	re := sqlparser.LikeToRegexp(keyFilter)

	result := make(map[string]string)
	for _, k := range keys {
		if !re.MatchString(k.Name) {
			continue
		}

		val, err := ts.getMetadata(ctx, k.Name)
		if err != nil {
			return nil, err
		}
		result[k.Name] = val
	}

	return result, nil
}

func (ts *Server) getMetadata(ctx context.Context, key string) (string, error) {
	keyPath := path.Join(MetadataPath, key)
	contents, _, err := ts.globalCell.Get(ctx, keyPath)
	if err != nil {
		return "", err
	}

	return string(contents), nil
}

func dispatchEvent(key string, status string) {
	event.Dispatch(&events.MetadataChange{
		Key:    key,
		Status: status,
	})
}
