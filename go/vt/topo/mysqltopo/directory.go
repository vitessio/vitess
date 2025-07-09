/*
Copyright 2025 The Vitess Authors.

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

package mysqltopo

import (
	"context"
	"strings"

	"vitess.io/vitess/go/vt/topo"
)

// ListDir is part of the topo.Conn interface.
func (s *Server) ListDir(ctx context.Context, dirPath string, full bool) ([]topo.DirEntry, error) {
	// Convert to full path using the server's root
	fullDirPath := s.fullPath(dirPath)

	// Prepare the query to get all direct children
	// Ensure we don't have double slashes
	dirPrefix := fullDirPath
	if !strings.HasSuffix(dirPrefix, "/") {
		dirPrefix = dirPrefix + "/"
	}

	// Get files in this directory - only direct children
	fileResult, err := s.query(`
		SELECT SUBSTRING_INDEX(SUBSTRING(path, LENGTH(%s) + 1), '/', 1) AS name
		FROM topo_files
		WHERE path LIKE %s AND path NOT LIKE %s
		GROUP BY name
	`, dirPrefix, dirPrefix+"%", dirPrefix+"%/%")

	if err != nil {
		return nil, err
	}

	// Get subdirectories - only direct children
	dirResult, err := s.query(`
		SELECT SUBSTRING_INDEX(SUBSTRING(path, LENGTH(%s) + 1), '/', 1) AS name
		FROM topo_directories
		WHERE path LIKE %s AND path != %s AND path NOT LIKE %s
		GROUP BY name
	`, dirPrefix, dirPrefix+"%", fullDirPath, dirPrefix+"%/%")

	if err != nil {
		return nil, err
	}

	// Combine results
	entries := make(map[string]topo.DirEntry)

	// Process file results
	for _, row := range fileResult.Rows {
		name := row[0].ToString()

		// Skip if empty
		if name == "" {
			continue
		}

		entry := topo.DirEntry{
			Name: name,
		}
		if full {
			entry.Type = topo.TypeFile
			entry.Ephemeral = false
		}
		entries[name] = entry
	}

	// Process directory results
	for _, row := range dirResult.Rows {
		name := row[0].ToString()

		// Skip if empty
		if name == "" {
			continue
		}

		entry := topo.DirEntry{
			Name: name,
		}
		if full {
			entry.Type = topo.TypeDirectory
			entry.Ephemeral = false
		}
		entries[name] = entry
	}

	// Check if the directory exists when no entries found
	if len(entries) == 0 {
		// Check if the directory itself exists
		if fullDirPath != "/" {
			checkResult, err := s.queryRow("SELECT 1 FROM topo_directories WHERE path = %s", fullDirPath)
			if err != nil {
				return nil, err
			}
			if len(checkResult.Rows) == 0 {
				return nil, topo.NewError(topo.NoNode, dirPath)
			}
		}
	}

	// Convert map to slice
	result := make([]topo.DirEntry, 0, len(entries))
	for _, entry := range entries {
		result = append(result, entry)
	}

	// Sort the results
	topo.DirEntriesSortByName(result)
	return result, nil
}
