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
	"path"
	"sort"
	"strings"

	"vitess.io/vitess/go/vt/topo"
)

// ListDir is part of the topo.Conn interface.
func (s *Server) ListDir(ctx context.Context, dirPath string, full bool) ([]topo.DirEntry, error) {
	if err := s.checkClosed(); err != nil {
		return nil, convertError(err, dirPath)
	}

	fullDirPath := s.resolvePath(dirPath)
	rows, err := s.db.QueryContext(ctx, "SELECT path FROM topo_data WHERE path LIKE ?", matchDirectory(fullDirPath))
	if err != nil {
		return nil, convertError(err, dirPath)
	}
	defer rows.Close()

	// Collect all paths that are direct children of the directory
	childrenMap := make(map[string]bool)
	for rows.Next() {
		var filePath string
		if err := rows.Scan(&filePath); err != nil {
			return nil, convertError(err, dirPath)
		}

		relativePath := s.relativePath(filePath, fullDirPath)
		if relativePath == "" {
			continue
		}

		// Get the immediate child name (first path component)
		parts := strings.Split(relativePath, "/")
		if len(parts) > 0 && parts[0] != "" {
			childrenMap[parts[0]] = len(parts) == 1 // true if it's a file, false if it's a directory
		}
	}

	if err := rows.Err(); err != nil {
		return nil, convertError(err, dirPath)
	}

	lockRows, err := s.db.QueryContext(ctx, "SELECT path FROM topo_locks WHERE path LIKE ?", matchDirectory(fullDirPath))
	if err != nil {
		return nil, convertError(err, dirPath)
	}
	defer lockRows.Close()

	for lockRows.Next() {
		var lockPath string
		if err := lockRows.Scan(&lockPath); err != nil {
			return nil, convertError(err, dirPath)
		}

		relativePath := s.relativePath(lockPath, fullDirPath)
		if relativePath == "" {
			continue
		}

		// Get the immediate child name (first path component)
		parts := strings.Split(relativePath, "/")
		if len(parts) > 0 && parts[0] != "" {
			// Mark as ephemeral file
			childrenMap[parts[0]] = true
		}
	}

	if err := lockRows.Err(); err != nil {
		return nil, convertError(err, dirPath)
	}

	if len(childrenMap) == 0 {
		return nil, topo.NewError(topo.NoNode, dirPath)
	}

	// Convert to DirEntry slice
	var entries []topo.DirEntry
	for name, isFile := range childrenMap {
		entry := topo.DirEntry{
			Name: name,
		}

		if full {
			if isFile {
				entry.Type = topo.TypeFile
			} else {
				entry.Type = topo.TypeDirectory
			}

			// Check if this is an ephemeral entry (lock file)
			lockPath := path.Join(fullDirPath, name)
			var lockExists bool
			err := s.db.QueryRowContext(ctx, "SELECT 1 FROM topo_locks WHERE path = ?", lockPath).Scan(&lockExists)
			if err == nil {
				entry.Ephemeral = true
			}
		}

		entries = append(entries, entry)
	}

	// Sort entries by name
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Name < entries[j].Name
	})

	return entries, nil
}
