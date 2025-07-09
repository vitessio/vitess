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
	"fmt"
	"path"
	"strings"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/topo"
)

// Create is part of the topo.Conn interface.
func (s *Server) Create(ctx context.Context, filePath string, contents []byte) (topo.Version, error) {
	// Convert to full path using the server's root
	fullPath := s.fullPath(filePath)

	// Check if the file already exists
	result, err := s.queryRow("SELECT version FROM topo_files WHERE path = %s", fullPath)
	if err != nil {
		return nil, convertError(err, filePath)
	}
	if len(result.Rows) > 0 {
		// File exists
		return nil, topo.NewError(topo.NodeExists, filePath)
	}

	// Create parent directories if needed
	if err := s.createParentDirectories(ctx, fullPath); err != nil {
		return nil, convertError(err, filePath)
	}

	// Create the file
	result, err = s.exec("INSERT INTO topo_files (path, data) VALUES (%s, %s)", fullPath, encodeBinaryData(contents))
	if err != nil {
		return nil, convertError(err, filePath)
	}

	// Get the version (auto-increment ID)
	id := result.InsertID

	// Notify watchers
	if err := s.notifyWatchers(ctx, fullPath); err != nil {
		// Log error but don't fail the operation
		// The watch mechanism will eventually catch up
		log.Warningf("Failed to notify watchers for file %s: %v", fullPath, err)
	}

	return MySQLVersion(id), nil
}

// Update is part of the topo.Conn interface.
func (s *Server) Update(ctx context.Context, filePath string, contents []byte, version topo.Version) (topo.Version, error) {
	// Convert to full path using the server's root
	fullPath := s.fullPath(filePath)

	// Create parent directories if needed
	if err := s.createParentDirectories(ctx, fullPath); err != nil {
		return nil, convertError(err, filePath)
	}

	var result *sqltypes.Result
	var err error

	if version != nil {
		// Conditional update
		mysqlVersion, ok := version.(MySQLVersion)
		if !ok {
			return nil, fmt.Errorf("bad version type %T, expected MySQLVersion", version)
		}
		result, err = s.exec("UPDATE topo_files SET data = %s, version = version + 1 WHERE path = %s AND version = %s",
			encodeBinaryData(contents), fullPath, fmt.Sprintf("%d", int64(mysqlVersion)))
	} else {
		// Unconditional update or create
		result, err = s.exec("INSERT INTO topo_files (path, data) VALUES (%s, %s) ON DUPLICATE KEY UPDATE data = VALUES(data), version = version + 1",
			fullPath, encodeBinaryData(contents))
	}

	if err != nil {
		return nil, convertError(err, filePath)
	}

	// Check if the update was successful
	if result.RowsAffected == 0 && version != nil {
		// No rows were affected, which means the version didn't match
		return nil, topo.NewError(topo.BadVersion, filePath)
	}

	// Get the new version
	result, err = s.queryRow("SELECT version FROM topo_files WHERE path = %s", fullPath)
	if err != nil {
		return nil, convertError(err, filePath)
	}
	if len(result.Rows) == 0 {
		return nil, topo.NewError(topo.NoNode, filePath)
	}

	newVersion, err := result.Rows[0][0].ToInt64()
	if err != nil {
		return nil, err
	}

	// Notify watchers
	if err := s.notifyWatchers(ctx, fullPath); err != nil {
		// Log error but don't fail the operation
		log.Warningf("Failed to notify watchers for file %s: %v", fullPath, err)
	}

	return MySQLVersion(newVersion), nil
}

// Get is part of the topo.Conn interface.
func (s *Server) Get(ctx context.Context, filePath string) ([]byte, topo.Version, error) {
	// Convert to full path using the server's root
	fullPath := s.fullPath(filePath)

	result, err := s.queryRow("SELECT data, version FROM topo_files WHERE path = %s", fullPath)
	if err != nil {
		return nil, nil, convertError(err, filePath)
	}
	if len(result.Rows) == 0 {
		return nil, nil, topo.NewError(topo.NoNode, filePath)
	}

	data, err := decodeBinaryData(result.Rows[0][0])
	if err != nil {
		return nil, nil, err
	}
	version, err := result.Rows[0][1].ToInt64()
	if err != nil {
		return nil, nil, err
	}

	return data, MySQLVersion(version), nil
}

// GetVersion is part of the topo.Conn interface.
func (s *Server) GetVersion(ctx context.Context, filePath string, version int64) ([]byte, error) {
	// Convert to full path using the server's root
	fullPath := s.fullPath(filePath)

	result, err := s.queryRow("SELECT data FROM topo_files WHERE path = %s AND version = %s",
		fullPath, fmt.Sprintf("%d", version))
	if err != nil {
		return nil, convertError(err, filePath)
	}
	if len(result.Rows) == 0 {
		return nil, topo.NewError(topo.NoNode, filePath)
	}

	data, err := decodeBinaryData(result.Rows[0][0])
	if err != nil {
		return nil, err
	}
	return data, nil
}

// List is part of the topo.Conn interface.
func (s *Server) List(ctx context.Context, filePathPrefix string) ([]topo.KVInfo, error) {
	// Convert to full path using the server's root
	fullPathPrefix := s.fullPath(filePathPrefix)

	// Query for all files with the given prefix
	result, err := s.query("SELECT path, data, version FROM topo_files WHERE path LIKE %s", fullPathPrefix+"%")
	if err != nil {
		return nil, convertError(err, filePathPrefix)
	}

	var results []topo.KVInfo
	for _, row := range result.Rows {
		fullPath := row[0].ToString()
		// Convert back to relative path for the result
		relativePath := s.relativePath(fullPath)

		data, err := decodeBinaryData(row[1])
		if err != nil {
			return nil, err
		}
		version, err := row[2].ToInt64()
		if err != nil {
			return nil, err
		}

		results = append(results, topo.KVInfo{
			Key:     []byte(relativePath),
			Value:   data,
			Version: MySQLVersion(version),
		})
	}

	if len(results) == 0 {
		return nil, topo.NewError(topo.NoNode, filePathPrefix)
	}

	return results, nil
}

// Delete is part of the topo.Conn interface.
func (s *Server) Delete(ctx context.Context, filePath string, version topo.Version) error {
	// Convert to full path using the server's root
	fullPath := s.fullPath(filePath)

	var result *sqltypes.Result
	var err error

	if version != nil {
		// Conditional delete
		mysqlVersion, ok := version.(MySQLVersion)
		if !ok {
			return fmt.Errorf("bad version type %T, expected MySQLVersion", version)
		}
		result, err = s.exec("DELETE FROM topo_files WHERE path = %s AND version = %s",
			fullPath, fmt.Sprintf("%d", int64(mysqlVersion)))
	} else {
		// Unconditional delete
		result, err = s.exec("DELETE FROM topo_files WHERE path = %s", fullPath)
	}

	if err != nil {
		return convertError(err, filePath)
	}

	// Check if the delete was successful
	if result.RowsAffected == 0 {
		if version != nil {
			// No rows were affected, check if the file exists
			checkResult, err := s.queryRow("SELECT 1 FROM topo_files WHERE path = %s", fullPath)
			if err != nil {
				return convertError(err, filePath)
			}
			if len(checkResult.Rows) == 0 {
				return topo.NewError(topo.NoNode, filePath)
			}
			return topo.NewError(topo.BadVersion, filePath)
		}
		return topo.NewError(topo.NoNode, filePath)
	}

	// Clean up empty parent directories
	if err := s.cleanupEmptyDirectories(ctx, fullPath); err != nil {
		// Log error but don't fail the operation
		log.Warningf("Failed to cleanup empty directories for file %s: %v", fullPath, err)
	}

	// Notify watchers
	if err := s.notifyWatchers(ctx, fullPath); err != nil {
		// Log error but don't fail the operation
		log.Warningf("Failed to notify watchers for file %s: %v", fullPath, err)
	}

	return nil
}

// createParentDirectories creates all parent directories for a given file path.
// The filePath should already be the full path including the server's root.
func (s *Server) createParentDirectories(ctx context.Context, filePath string) error {
	// Get the directory path
	dirPath := path.Dir(filePath)
	if dirPath == "/" || dirPath == "." {
		return nil
	}

	// Split the path into components
	components := strings.Split(dirPath, "/")
	currentPath := ""

	// Create each directory component
	for _, component := range components {
		if component == "" {
			continue
		}

		if currentPath == "" {
			currentPath = "/" + component
		} else {
			currentPath = currentPath + "/" + component
		}

		// Insert the directory if it doesn't exist
		_, err := s.exec("INSERT IGNORE INTO topo_directories (path) VALUES (%s)", currentPath)
		if err != nil {
			return err
		}
	}

	return nil
}

// cleanupEmptyDirectories removes empty directories after a file is deleted.
// The filePath should already be the full path including the server's root.
func (s *Server) cleanupEmptyDirectories(ctx context.Context, filePath string) error {
	// Get the directory path
	dirPath := path.Dir(filePath)
	if dirPath == "/" || dirPath == "." {
		return nil
	}

	// Check if the directory is empty by looking for:
	// 1. Any files that are direct children of this directory
	// 2. Any subdirectories that are direct children of this directory
	result, err := s.queryRow(`
		SELECT COUNT(*) FROM (
			SELECT 1 FROM topo_files WHERE path LIKE %s AND path NOT LIKE %s
			UNION ALL
			SELECT 1 FROM topo_directories WHERE path LIKE %s AND path != %s AND path NOT LIKE %s
		) AS t
	`, dirPath+"/%", dirPath+"/%/%", dirPath+"/%", dirPath, dirPath+"/%/%")

	if err != nil {
		return err
	}
	if len(result.Rows) == 0 {
		return fmt.Errorf("no count result")
	}

	count, err := result.Rows[0][0].ToInt64()
	if err != nil {
		return err
	}

	if count == 0 {
		// Directory is empty, delete it
		_, err := s.exec("DELETE FROM topo_directories WHERE path = %s", dirPath)
		if err != nil {
			return err
		}

		// Recursively cleanup parent directories
		return s.cleanupEmptyDirectories(ctx, dirPath)
	}

	return nil
}

// notifyWatchers notifies all watchers of a change to a file.
func (s *Server) notifyWatchers(ctx context.Context, filePath string) error {
	// This is a placeholder for the actual implementation
	// In a real implementation, we would use MySQL replication to notify watchers
	return nil
}
