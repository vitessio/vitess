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
	"database/sql"

	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/topo"
)

// beginTxWithReadCommitted starts a transaction with READ COMMITTED isolation level
// to avoid gap locking issues in MySQL.
func (s *Server) beginTxWithReadCommitted(ctx context.Context) (*sql.Tx, error) {
	return s.db.BeginTx(ctx, &sql.TxOptions{
		Isolation: sql.LevelReadCommitted,
	})
}

// Create is part of the topo.Conn interface.
func (s *Server) Create(ctx context.Context, filePath string, contents []byte) (topo.Version, error) {
	if err := s.checkClosed(); err != nil {
		return nil, convertError(err, filePath)
	}

	fullPath := s.resolvePath(filePath)

	// Use a transaction with READ COMMITTED isolation to avoid gap locking
	tx, err := s.beginTxWithReadCommitted(ctx)
	if err != nil {
		return nil, convertError(err, fullPath)
	}
	defer func() {
		if tx != nil {
			_ = tx.Rollback() // Rollback if not committed
		}
	}()

	// Check if the file already exists
	var exists bool
	err = tx.QueryRowContext(ctx, "SELECT 1 FROM topo_data WHERE path = ?", fullPath).Scan(&exists)
	if err != nil && err != sql.ErrNoRows {
		return nil, convertError(err, fullPath)
	}
	if exists {
		return nil, topo.NewError(topo.NodeExists, fullPath)
	}

	// Insert the new file with version 1
	_, err = tx.ExecContext(ctx, "INSERT INTO topo_data (path, data, version) VALUES (?, ?, 1)", fullPath, contents)
	if err != nil {
		return nil, convertError(err, fullPath)
	}

	// Commit the transaction
	if err := tx.Commit(); err != nil {
		return nil, convertError(err, fullPath)
	}
	tx = nil // Prevent rollback on defer
	return MySQLVersion(1), nil
}

// Update is part of the topo.Conn interface.
func (s *Server) Update(ctx context.Context, filePath string, contents []byte, version topo.Version) (topo.Version, error) {
	if err := s.checkClosed(); err != nil {
		return nil, convertError(err, filePath)
	}

	fullPath := s.resolvePath(filePath)

	// Use a transaction with READ COMMITTED isolation to avoid gap locking
	tx, err := s.beginTxWithReadCommitted(ctx)
	if err != nil {
		return nil, convertError(err, fullPath)
	}
	defer func() {
		if tx != nil {
			_ = tx.Rollback() // Rollback if not committed
		}
	}()

	if version != nil {
		// Conditional update - check version matches and increment it
		var currentVersion int64
		err = tx.QueryRowContext(ctx, "SELECT version FROM topo_data WHERE path = ?", fullPath).Scan(&currentVersion)
		if err == sql.ErrNoRows {
			return nil, topo.NewError(topo.NoNode, fullPath)
		}
		if err != nil {
			return nil, convertError(err, fullPath)
		}

		expectedVersion := int64(version.(MySQLVersion))
		if currentVersion != expectedVersion {
			log.Infof("Version mismatch for %s: current=%d, expected=%d", fullPath, currentVersion, expectedVersion)
			return nil, topo.NewError(topo.BadVersion, fullPath)
		}

		// Update with version increment
		newVersion := currentVersion + 1
		result, err := tx.ExecContext(ctx,
			"UPDATE topo_data SET data = ?, version = ? WHERE path = ? AND version = ?",
			contents, newVersion, fullPath, expectedVersion)
		if err != nil {
			return nil, convertError(err, fullPath)
		}

		rowsAffected, err := result.RowsAffected()
		if err != nil {
			return nil, convertError(err, fullPath)
		}
		if rowsAffected == 0 {
			return nil, topo.NewError(topo.BadVersion, fullPath)
		}

		// Commit the transaction
		if err := tx.Commit(); err != nil {
			return nil, convertError(err, fullPath)
		}
		tx = nil // Prevent rollback on defer
		return MySQLVersion(newVersion), nil
	}
	// Else, unconditional update (upsert)
	// First try to update existing record
	var currentVersion int64
	err = tx.QueryRowContext(ctx, "SELECT version FROM topo_data WHERE path = ?", fullPath).Scan(&currentVersion)

	if err == sql.ErrNoRows {
		// Record doesn't exist, insert it with version 1
		_, err = tx.ExecContext(ctx,
			"INSERT INTO topo_data (path, data, version) VALUES (?, ?, 1)",
			fullPath, contents)
		if err != nil {
			return nil, convertError(err, fullPath)
		}

		// Commit the transaction
		if err := tx.Commit(); err != nil {
			return nil, convertError(err, fullPath)
		}
		tx = nil // Prevent rollback on defer
		return MySQLVersion(1), nil
	} else if err != nil {
		return nil, convertError(err, fullPath)
	} else {
		// Record exists, update it with incremented version
		newVersion := currentVersion + 1
		_, err = tx.ExecContext(ctx,
			"UPDATE topo_data SET data = ?, version = ? WHERE path = ?",
			contents, newVersion, fullPath)
		if err != nil {
			return nil, convertError(err, fullPath)
		}

		// Commit the transaction
		if err := tx.Commit(); err != nil {
			return nil, convertError(err, fullPath)
		}
		tx = nil // Prevent rollback on defer
		return MySQLVersion(newVersion), nil
	}
}

// Get is part of the topo.Conn interface.
func (s *Server) Get(ctx context.Context, filePath string) ([]byte, topo.Version, error) {
	if err := s.checkClosed(); err != nil {
		return nil, nil, convertError(err, filePath)
	}

	fullPath := s.resolvePath(filePath)

	var data []byte
	var version int64
	err := s.db.QueryRowContext(ctx, "SELECT data, version FROM topo_data WHERE path = ?", fullPath).Scan(&data, &version)
	if err == sql.ErrNoRows {
		return nil, nil, topo.NewError(topo.NoNode, fullPath)
	}
	if err != nil {
		return nil, nil, convertError(err, fullPath)
	}

	return data, MySQLVersion(version), nil
}

// GetVersion is part of the topo.Conn interface.
func (s *Server) GetVersion(ctx context.Context, filePath string, version int64) ([]byte, error) {
	if err := s.checkClosed(); err != nil {
		return nil, convertError(err, filePath)
	}

	fullPath := s.resolvePath(filePath)

	// MySQL doesn't store historical versions by default, so we can only get the current version
	// if it matches the requested version
	var data []byte
	var currentVersion int64
	err := s.db.QueryRowContext(ctx, "SELECT data, version FROM topo_data WHERE path = ?", fullPath).Scan(&data, &currentVersion)
	if err == sql.ErrNoRows {
		return nil, topo.NewError(topo.NoNode, fullPath)
	}
	if err != nil {
		return nil, convertError(err, fullPath)
	}
	if currentVersion != version {
		return nil, topo.NewError(topo.NoNode, fullPath)
	}
	return data, nil
}

// List is part of the topo.Conn interface.
func (s *Server) List(ctx context.Context, filePathPrefix string) ([]topo.KVInfo, error) {
	if err := s.checkClosed(); err != nil {
		return nil, convertError(err, filePathPrefix)
	}

	fullPathPrefix := s.resolvePath(filePathPrefix)
	rows, err := s.db.QueryContext(ctx, "SELECT path, data, version FROM topo_data WHERE path LIKE ?", matchDirectory(fullPathPrefix))
	if err != nil {
		return nil, convertError(err, fullPathPrefix)
	}
	defer rows.Close()

	var results []topo.KVInfo
	for rows.Next() {
		var kvPath string
		var data []byte
		var version int64

		if err := rows.Scan(&kvPath, &data, &version); err != nil {
			return nil, convertError(err, fullPathPrefix)
		}

		results = append(results, topo.KVInfo{
			Key:     []byte(kvPath),
			Value:   data,
			Version: MySQLVersion(version),
		})
	}

	if err := rows.Err(); err != nil {
		return nil, convertError(err, fullPathPrefix)
	}

	if len(results) == 0 {
		return nil, topo.NewError(topo.NoNode, fullPathPrefix)
	}

	return results, nil
}

// Delete is part of the topo.Conn interface.
func (s *Server) Delete(ctx context.Context, filePath string, version topo.Version) error {
	if err := s.checkClosed(); err != nil {
		return convertError(err, filePath)
	}

	fullPath := s.resolvePath(filePath)

	// Use a transaction with READ COMMITTED isolation to avoid gap locking
	tx, err := s.beginTxWithReadCommitted(ctx)
	if err != nil {
		return convertError(err, fullPath)
	}
	defer func() {
		if tx != nil {
			_ = tx.Rollback() // Rollback if not committed
		}
	}()

	if version != nil {
		// Conditional delete - check version matches
		result, err := tx.ExecContext(ctx, "DELETE FROM topo_data WHERE path = ? AND version = ?", fullPath, int64(version.(MySQLVersion)))
		if err != nil {
			return convertError(err, fullPath)
		}

		rowsAffected, err := result.RowsAffected()
		if err != nil {
			return convertError(err, fullPath)
		}
		if rowsAffected == 0 {
			// Check if the file exists with a different version
			var exists bool
			err = tx.QueryRowContext(ctx, "SELECT 1 FROM topo_data WHERE path = ?", fullPath).Scan(&exists)
			if err == sql.ErrNoRows {
				return topo.NewError(topo.NoNode, fullPath)
			}
			if err != nil {
				return convertError(err, fullPath)
			}
			return topo.NewError(topo.BadVersion, fullPath)
		}
	} else {
		// Unconditional delete
		result, err := tx.ExecContext(ctx, "DELETE FROM topo_data WHERE path = ?", fullPath)
		if err != nil {
			return convertError(err, fullPath)
		}

		rowsAffected, err := result.RowsAffected()
		if err != nil {
			return convertError(err, fullPath)
		}
		if rowsAffected == 0 {
			return topo.NewError(topo.NoNode, fullPath)
		}
	}

	// Commit the transaction
	if err := tx.Commit(); err != nil {
		return convertError(err, fullPath)
	}
	tx = nil // Prevent rollback on defer
	return nil
}
