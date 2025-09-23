/*
Copyright 2024 The Vitess Authors.

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
)

// TxnOpResult represents the result of an operation within a transaction.
type TxnOpResult struct {
	Contents   []byte
	DirEntries []DirEntry
	Key        string
	Version    Version
}

// Txn defines the interface that must be implemented by topology
// plug-ins to be used with Vitess.
type Txn interface {
	//
	// Directory support
	//

	// ListDir returns the entries in a directory.  The returned
	// list should be sorted by entry.Name.
	// If there are no files under the provided path, returns ErrNoNode.
	// dirPath is a path relative to the root directory of the cell.
	// If full is set, we want all the fields in DirEntry to be filled in.
	// If full is not set, only Name will be used. This is intended for
	// implementations where getting more than the names is more expensive,
	// as in most cases only the names are needed.
	ListDir(dirPath string, full bool) Txn

	//
	// File support
	// if version == nil, then it’s an unconditional update / delete.
	//

	// Create creates the initial version of a file.
	// Returns ErrNodeExists if the file exists.
	// filePath is a path relative to the root directory of the cell.
	Create(filePath string, contents []byte) Txn

	// Update updates the file with the provided filename with the
	// new content.
	// If version is nil, it is an unconditional update
	// (which is then the same as a Create is the file doesn't exist).
	// filePath is a path relative to the root directory of the cell.
	// It returns the new Version of the file after update.
	// Returns ErrBadVersion if the provided version is not current.
	Update(filePath string, contents []byte, version Version) Txn

	// Get returns the content and version of a file.
	// filePath is a path relative to the root directory of the cell.
	// Can return ErrNoNode if the file doesn't exist.
	Get(filePath string) Txn

	// GetVersion returns the content of a file at the given version.
	// filePath is a path relative to the root directory of the cell.
	// Can return ErrNoNode if the file doesn't exist at the given
	// version or ErrNoImplementation if the topo server does not
	// support storing multiple versions and retrieving a specific one.
	GetVersion(filePath string, version int64) Txn

	// List returns KV pairs, along with metadata like the version, for
	// entries where the key contains the specified prefix.
	// filePathPrefix is a path relative to the root directory of the cell.
	// Can return ErrNoNode if there are no matches.
	List(filePathPrefix string) Txn

	// Delete deletes the provided file.
	// If version is nil, it is an unconditional delete.
	// If the last entry of a directory is deleted, using ListDir
	// on its parent directory should not return the directory.
	// For instance, when deleting /keyspaces/aaa/Keyspace, and if
	// there is no other file in /keyspaces/aaa, then aaa should not
	// appear any more when listing /keyspaces.
	// filePath is a path relative to the root directory of the cell.
	//
	// Delete will never be called on a directory.
	// Returns ErrNodeExists if the file doesn't exist.
	// Returns ErrBadVersion if the provided version is not current.
	Delete(filePath string, version Version) Txn

	// Commit commits the transaction.
	Commit(ctx context.Context) ([]TxnOpResult, error)
}
