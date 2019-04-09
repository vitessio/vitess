/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreedto in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package topo

import (
	"path"

	"github.com/gogo/protobuf/proto"
	"golang.org/x/net/context"

	workflowpb "vitess.io/vitess/go/vt/proto/workflow"
)

// This file provides the utility methods to save / retrieve workflows
// in the topology global cell.

const (
	workflowsPath    = "workflows"
	workflowFilename = "Workflow"
)

func pathForWorkflow(uuid string) string {
	return path.Join(workflowsPath, uuid, workflowFilename)
}

// WorkflowInfo is a meta struct that contains the version of a Workflow.
type WorkflowInfo struct {
	version Version
	*workflowpb.Workflow
}

// GetWorkflowNames returns the names of the existing
// workflows. They are sorted by uuid.
func (ts *Server) GetWorkflowNames(ctx context.Context) ([]string, error) {
	entries, err := ts.globalCell.ListDir(ctx, workflowsPath, false /*full*/)
	switch {
	case IsErrType(err, NoNode):
		return nil, nil
	case err == nil:
		return DirEntriesToStringArray(entries), nil
	default:
		return nil, err
	}
}

// CreateWorkflow creates the given workflow, and returns the initial
// WorkflowInfo.
func (ts *Server) CreateWorkflow(ctx context.Context, w *workflowpb.Workflow) (*WorkflowInfo, error) {
	// Pack the content.
	contents, err := proto.Marshal(w)
	if err != nil {
		return nil, err
	}

	// Save it.
	filePath := pathForWorkflow(w.Uuid)
	version, err := ts.globalCell.Create(ctx, filePath, contents)
	if err != nil {
		return nil, err
	}
	return &WorkflowInfo{
		version:  version,
		Workflow: w,
	}, nil
}

// GetWorkflow reads a workflow from the global cell.
func (ts *Server) GetWorkflow(ctx context.Context, uuid string) (*WorkflowInfo, error) {
	// Read the file.
	filePath := pathForWorkflow(uuid)
	contents, version, err := ts.globalCell.Get(ctx, filePath)
	if err != nil {
		return nil, err
	}

	// Unpack the contents.
	w := &workflowpb.Workflow{}
	if err := proto.Unmarshal(contents, w); err != nil {
		return nil, err
	}

	return &WorkflowInfo{
		version:  version,
		Workflow: w,
	}, nil
}

// SaveWorkflow saves the WorkflowInfo object. If the version is not
// good any more, ErrBadVersion is returned.
func (ts *Server) SaveWorkflow(ctx context.Context, wi *WorkflowInfo) error {
	// Pack the content.
	contents, err := proto.Marshal(wi.Workflow)
	if err != nil {
		return err
	}

	// Save it.
	filePath := pathForWorkflow(wi.Uuid)
	version, err := ts.globalCell.Update(ctx, filePath, contents, wi.version)
	if err != nil {
		return err
	}

	// Remember the new version.
	wi.version = version
	return nil
}

// DeleteWorkflow deletes the specified workflow.  After this, the
// WorkflowInfo object should not be used any more.
func (ts *Server) DeleteWorkflow(ctx context.Context, wi *WorkflowInfo) error {
	filePath := pathForWorkflow(wi.Uuid)
	return ts.globalCell.Delete(ctx, filePath, wi.version)
}
