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

package gitutil

import (
	"context"

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/storage/memory"
)

// getGitRefSHAString returns the HEAD commit of the provided reference as a string.
func getGitRefSHAString(ctx context.Context, gitURL string, refName plumbing.ReferenceName) (string, error) {
	r, err := git.CloneContext(ctx, memory.NewStorage(), nil, &git.CloneOptions{
		URL:           gitURL,
		ReferenceName: refName,
		Depth:         1,          // shallow clone
		NoCheckout:    true,       // don't checkout files
		SingleBranch:  true,       // don't fetch remote branches
		Tags:          git.NoTags, // don't fetch remote tags
	})
	if err != nil {
		return "", err
	}
	h, err := r.Head()
	if err != nil || h == nil {
		return "", err
	}
	return h.Hash().String(), nil
}

// GetGitBranchSHAString returns the HEAD commit of the provided branch name as a string.
func GetGitBranchSHAString(ctx context.Context, gitURL, branchName string) (string, error) {
	return getGitRefSHAString(ctx, gitURL, plumbing.NewBranchReferenceName(branchName))
}

// GetGitHeadSHAString returns the HEAD commit of the default branch of a repo as a string.
func GetGitHeadSHAString(ctx context.Context, gitURL string) (string, error) {
	return getGitRefSHAString(ctx, gitURL, plumbing.HEAD)
}

// GetGitTagSHAString returns the HEAD commit of the provided tag name as a string.
func GetGitTagSHAString(ctx context.Context, gitURL, tagName string) (string, error) {
	return getGitRefSHAString(ctx, gitURL, plumbing.NewTagReferenceName(tagName))
}
