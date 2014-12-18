// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package proto contains interfaces that other packages may need to interact
// with vtctld, such as to implement plugins.
package proto

import (
	"html/template"
	"net/http"
)

// ActionRepository is an interface for Explorer plugins to populate actions.
type ActionRepository interface {
	PopulateKeyspaceActions(actions map[string]template.URL, keyspace string)
	PopulateShardActions(actions map[string]template.URL, keyspace, shard string)
	PopulateTabletActions(actions map[string]template.URL, tabletAlias string, r *http.Request)
}
