// Copyright 2016, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package worker

import (
	"github.com/youtube/vitess/go/sqltypes"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
)

// ResultReader is an advanced version of sqltypes.ResultStream.
// In addition to the streamed Result messages (which contain a set of rows),
// it will expose the Fields (columns information) of the result separately.
//
// Note that some code in the worker package checks if instances of ResultReader
// are equal. In consequence, any ResultReader implementation must always use
// pointer receivers. This way, implementations are always referred by their
// pointer type and the equal comparison of ResultReader instances behaves as
// expected.
type ResultReader interface {
	// Fields returns the field information for the columns in the result.
	Fields() []*querypb.Field

	// Next is identical to sqltypes.ResultStream.Recv().
	// It returns the next result on the stream.
	// It will return io.EOF if the stream ended.
	Next() (*sqltypes.Result, error)
}
