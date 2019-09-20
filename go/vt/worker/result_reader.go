/*
Copyright 2019 The Vitess Authors.

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

package worker

import (
	"context"

	"vitess.io/vitess/go/sqltypes"

	querypb "vitess.io/vitess/go/vt/proto/query"
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
	Close(ctx context.Context)
}
