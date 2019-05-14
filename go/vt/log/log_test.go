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

package log

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLogListener(t *testing.T) {
	listener := &TestableListener{}
	Subscribe(listener)

	InfofC(context.Background(), "format")
	require.Equal(t, "format", listener.seen[0].format)
}

var _ Listener = (*TestableListener)(nil)

type TestableListener struct {
	seen []event
}

func (tl *TestableListener) Listen(ctx context.Context, level, format string, args ...interface{}) {
	tl.seen = append(tl.seen, event{ctx, level, format, args})
}

type event struct {
	ctx           context.Context
	level, format string
	args          []interface{}
}
