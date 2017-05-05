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

package txthrottler

import (
	"github.com/golang/mock/gomock"
	"github.com/youtube/vitess/go/vt/topo"
)

// NewMockServer wraps a MockImpl in a Server object.
// It returns the wrapped object so that the caller can set expectations on it.
func NewMockServer(ctrl *gomock.Controller) (topo.Server, *MockImpl) {
	mockImpl := NewMockImpl(ctrl)
	return topo.Server{Impl: mockImpl}, mockImpl
}
