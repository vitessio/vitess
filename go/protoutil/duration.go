/*
Copyright 2021 The Vitess Authors.

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

package protoutil

import (
	"time"

	"github.com/golang/protobuf/ptypes"

	durationpb "github.com/golang/protobuf/ptypes/duration"
)

// DurationFromProto converts a durationpb type to a time.Duration. It returns a
// three-tuple of (dgo, ok, err) where dgo is the go time.Duration, ok indicates
// whether the proto value was set, and err is set on failure to convert the
// proto value.
func DurationFromProto(dpb *durationpb.Duration) (time.Duration, bool, error) {
	if dpb == nil {
		return 0, false, nil
	}

	dgo, err := ptypes.Duration(dpb)
	if err != nil {
		return 0, true, err
	}

	return dgo, true, nil
}
