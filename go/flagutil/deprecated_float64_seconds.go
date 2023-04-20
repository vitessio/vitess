/*
Copyright 2023 The Vitess Authors.

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

package flagutil

import (
	"strconv"
	"time"

	"vitess.io/vitess/go/vt/log"
)

type DeprecatedFloat64Seconds struct {
	name string
	val  time.Duration
}

var _ Value[time.Duration] = (*DeprecatedFloat64Seconds)(nil)

func NewDeprecatedFloat64Seconds(name string, defVal time.Duration) DeprecatedFloat64Seconds {
	return DeprecatedFloat64Seconds{
		name: name,
		val:  defVal,
	}
}

func (f *DeprecatedFloat64Seconds) String() string { return f.val.String() }
func (f *DeprecatedFloat64Seconds) Type() string   { return "duration" }

func (f *DeprecatedFloat64Seconds) Set(arg string) error {
	v, err := time.ParseDuration(arg)
	if err != nil {
		log.Warningf("failed to parse %s as duration (err: %v); falling back to parsing to %s as seconds. this is deprecated and will be removed in a future release", f.name, err, f.val)

		n, err := strconv.ParseFloat(arg, 64)
		if err != nil {
			return err
		}

		v = time.Duration(n * float64(time.Second))
	}

	f.val = v
	return nil
}

func (f DeprecatedFloat64Seconds) Clone() DeprecatedFloat64Seconds {
	return DeprecatedFloat64Seconds{
		name: f.name,
		val:  f.val,
	}
}

func (f DeprecatedFloat64Seconds) Name() string       { return f.name }
func (f DeprecatedFloat64Seconds) Get() time.Duration { return f.val }

func (f *DeprecatedFloat64Seconds) UnmarshalJSON(data []byte) error {
	return f.Set(string(data))
}
