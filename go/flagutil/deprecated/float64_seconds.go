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

package deprecated

import (
	"strconv"
	"time"

	"vitess.io/vitess/go/flagutil"
	"vitess.io/vitess/go/vt/log"
)

type Float64Seconds struct {
	name string
	val  time.Duration
}

var _ flagutil.Value[time.Duration] = (*Float64Seconds)(nil)

func NewFloat64Seconds(name string, defVal time.Duration) Float64Seconds {
	return Float64Seconds{
		name: name,
		val:  defVal,
	}
}

func (f *Float64Seconds) String() string { return f.val.String() }
func (f *Float64Seconds) Type() string   { return "duration" }

func (f *Float64Seconds) Set(arg string) error {
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

func (f Float64Seconds) Get() time.Duration { return f.val }
