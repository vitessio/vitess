/*
Copyright 2024 The Vitess Authors.

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
	"errors"
	"strconv"
	"time"

	"github.com/spf13/pflag"
)

// FloatOrDuration is a flag that can be set with either a float64 (interpreted as seconds) or a time.Duration
// The parsed value is stored in the Duration field, and the target pointer is updated with the parsed value
type FloatOrDuration struct {
	Target   *time.Duration // Pointer to the external duration
	Duration time.Duration  // Stores the current parsed value
}

// String returns the current value as a string
func (f *FloatOrDuration) String() string {
	return f.Duration.String()
}

// Set parses the input and updates the duration
func (f *FloatOrDuration) Set(value string) error {
	// Try to parse as float64 first (interpreted as seconds)
	if floatVal, err := strconv.ParseFloat(value, 64); err == nil {
		f.Duration = time.Duration(floatVal * float64(time.Second))
		*f.Target = f.Duration // Update the target pointer
		return nil
	}

	// Try to parse as time.Duration
	if duration, err := time.ParseDuration(value); err == nil {
		f.Duration = duration
		*f.Target = f.Duration // Update the target pointer
		return nil
	}

	return errors.New("value must be either a float64 (interpreted as seconds) or a valid time.Duration")
}

// Type returns the type description
func (f *FloatOrDuration) Type() string {
	return "time.Duration"
}

// FloatDuration defines a flag with the specified name, default value, and usage string and binds it to a time.Duration variable.
func FloatDuration(fs *pflag.FlagSet, p *time.Duration, name string, defaultValue time.Duration, usage string) {
	*p = defaultValue
	fd := FloatOrDuration{
		Target:   p,
		Duration: defaultValue,
	}
	fs.Var(&fd, name, usage)
}
