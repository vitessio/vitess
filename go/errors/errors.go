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

package errors

// Wrapped is used to unwrap an error created by errors.Join() in Go 1.20
type Wrapped interface {
	Unwrap() []error
}

// Unwrap unwraps an error created by errors.Join() in Go 1.20, into its components
func Unwrap(err error) []error {
	if err == nil {
		return nil
	}
	if u, ok := err.(Wrapped); ok {
		return u.Unwrap()
	}
	return nil
}

// Unwrap unwraps an error created by errors.Join() in Go 1.20, into its components, recursively
func UnwrapAll(err error) (errs []error) {
	if err == nil {
		return nil
	}
	if u, ok := err.(Wrapped); ok {
		for _, e := range u.Unwrap() {
			errs = append(errs, UnwrapAll(e)...)
		}
		return errs
	}
	return []error{err}
}

// Unwrap unwraps an error created by errors.Join() in Go 1.20, into its components, recursively,
// and returns one (the first) unwrapped error
func UnwrapFirst(err error) error {
	if err == nil {
		return nil
	}
	return UnwrapAll(err)[0]
}
