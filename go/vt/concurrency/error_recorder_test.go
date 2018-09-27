/*
Copyright 2018 The Vitess Authors

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

package concurrency

import (
	"fmt"
	"reflect"
	"testing"
)

func TestAllErrorRecorderAppend(t *testing.T) {
	aer := AllErrorRecorder{}

	if aer.HasErrors() != false {
		t.Errorf("HasErrors() should be false")
	}

	if aer.Error() != nil {
		t.Errorf("Error() expected nil")
	}

	err1 := fmt.Errorf("error 1")

	aer.RecordError(err1)

	if aer.HasErrors() != true {
		t.Errorf("HasErrors() should be true")
	}

	gotErr := aer.Error().Error()
	wantErr := "error 1"
	if gotErr != wantErr {
		t.Errorf("Error() expected %v, got %v", wantErr, gotErr)
	}

	gotErrs := aer.GetErrors(1)
	wantErrs := []error{err1}
	if !reflect.DeepEqual(gotErrs, wantErrs) {
		t.Errorf("GetErrors() expected %v, gotErrs %v", wantErrs, gotErrs)
	}

	err2 := fmt.Errorf("error 2")
	aer.RecordError(err2)

	if aer.HasErrors() != true {
		t.Errorf("HasErrors() should be true")
	}

	gotErr = aer.Error().Error()
	wantErr = "error 1;error 2"
	if gotErr != wantErr {
		t.Errorf("Error() expected %v, got %v", wantErr, gotErr)
	}

	gotErrs = aer.GetErrors(2)
	wantErrs = []error{err1, err2}
	if !reflect.DeepEqual(gotErrs, wantErrs) {
		t.Errorf("GetErrors() expected %v, gotErrs %v", gotErrs, wantErrs)
	}

	// This grows the internal representation to include nil entries
	gotErrs = aer.GetErrors(4)
	wantErrs = []error{err1, err2, nil, nil}
	if !reflect.DeepEqual(gotErrs, wantErrs) {
		t.Errorf("GetErrors() expected %v, gotErrs %v", wantErrs, gotErrs)
	}

	// The aggregated representation ignores these
	gotErr = aer.Error().Error()
	wantErr = "error 1;error 2"
	if gotErr != wantErr {
		t.Errorf("Error() expected %v, got %v", wantErr, gotErr)
	}

	err3 := fmt.Errorf("error 3")
	aer.RecordError(err3)

	if aer.HasErrors() != true {
		t.Errorf("HasErrors() should be true")
	}

	gotErr = aer.Error().Error()
	wantErr = "error 1;error 2;error 3"
	if gotErr != wantErr {
		t.Errorf("Error() expected %v, got %v", wantErr, gotErr)
	}

	// This somewhat confusing output format is due to the internal array growing
	// from the previous "GetErrors(4)" call
	gotErrs = aer.GetErrors(5)
	wantErrs = []error{err1, err2, nil, nil, err3}
	if !reflect.DeepEqual(gotErrs, wantErrs) {
		t.Errorf("GetErrors() expected %v, gotErrs %v", wantErrs, gotErrs)
	}

}

func TestAllErrorRecorderAppendAt(t *testing.T) {
	aer := AllErrorRecorder{}

	if aer.HasErrors() != false {
		t.Errorf("HasErrors() should be false")
	}

	err0 := fmt.Errorf("error 0")

	aer.RecordErrorAt(0, err0)

	if aer.HasErrors() != true {
		t.Errorf("HasErrors() should be true")
	}

	gotErrs := aer.GetErrors(1)
	wantErrs := []error{err0}
	if !reflect.DeepEqual(gotErrs, wantErrs) {
		t.Errorf("GetErrors() expected %v, gotErrs %v", wantErrs, gotErrs)
	}

	err3 := fmt.Errorf("error 3")
	aer.RecordErrorAt(3, err3)

	if aer.HasErrors() != true {
		t.Errorf("HasErrors() should be true")
	}

	gotErrs = aer.GetErrors(4)
	wantErrs = []error{err0, nil, nil, err3}
	if !reflect.DeepEqual(gotErrs, wantErrs) {
		t.Errorf("GetErrors() expected %v, gotErrs %v", gotErrs, wantErrs)
	}

	gotErrs = aer.GetErrors(5)
	wantErrs = []error{err0, nil, nil, err3, nil}
	if !reflect.DeepEqual(gotErrs, wantErrs) {
		t.Errorf("GetErrors() expected %v, gotErrs %v", wantErrs, gotErrs)
	}

	err2 := fmt.Errorf("error 2")
	aer.RecordErrorAt(2, err2)

	if aer.HasErrors() != true {
		t.Errorf("HasErrors() should be true")
	}

	gotErrs = aer.GetErrors(5)
	wantErrs = []error{err0, nil, err2, err3, nil}
	if !reflect.DeepEqual(gotErrs, wantErrs) {
		t.Errorf("GetErrors() expected %v, gotErrs %v", wantErrs, gotErrs)
	}
}
