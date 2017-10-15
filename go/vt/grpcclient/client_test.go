/*
Copyright 2017 Google Inc.

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

package grpcclient

import (
	"strings"
	"testing"

	"google.golang.org/grpc"
)

func TestDialErrors(t *testing.T) {
	tcases := []struct {
		address, err string
	}{{
		address: "badhost",
		err:     "dial tcp: address badhost: missing port in address",
	}, {
		address: "badhost:123456",
		err:     "dial tcp: address 123456: invalid port",
	}, {
		address: "[::]:12346",
		err:     "dial tcp [::]:12346: getsockopt: connection refused",
	}}
	for _, tcase := range tcases {
		_, err := Dial(tcase.address, grpc.WithInsecure())
		if err == nil || !strings.Contains(err.Error(), tcase.err) {
			t.Errorf("Dial(%s): %v, must contain %s", tcase.address, err, tcase.err)
		}
	}
}
