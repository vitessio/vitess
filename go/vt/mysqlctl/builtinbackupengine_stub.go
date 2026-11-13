//go:build !linux

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
li*/

package mysqlctl

import (
	"os"

	"vitess.io/vitess/go/vt/vterrors"

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

func openForSequential(name string) (*os.File, error) {
	return os.Open(name)
}

func openForDirectWrite(name string) (*os.File, error) {
	return nil, vterrors.New(vtrpcpb.Code_UNIMPLEMENTED, "O_DIRECT is not supported on this platform")
}

func fallocateFile(f *os.File, size int64) error {
	return vterrors.New(vtrpcpb.Code_UNIMPLEMENTED, "fallocate is not supported on this platform")
}

func disableDirectIO(f *os.File) error {
	return nil
}
