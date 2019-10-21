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

// Package splitquery contains the logic needed for implementing the tabletserver's SplitQuery RPC.
//
// It defines the Splitter type that drives the query splitting procedure. It cooperates with the
// SplitParams type and splitAlgorithmInterface interface. See example_test.go for a usage example.
//
// General guidelines for contributing to this package:
// 1) Error messages should not contain the "splitquery:" prefix. It will be added by the calling
// code in 'tabletserver'.
package splitquery
