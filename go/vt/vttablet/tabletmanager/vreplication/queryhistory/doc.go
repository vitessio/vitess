/*
Copyright 2026 The Vitess Authors.

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

// Package queryhistory provides tools for verifying that a SQL statement
// history conforms to a set of expectations.
//
// For example...
//
//	expectations := Expect("delete1", "insert1").
//	    Then(func(sequence ExpectationSequence) ExpectationSequence {
//	        c1 := sequence.Then(Eventually("insert2")).
//	                       Then(Eventually("update1"))
//	        c2 := sequence.Then(Eventually("insert3")).
//	                       Then(Eventually("update2"))
//	        c1.Then(c2.Eventually())
//	        return c2
//	    }).
//	    Then(Immediately("delete2")
//
// ...creates a sequence of expectations, such that:
//
//   - "delete1" is expected first,
//   - "insert1" immediately follows "delete1",
//   - "insert2" and "insert3" eventually follow "insert1", in any order,
//   - "update1" eventually follows "insert2"
//   - "update2" eventually follows "insert3"
//   - "update2" eventually follows "update1"
//   - "delete2" immediately follows "update2"
//
// To verify a sequence of expectations, construct a verifier...
//
//	verifier := NewVerifier(expectations)
//
// ...and make successive calls with actual queries:
//
//	result := verifier.AcceptQuery("insert1")
//
// If the verifier accepts a query, it modifies its internal state in order to
// verify sequenced expectations (e.g. that "q2" eventually follows "q1").
package queryhistory
