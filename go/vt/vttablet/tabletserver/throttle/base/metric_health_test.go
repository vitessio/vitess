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

// This codebase originates from https://github.com/github/freno, See https://github.com/github/freno/blob/master/LICENSE
/*
	MIT License

	Copyright (c) 2017 GitHub

	Permission is hereby granted, free of charge, to any person obtaining a copy
	of this software and associated documentation files (the "Software"), to deal
	in the Software without restriction, including without limitation the rights
	to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
	copies of the Software, and to permit persons to whom the Software is
	furnished to do so, subject to the following conditions:

	The above copyright notice and this permission notice shall be included in all
	copies or substantial portions of the Software.

	THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
	IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
	FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
	AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
	LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
	OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
	SOFTWARE.
*/
package base

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAggregate(t *testing.T) {
	{
		m1 := MetricHealthMap{
			"a": &MetricHealth{SecondsSinceLastHealthy: 0},
		}
		m2 := MetricHealthMap{}
		m1.Aggregate(m2)
		assert.Equal(t, 1, len(m1))
		assert.Equal(t, int64(0), m1["a"].SecondsSinceLastHealthy)
	}
	{
		m1 := MetricHealthMap{
			"a": &MetricHealth{SecondsSinceLastHealthy: 0},
		}
		m2 := MetricHealthMap{}
		m2.Aggregate(m1)
		assert.Equal(t, 1, len(m2))
		assert.Equal(t, int64(0), m2["a"].SecondsSinceLastHealthy)
	}
	{
		m1 := MetricHealthMap{
			"a": &MetricHealth{SecondsSinceLastHealthy: 7},
		}
		m2 := MetricHealthMap{}
		m1.Aggregate(m2)
		assert.Equal(t, 1, len(m1))
		assert.Equal(t, int64(7), m1["a"].SecondsSinceLastHealthy)
	}
	{
		m1 := MetricHealthMap{
			"a": &MetricHealth{SecondsSinceLastHealthy: 7},
		}
		m2 := MetricHealthMap{}
		m2.Aggregate(m1)
		assert.Equal(t, 1, len(m2))
		assert.Equal(t, int64(7), m2["a"].SecondsSinceLastHealthy)
	}
	{
		m1 := MetricHealthMap{
			"a": &MetricHealth{SecondsSinceLastHealthy: 7},
		}
		m2 := MetricHealthMap{
			"a": &MetricHealth{SecondsSinceLastHealthy: 11},
		}
		m1.Aggregate(m2)
		assert.Equal(t, 1, len(m1))
		assert.Equal(t, int64(11), m1["a"].SecondsSinceLastHealthy)
	}
	{
		m1 := MetricHealthMap{
			"a": &MetricHealth{SecondsSinceLastHealthy: 11},
		}
		m2 := MetricHealthMap{
			"a": &MetricHealth{SecondsSinceLastHealthy: 7},
		}
		m1.Aggregate(m2)
		assert.Equal(t, 1, len(m1))
		assert.Equal(t, int64(11), m1["a"].SecondsSinceLastHealthy)
	}
	{
		m1 := MetricHealthMap{
			"a": &MetricHealth{SecondsSinceLastHealthy: 7},
			"b": &MetricHealth{SecondsSinceLastHealthy: 19},
		}
		m2 := MetricHealthMap{
			"a": &MetricHealth{SecondsSinceLastHealthy: 11},
			"b": &MetricHealth{SecondsSinceLastHealthy: 17},
		}
		m1.Aggregate(m2)
		assert.Equal(t, 2, len(m1))
		assert.Equal(t, int64(11), m1["a"].SecondsSinceLastHealthy)
		assert.Equal(t, int64(19), m1["b"].SecondsSinceLastHealthy)
	}
	{
		m1 := MetricHealthMap{
			"a": &MetricHealth{SecondsSinceLastHealthy: 7},
			"b": &MetricHealth{SecondsSinceLastHealthy: 19},
		}
		m2 := MetricHealthMap{
			"a": &MetricHealth{SecondsSinceLastHealthy: 11},
			"c": &MetricHealth{SecondsSinceLastHealthy: 17},
		}
		m1.Aggregate(m2)
		assert.Equal(t, 3, len(m1), 3)
		assert.Equal(t, int64(11), m1["a"].SecondsSinceLastHealthy)
		assert.Equal(t, int64(19), m1["b"].SecondsSinceLastHealthy)
		assert.Equal(t, int64(17), m1["c"].SecondsSinceLastHealthy)
	}
	{
		m0 := MetricHealthMap{}
		m1 := MetricHealthMap{
			"a": &MetricHealth{SecondsSinceLastHealthy: 7},
			"b": &MetricHealth{SecondsSinceLastHealthy: 19},
		}
		m2 := MetricHealthMap{
			"a": &MetricHealth{SecondsSinceLastHealthy: 11},
			"c": &MetricHealth{SecondsSinceLastHealthy: 17},
		}
		m0.Aggregate(m2)
		m0.Aggregate(m1)
		assert.Equal(t, 3, len(m0))
		assert.Equal(t, int64(11), m0["a"].SecondsSinceLastHealthy)
		assert.Equal(t, int64(19), m0["b"].SecondsSinceLastHealthy)
		assert.Equal(t, int64(17), m0["c"].SecondsSinceLastHealthy)
	}
}
