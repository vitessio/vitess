/*
 Copyright 2017 GitHub Inc.

 Licensed under MIT License. See https://github.com/github/freno/blob/master/LICENSE
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
