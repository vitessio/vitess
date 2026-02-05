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

package opentsdb

import (
	"encoding/json"
	"expvar"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/stats"
)

func TestFloatFunc(t *testing.T) {
	name := "float_func_name"
	f := stats.FloatFunc(func() float64 {
		return 1.2
	})

	stats.Publish(name, f)

	checkOutput(t, name, `
	[
		{
		  "metric": "vtgate.float_func_name",
		  "timestamp": 1234,
		  "value": 1.2,
		  "tags": {
			"host": "localhost"
		  }
		}
	  ]`)
}

func TestOpenTsdbCounter(t *testing.T) {
	name := "counter_name"
	c := stats.NewCounter(name, "counter description")
	c.Add(1)

	checkOutput(t, name, `
		[
		  {
		    "metric": "vtgate.counter_name",
		    "timestamp": 1234,
		    "value": 1,
		    "tags": {
		      "host": "localhost"
		    }
		  }
		]`)
}

func TestOpenTsdbCounterFunc(t *testing.T) {
	name := "counter_fn_name"
	stats.NewCounterFunc(name, "help", func() int64 {
		return 2
	})
	checkOutput(t, name, `
		[
		  {
		    "metric": "vtgate.counter_fn_name",
		    "timestamp": 1234,
		    "value": 2,
		    "tags": {
		      "host": "localhost"
		    }
		  }
		]`)
}

func TestGaugesWithMultiLabels(t *testing.T) {
	name := "gauges_with_multi_labels_name"
	gauges := stats.NewGaugesWithMultiLabels(name, "help", []string{"flavor", "texture"})
	gauges.Add([]string{"sour", "brittle"}, 3)

	checkOutput(t, name, `
		[
			{
		    "metric": "vtgate.gauges_with_multi_labels_name",
		    "timestamp": 1234,
		    "value": 3,
		    "tags": {
		      "flavor": "sour",
		      "host": "localhost",
		      "texture": "brittle"
		    }
		  }
		]`)
}

func TestGaugesFuncWithMultiLabels(t *testing.T) {
	name := "gauges_func_with_multi_labels_name"
	stats.NewGaugesFuncWithMultiLabels(name, "help", []string{"flavor", "texture"}, func() map[string]int64 {
		m := make(map[string]int64)
		m["foo.bar"] = 1
		m["bar.baz"] = 2
		return m
	})

	checkOutput(t, name, `
		[
			{
				"metric": "vtgate.gauges_func_with_multi_labels_name",
				"timestamp": 1234,
				"value": 2,
				"tags": {
					"flavor": "bar",
					"host": "localhost",
					"texture": "baz"
				}
			},
			{
				"metric": "vtgate.gauges_func_with_multi_labels_name",
				"timestamp": 1234,
				"value": 1,
				"tags": {
					"flavor": "foo",
					"host": "localhost",
					"texture": "bar"
				}
			}
		]`)
}

func TestGaugesWithSingleLabel(t *testing.T) {
	name := "gauges_with_single_label_name"
	s := stats.NewGaugesWithSingleLabel(name, "help", "label1")
	s.Add("bar", 1)

	checkOutput(t, name, `
		[
			{
				"metric": "vtgate.gauges_with_single_label_name",
				"timestamp": 1234,
				"value": 1,
				"tags": {
					"host": "localhost",
					"label1": "bar"
				}
			}
		]`)
}

func TestCountersWithSingleLabel(t *testing.T) {
	name := "counter_with_single_label_name"
	s := stats.NewCountersWithSingleLabel(name, "help", "label", "tag1", "tag2")
	s.Add("tag1", 2)

	checkOutput(t, name, `
		[
			{
			"metric": "vtgate.counter_with_single_label_name",
			"timestamp": 1234,
			"value": 2,
			"tags": {
				"host": "localhost",
				"label": "tag1"
			}
			},
			{
			"metric": "vtgate.counter_with_single_label_name",
			"timestamp": 1234,
			"value": 0,
			"tags": {
				"host": "localhost",
				"label": "tag2"
			}
			}
		]`)
}

func TestCountersWithMultiLabels(t *testing.T) {
	name := "counter_with_multiple_label_name"
	s := stats.NewCountersWithMultiLabels(name, "help", []string{"label1", "label2"})
	s.Add([]string{"foo", "bar"}, 1)

	checkOutput(t, name, `
		[
			{
			"metric": "vtgate.counter_with_multiple_label_name",
			"timestamp": 1234,
			"value": 1,
			"tags": {
				"host": "localhost",
				"label1": "foo",
				"label2": "bar"
			}
			}
		]`)
}

func TestCountersFuncWithMultiLabels(t *testing.T) {
	name := "counter_func_with_multiple_labels_name"
	stats.NewCountersFuncWithMultiLabels(name, "help", []string{"label1", "label2"}, func() map[string]int64 {
		m := make(map[string]int64)
		m["foo.bar"] = 1
		m["bar.baz"] = 2
		return m
	})

	checkOutput(t, name, `
		[
			{
			"metric": "vtgate.counter_func_with_multiple_labels_name",
			"timestamp": 1234,
			"value": 2,
			"tags": {
				"host": "localhost",
				"label1": "bar",
				"label2": "baz"
			}
			},
			{
			"metric": "vtgate.counter_func_with_multiple_labels_name",
			"timestamp": 1234,
			"value": 1,
			"tags": {
				"host": "localhost",
				"label1": "foo",
				"label2": "bar"
			}
			}
		]`)
}

func TestGaugeFloat64(t *testing.T) {
	name := "gauge_float64_name"
	s := stats.NewGaugeFloat64(name, "help")
	s.Set(3.14)

	checkOutput(t, name, `
	[
		{
		  "metric": "vtgate.gauge_float64_name",
		  "timestamp": 1234,
		  "value": 3.14,
		  "tags": {
			"host": "localhost"
		  }
		}
	  ]`)
}

func TestGaugeFunc(t *testing.T) {
	name := "gauge_func_name"
	stats.NewGaugeFunc(name, "help", func() int64 {
		return 2
	})

	checkOutput(t, name, `
	[
		{
		  "metric": "vtgate.gauge_func_name",
		  "timestamp": 1234,
		  "value": 2,
		  "tags": {
			"host": "localhost"
		  }
		}
	  ]`)
}

func TestCounterDuration(t *testing.T) {
	name := "counter_duration_name"
	s := stats.NewCounterDuration(name, "help")
	s.Add(1 * time.Millisecond)

	checkOutput(t, name, `
	[
		{
		  "metric": "vtgate.counter_duration_name",
		  "timestamp": 1234,
		  "value": 1000000,
		  "tags": {
			"host": "localhost"
		  }
		}
	  ]`)
}

func TestCounterDurationFunc(t *testing.T) {
	name := "counter_duration_func_name"
	stats.NewCounterDurationFunc(name, "help", func() time.Duration {
		return 1 * time.Millisecond
	})

	checkOutput(t, name, `
		[
			{
			"metric": "vtgate.counter_duration_func_name",
			"timestamp": 1234,
			"value": 1000000,
			"tags": {
				"host": "localhost"
			}
			}
		]`)
}

func TestMultiTimings(t *testing.T) {
	name := "multi_timings_name"
	s := stats.NewMultiTimings(name, "help", []string{"label1", "label2"})
	s.Add([]string{"foo", "bar"}, 1)

	checkOutput(t, name, `
	[
		{
		  "metric": "vtgate.multi_timings_name.1000000",
		  "timestamp": 1234,
		  "value": 0,
		  "tags": {
			"host": "localhost",
			"label1": "foo",
			"label2": "bar"
		  }
		},
		{
		  "metric": "vtgate.multi_timings_name.10000000",
		  "timestamp": 1234,
		  "value": 0,
		  "tags": {
			"host": "localhost",
			"label1": "foo",
			"label2": "bar"
		  }
		},
		{
		  "metric": "vtgate.multi_timings_name.100000000",
		  "timestamp": 1234,
		  "value": 0,
		  "tags": {
			"host": "localhost",
			"label1": "foo",
			"label2": "bar"
		  }
		},
		{
		  "metric": "vtgate.multi_timings_name.1000000000",
		  "timestamp": 1234,
		  "value": 0,
		  "tags": {
			"host": "localhost",
			"label1": "foo",
			"label2": "bar"
		  }
		},
		{
		  "metric": "vtgate.multi_timings_name.10000000000",
		  "timestamp": 1234,
		  "value": 0,
		  "tags": {
			"host": "localhost",
			"label1": "foo",
			"label2": "bar"
		  }
		},
		{
		  "metric": "vtgate.multi_timings_name.500000",
		  "timestamp": 1234,
		  "value": 1,
		  "tags": {
			"host": "localhost",
			"label1": "foo",
			"label2": "bar"
		  }
		},
		{
		  "metric": "vtgate.multi_timings_name.5000000",
		  "timestamp": 1234,
		  "value": 0,
		  "tags": {
			"host": "localhost",
			"label1": "foo",
			"label2": "bar"
		  }
		},
		{
		  "metric": "vtgate.multi_timings_name.50000000",
		  "timestamp": 1234,
		  "value": 0,
		  "tags": {
			"host": "localhost",
			"label1": "foo",
			"label2": "bar"
		  }
		},
		{
		  "metric": "vtgate.multi_timings_name.500000000",
		  "timestamp": 1234,
		  "value": 0,
		  "tags": {
			"host": "localhost",
			"label1": "foo",
			"label2": "bar"
		  }
		},
		{
		  "metric": "vtgate.multi_timings_name.5000000000",
		  "timestamp": 1234,
		  "value": 0,
		  "tags": {
			"host": "localhost",
			"label1": "foo",
			"label2": "bar"
		  }
		},
		{
		  "metric": "vtgate.multi_timings_name.count",
		  "timestamp": 1234,
		  "value": 1,
		  "tags": {
			"host": "localhost",
			"label1": "foo",
			"label2": "bar"
		  }
		},
		{
		  "metric": "vtgate.multi_timings_name.inf",
		  "timestamp": 1234,
		  "value": 0,
		  "tags": {
			"host": "localhost",
			"label1": "foo",
			"label2": "bar"
		  }
		},
		{
		  "metric": "vtgate.multi_timings_name.time",
		  "timestamp": 1234,
		  "value": 1,
		  "tags": {
			"host": "localhost",
			"label1": "foo",
			"label2": "bar"
		  }
		}
	  ]`)
}

func TestHistogram(t *testing.T) {
	name := "histogram_name"
	s := stats.NewHistogram(name, "help", []int64{1, 2})
	s.Add(2)

	checkOutput(t, name, `
	[
		{
		  "metric": "vtgate.histogram_name.1",
		  "timestamp": 1234,
		  "value": 0,
		  "tags": {
			"host": "localhost"
		  }
		},
		{
		  "metric": "vtgate.histogram_name.2",
		  "timestamp": 1234,
		  "value": 1,
		  "tags": {
			"host": "localhost"
		  }
		},
		{
		  "metric": "vtgate.histogram_name.count",
		  "timestamp": 1234,
		  "value": 1,
		  "tags": {
			"host": "localhost"
		  }
		},
		{
		  "metric": "vtgate.histogram_name.inf",
		  "timestamp": 1234,
		  "value": 0,
		  "tags": {
			"host": "localhost"
		  }
		},
		{
		  "metric": "vtgate.histogram_name.total",
		  "timestamp": 1234,
		  "value": 2,
		  "tags": {
			"host": "localhost"
		  }
		}
	  ]`)
}

type myVar bool

func (mv *myVar) String() string {
	return `{"myKey": 1.2}`
}

func TestExpvar(t *testing.T) {
	name := "blah_expvar"
	expvar.Publish(name, new(myVar))
	checkOutput(t, name, `
		[
		  {
		    "metric": "vtgate.expvar.blah_expvar.mykey",
		    "timestamp": 1234,
		    "value": 1.2,
		    "tags": {
		      "host": "localhost"
		    }
		  }
		]`)
}

func TestOpenTsdbTimings(t *testing.T) {
	name := "blah_timings"
	cats := []string{"cat1", "cat2"}
	timing := stats.NewTimings(name, "help", "category", cats...)
	timing.Add("cat1", time.Duration(1000000000))
	timing.Add("cat1", time.Duration(1))

	checkOutput(t, name, `
		[
		  {
		    "metric": "vtgate.blah_timings.1000000",
		    "timestamp": 1234,
		    "value": 0,
		    "tags": {
		      "category": "cat1",
		      "host": "localhost"
		    }
		  },
		  {
		    "metric": "vtgate.blah_timings.1000000",
		    "timestamp": 1234,
		    "value": 0,
		    "tags": {
		      "category": "cat2",
		      "host": "localhost"
		    }
		  },
		  {
		    "metric": "vtgate.blah_timings.10000000",
		    "timestamp": 1234,
		    "value": 0,
		    "tags": {
		      "category": "cat1",
		      "host": "localhost"
		    }
		  },
		  {
		    "metric": "vtgate.blah_timings.10000000",
		    "timestamp": 1234,
		    "value": 0,
		    "tags": {
		      "category": "cat2",
		      "host": "localhost"
		    }
		  },
		  {
		    "metric": "vtgate.blah_timings.100000000",
		    "timestamp": 1234,
		    "value": 0,
		    "tags": {
		      "category": "cat1",
		      "host": "localhost"
		    }
		  },
		  {
		    "metric": "vtgate.blah_timings.100000000",
		    "timestamp": 1234,
		    "value": 0,
		    "tags": {
		      "category": "cat2",
		      "host": "localhost"
		    }
		  },
		  {
		    "metric": "vtgate.blah_timings.1000000000",
		    "timestamp": 1234,
		    "value": 1,
		    "tags": {
		      "category": "cat1",
		      "host": "localhost"
		    }
		  },
		  {
		    "metric": "vtgate.blah_timings.1000000000",
		    "timestamp": 1234,
		    "value": 0,
		    "tags": {
		      "category": "cat2",
		      "host": "localhost"
		    }
		  },
		  {
		    "metric": "vtgate.blah_timings.10000000000",
		    "timestamp": 1234,
		    "value": 0,
		    "tags": {
		      "category": "cat1",
		      "host": "localhost"
		    }
		  },
		  {
		    "metric": "vtgate.blah_timings.10000000000",
		    "timestamp": 1234,
		    "value": 0,
		    "tags": {
		      "category": "cat2",
		      "host": "localhost"
		    }
		  },
		  {
		    "metric": "vtgate.blah_timings.500000",
		    "timestamp": 1234,
		    "value": 1,
		    "tags": {
		      "category": "cat1",
		      "host": "localhost"
		    }
		  },
		  {
		    "metric": "vtgate.blah_timings.500000",
		    "timestamp": 1234,
		    "value": 0,
		    "tags": {
		      "category": "cat2",
		      "host": "localhost"
		    }
		  },
		  {
		    "metric": "vtgate.blah_timings.5000000",
		    "timestamp": 1234,
		    "value": 0,
		    "tags": {
		      "category": "cat1",
		      "host": "localhost"
		    }
		  },
		  {
		    "metric": "vtgate.blah_timings.5000000",
		    "timestamp": 1234,
		    "value": 0,
		    "tags": {
		      "category": "cat2",
		      "host": "localhost"
		    }
		  },
		  {
		    "metric": "vtgate.blah_timings.50000000",
		    "timestamp": 1234,
		    "value": 0,
		    "tags": {
		      "category": "cat1",
		      "host": "localhost"
		    }
		  },
		  {
		    "metric": "vtgate.blah_timings.50000000",
		    "timestamp": 1234,
		    "value": 0,
		    "tags": {
		      "category": "cat2",
		      "host": "localhost"
		    }
		  },
		  {
		    "metric": "vtgate.blah_timings.500000000",
		    "timestamp": 1234,
		    "value": 0,
		    "tags": {
		      "category": "cat1",
		      "host": "localhost"
		    }
		  },
		  {
		    "metric": "vtgate.blah_timings.500000000",
		    "timestamp": 1234,
		    "value": 0,
		    "tags": {
		      "category": "cat2",
		      "host": "localhost"
		    }
		  },
		  {
		    "metric": "vtgate.blah_timings.5000000000",
		    "timestamp": 1234,
		    "value": 0,
		    "tags": {
		      "category": "cat1",
		      "host": "localhost"
		    }
		  },
		  {
		    "metric": "vtgate.blah_timings.5000000000",
		    "timestamp": 1234,
		    "value": 0,
		    "tags": {
		      "category": "cat2",
		      "host": "localhost"
		    }
		  },
		  {
		    "metric": "vtgate.blah_timings.count",
		    "timestamp": 1234,
		    "value": 2,
		    "tags": {
		      "category": "cat1",
		      "host": "localhost"
		    }
		  },
		  {
		    "metric": "vtgate.blah_timings.count",
		    "timestamp": 1234,
		    "value": 0,
		    "tags": {
		      "category": "cat2",
		      "host": "localhost"
		    }
		  },
		  {
		    "metric": "vtgate.blah_timings.inf",
		    "timestamp": 1234,
		    "value": 0,
		    "tags": {
		      "category": "cat1",
		      "host": "localhost"
		    }
		  },
		  {
		    "metric": "vtgate.blah_timings.inf",
		    "timestamp": 1234,
		    "value": 0,
		    "tags": {
		      "category": "cat2",
		      "host": "localhost"
		    }
		  },
		  {
		    "metric": "vtgate.blah_timings.time",
		    "timestamp": 1234,
		    "value": 1000000001,
		    "tags": {
		      "category": "cat1",
		      "host": "localhost"
		    }
		  },
		  {
		    "metric": "vtgate.blah_timings.time",
		    "timestamp": 1234,
		    "value": 0,
		    "tags": {
		      "category": "cat2",
		      "host": "localhost"
		    }
		  }
		]`)
}

func TestCounterForEmptyCollectorPrefix(t *testing.T) {
	name := "counter_for_empty_collector_prefix_name"
	c := stats.NewCounter(name, "counter description")
	c.Add(1)

	expectedOutput := `
		[
			{
				"metric": "counter_for_empty_collector_prefix_name",
				"timestamp": 1234,
				"value": 1,
				"tags": {
					"host": "test_localhost"
				}
			}
	]`

	dc := &collector{
		commonTags: map[string]string{"host": "test localhost"},
		prefix:     "",
		timestamp:  int64(1234),
	}
	expvar.Do(func(kv expvar.KeyValue) {
		if kv.Key == name {
			dc.addExpVar(kv)
			sort.Sort(byMetric(dc.data))

			gotBytes, err := json.MarshalIndent(dc.data, "", "  ")
			assert.NoErrorf(t, err, "failed to marshal json")

			var got any
			err = json.Unmarshal(gotBytes, &got)
			assert.NoErrorf(t, err, "failed to unmarshal json")

			var want any
			err = json.Unmarshal([]byte(expectedOutput), &want)
			assert.NoErrorf(t, err, "failed to unmarshal json")

			assert.Equal(t, want, got)
		}
	})
}

func checkOutput(t *testing.T, statName string, wantJSON string) {
	b := &backend{
		prefix:     "vtgate",
		commonTags: map[string]string{"host": "localhost"},
	}
	timestamp := int64(1234)

	dc := &collector{
		commonTags: b.commonTags,
		prefix:     b.prefix,
		timestamp:  timestamp,
	}
	found := false
	expvar.Do(func(kv expvar.KeyValue) {
		if kv.Key == statName {
			found = true

			dc.addExpVar(kv)
			sort.Sort(byMetric(dc.data))

			gotBytes, err := json.MarshalIndent(dc.data, "", "  ")
			assert.NoErrorf(t, err, "failed to marshal json")

			var got any
			err = json.Unmarshal(gotBytes, &got)
			assert.NoErrorf(t, err, "failed to unmarshal json")

			var want any
			err = json.Unmarshal([]byte(wantJSON), &want)
			assert.NoErrorf(t, err, "failed to unmarshal json")

			assert.Equal(t, want, got)
		}
	})
	assert.True(t, found, "stat %s not found", statName)
}
