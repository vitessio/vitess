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
	"reflect"
	"sort"
	"testing"
	"time"

	"vitess.io/vitess/go/stats"
)

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
		      "histograms": "cat1",
		      "host": "localhost"
		    }
		  },
		  {
		    "metric": "vtgate.blah_timings.1000000",
		    "timestamp": 1234,
		    "value": 0,
		    "tags": {
		      "histograms": "cat2",
		      "host": "localhost"
		    }
		  },
		  {
		    "metric": "vtgate.blah_timings.10000000",
		    "timestamp": 1234,
		    "value": 0,
		    "tags": {
		      "histograms": "cat1",
		      "host": "localhost"
		    }
		  },
		  {
		    "metric": "vtgate.blah_timings.10000000",
		    "timestamp": 1234,
		    "value": 0,
		    "tags": {
		      "histograms": "cat2",
		      "host": "localhost"
		    }
		  },
		  {
		    "metric": "vtgate.blah_timings.100000000",
		    "timestamp": 1234,
		    "value": 0,
		    "tags": {
		      "histograms": "cat1",
		      "host": "localhost"
		    }
		  },
		  {
		    "metric": "vtgate.blah_timings.100000000",
		    "timestamp": 1234,
		    "value": 0,
		    "tags": {
		      "histograms": "cat2",
		      "host": "localhost"
		    }
		  },
		  {
		    "metric": "vtgate.blah_timings.1000000000",
		    "timestamp": 1234,
		    "value": 1,
		    "tags": {
		      "histograms": "cat1",
		      "host": "localhost"
		    }
		  },
		  {
		    "metric": "vtgate.blah_timings.1000000000",
		    "timestamp": 1234,
		    "value": 0,
		    "tags": {
		      "histograms": "cat2",
		      "host": "localhost"
		    }
		  },
		  {
		    "metric": "vtgate.blah_timings.10000000000",
		    "timestamp": 1234,
		    "value": 0,
		    "tags": {
		      "histograms": "cat1",
		      "host": "localhost"
		    }
		  },
		  {
		    "metric": "vtgate.blah_timings.10000000000",
		    "timestamp": 1234,
		    "value": 0,
		    "tags": {
		      "histograms": "cat2",
		      "host": "localhost"
		    }
		  },
		  {
		    "metric": "vtgate.blah_timings.500000",
		    "timestamp": 1234,
		    "value": 1,
		    "tags": {
		      "histograms": "cat1",
		      "host": "localhost"
		    }
		  },
		  {
		    "metric": "vtgate.blah_timings.500000",
		    "timestamp": 1234,
		    "value": 0,
		    "tags": {
		      "histograms": "cat2",
		      "host": "localhost"
		    }
		  },
		  {
		    "metric": "vtgate.blah_timings.5000000",
		    "timestamp": 1234,
		    "value": 0,
		    "tags": {
		      "histograms": "cat1",
		      "host": "localhost"
		    }
		  },
		  {
		    "metric": "vtgate.blah_timings.5000000",
		    "timestamp": 1234,
		    "value": 0,
		    "tags": {
		      "histograms": "cat2",
		      "host": "localhost"
		    }
		  },
		  {
		    "metric": "vtgate.blah_timings.50000000",
		    "timestamp": 1234,
		    "value": 0,
		    "tags": {
		      "histograms": "cat1",
		      "host": "localhost"
		    }
		  },
		  {
		    "metric": "vtgate.blah_timings.50000000",
		    "timestamp": 1234,
		    "value": 0,
		    "tags": {
		      "histograms": "cat2",
		      "host": "localhost"
		    }
		  },
		  {
		    "metric": "vtgate.blah_timings.500000000",
		    "timestamp": 1234,
		    "value": 0,
		    "tags": {
		      "histograms": "cat1",
		      "host": "localhost"
		    }
		  },
		  {
		    "metric": "vtgate.blah_timings.500000000",
		    "timestamp": 1234,
		    "value": 0,
		    "tags": {
		      "histograms": "cat2",
		      "host": "localhost"
		    }
		  },
		  {
		    "metric": "vtgate.blah_timings.5000000000",
		    "timestamp": 1234,
		    "value": 0,
		    "tags": {
		      "histograms": "cat1",
		      "host": "localhost"
		    }
		  },
		  {
		    "metric": "vtgate.blah_timings.5000000000",
		    "timestamp": 1234,
		    "value": 0,
		    "tags": {
		      "histograms": "cat2",
		      "host": "localhost"
		    }
		  },
		  {
		    "metric": "vtgate.blah_timings.count",
		    "timestamp": 1234,
		    "value": 2,
		    "tags": {
		      "histograms": "cat1",
		      "host": "localhost"
		    }
		  },
		  {
		    "metric": "vtgate.blah_timings.count",
		    "timestamp": 1234,
		    "value": 0,
		    "tags": {
		      "histograms": "cat2",
		      "host": "localhost"
		    }
		  },
		  {
		    "metric": "vtgate.blah_timings.inf",
		    "timestamp": 1234,
		    "value": 0,
		    "tags": {
		      "histograms": "cat1",
		      "host": "localhost"
		    }
		  },
		  {
		    "metric": "vtgate.blah_timings.inf",
		    "timestamp": 1234,
		    "value": 0,
		    "tags": {
		      "histograms": "cat2",
		      "host": "localhost"
		    }
		  },
		  {
		    "metric": "vtgate.blah_timings.time",
		    "timestamp": 1234,
		    "value": 1000000001,
		    "tags": {
		      "histograms": "cat1",
		      "host": "localhost"
		    }
		  },
		  {
		    "metric": "vtgate.blah_timings.time",
		    "timestamp": 1234,
		    "value": 0,
		    "tags": {
		      "histograms": "cat2",
		      "host": "localhost"
		    }
		  }
		]`)
}

func checkOutput(t *testing.T, statName string, wantJSON string) {
	backend := &openTSDBBackend{
		prefix:     "vtgate",
		commonTags: map[string]string{"host": "localhost"},
	}
	timestamp := int64(1234)

	dc := &dataCollector{
		settings:  backend,
		timestamp: timestamp,
	}
	found := false
	expvar.Do(func(kv expvar.KeyValue) {
		if kv.Key == statName {
			found = true

			dc.addExpVar(kv)
			sort.Sort(byMetric(dc.dataPoints))

			gotBytes, err := json.MarshalIndent(dc.dataPoints, "", "  ")
			if err != nil {
				t.Errorf("Failed to marshal json: %v", err)
				return
			}
			var got interface{}
			err = json.Unmarshal(gotBytes, &got)
			if err != nil {
				t.Errorf("Failed to marshal json: %v", err)
				return
			}

			var want interface{}
			err = json.Unmarshal([]byte(wantJSON), &want)
			if err != nil {
				t.Errorf("Failed to marshal json: %v", err)
				return
			}

			if !reflect.DeepEqual(got, want) {
				t.Errorf("addExpVar(%#v) = %s, want %s", kv, string(gotBytes), wantJSON)
			}
		}
	})
	if !found {
		t.Errorf("Stat %s not found?...", statName)
	}
}
