package statsd

import (
	"expvar"
	"net"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/DataDog/datadog-go/statsd"
	"gotest.tools/assert"

	"vitess.io/vitess/go/stats"
)

func getBackend(t *testing.T) (StatsBackend, *net.UDPConn) {
	addr := "localhost:1201"
	udpAddr, _ := net.ResolveUDPAddr("udp", addr)
	server, _ := net.ListenUDP("udp", udpAddr)
	bufferLength := 9
	client, _ := statsd.NewBuffered(addr, bufferLength)
	client.Namespace = "test."
	var sb StatsBackend
	sb.namespace = "foo"
	sb.sampleRate = 1
	sb.statsdClient = client
	stats.RegisterTimerHook(func(stats, name string, value int64, timings *stats.Timings) {
		tags := makeLabels(strings.Split(timings.Label(), "."), name)
		client.TimeInMilliseconds(stats, float64(value), tags, sb.sampleRate)
	})
	stats.RegisterHistogramHook(func(name string, val int64) {
		client.Histogram(name, float64(val), []string{}, sb.sampleRate)
	})
	return sb, server
}

func TestStatsdCounter(t *testing.T) {
	sb, server := getBackend(t)
	defer server.Close()
	name := "counter_name"
	c := stats.NewCounter(name, "counter description")
	c.Add(1)
	found := false
	expvar.Do(func(kv expvar.KeyValue) {
		found = true
		if kv.Key == name {
			sb.addExpVar(kv)
			if err := sb.statsdClient.Flush(); err != nil {
				t.Errorf("Error flushing: %s", err)
			}
			bytes := make([]byte, 4096)
			n, err := server.Read(bytes)
			if err != nil {
				t.Fatal(err)
			}
			result := string(bytes[:n])
			expected := "test.counter_name:1|c"
			assert.Equal(t, result, expected)
		}
	})
	if !found {
		t.Errorf("Stat %s not found...", name)
	}
}

func TestStatsdGauge(t *testing.T) {
	sb, server := getBackend(t)
	defer server.Close()
	name := "gauge_name"
	s := stats.NewGauge(name, "help")
	s.Set(10)
	found := false
	expvar.Do(func(kv expvar.KeyValue) {
		if kv.Key == name {
			found = true
			sb.addExpVar(kv)
			if err := sb.statsdClient.Flush(); err != nil {
				t.Errorf("Error flushing: %s", err)
			}
			bytes := make([]byte, 4096)
			n, err := server.Read(bytes)
			if err != nil {
				t.Fatal(err)
			}
			result := string(bytes[:n])
			expected := "test.gauge_name:10.000000|g"
			assert.Equal(t, result, expected)
		}
	})
	if !found {
		t.Errorf("Stat %s not found...", name)
	}
}

func TestStatsdGaugeFunc(t *testing.T) {
	sb, server := getBackend(t)
	defer server.Close()
	name := "gauge_func_name"
	stats.NewGaugeFunc(name, "help", func() int64 {
		return 2
	})
	found := false
	expvar.Do(func(kv expvar.KeyValue) {
		if kv.Key == name {
			found = true
			sb.addExpVar(kv)
			if err := sb.statsdClient.Flush(); err != nil {
				t.Errorf("Error flushing: %s", err)
			}
			bytes := make([]byte, 4096)
			n, err := server.Read(bytes)
			if err != nil {
				t.Fatal(err)
			}
			result := string(bytes[:n])
			expected := "test.gauge_func_name:2.000000|g"
			assert.Equal(t, result, expected)
		}
	})
	if !found {
		t.Errorf("Stat %s not found...", name)
	}
}

func TestStatsdCounterDuration(t *testing.T) {
	sb, server := getBackend(t)
	defer server.Close()
	name := "counter_duration_name"
	s := stats.NewCounterDuration(name, "help")
	s.Add(1 * time.Millisecond)
	found := false
	expvar.Do(func(kv expvar.KeyValue) {
		if kv.Key == name {
			found = true
			sb.addExpVar(kv)
			if err := sb.statsdClient.Flush(); err != nil {
				t.Errorf("Error flushing: %s", err)
			}
			bytes := make([]byte, 4096)
			n, err := server.Read(bytes)
			if err != nil {
				t.Fatal(err)
			}
			result := string(bytes[:n])
			expected := "test.counter_duration_name:1.000000|ms"
			assert.Equal(t, result, expected)
		}
	})
	if !found {
		t.Errorf("Stat %s not found...", name)
	}
}

func TestStatsdCountersWithSingleLabel(t *testing.T) {
	sb, server := getBackend(t)
	defer server.Close()
	name := "counter_with_single_label_name"
	s := stats.NewCountersWithSingleLabel(name, "help", "label", "tag1", "tag2")
	s.Add("tag1", 2)
	found := false
	expvar.Do(func(kv expvar.KeyValue) {
		if kv.Key == name {
			found = true
			sb.addExpVar(kv)
			if err := sb.statsdClient.Flush(); err != nil {
				t.Errorf("Error flushing: %s", err)
			}
			bytes := make([]byte, 4096)
			n, err := server.Read(bytes)
			if err != nil {
				t.Fatal(err)
			}
			result := strings.Split(string(bytes[:n]), "\n")
			sort.Strings(result)
			expected := []string{
				"test.counter_with_single_label_name:0|c|#label:tag2",
				"test.counter_with_single_label_name:2|c|#label:tag1",
			}
			for i, res := range result {
				assert.Equal(t, res, expected[i])
			}
		}
	})
	if !found {
		t.Errorf("Stat %s not found...", name)
	}
}

func TestStatsdCountersWithMultiLabels(t *testing.T) {
	sb, server := getBackend(t)
	defer server.Close()
	name := "counter_with_multiple_label_name"
	s := stats.NewCountersWithMultiLabels(name, "help", []string{"label1", "label2"})
	s.Add([]string{"foo", "bar"}, 1)
	found := false
	expvar.Do(func(kv expvar.KeyValue) {
		if kv.Key == name {
			found = true
			sb.addExpVar(kv)
			if err := sb.statsdClient.Flush(); err != nil {
				t.Errorf("Error flushing: %s", err)
			}
			bytes := make([]byte, 4096)
			n, err := server.Read(bytes)
			if err != nil {
				t.Fatal(err)
			}
			result := string(bytes[:n])
			expected := "test.counter_with_multiple_label_name:1|c|#label1:foo,label2:bar"
			assert.Equal(t, result, expected)
		}
	})
	if !found {
		t.Errorf("Stat %s not found...", name)
	}
}

func TestStatsdCountersFuncWithMultiLabels(t *testing.T) {
	sb, server := getBackend(t)
	defer server.Close()
	name := "counter_func_with_multiple_labels_name"
	stats.NewCountersFuncWithMultiLabels(name, "help", []string{"label1", "label2"}, func() map[string]int64 {
		m := make(map[string]int64)
		m["foo.bar"] = 1
		m["bar.baz"] = 2
		return m
	})
	found := false
	expvar.Do(func(kv expvar.KeyValue) {
		if kv.Key == name {
			found = true
			sb.addExpVar(kv)
			if err := sb.statsdClient.Flush(); err != nil {
				t.Errorf("Error flushing: %s", err)
			}
			bytes := make([]byte, 4096)
			n, err := server.Read(bytes)
			if err != nil {
				t.Fatal(err)
			}
			result := strings.Split(string(bytes[:n]), "\n")
			sort.Strings(result)
			expected := []string{
				"test.counter_func_with_multiple_labels_name:1|c|#label1:foo,label2:bar",
				"test.counter_func_with_multiple_labels_name:2|c|#label1:bar,label2:baz",
			}
			for i, res := range result {
				assert.Equal(t, res, expected[i])
			}
		}
	})
	if !found {
		t.Errorf("Stat %s not found...", name)
	}
}

func TestStatsdGaugesWithMultiLabels(t *testing.T) {
	sb, server := getBackend(t)
	defer server.Close()
	name := "gauges_with_multiple_label_name"
	s := stats.NewGaugesWithMultiLabels(name, "help", []string{"label1", "label2"})
	s.Add([]string{"foo", "bar"}, 3)
	found := false
	expvar.Do(func(kv expvar.KeyValue) {
		if kv.Key == name {
			found = true
			sb.addExpVar(kv)
			if err := sb.statsdClient.Flush(); err != nil {
				t.Errorf("Error flushing: %s", err)
			}
			bytes := make([]byte, 4096)
			n, err := server.Read(bytes)
			if err != nil {
				t.Fatal(err)
			}
			result := string(bytes[:n])
			expected := "test.gauges_with_multiple_label_name:3.000000|g|#label1:foo,label2:bar"
			assert.Equal(t, result, expected)
		}
	})
	if !found {
		t.Errorf("Stat %s not found...", name)
	}
}

func TestStatsdGaugesFuncWithMultiLabels(t *testing.T) {
	sb, server := getBackend(t)
	defer server.Close()
	name := "gauges_func_with_multiple_labels_name"
	stats.NewGaugesFuncWithMultiLabels(name, "help", []string{"label1", "label2"}, func() map[string]int64 {
		m := make(map[string]int64)
		m["foo.bar"] = 1
		m["bar.baz"] = 2
		return m
	})
	found := false
	expvar.Do(func(kv expvar.KeyValue) {
		if kv.Key == name {
			found = true
			sb.addExpVar(kv)
			if err := sb.statsdClient.Flush(); err != nil {
				t.Errorf("Error flushing: %s", err)
			}
			bytes := make([]byte, 4096)
			n, err := server.Read(bytes)
			if err != nil {
				t.Fatal(err)
			}
			result := strings.Split(string(bytes[:n]), "\n")
			sort.Strings(result)
			expected := []string{
				"test.gauges_func_with_multiple_labels_name:1.000000|g|#label1:foo,label2:bar",
				"test.gauges_func_with_multiple_labels_name:2.000000|g|#label1:bar,label2:baz",
			}
			for i, res := range result {
				assert.Equal(t, res, expected[i])
			}
		}
	})
	if !found {
		t.Errorf("Stat %s not found...", name)
	}
}

func TestStatsdGaugesWithSingleLabel(t *testing.T) {
	sb, server := getBackend(t)
	defer server.Close()
	name := "gauges_with_single_label_name"
	s := stats.NewGaugesWithSingleLabel(name, "help", "label1")
	s.Add("bar", 1)
	found := false
	expvar.Do(func(kv expvar.KeyValue) {
		if kv.Key == name {
			found = true
			sb.addExpVar(kv)
			if err := sb.statsdClient.Flush(); err != nil {
				t.Errorf("Error flushing: %s", err)
			}
			bytes := make([]byte, 4096)
			n, err := server.Read(bytes)
			if err != nil {
				t.Fatal(err)
			}
			result := string(bytes[:n])
			expected := "test.gauges_with_single_label_name:1.000000|g|#label1:bar"
			assert.Equal(t, result, expected)
		}
	})
	if !found {
		t.Errorf("Stat %s not found...", name)
	}
}

func TestStatsdMultiTimings(t *testing.T) {
	sb, server := getBackend(t)
	defer server.Close()
	name := "multi_timings_name"
	s := stats.NewMultiTimings(name, "help", []string{"label1", "label2"})
	s.Add([]string{"foo", "bar"}, 10*time.Millisecond)
	found := false
	expvar.Do(func(kv expvar.KeyValue) {
		if kv.Key == name {
			found = true
			sb.addExpVar(kv)
			if err := sb.statsdClient.Flush(); err != nil {
				t.Errorf("Error flushing: %s", err)
			}
			bytes := make([]byte, 49152)
			n, err := server.Read(bytes)
			if err != nil {
				t.Fatal(err)
			}
			result := string(bytes[:n])
			expected := "test.multi_timings_name:10.000000|ms|#label1:foo,label2:bar"
			assert.Equal(t, result, expected)
		}
	})
	if !found {
		t.Errorf("Stat %s not found...", name)
	}
}

func TestStatsdTimings(t *testing.T) {
	sb, server := getBackend(t)
	defer server.Close()
	name := "timings_name"
	s := stats.NewTimings(name, "help", "label1")
	s.Add("foo", 2*time.Millisecond)
	found := false
	expvar.Do(func(kv expvar.KeyValue) {
		if kv.Key == name {
			found = true
			sb.addExpVar(kv)
			if err := sb.statsdClient.Flush(); err != nil {
				t.Errorf("Error flushing: %s", err)
			}
			bytes := make([]byte, 49152)
			n, err := server.Read(bytes)
			if err != nil {
				t.Fatal(err)
			}
			result := string(bytes[:n])
			expected := "test.timings_name:2.000000|ms|#label1:foo"
			assert.Equal(t, result, expected)
		}
	})
	if !found {
		t.Errorf("Stat %s not found...", name)
	}
}

func TestStatsdHistogram(t *testing.T) {
	sb, server := getBackend(t)
	defer server.Close()
	name := "histogram_name"
	s := stats.NewHistogram(name, "help", []int64{1, 5, 10})
	s.Add(2)
	s.Add(3)
	s.Add(6)
	found := false
	expvar.Do(func(kv expvar.KeyValue) {
		if kv.Key == name {
			found = true
			sb.addExpVar(kv)
			if err := sb.statsdClient.Flush(); err != nil {
				t.Errorf("Error flushing: %s", err)
			}
			bytes := make([]byte, 4096)
			n, err := server.Read(bytes)
			if err != nil {
				t.Fatal(err)
			}
			result := string(bytes[:n])
			expected := []string{
				"test.histogram_name:2.000000|h",
				"test.histogram_name:3.000000|h",
				"test.histogram_name:6.000000|h",
			}
			for i, res := range strings.Split(result, "\n") {
				assert.Equal(t, res, expected[i])
			}
		}
	})
	if !found {
		t.Errorf("Stat %s not found...", name)
	}
}
