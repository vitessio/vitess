package promstats

import (
	"expvar"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/youtube/vitess/go/stats"

	pb "github.com/prometheus/client_model/go"
)

func testMetric(t *testing.T, v expvar.Var, load func(), desc string, vals ...string) {
	coll := NewCollector(prometheus.Opts{
		Name: "test_name",
		Help: "test_help",
	}, v)
	if load != nil {
		load()
	}
	ch := make(chan prometheus.Metric, 1)
	go func() {
		coll.Collect(ch)
		close(ch)
	}()
	for _, val := range vals {
		met, ok := <-ch
		if !ok {
			t.Error("coll.Collect(ch): too few metrics returned")
		}
		if got := met.Desc().String(); got != desc {
			t.Errorf("met.Desc().String(): %q, want %q", got, desc)
		}
		m := pb.Metric{}
		if err := met.Write(&m); err != nil {
			t.Fatalf("met.Write(): err=%s, want nil", err)
		}
		if got := m.String(); got != val {
			t.Errorf("met.Write(&m); m.String(): %q, want %q", got, val)
		}
	}
	_, ok := <-ch
	if ok {
		t.Error("coll.Collect(ch): too many metrics returned")
	}
}

func TestInt(t *testing.T) {
	v := stats.NewInt("")
	load := func() {
		v.Set(1234)
	}
	testMetric(t, v, load,
		`Desc{fqName: "test_name", help: "test_help", constLabels: {}, variableLabels: []}`,
		`gauge:<value:1234 > `,
	)
}

func TestIntFunc(t *testing.T) {
	f := func() int64 {
		return 1234
	}
	v := stats.IntFunc(f)
	testMetric(t, v, nil,
		`Desc{fqName: "test_name", help: "test_help", constLabels: {}, variableLabels: []}`,
		`gauge:<value:1234 > `,
	)
}

func TestDuration(t *testing.T) {
	v := stats.NewDuration("")
	load := func() {
		v.Set(42 * time.Minute)
	}
	testMetric(t, v, load,
		`Desc{fqName: "test_name", help: "test_help", constLabels: {}, variableLabels: []}`,
		`gauge:<value:2520 > `,
	)
}

func TestDurationFunc(t *testing.T) {
	f := func() time.Duration {
		return 42 * time.Minute
	}
	v := stats.DurationFunc(f)
	testMetric(t, v, nil,
		`Desc{fqName: "test_name", help: "test_help", constLabels: {}, variableLabels: []}`,
		`gauge:<value:2520 > `,
	)
}

func TestFloat(t *testing.T) {
	v := stats.NewFloat("Floaty McFloatface")
	load := func() {
		v.Set(1234)
	}
	testMetric(t, v, load,
		`Desc{fqName: "test_name", help: "test_help", constLabels: {}, variableLabels: []}`,
		`gauge:<value:1234 > `,
	)
}

func TestFloatFunc(t *testing.T) {
	f := func() float64 {
		return 1234
	}
	v := stats.FloatFunc(f)
	testMetric(t, v, nil,
		`Desc{fqName: "test_name", help: "test_help", constLabels: {}, variableLabels: []}`,
		`gauge:<value:1234 > `,
	)
}

func TestCounters(t *testing.T) {
	v := stats.NewCounters("")
	load := func() {
		v.Add("a", 1)
	}
	testMetric(t, v, load,
		`Desc{fqName: "test_name", help: "test_help", constLabels: {}, variableLabels: [tag]}`,
		`label:<name:"tag" value:"a" > gauge:<value:1 > `,
	)
}

func TestCountersFunc(t *testing.T) {
	f := func() map[string]int64 {
		return map[string]int64{"a": 1234}
	}
	v := stats.CountersFunc(f)
	testMetric(t, v, nil,
		`Desc{fqName: "test_name", help: "test_help", constLabels: {}, variableLabels: [tag]}`,
		`label:<name:"tag" value:"a" > gauge:<value:1234 > `,
	)
}

func TestMultiCounters(t *testing.T) {
	v := stats.NewMultiCounters("", []string{"label1", "label2"})
	load := func() {
		v.Add([]string{"a", "b"}, 1)
	}
	testMetric(t, v, load,
		`Desc{fqName: "test_name", help: "test_help", constLabels: {}, variableLabels: [label1 label2]}`,
		`label:<name:"label1" value:"a" > label:<name:"label2" value:"b" > gauge:<value:1 > `,
	)
}

func TestMultiCountersFunc(t *testing.T) {
	f := func() map[string]int64 {
		return map[string]int64{
			"a.b": 1,
		}
	}
	v := stats.NewMultiCountersFunc("", []string{"label1", "label2"}, f)
	testMetric(t, v, nil,
		`Desc{fqName: "test_name", help: "test_help", constLabels: {}, variableLabels: [label1 label2]}`,
		`label:<name:"label1" value:"a" > label:<name:"label2" value:"b" > gauge:<value:1 > `,
	)
}

func TestHistogram(t *testing.T) {
	v := stats.NewHistogram("", []int64{1, 3, 5, 7})
	load := func() {
		for i := int64(0); i < 10; i++ {
			v.Add(i)
		}
	}
	testMetric(t, v, load,
		`Desc{fqName: "test_name", help: "test_help", constLabels: {}, variableLabels: []}`,
		`histogram:<sample_count:10 sample_sum:45 bucket:<cumulative_count:2 upper_bound:1 > bucket:<cumulative_count:4 upper_bound:3 > bucket:<cumulative_count:6 upper_bound:5 > bucket:<cumulative_count:8 upper_bound:7 > > `,
	)
}

func TestTimings(t *testing.T) {
	v := stats.NewTimings("")
	load := func() {
		for i := 100 * time.Microsecond; i < time.Second; i *= 2 {
			v.Add("a", i)
		}
	}
	testMetric(t, v, load,
		`Desc{fqName: "test_name", help: "test_help", constLabels: {}, variableLabels: [category]}`,
		`label:<name:"category" value:"a" > histogram:<sample_count:14 sample_sum:1.6383 bucket:<cumulative_count:3 upper_bound:0.0005 > bucket:<cumulative_count:4 upper_bound:0.001 > bucket:<cumulative_count:6 upper_bound:0.005 > bucket:<cumulative_count:7 upper_bound:0.01 > bucket:<cumulative_count:9 upper_bound:0.05 > bucket:<cumulative_count:10 upper_bound:0.1 > bucket:<cumulative_count:13 upper_bound:0.5 > bucket:<cumulative_count:14 upper_bound:1 > bucket:<cumulative_count:14 upper_bound:5 > bucket:<cumulative_count:14 upper_bound:10 > > `,
	)
}

func TestMultiTimings(t *testing.T) {
	v := stats.NewMultiTimings("", []string{"label1", "label2"})
	load := func() {
		for i := 100 * time.Microsecond; i < time.Second; i *= 2 {
			v.Add([]string{"a", "b"}, i)
		}
	}
	testMetric(t, v, load,
		`Desc{fqName: "test_name", help: "test_help", constLabels: {}, variableLabels: [label1 label2]}`,
		`label:<name:"label1" value:"a" > label:<name:"label2" value:"b" > histogram:<sample_count:14 sample_sum:1.6383 bucket:<cumulative_count:3 upper_bound:0.0005 > bucket:<cumulative_count:4 upper_bound:0.001 > bucket:<cumulative_count:6 upper_bound:0.005 > bucket:<cumulative_count:7 upper_bound:0.01 > bucket:<cumulative_count:9 upper_bound:0.05 > bucket:<cumulative_count:10 upper_bound:0.1 > bucket:<cumulative_count:13 upper_bound:0.5 > bucket:<cumulative_count:14 upper_bound:1 > bucket:<cumulative_count:14 upper_bound:5 > bucket:<cumulative_count:14 upper_bound:10 > > `,
	)
}
