package promstats

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/youtube/vitess/go/stats"

	pb "github.com/prometheus/client_model/go"
)

func TestInt(t *testing.T) {
	v := stats.NewInt("")
	c := NewCollector(prometheus.Opts{
		Name: "test_name",
		Help: "test_help",
	}, v)
	v.Set(1234)
	metrics := make(chan prometheus.Metric, 1)
	go c.Collect(metrics)
	met := <-metrics
	if want, got := `Desc{fqName: "test_name", help: "test_help", constLabels: {}, variableLabels: []}`, met.Desc().String(); want != got {
		t.Errorf("want %q, got %q", want, got)
	}
	m := pb.Metric{}
	met.Write(&m)
	if want, got := `gauge:<value:1234 > `, m.String(); want != got {
		t.Errorf("want %q, got %q", want, got)
	}
}

func TestIntFunc(t *testing.T) {
	v := stats.IntFunc(func() int64 { return 1234 })
	c := NewCollector(prometheus.Opts{
		Name: "test_name",
		Help: "test_help",
	}, v)
	metrics := make(chan prometheus.Metric, 1)
	go c.Collect(metrics)
	met := <-metrics
	if want, got := `Desc{fqName: "test_name", help: "test_help", constLabels: {}, variableLabels: []}`, met.Desc().String(); want != got {
		t.Errorf("want %q, got %q", want, got)
	}
	m := pb.Metric{}
	met.Write(&m)
	if want, got := `gauge:<value:1234 > `, m.String(); want != got {
		t.Errorf("want %q, got %q", want, got)
	}
}

func TestDuration(t *testing.T) {
	v := stats.NewDuration("")
	c := NewCollector(prometheus.Opts{
		Name: "test_name",
		Help: "test_help",
	}, v)
	v.Set(42 * time.Minute)
	metrics := make(chan prometheus.Metric, 1)
	go c.Collect(metrics)
	met := <-metrics
	if want, got := `Desc{fqName: "test_name", help: "test_help", constLabels: {}, variableLabels: []}`, met.Desc().String(); want != got {
		t.Errorf("want %q, got %q", want, got)
	}
	m := pb.Metric{}
	met.Write(&m)
	if want, got := `gauge:<value:2520 > `, m.String(); want != got {
		t.Errorf("want %q, got %q", want, got)
	}
}

func TestDurationFunc(t *testing.T) {
	v := stats.DurationFunc(func() time.Duration { return 42 * time.Minute })
	c := NewCollector(prometheus.Opts{
		Name: "test_name",
		Help: "test_help",
	}, v)
	metrics := make(chan prometheus.Metric, 1)
	go c.Collect(metrics)
	met := <-metrics
	if want, got := `Desc{fqName: "test_name", help: "test_help", constLabels: {}, variableLabels: []}`, met.Desc().String(); want != got {
		t.Errorf("want %q, got %q", want, got)
	}
	m := pb.Metric{}
	met.Write(&m)
	if want, got := `gauge:<value:2520 > `, m.String(); want != got {
		t.Errorf("want %q, got %q", want, got)
	}
}

func TestFloat(t *testing.T) {
	v := stats.NewFloat("Floaty McFloatface")
	c := NewCollector(prometheus.Opts{
		Name: "test_name",
		Help: "test_help",
	}, v)
	v.Set(1234)
	metrics := make(chan prometheus.Metric, 1)
	go c.Collect(metrics)
	met := <-metrics
	if want, got := `Desc{fqName: "test_name", help: "test_help", constLabels: {}, variableLabels: []}`, met.Desc().String(); want != got {
		t.Errorf("want %q, got %q", want, got)
	}
	m := pb.Metric{}
	met.Write(&m)
	if want, got := `gauge:<value:1234 > `, m.String(); want != got {
		t.Errorf("want %q, got %q", want, got)
	}
}

func TestFloatFunc(t *testing.T) {
	v := stats.FloatFunc(func() float64 { return 1234 })
	c := NewCollector(prometheus.Opts{
		Name: "test_name",
		Help: "test_help",
	}, v)
	metrics := make(chan prometheus.Metric, 1)
	go c.Collect(metrics)
	met := <-metrics
	if want, got := `Desc{fqName: "test_name", help: "test_help", constLabels: {}, variableLabels: []}`, met.Desc().String(); want != got {
		t.Errorf("want %q, got %q", want, got)
	}
	m := pb.Metric{}
	met.Write(&m)
	if want, got := `gauge:<value:1234 > `, m.String(); want != got {
		t.Errorf("want %q, got %q", want, got)
	}
}

func TestCounters(t *testing.T) {
	v := stats.NewCounters("")
	c := NewCollector(prometheus.Opts{
		Name: "test_name",
		Help: "test_help",
	}, v)
	v.Add("a", 1)
	metrics := make(chan prometheus.Metric, 1)
	go c.Collect(metrics)
	met := <-metrics
	if want, got := `Desc{fqName: "test_name", help: "test_help", constLabels: {}, variableLabels: [tag]}`, met.Desc().String(); want != got {
		t.Errorf("want %q, got %q", want, got)
	}
	m := pb.Metric{}
	met.Write(&m)
	if want, got := `label:<name:"tag" value:"a" > gauge:<value:1 > `, m.String(); want != got {
		t.Errorf("want %q, got %q", want, got)
	}
}

func TestCountersFunc(t *testing.T) {
	v := stats.CountersFunc(func() map[string]int64 { return map[string]int64{"a": 1234} })
	c := NewCollector(prometheus.Opts{
		Name: "test_name",
		Help: "test_help",
	}, v)
	metrics := make(chan prometheus.Metric, 1)
	go c.Collect(metrics)
	met := <-metrics
	if want, got := `Desc{fqName: "test_name", help: "test_help", constLabels: {}, variableLabels: [tag]}`, met.Desc().String(); want != got {
		t.Errorf("want %q, got %q", want, got)
	}
	m := pb.Metric{}
	met.Write(&m)
	if want, got := `label:<name:"tag" value:"a" > gauge:<value:1234 > `, m.String(); want != got {
		t.Errorf("want %q, got %q", want, got)
	}
}

func TestMultiCounters(t *testing.T) {
	v := stats.NewMultiCounters("", []string{"label1", "label2"})
	c := NewCollector(prometheus.Opts{
		Name: "test_name",
		Help: "test_help",
	}, v)
	v.Add([]string{"a", "b"}, 1)
	metrics := make(chan prometheus.Metric, 1)
	go c.Collect(metrics)
	met := <-metrics
	if want, got := `Desc{fqName: "test_name", help: "test_help", constLabels: {}, variableLabels: [label1 label2]}`, met.Desc().String(); want != got {
		t.Errorf("want %q, got %q", want, got)
	}
	m := pb.Metric{}
	if err := met.Write(&m); err != nil {
		t.Fatalf("met.Write(_): want nil, got %s", err)
	}
	if want, got := `label:<name:"label1" value:"a" > label:<name:"label2" value:"b" > gauge:<value:1 > `, m.String(); want != got {
		t.Errorf("want %q, got %q", want, got)
	}
}

func TestMultiCountersFunc(t *testing.T) {
	v := stats.NewMultiCountersFunc("", []string{"label1", "label2"}, func() map[string]int64 {
		return map[string]int64{
			"a.b": 1,
		}
	})
	c := NewCollector(prometheus.Opts{
		Name: "test_name",
		Help: "test_help",
	}, v)
	metrics := make(chan prometheus.Metric, 1)
	go c.Collect(metrics)
	met := <-metrics
	if want, got := `Desc{fqName: "test_name", help: "test_help", constLabels: {}, variableLabels: [label1 label2]}`, met.Desc().String(); want != got {
		t.Errorf("want %q, got %q", want, got)
	}
	m := pb.Metric{}
	if err := met.Write(&m); err != nil {
		t.Fatalf("met.Write(_): want nil, got %s", err)
	}
	if want, got := `label:<name:"label1" value:"a" > label:<name:"label2" value:"b" > gauge:<value:1 > `, m.String(); want != got {
		t.Errorf("want %q, got %q", want, got)
	}
}

func TestHistogram(t *testing.T) {
	v := stats.NewHistogram("", []int64{1, 3, 5, 7})
	c := NewCollector(prometheus.Opts{
		Name: "test_name",
		Help: "test_help",
	}, v)
	for i := int64(0); i < 10; i++ {
		v.Add(i)
	}
	metrics := make(chan prometheus.Metric, 1)
	go c.Collect(metrics)
	met := <-metrics
	if want, got := `Desc{fqName: "test_name", help: "test_help", constLabels: {}, variableLabels: []}`, met.Desc().String(); want != got {
		t.Errorf("want %q, got %q", want, got)
	}
	m := pb.Metric{}
	if err := met.Write(&m); err != nil {
		t.Fatalf("met.Write(_): want nil, got %s", err)
	}
	if want, got := `histogram:<sample_count:10 sample_sum:45 bucket:<cumulative_count:2 upper_bound:1 > bucket:<cumulative_count:4 upper_bound:3 > bucket:<cumulative_count:6 upper_bound:5 > bucket:<cumulative_count:8 upper_bound:7 > > `, m.String(); want != got {
		t.Errorf("want %q, got %q", want, got)
	}
}

func TestTimings(t *testing.T) {
	v := stats.NewTimings("")
	c := NewCollector(prometheus.Opts{
		Name: "test_name",
		Help: "test_help",
	}, v)
	for i := 100 * time.Microsecond; i < time.Second; i *= 2 {
		v.Add("a", i)
	}
	metrics := make(chan prometheus.Metric, 1)
	go c.Collect(metrics)
	met := <-metrics
	if want, got := `Desc{fqName: "test_name", help: "test_help", constLabels: {}, variableLabels: [category]}`, met.Desc().String(); want != got {
		t.Errorf("want %q, got %q", want, got)
	}
	m := pb.Metric{}
	if err := met.Write(&m); err != nil {
		t.Fatalf("met.Write(_): want nil, got %s", err)
	}
	if want, got := `label:<name:"category" value:"a" > histogram:<sample_count:14 sample_sum:1.6383 bucket:<cumulative_count:3 upper_bound:0.0005 > bucket:<cumulative_count:4 upper_bound:0.001 > bucket:<cumulative_count:6 upper_bound:0.005 > bucket:<cumulative_count:7 upper_bound:0.01 > bucket:<cumulative_count:9 upper_bound:0.05 > bucket:<cumulative_count:10 upper_bound:0.1 > bucket:<cumulative_count:13 upper_bound:0.5 > bucket:<cumulative_count:14 upper_bound:1 > bucket:<cumulative_count:14 upper_bound:5 > bucket:<cumulative_count:14 upper_bound:10 > > `, m.String(); want != got {
		t.Errorf("want %q, got %q", want, got)
	}
}

func TestMultiTimings(t *testing.T) {
	v := stats.NewMultiTimings("", []string{"label1", "label2"})
	c := NewCollector(prometheus.Opts{
		Name: "test_name",
		Help: "test_help",
	}, v)
	for i := 100 * time.Microsecond; i < time.Second; i *= 2 {
		v.Add([]string{"a", "b"}, i)
	}
	metrics := make(chan prometheus.Metric, 1)
	go c.Collect(metrics)
	met := <-metrics
	if want, got := `Desc{fqName: "test_name", help: "test_help", constLabels: {}, variableLabels: [label1 label2]}`, met.Desc().String(); want != got {
		t.Errorf("want %q, got %q", want, got)
	}
	m := pb.Metric{}
	if err := met.Write(&m); err != nil {
		t.Fatalf("met.Write(_): want nil, got %s", err)
	}
	if want, got := `label:<name:"label1" value:"a" > label:<name:"label2" value:"b" > histogram:<sample_count:14 sample_sum:1.6383 bucket:<cumulative_count:3 upper_bound:0.0005 > bucket:<cumulative_count:4 upper_bound:0.001 > bucket:<cumulative_count:6 upper_bound:0.005 > bucket:<cumulative_count:7 upper_bound:0.01 > bucket:<cumulative_count:9 upper_bound:0.05 > bucket:<cumulative_count:10 upper_bound:0.1 > bucket:<cumulative_count:13 upper_bound:0.5 > bucket:<cumulative_count:14 upper_bound:1 > bucket:<cumulative_count:14 upper_bound:5 > bucket:<cumulative_count:14 upper_bound:10 > > `, m.String(); want != got {
		t.Errorf("want %q, got %q", want, got)
	}
}
