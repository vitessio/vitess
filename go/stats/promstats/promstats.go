package promstats

import (
	"net/http"

	prom "github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func init() {
	http.Handle("/metrics", promhttp.Handler())
}

func NewCounter(name string, help string, labels []string) *prom.CounterVec {
	promCounter := prom.NewCounterVec(prom.CounterOpts{
		Name: name,
		Help: help,
	}, labels)

	//TODO: add error handling for if this gets called twice on the same string
	prom.MustRegister(promCounter)
	return promCounter
}

func Add(counter *prom.CounterVec, labels map[string]string) {
	counter.With(prom.Labels(labels)).Add(1)
}
