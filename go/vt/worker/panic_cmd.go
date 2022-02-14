package worker

import (
	"flag"
	"html/template"
	"net/http"

	"vitess.io/vitess/go/vt/vterrors"

	"context"

	"vitess.io/vitess/go/vt/wrangler"
)

func commandPanic(wi *Instance, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) (Worker, error) {
	worker, err := NewPanicWorker(wr)
	if err != nil {
		return nil, vterrors.Wrap(err, "Could not create Panic worker")
	}
	return worker, nil
}

func interactivePanic(ctx context.Context, wi *Instance, wr *wrangler.Wrangler, w http.ResponseWriter, r *http.Request) (Worker, *template.Template, map[string]interface{}, error) {
	wrk, err := NewPanicWorker(wr)
	if err != nil {
		return nil, nil, nil, vterrors.Wrap(err, "Could not create Panic worker")
	}
	return wrk, nil, nil, nil
}

func init() {
	AddCommand("Debugging", Command{"Panic",
		commandPanic, interactivePanic,
		"<message>",
		"For internal tests only. Will call panic() when executed."})
}
