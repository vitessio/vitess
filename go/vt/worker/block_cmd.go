package worker

import (
	"flag"
	"html/template"
	"net/http"

	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"

	"context"

	"vitess.io/vitess/go/vt/wrangler"
)

const blockHTML = `
<!DOCTYPE html>
<head>
  <title>Block Action</title>
</head>
<body>
  <h1>Block Action</h1>

    {{if .Error}}
      <b>Error:</b> {{.Error}}</br>
    {{else}}
	    <form action="/Debugging/Block" method="post">
	      <INPUT type="submit" name="submit" value="Block (until canceled)"/>
    </form>
    {{end}}
</body>
`

var blockTemplate = mustParseTemplate("block", blockHTML)

func commandBlock(wi *Instance, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) (Worker, error) {
	if err := subFlags.Parse(args); err != nil {
		return nil, err
	}
	if subFlags.NArg() != 0 {
		subFlags.Usage()
		return nil, vterrors.New(vtrpc.Code_INVALID_ARGUMENT, "command Block does not accept any parameter")
	}

	worker, err := NewBlockWorker(wr)
	if err != nil {
		return nil, vterrors.Wrap(err, "Could not create Block worker")
	}
	return worker, nil
}

func interactiveBlock(ctx context.Context, wi *Instance, wr *wrangler.Wrangler, w http.ResponseWriter, r *http.Request) (Worker, *template.Template, map[string]interface{}, error) {
	if err := r.ParseForm(); err != nil {
		return nil, nil, nil, vterrors.Wrap(err, "cannot parse form")
	}

	if submit := r.FormValue("submit"); submit == "" {
		result := make(map[string]interface{})
		return nil, blockTemplate, result, nil
	}

	wrk, err := NewBlockWorker(wr)
	if err != nil {
		return nil, nil, nil, vterrors.Wrap(err, "Could not create Block worker")
	}
	return wrk, nil, nil, nil
}

func init() {
	AddCommand("Debugging", Command{"Block",
		commandBlock, interactiveBlock,
		"<message>",
		"For internal tests only. When triggered, the command will block until canceled."})
}
