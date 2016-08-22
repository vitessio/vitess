package throttler

import (
	"fmt"
	"html/template"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/youtube/vitess/go/vt/logz"
)

const listHTML = `<!DOCTYPE html>
<title>{{len .Throttlers}} Active Throttler(s)</title>
<ul>
{{range .Throttlers}}
  <li>
    <a href="/throttlerz/{{.}}">{{.}}</a>
  </li>
{{end}}
</ul>
`

const detailsHTML = `<!DOCTYPE html>
<title>Details for Throttler '{{.}}'</title>
<a href="/throttlerz/{{.}}/log">adapative throttling log</a>
TODO(mberlin): Add graphs here.
`

const logHeaderHTML = `
  <style>
		table.gridtable th {
		  /* Override the nowrap default to avoid that the table overflows. */
			white-space: normal;
		}
  </style>
	<thead>
		<tr>
			<th>Now
			<th>Rate Change
			<th>Old Rate
			<th>New Rate
			<th>Tablet
			<th>Lag
			<th>Last Change
			<th>Actual Rate
			<th>Good/&#8203;Bad?
			<th>If Skipped
			<th>Highest Good
			<th>Lowest Bad
			<th>Old State
			<th>Tested State
			<th>New State
			<th>Lag Before
			<th>Recorded Ago
			<th>Master Rate
			<th>Slave Rate
			<th>Old Backlog
			<th>New Backlog
			<th>Reason
  <!-- Do not omit closing thead tag or the browser won't automaticall start a
       tbody tag and this will break the table sorting. -->
  </thead>
`

const logEntryHTML = `
    <tr class="{{.ColorLevel}}">
      <td>{{.Now.Format "15:04:05"}}
      <td>{{.RateChange}}
      <td>{{.OldRate}}
      <td>{{.NewRate}}
      <td>{{.Alias}}
      <td>{{.LagRecordNow.Stats.SecondsBehindMaster}}s
      <td>{{.TimeSinceLastRateChange}}
      <td>{{.CurrentRate}}
      <td>{{.GoodOrBad}}
      <td>{{.MemorySkipReason}}
      <td>{{.HighestGood}}
      <td>{{.LowestBad}}
      <td>{{.OldState}}
      <td>{{.TestedState}}
      <td>{{.NewState}}
      <td>{{.LagBefore}}
      <td>{{.AgeOfBeforeLag}}
      <td>{{.MasterRate}}
      <td>{{.GuessedSlaveRate}}
      <td>{{.GuessedSlaveBacklogOld}}
      <td>{{.GuessedSlaveBacklogNew}}
      <td>{{.Reason}}
`

const logFooterHTML = `
{{.Count}} lag records spanning the last {{.TimeSpan}} minutes are displayed.
`

var (
	listTemplate = template.Must(template.New("list").Parse(listHTML))

	detailsTemplate = template.Must(template.New("details").Parse(detailsHTML))

	logEntryTemplate  = template.Must(template.New("logEntry").Parse(logEntryHTML))
	logFooterTemplate = template.Must(template.New("logFooter").Parse(logFooterHTML))
)

func init() {
	http.HandleFunc("/throttlerz/", func(w http.ResponseWriter, r *http.Request) {
		throttlerzHandler(w, r, GlobalManager)
	})
}

func throttlerzHandler(w http.ResponseWriter, r *http.Request, m *managerImpl) {
	// Longest supported URL: /throttlerz/<name>/log
	parts := strings.Split(r.URL.Path, "/")

	switch len(parts) {
	case 3:
		name := parts[2]
		if name == "" {
			listThrottlers(w, m)
		} else {
			showThrottlerDetails(w, name)
		}
	case 4:
		name := parts[2]
		subPage := parts[3]
		switch subPage {
		case "log":
			showThrottlerLog(w, m, name)
		default:
			errMsg := fmt.Sprintf("invalid sub page: %v for throttler: %v: expected path: /throttlerz/<throttler name>/log", subPage, name)
			http.Error(w, errMsg, http.StatusInternalServerError)
		}
	default:
		errMsg := fmt.Sprintf("invalid /throttlerz path: %q expected paths: /throttlerz, /throttlerz/<throttler name> or /throttlerz/<throttler name>/log", r.URL.Path)
		http.Error(w, errMsg, http.StatusInternalServerError)
	}
}

func listThrottlers(w http.ResponseWriter, m *managerImpl) {
	throttlers := m.Throttlers()
	listTemplate.Execute(w, map[string]interface{}{
		"Throttlers": throttlers,
	})
}

func showThrottlerDetails(w http.ResponseWriter, name string) {
	detailsTemplate.Execute(w, name)
}

func showThrottlerLog(w http.ResponseWriter, m *managerImpl, name string) {
	results, err := m.Log(name)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	logz.StartHTMLTable(w)

	if _, err := io.WriteString(w, logHeaderHTML); err != nil {
		panic(fmt.Sprintf("failed to execute logHeader template: %v", err))
	}
	for _, r := range results {
		// Color based on the new state.
		var colorLevel string
		switch r.NewState {
		case stateIncreaseRate:
			colorLevel = "low"
		case stateDecreaseAndGuessRate:
			colorLevel = "medium"
		case stateEmergency:
			colorLevel = "high"
		}
		data := struct {
			result
			ColorLevel string
		}{r, colorLevel}

		if err := logEntryTemplate.Execute(w, data); err != nil {
			panic(fmt.Sprintf("failed to execute logEntry template: %v", err))
		}
	}

	logz.EndHTMLTable(w)

	// Print footer.
	count := len(results)
	var d time.Duration
	if count > 0 {
		d = results[0].Now.Sub(results[count-1].Now)
	}
	if err := logFooterTemplate.Execute(w, map[string]interface{}{
		"Count":    count,
		"TimeSpan": fmt.Sprintf("%.1f", d.Minutes()),
	}); err != nil {
		panic(fmt.Sprintf("failed to execute logFooter template: %v", err))
	}
}
