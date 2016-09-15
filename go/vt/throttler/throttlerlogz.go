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

const logHeaderHTML = `
  <style>
		table.gridtable th {
		  /* Override the nowrap default to avoid that the table overflows. */
			white-space: normal;
		}
  </style>
	<thead>
		<tr>
			<th>Now</th>
			<th>Rate Change</th>
			<th>Old Rate</th>
			<th>New Rate</th>
			<th>Tablet</th>
			<th>Lag</th>
			<th>Last Change</th>
			<th>Actual Rate</th>
			<th>Good/&#8203;Bad?</th>
			<th>If Skipped</th>
			<th>Highest Good</th>
			<th>Lowest Bad</th>
			<th>Old State</th>
			<th>Tested State</th>
			<th>New State</th>
			<th>Lag Before</th>
			<th>Recorded Ago</th>
			<th>Master Rate</th>
			<th>Slave Rate</th>
			<th>Old Backlog</th>
			<th>New Backlog</th>
			<th>Reason</th>
	  </tr>
  </thead>
`

const logEntryHTML = `
    <tr class="{{.ColorLevel}}">
      <td>{{.Now.Format "15:04:05"}}</td>
      <td>{{.RateChange}}</td>
      <td>{{.OldRate}}</td>
      <td>{{.NewRate}}</td>
      <td>{{.Alias}}</td>
      <td>{{.LagRecordNow.Stats.SecondsBehindMaster}}s</td>
      <td>{{.TimeSinceLastRateChange}}</td>
      <td>{{.CurrentRate}}</td>
      <td>{{.GoodOrBad}}</td>
      <td>{{.MemorySkipReason}}</td>
      <td>{{.HighestGood}}</td>
      <td>{{.LowestBad}}</td>
      <td>{{.OldState}}</td>
      <td>{{.TestedState}}</td>
      <td>{{.NewState}}</td>
      <td>{{.LagBefore}}</td>
      <td>{{.AgeOfBeforeLag}}</td>
      <td>{{.MasterRate}}</td>
      <td>{{.GuessedSlaveRate}}</td>
      <td>{{.GuessedSlaveBacklogOld}}</td>
      <td>{{.GuessedSlaveBacklogNew}}</td>
      <td>{{.Reason}}</td>
    </tr>
`

const logFooterHTML = `
{{.Count}} lag records spanning the last {{.TimeSpan}} minutes are displayed.
`

var (
	logEntryTemplate  = template.Must(template.New("logEntry").Parse(logEntryHTML))
	logFooterTemplate = template.Must(template.New("logFooter").Parse(logFooterHTML))
)

func init() {
	http.HandleFunc("/throttlerlogz/", func(w http.ResponseWriter, r *http.Request) {
		throttlerlogzHandler(w, r, GlobalManager)
	})
}

func throttlerlogzHandler(w http.ResponseWriter, r *http.Request, m *managerImpl) {
	// Longest supported URL: /throttlerlogz/<name>
	parts := strings.SplitN(r.URL.Path, "/", 3)

	if len(parts) != 3 {
		errMsg := fmt.Sprintf("invalid /throttlerlogz path: %q expected paths: /throttlerlogz/ or /throttlerlogz/<throttler name>", r.URL.Path)
		http.Error(w, errMsg, http.StatusInternalServerError)
		return
	}

	name := parts[2]
	if name == "" {
		// If no name is given, redirect to the list of throttlers at /throttlerz.
		http.Redirect(w, r, "/throttlerz", http.StatusTemporaryRedirect)
		return
	}

	showThrottlerLog(w, m, name)
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
		// Color based on max(tested state, new state).
		state := r.TestedState
		if stateGreater(r.NewState, state) {
			state = r.NewState
		}
		var colorLevel string
		switch state {
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
