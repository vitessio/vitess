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
