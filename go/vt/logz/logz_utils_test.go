/*
Copyright 2024 The Vitess Authors.

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

package logz

import (
	"testing"

	"net/http"
	"net/http/httptest"

	"github.com/stretchr/testify/require"
)

func TestWrappable(t *testing.T) {
	tests := []struct {
		input  string
		output string
	}{
		{
			input:  "s",
			output: "s",
		},
		{
			input:  "val,ue",
			output: "val,\u200bue",
		},
		{
			input:  ")al,ue",
			output: ")\u200bal,\u200bue",
		},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			require.Equal(t, tt.output, Wrappable(tt.input))
		})
	}
}

func TestStartAndEndHTMLTable(t *testing.T) {
	// Create a mock HTTP response writer
	w := httptest.NewRecorder()

	// Call the function to be tested
	StartHTMLTable(w)

	// Check the response status code
	require.Equal(t, http.StatusOK, w.Code)

	// Define the expected HTML content
	expectedHTML := `<!DOCTYPE html>
<style type="text/css">
		table.gridtable {
			font-family: verdana,arial,sans-serif;
			font-size:11px;
			border-width: 1px;
			border-collapse: collapse;
                        table-layout:fixed;
                        overflow: hidden;
		}
		table.gridtable th {
			border-width: 1px;
			padding: 8px;
			border-style: solid;
			background-color: #dedede;
			white-space: nowrap;
		}
		table.gridtable tr.low {
			background-color: #f0f0f0;
		}
		table.gridtable tr.medium {
			background-color: #ffcc00;
		}
		table.gridtable tr.high {
			background-color: #ff3300;
		}
                table.gridtable tr.error {
			background-color: #00ddff;
                }
		table.gridtable td {
			border-width: 1px;
			padding: 4px;
			border-style: solid;
		}
    table.gridtable th {
      padding-left: 2em;
      padding-right: 2em;
    }
    table.gridtable thead th {
      cursor: pointer;
    }

    table.gridtable th.descending:before {
      content: "▲";
      float: left;
    }
    table.gridtable th.ascending:before {
      content: "▼";
      float: left;
    }
</style>

<table class="gridtable">
`

	// Check if the response body matches the expected HTML content
	require.Contains(t, w.Body.String(), expectedHTML)

	// Call the function to be tested
	EndHTMLTable(w)

	// Check the response status code
	require.Equal(t, http.StatusOK, w.Code)

	expectedHTML = `
</table>
<script type="text/javascript">
function wrapInner(parent, element, style) {

  const wrapper = document.createElement(element);
  wrapper.style = style

  const div = parent.appendChild(wrapper);

  while(parent.firstChild !== wrapper) {
      wrapper.appendChild(parent.firstChild);
  }
}

function sortableByColumn(element) {
  const body = element.querySelector('tbody')
  const header = element.querySelector('thead')

  const contents = (element, i) => {
    const data = element.querySelectorAll("td")[i].innerText.toLowerCase();
    if (data == "n/a") {
      return -1;
    }

    var asNumber = parseFloat(data);
    if (data.slice(-1) == "s" && !isNaN(asNumber) && data.slice(0, -1) == asNumber) {
      // Return durations (e.g. "11.2s") always as number.
      return asNumber;
    }
    return data == asNumber ? asNumber : data;
  }

  element.querySelectorAll('thead tr th').forEach((th, index) => {
    wrapInner(th, "div", "overflow: auto; display: inline-block;");
    let direction = -1;

    th.onclick=() => {
      direction *= -1;
      const headerCells = header.querySelectorAll('th');
      headerCells.forEach((hc) => {
        hc.classList.remove('ascending', 'descending');
      })
      th.classList.add(direction > 0? 'ascending' : 'descending');
      const rows = body.querySelectorAll('tr');

      const sortedRows = Array.from(rows).sort(function(left, right) {
        var cl = contents(left, index);
        var cr = contents(right, index);
        if (cl === cr) {
          return 0
        } else {
          return contents(left, index) > contents(right, index)? direction : -direction;
        }
      });

      // Remove original rows
      rows.forEach(row => row.remove());

      // Add new sorted rows
      sortedRows.forEach(row => {
        body.appendChild(row);
      });
    }
  })
}

// Execute the function when the DOM loads
(function() {
  const table = document.querySelector('table');
  sortableByColumn(table)
})();
</script>
`

	// Check if the response body matches the expected HTML content
	require.Contains(t, w.Body.String(), expectedHTML)
}
