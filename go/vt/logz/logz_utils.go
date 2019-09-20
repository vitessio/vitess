/*
Copyright 2019 The Vitess Authors.

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

// Package logz provides an infrastructure to expose a list of entries as
// a sortable table on a webpage.
//
// It is used by many internal vttablet pages e.g. /queryz, /querylogz, /schemaz
// /streamqueryz or /txlogz.
//
// See tabletserver/querylogz.go for an example how to use it.
package logz

import (
	"bytes"
	"net/http"
)

// StartHTMLTable writes the start of a logz-style table to an HTTP response.
func StartHTMLTable(w http.ResponseWriter) {
	w.Write([]byte(`<!DOCTYPE html>
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

                table.gridtable th.descending:before {
                  content: "▲";
                  float: left;
                }
                table.gridtable th.ascending:before {
                  content: "▼";
                  float: left;
                }
</style>

<script src="http://ajax.googleapis.com/ajax/libs/jquery/2.1.0/jquery.min.js"></script>

<script type="text/javascript">
$.fn.sortableByColumn = function() {
  var body = this.find('tbody');
  var header = this.find('thead');
  var contents = function(el, i) {
    var data = $(el).children('td').eq(i).text().toLowerCase();
    if (data == "n/a") {
      return -1;
    }

    var asNumber = parseFloat(data);
    if (data.slice(-1) == "s" && !isNaN(asNumber) && data.slice(0, -1) == asNumber) {
      // Return durations (e.g. "11.2s") always as number.
      return asNumber;
    }
    return data == asNumber ? asNumber : data;
  };

  this.find('thead tr th').each(function(index, th) {
    $(th).wrapInner('<div width="5em;"></div>');

    var direction = -1;
    $(th).click(function() {
      direction *= -1;

      header.find('th').removeClass('ascending descending');
      $(th).addClass(direction > 0? 'ascending' : 'descending');
      var rows = body.find('tr').detach();
      rows.sort(function(left, right) {
        var cl = contents(left, index);
        var cr = contents(right, index);
        if (cl === cr) {
          return 0
        } else {
          return contents(left, index) > contents(right, index)? direction : -direction;
        }
      });

      body.append(rows);
    });
  });
}

$(function() {
  $('table').sortableByColumn();
});
</script>

<table class="gridtable">
`))
}

// EndHTMLTable writes the end of a logz-style table to an HTTP response.
func EndHTMLTable(w http.ResponseWriter) {
	w.Write([]byte(`
</table>
`))
}

// Wrappable inserts zero-width whitespaces to make
// the string wrappable.
func Wrappable(in string) string {
	buf := bytes.NewBuffer(nil)
	for _, ch := range in {
		buf.WriteRune(ch)
		if ch == ',' || ch == ')' {
			// zero-width whitespace
			buf.WriteRune('\u200B')
		}
	}
	return buf.String()
}
