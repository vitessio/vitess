// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletserver

import (
	"bytes"
	"net/http"
)

func startHTMLTable(w http.ResponseWriter) {
	w.Write([]byte(`
		<!DOCTYPE html>
		<html>
		<head>
		<style type="text/css">
		table.gridtable {
			font-family: verdana,arial,sans-serif;
			font-size:11px;
			border-width: 1px;
			border-collapse: collapse;
                        table-layout:fixed;
                        overflow: hidden;
                        white-space: nowrap;
		}
		table.gridtable th {
			border-width: 1px;
			padding: 8px;
			border-style: solid;
			background-color: #dedede;
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
		</head>
		<body>
		<table class="gridtable">
	`))
}

func endHTMLTable(w http.ResponseWriter) {
	defer w.Write([]byte(`
</table>
<script src="http://ajax.googleapis.com/ajax/libs/jquery/2.1.0/jquery.min.js"></script>

<script type="text/javascript">
$.fn.sortableByColumn = function() {
  var body = this.find('tbody');
  var header = this.find('thead');
  var contents = function(el, i) {
    var data = $(el).children('td').eq(i).text().toLowerCase();
    var asNumber = parseFloat(data);
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
</body>
</html>`))
}

// wrappable inserts zero-width whitespaces to make
// the string wrappable.
func wrappable(in string) string {
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
