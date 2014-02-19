// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletserver

import "net/http"

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
		table.gridtable td {
			border-width: 1px;
			padding: 4px;
			border-style: solid;
		}
		</style>
		</head>
		<body>
		<table class = "gridtable">
	`))
}

func endHTMLTable(w http.ResponseWriter) {
	defer w.Write([]byte(`
		</table>
		</body>
		</html>
	`))
}
