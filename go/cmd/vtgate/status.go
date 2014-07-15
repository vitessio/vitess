package main

import (
	"github.com/youtube/vitess/go/vt/servenv"
	_ "github.com/youtube/vitess/go/vt/status"
)

var (
	topoTemplate = `
<style>
  table {
    border-collapse: collapse;
  }
  td, th {
    border: 1px solid #999;
    padding: 0.5rem;
  }
</style>
<table>
  <tr>
    <th colspan="2">SrvKeyspace Names Cache</th>
  </tr>
  <tr>
    <th>Cell</th>
    <th>SrvKeyspace Names</th>
  </tr>
  {{range $i, $skn := .SrvKeyspaceNames}}
  <tr>
    <td>{{github_com_youtube_vitess_vtctld_srv_cell $skn.Cell}}</td>
    <td>{{if $skn.LastError}}<b>{{$skn.LastError}}</b><br/>Client: {{$skn.LastErrorContext.HTML}}{{else}}{{range $j, $value := $skn.Value}}{{github_com_youtube_vitess_vtctld_srv_keyspace $skn.Cell $value}}&nbsp;{{end}}{{end}}</td>
  </tr>
  {{end}}
</table>
<br>
<table>
  <tr>
    <th colspan="3">SrvKeyspace Cache</th>
  </tr>
  <tr>
    <th>Cell</th>
    <th>Keyspace</th>
    <th>SrvKeyspace</th>
  </tr>
  {{range $i, $sk := .SrvKeyspaces}}
  <tr>
    <td>{{github_com_youtube_vitess_vtctld_srv_cell $sk.Cell}}</td>
    <td>{{github_com_youtube_vitess_vtctld_srv_keyspace $sk.Cell $sk.Keyspace}}</td>
    <td>{{if $sk.LastError}}<b>{{$sk.LastError}}</b><br/>Client: {{$sk.LastErrorContext.HTML}}{{else}}{{$sk.StatusAsHTML}}{{end}}</td>
  </tr>
  {{end}}
</table>
<br>
<table>
  <tr>
    <th colspan="5">EndPoints Cache</th>
  </tr>
  <tr>
    <th>Cell</th>
    <th>Keyspace</th>
    <th>Shard</th>
    <th>TabletType</th>
    <th>EndPoints</th>
  </tr>
  {{range $i, $ep := .EndPoints}}
  <tr>
    <td>{{github_com_youtube_vitess_vtctld_srv_cell $ep.Cell}}</td>
    <td>{{github_com_youtube_vitess_vtctld_srv_keyspace $ep.Cell $ep.Keyspace}}</td>
    <td>{{github_com_youtube_vitess_vtctld_srv_shard $ep.Cell $ep.Keyspace $ep.Shard}}</td>
    <td>{{github_com_youtube_vitess_vtctld_srv_type $ep.Cell $ep.Keyspace $ep.Shard $ep.TabletType}}</td>
    <td>{{if $ep.LastError}}<b>{{$ep.LastError}}</b><br/>Client: {{$ep.LastErrorContext.HTML}}{{else}}{{$ep.StatusAsHTML}}{{end}}</td>
  </tr>
  {{end}}
</table>
<small>This is just a cache, so some data may not be visible here yet.</small>
`

	statsTemplate = `
<style>
  #stats-charts div {
    display: inline-block;
  }
</style>

<table id="stats-charts">
  <tr>
    <td><div id="qps_by_db_type"></div></td>
    <td><div id="errors_by_db_type"></div></td>
  </tr>
  <tr>
    <td><div id="qps_by_keyspace"></div></td>
    <td><div id="errors_by_keyspace"></div></td>
  </tr>
  <tr>
    <td><div id="qps_by_operation"></div></td>
    <td><div id="errors_by_operation"></div></td>
  </tr>
</table>

<script type="text/javascript" src="https://www.google.com/jsapi"></script>

<script type="text/javascript">
google.load("jquery", "1.4.0");
google.load("visualization", "1", {packages:["corechart"]});

// minutesAgo returns the a time object representing i minutes before
// d.
function minutesAgo(d, i) {
  var copy = new Date(d);
  copy.setMinutes(copy.getMinutes() - i);
  return copy
}

// massageData takes rates from input and returns data that's suitable
// to present in a chart.
function massageData(input, now) {
  delete input['All'];
  var planTypes = Object.keys(input);
  if (planTypes.length === 0) {
    planTypes = ["All"];
    input["All"] = [];
  }

  var data = [["Time"].concat(planTypes)];

  for (var i = 0; i < 15; i++) {
    var datum = [minutesAgo(now, i)];
    for (var j = 0; j < planTypes.length; j++) {
      if (i < input[planTypes[0]].length) {
        datum.push(+input[planTypes[j]][i].toFixed(2));
      } else {
        datum.push(0);
      }
    }
    data.push(datum)
  }
  return data
}

var updateCallbacks = [];

function drawQPSChart(elId, key, title) {
  var div = $(elId).height(400).width(600).unwrap()[0]
  var chart = new google.visualization.AreaChart(div);

  var options = {
    title: title,
    focusTarget: 'category',
isStacked: true,
    vAxis: {
      viewWindow: {min: 0},
    }
  };

  var redrawing = function(input_data, now) {
    chart.draw(google.visualization.arrayToDataTable(massageData(input_data[key], now)), options);
  }

  updateCallbacks.push(redrawing)
}

function update() {
  var varzData;
  var up = function() {
  $.getJSON('/debug/vars', function(d) {
    for (var i = 0; i < updateCallbacks.length; i++) {
      updateCallbacks[i](d, new Date());
    }
  });
  }
  up()
  window.setInterval(up, 30000)
}

google.setOnLoadCallback(function() {
  drawQPSChart('#qps_by_db_type', 'QPSByDbType', 'QPS by DB type');
  drawQPSChart('#qps_by_keyspace', 'QPSByKeyspace', 'QPS by keyspace');
  drawQPSChart('#qps_by_operation', 'QPSByOperation', 'QPS by operation');

  drawQPSChart('#errors_by_db_type', 'ErrorsByDbType', 'Errors by DB type');
  drawQPSChart('#errors_by_keyspace', 'ErrorsByKeyspace', 'Errors by keyspace');
  drawQPSChart('#errors_by_operation', 'ErrorsByOperation', 'Errors by operation');
  update();
});

</script>
`
)

func init() {
	servenv.OnRun(func() {
		servenv.AddStatusPart("Topology Cache", topoTemplate, func() interface{} {
			return resilientSrvTopoServer.CacheStatus()
		})
		servenv.AddStatusPart("Stats", statsTemplate, func() interface{} {
			return nil
		})
	})
}
