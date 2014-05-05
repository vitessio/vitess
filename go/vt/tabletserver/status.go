package tabletserver

import (
	"github.com/youtube/vitess/go/vt/servenv"
)

// This file contains the status web page export for tabletserver

var queryserviceStatusTemplate = `
State: {{.State}}<br>
<div id="qps_chart">QPS: {{.CurrentQPS}}</div>
<script type="text/javascript" src="https://www.google.com/jsapi"></script>
<script type="text/javascript">

google.load("jquery", "1.4.0");
google.load("visualization", "1", {packages:["corechart"]});

function minutesAgo(d, i) {
  var copy = new Date(d);
  copy.setMinutes(copy.getMinutes() - i);
  return copy
}

function drawQPSChart() {
  var div = $('#qps_chart').height(500).width(900).unwrap()[0]
  var chart = new google.visualization.LineChart(div);

  var options = {
    title: "QPS",
    focusTarget: 'category',
    vAxis: {
      viewWindow: {min: 0},
    }
  };


  var redraw = function() {
    $.getJSON("/debug/vars", function(input_data) {
      var now = new Date();
      var qps = input_data.QPS;
      var planTypes = Object.keys(qps);
      if (planTypes.length === 0) {
        planTypes = ["All"];
        qps["All"] = [];
      }

      var data = [["Time"].concat(planTypes)];

      for (var i = 0; i < 15; i++) {
        var datum = [minutesAgo(now, i)];
        for (var j = 0; j < planTypes.length; j++) {
          if (i < qps.All.length) {
            datum.push(+qps[planTypes[j]][i].toFixed(2));
          } else {
            datum.push(0);
          }
        }
        data.push(datum)
      }
      chart.draw(google.visualization.arrayToDataTable(data), options);
    })
  };

  redraw();

  // redraw every 30 seconds.
  window.setInterval(redraw, 30000);
}
google.setOnLoadCallback(drawQPSChart);
</script>

`

type queryserviceStatus struct {
	State      string
	CurrentQPS float64
}

// AddStatusPart registers the status part for the status page.
func AddStatusPart() {
	servenv.AddStatusPart("Queryservice", queryserviceStatusTemplate, func() interface{} {
		status := queryserviceStatus{
			State: SqlQueryRpcService.GetState(),
		}
		rates := QPSRates.Get()
		if qps, ok := rates["All"]; ok && len(qps) > 0 {
			status.CurrentQPS = qps[0]

		}
		return status
	})
}
