import { Component, EventEmitter, Input, Output, OnInit, ElementRef, ViewChild} from '@angular/core';
import { CORE_DIRECTIVES } from '@angular/common';
import { Subscription } from 'rxjs/Subscription';
import { Observable } from 'rxjs/Observable';
import { ROUTER_DIRECTIVES } from '@angular/router';

declare var Plotly: any;

@Component({
  moduleId: module.id,
  selector: 'plotlychart',
   template: `
    <div id="myPlotlyDiv"
         name="myPlotlyDiv">
        <!-- Plotly chart 1 will be drawn inside this DIV -->
    </div>
     <div id="myPlotlyDiv2"
         name="myPlotlyDiv">
        <!-- Plotly chart 2 will be drawn inside this DIV -->
    </div>
   `,
  styleUrls: [],
  directives: [CORE_DIRECTIVES, ROUTER_DIRECTIVES]
})

export class PlotlyComponent implements OnInit {

  colorscaleValue = [
    [0.0, '#17A234'],
    [1.0, '#A22417'],
  ];

  data = [];
  data2 = [];
  tablets = [];
  tablets2 = [];
  xLabels = [];
  xLabels2 = [];
  yLabels = [];
  yLabels2 = [];

  ngOnInit() {
    /* For heatmap 1 ( ks1 - cellA - all) */
    this.getDataForKC("ks1", "cellA");
    var chartInfo = [{
      z: this.data,
      colorscale: this.colorscaleValue,
      type: 'heatmap',
    }];
    var axisTemplate = {
      showgrid: false,
      zeroline: false,
      side: 'top',
      ticks: ''
    };
    var chartLayout = {
      xaxis: axisTemplate,
      yaxis: axisTemplate,
    };
    Plotly.newPlot('myPlotlyDiv', chartInfo, chartLayout,  {displayModeBar: false});

    /* For heatmap 2 (all - all - replica) */
    this.getDataForK("ks1");
    var chart2Info = [{
      z: this.data2,
      colorscale: this.colorscaleValue,
      type: 'heatmap'
    }];
    Plotly.newPlot('myPlotlyDiv2', chart2Info, chartLayout);
  }

  /* Needed to establish the on-click functionality of the heatmap. */
  ngAfterViewInit() {
    let elem = <any>(document.getElementById('myPlotlyDiv'));
    elem.on('plotly_click', function(data){
      alert("clicked");
    });
    let elem2 = <any>(document.getElementById('myPlotlyDiv2'));
    elem2.on('plotly_click', function(data){
      alert("clicked");
    });
  }

  /* Mock data for tablets */
  /* (TODO) remove this once vtcombo is set up */
  /* getDataForKC mimics a backend api call which returns a list of tablets
     for a keyspace and cell. */
  getDataForKC (keyspace: string, cell:string ) {
    var stats =  this.getStatsForKC(keyspace, cell);
    var prevType = stats[0].Tablet.type;
    var taskNum = 0;
    var maxShardNum = 0;
    var knownShards = [];
    var tempData = [];
    var tempTablets = [];
    var flag = false;

    for(var tabletStat in stats) {
      if(prevType != stats[tabletStat].Tablet.type ) {
        this.yLabels.push(prevType+ " - task " + taskNum)
        this.data.push(tempData.slice(0));
        this.tablets.push(tempTablets.slice(0));
        if(knownShards.length > maxShardNum) {
          maxShardNum = knownShards.length
        }
        tempData = [];
        tempTablets = [];
        knownShards = [];

        taskNum = 0;
        prevType = stats[tabletStat].Tablet.type;
        flag = true;
      }
      if(knownShards.indexOf(stats[tabletStat].Tablet.shard) != -1) {
        this.yLabels.push(prevType+ " - task " + taskNum)
        this.data.push(tempData.slice(0));
        this.tablets.push(tempTablets.slice(0));
        if(knownShards.length > maxShardNum) {
          maxShardNum = knownShards.length
        }
        tempData = [];
        tempTablets = [];
        knownShards = [];

        taskNum++;
        flag = true;
      }

      tempData.push(stats[tabletStat].Stats.seconds_behind_master);
      tempTablets.push(stats[tabletStat]);
      knownShards.push(stats[tabletStat].Tablet.shard);
    }
    this.data.push(tempData.slice(0));
    this.tablets.push(tempTablets.slice(0));
    this.yLabels.push(prevType+ " - task " + taskNum);

    for( var j = 0; j < maxShardNum; j++) { 
       this.xLabels.push("" + j);
    }
  }

  /* getDataForKC mimics a backend api call which returns a list of tablets 
     for a keyspace. */
  getDataForK (keyspace: string) {
    var stats = this.getStatsForK(keyspace);
    var prevType = stats[0].Tablet.type;
    var prevCell = stats[0].Tablet.alias.cell;
    var taskNum = 0;
    var maxShardNum = 0;
    var knownShards = [];
    var tempData = [];
    var tempTablets = [];
    var flag = false;

    for(var tabletStat in stats) {
      if(prevType != stats[tabletStat].Tablet.type ) {
        this.yLabels2.push(prevType+ " - task " + taskNum)
        this.data2.push(tempData.slice(0));
        this.tablets2.push(tempTablets.slice(0));
        if(knownShards.length > maxShardNum) {
          maxShardNum = knownShards.length
        }
        tempData = [];
        tempTablets = [];
        knownShards = [];
        taskNum = 0;
        prevType = stats[tabletStat].Tablet.type;
        prevCell = stats[tabletStat].Tablet.alias.cell;
        flag = true;
      }
      else if(prevCell != stats[tabletStat].Tablet.alias.cell) {
        this.yLabels2.push(prevType+ " - task " + taskNum)
        this.data2.push(tempData.slice(0));
        this.tablets2.push(tempTablets.slice(0));
        if(knownShards.length > maxShardNum) {
          maxShardNum = knownShards.length
        }
        tempData = [];
        tempTablets = [];
        knownShards = [];

        taskNum = 0;
        prevType = stats[tabletStat].Tablet.type;
        prevCell = stats[tabletStat].Tablet.alias.cell;
        flag = true;
      }
      else if(knownShards.indexOf(stats[tabletStat].Tablet.shard) != -1) {
        this.yLabels2.push(prevType+ " - task " + taskNum)
        this.data2.push(tempData.slice(0));
        this.tablets2.push(tempTablets.slice(0));
        if(knownShards.length > maxShardNum) {
          maxShardNum = knownShards.length
        }
        tempData = [];
        tempTablets = [];
        knownShards = [];
        taskNum++;
        flag = true;
      }

      tempData.push(stats[tabletStat].Stats.seconds_behind_master);
      tempTablets.push(stats[tabletStat]);
      knownShards.push(stats[tabletStat].Tablet.shard);
    }
    this.data2.push(tempData.slice(0));
    this.tablets2.push(tempTablets.slice(0));
    this.yLabels2.push(prevType+ " - task " + taskNum);

    for( var j = 0; j < maxShardNum; j++) {
       this.xLabels2.push("" + j);
    }
  }


  /*KEYSPACE 1 */
  tablet1 = {
    Tablet: {
       alias: {
        cell: "cellA",
        uid: 1,
       },
       keypsace: "ks1",
       shard: "-80",
       type: "master",
    },
    Target: {
      keyspace: "ks1",
      shard: "-80",
      type: "master",
    },
    Serving: true,
    Stats: {
      health_error: "",
      seconds_behind_master: 0,
      cpu_usage: 1.0,
      qps: 2.0,
    }
  }

  tablet2 = {
    Tablet: {
       alias: {
        cell: "cellB",
        uid: 2,
       },
       keypsace: "ks1",
       shard: "-80",
       type: "replica",
    },
    Target: {
      keyspace: "ks1",
      shard: "-80",
      type: "replica",
    },
    Serving: true,
    Stats: {
      health_error: "",
      seconds_behind_master: 5,
      cpu_usage: 2.0,
      qps: 3.0,
    }
  }

  tablet3 = {
    Tablet: {
       alias: {
        cell: "cellA", 
        uid: 3,
       },
       keypsace: "ks1",
       shard: "80-",
       type: "replica",
    },
    Target: {
      keyspace: "ks1",
      shard: "80-",
      type: "replica",
    },
    Serving: true,
    Stats: {
      health_error: "",
      seconds_behind_master: 3,
      cpu_usage: 1.0,
      qps: 2.0,
    }
  }

  tablet4 = {
    Tablet: {
       alias: {
        cell: "cellB", 
        uid: 4,
       },
       keypsace: "ks1",
       shard: "80-",
       type: "master",
    },
    Target: {
      keyspace: "ks1",
      shard: "80-",
      type: "master",
    },
    Serving: true,
    Stats: {
      health_error: "",
      seconds_behind_master: 2,
      cpu_usage: 1.0,
      qps: 2.0,
    }
  }


  /*KEYSPACE 2*/
  tablet5 = {
    Tablet: {
       alias: {
        cell: "cellA",
        uid: 5,
       },
       keypsace: "ks2",
       shard: "-40",
       type: "replica",
    },
    Target: {
      keyspace: "ks2",
      shard: "-40",
      type: "replica",
    },
    Serving: true,
    Stats: {
      health_error: "",
      seconds_behind_master: 0,
      cpu_usage: 1.0,
      qps: 2.0,
    }
  }

  tablet6 = {
    Tablet: {
       alias: {
        cell: "cellA",
        uid: 6,
       },
       keypsace: "ks2",
       shard: "-40",
       type: "master",
    },
    Target: {
      keyspace: "ks2",
      shard: "-40",
      type: "master",
    },
    Serving: true,
    Stats: {
      health_error: "",
      seconds_behind_master: 8,
      cpu_usage: 1.0,
      qps: 2.0,
    }
  }

  tablet7 = {
    Tablet: {
       alias: {
        cell: "cellB",
        uid: 7,
       },
       keypsace: "ks2",
       shard: "-40",
       type: "replica",
    },
    Target: {
      keyspace: "ks2",
      shard: "-40",
      type: "replica",
    },
    Serving: true,
    Stats: {
      health_error: "",
      seconds_behind_master: 4,
      cpu_usage: 1.0,
      qps: 2.0,
    }
  }

  tablet8 = {
    Tablet: {
       alias: {
        cell: "cellA",
        uid: 8,
       },
       keypsace: "ks2",
       shard: "40-80",
       type: "replica",
    },
    Target: {
      keyspace: "ks2",
      shard: "40-80",
      type: "replica",
    },
    Serving: true,
    Stats: {
      health_error: "",
      seconds_behind_master: 0,
      cpu_usage: 1.0,
      qps: 2.0,
    }
  }

  tablet9 = {
    Tablet: {
       alias: {
        cell: "cellB",
        uid: 9,
       },
       keypsace: "ks2",
       shard: "40-80",
       type: "master",
    },
    Target: {
      keyspace: "ks2",
      shard: "40-80",
      type: "master",
    },
    Serving: true,
    Stats: {
      health_error: "",
      seconds_behind_master: 0,
      cpu_usage: 1.0,
      qps: 2.0,
    }
  }

  tablet10 = {
    Tablet: {
       alias: {
        cell: "cellB",
        uid: 10,
       },
       keypsace: "ks2",
       shard: "40-80",
       type: "replica",
    },
    Target: {
      keyspace: "ks2",
      shard: "40-80",
      type: "replica",
    },
    Serving: true,
    Stats: {
      health_error: "",
      seconds_behind_master: 2,
      cpu_usage: 1.0,
      qps: 2.0,
    }
  }

  tablet11 = {
    Tablet: {
       alias: {
        cell: "cellA",
        uid: 11,
       },
       keypsace: "ks2",
       shard: "80-C0",
       type: "replica",
    },
    Target: {
      keyspace: "ks2",
      shard: "80-C0",
      type: "replica",
    },
    Serving: true,
    Stats: {
      health_error: "",
      seconds_behind_master: 0,
      cpu_usage: 1.0,
      qps: 2.0,
    }
  }

  tablet12 = {
    Tablet: {
       alias: {
        cell: "cellB",
        uid: 12,
       },
       keypsace: "ks2",
       shard: "80-C0",
       type: "master",
    },
    Target: {
      keyspace: "ks2",
      shard: "80-C0",
      type: "master",
    },
    Serving: true,
    Stats: {
      health_error: "",
      seconds_behind_master: 1,
      cpu_usage: 1.0,
      qps: 2.0,
    }
  }

  tablet13 = {
    Tablet: {
       alias: {
        cell: "cellB",
        uid: 13,
       },
       keypsace: "ks2",
       shard: "80-C0",
       type: "replica",
    },
    Target: {
      keyspace: "ks2",
      shard: "80-C0",
      type: "replica",
    },
    Serving: true,
    Stats: {
      health_error: "",
      seconds_behind_master: 2,
      cpu_usage: 1.0,
      qps: 2.0,
    },
  }

  tablet14 = {
    Tablet: {
       alias: {
        cell: "cellA",
        uid: 14,
       },
       keypsace: "ks2",
       shard: "C0-",
       type: "replica",
    },
    Target: {
      keyspace: "ks2",
      shard: "C0-",
      type: "replica",
    },
    Serving: true,
    Stats: {
      health_error: "",
      seconds_behind_master: 7,
      cpu_usage: 1.0,
      qps: 2.0,
    }
  }

  tablet15 = {
    Tablet: {
       alias: {
        cell: "cellA",
        uid: 15,
       },
       keypsace: "ks2",
       shard: "C0-",
       type: "replica",
    },
    Target: {
      keyspace: "ks2",
      shard: "C0-",
      type: "replica",
    },
    Serving: true,
    Stats: {
      health_error: "",
      seconds_behind_master: 1,
      cpu_usage: 1.0,
      qps: 2.0,
    }
  }

  tablet16 = {
    Tablet: {
       alias: {
        cell: "cellB",
        uid: 16,
       },
       keypsace: "ks2",
       shard: "C0-",
       type: "master",
    },
    Target: {
      keyspace: "ks2",
      shard: "C0-",
      type: "master",
    },
    Serving: true,
    Stats: {
      health_error: "",
      seconds_behind_master: 2,
      cpu_usage: 1.0,
      qps: 2.0,
    }
  }

  /*KEYSPACE 3*/
  tablet17 = {
    Tablet: {
       alias: {
        cell: "cellA",
        uid: 17,
       },
       keypsace: "ks3",
       shard: "0",
       type: "master",
    },
    Target: {
      keyspace: "ks3",
      shard: "0",
      type: "master",
    },
    Serving: true,
    Stats: {
      health_error: "",
      seconds_behind_master: 0,
      cpu_usage: 1.0,
      qps: 2.0,
    }
  }

  tablet18 = {
    Tablet: {
       alias: {
        cell: "cellB",
        uid: 18,
       },
       keypsace: "ks3",
       shard: "0",
       type: "replica",
    },
    Target: {
      keyspace: "ks3",
      shard: "0",
      type: "replica",
    },
    Serving: true,
    Stats: {
      health_error: "",
      seconds_behind_master: 0,
      cpu_usage: 1.0,
      qps: 2.0,
    }
  }

   tablet_status_map = [
    this.tablet1, this.tablet2, this.tablet3, this.tablet4, this.tablet5,
    this.tablet6, this.tablet7, this.tablet8, this.tablet9, this.tablet10,
    this.tablet11, this.tablet12, this.tablet13, this.tablet14,
    this.tablet15, this.tablet16, this.tablet17, this.tablet18
   ]

   getStatsForKC (keyspace:string, cell:string) {
      var tablet_statuses_to_return = []
      for ( var i = 0; i < this.tablet_status_map.length; i++)  {
        if ( keyspace === this.tablet_status_map[i].Tablet.keypsace && 
             cell === this.tablet_status_map[i].Tablet.alias.cell) {
             tablet_statuses_to_return.push(this.tablet_status_map[i]);
        }
      }
      return tablet_statuses_to_return;
   }

   getStatsForK (keyspace: string) {
      var tablet_statuses_to_return = []
      for ( var i = 0; i < this.tablet_status_map.length; i++)  {
        if ( keyspace === this.tablet_status_map[i].Tablet.keypsace) {
             tablet_statuses_to_return.push(this.tablet_status_map[i]);
        }
      }
      return tablet_statuses_to_return;
   }
}


