import { Component, OnInit } from '@angular/core'
import { CORE_DIRECTIVES } from '@angular/common';
import { PolymerElement } from '@vaadin/angular2-polymer';

@Component({
  selector: 'Ploymer chart',
 template: `
  <vaadin-heatmap-chart id="heat-map">
  <chart-title>Sales per employee per weekday</chart-title>
  <x-axis>
    <categories>{{xLabels}}</categories>
  </x-axis>
  <y-axis title>
    <categories>master-task 0, replica-task 0</categories>
  </y-axis>
  <color-axis min="0" min-color="#FFFFFF" max-color="#3090F0"></color-axis>
  <legend align="right" layout="vertical" margin="0" vertical-align="top" y="25" symbol-height="280"></legend>
  <plot-options>
    <heatmap>
      <data-labels enabled="true" color="#000000"></data-labels>
    </heatmap>
  </plot-options>
  <data-series name="Sales per employee">
    <data>
      [0, 0, 0], [1, 1, 3]
      /*[0, 0, 10], [1,0, 1]
      [0, 0, 10], [0, 1, 19], [0, 2, 8], [0, 3, 24], [0, 4, 67], [1, 0, 92], [1, 1, 58], [1, 2, 78], [1, 3, 117], [1, 4, 48], [2, 0, 35], [2, 1, 15], [2, 2, 123], [2, 3, 64], [2, 4, 52], [3, 0, 72], [3, 1, 132], [3, 2, 114], [3, 3, 19], [3, 4, 16], [4, 0, 38],
      [4, 1, 5], [4, 2, 8], [4, 3, 117], [4, 4, 115], [5, 0, 88], [5, 1, 32], [5, 2, 12], [5, 3, 6], [5, 4, 120], [6, 0, 13], [6, 1, 44], [6, 2, 88], [6, 3, 98], [6, 4, 96], [7, 0, 31], [7, 1, 1], [7, 2, 82], [7, 3, 32], [7, 4, 30], [8, 0, 85],
      [8, 1, 97], [8, 2, 123], [8, 3, 64], [8, 4, 84], [9, 0, 47], [9, 1, 114], [9, 2, 31], [9, 3, 48], [9, 4, 91]*/
    </data>
  </data-series>
</vaadin-heatmap-chart>
  `,
  directives: [ PolymerElement('vaadin-heatmap-chart') ]

})

export class ChartComponent implements OnInit {

  data = [];
  tablets = [];
  xLabels = [];
  yLabels = [];

   getShardNumber(shardName: string) {
    if(shardName === "0") {
      return 0;
    }
    var parts = shardName.split("-");
    var lowerBound = parts[0];
    var upperBound = parts[1];
    if(lowerBound === "") {
      return 0;
    }
    if(upperBound === "") {
      var len = lowerBound.length;
      if(len == 1) { upperBound = "F"; }
      else if(len == 2) { upperBound = "FF"; }
      else if(len == 3) { upperBound = "FFF"; }
      else if(len == 4) { upperBound = "FFFF"; }
    }
    var interval = (parseInt(upperBound,16)+1) - parseInt(lowerBound, 16);
    let num = parseInt(lowerBound, 16)/interval;
    return num;
  }

  getString() { 
    return "" + this.yLabels[0] + ", " + this.yLabels[1];
  }


   getDataForKC (keyspace: string, cell:string ) {
    var stats =  this.getStatsForKC(keyspace, cell);
    var prevShardNum = -1;
    var shardNum = -1;
    var maxShardNum = 0;
    var taskNum = 0;
    var taskIndex = 0;
    var type = "";
    var prevType = "";

    for(var tabletStat in stats) {
      shardNum = this.getShardNumber(stats[tabletStat].Tablet.shard);
      if(shardNum > maxShardNum) {
        maxShardNum = shardNum;
      }
      type = stats[tabletStat].Tablet.type;
      if(prevType != type) {
        this.yLabels.push(type + " - task " + taskNum);
        taskIndex++;
        taskNum = 0;
        prevType = type;
      }
      else if (prevShardNum > shardNum) {
        this.yLabels.push(type + " - task " + taskNum);
        taskNum++;
        taskIndex++;
      }
      var tempArr = [shardNum, taskIndex-1, stats[tabletStat].Stats.seconds_behind_master];
      console.dir("tempArr: " + tempArr);
      this.data.push(tempArr);
      prevShardNum = shardNum;
    }
    for( var j = 0; j < maxShardNum+1; j++) {
       this.xLabels.push("" + j);
    }
    console.log("Data: " + this.data);
    console.log("x: " + this.xLabels);
    console.log("y: " + this.yLabels);
 }

  ngOnInit() { 
    this.getDataForKC("ks1", "cellA");
    this.data = [
      [0, 0, 10],
      [0, 1, 1]
    ];
  }


  /*************************DATA*************************/
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
             console.dir("PUSHING: " + this.tablet_status_map[i]);
             tablet_statuses_to_return.push(this.tablet_status_map[i]);
        }
      }
      console.dir("in other func: " +tablet_statuses_to_return);
      return tablet_statuses_to_return;
   }

   getStatsForK (keyspace: string) {
      var tablet_statuses_to_return = []
      for ( var i = 0; i < this.tablet_status_map.length; i++)  {
        if ( keyspace === this.tablet_status_map[i].Tablet.keypsace) {
             console.dir("PUSHING: " + this.tablet_status_map[i]);
             tablet_statuses_to_return.push(this.tablet_status_map[i]);
        }
      }
      console.dir("in other func: " +tablet_statuses_to_return);
      return tablet_statuses_to_return;

   }
}




