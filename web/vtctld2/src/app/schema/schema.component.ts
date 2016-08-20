import { Component, OnInit } from '@angular/core';
import { Dropdown } from 'primeng/primeng';
import { KeyspaceService } from '../api/keyspace.service';
import { ShardService } from '../api/shard.service';
import { TabletService } from '../api/tablet.service';
@Component({
  selector: 'vt-schema',
  templateUrl: './schema.component.html',
  styleUrls: ['./schema.component.css'],
  directives: [Dropdown],
  providers: [KeyspaceService, ShardService, TabletService],
})
export class SchemaComponent implements OnInit {
  keyspaces= [];
  selectedKeyspace= undefined;
  shards= [];
  selectedShard= undefined;
  tablets= [];
  selectedTablet= undefined;


  constructor(private keyspaceService: KeyspaceService, private shardService: ShardService, private tabletService: TabletService) {

  }

  ngOnInit() {
    this.getKeyspaceNames();
  }

  getKeyspaceNames() {
    this.keyspaceService.getKeyspaceNames().subscribe(keyspaceNames => {
      this.keyspaces = keyspaceNames.map(keyspaceName => {
        return {label: keyspaceName, value: keyspaceName};
      });
      this.keyspaces.sort(this.cmp);
      if (this.keyspaces.length > 0 ) {
        this.selectedKeyspace = this.keyspaces[0].value;
        this.getShards(this.selectedKeyspace);
      }
    });
  }

  cmp(a, b): number {
    let aLowercase = a.label.toLowerCase();
    let bLowercase = b.label.toLowerCase();
    if (aLowercase > bLowercase) {
      return 1;
    }
    if (bLowercase > aLowercase) {
      return -1;
    }
    return 0;
  }

  getShards(keyspaceName) {
    if (keyspaceName) {
      this.shardService.getShards(keyspaceName).subscribe(shards => {
        this.shards = shards.map(shard => {
          return {label: shard, value: shard};
        });
        if (this.shards.length > 0) {
          this.selectedShard = this.shards[0].value;
          this.getTablets(this.selectedKeyspace, this.selectedShard);
        }
      });
    }
    return [];
  }

  getTablets(keyspaceName, shardName) {
    this.tabletService.getTabletList(keyspaceName + '/' + shardName).subscribe(tablets => {
      console.log(tablets);
      this.tablets = tablets.map(tablet => {
        return {label: `${tablet.cell}-${tablet.uid}`, value: `${tablet.cell}-${tablet.uid}`};
      });
      if (this.tablets.length > 0) {
        this.selectedTablet = this.tablets[0].value;
        this.fetchSchema(this.selectedKeyspace, this.selectedShard, this.selectedTablet);
      }
    });
  }

  fetchSchema(keyspaceName, shardName, tabletName) {
    console.log('Fetching:', keyspaceName, shardName, tabletName);
  }
}
