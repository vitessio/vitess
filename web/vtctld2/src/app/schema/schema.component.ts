import { Component, OnInit } from '@angular/core';
import { Dropdown } from 'primeng/primeng';
import { Column, DataTable, Header } from 'primeng/primeng';

import { KeyspaceService } from '../api/keyspace.service';
import { ShardService } from '../api/shard.service';
import { TabletService } from '../api/tablet.service';
import { VtctlService } from '../api/vtctl.service';
@Component({
  selector: 'vt-schema',
  templateUrl: './schema.component.html',
  styleUrls: ['./schema.component.css'],
  directives: [Dropdown, Column, DataTable, Header],
  providers: [KeyspaceService, ShardService, TabletService, VtctlService],
})
export class SchemaComponent implements OnInit {
  keyspaces= [];
  selectedKeyspace= undefined;
  shards= [];
  selectedShard= undefined;
  tablets= [];
  selectedTablet= undefined;
  schemas = [];
  selectedSchema= undefined;
  vSchemas = [];
  selectedVSchema= undefined;

  constructor(private keyspaceService: KeyspaceService,
              private shardService: ShardService,
              private tabletService: TabletService,
              private vtctlService: VtctlService) {

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
    this.schemas = [];
    this.selectedSchema = undefined;
    this.vSchemas = [];
    this.selectedVSchema = undefined;
    this.vtctlService.sendPostRequest(this.vtctlService.vtctlUrl, ['GetSchema', tabletName]).subscribe(resp => {
      if (!resp.Error) {
        let schemaResp = JSON.parse(resp.Output);
        this.schemas = schemaResp.table_definitions;
      }
    });

    /*
    this.vtctlService.sendPostRequest(this.vtctlService.vtctlUrl, ['GetVSchema', tabletName]).subscribe(resp => {
      if (!resp.Error) {
        let vSchemaResp = JSON.parse(resp.Output);
        this.vSchemas = vSchemaResp.table_definitions;
      }
    });

    this.vtctlService.sendPostRequest(this.vtctlService.vtctlUrl, ['GetSrvVSchema', tabletName]).subscribe(resp => {
      console.log('SrvVSchema:', resp);
    });
    */
  }

  print(obj) {
     console.log(obj);
     return 'ABC';
  }

  parseColumns(schema) {
    let pks = {};
    let i = 1;
    for (let pk of schema.primary_key_columns) {
      pks[pk] = i;
      i++;
    }
    i = 0;
    return schema.columns.map(column => {
      i++;
      let pk_index = column in pks ?  pks[column].toString() : '~';
      let obj = {name: column, pk: pk_index, index: i};
      console.log('***', obj);
      return obj;
    });
  }
}
