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
  styleUrls: ['./schema.component.css', '../styles/vt.style.css'],
  directives: [Dropdown, Column, DataTable, Header],
  providers: [KeyspaceService, ShardService, TabletService, VtctlService],
})
export class SchemaComponent implements OnInit {
  dialog= false;
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
      this.tablets = tablets.map(tablet => {
        let alias = `${tablet.cell}-${tablet.uid}`;
        return {label: alias, value: alias};
      });
      if (this.tablets.length > 0) {
        this.selectedTablet = this.tablets[0].value;
        this.fetchSchema(this.selectedKeyspace, this.selectedTablet);
      }
    });
  }

  fetchSchema(keyspaceName, tabletName) {
    this.schemas = [];
    this.selectedSchema = undefined;
    this.vSchemas = [];
    this.selectedVSchema = undefined;
    let schemaStream = this.vtctlService.sendPostRequest(this.vtctlService.vtctlUrl, ['GetSchema', tabletName]);
    let vScemaStream = this.vtctlService.sendPostRequest(this.vtctlService.vtctlUrl, ['GetVSchema', keyspaceName]);
    let allSchemaStream = schemaStream.combineLatest(vScemaStream);
    allSchemaStream.subscribe(streams => {
      let schemaResp = streams[0];
      let vSchemaResp = streams[1];

      if (!vSchemaResp.Error) {
        vSchemaResp = JSON.parse(vSchemaResp.Output);
        let vSchemas = Object.keys(vSchemaResp.vindexes).map(vname => {
          let vtype = vSchemaResp.vindexes[vname].type;
          let vparams = vSchemaResp.vindexes[vname].params ? vSchemaResp.vindexes[vname].params : '';
          let vowner = vSchemaResp.vindexes[vname].owner ? vSchemaResp.vindexes[vname].owner : '';
          return {name: vname, type: vtype, params: vparams, owner: vowner};
        });
        this.vSchemas = vSchemas;
      }

      if (!schemaResp.Error) {
        schemaResp = JSON.parse(schemaResp.Output);
        if (!vSchemaResp.Error) {
          let vindexes = this.createVindexMap(vSchemaResp.tables);
          this.schemas = schemaResp.table_definitions.map(table => {
            return this.parseColumns(table, vindexes);
          });
        } else {
          this.schemas = schemaResp.table_definitions.map(table => {
            return this.parseColumns(table);
          });
        }
      }
    });
  }

  createVindexMap(tables) {
    let vindexes = {};
    for (let tableName of Object.keys(tables)) {
      vindexes[tableName] = {};
      if (tables[tableName]['column_vindexes']) {
        for (let vindex of tables[tableName]['column_vindexes']) {
          vindexes[tableName][vindex.column] = { vindex: vindex.name};
        }
      }

      if (tables[tableName]['auto_increment']) {
        let sequence = tables[tableName]['auto_increment'];
        if (vindexes[tableName][sequence.column]) {
          vindexes[tableName][sequence.column]['sequence'] = sequence.sequence;
        } else {
          vindexes[tableName][sequence.column] = {sequence: sequence.sequence};
        }
      }
    }
    return vindexes;
  }

  parseColumns(table, vindexes= undefined) {
    let pks = {};
    let i = 1;
    for (let pk of table.primary_key_columns) {
      pks[pk] = i;
      i++;
    }
    let columnIndex = 1;
    table['columns'] = table.columns.map(column => {
      let pk_index = column in pks ?  pks[column].toString() : '~';
      let newColumn = {name: column, pk: pk_index, index: columnIndex};
      columnIndex++;
      if (vindexes) {
        if (vindexes[table.name]) {
          if (vindexes[table.name][column]) {
            if (vindexes[table.name][column]['vindex']) {
              newColumn['vindex'] = vindexes[table.name][column]['vindex'];
            }
            if (vindexes[table.name][column]['sequence']) {
              newColumn['sequence'] = vindexes[table.name][column]['sequence'];
            }
          }
        }
      }
      return newColumn;
    });
    return table;
  }
}
