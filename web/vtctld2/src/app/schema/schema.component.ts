import { Component, OnInit } from '@angular/core';

import { KeyspaceService } from '../api/keyspace.service';
import { ShardService } from '../api/shard.service';
import { TabletService } from '../api/tablet.service';
import { VtctlService } from '../api/vtctl.service';
@Component({
  selector: 'vt-schema',
  templateUrl: './schema.component.html',
  styleUrls: ['./schema.component.css', '../styles/vt.style.css'],
})
export class SchemaComponent implements OnInit {
  dialog = false;
  keyspaces = [];
  selectedKeyspace: any;
  shards = [];
  selectedShard: any;
  tablets = [];
  selectedTablet: any;
  schemas = [];
  selectedSchema: any;
  vSchemas = [];
  selectedVSchema: any;

  constructor(private keyspaceService: KeyspaceService,
              private shardService: ShardService,
              private tabletService: TabletService,
              private vtctlService: VtctlService) {

  }

  ngOnInit() {
    this.getKeyspaceNames();
  }

  getKeyspaceNames() {
    this.resetSchemas();
    this.resetVSchemas();
    this.keyspaceService.getKeyspaceNames().subscribe(keyspaceNames => {
      this.keyspaces = keyspaceNames.map(keyspaceName => {
        return {label: keyspaceName, value: keyspaceName};
      });
      this.keyspaces.sort(SchemaComponent.cmp);
      if (this.keyspaces.length > 0 ) {
        this.selectedKeyspace = this.keyspaces[0].value;
        this.getShards(this.selectedKeyspace);
      }
    });
  }

  static cmp(a, b): number {
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
    this.resetSchemas();
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
    this.resetSchemas();
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
    this.resetSchemas();
    this.resetVSchemas();
    let schemaStream = this.vtctlService.runCommand(['GetSchema', tabletName]);
    let vScemaStream = this.vtctlService.runCommand(['GetVSchema', keyspaceName]);
    let allSchemaStream = schemaStream.combineLatest(vScemaStream);
    allSchemaStream.subscribe(streams => {
      let schemaResp = streams[0];
      let vSchemaResp = streams[1];

      if (!vSchemaResp.Error) {
        vSchemaResp = JSON.parse(vSchemaResp.Output);
        if ('vindexes' in vSchemaResp) {
          let vSchemas = Object.keys(vSchemaResp.vindexes).map(vname => {
            let vtype = vSchemaResp.vindexes[vname].type;
            let vparams = vSchemaResp.vindexes[vname].params ? vSchemaResp.vindexes[vname].params : '';
            let vowner = vSchemaResp.vindexes[vname].owner ? vSchemaResp.vindexes[vname].owner : '';
            return {name: vname, type: vtype, params: vparams, owner: vowner};
          });
          this.vSchemas = vSchemas;
        } else {
          this.vSchemas = [];
        }
      }

      if (!schemaResp.Error) {
        schemaResp = JSON.parse(schemaResp.Output);
        if ('table_definitions' in schemaResp) {
          if (!vSchemaResp.Error && 'tables' in vSchemaResp) {
            let vindexes = this.createVindexMap(vSchemaResp.tables);
            this.schemas = schemaResp.table_definitions.map(table => {
              return this.parseColumns(table, vindexes);
            });
          } else {
            this.schemas = schemaResp.table_definitions.map(table => {
              return this.parseColumns(table);
            });
          }
        } else {
          this.schemas = [];
        }
      }

      // We just reloaded the data, clear the selected value.
      this.selectedSchema = undefined;
      this.selectedVSchema = undefined;
    });
  }

  resetSchemas() {
    this.schemas = [];
    this.selectedSchema = undefined;
  }

  resetVSchemas() {
    this.vSchemas = [];
    this.selectedVSchema = undefined;
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

  parseColumns(table, vindexes = undefined) {
    // For each primary key column, store its index (position) within the
    // primary key because we show this in the schema popup.
    let pkColumnIndexes = {};
    if ('primary_key_columns' in table) {
      let i = 1;
      for (let pkColumn of table.primary_key_columns) {
        pkColumnIndexes[pkColumn] = i;
        i++;
      }
    }

    // Generate column entries for schema popup.
    let columnIndex = 1;
    table['columns'] = table.columns.map(column => {
      let pkColumnIndex = column in pkColumnIndexes ? pkColumnIndexes[column].toString() : '~';
      let newColumn = {name: column, index: columnIndex, pk: pkColumnIndex};
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
