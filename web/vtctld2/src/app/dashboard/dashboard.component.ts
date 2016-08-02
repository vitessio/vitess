import { Component, OnInit} from '@angular/core';
import { ROUTER_DIRECTIVES, Router } from '@angular/router';
import { KeyspaceService } from '../api/keyspace.service';
import { KeyspaceNameFlag, ShardingColumnNameFlag, ShardingColumnTypeFlag, ForceFlag, RecursiveFlag } from '../shared/flags/keyspace.flags';
import { DialogContent } from '../shared/Dialog/dialogContent'
import { MD_CARD_DIRECTIVES } from '@angular2-material/card';
import { MD_CHECKBOX_DIRECTIVES } from '@angular2-material/checkbox';
import { MD_INPUT_DIRECTIVES } from '@angular2-material/input';
import { MD_PROGRESS_BAR_DIRECTIVES } from '@angular2-material/progress-bar';
import { MD_BUTTON_DIRECTIVES } from '@angular2-material/button';
import { MD_LIST_DIRECTIVES } from '@angular2-material/list/list';
import { PolymerElement } from '@vaadin/angular2-polymer';
import { Observable } from 'rxjs/Observable';
import { ShardService } from '../api/shard.service';
import { DialogComponent } from '../shared/Dialog/dialog.component';
import { DialogSettings } from '../shared/Dialog/dialogSettings';
import { AddButtonComponent } from '../shared/addButton/addButton.component';

@Component({
  moduleId: module.id,
  selector: 'vt-dashboard',
  templateUrl: './dashboard.component.html',
  styleUrls: ['../styles/vt.style.css'],
  providers: [
              KeyspaceService,
              ShardService],
  directives: [
              ROUTER_DIRECTIVES,
              MD_CARD_DIRECTIVES,
              MD_PROGRESS_BAR_DIRECTIVES,
              MD_CHECKBOX_DIRECTIVES,
              MD_INPUT_DIRECTIVES,
              MD_BUTTON_DIRECTIVES,
              MD_LIST_DIRECTIVES,
              PolymerElement('paper-dialog'),
              DialogComponent,
              AddButtonComponent],
  
})
export class DashboardComponent implements OnInit{
  title = 'Vitess Control Panel';
  keyspaces = [];
  
  keyspacesReady = false;
  dialogSettings : DialogSettings;
  dialogContent: DialogContent;
  constructor(
              private keyspaceService: KeyspaceService,
              private router: Router) {

  }

  ngOnInit() {
    this.getKeyspaces();
    this.dialogContent = new DialogContent();
    this.dialogSettings = new DialogSettings();
  }

  getKeyspaces() {
    this.keyspaces = [];
    this.keyspaceService.getKeyspaces().subscribe( keyspaceStreams => {
      keyspaceStreams.subscribe( keyspaceStream => {
        keyspaceStream.subscribe( keyspace => {
          this.keyspaces.push(keyspace); // TODO(dsslater): This can be replaced with a binary search and insert
          this.keyspaces.sort(this.cmp);
        })
      })
    })
  }

  cmp(a: DialogContent, b: DialogContent): number {
    if (a.name.toLowerCase() > b.name.toLowerCase()) return 1;
    if (b.name.toLowerCase() > a.name.toLowerCase()) return -1;
    return 0;
  }

  createKeySpace() {
    this.dialogSettings.startPending();
    this.keyspaceService.createKeyspace(this.dialogContent).subscribe( resp => {
      if (resp.Error == true) {
        this.dialogSettings.setMessage("There was a problem creating ", ": " + resp.Output);
        this.dialogSettings.endPending();
      } else {
        this.getKeyspaces();
        this.dialogSettings.endPending();
      }
    });
  }

  editKeyspace() {
    this.dialogSettings.startPending();
    this.keyspaceService.editKeyspace(this.dialogContent).subscribe( resp => {
      if (resp.Error == true) {
        this.dialogSettings.setMessage("There was a problem editing ", ": " + resp.Output);
        this.dialogSettings.endPending();
      } else {
        this.getKeyspaces();
        this.dialogSettings.endPending();
      }
    });
  }

  deleteKeyspace() {
    this.dialogSettings.startPending();
    this.keyspaceService.deleteKeyspace(this.dialogContent).subscribe( resp => {
      if (resp.Error == true) {
        this.dialogSettings.setMessage("There was a problem deleting ", ": " + resp.Output);
        this.dialogSettings.endPending();
      } else {
        this.getKeyspaces();
        this.dialogSettings.endPending();
      }
    });
  }

  toggleModal() {
    this.dialogSettings.openModal = !this.dialogSettings.openModal;
  }

  prepareEdit(keyspace: any){
    this.dialogSettings = new DialogSettings("Edit", this.editKeyspace.bind(this), "Edit " + keyspace.name, "");
    let flags = [];
    flags["shardingColumnName"] = new ShardingColumnNameFlag(0, "shardingColumnName", keyspace.shardingColumnName);
    flags["shardingColumnType"] = new ShardingColumnTypeFlag(1, "shardingColumnType", this.getNameFromInt(keyspace.shardingColumnType));
    flags["force"] = new ForceFlag(2, "force");
    this.dialogContent = new DialogContent(keyspace.name, flags, {}, this.prepare.bind(this));  
  }

  prepareNew(){
    this.dialogSettings = new DialogSettings("Create", this.createKeySpace.bind(this), "Create a new Keyspace", "");
    let flags = {};
    flags["keyspaceName"] = new KeyspaceNameFlag(0, "keyspaceName");
    flags["shardingColumnName"] = new ShardingColumnNameFlag(1, "shardingColumnName");
    flags["shardingColumnType"] = new ShardingColumnTypeFlag(2, "shardingColumnType");
    flags["force"] = new ForceFlag(3, "force");
    this.dialogContent = new DialogContent("", flags, {"keyspaceName":true}, this.prepare.bind(this));
  }

  prepareDelete(keyspace: any) {
    this.dialogSettings = new DialogSettings("Delete", this.deleteKeyspace.bind(this), "Delete " + keyspace.name, "Are you sure you want to delete " + keyspace.name + "?");
    let flags = {};
    flags["recursive"] = new RecursiveFlag(0, "recursive");
    this.dialogContent = new DialogContent(keyspace.name, flags);
  }

  blockClicks(event: any){
    event.stopPropagation();
  }

  navigate(keyspaceName: string) {
    this.router.navigate(["/keyspace"], {queryParams: { keyspace: keyspaceName }});
  }

  getTypeFromName(type: string): string {
    switch (type) {
      case "unsigned bigint":
        return "UINT64";
      case "varbinary":
        return "BYTES";
      default:
        return ""; 
    }
  }

  getNameFromInt(type: number): string {
    switch (type) {
      case 1:
        return "unsigned bigint";
      case 2:
        return "varbinary";
      default:
        return ""; 
    }
  }

  prepare(flags) {
    if (flags["shardingColumnType"].getStrValue() != "") {
      flags["shardingColumnType"].setValue(this.getTypeFromName(flags["shardingColumnType"].getStrValue()));
    }
    return {
            success: true,
            message: "",
            flags: flags};
  }

  logView() {
    this.dialogSettings.dialogForm = false;
    this.dialogSettings.dialogLog = true;
  }

  canDeactivate(): Observable<boolean> | boolean {
    return !this.dialogSettings.pending;
  }
}
