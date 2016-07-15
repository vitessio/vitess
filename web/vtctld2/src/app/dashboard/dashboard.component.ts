import { Component, OnInit} from '@angular/core';
import { ROUTER_DIRECTIVES, Router } from '@angular/router';
import { KeyspaceService } from '../shared/keyspaceService/keyspace.service';
import { MD_CARD_DIRECTIVES } from '@angular2-material/card';
import { PolymerElement } from '@vaadin/angular2-polymer';

@Component({
  moduleId: module.id,
  selector: 'vt-dashboard',
  templateUrl: './dashboard.component.html',
  styleUrls: ['./dashboard.component.css'],
  providers: [
              KeyspaceService],
  directives: [
              ROUTER_DIRECTIVES,
              MD_CARD_DIRECTIVES,
              PolymerElement('paper-dialog'),
              PolymerElement('paper-button'),
              PolymerElement('paper-input'),
              PolymerElement('paper-item'),
              PolymerElement('paper-toast')],
  
})
export class DashboardComponent implements OnInit{
  title = 'Vitess Control Panel';
  keyspaces = [];
  openModal = false;
  keyspacesReady = false;
  form = false;
  actionWord: string;
  actionWordPast: string;
  actionFunction: any;
  dialogTitle: string;
  dialogSubtitle: string;
  toastText: string;
  //New/Edited Keyspace Information
  NKS = {
    name : "",
    shardingColumnName : "",
    shardingColumnType : "",
  };
  test(event) {
    console.log(event);
    document.querySelector("#TEST").setAttribute("disabled", (this.NKS.name == "").toString());
  }
  test1 = true;
  constructor(
              private keyspaceService: KeyspaceService, 
              private router: Router) {}

  ngOnInit() {
    this.getKeyspaces();
  }
  getKeyspaces() {
    this.keyspaces = [];
    this.keyspaceService.getKeyspaces().subscribe( keyspaceStreams => {
      keyspaceStreams.subscribe( keyspaceStream => {
        keyspaceStream.subscribe( keyspace => {
          this.keyspaces.push(keyspace);
          this.keyspaces.sort(this.cmp);
        })
      })
    })
  }
  cmp(a,b) {
    if (a.name.toLowerCase() > b.name.toLowerCase()) return 1;
    if (b.name.toLowerCase() > a.name.toLowerCase()) return -1;
    return 0;
  }
  createKeySpace() {
    //Prevents user from circumnavigating column type and name codependence
    if (this.NKS.shardingColumnName == "") this.NKS.shardingColumnType = "";
    this.keyspaceService.createKeyspace(this.NKS).subscribe( resp => {
      this.getKeyspaces();
    });
  }
  editKeyspace() {
    //Prevents user from circumnavigating column type and name codependence
    if (this.NKS.shardingColumnName == "") this.NKS.shardingColumnType = "";
    this.keyspaceService.editKeyspace(this.NKS).subscribe( resp => {
      this.getKeyspaces();
    });
  }
  deleteKeyspace() {
    this.keyspaceService.deleteKeyspace(this.NKS).subscribe( resp => {
      this.getKeyspaces();
    });
  }
  toggleForm() {
    this.openModal = !this.openModal;
  }
  populateNKS(keyspace){
    this.actionWord = "Edit";
    this.actionWordPast = "Edited";
    this.actionFunction = this.editKeyspace;
    this.dialogTitle = "Edit " + keyspace.name;
    this.NKS.name = keyspace.name;
    this.NKS.shardingColumnName = keyspace.shardingColumnName;
    this.NKS.shardingColumnType = this.getName(keyspace.shardingColumnType);
    this.form = true;
  }
  clearNKS(){
    this.actionWord = "Create";
    this.actionWordPast = "Created";
    this.actionFunction = this.createKeySpace;
    this.dialogTitle = "Create a new Keyspace";
    this.NKS.name = "";
    this.NKS.shardingColumnName = "";
    this.NKS.shardingColumnType = "";
    this.form = true;
  }
  deleteNKS(keyspace) {
    this.actionWord = "Delete";
    this.actionWordPast = "Deleted";
    this.actionFunction = this.deleteKeyspace;
    this.dialogTitle = "Delete " + keyspace.name;
    this.dialogSubtitle = "Are you sure you want to delete " + keyspace.name + "?";
    this.NKS.name = keyspace.name;
    this.form = false;
  }
  blockClicks(event){
    event.stopPropagation();
  }
  navigate(keyspaceName) {
    this.router.navigate(["/keyspace"], {queryParams: { keyspace: keyspaceName }});
  }
  getType(type) {
    switch (type) {
      case "unsigned bigint":
        return "UINT64";
      case "varbinary":
        return "BYTES";
      default:
        return ""; 
    }
  }
  getName(type) {
    switch (type) {
      case 1:
        return "UINT64";
      case 2:
        return "BYTES";
      default:
        return ""; 
    }
  }
  typeSelected(e) {
    //Odd Polymer event syntax
    this.NKS.shardingColumnType = this.getType(e.detail.item.__dom.firstChild.data);
  }
}
