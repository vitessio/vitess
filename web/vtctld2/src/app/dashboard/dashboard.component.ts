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
  openForm = false;
  keyspacesReady = false;
  actionWord: string;
  actionWordPast: string;
  actionFunction: any;
  dialogTitle: string;
  toastText: string;
  //New/Edited Keyspace Information
  NKS = {
    name : "",
  };
  
  constructor(
              private keyspaceService: KeyspaceService, 
              private router: Router) {}

  ngOnInit() {
    this.getKeyspaces();
  }

  getKeyspaces() {
    this.keyspaceService.getKeyspaces().subscribe(keyspaces => {
      this.keyspaces = keyspaces;
      this.keyspacesReady = true;
    });
  }
  
  submitForm(){
    /*Temporary Function, will be replaced with CRUD interface*/
    console.log("SUBMIT: ", this.NKS);
  }

  toggleForm() {
    this.openForm = !this.openForm;
  }
  populateNKS(keyspace){
    this.actionWord = "Edit";
    this.actionWordPast = "Edited";
    this.actionFunction = this.submitForm;
    this.dialogTitle = "Edit " + keyspace.name;
    this.NKS.name = keyspace.name;
  }
  clearNKS(){
    this.actionWord = "Create";
    this.actionWordPast = "Created";
    this.actionFunction = this.submitForm;
    this.dialogTitle = "Create a new Keyspace";
    this.NKS.name = "";
  }
  blockClicks(event){
    event.stopPropagation();
  }
  navigate(keyspaceName) {
    this.router.navigateByUrl("/keyspace?keyspace=" + keyspaceName);
  }
}
