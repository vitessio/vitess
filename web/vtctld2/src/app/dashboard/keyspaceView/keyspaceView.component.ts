import { Component, Input, OnInit, OnDestroy } from '@angular/core';
import { ROUTER_DIRECTIVES, ActivatedRoute, Router } from '@angular/router';
import { KeyspaceService } from '../../shared/keyspaceService/keyspace.service';
import { MD_CARD_DIRECTIVES } from '@angular2-material/card';
import { PolymerElement } from '@vaadin/angular2-polymer';

@Component({
  moduleId: module.id,
  selector: 'vt-keyspace-view',
  templateUrl: './keyspaceView.component.html',
  styleUrls: ['./keyspaceView.component.css'],
  directives: [
              ROUTER_DIRECTIVES,
              MD_CARD_DIRECTIVES,
              PolymerElement('paper-dialog'),
              PolymerElement('paper-button'),
              PolymerElement('paper-input'),
              PolymerElement('paper-item'),
              PolymerElement('paper-toast')],
  providers: [KeyspaceService],
})
export class KeyspaceViewComponent implements OnInit, OnDestroy{
  private routeSub: any;
  keyspaceName: string;
  shardsReady = false;
  keyspace = {};
  openForm = false;
  actionWord: string;
  actionWordPast: string;
  actionFunction: any;
  dialogTitle: string;
  toastText: string;
  //New/Edited Shard Information
  NSH = {
    name : "",
    lowerBound : "",
    upperBound : ""
  };
  constructor(
    private route: ActivatedRoute,
    private router: Router,
    private keyspaceService: KeyspaceService) {}

  ngOnInit() {
    this.routeSub = this.router.routerState.queryParams
      .subscribe(params => {
        this.keyspaceName = params['keyspace'];
        this.getKeyspace(this.keyspaceName);
      }
    );
  }

  ngOnDestroy() {
    this.routeSub.unsubscribe();
  }

  getKeyspace(keyspaceName) {
    this.keyspaceService.getKeyspace(keyspaceName).subscribe((keyspaceStream) => {
      keyspaceStream.subscribe( keyspace => {
        this.keyspace = keyspace;
      });
    });
  }

  getShardParams(shardName: string) {
    return {keyspace: this.keyspaceName, shard: shardName};
  }

  toggleForm() {
    this.openForm = !this.openForm;
  }

  submitForm(){
    /*Temporary Function, will be replaced with CRUD interface*/
    console.log("SUBMIT: ", this.NSH);
  }

  populateNSH(shard){
    this.actionWord = "Edit";
    this.actionWordPast = "Edited";
    this.actionFunction = this.submitForm;
    this.dialogTitle = "Edit " + shard;
    this.NSH.name = shard;
    this.NSH.lowerBound = this.getLower(shard);
    this.NSH.upperBound = this.getUpper(shard);
    this.toastText = "Edited Shard";
  }
  getLower(shard) {
    let index = shard.indexOf('-');
    if (index <= 0) {
      return "";
    } else {
      return shard.substring(0,index);
    }
  }
  getUpper(shard){
    let index = shard.indexOf('-');
    return shard.substring(index + 1);
  }
  clearNSH(){
    this.actionWord = "Create";
    this.actionWordPast = "Created";
    this.actionFunction = this.submitForm;
    this.dialogTitle = "Create a New Shard";
    this.NSH.name = "";
    this.NSH.lowerBound = "";
    this.NSH.upperBound = "";
    this.toastText = "Created a New Shard"
  }
  blockClicks(event){
    event.stopPropagation();
  }
  nav(keyspaceName, shardName) {
    this.router.navigateByUrl("/shard?keyspace=" + keyspaceName + "&shard="+shardName);
  }

  getName(lowerBound, upperBound) {
    let LB = lowerBound == "0" ? "": lowerBound;
    let UB = upperBound == "0" ? "": upperBound;
    let full = LB + "-" + UB;
    this.NSH.name = full == "-" ? "0": full;
    return this.NSH.name;
  }
}
