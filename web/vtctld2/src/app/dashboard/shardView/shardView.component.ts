import { Component, OnInit, OnDestroy } from '@angular/core';
import { ROUTER_DIRECTIVES, ActivatedRoute, Router } from '@angular/router';
import { TabletService } from '../../shared/tabletService/tablet.service';
import { MD_CARD_DIRECTIVES } from '@angular2-material/card';
import { DataTable, Column } from 'primeng/primeng';
import { PolymerElement } from '@vaadin/angular2-polymer';

@Component({
  moduleId: module.id,
  selector: 'vt-shard-view',
  templateUrl: './shardView.component.html',
  styleUrls: ['./shardView.component.css'],
  directives: [
              ROUTER_DIRECTIVES,
              MD_CARD_DIRECTIVES,
              DataTable,
              Column,
              PolymerElement('paper-dialog'),
              PolymerElement('paper-button'),
              PolymerElement('paper-item'),
              PolymerElement('paper-listbox'),
              PolymerElement('paper-dropdown-menu'),
              PolymerElement('paper-toast')],
  providers: [
              TabletService],
})
export class ShardViewComponent implements OnInit, OnDestroy{
  private routeSub: any;
  keyspaceName: string;
  shardName: string;
  tablets = [];
  tabletsReady = false;
  constructor(
    private route: ActivatedRoute,
    private router: Router,
    private tabletService: TabletService) {}
  openForm = false;
  actionWord : string;
  actionWordPast : string;
  actionFunction : any;
  dialogTitle: string;
  //New/Edited Tablet Information
  NTB = {
    name : "",
    type : 0,
  };
  ngOnInit() {
    this.routeSub = this.router.routerState.queryParams
      .subscribe(params => {
        this.keyspaceName = params['keyspace'];
        this.shardName = params['shard'];
        this.getTablets(this.keyspaceName, this.shardName);
      }
    );
  }

  ngOnDestroy() {
    this.routeSub.unsubscribe();
  }

  getTablets(keyspaceName, shardName) {
    this.tabletService.getTablets(keyspaceName, shardName).subscribe((tablets) => {
      this.tablets = tablets;
      this.tabletsReady = true;
    });
  }

  getName(type) {
    switch (type) {
      case 1:
        return "Master";
      case 2:
        return "Replica";
      case 3:
        return "Read Only";
      default:
        return "";
    }
  }
  getNumber(name) {
    switch (name) {
      case "Master":
        return 1;
      case "Replica":
        return 2;
      case "Read Only":
        return 3;
      default:
        return 0;
    }
  }

  toggleForm() {
    this.openForm = !this.openForm;
  }

  submitForm(){
    /*Temporary Function, will be replaced with CRUD interface*/
    console.log("SUBMIT: ", this.NTB);
  }

  populateNSH(type){
    this.actionWord = "Edit";
    this.actionWordPast = "Edited";
    this.actionFunction = this.submitForm;
    this.dialogTitle = "Edit " + "Tablet";
    this.NTB.type = type;
  }

  clearNSH(){
    this.actionWord = "Create";
    this.actionWordPast = "Created";
    this.actionFunction = this.submitForm;
    this.dialogTitle = "Create a New Tablet";
    this.NTB.type = 0;
  }
  blockClicks(event){
    event.stopPropagation();
  }
  typeSelected(e) {
    //Odd Polymer event syntax
    this.NTB.type = this.getNumber(e.detail.item.__dom.firstChild.data);
  }
}
