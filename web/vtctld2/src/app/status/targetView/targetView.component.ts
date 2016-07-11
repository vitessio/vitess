import { Component,OnInit } from '@angular/core';
import { MD_TOOLBAR_DIRECTIVES } from '@angular2-material/toolbar';
import { MD_LIST_DIRECTIVES } from '@angular2-material/list';
import { MD_CARD_DIRECTIVES } from '@angular2-material/card';

import { TargetService } from '../../shared/targetService/target.service';

@Component({
  moduleId: module.id,
  selector: 'vt-target',
  templateUrl: './targetView.component.html',
  styleUrls: ['./targetView.component.css'],
  directives: [
    MD_LIST_DIRECTIVES,
    MD_TOOLBAR_DIRECTIVES,
    MD_CARD_DIRECTIVES,
  ],
  providers: [TargetService],
})

export class TargetViewComponent implements OnInit{
  title = 'Vitess Control Panel';

  tablets = [];

  constructor(
     private targetService: TargetService
  ) {}

  ngOnInit() {
    this.getTablets();
  }

  getTablets() {
    this.targetService.getTablets().subscribe( data => {
      /*console.log("GOT RESPONSE!!");
      this.tablets = data;
      console.log("ENTERED");*/
    });
  }
  
}

