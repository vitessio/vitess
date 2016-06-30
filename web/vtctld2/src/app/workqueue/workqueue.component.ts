import { Component } from '@angular/core';
import { MD_CARD_DIRECTIVES } from '@angular2-material/card';
import {MdProgressBar} from '@angular2-material/progress-bar/progress-bar';
@Component({
  moduleId: module.id,
  selector: 'vt-workqueue',
  templateUrl: './workqueue.component.html',
  styleUrls: ['./workqueue.component.css', '/@angular2-material/card/card.css'],
  directives: [
    MD_CARD_DIRECTIVES,
    MdProgressBar
  ],
})
export class WorkqueueComponent {
  title = 'Vitess Control Panel';
  jobs = [
  	  {name:"Resharding", progress:60}, 
  	  {name:"Online BackUp", progress:20}, 
  	  {name:"Offline Backup", progress:0},
  ];
}
