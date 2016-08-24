import { Component } from '@angular/core';

import { MD_CARD_DIRECTIVES } from '@angular2-material/card';
import { MD_PROGRESS_BAR_DIRECTIVES } from '@angular2-material/progress-bar';

@Component({
  selector: 'vt-tasks',
  templateUrl: './tasks.component.html',
  styleUrls: ['./tasks.component.css'],
  directives: [
    MD_CARD_DIRECTIVES,
    MD_PROGRESS_BAR_DIRECTIVES
  ],
})

export class TasksComponent {
  jobs = [
    {name: 'Resharding', progress: 60},
    {name: 'Online BackUp', progress: 20},
    {name: 'Offline Backup', progress: 0},
  ];
}
