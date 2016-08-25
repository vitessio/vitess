import { Component } from '@angular/core';

@Component({
  selector: 'vt-tasks',
  templateUrl: './tasks.component.html',
  styleUrls: ['./tasks.component.css'],
})
export class TasksComponent {
  jobs = [
    {name: 'Resharding', progress: 60},
    {name: 'Online BackUp', progress: 20},
    {name: 'Offline Backup', progress: 0},
  ];
}
