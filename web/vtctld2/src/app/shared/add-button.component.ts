import { Component, Input } from '@angular/core';

@Component({
  selector: 'vt-add-button',
  templateUrl: './add-button.component.html',
  styleUrls: ['./add-button.component.css'],
})
export class AddButtonComponent {
  @Input() hoverText: string;
}
