import { Component, Input } from '@angular/core';
import { MdButton } from '@angular2-material/button/button';
import { MdIcon } from '@angular2-material/icon/icon';

@Component({
  moduleId: module.id,
  selector: 'vt-addButton',
  templateUrl: './addButton.component.html',
  styleUrls: ['./addButton.component.css'],
  directives: [MdButton, MdIcon]
  
})
export class AddButtonComponent{
  @Input() hoverText: string;
}
