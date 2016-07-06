import { Component, OnInit, Input, Output, EventEmitter} from '@angular/core';
import { ROUTER_DIRECTIVES } from '@angular/router';
import { MD_CARD_DIRECTIVES } from '@angular2-material/card';
import { MD_BUTTON_DIRECTIVES } from '@angular2-material/button';
import { MdIcon, MdIconRegistry } from '@angular2-material/icon';

@Component({
  moduleId: module.id,
  selector: 'vt-newkeyspace',
  templateUrl: './newkeyspace.component.html',
  styleUrls: ['./newkeyspace.component.css'],
  providers: [
              MdIconRegistry],
  directives: [
              ROUTER_DIRECTIVES,
              MD_CARD_DIRECTIVES,
              MD_BUTTON_DIRECTIVES,
              MdIcon],
  
})
export class NewKeyspaceComponent implements OnInit{
  title = 'Vitess Control Panel';
  keyspaces = [];

  //PopUp code
  @Input() openForm: boolean;
  @Input() toggleForm: any;
  @Output() onClose = new EventEmitter();
  ngOnInit() {
    console.log("NKS: ", this.openForm, this.toggleForm);
  }
  callToggleForm(){
    this.onClose.emit({ value:""});
    this.toggleForm();
  }
  blockClicks(event){
    event.stopPropagation();
  }

  //Form Code
  NKS = {
    name : "",
    lowerBound : "",
    upperBound : "",
    master : false,
  };
  
  submitForm(){
    console.log(this.NKS);
  }
}
