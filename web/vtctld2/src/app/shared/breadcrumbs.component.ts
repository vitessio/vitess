import { Component, Input } from '@angular/core';
import { ROUTER_DIRECTIVES } from '@angular/router';

import { MD_CARD_DIRECTIVES } from '@angular2-material/card';
import { MD_BUTTON_DIRECTIVES } from '@angular2-material/button';
import { MdIcon } from '@angular2-material/icon';

class Breadcrumb {
  name: string;
  queryParams: any;
}

@Component({
  moduleId: module.id,
  selector: 'vt-breadcrumbs',
  templateUrl: './breadcrumbs.component.html',
  styleUrls: ['./breadcrumbs.component.css'],
  directives: [
    ROUTER_DIRECTIVES,
    MD_CARD_DIRECTIVES,
    MD_BUTTON_DIRECTIVES,
    MdIcon,
  ],
})
export class BreadcrumbsComponent {
  @Input() icon = 'label';
  @Input() route: any[];
  @Input() crumbs: Breadcrumb[];
  @Input() separator = '|';

  isSelected( name: string ) {
    var lastName = this.crumbs[this.crumbs.length-1].name;
    if(name === lastName) {
      return true;
    }
    return false;
  }
}
