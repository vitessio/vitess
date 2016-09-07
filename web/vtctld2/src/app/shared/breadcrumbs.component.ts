import { Component, Input } from '@angular/core';

class Breadcrumb {
  name: string;
  queryParams: any;
}

@Component({
  selector: 'vt-breadcrumbs',
  templateUrl: './breadcrumbs.component.html',
  styleUrls: ['./breadcrumbs.component.css'],
})
export class BreadcrumbsComponent {
  @Input() icon = 'label';
  @Input() route: any[];
  @Input() crumbs: Breadcrumb[];
  @Input() separator = '|';
}
