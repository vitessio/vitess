import { Component, OnInit, OnDestroy } from '@angular/core';
import { Router, ROUTER_DIRECTIVES } from '@angular/router';

import { MD_LIST_DIRECTIVES } from '@angular2-material/list';
import { MD_CARD_DIRECTIVES } from '@angular2-material/card';
import { MdIcon } from '@angular2-material/icon';

import { TopoDataService } from '../api/topo-data.service';
import { BreadcrumbsComponent } from '../shared/breadcrumbs.component';

@Component({
  selector: 'vt-topo',
  templateUrl: './topo-browser.component.html',
  styleUrls: ['./topo-browser.component.css'],
  providers: [
    TopoDataService,
  ],
  directives: [
    ROUTER_DIRECTIVES,
    MD_LIST_DIRECTIVES,
    MD_CARD_DIRECTIVES,
    MdIcon,
    BreadcrumbsComponent,
  ],
})
export class TopoBrowserComponent implements OnInit, OnDestroy {
  title = 'Topology Browser';
  node = {};
  path = '';
  breadcrumbs = [];

  private routeSub: any;
  private nodeSub: any;

  constructor(
    private topoData: TopoDataService,
    private router: Router) {
  }

  ngOnInit() {
    this.routeSub = this.router.routerState.queryParams
      .subscribe(params => this.getNode(params['path']));
  }

  ngOnDestroy() {
    this.routeSub.unsubscribe();
    if (this.nodeSub) {
      this.nodeSub.unsubscribe();
    }
  }

  childLink(child: string) {
    return { path: this.path + '/' + child };
  }

  getNode(path: string) {
    this.path = '';
    this.breadcrumbs = [];

    if (path) {
      this.path = path;

      let parts = path.split('/');
      let partialPath = '';

      for (let i = 1; i < parts.length; ++i) {
        partialPath = partialPath + '/' + parts[i];
        this.breadcrumbs.push({
          name: parts[i],
          queryParams: { path: partialPath },
        });
      }
    }

    if (this.nodeSub) {
      this.nodeSub.unsubscribe();
    }

    this.nodeSub = this.topoData.getNode(this.path).subscribe(
      node => this.node = node,
      error => this.node = { Error: error });
  }
}
