import { ActivatedRoute } from '@angular/router';
import { Component, OnInit, OnDestroy } from '@angular/core';

import { TopoDataService } from '../api/topo-data.service';

@Component({
  selector: 'vt-topo',
  templateUrl: './topo-browser.component.html',
  styleUrls: ['./topo-browser.component.css'],
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
    private route: ActivatedRoute) {
  }

  ngOnInit() {
    this.routeSub = this.route.queryParams
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
