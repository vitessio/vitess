import { Http } from '@angular/http';
import { Injectable } from '@angular/core';

import { Observable } from 'rxjs/Observable';

// FeaturesService asks vtctld for the feature map, and populates
// member variables from it. It is constructed once when the main UI
// is displayed, and can then be used anywhere.
@Injectable()
export class FeaturesService {
  activeReparents = false;
  showStatus = false;
  showTopologyCRUD = false;
  showWorkflows = false;
  workflows = [];

  private featuresUrl = '../api/features';
  constructor(private http: Http) {
    this.getFeatures().subscribe(update => {
      this.activeReparents = update.activeReparents;
      this.showStatus = update.showStatus;
      this.showTopologyCRUD = update.showTopologyCRUD;
      this.showWorkflows = update.showWorkflows;
      this.workflows = update.workflows;
    });
  }

  getFeatures(): Observable<any> {
    return this.http.get(this.featuresUrl)
      .map(resp => resp.json());
  }
}
