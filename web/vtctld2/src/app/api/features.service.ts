import { Http } from '@angular/http';
import { Injectable } from '@angular/core';

import { Observable } from 'rxjs/Observable';

// FeaturesService asks vtctld for the feature map, and populates
// member variables from it. It is constructed once when the main UI
// is displayed, and can then be used anywhere.
@Injectable()
export class FeaturesService {
  showStatus = false;
  showWorkflows = false;
  activeReparents = false;

  private featuresUrl = '../api/features';
  constructor(private http: Http) {
    this.getFeatures().subscribe(update => {
      this.showStatus = update.showStatus;
      this.showWorkflows = update.showWorkflows;
      this.activeReparents = update.activeReparents;
    });
  }

  getFeatures(): Observable<any> {
    return this.http.get(this.featuresUrl)
      .map(resp => resp.json());
  }
}
