import { Component } from '@angular/core';
import { FeaturesService } from './api/features.service';

import './rxjs-operators';

@Component({
  selector: 'vt-app-root',
  templateUrl: 'app.component.html',
  styleUrls: ['app.component.css'],
  providers: [FeaturesService],
})
export class AppComponent {
  title = 'Vitess Control Panel';

  constructor(private featuresService: FeaturesService) {}
}
