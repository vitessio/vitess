import { Component } from '@angular/core';
import { HTTP_PROVIDERS } from '@angular/http';

@Component({
  moduleId: module.id,
  selector: 'app-root',
  templateUrl: 'app.component.html',
  styleUrls: ['app.component.css'],
  providers: [
    HTTP_PROVIDERS
  ]
})
export class AppComponent {
  title = 'app works!';
}
