import { CanDeactivate } from '@angular/router';

import { Observable }    from 'rxjs/Observable';

export interface CanComponentDeactivate {
 canDeactivate: () => boolean | Observable<boolean>;
}

export class CanDeactivateGuard implements CanDeactivate<CanComponentDeactivate> {
  canDeactivate(component: CanComponentDeactivate): Observable<boolean> | boolean {
    return component.canDeactivate ? component.canDeactivate() : true;
  }
}
