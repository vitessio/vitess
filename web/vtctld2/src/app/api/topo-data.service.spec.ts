/* tslint:disable:no-unused-variable */

import {
  beforeEach, beforeEachProviders,
  describe, xdescribe,
  expect, it, xit,
  async, inject
} from '@angular/core/testing';
import { TopoDataService } from './topo-data.service';

describe('TopoData Service', () => {
  beforeEachProviders(() => [TopoDataService]);

  it('should ...',
      inject([TopoDataService], (service: TopoDataService) => {
    expect(service).toBeTruthy();
  }));
});
