import { Http } from '@angular/http';
import { Injectable } from '@angular/core';

@Injectable()
export class KeyspaceService {

  constructor(private http: Http) {}
}
