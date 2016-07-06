import { Injectable } from '@angular/core';
import { Http } from '@angular/http';
import { Keyspace } from '../shared/keyspaceObject/keyspace'

@Injectable()
export class KeyspaceService {
  private keyspacesUrl = '/app/keyspaces';

  constructor(private http: Http) {}

  getKeyspaces() {
    return this.http.get(this.keyspacesUrl)
    .map( (resp) => {
      return resp.json();
    })
    .map ( (keyspaces: any) => {
      return keyspaces.data;
    })
  }

  getKeyspace(keyspaceName) {
    return this.http.get(this.keyspacesUrl + '/?name=' + keyspaceName)
    .map( (resp) => {
      return resp.json();
    })
    .map ( (keyspaces: any) => {
      return keyspaces.data;
    })
  }
}