import { Injectable } from '@angular/core';

@Injectable()
export class KeyspaceService {
  keyspaces = {
    KS1: {
      servingShards: [
        '-80', 
        '80-',
      ],
      nonservingShards: [
        '0',
      ],
      healthy: true,
    },
    KS2: {
      servingShards: [
        '-40',
        '40-80',
        '80-c0',
        'c0-',
      ],
      nonservingShards: [
        '0',
      ],
      healthy: true,
    },
    KS3: {
      servingShards: [
      ],
      nonservingShards: [
        '0',
      ],
      healthy: false,
    }
  };
  listKeyspaces() {
    return Promise.resolve(Object.keys(this.keyspaces));
  };
  healthy(keyspace) {
    return Promise.resolve(this.keyspaces[keyspace].healthy);
  }
  getKeyspace(keyspaceName) {
    return Promise.resolve(this.keyspaces[keyspaceName])
  }
}