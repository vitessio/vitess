import { RealtimeStats } from './realtimeStats';

export class Tablet {
 constructor(private cell: string, private id: string, private keyspace: string,
             private shard: string, private tabletType: string, public stats: RealtimeStats) {
  }
}
