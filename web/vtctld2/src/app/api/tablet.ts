import { RealtimeStats } from './realtime-stats';

export class Tablet {
 constructor(private cell: string, private uid: string, private keyspace: string,
             private shard: string, private tabletType: string, public stats: RealtimeStats) {
  }
}
