import { RealtimeStats } from './realtimeStats'

export class Tablet {
  cell: string;
  uid: string;
  keyspaceName: string;
  shardName: string;
  type: string;
  stats: RealtimeStats;

  constructor(cell:string, id:string, keyspaceName:string, shardName:string, type:string, stats:RealtimeStats) {
    this.cell = cell;
    this.uid = id;
    this.keyspaceName = keyspaceName;
    this.shardName = shardName;
    this.type = type;
    this.stats = stats;
  }
}
