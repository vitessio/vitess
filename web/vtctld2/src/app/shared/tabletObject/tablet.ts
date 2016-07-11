export class Tablet {
  cell: string;
  uid: string;
  keyspaceName: string;
  shardName: string;
  type: string;
  stats: {
    SecondsBehindMaster: number,
    CpuUsage: number,
    Qps: number,
    HealthError: string, /*"" if no error */
  }
  constructor(cell, id, keyspaceName, shardName, type, stats) {
    this.cell = cell;
    this.uid = id;
    this.keyspaceName = keyspaceName;
    this.shardName = shardName;
    this.type = type;
    this.stats = stats;
  }
}
