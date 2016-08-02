export class Keyspace {
  public name: string;
  public servingShards: string[];
  public nonservingShards: string[];
  public allShards: any;
  public shardingColumnName: string;
  public shardingColumnType: string;
  constructor(name, servingShards= [], nonservingShards= []) {
    this.name = name;
    this.servingShards = servingShards;
    this.nonservingShards = nonservingShards;
    this.allShards = {};
    this.shardingColumnName = '';
    this.shardingColumnType = '';
  }

  public addServingShard(shard: string) {
    this.servingShards.push(shard);
    this.allShards[shard] = true;
  }

  public addNonservingShard(shard: string) {
    this.nonservingShards.push(shard);
    this.allShards[shard] = true;
  }

  public contains(shard) {
    return shard in this.allShards;
  }
}
