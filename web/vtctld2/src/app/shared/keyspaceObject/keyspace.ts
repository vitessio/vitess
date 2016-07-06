export class Keyspace{
  name: string;
  servingShards: string[];
  nonservingShards: string[];
  healthy: boolean;
  constructor(name, servingShards, nonservingShards, healthy) {
    this.name = name;
    this.servingShards = servingShards;
    this.nonservingShards =nonservingShards;
    this.healthy = healthy;
  }
}
