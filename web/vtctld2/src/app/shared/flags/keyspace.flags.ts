import { InputFlag, CheckBoxFlag, DropDownFlag } from './flag';

export class KeyspaceNameFlag extends InputFlag {
  constructor(position: number, id: string, value: string="") {
    super(position, id, "Keyspace Name", "Required. The name of a sharded database that contains one or more tables.", value);
  }
}

export class ShardingColumnNameFlag extends InputFlag {
  constructor(position: number, id: string, value: string="") {
    super(position, id, "Sharding Column Name", "Specifies the column to use for sharding operations.", value);
  }
}

export class ShardingColumnTypeFlag extends DropDownFlag {
  constructor(position: number, id: string, value: string="") {
    super(position, id, "Sharding Column Type", "Specifies the type of the column to use for sharding operations.", value);
    this.setOptions([
                    {label:"unsigned bigint", text: "unsigned bigint"}, 
                    {label:"varbinary", text: "varbinary"}]);
    this.setBlockOnEmptyList(["shardingColumnName"]);
  }
}

export class ForceFlag extends CheckBoxFlag {
  constructor(position: number, id: string, value: boolean=false) {
    super(position, id, "Force", "Updates fields even if they are already set. Use caution before calling this command.", value);
  }
}

export class RecursiveFlag extends CheckBoxFlag {
  constructor(position: number, id: string, value: boolean=false) {
    super(position, id, "Recursive", "Also recursively delete all shards in the keyspace.", value);
  }
}