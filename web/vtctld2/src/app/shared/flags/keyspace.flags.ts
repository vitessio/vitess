import { CheckBoxFlag, DropDownFlag, InputFlag } from './flag';
import { Proto } from '../proto';

// Groups of flags for vtctl actions.
export class DeleteKeyspaceFlags {
  flags= {};
  constructor(keyspaceName: string) {
    this.flags['keyspace_name'] = new KeyspaceNameFlag(0, 'keyspace_name', keyspaceName, false);
    this.flags['keyspace_name']['positional'] = true;
    this.flags['recursive'] = new RecursiveFlag(1, 'recursive');
  }
}

export class EditKeyspaceFlags {
  flags= {};
  constructor(keyspace) {
    this.flags['keyspace_name'] = new KeyspaceNameFlag(0, 'keyspace_name', keyspace.name, false);
    this.flags['keyspace_name']['positional'] = true;
    this.flags['sharding_column_name'] = new ShardingColumnNameFlag(1, 'sharding_column_name', keyspace.shardingColumnName);
    this.flags['sharding_column_name']['positional'] = true;
    let shardingColumnType = Proto.SHARDING_COLUMN_TYPES[keyspace.shardingColumnType];
    this.flags['sharding_column_type'] = new ShardingColumnTypeFlag(2, 'sharding_column_type', shardingColumnType);
    this.flags['sharding_column_type']['positional'] = true;
    this.flags['force'] = new ForceFlag(3, 'force');
  }
}

export class NewKeyspaceFlags {
  flags= {};
  constructor() {
    this.flags['keyspace_name'] = new KeyspaceNameFlag(0, 'keyspace_name');
    this.flags['keyspace_name']['positional'] = true;
    this.flags['sharding_column_name'] = new ShardingColumnNameFlag(1, 'sharding_column_name');
    this.flags['sharding_column_type'] = new ShardingColumnTypeFlag(2, 'sharding_column_type');
    this.flags['force'] = new ForceFlag(3, 'force');
  }
}

export class RebuildKeyspaceGraphFlags {
  flags= {};
  constructor(keyspaceName) {
    this.flags['keyspace_name'] = new KeyspaceNameFlag(0, 'keyspace_name', keyspaceName, false);
    this.flags['keyspace_name']['positional'] = true;
    this.flags['cells'] = new CellsFlag(1, 'cells');
  }
}

export class ReloadSchemaKeyspaceFlags {
  flags= {};
  constructor(keyspaceName) {
    this.flags['keyspace_name'] = new KeyspaceNameFlag(0, 'keyspace_name', keyspaceName, false);
    this.flags['keyspace_name']['positional'] = true;
    this.flags['concurrency'] = new ConcurrencyFlag(1, 'concurrency', '10');
  }
}

export class RemoveKeyspaceCellFlags {
  flags= {};
  constructor(keyspaceName) {
    this.flags['keyspace_name'] = new KeyspaceNameFlag(0, 'keyspace_name', keyspaceName, false);
    this.flags['keyspace_name']['positional'] = true;
    this.flags['cell_name'] = new CellNameFlag(1, 'cell_name');
    this.flags['cell_name']['positional'] = true;
    this.flags['force'] = new RKCForceFlag(2, 'force');
    this.flags['recursive'] = new RKCRecursiveFlag(3, 'recursive');
  }
}

export class ValidateAllFlags {
  flags= {};
  constructor() {
    this.flags['ping-tablets'] = new PingTabletsFlag(0, 'ping-tablets');
  }
}

export class ValidateKeyspaceFlags {
  flags= {};
  constructor(keyspaceName) {
    this.flags['keyspace_name'] = new KeyspaceNameFlag(0, 'keyspace_name', keyspaceName, false);
    this.flags['keyspace_name']['positional'] = true;
    this.flags['ping-tablets'] = new PingTabletsFlag(1, 'ping-tablets');
  }
}

export class ValidateSchemaFlags {
  flags= {};
  constructor(keyspaceName) {
    this.flags['keyspace_name'] = new KeyspaceNameFlag(0, 'keyspace_name', keyspaceName, false);
    this.flags['keyspace_name']['positional'] = true;
  }
}

export class ValidateVersionFlags {
  flags= {};
  constructor(keyspaceName) {
    this.flags['keyspace_name'] = new KeyspaceNameFlag(0, 'keyspace_name', keyspaceName, false);
    this.flags['keyspace_name']['positional'] = true;
  }
}

// Individual flags for vtctl actions.
export class CellsFlag extends InputFlag {
  constructor(position: number, id: string, value= '') {
    super(position, id, 'Cells', 'Specifies a comma-separated list of cells to update.', value);
  }
}

export class CellNameFlag extends InputFlag {
  constructor(position: number, id: string, value= '', show= true) {
    super(position, id, 'Cell Name', ' Required. A cell is a location for a service.', value, show);
  }
}

export class ConcurrencyFlag extends InputFlag {
  constructor(position: number, id: string, value= '', show= true) {
    super(position, id, 'Concurrency', 'How many tablets to work on concurrently.', value, show);
  }
}

export class ForceFlag extends CheckBoxFlag {
  constructor(position: number, id: string, value= false) {
    super(position, id, 'Force', 'Updates fields even if they are already set. Use caution before calling this command.', value);
  }
}

export class KeyspaceNameFlag extends InputFlag {
  constructor(position: number, id: string, value= '', show= true) {
    super(position, id, 'Keyspace Name', 'The name of a database that contains one or more tables.', value, show);
  }
}

export class PingTabletsFlag extends CheckBoxFlag {
  constructor(position: number, id: string, value= false) {
    super(position, id, 'Ping Tablets', 'Indicates whether all tablets should be pinged during the validation process.', value);
  }
}

export class RecursiveFlag extends CheckBoxFlag {
  constructor(position: number, id: string, value= false) {
    super(position, id, 'Recursive', 'Also recursively delete all shards in the keyspace.', value);
  }
}

export class RKCForceFlag extends CheckBoxFlag {
  constructor(position: number, id: string, value= false) {
    super(position, id, 'Force', `Proceeds even if the cell's topology server cannot be reached.`, value);
  }
}

export class RKCRecursiveFlag extends CheckBoxFlag {
  constructor(position: number, id: string, value= false) {
    super(position, id, 'Recursive', 'Also delete all tablets in that cell belonging to the specified keyspace.', value);
  }
}

export class ShardingColumnNameFlag extends InputFlag {
  constructor(position: number, id: string, value= '') {
    super(position, id, 'Sharding Column Name', 'Specifies the column to use for sharding operations.', value);
  }
}

export class ShardingColumnTypeFlag extends DropDownFlag {
  constructor(position: number, id: string, value= 'UINT64') {
    super(position, id, 'Sharding Column Type', 'Specifies the type of the column to use for sharding operations.', value);
    let options = [];
    Proto.SHARDING_COLUMN_NAMES.forEach(name => {
        if (name !== '') {
          options.push({label: name, value: Proto.SHARDING_COLUMN_NAME_TO_TYPE[name]});
        }
    });
    this.setOptions(options);
    this.setBlockOnEmptyList(['sharding_column_name']);
  }
}
