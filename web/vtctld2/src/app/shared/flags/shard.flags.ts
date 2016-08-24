import { CheckBoxFlag, DropDownFlag, InputFlag } from './flag';

// Groups of flags for vtctl actions.
export class DeleteShardFlags {
  flags= {};
  constructor(keyspaceName, shardName) {
    this.flags['shard_ref'] = new ShardRefFlag(0, 'shard_ref', keyspaceName + '/' + shardName);
    this.flags['shard_ref']['positional'] = true;
    this.flags['recursive'] = new RecursiveFlag(1, 'recursive');
  }
}

export class InitShardMasterFlags {
  flags= {};
  constructor(keyspaceName, shardName, tablets) {
    this.flags['shard_ref'] = new ShardRefFlag(0, 'shard_ref', keyspaceName + '/' + shardName);
    this.flags['shard_ref']['positional'] = true;
    this.flags['tablet_alias'] = new TabletSelectFlag(1, 'tablet_alias', '', tablets);
    this.flags['tablet_alias']['positional'] = true;
    this.flags['force'] = new ISMForceFlag(2, 'force');
    this.flags['wait_slave_timeout'] = new WaitSlaveTimeoutFlag(3, 'wait_slave_timeout');
  }
}

export class NewShardFlags {
  flags= {};
  constructor(keyspaceName) {
    this.flags['keyspace_name'] = new KeyspaceNameFlag(0, 'keyspace_name', keyspaceName);
    this.flags['shard_ref'] = new ShardRefFlag(1, 'shard_ref', '');
    this.flags['shard_ref']['positional'] = true;
    this.flags['lower_bound'] = new LowerBoundFlag(2, 'lower_bound', '');
    this.flags['upper_bound'] = new UpperBoundFlag(3, 'upper_bound', '');
  }
}

export class ValidateShardFlags {
  flags= {};
  constructor(keyspaceName, shardName) {
    this.flags['shard_ref'] = new ShardRefFlag(0, 'shard_ref', keyspaceName + '/' + shardName);
    this.flags['shard_ref']['positional'] = true;
    this.flags['ping-tablet'] = new PingTabletsFlag(1, 'ping-tablet');
  }
}

// Individual flags for vtctl actions.
export class ForceFlag extends CheckBoxFlag {
  constructor(position: number, id: string, value= false) {
    super(position, id, 'Force', 'Proceeds with the command even if the keyspace already exists.', value);
  }
}

export class ISMForceFlag extends CheckBoxFlag {
  constructor(position: number, id: string, value= false) {
    super(position, id, 'Force', 'Will force the reparent even if the provided tablet is not a master or the shard master.', value);
  }
}

export class KeyspaceNameFlag extends InputFlag {
  constructor(position: number, id: string, value= '') {
    super(position, id, '', '', value, false);
  }
}

export class LowerBoundFlag extends InputFlag {
  constructor(position: number, id: string, value= '') {
    super(position, id, 'Lower Bound', 'The Lower Bound of the shard. Leave blank to indicate the minimum value.', value);
  }
}

export class PingTabletsFlag extends CheckBoxFlag {
  constructor(position: number, id: string, value= false) {
    super(position, id, 'Ping Tablets', 'Indicates whether all tablets should be pinged during the validation process.', value);
  }
}

export class RecursiveFlag extends CheckBoxFlag {
  constructor(position: number, id: string, value= false) {
    super(position, id, 'Recursive', 'Also delete all tablets belonging to the shard.', value);
  }
}

export class ShardRefFlag extends InputFlag {
  constructor(position: number, id: string, value= '') {
    super(position, id, '', '', value, false);
  }
}

export class TabletSelectFlag extends DropDownFlag {
  constructor(position: number, id: string, value= '', tablets: any[]) {
    super(position, id, 'Select a tablet', ' Required. A Tablet Alias to make the master.', value);
    let options = [{label: '', value: ''}];
    tablets.forEach(tablet => {
      options.push({label: tablet.ref, value: tablet.ref});
    });
    this.setOptions(options);
  }
}

export class UpperBoundFlag extends InputFlag {
  constructor(position: number, id: string, value= '') {
    super(position, id, 'Upper Bound', 'The Upper Bound of the shard. Leave blank to indicate the maximum value.', value);
  }
}

export class WaitSlaveTimeoutFlag extends InputFlag {
  constructor(position: number, id: string, value= '') {
    super(position, id, 'Wait Slave Timeout', 'Time to wait for slaves to catch up in reparenting.', value);
  }
}
