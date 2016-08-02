import { CheckBoxFlag, InputFlag } from './flag';

export class KeyspaceNameFlag extends InputFlag {
  constructor(position: number, id: string, value= '') {
    super(position, id, '', '', value, false);
  }
}

export class ShardNameFlag extends InputFlag {
  constructor(position: number, id: string, value= '') {
    super(position, id, '', '', value, false);
  }
}

export class LowerBoundFlag extends InputFlag {
  constructor(position: number, id: string, value= '') {
    super(position, id, 'Lower Bound', 'The Lower Bound of the shard. Leave blank to indicate the minimum value.', value);
  }
}

export class UpperBoundFlag extends InputFlag {
  constructor(position: number, id: string, value= '') {
    super(position, id, 'Upper Bound', 'The Upper Bound of the shard. Leave blank to indicate the maximum value.', value);
  }
}

export class ForceFlag extends CheckBoxFlag {
  constructor(position: number, id: string, value= false) {
    super(position, id, 'Force', 'Proceeds with the command even if the keyspace already exists.', value);
  }
}

export class RecursiveFlag extends CheckBoxFlag {
  constructor(position: number, id: string, value= false) {
    super(position, id, 'Recursive', 'Also delete all tablets belonging to the shard.', value);
  }
}
