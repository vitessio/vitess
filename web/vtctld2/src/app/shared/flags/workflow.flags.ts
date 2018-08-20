import { CheckBoxFlag, DropDownFlag, InputFlag } from './flag';

export class NewWorkflowFlags {
  flags= {};
  constructor(workflows) {
    // General flags.
    this.flags['skip_start'] = new SkipStartFlag(0, 'skip_start');
    this.flags['skip_start'].positional = true;
    this.flags['skip_start'].namedPositional = 'skip_start';
    this.flags['factory_name'] = new FactoryNameFlag(1, 'factory_name', workflows);
    this.flags['factory_name'].positional = true;

    // Flags for the Sleep workflow.
    this.flags['sleep_duration'] = new SleepDurationFlag(2, 'sleep_duration');
    this.flags['sleep_duration'].positional = true;
    this.flags['sleep_duration'].namedPositional = 'duration';

    // Flags for the Schema Swap workflow.
    this.flags['schema_swap_keyspace'] = new SchemaSwapKeyspaceFlag(3, 'schema_swap_keyspace');
    this.flags['schema_swap_keyspace'].positional = true;
    this.flags['schema_swap_keyspace'].namedPositional = 'keyspace';
    this.flags['schema_swap_sql'] = new SchemaSwapSQLFlag(4, 'schema_swap_sql');
    this.flags['schema_swap_sql'].positional = true;
    this.flags['schema_swap_sql'].namedPositional = 'sql';

    // Flags for Horizontal Resharding workflow.
    this.flags['horizontal_resharding_keyspace'] = new HorizontalReshardingKeyspaceFlag(5, 'horizontal_resharding_keyspace', 'horizontal_resharding');
    this.flags['horizontal_resharding_keyspace'].positional = true;
    this.flags['horizontal_resharding_keyspace'].namedPositional = 'keyspace';
    this.flags['horizontal_resharding_source_shards'] = new HorizontalReshardingSourceShardsFlag(6, 'horizontal_resharding_source_shards', 'horizontal_resharding');
    this.flags['horizontal_resharding_source_shards'].positional = true;
    this.flags['horizontal_resharding_source_shards'].namedPositional = 'source_shards';
    this.flags['horizontal_resharding_destination_shards'] = new HorizontalReshardingDestinationShardsFlag(7, 'horizontal_resharding_destination_shards', 'horizontal_resharding');
    this.flags['horizontal_resharding_destination_shards'].positional = true;
    this.flags['horizontal_resharding_destination_shards'].namedPositional = 'destination_shards';
    this.flags['horizontal_resharding_vtworkers'] = new HorizontalReshardingVtworkerFlag(8, 'horizontal_resharding_vtworkers', 'horizontal_resharding');
    this.flags['horizontal_resharding_vtworkers'].positional = true;
    this.flags['horizontal_resharding_vtworkers'].namedPositional = 'vtworkers';
    this.flags['horizontal_resharding_split_cmd'] = new SplitCloneCommand(9, 'horizontal_resharding_split_cmd', 'horizontal_resharding');
    this.flags['horizontal_resharding_split_cmd'].positional = true;
    this.flags['horizontal_resharding_split_cmd'].namedPositional = 'split_cmd';
    this.flags['horizontal_resharding_split_diff_dest_tablet_type'] = new SplitDiffTabletType(10, 'horizontal_resharding_split_diff_dest_tablet_type', 'horizontal_resharding');
    this.flags['horizontal_resharding_split_diff_dest_tablet_type'].positional = true;
    this.flags['horizontal_resharding_split_diff_dest_tablet_type'].namedPositional = 'split_diff_dest_tablet_type';
    this.flags['horizontal_resharding_min_healthy_rdonly_tablets'] = new HorizontalReshardingMinHealthyRdonlyTablets(11, 'horizontal_resharding_min_healthy_rdonly_tablets', 'horizontal_resharding');
    this.flags['horizontal_resharding_min_healthy_rdonly_tablets'].positional = true;
    this.flags['horizontal_resharding_min_healthy_rdonly_tablets'].namedPositional = 'min_healthy_rdonly_tablets';
    this.flags['horizontal_resharding_enable_approvals'] = new HorizontalReshardingEnableApprovalsFlag(12, 'horizontal_resharding_enable_approvals', 'horizontal_resharding');
    this.flags['horizontal_resharding_enable_approvals'].positional = true;
    this.flags['horizontal_resharding_enable_approvals'].namedPositional = 'enable_approvals';

    // Flags for keyspace resharding workflow.
    this.flags['keyspace_resharding_keyspace'] = new HorizontalReshardingKeyspaceFlag(13, 'keyspace_resharding_keyspace', 'keyspace_resharding');
    this.flags['keyspace_resharding_keyspace'].positional = true;
    this.flags['keyspace_resharding_keyspace'].namedPositional = 'keyspace';
    this.flags['keyspace_resharding_vtworkers'] = new HorizontalReshardingVtworkerFlag(14, 'keyspace_resharding_vtworkers', 'keyspace_resharding');
    this.flags['keyspace_resharding_vtworkers'].positional = true;
    this.flags['keyspace_resharding_vtworkers'].namedPositional = 'vtworkers';
    this.flags['keyspace_resharding_split_cmd'] = new SplitCloneCommand(15, 'keyspace_resharding_split_cmd', 'keyspace_resharding');
    this.flags['keyspace_resharding_split_cmd'].positional = true;
    this.flags['keyspace_resharding_split_cmd'].namedPositional = 'split_cmd';
    this.flags['keyspace_resharding_split_diff_dest_tablet_type'] = new SplitDiffTabletType(16, 'keyspace_resharding_split_diff_dest_tablet_type', 'keyspace_resharding');
    this.flags['keyspace_resharding_split_diff_dest_tablet_type'].positional = true;
    this.flags['keyspace_resharding_split_diff_dest_tablet_type'].namedPositional = 'split_diff_dest_tablet_type';
    this.flags['keyspace_resharding_min_healthy_rdonly_tablets'] = new HorizontalReshardingMinHealthyRdonlyTablets(17, 'keyspace_resharding_min_healthy_rdonly_tablets', 'keyspace_resharding');
    this.flags['keyspace_resharding_min_healthy_rdonly_tablets'].positional = true;
    this.flags['keyspace_resharding_min_healthy_rdonly_tablets'].namedPositional = 'min_healthy_rdonly_tablets';
    this.flags['keyspace_resharding_enable_approvals'] = new HorizontalReshardingEnableApprovalsFlag(18, 'keyspace_resharding_enable_approvals', 'keyspace_resharding');
    this.flags['keyspace_resharding_enable_approvals'].positional = true;
    this.flags['keyspace_resharding_enable_approvals'].namedPositional = 'enable_approvals';

  }
}

export class SplitCloneCommand extends DropDownFlag {
    constructor(position: number, id: string, setDisplayOn: string) {
        super(position, id, 'Split Clone Command', 'Specifies the split command to use.', '');
        let options = [];
        options.push({
            label: 'SplitClone',
            value: 'SplitClone'
        });
        options.push({
            label: 'LegacySplitClone',
            value: 'LegacySplitClone'
        });
        this.setOptions(options);
        this.value = options[0].value;
        this.setDisplayOn('factory_name', setDisplayOn);
    }
}

export class SplitDiffTabletType extends DropDownFlag {
    constructor(position: number, id: string, setDisplayOn: string) {
        super(position, id, 'SplitDiff destination tablet type', 'Specifies tablet type to use in destination shards while performing SplitDiff operation', '');
        let options = [];
        options.push({
            label: 'RDONLY',
            value: 'RDONLY'
        });
        options.push({
            label: 'REPLICA',
            value: 'REPLICA'
        });
        this.setOptions(options);
        this.value = options[0].value;
        this.setDisplayOn('factory_name', setDisplayOn);
    }
}

export class FactoryNameFlag extends DropDownFlag {
  constructor(position: number, id: string, workflows) {
    super(position, id, 'Factory Name', 'Specifies the type of workflow to create.', '');
    let options = [];
    if (workflows.keyspace_resharding) {
      options.push({
          label: 'Keyspace resharding',
          value: 'keyspace_resharding'
      });
    }

    if (workflows.horizontal_resharding) {
      options.push({
        label: 'Horizontal Resharding',
        value: 'horizontal_resharding'
      });
    }
    if (workflows.schema_swap) {
      options.push({
        label: 'Schema Swap',
        value: 'schema_swap'
      });
    }
    if (workflows.sleep) {
      options.push({
        label: 'Sleep',
        value: 'sleep'
      });
    }
    if (workflows.topo_validator) {
      options.push({
        label: 'Topology Validator',
        value: 'topo_validator',
      });
    }
    this.setOptions(options);
    this.value = options[0].value;
  }
}

export class SkipStartFlag extends CheckBoxFlag {
  constructor(position: number, id: string, value= false) {
    super(position, id, 'Skip Start', 'Create the workflow, but don\'t start it.', value);
  }
}

export class SleepDurationFlag extends InputFlag {
  constructor(position: number, id: string, value= '') {
    super(position, id, 'Sleep Duration', 'Time to sleep for, in seconds.', value);
    this.setDisplayOn('factory_name', 'sleep');
  }
}

export class SchemaSwapKeyspaceFlag extends InputFlag {
  constructor(position: number, id: string, value= '') {
    super(position, id, 'Keyspace', 'Name of a keyspace.', value);
    this.setDisplayOn('factory_name', 'schema_swap');
  }
}

export class SchemaSwapSQLFlag extends InputFlag {
  constructor(position: number, id: string, value= '') {
    super(position, id, 'SQL', 'SQL representing the schema change.', value);
    this.setDisplayOn('factory_name', 'schema_swap');
  }
}

export class HorizontalReshardingKeyspaceFlag extends InputFlag {
    constructor(position: number, id: string, setDisplayOn: string, value= '') {
    super(position, id, 'Keyspace', 'Name of a keyspace.', value);
    this.setDisplayOn('factory_name', setDisplayOn);
  }
}

export class HorizontalReshardingSourceShardsFlag extends InputFlag {
    constructor(position: number, id: string, setDisplayOn: string, value= '') {
        super(position, id, 'Source Shards', 'A comma-separated list of source shards.', value);
        this.setDisplayOn('factory_name', setDisplayOn);
    }
}

export class HorizontalReshardingDestinationShardsFlag extends InputFlag {
    constructor(position: number, id: string, setDisplayOn: string, value= '') {
        super(position, id, 'Destination Shards', 'A comma-separated list of destination shards.', value);
        this.setDisplayOn('factory_name', setDisplayOn);
    }
}

export class HorizontalReshardingVtworkerFlag extends InputFlag {
  constructor(position: number, id: string, setDisplayOn: string, value= '') {
    super(position, id, 'vtworker Addresses', 'Comma-separated list of vtworker addresses.', value);
    this.setDisplayOn('factory_name', setDisplayOn);
  }
}

export class HorizontalReshardingMinHealthyRdonlyTablets extends InputFlag {
  constructor(position: number, id: string, setDisplayOn: string, value= '') {
    super(position, id, 'min healthy rdonly tablets', 'Minimum number of healthy RDONLY tablets required in source shards', value);
    this.setDisplayOn('factory_name', setDisplayOn);
  }
}

export class HorizontalReshardingEnableApprovalsFlag extends CheckBoxFlag {
  constructor(position: number, id: string, setDisplayOn: string, value=true) {
    super(position, id, 'enable approvals', 'Set true if use user approvals of task execution.', value);
    this.setDisplayOn('factory_name', setDisplayOn);
  }
}

// WorkflowFlags is used by the Start / Stop / Delete dialogs.
export class WorkflowFlags {
  flags= {};
  constructor(path) {
    this.flags['workflow_uuid'] = new WorkflowUuidFlag(0, 'workflow_uuid', path);
    this.flags['workflow_uuid']['positional'] = true;
  }
}

export class WorkflowUuidFlag extends InputFlag {
  constructor(position: number, id: string, value= '') {
    super(position, id, '', '', value, false);
  }
}
