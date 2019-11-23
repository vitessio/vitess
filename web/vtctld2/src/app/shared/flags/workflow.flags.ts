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
    this.flags['horizontal_resharding_exclude_tables'] = new HorizontalReshardingExcludeTablesFlag(9, 'horizontal_resharding_exclude_tables', 'horizontal_resharding');
    this.flags['horizontal_resharding_exclude_tables'].positional = true;
    this.flags['horizontal_resharding_exclude_tables'].namedPositional = 'exclude_tables';
    this.flags['horizontal_resharding_split_cmd'] = new SplitCloneCommand(10, 'horizontal_resharding_split_cmd', 'horizontal_resharding');
    this.flags['horizontal_resharding_split_cmd'].positional = true;
    this.flags['horizontal_resharding_split_cmd'].namedPositional = 'split_cmd';
    this.flags['horizontal_resharding_split_diff_cmd'] = new SplitDiffCommand(11, 'horizontal_resharding_split_diff_cmd', 'horizontal_resharding');
    this.flags['horizontal_resharding_split_diff_cmd'].positional = true;
    this.flags['horizontal_resharding_split_diff_cmd'].namedPositional = 'split_diff_cmd';
    this.flags['horizontal_resharding_split_diff_dest_tablet_type'] = new SplitDiffTabletType(12, 'horizontal_resharding_split_diff_dest_tablet_type', 'horizontal_resharding');
    this.flags['horizontal_resharding_split_diff_dest_tablet_type'].positional = true;
    this.flags['horizontal_resharding_split_diff_dest_tablet_type'].namedPositional = 'split_diff_dest_tablet_type';
    this.flags['horizontal_resharding_min_healthy_rdonly_tablets'] = new HorizontalReshardingMinHealthyRdonlyTablets(13, 'horizontal_resharding_min_healthy_rdonly_tablets', 'horizontal_resharding');
    this.flags['horizontal_resharding_min_healthy_rdonly_tablets'].positional = true;
    this.flags['horizontal_resharding_min_healthy_rdonly_tablets'].namedPositional = 'min_healthy_rdonly_tablets';
    this.flags['horizontal_resharding_skip_split_ratio_check'] = new HorizontalReshardingSkipSplitRatioCheckFlag(14, 'horizontal_resharding_skip_split_ratio_check', 'Skip Split Ratio Check', 'horizontal_resharding');
    this.flags['horizontal_resharding_skip_split_ratio_check'].positional = true;
    this.flags['horizontal_resharding_skip_split_ratio_check'].namedPositional = 'skip_split_ratio_check';
    this.flags['horizontal_resharding_use_consistent_snapshot'] = new HorizontalReshardingConsistentSnapshotFlag(15, 'horizontal_resharding_use_consistent_snapshot', 'Use Consistent Snapshot', 'horizontal_resharding');
    this.flags['horizontal_resharding_use_consistent_snapshot'].positional = true;
    this.flags['horizontal_resharding_use_consistent_snapshot'].namedPositional = 'use_consistent_snapshot';
    this.flags['horizontal_resharding_enable_approvals_copy_schema'] = new HorizontalReshardingEnableApprovalsFlag(16, 'horizontal_resharding_enable_approvals_copy_schema', 'Copy Schema enable approvals', 'horizontal_resharding');
    this.flags['horizontal_resharding_enable_approvals_copy_schema'].positional = true;
    this.flags['horizontal_resharding_enable_approvals_copy_schema'].namedPositional = 'copy_schema';
    this.flags['horizontal_resharding_enable_approvals_clone'] = new HorizontalReshardingEnableApprovalsFlag(17, 'horizontal_resharding_enable_approvals_clone', 'Clone enable approvals', 'horizontal_resharding');
    this.flags['horizontal_resharding_enable_approvals_clone'].positional = true;
    this.flags['horizontal_resharding_enable_approvals_clone'].namedPositional = 'clone';
    this.flags['horizontal_resharding_enable_approvals_wait_filtered_replication'] = new HorizontalReshardingEnableApprovalsFlag(18, 'horizontal_resharding_enable_approvals_wait_filtered_replication', 'Wait filtered replication enable approvals', 'horizontal_resharding');
    this.flags['horizontal_resharding_enable_approvals_wait_filtered_replication'].positional = true;
    this.flags['horizontal_resharding_enable_approvals_wait_filtered_replication'].namedPositional = 'wait_for_filtered_replication';
    this.flags['horizontal_resharding_enable_approvals_diff'] = new HorizontalReshardingEnableApprovalsFlag(19, 'horizontal_resharding_enable_approvals_diff', 'Diff enable approvals', 'horizontal_resharding');
    this.flags['horizontal_resharding_enable_approvals_diff'].positional = true;
    this.flags['horizontal_resharding_enable_approvals_diff'].namedPositional = 'diff';
    this.flags['horizontal_resharding_enable_approvals_migrate_serving_types'] = new HorizontalReshardingEnableApprovalsFlag(20, 'horizontal_resharding_enable_approvals_migrate_serving_types', 'Migrate serving types enable approvals', 'horizontal_resharding');
    this.flags['horizontal_resharding_enable_approvals_migrate_serving_types'].positional = true;
    this.flags['horizontal_resharding_enable_approvals_migrate_serving_types'].namedPositional = 'migrate_rdonly,migrate_replica,migrate_master';


    this.flags['horizontal_resharding_phase_enable_approvals'] = new HorizontalReshardingPhaseEnableApprovalFlag(21, 'horizontal_resharding_phase_enable_approvals');
    this.flags['horizontal_resharding_phase_enable_approvals'].positional = true;
    this.flags['horizontal_resharding_phase_enable_approvals'].namedPositional = 'phase_enable_approvals';


    // // Flags for keyspace resharding workflow.
    this.flags['hr_workflow_gen_keyspace'] = new HorizontalReshardingKeyspaceFlag(22, 'hr_workflow_gen_keyspace', 'hr_workflow_gen');
    this.flags['hr_workflow_gen_keyspace'].positional = true;
    this.flags['hr_workflow_gen_keyspace'].namedPositional = 'keyspace';
    this.flags['hr_workflow_gen_vtworkers'] = new HorizontalReshardingVtworkerFlag(23, 'hr_workflow_gen_vtworkers', 'hr_workflow_gen');
    this.flags['hr_workflow_gen_vtworkers'].positional = true;
    this.flags['hr_workflow_gen_vtworkers'].namedPositional = 'vtworkers';
    this.flags['hr_workflow_gen_exclude_tables'] = new HorizontalReshardingExcludeTablesFlag(24, 'hr_workflow_gen_exclude_tables', 'hr_workflow_gen');
    this.flags['hr_workflow_gen_exclude_tables'].positional = true;
    this.flags['hr_workflow_gen_exclude_tables'].namedPositional = 'exclude_tables';
    this.flags['hr_workflow_gen_split_cmd'] = new SplitCloneCommand(25, 'hr_workflow_gen_split_cmd', 'hr_workflow_gen');
    this.flags['hr_workflow_gen_split_cmd'].positional = true;
    this.flags['hr_workflow_gen_split_cmd'].namedPositional = 'split_cmd';
    this.flags['hr_workflow_gen_split_diff_cmd'] = new SplitDiffCommand(26, 'hr_workflow_gen_split_diff_cmd', 'hr_workflow_gen');
    this.flags['hr_workflow_gen_split_diff_cmd'].positional = true;
    this.flags['hr_workflow_gen_split_diff_cmd'].namedPositional = 'split_diff_cmd';
    this.flags['hr_workflow_gen_split_diff_dest_tablet_type'] = new SplitDiffTabletType(27, 'hr_workflow_gen_split_diff_dest_tablet_type', 'hr_workflow_gen');
    this.flags['hr_workflow_gen_split_diff_dest_tablet_type'].positional = true;
    this.flags['hr_workflow_gen_split_diff_dest_tablet_type'].namedPositional = 'split_diff_dest_tablet_type';
    this.flags['hr_workflow_gen_min_healthy_rdonly_tablets'] = new HorizontalReshardingMinHealthyRdonlyTablets(28, 'hr_workflow_gen_min_healthy_rdonly_tablets', 'hr_workflow_gen');
    this.flags['hr_workflow_gen_min_healthy_rdonly_tablets'].positional = true;
    this.flags['hr_workflow_gen_min_healthy_rdonly_tablets'].namedPositional = 'min_healthy_rdonly_tablets';
    this.flags['hr_workflow_gen_skip_split_ratio_check'] = new HorizontalReshardingSkipSplitRatioCheckFlag(29, 'hr_workflow_gen_skip_split_ratio_check', 'Skip Split Ratio Check', 'hr_workflow_gen');
    this.flags['hr_workflow_gen_skip_split_ratio_check'].positional = true;
    this.flags['hr_workflow_gen_skip_split_ratio_check'].namedPositional = 'skip_split_ratio_check';
    this.flags['hr_workflow_gen_use_consistent_snapshot'] = new HorizontalReshardingConsistentSnapshotFlag(30, 'hr_workflow_gen_use_consistent_snapshot', 'Use Consistent Snapshot', 'hr_workflow_gen');
    this.flags['hr_workflow_gen_use_consistent_snapshot'].positional = true;
    this.flags['hr_workflow_gen_use_consistent_snapshot'].namedPositional = 'use_consistent_snapshot';
    this.flags['hr_workflow_gen_enable_approvals_copy_schema'] = new HorizontalReshardingEnableApprovalsFlag(31, 'hr_workflow_gen_enable_approvals_copy_schema', 'Copy Schema enable approvals', 'hr_workflow_gen');
    this.flags['hr_workflow_gen_enable_approvals_copy_schema'].positional = true;
    this.flags['hr_workflow_gen_enable_approvals_copy_schema'].namedPositional = 'copy_schema';
    this.flags['hr_workflow_gen_enable_approvals_clone'] = new HorizontalReshardingEnableApprovalsFlag(32, 'hr_workflow_gen_enable_approvals_clone', 'Clone enable approvals', 'hr_workflow_gen');
    this.flags['hr_workflow_gen_enable_approvals_clone'].positional = true;
    this.flags['hr_workflow_gen_enable_approvals_clone'].namedPositional = 'clone';
    this.flags['hr_workflow_gen_enable_approvals_wait_filtered_replication'] = new HorizontalReshardingEnableApprovalsFlag(33, 'hr_workflow_gen_enable_approvals_wait_filtered_replication', 'Wait filtered replication enable approvals', 'hr_workflow_gen');
    this.flags['hr_workflow_gen_enable_approvals_wait_filtered_replication'].positional = true;
    this.flags['hr_workflow_gen_enable_approvals_wait_filtered_replication'].namedPositional = 'wait_for_filtered_replication';
    this.flags['hr_workflow_gen_enable_approvals_diff'] = new HorizontalReshardingEnableApprovalsFlag(34, 'hr_workflow_gen_enable_approvals_diff', 'Diff enable approvals', 'hr_workflow_gen');
    this.flags['hr_workflow_gen_enable_approvals_diff'].positional = true;
    this.flags['hr_workflow_gen_enable_approvals_diff'].namedPositional = 'diff';
    this.flags['hr_workflow_gen_enable_approvals_migrate_serving_types'] = new HorizontalReshardingEnableApprovalsFlag(35, 'hr_workflow_gen_enable_approvals_migrate_serving_types', 'Migrate serving types enable approvals', 'hr_workflow_gen');
    this.flags['hr_workflow_gen_enable_approvals_migrate_serving_types'].positional = true;
    this.flags['hr_workflow_gen_enable_approvals_migrate_serving_types'].namedPositional = 'migrate_rdonly,migrate_replica,migrate_master';


    this.flags['hr_workflow_gen_phase_enable_approvals'] = new HorizontalReshardingPhaseEnableApprovalFlag(36, 'hr_workflow_gen_phase_enable_approvals');
    this.flags['hr_workflow_gen_phase_enable_approvals'].positional = true;
    this.flags['hr_workflow_gen_phase_enable_approvals'].namedPositional = 'phase_enable_approvals';
    this.flags['hr_workflow_gen_skip_start_workflows'] = new ReshardingWorkflowGenSkipStartFlag(37, 'hr_workflow_gen_skip_start_workflows');
    this.flags['hr_workflow_gen_skip_start_workflows'].positional = true;
    this.flags['hr_workflow_gen_skip_start_workflows'].namedPositional = 'skip_start_workflows';

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

export class SplitDiffCommand extends DropDownFlag {
  constructor(position: number, id: string, setDisplayOn: string) {
    super(position, id, 'Split Diff Command', 'Specifies the split diff command to use.', '');
    let options = [];
    options.push({
      label: 'SplitDiff',
      value: 'SplitDiff'
    });
    options.push({
      label: 'MultiSplitDiff',
      value: 'MultiSplitDiff'
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
    if (workflows.hr_workflow_gen) {
      options.push({
          label: 'Horizontal Resharding Workflow Generator',
          value: 'hr_workflow_gen'
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
  constructor(position: number, id: string, value= true) {
    super(position, id, 'Skip Start', 'Create the workflow, but don\'t start it.', value);
  }
}

export class ReshardingWorkflowGenSkipStartFlag extends CheckBoxFlag {
    constructor(position: number, id: string, value= true) {
        super(position, id, 'Skip Start Workflows', 'Skip start in all horizontal resharding workflows to be created.', value);
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

export class HorizontalReshardingExcludeTablesFlag extends InputFlag {
  constructor(position: number, id: string, setDisplayOn: string, value= '') {
    super(position, id, 'exclude tables', 'Comma-separated list of tables to exclude', value);
    this.setDisplayOn('factory_name', setDisplayOn);
  }
}

export class HorizontalReshardingPhaseEnableApprovalFlag extends InputFlag {
  // This is a hidden flag
  constructor(position: number, id: string, value= '') {
    super(position, id, 'phase enable approval', '', value);
    this.setDisplayOn('factory_name', '__hidden__');
  }
}
export class HorizontalReshardingMinHealthyRdonlyTablets extends InputFlag {
  constructor(position: number, id: string, setDisplayOn: string, value= '') {
    super(position, id, 'min healthy rdonly tablets', 'Minimum number of healthy RDONLY tablets required in source shards', value);
    this.setDisplayOn('factory_name', setDisplayOn);
  }
}

export class HorizontalReshardingEnableApprovalsFlag extends CheckBoxFlag {
  constructor(position: number, id: string, name: string, setDisplayOn: string, value=true) {
    super(position, id, name, 'Set true if use user approvals of task execution.', value);
    this.setDisplayOn('factory_name', setDisplayOn);
  }
}

export class HorizontalReshardingConsistentSnapshotFlag extends CheckBoxFlag {
  constructor(position: number, id: string, name: string, setDisplayOn: string, value = false) {
    super(position, id, name, 'Use consistent snapshot to have a stable view of the data.', value);
    this.setDisplayOn('factory_name', setDisplayOn);
  }
}

export class HorizontalReshardingSkipSplitRatioCheckFlag extends CheckBoxFlag {
  constructor(position: number, id: string, name: string, setDisplayOn: string, value = false) {
    super(position, id, name, 'Skip the validation on minimum healthy rdonly tablets', value);
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
