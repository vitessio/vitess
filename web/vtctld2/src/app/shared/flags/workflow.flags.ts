import { CheckBoxFlag, DropDownFlag, InputFlag } from './flag';

export class NewWorkflowFlags {
  flags= {};
  constructor() {
    // General flags.
    this.flags['skip_start'] = new SkipStartFlag(0, 'skip_start');
    this.flags['skip_start'].positional = true;
    this.flags['skip_start'].namedPositional = 'skip_start';
    this.flags['factory_name'] = new FactoryNameFlag(1, 'factory_name');
    this.flags['factory_name'].positional = true;

    // Flags for the Sleep workflow.
    this.flags['sleep_duration'] = new SleepDurationFlag(2, 'sleep_duration');
    this.flags['sleep_duration'].positional = true;
    this.flags['sleep_duration'].namedPositional = 'duration';
  }
}

export class FactoryNameFlag extends DropDownFlag {
  constructor(position: number, id: string, value= 'sleep') {
    super(position, id, 'Factory Name', 'Specifies the type of workflow to create.', value);
    let options = [
      {
        label: 'Sleep',
        value: 'sleep'
      },
      // Remove other when we have other workflows. This is just meant to
      // test the display of the Sleep Duration parameter.
      {
        label: 'Other',
        value: 'other'
      }
    ];
    this.setOptions(options);
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
