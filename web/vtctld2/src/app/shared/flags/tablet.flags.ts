import { CheckBoxFlag, InputFlag } from './flag';

// Groups of flags for vtctl actions.
export class DeleteTabletFlags {
  flags= {};
  constructor(tabletAlias) {
    this.flags['tablet_alias'] = new TabletAlias(0, 'tablet_alias', tabletAlias);
    this.flags['tablet_alias']['positional'] = true;
    this.flags['allow_master'] = new AllowMasterFlag(1, 'allow_master');
  }
}

export class PingTabletFlags {
  flags= {};
  constructor(tabletAlias) {
    this.flags['tablet_alias'] = new TabletAlias(0, 'tablet_alias', tabletAlias);
    this.flags['tablet_alias']['positional'] = true;
  }
}

export class RefreshTabletFlags {
  flags= {};
  constructor(tabletAlias) {
    this.flags['tablet_alias'] = new TabletAlias(0, 'tablet_alias', tabletAlias);
    this.flags['tablet_alias']['positional'] = true;
  }
}

// Individual flags for vtctl actions.
export class AllowMasterFlag extends CheckBoxFlag {
  constructor(position: number, id: string, value= false) {
    super(position, id, 'Allow Master', 'Allows for the master tablet of a shard to be deleted. Use with caution.', value);
  }
}

export class TabletAlias extends InputFlag {
  constructor(position: number, id: string, value= '') {
    super(position, id, '', '', value, false);
  }
}
