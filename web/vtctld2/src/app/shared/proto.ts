/*
  All members of the Proto class need to be kept in sync with their respective
  Proto files.
*/
export class Proto {
  // enum TabletType: vitess/proto/topodata.proto
  public static get VT_TABLET_TYPES(): string[] {
    return [
      'unknown', 'master', 'replica', 'rdonly', 'spare', 'experimental',
      'backup', 'restore', 'drained'
    ];
  }

  // enum KeyspaceIdType: vitess/proto/topodata.proto
  public static get SHARDING_COLUMN_TYPES() {
    return [
      'UNSET',
      'UINT64',
      'BYTES',
    ];
  }

  // enum KeyspaceIdType: vitess/proto/topodata.proto
  public static get SHARDING_COLUMN_NAME_TO_TYPE() {
    return {
      '': 'UNSET',
      'unsigned bigint': 'UINT64',
      'varbinary': 'BYTES',
    };
  }

  // enum KeyspaceIdType: vitess/proto/topodata.proto
  public static get SHARDING_COLUMN_NAMES() {
    return [
      '',
      'unsigned bigint',
      'varbinary',
    ];
  }
}
