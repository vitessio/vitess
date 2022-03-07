/**
 * Copyright 2021 The Vitess Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { invert } from 'lodash';
import { mysqlctl } from '../proto/vtadmin';

/**
 * BACKUP_STATUSES maps numeric tablet types back to human-readable strings.
 */
export const BACKUP_STATUSES = invert(mysqlctl.BackupInfo.Status);

/**
 * formatStatus formats a Backup.Status enum value as a human-readable string.
 * Defaults to Backup.Status.UNKNOWN for null, undefined, or otherwise invalid values.
 */
export const formatStatus = (status: mysqlctl.BackupInfo.Status | null | undefined) => {
    if (typeof status !== 'number' || !(status in BACKUP_STATUSES)) {
        return BACKUP_STATUSES[mysqlctl.BackupInfo.Status.UNKNOWN];
    }

    return BACKUP_STATUSES[status];
};
