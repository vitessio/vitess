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

import { Pip, PipState } from './Pip';
import { mysqlctl } from '../../proto/vtadmin';

interface Props {
    status: mysqlctl.BackupInfo.Status | null | undefined;
}

const STATUS_STATES: { [s in mysqlctl.BackupInfo.Status]: PipState } = {
    [mysqlctl.BackupInfo.Status.UNKNOWN]: null,
    [mysqlctl.BackupInfo.Status.INCOMPLETE]: 'warning',
    [mysqlctl.BackupInfo.Status.COMPLETE]: 'primary',
    [mysqlctl.BackupInfo.Status.INVALID]: 'danger',
    [mysqlctl.BackupInfo.Status.VALID]: 'success',
};

export const BackupStatusPip = ({ status }: Props) => {
    const state = status ? STATUS_STATES[status] : null;
    return <Pip state={state} />;
};
