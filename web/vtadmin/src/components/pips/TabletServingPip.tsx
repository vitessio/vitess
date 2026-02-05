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
import { vtadmin as pb } from '../../proto/vtadmin';

interface Props {
    state?: pb.Tablet.ServingState | string | null | undefined;
}

const SERVING_STATES: { [ss in pb.Tablet.ServingState]: PipState } = {
    [pb.Tablet.ServingState.NOT_SERVING]: 'danger',
    [pb.Tablet.ServingState.SERVING]: 'success',
    [pb.Tablet.ServingState.UNKNOWN]: null,
};

export const TabletServingPip = ({ state }: Props) => {
    const ss = state && typeof state === 'number' ? SERVING_STATES[state] : null;
    return <Pip state={ss} />;
};
