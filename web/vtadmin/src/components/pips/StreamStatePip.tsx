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

interface Props {
    state?: string | null | undefined;
}

// TODO(doeg): add a protobuf enum for this (https://github.com/vitessio/vitess/projects/12#card-60190340)
const STREAM_STATES: { [key: string]: PipState } = {
    Copying: 'primary',
    Error: 'danger',
    Running: 'success',
    Stopped: null,
};

export const StreamStatePip = ({ state }: Props) => {
    const pipState = state ? STREAM_STATES[state] : null;
    return <Pip state={pipState} />;
};
