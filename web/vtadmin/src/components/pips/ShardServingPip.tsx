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
    // If shard status is still pending, a neutral pip is used.
    // The distinction between "is loading" and "is_master_serving is undefined"
    // is useful, as it's common for the shard `shard.is_master_serving` to be
    // excluded from API responses for non-serving shards (instead of being
    // explicitly false.)
    isLoading?: boolean | null | undefined;
    isServing?: boolean | null | undefined;
}

export const ShardServingPip = ({ isLoading, isServing }: Props) => {
    let state: PipState = isServing ? 'success' : 'danger';
    return <Pip state={isLoading ? null : state} />;
};
