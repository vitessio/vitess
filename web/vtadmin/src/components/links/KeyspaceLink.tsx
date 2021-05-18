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

import React from 'react';
import { Link } from 'react-router-dom';
import { stringify } from '../../util/queryString';

interface Props {
    className?: string;
    clusterID: string | null | undefined;
    name: string | null | undefined;
    // Shorthand property equivalent to "shardFilter:${shard}"
    // Note that `shard` will be ignored if `shardFilter` is defined.
    shard?: string | null | undefined;
    shardFilter?: string | null | undefined;
}

export const KeyspaceLink: React.FunctionComponent<Props> = ({
    children,
    className,
    clusterID,
    name,
    shard,
    ...props
}) => {
    if (!clusterID || !name) {
        return <span className={className}>{children}</span>;
    }

    let shardFilter = props.shardFilter;
    if (!shardFilter && !!shard) {
        shardFilter = `shard:${shard}`;
    }

    const to = {
        pathname: `/keyspace/${clusterID}/${name}`,
        search: stringify({ shardFilter }),
    };

    return (
        <Link className={className} to={to}>
            {children}
        </Link>
    );
};
