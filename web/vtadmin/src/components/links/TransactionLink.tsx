/**
 * Copyright 2024 The Vitess Authors.
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

interface Props {
    className?: string;
    clusterID: string | null | undefined;
    dtid: string | null | undefined;
}

export const TransactionLink: React.FunctionComponent<Props> = ({ children, className, clusterID, dtid, ...props }) => {
    if (!clusterID || !dtid) {
        return <span className={className}>{children}</span>;
    }

    const to = {
        pathname: `/transaction/${clusterID}/${dtid}`,
    };

    return (
        <Link className={className} to={to}>
            {children}
        </Link>
    );
};
