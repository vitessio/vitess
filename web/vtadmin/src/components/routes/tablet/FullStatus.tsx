import React from 'react';
import { Link } from 'react-router-dom';
import { useGetFullStatus } from '../../../hooks/api';
import { topodata, vtadmin } from '../../../proto/vtadmin';
import { formatAlias } from '../../../util/tablets';
import { Code } from '../../Code';
import style from './Tablet.module.scss';
import { isNil } from 'lodash-es';

interface Props {
    tablet: vtadmin.Tablet;
}

function stateReplacer(key: any, val: any) {
    if (key === 'io_state' || key === 'sql_state') {
        if (val === 3) {
            return 'Replication Running';
        } else if (val === 2) {
            return 'Replication Connected';
        } else if (val === 1) {
            return 'Replication Stopped';
        }
    }
    return val;
}

const FullStatus: React.FC<Props> = ({ tablet }) => {
    const { data, error } = useGetFullStatus({
        // Ok to use ? operator here; if params are null
        // will fall back to error = true case
        clusterID: tablet.cluster?.id as string,
        alias: formatAlias(tablet.tablet?.alias) as string,
    });

    if (error) {
        return (
            <div className={style.placeholder}>
                <span className={style.errorEmoji}>😰</span>
                <h1>An error occurred</h1>
                <code>{error.message}</code>
                <p>
                    <Link to="/tablets">← All tablets</Link>
                </p>
            </div>
        );
    }
    if (!isNil(data) && !isNil(data.status)) {
        if (data.status.semi_sync_primary_enabled !== true) {
            data.status.semi_sync_primary_enabled = false;
        }
        if (data.status.semi_sync_replica_enabled !== true) {
            data.status.semi_sync_replica_enabled = false;
        }
        if (data.status.semi_sync_primary_status !== true) {
            data.status.semi_sync_primary_status = false;
        }
        if (data.status.semi_sync_replica_status !== true) {
            data.status.semi_sync_replica_status = false;
        }
    }

    return <Code code={JSON.stringify(data, stateReplacer, 2)} />;
};

export default FullStatus;
