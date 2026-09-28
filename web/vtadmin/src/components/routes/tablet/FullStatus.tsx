import React from 'react';
import { Link } from 'react-router-dom';
import { useGetFullStatus } from '../../../hooks/api';
import { vtadmin } from '../../../proto/vtadmin';
import { formatAlias } from '../../../util/tablets';
import { Code } from '../../Code';
import style from './Tablet.module.scss';

interface Props {
    tablet: vtadmin.Tablet;
}

function stateReplacer(key: string, val: number) {
    if (key === 'io_state' || key === 'sql_state') {
        if (val === 3) {
            return 'Running';
        } else if (val === 2) {
            return 'Connecting';
        } else if (val === 1) {
            return 'Stopped';
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

    if (data && data.status) {
        data.status.semi_sync_primary_enabled = !!data.status.semi_sync_primary_enabled;
        data.status.semi_sync_replica_enabled = !!data.status.semi_sync_replica_enabled;
        data.status.semi_sync_primary_status = !!data.status.semi_sync_primary_status;
        data.status.semi_sync_replica_status = !!data.status.semi_sync_replica_status;
    }

    return <Code code={JSON.stringify(data, stateReplacer, 2)} />;
};

export default FullStatus;
