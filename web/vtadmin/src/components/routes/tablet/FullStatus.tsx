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

    return <Code code={JSON.stringify(data, null, 2)} />;
};

export default FullStatus;
