import React, { useState } from 'react'
import { useParams } from 'react-router-dom';
import { vtadmin } from '../../../proto/vtadmin';
import { isPrimary } from '../../../util/tablets';
import { Icon, Icons } from '../../Icon';
import { TextInput } from '../../TextInput';

interface AdvancedProps {
    tablet: vtadmin.Tablet | undefined
}

interface RouteParams {
    alias: string;
    clusterID: string;
}

const Advanced: React.FC<AdvancedProps> = ({ tablet }) => {
    const { clusterID, alias } = useParams<RouteParams>();
    const [typedAlias, setTypedAlias] = useState('')
    return (
        <div>
            <h3 className="mt-8 mb-2 font-medium">Danger</h3>
            <div className="border border-danger rounded-lg p-8">
                <p className="text-base font-bold m-0">Delete Tablet</p>
                <p className="text-base">
                    Delete tablet <span className="font-bold">{alias}</span>. Doing so will remove it from the topology, but vttablet and MySQL won't be touched.
                </p>
                {isPrimary(tablet) && <p className="text-danger"><Icon icon={Icons.alertFail} className='fill-current text-danger inline mr-2' />Tablet {alias} is the primary tablet. Flag <span className="font-mono bg-red-100 p-1 text-sm">-allow_master=true</span> will be applied in order to delete the primary tablet.</p>}

                <p className="text-base">Please type the tablet's alias to delete the tablet:</p>
                <div className="w-1/3">
                    <TextInput placeholder="zone-xxx" value={typedAlias} onChange={e => setTypedAlias(e.target.value)} />
                </div>
                <button className="btn btn-secondary btn-danger mt-4" disabled={typedAlias !== alias}>Delete</button>
            </div>
        </div>
    )
}

export default Advanced