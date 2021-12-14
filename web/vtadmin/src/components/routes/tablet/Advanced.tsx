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
            <div className="my-8">
                <h3 className="mb-4 font-medium">Replication</h3>
                <div className="w-full border rounded-lg border-gray-400" >
                    <div className="p-8 border-b border-gray-400">
                        <p className="text-base font-bold m-0">Start Replication</p>
                        <p className="text-base m-0">
                            This will run the underlying database command to start replication on tablet <span className="font-bold">{alias}</span>. For example, in mysql 5.7, this is <span className="font-mono text-sm p-1 bg-gray-100">start slave</span>.
                        </p>
                        {isPrimary(tablet) && <p className="text-danger"><Icon icon={Icons.alertFail} className='fill-current text-danger inline mr-2' />Command StartTablet cannot be run on the primary tablet.</p>}
                        <button className="btn btn-secondary mt-4" disabled={isPrimary(tablet)}>Start replication</button>
                    </div>
                    <div className="p-8">
                        <p className="text-base font-bold m-0">Stop Replication</p>
                        <p className="text-base m-0">
                            This will run the underlying database command to stop replication on tablet <span className="font-bold">{alias}</span>. For example, in mysql 5.7, this is <span className="font-mono text-sm p-1 bg-gray-100">stop slave</span>.
                        </p>
                        {isPrimary(tablet) && <p className="text-danger"><Icon icon={Icons.alertFail} className='fill-current text-danger inline mr-2' />Command StopTablet cannot be run on the primary tablet.</p>}
                        <button className="btn btn-secondary mt-4" disabled={isPrimary(tablet)}>Stop replication</button>
                    </div>
                </div>
            </div>
            <div className="my-8">
                <h3 className="mb-4 font-medium">Danger</h3>
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
        </div>
    )
}

export default Advanced