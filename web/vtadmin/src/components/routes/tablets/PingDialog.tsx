import React from 'react'
import { usePingTablet } from '../../../hooks/api';
import Dialog from '../../dialog/Dialog';

const PingDialog: React.FC<{ alias: string, clusterID?: string, isOpen: boolean, onClose: () => void }> = ({ alias, clusterID, isOpen, onClose }) => {
    const { data: pingResponse } = usePingTablet({ alias, clusterID });
    return (
        <Dialog title={`Pinging tablet ${alias}`} isOpen={isOpen} onClose={onClose}>
            <div>{String(pingResponse)}</div>
        </Dialog>
    )
}

export default PingDialog