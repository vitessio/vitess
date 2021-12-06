import React, { useState } from 'react';
import Dropdown from '../../dropdown/Dropdown';
import MenuItem from '../../dropdown/MenuItem';
import { Icons } from '../../Icon';
import PingDialog from './PingDialog';

interface InfoDropdownProps {
    alias: string;
    clusterID?: string;
}

const InfoDropdown: React.FC<InfoDropdownProps> = ({ alias, clusterID }) => {
    const [isPingOpen, setPingOpen] = useState(false);

    return (
        <div className="w-min inline-block">
            <Dropdown
                dropdownButton={Icons.info}
                position="bottom-right"
            >
                <MenuItem onClick={() => setPingOpen(true)}>Ping</MenuItem>
                <MenuItem>Refresh state</MenuItem>
                <MenuItem>Run health check</MenuItem>
            </Dropdown >
            <PingDialog alias={alias} clusterID={clusterID} isOpen={isPingOpen} onClose={() => setPingOpen(false)} />
        </div >
    );
};

export default InfoDropdown;
