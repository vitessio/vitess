import React, { useState } from 'react';
import Dropdown, { DropdownButtonProps } from '../../dropdown/Dropdown';
import MenuItem from '../../dropdown/MenuItem';
import { Icon, Icons } from '../../Icon';
import PingDialog from './PingDialog';

interface InfoDropdownProps {
    alias: string;
    clusterID?: string;
}

const InfoDropdown: React.FC<InfoDropdownProps> = ({ alias, clusterID }) => {
    const [isPingOpen, setPingOpen] = useState(false);
    const Button: React.FC<DropdownButtonProps> = ({ ariaExpanded }) => (<button
        type="button"
        className="flex relative justify-center items-center border border-gray-300 shadow-sm h-12 w-12 bg-white text-sm font-medium text-gray-700 hover:bg-gray-50 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-offset-gray-100 focus:ring-indigo-500 focus:z-10"
        id="menu-button"
        aria-label="info-dropdown"
        aria-expanded={ariaExpanded}
        title="info-dropdown"
    >
        <div className="transform scale-75">
            <Icon icon={Icons.info} />
        </div>
    </button>)
    return (
        <div className="w-min inline-block">
            <Dropdown
                Button={Button}
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
