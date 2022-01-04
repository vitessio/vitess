import React, { useState } from 'react';
import { useHealthCheck, usePingTablet, useRefreshState } from '../../../hooks/api';
import Dropdown from '../../dropdown/Dropdown';
import MenuItem from '../../dropdown/MenuItem';
import { Icons } from '../../Icon';
import InfoDialog, { BaseInfoDialogProps } from './InfoDialog';
interface InfoDropdownProps {
    alias: string;
    clusterID?: string;
}

const InfoDropdown: React.FC<InfoDropdownProps> = ({ alias, clusterID }) => {
    const [currentDialog, setCurrentDialog] = useState('');
    const [isOpen, setIsOpen] = useState(false);
    const closeDialog = () => setIsOpen(false);

    const openDialog = (key: string) => {
        setCurrentDialog(key);
        setIsOpen(true);
    };

    const dialogConfigs: Record<string, BaseInfoDialogProps> = {
        ping: {
            useHook: () => usePingTablet({ alias, clusterID }, { enabled: false }),
            successDescription: `Successfully reached tablet ${alias} via RPC.`,
            errorDescription: `There was an issue pinging tablet ${alias}`,
            loadingTitle: `Pinging tablet ${alias}`,
            loadingDescription: 'Checking to see if tablet is reachable via RPC...',
        },
        refresh: {
            useHook: () => useRefreshState({ alias, clusterID }, { enabled: false }),
            successDescription: `Successfully refreshed tablet ${alias}.`,
            errorDescription: `There was an issue refreshing tablet`,
            loadingTitle: `Refreshing tablet ${alias}`,
            loadingDescription: 'Refreshing tablet record on tablet...',
        },
        healthcheck: {
            useHook: () => useHealthCheck({ alias, clusterID }, { enabled: false }),
            successDescription: `Tablet ${alias} looks healthy.`,
            errorDescription: `There was an issue running a health check on the tablet`,
            loadingTitle: `Running health check`,
            loadingDescription: `Running health check on tablet ${alias}`,
        },
    };

    // Default config is needed to avoid "hook order" inconsistency
    // Queries are not executed until dialog is open; no unneccessary queries are executed as a result.

    const defaultConfig = dialogConfigs['ping'];

    return (
        <div className="w-min inline-block">
            <Dropdown dropdownButton={Icons.info} position="bottom-right">
                <MenuItem onClick={() => openDialog('ping')}>Ping</MenuItem>
                <MenuItem onClick={() => openDialog('refresh')}>Refresh state</MenuItem>
                <MenuItem onClick={() => openDialog('healthcheck')}>Run health check</MenuItem>
            </Dropdown>
            <InfoDialog isOpen={isOpen} onClose={closeDialog} {...(dialogConfigs[currentDialog] || defaultConfig)} />
        </div>
    );
};

export default InfoDropdown;
