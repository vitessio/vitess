import React, { useState } from 'react';
import Dropdown from '../../dropdown/Dropdown';
import MenuItem from '../../dropdown/MenuItem';
import { Icons } from '../../Icon';
import Dialog from '../../dialog/Dialog'
interface KeyspaceActionsProps {
    keyspace: string
    clusterID: string
}

const KeyspaceActions: React.FC<KeyspaceActionsProps> = ({ keyspace, clusterID }) => {
    const [isOpen, setIsOpen] = useState(false);

    const closeDialog = () => setIsOpen(false);
    const openDialog = () => {
        setIsOpen(true);
    };


    return (
        <div className="w-min inline-block">
            <Dropdown dropdownButton={Icons.info} position="bottom-right">
                <MenuItem onClick={openDialog}>Validate Keyspace</MenuItem>
            </Dropdown>
            <Dialog
                isOpen={isOpen}
                onClose={closeDialog}
                confirmText="Validate"
                cancelText="Cancel"
                onCancel={closeDialog}
                title="Validate Keyspace"
                description={`Validates that all nodes reachable from keyspace "${keyspace}" are consistent.`}
            >
                <div className="h-12 w-12">
                    Hello
                </div>
            </Dialog>
        </div>
    );
};

export default KeyspaceActions;
