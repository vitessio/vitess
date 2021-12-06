import React from 'react';
import Divider from '../../dropdown/Divider';
import Dropdown from '../../dropdown/Dropdown';
import MenuItem from '../../dropdown/MenuItem';
import { Icons } from '../../Icon';

interface ChangeDropdownProps { }

const ChangeDropdown: React.FC<ChangeDropdownProps> = () => {

    return (
        <Dropdown
            dropdownButton={Icons.wrench}
            position="bottom-right"
        >
            <MenuItem>Ignore Health Error</MenuItem>
            <MenuItem>Set ReadOnly</MenuItem>
            <MenuItem>Set ReadWrite</MenuItem>
            <Divider />
            <MenuItem intent="danger">Delete Tablet</MenuItem>
        </Dropdown>
    );
};

export default ChangeDropdown;
