import React from 'react';
import Divider from '../../dropdown/Divider';
import Dropdown, { DropdownButtonProps } from '../../dropdown/Dropdown';
import MenuItem from '../../dropdown/MenuItem';
import { Icon, Icons } from '../../Icon';

interface ChangeDropdownProps { }

const ChangeDropdown: React.FC<ChangeDropdownProps> = () => {
    const Button: React.FC<DropdownButtonProps> = ({ ariaExpanded }) => (<button
        type="button"
        className="-m-0.5 flex justify-center items-center border border-gray-300 shadow-sm h-12 w-12 bg-white text-sm font-medium text-gray-700 hover:bg-gray-50 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-offset-gray-100 focus:ring-indigo-500"
        id="menu-button"
        aria-expanded={ariaExpanded}
        aria-haspopup="true"
        aria-label="change"
        title="change"
    >
        <div className="transform scale-75">
            <Icon icon={Icons.wrench} />
        </div>
    </button>)
    return (
        <Dropdown
            Button={
                Button
            }
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
