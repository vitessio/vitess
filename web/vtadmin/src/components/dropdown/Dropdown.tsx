import React, { useState } from 'react';
import { Icon, Icons } from '../Icon';
import style from './Dropdown.module.scss';
interface DropdownProps {
    // Optionally pass in your own button if you don't want it styled like DropdownButton
    dropdownButton: React.FC<DropdownButtonProps> | Icons
    position?: 'top-left' | 'top-right' | 'bottom-right' | 'bottom-left';
}

export interface DropdownButtonProps {
    ariaExpanded: boolean
}

const positions: Record<string, string> = {
    'top-left': style.topLeft,
    'top-right': style.topRight,
    'bottom-right': 'left-0',
    'bottom-left': 'right-0',
    default: 'right-0',
};

export const DropdownButton: React.FC<DropdownButtonProps & { icon: Icons }> = ({ ariaExpanded, icon }) => (<button
    type="button"
    className="flex relative justify-center items-center border border-gray-300 shadow-sm h-12 w-12 bg-white text-sm font-medium text-gray-700 hover:bg-gray-50 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-offset-gray-100 focus:ring-indigo-500 focus:z-10"
    id="menu-button"
    aria-expanded={ariaExpanded}
    aria-haspopup="true"
    aria-label="change"
    title="change"
>
    <div className="transform scale-75">
        <Icon icon={icon} />
    </div>
</button>)

const Dropdown: React.FC<DropdownProps> = ({ children, dropdownButton, position }) => {
    const [open, setOpen] = useState(false);

    let button;
    if (typeof dropdownButton == 'string') {
        button = <DropdownButton ariaExpanded={open} icon={dropdownButton as Icons} />
    } else {
        const ButtonComponent = dropdownButton as React.FC<DropdownButtonProps>
        button = <ButtonComponent ariaExpanded={open} />
    }
    return (
        <div className="relative inline-block text-left" onBlur={() => setOpen(false)}>
            <div onClick={() => setOpen(!open)}>{button}</div>

            <div
                className={`${open ? style.entering : style.leaving} py-2 z-1 origin-top-right absolute ${positions[position as string] || positions.default
                    } md:-left-3full mt-2 w-max rounded-lg shadow-lg bg-white ring-1 ring-black ring-opacity-5 divide-y divide-gray-100 focus:outline-none`}
                role="menu"
                aria-orientation="vertical"
                aria-labelledby="menu-button"
                tabIndex={-1}
            >
                {children}
            </div>
        </div>
    );
};

export default Dropdown;
