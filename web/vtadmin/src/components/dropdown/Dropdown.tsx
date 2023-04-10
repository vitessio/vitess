import React, { Fragment } from 'react';
import { Icon, Icons } from '../Icon';
import { Menu, Transition } from '@headlessui/react';
import style from './Dropdown.module.scss';
interface DropdownProps {
    // Optionally pass in your own button if you don't want it styled like DropdownButton
    dropdownButton: React.FC | Icons;
    position?: 'top-left' | 'top-right' | 'bottom-right' | 'bottom-left';
}

const positions: Record<string, string> = {
    'top-left': style.topLeft,
    'top-right': style.topRight,
    'bottom-right': 'left-0',
    'bottom-left': 'right-0',
    default: 'right-0',
};

export const DropdownButton: React.FC<{ icon: Icons }> = ({ icon }) => (
    <Menu.Button
        type="button"
        className="flex relative justify-center items-center border border-gray-300 shadow-sm h-12 w-12 bg-white text-lg font-medium text-gray-700 hover:bg-gray-50 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-offset-gray-100 focus:ring-indigo-500 focus:z-10"
        id="menu-button"
        aria-haspopup="true"
        aria-label="change"
        title="change"
    >
        <div className="transform">
            <Icon icon={icon} />
        </div>
    </Menu.Button>
);

const Dropdown: React.FC<DropdownProps> = ({ children, dropdownButton, position }) => {
    let button;
    if (typeof dropdownButton == 'string') {
        button = <DropdownButton icon={dropdownButton as Icons} />;
    } else {
        const ButtonComponent = dropdownButton as React.FC;
        button = <ButtonComponent />;
    }
    return (
        <Menu as="div" className="relative inline-block text-left">
            {button}
            <Transition
                as={Fragment}
                enter="transition ease-out duration-100"
                enterFrom="transform opacity-0 scale-95"
                enterTo="transform opacity-100 scale-100"
                leave="transition ease-in duration-75"
                leaveFrom="transform opacity-100 scale-100"
                leaveTo="transform opacity-0 scale-95"
            >
                <Menu.Items
                    className={`py-2 z-10 origin-top-right absolute ${
                        positions[position as string] || positions.default
                    } md:-left-3full mt-2 w-max rounded-lg shadow-lg bg-white ring-1 ring-black ring-opacity-5 divide-y divide-gray-100 focus:outline-none`}
                    role="menu"
                    aria-orientation="vertical"
                    aria-labelledby="menu-button"
                >
                    {children}
                </Menu.Items>
            </Transition>
        </Menu>
    );
};

export default Dropdown;
