import React from 'react'
import Dropdown from '../../dropdown/Dropdown';
import MenuItem from '../../dropdown/MenuItem';
import { Icon, Icons } from '../../Icon';

interface InfoDropdownProps {

}

const InfoDropdown: React.FC<InfoDropdownProps> = () => {
    return (
        <Dropdown button={
            <button 
                type="button" 
                className="flex relative justify-center items-center border border-gray-300 shadow-sm h-12 w-12 bg-white text-sm font-medium text-gray-700 hover:bg-gray-50 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-offset-gray-100 focus:ring-indigo-500 focus:z-10"
                id="menu-button"
                aria-expanded="true"
                aria-haspopup="true"
                aria-label='status'
                title='status'
            >
                <div className="transform scale-75"><Icon icon={Icons.info} /></div>
            </button>}
            position="bottom-right"
        >
            <MenuItem>Ping</MenuItem>
            <MenuItem>Refresh state</MenuItem>
            <MenuItem>Run health check</MenuItem>
        </Dropdown>
    )
}

export default InfoDropdown