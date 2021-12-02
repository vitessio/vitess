import React, { useState } from 'react'
import style from './Dropdown.module.scss'

interface DropdownProps {
    button: React.ReactElement
    position?: 'top-left' | 'top-right' | 'bottom-right' | 'bottom-left'
}

const positions: Record<string, string> = {
    'top-left': style.topLeft,
    'top-right': style.topRight,
    'bottom-right': 'left-0',
    'bottom-left': 'right-0',
    'default': 'right-0'
}

const Dropdown: React.FC<DropdownProps> = ({ children, button, position }) => {
    const [open, setOpen] = useState(false)
    return (
        <div className="relative inline-block text-left" onBlur={() => setOpen(false)}>
            <div onClick={() => setOpen(!open)}>
                {button}
            </div>

            <div
                className={`${open ? style.entering : style.leaving} pt-2 z-1 origin-top-right absolute ${positions[position as string] || positions.default} mt-2 w-max rounded-lg shadow-lg bg-white ring-1 ring-black ring-opacity-5 divide-y divide-gray-100 focus:outline-none`}
                role="menu"
                aria-orientation="vertical"
                aria-labelledby="menu-button"
                tabIndex={-1}
            >
                {children}
            </div>
        </div>
    )
}

export default Dropdown