import React from 'react'

interface MenuItemProps {
    className?: String
    intent?: 'danger' | 'warning' | 'success' | 'none'
    onClick?: () => void
}

const MenuItem: React.FC<MenuItemProps> = ({ children, className, intent = 'none', ...props }) => {
    return (
        <button onMouseDown={(e) => e.preventDefault()}
            className={`transition-colors font-sans border-none text-left block px-4 py-2 hover:bg-gray-100 text-${intent} hover:text-${intent === 'none' ? 'vitess-blue' : intent} w-full ${className || ''}`}
            role="menuitem"
            tabIndex={-1}
            {...props}
        >
            {children}
        </button>
    )
}

export default MenuItem