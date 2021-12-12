import React from 'react';

interface MenuItemProps {
    className?: string;
    intent?: 'danger' | 'warning' | 'success' | 'none';
    onClick?: () => void;
}

const MenuItem: React.FC<MenuItemProps> = ({ children, className, intent = 'none', ...props }) => {
    return (
        <button
            onMouseDown={(e) => e.preventDefault()}
            className={`transition-colors font-sans border-none text-left block px-6 py-4 hover:bg-gray-100 text-${intent} hover:text-${
                intent === 'none' ? 'vtblue' : intent
            } w-full ${className || ''}`}
            role="menuitem"
            tabIndex={-1}
            {...props}
        >
            {children}
        </button>
    );
};

export default MenuItem;
