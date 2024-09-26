import React from 'react';
import { Intent } from '../intent';

interface MenuItemProps {
    className?: string;
    intent?: Intent;
    disabled?: boolean;
    onClick?: () => void;
}

const MenuItem: React.FC<MenuItemProps> = ({ children, disabled, className, intent = 'none', ...props }) => {
    return (
        <button
            disabled={disabled}
            onMouseDown={(e) => e.preventDefault()}
            className={`transition-colors font-sans border-none text-left block px-6 py-4 disabled:bg-gray-200 disabled:text-secondary hover:bg-gray-100 text-${intent} hover:text-${
                intent === Intent.none ? 'vtblue' : intent
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
