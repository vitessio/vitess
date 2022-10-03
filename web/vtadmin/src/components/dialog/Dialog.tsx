import { Fragment, useRef } from 'react';
import { Dialog as HUDialog, Transition } from '@headlessui/react';
import { Icon, Icons } from '../Icon';

interface DialogProps {
    icon?: Icons;
    isOpen: boolean;
    title?: string;
    description?: string;
    content?: React.ReactElement;
    onCancel?: () => void;
    onConfirm?: Function;
    onClose?: () => void;
    loading?: boolean;
    loadingText?: string;
    confirmText?: string;
    cancelText?: string;
    footer?: React.ReactElement;
    hideFooter?: boolean;
    children?: React.ReactElement;
    hideConfirm?: boolean;
    hideCancel?: boolean;
    className?: string;
}

/**
 * Dialog is a controlled component, and its open state is controlled by the isOpen property.
 * User should pass in a function to set isOpen to the onClose property.
 */
const Dialog: React.FC<DialogProps> = ({
    icon,
    title,
    description,
    children,
    isOpen,
    loading,
    loadingText,
    confirmText,
    cancelText,
    footer,
    hideFooter,
    hideCancel,
    hideConfirm,
    onCancel,
    onConfirm,
    onClose,
    className,
}) => {
    const cancelButtonRef = useRef(null);

    return (
        <Transition.Root show={isOpen} as={Fragment}>
            <HUDialog
                as="div"
                className="fixed z-10 inset-0 overflow-y-auto"
                initialFocus={cancelButtonRef}
                onClose={(_) => {
                    onClose && onClose();
                }}
            >
                <div className="flex items-end justify-center min-h-screen pt-4 px-4 pb-20 text-center sm:block sm:p-0">
                    <Transition.Child
                        as={Fragment}
                        enter="ease-out duration-300"
                        enterFrom="opacity-0"
                        enterTo="opacity-100"
                        leave="ease-in duration-200"
                        leaveFrom="opacity-100"
                        leaveTo="opacity-0"
                    >
                        <HUDialog.Overlay className="fixed inset-0 bg-gray-500 bg-opacity-75 transition-opacity" />
                    </Transition.Child>

                    {/* This element is to trick the browser into centering the modal contents. */}
                    <span className="hidden sm:inline-block sm:align-middle sm:h-screen" aria-hidden="true">
                        &#8203;
                    </span>
                    <Transition.Child
                        as={Fragment}
                        enter="ease-out duration-300"
                        enterFrom="opacity-0 translate-y-4 sm:translate-y-0 sm:scale-95"
                        enterTo="opacity-100 translate-y-0 sm:scale-100"
                        leave="ease-in duration-200"
                        leaveFrom="opacity-100 translate-y-0 sm:scale-100"
                        leaveTo="opacity-0 translate-y-4 sm:translate-y-0 sm:scale-95"
                    >
                        <div
                            className={`inline-block align-bottom bg-white rounded-lg text-left overflow-hidden shadow-2xl transform transition-all sm:my-8 sm:align-middle sm:max-w-lg sm:w-full ${className}`}
                        >
                            <div className="bg-white px-4 py-5 sm:py-8 sm:px-6 w-full">
                                <div className="sm:flex sm:items-start">
                                    {icon && (
                                        <div className="mx-auto flex-shrink-0 flex items-center justify-center h-12 w-12 rounded-full bg-red-100 sm:mx-0 sm:h-10 sm:w-10">
                                            <Icon icon={icon} />
                                        </div>
                                    )}
                                    <div className="text-center sm:mt-0 sm:text-left w-full">
                                        {title && (
                                            <HUDialog.Title
                                                as="h2"
                                                className="text-xl leading-6 font-medium text-primary"
                                            >
                                                {title}
                                            </HUDialog.Title>
                                        )}
                                        {description && (
                                            <div className="mt-2">
                                                <p className="text-sm text-secondary">{description}</p>
                                            </div>
                                        )}
                                        {Boolean(children) && children}
                                    </div>
                                </div>
                            </div>
                            {!footer && !hideFooter && (
                                <div className="px-4 py-3 flex gap-2 sm:px-6 sm:flex-row-reverse">
                                    {!hideConfirm && (
                                        <button
                                            disabled={loading}
                                            type="button"
                                            className="btn"
                                            onClick={() => {
                                                onConfirm && onConfirm();
                                            }}
                                        >
                                            {loading ? loadingText : confirmText || 'Confirm'}
                                        </button>
                                    )}
                                    {!hideCancel && (
                                        <button
                                            type="button"
                                            className="btn btn-secondary"
                                            onClick={() => {
                                                onCancel && onCancel();
                                                onClose && onClose();
                                            }}
                                            ref={cancelButtonRef}
                                        >
                                            {cancelText || 'Cancel'}
                                        </button>
                                    )}
                                </div>
                            )}
                            {Boolean(footer) && footer}
                        </div>
                    </Transition.Child>
                </div>
            </HUDialog>
        </Transition.Root>
    );
};

export default Dialog;
