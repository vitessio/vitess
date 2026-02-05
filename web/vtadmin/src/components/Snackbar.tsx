import React from 'react';
import { Icon, Icons } from './Icon';
import { Intent } from './intent';
import { ToastContainer, toast, Slide, ToastOptions } from 'react-toastify';
import 'react-toastify/dist/ReactToastify.css';

interface SnackbarProps {
    message: string;
    icon?: Icons;
    intent?: Intent;
    closeToast?: () => void;
}

// Needed to choose lighter background color
const translateIntentColor = (intent: Intent) => {
    switch (intent) {
        case Intent.danger:
            return 'red-50';
        case Intent.warning:
            return 'yellow-50';
        case Intent.success:
            return 'green-50';
        case Intent.none:
            return 'gray-50';
    }
};

const Snackbar: React.FC<SnackbarProps> = ({ closeToast, message, icon, intent = Intent.none, ...props }) => {
    const intentColor = intent === Intent.none ? 'gray-900' : `${intent}`;

    return (
        <div
            className={`flex font-medium text-sm text-${intentColor} border border-${intentColor} items-center bg-gray-100 justify-between py-6 px-8 z-20 rounded-xl bg-${translateIntentColor(
                intent
            )}`}
        >
            <div className="flex items-center">
                {icon && (
                    <div className="shrink-0">
                        <Icon icon={icon} className={`shrink-0 mr-4 fill-current h-8 w-8 min-h-8 min-w-8`} />
                    </div>
                )}
                <div className="grow-0 whitespace-normal">{message}</div>
            </div>
            <button onClick={closeToast}>
                <Icon icon={Icons.delete} className="fill-current text-gray-900 h-6 w-6 ml-8" />
            </button>
        </div>
    );
};

interface SnackbarContextProps {
    warn: (message: string) => void;
    danger: (message: string) => void;
    success: (message: string) => void;
    info: (message: string) => void;
}

const defaultProps: SnackbarContextProps = {
    warn: (message: string) => message,
    danger: (message: string) => message,
    success: (message: string) => message,
    info: (message: string) => message,
};

interface AddSnackbarParams {
    message: string;
    icon?: Icons;
    intent?: Intent;
}

export const SnackbarContext = React.createContext<SnackbarContextProps>(defaultProps);
const addSnackbar = (props: AddSnackbarParams, options?: ToastOptions) => {
    toast(({ closeToast }) => <Snackbar closeToast={closeToast} key={props.message} {...props} />, {
        className: 'mb-2 rounded-2xl',
        ...options,
    });
};

export const warn = (message: string, options?: ToastOptions) =>
    addSnackbar({ message, intent: Intent.warning, icon: Icons.alertFail }, options);
export const danger = (message: string, options?: ToastOptions) =>
    addSnackbar({ message, intent: Intent.danger, icon: Icons.alertFail }, options);
export const success = (message: string, options?: ToastOptions) =>
    addSnackbar({ message, intent: Intent.success, icon: Icons.checkSuccess }, options);
export const info = (message: string, options?: ToastOptions) =>
    addSnackbar({ message, intent: Intent.none, icon: Icons.info }, options);

export const SnackbarContainer: React.FC = ({ children }) => {
    return (
        <div className="fixed right-10 bottom-6" id="snackbar-container">
            <ToastContainer
                toastClassName="mb-2"
                autoClose={false}
                position="bottom-right"
                closeButton={false}
                closeOnClick
                hideProgressBar
                pauseOnHover
                transition={Slide}
            />
        </div>
    );
};

export default Snackbar;
