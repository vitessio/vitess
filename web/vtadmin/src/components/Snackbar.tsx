import { Transition } from '@headlessui/react'
import React, { useContext, useEffect, useState } from 'react'
import { Icon, Icons } from './Icon'
import { Intent } from './intent'
import { v4 } from 'uuid'

interface SnackbarProps {
    message: string
    icon?: Icons
    intent?: Intent
    id: string
}

// Needed to choose lighter background color
const translateIntentColor = (intent: Intent) => {
    switch (intent) {
        case Intent.danger:
            return 'red-50'
        case Intent.warning:
            return 'yellow-50'
        case Intent.success:
            return 'green-50'
        case Intent.none:
            return 'gray-50'
    }
}

const Snackbar: React.FC<SnackbarProps & { onClose: () => void }> = ({ onClose, message, icon, intent = Intent.none }) => {
    const intentColor = intent === Intent.none ? 'gray' : `${intent}`
    const [show, setShow] = useState(false)

    useEffect(() => {
        const timeout = setTimeout(() => setShow(true), 300)
        return () => clearTimeout(timeout)
    }, [])
    return (
        <Transition
            show={show}
            as='div'
            className={`bg-white border border-${intentColor} mb-4 shadow rounded-xl`}
            enter="transition-opacity transition-transform duration-200"
            enterFrom="opacity-0 scale-75 translate-y-6"
            enterTo="opacity-100 scale-100 translate-y-0"
            leave="transition-opacity transition-transform duration-75"
            leaveFrom="opacity-100 translate-y-0"
            leaveTo="opacity-0 -translate-y-6"
        >
            <div className={`flex font-medium text-sm text-${intentColor} items-center bg-gray-100 justify-between py-6 px-8 z-10 rounded-xl bg-${translateIntentColor(intent)}`}>
                <div className='flex items-center'>
                    {icon && <Icon icon={icon} className={`mr-4 fill-current h-8 w-8`} />}
                    {message}
                </div>
                <button onClick={onClose}><Icon icon={Icons.delete} className='fill-current text-gray-900 h-6 w-6 ml-8' /></button>
            </div >
        </Transition >
    )
}

interface SnackbarContextProps {
    warn: (message: string) => void
    danger: (message: string) => void
    success: (message: string) => void
    info: (message: string) => void
}

const defaultProps: SnackbarContextProps = {
    warn: (message: string) => message,
    danger: (message: string) => message,
    success: (message: string) => message,
    info: (message: string) => message
}

interface AddSnackbarParams {
    message: string
    icon?: Icons
    intent?: Intent
}

export const SnackbarContext = React.createContext<SnackbarContextProps>(defaultProps)
export const useSnackbar = () => useContext(SnackbarContext)

export const SnackbarProvider: React.FC = ({ children }) => {
    const [snackbars, setSnackbars] = useState<(SnackbarProps & { id: string })[]>([])
    const [timeouts, setTimeouts] = useState<NodeJS.Timeout[]>([])

    const removeSnackbar = (id: string) => setSnackbars(snackbars.filter(s => s.id !== id))

    const addSnackbar = (props: AddSnackbarParams) => {
        const id = v4()
        setSnackbars([...snackbars, { ...props, id }])
        const timeout = setTimeout(() => removeSnackbar(id), 3000)
        setTimeouts([...timeouts, timeout])
    }


    // eslint-disable-next-line react-hooks/exhaustive-deps
    useEffect(() => { return () => timeouts.forEach(t => clearTimeout(t)) }, [])

    const warn = (message: string) => addSnackbar({ message, intent: Intent.warning, icon: Icons.alertFail })
    const danger = (message: string) => addSnackbar({ message, intent: Intent.danger, icon: Icons.alertFail })
    const success = (message: string) => addSnackbar({ message, intent: Intent.success, icon: Icons.checkSuccess })
    const info = (message: string) => addSnackbar({ message, intent: Intent.none, icon: Icons.info })

    return (
        <SnackbarContext.Provider value={{ warn, danger, success, info }}>
            <div className='fixed flex flex-col right-10 bottom-6' id='snackbar-container'>
                {snackbars.map((props, i) => <Snackbar key={`${props.message}_${i}`} {...props} onClose={() => removeSnackbar(props.id)} />)}
            </div>
            {children}
        </SnackbarContext.Provider>
    )
}

export default Snackbar