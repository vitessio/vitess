import { Fragment, useRef } from 'react'
import { Dialog as HUDialog, Transition } from '@headlessui/react'
import { Icon, Icons } from '../Icon'
import { Button } from '../Button'

interface DialogProps {
  icon?: Icons
  isOpen: boolean
  title?: string
  description?: string
  content?: React.ReactElement
  onCancel?: () => void
  onConfirm?: () => void
  onClose?: () => void
  confirmText?: string
  cancelText?: string
  footer?: React.ReactElement
  children?: React.ReactElement
}

/**
 * Dialog is a controlled component, and its open state is controlled by the isOpen property.
 * User should pass in a function to set isOpen to the onClose property.
 */
const Dialog: React.FC<DialogProps> = ({ icon, title, description, children, isOpen, confirmText, cancelText, footer, onCancel, onConfirm, onClose }) => {

  const cancelButtonRef = useRef(null)

  return (
    <Transition.Root show={isOpen} as={Fragment}>
      <HUDialog as="div" className="fixed z-10 inset-0 overflow-y-auto" initialFocus={cancelButtonRef} onClose={(_) => { onClose && onClose() }}>
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
            <div className="inline-block align-bottom bg-white rounded-lg text-left overflow-hidden shadow-xl transform transition-all sm:my-8 sm:align-middle sm:max-w-lg sm:w-full">
              <div className="bg-white px-4 pt-5 pb-4 sm:p-6 sm:pb-4">
                <div className="sm:flex sm:items-start">
                  {icon && (
                    <div className="mx-auto flex-shrink-0 flex items-center justify-center h-12 w-12 rounded-full bg-red-100 sm:mx-0 sm:h-10 sm:w-10">
                      <Icon icon={icon} />
                    </div>
                  )
                  }
                  <div className="mt-3 text-center sm:mt-0 sm:ml-4 sm:text-left">
                    {title && (
                      <HUDialog.Title as="h2" className="text-xl leading-6 font-medium text-primary">
                        {title}
                      </HUDialog.Title>
                    )}
                    {description && (
                      <div className="mt-2">
                        <p className="text-sm text-secondary">
                          {description}
                        </p>
                      </div>)
                    }
                    {Boolean(children) && children}
                  </div>
                </div>
              </div>
              {!footer && (
                <div className="px-4 py-3 sm:px-6 sm:flex sm:flex-row-reverse">
                  <Button
                    type="button"
                    className='px-4 py-2 text-base font-medium ml-2'
                    onClick={() => {
                      onConfirm && onConfirm()
                      onClose && onClose()
                    }}
                  >
                    {confirmText || 'Confirm'}
                  </Button>
                  <Button
                    type="button"
                    className='px-4 py-2 text-base font-medium'
                    onClick={() => {
                      onCancel && onCancel()
                      onClose && onClose()
                    }}
                    secondary={true}
                    ref={cancelButtonRef}
                  >
                    {cancelText || 'Cancel'}
                  </Button>
                </div>
              )}
              {Boolean(footer) && footer}
            </div>
          </Transition.Child>
        </div>
      </HUDialog>
    </Transition.Root>
  )
}

export default Dialog