import React from 'react'
import { Label } from './Label'

interface Props {
  checked: boolean
  label: string
  description?: string
  className?: string
  onChange: () => void
}

const Checkbox: React.FC<Props> = ({ label, checked, description, className, onChange }) => {
  return (
    <div className={className}>
      <div className="flex items-center">
        <input onChange={onChange} checked={checked} className="form-check-input appearance-none h-6 w-6 border border-gray-300 rounded-sm bg-white checked:bg-blue-600 checked:border-blue-600 focus:outline-none transition duration-200 align-top bg-no-repeat bg-center bg-contain float-left mr-2 cursor-pointer" type="checkbox" value="" id={label} />
        <Label label={label} htmlFor={label} />
      </div>
      <div>
        {description}
      </div>
    </div>
  )
}

export default Checkbox