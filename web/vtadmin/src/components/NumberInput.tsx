import * as React from 'react';
import cx from 'classnames';

import style from './TextInput.module.scss';

type NativeInputProps = Omit<
    React.DetailedHTMLProps<React.InputHTMLAttributes<HTMLInputElement>, HTMLInputElement>,
    // Omit the props that we explicitly want to overwrite
    'size'
>;
interface Props extends NativeInputProps {
    // className is applied to the <input>, not the parent container.
    className?: string;
    size?: string;
}

export const NumberInput = ({ className, size, ...props }: Props) => {
    const inputClass = cx(style.input, className, {
        [style.large]: size === 'large',
    });
    // Order of elements matters: the <input> comes before the icons so that
    // we can use CSS adjacency selectors like `input:focus ~ .icon`.
    return (
        <div className={`${className} ${style.inputContainer}`}>
            <input type="number" className={`form-control bg-clip-padding ${inputClass} !pr-0`} {...props} />
        </div>
    );
};
