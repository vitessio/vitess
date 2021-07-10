import * as React from 'react';
import cx from 'classnames';

import { Icon, Icons } from './Icon';

import style from './TextInput.module.scss';

type NativeInputProps = Omit<
    React.DetailedHTMLProps<React.InputHTMLAttributes<HTMLInputElement>, HTMLInputElement>,
    // Omit the props that we explicitly want to overwrite
    'size'
>;
interface Props extends NativeInputProps {
    // className is applied to the <input>, not the parent container.
    className?: string;
    iconLeft?: Icons;
    iconRight?: Icons;
    size?: 'large';
}

export const TextInput = ({ className, iconLeft, iconRight, size, ...props }: Props) => {
    const inputClass = cx(style.input, {
        [style.large]: size === 'large',
        [style.withIconLeft]: !!iconLeft,
        [style.withIconRight]: !!iconRight,
    });

    // Order of elements matters: the <input> comes before the icons so that
    // we can use CSS adjacency selectors like `input:focus ~ .icon`.
    return (
        <div className={style.inputContainer}>
            <input {...props} className={inputClass} type="text" />
            {iconLeft && <Icon className={style.iconLeft} icon={iconLeft} />}
            {iconRight && <Icon className={style.iconRight} icon={iconRight} />}
        </div>
    );
};
