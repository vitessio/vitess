import { Switch } from '@headlessui/react';

interface ToggleParams {
    enabled: boolean;
    className?: string;
    onChange: () => void;
}

const Toggle: React.FC<ToggleParams> = ({ enabled, className, onChange }) => {
    return (
        <div className={className}>
            <Switch checked={enabled} onChange={onChange} className={enabled ? 'toggle on' : 'toggle off'}>
                <span className="sr-only">Use setting</span>
                <span aria-hidden="true" className={enabled ? 'toggle-button on' : 'toggle-button off'} />
            </Switch>
        </div>
    );
};

export default Toggle;
