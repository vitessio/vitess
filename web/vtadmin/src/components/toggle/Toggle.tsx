import { Switch } from '@headlessui/react';

interface ToggleParams {
    enabled: boolean;
    onChange: () => void;
}

const Toggle: React.FC<ToggleParams> = ({ enabled, onChange }) => {
    return (
        <div>
            <Switch checked={enabled} onChange={onChange} className={enabled ? 'toggle on' : 'toggle off'}>
                <span className="sr-only">Use setting</span>
                <span aria-hidden="true" className={enabled ? 'toggle-button on' : 'toggle-button off'} />
            </Switch>
        </div>
    );
};

export default Toggle;
