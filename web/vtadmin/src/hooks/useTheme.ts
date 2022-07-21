import { useState } from 'react';

export enum Theme {
    dark = 'dark',
    light = 'light',
}

const DEFAULT_THEME: Theme = Theme.light;
const HTML_THEME_ATTR = 'data-vtadmin-theme';

// useTheme is a hook for using and setting the display theme.
// This particular implementation is super quick + dirty for the purposes
// of the Debug page. The :) correct implementation :) will be more robust,
// with persistence, system defaults, and night shift mode.
export const useTheme = (): [Theme, (t: Theme) => void] => {
    const currentTheme = document.documentElement.getAttribute(HTML_THEME_ATTR);
    const tidyTheme = currentTheme && currentTheme in Theme ? (currentTheme as Theme) : DEFAULT_THEME;
    const [theme, setTheme] = useState<Theme>(tidyTheme);

    const _setTheme = (nextTheme: Theme): void => {
        document.documentElement.setAttribute(HTML_THEME_ATTR, nextTheme);
        setTheme(nextTheme);
    };

    return [theme, _setTheme];
};
