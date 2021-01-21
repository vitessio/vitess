import * as React from 'react';

export const Debug = () => {
    return (
        <div>
            <h1>Debugging ✨🦋🐛🐝🐞🐜🕷🕸🦂🦗🦟✨</h1>

            <h2>Environment variables</h2>
            <pre>{JSON.stringify(process.env, null, 2)}</pre>
        </div>
    );
};
