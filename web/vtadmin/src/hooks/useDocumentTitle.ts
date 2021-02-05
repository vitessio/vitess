import { useEffect } from 'react';

// useDocumentTitle is a simple hook to set the document.title of the page.
//
// Note that there's a noticeable delay, around ~500ms, between
// when the parent component renders and when the document.title
// is updated. It's not terrible... but it is a little bit annoying.
//
// Unfortunately, neither React hooks nor react-router offer
// a mechanism to hook into the "componentWillMount" stage before the
// first render. This problem seems to be common even in libraries
// like react-helmet; see https://github.com/nfl/react-helmet/issues/189.
//
// Other approaches that still, unfortunately, exhibit the lag:
//
//  - Setting document.title directly in component functions
//  - Setting document.title in a Route component's render prop
//  - Setting document.title on history.listen events
//
export const useDocumentTitle = (title: string) => {
    // Update document.title whenever the `title` argument changes.
    useEffect(() => {
        document.title = `${title} | VTAdmin`;
    }, [title]);

    // Restore the default document title on unmount.
    // (While one may think this might be the source of the aforementioned delay
    // (and that idea wouldn't be far off since this indeed double-updates the title)
    // that is not the case as the lag happens even without this.)
    useEffect(() => {
        return () => {
            document.title = 'VTAdmin';
        };
    }, []);
};
