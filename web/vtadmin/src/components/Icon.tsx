import * as icons from '../icons';

interface Props {
    className?: string;
    icon: Icons;
}

// All icons are from the VTAdmin Figma icon library:
// https://www.figma.com/file/By3SoETBRHpOirv3Ctfxdq/Designs
export const Icon = ({ icon, ...props }: Props) => {
    const componentName = icon.charAt(0).toUpperCase() + icon.slice(1);

    const IconComponent = (icons as any)[componentName];
    if (!IconComponent) {
        console.warn(`Invalid icon: ${icon}`);
        return null;
    }

    return <IconComponent {...props} />;
};

export enum Icons {
    add = 'add',
    alertFail = 'alertFail',
    archive = 'archive',
    archiveAuto = 'archiveAuto',
    archiveForever = 'archiveForever',
    archiveRestore = 'archiveRestore',
    arrowDown = 'arrowDown',
    arrowLeft = 'arrowLeft',
    arrowRight = 'arrowRight',
    arrowUp = 'arrowUp',
    avatar = 'avatar',
    boxChecked = 'boxChecked',
    boxEmpty = 'boxEmpty',
    boxIndeterminate = 'boxIndeterminate',
    bug = 'bug',
    chart = 'chart',
    checkSuccess = 'checkSuccess',
    chevronDown = 'chevronDown',
    chevronLeft = 'chevronLeft',
    chevronRight = 'chevronRight',
    chevronUp = 'chevronUp',
    circleAdd = 'circleAdd',
    circleDelete = 'circleDelete',
    circleRemove = 'circleRemove',
    circleWorkflow = 'circleWorkflow',
    clear = 'clear',
    code = 'code',
    copy = 'copy',
    delete = 'delete',
    document = 'document',
    download = 'download',
    dropDown = 'dropDown',
    dropUp = 'dropUp',
    ellipsis = 'ellipsis',
    gear = 'gear',
    history = 'history',
    info = 'info',
    keyG = 'keyG',
    keyK = 'keyK',
    keyR = 'keyR',
    keyS = 'keyS',
    keyT = 'keyT',
    keyboard = 'keyboard',
    link = 'link',
    pageFirst = 'pageFirst',
    pageLast = 'pageLast',
    pause = 'pause',
    question = 'question',
    radioEmpty = 'radioEmpty',
    radioSelected = 'radioSelected',
    refresh = 'refresh',
    remove = 'remove',
    retry = 'retry',
    runQuery = 'runQuery',
    search = 'search',
    sort = 'sort',
    spinnerLoading = 'spinnerLoading',
    start = 'start',
    wrench = 'wrench',
}
