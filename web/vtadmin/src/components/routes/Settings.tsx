import * as React from 'react';
import { useDocumentTitle } from '../../hooks/useDocumentTitle';
import { Theme, useTheme } from '../../hooks/useTheme';
import { Icon, Icons } from '../Icon';
import { Select } from '../inputs/Select';
import { Intent } from '../intent';
import { ContentContainer } from '../layout/ContentContainer';
import { warn, danger, success, info } from '../Snackbar';
import { Tab } from '../tabs/Tab';
import { TabContainer } from '../tabs/TabContainer';
import { TextInput } from '../TextInput';
import Toggle from '../toggle/Toggle';
import { Tooltip } from '../tooltip/Tooltip';
import { env } from '../../util/env';
import style from './Settings.module.scss';

/* eslint-disable jsx-a11y/anchor-is-valid */
export const Settings = () => {
    useDocumentTitle('Debug');
    const [theme, setTheme] = useTheme();
    const [formData, setFormData] = React.useState<{ [key: string]: any }>({});
    const [enabled, setEnabled] = React.useState(false);

    return (
        <ContentContainer>
            <div className={style.container}>
                <h1 className="mt-8">Settings</h1>

                <h2 className="mt-12 mb-8">Environment variables</h2>
                <pre>{JSON.stringify(env(), null, 2)}</pre>

                {import.meta.env.NODE_ENV !== 'production' && (
                    <>
                        <h2 className="mt-12 mb-8">Style Guide</h2>

                        <section>
                            <h3 className="mt-12 mb-8">Theme</h3>
                            <div>
                                {Object.values(Theme).map((t) => (
                                    <div key={t}>
                                        <label>
                                            <input
                                                checked={theme === t}
                                                name="theme"
                                                onChange={() => setTheme(t)}
                                                type="radio"
                                                value={t}
                                            />
                                            {t}
                                        </label>
                                    </div>
                                ))}
                            </div>
                        </section>

                        <section>
                            <h3 className="mt-12 mb-8">Colours</h3>
                            {[
                                [style.danger, style.danger50, style.danger200],
                                [style.success, style.success50, style.success200],
                                [style.warning, style.warning50, style.warning200],
                                [style.vtblue, style.vtblue50, style.vtblue200],
                                [style.vtblueDark, style.vtblueDark50, style.vtblueDark200],
                                [
                                    style.gray75,
                                    style.gray100,
                                    style.gray200,
                                    style.gray400,
                                    style.gray600,
                                    style.gray800,
                                    style.gray900,
                                ],
                            ].map((colors, idx) => {
                                return (
                                    <div className="flex my-8" key={idx}>
                                        {colors.map((c) => (
                                            <div className={`${c} mr-4 font-semibold text-sm`} key={c}>
                                                <div className="w-40 h-16 rounded" />
                                            </div>
                                        ))}
                                    </div>
                                );
                            })}
                        </section>

                        <section>
                            <h3 className="mt-12 mb-8">Typography</h3>

                            <div className="my-16">
                                <p className="text-sm">The quick brown fox ...</p>
                                <p className="text-base">The quick brown fox ...</p>
                                <p className="text-lg">The quick brown fox ...</p>
                                <p className="text-xl">The quick brown fox ...</p>
                                <p className="text-2xl">The quick brown fox ...</p>
                                <p className="text-3xl">The quick brown fox ...</p>
                            </div>

                            <div className="my-16">
                                <p className="text-sm font-mono">The quick brown fox ...</p>
                                <p className="text-base font-mono">The quick brown fox ...</p>
                                <p className="text-lg font-mono">The quick brown fox ...</p>
                                <p className="text-xl font-mono">The quick brown fox ...</p>
                                <p className="text-2xl font-mono">The quick brown fox ...</p>
                                <p className="text-3xl font-mono">The quick brown fox ...</p>
                            </div>

                            <div className="max-w-prose my-16">
                                <p>
                                    Lorem ipsum dolor sit amet, consectetur adipiscing elit. Morbi in facilisis libero,
                                    eget congue lorem. Nulla non ligula eget erat aliquam lacinia a eget felis. Nulla
                                    turpis sapien, ultricies sit amet suscipit quis, auctor id risus. Donec pellentesque
                                    tellus metus, in eleifend lacus euismod eget. Vestibulum vehicula ut metus id
                                    vestibulum. Nam vulputate sapien sit amet tempor efficitur. Donec dictum tellus nec
                                    leo fringilla, eu tempor neque posuere. Integer porta, velit quis interdum
                                    ultricies, quam enim suscipit eros, quis ornare metus ante vitae est. Sed quis
                                    dignissim justo, at ultrices urna. Suspendisse gravida, tortor sed semper tristique,
                                    erat metus vulputate augue, quis tempor mi nisi vitae magna. Donec auctor fermentum
                                    magna, ut feugiat odio tincidunt sit amet. Donec accumsan lorem mi, ut placerat ex
                                    lacinia vel. Nullam ut magna feugiat, ornare nibh vel, tincidunt nisi. Fusce
                                    tincidunt malesuada posuere.
                                </p>

                                <p>
                                    Cras elementum lacinia tristique. Vestibulum nec sem sit amet velit lobortis
                                    accumsan ut eget lacus. Nulla eros ipsum, pellentesque sed vehicula id, pulvinar ut
                                    dolor. Proin facilisis ligula vel faucibus iaculis. Donec venenatis massa lorem, sed
                                    tempus libero vulputate nec. Vestibulum semper nibh id tortor dapibus, a
                                    pellentesque libero porttitor. Pellentesque id elit nulla. Duis congue hendrerit
                                    rhoncus. Vivamus accumsan tincidunt augue in sollicitudin. Cras sed augue eget elit
                                    semper interdum tristique at ligula. Aliquam vel pretium sem. Donec egestas nisi
                                    blandit congue pretium.
                                </p>

                                <p>
                                    Duis vulputate blandit ante nec bibendum. Duis ipsum augue, tempus et viverra et,
                                    ultricies ac sapien. Nullam vel laoreet turpis, in convallis eros. Suspendisse sit
                                    amet magna turpis. Curabitur mi risus, facilisis vel fringilla ut, facilisis nec
                                    tortor. Mauris rutrum vehicula justo ac dapibus. Sed aliquam, eros non bibendum
                                    sodales, mauris est iaculis dui, ut tristique nibh sapien ac sapien. Lorem ipsum
                                    dolor sit amet, consectetur adipiscing elit.
                                </p>
                            </div>
                        </section>

                        <section>
                            <h3 className="mt-12 mb-8">Tabs</h3>

                            <TabContainer className={style.tabContainer} size="small">
                                <Tab text="Small Tab" to="/debug/small/1" />
                                <Tab text="Small Tab" to="/debug/small/2" count={1} />
                                <Tab text="Small Tab" to="/debug/small/3" status="danger" />
                            </TabContainer>

                            <TabContainer className={style.tabContainer} size="medium">
                                <Tab text="Medium Tab" to="/debug/medium/1" />
                                <Tab text="Medium Tab" to="/debug/medium/2" status="success" count={1000} />
                                <Tab text="Medium Tab" to="/debug/medium/3" />
                            </TabContainer>

                            <TabContainer className={style.tabContainer} size="large">
                                <Tab text="Large Tab" to="/debug/large/1" />
                                <Tab text="Large Tab" to="/debug/large/2" status="primary" />
                                <Tab text="Large Tab" to="/debug/large/3" count={10000} />
                            </TabContainer>
                        </section>

                        <section>
                            <h3 className="mt-12 mb-8">Icons</h3>
                            <div className={style.iconContainer}>
                                {Object.values(Icons).map((i) => (
                                    <Tooltip text={i}>
                                        <Icon className={style.icon} icon={i} key={i} tabIndex={0} />
                                    </Tooltip>
                                ))}
                            </div>
                        </section>
                        <section>
                            <h3 className="mt-12 mb-8">Toggle</h3>
                            <Toggle enabled={enabled} onChange={() => setEnabled(!enabled)} />
                        </section>
                        <section>
                            <h3 className="mt-12 mb-8">Select</h3>
                            <div className={style.dropdownContainer}>
                                <div className={style.dropdownRow}>
                                    <Select
                                        itemToString={(fruit) => fruit?.name || ''}
                                        items={FRUITS}
                                        label="Fruits"
                                        onChange={(fruit) => setFormData({ ...formData, selectFruitDefault: fruit })}
                                        placeholder="Choose a fruit"
                                        renderItem={(fruit) => `${fruit.emoji} ${fruit.name}`}
                                        selectedItem={formData.selectFruitDefault || null}
                                    />
                                    <Select
                                        items={FRUIT_NAMES}
                                        label="Fruit names"
                                        onChange={(fruitName) =>
                                            setFormData({ ...formData, selectFruitNameDefault: fruitName })
                                        }
                                        placeholder="Choose a fruit name"
                                        selectedItem={formData.selectFruitNameDefault || null}
                                    />
                                    <Select
                                        disabled
                                        itemToString={(fruit) => fruit?.name || ''}
                                        items={FRUITS}
                                        label="Fruits"
                                        onChange={(fruit) => setFormData({ ...formData, selectFruitDefault: fruit })}
                                        placeholder="Choose a fruit"
                                        renderItem={(fruit) => `${fruit.emoji} ${fruit.name}`}
                                        selectedItem={formData.selectFruitDefault || null}
                                    />
                                    <Select
                                        items={[]}
                                        emptyPlaceholder="No fruits :("
                                        label="Empty Fruits"
                                        onChange={(fruit) => setFormData({ ...formData, selectFruitDefault: fruit })}
                                        placeholder="Choose a fruit"
                                        renderItem={(fruit) => `${fruit.emoji} ${fruit.name}`}
                                        selectedItem={formData.selectFruitDefault || null}
                                    />
                                </div>
                                <div className={style.dropdownRow}>
                                    <Select
                                        itemToString={(fruit) => fruit?.name || ''}
                                        items={FRUITS}
                                        label="Fruits"
                                        onChange={(fruit) => setFormData({ ...formData, selectFruitLarge: fruit })}
                                        placeholder="Choose a fruit"
                                        renderItem={(fruit) => `${fruit.emoji} ${fruit.name}`}
                                        selectedItem={formData.selectFruitLarge || null}
                                        size="large"
                                    />
                                    <Select
                                        items={FRUIT_NAMES}
                                        label="Fruit names"
                                        onChange={(fruitName) =>
                                            setFormData({ ...formData, selectFruitNameLarge: fruitName })
                                        }
                                        placeholder="Choose a fruit name"
                                        size="large"
                                        selectedItem={formData.selectFruitNameLarge || null}
                                    />
                                    <Select
                                        disabled
                                        itemToString={(fruit) => fruit?.name || ''}
                                        items={FRUITS}
                                        label="Fruits"
                                        onChange={(fruit) => setFormData({ ...formData, selectFruitLarge: fruit })}
                                        placeholder="Choose a fruit"
                                        renderItem={(fruit) => `${fruit.emoji} ${fruit.name}`}
                                        selectedItem={formData.selectFruitLarge || null}
                                        size="large"
                                    />
                                </div>
                            </div>
                        </section>

                        <section>
                            <h3 className="mt-12 mb-8">Text Inputs</h3>
                            <div className={style.inputContainer}>
                                <TextInput placeholder="Basic text input" />
                                <TextInput iconLeft={Icons.search} placeholder="With leftIcon" />
                                <TextInput iconRight={Icons.delete} placeholder="With rightIcon" />
                                <TextInput
                                    iconLeft={Icons.search}
                                    iconRight={Icons.delete}
                                    placeholder="With leftIcon and rightIcon"
                                />
                                <TextInput disabled placeholder="Disabled" />
                                <TextInput
                                    disabled
                                    iconLeft={Icons.search}
                                    iconRight={Icons.delete}
                                    placeholder="Disabled with icons"
                                />
                                <div className={style.inputRow}>
                                    <TextInput
                                        iconLeft={Icons.search}
                                        iconRight={Icons.delete}
                                        size="large"
                                        placeholder="Button-adjacent"
                                    />
                                    <button className="btn btn-lg" type="button">
                                        Primary
                                    </button>
                                    <button className="btn btn-lg btn-secondary" type="button">
                                        Secondary
                                    </button>
                                </div>
                                <div className={style.inputRow}>
                                    <TextInput
                                        iconLeft={Icons.search}
                                        iconRight={Icons.delete}
                                        placeholder="Button-adjacent"
                                    />
                                    <button className="btn" type="button">
                                        Primary
                                    </button>
                                    <button className="btn btn-secondary" type="button">
                                        Secondary
                                    </button>
                                </div>
                            </div>
                        </section>

                        <section>
                            <h3 className="mt-12 mb-8">Buttons</h3>

                            {['btn-lg', '', 'btn-sm'].map((s, idx) => {
                                return (
                                    <div className="my-16">
                                        {['', 'btn-danger', 'btn-warning', 'btn-success'].map((v) => {
                                            return (
                                                <div className="flex gap-4 my-6" key={`${idx}-${v}`}>
                                                    <button className={`btn ${s} ${v}`}>Button</button>
                                                    <a className={`btn ${s} ${v}`} href="#">
                                                        Link
                                                    </a>

                                                    <button className={`btn ${s} ${v} btn-secondary`}>Button</button>
                                                    <a className={`btn ${s} ${v} btn-secondary`} href="#">
                                                        Link
                                                    </a>

                                                    <button className={`btn ${s} ${v} btn-secondary`}>
                                                        <Icon icon={Icons.circleAdd} />
                                                        Button
                                                    </button>
                                                    <a className={`btn ${s} ${v} btn-secondary`} href="#">
                                                        <Icon icon={Icons.circleAdd} />
                                                        Link
                                                    </a>

                                                    <button className={`btn ${s} ${v}`} disabled>
                                                        Button
                                                    </button>
                                                    <button className={`btn ${s} ${v} btn-secondary`} disabled>
                                                        <Icon icon={Icons.circleAdd} />
                                                        Button
                                                    </button>
                                                </div>
                                            );
                                        })}
                                    </div>
                                );
                            })}
                        </section>
                        <section>
                            <Snackbars />
                        </section>
                    </>
                )}
            </div>
        </ContentContainer>
    );
};

const Snackbars: React.FC = () => {
    const intents = Object.keys(Intent);
    return (
        <div>
            <h3 className="mt-12 mb-8">Snackbars</h3>

            {intents.map((i) => {
                const onClick = () => {
                    switch (i) {
                        case 'danger':
                            danger('This is a danger snackbar.');
                            break;
                        case 'success':
                            success('This is a success snackbar.');
                            break;
                        case 'none':
                            info('This is an info snackbar.');
                            break;
                        case 'warning':
                            warn('This is a warn snackbar with a very very very very very very very long message.');
                            break;
                    }
                };
                return (
                    <button onClick={onClick} className={`btn btn-secondary mr-2`} key={i}>
                        {i}
                    </button>
                );
            })}
        </div>
    );
};

const FRUITS: { emoji: string; name: string; id: string }[] = [
    { emoji: '🍎', name: 'apple (red)', id: 'red_apple' },
    { emoji: '🍏', name: 'apple (green)', id: 'green_apple' },
    { emoji: '🍌', name: 'banana', id: 'banana' },
    { emoji: '🍒', name: 'cherry', id: 'cherry' },
    { emoji: '🥥', name: 'coconut', id: 'coconut' },
    { emoji: '🍇', name: 'grape', id: 'grape' },
    { emoji: '🥝', name: 'kiwi', id: 'kiwi' },
    { emoji: '🍋', name: 'lemon', id: 'lemon' },
    { emoji: '🍈', name: 'melon', id: 'melon' },
    { emoji: '🍊', name: 'orange', id: 'orange' },
    { emoji: '🍑', name: 'peach', id: 'peach' },
    { emoji: '🍐', name: 'pear', id: 'pear' },
    { emoji: '🍍', name: 'pineapple', id: 'pineapple' },
    { emoji: '🍓', name: 'strawberry', id: 'strawberry' },
    { emoji: '🍉', name: 'watermelon', id: 'watermelon' },
];

const FRUIT_NAMES = FRUITS.map((f) => f.name).slice(0, 5);
