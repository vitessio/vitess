import * as React from 'react';
import { useDocumentTitle } from '../../hooks/useDocumentTitle';
import { Theme, useTheme } from '../../hooks/useTheme';
import { Button } from '../Button';
import { Icon, Icons } from '../Icon';
import { Select } from '../inputs/Select';
import { TextInput } from '../TextInput';
import style from './Debug.module.scss';

export const Debug = () => {
    useDocumentTitle('Debug');
    const [theme, setTheme] = useTheme();
    const [formData, setFormData] = React.useState<{ [key: string]: any }>({});

    return (
        <div className={style.container}>
            <h1>Debugging ✨🦋🐛🐝🐞🐜🕷🕸🦂🦗🦟✨</h1>

            <h2>Environment variables</h2>
            <pre>{JSON.stringify(process.env, null, 2)}</pre>

            <h2>Style Guide</h2>

            <section>
                <h3>Theme</h3>
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
                <h3>Icons</h3>
                <div className={style.iconContainer}>
                    {Object.values(Icons).map((i) => (
                        <Icon className={style.icon} icon={i} key={i} />
                    ))}
                </div>
            </section>

            <section>
                <h3>Select</h3>
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
                            onChange={(fruitName) => setFormData({ ...formData, selectFruitNameDefault: fruitName })}
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
                            onChange={(fruitName) => setFormData({ ...formData, selectFruitNameLarge: fruitName })}
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
                <h3>Text Inputs</h3>
                <div className={style.inputContainer}>
                    <TextInput autoFocus placeholder="Basic text input" />
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
                        <Button size="large">Primary</Button>
                        <Button secondary size="large">
                            Secondary
                        </Button>
                    </div>
                    <div className={style.inputRow}>
                        <TextInput iconLeft={Icons.search} iconRight={Icons.delete} placeholder="Button-adjacent" />
                        <Button>Primary</Button>
                        <Button secondary>Secondary</Button>
                    </div>
                </div>
            </section>

            <section>
                <h3>Buttons</h3>
                <div className={style.buttonContainer}>
                    {/* Large */}
                    <Button size="large">Button</Button>
                    <Button secondary size="large">
                        Button
                    </Button>
                    <Button icon={Icons.circleAdd} size="large">
                        Button
                    </Button>
                    <Button icon={Icons.circleAdd} secondary size="large">
                        Button
                    </Button>
                    <Button disabled size="large">
                        Button
                    </Button>
                    <Button disabled secondary size="large">
                        Button
                    </Button>
                    <Button disabled icon={Icons.circleAdd} size="large">
                        Button
                    </Button>
                    <Button disabled icon={Icons.circleAdd} secondary size="large">
                        Button
                    </Button>

                    {/* Medium */}
                    <Button size="medium">Button</Button>
                    <Button secondary size="medium">
                        Button
                    </Button>
                    <Button icon={Icons.circleAdd} size="medium">
                        Button
                    </Button>
                    <Button icon={Icons.circleAdd} secondary size="medium">
                        Button
                    </Button>
                    <Button disabled size="medium">
                        Button
                    </Button>
                    <Button disabled secondary size="medium">
                        Button
                    </Button>
                    <Button disabled icon={Icons.circleAdd} size="medium">
                        Button
                    </Button>
                    <Button disabled icon={Icons.circleAdd} secondary size="medium">
                        Button
                    </Button>

                    {/* Small */}
                    <Button size="small">Button</Button>
                    <Button secondary size="small">
                        Button
                    </Button>
                    <Button icon={Icons.circleAdd} size="small">
                        Button
                    </Button>
                    <Button icon={Icons.circleAdd} secondary size="small">
                        Button
                    </Button>
                    <Button disabled size="small">
                        Button
                    </Button>
                    <Button disabled secondary size="small">
                        Button
                    </Button>
                    <Button disabled icon={Icons.circleAdd} size="small">
                        Button
                    </Button>
                    <Button disabled icon={Icons.circleAdd} secondary size="small">
                        Button
                    </Button>
                </div>
            </section>
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
