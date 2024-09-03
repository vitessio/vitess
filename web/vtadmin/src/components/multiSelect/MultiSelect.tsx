import { Listbox } from "@headlessui/react";

interface Props<T> {
  items: T[];
  selectedItems: T[];
  setSelectedItems: (items: T[]) => void;
  itemToString: (item: T) => string;
  renderDisplayText: (items: T[]) => string;
}

export const MultiSelect = <T,>({
  items,
  selectedItems,
  setSelectedItems,
  renderDisplayText,
  itemToString,
}: Props<T>) => {
  return (
    <Listbox value={selectedItems} onChange={setSelectedItems} multiple>
      <Listbox.Button>{renderDisplayText(selectedItems)}</Listbox.Button>
      <Listbox.Options>
        {items.map((item) => (
          <Listbox.Option key={item as React.Key} value={item}>
            {itemToString(item)}
          </Listbox.Option>
        ))}
      </Listbox.Options>
    </Listbox>
  );
};
