/*
  Parent flag class for dialog content and its UI subclasses. All production
  classes should extend one of the subclasses, not Flag itself. Currently, the
  subclasses support:
    InputFlag:    Takes in user raw input and stores it as a string. The UI
                  component is a material2 input.
    CheckBoxFlag: Allows a user to set a boolean using a checkbox and stores
                  the value as a boolean instead of a string. The UI component
                  is a material2 checkbox.
    DropDownFlag: Given a set of options the dropdown menu allows you to offer
                  the user specified choices for actions. The value is stored
                  as a string and the UI component leverages a PrimeNG 
                  dropdown because material2 components are not ready yet.
*/

export class Flag {
  public position: number;
  public type: string;
  public id: string;
  public name: string;
  public description: string;
  public value: any;
  private blockOnEmptyList= []; // Block this flag if any flag in this list is empty/false.
  private blockOnFilledList= []; // Block this flag if any flag in this list is filled/true.
  private displayOnFlag: string; // Only display this flag if the given flag has the given value.
  private displayOnValue: string;
  public show: boolean;
  public positional = false;
  public namedPositional: string; // if positional and this is set, this will be a named parameter.

  constructor(position: number, type: string, id: string, name: string, description= '', value= '', show= true) {
    this.position = position;
    this.type = type;
    this.id = id;
    this.name = name;
    this.description = description;
    this.value = value;
    this.show = show;
  }

  public setBlockOnEmptyList(blockOnEmptyList: string[]) {
    this.blockOnEmptyList = blockOnEmptyList;
  }

  public getBlockOnEmptyList(): any[] {
    return this.blockOnEmptyList;
  }

  public setBlockOnFilledList(blockOnFilledList: string[]) {
    this.blockOnFilledList = blockOnFilledList;
  }

  public getBlockOnFilledList(): any[] {
    return this.blockOnFilledList;
  }

  public setDisplayOn(flag: string, value: string) {
    this.displayOnFlag = flag;
    this.displayOnValue = value;
  }

  public getDisplayOnFlag(): string {
    return this.displayOnFlag;
  }

  public getDisplayOnValue(): string {
    return this.displayOnValue;
  }

  public getStrValue(): string {
    return this.value;
  }

  public getValue(): any {
    return this.value;
  }

  public setValue(value) {
    this.value = value;
  }

  public isEmpty(): boolean {
    return this.value === '';
  }

  public isFilled(): boolean {
    return this.value !== '';
  }

  public getArgs() {
    if (this.getStrValue() === '' || !this.positional) {
      return [];
    }
    if (this.namedPositional !== undefined) {
      if (this.getValue() === true) {
        return [`-${this.namedPositional}`];
      }
      // Named positional arguments are used for workflow creation, mainly.
      return [`-${this.namedPositional}=${this.getStrValue()}`];
    }
    // Positional arguments only need a value not a key.
    return [this.getStrValue()];
  }

  public getFlags() {
    if (this.getStrValue() === '' || this.positional) {
      return [];
    }
    // Non-positional arguments need a key value pair.
    if (this.getValue() === true) {
      return [`-${this.id}`];
    }
    return [`-${this.id}=${this.getStrValue()}`];
  }
}

export class InputFlag extends Flag {
  public value: string;

  constructor(position: number, id: string, name: string, description= '', value= '', show= true) {
    super(position, 'input', id, name, description, value, show);
    this.value = value;
  }
}

export class CheckBoxFlag extends Flag {
  public value: boolean;

  constructor(position: number, id: string, name: string, description= '', value= false, show= true) {
    super(position, 'checkBox', id, name, description, '', show);
    this.value = value;
  }

  public getStrValue() {
    return this.value ? 'true' : 'false';
  }
}

export class DropDownFlag extends Flag {
  public value: string;
  public options: string[];

  constructor(position: number, id: string, name: string, description= '', value= '', show= true) {
    super(position, 'dropDown', id, name, description, value, show);
  }

  public setOptions(options: any[]) {
    this.options = options;
  }

  public getOptions(): any[] {
    return this.options;
  }
}
