/*
  Parent flag class for dialog content and it's UI subclasses. All production
  classes should extend one of the subclasses, not Flag itself. Currently, the
  subclasses support:
    InputFlag:    Takes in user raw input and stores it as a string. The UI
                  component is a material2 input.
    CheckBoxFlag: Allows a user to set a boolean using a checkbox and stores
                  the value as a boolean instead of a string. The UI component
                  is a material2 checkbox.
    DropDownFlag: Given a set of options the dropdown menu allows you to offer
                  the user specified choices for actions. The value is stored
                  as a string and the UI component leverages a 
                  paper-dropdown-menu, paper-listbox, and paper-item as 
                  material2 components aren't ready yet.
*/

export class Flag {
  public position: number;
  public type: string;
  public id: string;
  public name: string;
  public description: string;
  public value: any;
  private blockOnEmptyList: string[]; // Block this flag if any flag in this list is empty/false.
  private blockOnFilledList: string[]; // Block this flag if any flag in this list is filled/true.
  public show: boolean;
  public constructor(position: number, type: string, id: string, name: string, description: string="", value: string="", show: boolean=true) {
    this.position = position;
    this.type = type;
    this.id = id;
    this.name = name;
    this.description = description;
    this.value = value;
    this.blockOnEmptyList = [];
    this.blockOnFilledList = [];
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
  public getStrValue(): string {
    return this.value
  }
  public getValue(): any {
    return this.value;
  }
  public setValue(value: string) {
    this.value = value;
  }
  public isEmpty(): boolean {
    return this.value == "";
  }
  public isFilled(): boolean {
    return this.value != "";
  }
}

export class InputFlag extends Flag {
  public value: string;
  public constructor(position: number, id: string, name: string, description: string="", value: string="", show: boolean=true) {
    super(position, "input", id, name, description, value);
    this.value = value;
  }
}

export class CheckBoxFlag extends Flag {
  public value: boolean;
  public constructor(position: number, id: string, name: string, description: string="", value: boolean=false, show: boolean=true) {
    super(position, "checkBox", id, name, description, "", show);
    this.value = value;
  }
  public getStrValue() {
    return this.value ? "true" : "false";
  }
}

export class DropDownFlag extends Flag {
  public value: string;
  public options: string[];
  public constructor(position: number, id: string, name: string, description: string="", value: string="", show: boolean=true) {
    super(position, "dropDown", id, name, description, value, show);
  }
  public setOptions(options: any[]) {
    this.options = options;
  }
  public getOptions(): any[] {
    return this.options;
  }
}