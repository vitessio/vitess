/*
  DialogContent is an object that handles all of the information the dialog is
  populated with for an eventual server call. It takes in the name of the 
  DialogContent so it can be identified and a map of flagIds to flag objects 
  the dialog should bundle in the eventual server call. It also accepts a map
  of required flagIds mapped to an unused boolean. Finally, the DialogContent
  can be givem a prepare function that can be used to do data validation of 
  the flag values and return a new flag object with sanitized or changed 
  values for the server call. 
*/
import { Flag } from '../flags/flag';
import { PrepareResponse } from '../prepare-response';

export class DialogContent {
  public nameId: string; // Id for the flag whose value should be used for name properties
  public flags: {};
  public requiredFlags: {};
  public action: string;
  private prepareFunction: any;

  constructor(nameId = '', flags = {}, requiredFlags = {}, prepareFunction = undefined, action = '') {
    this.nameId = nameId;
    this.flags = flags;
    this.requiredFlags = requiredFlags;
    this.prepareFunction = prepareFunction;
    this.action = action;
  }

  getName(): string {
    return this.flags[this.nameId] ? this.flags[this.nameId].getStrValue() : '';
  }

  setName(name: string) {
    if (this.flags[this.nameId]) {
      this.flags[this.nameId].setValue(name);
    }
  }

  /*
    Currently turns the flagIds and their values into a url encoded string for
    submission to the server.
  */
  public getPostBody(flags = undefined): string[] {
    if (!flags) {
      flags = this.getFlags();
    }
    let flagArgs = [];
    let posArgs = [];
    flagArgs.push(this.action);
    for (let flag of flags) {
      flagArgs = flagArgs.concat(flag.getFlags());
      posArgs = posArgs.concat(flag.getArgs());
    }
    return flagArgs.concat(posArgs);
  }

  public getBody(action: string): string {
    let body = 'action=' + action;
    for (let flagName of Object.keys(this.flags)) {
      let flagStr = `&${flagName}=${this.flags[flagName].getValue()}`;
      body += flagStr;
    }
    return body;
  }

  /*
    Checks to see if a particular flagId should be set as visible to the user.
    First checks if the flag doesn't exist or if it has explicitly been set to
    not be editable. Then it iterates through the blockOnEmpty and 
    BlockOnFilled lists to ensure that the values of other flags does not 
    prevent this one from being shown.

    NOTE: If you populate either block list you should also create a prepare
    function that sanitizes the flag values to prevent unexpected data from 
    being passed to the server.

  */
  public canDisplay(flagId: string) {
    if (this.flags[flagId] === undefined || this.flags[flagId].show === false) {
      // This is the case where the flag is just disabled.
      return false;
    }
    for (let testEmpty of this.flags[flagId].blockOnEmptyList) {
      // We do not display this flag if a list of strings is empty.
      if (this.flags[testEmpty].isEmpty()) {
        return false;
      }
    }
    for (let testFilled of this.flags[flagId].blockOnFilledList) {
      // We do not display this flag if a list of strings is not empty.
      if (this.flags[testFilled].isFilled()) {
        return false;
      }
    }
    if (this.flags[flagId].getDisplayOnFlag() !== undefined) {
      // We only display this flag if another flag has a specific
      // value.  This is used to display conditional flags if a
      // dropdown has a given value.
      let dependsOn = this.flags[flagId].getDisplayOnFlag();
      if (this.flags[dependsOn] === undefined || this.flags[dependsOn].show === false) {
         return false;
      }
      return this.flags[dependsOn].getStrValue() === this.flags[flagId].getDisplayOnValue();
    }
    return true;
  }

  public getParam(paramId: string): string {
    if (paramId in this.flags) {
      return this.flags[paramId].getStrValue();
    }
    return '';
  }

  public setParam(paramId: string, value: any) {
    if (paramId in this.flags) {
      this.flags[paramId].setValue(value);
    }
  }

  /*
    Returns a sorted list of the flags in the flag object based on their 
    position parameter.
  */
  public getFlags(flagsMap = undefined): Flag[] {
    if (!flagsMap) {
      flagsMap = this.flags;
    }
    let flags = [];
    for (let flagName of Object.keys(flagsMap)) {
      flags.push(flagsMap[flagName]);
    }
    flags.sort(this.orderFlags);
    return flags;
  }

  private orderFlags(a, b): number {
    return a.position - b.position;
  }

  /*
    Checks that all required flags have been set to some non-empty value
    if more granularity is required use a prepareFunction.
  */
  public canSubmit(): boolean {
    for (let flagId of Object.keys(this.requiredFlags)) {
      if (this.getParam(flagId) === '') {
        return false;
      }
    }
    return true;
  }

  public isRequired(flagId: string): boolean {
    return flagId in this.requiredFlags;
  }

  /*
    If a prepareFunction has been provided it will be called. If the
    PrepareRepsonse object returned by the prepareFunction has success set to
    true than the instance flags will bet set to the flags in the response.
    The PrepareResponse will also be returned to the caller.
  */
  public prepare(setFlags = true): PrepareResponse {
    if (this.prepareFunction === undefined) {
      return new PrepareResponse(true, this.flags);
    }
    if (!setFlags) {
      return this.prepareFunction(this.flags);
    }
    let resp = this.prepareFunction(this.flags);
    if (resp.success) {
      this.flags = resp.flags;
    }
    return resp;
  }

  public interpolateMessage(fmtString: string): string {
    let indexOfOpenDoubleBracket = fmtString.indexOf('{{');
    while (indexOfOpenDoubleBracket !== -1) {
      let indexOfCloseDoubleBracket = fmtString.indexOf('}}', indexOfOpenDoubleBracket);
      if (indexOfCloseDoubleBracket !== -1) {
        let lookUpId = fmtString.substring(indexOfOpenDoubleBracket + 2, indexOfCloseDoubleBracket);
        if (this.flags[lookUpId] && this.flags[lookUpId].getStrValue()) {
          fmtString = fmtString.substring(0, indexOfOpenDoubleBracket)
                      + this.flags[lookUpId].getStrValue()
                      + fmtString.substring(indexOfCloseDoubleBracket + 2);
        } else {
          fmtString = fmtString.substring(0, indexOfOpenDoubleBracket)
                      + fmtString.substring(indexOfCloseDoubleBracket + 2);
        }
        indexOfOpenDoubleBracket = fmtString.indexOf('{{');
      } else {
        break;
      }
    }
    return fmtString;
  }
}
