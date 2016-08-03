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
  public name: string;
  public flags: {};
  public requiredFlags: {};
  private prepareFunction: any;
  public constructor(name= '', flags: any = {}, requiredFlags: any = {}, prepareFunction: any= undefined) {
    this.name = name;
    this.flags = flags;
    this.requiredFlags = requiredFlags;
    this.prepareFunction = prepareFunction;
  }

  /*
    Currently turns the flagIds and their values into a url encoded string for
    submission to the server.

    TODO(dsslater): generalize and sanatize for non url encoded transmission.
  */
  public getBody(action: string): string {
    let body = 'action=' + action;
    for (let flagName of Object.keys(this.flags)) {
      let flagStr = '&' + flagName + '=' + this.flags[flagName].getValue();
      body += flagStr;
    }
    return body;
  }

  /*
    Checks to see if a particular flagId should be set as vissible to the user.
    First checks if the flag doesnt exist or if it has explicitly been set to
    not be editable. Then it iterates through the blockOnEmpty and 
    BlockOnFilled lists to ensure that the values of other flags does not 
    prevent this one from being shown.

    NOTE: If you populate either block list you should also create a prepare
    function that sanitizes the flag values to prevent unexpected data from 
    being passed to the server.

  */
  public canDisplay(flagId: string) {
    if (this.flags[flagId] === undefined || this.flags[flagId].show === false) {
      return false;
    }
    for (let testEmpty of this.flags[flagId].blockOnEmptyList) {
      if (this.flags[testEmpty].isEmpty()) {
        return false;
      }
    }
    for (let testFilled of this.flags[flagId].blockOnFilledList) {
      if (this.flags[testFilled].isFilled()) {
        return false;
      }
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
  public getFlags(): Flag[] {
    let flags = [];
    for (let flagName of Object.keys(this.flags)) {
      flags.push(this.flags[flagName]);
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
  public prepare(): PrepareResponse {
    if (this.prepareFunction === undefined) {
      return new PrepareResponse(true);
    }
    let resp = this.prepareFunction(this.flags);
    if (resp.success) {
      this.flags = resp.flags;
    }
    return resp;
  }
}
