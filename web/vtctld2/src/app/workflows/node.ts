export const enum ActionState {
  ENABLED,
  DISABLED,
}

export const enum ActionStyle {
  NORMAL,
  WARNING, // Display warning dialog to confirm action with message
  WAITING, // Highlight to user that the process is waiting on action.
  TRIGGERED,
}

/* 
  Object for any action buttons you want the UI to display to the user.
  Default behavior should be that Action.message appears as a tooltip.
*/
export class Action {
  public name: string;
  public state: ActionState;
  public style: ActionStyle;
  public message: string; // Message to be displayed with action.

  constructor(name: string, state: ActionState, style: ActionStyle, message= '') {
    this.name = name;
    this.state = state;
    this.style = style;
    this.message = message;
  }

  isEnabled() {
    return this.state === ActionState.ENABLED;
  }

  isDisabled() {
    return this.state === ActionState.DISABLED || this.style === ActionStyle.TRIGGERED;
  }

  isWarning() {
    return this.style === ActionStyle.WARNING;
  }

  isWaiting() {
    return this.style === ActionStyle.WAITING;
  }

  isNormal() {
    return this.style === ActionStyle.NORMAL;
  }

  isTriggered() {
    return this.style === ActionStyle.TRIGGERED;
  }
}

export const enum State {
  NOT_STARTED,
  RUNNING,
  DONE
}

export const enum Display { // Only relevant if State is RUNNING.
  INDETERMINATE,  // Only relevant if State is RUNNING.
  DETERMINATE,
  NONE          // Even if Display is NONE progressMsg will still be shown.
}

export class Node {
  public name: string;
  public path: string; // Path to element Ex, “GrandparentID/ParentId/ID”.
  public children: Node[];
  public lastChanged= 0; // Time last changed in seconds.
  public progress= 0; // Should be an int from 0-100 for percentage
  public progressMsg= ''; // Ex. “34/256” “25%” “calculating”  
  public state= State.NOT_STARTED;
  public display= Display.NONE;
  public message= ''; // Instructions for user
  public log= ''; // Log from command
  public disabled= false; // Use for blocking further actions
  public actions: Action[];

  constructor(name: string, path: string, children: any) {
    this.name = name;
    this.path = path;
    this.children = children;
  }

  public update(changes: any) {
    let properties = Object.keys(changes);
      for (let property of properties) {
        this[property] = changes[property];
      }
  }

  public isRoot(): boolean {
    return this.path.split('/').length === 1;
  }

  public isDeterminate() {
    return this.display === Display.DETERMINATE;
  }

  public isIndeterminate() {
    return this.display === Display.INDETERMINATE;
  }

  public isNotStarted() {
    return this.state === State.NOT_STARTED;
  }

  public isRunning() {
    return this.state === State.RUNNING;
  }

  public isDone() {
    return this.state === State.DONE;
  }

}
