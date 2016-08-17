export const enum ActionState {
  ENABLED,
  DISABLED
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

export class Workflow {
  public name: string;
  public path: string; // Path to element Ex, “GrandparentID/ParentId/ID”.
  public children: any[];
  public lastChanged: number; // Time last changed in seconds.
  public progress: number; // Should be an int from 0-100 for percentage
  public progressMsg: string; // Ex. “34/256” “25%” “calculating”  
  public state: State;
  public display: Display;
  public message: string; // Instructions for user
  public log: string; // Log from command
  public disabled: boolean; // Use for blocking further actions
  public actions: Action[];

  constructor(name: string, path: string, children: any, message= '',
              state= State.NOT_STARTED, lastChanged= 0, display= Display.NONE,
              progress= 0, progressMsg= '', disabled= false, actions= [], log= '') {
    this.name = name;
    this.path = path;
    this.children = children;
    this.lastChanged = lastChanged;
    this.progress = progress;
    this.progressMsg = progressMsg;
    this.state = state;
    this.display = display;
    this.log = log;
    this.message = message;
    this.disabled = disabled;
    this.actions = actions;
  }

  public isWorkFlow(): boolean {
    return this.children.length > 0;
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
