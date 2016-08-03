/*
  Object for controlling dialog metadata is passed to a vt-dialog component to
  sychronize behavior regarding modals, loading screens, buttons and actions.
*/

export class DialogSettings {
  public actionWord: string;
  public actionFunction: any;
  public dialogTitle: string;
  public dialogSubtitle: string;
  public beforeNameRespText: string;
  public afterNameRespText: string;
  public openModal= false;
  public dialogForm= true;
  public dialogLog= false;
  public pending= false;

  public constructor(actionWord= '', actionFunction= undefined, dialogTitle= '',
                     dialogSubtitle= '') {
    this.actionWord = actionWord;
    this.actionFunction = actionFunction;
    this.dialogTitle = dialogTitle;
    this.dialogSubtitle = dialogSubtitle;
  }

  public startPending() {
    this.pending = true;
  }
  public endPending() {
    this.pending = false;
  }
  public setMessage(before, after) {
    this.beforeNameRespText = before;
    this.afterNameRespText = after;
  }
}

