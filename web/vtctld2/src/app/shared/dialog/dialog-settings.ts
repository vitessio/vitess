/*
  Object for controlling dialog metadata is passed to a vt-dialog component to
  sychronize behavior regarding modals, loading screens, buttons and actions.
*/

export class DialogSettings {
  public actionWord: string;
  public dialogTitle: string;
  public dialogSubtitle: string;
  public respText: string;
  public errMsg: string;
  public logText: string;
  public open = false;
  public dialogForm = true;
  public dialogLog = false;
  public pending = false;
  public onCloseFunction = undefined;

  constructor(actionWord = '', dialogTitle = '', dialogSubtitle = '', errMsg = '') {
    this.actionWord = actionWord;
    this.dialogTitle = dialogTitle;
    this.dialogSubtitle = dialogSubtitle;
    this.errMsg = errMsg;
  }

  public startPending() {
    this.pending = true;
  }

  public endPending() {
    this.pending = false;
  }

  public setMessage(message) {
    this.respText = message;
  }

  public setLog(message) {
    this.logText = message;
  }

  // Opens/closes the gray modal behind a dialog box.
  public toggleModal() {
    this.open = !this.open;
  }
}

