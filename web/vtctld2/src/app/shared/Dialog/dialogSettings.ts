/*
  Object for controlling dialog metadata is passed to a vt-dialog component to
  sychronize behavior regarding modals, loading screens, buttons and actions.
*/

export class DialogSettings{
  public actionWord: string;
  public actionFunction: any;
  public dialogTitle: string;
  public dialogSubtitle: string;
  public beforeNameRespText: string;
  public afterNameRespText: string;
  public openModal: boolean;
  public dialogForm: boolean;
  public dialogLog: boolean;
  public pending: boolean;

  public constructor(actionWord="", actionFunction=undefined, dialogTitle="", dialogSubtitle="", beforeNameRespText="", afterNameRespText="", openModal=false, dialogForm=true, dialogLog=false, pending=false) {
    this.actionWord = actionWord;
    this.actionFunction = actionFunction;
    this.dialogTitle = dialogTitle;
    this.dialogSubtitle = dialogSubtitle;
    this.beforeNameRespText = beforeNameRespText == "" ? this.getPastTense(actionWord) + " " : beforeNameRespText;
    this.afterNameRespText = afterNameRespText;
    this.openModal = openModal;
    this.dialogForm = dialogForm;
    this.dialogLog = dialogLog;
    this.pending = pending;
  }
  
  private getPastTense(word) {
    if (word.charAt(word.length - 1) == "e" ) {
      return word + "d"
    }
    return word + "ed"
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

