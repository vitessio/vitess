export class PrepareResponse {
  public success: boolean;
  public flags: any;
  public message: string;

  constructor(success: boolean, flags= {}, message= '') {
    this.success = success;
    this.flags = flags;
    this.message = message;
  }
}
