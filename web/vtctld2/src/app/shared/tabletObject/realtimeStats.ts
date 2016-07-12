export class RealtimeStats {
  SecondsBehindMaster: number;
  CpuUsage: number;
  Qps: number;
  HealthError: string; /*"" if no error */

/*  constructor(sbm: number, cpu:number, qps:number, error:string) {
    this.SecondsBehindMaster = sbm;
    this.CpuUsage = cpu;
    this.Qps = qps;
    this.HealthError = error;
  }
*/
  constructor(cpy: any) {
    this.SecondsBehindMaster = cpy.SecondsBehindMaster;
    this.CpuUsage = cpy.CpuUsage;
    this.Qps = cpy.Qps;
    this.HealthError = cpy.HealthError;
  }
}
