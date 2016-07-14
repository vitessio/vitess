export class RealtimeStats {
  SecondsBehindMaster: number;
  CpuUsage: number;
  Qps: number;
  HealthError: string; /*"" if no error */

  constructor(cpy: any) {
    this.SecondsBehindMaster = cpy.SecondsBehindMaster;
    this.CpuUsage = cpy.CpuUsage;
    this.Qps = cpy.Qps;
    this.HealthError = cpy.HealthError;
  }
}
