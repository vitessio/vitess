// RealtimeStats is a copy of the RealtimeStats protobuf message from proto/query.proto.
export class RealtimeStats {
  SecondsBehindMaster: number;
  CpuUsage: number;
  Qps: number;
  // HealthError is the last error we got from health check or empty if the server is healthy.
  HealthError: string;
}
