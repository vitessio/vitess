export class InMemoryDataService {
  
  /*Serves as the 'Server data' for the in memory database*/
  createDb() {
    let tablet_statuses = [
      {"100": {SecondsBehindMaster: 1, CpuUsage: 22, Qps: 5, HealthError:""}},
      {"200": {SecondsBehindMaster: 1, CpuUsage: 22, Qps: 5, HealthError:"Too far behind"}}
    ];

    return {tablet_statuses};
  }
}
