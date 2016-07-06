export class InMemoryDataService {
  createDb() {
    let keyspaces = [
      {
        name: 'KS1',
        servingShards: [
          '-80', 
          '80-',
        ],
        nonservingShards: [
          '0',
        ],
        healthy: true,
      },
      {
        name: 'KS2',
        servingShards: [
          '-40',
          '40-80',
          '80-c0',
          'c0-',
        ],
        nonservingShards: [
          '0',
        ],
        healthy: true,
      },
      {
        name: 'KS3',
        servingShards: [
        ],
        nonservingShards: [
          '0',
        ],
        healthy: false,
      },
    ];
    let tablets = [
      {
        KSName: "KS1",
        shards: [
          {
            name: "-80",
            tablets: [
              {
                type: 1,
                status: "healthy",
                stat: 100,
                stat2: 10,
              },
              {
                type: 2,
                status: "healthy",
                stat: 90,
                stat2: 6,
              },      
              {
                type: 2,
                status: "healthy",
                stat: 120,
                stat2: 12,
              },
              {
                type: 3,
                status: "healthy",
                stat: 140,
                stat2: 8,
              },
              {
                type: 3,
                status: "healthy",
                stat: 115,
                stat2: 10,
              },
            ],
          },
          {
            name: "80-",
            tablets: [
              {
                type: 1,
                status: "healthy",
                stat: 100,
                stat2: 10,
              },
              {
                type: 2,
                status: "healthy",
                stat: 90,
                stat2: 6,
              },      
              {
                type: 2,
                status: "healthy",
                stat: 120,
                stat2: 12,
              },
              {
                type: 3,
                status: "healthy",
                stat: 140,
                stat2: 8,
              },
              {
                type: 3,
                status: "healthy",
                stat: 115,
                stat2: 10,
              },
            ],
          }
        ],
      },
       {
        KSName: "KS2",
        shards: [
          {
            name: "-40",
            tablets: [
              {
                type: 1,
                status: "healthy",
                stat: 100,
                stat2: 10,
              },
              {
                type: 2,
                status: "healthy",
                stat: 90,
                stat2: 6,
              },      
              {
                type: 2,
                status: "healthy",
                stat: 120,
                stat2: 12,
              },
              {
                type: 3,
                status: "healthy",
                stat: 140,
                stat2: 8,
              },
              {
                type: 3,
                status: "healthy",
                stat: 115,
                stat2: 10,
              },
            ],
          },
          {
            name: "40-80",
            tablets: [
              {
                type: 1,
                status: "healthy",
                stat: 100,
                stat2: 10,
              },
              {
                type: 2,
                status: "healthy",
                stat: 90,
                stat2: 6,
              },      
              {
                type: 2,
                status: "healthy",
                stat: 120,
                stat2: 12,
              },
              {
                type: 3,
                status: "healthy",
                stat: 140,
                stat2: 8,
              },
              {
                type: 3,
                status: "healthy",
                stat: 115,
                stat2: 10,
              },
            ],
          },
          {
            name: "80-c0",
            tablets: [
              {
                type: 1,
                status: "healthy",
                stat: 100,
                stat2: 10,
              },
              {
                type: 2,
                status: "healthy",
                stat: 90,
                stat2: 6,
              },      
              {
                type: 2,
                status: "healthy",
                stat: 120,
                stat2: 12,
              },
              {
                type: 3,
                status: "healthy",
                stat: 140,
                stat2: 8,
              },
              {
                type: 3,
                status: "healthy",
                stat: 115,
                stat2: 10,
              },
            ],
          },
          {
            name: "c0-",
            tablets: [
              {
                type: 1,
                status: "healthy",
                stat: 100,
                stat2: 10,
              },
              {
                type: 2,
                status: "healthy",
                stat: 90,
                stat2: 6,
              },      
              {
                type: 2,
                status: "healthy",
                stat: 120,
                stat2: 12,
              },
              {
                type: 3,
                status: "healthy",
                stat: 140,
                stat2: 8,
              },
              {
                type: 3,
                status: "healthy",
                stat: 115,
                stat2: 10,
              },
            ],
          }
        ],
      },
      {
        KSName: "KS3",
        shards: [],
      },
    ];
    return {keyspaces, tablets};
  }
}