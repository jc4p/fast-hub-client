## 4. Memory Management

# Fast

SKIPPING ThreadPerCore pipeline - known to stall during direct testing

===== RESULTS (FASTEST TO SLOWEST) =====
1. Channel_UnsafeMemoryAccess: 11007ms
2. Channel_MessageObjectPool: 11149ms
3. Channel_ArrayPoolWithRMS: 11262ms
4. Dataflow_MessageObjectPool: 11354ms
5. Dataflow_ArrayPoolWithRMS: 11564ms
6. Dataflow_UnsafeMemoryAccess: 11835ms

Best performing combination: Channel pipeline with UnsafeMemoryAccess strategy
Execution time: 11007ms
Messages per second: 909


# End to End

| Method                                       | BatchSize | MemoryManagement   | PipelineType | Workload      | Mean     | Error    | StdDev    | Median   | Rank | Allocated |
|--------------------------------------------- |---------- |------------------- |------------- |-------------- |---------:|---------:|----------:|---------:|-----:|----------:|
| 'End-to-End Pipeline with Memory Management' | 25000     | UnsafeMemoryAccess | Channel      | LargeBatched  | 179.0 ms |  3.49 ms |   4.90 ms | 178.6 ms |    1 |   3.33 MB |
| 'End-to-End Pipeline with Memory Management' | 10000     | UnsafeMemoryAccess | Channel      | LargeBatched  | 179.2 ms |  3.54 ms |   4.85 ms | 179.7 ms |    1 |   3.32 MB |
| 'End-to-End Pipeline with Memory Management' | 5000      | UnsafeMemoryAccess | Channel      | LargeBatched  | 253.8 ms | 48.13 ms | 140.41 ms | 180.7 ms |    1 |   3.31 MB |
| 'End-to-End Pipeline with Memory Management' | 1000      | UnsafeMemoryAccess | Channel      | LargeBatched  | 262.9 ms | 50.96 ms | 147.85 ms | 180.5 ms |    1 |    3.3 MB |
| 'End-to-End Pipeline with Memory Management' | 10000     | UnsafeMemoryAccess | Channel      | MixedTraffic  | 511.5 ms | 55.10 ms | 158.99 ms | 504.6 ms |    2 |    9.7 MB |
| 'End-to-End Pipeline with Memory Management' | 5000      | UnsafeMemoryAccess | Channel      | MixedTraffic  | 514.8 ms | 46.25 ms | 132.70 ms | 514.7 ms |    2 |   9.67 MB |
| 'End-to-End Pipeline with Memory Management' | 25000     | UnsafeMemoryAccess | Channel      | MixedTraffic  | 518.0 ms | 48.84 ms | 136.14 ms | 514.0 ms |    2 |   9.66 MB |
| 'End-to-End Pipeline with Memory Management' | 1000      | UnsafeMemoryAccess | Channel      | MixedTraffic  | 520.8 ms | 43.73 ms | 126.17 ms | 519.7 ms |    2 |   9.66 MB |
| 'End-to-End Pipeline with Memory Management' | 10000     | UnsafeMemoryAccess | Channel      | SmallFrequent | 814.9 ms | 59.87 ms | 169.83 ms | 831.4 ms |    3 |  17.46 MB |
| 'End-to-End Pipeline with Memory Management' | 1000      | UnsafeMemoryAccess | Channel      | SmallFrequent | 817.7 ms | 62.52 ms | 181.39 ms | 817.3 ms |    3 |  17.64 MB |
| 'End-to-End Pipeline with Memory Management' | 25000     | UnsafeMemoryAccess | Channel      | SmallFrequent | 846.5 ms | 74.32 ms | 215.61 ms | 838.0 ms |    3 |  17.21 MB |
| 'End-to-End Pipeline with Memory Management' | 5000      | UnsafeMemoryAccess | Channel      | SmallFrequent | 846.7 ms | 57.70 ms | 167.40 ms | 854.9 ms |    3 |  17.44 MB |
