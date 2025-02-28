BENCHMARKS.md

### 1. gRPC Connection Management

Overall:

BenchmarkDotNet v0.14.0, Ubuntu 22.04 LTS (Jammy Jellyfish) WSL
AMD Ryzen 9 7900X, 1 CPU, 24 logical and 12 physical cores
.NET SDK 9.0.100-rc.2.24474.11
  [Host]     : .NET 9.0.0 (9.0.24.47305), X64 RyuJIT AVX-512F+CD+BW+DQ+VL+VBMI
  Job-IVEYTY : .NET 9.0.0 (9.0.24.47305), X64 RyuJIT AVX-512F+CD+BW+DQ+VL+VBMI

InvocationCount=1  UnrollFactor=1

| Method                     | MessageCount | ConcurrentConnections | Mean      | Error     | StdDev     | Median     | Ratio | RatioSD | Rank | Allocated  | Alloc Ratio |
|--------------------------- |------------- |---------------------- |----------:|----------:|-----------:|-----------:|------:|--------:|-----:|-----------:|------------:|
| MultiplexedChannelManager  | 10           | 5                     |  57.80 ms | 27.429 ms |  80.011 ms |   1.382 ms | 30.96 |   61.41 |    1 |  459.64 KB |        1.28 |
| OptimizedConnectionManager | 10           | 5                     | 133.81 ms | 56.390 ms | 164.492 ms |   1.291 ms | 71.67 |  129.24 |    1 |  360.42 KB |        1.00 |
|                            |              |                       |           |           |            |            |       |         |      |            |             |
| MultiplexedChannelManager  | 10           | 20                    |  46.54 ms | 26.275 ms |  76.645 ms |   1.021 ms | 32.31 |   73.65 |    1 |  519.13 KB |        1.37 |
| OptimizedConnectionManager | 10           | 20                    | 132.89 ms | 57.109 ms | 165.685 ms |   1.022 ms | 92.26 |  166.79 |    1 |   379.6 KB |        1.00 |
|                            |              |                       |           |           |            |            |       |         |      |            |             |
| MultiplexedChannelManager  | 100          | 5                     | 263.46 ms | 43.924 ms | 124.604 ms | 185.569 ms |  0.93 |    4.22 |    1 | 1282.97 KB |        0.88 |
| OptimizedConnectionManager | 100          | 5                     | 570.60 ms | 89.541 ms | 258.346 ms | 499.418 ms |  2.02 |    9.07 |    2 | 1456.44 KB |        1.00 |
|                            |              |                       |           |           |            |            |       |         |      |            |             |
| MultiplexedChannelManager  | 100          | 20                    | 175.05 ms |  3.562 ms |   9.067 ms | 176.392 ms |  0.49 |    0.03 |    1 | 1796.34 KB |        1.16 |
| OptimizedConnectionManager | 100          | 20                    | 356.12 ms |  7.025 ms |   9.616 ms | 357.374 ms |  1.00 |    0.04 |    2 | 1552.36 KB |        1.00 |

// * Warnings *
MinIterationTime
  ConnectionManagerBenchmarks.MultiplexedChannelManager: InvocationCount=1, UnrollFactor=1  -> The minimum observed iteration time is 865.667us which is very small. It's recommended to increase it to at least 100ms using more operations.
  ConnectionManagerBenchmarks.OptimizedConnectionManager: InvocationCount=1, UnrollFactor=1 -> The minimum observed iteration time is 725.019us which is very small. It's recommended to increase it to at least 100ms using more operations.
  ConnectionManagerBenchmarks.MultiplexedChannelManager: InvocationCount=1, UnrollFactor=1  -> The minimum observed iteration time is 631.075us which is very small. It's recommended to increase it to at least 100ms using more operations.
  ConnectionManagerBenchmarks.OptimizedConnectionManager: InvocationCount=1, UnrollFactor=1 -> The minimum observed iteration time is 701.249us which is very small. It's recommended to increase it to at least 100ms using more operations.
  ConnectionManagerBenchmarks.MultiplexedChannelManager: InvocationCount=1, UnrollFactor=1  -> The minimum observed iteration time is 6.808ms which is very small. It's recommended to increase it to at least 100ms using more operations.
  ConnectionManagerBenchmarks.OptimizedConnectionManager: InvocationCount=1, UnrollFactor=1 -> The minimum observed iteration time is 6.953ms which is very small. It's recommended to increase it to at least 100ms using more operations.
MultimodalDistribution
  ConnectionManagerBenchmarks.MultiplexedChannelManager: InvocationCount=1, UnrollFactor=1  -> It seems that the distribution can have several modes (mValue = 3.02)
  ConnectionManagerBenchmarks.OptimizedConnectionManager: InvocationCount=1, UnrollFactor=1 -> It seems that the distribution is bimodal (mValue = 3.32)
  ConnectionManagerBenchmarks.OptimizedConnectionManager: InvocationCount=1, UnrollFactor=1 -> It seems that the distribution is bimodal (mValue = 3.29)
  ConnectionManagerBenchmarks.MultiplexedChannelManager: InvocationCount=1, UnrollFactor=1  -> It seems that the distribution is bimodal (mValue = 3.38)
  ConnectionManagerBenchmarks.OptimizedConnectionManager: InvocationCount=1, UnrollFactor=1 -> It seems that the distribution is bimodal (mValue = 4.09)
  ConnectionManagerBenchmarks.MultiplexedChannelManager: InvocationCount=1, UnrollFactor=1  -> It seems that the distribution is bimodal (mValue = 3.26)
  ConnectionManagerBenchmarks.OptimizedConnectionManager: InvocationCount=1, UnrollFactor=1 -> It seems that the distribution is bimodal (mValue = 3.33)

// * Hints *
Outliers
  ConnectionManagerBenchmarks.MultiplexedChannelManager: InvocationCount=1, UnrollFactor=1  -> 2 outliers were removed (504.57 ms, 519.61 ms)
  ConnectionManagerBenchmarks.OptimizedConnectionManager: InvocationCount=1, UnrollFactor=1 -> 2 outliers were removed (977.46 ms, 1.01 s)
  ConnectionManagerBenchmarks.MultiplexedChannelManager: InvocationCount=1, UnrollFactor=1  -> 2 outliers were removed (496.17 ms, 533.84 ms)
  ConnectionManagerBenchmarks.OptimizedConnectionManager: InvocationCount=1, UnrollFactor=1 -> 3 outliers were removed (1.02 s..1.05 s)
  ConnectionManagerBenchmarks.MultiplexedChannelManager: InvocationCount=1, UnrollFactor=1  -> 7 outliers were removed (634.54 ms..1.18 s)
  ConnectionManagerBenchmarks.OptimizedConnectionManager: InvocationCount=1, UnrollFactor=1 -> 4 outliers were removed (1.30 s..1.65 s)
  ConnectionManagerBenchmarks.MultiplexedChannelManager: InvocationCount=1, UnrollFactor=1  -> 24 outliers were removed, 25 outliers were detected (147.34 ms, 465.79 ms..1.21 s)
  ConnectionManagerBenchmarks.OptimizedConnectionManager: InvocationCount=1, UnrollFactor=1 -> 7 outliers were removed (959.58 ms..1.06 s)

// * Legends *
  MessageCount          : Value of the 'MessageCount' parameter
  ConcurrentConnections : Value of the 'ConcurrentConnections' parameter
  Mean                  : Arithmetic mean of all measurements
  Error                 : Half of 99.9% confidence interval
  StdDev                : Standard deviation of all measurements
  Median                : Value separating the higher half of all measurements (50th percentile)
  Ratio                 : Mean of the ratio distribution ([Current]/[Baseline])
  RatioSD               : Standard deviation of the ratio distribution ([Current]/[Baseline])
  Rank                  : Relative position of current benchmark mean among all benchmarks (Arabic style)
  Allocated             : Allocated memory per single operation (managed only, inclusive, 1KB = 1024B)
  Alloc Ratio           : Allocated memory ratio distribution ([Current]/[Baseline])
  1 ms                  : 1 Millisecond (0.001 sec)

// * Diagnostic Output - MemoryDiagnoser *


// ***** BenchmarkRunner: End *****
Run time: 00:03:19 (199.22 sec), executed benchmarks: 8

Global total time: 00:03:24 (204.43 sec), executed benchmarks: 8
// * Artifacts cleanup *
Artifacts cleanup is finished
Benchmarks complete.
kasra@kasra-win10:~/work/hub-client-test/HubClient/HubClient.Benchmarks$


Scalability:


| Method                     | MessageCount | ConcurrentConnections | Mean     | Error    | StdDev   | Median    | Ratio | RatioSD | Rank | Gen0       | Gen1      | Allocated | Alloc Ratio |
|--------------------------- |------------- |---------------------- |---------:|---------:|---------:|----------:|------:|--------:|-----:|-----------:|----------:|----------:|------------:|
| MultiplexedChannelManager  | 5000         | 20                    |  2.746 s | 0.0995 s | 0.2854 s |  2.7242 s |  0.53 |    0.07 |    1 |  1000.0000 |         - |  54.61 MB |        1.00 |
| OptimizedConnectionManager | 5000         | 20                    |  5.194 s | 0.1526 s | 0.4330 s |  5.1000 s |  1.01 |    0.12 |    2 |  1000.0000 |         - |  54.64 MB |        1.00 |
|                            |              |                       |          |          |          |           |       |         |      |            |           |           |             |
| MultiplexedChannelManager  | 5000         | 50                    |  1.423 s | 0.0837 s | 0.2414 s |  1.3662 s |  0.51 |    0.13 |    1 |  1000.0000 |         - |  55.69 MB |        1.00 |
| OptimizedConnectionManager | 5000         | 50                    |  2.913 s | 0.2214 s | 0.6494 s |  2.6751 s |  1.05 |    0.32 |    2 |  1000.0000 |         - |  55.42 MB |        1.00 |
|                            |              |                       |          |          |          |           |       |         |      |            |           |           |             |
| MultiplexedChannelManager  | 5000         | 100                   |  1.100 s | 0.0916 s | 0.2685 s |  0.9062 s |  0.54 |    0.19 |    1 |  1000.0000 |         - |  56.15 MB |        0.99 |
| OptimizedConnectionManager | 5000         | 100                   |  2.144 s | 0.1821 s | 0.5284 s |  1.7575 s |  1.06 |    0.37 |    2 |  1000.0000 |         - |  56.67 MB |        1.00 |
|                            |              |                       |          |          |          |           |       |         |      |            |           |           |             |
| MultiplexedChannelManager  | 10000        | 20                    |  5.124 s | 0.1044 s | 0.3030 s |  5.0880 s |  0.52 |    0.04 |    1 |  3000.0000 |         - | 109.01 MB |        1.00 |
| OptimizedConnectionManager | 10000        | 20                    |  9.904 s | 0.1963 s | 0.5207 s |  9.8705 s |  1.00 |    0.07 |    2 |  3000.0000 |         - | 108.68 MB |        1.00 |
|                            |              |                       |          |          |          |           |       |         |      |            |           |           |             |
| MultiplexedChannelManager  | 10000        | 50                    |  2.337 s | 0.0646 s | 0.1821 s |  2.2901 s |  0.49 |    0.07 |    1 |  3000.0000 | 1000.0000 | 111.52 MB |        1.00 |
| OptimizedConnectionManager | 10000        | 50                    |  4.788 s | 0.1980 s | 0.5776 s |  4.6262 s |  1.01 |    0.17 |    2 |  3000.0000 | 1000.0000 | 111.07 MB |        1.00 |
|                            |              |                       |          |          |          |           |       |         |      |            |           |           |             |
| MultiplexedChannelManager  | 10000        | 100                   |  1.586 s | 0.0912 s | 0.2603 s |  1.4704 s |  0.51 |    0.11 |    1 |  3000.0000 | 1000.0000 | 112.98 MB |        1.00 |
| OptimizedConnectionManager | 10000        | 100                   |  3.157 s | 0.1641 s | 0.4601 s |  3.0935 s |  1.02 |    0.21 |    2 |  3000.0000 | 1000.0000 | 112.95 MB |        1.00 |
|                            |              |                       |          |          |          |           |       |         |      |            |           |           |             |
| MultiplexedChannelManager  | 25000        | 20                    | 12.103 s | 0.2368 s | 0.3471 s | 12.0922 s |  0.50 |    0.02 |    1 |  8000.0000 | 1000.0000 | 272.23 MB |        1.00 |
| OptimizedConnectionManager | 25000        | 20                    | 24.316 s | 0.4790 s | 0.9229 s | 24.2135 s |  1.00 |    0.05 |    2 |  8000.0000 |         - | 272.59 MB |        1.00 |
|                            |              |                       |          |          |          |           |       |         |      |            |           |           |             |
| MultiplexedChannelManager  | 25000        | 50                    |  5.224 s | 0.1042 s | 0.2816 s |  5.1821 s |  0.51 |    0.04 |    1 |  9000.0000 | 2000.0000 | 277.57 MB |        1.00 |
| OptimizedConnectionManager | 25000        | 50                    | 10.376 s | 0.2186 s | 0.6377 s | 10.3092 s |  1.00 |    0.09 |    2 |  9000.0000 | 3000.0000 | 278.04 MB |        1.00 |
|                            |              |                       |          |          |          |           |       |         |      |            |           |           |             |
| MultiplexedChannelManager  | 25000        | 100                   |  3.043 s | 0.1043 s | 0.2993 s |  2.9529 s |  0.53 |    0.07 |    1 | 10000.0000 | 1000.0000 | 284.26 MB |        1.00 |
| OptimizedConnectionManager | 25000        | 100                   |  5.832 s | 0.1803 s | 0.5260 s |  5.6996 s |  1.01 |    0.13 |    2 |  9000.0000 | 3000.0000 | 284.96 MB |        1.00 |


Extreme Concurrency:

InvocationCount=1  UnrollFactor=1

| Method                     | MessageCount | ConcurrentConnections | Mean    | Error    | StdDev   | Median  | Ratio | RatioSD | Rank | Gen0       | Gen1      | Allocated | Alloc Ratio |
|--------------------------- |------------- |---------------------- |--------:|---------:|---------:|--------:|------:|--------:|-----:|-----------:|----------:|----------:|------------:|
| MultiplexedChannel-Fixed16 | 25000        | 100                   | 2.986 s | 0.1032 s | 0.2960 s | 2.867 s |  1.00 |    0.13 |    1 |  9000.0000 | 1000.0000 | 283.99 MB |        1.00 |
| MultiplexedChannel-Dynamic | 25000        | 100                   | 3.015 s | 0.1004 s | 0.2881 s | 2.927 s |  1.01 |    0.13 |    1 |  9000.0000 | 1000.0000 | 283.94 MB |        1.00 |
| MultiplexedChannel-Fixed8  | 25000        | 100                   | 3.061 s | 0.1042 s | 0.2973 s | 3.090 s |  1.02 |    0.14 |    1 |  9000.0000 | 1000.0000 | 283.58 MB |        1.00 |
|                            |              |                       |         |          |          |         |       |         |      |            |           |           |             |
| MultiplexedChannel-Fixed8  | 25000        | 250                   | 1.792 s | 0.1123 s | 0.3036 s | 1.838 s |  1.01 |    0.22 |    1 | 10000.0000 | 1000.0000 | 288.67 MB |        1.00 |
| MultiplexedChannel-Dynamic | 25000        | 250                   | 1.809 s | 0.0899 s | 0.2383 s | 1.888 s |  1.02 |    0.20 |    1 | 10000.0000 | 1000.0000 |  288.4 MB |        1.00 |
| MultiplexedChannel-Fixed16 | 25000        | 250                   | 1.813 s | 0.0932 s | 0.2630 s | 1.816 s |  1.02 |    0.21 |    1 | 10000.0000 | 1000.0000 | 289.39 MB |        1.00 |
|                            |              |                       |         |          |          |         |       |         |      |            |           |           |             |
| MultiplexedChannel-Dynamic | 25000        | 500                   | 1.440 s | 0.0554 s | 0.1489 s | 1.471 s |  1.01 |    0.16 |    1 | 10000.0000 | 2000.0000 | 289.28 MB |        1.00 |
| MultiplexedChannel-Fixed16 | 25000        | 500                   | 1.461 s | 0.0498 s | 0.1373 s | 1.484 s |  1.03 |    0.16 |    1 | 10000.0000 | 2000.0000 |  290.2 MB |        1.00 |
| MultiplexedChannel-Fixed8  | 25000        | 500                   | 1.475 s | 0.0495 s | 0.1331 s | 1.505 s |  1.04 |    0.16 |    1 | 10000.0000 | 1000.0000 | 287.19 MB |        0.99 |
|                            |              |                       |         |          |          |         |       |         |      |            |           |           |             |
| MultiplexedChannel-Fixed8  | 25000        | 750                   | 1.324 s | 0.0713 s | 0.1965 s | 1.387 s |  1.02 |    0.25 |    1 | 11000.0000 | 3000.0000 | 289.32 MB |        1.02 |
| MultiplexedChannel-Dynamic | 25000        | 750                   | 1.334 s | 0.0678 s | 0.1833 s | 1.392 s |  1.03 |    0.25 |    1 | 11000.0000 | 3000.0000 | 285.03 MB |        1.00 |
| MultiplexedChannel-Fixed16 | 25000        | 750                   | 1.347 s | 0.0573 s | 0.1558 s | 1.397 s |  1.04 |    0.24 |    1 | 11000.0000 | 4000.0000 | 285.71 MB |        1.00 |
|                            |              |                       |         |          |          |         |       |         |      |            |           |           |             |
| MultiplexedChannel-Dynamic | 25000        | 1000                  | 1.293 s | 0.0695 s | 0.1913 s | 1.350 s |  1.03 |    0.28 |    1 | 12000.0000 | 6000.0000 | 287.38 MB |        1.00 |
| MultiplexedChannel-Fixed16 | 25000        | 1000                  | 1.339 s | 0.0430 s | 0.1133 s | 1.360 s |  1.07 |    0.25 |    1 | 12000.0000 | 6000.0000 |  283.5 MB |        0.99 |
| MultiplexedChannel-Fixed8  | 25000        | 1000                  | 1.343 s | 0.0408 s | 0.1104 s | 1.362 s |  1.07 |    0.25 |    1 | 13000.0000 | 7000.0000 | 283.43 MB |        0.99 |


Extreme Scaling:

| Method             | MessageCount | ConcurrentConnections | Mean    | Error    | StdDev   | Rank | Gen0       | Gen1       | Gen2      | Allocated |
|------------------- |------------- |---------------------- |--------:|---------:|---------:|-----:|-----------:|-----------:|----------:|----------:|
| DynamicMultiplexer | 25000        | 1000                  | 1.349 s | 0.0413 s | 0.1137 s |    1 | 12000.0000 |  5000.0000 |         - | 285.34 MB |
| DynamicMultiplexer | 25000        | 2500                  | 1.488 s | 0.0735 s | 0.1999 s |    2 | 24000.0000 | 21000.0000 | 4000.0000 | 343.23 MB |
| DynamicMultiplexer | 25000        | 5000                  | 1.645 s | 0.0602 s | 0.1608 s |    3 | 22000.0000 | 19000.0000 | 1000.0000 | 356.22 MB |
| DynamicMultiplexer | 25000        | 7500                  | 1.807 s | 0.1400 s | 0.3736 s |    3 | 20000.0000 | 17000.0000 | 1000.0000 | 329.41 MB |
| DynamicMultiplexer | 25000        | 10000                 | 1.822 s | 0.0843 s | 0.2293 s |    3 | 20000.0000 | 18000.0000 | 1000.0000 | 327.82 MB |


Extreme Scaling uncapped channels:


| Method             | MessageCount | ConcurrentConnections | Mean    | Error    | StdDev   | Median  | Rank | Gen0       | Gen1       | Gen2      | Allocated |
|------------------- |------------- |---------------------- |--------:|---------:|---------:|--------:|-----:|-----------:|-----------:|----------:|----------:|
| DynamicMultiplexer | 25000        | 1000                  | 1.327 s | 0.0573 s | 0.1607 s | 1.366 s |    1 | 13000.0000 |  7000.0000 |         - | 282.34 MB |
| DynamicMultiplexer | 25000        | 2500                  | 1.513 s | 0.0630 s | 0.1682 s | 1.562 s |    2 | 19000.0000 | 16000.0000 |         - | 337.68 MB |
| DynamicMultiplexer | 25000        | 5000                  | 1.611 s | 0.0764 s | 0.2038 s | 1.631 s |    2 | 25000.0000 | 22000.0000 | 1000.0000 | 395.83 MB |
| DynamicMultiplexer | 25000        | 7500                  | 1.650 s | 0.0801 s | 0.2166 s | 1.643 s |    2 | 25000.0000 | 24000.0000 | 1000.0000 | 373.11 MB |
| DynamicMultiplexer | 25000        | 10000                 | 1.772 s | 0.0941 s | 0.2559 s | 1.782 s |    2 | 22000.0000 | 19000.0000 | 1000.0000 | 353.14 MB |
