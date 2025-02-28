## 2. Message Serialization & Handling

// * Summary *
| Method                            | MessageCount | Mean         | Error       | StdDev      | Median       | Ratio  | RatioSD | Rank | Allocated  | Alloc Ratio |
|---------------------------------- |------------- |-------------:|------------:|------------:|-------------:|-------:|--------:|-----:|-----------:|------------:|
| ExtremeScale_Comparison           | 5000         |     1.276 us |   0.2285 us |   0.6556 us |     1.197 us |   0.05 |    0.03 |    1 |      400 B |        0.30 |
| SerializeToBuffer_SpanBased       | 5000         |    26.794 us |   2.6785 us |   7.5985 us |    25.589 us |   0.98 |    0.42 |    2 |      688 B |        0.51 |
| Serialize_SpanBased               | 5000         |    26.858 us |   3.5532 us |  10.3086 us |    23.534 us |   0.98 |    0.50 |    2 |     1336 B |        0.99 |
| SerializeToBuffer_PooledMessage   | 5000         |    28.114 us |   2.7861 us |   7.7666 us |    26.366 us |   1.03 |    0.44 |    2 |     1584 B |        1.17 |
| Serialize_PooledMessage           | 5000         |    28.176 us |   2.9321 us |   8.0760 us |    26.039 us |   1.03 |    0.45 |    2 |     1312 B |        0.97 |
| Serialize_Standard                | 5000         |    30.878 us |   4.2533 us |  12.2718 us |    25.929 us |   1.13 |    0.59 |    2 |     1352 B |        1.00 |
| SerializeToStream_PooledBuffer    | 5000         |    35.205 us |   5.1053 us |  13.9756 us |    30.488 us |   1.29 |    0.67 |    2 |     5256 B |        3.89 |
| SerializeToStream_PooledMessage   | 5000         |    38.121 us |   6.3968 us |  18.1466 us |    33.229 us |   1.40 |    0.82 |    2 |     5256 B |        3.89 |
| SerializeToStream_SpanBased       | 5000         |    39.544 us |   7.1258 us |  20.4454 us |    31.244 us |   1.45 |    0.91 |    2 |     4248 B |        3.14 |
| SerializeToBuffer_Standard        | 5000         |    43.136 us |   7.0606 us |  20.5961 us |    35.291 us |   1.58 |    0.93 |    2 |     1296 B |        0.96 |
| Deserialize_SpanBased             | 5000         |    45.941 us |   5.2924 us |  15.0137 us |    45.501 us |   1.68 |    0.78 |    2 |     1696 B |        1.25 |
| Deserialize_PooledMessage         | 5000         |    46.799 us |   6.0664 us |  17.4058 us |    43.348 us |   1.72 |    0.86 |    2 |     2224 B |        1.64 |
| SerializeToStream_Standard        | 5000         |    48.661 us |  11.7011 us |  33.7604 us |    32.717 us |   1.78 |    1.41 |    2 |     5256 B |        3.89 |
| Deserialize_Standard              | 5000         |    49.784 us |   5.8175 us |  16.9699 us |    46.233 us |   1.82 |    0.87 |    3 |     2064 B |        1.53 |
| Serialize_PooledBuffer            | 5000         |    54.427 us |   6.1830 us |  17.7401 us |    50.642 us |   1.99 |    0.92 |    3 |     5088 B |        3.76 |
| SerializeToBuffer_PooledBuffer    | 5000         |    55.664 us |   5.7290 us |  16.2523 us |    51.182 us |   2.04 |    0.89 |    3 |     5536 B |        4.09 |
| Deserialize_PooledBuffer          | 5000         |    56.929 us |   7.2144 us |  20.8151 us |    54.814 us |   2.09 |    1.03 |    3 |     2136 B |        1.58 |
| Throughput_Standard               | 5000         | 2,779.229 us | 192.8579 us | 501.2634 us | 2,592.385 us | 101.87 |   37.38 |    4 |  5341928 B |    3,951.13 |
| Throughput_SpanBased              | 5000         | 2,824.383 us | 214.4463 us | 553.5543 us | 2,611.671 us | 103.52 |   38.88 |    4 |  5338560 B |    3,948.64 |
| Throughput_PooledMessage          | 5000         | 3,073.135 us | 217.9571 us | 554.7704 us | 2,837.204 us | 112.64 |   41.34 |    4 |  5656824 B |    4,184.04 |
| OptimizedThroughput_SpanBased     | 5000         | 3,269.745 us | 287.0828 us | 771.2293 us | 2,929.470 us | 119.85 |   47.91 |    4 |  6301712 B |    4,661.03 |
| OptimizedThroughput_PooledMessage | 5000         | 3,455.071 us | 287.0906 us | 730.7374 us | 3,152.814 us | 126.64 |   48.70 |    4 |  6627000 B |    4,901.63 |
| Throughput_PooledBuffer           | 5000         | 7,728.776 us | 294.3737 us | 800.8653 us | 7,490.431 us | 283.29 |   94.39 |    5 | 27344000 B |   20,224.85 |


With Buffer Reuse:

| Method                            | MessageCount | Mean            | Error         | StdDev       | Median          | Ratio      | RatioSD    | Rank | Gen0       | Gen1      | Gen2      | Allocated    | Alloc Ratio |
|---------------------------------- |------------- |----------------:|--------------:|-------------:|----------------:|-----------:|-----------:|-----:|-----------:|----------:|----------:|-------------:|------------:|
| MemoryMappedFile_Throughput       | 500000       |              NA |            NA |           NA |              NA |          ? |          ? |    ? |         NA |        NA |        NA |           NA |           ? |
| Serialize_PooledMessage           | 500000       |        24.21 us |      4.778 us |     13.56 us |        20.96 us |       1.26 |       1.02 |    1 |          - |         - |         - |      1.29 KB |        1.00 |
| Serialize_Standard                | 500000       |        24.90 us |      4.652 us |     13.20 us |        20.72 us |       1.30 |       1.02 |    1 |          - |         - |         - |      1.29 KB |        1.00 |
| SerializeToBuffer_Standard        | 500000       |        27.02 us |      3.765 us |     10.62 us |        26.89 us |       1.41 |       0.95 |    1 |          - |         - |         - |      1.34 KB |        1.04 |
| Serialize_SpanBased               | 500000       |        29.27 us |      4.839 us |     13.49 us |        27.37 us |       1.53 |       1.11 |    1 |          - |         - |         - |      1.28 KB |        0.99 |
| SerializeToStream_SpanBased       | 500000       |        31.01 us |      5.393 us |     15.39 us |        28.81 us |       1.62 |       1.23 |    1 |          - |         - |         - |      5.41 KB |        4.20 |
| SerializeToBuffer_PooledMessage   | 500000       |        32.84 us |      4.573 us |     12.90 us |        30.43 us |       1.71 |       1.16 |    1 |          - |         - |         - |      1.34 KB |        1.04 |
| SerializeToBuffer_SpanBased       | 500000       |        34.35 us |      5.853 us |     16.89 us |        29.71 us |       1.79 |       1.35 |    1 |          - |         - |         - |      1.31 KB |        1.02 |
| SerializeToStream_Standard        | 500000       |        34.83 us |      4.804 us |     13.71 us |        31.50 us |       1.82 |       1.23 |    1 |          - |         - |         - |       4.8 KB |        3.73 |
| SerializeToStream_PooledBuffer    | 500000       |        36.40 us |      7.194 us |     20.05 us |        30.68 us |       1.90 |       1.53 |    1 |          - |         - |         - |      5.13 KB |        3.98 |
| SerializeToStream_PooledMessage   | 500000       |        38.99 us |      8.854 us |     25.40 us |        30.53 us |       2.03 |       1.82 |    1 |          - |         - |         - |      4.48 KB |        3.47 |
| Deserialize_PooledBuffer          | 500000       |        41.36 us |      6.455 us |     18.63 us |        35.91 us |       2.16 |       1.56 |    1 |          - |         - |         - |      2.27 KB |        1.76 |
| Deserialize_PooledMessage         | 500000       |        45.42 us |      5.346 us |     15.25 us |        43.50 us |       2.37 |       1.51 |    1 |          - |         - |         - |      2.48 KB |        1.92 |
| Deserialize_Standard              | 500000       |        48.14 us |      6.898 us |     20.01 us |        41.85 us |       2.51 |       1.74 |    1 |          - |         - |         - |      2.16 KB |        1.67 |
| Deserialize_SpanBased             | 500000       |        50.69 us |      6.075 us |     17.72 us |        47.66 us |       2.65 |       1.71 |    1 |          - |         - |         - |      1.88 KB |        1.45 |
| Serialize_PooledBuffer            | 500000       |        52.56 us |      8.382 us |     24.32 us |        49.67 us |       2.74 |       2.00 |    1 |          - |         - |         - |      5.63 KB |        4.36 |
| SerializeToBuffer_PooledBuffer    | 500000       |        59.20 us |      7.252 us |     20.33 us |        54.89 us |       3.09 |       1.99 |    1 |          - |         - |         - |      5.69 KB |        4.41 |
| ExtremeScale_Comparison           | 500000       |   119,109.87 us |  2,353.752 us |  5,773.79 us |   118,077.06 us |   6,215.36 |   3,219.23 |    2 |  9000.0000 | 9000.0000 | 9000.0000 |  33063.62 KB |   25,649.35 |
| Throughput_Standard               | 500000       |   165,421.18 us |  3,257.842 us |  4,000.92 us |   165,072.31 us |   8,631.96 |   4,453.08 |    3 |  9000.0000 | 9000.0000 | 9000.0000 | 104546.43 KB |   81,102.68 |
| Throughput_SpanBased              | 500000       |   165,889.32 us |  3,309.533 us |  3,678.54 us |   165,543.83 us |   8,656.39 |   4,464.87 |    3 |  9000.0000 | 9000.0000 | 9000.0000 |  104443.1 KB |   81,022.53 |
| Throughput_PooledMessage          | 500000       |   166,953.14 us |  3,207.638 us |  4,056.64 us |   165,926.55 us |   8,711.90 |   4,494.34 |    3 |  9000.0000 | 9000.0000 | 9000.0000 | 110519.97 KB |   85,736.70 |
| OptimizedThroughput_SpanBased     | 500000       |   248,730.00 us |  4,415.175 us |  4,129.96 us |   249,481.64 us |  12,979.16 |   6,691.69 |    4 | 11000.0000 |         - |         - | 617582.23 KB |  479,094.09 |
| OptimizedThroughput_PooledMessage | 500000       |   254,518.23 us |  4,460.278 us |  4,172.15 us |   254,338.75 us |  13,281.20 |   6,847.30 |    4 | 11000.0000 |         - |         - | 646605.38 KB |  501,609.02 |
| Throughput_PooledBuffer           | 500000       |   265,708.33 us |  4,301.670 us |  4,023.79 us |   264,751.84 us |  13,865.12 |   7,147.73 |    4 | 10000.0000 | 9000.0000 | 9000.0000 | 533957.09 KB |  414,221.26 |
| StreamPipeline_WithBufferReuse    | 500000       |   326,451.47 us |  6,012.081 us |  5,329.55 us |   326,510.64 us |  17,034.80 |   8,782.68 |    5 | 15000.0000 | 5000.0000 | 5000.0000 | 614433.32 KB |  476,651.30 |
| ChannelPipeline_Concurrent        | 500000       | 6,002,411.66 us | 54,974.068 us | 51,422.78 us | 6,007,150.75 us | 313,216.19 | 161,412.77 |    6 | 16000.0000 | 1000.0000 | 1000.0000 | 795746.29 KB |  617,306.21 |

