## 5. Intermediate Storage Strategy

// Buffered Write 10k vs 25k batch size

| Method                                 | BatchSize | Compression | RowGroupSize | Approach      | MessageCount | Workload     | Mean    | Error   | StdDev  | Rank | Gen0      | Gen1      | Allocated |
|--------------------------------------- |---------- |------------ |------------- |-------------- |------------- |------------- |--------:|--------:|--------:|-----:|----------:|----------:|----------:|
| 'Fetch from gRPC and store to Parquet' | 25000     | Snappy      | 5000         | BufferedWrite | 50000        | LargeBatched | 45.68 s | 2.028 s | 2.708 s |    1 | 2000.0000 | 1000.0000 | 178.26 MB |
| 'Fetch from gRPC and store to Parquet' | 10000     | Snappy      | 5000         | BufferedWrite | 50000        | LargeBatched | 47.73 s | 2.826 s | 3.773 s |    1 | 6000.0000 | 4000.0000 | 178.29 MB |


// Direct Write 10k vs 25k batch size

| Method                                 | BatchSize | Compression | RowGroupSize | Approach    | MessageCount | Workload     | Mean    | Error   | StdDev  | Rank | Gen0      | Gen1      | Allocated |
|--------------------------------------- |---------- |------------ |------------- |------------ |------------- |------------- |--------:|--------:|--------:|-----:|----------:|----------:|----------:|
| 'Fetch from gRPC and store to Parquet' | 25000     | Snappy      | 5000         | DirectWrite | 50000        | LargeBatched | 47.74 s | 2.692 s | 3.594 s |    1 | 2000.0000 | 1000.0000 | 182.22 MB |
| 'Fetch from gRPC and store to Parquet' | 10000     | Snappy      | 5000         | DirectWrite | 50000        | LargeBatched | 48.27 s | 2.484 s | 3.317 s |    1 | 2000.0000 | 1000.0000 | 177.88 MB |
