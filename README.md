# CSV vs Parquet

This is a simple Go project to test the speed differences between writing to CSV and writing to Parquet.

The dataset will be N randomly generated entires with 3 columns.

## Benchmarks

Using the Hyperfine benchmarking tool, I ran the built binary for the CSV file and the Parquet file and as you can see from the results below the parquet is much faster writing 100,000 rows.

$ ./tests.sh
Benchmark 1: ./app -type csv -amount 100000
  Time (mean ± σ):     244.2 ms ±   8.7 ms    [User: 22.7 ms, System: 197.6 ms]
  Range (min … max):   234.4 ms … 261.9 ms    11 runs

Benchmark 1: ./app -type parquet -amount 100000
  Time (mean ± σ):      38.3 ms ±   3.8 ms    [User: 62.0 ms, System: 9.9 ms]
  Range (min … max):    26.4 ms …  44.4 ms    69 runs
