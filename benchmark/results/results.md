# Benchmark results

Machine: Intel(R) Xeon(R) Processor @ 2.10GHz (4 logical CPUs), Linux 6.18.44-fc-v310, rustc 1.96.0 (ac68faa20 2026-05-25)

Every format stores the same events. Query times are the median of 5 runs after one warm-up; write times are the median of 3 runs. All queries run on a single thread against the file held in memory. Every format returned identical results for every query (checked on each run).

## telemetry events: 10000 rows

| Format | Size (MiB) | Bytes per event | Size vs JSON | Write (ms) | Write (events/s) | Open (ms) |
|---|---:|---:|---:|---:|---:|---:|
| oe | 0.28 | 29.1 | 11.9% | 24 | 416394 | 0.10 |
| parquet | 0.13 | 13.7 | 5.6% | 4 | 2232074 | 0.01 |
| vortex | 0.16 | 16.6 | 6.8% | 29 | 339903 | 0.64 |
| json | 2.34 | 245.1 | 100.0% | 13 | 765256 | 0.00 |
| json+zstd | 0.22 | 23.4 | 9.5% | 15 | 653379 | 0.00 |

| Query (median ms; entity_lookup is per lookup) | Rows matched | oe | parquet | vortex | json | json+zstd |
|---|---:|---:|---:|---:|---:|---:|
| scan_all_columns | 10000 | 1.90 | 1.29 | 0.81 | 17.53 | 19.64 |
| scan_one_number | 10000 | 0.02 | 0.17 | 0.04 | 17.25 | 14.25 |
| scan_one_payload_number | 10000 | 0.08 | 0.17 | 0.11 | 13.93 | 14.18 |
| scan_one_payload_text | 10000 | 0.07 | 0.09 | 0.05 | 16.51 | 14.34 |
| filter_type_and_payload | 12 | 0.07 | 0.43 | 0.27 | 13.67 | 15.71 |
| time_window_1pct | 100 | 0.02 | 0.27 | 0.11 | 14.43 | 14.55 |
| entity_lookup | 820 | 0.11 | 0.29 | 0.12 | 13.00 | 14.40 |

## telemetry events: 100000 rows

| Format | Size (MiB) | Bytes per event | Size vs JSON | Write (ms) | Write (events/s) | Open (ms) |
|---|---:|---:|---:|---:|---:|---:|
| oe | 3.30 | 34.6 | 14.1% | 224 | 447424 | 0.47 |
| parquet | 1.11 | 11.6 | 4.7% | 37 | 2734926 | 0.03 |
| vortex | 1.76 | 18.5 | 7.5% | 126 | 796439 | 0.60 |
| json | 23.36 | 245.0 | 100.0% | 135 | 742585 | 0.00 |
| json+zstd | 2.36 | 24.7 | 10.1% | 166 | 603002 | 0.00 |

| Query (median ms; entity_lookup is per lookup) | Rows matched | oe | parquet | vortex | json | json+zstd |
|---|---:|---:|---:|---:|---:|---:|
| scan_all_columns | 100000 | 21.57 | 9.16 | 6.15 | 181.70 | 210.75 |
| scan_one_number | 100000 | 0.20 | 1.48 | 0.16 | 131.53 | 160.72 |
| scan_one_payload_number | 100000 | 0.86 | 1.07 | 0.91 | 128.78 | 163.02 |
| scan_one_payload_text | 100000 | 0.83 | 0.76 | 0.40 | 132.71 | 165.46 |
| filter_type_and_payload | 143 | 0.76 | 3.31 | 0.31 | 139.42 | 177.82 |
| time_window_1pct | 1000 | 0.05 | 1.95 | 0.14 | 129.68 | 175.50 |
| entity_lookup | 834 | 2.18 | 2.62 | 0.84 | 132.81 | 172.95 |

## telemetry events: 1000000 rows

| Format | Size (MiB) | Bytes per event | Size vs JSON | Write (ms) | Write (events/s) | Open (ms) |
|---|---:|---:|---:|---:|---:|---:|
| oe | 36.57 | 38.3 | 15.7% | 2288 | 437026 | 3.33 |
| parquet | 11.73 | 12.3 | 5.0% | 451 | 2216814 | 0.09 |
| vortex | 18.63 | 19.5 | 8.0% | 1109 | 901751 | 0.67 |
| json | 233.58 | 244.9 | 100.0% | 1383 | 722813 | 5.48 |
| json+zstd | 24.21 | 25.4 | 10.4% | 1911 | 523151 | 0.00 |

| Query (median ms; entity_lookup is per lookup) | Rows matched | oe | parquet | vortex | json | json+zstd |
|---|---:|---:|---:|---:|---:|---:|
| scan_all_columns | 1000000 | 223.49 | 98.01 | 66.25 | 2159.42 | 2293.02 |
| scan_one_number | 1000000 | 1.93 | 15.16 | 1.76 | 1296.06 | 1602.09 |
| scan_one_payload_number | 1000000 | 8.80 | 10.63 | 8.93 | 1359.79 | 1739.16 |
| scan_one_payload_text | 1000000 | 8.16 | 6.96 | 3.86 | 1391.46 | 1729.43 |
| filter_type_and_payload | 1237 | 7.81 | 31.69 | 2.60 | 1474.12 | 1762.12 |
| time_window_1pct | 10000 | 0.11 | 2.53 | 0.42 | 1427.95 | 1625.21 |
| entity_lookup | 837 | 7.56 | 27.64 | 8.00 | 1398.20 | 1657.52 |

## messages events: 10000 rows

| Format | Size (MiB) | Bytes per event | Size vs JSON | Write (ms) | Write (events/s) | Open (ms) |
|---|---:|---:|---:|---:|---:|---:|
| oe | 0.62 | 65.2 | 19.9% | 30 | 329456 | 0.14 |
| parquet | 0.43 | 45.5 | 13.9% | 10 | 1003421 | 0.02 |
| vortex | 0.45 | 47.0 | 14.4% | 45 | 224352 | 0.62 |
| json | 3.12 | 326.9 | 100.0% | 14 | 730300 | 0.00 |
| json+zstd | 0.57 | 59.9 | 18.3% | 25 | 402472 | 0.00 |

| Query (median ms; entity_lookup is per lookup) | Rows matched | oe | parquet | vortex | json | json+zstd |
|---|---:|---:|---:|---:|---:|---:|
| scan_all_columns | 10000 | 3.86 | 2.52 | 0.97 | 19.94 | 21.54 |
| scan_one_number | 10000 | 0.02 | 0.17 | 0.04 | 13.62 | 17.05 |
| scan_one_payload_number | 10000 | 0.01 | 0.18 | 0.03 | 13.67 | 16.27 |
| scan_one_payload_text | 10000 | 1.52 | 1.19 | 0.30 | 20.79 | 17.66 |
| filter_type_and_payload | 265 | 0.08 | 0.55 | 0.25 | 19.74 | 17.96 |
| time_window_1pct | 100 | 0.01 | 0.34 | 0.12 | 13.50 | 16.32 |
| entity_lookup | 1751 | 0.13 | 0.30 | 0.15 | 13.89 | 16.81 |

## messages events: 100000 rows

| Format | Size (MiB) | Bytes per event | Size vs JSON | Write (ms) | Write (events/s) | Open (ms) |
|---|---:|---:|---:|---:|---:|---:|
| oe | 6.74 | 70.7 | 21.7% | 283 | 353169 | 0.71 |
| parquet | 4.10 | 43.0 | 13.2% | 95 | 1056156 | 0.04 |
| vortex | 4.64 | 48.6 | 14.9% | 180 | 555218 | 0.64 |
| json | 31.13 | 326.4 | 100.0% | 137 | 731828 | 0.00 |
| json+zstd | 5.83 | 61.1 | 18.7% | 294 | 339579 | 0.00 |

| Query (median ms; entity_lookup is per lookup) | Rows matched | oe | parquet | vortex | json | json+zstd |
|---|---:|---:|---:|---:|---:|---:|
| scan_all_columns | 100000 | 38.07 | 21.92 | 9.11 | 211.88 | 249.49 |
| scan_one_number | 100000 | 0.17 | 1.47 | 0.14 | 145.60 | 178.65 |
| scan_one_payload_number | 100000 | 0.12 | 1.27 | 0.15 | 151.86 | 178.71 |
| scan_one_payload_text | 100000 | 15.08 | 10.06 | 2.34 | 146.87 | 195.48 |
| filter_type_and_payload | 2655 | 0.77 | 4.10 | 0.45 | 150.27 | 186.94 |
| time_window_1pct | 1000 | 0.01 | 2.81 | 0.13 | 148.56 | 190.65 |
| entity_lookup | 1203 | 2.04 | 2.57 | 0.90 | 141.63 | 181.56 |

## messages events: 1000000 rows

| Format | Size (MiB) | Bytes per event | Size vs JSON | Write (ms) | Write (events/s) | Open (ms) |
|---|---:|---:|---:|---:|---:|---:|
| oe | 70.87 | 74.3 | 22.8% | 3264 | 306386 | 6.44 |
| parquet | 41.65 | 43.7 | 13.4% | 1171 | 854316 | 0.11 |
| vortex | 47.32 | 49.6 | 15.2% | 1717 | 582526 | 0.73 |
| json | 311.21 | 326.3 | 100.0% | 1496 | 668381 | 7.46 |
| json+zstd | 58.99 | 61.9 | 19.0% | 2685 | 372438 | 0.00 |

| Query (median ms; entity_lookup is per lookup) | Rows matched | oe | parquet | vortex | json | json+zstd |
|---|---:|---:|---:|---:|---:|---:|
| scan_all_columns | 1000000 | 407.05 | 255.47 | 88.55 | 2575.00 | 2963.26 |
| scan_one_number | 1000000 | 1.95 | 16.90 | 1.53 | 1535.00 | 1809.53 |
| scan_one_payload_number | 1000000 | 1.28 | 15.93 | 1.51 | 1520.84 | 1929.48 |
| scan_one_payload_text | 1000000 | 158.09 | 117.64 | 23.58 | 1585.85 | 2014.16 |
| filter_type_and_payload | 26269 | 7.73 | 43.50 | 3.22 | 1531.32 | 1928.06 |
| time_window_1pct | 10000 | 0.04 | 3.69 | 0.43 | 1540.83 | 1932.51 |
| entity_lookup | 5411 | 6.36 | 30.17 | 6.44 | 1456.15 | 1934.87 |

