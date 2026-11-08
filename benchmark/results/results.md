# Benchmark results

Machine: Intel(R) Xeon(R) Processor @ 2.10GHz (4 logical CPUs), Linux 6.18.44-fc-v310, rustc 1.96.0 (ac68faa20 2026-05-25)

Every format stores the same events. Query times are the median of 5 runs after one warm-up; write times are the median of 3 runs. All queries run on a single thread against the file held in memory. Every format returned identical results for every query (checked on each run).

## telemetry events: 10000 rows

| Format | Size (MiB) | Bytes per event | Size vs JSON | Write (ms) | Write (events/s) | Open (ms) | Open + first lookup (ms) |
|---|---:|---:|---:|---:|---:|---:|---:|
| oe | 0.23 | 23.8 | 9.7% | 27 | 364086 | 0.14 | 0.23 |
| oe (lazy open) | 0.23 | 23.8 | 9.7% | 25 | 406729 | 0.16 | 0.42 |
| parquet | 0.13 | 13.7 | 5.6% | 5 | 2125644 | 0.02 | 0.37 |
| vortex | 0.16 | 16.6 | 6.8% | 32 | 310978 | 0.58 | 0.94 |
| json | 2.34 | 245.1 | 100.0% | 10 | 1036498 | 0.00 | 13.89 |
| json+zstd | 0.22 | 23.4 | 9.5% | 16 | 631636 | 0.00 | 12.68 |

| Query (median ms; entity_lookup is per lookup) | Rows matched | oe | oe (lazy open) | parquet | vortex | json | json+zstd |
|---|---:|---:|---:|---:|---:|---:|---:|
| scan_all_columns | 10000 | 0.59 | 0.95 | 1.48 | 1.00 | 18.27 | 15.82 |
| scan_one_number | 10000 | 0.02 | 0.02 | 0.17 | 0.04 | 13.06 | 12.59 |
| scan_one_payload_number | 10000 | 0.07 | 0.13 | 0.18 | 0.10 | 13.22 | 15.19 |
| scan_one_payload_text | 10000 | 0.06 | 0.11 | 0.10 | 0.05 | 13.67 | 12.04 |
| filter_type_and_payload | 12 | 0.06 | 0.12 | 0.45 | 0.31 | 14.24 | 12.53 |
| time_window_1pct | 100 | 0.02 | 0.03 | 0.28 | 0.11 | 13.97 | 12.29 |
| entity_lookup | 820 | 0.09 | 0.18 | 0.31 | 0.13 | 13.68 | 12.14 |

Removing events (redaction). OE records a deletion vector; the other formats rewrite the whole file.

| Case | Events removed | oe (ms) | oe (bytes written) | oe (lazy open) (ms) | oe (lazy open) (bytes written) | parquet (ms) | parquet (bytes written) | vortex (ms) | vortex (bytes written) | json (ms) | json (bytes written) | json+zstd (ms) | json+zstd (bytes written) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| redact_1_event | 1 | 0.00 | 9 | 0.00 | 9 | 9.73 | 134711 | 35.08 | 165968 | 32.83 | 2450649 | 33.70 | 233602 |
| redact_1pct_of_events | 100 | 0.03 | 405 | 0.04 | 405 | 9.55 | 133804 | 29.42 | 165504 | 33.38 | 2426385 | 39.67 | 231239 |

## telemetry events: 100000 rows

| Format | Size (MiB) | Bytes per event | Size vs JSON | Write (ms) | Write (events/s) | Open (ms) | Open + first lookup (ms) |
|---|---:|---:|---:|---:|---:|---:|---:|
| oe | 2.65 | 27.7 | 11.3% | 352 | 284375 | 1.22 | 1.50 |
| oe (lazy open) | 2.65 | 27.7 | 11.3% | 299 | 334250 | 0.90 | 1.66 |
| parquet | 1.11 | 11.6 | 4.7% | 43 | 2333101 | 0.05 | 2.68 |
| vortex | 1.76 | 18.5 | 7.5% | 113 | 886904 | 0.51 | 1.41 |
| json | 23.36 | 245.0 | 100.0% | 108 | 929294 | 0.00 | 119.78 |
| json+zstd | 2.36 | 24.7 | 10.1% | 168 | 594929 | 0.00 | 149.57 |

| Query (median ms; entity_lookup is per lookup) | Rows matched | oe | oe (lazy open) | parquet | vortex | json | json+zstd |
|---|---:|---:|---:|---:|---:|---:|---:|
| scan_all_columns | 100000 | 5.16 | 6.74 | 10.50 | 6.49 | 179.46 | 235.27 |
| scan_one_number | 100000 | 0.13 | 0.18 | 1.56 | 0.14 | 110.46 | 141.68 |
| scan_one_payload_number | 100000 | 0.68 | 0.90 | 1.08 | 0.71 | 108.92 | 143.42 |
| scan_one_payload_text | 100000 | 0.52 | 0.73 | 0.78 | 0.55 | 126.96 | 155.81 |
| filter_type_and_payload | 143 | 0.65 | 0.90 | 3.37 | 0.30 | 124.47 | 159.13 |
| time_window_1pct | 1000 | 0.04 | 0.05 | 2.03 | 0.10 | 131.06 | 188.87 |
| entity_lookup | 834 | 0.33 | 0.47 | 2.64 | 0.85 | 114.68 | 153.47 |

Removing events (redaction). OE records a deletion vector; the other formats rewrite the whole file.

| Case | Events removed | oe (ms) | oe (bytes written) | oe (lazy open) (ms) | oe (lazy open) (bytes written) | parquet (ms) | parquet (bytes written) | vortex (ms) | vortex (bytes written) | json (ms) | json (bytes written) | json+zstd (ms) | json+zstd (bytes written) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| redact_1_event | 1 | 0.00 | 9 | 0.00 | 9 | 89.59 | 1162107 | 148.82 | 1846136 | 354.17 | 24497827 | 378.82 | 2470339 |
| redact_1pct_of_events | 1000 | 0.79 | 4005 | 0.85 | 4005 | 88.74 | 1129279 | 177.04 | 1834576 | 340.15 | 24253096 | 406.82 | 2446064 |

## telemetry events: 1000000 rows

| Format | Size (MiB) | Bytes per event | Size vs JSON | Write (ms) | Write (events/s) | Open (ms) | Open + first lookup (ms) |
|---|---:|---:|---:|---:|---:|---:|---:|
| oe | 29.91 | 31.4 | 12.8% | 3141 | 318367 | 10.32 | 10.08 |
| oe (lazy open) | 29.91 | 31.4 | 12.8% | 2989 | 334600 | 8.50 | 14.88 |
| parquet | 11.73 | 12.3 | 5.0% | 481 | 2078552 | 0.14 | 28.45 |
| vortex | 18.63 | 19.5 | 8.0% | 1183 | 845162 | 0.71 | 6.80 |
| json | 233.58 | 244.9 | 100.0% | 1071 | 933968 | 5.29 | 1274.41 |
| json+zstd | 24.21 | 25.4 | 10.4% | 1582 | 632183 | 0.00 | 1606.58 |

| Query (median ms; entity_lookup is per lookup) | Rows matched | oe | oe (lazy open) | parquet | vortex | json | json+zstd |
|---|---:|---:|---:|---:|---:|---:|---:|
| scan_all_columns | 1000000 | 55.53 | 55.39 | 103.64 | 77.28 | 1998.91 | 2273.65 |
| scan_one_number | 1000000 | 1.48 | 1.50 | 15.83 | 2.03 | 1186.65 | 1574.51 |
| scan_one_payload_number | 1000000 | 6.88 | 6.83 | 11.00 | 8.99 | 1191.47 | 1630.30 |
| scan_one_payload_text | 1000000 | 5.54 | 5.53 | 7.81 | 4.73 | 1330.18 | 1679.80 |
| filter_type_and_payload | 1237 | 7.00 | 7.01 | 34.91 | 2.18 | 1287.19 | 1729.94 |
| time_window_1pct | 10000 | 0.09 | 0.09 | 2.79 | 0.54 | 1168.66 | 1538.38 |
| entity_lookup | 837 | 1.42 | 1.43 | 29.51 | 6.01 | 1295.02 | 1644.27 |

Removing events (redaction). OE records a deletion vector; the other formats rewrite the whole file.

| Case | Events removed | oe (ms) | oe (bytes written) | oe (lazy open) (ms) | oe (lazy open) (bytes written) | parquet (ms) | parquet (bytes written) | vortex (ms) | vortex (bytes written) | json (ms) | json (bytes written) | json+zstd (ms) | json+zstd (bytes written) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| redact_1_event | 1 | 0.00 | 9 | 0.00 | 9 | 930.62 | 12309274 | 1554.12 | 19543608 | 3392.05 | 244921417 | 4669.87 | 25386110 |
| redact_1pct_of_events | 10000 | 10.99 | 160005 | 11.74 | 160005 | 968.97 | 12020757 | 1472.11 | 19543040 | 3432.86 | 242472519 | 4270.95 | 25138782 |

## messages events: 10000 rows

| Format | Size (MiB) | Bytes per event | Size vs JSON | Write (ms) | Write (events/s) | Open (ms) | Open + first lookup (ms) |
|---|---:|---:|---:|---:|---:|---:|---:|
| oe | 0.60 | 62.4 | 19.1% | 44 | 227605 | 0.25 | 0.23 |
| oe (lazy open) | 0.60 | 62.4 | 19.1% | 39 | 257125 | 0.08 | 0.22 |
| parquet | 0.43 | 45.5 | 13.9% | 11 | 902155 | 0.02 | 0.37 |
| vortex | 0.45 | 47.0 | 14.4% | 46 | 215472 | 0.53 | 0.86 |
| json | 3.12 | 326.9 | 100.0% | 15 | 662221 | 0.00 | 14.03 |
| json+zstd | 0.57 | 59.9 | 18.3% | 23 | 427342 | 0.00 | 20.13 |

| Query (median ms; entity_lookup is per lookup) | Rows matched | oe | oe (lazy open) | parquet | vortex | json | json+zstd |
|---|---:|---:|---:|---:|---:|---:|---:|
| scan_all_columns | 10000 | 1.30 | 1.05 | 2.80 | 1.03 | 20.51 | 19.22 |
| scan_one_number | 10000 | 0.02 | 0.02 | 0.18 | 0.04 | 13.92 | 18.21 |
| scan_one_payload_number | 10000 | 0.02 | 0.01 | 0.17 | 0.03 | 14.05 | 17.91 |
| scan_one_payload_text | 10000 | 0.46 | 0.36 | 1.17 | 0.27 | 14.91 | 18.70 |
| filter_type_and_payload | 265 | 0.09 | 0.08 | 0.56 | 0.24 | 15.15 | 20.14 |
| time_window_1pct | 100 | 0.01 | 0.01 | 0.38 | 0.23 | 14.33 | 15.45 |
| entity_lookup | 1751 | 0.02 | 0.02 | 0.32 | 0.20 | 14.37 | 18.10 |

Removing events (redaction). OE records a deletion vector; the other formats rewrite the whole file.

| Case | Events removed | oe (ms) | oe (bytes written) | oe (lazy open) (ms) | oe (lazy open) (bytes written) | parquet (ms) | parquet (bytes written) | vortex (ms) | vortex (bytes written) | json (ms) | json (bytes written) | json+zstd (ms) | json+zstd (bytes written) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| redact_1_event | 1 | 0.00 | 9 | 0.00 | 9 | 18.54 | 452866 | 39.30 | 473520 | 36.81 | 3268410 | 45.63 | 598999 |
| redact_1pct_of_events | 100 | 0.02 | 405 | 0.02 | 405 | 18.22 | 448727 | 55.84 | 466192 | 37.31 | 3235234 | 47.46 | 592934 |

## messages events: 100000 rows

| Format | Size (MiB) | Bytes per event | Size vs JSON | Write (ms) | Write (events/s) | Open (ms) | Open + first lookup (ms) |
|---|---:|---:|---:|---:|---:|---:|---:|
| oe | 6.26 | 65.6 | 20.1% | 403 | 248190 | 1.20 | 1.84 |
| oe (lazy open) | 6.26 | 65.6 | 20.1% | 430 | 232731 | 0.77 | 1.80 |
| parquet | 4.10 | 43.0 | 13.2% | 96 | 1039124 | 0.05 | 2.56 |
| vortex | 4.64 | 48.6 | 14.9% | 171 | 583172 | 0.61 | 1.67 |
| json | 31.13 | 326.4 | 100.0% | 136 | 737854 | 0.00 | 156.32 |
| json+zstd | 5.83 | 61.1 | 18.7% | 248 | 402885 | 0.00 | 198.41 |

| Query (median ms; entity_lookup is per lookup) | Rows matched | oe | oe (lazy open) | parquet | vortex | json | json+zstd |
|---|---:|---:|---:|---:|---:|---:|---:|
| scan_all_columns | 100000 | 10.55 | 8.42 | 21.96 | 10.58 | 246.29 | 286.02 |
| scan_one_number | 100000 | 0.21 | 0.14 | 1.36 | 0.36 | 145.21 | 195.07 |
| scan_one_payload_number | 100000 | 0.13 | 0.11 | 1.13 | 0.21 | 146.92 | 201.25 |
| scan_one_payload_text | 100000 | 4.43 | 3.04 | 9.79 | 2.94 | 138.29 | 212.41 |
| filter_type_and_payload | 2655 | 0.98 | 0.66 | 4.24 | 0.61 | 136.13 | 201.64 |
| time_window_1pct | 1000 | 0.02 | 0.01 | 2.82 | 0.15 | 159.95 | 194.38 |
| entity_lookup | 1203 | 0.64 | 0.41 | 2.68 | 1.01 | 139.07 | 191.32 |

Removing events (redaction). OE records a deletion vector; the other formats rewrite the whole file.

| Case | Events removed | oe (ms) | oe (bytes written) | oe (lazy open) (ms) | oe (lazy open) (bytes written) | parquet (ms) | parquet (bytes written) | vortex (ms) | vortex (bytes written) | json (ms) | json (bytes written) | json+zstd (ms) | json+zstd (bytes written) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| redact_1_event | 1 | 0.00 | 9 | 0.00 | 9 | 172.15 | 4300907 | 266.41 | 4860768 | 405.29 | 32636819 | 572.62 | 6111549 |
| redact_1pct_of_events | 1000 | 1.21 | 4005 | 0.82 | 4005 | 166.04 | 4232767 | 233.05 | 4816112 | 381.51 | 32309495 | 511.55 | 6051823 |

## messages events: 1000000 rows

| Format | Size (MiB) | Bytes per event | Size vs JSON | Write (ms) | Write (events/s) | Open (ms) | Open + first lookup (ms) |
|---|---:|---:|---:|---:|---:|---:|---:|
| oe | 65.93 | 69.1 | 21.2% | 4209 | 237601 | 15.06 | 13.02 |
| oe (lazy open) | 65.93 | 69.1 | 21.2% | 4024 | 248506 | 11.11 | 19.35 |
| parquet | 41.65 | 43.7 | 13.4% | 1023 | 977125 | 0.11 | 28.79 |
| vortex | 47.32 | 49.6 | 15.2% | 1481 | 675400 | 0.66 | 6.83 |
| json | 311.21 | 326.3 | 100.0% | 1263 | 791492 | 5.01 | 1509.84 |
| json+zstd | 58.99 | 61.9 | 19.0% | 2542 | 393372 | 2.19 | 1904.07 |

| Query (median ms; entity_lookup is per lookup) | Rows matched | oe | oe (lazy open) | parquet | vortex | json | json+zstd |
|---|---:|---:|---:|---:|---:|---:|---:|
| scan_all_columns | 1000000 | 90.67 | 81.96 | 238.64 | 89.79 | 2627.50 | 3168.06 |
| scan_one_number | 1000000 | 1.48 | 2.67 | 12.41 | 1.50 | 1342.29 | 1865.21 |
| scan_one_payload_number | 1000000 | 1.13 | 2.04 | 10.98 | 1.36 | 1577.44 | 1868.94 |
| scan_one_payload_text | 1000000 | 31.10 | 35.17 | 105.39 | 25.76 | 1574.73 | 2061.30 |
| filter_type_and_payload | 26269 | 6.39 | 7.57 | 37.56 | 2.59 | 1490.26 | 2049.03 |
| time_window_1pct | 10000 | 0.03 | 0.04 | 3.16 | 0.32 | 1607.56 | 1888.30 |
| entity_lookup | 5411 | 1.25 | 1.51 | 22.92 | 5.76 | 1378.89 | 1854.57 |

Removing events (redaction). OE records a deletion vector; the other formats rewrite the whole file.

| Case | Events removed | oe (ms) | oe (bytes written) | oe (lazy open) (ms) | oe (lazy open) (bytes written) | parquet (ms) | parquet (bytes written) | vortex (ms) | vortex (bytes written) | json (ms) | json (bytes written) | json+zstd (ms) | json+zstd (bytes written) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| redact_1_event | 1 | 0.00 | 9 | 0.00 | 9 | 1876.40 | 43671434 | 2348.97 | 49602912 | 4379.95 | 326324891 | 6464.45 | 61854744 |
| redact_1pct_of_events | 10000 | 12.58 | 160005 | 14.49 | 160005 | 1821.79 | 43058471 | 2056.88 | 49314872 | 4550.15 | 323067360 | 6379.71 | 61243459 |

## chat events: 10000 rows

| Format | Size (MiB) | Bytes per event | Size vs JSON | Write (ms) | Write (events/s) | Open (ms) | Open + first lookup (ms) |
|---|---:|---:|---:|---:|---:|---:|---:|
| oe | 0.55 | 57.2 | 16.4% | 41 | 245607 | 0.20 | 0.33 |
| oe (lazy open) | 0.55 | 57.2 | 16.4% | 40 | 247669 | 0.10 | 0.24 |
| parquet | 0.36 | 37.3 | 10.7% | 8 | 1200731 | 0.02 | 0.35 |
| vortex | 0.42 | 43.6 | 12.5% | 45 | 223525 | 0.53 | 0.81 |
| json | 3.32 | 348.4 | 100.0% | 13 | 782402 | 0.00 | 14.82 |
| json+zstd | 0.49 | 51.1 | 14.7% | 21 | 485821 | 0.00 | 17.45 |

| Query (median ms; entity_lookup is per lookup) | Rows matched | oe | oe (lazy open) | parquet | vortex | json | json+zstd |
|---|---:|---:|---:|---:|---:|---:|---:|
| scan_all_columns | 10000 | 1.02 | 0.82 | 2.13 | 0.98 | 19.75 | 22.72 |
| scan_one_number | 10000 | 0.03 | 0.02 | 0.13 | 0.03 | 13.64 | 16.83 |
| scan_one_payload_number | 10000 | 0.01 | 0.01 | 0.05 | 0.03 | 13.66 | 17.04 |
| scan_one_payload_text | 10000 | 0.35 | 0.25 | 0.87 | 0.26 | 15.58 | 18.15 |
| filter_type_and_payload | 266 | 0.08 | 0.06 | 0.29 | 0.17 | 22.17 | 17.69 |
| time_window_1pct | 100 | 0.02 | 0.01 | 0.19 | 0.09 | 21.12 | 17.13 |
| entity_lookup | 36977 | 0.07 | 0.06 | 0.25 | 0.13 | 14.66 | 17.35 |
| latest_50_in_room | 29391 | 0.10 | 0.07 | 0.41 | 0.22 | 15.22 | 18.94 |
| thread_replies | 21 | 0.07 | 0.05 | 0.30 | 0.17 | 14.40 | 18.28 |
| event_by_id | 20 | 0.01 | 0.00 | 0.55 | 0.14 | 14.50 | 17.94 |

Removing events (redaction). OE records a deletion vector; the other formats rewrite the whole file.

| Case | Events removed | oe (ms) | oe (bytes written) | oe (lazy open) (ms) | oe (lazy open) (bytes written) | parquet (ms) | parquet (bytes written) | vortex (ms) | vortex (bytes written) | json (ms) | json (bytes written) | json+zstd (ms) | json+zstd (bytes written) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| redact_1_event | 1 | 0.00 | 9 | 0.00 | 9 | 16.97 | 370690 | 54.86 | 433524 | 36.19 | 3483690 | 43.61 | 511242 |
| redact_1pct_of_events | 100 | 0.02 | 405 | 0.02 | 405 | 15.18 | 367096 | 56.62 | 434356 | 35.35 | 3450121 | 44.07 | 506512 |

## chat events: 100000 rows

| Format | Size (MiB) | Bytes per event | Size vs JSON | Write (ms) | Write (events/s) | Open (ms) | Open + first lookup (ms) |
|---|---:|---:|---:|---:|---:|---:|---:|
| oe | 5.72 | 60.0 | 17.2% | 393 | 254457 | 1.64 | 1.92 |
| oe (lazy open) | 5.72 | 60.0 | 17.2% | 374 | 267222 | 0.85 | 1.84 |
| parquet | 3.41 | 35.8 | 10.2% | 103 | 968615 | 0.07 | 3.09 |
| vortex | 4.18 | 43.9 | 12.5% | 209 | 479063 | 0.60 | 0.93 |
| json | 33.34 | 349.5 | 100.0% | 136 | 737897 | 0.00 | 140.44 |
| json+zstd | 5.10 | 53.5 | 15.3% | 201 | 497362 | 0.00 | 192.44 |

| Query (median ms; entity_lookup is per lookup) | Rows matched | oe | oe (lazy open) | parquet | vortex | json | json+zstd |
|---|---:|---:|---:|---:|---:|---:|---:|
| scan_all_columns | 100000 | 8.95 | 10.42 | 22.70 | 8.31 | 235.66 | 273.40 |
| scan_one_number | 100000 | 0.18 | 0.18 | 1.60 | 0.19 | 157.12 | 174.04 |
| scan_one_payload_number | 100000 | 0.13 | 0.13 | 0.39 | 0.11 | 158.08 | 201.90 |
| scan_one_payload_text | 100000 | 2.88 | 3.42 | 9.56 | 2.18 | 156.08 | 214.64 |
| filter_type_and_payload | 2904 | 0.48 | 0.62 | 3.41 | 0.31 | 133.29 | 206.22 |
| time_window_1pct | 1000 | 0.01 | 0.02 | 2.10 | 0.11 | 159.72 | 194.10 |
| entity_lookup | 81077 | 0.44 | 0.53 | 2.95 | 0.28 | 138.77 | 201.43 |
| latest_50_in_room | 64860 | 0.41 | 0.52 | 4.36 | 0.44 | 146.37 | 211.40 |
| thread_replies | 23 | 0.07 | 0.08 | 3.49 | 0.39 | 136.13 | 177.36 |
| event_by_id | 20 | 0.07 | 0.09 | 4.43 | 0.22 | 138.59 | 187.83 |

Removing events (redaction). OE records a deletion vector; the other formats rewrite the whole file.

| Case | Events removed | oe (ms) | oe (bytes written) | oe (lazy open) (ms) | oe (lazy open) (bytes written) | parquet (ms) | parquet (bytes written) | vortex (ms) | vortex (bytes written) | json (ms) | json (bytes written) | json+zstd (ms) | json+zstd (bytes written) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| redact_1_event | 1 | 0.00 | 9 | 0.00 | 9 | 186.16 | 3580849 | 263.34 | 4381044 | 390.25 | 34954335 | 536.98 | 5350605 |
| redact_1pct_of_events | 1000 | 0.63 | 4005 | 0.56 | 4005 | 194.69 | 3521126 | 319.92 | 4330708 | 360.45 | 34607634 | 489.44 | 5298958 |

## chat events: 1000000 rows

| Format | Size (MiB) | Bytes per event | Size vs JSON | Write (ms) | Write (events/s) | Open (ms) | Open + first lookup (ms) |
|---|---:|---:|---:|---:|---:|---:|---:|
| oe | 63.25 | 66.3 | 18.9% | 4258 | 234868 | 16.55 | 13.95 |
| oe (lazy open) | 63.25 | 66.3 | 18.9% | 4248 | 235389 | 11.88 | 22.58 |
| parquet | 35.96 | 37.7 | 10.8% | 1047 | 955018 | 0.13 | 25.19 |
| vortex | 45.86 | 48.1 | 13.7% | 1985 | 503712 | 0.83 | 2.47 |
| json | 334.22 | 350.5 | 100.0% | 1274 | 785192 | 7.28 | 1533.57 |
| json+zstd | 52.90 | 55.5 | 15.8% | 2443 | 409373 | 0.00 | 1839.26 |

| Query (median ms; entity_lookup is per lookup) | Rows matched | oe | oe (lazy open) | parquet | vortex | json | json+zstd |
|---|---:|---:|---:|---:|---:|---:|---:|
| scan_all_columns | 1000000 | 90.75 | 107.39 | 249.90 | 91.77 | 2713.48 | 3126.20 |
| scan_one_number | 1000000 | 2.23 | 2.10 | 15.46 | 1.62 | 1585.20 | 1920.32 |
| scan_one_payload_number | 1000000 | 1.08 | 1.39 | 3.27 | 1.44 | 1490.62 | 1991.25 |
| scan_one_payload_text | 1000000 | 34.26 | 30.85 | 87.87 | 24.17 | 1603.76 | 2056.88 |
| filter_type_and_payload | 30248 | 6.21 | 5.34 | 30.18 | 2.66 | 1558.48 | 1934.57 |
| time_window_1pct | 10000 | 0.05 | 0.04 | 2.31 | 0.54 | 1611.82 | 1799.74 |
| entity_lookup | 200587 | 2.17 | 1.52 | 24.81 | 1.70 | 1505.14 | 1999.30 |
| latest_50_in_room | 160250 | 2.11 | 1.56 | 42.82 | 2.24 | 1554.16 | 2015.81 |
| thread_replies | 22 | 0.32 | 0.27 | 21.86 | 2.99 | 1535.55 | 1937.38 |
| event_by_id | 20 | 0.10 | 0.08 | 5.69 | 0.71 | 1570.70 | 2063.98 |

Removing events (redaction). OE records a deletion vector; the other formats rewrite the whole file.

| Case | Events removed | oe (ms) | oe (bytes written) | oe (lazy open) (ms) | oe (lazy open) (bytes written) | parquet (ms) | parquet (bytes written) | vortex (ms) | vortex (bytes written) | json (ms) | json (bytes written) | json+zstd (ms) | json+zstd (bytes written) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| redact_1_event | 1 | 0.00 | 9 | 0.00 | 9 | 1931.69 | 37698174 | 2619.13 | 48035456 | 4649.46 | 350453613 | 6544.92 | 55469073 |
| redact_1pct_of_events | 10000 | 12.94 | 160005 | 12.58 | 160005 | 2019.07 | 37167023 | 2887.93 | 47241092 | 4569.84 | 346953322 | 6168.49 | 54925975 |

## Durable ingest

Each batch is acknowledged only after the file is synced to disk (300 batches per row). Parquet and Vortex cannot append, so each batch is a new file.

| Batch size | Format | Events/s | Ack p50 (ms) | Ack p99 (ms) | Bytes on disk |
|---:|---|---:|---:|---:|---:|
| 1 | oej journal | 4671 | 0.182 | 0.696 | 1228800 |
| 1 | json (append) | 5343 | 0.159 | 0.486 | 104659 |
| 1 | parquet (file per batch) | 1101 | 0.879 | 1.533 | 1094080 |
| 1 | vortex (file per batch) | 542 | 1.734 | 2.885 | 3131136 |
| 16 | oej journal | 63245 | 0.240 | 0.469 | 2457600 |
| 16 | json (append) | 57166 | 0.241 | 0.669 | 1664629 |
| 16 | parquet (file per batch) | 16337 | 0.961 | 1.563 | 1373373 |
| 16 | vortex (file per batch) | 1110 | 13.911 | 23.733 | 4804864 |
| 128 | oej journal | 208449 | 0.559 | 1.093 | 19660800 |
| 128 | json (append) | 179279 | 0.668 | 1.693 | 13400243 |
| 128 | parquet (file per batch) | 86561 | 1.429 | 2.731 | 2851044 |
| 128 | vortex (file per batch) | 6143 | 21.031 | 32.633 | 7595608 |

