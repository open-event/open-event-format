# Benchmark results

Machine: Intel(R) Xeon(R) Processor @ 2.10GHz (4 logical CPUs), Linux 6.18.44-fc-v310, rustc 1.96.0 (ac68faa20 2026-05-25)

Every format stores the same events. Query times are the median of 5 runs after one warm-up; write times are the median of 3 runs. All queries run on a single thread against the file held in memory. Every format returned identical results for every query (checked on each run).

## telemetry events: 10000 rows

| Format | Size (MiB) | Bytes per event | Size vs JSON | Write (ms) | Write (events/s) | Open (ms) | Open + first lookup (ms) |
|---|---:|---:|---:|---:|---:|---:|---:|
| oe | 0.47 | 49.5 | 20.2% | 39 | 256161 | 0.18 | 0.29 |
| oe (lazy open) | 0.47 | 49.5 | 20.2% | 30 | 331309 | 0.13 | 0.27 |
| parquet | 0.13 | 13.7 | 5.6% | 5 | 1898867 | 0.02 | 0.51 |
| vortex | 0.16 | 16.6 | 6.8% | 45 | 224535 | 0.50 | 0.82 |
| json | 2.34 | 245.1 | 100.0% | 14 | 728088 | 0.00 | 14.99 |
| json+zstd | 0.22 | 23.4 | 9.5% | 17 | 586786 | 0.00 | 16.41 |

| Query (median ms; entity_lookup is per lookup) | Rows matched | oe | oe (lazy open) | parquet | vortex | json | json+zstd |
|---|---:|---:|---:|---:|---:|---:|---:|
| scan_all_columns | 10000 | 0.94 | 0.90 | 2.13 | 0.83 | 18.68 | 20.78 |
| scan_one_number | 10000 | 0.02 | 0.02 | 0.28 | 0.03 | 13.39 | 15.34 |
| scan_one_payload_number | 10000 | 0.09 | 0.10 | 0.26 | 0.10 | 13.38 | 22.87 |
| scan_one_payload_text | 10000 | 0.07 | 0.07 | 0.24 | 0.05 | 19.59 | 24.03 |
| filter_type_and_payload | 12 | 0.08 | 0.09 | 0.75 | 0.24 | 14.93 | 23.76 |
| time_window_1pct | 100 | 0.02 | 0.02 | 0.55 | 0.11 | 13.89 | 14.42 |
| entity_lookup | 820 | 0.12 | 0.13 | 0.47 | 0.14 | 14.35 | 14.73 |

Removing events (redaction). OE records a deletion vector; the other formats rewrite the whole file.

| Case | Events removed | oe (ms) | oe (bytes written) | oe (lazy open) (ms) | oe (lazy open) (bytes written) | parquet (ms) | parquet (bytes written) | vortex (ms) | vortex (bytes written) | json (ms) | json (bytes written) | json+zstd (ms) | json+zstd (bytes written) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| redact_1_event | 1 | 0.00 | 9 | 0.00 | 9 | 15.32 | 134711 | 36.89 | 165968 | 45.29 | 2450649 | 40.45 | 233602 |
| redact_1pct_of_events | 100 | 0.04 | 405 | 0.02 | 405 | 16.76 | 133804 | 41.41 | 165504 | 34.82 | 2426385 | 41.95 | 231239 |

## telemetry events: 100000 rows

| Format | Size (MiB) | Bytes per event | Size vs JSON | Write (ms) | Write (events/s) | Open (ms) | Open + first lookup (ms) |
|---|---:|---:|---:|---:|---:|---:|---:|
| oe | 5.24 | 55.0 | 22.4% | 451 | 221690 | 1.91 | 2.10 |
| oe (lazy open) | 5.24 | 55.0 | 22.4% | 397 | 252084 | 1.16 | 1.99 |
| parquet | 1.11 | 11.6 | 4.7% | 44 | 2298182 | 0.04 | 2.78 |
| vortex | 1.76 | 18.5 | 7.5% | 153 | 651681 | 0.62 | 1.75 |
| json | 23.36 | 245.0 | 100.0% | 153 | 655388 | 0.00 | 148.80 |
| json+zstd | 2.36 | 24.7 | 10.1% | 182 | 549501 | 0.00 | 177.47 |

| Query (median ms; entity_lookup is per lookup) | Rows matched | oe | oe (lazy open) | parquet | vortex | json | json+zstd |
|---|---:|---:|---:|---:|---:|---:|---:|
| scan_all_columns | 100000 | 13.65 | 10.13 | 10.94 | 7.73 | 220.06 | 265.04 |
| scan_one_number | 100000 | 0.31 | 0.18 | 1.62 | 0.20 | 146.62 | 180.17 |
| scan_one_payload_number | 100000 | 1.27 | 0.89 | 1.08 | 0.94 | 168.89 | 177.82 |
| scan_one_payload_text | 100000 | 1.25 | 0.80 | 0.71 | 0.47 | 146.43 | 180.61 |
| filter_type_and_payload | 143 | 1.25 | 0.94 | 3.23 | 0.38 | 159.23 | 192.06 |
| time_window_1pct | 1000 | 0.08 | 0.05 | 2.00 | 0.16 | 182.38 | 200.06 |
| entity_lookup | 834 | 0.30 | 0.20 | 2.60 | 0.96 | 158.64 | 181.28 |

Removing events (redaction). OE records a deletion vector; the other formats rewrite the whole file.

| Case | Events removed | oe (ms) | oe (bytes written) | oe (lazy open) (ms) | oe (lazy open) (bytes written) | parquet (ms) | parquet (bytes written) | vortex (ms) | vortex (bytes written) | json (ms) | json (bytes written) | json+zstd (ms) | json+zstd (bytes written) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| redact_1_event | 1 | 0.00 | 9 | 0.00 | 9 | 98.27 | 1162107 | 208.08 | 1846136 | 374.89 | 24497827 | 463.69 | 2470339 |
| redact_1pct_of_events | 1000 | 1.08 | 4005 | 1.29 | 4005 | 113.57 | 1129279 | 203.54 | 1834576 | 364.63 | 24253096 | 528.90 | 2446064 |

## telemetry events: 1000000 rows

| Format | Size (MiB) | Bytes per event | Size vs JSON | Write (ms) | Write (events/s) | Open (ms) | Open + first lookup (ms) |
|---|---:|---:|---:|---:|---:|---:|---:|
| oe | 58.80 | 61.7 | 25.2% | 3731 | 267997 | 12.07 | 14.21 |
| oe (lazy open) | 58.80 | 61.7 | 25.2% | 3654 | 273646 | 7.73 | 20.08 |
| parquet | 11.73 | 12.3 | 5.0% | 543 | 1843264 | 0.10 | 29.71 |
| vortex | 18.63 | 19.5 | 8.0% | 1326 | 754161 | 0.83 | 8.15 |
| json | 233.58 | 244.9 | 100.0% | 1338 | 747468 | 6.44 | 1547.94 |
| json+zstd | 24.21 | 25.4 | 10.4% | 2257 | 443024 | 0.00 | 1879.15 |

| Query (median ms; entity_lookup is per lookup) | Rows matched | oe | oe (lazy open) | parquet | vortex | json | json+zstd |
|---|---:|---:|---:|---:|---:|---:|---:|
| scan_all_columns | 1000000 | 107.55 | 112.49 | 121.77 | 85.53 | 2403.40 | 2763.39 |
| scan_one_number | 1000000 | 2.36 | 1.98 | 16.71 | 1.82 | 1440.81 | 1781.00 |
| scan_one_payload_number | 1000000 | 10.46 | 9.84 | 10.85 | 11.92 | 1504.97 | 1739.15 |
| scan_one_payload_text | 1000000 | 9.74 | 7.85 | 8.17 | 4.15 | 1555.69 | 1937.81 |
| filter_type_and_payload | 1237 | 11.77 | 9.63 | 34.74 | 2.13 | 1634.03 | 1996.14 |
| time_window_1pct | 10000 | 0.15 | 0.12 | 2.83 | 0.46 | 1498.13 | 1828.83 |
| entity_lookup | 837 | 2.09 | 2.18 | 28.75 | 5.73 | 1592.85 | 1869.69 |

Removing events (redaction). OE records a deletion vector; the other formats rewrite the whole file.

| Case | Events removed | oe (ms) | oe (bytes written) | oe (lazy open) (ms) | oe (lazy open) (bytes written) | parquet (ms) | parquet (bytes written) | vortex (ms) | vortex (bytes written) | json (ms) | json (bytes written) | json+zstd (ms) | json+zstd (bytes written) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| redact_1_event | 1 | 0.00 | 9 | 0.00 | 9 | 1099.22 | 12309274 | 1751.99 | 19543608 | 4496.02 | 244921417 | 5027.92 | 25386110 |
| redact_1pct_of_events | 10000 | 17.49 | 160005 | 23.67 | 160005 | 1079.32 | 12020757 | 1774.48 | 19543040 | 4390.94 | 242472519 | 5069.79 | 25138782 |

## messages events: 10000 rows

| Format | Size (MiB) | Bytes per event | Size vs JSON | Write (ms) | Write (events/s) | Open (ms) | Open + first lookup (ms) |
|---|---:|---:|---:|---:|---:|---:|---:|
| oe | 0.81 | 84.8 | 26.0% | 42 | 237075 | 0.24 | 0.33 |
| oe (lazy open) | 0.81 | 84.8 | 26.0% | 41 | 244289 | 0.10 | 0.24 |
| parquet | 0.43 | 45.5 | 13.9% | 11 | 886044 | 0.02 | 0.38 |
| vortex | 0.45 | 47.0 | 14.4% | 45 | 220283 | 0.54 | 0.88 |
| json | 3.12 | 326.9 | 100.0% | 13 | 785720 | 0.00 | 12.45 |
| json+zstd | 0.57 | 59.9 | 18.3% | 35 | 284079 | 0.00 | 19.78 |

| Query (median ms; entity_lookup is per lookup) | Rows matched | oe | oe (lazy open) | parquet | vortex | json | json+zstd |
|---|---:|---:|---:|---:|---:|---:|---:|
| scan_all_columns | 10000 | 2.43 | 2.17 | 2.80 | 1.05 | 28.63 | 26.38 |
| scan_one_number | 10000 | 0.02 | 0.02 | 0.22 | 0.03 | 19.22 | 26.51 |
| scan_one_payload_number | 10000 | 0.01 | 0.01 | 0.16 | 0.03 | 18.99 | 19.99 |
| scan_one_payload_text | 10000 | 1.13 | 1.02 | 1.06 | 0.27 | 15.63 | 19.51 |
| filter_type_and_payload | 265 | 0.10 | 0.07 | 0.54 | 0.28 | 15.47 | 19.66 |
| time_window_1pct | 100 | 0.02 | 0.01 | 0.35 | 0.16 | 15.91 | 17.56 |
| entity_lookup | 1751 | 0.03 | 0.02 | 0.29 | 0.15 | 16.81 | 18.67 |

Removing events (redaction). OE records a deletion vector; the other formats rewrite the whole file.

| Case | Events removed | oe (ms) | oe (bytes written) | oe (lazy open) (ms) | oe (lazy open) (bytes written) | parquet (ms) | parquet (bytes written) | vortex (ms) | vortex (bytes written) | json (ms) | json (bytes written) | json+zstd (ms) | json+zstd (bytes written) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| redact_1_event | 1 | 0.04 | 9 | 0.00 | 9 | 19.56 | 452866 | 53.65 | 473520 | 36.78 | 3268410 | 52.71 | 598999 |
| redact_1pct_of_events | 100 | 0.02 | 405 | 0.02 | 405 | 17.25 | 448727 | 65.14 | 466192 | 38.79 | 3235234 | 59.67 | 592934 |

## messages events: 100000 rows

| Format | Size (MiB) | Bytes per event | Size vs JSON | Write (ms) | Write (events/s) | Open (ms) | Open + first lookup (ms) |
|---|---:|---:|---:|---:|---:|---:|---:|
| oe | 8.58 | 89.9 | 27.6% | 454 | 220238 | 1.64 | 1.98 |
| oe (lazy open) | 8.58 | 89.9 | 27.6% | 480 | 208358 | 0.80 | 2.67 |
| parquet | 4.10 | 43.0 | 13.2% | 109 | 919161 | 0.06 | 3.12 |
| vortex | 4.64 | 48.6 | 14.9% | 234 | 428099 | 0.59 | 1.62 |
| json | 31.13 | 326.4 | 100.0% | 201 | 498202 | 0.00 | 159.39 |
| json+zstd | 5.83 | 61.1 | 18.7% | 296 | 337553 | 0.00 | 196.53 |

| Query (median ms; entity_lookup is per lookup) | Rows matched | oe | oe (lazy open) | parquet | vortex | json | json+zstd |
|---|---:|---:|---:|---:|---:|---:|---:|
| scan_all_columns | 100000 | 23.17 | 22.58 | 26.43 | 9.56 | 253.28 | 296.55 |
| scan_one_number | 100000 | 0.18 | 0.20 | 1.63 | 0.19 | 172.49 | 213.46 |
| scan_one_payload_number | 100000 | 0.14 | 0.15 | 1.37 | 0.14 | 175.09 | 193.83 |
| scan_one_payload_text | 100000 | 10.04 | 9.93 | 11.79 | 2.60 | 169.59 | 212.85 |
| filter_type_and_payload | 2655 | 0.82 | 0.81 | 4.49 | 0.47 | 170.41 | 264.53 |
| time_window_1pct | 1000 | 0.01 | 0.01 | 3.02 | 0.12 | 169.12 | 207.55 |
| entity_lookup | 1203 | 0.14 | 0.15 | 2.77 | 0.93 | 166.46 | 206.78 |

Removing events (redaction). OE records a deletion vector; the other formats rewrite the whole file.

| Case | Events removed | oe (ms) | oe (bytes written) | oe (lazy open) (ms) | oe (lazy open) (bytes written) | parquet (ms) | parquet (bytes written) | vortex (ms) | vortex (bytes written) | json (ms) | json (bytes written) | json+zstd (ms) | json+zstd (bytes written) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| redact_1_event | 1 | 0.00 | 9 | 0.00 | 9 | 176.31 | 4300907 | 257.67 | 4860768 | 439.83 | 32636819 | 677.63 | 6111549 |
| redact_1pct_of_events | 1000 | 1.15 | 4005 | 1.19 | 4005 | 186.94 | 4232767 | 317.54 | 4816112 | 441.41 | 32309495 | 604.43 | 6051823 |

## messages events: 1000000 rows

| Format | Size (MiB) | Bytes per event | Size vs JSON | Write (ms) | Write (events/s) | Open (ms) | Open + first lookup (ms) |
|---|---:|---:|---:|---:|---:|---:|---:|
| oe | 91.55 | 96.0 | 29.4% | 5517 | 181274 | 16.05 | 16.90 |
| oe (lazy open) | 91.55 | 96.0 | 29.4% | 5316 | 188111 | 11.04 | 23.70 |
| parquet | 41.65 | 43.7 | 13.4% | 1270 | 787270 | 0.15 | 31.27 |
| vortex | 47.32 | 49.6 | 15.2% | 1854 | 539370 | 0.82 | 11.82 |
| json | 311.21 | 326.3 | 100.0% | 1480 | 675617 | 8.27 | 1543.84 |
| json+zstd | 58.99 | 61.9 | 19.0% | 3319 | 301300 | 0.01 | 2153.08 |

| Query (median ms; entity_lookup is per lookup) | Rows matched | oe | oe (lazy open) | parquet | vortex | json | json+zstd |
|---|---:|---:|---:|---:|---:|---:|---:|
| scan_all_columns | 1000000 | 239.89 | 248.40 | 309.80 | 109.59 | 2948.01 | 3589.69 |
| scan_one_number | 1000000 | 1.85 | 1.98 | 16.60 | 1.67 | 1514.16 | 2085.97 |
| scan_one_payload_number | 1000000 | 1.41 | 1.50 | 14.40 | 1.55 | 1510.55 | 2119.63 |
| scan_one_payload_text | 1000000 | 114.90 | 110.75 | 126.13 | 28.84 | 1666.08 | 2374.67 |
| filter_type_and_payload | 26269 | 7.82 | 8.79 | 48.15 | 4.78 | 1693.60 | 2276.20 |
| time_window_1pct | 10000 | 0.04 | 0.04 | 4.06 | 0.72 | 1583.34 | 2176.45 |
| entity_lookup | 5411 | 2.12 | 1.72 | 32.32 | 10.27 | 1643.46 | 2254.25 |

Removing events (redaction). OE records a deletion vector; the other formats rewrite the whole file.

| Case | Events removed | oe (ms) | oe (bytes written) | oe (lazy open) (ms) | oe (lazy open) (bytes written) | parquet (ms) | parquet (bytes written) | vortex (ms) | vortex (bytes written) | json (ms) | json (bytes written) | json+zstd (ms) | json+zstd (bytes written) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| redact_1_event | 1 | 0.00 | 9 | 0.01 | 9 | 2249.71 | 43671434 | 2535.39 | 49602912 | 5684.62 | 326324891 | 7258.57 | 61854744 |
| redact_1pct_of_events | 10000 | 16.24 | 160005 | 26.56 | 160005 | 2200.50 | 43058471 | 2442.43 | 49314872 | 5753.55 | 323067360 | 7089.58 | 61243459 |

## chat events: 10000 rows

| Format | Size (MiB) | Bytes per event | Size vs JSON | Write (ms) | Write (events/s) | Open (ms) | Open + first lookup (ms) |
|---|---:|---:|---:|---:|---:|---:|---:|
| oe | 0.78 | 81.8 | 23.5% | 45 | 224316 | 0.21 | 0.39 |
| oe (lazy open) | 0.78 | 81.8 | 23.5% | 46 | 218691 | 0.12 | 0.37 |
| parquet | 0.36 | 37.3 | 10.7% | 12 | 864627 | 0.01 | 0.47 |
| vortex | 0.42 | 43.6 | 12.5% | 54 | 184600 | 0.57 | 0.96 |
| json | 3.32 | 348.4 | 100.0% | 15 | 668284 | 0.00 | 15.48 |
| json+zstd | 0.49 | 51.1 | 14.7% | 26 | 388907 | 0.00 | 20.28 |

| Query (median ms; entity_lookup is per lookup) | Rows matched | oe | oe (lazy open) | parquet | vortex | json | json+zstd |
|---|---:|---:|---:|---:|---:|---:|---:|
| scan_all_columns | 10000 | 2.13 | 2.15 | 2.60 | 1.07 | 21.24 | 26.42 |
| scan_one_number | 10000 | 0.03 | 0.03 | 0.16 | 0.03 | 15.34 | 19.53 |
| scan_one_payload_number | 10000 | 0.02 | 0.01 | 0.06 | 0.03 | 15.74 | 20.28 |
| scan_one_payload_text | 10000 | 0.93 | 0.97 | 1.06 | 0.32 | 16.51 | 20.46 |
| filter_type_and_payload | 266 | 0.07 | 0.09 | 0.36 | 0.21 | 15.72 | 19.97 |
| time_window_1pct | 100 | 0.02 | 0.02 | 0.23 | 0.11 | 15.32 | 18.95 |
| entity_lookup | 36977 | 0.08 | 0.09 | 0.34 | 0.17 | 15.40 | 18.87 |
| latest_50_in_room | 29391 | 0.08 | 0.09 | 0.52 | 0.28 | 16.03 | 19.53 |
| thread_replies | 21 | 0.08 | 0.08 | 0.35 | 0.30 | 15.21 | 22.09 |
| event_by_id | 20 | 0.01 | 0.01 | 0.52 | 0.19 | 22.64 | 21.77 |

Removing events (redaction). OE records a deletion vector; the other formats rewrite the whole file.

| Case | Events removed | oe (ms) | oe (bytes written) | oe (lazy open) (ms) | oe (lazy open) (bytes written) | parquet (ms) | parquet (bytes written) | vortex (ms) | vortex (bytes written) | json (ms) | json (bytes written) | json+zstd (ms) | json+zstd (bytes written) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| redact_1_event | 1 | 0.00 | 9 | 0.00 | 9 | 18.58 | 370690 | 59.89 | 433524 | 48.67 | 3483690 | 58.94 | 511242 |
| redact_1pct_of_events | 100 | 0.02 | 405 | 0.02 | 405 | 18.60 | 367096 | 61.29 | 434356 | 41.56 | 3450121 | 56.53 | 506512 |

## chat events: 100000 rows

| Format | Size (MiB) | Bytes per event | Size vs JSON | Write (ms) | Write (events/s) | Open (ms) | Open + first lookup (ms) |
|---|---:|---:|---:|---:|---:|---:|---:|
| oe | 8.03 | 84.2 | 24.1% | 475 | 210649 | 1.92 | 2.38 |
| oe (lazy open) | 8.03 | 84.2 | 24.1% | 447 | 223798 | 0.98 | 2.98 |
| parquet | 3.41 | 35.8 | 10.2% | 137 | 730386 | 0.06 | 3.59 |
| vortex | 4.18 | 43.9 | 12.5% | 285 | 350625 | 0.67 | 1.10 |
| json | 33.34 | 349.5 | 100.0% | 150 | 666907 | 0.00 | 173.30 |
| json+zstd | 5.10 | 53.5 | 15.3% | 261 | 383720 | 0.00 | 207.38 |

| Query (median ms; entity_lookup is per lookup) | Rows matched | oe | oe (lazy open) | parquet | vortex | json | json+zstd |
|---|---:|---:|---:|---:|---:|---:|---:|
| scan_all_columns | 100000 | 22.58 | 22.47 | 27.34 | 9.86 | 290.14 | 327.13 |
| scan_one_number | 100000 | 0.19 | 0.19 | 1.64 | 0.23 | 169.74 | 224.34 |
| scan_one_payload_number | 100000 | 0.13 | 0.13 | 0.38 | 0.18 | 177.36 | 210.07 |
| scan_one_payload_text | 100000 | 9.81 | 9.89 | 10.95 | 2.81 | 165.22 | 217.58 |
| filter_type_and_payload | 2904 | 0.63 | 0.69 | 3.57 | 0.45 | 162.18 | 231.16 |
| time_window_1pct | 1000 | 0.02 | 0.02 | 2.14 | 0.14 | 177.06 | 203.72 |
| entity_lookup | 81077 | 0.52 | 0.51 | 3.09 | 0.35 | 176.92 | 209.86 |
| latest_50_in_room | 64860 | 0.50 | 0.63 | 4.68 | 0.55 | 175.58 | 237.79 |
| thread_replies | 23 | 0.10 | 0.10 | 3.62 | 0.51 | 189.91 | 225.85 |
| event_by_id | 20 | 0.23 | 0.25 | 4.65 | 0.33 | 171.72 | 221.95 |

Removing events (redaction). OE records a deletion vector; the other formats rewrite the whole file.

| Case | Events removed | oe (ms) | oe (bytes written) | oe (lazy open) (ms) | oe (lazy open) (bytes written) | parquet (ms) | parquet (bytes written) | vortex (ms) | vortex (bytes written) | json (ms) | json (bytes written) | json+zstd (ms) | json+zstd (bytes written) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| redact_1_event | 1 | 0.00 | 9 | 0.00 | 9 | 264.51 | 3580849 | 317.06 | 4381044 | 481.43 | 34954335 | 770.32 | 5350605 |
| redact_1pct_of_events | 1000 | 1.06 | 4005 | 0.97 | 4005 | 207.62 | 3521126 | 318.52 | 4330708 | 458.77 | 34607634 | 728.34 | 5298958 |

## chat events: 1000000 rows

| Format | Size (MiB) | Bytes per event | Size vs JSON | Write (ms) | Write (events/s) | Open (ms) | Open + first lookup (ms) |
|---|---:|---:|---:|---:|---:|---:|---:|
| oe | 86.22 | 90.4 | 25.8% | 5775 | 173165 | 17.99 | 16.70 |
| oe (lazy open) | 86.22 | 90.4 | 25.8% | 6031 | 165814 | 10.01 | 23.68 |
| parquet | 35.96 | 37.7 | 10.8% | 1312 | 762074 | 0.16 | 30.37 |
| vortex | 45.86 | 48.1 | 13.7% | 2491 | 401406 | 1.13 | 3.13 |
| json | 334.22 | 350.5 | 100.0% | 1597 | 626167 | 8.93 | 1797.41 |
| json+zstd | 52.90 | 55.5 | 15.8% | 3005 | 332742 | 0.00 | 2274.13 |

| Query (median ms; entity_lookup is per lookup) | Rows matched | oe | oe (lazy open) | parquet | vortex | json | json+zstd |
|---|---:|---:|---:|---:|---:|---:|---:|
| scan_all_columns | 1000000 | 273.09 | 220.68 | 284.46 | 109.06 | 3193.64 | 3478.40 |
| scan_one_number | 1000000 | 2.89 | 2.02 | 17.65 | 2.13 | 1746.01 | 2144.29 |
| scan_one_payload_number | 1000000 | 2.01 | 1.41 | 4.39 | 1.82 | 1782.89 | 2138.60 |
| scan_one_payload_text | 1000000 | 100.66 | 93.71 | 116.19 | 36.40 | 1750.95 | 2365.67 |
| filter_type_and_payload | 30248 | 7.36 | 7.13 | 35.41 | 4.10 | 1758.07 | 2462.88 |
| time_window_1pct | 10000 | 0.05 | 0.05 | 2.73 | 0.92 | 1754.64 | 2171.95 |
| entity_lookup | 200587 | 2.10 | 2.07 | 29.61 | 2.44 | 1685.04 | 2366.13 |
| latest_50_in_room | 160250 | 3.19 | 2.05 | 56.49 | 2.61 | 1846.95 | 2420.43 |
| thread_replies | 22 | 0.71 | 0.41 | 26.96 | 3.81 | 1710.60 | 2175.86 |
| event_by_id | 20 | 0.55 | 0.29 | 6.19 | 1.03 | 1725.65 | 2241.56 |

Removing events (redaction). OE records a deletion vector; the other formats rewrite the whole file.

| Case | Events removed | oe (ms) | oe (bytes written) | oe (lazy open) (ms) | oe (lazy open) (bytes written) | parquet (ms) | parquet (bytes written) | vortex (ms) | vortex (bytes written) | json (ms) | json (bytes written) | json+zstd (ms) | json+zstd (bytes written) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| redact_1_event | 1 | 0.00 | 9 | 0.00 | 9 | 2387.13 | 37698174 | 3312.95 | 48035456 | 5753.74 | 350453613 | 7226.52 | 55469073 |
| redact_1pct_of_events | 10000 | 20.47 | 160005 | 21.46 | 160005 | 2448.90 | 37167023 | 3220.28 | 47241092 | 5724.38 | 346953322 | 7027.42 | 54925975 |

## Durable ingest

Each batch is acknowledged only after the file is synced to disk (300 batches per row). Parquet and Vortex cannot append, so each batch is a new file.

| Batch size | Format | Events/s | Ack p50 (ms) | Ack p99 (ms) | Bytes on disk |
|---:|---|---:|---:|---:|---:|
| 1 | oej journal | 3251 | 0.259 | 0.960 | 1228800 |
| 1 | json (append) | 3560 | 0.250 | 0.593 | 104659 |
| 1 | parquet (file per batch) | 872 | 1.109 | 1.812 | 1094080 |
| 1 | vortex (file per batch) | 468 | 2.092 | 3.044 | 3131136 |
| 16 | oej journal | 35567 | 0.372 | 2.207 | 2457600 |
| 16 | json (append) | 24742 | 0.567 | 1.399 | 1664629 |
| 16 | parquet (file per batch) | 12863 | 1.153 | 2.443 | 1373373 |
| 16 | vortex (file per batch) | 888 | 17.035 | 26.442 | 4804864 |
| 128 | oej journal | 111775 | 1.109 | 2.275 | 19660800 |
| 128 | json (append) | 169559 | 0.734 | 1.172 | 13400243 |
| 128 | parquet (file per batch) | 73443 | 1.710 | 2.679 | 2851044 |
| 128 | vortex (file per batch) | 5654 | 21.735 | 31.071 | 7595608 |

