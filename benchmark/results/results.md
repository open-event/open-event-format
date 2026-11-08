# Benchmark results

Machine: Intel(R) Xeon(R) Processor @ 2.10GHz (4 logical CPUs), Linux 6.18.44-fc-v310, rustc 1.96.0 (ac68faa20 2026-05-25)

Every format stores the same events. Query times are the median of 5 runs after one warm-up; write times are the median of 3 runs. All queries run on a single thread against the file held in memory. Every format returned identical results for every query (checked on each run).

## telemetry events: 10000 rows

| Format | Size (MiB) | Bytes per event | Size vs JSON | Write (ms) | Write (events/s) | Open (ms) | Open + first lookup (ms) |
|---|---:|---:|---:|---:|---:|---:|---:|
| oe | 0.47 | 49.5 | 20.2% | 31 | 327194 | 0.20 | 0.29 |
| oe (lazy open) | 0.47 | 49.5 | 20.2% | 27 | 371046 | 0.10 | 0.26 |
| parquet | 0.13 | 13.7 | 5.6% | 4 | 2348736 | 0.01 | 0.29 |
| vortex | 0.16 | 16.6 | 6.8% | 26 | 380013 | 0.46 | 0.74 |
| json | 2.34 | 245.1 | 100.0% | 9 | 1061026 | 0.00 | 11.10 |
| json+zstd | 0.22 | 23.4 | 9.5% | 16 | 618497 | 0.00 | 16.02 |

| Query (median ms; entity_lookup is per lookup) | Rows matched | oe | oe (lazy open) | parquet | vortex | json | json+zstd |
|---|---:|---:|---:|---:|---:|---:|---:|
| scan_all_columns | 10000 | 2.13 | 1.81 | 1.17 | 0.79 | 14.77 | 20.32 |
| scan_one_number | 10000 | 0.02 | 0.01 | 0.14 | 0.03 | 10.49 | 15.60 |
| scan_one_payload_number | 10000 | 0.09 | 0.07 | 0.15 | 0.09 | 11.10 | 15.61 |
| scan_one_payload_text | 10000 | 0.07 | 0.06 | 0.08 | 0.05 | 12.06 | 16.04 |
| filter_type_and_payload | 12 | 0.08 | 0.07 | 0.38 | 0.25 | 14.80 | 16.68 |
| time_window_1pct | 100 | 0.02 | 0.02 | 0.26 | 0.09 | 13.98 | 15.65 |
| entity_lookup | 820 | 0.11 | 0.09 | 0.25 | 0.11 | 13.96 | 15.96 |

Removing events (redaction). OE records a deletion vector; the other formats rewrite the whole file.

| Case | Events removed | oe (ms) | oe (bytes written) | oe (lazy open) (ms) | oe (lazy open) (bytes written) | parquet (ms) | parquet (bytes written) | vortex (ms) | vortex (bytes written) | json (ms) | json (bytes written) | json+zstd (ms) | json+zstd (bytes written) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| redact_1_event | 1 | 0.00 | 9 | 0.00 | 9 | 7.98 | 134711 | 30.01 | 165968 | 34.33 | 2450649 | 40.25 | 233602 |
| redact_1pct_of_events | 100 | 0.03 | 405 | 0.02 | 405 | 8.06 | 133804 | 29.90 | 165504 | 34.37 | 2426385 | 40.93 | 231239 |

## telemetry events: 100000 rows

| Format | Size (MiB) | Bytes per event | Size vs JSON | Write (ms) | Write (events/s) | Open (ms) | Open + first lookup (ms) |
|---|---:|---:|---:|---:|---:|---:|---:|
| oe | 5.50 | 57.7 | 23.6% | 365 | 274166 | 1.58 | 2.39 |
| oe (lazy open) | 5.50 | 57.7 | 23.6% | 333 | 300471 | 0.95 | 2.15 |
| parquet | 1.11 | 11.6 | 4.7% | 43 | 2314452 | 0.03 | 2.87 |
| vortex | 1.76 | 18.5 | 7.5% | 110 | 907307 | 0.50 | 1.38 |
| json | 23.36 | 245.0 | 100.0% | 138 | 723116 | 0.00 | 157.55 |
| json+zstd | 2.36 | 24.7 | 10.1% | 181 | 552799 | 0.00 | 152.59 |

| Query (median ms; entity_lookup is per lookup) | Rows matched | oe | oe (lazy open) | parquet | vortex | json | json+zstd |
|---|---:|---:|---:|---:|---:|---:|---:|
| scan_all_columns | 100000 | 28.95 | 20.52 | 11.92 | 6.12 | 215.49 | 221.12 |
| scan_one_number | 100000 | 0.21 | 0.15 | 1.61 | 0.17 | 139.80 | 165.50 |
| scan_one_payload_number | 100000 | 0.91 | 0.79 | 1.08 | 0.73 | 141.00 | 136.94 |
| scan_one_payload_text | 100000 | 0.71 | 0.63 | 0.76 | 0.49 | 148.05 | 165.87 |
| filter_type_and_payload | 143 | 0.84 | 0.77 | 3.46 | 0.28 | 156.54 | 147.56 |
| time_window_1pct | 1000 | 0.05 | 0.04 | 2.77 | 0.11 | 149.21 | 137.89 |
| entity_lookup | 834 | 0.61 | 0.54 | 2.67 | 0.87 | 136.53 | 142.22 |

Removing events (redaction). OE records a deletion vector; the other formats rewrite the whole file.

| Case | Events removed | oe (ms) | oe (bytes written) | oe (lazy open) (ms) | oe (lazy open) (bytes written) | parquet (ms) | parquet (bytes written) | vortex (ms) | vortex (bytes written) | json (ms) | json (bytes written) | json+zstd (ms) | json+zstd (bytes written) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| redact_1_event | 1 | 0.00 | 9 | 0.00 | 9 | 92.35 | 1162107 | 158.97 | 1846136 | 359.53 | 24497827 | 451.14 | 2470339 |
| redact_1pct_of_events | 1000 | 1.00 | 4005 | 1.04 | 4005 | 82.84 | 1129279 | 163.54 | 1834576 | 314.67 | 24253096 | 424.38 | 2446064 |

## telemetry events: 1000000 rows

| Format | Size (MiB) | Bytes per event | Size vs JSON | Write (ms) | Write (events/s) | Open (ms) | Open + first lookup (ms) |
|---|---:|---:|---:|---:|---:|---:|---:|
| oe | 58.80 | 61.7 | 25.2% | 3069 | 325821 | 9.42 | 12.38 |
| oe (lazy open) | 58.80 | 61.7 | 25.2% | 3248 | 307875 | 7.12 | 17.39 |
| parquet | 11.73 | 12.3 | 5.0% | 485 | 2063456 | 0.12 | 33.77 |
| vortex | 18.63 | 19.5 | 8.0% | 1052 | 950758 | 0.68 | 6.70 |
| json | 233.58 | 244.9 | 100.0% | 1225 | 816377 | 6.96 | 1380.69 |
| json+zstd | 24.21 | 25.4 | 10.4% | 1845 | 542024 | 0.00 | 1688.55 |

| Query (median ms; entity_lookup is per lookup) | Rows matched | oe | oe (lazy open) | parquet | vortex | json | json+zstd |
|---|---:|---:|---:|---:|---:|---:|---:|
| scan_all_columns | 1000000 | 206.15 | 218.78 | 89.97 | 78.33 | 2214.04 | 2449.45 |
| scan_one_number | 1000000 | 1.98 | 1.91 | 12.83 | 1.76 | 1387.43 | 1620.56 |
| scan_one_payload_number | 1000000 | 9.41 | 9.48 | 8.59 | 8.67 | 1410.60 | 1539.12 |
| scan_one_payload_text | 1000000 | 7.15 | 7.37 | 6.10 | 4.45 | 1446.77 | 1698.81 |
| filter_type_and_payload | 1237 | 9.02 | 9.74 | 26.85 | 2.29 | 1477.55 | 1732.54 |
| time_window_1pct | 10000 | 0.12 | 0.12 | 2.17 | 0.49 | 1426.11 | 1773.31 |
| entity_lookup | 837 | 2.62 | 2.93 | 29.41 | 6.11 | 1359.29 | 1767.58 |

Removing events (redaction). OE records a deletion vector; the other formats rewrite the whole file.

| Case | Events removed | oe (ms) | oe (bytes written) | oe (lazy open) (ms) | oe (lazy open) (bytes written) | parquet (ms) | parquet (bytes written) | vortex (ms) | vortex (bytes written) | json (ms) | json (bytes written) | json+zstd (ms) | json+zstd (bytes written) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| redact_1_event | 1 | 0.00 | 9 | 0.00 | 9 | 950.22 | 12309274 | 1569.27 | 19543608 | 3663.05 | 244921417 | 4995.76 | 25386110 |
| redact_1pct_of_events | 10000 | 14.09 | 160005 | 13.97 | 160005 | 1027.79 | 12020757 | 1634.10 | 19543040 | 3732.50 | 242472519 | 5048.27 | 25138782 |

## messages events: 10000 rows

| Format | Size (MiB) | Bytes per event | Size vs JSON | Write (ms) | Write (events/s) | Open (ms) | Open + first lookup (ms) |
|---|---:|---:|---:|---:|---:|---:|---:|
| oe | 0.82 | 86.1 | 26.3% | 53 | 190219 | 0.40 | 0.51 |
| oe (lazy open) | 0.82 | 86.1 | 26.3% | 47 | 214034 | 0.14 | 0.28 |
| parquet | 0.43 | 45.5 | 13.9% | 15 | 671297 | 0.02 | 0.41 |
| vortex | 0.45 | 47.0 | 14.4% | 50 | 200799 | 0.66 | 1.05 |
| json | 3.12 | 326.9 | 100.0% | 12 | 813658 | 0.00 | 16.38 |
| json+zstd | 0.57 | 59.9 | 18.3% | 20 | 490848 | 0.00 | 15.38 |

| Query (median ms; entity_lookup is per lookup) | Rows matched | oe | oe (lazy open) | parquet | vortex | json | json+zstd |
|---|---:|---:|---:|---:|---:|---:|---:|
| scan_all_columns | 10000 | 6.45 | 4.09 | 3.12 | 1.21 | 20.02 | 21.52 |
| scan_one_number | 10000 | 0.03 | 0.02 | 0.17 | 0.04 | 13.43 | 14.93 |
| scan_one_payload_number | 10000 | 0.02 | 0.01 | 0.22 | 0.04 | 13.93 | 16.86 |
| scan_one_payload_text | 10000 | 2.68 | 1.67 | 1.36 | 0.35 | 15.44 | 18.12 |
| filter_type_and_payload | 265 | 0.14 | 0.08 | 0.62 | 0.41 | 18.57 | 17.84 |
| time_window_1pct | 100 | 0.03 | 0.01 | 0.38 | 0.22 | 17.61 | 16.66 |
| entity_lookup | 1751 | 0.11 | 0.07 | 0.34 | 0.13 | 15.27 | 18.65 |

Removing events (redaction). OE records a deletion vector; the other formats rewrite the whole file.

| Case | Events removed | oe (ms) | oe (bytes written) | oe (lazy open) (ms) | oe (lazy open) (bytes written) | parquet (ms) | parquet (bytes written) | vortex (ms) | vortex (bytes written) | json (ms) | json (bytes written) | json+zstd (ms) | json+zstd (bytes written) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| redact_1_event | 1 | 0.00 | 9 | 0.00 | 9 | 17.94 | 452866 | 57.12 | 473520 | 39.20 | 3268410 | 55.97 | 598999 |
| redact_1pct_of_events | 100 | 0.04 | 405 | 0.02 | 405 | 18.77 | 448727 | 51.94 | 466192 | 30.88 | 3235234 | 55.97 | 592934 |

## messages events: 100000 rows

| Format | Size (MiB) | Bytes per event | Size vs JSON | Write (ms) | Write (events/s) | Open (ms) | Open + first lookup (ms) |
|---|---:|---:|---:|---:|---:|---:|---:|
| oe | 8.95 | 93.8 | 28.7% | 409 | 244546 | 1.47 | 3.00 |
| oe (lazy open) | 8.95 | 93.8 | 28.7% | 402 | 248992 | 0.91 | 2.37 |
| parquet | 4.10 | 43.0 | 13.2% | 115 | 869416 | 0.09 | 3.19 |
| vortex | 4.64 | 48.6 | 14.9% | 226 | 441937 | 0.64 | 1.79 |
| json | 31.13 | 326.4 | 100.0% | 122 | 817066 | 0.00 | 145.65 |
| json+zstd | 5.83 | 61.1 | 18.7% | 272 | 367444 | 0.00 | 199.00 |

| Query (median ms; entity_lookup is per lookup) | Rows matched | oe | oe (lazy open) | parquet | vortex | json | json+zstd |
|---|---:|---:|---:|---:|---:|---:|---:|
| scan_all_columns | 100000 | 42.48 | 32.71 | 24.69 | 10.37 | 237.21 | 273.34 |
| scan_one_number | 100000 | 0.18 | 0.13 | 1.34 | 0.19 | 136.23 | 178.66 |
| scan_one_payload_number | 100000 | 0.13 | 0.10 | 1.12 | 0.17 | 143.98 | 193.19 |
| scan_one_payload_text | 100000 | 15.79 | 15.87 | 10.23 | 2.88 | 143.65 | 211.48 |
| filter_type_and_payload | 2655 | 0.81 | 0.81 | 4.70 | 0.56 | 166.36 | 171.23 |
| time_window_1pct | 1000 | 0.01 | 0.01 | 3.17 | 0.12 | 159.22 | 201.82 |
| entity_lookup | 1203 | 0.62 | 0.60 | 2.90 | 1.00 | 157.14 | 198.27 |

Removing events (redaction). OE records a deletion vector; the other formats rewrite the whole file.

| Case | Events removed | oe (ms) | oe (bytes written) | oe (lazy open) (ms) | oe (lazy open) (bytes written) | parquet (ms) | parquet (bytes written) | vortex (ms) | vortex (bytes written) | json (ms) | json (bytes written) | json+zstd (ms) | json+zstd (bytes written) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| redact_1_event | 1 | 0.00 | 9 | 0.00 | 9 | 194.21 | 4300907 | 279.68 | 4860768 | 400.92 | 32636819 | 543.99 | 6111549 |
| redact_1pct_of_events | 1000 | 1.25 | 4005 | 1.18 | 4005 | 179.56 | 4232767 | 271.90 | 4816112 | 423.41 | 32309495 | 640.10 | 6051823 |

## messages events: 1000000 rows

| Format | Size (MiB) | Bytes per event | Size vs JSON | Write (ms) | Write (events/s) | Open (ms) | Open + first lookup (ms) |
|---|---:|---:|---:|---:|---:|---:|---:|
| oe | 92.89 | 97.4 | 29.8% | 4338 | 230495 | 13.97 | 13.87 |
| oe (lazy open) | 92.89 | 97.4 | 29.8% | 4258 | 234867 | 11.69 | 21.73 |
| parquet | 41.65 | 43.7 | 13.4% | 1048 | 954332 | 0.12 | 24.81 |
| vortex | 47.32 | 49.6 | 15.2% | 1841 | 543227 | 0.72 | 8.46 |
| json | 311.21 | 326.3 | 100.0% | 1218 | 821161 | 6.47 | 1376.23 |
| json+zstd | 58.99 | 61.9 | 19.0% | 2673 | 374161 | 0.00 | 2104.91 |

| Query (median ms; entity_lookup is per lookup) | Rows matched | oe | oe (lazy open) | parquet | vortex | json | json+zstd |
|---|---:|---:|---:|---:|---:|---:|---:|
| scan_all_columns | 1000000 | 431.50 | 338.88 | 235.61 | 108.31 | 2853.24 | 3290.83 |
| scan_one_number | 1000000 | 1.89 | 1.90 | 13.16 | 1.85 | 1414.45 | 1981.21 |
| scan_one_payload_number | 1000000 | 1.55 | 1.41 | 11.56 | 1.67 | 1528.73 | 1688.67 |
| scan_one_payload_text | 1000000 | 157.63 | 156.86 | 107.08 | 29.00 | 1673.17 | 2113.40 |
| filter_type_and_payload | 26269 | 6.58 | 8.25 | 37.80 | 3.41 | 1539.44 | 1937.11 |
| time_window_1pct | 10000 | 0.03 | 0.04 | 3.26 | 0.44 | 1601.73 | 2069.07 |
| entity_lookup | 5411 | 1.76 | 2.33 | 24.03 | 7.34 | 1514.46 | 2005.79 |

Removing events (redaction). OE records a deletion vector; the other formats rewrite the whole file.

| Case | Events removed | oe (ms) | oe (bytes written) | oe (lazy open) (ms) | oe (lazy open) (bytes written) | parquet (ms) | parquet (bytes written) | vortex (ms) | vortex (bytes written) | json (ms) | json (bytes written) | json+zstd (ms) | json+zstd (bytes written) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| redact_1_event | 1 | 0.00 | 9 | 0.00 | 9 | 1746.69 | 43671434 | 2368.89 | 49602912 | 4558.92 | 326324891 | 6220.66 | 61854744 |
| redact_1pct_of_events | 10000 | 13.93 | 160005 | 14.18 | 160005 | 1932.93 | 43058471 | 2396.40 | 49314872 | 4686.28 | 323067360 | 6272.62 | 61243459 |

## chat events: 10000 rows

| Format | Size (MiB) | Bytes per event | Size vs JSON | Write (ms) | Write (events/s) | Open (ms) | Open + first lookup (ms) |
|---|---:|---:|---:|---:|---:|---:|---:|
| oe | 0.79 | 82.8 | 23.8% | 38 | 260101 | 0.23 | 0.41 |
| oe (lazy open) | 0.79 | 82.8 | 23.8% | 42 | 237075 | 0.10 | 0.29 |
| parquet | 0.36 | 37.3 | 10.7% | 9 | 1159826 | 0.01 | 0.42 |
| vortex | 0.42 | 43.6 | 12.5% | 41 | 243963 | 0.47 | 0.80 |
| json | 3.32 | 348.4 | 100.0% | 12 | 812339 | 0.00 | 13.48 |
| json+zstd | 0.49 | 51.1 | 14.7% | 20 | 511036 | 0.00 | 16.52 |

| Query (median ms; entity_lookup is per lookup) | Rows matched | oe | oe (lazy open) | parquet | vortex | json | json+zstd |
|---|---:|---:|---:|---:|---:|---:|---:|
| scan_all_columns | 10000 | 4.94 | 3.13 | 2.25 | 0.93 | 17.57 | 21.39 |
| scan_one_number | 10000 | 0.03 | 0.02 | 0.13 | 0.03 | 11.96 | 15.69 |
| scan_one_payload_number | 10000 | 0.01 | 0.01 | 0.05 | 0.03 | 11.87 | 15.30 |
| scan_one_payload_text | 10000 | 1.46 | 1.17 | 0.86 | 0.33 | 12.62 | 16.59 |
| filter_type_and_payload | 266 | 0.07 | 0.06 | 0.31 | 0.17 | 13.49 | 15.47 |
| time_window_1pct | 100 | 0.02 | 0.01 | 0.19 | 0.09 | 12.37 | 14.74 |
| entity_lookup | 36977 | 0.08 | 0.06 | 0.27 | 0.16 | 13.20 | 16.01 |
| latest_50_in_room | 29391 | 0.09 | 0.06 | 0.45 | 0.23 | 13.66 | 17.27 |
| thread_replies | 21 | 0.08 | 0.07 | 0.29 | 0.17 | 12.11 | 16.73 |
| event_by_id | 20 | 0.01 | 0.01 | 0.44 | 0.17 | 12.51 | 18.99 |

Removing events (redaction). OE records a deletion vector; the other formats rewrite the whole file.

| Case | Events removed | oe (ms) | oe (bytes written) | oe (lazy open) (ms) | oe (lazy open) (bytes written) | parquet (ms) | parquet (bytes written) | vortex (ms) | vortex (bytes written) | json (ms) | json (bytes written) | json+zstd (ms) | json+zstd (bytes written) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| redact_1_event | 1 | 0.00 | 9 | 0.00 | 9 | 16.60 | 370690 | 48.09 | 433524 | 32.39 | 3483690 | 46.53 | 511242 |
| redact_1pct_of_events | 100 | 0.02 | 405 | 0.01 | 405 | 16.77 | 367096 | 49.03 | 434356 | 34.06 | 3450121 | 48.57 | 506512 |

## chat events: 100000 rows

| Format | Size (MiB) | Bytes per event | Size vs JSON | Write (ms) | Write (events/s) | Open (ms) | Open + first lookup (ms) |
|---|---:|---:|---:|---:|---:|---:|---:|
| oe | 8.13 | 85.3 | 24.4% | 403 | 247902 | 1.90 | 2.74 |
| oe (lazy open) | 8.13 | 85.3 | 24.4% | 422 | 236751 | 1.16 | 5.68 |
| parquet | 3.41 | 35.8 | 10.2% | 104 | 959731 | 0.07 | 3.17 |
| vortex | 4.18 | 43.9 | 12.5% | 260 | 384347 | 0.65 | 1.09 |
| json | 33.34 | 349.5 | 100.0% | 122 | 819959 | 0.00 | 154.80 |
| json+zstd | 5.10 | 53.5 | 15.3% | 236 | 422927 | 0.00 | 173.95 |

| Query (median ms; entity_lookup is per lookup) | Rows matched | oe | oe (lazy open) | parquet | vortex | json | json+zstd |
|---|---:|---:|---:|---:|---:|---:|---:|
| scan_all_columns | 100000 | 40.87 | 53.29 | 25.65 | 10.39 | 215.99 | 293.54 |
| scan_one_number | 100000 | 0.16 | 0.26 | 1.62 | 0.18 | 134.56 | 196.56 |
| scan_one_payload_number | 100000 | 0.12 | 0.20 | 0.41 | 0.20 | 130.80 | 164.52 |
| scan_one_payload_text | 100000 | 11.90 | 14.50 | 10.36 | 2.94 | 156.04 | 172.94 |
| filter_type_and_payload | 2904 | 0.54 | 0.63 | 3.60 | 0.66 | 138.68 | 170.00 |
| time_window_1pct | 1000 | 0.01 | 0.02 | 2.09 | 0.20 | 157.31 | 166.80 |
| entity_lookup | 81077 | 1.07 | 1.04 | 2.93 | 0.40 | 140.56 | 204.61 |
| latest_50_in_room | 64860 | 1.13 | 1.03 | 4.47 | 0.63 | 152.52 | 213.43 |
| thread_replies | 23 | 0.11 | 0.10 | 3.52 | 0.59 | 159.10 | 201.75 |
| event_by_id | 20 | 0.26 | 0.22 | 4.40 | 0.33 | 162.52 | 206.14 |

Removing events (redaction). OE records a deletion vector; the other formats rewrite the whole file.

| Case | Events removed | oe (ms) | oe (bytes written) | oe (lazy open) (ms) | oe (lazy open) (bytes written) | parquet (ms) | parquet (bytes written) | vortex (ms) | vortex (bytes written) | json (ms) | json (bytes written) | json+zstd (ms) | json+zstd (bytes written) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| redact_1_event | 1 | 0.00 | 9 | 0.00 | 9 | 190.23 | 3580849 | 316.44 | 4381044 | 375.49 | 34954335 | 572.80 | 5350605 |
| redact_1pct_of_events | 1000 | 0.85 | 4005 | 0.74 | 4005 | 169.20 | 3521126 | 317.18 | 4330708 | 368.16 | 34607634 | 601.94 | 5298958 |

## chat events: 1000000 rows

| Format | Size (MiB) | Bytes per event | Size vs JSON | Write (ms) | Write (events/s) | Open (ms) | Open + first lookup (ms) |
|---|---:|---:|---:|---:|---:|---:|---:|
| oe | 91.03 | 95.5 | 27.2% | 4246 | 235520 | 14.91 | 13.32 |
| oe (lazy open) | 91.03 | 95.5 | 27.2% | 4223 | 236821 | 9.95 | 20.92 |
| parquet | 35.96 | 37.7 | 10.8% | 1188 | 841758 | 0.16 | 29.74 |
| vortex | 45.86 | 48.1 | 13.7% | 2180 | 458741 | 0.92 | 3.51 |
| json | 334.22 | 350.5 | 100.0% | 1405 | 711594 | 6.70 | 1552.58 |
| json+zstd | 52.90 | 55.5 | 15.8% | 2677 | 373572 | 0.00 | 2002.26 |

| Query (median ms; entity_lookup is per lookup) | Rows matched | oe | oe (lazy open) | parquet | vortex | json | json+zstd |
|---|---:|---:|---:|---:|---:|---:|---:|
| scan_all_columns | 1000000 | 397.34 | 367.51 | 222.84 | 106.73 | 2748.93 | 3193.52 |
| scan_one_number | 1000000 | 1.68 | 1.98 | 16.58 | 2.06 | 1547.53 | 1952.85 |
| scan_one_payload_number | 1000000 | 1.14 | 1.32 | 4.07 | 1.61 | 1559.81 | 1953.41 |
| scan_one_payload_text | 1000000 | 118.46 | 144.65 | 113.14 | 28.92 | 1616.25 | 2059.61 |
| filter_type_and_payload | 30248 | 5.00 | 6.45 | 35.51 | 3.07 | 1452.72 | 2085.79 |
| time_window_1pct | 10000 | 0.04 | 0.05 | 2.78 | 0.56 | 1599.35 | 1935.49 |
| entity_lookup | 200587 | 5.79 | 7.31 | 30.43 | 2.00 | 1552.57 | 1934.89 |
| latest_50_in_room | 160250 | 6.00 | 7.50 | 47.17 | 2.56 | 1669.15 | 2139.66 |
| thread_replies | 22 | 0.28 | 0.34 | 17.65 | 4.01 | 1596.23 | 2035.62 |
| event_by_id | 20 | 0.19 | 0.25 | 5.18 | 1.15 | 1591.77 | 2089.92 |

Removing events (redaction). OE records a deletion vector; the other formats rewrite the whole file.

| Case | Events removed | oe (ms) | oe (bytes written) | oe (lazy open) (ms) | oe (lazy open) (bytes written) | parquet (ms) | parquet (bytes written) | vortex (ms) | vortex (bytes written) | json (ms) | json (bytes written) | json+zstd (ms) | json+zstd (bytes written) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| redact_1_event | 1 | 0.00 | 9 | 0.00 | 9 | 2007.53 | 37698174 | 2861.48 | 48035456 | 4972.02 | 350453613 | 6499.48 | 55469073 |
| redact_1pct_of_events | 10000 | 12.87 | 160005 | 14.55 | 160005 | 2145.08 | 37167023 | 2916.68 | 47241092 | 4772.44 | 346953322 | 6579.66 | 54925975 |

## Durable ingest

Each batch is acknowledged only after the file is synced to disk (300 batches per row). Parquet and Vortex cannot append, so each batch is a new file.

| Batch size | Format | Events/s | Ack p50 (ms) | Ack p99 (ms) | Bytes on disk |
|---:|---|---:|---:|---:|---:|
| 1 | oej journal | 5284 | 0.171 | 0.414 | 1228800 |
| 1 | json (append) | 6136 | 0.140 | 0.395 | 104659 |
| 1 | parquet (file per batch) | 1122 | 0.849 | 1.437 | 1094080 |
| 1 | vortex (file per batch) | 533 | 1.746 | 2.901 | 3131136 |
| 16 | oej journal | 52730 | 0.267 | 0.837 | 2457600 |
| 16 | json (append) | 74309 | 0.200 | 0.440 | 1664629 |
| 16 | parquet (file per batch) | 19840 | 0.791 | 1.172 | 1373373 |
| 16 | vortex (file per batch) | 1241 | 12.344 | 20.608 | 4804864 |
| 128 | oej journal | 172879 | 0.702 | 1.322 | 19660800 |
| 128 | json (append) | 174796 | 0.694 | 1.417 | 13400243 |
| 128 | parquet (file per batch) | 95335 | 1.337 | 1.843 | 2851044 |
| 128 | vortex (file per batch) | 6756 | 19.271 | 25.828 | 7595608 |

