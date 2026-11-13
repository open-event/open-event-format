# Benchmark results

Machine: Intel(R) Xeon(R) Processor @ 2.10GHz (4 logical CPUs), Linux 6.18.44-fc-v445, rustc 1.96.0 (ac68faa20 2026-05-25)

Every format stores the same events. Query times are the median of 5 runs after one warm-up; write times are the median of 3 runs. All queries run on a single thread against the file held in memory. Every format returned identical results for every query (checked on each run).

## telemetry events: 10000 rows

| Format | Size (MiB) | Bytes per event | Size vs JSON | Write (ms) | Write (events/s) | Open (ms) | Open + first lookup (ms) |
|---|---:|---:|---:|---:|---:|---:|---:|
| oe | 0.17 | 17.7 | 7.2% | 29 | 341607 | 0.16 | 0.15 |
| oe (lazy open) | 0.17 | 17.7 | 7.2% | 26 | 384793 | 0.08 | 0.11 |
| parquet | 0.13 | 13.7 | 5.6% | 4 | 2480888 | 0.01 | 0.26 |
| vortex | 0.16 | 16.6 | 6.8% | 27 | 371158 | 0.39 | 0.77 |
| json | 2.34 | 245.1 | 100.0% | 10 | 1026656 | 0.00 | 11.39 |
| json+zstd | 0.22 | 23.4 | 9.5% | 13 | 769142 | 0.00 | 12.28 |

| Query (median ms; entity_lookup is per lookup) | Rows matched | oe | oe (lazy open) | parquet | vortex | json | json+zstd |
|---|---:|---:|---:|---:|---:|---:|---:|
| scan_all_columns | 10000 | 0.64 | 0.52 | 1.21 | 0.84 | 15.52 | 15.95 |
| scan_one_number | 10000 | 0.02 | 0.02 | 0.14 | 0.03 | 10.88 | 11.98 |
| scan_one_payload_number | 10000 | 0.10 | 0.07 | 0.14 | 0.12 | 10.96 | 15.41 |
| scan_one_payload_text | 10000 | 0.07 | 0.06 | 0.08 | 0.05 | 11.49 | 16.39 |
| filter_type_and_payload | 12 | 0.08 | 0.07 | 0.37 | 0.26 | 12.44 | 19.01 |
| time_window_1pct | 100 | 0.02 | 0.02 | 0.23 | 0.09 | 11.04 | 15.89 |
| entity_lookup | 820 | 0.02 | 0.01 | 0.24 | 0.10 | 11.04 | 16.29 |

Removing events (redaction). OE records a deletion vector; the other formats rewrite the whole file.

| Case | Events removed | oe (ms) | oe (bytes written) | oe (lazy open) (ms) | oe (lazy open) (bytes written) | parquet (ms) | parquet (bytes written) | vortex (ms) | vortex (bytes written) | json (ms) | json (bytes written) | json+zstd (ms) | json+zstd (bytes written) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| redact_1_event | 1 | 0.00 | 9 | 0.00 | 9 | 8.38 | 134711 | 31.09 | 165968 | 26.87 | 2450649 | 41.78 | 233602 |
| redact_1pct_of_events | 100 | 0.02 | 405 | 0.02 | 405 | 8.49 | 133804 | 31.00 | 165504 | 26.28 | 2426385 | 61.87 | 231239 |

## telemetry events: 100000 rows

| Format | Size (MiB) | Bytes per event | Size vs JSON | Write (ms) | Write (events/s) | Open (ms) | Open + first lookup (ms) |
|---|---:|---:|---:|---:|---:|---:|---:|
| oe | 1.55 | 16.2 | 6.6% | 269 | 371193 | 1.05 | 1.19 |
| oe (lazy open) | 1.55 | 16.2 | 6.6% | 259 | 386222 | 0.72 | 0.94 |
| parquet | 1.11 | 11.6 | 4.7% | 57 | 1769344 | 0.04 | 3.05 |
| vortex | 1.76 | 18.5 | 7.5% | 121 | 827606 | 0.57 | 1.58 |
| json | 23.36 | 245.0 | 100.0% | 125 | 802040 | 0.00 | 150.68 |
| json+zstd | 2.36 | 24.7 | 10.1% | 194 | 514903 | 0.00 | 178.28 |

| Query (median ms; entity_lookup is per lookup) | Rows matched | oe | oe (lazy open) | parquet | vortex | json | json+zstd |
|---|---:|---:|---:|---:|---:|---:|---:|
| scan_all_columns | 100000 | 6.26 | 6.40 | 13.59 | 7.26 | 226.72 | 225.03 |
| scan_one_number | 100000 | 0.17 | 0.17 | 1.54 | 0.22 | 136.26 | 172.52 |
| scan_one_payload_number | 100000 | 0.94 | 0.90 | 1.10 | 0.79 | 140.46 | 176.48 |
| scan_one_payload_text | 100000 | 0.71 | 0.71 | 0.85 | 0.55 | 146.09 | 178.85 |
| filter_type_and_payload | 143 | 0.88 | 0.88 | 3.51 | 0.30 | 155.96 | 186.71 |
| time_window_1pct | 1000 | 0.05 | 0.05 | 2.00 | 0.15 | 159.69 | 173.92 |
| entity_lookup | 834 | 0.12 | 0.13 | 2.72 | 0.88 | 145.68 | 181.24 |

Removing events (redaction). OE records a deletion vector; the other formats rewrite the whole file.

| Case | Events removed | oe (ms) | oe (bytes written) | oe (lazy open) (ms) | oe (lazy open) (bytes written) | parquet (ms) | parquet (bytes written) | vortex (ms) | vortex (bytes written) | json (ms) | json (bytes written) | json+zstd (ms) | json+zstd (bytes written) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| redact_1_event | 1 | 0.00 | 9 | 0.00 | 9 | 118.48 | 1162107 | 186.41 | 1846136 | 361.39 | 24497827 | 481.14 | 2470339 |
| redact_1pct_of_events | 1000 | 0.80 | 4005 | 1.08 | 4005 | 117.70 | 1129279 | 205.12 | 1834576 | 368.51 | 24253096 | 450.61 | 2446064 |

## telemetry events: 1000000 rows

| Format | Size (MiB) | Bytes per event | Size vs JSON | Write (ms) | Write (events/s) | Open (ms) | Open + first lookup (ms) |
|---|---:|---:|---:|---:|---:|---:|---:|
| oe | 16.22 | 17.0 | 6.9% | 2753 | 363219 | 9.96 | 9.50 |
| oe (lazy open) | 16.22 | 17.0 | 6.9% | 2667 | 374907 | 8.77 | 12.47 |
| parquet | 11.73 | 12.3 | 5.0% | 518 | 1929187 | 0.13 | 29.51 |
| vortex | 18.63 | 19.5 | 8.0% | 1269 | 788132 | 0.59 | 6.81 |
| json | 233.58 | 244.9 | 100.0% | 1151 | 868960 | 6.07 | 1387.77 |
| json+zstd | 24.21 | 25.4 | 10.4% | 1848 | 541157 | 0.00 | 1752.38 |

| Query (median ms; entity_lookup is per lookup) | Rows matched | oe | oe (lazy open) | parquet | vortex | json | json+zstd |
|---|---:|---:|---:|---:|---:|---:|---:|
| scan_all_columns | 1000000 | 63.33 | 71.31 | 131.40 | 71.09 | 2248.45 | 2557.36 |
| scan_one_number | 1000000 | 1.90 | 2.66 | 16.05 | 1.76 | 1347.44 | 1669.03 |
| scan_one_payload_number | 1000000 | 9.20 | 12.18 | 10.98 | 8.71 | 1363.92 | 1698.39 |
| scan_one_payload_text | 1000000 | 7.16 | 10.74 | 7.62 | 4.54 | 1367.23 | 1690.66 |
| filter_type_and_payload | 1237 | 9.08 | 9.20 | 34.43 | 2.15 | 1524.44 | 1609.98 |
| time_window_1pct | 10000 | 0.12 | 0.12 | 2.73 | 0.52 | 1438.20 | 1661.02 |
| entity_lookup | 837 | 0.37 | 0.44 | 32.28 | 6.03 | 1444.66 | 1760.35 |

Removing events (redaction). OE records a deletion vector; the other formats rewrite the whole file.

| Case | Events removed | oe (ms) | oe (bytes written) | oe (lazy open) (ms) | oe (lazy open) (bytes written) | parquet (ms) | parquet (bytes written) | vortex (ms) | vortex (bytes written) | json (ms) | json (bytes written) | json+zstd (ms) | json+zstd (bytes written) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| redact_1_event | 1 | 0.00 | 9 | 0.00 | 9 | 960.96 | 12309274 | 1644.84 | 19543608 | 3854.60 | 244921417 | 4824.32 | 25386110 |
| redact_1pct_of_events | 10000 | 14.83 | 160005 | 14.58 | 160005 | 1021.79 | 12020757 | 1664.96 | 19543040 | 3772.60 | 242472519 | 4967.33 | 25138782 |

## messages events: 10000 rows

| Format | Size (MiB) | Bytes per event | Size vs JSON | Write (ms) | Write (events/s) | Open (ms) | Open + first lookup (ms) |
|---|---:|---:|---:|---:|---:|---:|---:|
| oe | 0.53 | 55.4 | 17.0% | 40 | 250058 | 0.18 | 0.22 |
| oe (lazy open) | 0.53 | 55.4 | 17.0% | 39 | 259107 | 0.09 | 0.30 |
| parquet | 0.43 | 45.5 | 13.9% | 12 | 826797 | 0.03 | 0.41 |
| vortex | 0.45 | 47.0 | 14.4% | 47 | 212821 | 0.57 | 0.88 |
| json | 3.12 | 326.9 | 100.0% | 12 | 817089 | 0.00 | 15.10 |
| json+zstd | 0.57 | 59.9 | 18.3% | 30 | 337595 | 0.00 | 19.79 |

| Query (median ms; entity_lookup is per lookup) | Rows matched | oe | oe (lazy open) | parquet | vortex | json | json+zstd |
|---|---:|---:|---:|---:|---:|---:|---:|
| scan_all_columns | 10000 | 1.00 | 1.48 | 2.87 | 1.05 | 21.74 | 25.91 |
| scan_one_number | 10000 | 0.02 | 0.02 | 0.17 | 0.03 | 14.67 | 18.52 |
| scan_one_payload_number | 10000 | 0.01 | 0.01 | 0.18 | 0.03 | 14.84 | 22.28 |
| scan_one_payload_text | 10000 | 0.36 | 0.38 | 1.19 | 0.31 | 15.90 | 22.62 |
| filter_type_and_payload | 265 | 0.08 | 0.08 | 0.59 | 0.24 | 15.85 | 19.83 |
| time_window_1pct | 100 | 0.01 | 0.01 | 0.37 | 0.18 | 14.82 | 18.76 |
| entity_lookup | 1751 | 0.02 | 0.03 | 0.31 | 0.17 | 15.08 | 27.15 |

Removing events (redaction). OE records a deletion vector; the other formats rewrite the whole file.

| Case | Events removed | oe (ms) | oe (bytes written) | oe (lazy open) (ms) | oe (lazy open) (bytes written) | parquet (ms) | parquet (bytes written) | vortex (ms) | vortex (bytes written) | json (ms) | json (bytes written) | json+zstd (ms) | json+zstd (bytes written) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| redact_1_event | 1 | 0.00 | 9 | 0.00 | 9 | 18.25 | 452866 | 62.06 | 473520 | 38.18 | 3268410 | 55.75 | 598999 |
| redact_1pct_of_events | 100 | 0.03 | 405 | 0.02 | 405 | 18.31 | 448727 | 52.33 | 466192 | 38.34 | 3235234 | 58.31 | 592934 |

## messages events: 100000 rows

| Format | Size (MiB) | Bytes per event | Size vs JSON | Write (ms) | Write (events/s) | Open (ms) | Open + first lookup (ms) |
|---|---:|---:|---:|---:|---:|---:|---:|
| oe | 5.14 | 53.9 | 16.5% | 311 | 321791 | 1.45 | 1.44 |
| oe (lazy open) | 5.14 | 53.9 | 16.5% | 330 | 303315 | 1.13 | 2.40 |
| parquet | 4.10 | 43.0 | 13.2% | 104 | 963157 | 0.04 | 2.56 |
| vortex | 4.64 | 48.6 | 14.9% | 201 | 496382 | 0.66 | 1.82 |
| json | 31.13 | 326.4 | 100.0% | 118 | 846798 | 0.00 | 157.08 |
| json+zstd | 5.83 | 61.1 | 18.7% | 262 | 382358 | 0.00 | 173.99 |

| Query (median ms; entity_lookup is per lookup) | Rows matched | oe | oe (lazy open) | parquet | vortex | json | json+zstd |
|---|---:|---:|---:|---:|---:|---:|---:|
| scan_all_columns | 100000 | 10.20 | 10.39 | 22.73 | 11.17 | 250.05 | 296.54 |
| scan_one_number | 100000 | 0.17 | 0.19 | 1.31 | 0.38 | 147.60 | 192.16 |
| scan_one_payload_number | 100000 | 0.13 | 0.14 | 1.18 | 0.25 | 145.92 | 192.12 |
| scan_one_payload_text | 100000 | 3.89 | 3.94 | 9.83 | 2.90 | 167.65 | 175.47 |
| filter_type_and_payload | 2655 | 0.80 | 0.81 | 3.94 | 0.64 | 162.18 | 168.47 |
| time_window_1pct | 1000 | 0.01 | 0.01 | 2.56 | 0.12 | 149.12 | 161.95 |
| entity_lookup | 1203 | 0.11 | 0.12 | 2.33 | 1.04 | 156.03 | 195.76 |

Removing events (redaction). OE records a deletion vector; the other formats rewrite the whole file.

| Case | Events removed | oe (ms) | oe (bytes written) | oe (lazy open) (ms) | oe (lazy open) (bytes written) | parquet (ms) | parquet (bytes written) | vortex (ms) | vortex (bytes written) | json (ms) | json (bytes written) | json+zstd (ms) | json+zstd (bytes written) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| redact_1_event | 1 | 0.00 | 9 | 0.00 | 9 | 195.87 | 4300907 | 269.87 | 4860768 | 407.66 | 32636819 | 612.26 | 6111549 |
| redact_1pct_of_events | 1000 | 0.73 | 4005 | 1.01 | 4005 | 196.47 | 4232767 | 263.78 | 4816112 | 407.68 | 32309495 | 569.97 | 6051823 |

## messages events: 1000000 rows

| Format | Size (MiB) | Bytes per event | Size vs JSON | Write (ms) | Write (events/s) | Open (ms) | Open + first lookup (ms) |
|---|---:|---:|---:|---:|---:|---:|---:|
| oe | 52.07 | 54.6 | 16.7% | 3365 | 297210 | 12.08 | 13.86 |
| oe (lazy open) | 52.07 | 54.6 | 16.7% | 3482 | 287200 | 10.62 | 19.20 |
| parquet | 41.65 | 43.7 | 13.4% | 1226 | 815426 | 0.10 | 30.04 |
| vortex | 47.32 | 49.6 | 15.2% | 1689 | 591895 | 0.84 | 7.59 |
| json | 311.21 | 326.3 | 100.0% | 1379 | 724973 | 7.31 | 1570.39 |
| json+zstd | 58.99 | 61.9 | 19.0% | 2879 | 347379 | 0.01 | 2050.85 |

| Query (median ms; entity_lookup is per lookup) | Rows matched | oe | oe (lazy open) | parquet | vortex | json | json+zstd |
|---|---:|---:|---:|---:|---:|---:|---:|
| scan_all_columns | 1000000 | 101.62 | 108.16 | 263.10 | 97.67 | 2939.64 | 3445.96 |
| scan_one_number | 1000000 | 1.90 | 2.27 | 15.71 | 1.76 | 1525.22 | 2026.99 |
| scan_one_payload_number | 1000000 | 1.42 | 1.49 | 13.96 | 1.62 | 1530.46 | 2060.02 |
| scan_one_payload_text | 1000000 | 40.32 | 39.91 | 127.06 | 30.68 | 1726.77 | 2277.92 |
| filter_type_and_payload | 26269 | 8.60 | 8.14 | 43.33 | 3.74 | 1654.77 | 2146.62 |
| time_window_1pct | 10000 | 0.04 | 0.04 | 3.82 | 0.39 | 1426.58 | 2046.95 |
| entity_lookup | 5411 | 0.35 | 0.35 | 27.30 | 7.38 | 1570.64 | 2078.11 |

Removing events (redaction). OE records a deletion vector; the other formats rewrite the whole file.

| Case | Events removed | oe (ms) | oe (bytes written) | oe (lazy open) (ms) | oe (lazy open) (bytes written) | parquet (ms) | parquet (bytes written) | vortex (ms) | vortex (bytes written) | json (ms) | json (bytes written) | json+zstd (ms) | json+zstd (bytes written) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| redact_1_event | 1 | 0.00 | 9 | 0.00 | 9 | 2043.40 | 43671434 | 2593.71 | 49602912 | 5033.07 | 326324891 | 6967.00 | 61854744 |
| redact_1pct_of_events | 10000 | 14.42 | 160005 | 14.00 | 160005 | 2195.49 | 43058471 | 2570.31 | 49314872 | 5082.91 | 323067360 | 6891.75 | 61243459 |

## chat events: 10000 rows

| Format | Size (MiB) | Bytes per event | Size vs JSON | Write (ms) | Write (events/s) | Open (ms) | Open + first lookup (ms) |
|---|---:|---:|---:|---:|---:|---:|---:|
| oe | 0.49 | 51.6 | 14.8% | 42 | 239375 | 0.17 | 0.37 |
| oe (lazy open) | 0.49 | 51.6 | 14.8% | 38 | 266575 | 0.10 | 0.36 |
| parquet | 0.36 | 37.3 | 10.7% | 10 | 959196 | 0.01 | 0.46 |
| vortex | 0.42 | 43.6 | 12.5% | 52 | 190730 | 0.59 | 0.98 |
| json | 3.32 | 348.4 | 100.0% | 14 | 694259 | 0.00 | 17.10 |
| json+zstd | 0.49 | 51.1 | 14.7% | 35 | 289155 | 0.00 | 27.95 |

| Query (median ms; entity_lookup is per lookup) | Rows matched | oe | oe (lazy open) | parquet | vortex | json | json+zstd |
|---|---:|---:|---:|---:|---:|---:|---:|
| scan_all_columns | 10000 | 1.08 | 1.04 | 2.74 | 1.14 | 23.93 | 24.92 |
| scan_one_number | 10000 | 0.03 | 0.03 | 0.17 | 0.03 | 15.14 | 18.50 |
| scan_one_payload_number | 10000 | 0.01 | 0.01 | 0.07 | 0.03 | 15.21 | 19.47 |
| scan_one_payload_text | 10000 | 0.34 | 0.33 | 1.03 | 0.31 | 15.90 | 20.17 |
| filter_type_and_payload | 266 | 0.07 | 0.07 | 0.37 | 0.20 | 12.69 | 19.97 |
| time_window_1pct | 100 | 0.02 | 0.02 | 0.23 | 0.11 | 12.39 | 19.01 |
| entity_lookup | 36977 | 0.08 | 0.07 | 0.33 | 0.17 | 15.87 | 19.68 |
| latest_50_in_room | 29391 | 0.08 | 0.08 | 0.52 | 0.34 | 16.55 | 20.67 |
| thread_replies | 21 | 0.07 | 0.07 | 0.36 | 0.33 | 16.02 | 19.45 |
| event_by_id | 20 | 0.01 | 0.01 | 0.51 | 0.29 | 16.17 | 20.37 |

Removing events (redaction). OE records a deletion vector; the other formats rewrite the whole file.

| Case | Events removed | oe (ms) | oe (bytes written) | oe (lazy open) (ms) | oe (lazy open) (bytes written) | parquet (ms) | parquet (bytes written) | vortex (ms) | vortex (bytes written) | json (ms) | json (bytes written) | json+zstd (ms) | json+zstd (bytes written) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| redact_1_event | 1 | 0.00 | 9 | 0.00 | 9 | 18.35 | 370690 | 60.69 | 433524 | 42.06 | 3483690 | 59.99 | 511242 |
| redact_1pct_of_events | 100 | 0.02 | 405 | 0.02 | 405 | 18.56 | 367096 | 60.35 | 434356 | 40.99 | 3450121 | 57.37 | 506512 |

## chat events: 100000 rows

| Format | Size (MiB) | Bytes per event | Size vs JSON | Write (ms) | Write (events/s) | Open (ms) | Open + first lookup (ms) |
|---|---:|---:|---:|---:|---:|---:|---:|
| oe | 5.09 | 53.4 | 15.3% | 319 | 313670 | 1.42 | 1.86 |
| oe (lazy open) | 5.09 | 53.4 | 15.3% | 326 | 306873 | 0.81 | 1.76 |
| parquet | 3.41 | 35.8 | 10.2% | 111 | 903906 | 0.08 | 3.43 |
| vortex | 4.18 | 43.9 | 12.5% | 271 | 369089 | 0.69 | 1.18 |
| json | 33.34 | 349.5 | 100.0% | 133 | 754694 | 0.00 | 162.73 |
| json+zstd | 5.10 | 53.5 | 15.3% | 288 | 347432 | 0.00 | 206.35 |

| Query (median ms; entity_lookup is per lookup) | Rows matched | oe | oe (lazy open) | parquet | vortex | json | json+zstd |
|---|---:|---:|---:|---:|---:|---:|---:|
| scan_all_columns | 100000 | 10.44 | 9.35 | 25.87 | 9.84 | 256.23 | 311.69 |
| scan_one_number | 100000 | 0.18 | 0.15 | 1.63 | 0.26 | 154.59 | 196.49 |
| scan_one_payload_number | 100000 | 0.13 | 0.13 | 0.39 | 0.15 | 158.67 | 198.37 |
| scan_one_payload_text | 100000 | 3.45 | 3.03 | 11.10 | 2.75 | 171.26 | 216.65 |
| filter_type_and_payload | 2904 | 0.62 | 0.52 | 3.56 | 0.41 | 168.09 | 211.52 |
| time_window_1pct | 1000 | 0.02 | 0.01 | 2.09 | 0.11 | 159.54 | 203.49 |
| entity_lookup | 81077 | 0.36 | 0.29 | 3.00 | 0.36 | 163.01 | 209.23 |
| latest_50_in_room | 64860 | 0.37 | 0.30 | 4.71 | 0.51 | 174.43 | 217.54 |
| thread_replies | 23 | 0.09 | 0.08 | 3.54 | 0.46 | 161.45 | 196.21 |
| event_by_id | 20 | 0.09 | 0.08 | 4.51 | 0.28 | 163.23 | 213.09 |

Removing events (redaction). OE records a deletion vector; the other formats rewrite the whole file.

| Case | Events removed | oe (ms) | oe (bytes written) | oe (lazy open) (ms) | oe (lazy open) (bytes written) | parquet (ms) | parquet (bytes written) | vortex (ms) | vortex (bytes written) | json (ms) | json (bytes written) | json+zstd (ms) | json+zstd (bytes written) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| redact_1_event | 1 | 0.00 | 9 | 0.00 | 9 | 208.89 | 3580849 | 340.71 | 4381044 | 443.15 | 34954335 | 594.00 | 5350605 |
| redact_1pct_of_events | 1000 | 0.60 | 4005 | 0.58 | 4005 | 209.80 | 3521126 | 320.41 | 4330708 | 452.41 | 34607634 | 610.77 | 5298958 |

## chat events: 1000000 rows

| Format | Size (MiB) | Bytes per event | Size vs JSON | Write (ms) | Write (events/s) | Open (ms) | Open + first lookup (ms) |
|---|---:|---:|---:|---:|---:|---:|---:|
| oe | 52.56 | 55.1 | 15.7% | 3354 | 298192 | 14.40 | 14.60 |
| oe (lazy open) | 52.56 | 55.1 | 15.7% | 3534 | 282940 | 9.35 | 27.77 |
| parquet | 35.96 | 37.7 | 10.8% | 1263 | 792059 | 0.21 | 29.78 |
| vortex | 45.86 | 48.1 | 13.7% | 2237 | 446968 | 0.94 | 2.81 |
| json | 334.22 | 350.5 | 100.0% | 1410 | 709154 | 8.35 | 1621.24 |
| json+zstd | 52.90 | 55.5 | 15.8% | 3024 | 330679 | 0.96 | 1984.52 |

| Query (median ms; entity_lookup is per lookup) | Rows matched | oe | oe (lazy open) | parquet | vortex | json | json+zstd |
|---|---:|---:|---:|---:|---:|---:|---:|
| scan_all_columns | 1000000 | 102.49 | 113.90 | 267.71 | 98.13 | 2868.94 | 3538.80 |
| scan_one_number | 1000000 | 2.14 | 2.09 | 14.94 | 1.84 | 1600.63 | 2359.61 |
| scan_one_payload_number | 1000000 | 1.36 | 1.39 | 4.03 | 1.52 | 1550.36 | 2035.98 |
| scan_one_payload_text | 1000000 | 37.42 | 37.40 | 105.77 | 27.43 | 1722.70 | 2195.34 |
| filter_type_and_payload | 30248 | 6.87 | 6.85 | 34.15 | 3.50 | 1689.77 | 2170.98 |
| time_window_1pct | 10000 | 0.05 | 0.05 | 2.60 | 0.82 | 1577.89 | 2141.70 |
| entity_lookup | 200587 | 1.73 | 1.77 | 28.76 | 2.06 | 1630.88 | 2111.11 |
| latest_50_in_room | 160250 | 1.91 | 1.79 | 46.19 | 2.41 | 1749.98 | 2213.24 |
| thread_replies | 22 | 0.30 | 0.29 | 22.27 | 3.21 | 1621.59 | 2148.35 |
| event_by_id | 20 | 0.09 | 0.10 | 5.55 | 0.68 | 1706.08 | 2119.38 |

Removing events (redaction). OE records a deletion vector; the other formats rewrite the whole file.

| Case | Events removed | oe (ms) | oe (bytes written) | oe (lazy open) (ms) | oe (lazy open) (bytes written) | parquet (ms) | parquet (bytes written) | vortex (ms) | vortex (bytes written) | json (ms) | json (bytes written) | json+zstd (ms) | json+zstd (bytes written) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| redact_1_event | 1 | 0.00 | 9 | 0.00 | 9 | 2228.90 | 37698174 | 2785.56 | 48035456 | 5106.61 | 350453613 | 6885.73 | 55469073 |
| redact_1pct_of_events | 10000 | 14.69 | 160005 | 16.38 | 160005 | 2115.93 | 37167023 | 3112.20 | 47241092 | 5060.36 | 346953322 | 7081.26 | 54925975 |

## Durable ingest

Each batch is acknowledged only after the file is synced to disk (300 batches per row). Parquet and Vortex cannot append, so each batch is a new file.

| Batch size | Format | Events/s | Ack p50 (ms) | Ack p99 (ms) | Bytes on disk |
|---:|---|---:|---:|---:|---:|
| 1 | oej journal | 5455 | 0.153 | 0.465 | 1228800 |
| 1 | json (append) | 8335 | 0.103 | 0.364 | 104659 |
| 1 | parquet (file per batch) | 1208 | 0.771 | 2.229 | 1094080 |
| 1 | vortex (file per batch) | 557 | 1.711 | 2.529 | 3131136 |
| 16 | oej journal | 50214 | 0.286 | 1.114 | 2457600 |
| 16 | json (append) | 64375 | 0.219 | 0.801 | 1664629 |
| 16 | parquet (file per batch) | 16186 | 0.959 | 2.100 | 1373373 |
| 16 | vortex (file per batch) | 1070 | 14.501 | 23.200 | 4804864 |
| 128 | oej journal | 180087 | 0.660 | 2.312 | 19660800 |
| 128 | json (append) | 181269 | 0.690 | 1.036 | 13400243 |
| 128 | parquet (file per batch) | 97024 | 1.275 | 2.036 | 2851044 |
| 128 | vortex (file per batch) | 6033 | 21.052 | 30.821 | 7595608 |

