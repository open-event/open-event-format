# Benchmark results

Machine: Intel(R) Xeon(R) Processor @ 2.10GHz (4 logical CPUs), Linux 6.18.44-fc-v310, rustc 1.96.0 (ac68faa20 2026-05-25)

Every format stores the same events. Query times are the median of 5 runs after one warm-up; write times are the median of 3 runs. All queries run on a single thread against the file held in memory. Every format returned identical results for every query (checked on each run).

## telemetry events: 10000 rows

| Format | Size (MiB) | Bytes per event | Size vs JSON | Write (ms) | Write (events/s) | Open (ms) | Open + first lookup (ms) |
|---|---:|---:|---:|---:|---:|---:|---:|
| oe | 0.37 | 38.9 | 15.9% | 39 | 258890 | 0.16 | 0.28 |
| oe (lazy open) | 0.37 | 38.9 | 15.9% | 32 | 310121 | 0.14 | 0.28 |
| parquet | 0.13 | 13.7 | 5.6% | 5 | 2019564 | 0.02 | 0.38 |
| vortex | 0.16 | 16.6 | 6.8% | 34 | 297527 | 0.51 | 0.89 |
| json | 2.34 | 245.1 | 100.0% | 12 | 839480 | 0.00 | 13.86 |
| json+zstd | 0.22 | 23.4 | 9.5% | 18 | 570366 | 0.00 | 17.70 |

| Query (median ms; entity_lookup is per lookup) | Rows matched | oe | oe (lazy open) | parquet | vortex | json | json+zstd |
|---|---:|---:|---:|---:|---:|---:|---:|
| scan_all_columns | 10000 | 0.69 | 0.60 | 1.50 | 0.97 | 19.28 | 21.20 |
| scan_one_number | 10000 | 0.02 | 0.02 | 0.18 | 0.03 | 13.20 | 15.36 |
| scan_one_payload_number | 10000 | 0.09 | 0.09 | 0.18 | 0.11 | 13.58 | 15.42 |
| scan_one_payload_text | 10000 | 0.06 | 0.06 | 0.09 | 0.06 | 14.32 | 15.87 |
| filter_type_and_payload | 12 | 0.08 | 0.09 | 0.47 | 0.27 | 16.68 | 16.61 |
| time_window_1pct | 100 | 0.02 | 0.02 | 0.28 | 0.12 | 24.64 | 15.69 |
| entity_lookup | 820 | 0.12 | 0.11 | 0.29 | 0.14 | 13.95 | 16.22 |

Removing events (redaction). OE records a deletion vector; the other formats rewrite the whole file.

| Case | Events removed | oe (ms) | oe (bytes written) | oe (lazy open) (ms) | oe (lazy open) (bytes written) | parquet (ms) | parquet (bytes written) | vortex (ms) | vortex (bytes written) | json (ms) | json (bytes written) | json+zstd (ms) | json+zstd (bytes written) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| redact_1_event | 1 | 0.00 | 9 | 0.00 | 9 | 9.84 | 134711 | 39.35 | 165968 | 33.67 | 2450649 | 42.55 | 233602 |
| redact_1pct_of_events | 100 | 0.02 | 405 | 0.02 | 405 | 10.13 | 133804 | 39.03 | 165504 | 33.69 | 2426385 | 40.51 | 231239 |

## telemetry events: 100000 rows

| Format | Size (MiB) | Bytes per event | Size vs JSON | Write (ms) | Write (events/s) | Open (ms) | Open + first lookup (ms) |
|---|---:|---:|---:|---:|---:|---:|---:|
| oe | 4.17 | 43.7 | 17.8% | 377 | 265137 | 1.55 | 1.29 |
| oe (lazy open) | 4.17 | 43.7 | 17.8% | 389 | 257156 | 1.11 | 2.12 |
| parquet | 1.11 | 11.6 | 4.7% | 56 | 1797390 | 0.04 | 2.92 |
| vortex | 1.76 | 18.5 | 7.5% | 140 | 715999 | 0.62 | 1.80 |
| json | 23.36 | 245.0 | 100.0% | 119 | 843411 | 0.00 | 143.25 |
| json+zstd | 2.36 | 24.7 | 10.1% | 183 | 546438 | 0.00 | 184.27 |

| Query (median ms; entity_lookup is per lookup) | Rows matched | oe | oe (lazy open) | parquet | vortex | json | json+zstd |
|---|---:|---:|---:|---:|---:|---:|---:|
| scan_all_columns | 100000 | 8.97 | 6.94 | 13.70 | 7.89 | 214.37 | 250.63 |
| scan_one_number | 100000 | 0.17 | 0.17 | 1.62 | 0.21 | 133.06 | 171.57 |
| scan_one_payload_number | 100000 | 0.92 | 0.92 | 1.09 | 0.95 | 138.56 | 174.16 |
| scan_one_payload_text | 100000 | 0.65 | 0.67 | 0.80 | 0.67 | 161.70 | 180.66 |
| filter_type_and_payload | 143 | 0.86 | 0.88 | 3.57 | 0.54 | 151.21 | 197.05 |
| time_window_1pct | 1000 | 0.05 | 0.07 | 2.09 | 0.21 | 150.35 | 185.74 |
| entity_lookup | 834 | 0.18 | 0.29 | 2.71 | 1.34 | 147.52 | 182.71 |

Removing events (redaction). OE records a deletion vector; the other formats rewrite the whole file.

| Case | Events removed | oe (ms) | oe (bytes written) | oe (lazy open) (ms) | oe (lazy open) (bytes written) | parquet (ms) | parquet (bytes written) | vortex (ms) | vortex (bytes written) | json (ms) | json (bytes written) | json+zstd (ms) | json+zstd (bytes written) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| redact_1_event | 1 | 0.00 | 9 | 0.00 | 9 | 97.21 | 1162107 | 196.94 | 1846136 | 367.64 | 24497827 | 473.47 | 2470339 |
| redact_1pct_of_events | 1000 | 1.12 | 4005 | 1.17 | 4005 | 102.19 | 1129279 | 188.88 | 1834576 | 360.99 | 24253096 | 480.01 | 2446064 |

## telemetry events: 1000000 rows

| Format | Size (MiB) | Bytes per event | Size vs JSON | Write (ms) | Write (events/s) | Open (ms) | Open + first lookup (ms) |
|---|---:|---:|---:|---:|---:|---:|---:|
| oe | 48.05 | 50.4 | 20.6% | 3640 | 274723 | 9.99 | 11.75 |
| oe (lazy open) | 48.05 | 50.4 | 20.6% | 3538 | 282617 | 8.17 | 18.19 |
| parquet | 11.73 | 12.3 | 5.0% | 551 | 1815908 | 0.10 | 34.61 |
| vortex | 18.63 | 19.5 | 8.0% | 1258 | 794887 | 0.73 | 7.30 |
| json | 233.58 | 244.9 | 100.0% | 1338 | 747173 | 5.12 | 1520.39 |
| json+zstd | 24.21 | 25.4 | 10.4% | 1941 | 515180 | 0.00 | 1851.15 |

| Query (median ms; entity_lookup is per lookup) | Rows matched | oe | oe (lazy open) | parquet | vortex | json | json+zstd |
|---|---:|---:|---:|---:|---:|---:|---:|
| scan_all_columns | 1000000 | 82.76 | 77.47 | 133.01 | 83.25 | 2344.50 | 2593.76 |
| scan_one_number | 1000000 | 1.88 | 2.55 | 15.66 | 1.92 | 1353.29 | 1737.93 |
| scan_one_payload_number | 1000000 | 9.21 | 11.97 | 10.92 | 8.80 | 1441.73 | 1703.40 |
| scan_one_payload_text | 1000000 | 6.41 | 10.06 | 7.71 | 4.98 | 1488.22 | 1820.22 |
| filter_type_and_payload | 1237 | 9.39 | 11.98 | 35.66 | 2.35 | 1549.93 | 1814.19 |
| time_window_1pct | 10000 | 0.12 | 0.14 | 3.58 | 0.56 | 1419.92 | 1792.02 |
| entity_lookup | 837 | 1.87 | 2.05 | 28.43 | 6.61 | 1456.11 | 1776.59 |

Removing events (redaction). OE records a deletion vector; the other formats rewrite the whole file.

| Case | Events removed | oe (ms) | oe (bytes written) | oe (lazy open) (ms) | oe (lazy open) (bytes written) | parquet (ms) | parquet (bytes written) | vortex (ms) | vortex (bytes written) | json (ms) | json (bytes written) | json+zstd (ms) | json+zstd (bytes written) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| redact_1_event | 1 | 0.00 | 9 | 0.00 | 9 | 1030.58 | 12309274 | 1721.15 | 19543608 | 3991.40 | 244921417 | 4680.09 | 25386110 |
| redact_1pct_of_events | 10000 | 14.44 | 160005 | 18.87 | 160005 | 1125.65 | 12020757 | 1648.22 | 19543040 | 3918.14 | 242472519 | 4836.97 | 25138782 |

## messages events: 10000 rows

| Format | Size (MiB) | Bytes per event | Size vs JSON | Write (ms) | Write (events/s) | Open (ms) | Open + first lookup (ms) |
|---|---:|---:|---:|---:|---:|---:|---:|
| oe | 0.80 | 83.4 | 25.5% | 47 | 213455 | 0.23 | 0.25 |
| oe (lazy open) | 0.80 | 83.4 | 25.5% | 39 | 257726 | 0.11 | 0.24 |
| parquet | 0.43 | 45.5 | 13.9% | 13 | 779025 | 0.02 | 0.38 |
| vortex | 0.45 | 47.0 | 14.4% | 49 | 202426 | 0.79 | 1.41 |
| json | 3.12 | 326.9 | 100.0% | 12 | 862238 | 0.00 | 13.71 |
| json+zstd | 0.57 | 59.9 | 18.3% | 24 | 415339 | 0.00 | 19.17 |

| Query (median ms; entity_lookup is per lookup) | Rows matched | oe | oe (lazy open) | parquet | vortex | json | json+zstd |
|---|---:|---:|---:|---:|---:|---:|---:|
| scan_all_columns | 10000 | 1.07 | 1.01 | 3.11 | 1.72 | 20.06 | 25.21 |
| scan_one_number | 10000 | 0.01 | 0.02 | 0.17 | 0.05 | 13.88 | 18.18 |
| scan_one_payload_number | 10000 | 0.01 | 0.01 | 0.18 | 0.05 | 13.51 | 18.40 |
| scan_one_payload_text | 10000 | 0.29 | 0.34 | 1.25 | 0.47 | 15.01 | 19.57 |
| filter_type_and_payload | 265 | 0.06 | 0.08 | 0.56 | 0.28 | 15.80 | 18.42 |
| time_window_1pct | 100 | 0.01 | 0.01 | 0.35 | 0.12 | 14.84 | 18.41 |
| entity_lookup | 1751 | 0.02 | 0.02 | 0.32 | 0.14 | 14.76 | 19.33 |

Removing events (redaction). OE records a deletion vector; the other formats rewrite the whole file.

| Case | Events removed | oe (ms) | oe (bytes written) | oe (lazy open) (ms) | oe (lazy open) (bytes written) | parquet (ms) | parquet (bytes written) | vortex (ms) | vortex (bytes written) | json (ms) | json (bytes written) | json+zstd (ms) | json+zstd (bytes written) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| redact_1_event | 1 | 0.00 | 9 | 0.00 | 9 | 19.02 | 452866 | 53.05 | 473520 | 37.94 | 3268410 | 52.72 | 598999 |
| redact_1pct_of_events | 100 | 0.01 | 405 | 0.07 | 405 | 21.91 | 448727 | 53.07 | 466192 | 50.29 | 3235234 | 56.06 | 592934 |

## messages events: 100000 rows

| Format | Size (MiB) | Bytes per event | Size vs JSON | Write (ms) | Write (events/s) | Open (ms) | Open + first lookup (ms) |
|---|---:|---:|---:|---:|---:|---:|---:|
| oe | 8.33 | 87.4 | 26.8% | 450 | 222117 | 1.55 | 2.00 |
| oe (lazy open) | 8.33 | 87.4 | 26.8% | 432 | 231699 | 0.85 | 2.20 |
| parquet | 4.10 | 43.0 | 13.2% | 108 | 926448 | 0.05 | 2.73 |
| vortex | 4.64 | 48.6 | 14.9% | 204 | 489277 | 0.60 | 1.72 |
| json | 31.13 | 326.4 | 100.0% | 134 | 748681 | 0.00 | 153.72 |
| json+zstd | 5.83 | 61.1 | 18.7% | 298 | 336053 | 0.00 | 196.99 |

| Query (median ms; entity_lookup is per lookup) | Rows matched | oe | oe (lazy open) | parquet | vortex | json | json+zstd |
|---|---:|---:|---:|---:|---:|---:|---:|
| scan_all_columns | 100000 | 9.50 | 9.66 | 30.74 | 9.60 | 249.77 | 285.86 |
| scan_one_number | 100000 | 0.17 | 0.18 | 1.63 | 0.18 | 156.69 | 209.04 |
| scan_one_payload_number | 100000 | 0.13 | 0.12 | 1.45 | 0.16 | 149.30 | 186.79 |
| scan_one_payload_text | 100000 | 3.40 | 3.40 | 12.55 | 2.56 | 178.50 | 201.58 |
| filter_type_and_payload | 2655 | 0.77 | 0.92 | 4.68 | 0.52 | 161.38 | 206.78 |
| time_window_1pct | 1000 | 0.01 | 0.02 | 3.54 | 0.13 | 170.94 | 198.34 |
| entity_lookup | 1203 | 0.15 | 0.15 | 2.88 | 0.96 | 160.38 | 194.53 |

Removing events (redaction). OE records a deletion vector; the other formats rewrite the whole file.

| Case | Events removed | oe (ms) | oe (bytes written) | oe (lazy open) (ms) | oe (lazy open) (bytes written) | parquet (ms) | parquet (bytes written) | vortex (ms) | vortex (bytes written) | json (ms) | json (bytes written) | json+zstd (ms) | json+zstd (bytes written) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| redact_1_event | 1 | 0.00 | 9 | 0.00 | 9 | 186.36 | 4300907 | 263.46 | 4860768 | 520.79 | 32636819 | 658.38 | 6111549 |
| redact_1pct_of_events | 1000 | 0.92 | 4005 | 1.00 | 4005 | 190.06 | 4232767 | 276.30 | 4816112 | 434.54 | 32309495 | 585.78 | 6051823 |

## messages events: 1000000 rows

| Format | Size (MiB) | Bytes per event | Size vs JSON | Write (ms) | Write (events/s) | Open (ms) | Open + first lookup (ms) |
|---|---:|---:|---:|---:|---:|---:|---:|
| oe | 89.81 | 94.2 | 28.9% | 4606 | 217131 | 13.04 | 12.85 |
| oe (lazy open) | 89.81 | 94.2 | 28.9% | 4395 | 227553 | 8.45 | 25.96 |
| parquet | 41.65 | 43.7 | 13.4% | 1184 | 844902 | 0.14 | 26.81 |
| vortex | 47.32 | 49.6 | 15.2% | 1788 | 559393 | 0.74 | 7.21 |
| json | 311.21 | 326.3 | 100.0% | 1436 | 696156 | 6.68 | 1605.80 |
| json+zstd | 58.99 | 61.9 | 19.0% | 2892 | 345833 | 0.00 | 2069.06 |

| Query (median ms; entity_lookup is per lookup) | Rows matched | oe | oe (lazy open) | parquet | vortex | json | json+zstd |
|---|---:|---:|---:|---:|---:|---:|---:|
| scan_all_columns | 1000000 | 105.87 | 98.23 | 262.43 | 103.28 | 2866.63 | 3399.61 |
| scan_one_number | 1000000 | 1.87 | 1.76 | 20.97 | 1.71 | 1425.85 | 2014.20 |
| scan_one_payload_number | 1000000 | 1.41 | 1.35 | 12.74 | 1.68 | 1518.97 | 1947.32 |
| scan_one_payload_text | 1000000 | 37.25 | 35.21 | 128.97 | 28.91 | 1709.27 | 2254.26 |
| filter_type_and_payload | 26269 | 10.48 | 7.92 | 44.48 | 3.23 | 1578.76 | 2152.88 |
| time_window_1pct | 10000 | 0.06 | 0.04 | 3.73 | 0.41 | 1638.98 | 2091.71 |
| entity_lookup | 5411 | 1.99 | 1.60 | 28.71 | 6.93 | 1579.16 | 2069.07 |

Removing events (redaction). OE records a deletion vector; the other formats rewrite the whole file.

| Case | Events removed | oe (ms) | oe (bytes written) | oe (lazy open) (ms) | oe (lazy open) (bytes written) | parquet (ms) | parquet (bytes written) | vortex (ms) | vortex (bytes written) | json (ms) | json (bytes written) | json+zstd (ms) | json+zstd (bytes written) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| redact_1_event | 1 | 0.00 | 9 | 0.00 | 9 | 2165.63 | 43671434 | 2471.69 | 49602912 | 4953.30 | 326324891 | 6466.77 | 61854744 |
| redact_1pct_of_events | 10000 | 13.84 | 160005 | 13.74 | 160005 | 2010.93 | 43058471 | 2719.21 | 49314872 | 4944.97 | 323067360 | 6430.87 | 61243459 |

## chat events: 10000 rows

| Format | Size (MiB) | Bytes per event | Size vs JSON | Write (ms) | Write (events/s) | Open (ms) | Open + first lookup (ms) |
|---|---:|---:|---:|---:|---:|---:|---:|
| oe | 0.76 | 80.2 | 23.0% | 45 | 223962 | 0.27 | 0.40 |
| oe (lazy open) | 0.76 | 80.2 | 23.0% | 44 | 228697 | 0.11 | 0.38 |
| parquet | 0.36 | 37.3 | 10.7% | 12 | 832741 | 0.01 | 0.50 |
| vortex | 0.42 | 43.6 | 12.5% | 64 | 155782 | 0.55 | 1.06 |
| json | 3.32 | 348.4 | 100.0% | 13 | 762563 | 0.00 | 14.59 |
| json+zstd | 0.49 | 51.1 | 14.7% | 21 | 486467 | 0.00 | 15.48 |

| Query (median ms; entity_lookup is per lookup) | Rows matched | oe | oe (lazy open) | parquet | vortex | json | json+zstd |
|---|---:|---:|---:|---:|---:|---:|---:|
| scan_all_columns | 10000 | 1.16 | 1.06 | 2.74 | 1.21 | 21.38 | 21.05 |
| scan_one_number | 10000 | 0.03 | 0.03 | 0.17 | 0.04 | 15.69 | 15.36 |
| scan_one_payload_number | 10000 | 0.01 | 0.01 | 0.07 | 0.03 | 15.85 | 15.09 |
| scan_one_payload_text | 10000 | 0.33 | 0.32 | 1.15 | 0.30 | 16.31 | 27.34 |
| filter_type_and_payload | 266 | 0.07 | 0.07 | 0.37 | 0.28 | 15.91 | 17.88 |
| time_window_1pct | 100 | 0.02 | 0.02 | 0.23 | 0.11 | 16.42 | 18.48 |
| entity_lookup | 36977 | 0.08 | 0.07 | 0.37 | 0.22 | 16.04 | 19.23 |
| latest_50_in_room | 29391 | 0.08 | 0.08 | 0.61 | 0.38 | 16.37 | 21.99 |
| thread_replies | 21 | 0.08 | 0.07 | 0.39 | 0.23 | 16.49 | 19.04 |
| event_by_id | 20 | 0.01 | 0.01 | 0.55 | 0.24 | 16.36 | 19.66 |

Removing events (redaction). OE records a deletion vector; the other formats rewrite the whole file.

| Case | Events removed | oe (ms) | oe (bytes written) | oe (lazy open) (ms) | oe (lazy open) (bytes written) | parquet (ms) | parquet (bytes written) | vortex (ms) | vortex (bytes written) | json (ms) | json (bytes written) | json+zstd (ms) | json+zstd (bytes written) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| redact_1_event | 1 | 0.00 | 9 | 0.00 | 9 | 19.01 | 370690 | 67.73 | 433524 | 38.92 | 3483690 | 58.49 | 511242 |
| redact_1pct_of_events | 100 | 0.02 | 405 | 0.02 | 405 | 19.56 | 367096 | 57.98 | 434356 | 41.59 | 3450121 | 56.53 | 506512 |

## chat events: 100000 rows

| Format | Size (MiB) | Bytes per event | Size vs JSON | Write (ms) | Write (events/s) | Open (ms) | Open + first lookup (ms) |
|---|---:|---:|---:|---:|---:|---:|---:|
| oe | 7.73 | 81.1 | 23.2% | 471 | 212512 | 2.05 | 2.58 |
| oe (lazy open) | 7.73 | 81.1 | 23.2% | 429 | 233137 | 1.00 | 2.24 |
| parquet | 3.41 | 35.8 | 10.2% | 115 | 872189 | 0.05 | 2.88 |
| vortex | 4.18 | 43.9 | 12.5% | 259 | 386553 | 0.67 | 1.14 |
| json | 33.34 | 349.5 | 100.0% | 141 | 708923 | 0.00 | 153.23 |
| json+zstd | 5.10 | 53.5 | 15.3% | 263 | 380577 | 0.00 | 197.68 |

| Query (median ms; entity_lookup is per lookup) | Rows matched | oe | oe (lazy open) | parquet | vortex | json | json+zstd |
|---|---:|---:|---:|---:|---:|---:|---:|
| scan_all_columns | 100000 | 10.79 | 10.38 | 29.48 | 10.23 | 258.52 | 288.65 |
| scan_one_number | 100000 | 0.26 | 0.27 | 1.88 | 0.22 | 161.21 | 201.12 |
| scan_one_payload_number | 100000 | 0.24 | 0.20 | 0.49 | 0.17 | 160.24 | 199.14 |
| scan_one_payload_text | 100000 | 4.94 | 5.44 | 11.21 | 2.80 | 159.85 | 213.85 |
| filter_type_and_payload | 2904 | 0.87 | 1.05 | 3.74 | 0.43 | 163.95 | 214.44 |
| time_window_1pct | 1000 | 0.02 | 0.03 | 2.79 | 0.18 | 137.44 | 204.22 |
| entity_lookup | 81077 | 0.61 | 0.83 | 4.01 | 0.37 | 152.62 | 198.04 |
| latest_50_in_room | 64860 | 0.57 | 0.74 | 5.05 | 0.49 | 165.99 | 214.94 |
| thread_replies | 23 | 0.11 | 0.07 | 3.59 | 0.50 | 167.89 | 204.92 |
| event_by_id | 20 | 0.17 | 0.15 | 4.22 | 0.34 | 171.35 | 213.28 |

Removing events (redaction). OE records a deletion vector; the other formats rewrite the whole file.

| Case | Events removed | oe (ms) | oe (bytes written) | oe (lazy open) (ms) | oe (lazy open) (bytes written) | parquet (ms) | parquet (bytes written) | vortex (ms) | vortex (bytes written) | json (ms) | json (bytes written) | json+zstd (ms) | json+zstd (bytes written) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| redact_1_event | 1 | 0.00 | 9 | 0.00 | 9 | 182.94 | 3580849 | 341.28 | 4381044 | 403.47 | 34954335 | 614.59 | 5350605 |
| redact_1pct_of_events | 1000 | 0.54 | 4005 | 0.77 | 4005 | 184.82 | 3521126 | 346.44 | 4330708 | 436.86 | 34607634 | 699.07 | 5298958 |

## chat events: 1000000 rows

| Format | Size (MiB) | Bytes per event | Size vs JSON | Write (ms) | Write (events/s) | Open (ms) | Open + first lookup (ms) |
|---|---:|---:|---:|---:|---:|---:|---:|
| oe | 83.33 | 87.4 | 24.9% | 4745 | 210736 | 12.32 | 12.85 |
| oe (lazy open) | 83.33 | 87.4 | 24.9% | 4561 | 219244 | 11.12 | 21.25 |
| parquet | 35.96 | 37.7 | 10.8% | 1110 | 901251 | 0.17 | 29.66 |
| vortex | 45.86 | 48.1 | 13.7% | 2269 | 440809 | 0.88 | 2.82 |
| json | 334.22 | 350.5 | 100.0% | 1378 | 725665 | 7.89 | 1592.63 |
| json+zstd | 52.90 | 55.5 | 15.8% | 2745 | 364352 | 0.00 | 2024.01 |

| Query (median ms; entity_lookup is per lookup) | Rows matched | oe | oe (lazy open) | parquet | vortex | json | json+zstd |
|---|---:|---:|---:|---:|---:|---:|---:|
| scan_all_columns | 1000000 | 116.90 | 105.74 | 265.25 | 106.30 | 2796.99 | 3271.45 |
| scan_one_number | 1000000 | 2.09 | 2.11 | 13.03 | 1.84 | 1550.87 | 2052.67 |
| scan_one_payload_number | 1000000 | 1.40 | 1.40 | 3.25 | 1.57 | 1553.51 | 2035.29 |
| scan_one_payload_text | 1000000 | 34.71 | 37.77 | 101.01 | 29.31 | 1706.17 | 2307.26 |
| filter_type_and_payload | 30248 | 6.59 | 6.96 | 33.94 | 3.24 | 1639.22 | 2193.00 |
| time_window_1pct | 10000 | 0.05 | 0.05 | 2.63 | 0.55 | 1649.51 | 2116.99 |
| entity_lookup | 200587 | 1.77 | 1.88 | 29.52 | 1.93 | 1570.09 | 2135.78 |
| latest_50_in_room | 160250 | 1.90 | 1.97 | 47.33 | 2.81 | 1703.76 | 2175.04 |
| thread_replies | 22 | 0.30 | 0.29 | 20.44 | 4.23 | 1664.44 | 2167.83 |
| event_by_id | 20 | 0.18 | 0.18 | 5.41 | 0.88 | 1762.60 | 2167.24 |

Removing events (redaction). OE records a deletion vector; the other formats rewrite the whole file.

| Case | Events removed | oe (ms) | oe (bytes written) | oe (lazy open) (ms) | oe (lazy open) (bytes written) | parquet (ms) | parquet (bytes written) | vortex (ms) | vortex (bytes written) | json (ms) | json (bytes written) | json+zstd (ms) | json+zstd (bytes written) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| redact_1_event | 1 | 0.00 | 9 | 0.00 | 9 | 2191.53 | 37698174 | 3042.29 | 48035456 | 5086.16 | 350453613 | 6908.99 | 55469073 |
| redact_1pct_of_events | 10000 | 14.36 | 160005 | 14.63 | 160005 | 2006.80 | 37167023 | 3020.38 | 47241092 | 4992.85 | 346953322 | 6811.98 | 54925975 |

## Durable ingest

Each batch is acknowledged only after the file is synced to disk (300 batches per row). Parquet and Vortex cannot append, so each batch is a new file.

| Batch size | Format | Events/s | Ack p50 (ms) | Ack p99 (ms) | Bytes on disk |
|---:|---|---:|---:|---:|---:|
| 1 | oej journal | 3543 | 0.257 | 0.605 | 1228800 |
| 1 | json (append) | 3392 | 0.269 | 0.883 | 104659 |
| 1 | parquet (file per batch) | 917 | 1.038 | 2.012 | 1094080 |
| 1 | vortex (file per batch) | 448 | 2.088 | 3.574 | 3131136 |
| 16 | oej journal | 32211 | 0.397 | 2.276 | 2457600 |
| 16 | json (append) | 49412 | 0.292 | 0.704 | 1664629 |
| 16 | parquet (file per batch) | 11583 | 1.315 | 3.702 | 1373373 |
| 16 | vortex (file per batch) | 1070 | 14.607 | 21.895 | 4804864 |
| 128 | oej journal | 193272 | 0.619 | 1.255 | 19660800 |
| 128 | json (append) | 155153 | 0.786 | 1.506 | 13400243 |
| 128 | parquet (file per batch) | 83318 | 1.471 | 2.493 | 2851044 |
| 128 | vortex (file per batch) | 5749 | 21.636 | 31.358 | 7595608 |

