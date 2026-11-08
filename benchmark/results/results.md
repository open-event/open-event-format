# Benchmark results

Machine: Intel(R) Xeon(R) Processor @ 2.10GHz (4 logical CPUs), Linux 6.18.44-fc-v310, rustc 1.96.0 (ac68faa20 2026-05-25)

Every format stores the same events. Query times are the median of 5 runs after one warm-up; write times are the median of 3 runs. All queries run on a single thread against the file held in memory. Every format returned identical results for every query (checked on each run).

## telemetry events: 10000 rows

| Format | Size (MiB) | Bytes per event | Size vs JSON | Write (ms) | Write (events/s) | Open (ms) | Open + first lookup (ms) |
|---|---:|---:|---:|---:|---:|---:|---:|
| oe | 0.47 | 49.5 | 20.2% | 37 | 273836 | 0.24 | 0.47 |
| oe (lazy open) | 0.47 | 49.5 | 20.2% | 27 | 373090 | 0.11 | 0.29 |
| parquet | 0.13 | 13.7 | 5.6% | 5 | 2029204 | 0.01 | 0.34 |
| vortex | 0.16 | 16.6 | 6.8% | 32 | 311785 | 0.51 | 0.88 |
| json | 2.34 | 245.1 | 100.0% | 12 | 833418 | 0.00 | 13.70 |
| json+zstd | 0.22 | 23.4 | 9.5% | 16 | 634065 | 0.00 | 15.70 |

| Query (median ms; entity_lookup is per lookup) | Rows matched | oe | oe (lazy open) | parquet | vortex | json | json+zstd |
|---|---:|---:|---:|---:|---:|---:|---:|
| scan_all_columns | 10000 | 3.01 | 2.32 | 1.47 | 0.90 | 20.15 | 20.17 |
| scan_one_number | 10000 | 0.02 | 0.02 | 0.19 | 0.03 | 13.04 | 14.81 |
| scan_one_payload_number | 10000 | 0.12 | 0.09 | 0.18 | 0.12 | 13.10 | 15.03 |
| scan_one_payload_text | 10000 | 0.10 | 0.07 | 0.09 | 0.06 | 13.66 | 15.58 |
| filter_type_and_payload | 12 | 0.11 | 0.08 | 0.47 | 0.28 | 14.33 | 16.12 |
| time_window_1pct | 100 | 0.03 | 0.02 | 0.28 | 0.11 | 13.31 | 15.23 |
| entity_lookup | 820 | 0.16 | 0.10 | 0.32 | 0.15 | 13.63 | 15.81 |

Removing events (redaction). OE records a deletion vector; the other formats rewrite the whole file.

| Case | Events removed | oe (ms) | oe (bytes written) | oe (lazy open) (ms) | oe (lazy open) (bytes written) | parquet (ms) | parquet (bytes written) | vortex (ms) | vortex (bytes written) | json (ms) | json (bytes written) | json+zstd (ms) | json+zstd (bytes written) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| redact_1_event | 1 | 0.00 | 9 | 0.00 | 9 | 9.73 | 134711 | 38.03 | 165968 | 33.29 | 2450649 | 50.43 | 233602 |
| redact_1pct_of_events | 100 | 0.05 | 405 | 0.03 | 405 | 9.95 | 133804 | 36.79 | 165504 | 33.45 | 2426385 | 45.86 | 231239 |

## telemetry events: 100000 rows

| Format | Size (MiB) | Bytes per event | Size vs JSON | Write (ms) | Write (events/s) | Open (ms) | Open + first lookup (ms) |
|---|---:|---:|---:|---:|---:|---:|---:|
| oe | 5.29 | 55.4 | 22.6% | 351 | 284991 | 1.49 | 3.12 |
| oe (lazy open) | 5.29 | 55.4 | 22.6% | 301 | 332669 | 0.81 | 3.93 |
| parquet | 1.11 | 11.6 | 4.7% | 45 | 2219683 | 0.04 | 2.83 |
| vortex | 1.76 | 18.5 | 7.5% | 141 | 707161 | 0.62 | 1.94 |
| json | 23.36 | 245.0 | 100.0% | 159 | 627091 | 0.00 | 144.53 |
| json+zstd | 2.36 | 24.7 | 10.1% | 195 | 512927 | 0.00 | 181.22 |

| Query (median ms; entity_lookup is per lookup) | Rows matched | oe | oe (lazy open) | parquet | vortex | json | json+zstd |
|---|---:|---:|---:|---:|---:|---:|---:|
| scan_all_columns | 100000 | 18.03 | 22.67 | 10.88 | 6.16 | 206.89 | 220.91 |
| scan_one_number | 100000 | 0.16 | 0.17 | 1.69 | 0.17 | 145.75 | 146.97 |
| scan_one_payload_number | 100000 | 0.70 | 0.90 | 1.12 | 0.71 | 137.04 | 182.10 |
| scan_one_payload_text | 100000 | 0.54 | 0.71 | 0.84 | 0.44 | 144.42 | 181.20 |
| filter_type_and_payload | 143 | 0.65 | 0.82 | 3.42 | 0.27 | 143.24 | 163.55 |
| time_window_1pct | 1000 | 0.04 | 0.06 | 2.12 | 0.10 | 141.69 | 180.65 |
| entity_lookup | 834 | 1.68 | 2.14 | 2.75 | 0.85 | 146.04 | 180.71 |

Removing events (redaction). OE records a deletion vector; the other formats rewrite the whole file.

| Case | Events removed | oe (ms) | oe (bytes written) | oe (lazy open) (ms) | oe (lazy open) (bytes written) | parquet (ms) | parquet (bytes written) | vortex (ms) | vortex (bytes written) | json (ms) | json (bytes written) | json+zstd (ms) | json+zstd (bytes written) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| redact_1_event | 1 | 0.00 | 9 | 0.00 | 9 | 96.16 | 1162107 | 147.67 | 1846136 | 398.36 | 24497827 | 472.43 | 2470339 |
| redact_1pct_of_events | 1000 | 0.78 | 4005 | 1.16 | 4005 | 94.95 | 1129279 | 180.10 | 1834576 | 353.62 | 24253096 | 483.58 | 2446064 |

## telemetry events: 1000000 rows

| Format | Size (MiB) | Bytes per event | Size vs JSON | Write (ms) | Write (events/s) | Open (ms) | Open + first lookup (ms) |
|---|---:|---:|---:|---:|---:|---:|---:|
| oe | 56.69 | 59.4 | 24.3% | 3578 | 279470 | 9.25 | 14.88 |
| oe (lazy open) | 56.69 | 59.4 | 24.3% | 3267 | 306064 | 6.20 | 20.91 |
| parquet | 11.73 | 12.3 | 5.0% | 461 | 2167874 | 0.09 | 28.96 |
| vortex | 18.63 | 19.5 | 8.0% | 1174 | 851854 | 0.84 | 7.83 |
| json | 233.58 | 244.9 | 100.0% | 1096 | 912388 | 6.45 | 1473.23 |
| json+zstd | 24.21 | 25.4 | 10.4% | 1719 | 581833 | 0.00 | 1673.31 |

| Query (median ms; entity_lookup is per lookup) | Rows matched | oe | oe (lazy open) | parquet | vortex | json | json+zstd |
|---|---:|---:|---:|---:|---:|---:|---:|
| scan_all_columns | 1000000 | 199.88 | 249.61 | 103.27 | 60.16 | 2217.44 | 2385.55 |
| scan_one_number | 1000000 | 1.88 | 1.96 | 16.27 | 1.67 | 1298.61 | 1682.83 |
| scan_one_payload_number | 1000000 | 9.22 | 9.18 | 10.80 | 8.58 | 1367.95 | 1652.61 |
| scan_one_payload_text | 1000000 | 7.11 | 7.38 | 7.94 | 4.62 | 1292.84 | 1635.53 |
| filter_type_and_payload | 1237 | 9.31 | 8.99 | 33.84 | 2.21 | 1426.72 | 1893.80 |
| time_window_1pct | 10000 | 0.12 | 0.12 | 2.79 | 0.53 | 1173.56 | 1711.95 |
| entity_lookup | 837 | 6.63 | 7.20 | 28.87 | 5.99 | 1403.30 | 1772.11 |

Removing events (redaction). OE records a deletion vector; the other formats rewrite the whole file.

| Case | Events removed | oe (ms) | oe (bytes written) | oe (lazy open) (ms) | oe (lazy open) (bytes written) | parquet (ms) | parquet (bytes written) | vortex (ms) | vortex (bytes written) | json (ms) | json (bytes written) | json+zstd (ms) | json+zstd (bytes written) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| redact_1_event | 1 | 0.00 | 9 | 0.00 | 9 | 944.35 | 12309274 | 1607.22 | 19543608 | 3715.38 | 244921417 | 4480.87 | 25386110 |
| redact_1pct_of_events | 10000 | 13.96 | 160005 | 14.00 | 160005 | 841.79 | 12020757 | 1590.45 | 19543040 | 3795.18 | 242472519 | 4675.23 | 25138782 |

## messages events: 10000 rows

| Format | Size (MiB) | Bytes per event | Size vs JSON | Write (ms) | Write (events/s) | Open (ms) | Open + first lookup (ms) |
|---|---:|---:|---:|---:|---:|---:|---:|
| oe | 0.82 | 85.5 | 26.2% | 39 | 258452 | 0.34 | 0.56 |
| oe (lazy open) | 0.82 | 85.5 | 26.2% | 38 | 264099 | 0.11 | 0.41 |
| parquet | 0.43 | 45.5 | 13.9% | 9 | 1130007 | 0.01 | 0.36 |
| vortex | 0.45 | 47.0 | 14.4% | 37 | 269826 | 0.50 | 0.81 |
| json | 3.12 | 326.9 | 100.0% | 11 | 880458 | 0.00 | 14.55 |
| json+zstd | 0.57 | 59.9 | 18.3% | 19 | 519118 | 0.00 | 16.70 |

| Query (median ms; entity_lookup is per lookup) | Rows matched | oe | oe (lazy open) | parquet | vortex | json | json+zstd |
|---|---:|---:|---:|---:|---:|---:|---:|
| scan_all_columns | 10000 | 3.93 | 3.19 | 2.20 | 0.87 | 20.91 | 33.11 |
| scan_one_number | 10000 | 0.02 | 0.01 | 0.13 | 0.02 | 13.95 | 20.93 |
| scan_one_payload_number | 10000 | 0.01 | 0.01 | 0.14 | 0.02 | 14.24 | 13.78 |
| scan_one_payload_text | 10000 | 1.53 | 1.23 | 0.98 | 0.22 | 15.04 | 14.29 |
| filter_type_and_payload | 265 | 0.08 | 0.06 | 0.44 | 0.18 | 15.20 | 18.87 |
| time_window_1pct | 100 | 0.01 | 0.01 | 0.27 | 0.12 | 14.32 | 17.83 |
| entity_lookup | 1751 | 0.12 | 0.10 | 0.26 | 0.11 | 12.00 | 17.95 |

Removing events (redaction). OE records a deletion vector; the other formats rewrite the whole file.

| Case | Events removed | oe (ms) | oe (bytes written) | oe (lazy open) (ms) | oe (lazy open) (bytes written) | parquet (ms) | parquet (bytes written) | vortex (ms) | vortex (bytes written) | json (ms) | json (bytes written) | json+zstd (ms) | json+zstd (bytes written) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| redact_1_event | 1 | 0.00 | 9 | 0.00 | 9 | 14.18 | 452866 | 48.90 | 473520 | 30.17 | 3268410 | 53.62 | 598999 |
| redact_1pct_of_events | 100 | 0.02 | 405 | 0.01 | 405 | 14.73 | 448727 | 48.66 | 466192 | 30.01 | 3235234 | 53.44 | 592934 |

## messages events: 100000 rows

| Format | Size (MiB) | Bytes per event | Size vs JSON | Write (ms) | Write (events/s) | Open (ms) | Open + first lookup (ms) |
|---|---:|---:|---:|---:|---:|---:|---:|
| oe | 8.70 | 91.2 | 27.9% | 389 | 257146 | 1.55 | 3.42 |
| oe (lazy open) | 8.70 | 91.2 | 27.9% | 368 | 271657 | 0.71 | 3.95 |
| parquet | 4.10 | 43.0 | 13.2% | 86 | 1167409 | 0.04 | 2.28 |
| vortex | 4.64 | 48.6 | 14.9% | 189 | 528741 | 0.55 | 1.34 |
| json | 31.13 | 326.4 | 100.0% | 129 | 775803 | 0.00 | 128.15 |
| json+zstd | 5.83 | 61.1 | 18.7% | 274 | 365281 | 0.00 | 159.38 |

| Query (median ms; entity_lookup is per lookup) | Rows matched | oe | oe (lazy open) | parquet | vortex | json | json+zstd |
|---|---:|---:|---:|---:|---:|---:|---:|
| scan_all_columns | 100000 | 41.18 | 32.09 | 24.76 | 9.68 | 228.94 | 291.60 |
| scan_one_number | 100000 | 0.17 | 0.14 | 1.29 | 0.30 | 154.56 | 223.92 |
| scan_one_payload_number | 100000 | 0.13 | 0.10 | 1.11 | 0.34 | 148.07 | 199.76 |
| scan_one_payload_text | 100000 | 12.24 | 12.38 | 8.85 | 2.93 | 166.07 | 208.59 |
| filter_type_and_payload | 2655 | 0.63 | 0.63 | 3.55 | 0.44 | 182.04 | 197.73 |
| time_window_1pct | 1000 | 0.01 | 0.01 | 2.42 | 0.12 | 161.47 | 206.98 |
| entity_lookup | 1203 | 1.63 | 1.59 | 2.20 | 0.79 | 140.98 | 195.59 |

Removing events (redaction). OE records a deletion vector; the other formats rewrite the whole file.

| Case | Events removed | oe (ms) | oe (bytes written) | oe (lazy open) (ms) | oe (lazy open) (bytes written) | parquet (ms) | parquet (bytes written) | vortex (ms) | vortex (bytes written) | json (ms) | json (bytes written) | json+zstd (ms) | json+zstd (bytes written) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| redact_1_event | 1 | 0.00 | 9 | 0.00 | 9 | 146.02 | 4300907 | 215.99 | 4860768 | 332.59 | 32636819 | 525.64 | 6111549 |
| redact_1pct_of_events | 1000 | 1.04 | 4005 | 0.99 | 4005 | 149.48 | 4232767 | 247.45 | 4816112 | 360.34 | 32309495 | 487.76 | 6051823 |

## messages events: 1000000 rows

| Format | Size (MiB) | Bytes per event | Size vs JSON | Write (ms) | Write (events/s) | Open (ms) | Open + first lookup (ms) |
|---|---:|---:|---:|---:|---:|---:|---:|
| oe | 90.67 | 95.1 | 29.1% | 4500 | 222234 | 14.17 | 14.77 |
| oe (lazy open) | 90.67 | 95.1 | 29.1% | 4611 | 216885 | 9.12 | 26.89 |
| parquet | 41.65 | 43.7 | 13.4% | 1226 | 815690 | 0.14 | 29.72 |
| vortex | 47.32 | 49.6 | 15.2% | 1823 | 548421 | 0.66 | 8.37 |
| json | 311.21 | 326.3 | 100.0% | 1383 | 722929 | 7.42 | 1533.28 |
| json+zstd | 58.99 | 61.9 | 19.0% | 2801 | 357063 | 0.00 | 1982.15 |

| Query (median ms; entity_lookup is per lookup) | Rows matched | oe | oe (lazy open) | parquet | vortex | json | json+zstd |
|---|---:|---:|---:|---:|---:|---:|---:|
| scan_all_columns | 1000000 | 432.80 | 449.04 | 239.33 | 103.89 | 2636.23 | 3299.44 |
| scan_one_number | 1000000 | 1.57 | 1.88 | 15.55 | 1.81 | 1412.32 | 1927.14 |
| scan_one_payload_number | 1000000 | 1.22 | 1.50 | 13.71 | 1.63 | 1497.75 | 1997.46 |
| scan_one_payload_text | 1000000 | 150.72 | 172.95 | 104.67 | 29.06 | 1653.06 | 2234.01 |
| filter_type_and_payload | 26269 | 8.21 | 14.63 | 37.26 | 3.58 | 1642.10 | 1924.47 |
| time_window_1pct | 10000 | 0.04 | 0.04 | 3.23 | 0.40 | 1598.66 | 1838.19 |
| entity_lookup | 5411 | 6.01 | 5.90 | 29.91 | 7.22 | 1496.41 | 2009.99 |

Removing events (redaction). OE records a deletion vector; the other formats rewrite the whole file.

| Case | Events removed | oe (ms) | oe (bytes written) | oe (lazy open) (ms) | oe (lazy open) (bytes written) | parquet (ms) | parquet (bytes written) | vortex (ms) | vortex (bytes written) | json (ms) | json (bytes written) | json+zstd (ms) | json+zstd (bytes written) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| redact_1_event | 1 | 0.00 | 9 | 0.00 | 9 | 1820.69 | 43671434 | 2241.54 | 49602912 | 4744.15 | 326324891 | 6448.84 | 61854744 |
| redact_1pct_of_events | 10000 | 12.68 | 160005 | 19.67 | 160005 | 1852.85 | 43058471 | 2388.79 | 49314872 | 4754.84 | 323067360 | 6319.51 | 61243459 |

## chat events: 10000 rows

| Format | Size (MiB) | Bytes per event | Size vs JSON | Write (ms) | Write (events/s) | Open (ms) | Open + first lookup (ms) |
|---|---:|---:|---:|---:|---:|---:|---:|
| oe | 0.76 | 80.0 | 23.0% | 40 | 247592 | 0.22 | 0.50 |
| oe (lazy open) | 0.76 | 80.0 | 23.0% | 39 | 253232 | 0.10 | 0.53 |
| parquet | 0.36 | 37.3 | 10.7% | 11 | 940425 | 0.01 | 0.48 |
| vortex | 0.42 | 43.6 | 12.5% | 53 | 190351 | 0.53 | 0.82 |
| json | 3.32 | 348.4 | 100.0% | 11 | 889395 | 0.00 | 22.79 |
| json+zstd | 0.49 | 51.1 | 14.7% | 19 | 534097 | 0.00 | 15.62 |

| Query (median ms; entity_lookup is per lookup) | Rows matched | oe | oe (lazy open) | parquet | vortex | json | json+zstd |
|---|---:|---:|---:|---:|---:|---:|---:|
| scan_all_columns | 10000 | 4.04 | 4.12 | 2.58 | 0.99 | 21.24 | 20.76 |
| scan_one_number | 10000 | 0.07 | 0.07 | 0.17 | 0.03 | 14.41 | 18.01 |
| scan_one_payload_number | 10000 | 0.01 | 0.01 | 0.07 | 0.03 | 14.33 | 25.65 |
| scan_one_payload_text | 10000 | 1.44 | 1.44 | 1.10 | 0.23 | 15.34 | 14.80 |
| filter_type_and_payload | 266 | 0.13 | 0.13 | 0.38 | 0.19 | 15.11 | 14.65 |
| time_window_1pct | 100 | 0.04 | 0.04 | 0.23 | 0.09 | 14.54 | 14.34 |
| entity_lookup | 36977 | 0.21 | 0.20 | 0.35 | 0.14 | 16.60 | 14.48 |
| latest_50_in_room | 29391 | 0.27 | 0.28 | 0.54 | 0.23 | 15.80 | 14.83 |
| thread_replies | 21 | 0.10 | 0.10 | 0.36 | 0.18 | 14.90 | 14.33 |
| event_by_id | 20 | 0.03 | 0.03 | 0.56 | 0.16 | 16.14 | 15.07 |

Removing events (redaction). OE records a deletion vector; the other formats rewrite the whole file.

| Case | Events removed | oe (ms) | oe (bytes written) | oe (lazy open) (ms) | oe (lazy open) (bytes written) | parquet (ms) | parquet (bytes written) | vortex (ms) | vortex (bytes written) | json (ms) | json (bytes written) | json+zstd (ms) | json+zstd (bytes written) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| redact_1_event | 1 | 0.00 | 9 | 0.00 | 9 | 17.71 | 370690 | 47.94 | 433524 | 39.41 | 3483690 | 54.46 | 511242 |
| redact_1pct_of_events | 100 | 0.05 | 405 | 0.02 | 405 | 17.71 | 367096 | 46.52 | 434356 | 33.79 | 3450121 | 54.26 | 506512 |

## chat events: 100000 rows

| Format | Size (MiB) | Bytes per event | Size vs JSON | Write (ms) | Write (events/s) | Open (ms) | Open + first lookup (ms) |
|---|---:|---:|---:|---:|---:|---:|---:|
| oe | 7.83 | 82.1 | 23.5% | 408 | 245233 | 2.01 | 5.14 |
| oe (lazy open) | 7.83 | 82.1 | 23.5% | 361 | 276752 | 1.11 | 5.86 |
| parquet | 3.41 | 35.8 | 10.2% | 99 | 1008994 | 0.05 | 3.02 |
| vortex | 4.18 | 43.9 | 12.5% | 241 | 415463 | 0.62 | 1.07 |
| json | 33.34 | 349.5 | 100.0% | 148 | 675822 | 0.00 | 159.19 |
| json+zstd | 5.10 | 53.5 | 15.3% | 279 | 358625 | 0.00 | 201.17 |

| Query (median ms; entity_lookup is per lookup) | Rows matched | oe | oe (lazy open) | parquet | vortex | json | json+zstd |
|---|---:|---:|---:|---:|---:|---:|---:|
| scan_all_columns | 100000 | 41.62 | 38.91 | 23.46 | 9.29 | 204.05 | 290.99 |
| scan_one_number | 100000 | 0.73 | 0.71 | 1.30 | 0.22 | 160.27 | 192.80 |
| scan_one_payload_number | 100000 | 0.13 | 0.13 | 0.30 | 0.15 | 160.90 | 193.37 |
| scan_one_payload_text | 100000 | 14.64 | 14.39 | 8.49 | 2.70 | 167.14 | 169.75 |
| filter_type_and_payload | 2904 | 1.25 | 1.25 | 2.99 | 0.38 | 164.16 | 167.53 |
| time_window_1pct | 1000 | 0.04 | 0.04 | 1.74 | 0.12 | 150.61 | 203.33 |
| entity_lookup | 81077 | 2.05 | 2.04 | 2.53 | 0.32 | 158.99 | 200.82 |
| latest_50_in_room | 64860 | 2.74 | 2.60 | 3.70 | 0.51 | 136.27 | 210.59 |
| thread_replies | 23 | 0.12 | 0.12 | 2.84 | 0.49 | 166.10 | 204.54 |
| event_by_id | 20 | 0.24 | 0.24 | 3.62 | 0.33 | 169.00 | 201.25 |

Removing events (redaction). OE records a deletion vector; the other formats rewrite the whole file.

| Case | Events removed | oe (ms) | oe (bytes written) | oe (lazy open) (ms) | oe (lazy open) (bytes written) | parquet (ms) | parquet (bytes written) | vortex (ms) | vortex (bytes written) | json (ms) | json (bytes written) | json+zstd (ms) | json+zstd (bytes written) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| redact_1_event | 1 | 0.00 | 9 | 0.00 | 9 | 149.52 | 3580849 | 300.30 | 4381044 | 436.56 | 34954335 | 570.64 | 5350605 |
| redact_1pct_of_events | 1000 | 0.93 | 4005 | 0.88 | 4005 | 178.90 | 3521126 | 297.50 | 4330708 | 440.05 | 34607634 | 531.80 | 5298958 |

## chat events: 1000000 rows

| Format | Size (MiB) | Bytes per event | Size vs JSON | Write (ms) | Write (events/s) | Open (ms) | Open + first lookup (ms) |
|---|---:|---:|---:|---:|---:|---:|---:|
| oe | 84.93 | 89.1 | 25.4% | 4394 | 227563 | 13.43 | 17.78 |
| oe (lazy open) | 84.93 | 89.1 | 25.4% | 4517 | 221375 | 7.78 | 26.31 |
| parquet | 35.96 | 37.7 | 10.8% | 1189 | 841186 | 0.15 | 29.69 |
| vortex | 45.86 | 48.1 | 13.7% | 2131 | 469246 | 0.78 | 2.41 |
| json | 334.22 | 350.5 | 100.0% | 1552 | 644263 | 8.66 | 1599.10 |
| json+zstd | 52.90 | 55.5 | 15.8% | 2703 | 369934 | 0.00 | 2137.66 |

| Query (median ms; entity_lookup is per lookup) | Rows matched | oe | oe (lazy open) | parquet | vortex | json | json+zstd |
|---|---:|---:|---:|---:|---:|---:|---:|
| scan_all_columns | 1000000 | 474.60 | 479.42 | 238.12 | 94.86 | 2624.23 | 3028.81 |
| scan_one_number | 1000000 | 5.93 | 7.56 | 12.87 | 1.59 | 1525.75 | 2038.83 |
| scan_one_payload_number | 1000000 | 1.09 | 1.40 | 3.16 | 1.32 | 1626.35 | 2021.81 |
| scan_one_payload_text | 1000000 | 114.91 | 144.80 | 87.91 | 22.94 | 1529.03 | 1991.96 |
| filter_type_and_payload | 30248 | 10.54 | 12.29 | 36.71 | 2.48 | 1626.77 | 1915.38 |
| time_window_1pct | 10000 | 0.10 | 0.11 | 2.97 | 0.41 | 1684.31 | 1939.91 |
| entity_lookup | 200587 | 31.11 | 31.95 | 24.70 | 1.55 | 1576.80 | 2046.84 |
| latest_50_in_room | 160250 | 36.41 | 36.24 | 45.48 | 2.09 | 1668.65 | 2116.98 |
| thread_replies | 22 | 0.36 | 0.39 | 18.18 | 2.69 | 1598.66 | 1971.11 |
| event_by_id | 20 | 0.28 | 0.29 | 6.19 | 0.61 | 1642.52 | 2094.17 |

Removing events (redaction). OE records a deletion vector; the other formats rewrite the whole file.

| Case | Events removed | oe (ms) | oe (bytes written) | oe (lazy open) (ms) | oe (lazy open) (bytes written) | parquet (ms) | parquet (bytes written) | vortex (ms) | vortex (bytes written) | json (ms) | json (bytes written) | json+zstd (ms) | json+zstd (bytes written) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| redact_1_event | 1 | 0.00 | 9 | 0.00 | 9 | 2020.54 | 37698174 | 2639.85 | 48035456 | 4965.75 | 350453613 | 6375.02 | 55469073 |
| redact_1pct_of_events | 10000 | 15.24 | 160005 | 12.69 | 160005 | 2016.47 | 37167023 | 2731.93 | 47241092 | 4797.65 | 346953322 | 6401.09 | 54925975 |

## Durable ingest

Each batch is acknowledged only after the file is synced to disk (300 batches per row). Parquet and Vortex cannot append, so each batch is a new file.

| Batch size | Format | Events/s | Ack p50 (ms) | Ack p99 (ms) | Bytes on disk |
|---:|---|---:|---:|---:|---:|
| 1 | oej journal | 3554 | 0.264 | 0.485 | 1228800 |
| 1 | json (append) | 5814 | 0.154 | 0.371 | 104659 |
| 1 | parquet (file per batch) | 1084 | 0.879 | 1.648 | 1094080 |
| 1 | vortex (file per batch) | 567 | 1.713 | 2.684 | 3131136 |
| 16 | oej journal | 42423 | 0.331 | 0.934 | 2457600 |
| 16 | json (append) | 51693 | 0.291 | 0.620 | 1664629 |
| 16 | parquet (file per batch) | 16567 | 0.930 | 1.440 | 1373373 |
| 16 | vortex (file per batch) | 1155 | 13.865 | 21.079 | 4804864 |
| 128 | oej journal | 263552 | 0.464 | 0.844 | 19660800 |
| 128 | json (append) | 244529 | 0.499 | 0.930 | 13400243 |
| 128 | parquet (file per batch) | 109628 | 1.148 | 1.575 | 2851044 |
| 128 | vortex (file per batch) | 6549 | 19.761 | 23.900 | 7595608 |

