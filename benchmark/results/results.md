# Benchmark results

Machine: Intel(R) Xeon(R) Processor @ 2.10GHz (4 logical CPUs), Linux 6.18.44-fc-v310, rustc 1.96.0 (ac68faa20 2026-05-25)

Every format stores the same events. Query times are the median of 5 runs after one warm-up; write times are the median of 3 runs. All queries run on a single thread against the file held in memory. Every format returned identical results for every query (checked on each run).

## telemetry events: 10000 rows

| Format | Size (MiB) | Bytes per event | Size vs JSON | Write (ms) | Write (events/s) | Open (ms) | Open + first lookup (ms) |
|---|---:|---:|---:|---:|---:|---:|---:|
| oe | 0.17 | 17.7 | 7.2% | 33 | 302153 | 0.11 | 0.12 |
| oe (lazy open) | 0.17 | 17.7 | 7.2% | 26 | 385767 | 0.10 | 0.14 |
| parquet | 0.13 | 13.7 | 5.6% | 4 | 2399255 | 0.01 | 0.26 |
| vortex | 0.16 | 16.6 | 6.8% | 35 | 283483 | 0.62 | 1.01 |
| json | 2.34 | 245.1 | 100.0% | 12 | 863997 | 0.00 | 13.66 |
| json+zstd | 0.22 | 23.4 | 9.5% | 15 | 677252 | 0.00 | 15.33 |

| Query (median ms; entity_lookup is per lookup) | Rows matched | oe | oe (lazy open) | parquet | vortex | json | json+zstd |
|---|---:|---:|---:|---:|---:|---:|---:|
| scan_all_columns | 10000 | 0.58 | 0.64 | 1.19 | 0.86 | 18.41 | 19.62 |
| scan_one_number | 10000 | 0.02 | 0.02 | 0.14 | 0.03 | 12.85 | 13.54 |
| scan_one_payload_number | 10000 | 0.08 | 0.08 | 0.14 | 0.08 | 13.38 | 12.30 |
| scan_one_payload_text | 10000 | 0.06 | 0.07 | 0.08 | 0.05 | 16.40 | 14.45 |
| filter_type_and_payload | 12 | 0.06 | 0.06 | 0.40 | 0.24 | 17.84 | 14.23 |
| time_window_1pct | 100 | 0.02 | 0.02 | 0.23 | 0.13 | 10.85 | 12.38 |
| entity_lookup | 820 | 0.01 | 0.01 | 0.25 | 0.17 | 12.48 | 12.66 |

Removing events (redaction). OE records a deletion vector; the other formats rewrite the whole file.

| Case | Events removed | oe (ms) | oe (bytes written) | oe (lazy open) (ms) | oe (lazy open) (bytes written) | parquet (ms) | parquet (bytes written) | vortex (ms) | vortex (bytes written) | json (ms) | json (bytes written) | json+zstd (ms) | json+zstd (bytes written) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| redact_1_event | 1 | 0.00 | 9 | 0.00 | 9 | 8.09 | 134711 | 31.49 | 165968 | 31.36 | 2450649 | 35.92 | 233602 |
| redact_1pct_of_events | 100 | 0.02 | 405 | 0.02 | 405 | 8.40 | 133804 | 38.06 | 165504 | 30.52 | 2426385 | 32.42 | 231239 |

## telemetry events: 100000 rows

| Format | Size (MiB) | Bytes per event | Size vs JSON | Write (ms) | Write (events/s) | Open (ms) | Open + first lookup (ms) |
|---|---:|---:|---:|---:|---:|---:|---:|
| oe | 1.55 | 16.2 | 6.6% | 365 | 273928 | 1.12 | 1.28 |
| oe (lazy open) | 1.55 | 16.2 | 6.6% | 369 | 270970 | 0.79 | 0.96 |
| parquet | 1.11 | 11.6 | 4.7% | 43 | 2310182 | 0.04 | 2.81 |
| vortex | 1.76 | 18.5 | 7.5% | 109 | 919013 | 0.49 | 1.47 |
| json | 23.36 | 245.0 | 100.0% | 103 | 973592 | 0.00 | 120.34 |
| json+zstd | 2.36 | 24.7 | 10.1% | 178 | 563268 | 0.00 | 169.68 |

| Query (median ms; entity_lookup is per lookup) | Rows matched | oe | oe (lazy open) | parquet | vortex | json | json+zstd |
|---|---:|---:|---:|---:|---:|---:|---:|
| scan_all_columns | 100000 | 5.46 | 5.08 | 10.91 | 5.92 | 210.34 | 219.56 |
| scan_one_number | 100000 | 0.17 | 0.16 | 1.61 | 0.14 | 104.74 | 138.23 |
| scan_one_payload_number | 100000 | 0.95 | 0.87 | 1.08 | 0.68 | 140.90 | 143.28 |
| scan_one_payload_text | 100000 | 0.67 | 0.64 | 0.79 | 0.45 | 113.03 | 158.34 |
| filter_type_and_payload | 143 | 0.88 | 0.82 | 3.38 | 0.27 | 135.87 | 162.25 |
| time_window_1pct | 1000 | 0.05 | 0.05 | 2.08 | 0.14 | 129.62 | 141.57 |
| entity_lookup | 834 | 0.14 | 0.13 | 2.64 | 0.77 | 141.21 | 148.14 |

Removing events (redaction). OE records a deletion vector; the other formats rewrite the whole file.

| Case | Events removed | oe (ms) | oe (bytes written) | oe (lazy open) (ms) | oe (lazy open) (bytes written) | parquet (ms) | parquet (bytes written) | vortex (ms) | vortex (bytes written) | json (ms) | json (bytes written) | json+zstd (ms) | json+zstd (bytes written) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| redact_1_event | 1 | 0.00 | 9 | 0.00 | 9 | 93.07 | 1162107 | 145.40 | 1846136 | 337.54 | 24497827 | 384.93 | 2470339 |
| redact_1pct_of_events | 1000 | 1.00 | 4005 | 0.84 | 4005 | 94.42 | 1129279 | 149.51 | 1834576 | 341.61 | 24253096 | 429.38 | 2446064 |

## telemetry events: 1000000 rows

| Format | Size (MiB) | Bytes per event | Size vs JSON | Write (ms) | Write (events/s) | Open (ms) | Open + first lookup (ms) |
|---|---:|---:|---:|---:|---:|---:|---:|
| oe | 16.22 | 17.0 | 6.9% | 4039 | 247606 | 10.70 | 10.51 |
| oe (lazy open) | 16.22 | 17.0 | 6.9% | 3962 | 252414 | 7.78 | 11.53 |
| parquet | 11.73 | 12.3 | 5.0% | 515 | 1940038 | 0.10 | 31.38 |
| vortex | 18.63 | 19.5 | 8.0% | 1126 | 888020 | 0.65 | 5.48 |
| json | 233.58 | 244.9 | 100.0% | 1167 | 857222 | 5.59 | 1364.29 |
| json+zstd | 24.21 | 25.4 | 10.4% | 1722 | 580731 | 0.00 | 1675.34 |

| Query (median ms; entity_lookup is per lookup) | Rows matched | oe | oe (lazy open) | parquet | vortex | json | json+zstd |
|---|---:|---:|---:|---:|---:|---:|---:|
| scan_all_columns | 1000000 | 57.70 | 73.73 | 116.09 | 75.90 | 2130.67 | 2378.17 |
| scan_one_number | 1000000 | 1.55 | 1.90 | 16.36 | 1.90 | 1187.90 | 1436.66 |
| scan_one_payload_number | 1000000 | 8.97 | 12.35 | 8.98 | 9.11 | 1353.60 | 1581.65 |
| scan_one_payload_text | 1000000 | 6.36 | 10.94 | 6.79 | 4.97 | 1270.28 | 1635.81 |
| filter_type_and_payload | 1237 | 9.21 | 11.84 | 29.79 | 2.33 | 1384.30 | 1664.53 |
| time_window_1pct | 10000 | 0.12 | 0.10 | 2.32 | 0.63 | 1311.71 | 1517.88 |
| entity_lookup | 837 | 0.38 | 0.37 | 24.99 | 5.30 | 1377.27 | 1634.84 |

Removing events (redaction). OE records a deletion vector; the other formats rewrite the whole file.

| Case | Events removed | oe (ms) | oe (bytes written) | oe (lazy open) (ms) | oe (lazy open) (bytes written) | parquet (ms) | parquet (bytes written) | vortex (ms) | vortex (bytes written) | json (ms) | json (bytes written) | json+zstd (ms) | json+zstd (bytes written) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| redact_1_event | 1 | 0.00 | 9 | 0.00 | 9 | 895.35 | 12309274 | 1640.87 | 19543608 | 3618.62 | 244921417 | 4439.10 | 25386110 |
| redact_1pct_of_events | 10000 | 14.68 | 160005 | 19.22 | 160005 | 989.88 | 12020757 | 1513.67 | 19543040 | 3682.55 | 242472519 | 4221.73 | 25138782 |

## messages events: 10000 rows

| Format | Size (MiB) | Bytes per event | Size vs JSON | Write (ms) | Write (events/s) | Open (ms) | Open + first lookup (ms) |
|---|---:|---:|---:|---:|---:|---:|---:|
| oe | 0.53 | 55.4 | 17.0% | 54 | 186512 | 0.23 | 0.30 |
| oe (lazy open) | 0.53 | 55.4 | 17.0% | 49 | 205684 | 0.09 | 0.19 |
| parquet | 0.43 | 45.5 | 13.9% | 12 | 806768 | 0.02 | 0.39 |
| vortex | 0.45 | 47.0 | 14.4% | 41 | 242762 | 0.44 | 0.74 |
| json | 3.12 | 326.9 | 100.0% | 10 | 987865 | 0.00 | 11.96 |
| json+zstd | 0.57 | 59.9 | 18.3% | 18 | 542529 | 0.00 | 14.49 |

| Query (median ms; entity_lookup is per lookup) | Rows matched | oe | oe (lazy open) | parquet | vortex | json | json+zstd |
|---|---:|---:|---:|---:|---:|---:|---:|
| scan_all_columns | 10000 | 1.13 | 1.01 | 2.80 | 0.98 | 16.64 | 18.85 |
| scan_one_number | 10000 | 0.01 | 0.02 | 0.17 | 0.05 | 10.71 | 13.56 |
| scan_one_payload_number | 10000 | 0.01 | 0.01 | 0.18 | 0.02 | 13.76 | 14.03 |
| scan_one_payload_text | 10000 | 0.29 | 0.36 | 1.22 | 0.31 | 17.99 | 14.32 |
| filter_type_and_payload | 265 | 0.08 | 0.07 | 0.56 | 0.18 | 12.71 | 14.49 |
| time_window_1pct | 100 | 0.01 | 0.01 | 0.36 | 0.12 | 11.61 | 13.65 |
| entity_lookup | 1751 | 0.02 | 0.02 | 0.32 | 0.12 | 11.30 | 14.45 |

Removing events (redaction). OE records a deletion vector; the other formats rewrite the whole file.

| Case | Events removed | oe (ms) | oe (bytes written) | oe (lazy open) (ms) | oe (lazy open) (bytes written) | parquet (ms) | parquet (bytes written) | vortex (ms) | vortex (bytes written) | json (ms) | json (bytes written) | json+zstd (ms) | json+zstd (bytes written) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| redact_1_event | 1 | 0.00 | 9 | 0.00 | 9 | 18.86 | 452866 | 42.74 | 473520 | 30.06 | 3268410 | 42.17 | 598999 |
| redact_1pct_of_events | 100 | 0.02 | 405 | 0.02 | 405 | 14.99 | 448727 | 43.10 | 466192 | 29.57 | 3235234 | 42.02 | 592934 |

## messages events: 100000 rows

| Format | Size (MiB) | Bytes per event | Size vs JSON | Write (ms) | Write (events/s) | Open (ms) | Open + first lookup (ms) |
|---|---:|---:|---:|---:|---:|---:|---:|
| oe | 5.14 | 53.9 | 16.5% | 413 | 242381 | 1.20 | 1.44 |
| oe (lazy open) | 5.14 | 53.9 | 16.5% | 461 | 216873 | 0.79 | 2.72 |
| parquet | 4.10 | 43.0 | 13.2% | 111 | 899271 | 0.05 | 2.55 |
| vortex | 4.64 | 48.6 | 14.9% | 167 | 599826 | 0.60 | 1.71 |
| json | 31.13 | 326.4 | 100.0% | 124 | 805816 | 0.00 | 150.64 |
| json+zstd | 5.83 | 61.1 | 18.7% | 259 | 385682 | 0.00 | 191.99 |

| Query (median ms; entity_lookup is per lookup) | Rows matched | oe | oe (lazy open) | parquet | vortex | json | json+zstd |
|---|---:|---:|---:|---:|---:|---:|---:|
| scan_all_columns | 100000 | 9.21 | 9.75 | 23.82 | 9.18 | 197.40 | 283.06 |
| scan_one_number | 100000 | 0.16 | 0.18 | 1.59 | 0.23 | 114.61 | 151.03 |
| scan_one_payload_number | 100000 | 0.12 | 0.13 | 1.42 | 0.17 | 116.49 | 185.08 |
| scan_one_payload_text | 100000 | 2.92 | 3.93 | 11.50 | 2.37 | 131.13 | 176.33 |
| filter_type_and_payload | 2655 | 0.62 | 0.90 | 4.56 | 0.43 | 134.62 | 196.47 |
| time_window_1pct | 1000 | 0.01 | 0.01 | 3.05 | 0.10 | 139.74 | 156.38 |
| entity_lookup | 1203 | 0.09 | 0.12 | 2.95 | 0.89 | 154.49 | 179.44 |

Removing events (redaction). OE records a deletion vector; the other formats rewrite the whole file.

| Case | Events removed | oe (ms) | oe (bytes written) | oe (lazy open) (ms) | oe (lazy open) (bytes written) | parquet (ms) | parquet (bytes written) | vortex (ms) | vortex (bytes written) | json (ms) | json (bytes written) | json+zstd (ms) | json+zstd (bytes written) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| redact_1_event | 1 | 0.00 | 9 | 0.00 | 9 | 192.82 | 4300907 | 257.21 | 4860768 | 411.33 | 32636819 | 497.11 | 6111549 |
| redact_1pct_of_events | 1000 | 0.78 | 4005 | 0.91 | 4005 | 189.48 | 4232767 | 242.21 | 4816112 | 368.61 | 32309495 | 510.39 | 6051823 |

## messages events: 1000000 rows

| Format | Size (MiB) | Bytes per event | Size vs JSON | Write (ms) | Write (events/s) | Open (ms) | Open + first lookup (ms) |
|---|---:|---:|---:|---:|---:|---:|---:|
| oe | 52.07 | 54.6 | 16.7% | 5890 | 169785 | 10.38 | 11.43 |
| oe (lazy open) | 52.07 | 54.6 | 16.7% | 5721 | 174800 | 8.97 | 17.54 |
| parquet | 41.65 | 43.7 | 13.4% | 1111 | 900001 | 0.13 | 28.97 |
| vortex | 47.32 | 49.6 | 15.2% | 1693 | 590795 | 0.78 | 7.21 |
| json | 311.21 | 326.3 | 100.0% | 1359 | 735742 | 6.91 | 1379.09 |
| json+zstd | 58.99 | 61.9 | 19.0% | 2475 | 404033 | 0.00 | 1825.79 |

| Query (median ms; entity_lookup is per lookup) | Rows matched | oe | oe (lazy open) | parquet | vortex | json | json+zstd |
|---|---:|---:|---:|---:|---:|---:|---:|
| scan_all_columns | 1000000 | 82.74 | 97.63 | 255.93 | 109.30 | 2591.48 | 3142.33 |
| scan_one_number | 1000000 | 1.46 | 1.55 | 13.42 | 1.99 | 1405.37 | 1793.40 |
| scan_one_payload_number | 1000000 | 1.11 | 1.17 | 11.40 | 1.65 | 1291.62 | 1896.14 |
| scan_one_payload_text | 1000000 | 38.68 | 32.73 | 120.68 | 28.46 | 1553.80 | 2184.24 |
| filter_type_and_payload | 26269 | 8.07 | 8.05 | 44.17 | 2.88 | 1437.18 | 2043.20 |
| time_window_1pct | 10000 | 0.04 | 0.05 | 3.87 | 0.54 | 1545.04 | 1978.31 |
| entity_lookup | 5411 | 0.32 | 0.38 | 28.80 | 5.67 | 1473.94 | 1838.44 |

Removing events (redaction). OE records a deletion vector; the other formats rewrite the whole file.

| Case | Events removed | oe (ms) | oe (bytes written) | oe (lazy open) (ms) | oe (lazy open) (bytes written) | parquet (ms) | parquet (bytes written) | vortex (ms) | vortex (bytes written) | json (ms) | json (bytes written) | json+zstd (ms) | json+zstd (bytes written) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| redact_1_event | 1 | 0.00 | 9 | 0.00 | 9 | 1909.07 | 43671434 | 2128.64 | 49602912 | 4373.29 | 326324891 | 6086.03 | 61854744 |
| redact_1pct_of_events | 10000 | 13.91 | 160005 | 14.42 | 160005 | 1841.29 | 43058471 | 2220.20 | 49314872 | 4493.08 | 323067360 | 6081.82 | 61243459 |

## chat events: 10000 rows

| Format | Size (MiB) | Bytes per event | Size vs JSON | Write (ms) | Write (events/s) | Open (ms) | Open + first lookup (ms) |
|---|---:|---:|---:|---:|---:|---:|---:|
| oe | 0.49 | 51.6 | 14.8% | 40 | 252485 | 0.18 | 0.34 |
| oe (lazy open) | 0.49 | 51.6 | 14.8% | 40 | 248863 | 0.09 | 0.26 |
| parquet | 0.36 | 37.3 | 10.7% | 9 | 1157951 | 0.01 | 0.39 |
| vortex | 0.42 | 43.6 | 12.5% | 42 | 236843 | 0.42 | 0.78 |
| json | 3.32 | 348.4 | 100.0% | 18 | 561443 | 0.00 | 15.33 |
| json+zstd | 0.49 | 51.1 | 14.7% | 22 | 445181 | 0.00 | 19.43 |

| Query (median ms; entity_lookup is per lookup) | Rows matched | oe | oe (lazy open) | parquet | vortex | json | json+zstd |
|---|---:|---:|---:|---:|---:|---:|---:|
| scan_all_columns | 10000 | 1.09 | 0.87 | 2.07 | 0.96 | 20.82 | 22.71 |
| scan_one_number | 10000 | 0.03 | 0.02 | 0.14 | 0.03 | 14.63 | 17.87 |
| scan_one_payload_number | 10000 | 0.01 | 0.01 | 0.05 | 0.03 | 14.37 | 14.99 |
| scan_one_payload_text | 10000 | 0.31 | 0.25 | 0.86 | 0.23 | 15.45 | 18.37 |
| filter_type_and_payload | 266 | 0.07 | 0.06 | 0.30 | 0.16 | 15.16 | 14.83 |
| time_window_1pct | 100 | 0.02 | 0.01 | 0.18 | 0.09 | 15.66 | 13.98 |
| entity_lookup | 36977 | 0.07 | 0.06 | 0.27 | 0.14 | 15.03 | 18.20 |
| latest_50_in_room | 29391 | 0.08 | 0.06 | 0.42 | 0.24 | 13.61 | 18.21 |
| thread_replies | 21 | 0.07 | 0.05 | 0.28 | 0.16 | 14.10 | 14.45 |
| event_by_id | 20 | 0.01 | 0.00 | 0.43 | 0.15 | 16.61 | 15.11 |

Removing events (redaction). OE records a deletion vector; the other formats rewrite the whole file.

| Case | Events removed | oe (ms) | oe (bytes written) | oe (lazy open) (ms) | oe (lazy open) (bytes written) | parquet (ms) | parquet (bytes written) | vortex (ms) | vortex (bytes written) | json (ms) | json (bytes written) | json+zstd (ms) | json+zstd (bytes written) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| redact_1_event | 1 | 0.00 | 9 | 0.00 | 9 | 14.81 | 370690 | 50.99 | 433524 | 30.21 | 3483690 | 46.84 | 511242 |
| redact_1pct_of_events | 100 | 0.03 | 405 | 0.02 | 405 | 14.55 | 367096 | 52.96 | 434356 | 37.36 | 3450121 | 82.94 | 506512 |

## chat events: 100000 rows

| Format | Size (MiB) | Bytes per event | Size vs JSON | Write (ms) | Write (events/s) | Open (ms) | Open + first lookup (ms) |
|---|---:|---:|---:|---:|---:|---:|---:|
| oe | 5.09 | 53.4 | 15.3% | 441 | 226548 | 1.47 | 1.70 |
| oe (lazy open) | 5.09 | 53.4 | 15.3% | 451 | 221821 | 0.73 | 1.91 |
| parquet | 3.41 | 35.8 | 10.2% | 99 | 1008433 | 0.09 | 3.98 |
| vortex | 4.18 | 43.9 | 12.5% | 197 | 507862 | 0.59 | 1.00 |
| json | 33.34 | 349.5 | 100.0% | 109 | 916397 | 0.00 | 148.43 |
| json+zstd | 5.10 | 53.5 | 15.3% | 235 | 424819 | 0.00 | 167.78 |

| Query (median ms; entity_lookup is per lookup) | Rows matched | oe | oe (lazy open) | parquet | vortex | json | json+zstd |
|---|---:|---:|---:|---:|---:|---:|---:|
| scan_all_columns | 100000 | 10.38 | 10.16 | 26.20 | 8.10 | 231.71 | 250.61 |
| scan_one_number | 100000 | 0.17 | 0.17 | 1.51 | 0.15 | 123.60 | 192.99 |
| scan_one_payload_number | 100000 | 0.13 | 0.13 | 0.32 | 0.11 | 128.43 | 201.28 |
| scan_one_payload_text | 100000 | 3.23 | 3.37 | 8.81 | 2.24 | 138.31 | 188.58 |
| filter_type_and_payload | 2904 | 0.47 | 0.71 | 3.42 | 0.29 | 144.69 | 201.32 |
| time_window_1pct | 1000 | 0.01 | 0.02 | 2.05 | 0.11 | 129.58 | 165.48 |
| entity_lookup | 81077 | 0.27 | 0.34 | 2.87 | 0.29 | 150.80 | 183.49 |
| latest_50_in_room | 64860 | 0.28 | 0.32 | 4.31 | 0.41 | 161.86 | 187.40 |
| thread_replies | 23 | 0.06 | 0.09 | 3.31 | 0.40 | 146.25 | 201.55 |
| event_by_id | 20 | 0.07 | 0.09 | 4.17 | 0.27 | 137.09 | 206.95 |

Removing events (redaction). OE records a deletion vector; the other formats rewrite the whole file.

| Case | Events removed | oe (ms) | oe (bytes written) | oe (lazy open) (ms) | oe (lazy open) (bytes written) | parquet (ms) | parquet (bytes written) | vortex (ms) | vortex (bytes written) | json (ms) | json (bytes written) | json+zstd (ms) | json+zstd (bytes written) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| redact_1_event | 1 | 0.00 | 9 | 0.00 | 9 | 167.58 | 3580849 | 284.01 | 4381044 | 370.07 | 34954335 | 591.39 | 5350605 |
| redact_1pct_of_events | 1000 | 0.41 | 4005 | 0.61 | 4005 | 154.71 | 3521126 | 300.65 | 4330708 | 416.49 | 34607634 | 500.27 | 5298958 |

## chat events: 1000000 rows

| Format | Size (MiB) | Bytes per event | Size vs JSON | Write (ms) | Write (events/s) | Open (ms) | Open + first lookup (ms) |
|---|---:|---:|---:|---:|---:|---:|---:|
| oe | 52.56 | 55.1 | 15.7% | 4864 | 205582 | 15.00 | 12.61 |
| oe (lazy open) | 52.56 | 55.1 | 15.7% | 4760 | 210106 | 10.01 | 17.87 |
| parquet | 35.96 | 37.7 | 10.8% | 1131 | 883890 | 0.18 | 25.21 |
| vortex | 45.86 | 48.1 | 13.7% | 2220 | 450521 | 0.84 | 2.19 |
| json | 334.22 | 350.5 | 100.0% | 1458 | 685828 | 9.17 | 1401.74 |
| json+zstd | 52.90 | 55.5 | 15.8% | 2653 | 376913 | 0.00 | 1979.08 |

| Query (median ms; entity_lookup is per lookup) | Rows matched | oe | oe (lazy open) | parquet | vortex | json | json+zstd |
|---|---:|---:|---:|---:|---:|---:|---:|
| scan_all_columns | 1000000 | 96.04 | 110.05 | 249.37 | 90.04 | 2699.50 | 3142.81 |
| scan_one_number | 1000000 | 1.62 | 2.00 | 19.95 | 1.67 | 1532.90 | 1683.53 |
| scan_one_payload_number | 1000000 | 1.17 | 1.37 | 5.19 | 1.41 | 1551.76 | 1628.53 |
| scan_one_payload_text | 1000000 | 29.22 | 36.32 | 89.49 | 24.08 | 1697.52 | 2104.64 |
| filter_type_and_payload | 30248 | 8.41 | 6.58 | 28.06 | 2.56 | 1472.47 | 1946.49 |
| time_window_1pct | 10000 | 0.05 | 0.05 | 2.19 | 0.52 | 1606.09 | 1919.60 |
| entity_lookup | 200587 | 1.79 | 1.89 | 24.16 | 1.57 | 1528.37 | 1967.09 |
| latest_50_in_room | 160250 | 1.83 | 1.93 | 44.62 | 2.14 | 1587.01 | 2028.52 |
| thread_replies | 22 | 0.26 | 0.33 | 17.29 | 2.87 | 1585.40 | 2040.07 |
| event_by_id | 20 | 0.10 | 0.10 | 5.07 | 0.68 | 1616.83 | 1951.12 |

Removing events (redaction). OE records a deletion vector; the other formats rewrite the whole file.

| Case | Events removed | oe (ms) | oe (bytes written) | oe (lazy open) (ms) | oe (lazy open) (bytes written) | parquet (ms) | parquet (bytes written) | vortex (ms) | vortex (bytes written) | json (ms) | json (bytes written) | json+zstd (ms) | json+zstd (bytes written) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| redact_1_event | 1 | 0.00 | 9 | 0.00 | 9 | 1960.39 | 37698174 | 2647.15 | 48035456 | 4660.22 | 350453613 | 6453.07 | 55469073 |
| redact_1pct_of_events | 10000 | 17.66 | 160005 | 12.70 | 160005 | 2101.46 | 37167023 | 2973.54 | 47241092 | 4541.98 | 346953322 | 6661.56 | 54925975 |

## Durable ingest

Each batch is acknowledged only after the file is synced to disk (300 batches per row). Parquet and Vortex cannot append, so each batch is a new file.

| Batch size | Format | Events/s | Ack p50 (ms) | Ack p99 (ms) | Bytes on disk |
|---:|---|---:|---:|---:|---:|
| 1 | oej journal | 5555 | 0.161 | 0.419 | 1228800 |
| 1 | json (append) | 7934 | 0.107 | 0.334 | 104659 |
| 1 | parquet (file per batch) | 1110 | 0.819 | 1.474 | 1094080 |
| 1 | vortex (file per batch) | 577 | 1.639 | 2.942 | 3131136 |
| 16 | oej journal | 55267 | 0.268 | 0.595 | 2457600 |
| 16 | json (append) | 61943 | 0.236 | 0.526 | 1664629 |
| 16 | parquet (file per batch) | 15164 | 1.037 | 1.778 | 1373373 |
| 16 | vortex (file per batch) | 1100 | 14.207 | 24.585 | 4804864 |
| 128 | oej journal | 140734 | 0.845 | 3.083 | 19660800 |
| 128 | json (append) | 174847 | 0.687 | 1.570 | 13400243 |
| 128 | parquet (file per batch) | 85190 | 1.414 | 3.260 | 2851044 |
| 128 | vortex (file per batch) | 6048 | 20.682 | 31.961 | 7595608 |

