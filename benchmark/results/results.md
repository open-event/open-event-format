# Benchmark results

Machine: Intel(R) Xeon(R) Processor @ 2.10GHz (4 logical CPUs), Linux 6.18.44-fc-v310, rustc 1.96.0 (ac68faa20 2026-05-25)

Every format stores the same events. Query times are the median of 5 runs after one warm-up; write times are the median of 3 runs. All queries run on a single thread against the file held in memory. Every format returned identical results for every query (checked on each run).

## telemetry events: 10000 rows

| Format | Size (MiB) | Bytes per event | Size vs JSON | Write (ms) | Write (events/s) | Open (ms) | Open + first lookup (ms) |
|---|---:|---:|---:|---:|---:|---:|---:|
| oe | 0.17 | 17.7 | 7.2% | 30 | 332786 | 0.13 | 0.12 |
| oe (lazy open) | 0.17 | 17.7 | 7.2% | 26 | 379240 | 0.09 | 0.15 |
| parquet | 0.13 | 13.7 | 5.6% | 5 | 1956461 | 0.01 | 0.38 |
| vortex | 0.16 | 16.6 | 6.8% | 30 | 336223 | 0.39 | 0.75 |
| json | 2.34 | 245.1 | 100.0% | 11 | 940272 | 0.00 | 13.66 |
| json+zstd | 0.22 | 23.4 | 9.5% | 14 | 729908 | 0.00 | 12.98 |

| Query (median ms; entity_lookup is per lookup) | Rows matched | oe | oe (lazy open) | parquet | vortex | json | json+zstd |
|---|---:|---:|---:|---:|---:|---:|---:|
| scan_all_columns | 10000 | 0.62 | 0.60 | 1.42 | 0.75 | 18.11 | 17.14 |
| scan_one_number | 10000 | 0.02 | 0.02 | 0.17 | 0.03 | 12.27 | 12.57 |
| scan_one_payload_number | 10000 | 0.09 | 0.09 | 0.19 | 0.15 | 12.23 | 12.10 |
| scan_one_payload_text | 10000 | 0.07 | 0.07 | 0.11 | 0.08 | 12.55 | 12.35 |
| filter_type_and_payload | 12 | 0.08 | 0.08 | 0.43 | 0.41 | 11.14 | 12.35 |
| time_window_1pct | 100 | 0.02 | 0.02 | 0.27 | 0.15 | 11.09 | 11.54 |
| entity_lookup | 820 | 0.02 | 0.02 | 0.34 | 0.11 | 10.47 | 11.90 |

Removing events (redaction). OE records a deletion vector; the other formats rewrite the whole file.

| Case | Events removed | oe (ms) | oe (bytes written) | oe (lazy open) (ms) | oe (lazy open) (bytes written) | parquet (ms) | parquet (bytes written) | vortex (ms) | vortex (bytes written) | json (ms) | json (bytes written) | json+zstd (ms) | json+zstd (bytes written) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| redact_1_event | 1 | 0.00 | 9 | 0.00 | 9 | 9.52 | 134711 | 33.42 | 165968 | 26.36 | 2450649 | 31.68 | 233602 |
| redact_1pct_of_events | 100 | 0.02 | 405 | 0.02 | 405 | 8.93 | 133804 | 40.21 | 165504 | 26.95 | 2426385 | 31.12 | 231239 |

## telemetry events: 100000 rows

| Format | Size (MiB) | Bytes per event | Size vs JSON | Write (ms) | Write (events/s) | Open (ms) | Open + first lookup (ms) |
|---|---:|---:|---:|---:|---:|---:|---:|
| oe | 1.55 | 16.2 | 6.6% | 311 | 321540 | 1.18 | 1.33 |
| oe (lazy open) | 1.55 | 16.2 | 6.6% | 336 | 297236 | 1.01 | 1.04 |
| parquet | 1.11 | 11.6 | 4.7% | 35 | 2823168 | 0.02 | 2.27 |
| vortex | 1.76 | 18.5 | 7.5% | 111 | 902053 | 0.54 | 1.43 |
| json | 23.36 | 245.0 | 100.0% | 111 | 900851 | 0.00 | 137.67 |
| json+zstd | 2.36 | 24.7 | 10.1% | 168 | 595965 | 0.00 | 144.78 |

| Query (median ms; entity_lookup is per lookup) | Rows matched | oe | oe (lazy open) | parquet | vortex | json | json+zstd |
|---|---:|---:|---:|---:|---:|---:|---:|
| scan_all_columns | 100000 | 6.48 | 7.95 | 9.09 | 6.48 | 191.64 | 248.66 |
| scan_one_number | 100000 | 0.18 | 0.22 | 1.31 | 0.14 | 132.17 | 163.70 |
| scan_one_payload_number | 100000 | 0.89 | 1.04 | 0.87 | 0.71 | 127.52 | 194.54 |
| scan_one_payload_text | 100000 | 0.64 | 0.90 | 0.65 | 0.49 | 137.66 | 164.11 |
| filter_type_and_payload | 143 | 0.84 | 0.96 | 2.79 | 0.28 | 124.19 | 189.22 |
| time_window_1pct | 1000 | 0.05 | 0.06 | 1.69 | 0.10 | 113.42 | 145.46 |
| entity_lookup | 834 | 0.12 | 0.12 | 2.16 | 0.80 | 113.03 | 159.26 |

Removing events (redaction). OE records a deletion vector; the other formats rewrite the whole file.

| Case | Events removed | oe (ms) | oe (bytes written) | oe (lazy open) (ms) | oe (lazy open) (bytes written) | parquet (ms) | parquet (bytes written) | vortex (ms) | vortex (bytes written) | json (ms) | json (bytes written) | json+zstd (ms) | json+zstd (bytes written) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| redact_1_event | 1 | 0.00 | 9 | 0.00 | 9 | 74.35 | 1162107 | 179.42 | 1846136 | 291.50 | 24497827 | 383.84 | 2470339 |
| redact_1pct_of_events | 1000 | 0.85 | 4005 | 0.74 | 4005 | 76.55 | 1129279 | 174.56 | 1834576 | 299.10 | 24253096 | 368.33 | 2446064 |

## telemetry events: 1000000 rows

| Format | Size (MiB) | Bytes per event | Size vs JSON | Write (ms) | Write (events/s) | Open (ms) | Open + first lookup (ms) |
|---|---:|---:|---:|---:|---:|---:|---:|
| oe | 16.22 | 17.0 | 6.9% | 3891 | 257006 | 8.73 | 7.31 |
| oe (lazy open) | 16.22 | 17.0 | 6.9% | 3806 | 262714 | 8.19 | 9.53 |
| parquet | 11.73 | 12.3 | 5.0% | 432 | 2315149 | 0.09 | 23.44 |
| vortex | 18.63 | 19.5 | 8.0% | 1064 | 939732 | 0.67 | 6.65 |
| json | 233.58 | 244.9 | 100.0% | 1019 | 981009 | 5.08 | 1401.46 |
| json+zstd | 24.21 | 25.4 | 10.4% | 1763 | 567362 | 0.00 | 1685.69 |

| Query (median ms; entity_lookup is per lookup) | Rows matched | oe | oe (lazy open) | parquet | vortex | json | json+zstd |
|---|---:|---:|---:|---:|---:|---:|---:|
| scan_all_columns | 1000000 | 72.71 | 69.14 | 84.13 | 78.46 | 2023.86 | 2428.54 |
| scan_one_number | 1000000 | 1.59 | 1.52 | 13.84 | 2.44 | 1283.30 | 1522.61 |
| scan_one_payload_number | 1000000 | 9.43 | 7.07 | 9.09 | 8.76 | 1324.34 | 1433.52 |
| scan_one_payload_text | 1000000 | 5.94 | 5.29 | 6.31 | 4.70 | 1371.71 | 1497.39 |
| filter_type_and_payload | 1237 | 7.12 | 7.14 | 28.14 | 2.24 | 1457.05 | 1743.27 |
| time_window_1pct | 10000 | 0.10 | 0.09 | 2.30 | 0.48 | 1380.17 | 1578.15 |
| entity_lookup | 837 | 0.30 | 0.34 | 28.47 | 6.23 | 1248.95 | 1648.74 |

Removing events (redaction). OE records a deletion vector; the other formats rewrite the whole file.

| Case | Events removed | oe (ms) | oe (bytes written) | oe (lazy open) (ms) | oe (lazy open) (bytes written) | parquet (ms) | parquet (bytes written) | vortex (ms) | vortex (bytes written) | json (ms) | json (bytes written) | json+zstd (ms) | json+zstd (bytes written) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| redact_1_event | 1 | 0.00 | 9 | 0.00 | 9 | 889.77 | 12309274 | 1569.32 | 19543608 | 3411.87 | 244921417 | 4456.79 | 25386110 |
| redact_1pct_of_events | 10000 | 13.77 | 160005 | 13.91 | 160005 | 916.82 | 12020757 | 1539.83 | 19543040 | 3240.50 | 242472519 | 4326.80 | 25138782 |

## messages events: 10000 rows

| Format | Size (MiB) | Bytes per event | Size vs JSON | Write (ms) | Write (events/s) | Open (ms) | Open + first lookup (ms) |
|---|---:|---:|---:|---:|---:|---:|---:|
| oe | 0.44 | 46.4 | 14.2% | 48 | 210252 | 0.13 | 0.14 |
| oe (lazy open) | 0.44 | 46.4 | 14.2% | 48 | 206954 | 0.10 | 0.17 |
| parquet | 0.43 | 45.5 | 13.9% | 11 | 944616 | 0.01 | 0.34 |
| vortex | 0.45 | 47.0 | 14.4% | 42 | 238340 | 0.55 | 0.84 |
| json | 3.12 | 326.9 | 100.0% | 11 | 949234 | 0.00 | 12.74 |
| json+zstd | 0.57 | 59.9 | 18.3% | 19 | 523651 | 0.00 | 14.77 |

| Query (median ms; entity_lookup is per lookup) | Rows matched | oe | oe (lazy open) | parquet | vortex | json | json+zstd |
|---|---:|---:|---:|---:|---:|---:|---:|
| scan_all_columns | 10000 | 1.97 | 2.18 | 2.56 | 1.02 | 17.13 | 18.83 |
| scan_one_number | 10000 | 0.01 | 0.02 | 0.14 | 0.03 | 10.70 | 13.51 |
| scan_one_payload_number | 10000 | 0.01 | 0.01 | 0.15 | 0.03 | 11.11 | 13.66 |
| scan_one_payload_text | 10000 | 1.31 | 1.45 | 1.04 | 0.28 | 11.69 | 14.38 |
| filter_type_and_payload | 265 | 0.06 | 0.06 | 0.47 | 0.21 | 11.71 | 14.54 |
| time_window_1pct | 100 | 0.01 | 0.01 | 0.29 | 0.12 | 11.23 | 14.82 |
| entity_lookup | 1751 | 0.02 | 0.02 | 0.25 | 0.13 | 12.99 | 16.35 |

Removing events (redaction). OE records a deletion vector; the other formats rewrite the whole file.

| Case | Events removed | oe (ms) | oe (bytes written) | oe (lazy open) (ms) | oe (lazy open) (bytes written) | parquet (ms) | parquet (bytes written) | vortex (ms) | vortex (bytes written) | json (ms) | json (bytes written) | json+zstd (ms) | json+zstd (bytes written) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| redact_1_event | 1 | 0.00 | 9 | 0.00 | 9 | 15.13 | 452866 | 45.60 | 473520 | 29.89 | 3268410 | 74.33 | 598999 |
| redact_1pct_of_events | 100 | 0.02 | 405 | 0.02 | 405 | 15.41 | 448727 | 43.61 | 466192 | 42.01 | 3235234 | 47.31 | 592934 |

## messages events: 100000 rows

| Format | Size (MiB) | Bytes per event | Size vs JSON | Write (ms) | Write (events/s) | Open (ms) | Open + first lookup (ms) |
|---|---:|---:|---:|---:|---:|---:|---:|
| oe | 4.26 | 44.7 | 13.7% | 471 | 212252 | 1.27 | 1.43 |
| oe (lazy open) | 4.26 | 44.7 | 13.7% | 468 | 213731 | 0.87 | 1.67 |
| parquet | 4.10 | 43.0 | 13.2% | 97 | 1032913 | 0.05 | 2.57 |
| vortex | 4.64 | 48.6 | 14.9% | 186 | 538472 | 0.54 | 1.52 |
| json | 31.13 | 326.4 | 100.0% | 117 | 851881 | 0.00 | 147.04 |
| json+zstd | 5.83 | 61.1 | 18.7% | 254 | 393228 | 0.00 | 192.36 |

| Query (median ms; entity_lookup is per lookup) | Rows matched | oe | oe (lazy open) | parquet | vortex | json | json+zstd |
|---|---:|---:|---:|---:|---:|---:|---:|
| scan_all_columns | 100000 | 20.10 | 19.89 | 22.35 | 8.56 | 225.97 | 245.35 |
| scan_one_number | 100000 | 0.15 | 0.17 | 1.60 | 0.16 | 145.21 | 186.48 |
| scan_one_payload_number | 100000 | 0.11 | 0.12 | 1.35 | 0.18 | 136.27 | 156.26 |
| scan_one_payload_text | 100000 | 16.79 | 16.78 | 11.08 | 2.28 | 158.92 | 189.16 |
| filter_type_and_payload | 2655 | 0.67 | 0.81 | 4.53 | 0.61 | 127.56 | 169.67 |
| time_window_1pct | 1000 | 0.01 | 0.01 | 3.03 | 0.21 | 142.04 | 163.97 |
| entity_lookup | 1203 | 0.10 | 0.14 | 2.86 | 0.97 | 144.86 | 159.49 |

Removing events (redaction). OE records a deletion vector; the other formats rewrite the whole file.

| Case | Events removed | oe (ms) | oe (bytes written) | oe (lazy open) (ms) | oe (lazy open) (bytes written) | parquet (ms) | parquet (bytes written) | vortex (ms) | vortex (bytes written) | json (ms) | json (bytes written) | json+zstd (ms) | json+zstd (bytes written) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| redact_1_event | 1 | 0.00 | 9 | 0.00 | 9 | 182.94 | 4300907 | 248.88 | 4860768 | 359.01 | 32636819 | 490.14 | 6111549 |
| redact_1pct_of_events | 1000 | 0.88 | 4005 | 1.00 | 4005 | 179.01 | 4232767 | 245.14 | 4816112 | 392.93 | 32309495 | 493.55 | 6051823 |

## messages events: 1000000 rows

| Format | Size (MiB) | Bytes per event | Size vs JSON | Write (ms) | Write (events/s) | Open (ms) | Open + first lookup (ms) |
|---|---:|---:|---:|---:|---:|---:|---:|
| oe | 43.23 | 45.3 | 13.9% | 6005 | 166540 | 10.61 | 10.91 |
| oe (lazy open) | 43.23 | 45.3 | 13.9% | 5903 | 169403 | 7.60 | 15.94 |
| parquet | 41.65 | 43.7 | 13.4% | 1092 | 915517 | 0.12 | 24.08 |
| vortex | 47.32 | 49.6 | 15.2% | 1785 | 560148 | 0.74 | 8.65 |
| json | 311.21 | 326.3 | 100.0% | 1193 | 838164 | 5.00 | 1363.81 |
| json+zstd | 58.99 | 61.9 | 19.0% | 2529 | 395400 | 0.01 | 1820.48 |

| Query (median ms; entity_lookup is per lookup) | Rows matched | oe | oe (lazy open) | parquet | vortex | json | json+zstd |
|---|---:|---:|---:|---:|---:|---:|---:|
| scan_all_columns | 1000000 | 239.34 | 200.66 | 227.15 | 89.79 | 2718.29 | 3047.89 |
| scan_one_number | 1000000 | 1.72 | 1.50 | 12.97 | 1.41 | 1383.77 | 1841.66 |
| scan_one_payload_number | 1000000 | 1.30 | 1.13 | 11.28 | 1.24 | 1397.23 | 1810.71 |
| scan_one_payload_text | 1000000 | 153.32 | 131.52 | 106.21 | 23.55 | 1490.16 | 1946.73 |
| filter_type_and_payload | 26269 | 8.26 | 6.42 | 38.39 | 2.79 | 1398.58 | 1848.81 |
| time_window_1pct | 10000 | 0.04 | 0.03 | 3.31 | 0.32 | 1493.12 | 1826.76 |
| entity_lookup | 5411 | 0.47 | 0.34 | 25.86 | 5.89 | 1405.87 | 1794.18 |

Removing events (redaction). OE records a deletion vector; the other formats rewrite the whole file.

| Case | Events removed | oe (ms) | oe (bytes written) | oe (lazy open) (ms) | oe (lazy open) (bytes written) | parquet (ms) | parquet (bytes written) | vortex (ms) | vortex (bytes written) | json (ms) | json (bytes written) | json+zstd (ms) | json+zstd (bytes written) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| redact_1_event | 1 | 0.00 | 9 | 0.00 | 9 | 1827.95 | 43671434 | 2230.35 | 49602912 | 4177.30 | 326324891 | 6255.63 | 61854744 |
| redact_1pct_of_events | 10000 | 13.50 | 160005 | 13.05 | 160005 | 1904.02 | 43058471 | 2217.32 | 49314872 | 4303.88 | 323067360 | 6146.44 | 61243459 |

## chat events: 10000 rows

| Format | Size (MiB) | Bytes per event | Size vs JSON | Write (ms) | Write (events/s) | Open (ms) | Open + first lookup (ms) |
|---|---:|---:|---:|---:|---:|---:|---:|
| oe | 0.41 | 42.6 | 12.2% | 48 | 206645 | 0.24 | 0.47 |
| oe (lazy open) | 0.41 | 42.6 | 12.2% | 43 | 230309 | 0.09 | 0.34 |
| parquet | 0.36 | 37.3 | 10.7% | 10 | 952843 | 0.01 | 0.41 |
| vortex | 0.42 | 43.6 | 12.5% | 50 | 201450 | 0.55 | 1.00 |
| json | 3.32 | 348.4 | 100.0% | 10 | 971586 | 0.00 | 14.77 |
| json+zstd | 0.49 | 51.1 | 14.7% | 24 | 415720 | 0.00 | 19.31 |

| Query (median ms; entity_lookup is per lookup) | Rows matched | oe | oe (lazy open) | parquet | vortex | json | json+zstd |
|---|---:|---:|---:|---:|---:|---:|---:|
| scan_all_columns | 10000 | 3.83 | 2.45 | 2.28 | 1.09 | 25.16 | 24.84 |
| scan_one_number | 10000 | 0.04 | 0.03 | 0.13 | 0.04 | 15.31 | 18.11 |
| scan_one_payload_number | 10000 | 0.02 | 0.01 | 0.05 | 0.03 | 15.81 | 18.96 |
| scan_one_payload_text | 10000 | 2.72 | 1.49 | 0.90 | 0.32 | 15.45 | 14.77 |
| filter_type_and_payload | 266 | 0.11 | 0.07 | 0.29 | 0.27 | 11.71 | 14.37 |
| time_window_1pct | 100 | 0.03 | 0.02 | 0.19 | 0.12 | 16.57 | 13.81 |
| entity_lookup | 36977 | 0.10 | 0.07 | 0.28 | 0.18 | 14.93 | 14.58 |
| latest_50_in_room | 29391 | 0.11 | 0.08 | 0.44 | 0.32 | 14.88 | 15.29 |
| thread_replies | 21 | 0.12 | 0.07 | 0.28 | 0.17 | 15.31 | 14.38 |
| event_by_id | 20 | 0.01 | 0.01 | 0.45 | 0.24 | 15.44 | 17.47 |

Removing events (redaction). OE records a deletion vector; the other formats rewrite the whole file.

| Case | Events removed | oe (ms) | oe (bytes written) | oe (lazy open) (ms) | oe (lazy open) (bytes written) | parquet (ms) | parquet (bytes written) | vortex (ms) | vortex (bytes written) | json (ms) | json (bytes written) | json+zstd (ms) | json+zstd (bytes written) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| redact_1_event | 1 | 0.00 | 9 | 0.00 | 9 | 14.61 | 370690 | 51.99 | 433524 | 38.79 | 3483690 | 55.51 | 511242 |
| redact_1pct_of_events | 100 | 0.07 | 405 | 0.02 | 405 | 14.90 | 367096 | 46.51 | 434356 | 39.11 | 3450121 | 46.96 | 506512 |

## chat events: 100000 rows

| Format | Size (MiB) | Bytes per event | Size vs JSON | Write (ms) | Write (events/s) | Open (ms) | Open + first lookup (ms) |
|---|---:|---:|---:|---:|---:|---:|---:|
| oe | 4.08 | 42.8 | 12.2% | 462 | 216451 | 1.26 | 1.51 |
| oe (lazy open) | 4.08 | 42.8 | 12.2% | 470 | 212770 | 0.90 | 1.82 |
| parquet | 3.41 | 35.8 | 10.2% | 103 | 973778 | 0.05 | 3.12 |
| vortex | 4.18 | 43.9 | 12.5% | 218 | 459766 | 0.49 | 0.82 |
| json | 33.34 | 349.5 | 100.0% | 122 | 818491 | 0.00 | 149.30 |
| json+zstd | 5.10 | 53.5 | 15.3% | 247 | 404762 | 0.00 | 203.23 |

| Query (median ms; entity_lookup is per lookup) | Rows matched | oe | oe (lazy open) | parquet | vortex | json | json+zstd |
|---|---:|---:|---:|---:|---:|---:|---:|
| scan_all_columns | 100000 | 25.96 | 21.17 | 24.05 | 7.45 | 240.22 | 284.63 |
| scan_one_number | 100000 | 0.18 | 0.18 | 1.59 | 0.17 | 147.50 | 176.64 |
| scan_one_payload_number | 100000 | 0.13 | 0.14 | 0.40 | 0.12 | 128.49 | 164.72 |
| scan_one_payload_text | 100000 | 16.53 | 14.79 | 10.02 | 2.08 | 159.29 | 198.21 |
| filter_type_and_payload | 2904 | 0.65 | 0.60 | 3.52 | 0.33 | 128.02 | 176.35 |
| time_window_1pct | 1000 | 0.02 | 0.02 | 2.12 | 0.10 | 143.87 | 195.06 |
| entity_lookup | 81077 | 0.34 | 0.35 | 3.05 | 0.29 | 151.62 | 199.27 |
| latest_50_in_room | 64860 | 0.39 | 0.34 | 4.39 | 0.39 | 154.92 | 207.07 |
| thread_replies | 23 | 0.09 | 0.08 | 2.79 | 0.38 | 149.64 | 202.29 |
| event_by_id | 20 | 0.09 | 0.09 | 4.29 | 0.26 | 139.45 | 200.60 |

Removing events (redaction). OE records a deletion vector; the other formats rewrite the whole file.

| Case | Events removed | oe (ms) | oe (bytes written) | oe (lazy open) (ms) | oe (lazy open) (bytes written) | parquet (ms) | parquet (bytes written) | vortex (ms) | vortex (bytes written) | json (ms) | json (bytes written) | json+zstd (ms) | json+zstd (bytes written) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| redact_1_event | 1 | 0.00 | 9 | 0.00 | 9 | 181.05 | 3580849 | 327.81 | 4381044 | 416.98 | 34954335 | 593.00 | 5350605 |
| redact_1pct_of_events | 1000 | 0.51 | 4005 | 0.58 | 4005 | 198.04 | 3521126 | 282.79 | 4330708 | 416.67 | 34607634 | 550.24 | 5298958 |

## chat events: 1000000 rows

| Format | Size (MiB) | Bytes per event | Size vs JSON | Write (ms) | Write (events/s) | Open (ms) | Open + first lookup (ms) |
|---|---:|---:|---:|---:|---:|---:|---:|
| oe | 42.24 | 44.3 | 12.6% | 5054 | 197869 | 12.99 | 11.12 |
| oe (lazy open) | 42.24 | 44.3 | 12.6% | 4923 | 203120 | 8.23 | 16.33 |
| parquet | 35.96 | 37.7 | 10.8% | 1263 | 792019 | 0.16 | 27.63 |
| vortex | 45.86 | 48.1 | 13.7% | 2193 | 455972 | 0.78 | 2.30 |
| json | 334.22 | 350.5 | 100.0% | 1313 | 761483 | 8.15 | 1348.84 |
| json+zstd | 52.90 | 55.5 | 15.8% | 2511 | 398281 | 0.01 | 1911.65 |

| Query (median ms; entity_lookup is per lookup) | Rows matched | oe | oe (lazy open) | parquet | vortex | json | json+zstd |
|---|---:|---:|---:|---:|---:|---:|---:|
| scan_all_columns | 1000000 | 288.37 | 272.24 | 269.93 | 86.49 | 2583.96 | 3074.74 |
| scan_one_number | 1000000 | 1.71 | 2.12 | 14.53 | 1.99 | 1468.81 | 1766.42 |
| scan_one_payload_number | 1000000 | 1.09 | 1.45 | 3.41 | 1.66 | 1497.02 | 1862.51 |
| scan_one_payload_text | 1000000 | 117.88 | 139.30 | 98.12 | 23.04 | 1483.14 | 2030.42 |
| filter_type_and_payload | 30248 | 5.18 | 5.56 | 31.49 | 2.78 | 1565.92 | 1913.27 |
| time_window_1pct | 10000 | 0.04 | 0.04 | 2.23 | 0.76 | 1486.49 | 1839.09 |
| entity_lookup | 200587 | 1.43 | 1.48 | 29.98 | 2.00 | 1408.54 | 1912.03 |
| latest_50_in_room | 160250 | 1.55 | 1.59 | 47.89 | 1.99 | 1560.93 | 2074.58 |
| thread_replies | 22 | 0.27 | 0.26 | 22.08 | 2.57 | 1582.75 | 1914.41 |
| event_by_id | 20 | 0.10 | 0.08 | 5.98 | 0.66 | 1574.55 | 1940.95 |

Removing events (redaction). OE records a deletion vector; the other formats rewrite the whole file.

| Case | Events removed | oe (ms) | oe (bytes written) | oe (lazy open) (ms) | oe (lazy open) (bytes written) | parquet (ms) | parquet (bytes written) | vortex (ms) | vortex (bytes written) | json (ms) | json (bytes written) | json+zstd (ms) | json+zstd (bytes written) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| redact_1_event | 1 | 0.00 | 9 | 0.00 | 9 | 1930.83 | 37698174 | 2774.18 | 48035456 | 4571.02 | 350453613 | 6004.84 | 55469073 |
| redact_1pct_of_events | 10000 | 15.37 | 160005 | 13.88 | 160005 | 2103.64 | 37167023 | 2819.82 | 47241092 | 4352.38 | 346953322 | 6280.73 | 54925975 |

## Durable ingest

Each batch is acknowledged only after the file is synced to disk (300 batches per row). Parquet and Vortex cannot append, so each batch is a new file.

| Batch size | Format | Events/s | Ack p50 (ms) | Ack p99 (ms) | Bytes on disk |
|---:|---|---:|---:|---:|---:|
| 1 | oej journal | 5097 | 0.163 | 0.625 | 1228800 |
| 1 | json (append) | 6310 | 0.135 | 0.437 | 104659 |
| 1 | parquet (file per batch) | 1003 | 0.977 | 1.617 | 1094080 |
| 1 | vortex (file per batch) | 547 | 1.783 | 3.320 | 3131136 |
| 16 | oej journal | 57871 | 0.250 | 0.824 | 2457600 |
| 16 | json (append) | 46290 | 0.313 | 0.877 | 1664629 |
| 16 | parquet (file per batch) | 15750 | 0.992 | 1.593 | 1373373 |
| 16 | vortex (file per batch) | 1111 | 14.116 | 23.210 | 4804864 |
| 128 | oej journal | 236389 | 0.508 | 1.106 | 19660800 |
| 128 | json (append) | 195697 | 0.606 | 1.508 | 13400243 |
| 128 | parquet (file per batch) | 88788 | 1.374 | 2.269 | 2851044 |
| 128 | vortex (file per batch) | 6428 | 19.719 | 29.177 | 7595608 |

