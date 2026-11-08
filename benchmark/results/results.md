# Benchmark results

Machine: Intel(R) Xeon(R) Processor @ 2.10GHz (4 logical CPUs), Linux 6.18.44-fc-v310, rustc 1.96.0 (ac68faa20 2026-05-25)

Every format stores the same events. Query times are the median of 5 runs after one warm-up; write times are the median of 3 runs. All queries run on a single thread against the file held in memory. Every format returned identical results for every query (checked on each run).

## telemetry events: 10000 rows

| Format | Size (MiB) | Bytes per event | Size vs JSON | Write (ms) | Write (events/s) | Open (ms) | Open + first lookup (ms) |
|---|---:|---:|---:|---:|---:|---:|---:|
| oe | 0.37 | 38.9 | 15.9% | 36 | 280844 | 0.14 | 0.27 |
| oe (lazy open) | 0.37 | 38.9 | 15.9% | 34 | 292161 | 0.12 | 0.30 |
| parquet | 0.13 | 13.7 | 5.6% | 6 | 1683519 | 0.02 | 0.45 |
| vortex | 0.16 | 16.6 | 6.8% | 37 | 269328 | 0.60 | 1.01 |
| json | 2.34 | 245.1 | 100.0% | 18 | 555285 | 0.00 | 21.10 |
| json+zstd | 0.22 | 23.4 | 9.5% | 26 | 383468 | 0.00 | 24.95 |

| Query (median ms; entity_lookup is per lookup) | Rows matched | oe | oe (lazy open) | parquet | vortex | json | json+zstd |
|---|---:|---:|---:|---:|---:|---:|---:|
| scan_all_columns | 10000 | 0.63 | 0.75 | 1.72 | 1.06 | 31.11 | 32.81 |
| scan_one_number | 10000 | 0.02 | 0.02 | 0.20 | 0.04 | 21.59 | 23.80 |
| scan_one_payload_number | 10000 | 0.09 | 0.10 | 0.20 | 0.11 | 19.09 | 12.51 |
| scan_one_payload_text | 10000 | 0.07 | 0.08 | 0.11 | 0.07 | 14.22 | 13.04 |
| filter_type_and_payload | 12 | 0.08 | 0.11 | 0.53 | 0.31 | 18.48 | 12.89 |
| time_window_1pct | 100 | 0.02 | 0.03 | 0.33 | 0.16 | 17.24 | 12.80 |
| entity_lookup | 820 | 0.12 | 0.12 | 0.34 | 0.15 | 14.42 | 13.18 |

Removing events (redaction). OE records a deletion vector; the other formats rewrite the whole file.

| Case | Events removed | oe (ms) | oe (bytes written) | oe (lazy open) (ms) | oe (lazy open) (bytes written) | parquet (ms) | parquet (bytes written) | vortex (ms) | vortex (bytes written) | json (ms) | json (bytes written) | json+zstd (ms) | json+zstd (bytes written) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| redact_1_event | 1 | 0.00 | 9 | 0.00 | 9 | 11.60 | 134711 | 36.96 | 165968 | 50.97 | 2450649 | 37.15 | 233602 |
| redact_1pct_of_events | 100 | 0.02 | 405 | 0.02 | 405 | 11.89 | 133804 | 58.36 | 165504 | 51.72 | 2426385 | 34.93 | 231239 |

## telemetry events: 100000 rows

| Format | Size (MiB) | Bytes per event | Size vs JSON | Write (ms) | Write (events/s) | Open (ms) | Open + first lookup (ms) |
|---|---:|---:|---:|---:|---:|---:|---:|
| oe | 4.17 | 43.7 | 17.8% | 373 | 268198 | 1.75 | 1.35 |
| oe (lazy open) | 4.17 | 43.7 | 17.8% | 351 | 284715 | 0.98 | 1.54 |
| parquet | 1.11 | 11.6 | 4.7% | 46 | 2185374 | 0.06 | 2.69 |
| vortex | 1.76 | 18.5 | 7.5% | 141 | 706907 | 0.56 | 1.75 |
| json | 23.36 | 245.0 | 100.0% | 128 | 781648 | 0.00 | 143.49 |
| json+zstd | 2.36 | 24.7 | 10.1% | 160 | 623802 | 0.00 | 179.84 |

| Query (median ms; entity_lookup is per lookup) | Rows matched | oe | oe (lazy open) | parquet | vortex | json | json+zstd |
|---|---:|---:|---:|---:|---:|---:|---:|
| scan_all_columns | 100000 | 5.45 | 8.02 | 10.80 | 7.68 | 204.97 | 242.40 |
| scan_one_number | 100000 | 0.14 | 0.21 | 1.34 | 0.26 | 178.87 | 184.93 |
| scan_one_payload_number | 100000 | 0.71 | 0.76 | 0.96 | 0.95 | 173.48 | 173.44 |
| scan_one_payload_text | 100000 | 0.64 | 0.50 | 0.69 | 0.51 | 122.83 | 180.14 |
| filter_type_and_payload | 143 | 1.05 | 0.66 | 2.90 | 0.38 | 151.36 | 192.77 |
| time_window_1pct | 1000 | 0.05 | 0.04 | 1.79 | 0.19 | 139.36 | 234.60 |
| entity_lookup | 834 | 0.18 | 0.21 | 2.16 | 1.02 | 148.44 | 163.51 |

Removing events (redaction). OE records a deletion vector; the other formats rewrite the whole file.

| Case | Events removed | oe (ms) | oe (bytes written) | oe (lazy open) (ms) | oe (lazy open) (bytes written) | parquet (ms) | parquet (bytes written) | vortex (ms) | vortex (bytes written) | json (ms) | json (bytes written) | json+zstd (ms) | json+zstd (bytes written) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| redact_1_event | 1 | 0.00 | 9 | 0.00 | 9 | 97.06 | 1162107 | 185.97 | 1846136 | 374.32 | 24497827 | 520.00 | 2470339 |
| redact_1pct_of_events | 1000 | 1.20 | 4005 | 1.17 | 4005 | 89.85 | 1129279 | 184.71 | 1834576 | 299.80 | 24253096 | 495.50 | 2446064 |

## telemetry events: 1000000 rows

| Format | Size (MiB) | Bytes per event | Size vs JSON | Write (ms) | Write (events/s) | Open (ms) | Open + first lookup (ms) |
|---|---:|---:|---:|---:|---:|---:|---:|
| oe | 48.05 | 50.4 | 20.6% | 3393 | 294761 | 9.64 | 11.96 |
| oe (lazy open) | 48.05 | 50.4 | 20.6% | 3467 | 288473 | 8.23 | 14.21 |
| parquet | 11.73 | 12.3 | 5.0% | 428 | 2338245 | 0.10 | 39.25 |
| vortex | 18.63 | 19.5 | 8.0% | 1160 | 861936 | 0.60 | 6.00 |
| json | 233.58 | 244.9 | 100.0% | 1265 | 790701 | 5.79 | 1308.67 |
| json+zstd | 24.21 | 25.4 | 10.4% | 1884 | 530700 | 0.00 | 1645.32 |

| Query (median ms; entity_lookup is per lookup) | Rows matched | oe | oe (lazy open) | parquet | vortex | json | json+zstd |
|---|---:|---:|---:|---:|---:|---:|---:|
| scan_all_columns | 1000000 | 109.33 | 60.56 | 116.69 | 61.53 | 2152.60 | 2267.85 |
| scan_one_number | 1000000 | 1.53 | 1.91 | 16.25 | 1.52 | 1277.91 | 1707.13 |
| scan_one_payload_number | 1000000 | 7.07 | 9.36 | 8.84 | 7.23 | 1366.32 | 1587.11 |
| scan_one_payload_text | 1000000 | 5.23 | 5.21 | 6.95 | 3.62 | 1497.42 | 1744.49 |
| filter_type_and_payload | 1237 | 7.03 | 7.39 | 28.15 | 1.79 | 1282.08 | 1810.26 |
| time_window_1pct | 10000 | 0.09 | 0.10 | 2.33 | 0.40 | 1275.65 | 1665.29 |
| entity_lookup | 837 | 1.48 | 1.52 | 42.40 | 4.89 | 1248.19 | 1743.61 |

Removing events (redaction). OE records a deletion vector; the other formats rewrite the whole file.

| Case | Events removed | oe (ms) | oe (bytes written) | oe (lazy open) (ms) | oe (lazy open) (bytes written) | parquet (ms) | parquet (bytes written) | vortex (ms) | vortex (bytes written) | json (ms) | json (bytes written) | json+zstd (ms) | json+zstd (bytes written) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| redact_1_event | 1 | 0.00 | 9 | 0.00 | 9 | 1000.19 | 12309274 | 1570.73 | 19543608 | 3752.36 | 244921417 | 4792.13 | 25386110 |
| redact_1pct_of_events | 10000 | 14.83 | 160005 | 13.14 | 160005 | 1048.85 | 12020757 | 1660.49 | 19543040 | 3720.57 | 242472519 | 4463.61 | 25138782 |

## messages events: 10000 rows

| Format | Size (MiB) | Bytes per event | Size vs JSON | Write (ms) | Write (events/s) | Open (ms) | Open + first lookup (ms) |
|---|---:|---:|---:|---:|---:|---:|---:|
| oe | 0.80 | 83.4 | 25.5% | 41 | 246640 | 0.18 | 0.18 |
| oe (lazy open) | 0.80 | 83.4 | 25.5% | 41 | 245095 | 0.11 | 0.28 |
| parquet | 0.43 | 45.5 | 13.9% | 15 | 648336 | 0.03 | 0.53 |
| vortex | 0.45 | 47.0 | 14.4% | 61 | 165114 | 0.71 | 1.09 |
| json | 3.12 | 326.9 | 100.0% | 12 | 844193 | 0.00 | 14.59 |
| json+zstd | 0.57 | 59.9 | 18.3% | 26 | 384681 | 0.00 | 19.73 |

| Query (median ms; entity_lookup is per lookup) | Rows matched | oe | oe (lazy open) | parquet | vortex | json | json+zstd |
|---|---:|---:|---:|---:|---:|---:|---:|
| scan_all_columns | 10000 | 0.81 | 1.36 | 3.59 | 1.37 | 21.15 | 24.75 |
| scan_one_number | 10000 | 0.01 | 0.02 | 0.22 | 0.03 | 14.25 | 18.39 |
| scan_one_payload_number | 10000 | 0.01 | 0.02 | 0.22 | 0.04 | 14.48 | 18.62 |
| scan_one_payload_text | 10000 | 0.27 | 0.45 | 1.66 | 0.43 | 15.66 | 19.56 |
| filter_type_and_payload | 265 | 0.06 | 0.09 | 0.75 | 0.40 | 15.60 | 19.66 |
| time_window_1pct | 100 | 0.01 | 0.01 | 0.47 | 0.18 | 13.33 | 18.38 |
| entity_lookup | 1751 | 0.02 | 0.03 | 0.44 | 0.24 | 14.38 | 18.69 |

Removing events (redaction). OE records a deletion vector; the other formats rewrite the whole file.

| Case | Events removed | oe (ms) | oe (bytes written) | oe (lazy open) (ms) | oe (lazy open) (bytes written) | parquet (ms) | parquet (bytes written) | vortex (ms) | vortex (bytes written) | json (ms) | json (bytes written) | json+zstd (ms) | json+zstd (bytes written) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| redact_1_event | 1 | 0.00 | 9 | 0.00 | 9 | 24.42 | 452866 | 64.55 | 473520 | 36.61 | 3268410 | 56.98 | 598999 |
| redact_1pct_of_events | 100 | 0.01 | 405 | 0.02 | 405 | 18.01 | 448727 | 53.35 | 466192 | 42.10 | 3235234 | 52.41 | 592934 |

## messages events: 100000 rows

| Format | Size (MiB) | Bytes per event | Size vs JSON | Write (ms) | Write (events/s) | Open (ms) | Open + first lookup (ms) |
|---|---:|---:|---:|---:|---:|---:|---:|
| oe | 8.33 | 87.4 | 26.8% | 426 | 234578 | 1.57 | 1.90 |
| oe (lazy open) | 8.33 | 87.4 | 26.8% | 437 | 228595 | 0.75 | 2.00 |
| parquet | 4.10 | 43.0 | 13.2% | 94 | 1066748 | 0.04 | 2.38 |
| vortex | 4.64 | 48.6 | 14.9% | 205 | 488166 | 0.61 | 1.79 |
| json | 31.13 | 326.4 | 100.0% | 132 | 757883 | 0.00 | 136.16 |
| json+zstd | 5.83 | 61.1 | 18.7% | 268 | 373232 | 0.00 | 204.53 |

| Query (median ms; entity_lookup is per lookup) | Rows matched | oe | oe (lazy open) | parquet | vortex | json | json+zstd |
|---|---:|---:|---:|---:|---:|---:|---:|
| scan_all_columns | 100000 | 10.72 | 8.17 | 22.59 | 10.57 | 222.34 | 279.13 |
| scan_one_number | 100000 | 0.21 | 0.14 | 1.90 | 0.34 | 145.00 | 173.87 |
| scan_one_payload_number | 100000 | 0.13 | 0.11 | 1.88 | 0.29 | 141.68 | 189.65 |
| scan_one_payload_text | 100000 | 3.70 | 2.96 | 15.86 | 2.85 | 164.45 | 187.76 |
| filter_type_and_payload | 2655 | 0.85 | 0.68 | 4.85 | 0.55 | 161.10 | 221.78 |
| time_window_1pct | 1000 | 0.01 | 0.01 | 3.15 | 0.14 | 134.21 | 218.37 |
| entity_lookup | 1203 | 0.15 | 0.12 | 2.91 | 1.02 | 130.27 | 201.38 |

Removing events (redaction). OE records a deletion vector; the other formats rewrite the whole file.

| Case | Events removed | oe (ms) | oe (bytes written) | oe (lazy open) (ms) | oe (lazy open) (bytes written) | parquet (ms) | parquet (bytes written) | vortex (ms) | vortex (bytes written) | json (ms) | json (bytes written) | json+zstd (ms) | json+zstd (bytes written) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| redact_1_event | 1 | 0.00 | 9 | 0.00 | 9 | 170.06 | 4300907 | 272.73 | 4860768 | 354.83 | 32636819 | 620.58 | 6111549 |
| redact_1pct_of_events | 1000 | 1.03 | 4005 | 0.98 | 4005 | 167.41 | 4232767 | 263.99 | 4816112 | 375.92 | 32309495 | 652.96 | 6051823 |

## messages events: 1000000 rows

| Format | Size (MiB) | Bytes per event | Size vs JSON | Write (ms) | Write (events/s) | Open (ms) | Open + first lookup (ms) |
|---|---:|---:|---:|---:|---:|---:|---:|
| oe | 89.81 | 94.2 | 28.9% | 4454 | 224534 | 13.83 | 12.16 |
| oe (lazy open) | 89.81 | 94.2 | 28.9% | 4366 | 229039 | 12.96 | 22.66 |
| parquet | 41.65 | 43.7 | 13.4% | 1254 | 797545 | 0.13 | 29.99 |
| vortex | 47.32 | 49.6 | 15.2% | 1760 | 568112 | 0.96 | 10.61 |
| json | 311.21 | 326.3 | 100.0% | 1383 | 722971 | 7.17 | 1342.17 |
| json+zstd | 58.99 | 61.9 | 19.0% | 2732 | 366087 | 0.00 | 1920.95 |

| Query (median ms; entity_lookup is per lookup) | Rows matched | oe | oe (lazy open) | parquet | vortex | json | json+zstd |
|---|---:|---:|---:|---:|---:|---:|---:|
| scan_all_columns | 1000000 | 95.38 | 98.75 | 282.73 | 109.64 | 2759.50 | 3310.00 |
| scan_one_number | 1000000 | 1.55 | 1.56 | 23.26 | 1.74 | 1378.88 | 1898.78 |
| scan_one_payload_number | 1000000 | 1.12 | 1.18 | 22.59 | 1.39 | 1331.58 | 1899.59 |
| scan_one_payload_text | 1000000 | 37.58 | 31.78 | 123.71 | 25.69 | 1550.35 | 2144.86 |
| filter_type_and_payload | 26269 | 8.21 | 8.54 | 47.24 | 3.03 | 1549.13 | 1980.40 |
| time_window_1pct | 10000 | 0.04 | 0.04 | 4.02 | 0.36 | 1469.74 | 1971.06 |
| entity_lookup | 5411 | 1.63 | 1.65 | 29.40 | 6.31 | 1456.81 | 1984.18 |

Removing events (redaction). OE records a deletion vector; the other formats rewrite the whole file.

| Case | Events removed | oe (ms) | oe (bytes written) | oe (lazy open) (ms) | oe (lazy open) (bytes written) | parquet (ms) | parquet (bytes written) | vortex (ms) | vortex (bytes written) | json (ms) | json (bytes written) | json+zstd (ms) | json+zstd (bytes written) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| redact_1_event | 1 | 0.00 | 9 | 0.00 | 9 | 2028.80 | 43671434 | 2232.33 | 49602912 | 4609.96 | 326324891 | 6502.09 | 61854744 |
| redact_1pct_of_events | 10000 | 14.39 | 160005 | 17.58 | 160005 | 1883.83 | 43058471 | 2363.22 | 49314872 | 4751.40 | 323067360 | 6685.85 | 61243459 |

## chat events: 10000 rows

| Format | Size (MiB) | Bytes per event | Size vs JSON | Write (ms) | Write (events/s) | Open (ms) | Open + first lookup (ms) |
|---|---:|---:|---:|---:|---:|---:|---:|
| oe | 0.76 | 80.2 | 23.0% | 46 | 218672 | 0.32 | 0.54 |
| oe (lazy open) | 0.76 | 80.2 | 23.0% | 46 | 218696 | 0.12 | 0.42 |
| parquet | 0.36 | 37.3 | 10.7% | 10 | 972754 | 0.01 | 0.41 |
| vortex | 0.42 | 43.6 | 12.5% | 47 | 214618 | 0.52 | 0.80 |
| json | 3.32 | 348.4 | 100.0% | 12 | 856880 | 0.00 | 12.07 |
| json+zstd | 0.49 | 51.1 | 14.7% | 26 | 382255 | 0.00 | 22.26 |

| Query (median ms; entity_lookup is per lookup) | Rows matched | oe | oe (lazy open) | parquet | vortex | json | json+zstd |
|---|---:|---:|---:|---:|---:|---:|---:|
| scan_all_columns | 10000 | 1.58 | 0.99 | 2.35 | 0.84 | 20.71 | 32.16 |
| scan_one_number | 10000 | 0.03 | 0.02 | 0.16 | 0.03 | 16.01 | 21.46 |
| scan_one_payload_number | 10000 | 0.02 | 0.01 | 0.06 | 0.02 | 14.52 | 14.64 |
| scan_one_payload_text | 10000 | 0.49 | 0.27 | 1.00 | 0.24 | 16.66 | 15.48 |
| filter_type_and_payload | 266 | 0.10 | 0.06 | 0.37 | 0.15 | 12.39 | 23.63 |
| time_window_1pct | 100 | 0.02 | 0.01 | 0.22 | 0.09 | 12.60 | 18.94 |
| entity_lookup | 36977 | 0.10 | 0.07 | 0.33 | 0.13 | 19.57 | 19.30 |
| latest_50_in_room | 29391 | 0.11 | 0.08 | 0.51 | 0.22 | 19.00 | 21.18 |
| thread_replies | 21 | 0.10 | 0.06 | 0.33 | 0.19 | 15.65 | 18.63 |
| event_by_id | 20 | 0.01 | 0.00 | 0.47 | 0.15 | 21.44 | 15.69 |

Removing events (redaction). OE records a deletion vector; the other formats rewrite the whole file.

| Case | Events removed | oe (ms) | oe (bytes written) | oe (lazy open) (ms) | oe (lazy open) (bytes written) | parquet (ms) | parquet (bytes written) | vortex (ms) | vortex (bytes written) | json (ms) | json (bytes written) | json+zstd (ms) | json+zstd (bytes written) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| redact_1_event | 1 | 0.00 | 9 | 0.00 | 9 | 16.67 | 370690 | 50.65 | 433524 | 39.42 | 3483690 | 49.35 | 511242 |
| redact_1pct_of_events | 100 | 0.02 | 405 | 0.02 | 405 | 15.05 | 367096 | 53.29 | 434356 | 38.63 | 3450121 | 49.07 | 506512 |

## chat events: 100000 rows

| Format | Size (MiB) | Bytes per event | Size vs JSON | Write (ms) | Write (events/s) | Open (ms) | Open + first lookup (ms) |
|---|---:|---:|---:|---:|---:|---:|---:|
| oe | 7.73 | 81.1 | 23.2% | 423 | 236416 | 2.05 | 2.31 |
| oe (lazy open) | 7.73 | 81.1 | 23.2% | 431 | 231852 | 1.09 | 2.95 |
| parquet | 3.41 | 35.8 | 10.2% | 117 | 857219 | 0.05 | 3.05 |
| vortex | 4.18 | 43.9 | 12.5% | 261 | 382724 | 0.65 | 1.12 |
| json | 33.34 | 349.5 | 100.0% | 135 | 739562 | 0.00 | 150.17 |
| json+zstd | 5.10 | 53.5 | 15.3% | 229 | 437446 | 0.00 | 223.62 |

| Query (median ms; entity_lookup is per lookup) | Rows matched | oe | oe (lazy open) | parquet | vortex | json | json+zstd |
|---|---:|---:|---:|---:|---:|---:|---:|
| scan_all_columns | 100000 | 8.58 | 13.59 | 25.11 | 10.02 | 237.13 | 289.36 |
| scan_one_number | 100000 | 0.14 | 0.35 | 1.59 | 0.24 | 138.29 | 193.59 |
| scan_one_payload_number | 100000 | 0.10 | 0.18 | 0.40 | 0.16 | 146.55 | 194.08 |
| scan_one_payload_text | 100000 | 2.62 | 4.94 | 9.88 | 2.84 | 147.35 | 214.33 |
| filter_type_and_payload | 2904 | 0.49 | 1.05 | 3.55 | 0.79 | 150.43 | 208.96 |
| time_window_1pct | 1000 | 0.01 | 0.02 | 2.03 | 0.37 | 163.29 | 197.26 |
| entity_lookup | 81077 | 0.44 | 0.85 | 2.90 | 0.66 | 162.97 | 199.47 |
| latest_50_in_room | 64860 | 0.53 | 0.88 | 4.48 | 0.90 | 168.25 | 174.65 |
| thread_replies | 23 | 0.08 | 0.13 | 4.94 | 0.95 | 133.90 | 203.08 |
| event_by_id | 20 | 0.18 | 0.25 | 6.74 | 0.54 | 143.31 | 216.55 |

Removing events (redaction). OE records a deletion vector; the other formats rewrite the whole file.

| Case | Events removed | oe (ms) | oe (bytes written) | oe (lazy open) (ms) | oe (lazy open) (bytes written) | parquet (ms) | parquet (bytes written) | vortex (ms) | vortex (bytes written) | json (ms) | json (bytes written) | json+zstd (ms) | json+zstd (bytes written) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| redact_1_event | 1 | 0.00 | 9 | 0.00 | 9 | 211.34 | 3580849 | 319.82 | 4381044 | 440.35 | 34954335 | 579.33 | 5350605 |
| redact_1pct_of_events | 1000 | 0.66 | 4005 | 1.16 | 4005 | 246.83 | 3521126 | 314.32 | 4330708 | 382.42 | 34607634 | 521.97 | 5298958 |

## chat events: 1000000 rows

| Format | Size (MiB) | Bytes per event | Size vs JSON | Write (ms) | Write (events/s) | Open (ms) | Open + first lookup (ms) |
|---|---:|---:|---:|---:|---:|---:|---:|
| oe | 83.33 | 87.4 | 24.9% | 4404 | 227065 | 15.53 | 13.26 |
| oe (lazy open) | 83.33 | 87.4 | 24.9% | 4434 | 225527 | 11.34 | 21.84 |
| parquet | 35.96 | 37.7 | 10.8% | 1119 | 893439 | 0.16 | 32.28 |
| vortex | 45.86 | 48.1 | 13.7% | 2273 | 439961 | 0.84 | 2.54 |
| json | 334.22 | 350.5 | 100.0% | 1289 | 775576 | 8.45 | 1489.25 |
| json+zstd | 52.90 | 55.5 | 15.8% | 2775 | 360372 | 0.00 | 2075.09 |

| Query (median ms; entity_lookup is per lookup) | Rows matched | oe | oe (lazy open) | parquet | vortex | json | json+zstd |
|---|---:|---:|---:|---:|---:|---:|---:|
| scan_all_columns | 1000000 | 112.71 | 87.97 | 230.85 | 105.91 | 2682.60 | 3298.10 |
| scan_one_number | 1000000 | 1.65 | 1.65 | 13.13 | 1.91 | 1481.27 | 1869.63 |
| scan_one_payload_number | 1000000 | 1.11 | 1.09 | 3.37 | 1.58 | 1425.18 | 1960.06 |
| scan_one_payload_text | 1000000 | 31.18 | 28.22 | 88.38 | 25.18 | 1703.13 | 2103.59 |
| filter_type_and_payload | 30248 | 6.83 | 7.03 | 30.09 | 2.64 | 1478.96 | 2150.75 |
| time_window_1pct | 10000 | 0.05 | 0.06 | 2.22 | 0.47 | 1658.67 | 1938.92 |
| entity_lookup | 200587 | 1.73 | 2.18 | 26.25 | 1.64 | 1589.81 | 1997.52 |
| latest_50_in_room | 160250 | 1.81 | 2.29 | 45.21 | 2.18 | 1593.11 | 2139.75 |
| thread_replies | 22 | 0.31 | 0.29 | 21.36 | 2.91 | 1653.85 | 2156.46 |
| event_by_id | 20 | 0.17 | 0.23 | 5.81 | 0.63 | 1612.01 | 2013.98 |

Removing events (redaction). OE records a deletion vector; the other formats rewrite the whole file.

| Case | Events removed | oe (ms) | oe (bytes written) | oe (lazy open) (ms) | oe (lazy open) (bytes written) | parquet (ms) | parquet (bytes written) | vortex (ms) | vortex (bytes written) | json (ms) | json (bytes written) | json+zstd (ms) | json+zstd (bytes written) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| redact_1_event | 1 | 0.00 | 9 | 0.00 | 9 | 1979.08 | 37698174 | 2775.33 | 48035456 | 4819.39 | 350453613 | 6429.29 | 55469073 |
| redact_1pct_of_events | 10000 | 13.23 | 160005 | 14.36 | 160005 | 1937.41 | 37167023 | 2861.52 | 47241092 | 4904.32 | 346953322 | 6569.03 | 54925975 |

## Durable ingest

Each batch is acknowledged only after the file is synced to disk (300 batches per row). Parquet and Vortex cannot append, so each batch is a new file.

| Batch size | Format | Events/s | Ack p50 (ms) | Ack p99 (ms) | Bytes on disk |
|---:|---|---:|---:|---:|---:|
| 1 | oej journal | 3038 | 0.293 | 0.933 | 1228800 |
| 1 | json (append) | 5081 | 0.167 | 0.505 | 104659 |
| 1 | parquet (file per batch) | 951 | 1.007 | 1.591 | 1094080 |
| 1 | vortex (file per batch) | 584 | 1.647 | 2.653 | 3131136 |
| 16 | oej journal | 68514 | 0.215 | 0.466 | 2457600 |
| 16 | json (append) | 64153 | 0.223 | 0.668 | 1664629 |
| 16 | parquet (file per batch) | 12742 | 1.295 | 1.879 | 1373373 |
| 16 | vortex (file per batch) | 1023 | 14.877 | 24.305 | 4804864 |
| 128 | oej journal | 222519 | 0.553 | 1.092 | 19660800 |
| 128 | json (append) | 204959 | 0.606 | 1.022 | 13400243 |
| 128 | parquet (file per batch) | 93994 | 1.324 | 2.133 | 2851044 |
| 128 | vortex (file per batch) | 5995 | 21.036 | 36.760 | 7595608 |

