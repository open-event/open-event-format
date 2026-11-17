# Vector search benchmark

Recall@10 against f32 brute force, per-query latency (p50/p99 over 100 queries), sealed blocks opened and rows reranked per query, segment bytes per vector (+ partition index), build time. Cosine, default settings (`SegmentSpec::new`, `DEFAULT_OVERSAMPLE`, `SearchOptions::default`). Machine: 4 logical CPUs, x86_64.

| Dataset | Workload | Configuration | Recall@10 | p50 ms | p99 ms | Blocks opened | Rows reranked | Bytes/vector (+ index) | Build ms (segment + index) |
|---|---|---|---:|---:|---:|---:|---:|---:|---:|
| 10000 × 128 time-ordered | top-10 | exact (f32 brute force) | 1.000 | 0.697 | 1.326 | – | – | 512 | – |
| 10000 × 128 time-ordered | top-10 | scan | 0.994 | 2.470 | 2.868 | 152.4 | 186.4 | 567 | 64 + 0 |
| 10000 × 128 time-ordered | top-10 | indexed (IVF) | 0.994 | 2.435 | 4.551 | 152.3 | 186.4 | 567 + 6 | 64 + 735 |
| 10000 × 128 time-ordered | top-10 | clustered merge + scan | 0.994 | 0.478 | 0.637 | 51.0 | 198.9 | 568 | 859 + 0 |
| 10000 × 128 time-ordered | top-10 | clustered merge + indexed | 0.994 | 0.347 | 0.432 | 15.0 | 187.9 | 568 + 6 | 859 + 728 |
| 10000 × 128 time-ordered | room filter 1% | exact (f32 brute force) | 1.000 | 0.134 | 0.160 | – | – | 512 | – |
| 10000 × 128 time-ordered | room filter 1% | scan | 0.998 | 8.087 | 8.901 | 417.7 | 6433.1 | 567 | 64 + 0 |
| 10000 × 128 time-ordered | room filter 1% | indexed (IVF) | 1.000 | 8.406 | 9.268 | 420.4 | 6811.2 | 567 + 6 | 64 + 735 |
| 10000 × 128 time-ordered | room filter 1% | clustered merge + scan | 0.998 | 5.988 | 6.416 | 311.8 | 6455.9 | 568 | 859 + 0 |
| 10000 × 128 time-ordered | room filter 1% | clustered merge + indexed | 1.000 | 6.478 | 9.937 | 277.7 | 7542.7 | 568 + 6 | 859 + 728 |
| 10000 × 128 time-ordered | room filter 50% | exact (f32 brute force) | 1.000 | 0.590 | 0.756 | – | – | 512 | – |
| 10000 × 128 time-ordered | room filter 50% | scan | 1.000 | 3.105 | 4.602 | 156.6 | 198.8 | 567 | 64 + 0 |
| 10000 × 128 time-ordered | room filter 50% | indexed (IVF) | 1.000 | 2.841 | 3.784 | 156.5 | 198.8 | 567 + 6 | 64 + 735 |
| 10000 × 128 time-ordered | room filter 50% | clustered merge + scan | 1.000 | 0.608 | 0.744 | 54.0 | 207.8 | 568 | 859 + 0 |
| 10000 × 128 time-ordered | room filter 50% | clustered merge + indexed | 1.000 | 0.504 | 0.645 | 17.4 | 199.2 | 568 + 6 | 859 + 728 |
| 10000 × 128 time-ordered | range | exact (f32 brute force) | 1.000 | 0.623 | 0.942 | – | – | 512 | – |
| 10000 × 128 time-ordered | range | scan | 1.000 | 2.752 | 3.881 | 153.8 | 198.7 | 567 | 64 + 0 |
| 10000 × 128 time-ordered | range | indexed (IVF) | 1.000 | 2.476 | 2.627 | 146.7 | 198.7 | 567 + 6 | 64 + 735 |
| 10000 × 128 time-ordered | range | clustered merge + scan | 1.000 | 0.535 | 1.933 | 68.0 | 2761.8 | 568 | 859 + 0 |
| 10000 × 128 time-ordered | range | clustered merge + indexed | 1.000 | 0.312 | 0.498 | 8.9 | 220.7 | 568 + 6 | 859 + 728 |
| 10000 × 128 cluster-ordered | top-10 | exact (f32 brute force) | 1.000 | 0.708 | 1.081 | – | – | 512 | – |
| 10000 × 128 cluster-ordered | top-10 | scan | 0.996 | 0.489 | 0.721 | 53.1 | 194.7 | 567 | 30 + 0 |
| 10000 × 128 cluster-ordered | top-10 | indexed (IVF) | 0.996 | 0.341 | 0.521 | 14.3 | 187.9 | 567 + 6 | 30 + 716 |
| 10000 × 128 cluster-ordered | room filter 1% | exact (f32 brute force) | 1.000 | 0.135 | 0.176 | – | – | 512 | – |
| 10000 × 128 cluster-ordered | room filter 1% | scan | 1.000 | 6.161 | 7.079 | 316.7 | 6462.5 | 567 | 30 + 0 |
| 10000 × 128 cluster-ordered | room filter 1% | indexed (IVF) | 1.000 | 6.532 | 7.340 | 280.0 | 7980.3 | 567 + 6 | 30 + 716 |
| 10000 × 128 cluster-ordered | room filter 50% | exact (f32 brute force) | 1.000 | 0.588 | 0.989 | – | – | 512 | – |
| 10000 × 128 cluster-ordered | room filter 50% | scan | 1.000 | 0.672 | 0.963 | 55.7 | 204.3 | 567 | 30 + 0 |
| 10000 × 128 cluster-ordered | room filter 50% | indexed (IVF) | 1.000 | 0.516 | 0.814 | 16.0 | 198.8 | 567 + 6 | 30 + 716 |
| 10000 × 128 cluster-ordered | range | exact (f32 brute force) | 1.000 | 0.641 | 0.889 | – | – | 512 | – |
| 10000 × 128 cluster-ordered | range | scan | 1.000 | 0.510 | 1.966 | 66.3 | 2100.1 | 567 | 30 + 0 |
| 10000 × 128 cluster-ordered | range | indexed (IVF) | 1.000 | 0.300 | 0.434 | 6.5 | 198.7 | 567 + 6 | 30 + 716 |
| 10000 × 768 time-ordered | top-10 | exact (f32 brute force) | 1.000 | 5.066 | 6.301 | – | – | 3072 | – |
| 10000 × 768 time-ordered | top-10 | scan | 0.992 | 12.593 | 14.389 | 152.6 | 182.6 | 3228 | 171 + 0 |
| 10000 × 768 time-ordered | top-10 | indexed (IVF) | 0.992 | 12.011 | 13.601 | 152.5 | 182.6 | 3228 + 24 | 171 + 5686 |
| 10000 × 768 time-ordered | top-10 | clustered merge + scan | 0.992 | 1.250 | 1.446 | 51.0 | 199.1 | 3228 | 6075 + 0 |
| 10000 × 768 time-ordered | top-10 | clustered merge + indexed | 0.992 | 0.888 | 1.050 | 13.5 | 188.9 | 3228 + 29 | 6075 + 5597 |
| 10000 × 768 time-ordered | room filter 1% | exact (f32 brute force) | 1.000 | 0.168 | 0.191 | – | – | 3072 | – |
| 10000 × 768 time-ordered | room filter 1% | scan | 1.000 | 36.015 | 41.732 | 421.1 | 6781.4 | 3228 | 171 + 0 |
| 10000 × 768 time-ordered | room filter 1% | indexed (IVF) | 1.000 | 36.735 | 41.642 | 423.1 | 7008.5 | 3228 + 24 | 171 + 5686 |
| 10000 × 768 time-ordered | room filter 1% | clustered merge + scan | 1.000 | 26.486 | 30.860 | 329.7 | 6824.0 | 3228 | 6075 + 0 |
| 10000 × 768 time-ordered | room filter 1% | clustered merge + indexed | 1.000 | 27.531 | 33.563 | 295.8 | 8263.5 | 3228 + 29 | 6075 + 5597 |
| 10000 × 768 time-ordered | room filter 50% | exact (f32 brute force) | 1.000 | 2.834 | 3.831 | – | – | 3072 | – |
| 10000 × 768 time-ordered | room filter 50% | scan | 1.000 | 13.707 | 20.232 | 157.4 | 199.9 | 3228 | 171 + 0 |
| 10000 × 768 time-ordered | room filter 50% | indexed (IVF) | 1.000 | 12.913 | 17.065 | 157.2 | 199.9 | 3228 + 24 | 171 + 5686 |
| 10000 × 768 time-ordered | room filter 50% | clustered merge + scan | 1.000 | 1.583 | 2.448 | 52.7 | 211.9 | 3228 | 6075 + 0 |
| 10000 × 768 time-ordered | room filter 50% | clustered merge + indexed | 1.000 | 1.030 | 1.271 | 15.0 | 203.0 | 3228 + 29 | 6075 + 5597 |
| 10000 × 768 time-ordered | range | exact (f32 brute force) | 1.000 | 4.922 | 6.359 | – | – | 3072 | – |
| 10000 × 768 time-ordered | range | scan | 1.000 | 13.070 | 28.602 | 154.5 | 199.9 | 3228 | 171 + 0 |
| 10000 × 768 time-ordered | range | indexed (IVF) | 1.000 | 12.516 | 14.395 | 148.1 | 199.8 | 3228 + 24 | 171 + 5686 |
| 10000 × 768 time-ordered | range | clustered merge + scan | 1.000 | 8.024 | 9.289 | 75.0 | 3485.2 | 3228 | 6075 + 0 |
| 10000 × 768 time-ordered | range | clustered merge + indexed | 1.000 | 0.854 | 1.124 | 7.7 | 199.8 | 3228 + 29 | 6075 + 5597 |
| 10000 × 768 cluster-ordered | top-10 | exact (f32 brute force) | 1.000 | 4.938 | 6.209 | – | – | 3072 | – |
| 10000 × 768 cluster-ordered | top-10 | scan | 0.994 | 1.301 | 1.511 | 51.8 | 203.3 | 3228 | 178 + 0 |
| 10000 × 768 cluster-ordered | top-10 | indexed (IVF) | 0.994 | 0.871 | 1.141 | 13.9 | 188.2 | 3228 + 28 | 178 + 5774 |
| 10000 × 768 cluster-ordered | room filter 1% | exact (f32 brute force) | 1.000 | 0.168 | 0.211 | – | – | 3072 | – |
| 10000 × 768 cluster-ordered | room filter 1% | scan | 1.000 | 24.758 | 34.164 | 317.6 | 6812.6 | 3228 | 178 + 0 |
| 10000 × 768 cluster-ordered | room filter 1% | indexed (IVF) | 1.000 | 26.222 | 36.903 | 282.9 | 8494.1 | 3228 + 28 | 178 + 5774 |
| 10000 × 768 cluster-ordered | room filter 50% | exact (f32 brute force) | 1.000 | 2.956 | 4.065 | – | – | 3072 | – |
| 10000 × 768 cluster-ordered | room filter 50% | scan | 1.000 | 2.174 | 2.733 | 53.1 | 213.5 | 3228 | 178 + 0 |
| 10000 × 768 cluster-ordered | room filter 50% | indexed (IVF) | 1.000 | 1.059 | 1.639 | 15.5 | 203.0 | 3228 + 28 | 178 + 5774 |
| 10000 × 768 cluster-ordered | range | exact (f32 brute force) | 1.000 | 5.063 | 6.571 | – | – | 3072 | – |
| 10000 × 768 cluster-ordered | range | scan | 1.000 | 7.991 | 12.762 | 74.4 | 3196.4 | 3228 | 178 + 0 |
| 10000 × 768 cluster-ordered | range | indexed (IVF) | 1.000 | 0.873 | 1.463 | 7.3 | 216.6 | 3228 + 28 | 178 + 5774 |
| multivector 1000 docs / 8185 tokens × 128 | MaxSim top-10 | exact (f32 MaxSim) | 1.000 | 2.053 | 2.233 | – | – | 512 | – |
| multivector 1000 docs / 8185 tokens × 128 | MaxSim top-10 | scan | 1.000 | 3.791 | 5.441 | 123.9 | 3995.8 | 551 | 26 + 0 |
| multivector 1000 docs / 8185 tokens × 128 | MaxSim top-10 | indexed (IVF) | 1.000 | 4.604 | 14.657 | 223.0 | 4491.9 | 551 + 7 | 26 + 525 |
| 100000 × 128 time-ordered | top-10 | exact (f32 brute force) | 1.000 | 7.669 | 10.348 | – | – | 512 | – |
| 100000 × 128 time-ordered | top-10 | scan | 0.876 | 11.377 | 14.824 | 1026.2 | 283.1 | 568 | 331 + 0 |
| 100000 × 128 time-ordered | top-10 | indexed (IVF) | 0.876 | 10.360 | 14.482 | 1004.0 | 283.1 | 568 + 3 | 331 + 22394 |
| 100000 × 128 time-ordered | top-10 | clustered merge + scan | 0.876 | 1.224 | 2.930 | 163.0 | 339.9 | 568 | 22902 + 0 |
| 100000 × 128 time-ordered | top-10 | clustered merge + indexed | 0.876 | 0.649 | 1.170 | 25.9 | 307.5 | 568 + 3 | 22902 + 22707 |
| 100000 × 128 time-ordered | room filter 1% | exact (f32 brute force) | 1.000 | 1.344 | 1.944 | – | – | 512 | – |
| 100000 × 128 time-ordered | room filter 1% | scan | 0.982 | 44.574 | 60.674 | 3188.9 | 1380.9 | 568 | 331 + 0 |
| 100000 × 128 time-ordered | room filter 1% | indexed (IVF) | 0.994 | 42.416 | 59.174 | 3120.7 | 1383.0 | 568 + 3 | 331 + 22394 |
| 100000 × 128 time-ordered | room filter 1% | clustered merge + scan | 0.982 | 9.277 | 12.314 | 1154.6 | 1594.5 | 568 | 22902 + 0 |
| 100000 × 128 time-ordered | room filter 1% | clustered merge + indexed | 0.993 | 9.445 | 24.221 | 712.4 | 1484.4 | 568 + 3 | 22902 + 22707 |
| 100000 × 128 time-ordered | room filter 50% | exact (f32 brute force) | 1.000 | 6.037 | 9.453 | – | – | 512 | – |
| 100000 × 128 time-ordered | room filter 50% | scan | 1.000 | 20.535 | 24.624 | 1246.9 | 628.5 | 568 | 331 + 0 |
| 100000 × 128 time-ordered | room filter 50% | indexed (IVF) | 1.000 | 18.899 | 28.233 | 1229.3 | 628.5 | 568 + 3 | 331 + 22394 |
| 100000 × 128 time-ordered | room filter 50% | clustered merge + scan | 1.000 | 1.324 | 1.877 | 176.8 | 646.6 | 568 | 22902 + 0 |
| 100000 × 128 time-ordered | room filter 50% | clustered merge + indexed | 1.000 | 0.796 | 1.185 | 29.4 | 629.4 | 568 + 3 | 22902 + 22707 |
| 100000 × 128 time-ordered | range | exact (f32 brute force) | 1.000 | 6.882 | 9.941 | – | – | 512 | – |
| 100000 × 128 time-ordered | range | scan | 1.000 | 19.263 | 21.862 | 1232.8 | 628.4 | 568 | 331 + 0 |
| 100000 × 128 time-ordered | range | indexed (IVF) | 1.000 | 16.907 | 20.718 | 1038.1 | 628.2 | 568 + 3 | 331 + 22394 |
| 100000 × 128 time-ordered | range | clustered merge + scan | 1.000 | 5.628 | 8.986 | 244.8 | 12794.5 | 568 | 22902 + 0 |
| 100000 × 128 time-ordered | range | clustered merge + indexed | 1.000 | 0.594 | 0.822 | 13.4 | 628.3 | 568 + 3 | 22902 + 22707 |
| 100000 × 128 cluster-ordered | top-10 | exact (f32 brute force) | 1.000 | 7.383 | 10.391 | – | – | 512 | – |
| 100000 × 128 cluster-ordered | top-10 | scan | 0.865 | 1.418 | 2.175 | 169.4 | 329.2 | 568 | 351 + 0 |
| 100000 × 128 cluster-ordered | top-10 | indexed (IVF) | 0.865 | 0.806 | 1.271 | 28.9 | 295.6 | 568 + 3 | 351 + 22685 |
| 100000 × 128 cluster-ordered | room filter 1% | exact (f32 brute force) | 1.000 | 1.402 | 2.624 | – | – | 512 | – |
| 100000 × 128 cluster-ordered | room filter 1% | scan | 0.983 | 10.003 | 13.860 | 1187.8 | 1611.7 | 568 | 351 + 0 |
| 100000 × 128 cluster-ordered | room filter 1% | indexed (IVF) | 0.991 | 10.226 | 27.492 | 772.2 | 1532.7 | 568 + 3 | 351 + 22685 |
| 100000 × 128 cluster-ordered | room filter 50% | exact (f32 brute force) | 1.000 | 6.254 | 9.481 | – | – | 512 | – |
| 100000 × 128 cluster-ordered | room filter 50% | scan | 1.000 | 1.418 | 1.859 | 183.2 | 644.9 | 568 | 351 + 0 |
| 100000 × 128 cluster-ordered | room filter 50% | indexed (IVF) | 1.000 | 0.848 | 1.295 | 34.5 | 628.9 | 568 + 3 | 351 + 22685 |
| 100000 × 128 cluster-ordered | range | exact (f32 brute force) | 1.000 | 6.602 | 10.408 | – | – | 512 | – |
| 100000 × 128 cluster-ordered | range | scan | 1.000 | 5.841 | 7.149 | 230.8 | 10333.5 | 568 | 351 + 0 |
| 100000 × 128 cluster-ordered | range | indexed (IVF) | 1.000 | 0.652 | 0.974 | 15.5 | 692.6 | 568 + 3 | 351 + 22685 |
| 100000 × 768 time-ordered | top-10 | exact (f32 brute force) | 1.000 | 64.862 | 77.208 | – | – | 3072 | – |
| 100000 × 768 time-ordered | top-10 | scan | 0.885 | 55.391 | 84.488 | 1022.9 | 277.5 | 3228 | 1782 + 0 |
| 100000 × 768 time-ordered | top-10 | indexed (IVF) | 0.885 | 51.760 | 72.465 | 1007.2 | 277.5 | 3228 + 10 | 1782 + 182711 |
| 100000 × 768 time-ordered | top-10 | clustered merge + scan | 0.885 | 3.789 | 4.851 | 163.0 | 333.8 | 3228 | 186580 + 0 |
| 100000 × 768 time-ordered | top-10 | clustered merge + indexed | 0.885 | 1.989 | 2.778 | 22.5 | 315.7 | 3228 + 9 | 186580 + 180338 |
| 100000 × 768 time-ordered | room filter 1% | exact (f32 brute force) | 1.000 | 1.817 | 2.388 | – | – | 3072 | – |
| 100000 × 768 time-ordered | room filter 1% | scan | 0.983 | 215.649 | 237.379 | 3177.8 | 1376.1 | 3228 | 1782 + 0 |
| 100000 × 768 time-ordered | room filter 1% | indexed (IVF) | 0.990 | 209.904 | 244.700 | 3118.3 | 1372.6 | 3228 + 10 | 1782 + 182711 |
| 100000 × 768 time-ordered | room filter 1% | clustered merge + scan | 0.983 | 32.323 | 54.124 | 989.1 | 1642.6 | 3228 | 186580 + 0 |
| 100000 × 768 time-ordered | room filter 1% | clustered merge + indexed | 0.990 | 29.622 | 120.062 | 711.5 | 1498.9 | 3228 + 9 | 186580 + 180338 |
| 100000 × 768 time-ordered | room filter 50% | exact (f32 brute force) | 1.000 | 40.076 | 45.308 | – | – | 3072 | – |
| 100000 × 768 time-ordered | room filter 50% | scan | 1.000 | 97.063 | 107.298 | 1246.6 | 627.1 | 3228 | 1782 + 0 |
| 100000 × 768 time-ordered | room filter 50% | indexed (IVF) | 1.000 | 90.317 | 105.543 | 1231.9 | 627.1 | 3228 + 10 | 1782 + 182711 |
| 100000 × 768 time-ordered | room filter 50% | clustered merge + scan | 1.000 | 4.151 | 6.868 | 169.8 | 647.3 | 3228 | 186580 + 0 |
| 100000 × 768 time-ordered | room filter 50% | clustered merge + indexed | 1.000 | 2.175 | 3.008 | 24.1 | 634.5 | 3228 + 9 | 186580 + 180338 |
| 100000 × 768 time-ordered | range | exact (f32 brute force) | 1.000 | 64.551 | 81.442 | – | – | 3072 | – |
| 100000 × 768 time-ordered | range | scan | 1.000 | 97.616 | 123.507 | 1232.9 | 626.9 | 3228 | 1782 + 0 |
| 100000 × 768 time-ordered | range | indexed (IVF) | 1.000 | 89.001 | 104.660 | 983.9 | 626.8 | 3228 + 10 | 1782 + 182711 |
| 100000 × 768 time-ordered | range | clustered merge + scan | 1.000 | 28.920 | 32.689 | 248.6 | 12757.2 | 3228 | 186580 + 0 |
| 100000 × 768 time-ordered | range | clustered merge + indexed | 1.000 | 2.303 | 3.329 | 16.7 | 707.0 | 3228 + 9 | 186580 + 180338 |
| 100000 × 768 cluster-ordered | top-10 | exact (f32 brute force) | 1.000 | 66.185 | 79.858 | – | – | 3072 | – |
| 100000 × 768 cluster-ordered | top-10 | scan | 0.897 | 3.991 | 6.046 | 164.8 | 327.1 | 3228 | 1755 + 0 |
| 100000 × 768 cluster-ordered | top-10 | indexed (IVF) | 0.897 | 2.219 | 3.100 | 21.8 | 310.4 | 3228 + 9 | 1755 + 182007 |
| 100000 × 768 cluster-ordered | room filter 1% | exact (f32 brute force) | 1.000 | 1.897 | 2.361 | – | – | 3072 | – |
| 100000 × 768 cluster-ordered | room filter 1% | scan | 0.978 | 33.645 | 46.638 | 1008.9 | 1627.2 | 3228 | 1755 + 0 |
| 100000 × 768 cluster-ordered | room filter 1% | indexed (IVF) | 0.995 | 32.739 | 132.522 | 764.3 | 1515.5 | 3228 + 9 | 1755 + 182007 |
| 100000 × 768 cluster-ordered | room filter 50% | exact (f32 brute force) | 1.000 | 40.811 | 44.925 | – | – | 3072 | – |
| 100000 × 768 cluster-ordered | room filter 50% | scan | 1.000 | 4.180 | 4.952 | 167.4 | 644.0 | 3228 | 1755 + 0 |
| 100000 × 768 cluster-ordered | room filter 50% | indexed (IVF) | 1.000 | 2.723 | 3.638 | 23.2 | 627.4 | 3228 + 9 | 1755 + 182007 |
| 100000 × 768 cluster-ordered | range | exact (f32 brute force) | 1.000 | 65.738 | 74.938 | – | – | 3072 | – |
| 100000 × 768 cluster-ordered | range | scan | 1.000 | 31.654 | 40.107 | 245.6 | 11356.9 | 3228 | 1755 + 0 |
| 100000 × 768 cluster-ordered | range | indexed (IVF) | 1.000 | 1.888 | 2.806 | 14.0 | 645.1 | 3228 + 9 | 1755 + 182007 |
| multivector 2000 docs / 16158 tokens × 128 | MaxSim top-10 | exact (f32 MaxSim) | 1.000 | 4.049 | 4.864 | – | – | 512 | – |
| multivector 2000 docs / 16158 tokens × 128 | MaxSim top-10 | scan | 1.000 | 8.440 | 9.953 | 264.0 | 16158.0 | 552 | 53 + 0 |
| multivector 2000 docs / 16158 tokens × 128 | MaxSim top-10 | indexed (IVF) | 1.000 | 13.894 | 19.178 | 644.9 | 17527.7 | 552 + 5 | 53 + 1523 |
