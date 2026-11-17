# Vector search benchmark

Recall@10 against f32 brute force, per-query latency (p50/p99 over 100 queries), sealed blocks opened and rows reranked per query, segment bytes per vector (+ partition index), build time. Cosine, `SegmentSpec::new` representations, `SearchOptions::default`, `oversample` 8 (default 8). Machine: 4 logical CPUs, x86_64.

| Dataset | Workload | Configuration | Recall@10 | p50 ms | p99 ms | Blocks opened | Rows reranked | Bytes/vector (+ index) | Build ms (segment + index) |
|---|---|---|---:|---:|---:|---:|---:|---:|---:|
| 10000 × 128 time-ordered | top-10 | exact (f32 brute force) | 1.000 | 0.721 | 0.939 | – | – | 512 | – |
| 10000 × 128 time-ordered | top-10 | scan | 1.000 | 2.741 | 3.666 | 153.5 | 197.4 | 567 | 63 + 0 |
| 10000 × 128 time-ordered | top-10 | indexed (IVF) | 1.000 | 2.675 | 4.127 | 153.3 | 197.4 | 567 + 6 | 63 + 47 |
| 10000 × 128 time-ordered | top-10 | clustered merge + scan | 1.000 | 0.526 | 0.610 | 50.3 | 205.6 | 568 | 165 + 0 |
| 10000 × 128 time-ordered | top-10 | clustered merge + indexed | 1.000 | 0.399 | 0.482 | 13.3 | 198.4 | 568 + 6 | 165 + 41 |
| 10000 × 128 time-ordered | room filter 1% | exact (f32 brute force) | 1.000 | 0.131 | 0.161 | – | – | 512 | – |
| 10000 × 128 time-ordered | room filter 1% | scan | 0.998 | 2.150 | 2.680 | 106.0 | 62.9 | 567 | 63 + 0 |
| 10000 × 128 time-ordered | room filter 1% | indexed (IVF) | 1.000 | 2.504 | 3.127 | 108.6 | 68.2 | 567 + 6 | 63 + 47 |
| 10000 × 128 time-ordered | room filter 1% | clustered merge + scan | 0.998 | 2.109 | 2.701 | 101.5 | 63.3 | 568 | 165 + 0 |
| 10000 × 128 time-ordered | room filter 1% | clustered merge + indexed | 0.999 | 2.350 | 3.117 | 104.1 | 68.6 | 568 + 6 | 165 + 41 |
| 10000 × 128 time-ordered | room filter 50% | exact (f32 brute force) | 1.000 | 0.599 | 0.930 | – | – | 512 | – |
| 10000 × 128 time-ordered | room filter 50% | scan | 1.000 | 3.508 | 9.931 | 159.6 | 198.8 | 567 | 63 + 0 |
| 10000 × 128 time-ordered | room filter 50% | indexed (IVF) | 1.000 | 3.238 | 4.052 | 159.5 | 198.8 | 567 + 6 | 63 + 47 |
| 10000 × 128 time-ordered | room filter 50% | clustered merge + scan | 1.000 | 1.001 | 1.176 | 56.1 | 206.4 | 568 | 165 + 0 |
| 10000 × 128 time-ordered | room filter 50% | clustered merge + indexed | 1.000 | 0.860 | 1.183 | 18.4 | 199.8 | 568 + 6 | 165 + 41 |
| 10000 × 128 time-ordered | range | exact (f32 brute force) | 1.000 | 0.622 | 0.881 | – | – | 512 | – |
| 10000 × 128 time-ordered | range | scan | 1.000 | 2.714 | 2.879 | 153.8 | 198.7 | 567 | 63 + 0 |
| 10000 × 128 time-ordered | range | indexed (IVF) | 1.000 | 2.695 | 3.040 | 146.7 | 198.7 | 567 + 6 | 63 + 47 |
| 10000 × 128 time-ordered | range | clustered merge + scan | 1.000 | 0.614 | 2.034 | 66.9 | 2678.9 | 568 | 165 + 0 |
| 10000 × 128 time-ordered | range | clustered merge + indexed | 1.000 | 0.380 | 0.568 | 7.1 | 198.7 | 568 + 6 | 165 + 41 |
| 10000 × 128 cluster-ordered | top-10 | exact (f32 brute force) | 1.000 | 0.691 | 0.913 | – | – | 512 | – |
| 10000 × 128 cluster-ordered | top-10 | scan | 1.000 | 0.556 | 0.792 | 53.1 | 204.2 | 567 | 30 + 0 |
| 10000 × 128 cluster-ordered | top-10 | indexed (IVF) | 1.000 | 0.406 | 0.648 | 14.4 | 198.7 | 567 + 6 | 30 + 54 |
| 10000 × 128 cluster-ordered | room filter 1% | exact (f32 brute force) | 1.000 | 0.128 | 0.202 | – | – | 512 | – |
| 10000 × 128 cluster-ordered | room filter 1% | scan | 1.000 | 2.196 | 2.871 | 105.3 | 64.5 | 567 | 30 + 0 |
| 10000 × 128 cluster-ordered | room filter 1% | indexed (IVF) | 1.000 | 2.484 | 3.190 | 108.1 | 69.6 | 567 + 6 | 30 + 54 |
| 10000 × 128 cluster-ordered | room filter 50% | exact (f32 brute force) | 1.000 | 0.579 | 0.844 | – | – | 512 | – |
| 10000 × 128 cluster-ordered | room filter 50% | scan | 1.000 | 1.035 | 1.402 | 58.7 | 204.3 | 567 | 30 + 0 |
| 10000 × 128 cluster-ordered | room filter 50% | indexed (IVF) | 1.000 | 0.878 | 8.975 | 19.1 | 198.9 | 567 + 6 | 30 + 54 |
| 10000 × 128 cluster-ordered | range | exact (f32 brute force) | 1.000 | 0.618 | 0.871 | – | – | 512 | – |
| 10000 × 128 cluster-ordered | range | scan | 1.000 | 0.572 | 2.157 | 66.3 | 2100.1 | 567 | 30 + 0 |
| 10000 × 128 cluster-ordered | range | indexed (IVF) | 1.000 | 0.375 | 0.616 | 6.5 | 198.7 | 567 + 6 | 30 + 54 |
| 10000 × 768 time-ordered | top-10 | exact (f32 brute force) | 1.000 | 5.092 | 11.115 | – | – | 3072 | – |
| 10000 × 768 time-ordered | top-10 | scan | 0.999 | 13.743 | 16.788 | 154.3 | 198.6 | 3228 | 176 + 0 |
| 10000 × 768 time-ordered | top-10 | indexed (IVF) | 0.999 | 12.224 | 16.185 | 154.1 | 198.6 | 3228 + 24 | 176 + 244 |
| 10000 × 768 time-ordered | top-10 | clustered merge + scan | 0.999 | 1.319 | 1.548 | 50.8 | 209.0 | 3228 | 571 + 0 |
| 10000 × 768 time-ordered | top-10 | clustered merge + indexed | 0.999 | 0.887 | 1.056 | 12.6 | 201.5 | 3228 + 29 | 571 + 254 |
| 10000 × 768 time-ordered | room filter 1% | exact (f32 brute force) | 1.000 | 0.168 | 0.191 | – | – | 3072 | – |
| 10000 × 768 time-ordered | room filter 1% | scan | 1.000 | 8.538 | 10.364 | 108.5 | 66.4 | 3228 | 176 + 0 |
| 10000 × 768 time-ordered | room filter 1% | indexed (IVF) | 1.000 | 8.990 | 10.039 | 110.8 | 71.6 | 3228 + 24 | 176 + 244 |
| 10000 × 768 time-ordered | room filter 1% | clustered merge + scan | 1.000 | 8.787 | 10.210 | 116.7 | 66.9 | 3228 | 571 + 0 |
| 10000 × 768 time-ordered | room filter 1% | clustered merge + indexed | 1.000 | 10.122 | 18.938 | 119.7 | 72.2 | 3228 + 29 | 571 + 254 |
| 10000 × 768 time-ordered | room filter 50% | exact (f32 brute force) | 1.000 | 2.898 | 3.852 | – | – | 3072 | – |
| 10000 × 768 time-ordered | room filter 50% | scan | 1.000 | 13.600 | 19.073 | 160.4 | 199.9 | 3228 | 176 + 0 |
| 10000 × 768 time-ordered | room filter 50% | indexed (IVF) | 1.000 | 14.071 | 18.626 | 160.2 | 199.9 | 3228 + 24 | 176 + 244 |
| 10000 × 768 time-ordered | room filter 50% | clustered merge + scan | 1.000 | 1.791 | 2.131 | 55.6 | 210.0 | 3228 | 571 + 0 |
| 10000 × 768 time-ordered | room filter 50% | clustered merge + indexed | 1.000 | 1.425 | 1.660 | 16.6 | 202.6 | 3228 + 29 | 571 + 254 |
| 10000 × 768 time-ordered | range | exact (f32 brute force) | 1.000 | 5.067 | 6.641 | – | – | 3072 | – |
| 10000 × 768 time-ordered | range | scan | 1.000 | 12.960 | 14.744 | 154.5 | 199.9 | 3228 | 176 + 0 |
| 10000 × 768 time-ordered | range | indexed (IVF) | 1.000 | 12.530 | 13.795 | 148.1 | 199.8 | 3228 + 24 | 176 + 244 |
| 10000 × 768 time-ordered | range | clustered merge + scan | 1.000 | 1.597 | 9.244 | 69.0 | 2835.1 | 3228 | 571 + 0 |
| 10000 × 768 time-ordered | range | clustered merge + indexed | 1.000 | 0.846 | 1.054 | 6.1 | 199.8 | 3228 + 29 | 571 + 254 |
| 10000 × 768 cluster-ordered | top-10 | exact (f32 brute force) | 1.000 | 5.060 | 6.371 | – | – | 3072 | – |
| 10000 × 768 cluster-ordered | top-10 | scan | 0.999 | 1.327 | 1.552 | 51.8 | 211.6 | 3228 | 179 + 0 |
| 10000 × 768 cluster-ordered | top-10 | indexed (IVF) | 0.999 | 0.913 | 1.102 | 13.4 | 198.2 | 3228 + 29 | 179 + 223 |
| 10000 × 768 cluster-ordered | room filter 1% | exact (f32 brute force) | 1.000 | 0.168 | 0.193 | – | – | 3072 | – |
| 10000 × 768 cluster-ordered | room filter 1% | scan | 1.000 | 8.454 | 10.113 | 108.9 | 68.7 | 3228 | 179 + 0 |
| 10000 × 768 cluster-ordered | room filter 1% | indexed (IVF) | 1.000 | 9.297 | 11.654 | 111.7 | 74.4 | 3228 + 29 | 179 + 223 |
| 10000 × 768 cluster-ordered | room filter 50% | exact (f32 brute force) | 1.000 | 2.903 | 4.010 | – | – | 3072 | – |
| 10000 × 768 cluster-ordered | room filter 50% | scan | 1.000 | 1.790 | 2.731 | 56.1 | 213.5 | 3228 | 179 + 0 |
| 10000 × 768 cluster-ordered | room filter 50% | indexed (IVF) | 1.000 | 1.369 | 1.858 | 18.1 | 203.0 | 3228 + 29 | 179 + 223 |
| 10000 × 768 cluster-ordered | range | exact (f32 brute force) | 1.000 | 5.158 | 6.530 | – | – | 3072 | – |
| 10000 × 768 cluster-ordered | range | scan | 1.000 | 8.210 | 10.518 | 74.4 | 3196.4 | 3228 | 179 + 0 |
| 10000 × 768 cluster-ordered | range | indexed (IVF) | 1.000 | 0.926 | 1.650 | 7.3 | 216.5 | 3228 + 29 | 179 + 223 |
| multivector 1000 docs / 8185 tokens × 128 | MaxSim top-10 | exact (f32 MaxSim) | 1.000 | 2.032 | 4.447 | – | – | 512 | – |
| multivector 1000 docs / 8185 tokens × 128 | MaxSim top-10 | scan | 1.000 | 5.456 | 9.359 | 123.9 | 3995.8 | 551 | 27 + 0 |
| multivector 1000 docs / 8185 tokens × 128 | MaxSim top-10 | indexed (IVF) | 1.000 | 3.932 | 7.187 | 123.8 | 4243.8 | 551 + 7 | 27 + 41 |
| 100000 × 128 time-ordered | top-10 | exact (f32 brute force) | 1.000 | 7.279 | 10.267 | – | – | 512 | – |
| 100000 × 128 time-ordered | top-10 | scan | 0.996 | 17.341 | 25.144 | 1213.1 | 608.9 | 568 | 418 + 0 |
| 100000 × 128 time-ordered | top-10 | indexed (IVF) | 0.996 | 15.512 | 17.355 | 1187.0 | 608.9 | 568 + 3 | 418 + 498 |
| 100000 × 128 time-ordered | top-10 | clustered merge + scan | 0.996 | 1.183 | 1.487 | 156.7 | 630.9 | 568 | 1396 + 0 |
| 100000 × 128 time-ordered | top-10 | clustered merge + indexed | 0.996 | 0.688 | 0.968 | 25.1 | 610.9 | 568 + 3 | 1396 + 465 |
| 100000 × 128 time-ordered | room filter 1% | exact (f32 brute force) | 1.000 | 1.415 | 1.819 | – | – | 512 | – |
| 100000 × 128 time-ordered | room filter 1% | scan | 0.991 | 9.699 | 20.080 | 764.4 | 175.9 | 568 | 418 + 0 |
| 100000 × 128 time-ordered | room filter 1% | indexed (IVF) | 0.994 | 10.765 | 20.428 | 695.2 | 160.3 | 568 + 3 | 418 + 498 |
| 100000 × 128 time-ordered | room filter 1% | clustered merge + scan | 0.991 | 7.153 | 15.083 | 677.5 | 177.5 | 568 | 1396 + 0 |
| 100000 × 128 time-ordered | room filter 1% | clustered merge + indexed | 0.994 | 8.729 | 24.919 | 670.2 | 218.1 | 568 + 3 | 1396 + 465 |
| 100000 × 128 time-ordered | room filter 50% | exact (f32 brute force) | 1.000 | 6.325 | 9.992 | – | – | 512 | – |
| 100000 × 128 time-ordered | room filter 50% | scan | 1.000 | 22.497 | 28.182 | 1250.9 | 628.5 | 568 | 418 + 0 |
| 100000 × 128 time-ordered | room filter 50% | indexed (IVF) | 1.000 | 19.993 | 23.568 | 1227.5 | 628.5 | 568 + 3 | 418 + 498 |
| 100000 × 128 time-ordered | room filter 50% | clustered merge + scan | 1.000 | 1.957 | 2.563 | 173.5 | 648.4 | 568 | 1396 + 0 |
| 100000 × 128 time-ordered | room filter 50% | clustered merge + indexed | 1.000 | 1.383 | 1.795 | 32.8 | 629.7 | 568 + 3 | 1396 + 465 |
| 100000 × 128 time-ordered | range | exact (f32 brute force) | 1.000 | 6.613 | 10.275 | – | – | 512 | – |
| 100000 × 128 time-ordered | range | scan | 1.000 | 20.366 | 23.065 | 1232.8 | 628.4 | 568 | 418 + 0 |
| 100000 × 128 time-ordered | range | indexed (IVF) | 1.000 | 17.932 | 23.992 | 1001.9 | 628.2 | 568 + 3 | 418 + 498 |
| 100000 × 128 time-ordered | range | clustered merge + scan | 1.000 | 5.718 | 7.249 | 250.0 | 14000.4 | 568 | 1396 + 0 |
| 100000 × 128 time-ordered | range | clustered merge + indexed | 1.000 | 0.687 | 0.949 | 13.4 | 628.3 | 568 + 3 | 1396 + 465 |
| 100000 × 128 cluster-ordered | top-10 | exact (f32 brute force) | 1.000 | 7.399 | 14.103 | – | – | 512 | – |
| 100000 × 128 cluster-ordered | top-10 | scan | 1.000 | 1.278 | 1.609 | 169.4 | 631.6 | 568 | 355 + 0 |
| 100000 × 128 cluster-ordered | top-10 | indexed (IVF) | 1.000 | 0.706 | 0.979 | 27.8 | 614.3 | 568 + 3 | 355 + 531 |
| 100000 × 128 cluster-ordered | room filter 1% | exact (f32 brute force) | 1.000 | 1.433 | 1.683 | – | – | 512 | – |
| 100000 × 128 cluster-ordered | room filter 1% | scan | 0.992 | 7.976 | 16.352 | 721.6 | 199.1 | 568 | 355 + 0 |
| 100000 × 128 cluster-ordered | room filter 1% | indexed (IVF) | 0.996 | 9.174 | 24.733 | 731.1 | 300.8 | 568 + 3 | 355 + 531 |
| 100000 × 128 cluster-ordered | room filter 50% | exact (f32 brute force) | 1.000 | 6.011 | 9.213 | – | – | 512 | – |
| 100000 × 128 cluster-ordered | room filter 50% | scan | 1.000 | 2.010 | 2.518 | 187.2 | 644.9 | 568 | 355 + 0 |
| 100000 × 128 cluster-ordered | room filter 50% | indexed (IVF) | 1.000 | 1.426 | 1.930 | 37.0 | 628.9 | 568 + 3 | 355 + 531 |
| 100000 × 128 cluster-ordered | range | exact (f32 brute force) | 1.000 | 7.119 | 10.217 | – | – | 512 | – |
| 100000 × 128 cluster-ordered | range | scan | 1.000 | 5.692 | 6.805 | 230.8 | 10333.5 | 568 | 355 + 0 |
| 100000 × 128 cluster-ordered | range | indexed (IVF) | 1.000 | 0.687 | 0.963 | 14.4 | 660.2 | 568 + 3 | 355 + 531 |
| 100000 × 768 time-ordered | top-10 | exact (f32 brute force) | 1.000 | 64.370 | 73.568 | – | – | 3072 | – |
| 100000 × 768 time-ordered | top-10 | scan | 0.997 | 119.124 | 130.926 | 1214.2 | 608.3 | 3228 | 1831 + 0 |
| 100000 × 768 time-ordered | top-10 | indexed (IVF) | 0.997 | 112.520 | 127.741 | 1195.4 | 608.3 | 3228 + 10 | 1831 + 2572 |
| 100000 × 768 time-ordered | top-10 | clustered merge + scan | 0.997 | 3.971 | 5.007 | 167.4 | 630.3 | 3228 | 6591 + 0 |
| 100000 × 768 time-ordered | top-10 | clustered merge + indexed | 0.997 | 2.032 | 2.849 | 20.8 | 615.3 | 3228 + 9 | 6591 + 2468 |
| 100000 × 768 time-ordered | room filter 1% | exact (f32 brute force) | 1.000 | 1.848 | 2.128 | – | – | 3072 | – |
| 100000 × 768 time-ordered | room filter 1% | scan | 0.990 | 26.503 | 81.052 | 756.9 | 173.0 | 3228 | 1831 + 0 |
| 100000 × 768 time-ordered | room filter 1% | indexed (IVF) | 0.990 | 29.708 | 86.301 | 677.0 | 135.0 | 3228 + 10 | 1831 + 2572 |
| 100000 × 768 time-ordered | room filter 1% | clustered merge + scan | 0.990 | 22.770 | 72.142 | 682.4 | 176.1 | 3228 | 6591 + 0 |
| 100000 × 768 time-ordered | room filter 1% | clustered merge + indexed | 0.989 | 25.538 | 114.464 | 708.1 | 256.3 | 3228 + 9 | 6591 + 2468 |
| 100000 × 768 time-ordered | room filter 50% | exact (f32 brute force) | 1.000 | 39.701 | 45.676 | – | – | 3072 | – |
| 100000 × 768 time-ordered | room filter 50% | scan | 1.000 | 97.999 | 119.832 | 1250.6 | 627.1 | 3228 | 1831 + 0 |
| 100000 × 768 time-ordered | room filter 50% | indexed (IVF) | 1.000 | 92.254 | 107.258 | 1233.1 | 627.1 | 3228 + 10 | 1831 + 2572 |
| 100000 × 768 time-ordered | room filter 50% | clustered merge + scan | 1.000 | 4.818 | 6.038 | 176.7 | 647.7 | 3228 | 6591 + 0 |
| 100000 × 768 time-ordered | room filter 50% | clustered merge + indexed | 1.000 | 2.738 | 3.660 | 26.1 | 633.2 | 3228 + 9 | 6591 + 2468 |
| 100000 × 768 time-ordered | range | exact (f32 brute force) | 1.000 | 64.787 | 76.548 | – | – | 3072 | – |
| 100000 × 768 time-ordered | range | scan | 1.000 | 96.058 | 122.679 | 1232.9 | 626.9 | 3228 | 1831 + 0 |
| 100000 × 768 time-ordered | range | indexed (IVF) | 1.000 | 88.530 | 104.026 | 983.9 | 626.8 | 3228 + 10 | 1831 + 2572 |
| 100000 × 768 time-ordered | range | clustered merge + scan | 1.000 | 30.516 | 57.947 | 263.0 | 13747.3 | 3228 | 6591 + 0 |
| 100000 × 768 time-ordered | range | clustered merge + indexed | 1.000 | 2.624 | 5.134 | 13.4 | 626.7 | 3228 + 9 | 6591 + 2468 |
| 100000 × 768 cluster-ordered | top-10 | exact (f32 brute force) | 1.000 | 64.741 | 71.910 | – | – | 3072 | – |
| 100000 × 768 cluster-ordered | top-10 | scan | 0.995 | 3.806 | 4.367 | 164.8 | 616.2 | 3228 | 2176 + 0 |
| 100000 × 768 cluster-ordered | top-10 | indexed (IVF) | 0.995 | 1.984 | 2.318 | 21.2 | 599.6 | 3228 + 9 | 2176 + 2510 |
| 100000 × 768 cluster-ordered | room filter 1% | exact (f32 brute force) | 1.000 | 1.869 | 2.144 | – | – | 3072 | – |
| 100000 × 768 cluster-ordered | room filter 1% | scan | 0.988 | 27.717 | 75.353 | 716.9 | 184.4 | 3228 | 2176 + 0 |
| 100000 × 768 cluster-ordered | room filter 1% | indexed (IVF) | 0.997 | 26.650 | 118.258 | 742.0 | 251.3 | 3228 + 9 | 2176 + 2510 |
| 100000 × 768 cluster-ordered | room filter 50% | exact (f32 brute force) | 1.000 | 39.931 | 45.207 | – | – | 3072 | – |
| 100000 × 768 cluster-ordered | room filter 50% | scan | 1.000 | 4.652 | 7.994 | 171.4 | 644.0 | 3228 | 2176 + 0 |
| 100000 × 768 cluster-ordered | room filter 50% | indexed (IVF) | 1.000 | 2.673 | 3.179 | 26.6 | 627.4 | 3228 + 9 | 2176 + 2510 |
| 100000 × 768 cluster-ordered | range | exact (f32 brute force) | 1.000 | 64.804 | 73.715 | – | – | 3072 | – |
| 100000 × 768 cluster-ordered | range | scan | 1.000 | 29.882 | 37.832 | 245.6 | 11356.9 | 3228 | 2176 + 0 |
| 100000 × 768 cluster-ordered | range | indexed (IVF) | 1.000 | 1.939 | 2.729 | 14.0 | 645.1 | 3228 + 9 | 2176 + 2510 |
| multivector 2000 docs / 16158 tokens × 128 | MaxSim top-10 | exact (f32 MaxSim) | 1.000 | 4.058 | 4.466 | – | – | 512 | – |
| multivector 2000 docs / 16158 tokens × 128 | MaxSim top-10 | scan | 1.000 | 9.200 | 13.596 | 264.0 | 16158.0 | 552 | 51 + 0 |
| multivector 2000 docs / 16158 tokens × 128 | MaxSim top-10 | indexed (IVF) | 1.000 | 10.807 | 14.475 | 264.0 | 17285.2 | 552 + 5 | 51 + 64 |
