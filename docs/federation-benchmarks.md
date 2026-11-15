# Publication benchmarks

Run from the checkout root with the pinned SDK:

```sh
bazel run @rules_go//go -- test ./internal/publication -run '^$' -bench '^BenchmarkPublication' -benchmem -benchtime=200ms -count=3
```

These are medians of three local samples, comparing JSON at `b9f31fe` with
binary protobuf using the same benchmark fixtures. Fixtures use synthetic
digests and valid public peer IDs, without blob or network I/O. The full
snapshot contains 8,192 completed records with two attempts and two receipts
each; delta tests change one pending job among 8,191 completed records.
To reproduce the JSON comparison, copy the current
`internal/publication/benchmark_test.go` into a checkout of that revision.

| Operation | Time: JSON → protobuf | Allocated: JSON → protobuf | Bytes: JSON → protobuf |
| --- | ---: | ---: | ---: |
| Snapshot marshal | 34.2 → 8.42 ms | 46.9 → 9.72 MiB | 5,382,776 → 2,056,212 |
| Initial durable save | 103 → 24.7 ms | 125.4 → 14.0 MiB | 5,382,862 → 2,056,266 |
| One-job durable delta | 1.78 → 1.49 ms | 10.6 → 3.08 KiB | 1,179 → 161 |
| Save including amortized compaction | 1.81 → 1.42 ms | 73.4 → 15.5 KiB | 6,434 → 2,169 |
| Recover checkpoint | 152 → 11.3 ms | 151.2 → 18.6 MiB | 5,382,672 → 2,056,113 |
| Recover 1,024 frames, one changed job | 276 → 84.8 ms | 186.0 → 22.6 MiB | 6,588,278 → 2,220,815 |
| Recover 512 frames, 64 changed jobs | 425 → 72.1 ms | 430.9 → 49.8 MiB | 21,023,175 → 5,251,913 |

The last row replays identical histories: one checkpoint and 511 deltas, each
changing 64 pending jobs. Protobuf recovery is about 5.9 times faster with 88%
less cumulative allocation for this workload. Bytes mean encoded snapshot,
written journal, or recovered journal bytes, according to the operation.

The separate capacity fixture changes 512 pending jobs per delta and must fill
at least 95% of the 32 MiB limit before reaching 1,024 frames:

| Encoding | Frames | Journal bytes | Recovery | Allocated |
| --- | ---: | ---: | ---: | ---: |
| JSON | 118 | 33,374,145 | 554 ms | 649.5 MiB |
| Protobuf | 635 | 33,506,750 | 264 ms | 320.6 MiB |

Protobuf holds about 5.4 times as much history in this capacity case; these two
rows do not replay equal work. An adjacent control run of the unchanged
1,024-job/16-peer scheduler measured 280 us for both encodings, with about
510 KiB allocated per selection (`-benchtime=500ms -count=3`).

Save timings include `fsync`; fixture setup is excluded. Each amortized
operation performs 1,024 saves and one compaction; the table and custom metrics
normalize per save, while standard `ns/op`, `B/op`, and `allocs/op` describe the
whole cycle. Its journal peaks at 6.28 MiB for JSON and 2.12 MiB for protobuf.
Recovery includes file reads, validation, replay, locking, and rebuilding the
comparison cache, with a warm filesystem cache. Timing varies with filesystem
and system load; byte counts exclude physical device write amplification.

Allocated bytes are cumulative allocation, not peak live heap or resident memory.
A separate `alloc_space` profile focused on protobuf recovery attributed about
53% to generated decoding, 36% to restoring domain state, and 10% to the single
file-read buffer; fixture construction was excluded from that attribution.
Benchmarks enforce the frame and journal limits. Compaction temporarily also
holds a checkpoint file, whose payload is bounded to 16 MiB.
