# Publication benchmarks

Run from the checkout root with the pinned SDK:

```sh
bazel run @rules_go//go -- test ./internal/publication -run '^$' -bench '^BenchmarkPublication' -benchmem -benchtime=200ms -count=3
```

These are medians of three local samples. Fixtures use synthetic digests and
valid public peer IDs, without blob or network I/O. The full snapshot contains
8,192 completed records with two attempts and two receipts each. Delta tests
change one job's readiness among 8,191 completed records. Scheduler selection
uses 1,024 eligible jobs across 256 buckets and 16 peer candidates.

| Operation | Time | Allocated bytes | Encoded/written bytes |
| --- | ---: | ---: | ---: |
| Snapshot marshal | 38.2 ms | 46.9 MiB | 5,382,776 |
| Initial durable save | 124 ms | 125.4 MiB | 5,382,862 |
| One-job durable delta | 1.80 ms | 11.0 KiB | 1,178 |
| Save including amortized compaction | 1.81 ms | 73.4 KiB | 6,434 |
| Recover checkpoint | 154 ms | 151.2 MiB | 5,382,672 read |
| Recover 1,024 frames | 275 ms | 186.0 MiB | 6,588,278 read |
| Recover near 32 MiB limit | 634 ms | 650.6 MiB | 33,551,222 read |
| Select next job | 339 us | 509 KiB | none |

Save timings include `fsync`; fixture setup is excluded. Each amortized benchmark
operation performs 1,024 saves and one compaction; its custom metrics and the
table normalize per save. Standard `ns/op`, `B/op`, and `allocs/op` for that
benchmark describe the whole cycle. The cycle peaks at a 6.28 MiB journal.
Recovery includes file reads, validation, replay, locking, and rebuilding the
comparison cache, with a warm filesystem cache. Allocated bytes are cumulative
allocation, not peak resident memory; recovery remains an allocation hotspot.

For this fixture, 4,096 saves would write about 25.1 MiB with compaction, versus
20.5 GiB if every save rewrote the full snapshot. Actual offer accounting and
observation batches change delta sizes. These are encoded byte counts, not
physical device writes or a network throughput prediction. Benchmarks enforce
the 1,024-frame and 32 MiB journal limits; compaction temporarily also holds a
checkpoint file, whose payload is bounded to 16 MiB.
