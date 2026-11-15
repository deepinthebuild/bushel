# Federated publication

Local REAPI writes complete independently of optional background publication.
Enable a producer with `-federation-publish=external` and willing receivers with
`-federation-accept`. Both default to off. Publication aims for two distinct
remote peer completions in addition to the producer's local copy. Declines,
partitions, eviction, and too few willing peers can leave work under-replicated.

The producer observes standard REAPI RequestMetadata on uploads and
FindMissingBlobs. Canonical external labels (`@@repo//pkg:target`) qualify under
`external`; main (`//pkg:target`, `@//...`, `@@//...`), apparent (`@repo//...`),
and unknown metadata are separate classes. Use comma-separated class names
`external,main,apparent,unknown`, or `all`, to include more classes.

Partitions hash producer identity, instance, invocation, and complete repository
spelling. Labels associate bytes with an action, including its inputs; they do
not establish the repository that produced those bytes. A bounded action cache
can recover an absent label within the same invocation. Conflicting labels
disable inference. Missing or malformed metadata never fails a valid CAS upload.
Labels, paths, and invocation IDs stay local.

FindMissing observations survive metadata-free uploads. A bounded, process-local
one-hour cache of 4,096 explicit positive observations also joins callbacks received
in the opposite order. Missing observations wait for an explicit local completion;
positive observations are verified again asynchronously before any offer.
Multiple workspace associations share one job
per hash/size pair. Byte-aware scheduling uses 256 workspace buckets; collisions
share scheduling credit. Jobs waiting an hour get oldest-eligible priority;
each attempt resets that waiting period so retries also yield turns. Reads from
peers populate the verified local CAS but never create publication jobs.
Generic catalog announcements and restart scans
do not create jobs either.

## Bounds and restart behavior

The producer retains at most 1,024 pending jobs, 20 GiB of declared pending blob
sizes, and 8,192 total records including completed/exhausted jobs. Records expire
24 hours after first observation; duplicates do not extend that deadline or
reset receipts and attempts. Full queues drop hints without failing local writes.
One offer runs at a time, at most once per second, with eight attempts per blob,
at most two per peer, and at most 2,048 offers per budget window. Successful first
copies immediately make the second copy eligible; failures use bounded backoff.

`-federation-send-bytes-per-hour` defaults to 1 GiB in fixed one-hour windows.
The full declared size is charged before each attempt, including failed or
declined attempts. These charges, jobs, and receipts are saved in a checksummed
journal under `data/publication` before dispatch. Appends contain changed records;
the journal compacts at 1,024 frames or 32 MiB. Interrupted attempts
remain charged after restart. Hints can be lost if the process exits before the
background worker saves them. State-write failures stop further dispatch and are
reported; corrupt complete records are rejected rather than silently resetting
allowances. An incomplete final append is discarded on recovery; it could not
have authorized a transfer because dispatch follows the completed durable save.

Receivers admit at most two concurrent blobs, eight control handlers, and a
16-control burst followed by one control per second. Their process-local rolling
hour budget defaults to 1 GiB (`-federation-receive-bytes-per-hour`), and the
largest accepted blob defaults to 256 MiB (`-federation-max-blob-bytes`). Failed
pulls retain their charge. Buildbarn storage and Bushel staging limits still
apply. A verified existing local blob can complete without another transfer.

The daemon reports pending bytes/age, unconfirmed attempts, verified peers,
completion, exhaustion, drops, and charged bytes once per minute when publication
is enabled. Completion is a peer's report of
verified admission at that moment; it is not a retention lease.

The [seed wire format](protocol-v1.md#optional-seed-offers) uses an authenticated
offer stream and Bitswap pulls restricted to the offering peer. Receivers verify
every chunk, the full digest, and canonical chunk boundaries before reporting a copy.
See [capacity benchmarks](federation-benchmarks.md) for persistence and scheduling costs.
