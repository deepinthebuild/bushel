Keep design notes concise. Every commit must build and pass tests.
Before pushing each commit, request reviews from the persistent Go style,
performance/benchmarking, distributed systems, and security reviewer sessions.
Resolve blocking findings before publishing. Preserve v1 wire compatibility;
changes to chunking parameters require a new protocol version.
