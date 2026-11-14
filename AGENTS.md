Keep design notes concise. Every commit must build and pass tests.
Before pushing each commit, request reviews from the persistent Go style,
performance/benchmarking, distributed systems, and security reviewer sessions.
Resolve blocking findings before publishing. Preserve v1 wire compatibility;
changes to chunking parameters require a new protocol version.

Never commit machine-specific absolute paths, identifying host details, secrets,
or private keys. Use paths relative to the checkout root in committed files.
Review staged content for these before every commit.
