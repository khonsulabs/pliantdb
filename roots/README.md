# bonsaidb-roots

This crate provides the `Roots` type, which is the transactional storage layer
for [`BonsaiDb`](https://dev.bonsaidb.io/). It is loosely inspired by
[`Couchstore`](https://github.com/couchbase/couchstore). 

## Features of `Roots`

The main highlights are:

### Append-only file format

The files stored are written in such a way that makes it easy to confirm what
data is valid and what data wasn't fully written to disk before a crash or power
outage occurred. This high concurrency during read operations, as they never are
blocked waiting for a writer to finish.

The major downside of append-only formats is that deleted data isn't cleaned up
until a maintenance process occurs: compaction. This process rewrites the files
contents, skipping over entries that are no longer alive. This process can
happen without blocking the file from being operated on, but it does
introduce IO overhead during the operation.

Roots will provide the APIs necessary to perform compaction, but due to the IO
overhead introduced by the operation, the implementation strategy is left to
`BonsaiDb` to allow for flexible customization and scheduling.

### Multi-tree ACID-compliant Storage

A transaction log is maintained such that if any failures occur, the database
can be recovered in a consistent state. All writers that were told their
transactions succeeded will still find their data after an unexpected power
event or crash.

### Pluggable encryption support

This crate defines a `Vault` trait that allows virtually any encryption backend
without `Roots` needing to know any details about it. `Roots` stores an
"encryption key ID" u32 that the `Vault` provides during encryption. When
decrypting, the stored ID is provided to the vault along with the encrypted
payload.

