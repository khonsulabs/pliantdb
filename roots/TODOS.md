# Todos

This is a temporary place to track TODOs for this project, until it's at the stage that it can be integrated into BonsaiDb.

- [x] Reverse scan (last)
- [ ] Consider switching key-scanning to read buffers as they are encountered. When trying to write a scan to return the last 5 records, it'd be nice to be able to use "if my_buffer.len() < 5 {..}" but due to the buffer reading callback happening later, you can't do that. Thus, you have to keep track of the count in the key retrieval API. Another option would be to add optional limits to the APIs.
- Unit tests
  - [x] remove
  - [ ] CompareAndSwap
  - [ ] compare_and_swap
- [ ] Benchmark multi-id and ranged queries
- [ ] Compaction
- [ ] Benchmark reads after compaction
- [ ] Ability to sequentially scan and reverse-scan transaction log.
- [ ] Implement B-Tree rebalancing. When a node has less than order / 2 nodes, it should be absorbed by the parent.
- [ ] File versioning
- [ ] Consider a WAL

      In testing SQLite, they have a WAL which is used to batch changes into thir database. It slows down queries, but it allows inserts to be incredibly fast. Originally, I threw out this idea because it seemed like it might require too much memory usage.

      But, I now realize we can contain the memory usage. In the chunk cacher, we can keep a separate list of cached chunks that are required to stay in memory until they're committed from the WAL. We can force this list to be a fixed size to keep memory usage down -- thus forcing a wait for the WAL to commit. Once the WAL has been committed, those chunks can be moved into the chunk cacher.

      Why could this be faster? The WAL can contain just the bare minimum to re-execute the modification. This means less writes need to be flushed to disk to say that the transaction is completed. For flows that are doing small transactions, we could do fake transactions in memory that create temporary nodes that only live in the chunk cache. Then, when the WAL is committed, we do one large write to the database with all the changes. The only reason this might win is by minimizing the total number of node rewrites by chunking the btree operations into one batch.
- [x] Refactor to const generics for B-Tree order
- [x] Tree get for Interior
- [x] Tree insert for Interior
- [x] Root split handling
- [x] Chunk caching
- [x] Consider supporting dynamic order
- [x] Ability to insert multiple records in one write.
- [x] Switch benchmarks to Criterion
- [x] Ability to scan ranges of keys
- [x] Ability to find an entry in the transaction log, using a binary search
- [x] High level API + multi-tree transactions
- [x] Implement simultaneous read/write
