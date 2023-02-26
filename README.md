# Craft

todo:
- snapshots
- membership changes
- cluster splitting
- rocksdb backend
- smart appendentries intervals for commands (immediate, interval-based batching...)
- nemesis development
- querying (dirty [leader, follower], linearizable command-based read)
  - may not need log entry for linearizable read... (leader receives read request, notes at what log index it should take place, when that log index is committed, responds to read as of that index)
  - leases like cockroachdb?

3.10 leadership transfer extension
4.2.1 server catch up "rounds" hueristic
