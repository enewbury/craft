# Craft

todo:
- cluster splitting
- smart appendentries intervals for commands (immediate, interval-based batching...)
- leader leases for linearizable reads
- broadcast replication lag to clients, to allow them to target non-linearizable reads to specific followers
- fix Craft top-level API, designate group by {name, nodes} rather than separate args
- be consistent with "members" vs "nodes" nomenclature
- snapshot deletion when no longer needed

4.2.1 server catch up "rounds" hueristic

done:
- nemesis development
- linearizability checker
- 3.10 leadership transfer extension
- PreVote
- CheckQuorum
- rocksdb backend

- snapshots
  when commit index bumps, consensus sends a message to machine to bump index and optionally snapshot
  if snapshotting, machine snapshots and sends message to consensus with snapshot index and path on disk
  consensus writes snapshot metadata to persistence
  break out to own SnapshotsManager process?
  
- log truncation
  runs periodically
  asks persistence for all snapshots, finds the most recent non-busy (not sending to follower) one, and truncates log to that point

  for consideration: if a follower is slow in catching up, and the log is snapshotted/truncated on the leader, how should the follower respond?
    nuke the follower's current state and start over fresh?

- querying (eventual, linearizable)
  - linearizable reads without log entry
