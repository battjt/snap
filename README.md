snap
====

This is a snap shot tool useful for backups.

Usage: java net.soliddesign.snap.Snap from_dir ssh_id snapshot_dir [label]

The default label is a time stamp.  The contents of from_dir will be copied to snapshot_dir/hash on an ssh target of ssh_id.  A directory will be made in snapshot_dir representing this snapshot and a link will be made from there to the snapshot_dir/hash.

Ssh support is not yet tested.  This design is still in flux.  It depends on a command line ssh client on the local machine named 'ssh'.

TODO
----
<ol>
<li> test
<li> parameterize 'ssh'
<li> evaluate parallelizing some of the I/O.  A big file copy stops everything else right now.  Wouldn't it be better to got the little files copied while the big file is transfering?
<li> work support for local snaps back into the app.
</ol>