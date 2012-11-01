snap
====

This is a snap shot tool useful for backups.

Usage: java net.soliddesign.snap.Snap from_dir snapshot_dir [label]

The default label is a time stamp.  The contents of from_dir will be copied to snapshot_dir/hash.  A directory will be made in snapshot_dir representing this snapshot and a link will be made from there to the snapshot_dir/hash.

FIXME: linked lists are not supported