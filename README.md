snap
====

This is a snap shot tool useful for backups.

Usage: snap from_dir snapshot_dir label_dir

The contents of from_dir will be copied to snapshot_dir/hash.  A directory label_dir will be made in snapshot_dir representing this snapshot and a links will be made from there to the snapshot_dir/hash.

[The Java version was the prototype.  I'm  unhappy at how difficult it is to do systems programming in Java, so I switched to C.  Some things were much easier, like SHA1(mmap(NULL, fd)), but some bugs crept up, like mutable strings and missing null terminators.]

The following are features that I need.

I don't need the following for my personal usage, but I'm sure someone will, so I'm listing it like I care.

TODO:
* better error handling (currently I only handle errors that I seen, not errors that are possible)
* documentation
* support other file types
** symbolic links
** devices
** permissions
** extended attributes