#!/bin/bash
#
# Usage: cloud.sh local_share_dir remote_share_dirs
#
#  This command will repeatedly call rsync to keep the local directories insync with remote directories.  At startup, it attempts to manually recover since the last sync.
#

if [ "--copy" = "$1" ]
then
  FILE=$2
  BASE=$3
  shift 3
  for remote in "$@"
  do
    rsync --delete -avP "${BASE}/${FILE}" "${remote}/${FILE}"
  done
  exit 0
fi

BASE="$1"
shift 1
export REMOTES="$@"

# initial sync
for remote in "$@"
do
  echo updating ${remote}
  rsync --delete -a "${BASE}/" "${remote}/"
done

(cd "$BASE";inotifywait -rm "./" -e create -e modify -e delete -e move --format '%w%f')|
xargs -I FILE /bin/bash $0 --copy FILE "${BASE}" "$@"
