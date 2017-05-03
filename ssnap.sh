#!/bin/sh

RSYNC=rsync
AWK=awk
COUNT=18

(echo;echo "** Maintaining $COUNT versions of backup."; echo)1>&2

# copy all but final arg (dest directory)
while [ $# -gt 1 ]
do
    ARGS="$ARGS $1"
    shift
done
cd "$1"

# hard link and partial
ARGS="$ARGS -ylH --partial"

# linkdest
for f in `echo "????-??-??-*"`
do
  ARGS="$ARGS --link-dest=../$f/"
done

# append dest directory with date
DIR=`date "+%Y-%m-%d-%H-%M-%S"`
ARGS="$ARGS $DIR/"

# perform copy
echo $RSYNC $ARGS 1>&2
$RSYNC $ARGS
ln -Tfs $DIR latest

# clean up
MOD=$(echo "$(date +%j) % $COUNT" | bc)
while [ `/bin/ls | wc -l` -gt $COUNT ]
do
    ls |
    $AWK 'NR=='$MOD' {print "(echo;echo \"** Deleting "$0".\"; echo) 1>&2; chmod -R u+xwr "$0"; rm -rf "$0}END{print "exit"}' |
    bash -x
done
