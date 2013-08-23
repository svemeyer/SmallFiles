#!/bin/sh

if [ $# -ne 2 ]
then
  echo "Usage: check-archives.sh <dir> [acount|fcount|dups]"
  echo ""
  echo "Please note: This will search for files called 'DARC*' in all subdirectories,"
  echo "therefore it is advisable to run this command on the root of your archives directory."
fi

DIR="$1"
CMD="$2"

case $CMD in
  "acount" )
    find "$DIR" -name "DARC*"|wc -l
    ;;

  "fcount" )
    for f in `find "$DIR" -name "DARC*"`
    do
      tar tf "$f"
    done|grep -e '[ABCDEF0123456789]\{36\}'|wc -l
    ;;

  "dups" )
    for f in `find "$DIR" -name "DARC*"`
    do
      tar tf "$f"
    done|grep -o -e '[ABCDEF0123456789]\{36\}'|sort|uniq -c
    ;;

  *) 
    echo "Invalid command: $CMD"
    exit 3
    ;;
esac
