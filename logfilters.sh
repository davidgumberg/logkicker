#!/bin/bash

# Todo: Implement this in logkicker, remember that string comparison is
# probably cheaper than parsing and comparing the datetimes!
if [ $# -ne 3 ]; then
  echo "Usage: $0 <logfile> <start_time> <end_time>"
  echo "{YYYY}-{MM}-{DD}T{HH}:{mm}:{ss}.{ssssss}Z"
  echo "Example: $0 logfile.txt '2025-07-11T17:07:36.000000Z' '2025-07-11T17:07:37.000000Z'" >&2
  exit 1
fi

# ISO8601 timestamps are lexically sortable.
awk -v start="$2" -v end="$3" '$1 >= start && $1 <= end' "$1"
