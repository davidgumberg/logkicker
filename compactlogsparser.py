#!/usr/bin/env python3

import logkicker

def is_compact_block_entry(entry:logkicker.LogEntry) -> bool:
    return entry.metadata.category == "cmpctblock"

def main(filepath):
    log_entries = logkicker.process_log(filepath)
    compact_entries = filter(is_compact_block_entry, log_entries)
    for entry in compact_entries:
        NotImplemented


if __name__ == "__main__":
    import sys
    if len(sys.argv) != 2:
        print("Usage: python script.py <filepath>")
        sys.exit(1)
    
    filepath = sys.argv[1]
    entries = main(filepath)

