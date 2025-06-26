#!/usr/bin/env python3

import re
import logkicker

def is_compact_block_entry(entry:logkicker.LogEntry) -> bool:
    return entry.metadata.category == "cmpctblock"

# Successfully reconstructed block 000000000000000000000fec9bd60e4700c173a61195b46527bda8861f6b1276 with 1 txn prefilled, 4105 txn from mempool (incl at least 0 from extra pool) and 0 txn (0 bytes) requested
cb_reconstruction_pattern = re.compile(r'Successfully reconstructed block .* and (\d+) txn \((\d+) bytes\) requested')

def is_cb_reconstruction(entry:logkicker.LogEntry) -> bool:
    if entry.metadata.category == "cmpctblock" and cb_reconstruction_pattern.match(entry.body):
        return True
    else: return False

# sending cmpctblock (25101 bytes) peer=1
cb_send_pattern = re.compile(r'sending cmpctblock \((\d+) bytes\) peer=(\d+)')

def is_cb_send(entry:logkicker.LogEntry) -> bool:
    if entry.metadata.category == "net" and cb_send_pattern.match(entry.body):
        return True
    else:
        return False

def if_we_care(entry:logkicker.LogEntry) -> bool:
    if is_cb_send(entry) or is_cb_reconstruction(entry):
new_block_pattern = re.compile(r'PeerManager::NewPoWValidBlock sending header-and-ids ([0-9a-f]+) to peer=(\d+)')

def is_new_block_pattern(entry:logkicker.LogEntry) -> bool:
    if entry.metadata.category == "net" and new_block_pattern.match(entry.body):
        return True
    else:
        return False

def main(filepath):
    looking_for_max_send = True
    for entry in logkicker.process_log_generator(filepath, if_we_care):
        print(entry.body)

if __name__ == "__main__":
    import sys
    if len(sys.argv) != 2:
        print("Usage: python script.py <filepath>")
        sys.exit(1)
    
    filepath = sys.argv[1]
    entries = main(filepath)
