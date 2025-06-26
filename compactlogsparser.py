#!/usr/bin/env python3

from dataclasses import dataclass
from enum import Enum
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

new_block_pattern = re.compile(r'PeerManager::NewPoWValidBlock sending header-and-ids ([0-9a-f]+) to peer=(\d+)')

def is_new_block_pattern(entry:logkicker.LogEntry) -> bool:
    if entry.metadata.category == "net" and new_block_pattern.match(entry.body):
        return True
    else:
        return False

class ReasonsToCare(Enum):
    WE_DONT = 0
    CB_SEND = 1
    CB_RECONSTRUCTION = 2
    NEW_BLOCK = 3

def we_care(entry:logkicker.LogEntry) -> ReasonsToCare:
    if is_cb_send(entry):
        return ReasonsToCare.CB_SEND
    elif is_cb_reconstruction(entry):
        return ReasonsToCare.CB_RECONSTRUCTION
    elif is_new_block_pattern(entry):
        return ReasonsToCare.NEW_BLOCK
    else:
        return ReasonsToCare.WE_DONT

@dataclass
class BlockReceived:
    txn_count: int
    received_cb_bytes: int
    received_cb_missing_bytes: int

@dataclass
class BlockSent:
    peer_id: int
    block_received: BlockReceived
    cmpctblock_bytes_sent: int
    
def main(filepath):
    looking_for_max_send = False
    
    for entry in logkicker.process_log_generator(filepath):
        match we_care(entry):
            case ReasonsToCare.CB_SEND:
                NotImplemented
            case ReasonsToCare.CB_RECONSTRUCTION:
                NotImplemented
            case ReasonsToCare.WE_DONT:
                continue

if __name__ == "__main__":
    import sys
    if len(sys.argv) != 2:
        print("Usage: python script.py <filepath>")
        sys.exit(1)
    
    filepath = sys.argv[1]
    entries = main(filepath)
