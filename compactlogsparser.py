#!/usr/bin/env python3

from dataclasses import dataclass
from enum import Enum
import re
from typing import Optional, Tuple
import logkicker

# Successfully reconstructed block 000000000000000000000fec9bd60e4700c173a61195b46527bda8861f6b1276 with 1 txn prefilled, 4105 txn from mempool (incl at least 0 from extra pool) and 0 txn (0 bytes) requested
CB_RECONSTRUCTION_PATTERN = re.compile(r'Successfully reconstructed block .* and (\d+) txn \((\d+) bytes\) requested')

# Initialized PartiallyDownloadedBlock for block 00000000000000000002165564043bef508ec2a8ddf81e15916114cbb5ce632b using a cmpctblock of 14691 bytes
CB_RECEIVE_PATTERN = re.compile(r'Initialized PartiallyDownloadedBlock for block ([0-9a-f]+) using a cmpctblock of (\d+) bytes')

# sending cmpctblock (25101 bytes) peer=1
CB_SEND_PATTERN = re.compile(r'sending cmpctblock \((\d+) bytes\) peer=(\d+)')


NEW_BLOCK_PATTERN = re.compile(r'PeerManager::NewPoWValidBlock sending header-and-ids ([0-9a-f]+) to peer=(\d+)')

class ReasonsToCare(Enum):
    WE_DONT = 0
    CB_SEND = 1
    CB_RECEIVE = 2
    CB_RECONSTRUCTION = 3
    NEW_BLOCK = 4

def we_care(entry:logkicker.LogEntry) -> Tuple[ReasonsToCare, Optional[re.Match[str]]]:
    if entry.metadata.category == "net":
        if match := CB_RECEIVE_PATTERN.match(entry.body):
            return ReasonsToCare.CB_RECEIVE, match
        elif match := CB_SEND_PATTERN.match(entry.body):
            return ReasonsToCare.CB_SEND, match
        elif match := CB_RECONSTRUCTION_PATTERN.match(entry.body):
            return ReasonsToCare.CB_RECONSTRUCTION, match
        elif match := NEW_BLOCK_PATTERN.match(entry.body):
            return ReasonsToCare.NEW_BLOCK, match
    elif entry.metadata.category == "cmpctblock":
        NotImplemented
    return ReasonsToCare.WE_DONT, None

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
        why, match = we_care(entry)
        match why:
            case ReasonsToCare.CB_RECEIVE:
                NotImplemented
            case ReasonsToCare.CB_SEND:
                NotImplemented
            case ReasonsToCare.CB_RECONSTRUCTION:
                NotImplemented
            case ReasonsToCare.NEW_BLOCK:
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
