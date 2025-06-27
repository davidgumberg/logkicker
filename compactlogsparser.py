#!/usr/bin/env python3

from collections import defaultdict
import pprint
from dataclasses import dataclass
from enum import Enum
import re
from typing import Optional, Tuple
import logkicker

# Successfully reconstructed block 000000000000000000000fec9bd60e4700c173a61195b46527bda8861f6b1276 with 1 txn prefilled, 4105 txn from mempool (incl at least 0 from extra pool) and 0 txn (0 bytes) requested
CB_RECONSTRUCTION_PATTERN = re.compile(r'Successfully reconstructed block ([0-9a-f]+) with (\d+) txn prefilled, (\d+) txn from mempool \(incl at least (\d+) from extra pool\) and (\d+) txn \((\d+) bytes\) requested')

# Initialized PartiallyDownloadedBlock for block 00000000000000000002165564043bef508ec2a8ddf81e15916114cbb5ce632b using a cmpctblock of 14691 bytes
CB_RECEIVE_PATTERN = re.compile(r'Initialized PartiallyDownloadedBlock for block ([0-9a-f]+) using a cmpctblock of (\d+) bytes')

# sending cmpctblock (25101 bytes) peer=1
CB_SEND_PATTERN = re.compile(r'sending cmpctblock \((\d+) bytes\) peer=(\d+)')

# PeerManager::NewPoWValidBlock sending header-and-ids 00000000000000000002165564043bef508ec2a8ddf81e15916114cbb5ce632b to peer=11
CB_TO_ANNOUNCE_PATTERN = re.compile(r'PeerManager::NewPoWValidBlock sending header-and-ids ([0-9a-f]+) to peer=(\d+)')

# received getdata for: cmpctblock 0000000000000000000085ae6fe4bb42bb2395c4fce575eac8f8dcaa8bea0750 peer=3
CB_REQUESTED_PATTERN = re.compile(r'received getdata for: cmpctblock ([0-9a-f]+) peer=(\d+)')

#     - Max send per-rtt: 14480 bytes
NET_MAX_SEND_PATTERN = re.compile(r'\s*- Max send per-rtt: (\d+) bytes') # We're gonna strip it

class ReasonsToCare(Enum):
    WE_DONT = 0
    CB_SEND = 1
    CB_RECEIVE = 2
    CB_RECONSTRUCTION = 3
    CB_TO_ANNOUNCE = 4
    CB_REQUESTED = 5
    NET_MAX_SEND = 6

def we_care(entry:logkicker.LogEntry) -> Tuple[ReasonsToCare, Optional[re.Match[str]]]:
    if entry.metadata.category == "net":
        if match := CB_SEND_PATTERN.match(entry.body):
            return ReasonsToCare.CB_SEND, match
        elif match := CB_TO_ANNOUNCE_PATTERN.match(entry.body):
            return ReasonsToCare.CB_TO_ANNOUNCE, match
        elif match := CB_REQUESTED_PATTERN.match(entry.body):
            return ReasonsToCare.CB_REQUESTED, match
        elif match := NET_MAX_SEND_PATTERN.match(entry.body):
            return ReasonsToCare.NET_MAX_SEND, match
    elif entry.metadata.category == "cmpctblock":
        if match := CB_RECEIVE_PATTERN.match(entry.body):
            return ReasonsToCare.CB_RECEIVE, match
        elif match := CB_RECONSTRUCTION_PATTERN.match(entry.body):
            return ReasonsToCare.CB_RECONSTRUCTION, match
    return ReasonsToCare.WE_DONT, None

@dataclass
class BlockReceived:
    # txn_count: int = 0
    received_cb_bytes: int = 0
    received_cb_missing_bytes: int = 0
    received_cb_missing_tx_count: int = 0

@dataclass
class BlockSent:
    block_received: BlockReceived
    peer_id: int
    cb_bytes_sent: int = 0
    tcp_window_bytes: int = 0
    
def main(filepath):
    blocks_received: dict[str, BlockReceived] = {}
    blocks_sent: dict[str, list[BlockSent]] = defaultdict(list)
    pending_block_send: Optional[BlockSent] = None
    pending_max_send: Optional[BlockSent] = None
    
    for entry in logkicker.process_log_generator(filepath):
        why, match = we_care(entry)
        if why == ReasonsToCare.WE_DONT or match == None:
            continue
        match why:
            case ReasonsToCare.CB_RECEIVE:
                blocks_received[match.group(1)] = BlockReceived(received_cb_bytes = int(match.group(2)))
            case ReasonsToCare.CB_RECONSTRUCTION:
                block = blocks_received[match.group(1)]
                block.received_cb_missing_tx_count = int(match.group(5))
                block.received_cb_missing_bytes = int(match.group(6))
            case ReasonsToCare.CB_TO_ANNOUNCE | ReasonsToCare.CB_REQUESTED: # lucky, they have the same pattern!
                blockhash = match.group(1)
                pending_block_send = BlockSent(block_received = blocks_received[blockhash], peer_id = int(match.group(2)))
                blocks_sent[blockhash].append(pending_block_send)
            case ReasonsToCare.CB_SEND:
                if not pending_block_send:
                    continue
                assert pending_block_send.peer_id == int(match.group(2))
                pending_block_send.cb_bytes_sent = int(match.group(1))
                pending_max_send = pending_block_send
                pending_block_send = None
            case ReasonsToCare.NET_MAX_SEND:
                if not pending_max_send:
                    continue
                pending_max_send.tcp_window_bytes = int(match.group(1))
                pending_max_send = None

    failed_blocks = [block for block in blocks_received.values() if block.received_cb_missing_tx_count > 0]

    reco_fail_count = len(failed_blocks)
    total_blocks_received = len(blocks_received)
    print(f"{len(failed_blocks)} out of {len(blocks_received)} blocks failed reconstruction.") 
    reco_rate = (total_blocks_received - reco_fail_count) / total_blocks_received * 100
    print(f"Reconstruction rate was {reco_rate}%")
        
    # Pretty print the blocks_sent data
    print("Blocks Sent Summary:")
    print("=" * 50)
    for block_hash, sent_list in blocks_sent.items():
        print(f"\nBlock: {block_hash}")
        print(f"Sent to {len(sent_list)} peer(s):")
        for i, block_sent in enumerate(sent_list, 1):
            print(f"  Peer {i}:")
            pprint.pprint(block_sent, indent=4, width=100)


if __name__ == "__main__":
    import sys
    if len(sys.argv) != 2:
        print("Usage: python script.py <filepath>")
        sys.exit(1)
    
    filepath = sys.argv[1]
    entries = main(filepath)
