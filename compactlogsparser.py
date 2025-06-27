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
    cb_size_bytes: int = 0
    cb_missing_bytes: int = 0
    cb_missing_tx_count: int = 0
    # this stat is only set for a node that does prefills
    cb_bytes_from_extra_pool: Optional[int] = None

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
                blocks_received[match.group(1)] = BlockReceived(cb_size_bytes = int(match.group(2)))
            case ReasonsToCare.CB_RECONSTRUCTION:
                block = blocks_received[match.group(1)]
                block.cb_missing_tx_count = int(match.group(5))
                block.cb_missing_bytes = int(match.group(6))
            case ReasonsToCare.CB_TO_ANNOUNCE | ReasonsToCare.CB_REQUESTED: # lucky, they have the same pattern!
                blockhash = match.group(1)
                # On rare occassions we receive full-sized blocks, so we don't know their cb size.
                if blockhash not in blocks_received:
                    continue
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

    failed_blocks = [block for block in blocks_received.values() if block.cb_missing_tx_count > 0]

    reco_fail_count = len(failed_blocks)
    total_blocks_received = len(blocks_received)
    print(f"{len(failed_blocks)} out of {len(blocks_received)} blocks failed reconstruction.") 
    reco_rate = (total_blocks_received - reco_fail_count) / total_blocks_received * 100
    print(f"Reconstruction rate was {reco_rate}%")
        
    total_cb_sent = total_cb_in_need = total_prefill_bytes = total_available_bytes_in_window = 0
    prefills_that_fit = 0
    # blocks that exceeded the first rtt window without any prefills
    exceeded_without_prefill = 0
    
    # Pretty print the blocks_sent data
    for block_hash, sent_list in blocks_sent.items():
        if block_hash not in blocks_received:
            continue
        received = blocks_received[block_hash]
        # the size of the CMPCTBLOCK message we received.
        received_size = received.cb_size_bytes

        for sent in sent_list:
            # running total of CMPCTBLOCK messages sent
            total_cb_sent += 1
            # running total of tcp windows for stats
            total_available_bytes_in_window += sent.tcp_window_bytes

            prefill_size = sent.cb_bytes_sent - received_size
            total_prefill_bytes += prefill_size

            # The number of rtt's it would have taken to announce the
            # CMPCTBLOCK with no additional prefilling
            rtts_needed_with_no_prefill:int = received_size // sent.tcp_window_bytes
            if (rtts_needed_with_no_prefill > 1):
                exceeded_without_prefill += 1
            # Bytes we've used in this window
            bytes_used_in_tcp_window = received_size % sent.tcp_window_bytes
            # The number of bytes of overhead we have up to the next tcp window boundary
            bytes_left_in_tcp_window:int = sent.tcp_window_bytes - bytes_used_in_tcp_window

            if prefill_size > 0:
                if prefill_size <= bytes_left_in_tcp_window:
                    # we got it for free 😎
                    prefills_that_fit += 1
                # this only works for the prefilling node. extra pool recovery size
                # is not logged, but we can recover this info from the difference
                # between sent size, todo: add logging to the branch
                # track the total number of cb's that needed prefills
                total_cb_in_need += 1
                received.cb_bytes_from_extra_pool = sent.cb_bytes_sent - received_size - received.cb_missing_bytes


    avg_prefill_bytes = total_prefill_bytes / total_cb_in_need
    avg_available_bytes = total_available_bytes_in_window / total_cb_sent

    prefill_needed_rate = total_cb_in_need / total_cb_sent
    prefill_fit_rate = prefills_that_fit / total_cb_in_need
    already_over_rtt_rate = exceeded_without_prefill / total_cb_sent
    print(f"{exceeded_without_prefill}/{total_cb_sent} CMPCTBLOCK's sent were already over the window for a single RTT. ({already_over_rtt_rate * 100:.2f}%)")
    print(f"{total_cb_in_need}/{total_cb_sent} blocks required prefills. ({prefill_needed_rate * 100:.2f}%)")
    print(f"For blocks needing prefills, average bytes needed for prefill: {avg_prefill_bytes:.2f}")
    print(f"Average available bytes up to the next tcp window boundary for all CMPCTBLOCK messages sent: {avg_available_bytes:.2f}")
    print(f"{prefills_that_fit} / {total_cb_in_need} blocks that needed prefills also would have had prefills that fit in the window. ({prefill_fit_rate * 100:.2f}%)")
if __name__ == "__main__":
    import sys
    if len(sys.argv) != 2:
        print("Usage: python script.py <filepath>")
        sys.exit(1)
    
    filepath = sys.argv[1]
    entries = main(filepath)
