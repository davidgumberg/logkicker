#!/usr/bin/env python3

from collections import defaultdict
import pprint
from dataclasses import dataclass
from enum import Enum
import re
from typing import Optional, Tuple
import logkicker
import datetime


@dataclass
class BlockReceived:
    time_received: datetime.datetime = datetime.datetime.now()
    time_reconstructed: datetime.datetime = datetime.datetime.now()
    cb_size_bytes: int = 0
    cb_missing_bytes: int = 0
    cb_missing_tx_count: int = 0
    cb_bytes_from_extra_pool: Optional[int] = None


@dataclass
class BlockSent:
    block_received: BlockReceived
    peer_id: int
    time_sent: datetime.datetime = datetime.datetime.now()
    cb_bytes_sent: int = 0
    tcp_window_bytes: int = 0


class ReasonsToCare(Enum):
    WE_DONT = 0
    CB_SEND = 1
    CB_RECEIVE = 2
    CB_RECONSTRUCTION = 3
    CB_TO_ANNOUNCE = 4
    CB_REQUESTED = 5
    NET_MAX_SEND = 6


@dataclass(frozen=True)
class LogPattern:
    """
    Encapsulates a log pattern, its category, the reason it's important,
    and how to parse its captured data.
    """
    reason: ReasonsToCare
    category: str
    regex: re.Pattern[str]
    # Maps a captured field name to a function for type conversion (e.g., int)
    # type_casts: Dict[str, TypeConverter] = field(default_factory=dict)


LOG_PATTERNS = [
    LogPattern(
        reason=ReasonsToCare.CB_RECONSTRUCTION,
        category="cmpctblock",
        # Successfully reconstructed block 000000000000000000000fec9bd60e4700c173a61195b46527bda8861f6b1276 with 1 txn prefilled, 4105 txn from mempool (incl at least 0 from extra pool) and 0 txn (0 bytes) requested
        regex=re.compile(
            r'Successfully reconstructed block (?P<blockhash>[0-9a-f]+) with '
            r'(?P<prefill_count>\d+) txn prefilled, (?P<mempool_count>\d+) '
            r'txn from mempool \(incl at least (?P<extrapool_count>\d+) from '
            r'extra pool\) and (?P<requested_count>\d+) txn '
            r'\((?P<requested_bytes>\d+) bytes\) requested'
        ),
        # type_casts={
            # 'prefilled_txns': int, 'mempool_txns': int, 'extra_pool_txns': int,
            # 'requested_txns': int, 'requested_bytes': int
        # }
    ),
    LogPattern(
        reason=ReasonsToCare.CB_RECEIVE,
        category="cmpctblock",
        # Initialized PartiallyDownloadedBlock for block 00000000000000000002165564043bef508ec2a8ddf81e15916114cbb5ce632b using a cmpctblock of 14691 bytes
        regex=re.compile(r'Initialized PartiallyDownloadedBlock for block (?P<blockhash>[0-9a-f]+) using a cmpctblock of (?P<cmpctblock_bytes>\d+) bytes'),
        # type_casts={'cmpctblock_bytes': int}
    ),
    LogPattern(
        reason=ReasonsToCare.CB_SEND,
        category="net",
        # sending cmpctblock (25101 bytes) peer=1
        regex=re.compile(r'sending cmpctblock \((?P<cmpctblock_bytes>\d+) bytes\) peer=(?P<peer_id>\d+)'),
        # type_casts={'cmpctblock_bytes': int, 'peer_id': int}
    ),
    LogPattern(
        reason=ReasonsToCare.CB_TO_ANNOUNCE,
        category="net",
        # PeerManager::NewPoWValidBlock sending header-and-ids 00000000000000000002165564043bef508ec2a8ddf81e15916114cbb5ce632b to peer=11
        regex=re.compile(r'PeerManager::NewPoWValidBlock sending header-and-ids (?P<blockhash>[0-9a-f]+) to peer=(?P<peer_id>\d+)'),
        # type_casts={'peer_id': int}
    ),
    LogPattern(
        reason=ReasonsToCare.CB_REQUESTED,
        category="net",
        # received getdata for: cmpctblock 0000000000000000000085ae6fe4bb42bb2395c4fce575eac8f8dcaa8bea0750 peer=3
        regex=re.compile(r'received getdata for: cmpctblock (?P<blockhash>[0-9a-f]+) peer=(?P<peer_id>\d+)'),
        # type_casts={'peer_id': int}
    ),
    LogPattern(
        reason=ReasonsToCare.NET_MAX_SEND,
        category="net",
        #     - Max send per-rtt: 14480 bytes
        regex=re.compile(r'\s*- Max send per-rtt: (?P<max_send_bytes>\d+) bytes'), # We're gonna strip it
        # type_casts={'max_send_bytes': int}
    ),
]


def we_care(entry: logkicker.LogEntry) -> Tuple[ReasonsToCare, Optional[logkicker.LogEntry]]:
    entry_category = entry.metadata.category
    for pattern in LOG_PATTERNS:
        if entry_category == pattern.category:
            if match := pattern.regex.match(entry.body):
                # Get a dictionary of {name: string_value} from the named
                # groups
                entry.data = match.groupdict()

                # Apply type conversions for fields specified in the pattern
                # for field_name, cast_function in pattern.type_casts.items():
                #    if field_name in data:
                #        data[field_name] = cast_function(data[field_name])

                return pattern.reason, entry
    return ReasonsToCare.WE_DONT, None

def parse_cb_log(filepath: str) -> Tuple[dict[str, BlockReceived], dict[str, list[BlockSent]]]:
    blocks_received: dict[str, BlockReceived] = {}
    blocks_sent: dict[str, list[BlockSent]] = defaultdict(list)
    pending_block_send: Optional[BlockSent] = None
    pending_max_send: Optional[BlockSent] = None

    for entry in logkicker.process_log_generator(filepath):
        why, what = we_care(entry)
        if why == ReasonsToCare.WE_DONT or what is None:
            continue
        match why:
            case ReasonsToCare.CB_RECEIVE:
                blocks_received[what.data['blockhash']] = BlockReceived(time_received=what.time(), cb_size_bytes=int(what.data['cmpctblock_bytes']))
            case ReasonsToCare.CB_RECONSTRUCTION:
                block = blocks_received[what.data['blockhash']]
                block.cb_missing_tx_count = int(what.data['requested_count'])
                block.cb_missing_bytes = int(what.data['requested_bytes'])
                block.time_reconstructed = what.time()
            case ReasonsToCare.CB_TO_ANNOUNCE | ReasonsToCare.CB_REQUESTED: # lucky, they have the same pattern!
                blockhash = what.data['blockhash']
                # On rare occassions we receive full-sized blocks, so we don't know their cb size.
                if blockhash not in blocks_received:
                    continue
                pending_block_send = BlockSent(block_received=blocks_received[blockhash], peer_id=int(what.data['peer_id']))
                blocks_sent[blockhash].append(pending_block_send)
            case ReasonsToCare.CB_SEND:
                if not pending_block_send:
                    continue
                assert pending_block_send.peer_id == int(what.data['peer_id'])
                pending_block_send.cb_bytes_sent = int(what.data['cmpctblock_bytes'])
                pending_block_send.time_sent = what.time()
                pending_max_send = pending_block_send
                pending_block_send = None
            case ReasonsToCare.NET_MAX_SEND:
                if not pending_max_send:
                    continue
                pending_max_send.tcp_window_bytes = int(what.data['max_send_bytes'])
                pending_max_send = None
    return blocks_received, blocks_sent



def main(filepath):
    blocks_received, blocks_sent = parse_cb_log(filepath)

    blocks_failing_reconstruction = [block for block in blocks_received.values() if block.cb_missing_tx_count > 0]

    print(f"{len(blocks_failing_reconstruction)} out of {len(blocks_received)} blocks received failed reconstruction.")
    fail_rate = len(blocks_failing_reconstruction) / len(blocks_received)
    reco_rate = 1 - fail_rate
    print(f"Reconstruction rate was {reco_rate * 100:.2f}%")

    total_cb_sent = total_prefilled_cb_sent = 0
    total_prefill_bytes = total_prefill_extra_bytes = 0
    total_available_bytes_in_all_windows = total_available_bytes_in_needed_windows = 0
    # blocks where the whole prefill fit
    prefills_that_fit = 0
    # blocks where the prefill without extra_txn fit
    no_extra_prefills_that_fit = 0
    # blocks that exceeded the first rtt window without any prefills
    exceeded_without_prefill = 0

    # Pretty print the blocks_sent data
    for blockhash, sent_list in blocks_sent.items():
        if blockhash not in blocks_received:
            continue
        received = blocks_received[blockhash]
        # the size of the CMPCTBLOCK message we received.
        received_size = received.cb_size_bytes

        for sent in sent_list:
            # running total of CMPCTBLOCK messages sent
            total_cb_sent += 1

            prefill_size = sent.cb_bytes_sent - received_size
            total_prefill_bytes += prefill_size

            # The number of rtt's it would have taken to announce the
            # CMPCTBLOCK with no additional prefilling
            rtts_needed_with_no_prefill: int = received_size // sent.tcp_window_bytes
            if (rtts_needed_with_no_prefill > 1):
                exceeded_without_prefill += 1
            # Bytes used in this window before prefill
            bytes_used_in_tcp_window = received_size % sent.tcp_window_bytes
            # The number of bytes of overhead we have up to the next tcp window boundary
            bytes_left_in_tcp_window: int = sent.tcp_window_bytes - bytes_used_in_tcp_window

            # running total of tcp windows for stats
            total_available_bytes_in_all_windows += bytes_left_in_tcp_window

            if prefill_size > 0:
                # available
                total_available_bytes_in_needed_windows += bytes_left_in_tcp_window
                total_prefilled_cb_sent += 1
                if prefill_size <= bytes_left_in_tcp_window:
                    # we got it for free 😎
                    prefills_that_fit += 1
                # this only works for the prefilling node. extra pool recovery size
                # is not logged, but we can recover this info from the difference
                # between sent size, todo: add logging to the branch
                # track the total number of cb's that needed prefills
                received.cb_bytes_from_extra_pool = sent.cb_bytes_sent - received_size - received.cb_missing_bytes
                total_prefill_extra_bytes += received.cb_bytes_from_extra_pool
                if prefill_size - received.cb_bytes_from_extra_pool <= bytes_left_in_tcp_window:
                    # We probably win a reconstruction here, but more data is needed.
                    no_extra_prefills_that_fit += 1

    sent_per_block = total_cb_sent / len(blocks_received)
    print(f"{total_cb_sent} CMPCTBLOCK messages sent, {sent_per_block:.2f} per block.")

    already_over_rtt_rate = exceeded_without_prefill / total_cb_sent
    print(f"{exceeded_without_prefill}/{total_cb_sent} CMPCTBLOCK's sent were already over the window for a single RTT before prefilling. ({already_over_rtt_rate * 100:.2f}%)")

    avg_available_bytes = total_available_bytes_in_all_windows / total_cb_sent
    print(f"Avg available prefill bytes for all CMPCTBLOCK's we sent: {avg_available_bytes:.2f}")
    if total_prefill_bytes > 0:
        avg_available_bytes_for_needed = total_available_bytes_in_needed_windows / total_cb_sent
        print(f"Avg available prefill bytes for prefilled CMPCTBLOCK's we sent: {avg_available_bytes_for_needed:.2f}")

        avg_prefill_bytes = total_prefill_bytes / total_prefilled_cb_sent
        print(f"Avg total prefill size for CMPCTBLOCK's we prefilled: {avg_prefill_bytes:.2f}")

        avg_extra_prefill_bytes = total_prefill_extra_bytes / total_prefilled_cb_sent
        print(f"Avg bytes of prefill that were extra_txn: {avg_extra_prefill_bytes:.2f}")

        avg_prefill_without_extra = (total_prefill_bytes - total_prefill_extra_bytes) / total_prefilled_cb_sent
        print(f"Avg prefill size w/o extras: {avg_prefill_without_extra:.2f}")

        prefill_needed_rate = total_prefilled_cb_sent / total_cb_sent
        prefill_fit_rate = prefills_that_fit / total_prefilled_cb_sent
        print(f"{total_prefilled_cb_sent}/{total_cb_sent} blocks sent required prefills. ({prefill_needed_rate * 100:.2f}%)")
        print(f"{prefills_that_fit}/{total_prefilled_cb_sent} prefilled blocks sent fit in the available bytes. ({prefill_fit_rate * 100:.2f}%)")
        print(f"{no_extra_prefills_that_fit}/{total_prefilled_cb_sent} prefilled blocks would have fit if we hadn't prefilled extra txn's. ({no_extra_prefills_that_fit / total_prefilled_cb_sent * 100:.2f}%)")


if __name__ == "__main__":
    import sys
    if len(sys.argv) != 2:
        print("Usage: python script.py <filepath>")
        sys.exit(1)

    filepath = sys.argv[1]
    entries = main(filepath)
