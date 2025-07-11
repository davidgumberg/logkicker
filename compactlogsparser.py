#!/usr/bin/env python3

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

import argparse
from collections import defaultdict
import pprint
from dataclasses import dataclass
from enum import Enum
import re
import sys
from typing import Optional, Tuple
import logkicker
import datetime


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


@dataclass
class BlockReceived:
    time_received: datetime.datetime = datetime.datetime.now()
    time_reconstructed: datetime.datetime = datetime.datetime.now()
    received_size: int = 0
    bytes_missing: int = 0
    received_tx_missing: int = 0
    extra_pool_prefill_size: Optional[int] = None


@dataclass
class BlockSent:
    block_received: BlockReceived
    peer_id: int
    time_sent: datetime.datetime = datetime.datetime.now()
    send_size: int = 0
    tcp_window_size: int = 0


def parse_cb_log(
    filepath: str
) -> Tuple[dict[str, BlockReceived], dict[str, list[BlockSent]]]:

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
                blocks_received[what.data['blockhash']] = BlockReceived(time_received=what.time(), received_size=int(what.data['cmpctblock_bytes']))
            case ReasonsToCare.CB_RECONSTRUCTION:
                block = blocks_received[what.data['blockhash']]
                block.received_tx_missing = int(what.data['requested_count'])
                block.bytes_missing = int(what.data['requested_bytes'])
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
                pending_block_send.send_size = int(what.data['cmpctblock_bytes'])
                pending_block_send.time_sent = what.time()
                pending_max_send = pending_block_send
                pending_block_send = None
            case ReasonsToCare.NET_MAX_SEND:
                if not pending_max_send:
                    continue
                pending_max_send.tcp_window_size = int(what.data['max_send_bytes'])
                pending_max_send = None
    return blocks_received, blocks_sent


def create_dataframes(blocks_received: dict[str, BlockReceived], blocks_sent: dict[str, list[BlockSent]]):
    # Create DataFrame for received blocks
    received_df = pd.DataFrame.from_dict(blocks_received, orient='index')
    received_df.rename_axis("blockhash")

    # Create DataFrame for sent blocks, including data from received blocks
    sent_data = []
    for blockhash, sent_list in blocks_sent.items():
        if blockhash not in blocks_received:
            continue  # Skip if blockhash not in received, as per original logic
        received = blocks_received[blockhash]
        for sent in sent_list:
            sent_dict = {
                'blockhash': blockhash,
                'time_sent': sent.time_sent,
                'peer_id': sent.peer_id,
                'tcp_window_size': sent.tcp_window_size,
                'received_size': received.received_size,  # From received block
                'received_bytes_missing': received.bytes_missing,
                'received_tx_missing': received.received_tx_missing,
                'send_size': sent.send_size,
            }
            sent_data.append(sent_dict)
    sent_df = pd.DataFrame(sent_data)

    # Add derived columns
    sent_df['prefill_size'] = sent_df['send_size'] - sent_df['received_size']
    # this only works for the prefilling node. extra pool recovery size
    # is not logged, but we can recover this info from the difference
    # between sent size, todo: add logging to the branch
    # track the total number of cb's that needed prefills
    sent_df['extra_pool_prefill_size'] = np.where(sent_df['prefill_size'] > 0, sent_df['prefill_size'] - sent_df['received_bytes_missing'], 0)
    sent_df['window_bytes_used'] = sent_df['received_size'] % sent_df['tcp_window_size']
    sent_df['window_bytes_available'] = sent_df['tcp_window_size'] - sent_df['window_bytes_used']
    sent_df['rtts_without_prefill'] = (sent_df['received_size'] // sent_df['tcp_window_size']).astype(int)

    return received_df, sent_df


def compute_stats(received: pd.DataFrame, sent: pd.DataFrame) -> None:
    # Reconstruction stats
    total_cb_received = len(received)
    if total_cb_received == 0:
        return
    failed_blocks = (received['received_tx_missing'] > 0).sum()
    fail_rate = failed_blocks / total_cb_received
    reco_rate = 1 - fail_rate
    print(f"{failed_blocks} out of {total_cb_received} blocks received failed reconstruction.")
    print(f"Reconstruction rate was {reco_rate * 100:.2f}%")

    # Sending stats
    total_cb_sent = len(sent)
    sent_per_block = total_cb_sent / total_cb_received if total_cb_received > 0 else 0
    exceeded_without_prefill = (sent['rtts_without_prefill'] > 1).sum()
    already_over_rtt_rate = exceeded_without_prefill / total_cb_sent if total_cb_sent > 0 else 0

    avg_available_bytes_all = sent['window_bytes_available'].mean()

    prefilled_sends = sent[sent['prefill_size'] > 0]
    total_prefilled_cb_sent = len(prefilled_sends)
    if total_prefilled_cb_sent == 0:
        return
    print(f"{total_cb_sent} CMPCTBLOCK messages sent, {sent_per_block:.2f} per block.")
    print(f"{exceeded_without_prefill}/{total_cb_sent} CMPCTBLOCK's sent were already over the window for a single RTT before prefilling. ({already_over_rtt_rate * 100:.2f}%)")
    print(f"Avg available prefill bytes for all CMPCTBLOCK's we sent: {avg_available_bytes_all:.2f}")

    avg_prefill_bytes = prefilled_sends['prefill_size'].mean()
    avg_extra_prefill_bytes = prefilled_sends['extra_pool_prefill_size'].mean()
    avg_prefill_without_extra = (prefilled_sends['prefill_size'] - prefilled_sends['extra_pool_prefill_size']).mean()
    prefills_that_fit = (prefilled_sends['prefill_size'] <= prefilled_sends['window_bytes_available']).sum()
    prefill_fit_rate = prefills_that_fit / total_prefilled_cb_sent
    no_extra_prefills_that_fit_count = ((prefilled_sends['prefill_size'] - prefilled_sends['extra_pool_prefill_size']) <= prefilled_sends['window_bytes_available']).sum()
    no_extra_prefills_that_fit_rate = no_extra_prefills_that_fit_count / total_prefilled_cb_sent
    total_available_bytes_in_needed_windows = prefilled_sends['window_bytes_available'].sum()
    avg_available_bytes_for_needed = total_available_bytes_in_needed_windows / total_prefilled_cb_sent
    prefill_needed_rate = total_prefilled_cb_sent / total_cb_sent

    print(f"Avg available prefill bytes for prefilled CMPCTBLOCK's we sent: {avg_available_bytes_for_needed:.2f}")
    print(f"Avg total prefill size for CMPCTBLOCK's we prefilled: {avg_prefill_bytes:.2f}")
    print(f"Avg bytes of prefill that were extra_txn: {avg_extra_prefill_bytes:.2f}")
    print(f"Avg prefill size w/o extras: {avg_prefill_without_extra:.2f}")
    print(f"{total_prefilled_cb_sent}/{total_cb_sent} blocks sent required prefills. ({prefill_needed_rate * 100:.2f}%)")
    print(f"{prefills_that_fit}/{total_prefilled_cb_sent} prefilled blocks sent fit in the available bytes. ({prefill_fit_rate * 100:.2f}%)")
    print(f"{no_extra_prefills_that_fit_count}/{total_prefilled_cb_sent} prefilled blocks would have fit if we hadn't prefilled extra txn's. ({no_extra_prefills_that_fit_rate * 100:.2f}%)")


def make_plots(received: pd.DataFrame, sent: pd.DataFrame):
    # Filter for prefilled sends for plotting
    prefilled_sends = sent[sent['prefill_size'] > 0]

    # Plot 1: Histogram of prefill sizes for prefilled blocks
    plt.figure(figsize=(10, 6))
    sns.histplot(prefilled_sends['prefill_size'], bins=50, kde=True)
    plt.title('Histogram of Prefill Sizes for Prefilled Blocks')
    plt.xlabel('Prefill Size (bytes)')
    plt.ylabel('Frequency')
    plt.show()

    # Plot 2: Scatter plot of compact block size vs TCP window size, colored by prefill status
    plt.figure(figsize=(10, 6))
    sns.scatterplot(data=sent, x='received_size', y='tcp_window_size', hue=(sent['prefill_size'] > 0))
    plt.title('Compact Block Size vs TCP Window Size')
    plt.xlabel('Compact Block Size (bytes)')
    plt.ylabel('TCP Window Size (bytes)')
    plt.legend(title='Prefill Needed')
    plt.show()

    # Additional plots can be added here, e.g., reconstruction failure rate over time or other distributions


def output_excel(received: pd.DataFrame, sent: pd.DataFrame, filename='compactblocksdata.xslx'):
    # sadly necessary to strip TZ from datetime fields:
    for df in [received, sent]:
        dt_cols = df.select_dtypes(include=['datetime64[ns, UTC]']).columns
        for col in dt_cols:
                df[col] = df[col].dt.tz_localize(None)
    # Write both dataframes to one Excel file
    with pd.ExcelWriter(filename, engine='xslxwriter') as writer:
        sent.to_excel(writer, sheet_name='sent')
        received.to_excel(writer, sheet_name='received')
    print(f"Data saved to {filename}")


def main():
    parser = argparse.ArgumentParser(description="Process compact block logs.")
    command_parser = parser.add_subparsers(dest='command')

    # Parse command
    parse_command = command_parser.add_parser('parse', help='Parse a log file into a Libreoffice Calc file.')
    parse_command.add_argument('logfile', help='Path to the log file.')
    parse_command.add_argument('output', nargs='?', default='cb_data_output.xslx', help='Output Calc file path.')

    # Stats command
    stats_command = command_parser.add_parser('stats', help='Compute and print statistics from an xslx.')
    stats_command.add_argument('excelfile', help='Path to the xslx file.')

    # Plot command
    plot_command = command_parser.add_parser('plot', help='Generate plots from a xslx.')
    plot_command.add_argument('excelfile', help='Path to the xslx file.')

    args = parser.parse_args()

    if args.command == 'parse':
        blocks_received, blocks_sent = create_dataframes(*parse_cb_log(args.logfile))
        output_excel(blocks_received, blocks_sent, args.output)

    elif args.command == 'stats':
        received = pd.read_excel(args.xlsxfile, sheet_name='received')
        sent = pd.read_excel(args.xlsxfile, sheet_name='sent')

        compute_stats(received, sent)

    elif args.command == 'plot':
        received = pd.read_excel(args.xlsxfile, sheet_name='received')
        sent = pd.read_excel(args.xlsxfile, sheet_name='sent')

        make_plots(received_df, sent_df)

    else:
        parser.print_usage()
        parser.print_help()


if __name__ == "__main__":
    main()
