#!/usr/bin/env python3

import argparse
from collections import defaultdict
from dataclasses import dataclass
import datetime
from enum import Enum
import matplotlib.pyplot as plt
import pandas as pd
import re
from typing import Optional, Tuple

from compactblocks.plots import plot_prefill_distributions, plot_received_size, plot_reconstruction_histogram_and_scatterplot, plot_tcp_window_histogram
from compactblocks.stats import received_stats, sent_stats, sent_already_over_stats
import logkicker.logkicker as logkicker

pd.options.mode.copy_on_write = True


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
    time_received: Optional[datetime.datetime] = None
    time_reconstructed: Optional[datetime.datetime] = None
    received_size: int = 0
    bytes_missing: int = 0
    received_tx_missing: int = 0
    reconstruction_time_ns: float = 0.0


@dataclass
class BlockSent:
    block_received: BlockReceived
    peer_id: int
    time_sent: Optional[datetime.datetime] = None
    send_size: int = 0
    tcp_window_size: int = 0


def parse_cb_log(
    filepath: str
) -> Tuple[dict[str, BlockReceived], dict[str, list[BlockSent]]]:

    blocks_received: dict[str, BlockReceived] = {}
    blocks_sent: dict[str, list[BlockSent]] = defaultdict(list)
    pending_block_send: Optional[BlockSent] = None
    pending_max_send: Optional[BlockSent] = None
    hash_pending_reconstruction: Optional[str] = None

    for entry in logkicker.process_log_generator(filepath):
        why, what = we_care(entry)
        if why == ReasonsToCare.WE_DONT or what is None:
            continue
        match why:
            case ReasonsToCare.CB_RECEIVE:
                if hash_pending_reconstruction is not None:
                    # handle the case where the last block we received never
                    # got reconstructed, that means it was either orphaned
                    # before reconstruction, or we got it via old-school BLOCK
                    # message, so we're going to delete it.
                    del blocks_received[hash_pending_reconstruction]

                hash_pending_reconstruction = what.data['blockhash']
                blocks_received[what.data['blockhash']] = BlockReceived(time_received=what.time(), received_size=int(what.data['cmpctblock_bytes']))
            case ReasonsToCare.CB_RECONSTRUCTION:
                if hash_pending_reconstruction != what.data['blockhash']:
                    # Reconstructing a block we never heard about it, or the
                    # wrong block, shouldn't happen, maybe in a re-org? Let's
                    # just skip it.
                    print(f"Warning: Message found reconstructing block {what.data['blockhash']}, which we didn't expect.")
                    continue
                block = blocks_received[what.data['blockhash']]
                block.received_tx_missing = int(what.data['requested_count'])
                block.bytes_missing = int(what.data['requested_bytes'])
                block.time_reconstructed = what.time()

                # Nothing is pending now
                hash_pending_reconstruction = None
            case ReasonsToCare.CB_TO_ANNOUNCE | ReasonsToCare.CB_REQUESTED:  # lucky, they have the same pattern!
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
    # The dict key is the blockhash.
    received_df = received_df.rename_axis("blockhash")

    # Add derived columns for received
    received_df['reconstruction_time_ns'] = (received_df['time_reconstructed'] - received_df['time_received']).astype('int64')

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
    sent_df = sent_df.set_index('blockhash')

    # Add derived columns for sent
    sent_df['prefill_size'] = sent_df['send_size'] - sent_df['received_size']
    sent_df['window_bytes_used'] = sent_df['received_size'] % sent_df['tcp_window_size']
    sent_df['window_bytes_available'] = sent_df['tcp_window_size'] - sent_df['window_bytes_used']
    sent_df['rtts_without_prefill'] = (sent_df['received_size'] // sent_df['tcp_window_size']).astype(int)

    return received_df, sent_df

def compute_stats(received: pd.DataFrame, sent: pd.DataFrame) -> None:
    received_stats(received)
    sent_stats(sent)
    sent_already_over_stats(sent)


def make_plots(received: pd.DataFrame, sent: pd.DataFrame, filename: str):
    plot_received_size(received)
    plot_reconstruction_histogram_and_scatterplot(received, filename)
    plot_tcp_window_histogram(sent)
    plot_prefill_distributions(sent)
    plt.show()


def output_excel(received: pd.DataFrame, sent: pd.DataFrame, filename='compactblocksdata.xlsx'):
    # sadly necessary to strip TZ from datetime fields in two ways:
    for df in [received, sent]:
        dt_cols = df.select_dtypes(include=['datetime64[ns, UTC]']).columns
        for col in dt_cols:
            df[col] = df[col].dt.tz_localize(None)
    # Write both dataframes to one Excel file
    with pd.ExcelWriter(
        filename,
        engine='xlsxwriter',
        # necessary to strip TZ from datetime fields.
        engine_kwargs={"options": {"remove_timezone": True}}
    ) as writer:
        sent.to_excel(writer, sheet_name='sent',)
        received.to_excel(writer, sheet_name='received')
    print(f"Data saved to {filename}")


def main():
    parser = argparse.ArgumentParser(description="Process compact block logs.")
    command_parser = parser.add_subparsers(dest='command')

    # Parse command
    parse_command = command_parser.add_parser('parse', help='Parse a log file into a Libreoffice Calc file.')
    parse_command.add_argument('logfile', help='Path to the log file.')
    parse_command.add_argument('output', nargs='?', default='compactblocksdata.xlsx', help='Output Calc file path.')

    # Stats command
    stats_command = command_parser.add_parser('stats', help='Compute and print statistics from an xlsx.')
    stats_command.add_argument('xlsxfile', help='Path to the xlsx file.')

    # Plot command
    plot_command = command_parser.add_parser('plot', help='Generate plots from a xlsx.')
    plot_command.add_argument('xlsxfile', help='Path to the xlsx file.')

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

        make_plots(received, sent, args.xlsxfile)

    else:
        parser.print_usage()
        parser.print_help()


if __name__ == "__main__":
    main()
