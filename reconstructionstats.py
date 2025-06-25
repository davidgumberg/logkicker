#!/usr/bin/env python3

import re
from dataclasses import dataclass
from datetime import datetime
from typing import Optional
import dateutil.parser

# todo: this should be generated in some way,
# hint:
# git grep -E "Thread(Ren|SetInternalN)ame" | sed -n 's/.*Thread\(Ren\|SetInternalN\)ame("\([^"]*\)").*/\2/p'
# git grep "&util::TraceThread" | sed -n 's/.*&util::TraceThread, "\([^"]*\)".*/\1/p'
threadname_strings = [
    "init",
    "http",
    "shutoff",
    "capnp-loop",
    "main",
    "qt-clientmodl",
    "qt-init",
    "qt-rpcconsole",
    "qt-walletctrl",
    "test",
    "initload",
    "mapport",
    "net",
    "dnsseed",
    "addcon",
    "opencon",
    "msghand",
    "i2paccept",
    "torcontrol",
]

numerousthreadname_patterns = [
    re.compile(r"^scriptch\.\d+$"),
    re.compile(r"^httpworker\.\d+$"),
]

# https://github.com/bitcoin/bitcoin/blob/471ee9d6b8a17d2839fc602309ddad45ff127a4d/src/logging.cpp#L170-L202
loggingcategory_strings = [
    "all",
    "net",
    "tor",
    "mempool",
    "http",
    "bench",
    "zmq",
    "walletdb",
    "rpc",
    "estimatefee",
    "addrman",
    "selectcoins",
    "reindex",
    "cmpctblock",
    "rand",
    "prune",
    "proxy",
    "mempoolrej",
    "libevent",
    "coindb",
    "qt",
    "leveldb",
    "validation",
    "i2p",
    "ipc",
    "lock",
    "blockstorage",
    "txreconciliation",
    "scan",
    "txpackages",
]

# AI Slop ⛽
logcategory_pattern = re.compile(r'^(' + '|'.join(re.escape(cat) for cat in loggingcategory_strings) + r')(?::(\w+))?$')

# e.g. src/net_processing.cpp:1154
sourcelocation_pattern = re.compile(r"^([^:]*\.(?:cpp|h)):(\d+)$")

# e.g. Function_Name9
functionname_pattern = re.compile(r'^(?:[a-zA-Z_][a-zA-Z0-9_]*|operator.+)$')

# only time and body are not optional, everything else might be omitted.
# {time} [{thread}] [{file:line}] [{function}] [{logcategory:loglevel}] [walletname] { BODY }
# 2025-06-25T20:15:37.882709Z [shutoff] [wallet/wallet.h:937] [WalletLogPrintf] [all:info] [Waleto] Releasing wallet Waleto..

class LogEntry:
    @dataclass
    class Metadata:
        time: datetime
        category: Optional[str] = None
        thread: Optional[str] = None
        file: Optional[str] = None
        line_num: Optional[int] = None
        function: Optional[str] = None
        loglevel: Optional[str] = None
        wallet_name: Optional[str] = None

    metadata: Metadata
    body: str

    def __init__(self, line):
        self.process_line_metadata(line)
        # self.print_wallet()
    
    # The whole burger's here, this is needed in order to split the body from
    # metadata.
    def process_line_metadata(self, logline):
        thread = None
        file = None
        line_num = None
        function = None
        loglevel = None
        category = None
        wallet_name = None

        time_string, remainder = logline.split(' ', 1)
        time = dateutil.parser.parse(time_string)

        # we are going to use this to split the body from the metadata, this is
        # a lul trucky b/c the body may have brackets
        metadata_pattern = re.compile(r'^(\s*(?:\[[^\]]+\]\s*)*)(.*?)$')
        metadata_pattern_matches = metadata_pattern.match(remainder)
        if metadata_pattern_matches is None:
            raise ValueError(f"Couldn't tell the body from the head! {logline}")
        metadata = metadata_pattern_matches.group(1)
        self.body = metadata_pattern_matches.group(2)

        bracket_pattern = r'\[([^\]]+)\]'
        matches = re.findall(bracket_pattern, metadata)
        # some lines.. some lines just don't have any metadata
        if not matches:
            self.metadata = self.Metadata(time=time)
            return

        right_side = matches.pop()
        logcategory_match = logcategory_pattern.match(right_side)
        # The first item from the right is either a wallet name, or it's a log category
        if logcategory_match is None:
            wallet_name = right_side
            right_side = matches.pop()
            logcategory_match = logcategory_pattern.match(right_side)
            if logcategory_match == None:
                raise ValueError(f"Didn't see a valid logcategory! {logline}")

        category = logcategory_match.group(1) 
        loglevel = logcategory_match.group(2)  # Will be None if no :loglevel part
            
        for metadatum in matches:
            if metadatum in threadname_strings:
                thread = metadatum
            elif any(pattern.match(metadatum) for pattern in numerousthreadname_patterns):
                thread = metadatum
            elif sourcelocation_match := sourcelocation_pattern.match(metadatum): # match filename pattern
                file = sourcelocation_match.group(1)
                line_num = int(sourcelocation_match.group(2))
            elif functionname_pattern.match(metadatum):
                function = metadatum
            else:
                raise ValueError(f"Invalid metadatum!: {metadatum} in {logline}")

        self.metadata = self.Metadata(
            time=time,
            category=category,
            thread=thread,
            file=file,
            line_num=line_num,
            function=function,
            loglevel=loglevel,
            wallet_name=wallet_name
        )        
    
    def print_wallet(self):
        if(self.metadata.thread):
            print(f"Thread: {self.metadata.thread}")
        if(self.metadata.file and self.metadata.line_num): 
            print(f"File: {self.metadata.file}, Line: {self.metadata.line_num}")
        if(self.metadata.category):
            print(f"Category: {self.metadata.category}, LogLevel = {self.metadata.loglevel}")
        if(self.metadata.function):
            print(f"Function: {self.metadata.function}")
        if(self.metadata.wallet_name):
            print(f"Wallet name: {self.metadata.wallet_name}")
        print(f"Body: {self.body}") 

def process_log(filepath):
    log_entries = []
    with open(filepath, 'r') as log:
        for line in log:
            if not line:
                continue
            line = line.strip()
            if line == "":
                continue
            log_entries.append(LogEntry(line))
        return log_entries

def main(filepath):
    log_entries = process_log(filepath)
    return log_entries


if __name__ == "__main__":
    import sys
    if len(sys.argv) != 2:
        print("Usage: python script.py <filepath>")
        sys.exit(1)
    
    filepath = sys.argv[1]
    entries = main(filepath)
    print(f"Processed {len(entries)} log entries")



