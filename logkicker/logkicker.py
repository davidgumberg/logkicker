import re
from dataclasses import dataclass
from typing import Callable, Generator, Optional
import dateutil.parser

# todo: this should be generated in some way,
# hint:
# git grep -E "Thread(Ren|SetInternalN)ame" | sed -n 's/.*Thread\(Ren\|SetInternalN\)ame("\([^"]*\)").*/\2/p'
# git grep "&util::TraceThread" | sed -n 's/.*&util::TraceThread, "\([^"]*\)".*/\1/p'
THREADNAME_STRINGS = frozenset([
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
])

NUMEROUS_THREADNAME_PATTERNS = [
    re.compile(r"^scriptch\.\d+$"),
    re.compile(r"^httpworker\.\d+$"),
]

# https://github.com/bitcoin/bitcoin/blob/471ee9d6b8a17d2839fc602309ddad45ff127a4d/src/logging.cpp#L170-L202
LOGGINGCATEGORY_STRINGS = frozenset([
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
])

METADATA_PATTERN = re.compile(r'^(\s*(?:\[[^\]]+\]\s*)*)(.*?)$')
BRACKET_PATTERN = re.compile(r'\[([^\]]+)\]')

# Combine categories 
LOGCATEGORY_PATTERN = re.compile(r'^(' + '|'.join(re.escape(cat) for cat in LOGGINGCATEGORY_STRINGS) + r')(?::(\w+))?$')

# e.g. src/net_processing.cpp:1154
SOURCELOCATION_PATTERN = re.compile(r"^([^:]*\.(?:cpp|h)):(\d+)$")

# e.g. Function_Name9
FUNCTIONNAME_PATTERN = re.compile(r'^(?:[a-zA-Z_][a-zA-Z0-9_]*|operator.+)$')

# only time and body are not optional, everything else might be omitted.
# {time} [{thread}] [{file:line}] [{function}] [{logcategory:loglevel}] [walletname] { BODY }
# 2025-06-25T20:15:37.882709Z [shutoff] [wallet/wallet.h:937] [WalletLogPrintf] [all:info] [Waleto] Releasing wallet Waleto..

class LogEntry:
    @dataclass
    class Metadata:
        time_str: str
        category: Optional[str] = None
        thread: Optional[str] = None
        file: Optional[str] = None
        line_num: Optional[int] = None
        function: Optional[str] = None
        loglevel: Optional[str] = None
        wallet_name: Optional[str] = None

    metadata: Metadata
    body: str
    # a dict containing the parsed variables of the log message.
    data: dict

    def time(self):
        return dateutil.parser.parse(self.metadata.time_str)

    def __init__(self, line):
        self.process_line_metadata(line)
        # self.print_metadata()

    # The whole burger's here, this is needed in order to split the body from
    # metadata.
    # todo: don't process all metadata up-front, we need more granular ways to
    # inquire about metadata, so that we can return early if e.g. we're
    # filtering on category, just find a category and return, leave other
    # fields unpopulated.
    def process_line_metadata(self, logline):
        thread = None
        file = None
        line_num = None
        function = None
        loglevel = None
        category = None
        wallet_name = None

        try:
            time_str, remainder = logline.split(' ', 1)
        except ValueError:
             print(f"Warning: Malformed logline: {logline}\n")
             return

        # we are going to use this to split the body from the metadata, this is
        # a lul trucky b/c the body may have brackets
        metadata_pattern_matches = METADATA_PATTERN.match(remainder)
        if metadata_pattern_matches is None:
            raise ValueError(f"Couldn't tell the body from the head! {logline}")
        metadata = metadata_pattern_matches.group(1)
        self.body = metadata_pattern_matches.group(2)

        matches = BRACKET_PATTERN.findall(metadata)
        # some lines.. some lines just don't have any metadata
        if not matches:
            self.metadata = self.Metadata(time_str=time_str)
            return

        right_side = matches.pop()
        logcategory_match = LOGCATEGORY_PATTERN.match(right_side)
        # The first item from the right is either a wallet name, or it's a log category
        if logcategory_match is None:
            wallet_name = right_side
            right_side = matches.pop()
            logcategory_match = LOGCATEGORY_PATTERN.match(right_side)
            if logcategory_match == None:
                raise ValueError(f"Didn't see a valid logcategory! {logline}")

        category = logcategory_match.group(1) 
        loglevel = logcategory_match.group(2)  # Will be None if no :loglevel part

        for metadatum in matches:
            if metadatum in THREADNAME_STRINGS:
                thread = metadatum
            elif any(pattern.match(metadatum) for pattern in NUMEROUS_THREADNAME_PATTERNS):
                thread = metadatum
            elif sourcelocation_match := SOURCELOCATION_PATTERN.match(metadatum): # match filename pattern
                file = sourcelocation_match.group(1)
                line_num = int(sourcelocation_match.group(2))
            elif FUNCTIONNAME_PATTERN.match(metadatum):
                function = metadatum
            else:
                raise ValueError(f"Invalid metadatum!: {metadatum} in {logline}")

        self.metadata = self.Metadata(
            time_str=time_str,
            category=category,
            thread=thread,
            file=file,
            line_num=line_num,
            function=function,
            loglevel=loglevel,
            wallet_name=wallet_name
        )

    def print_metadata(self):
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

def process_log_generator(
    filepath: str, 
    filter_func: Optional[Callable[[LogEntry], bool]] = None
) -> Generator[LogEntry, None, None]:
    """Generator that yields filtered log entries"""
    with open(filepath, 'r', buffering=8192) as log:
        for line in log:
            line = line.strip()
            if line:
                entry = LogEntry(line)
                if filter_func is None or filter_func(entry):
                    yield entry

def process_log(filepath: str) -> list[LogEntry]:
    return list(process_log_generator(filepath))
