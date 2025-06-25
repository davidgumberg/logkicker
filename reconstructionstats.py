import re
from dataclasses import dataclass
from datetime import datetime
from typing import Optional
import dateutil.parser

# only time and category and body are not optional, everything else might be omitted.
# {time} [{thread}] [{file:line}] [{function}] [{loglevel:category}] { BODY }
# 2025-06-25T01:44:46.462486Z [initload] [node/mempool_persist.cpp:78] [LoadMempool] [all:info] Progress loading mempool transactions from file: 40% (tried 132, 196 remaining)
class LogEntry:
    @dataclass
    class Metadata:
        time: datetime
        category: str
        thread: Optional[str] = None
        file: Optional[str] = None
        line: Optional[int] = None
        function: Optional[str] = None
        loglevel: Optional[str] = None

    def __init__(self, line):
        self.body = ""
        self.process_line_metadata(line)
    
    # 
    def process_line_metadata(self, line):
        time, line = line.split(' ', 1)

        parsed_time = dateutil.parser.parse(time)
        print(parsed_time)

        self.metadata = NotImplemented
        self.body = NotImplemented # Everything except for the metadata


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



