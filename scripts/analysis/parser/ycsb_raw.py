import re

from common.log import LogData
from transformer.base import FatalLogIssue
from .base import BasicParser


class YCSBRawLogParser(BasicParser):
    LOGID_PATTERN = r"(client-\d+)\.ycsb"

    def _parse_file(self, path: str, logid: str) -> LogData:
        ld = LogData("latency", logid, path)

        with open(path, "r") as f:
            timestamp = 0
            is_empty = True
            # keep last timestamp when encountering lines without
            for line in f:
                is_empty = False
                line = line.strip()
                if not line:
                    continue

                split_line = line.split(",")
                if len(split_line) != 3 or "latency raw data" in line:
                    if re.match(r"(READ|VERIFY|UPDATE|CLEANUP) latency raw data: op, timestamp\(ms\), latency\(us\)",
                                line):
                        continue
                    ld.add_warning(timestamp, line)
                    continue

                try:
                    timestamp = float(int(split_line[1])) * 1e-3
                    ld.add_data(timestamp, client=-1, latency=int(split_line[2]), tag=split_line[0])
                except ValueError:
                    ld.add_warning(timestamp, line)

        if is_empty:
            raise FatalLogIssue("Broken logfile " + path)

        return ld
