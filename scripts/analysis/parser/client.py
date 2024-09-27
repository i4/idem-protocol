import re

from common.log import LogData
from .base import BasicParser


class ClientLogParser(BasicParser):
    LOGID_PATTERN = r"(client-\d+)\.log"

    def _parse_file(self, path: str, logid: str) -> LogData:
        ld = LogData("client", logid, path)

        with open(path, "r") as f:
            at_start = True
            timestamp = None
            for line in f:
                line = line.rstrip()
                if not line:
                    continue

                split_line = line.split(None, 1)
                # ignore number prefixed with whitespace
                if line[0].isdigit() and split_line[0].isdigit() and len(split_line) > 1:
                    ts = float(int(split_line[0])) * 1e-6
                    # ignore numbers that are too low to be timestamps
                    if ts > 1000:
                        timestamp = ts
                        # remove timestamp at line start
                        line = split_line[1]
                # keep last timestamp when encountering a line without

                if at_start:
                    if "[EVENT] BENCH: Startup delay" in line:
                        at_start = False
                        # ignore printed delay and use the log timestamp instead
                        ld.add_data(timestamp, started=1)
                    continue
                elif line.startswith("main [EVENT] BENCH: END: "):
                    break

                split_line = line.split(None, 3)
                if len(split_line) >= 4 and split_line[0].isdigit() \
                        and split_line[1].isdigit() and (split_line[2].isdigit() or split_line[2] == "NaN"):
                    # assume this is measurement data
                    throughput = int(split_line[1])
                    if split_line[2] == "NaN":
                        latency = min_latency = max_latency = -1
                    else:
                        latency = int(split_line[2])
                        remainder = split_line[3]
                        groups = re.match(r"\( *(\d+)/ *(\d+)\)", remainder).groups()
                        min_latency = int(groups[0])
                        max_latency = int(groups[1])
                    ld.add_data(timestamp, throughput=throughput, latency=latency, latency_min=min_latency,
                                latency_max=max_latency)
                else:
                    ld.add_warning(timestamp, line)

        return ld
