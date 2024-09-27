from common.log import LogData
from .base import BasicParser


class LatencyLogParser(BasicParser):
    LOGID_PATTERN = r"(client-\d+)\.latency"

    def _parse_file(self, path: str, logid: str) -> LogData:
        ld = LogData("latency", logid, path)

        with open(path, "r") as f:
            timestamp = None
            # keep last timestamp when encountering lines without
            for line in f:
                line = line.strip()
                if not line:
                    continue

                split_line = line.split()
                if len(split_line) != 4:
                    ld.add_warning(timestamp, line)
                    continue

                try:
                    timestamp = float(int(split_line[1])) * 1e-6
                    ld.add_data(timestamp, client=int(split_line[0]), latency=int(split_line[2]), tag=split_line[3])
                except ValueError:
                    ld.add_warning(timestamp, line)

        return ld
