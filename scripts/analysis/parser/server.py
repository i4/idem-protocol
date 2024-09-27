from common.log import LogData
from .base import BasicParser


class ServerLogParser(BasicParser):
    LOGID_PATTERN = r"(server-\d+)\.log"

    def _parse_file(self, path: str, logid: str) -> LogData:
        ld = LogData("server", logid, path)

        with open(path, "r") as f:
            at_start = True
            timestamp = None
            for line in f:
                line = line.strip()
                if not line:
                    continue

                split_line = line.split(None, 1)
                if split_line[0].isdigit() and len(split_line) > 1:
                    timestamp = float(int(split_line[0])) * 1e-6
                    # remove timestamp at line start
                    line = split_line[1]
                # keep last timestamp when encountering lines without

                if at_start:
                    if line == "main [EVENT] RPLCA: READY":
                        at_start = False
                        ld.add_data(timestamp, started=1)
                    continue

                ld.add_warning(timestamp, line)

        return ld
