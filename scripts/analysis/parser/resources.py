from common.log import LogData
from .base import BasicParser


class ResourcesLogParser(BasicParser):
    LOGID_PATTERN = r"((client|server)-\d+)-resources\.log"

    def _parse_file(self, path: str, logid: str) -> LogData:
        ld = LogData("resources", logid, path)

        with open(path, "r") as f:
            timestamp = None
            # keep last timestamp when encountering lines without
            for line in f:
                line = line.strip()
                if not line:
                    continue

                parts = line.split(None, 2)
                try:
                    timestamp = float(parts[0])
                    if len(parts) == 1:
                        # should only happen from time to time for the last line in the logfile
                        continue
                    cpu_percent = float(parts[1])
                    memory_percent = float(parts[2])
                    ld.add_data(timestamp, cpu_percent=cpu_percent, memory_percent=memory_percent)
                except (IndexError, ValueError):
                    ld.add_warning(timestamp, line)

        return ld
