from common.log import LogData
from .base import BasicParser


class SignatureBenchLogParser(BasicParser):
    LOGID_PATTERN = r"(host-\d+)\.log"

    def _parse_file(self, path: str, logid: str) -> LogData:
        ld = LogData("signature", logid, path)
        timestamp = 0.

        with open(path, "r") as f:
            for line in f:
                parts = line.strip().split()
                if parts[1] == "Verify/s" or parts[1] == "Sign/s":
                    ld.add_data(timestamp, operation=parts[1].split("/")[0].lower(), duration=int(parts[2]) * 1e-9,
                                count=1000)
                    timestamp += int(parts[2]) * 1e-9
                else:
                    ld.add_warning(timestamp, "Unknown line " + line)

        return ld
