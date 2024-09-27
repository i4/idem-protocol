import os
import re
from typing import Protocol, Optional

from common.log import LogData


class Parser(Protocol):
    def can_handle(self, fn: str) -> bool: ...

    def parse_file(self, path: str) -> LogData: ...


class BasicParser(Parser):
    # must match filename and contain one capturing group, e.g. r"(client-\d+)\.log"
    LOGID_PATTERN = r""

    def extract_id(self, fn: str) -> Optional[str]:
        match = re.match(self.LOGID_PATTERN, fn)
        if match is None:
            return None
        return match.group(1)

    def can_handle(self, fn: str) -> bool:
        return self.extract_id(fn) is not None

    def parse_file(self, path: str) -> LogData:
        return self._parse_file(path, self.extract_id(os.path.basename(path)))

    def _parse_file(self, path: str, logid: str) -> LogData: ...
