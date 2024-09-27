import json
from typing import Iterable, Optional

from common.log import LogData, encode_logdata
from .base import Transformer


class OutputJSONTransformer(Transformer):
    def __init__(self, path: str, logtype: Optional[str] = None):
        self.path = path
        self.logtype = logtype

    def transform(self, logs: Iterable[LogData], start_time: float, end_time: float) -> Iterable[LogData]:
        if self.logtype is not None:
            fl = []
            for log in logs:
                if log.logtype == self.logtype:
                    fl.append(log)
            logs = fl

        with open(self.path + ".json", "w") as f:
            # serialize to string and write afterwards, much faster than directly writing to a file
            s = json.dumps(logs, default=encode_logdata, indent=4, sort_keys=True)
            f.write(s)
            # very, very slow:
            # json.dump(logs, f, default=encode_logdata)

        return logs
