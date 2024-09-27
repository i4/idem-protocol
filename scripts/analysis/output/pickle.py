import pickle
from typing import Iterable, Optional

from common.log import LogData
from .base import Transformer


class OutputPickleTransformer(Transformer):
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

        with open(self.path + ".pickle", "wb") as f:
            pickle.dump(logs, f)

        return logs
