from typing import Iterable, Optional, Callable

from common.log import LogData
from transformer.base import Transformer


class OutputTransformer(Transformer):
    OUTPUT_LOGTYPE = ""

    def transform(self, logs: Iterable[LogData], start_time: float, end_time: float) -> Iterable[LogData]:
        for log in logs:
            if log.logtype != self.OUTPUT_LOGTYPE:
                continue

            self.output(log, start_time, end_time)

        return logs

    def writeCSV(self, log: LogData, fn: str, columns: Iterable[str],
                 value_filter: Optional[Callable[[dict], bool]] = None) -> None:
        with open(fn, "w") as f:
            f.write(" ".join(columns))
            f.write("\n")
            for e in log.entries():
                if value_filter is not None and not value_filter(e):
                    continue
                line = [e[col] for col in columns]
                f.write(" ".join([str(i) for i in line]))
                f.write("\n")

    def output(self, log: LogData, start_time: float, end_time: float) -> None:
        ...
