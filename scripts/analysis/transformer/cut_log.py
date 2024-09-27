from common.log import LogData
from transformer.base import Filter


class CutLogFilter(Filter):
    def can_handle(self, log: LogData) -> bool:
        return True

    def filter(self, log: LogData, start_time: float, end_time: float) -> LogData:
        out = log.derive()
        for entry in log.entries():
            if start_time <= entry["timestamp"] < end_time:
                out.add_entry(entry)

        return out
