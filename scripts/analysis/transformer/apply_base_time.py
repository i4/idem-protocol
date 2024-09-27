from common.log import LogData
from transformer.base import Filter


class ApplyBaseTimeFilter(Filter):
    def can_handle(self, log: LogData) -> bool:
        return True

    def filter(self, log: LogData, abs_start_time: float, abs_end_time: float) -> LogData:
        for e in log.entries():
            e["timestamp"] = e["timestamp"] - log.base_time

        return log
