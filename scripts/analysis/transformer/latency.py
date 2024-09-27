from common.log import LogData
from .base import FatalWarningFilter


class LatencyLogFilter(FatalWarningFilter):
    def can_handle(self, log: LogData) -> bool:
        return log.logtype == "latency"
