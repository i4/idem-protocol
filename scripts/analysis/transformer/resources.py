from common.log import LogData
from .base import FatalWarningFilter


class ResourceLogFilter(FatalWarningFilter):
    def can_handle(self, log: LogData) -> bool:
        return log.logtype == "resources"
