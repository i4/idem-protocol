import math
from typing import Iterable

from common.log import LogData
from .base import Filter, PatternChecker, Rule, FatalLogIssue


class ClientLogFilter(Filter):
    def __init__(self, ignore_lines: Iterable[Rule]):
        super().__init__()
        self._ignore_lines = ignore_lines

    def can_handle(self, log: LogData) -> bool:
        return log.logtype == "client"

    def filter(self, log: LogData, start_time: float, end_time: float) -> LogData:
        out = log.derive()
        checker = PatternChecker(self._ignore_lines, start_time, end_time)
        has_start = False
        data_points = 0

        for entry in log.entries():
            if "warning" in entry:
                if not checker.matches(entry["timestamp"], entry["warning"]):
                    out.add_entry(entry)
            else:
                out.add_entry(entry)
                # if "latency" in entry and entry["latency"] == -1 and start_time <= entry["timestamp"] < end_time:
                #     out.add_warning(entry["timestamp"], "Unexpected latency spike (> 1 second!)")
                if "started" in entry and entry["timestamp"] < start_time:
                    # ignore start confirmation if it is too late
                    has_start = True
                if "throughput" in entry and start_time <= entry["timestamp"] < end_time:
                    data_points += 1

        if not has_start:
            raise FatalLogIssue("No or too late log start")

        # fatal error if not enough data points during time range
        expected_data = int(math.ceil(end_time - start_time))
        if abs(expected_data - data_points) > 1:
            raise FatalLogIssue("Unexpected log length, got {} expected {}".format(data_points, expected_data))

        return out
