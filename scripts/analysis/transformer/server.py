from typing import Iterable

from common.log import LogData
from .base import Filter, PatternChecker, Rule, FatalLogIssue


class ServerLogFilter(Filter):
    def __init__(self, ignore_lines: Iterable[Rule], must_have_lines: Iterable[Rule]):
        super().__init__()
        self._ignore_lines = ignore_lines
        self._must_have_lines = must_have_lines

    def can_handle(self, log: LogData) -> bool:
        return log.logtype == "server"

    def filter(self, log: LogData, start_time: float, end_time: float) -> LogData:
        out = log.derive()
        ignores = PatternChecker(self._ignore_lines, start_time, end_time)
        must_haves = PatternChecker(self._must_have_lines, start_time, end_time)
        has_start = False

        for entry in log.entries():
            if "warning" in entry:
                must_haves.matches(entry["timestamp"], entry["warning"])
                if not ignores.matches(entry["timestamp"], entry["warning"]):
                    out.add_entry(entry)
            else:
                out.add_entry(entry)
                if "started" in entry and entry["timestamp"] < start_time:
                    # ignore start confirmation if it is too late
                    has_start = True

        if not has_start:
            raise FatalLogIssue("No or too late log start")

        unmatched_rules = must_haves.unmatched_rules()
        if unmatched_rules:
            raise FatalLogIssue("Did not find expected patterns:\n    "
                                + "\n    ".join([str(r.pattern.pattern) for r in unmatched_rules]))

        return out
