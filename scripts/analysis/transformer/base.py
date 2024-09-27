import re
from collections import defaultdict
from typing import Iterable, Dict, Protocol
from typing import List

from common.errors import printerr
from common.log import LogData


class Transformer(Protocol):
    def transform(self, logs: Iterable[LogData], start_time: float, end_time: float) -> Iterable[LogData]: ...


class FatalLogIssue(Exception):
    def __init__(self, reason: str, entry: dict = None):
        self.reason = reason
        self.entry = entry

    def __str__(self):
        if self.entry is None:
            return self.reason
        return "{} on entry {}".format(self.reason, self.entry)


class AverageTransformer(Transformer):
    def __init__(self, logtype, derived_type):
        super().__init__()
        self.logtype = logtype
        self.derived_type = derived_type

    def transform(self, logs: Iterable[LogData], start_time: float, end_time: float) -> Iterable[LogData]:
        data_sets = defaultdict(lambda: {})  # type: defaultdict[str, Dict[int, LogData]]

        for log in logs:
            if log.logtype != self.logtype:
                continue
            subset = data_sets[log.logid]
            data_id = log.client_count
            if data_id in subset:
                raise FatalLogIssue(
                    "Multiple logs for one client count with logtype {} {}!".format(self.logtype, log.source_path))
            subset[data_id] = log

        if not data_sets:
            return logs

        logs = [log for log in logs]
        for client_id, subset in data_sets.items():
            keys = sorted(subset.keys())
            ld = subset[keys[0]].derive(self.derived_type)
            ld.set_scenario(ld.scenario, -1)

            for key in keys:
                log = subset[key]
                entries = self._average(log, start_time, end_time)
                for e in entries:
                    ld.add_entry(e)

            logs.append(ld)
        return logs

    def _average(self, log: LogData, start_time: float, end_time: float) -> Iterable[Dict]:
        pass


class Filter(Transformer):
    def transform(self, logs: Iterable[LogData], start_time: float, end_time: float) -> Iterable[LogData]:
        filtered_logs = []  # type: List[LogData]
        for log in logs:
            if not self.can_handle(log):
                filtered_logs.append(log)
                continue
            try:
                log = self.filter(log, start_time, end_time)
            except:
                printerr("Error while processing {} {} {}".format(log.logtype, log.logid, log.source_path))
                raise

            filtered_logs.append(log)
        return filtered_logs

    def can_handle(self, log: LogData) -> bool:
        ...

    # returns filtered Log
    def filter(self, log: LogData, start_time: float, end_time: float) -> LogData:
        ...


class FatalWarningFilter(Filter):
    def filter(self, log: LogData, start_time: float, end_time: float) -> LogData:
        for entry in log.entries():
            if "warning" in entry:
                raise FatalLogIssue("Unexpected warning", entry)

        return log


class Rule:
    def __init__(self, pattern: str, count: float = 0, ok_during_startup: bool = False,
                 ok_during_shutdown: bool = False):
        self.pattern = re.compile(pattern)
        self.count = count
        self.ok_during_startup = ok_during_startup
        self.ok_during_shutdown = ok_during_shutdown


class PatternChecker:
    def __init__(self, rules: Iterable[Rule], start_time: float, end_time: float):
        self.rules = rules
        self.start_time = start_time
        self.end_time = end_time
        self.ignored = defaultdict(lambda: 0)

    def matches(self, timestamp: float, line: str) -> bool:
        ignore = False
        for rule in self.rules:
            if rule.pattern.match(line):
                # ignore specific warning during startup / shutdown
                if timestamp < self.start_time and rule.ok_during_startup \
                        or timestamp > self.end_time and rule.ok_during_shutdown:
                    ignore = True
                    break
                self.ignored[rule.pattern] += 1
                if self.ignored[rule.pattern] <= rule.count:
                    ignore = True
                break
        return ignore

    def unmatched_rules(self) -> Iterable[Rule]:
        rules = []
        for rule in self.rules:
            if self.ignored[rule.pattern] <= rule.count:
                rules.append(rule)

        return rules
