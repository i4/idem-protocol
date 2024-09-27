import json
import os
import pickle
import re
from typing import Iterable, Any, Optional, Sequence, List, Dict


class LogData:
    def __init__(self, logtype: str, logid: str, source_path: str):
        self.data = []
        self.logtype = logtype
        self.logid = logid
        self.source_path = source_path
        self.scenario = None  # type: Optional[str]
        self.client_count = None  # type: Optional[int]
        self.base_time = None  # type: Optional[float]

    # Timestamp must be normalized to us (microseconds)
    def add_data(self, timestamp: float, **values: Any) -> Dict[str, Any]:
        values["timestamp"] = timestamp
        self.data.append(values)
        return values

    def add_warning(self, timestamp: float, text: str) -> Dict[str, Any]:
        warning = {"timestamp": timestamp, "warning": text}
        self.data.append(warning)
        return warning

    def add_entry(self, entry) -> None:
        self.data.append(entry)

    def entries(self) -> Iterable[dict]:
        return self.data

    def set_scenario(self, scenario: str, client_count: int) -> None:
        self.scenario = scenario
        self.client_count = client_count

    def set_base_time(self, base_time: float) -> None:
        self.base_time = base_time

    def derive(self, logtype: Optional[str] = None) -> 'LogData':
        if logtype is None:
            log = LogData(self.logtype, self.logid, self.source_path)
        else:
            log = LogData(logtype, self.logid, "derived from " + self.source_path)
        log.set_scenario(self.scenario, self.client_count)
        log.set_base_time(self.base_time)
        return log

    def extract(self, fields: Sequence[str]) -> Sequence[List[Any]]:
        field_idx = range(len(fields))
        data = [[] for _ in field_idx]
        for e in self.entries():
            has_fields = True
            for field in fields:
                if field not in e:
                    has_fields = False
                    break
            if not has_fields:
                continue

            for i in field_idx:
                data[i].append(e[fields[i]])

        return data


# Use as `json.dumps(logs, default=encode_logdata)`
def encode_logdata(obj):
    if isinstance(obj, LogData):
        return {"logid": obj.logid, "logtype": obj.logtype, "data": obj.data, "source_path": obj.source_path,
                "scenario": obj.scenario, "client_count": obj.client_count, "base_time": obj.base_time}
    # Let the base class default method raise the TypeError
    raise TypeError("don't know how to encode object of type {}".format(type(obj)))


# Use as `json.loads(logstr, object_hook=decode_logdata)`
def decode_logdata(dct):
    if "logtype" in dct and "logid" in dct and "source_path" in dct:
        log = LogData(dct["logtype"], dct["logid"], dct["source_path"])
        log.set_scenario(dct["scenario"], dct["client_count"])
        log.set_base_time(dct["base_time"])
        for e in dct["data"]:
            log.add_entry(e)
        return log
    return dct


def find_matching_files(fn_pattern):
    regexp = re.compile(fn_pattern)
    files = [f for f in os.listdir(".") if os.path.isfile(f) and regexp.match(f)]
    return files


def load_pickle_logs(fn_pattern: str = r"parsed-.+\.pickle") -> Iterable[Iterable[LogData]]:
    files = find_matching_files(fn_pattern)
    logs = []

    if not files:
        raise FileNotFoundError("No files found which match {}".format(fn_pattern))

    for fn in files:
        with open(fn, 'rb') as f:
            logs.append(pickle.load(f))

    return logs


def load_json_logs(fn_pattern: str = r"parsed-.+\.json") -> Iterable[Iterable[LogData]]:
    files = find_matching_files(fn_pattern)
    logs = []

    for fn in files:
        with open(fn) as f:
            logs.append(json.load(f, object_hook=decode_logdata))

    return logs


def select_logtype(logs: Iterable[LogData], logtype: str) -> Iterable[LogData]:
    return [log for log in logs if log.logtype == logtype]
