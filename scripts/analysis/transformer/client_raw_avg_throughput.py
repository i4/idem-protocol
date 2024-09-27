from collections import defaultdict
from typing import Iterable, List, Dict

from common.log import LogData
from transformer.client_raw_throughput import ClientRawThroughputTransformer


class ClientRawAvgThroughputTransformer(ClientRawThroughputTransformer):
    def transform(self, logs: Iterable[LogData], start_time: float, end_time: float) -> Iterable[LogData]:
        logs = [log for log in logs]

        grouped_logs = defaultdict(lambda: [])  # type: Dict[str, List[LogData]]
        for log in logs:
            if log.logtype != "latency":
                continue

            grouped_logs[log.scenario].append(log)

        for scenario, group in grouped_logs.items():
            if len(group) <= 1:
                continue

            tmp = LogData("latency", "client-avg", "derived avg")
            for log in group:
                for e in log.entries():
                    tmp.add_entry(e)

            ld = group[0].derive("client-raw-avg-throughput")
            ld.logid = "client-avg"
            entries = self._average(tmp, start_time, end_time)
            for e in entries:
                ld.add_entry(e)

            logs.append(ld)
        return logs
