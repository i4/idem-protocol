from collections import defaultdict
from typing import Iterable, List, Dict, Set

from common.log import LogData
from transformer.base import Transformer, FatalLogIssue


class ClientRawAveragesTransformer(Transformer):
    def transform(self, logs: Iterable[LogData], start_time: float, end_time: float) -> Iterable[LogData]:
        logs = [log for log in logs]

        grouped_logs = defaultdict(lambda: defaultdict(lambda: []))  # type: Dict[str, Dict[int, List[LogData]]]
        grouped_logids = defaultdict(lambda: defaultdict(lambda: set()))  # type: Dict[str, Dict[int, Set[str]]]
        for log in logs:
            if log.logtype != "latency":
                continue
            if log.client_count is None:
                raise FatalLogIssue("Missing client count for log {} {}!".format(log.logtype, log.source_path))

            key = log.scenario
            grouped_logs[key][log.client_count].append(log)
            if log.logid in grouped_logids[key][log.client_count]:
                raise FatalLogIssue(
                    "Multiple logs for one client count with logtype {} {}!".format(log.logtype, log.source_path))
            grouped_logids[key][log.client_count].add(log.logid)

        for scenario, ccgroup in grouped_logs.items():
            if len(ccgroup) < 1:
                continue

            ld = ccgroup[next(iter(ccgroup.keys()))][0].derive("client-raw-averages")
            ld.logid = "client-avg"

            for client_count, group in ccgroup.items():
                tmp = LogData("latency", "client-avg", "derived avg")
                tmp.client_count = client_count
                for log in group:
                    for e in log.entries():
                        tmp.add_entry(e)

                entries = self._average(tmp, start_time, end_time)
                for e in entries:
                    ld.add_entry(e)

            logs.append(ld)
        return logs

    @staticmethod
    def _average(log: LogData, start_time: float, end_time: float) -> Iterable[Dict]:
        seen_client = set()

        # throughput
        throughput_data = defaultdict(lambda: 0)
        # latency
        latencies = defaultdict(lambda: 0)

        for e in log.entries():
            if e["client"] not in seen_client:
                # skip first request of each client
                seen_client.add(e["client"])
                continue

            # count based on reply time
            # with gaps in the timestamps, the request time should be used
            time = int(e["timestamp"])
            if start_time <= time < end_time:
                tag = e["tag"]
                throughput_data[tag] += 1
                latencies[tag] += e["latency"]

        out = []
        for tag in throughput_data.keys():
            latency = 0
            if throughput_data[tag] > 0:
                latency = latencies[tag] / throughput_data[tag]

            p = {
                "client_count": log.client_count,
                "timestamp": start_time + 0.01,
                "tag": tag,
                "throughput": throughput_data[tag] / (end_time - start_time),
                "latency": latency,
            }
            out.append(p)
        return out
