from collections import defaultdict
from typing import Iterable, Dict

from common.log import LogData
from common.statistics import average, percentile
from transformer.base import AverageTransformer


class ClientRawLatenciesTransformer(AverageTransformer):
    def __init__(self):
        super().__init__("latency", "client-raw-latencies")

    def _average(self, log: LogData, start_time: float, end_time: float) -> Iterable[Dict]:
        seen_client = set()

        # throughput
        throughput_data = defaultdict(lambda: defaultdict(lambda: 0))
        throughput_per_sec = defaultdict(lambda: [])
        # latency
        latencies = defaultdict(lambda: [])

        int_start_time = int(start_time)
        int_end_time = int(end_time)
        for e in log.entries():
            if e["client"] not in seen_client:
                # skip first request of each client
                seen_client.add(e["client"])
                continue

            # count based on reply time
            # with gaps in the timestamps, the request time should be used
            time = int(e["timestamp"])
            if int_start_time <= time < int_end_time:
                tag = e["tag"]
                throughput_data[tag][time] += 1
                latencies[tag].append(e["latency"])

        for tag in throughput_data.keys():
            for i in range(int_start_time, int_end_time):
                throughput_per_sec[tag].append(throughput_data[tag][i])

        out = []
        for tag in throughput_data.keys():
            p = {
                "timestamp": start_time,
                "client_count": log.client_count,
                "tag": tag,
                "throughput_average": average(throughput_per_sec[tag]),
                "throughput_median": percentile(throughput_per_sec[tag], 0.5),
                "latency_average": average(latencies[tag]),
                "latency_50": percentile(latencies[tag], 0.5),
                "latency_90": percentile(latencies[tag], 0.9),
                "latency_99": percentile(latencies[tag], 0.99),
            }
            out.append(p)
        return out
