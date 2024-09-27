from typing import Iterable, Dict

from common.log import LogData
from common.statistics import average, median, stddev
from transformer.base import AverageTransformer


class ClientLogAveragesTransformer(AverageTransformer):
    def __init__(self):
        super().__init__("client", "client-log-averages")

    def _average(self, log: LogData, start_time: float, end_time: float) -> Iterable[Dict]:
        data = [d for d in log.entries() if "throughput" in d and start_time <= d["timestamp"] < end_time]

        throughput = [d["throughput"] for d in data]
        latency = [d["latency"] for d in data]
        avg_throughput = average(throughput)
        med_throughput = median(throughput)
        stddev_throughput = stddev(throughput)
        # average of the average latency -> shows latency stability over time
        avg_latency = average(latency)
        # median of averages, can't reconstruct the real median
        med_latency = median(latency)
        # variance of the average, the real value would require each individual duration
        stddev_latency = stddev(latency)

        return [{
            "timestamp": data[0]["timestamp"],
            "client_count": log.client_count,
            "throughput_average": avg_throughput,
            "throughput_median": med_throughput,
            "throughput_stddev": stddev_throughput,
            "latency_average": avg_latency,
            "latency_median": med_latency,
            "latency_stddev": stddev_latency
        }]
