from collections import defaultdict
from typing import Iterable, Dict

from common.log import LogData
from transformer.base import Transformer


class ClientRawThroughputTransformer(Transformer):
    def transform(self, logs: Iterable[LogData], start_time: float, end_time: float) -> Iterable[LogData]:
        logs = [log for log in logs]

        for log in logs:
            if log.logtype != "latency":
                continue

            ld = log.derive("client-raw-throughput")
            entries = self._average(log, start_time, end_time)
            for e in entries:
                ld.add_entry(e)

            logs.append(ld)
        return logs

    @staticmethod
    def _average(log: LogData, start_time: float, end_time: float) -> Iterable[Dict]:
        seen_client = set()

        # throughput
        throughput_data = defaultdict(lambda: defaultdict(lambda: 0))
        # latency
        latencies = defaultdict(lambda: defaultdict(lambda: 0))

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
                latencies[tag][time] += e["latency"]

        out = []
        for tag in throughput_data.keys():
            for i in range(int_start_time, int_end_time):
                latency = 0
                if throughput_data[tag][i] > 0:
                    latency = latencies[tag][i] / throughput_data[tag][i]

                p = {
                    "timestamp": i,
                    "tag": tag,
                    "throughput": throughput_data[tag][i],
                    "latency": latency,
                }
                out.append(p)
        return out
