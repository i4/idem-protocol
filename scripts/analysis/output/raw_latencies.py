from common.log import LogData
from .base import OutputTransformer


class OutputRawLatenciesTransformer(OutputTransformer):
    OUTPUT_LOGTYPE = "client-raw-latencies"

    def output(self, log: LogData, start_time: float, end_time: float) -> None:
        self.writeCSV(log, "raw-latencies-{}-{}.txt".format(log.scenario, log.logid),
                      ["client_count", "tag", "throughput_average", "throughput_median",
                       "latency_average", "latency_50", "latency_90", "latency_99"])
