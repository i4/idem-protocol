from common.log import LogData
from .base import OutputTransformer


class OutputLogThroughputTransformer(OutputTransformer):
    OUTPUT_LOGTYPE = "client"

    def output(self, log: LogData, start_time: float, end_time: float) -> None:
        self.writeCSV(log, "log-throughput-{}-{}-{}.txt".format(log.scenario, log.logid, log.client_count),
                      ["timestamp", "throughput", "latency", "latency_min", "latency_max"],
                      value_filter=lambda e: "throughput" in e)
