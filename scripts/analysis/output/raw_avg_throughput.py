from common.log import LogData
from .base import OutputTransformer


class OutputRawAvgThroughputTransformer(OutputTransformer):
    OUTPUT_LOGTYPE = "client-raw-avg-throughput"

    def output(self, log: LogData, start_time: float, end_time: float) -> None:
        self.writeCSV(log, "raw-avg-throughput-{}-{}.txt".format(log.scenario, log.client_count),
                      ["tag", "timestamp", "throughput", "latency"])
