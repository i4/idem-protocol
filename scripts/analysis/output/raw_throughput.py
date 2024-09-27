from common.log import LogData
from .base import OutputTransformer


class OutputRawThroughputTransformer(OutputTransformer):
    OUTPUT_LOGTYPE = "client-raw-throughput"

    def output(self, log: LogData, start_time: float, end_time: float) -> None:
        self.writeCSV(log, "raw-throughput-{}-{}-{}.txt".format(log.scenario, log.logid, log.client_count),
                      ["tag", "timestamp", "throughput", "latency"])
