from common.log import LogData
from .base import OutputTransformer


class OutputRawAveragesTransformer(OutputTransformer):
    OUTPUT_LOGTYPE = "client-raw-averages"

    def output(self, log: LogData, start_time: float, end_time: float) -> None:
        self.writeCSV(log, "raw-averages-{}.txt".format(log.scenario),
                      ["client_count", "tag", "throughput", "latency"])
