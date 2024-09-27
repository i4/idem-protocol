from common.log import LogData
from .base import OutputTransformer


class OutputLogAveragesTransformer(OutputTransformer):
    OUTPUT_LOGTYPE = "client-log-averages"

    def output(self, log: LogData, start_time: float, end_time: float) -> None:
        self.writeCSV(log, "log-avgs-{}-{}.txt".format(log.scenario, log.logid),
                      ["client_count", "throughput_average", "throughput_median", "throughput_stddev",
                       "latency_average", "latency_median", "latency_stddev"])
