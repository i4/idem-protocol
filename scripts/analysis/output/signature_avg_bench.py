from common.log import LogData
from .base import OutputTransformer


class OutputSignatureAvgBenchTransformer(OutputTransformer):
    OUTPUT_LOGTYPE = "signature-averages"

    def output(self, log: LogData, start_time: float, end_time: float) -> None:
        self.writeCSV(log, "avg-signatures-{}-{}.txt".format(log.scenario, log.logid),
                      ["operation", "count", "median", "lower_quartile", "upper_quartile"])
