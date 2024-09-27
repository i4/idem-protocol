from common.log import LogData
from .base import OutputTransformer


class OutputSignatureBenchTransformer(OutputTransformer):
    OUTPUT_LOGTYPE = "signature"

    def output(self, log: LogData, start_time: float, end_time: float) -> None:
        for tpe in ("sign", "verify"):
            self.writeCSV(log, "raw-signatures-{}-{}-{}.txt".format(log.scenario, tpe, log.logid),
                          ["operation", "duration", "count"],
                          value_filter=lambda e: e["operation"] == tpe)
