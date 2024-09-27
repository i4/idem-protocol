from common.errors import printerr
from common.log import LogData
from transformer.base import Filter


class PrintWarningsFilter(Filter):
    def can_handle(self, log: LogData) -> bool:
        return True

    def filter(self, log: LogData, start_time: float, end_time: float) -> LogData:
        ctr = 0
        for entry in log.entries():
            if "warning" not in entry:
                continue
            ctr += 1
            if ctr == 1:
                printerr("=== ", log.source_path, log.logtype, log.logid)
            if ctr <= 10:
                printerr('> ', "{:10.6f}".format(entry["timestamp"]), entry["warning"])
            elif ctr == 11:
                printerr("... Only showing the first 10 errors")

        return log
