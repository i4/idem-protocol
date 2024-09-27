from common.log import LogData
from .base import Filter


class ServerChannelLogFilter(Filter):
    def can_handle(self, log: LogData) -> bool:
        return log.logtype == "server" and "channel-micro" in log.scenario \
               and log.logid in ("server-4", "server-5", "server-6")

    def filter(self, log: LogData, start_time: float, end_time: float) -> LogData:
        out = log.derive()
        first = None
        last = None

        for entry in log.entries():
            if "warning" in entry:
                line = entry["warning"]
                if "CMIC-DST:" in line:
                    p = line.split(" ")
                    messages = int(p[-1])
                    entry = out.add_data(entry["timestamp"], messages=messages)

                    if first is None and entry["timestamp"] >= start_time:
                        first = entry
                    if entry["timestamp"] < end_time:
                        last = entry

            out.add_entry(entry)

        message_delta = 0
        time_delta = 0.
        throughput = 0.

        if last is not None and first is not None and last["timestamp"] - first["timestamp"] > 10:
            time_delta = last["timestamp"] - first["timestamp"]
            message_delta = last["messages"] - first["messages"]
            throughput = message_delta / time_delta

        out.add_data((start_time + end_time) / 2, message_delta=message_delta, time_delta=time_delta,
                     throughput=throughput)
        return out
