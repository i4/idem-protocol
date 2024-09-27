import os

from common.log import LogData
from transformer.base import FatalLogIssue
from .base import Parser


class PingLogParser(Parser):
    def can_handle(self, fn: str) -> bool:
        return fn == "test.log"

    def parse_file(self, path: str) -> LogData:
        ld = LogData("ping", "ping", path)
        timestamp = 42.
        # fake entry to make auto range happy
        ld.add_data(timestamp - 10)

        name_map = {}
        loc_map = {}
        with open(os.path.join(os.path.dirname(path), "servers"), "r") as f:
            for line in f:
                if line.startswith("#"):
                    continue
                parts = [p.strip() for p in line.strip().split("=")]
                if len(parts) == 2:
                    name_map[parts[1]] = parts[0]
                    if parts[0].endswith("loc"):
                        loc_map[parts[0][:-3]] = int(parts[1])

        who = None
        with open(path, "r") as f:
            started = False
            for line in f:
                parts = line.strip().split()
                if parts[0] == "[BENCH]":
                    parts = parts[1:]

                if parts[0] == "===":
                    started = True
                    who = (name_map[parts[3]], name_map[parts[5]])
                elif parts[0] == "rtt":
                    if who is None:
                        raise FatalLogIssue("Broken logfile " + path)

                    times = parts[3].split("/")

                    ld.add_data(timestamp, src=who[0], dst=who[1], srcLoc=loc_map.get(who[0], -1),
                                dstLoc=loc_map.get(who[1], -1), ping=float(times[1]), variance=float(times[3]))
                    if float(times[3]) > 5:
                        ld.add_warning(timestamp, "High variance between {}".format(who))

                    who = None
                elif len(parts) == 3 and parts[1] == "Complete":
                    started = False
                elif started:
                    ld.add_warning(timestamp, "Unknown line " + line)

        ld.add_data(timestamp + 10)
        return ld
