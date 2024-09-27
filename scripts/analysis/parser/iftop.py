import ipaddress
import os
import socket
from collections import defaultdict

from common.log import LogData
from .base import BasicParser


class IftopLogParser(BasicParser):
    LOGID_PATTERN = r"((client|server)-\d+)-iftop\.log"

    def _parse_file(self, path: str, logid: str) -> LogData:
        ld = LogData("iftop", logid, path)

        # line sample:
        #                                                                  2 seconds
        # 1575912971    1 10.0.2.209                               =>        60B        60B        60B       120B
        # 1575912971      10.0.3.171                               <=        40B        40B        40B        80B

        samples = defaultdict(lambda: {})
        servers = set()
        timestamps = set()

        name_map = {}
        with open(os.path.join(os.path.dirname(path), "servers"), "r") as f:
            for line in f:
                if line.startswith("#"):
                    continue
                parts = [p.strip() for p in line.strip().split("=")]
                if len(parts) == 2:
                    try:
                        # throws an value error if not a valid ip address
                        ipaddress.ip_address(parts[1])
                    except ValueError:
                        try:
                            # try to resolve the address
                            parts[1] = socket.gethostbyname(parts[1])
                        except socket.gaierror:
                            # nothing we can do
                            pass

                    if parts[1] not in name_map:
                        name_map[parts[1]] = parts[0]

        with open(path, "r") as f:
            before_start = True
            after_end = False
            at_out = True
            # use time at the start of a new block
            current_time = None

            for line in f:
                line = line.strip()
                if line.endswith(
                        "--------------------------------------------------------------------------------------------"):
                    if before_start:
                        before_start = False
                        at_out = True
                        cat_in = defaultdict(lambda: 0)
                        cat_out = defaultdict(lambda: 0)
                        lp = line.split()
                        current_time = float(lp[0])
                        continue
                    elif not after_end:
                        for dst in cat_in.keys():
                            timestamps.add(current_time - 1)
                            timestamps.add(current_time)
                            servers.add(dst)

                            inb = cat_in[dst]
                            outb = cat_out[dst]
                            # print(current_time, start_time)
                            samples[current_time - 1][dst] = (inb, outb)
                            samples[current_time][dst] = (inb, outb)
                        after_end = True
                elif line.endswith(
                        "============================================================================================"):
                    before_start = True
                    after_end = False

                if before_start or after_end:
                    continue

                lp = line.split()
                if len(lp) == 1:
                    continue

                # out and in are always printed in pairs
                if at_out:
                    outb = self.parse_iftop_number(lp[4])
                    at_out = False
                else:
                    inb = self.parse_iftop_number(lp[3])
                    dst = lp[1]
                    at_out = True

                    # map name if known
                    if dst in name_map:
                        dst = name_map[dst]

                    cat_in[dst] += inb
                    cat_out[dst] += outb

        for t in timestamps:
            for dst in servers:
                if dst not in samples[t]:
                    # no output means no traffic, fill up missing bits
                    samples[t][dst] = (0, 0)

        for t in sorted(timestamps):
            for dst in sorted(servers):
                val = samples[t][dst]
                ld.add_data(t, destination=dst, rx_bytes=val[0], tx_bytes=val[1])

        return ld

    @staticmethod
    def parse_iftop_number(num):
        # num = 40B ; 50.1KB ; 2.25MB
        # cut of B(yte)
        num = num[:-1]
        multiplier = 1
        if num[-1] == "K":
            multiplier = 1024
        elif num[-1] == "M":
            multiplier = 1024 * 1024
        if multiplier != 1:
            # cut of multiplier
            num = num[:-1]

        return float(num) * multiplier
