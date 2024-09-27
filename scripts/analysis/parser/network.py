from common.log import LogData
from .base import BasicParser

NETWORK_IGNORE_USAGE = [
    "sshd: ubuntu@pts/"
]
NETWORK_KNOWN_USAGE = [
    "java/"
]
NETWORK_EXPECTED_PORT_RANGE = (8170, 8190)


class NetworkLogParser(BasicParser):
    LOGID_PATTERN = r"((client|server)-\d+)-network\.log"

    def _parse_file(self, path: str, logid: str) -> LogData:
        ld = LogData("network", logid, path)

        samples = []
        with open(path, "r") as f:
            net_in = 0
            net_out = 0
            before_start = True
            timestamp = None
            for line in f:
                line = line.strip()
                if not line:
                    continue

                split_line = line.split(None, 1)
                if split_line[0].isdigit():
                    timestamp = float(split_line[0])
                    # remove timestamp at line start
                    line = split_line[1]
                # keep last timestamp when encountering a line without

                if line == "Refreshing:":
                    if before_start:
                        before_start = False
                    else:
                        samples.append((timestamp, net_in, net_out))
                        net_in = 0
                        net_out = 0

                if line == "Refreshing:" or not line or before_start:
                    continue

                parts = line.split("\t")
                who = parts[0]
                if self.is_ignored_usage(who):
                    continue

                entry_out = int(float(parts[1]))
                entry_in = int(float(parts[2]))

                if entry_out == 0 and entry_in == 0:
                    continue

                if not self.is_known_usage(who):
                    print("Unknown network usage by {}".format(who))

                net_in += entry_in
                net_out += entry_out

        if not before_start and timestamp is not None:
            samples.append((timestamp, net_in, net_out))

        for (t, inb, outb) in self.convert_to_usage_deltas(samples):
            ld.add_data(t, rx_bytes=inb, tx_bytes=outb)

        return ld

    @staticmethod
    def is_ignored_usage(who):
        for prefix in NETWORK_IGNORE_USAGE:
            if who.startswith(prefix):
                return True
        return False

    @staticmethod
    def is_known_usage(who):
        is_known = False
        for prefix in NETWORK_KNOWN_USAGE:
            if who.startswith(prefix):
                is_known = True
                break
        else:
            # look for first port in e.g. "10.0.1.68:8172-54.183.253.217:36366/0/0"
            addrs = who.split("/")[0].split("-")
            for addr in addrs:
                portstr = addr.split(":", 1)[-1]
                if not portstr.isdigit():
                    continue

                port = int(portstr)
                if NETWORK_EXPECTED_PORT_RANGE[0] <= port < NETWORK_EXPECTED_PORT_RANGE[1]:
                    is_known = True
        return is_known

    @staticmethod
    def convert_to_usage_deltas(samples):
        last = None
        for sample in samples:
            if last is None:
                diff = sample
            else:
                diff = (sample[0], sample[1] - last[1], sample[2] - last[2])
            last = sample
            yield diff
