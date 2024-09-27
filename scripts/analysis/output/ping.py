from collections import defaultdict

from common.log import LogData
from .base import OutputTransformer


class OutputPingTransformer(OutputTransformer):
    OUTPUT_LOGTYPE = "ping"

    def output(self, log: LogData, start_time: float, end_time: float) -> None:
        lines = []
        for e in log.entries():
            if "ping" in e:
                lines.append("PING_TIME_MAP['{}']['{}'] = {}\n".format(e["src"], e["dst"], e["ping"]))
        lines.sort()
        with open("ping-{}.txt".format(log.scenario), "w") as f:
            for l in lines:
                f.write(l)

        lines = []
        for e in log.entries():
            if "ping" in e:
                lines.append("latency_{}_{} = {:.2f}\n".format(e["srcLoc"], e["dstLoc"], e["ping"] / 2))
        lines.sort()
        with open("ping-locs-{}.txt".format(log.scenario), "w") as f:
            for l in lines:
                f.write(l)

        avgs = defaultdict(lambda: [0, 0])
        for e in log.entries():
            if "ping" in e:
                k = "ping_{}_{}".format(e["srcLoc"], e["dstLoc"])
                avgs[k][0] += e["ping"]
                avgs[k][1] += 1
        with open("ping-avg-locs-{}.txt".format(log.scenario), "w") as f:
            for k in sorted(avgs.keys()):
                val = avgs[k]
                f.write("{} = {:.2f}\n".format(k, val[0] / val[1]))

        with open("fake-network-{}.sh".format(log.scenario), "w") as f:
            # update hosts ips
            hosts = ["refit15", "refit16", "refit17", "refit18"]
            f.write("""#!/bin/bash
hosts=( """ + " ".join(hosts) + """ )
hostip=( 10.188.42.52 10.188.42.53 10.188.42.54 10.188.42.55 )
# cleanup
tc qdisc del dev eth0 root
# prio by default maps traffic to bands 1-3 based on the packet priority header
# leave these classes untouched and map our extra traffic to the upper classes
tc qdisc add dev eth0 root handle 1: prio bands """ + str(len(hosts) + 2) + """
tc qdisc add dev eth0 parent 1:1 sfq
tc qdisc add dev eth0 parent 1:2 sfq
tc qdisc add dev eth0 parent 1:3 sfq
""")
            for i, host in enumerate(hosts):
                f.write("if [[ $(hostname) == ${{hosts[{}]}} ]]; then\n".format(i))
                ctr = 3
                for j, other in enumerate(hosts):
                    if i == j:
                        continue
                    ctr += 1
                    avg_key = "ping_{}_{}".format(min(i, j), max(i, j))
                    v = avgs[avg_key]
                    f.write(
                        "\ttc qdisc add dev eth0 parent 1:{} netem delay {:.2f}ms\n".format(ctr, v[0] / v[1] / 2))
                    # add filters to the prio qdisc, flowid is the id where the netems are attached
                    # u32 == generic matching
                    # Rules are grouped by priority, lower priorities are matched first
                    f.write(
                        "\ttc filter add dev eth0 protocol ip parent 1: prio 1 "
                        "u32 match ip dst ${{hostip[{}]}} flowid 1:{}\n".format(j, ctr))
                f.write("el")
            f.write("""se\n\techo "unknown hostname"\n\texit 1\nfi\n""")

        with open("servers-fake-{}".format(log.scenario), "w") as f:
            for i, host in enumerate(hosts):
                for j, other in enumerate(hosts):
                    if i >= j:
                        continue
                    avg_key = "ping_{}_{}".format(min(i, j), max(i, j))
                    v = avgs[avg_key]
                    f.write("latency_{}_{} = {:.2f}\n".format(i, j, v[0] / v[1] / 2))
