#!/usr/bin/env python3
import argparse
import random
import subprocess
from concurrent.futures.thread import ThreadPoolExecutor
from typing import Sequence, Tuple

from benchmark import GenericTestRun, main


class MeasureServerPings(GenericTestRun):
    DISTRIBUTE_TO_TYPES = []  # type: Sequence[str]
    SKIP_MAKE = True

    def __init__(self, *args):
        super().__init__(*args)
        self.duration = self.args.duration

    @classmethod
    def setup_args(cls, parser: argparse.ArgumentParser) -> None:
        parser.add_argument("duration", help="Duration of the test run", type=int)

    def _ping(self, src, dst, srcIdx, dstIdx) -> Tuple[str, int]:
        ssh = self.start_ssh(self.config["remote.user"], src, "ping", "-c", str(self.duration), dst,
                             stdout=subprocess.PIPE)
        try:
            (output, err) = ssh.communicate(timeout=self.duration + 15)
        except subprocess.Ti̇meoutExpired:
            ssh.kill()
            (output, err) = ssh.communicate()
        log = "=== Ping from {} to {} idx {} {} ===".format(src, dst, srcIdx, dstIdx)
        if ssh.returncode != 0:
            log += "\nMeasurement error {} {}".format(ssh.returncode, err)
            return log, ssh.returncode

        for line in output.decode("utf-8").splitlines():
            if "rtt min/avg/max/mdev" in line:
                log += "\n" + line

        return log, 0

    def stage_bench(self) -> bool:
        servers = self.get_servers(("replica.network.addresses",))

        ok = True
        with ThreadPoolExecutor(max_workers=30) as executor:
            tasks = []
            for (i, src) in enumerate(servers):
                for j in range(i + 1, len(servers)):
                    dst = servers[j]
                    tasks.append((src, dst, i, j))

            random.seed(42)
            # don't open too many ssh connections to a server at once
            random.shuffle(tasks)
            results = [executor.submit(self._ping, *t) for t in tasks]
            for res in results:
                r = res.result()
                self.log(r[0])
                if r[1] != 0:
                    ok = False

        return ok


if __name__ == '__main__':
    main(MeasureServerPings)
