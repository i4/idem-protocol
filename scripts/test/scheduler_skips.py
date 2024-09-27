#!/usr/bin/env python3
import argparse

from benchmark import main, JavaTestRun


class SchedulerSkips(JavaTestRun):
    DISTRIBUTE_TO_TYPES = ["client.network.addresses"]

    def __init__(self, *args):
        super().__init__(*args)
        self.duration = self.args.duration

    @classmethod
    def setup_args(cls, parser: argparse.ArgumentParser) -> None:
        parser.add_argument("duration", help="Duration of the test run", type=int)

    def stage_bench(self) -> bool:
        client = self.get_servers(["client.network.addresses"])[0]
        cmd = self.gen_remote_java_cmd("client", self.duration, client,
                                       "refit.test.SchedulingMicrobenchmark", str(self.duration))
        self.run_blocking(*cmd)

        return True


if __name__ == '__main__':
    main(SchedulerSkips)
