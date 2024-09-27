#!/usr/bin/env python3
import argparse
import time

from benchmark import main, JavaTestRun


class TestMicroBenchmark(JavaTestRun):
    DISTRIBUTE_TO_TYPES = ["replica.network.addresses"]

    def __init__(self, *args):
        super().__init__(*args)
        self.java_class = self.args.java_class
        self.duration = self.args.duration

    @classmethod
    def setup_args(cls, parser: argparse.ArgumentParser) -> None:
        parser.add_argument("java_class", help="Java class to run", type=str)
        parser.add_argument("duration", help="Duration of the test run", type=int)

    def stage_bench(self) -> bool:
        servers = self.get_unique_servers()
        for (i, server) in enumerate(servers):
            identifier = "host-{}".format(i)
            self.log("Starting {}".format(identifier))
            cmd = self.gen_remote_java_cmd(identifier, self.duration, server, self.java_class, str(self.duration))
            self.open_screen(identifier, *cmd)

        time.sleep(self.duration)
        self.wait_screen(30)
        return True


if __name__ == '__main__':
    main(TestMicroBenchmark)
