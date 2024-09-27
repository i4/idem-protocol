#!/usr/bin/env python3
import argparse
import time

from benchmark import main, JavaTestRun


class TestPing(JavaTestRun):
    DISTRIBUTE_TO_TYPES = ["replica.network.addresses"]

    def __init__(self, *args):
        super().__init__(*args)
        self.duration = self.args.duration

    @classmethod
    def setup_args(cls, parser: argparse.ArgumentParser) -> None:
        parser.add_argument("duration", help="Duration of the test run", type=int)

    def stage_bench(self) -> bool:
        servers = self.get_servers(["replica.network.addresses"])
        server_screens = []
        for (i, server) in enumerate(servers):
            identifier = "server-{}".format(i)
            self.log("Starting {}".format(identifier))
            cmd = self.gen_remote_java_cmd(identifier, self.duration, server, "refit.test.ping.REFITPingReplica",
                                           str(i))
            self.open_screen(identifier, *cmd)
            server_screens.append(identifier)

        time.sleep(self.duration)
        return True


if __name__ == '__main__':
    main(TestPing)
