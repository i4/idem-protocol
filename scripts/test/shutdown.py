#!/usr/bin/env python3
import argparse
from typing import Sequence

from benchmark import GenericTestRun, main


class Shutdown(GenericTestRun):
    DISTRIBUTE_TO_TYPES = []  # type: Sequence[str]

    @classmethod
    def setup_args(cls, parser: argparse.ArgumentParser) -> None:
        parser.add_argument("duration", help="Duration of the test run", type=int)

    def stage_distr(self) -> bool:
        return True

    def stage_bench(self) -> bool:
        servers = self.get_servers(("client.network.addresses", "replica.network.addresses"))
        shutdown = [self.start_ssh(self.config["remote.user"], server,
                                   "sudo", "shutdown", "now") for server in servers]
        return self.wait_procs(shutdown, kill_on_error=False)

    def stage_store(self) -> bool:
        return True


if __name__ == '__main__':
    main(Shutdown)
