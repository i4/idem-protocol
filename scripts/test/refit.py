#!/usr/bin/env python3
import argparse

from benchmark import main, JavaTestRun


class Refit(JavaTestRun):
    DISTRIBUTE_TO_TYPES = ["client.network.addresses", "replica.network.addresses"]

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
            cmd = self.gen_remote_java_cmd(identifier, self.duration, server, "refit.replica.REFITReplicaRunner",
                                           str(i))
            self.open_screen(identifier, *cmd)
            server_screens.append(identifier)

        clients = self.get_servers(["client.network.addresses"])
        client_screens = []
        proc = None
        for i in range(len(clients) - 1, -1, -1):
            client = clients[i]
            identifier = "client-{}".format(i)
            cmd = self.gen_remote_java_cmd(identifier, self.duration, client, "refit.client.REFITBenchmark",
                                           str(self.duration), str(i))
            if i > 0:
                self.open_screen(identifier, *cmd)
                client_screens.append(identifier)
            else:
                proc = self.run_blocking(*cmd)
                print("Main client finished with return code {}".format(proc.returncode))
        if len(clients) == 0:
            # just sleep if no clients are required
            cmd = ["sleep", str(self.duration)]
            proc = self.run_blocking(*cmd)
            print("Main client finished with return code {}".format(proc.returncode))

        self.log("Waiting for clients")
        self.wait_screen(10, client_screens)

        return proc is not None and proc.returncode == 0


if __name__ == '__main__':
    main(Refit)
