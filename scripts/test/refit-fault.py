#!/usr/bin/env python3
from benchmark import main
from refit import Refit


class RefitFaulty(Refit):
    def stage_bench(self) -> bool:
        servers = self.get_servers(["replica.network.addresses"])
        server_screens = []
        for (i, server) in enumerate(servers):
            identifier = "server-{}".format(i)
            self.log("Starting {}".format(identifier))
            local_duration = self.duration
            timeout = 10
            if i == 0:
                local_duration = local_duration // 2
                timeout = 0
            cmd = self.gen_remote_java_cmd(identifier, local_duration, server, "refit.replica.REFITReplica", str(i),
                                           extra_timeout=timeout)
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

        self.log("Waiting for clients")
        self.wait_screen(5, client_screens)

        return proc is not None and proc.returncode == 0


if __name__ == '__main__':
    main(RefitFaulty)
