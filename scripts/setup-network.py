#!/usr/bin/python3
"""
Setup individual network namespaces and use netem to inject latencies between them

Expects a server configuration configured with the following entries. For each defined
server name there must be a variant suffixed as "real" specifying the ip/host name useable
for ssh and "loc" to specify the location. Then it setups the latencies between IPs of
different locations according to the latency_*_* lines.

server0 = 10.188.44.100
server0real = refit15.cs.fau.de
server0loc = 0

latency_0_1 = ...
...

"""

import json
import socket
import subprocess
from collections import defaultdict
from configparser import ConfigParser
from typing import Dict, Tuple, Set


def setup_bridge(myip):
    bridge_name = "br1"
    if subprocess.run(["ip", "link", "show", bridge_name], capture_output=True).returncode == 0:
        # bridge already exists
        return

    router_ip = "10.188.42.1"

    eth0 = json.loads(
        subprocess.run(["ip", "-j", "link", "show", "eth0"], check=True, capture_output=True).stdout.decode())[0]

    cmds = [
        # attach eth0 to bridge
        ["ip", "link", "add", bridge_name, "address", eth0["address"], "type", "bridge"],
        ["ip", "link", "set", "dev", "eth0", "master", bridge_name],
        ["ip", "a", "d", "{}/24".format(myip), "dev", "eth0"],
        ["ip", "route", "del", "default", "via", router_ip, "dev", "eth0"],
        # move ip to bridge
        ["ip", "a", "a", "{}/24".format(myip), "dev", bridge_name],
        ["ip", "link", "set", bridge_name, "up"],
        ["ip", "route", "add", "default", "via", router_ip, "dev", bridge_name],
        # allow routing to veths
        ["iptables", "-A", "FORWARD", "-i", bridge_name, "-j", "ACCEPT"],
        ["iptables", "-A", "FORWARD", "-o", bridge_name, "-j", "ACCEPT"],
    ]

    for cmd in cmds:
        r = subprocess.run(cmd, capture_output=True)
        print(r)


def setup_addrs(addrs: Dict[str, str]):
    for k, ip in addrs.items():
        netname = "refit-{}".format(k)
        vethname = "veth{}".format(k)

        if subprocess.run(["ip", "netns", "pids", netname], capture_output=True).returncode == 0:
            # network namespace already exists
            continue

        cmds = [
            ["ip", "netns", "add", netname],
            ["ip", "link", "add", vethname, "master", "br1", "type", "veth", "peer", "name", vethname, "netns",
             netname],
            ["ip", "link", "set", vethname, "up"],
            ["ip", "netns", "exec", netname, "bash", "-c",
             "ip a add {0}/24 dev {1}; ip link set dev {1} up; ip link set dev lo up".format(ip, vethname)],
        ]

        for cmd in cmds:
            r = subprocess.run(cmd, capture_output=True, check=True)
            print(r)


def setup_latencies(addrs: Dict[str, str], ips_per_loc: Dict[int, Set[str]], latencies: Dict[Tuple[int, int], float]):
    for k, ip in addrs.items():
        netname = "refit-{}".format(k)
        vethname = "veth{}".format(k)
        for l, ips in ips_per_loc.items():
            if ip in ips:
                myloc = l
                break
        else:
            raise AssertionError("no location found for {}".format(ip))

        # cleanup
        subprocess.run(["ip", "netns", "exec", netname, "bash", "-c", "tc qdisc del dev {} root".format(vethname)],
                       capture_output=True)

        cmds = [
            # prio by default maps traffic to bands 1-3 based on the packet priority header
            # leave these classes untouched and map our extra traffic to the upper classes
            "tc qdisc del dev {0} root; tc qdisc add dev {0} root handle 1: prio bands 7;"
            " tc qdisc add dev {0} parent 1:1 sfq; tc qdisc add dev {0} parent 1:2 sfq;"
            " tc qdisc add dev {0} parent 1:3 sfq".format(vethname),
        ]
        for i in range(4):
            cmds.append("tc qdisc add dev {} parent 1:{} netem delay {}ms".format(vethname, 4 + i, latencies[myloc, i]))
            for dst in ips_per_loc[i]:
                cmds.append(
                    "tc filter add dev {} protocol ip parent 1:"
                    " prio 1 u32 match ip dst {} flowid 1:{}".format(vethname, dst, 4 + i))

        for cmd in cmds:
            r = subprocess.run(["ip", "netns", "exec", netname, "bash", "-c", cmd], capture_output=True, check=True)
            print(r)
        setup_tcp_buffers(netname)


def setup_tcp_buffers(netname: str):
    cmds = [
        ["sysctl", "-w", "net.ipv4.tcp_slow_start_after_idle=0"],
        ["sysctl", "-w", "net.ipv4.tcp_rmem='4096 87380 33554432'"],
        ["sysctl", "-w", "net.ipv4.tcp_wmem='4096 65536 33554432'"],
    ]
    for cmd in cmds:
        r = subprocess.run(["ip", "netns", "exec", netname, "bash", "-c"] + [" ".join(cmd)],
                           capture_output=True, check=True)
        print(r)


def setup_tcp_buffers_global():
    cmds = [
        ["sysctl", "-w", "net.core.rmem_max=33554432"],
        ["sysctl", "-w", "net.core.wmem_max=33554432"],
    ]
    for cmd in cmds:
        r = subprocess.run(cmd, capture_output=True, check=True)
        print(r)


def parse(fn: str) -> dict:
    config = {}
    with open(fn, "r") as f:
        data = f.read()

    cp = ConfigParser()
    cp.read_string("[DEFAULT]\n" + data)
    config.update(cp["DEFAULT"])
    return config


def to_ip(addr: str) -> str:
    try:
        # try to resolve the address
        addr = socket.gethostbyname(addr)
    except socket.gaierror:
        # nothing we can do
        pass
    return addr


def load_addrs(myip: str, server_fn: str) -> Dict[str, str]:
    conf = parse(server_fn)

    my_servers = []
    for k in conf.keys():
        if k.endswith("real") and to_ip(conf[k]) == myip:
            my_servers.append(k[:-4])

    my_addrs = {}
    for s in my_servers:
        my_addrs[s] = conf[s]

    print(my_addrs)
    return my_addrs


def load_locations(server_fn: str) -> Tuple[Dict[int, Set[str]], Dict[Tuple[int, int], float]]:
    conf = parse(server_fn)

    ips_per_loc = defaultdict(lambda: set())
    for k in conf.keys():
        if k.endswith("loc"):
            loc = int(conf[k])
            if loc == 4:
                # spider hack
                loc = 0
                print("mapping location 4 to 0")
            elif loc > 4 or loc < 0:
                raise AssertionError("not supported")
            ips_per_loc[loc].add(conf[k[:-3]])

    latencies = {}  # type: Dict[Tuple[int,int], float]
    for loc in ips_per_loc.keys():
        for loc2 in ips_per_loc.keys():
            if loc2 <= loc:
                continue
            latencies[(loc, loc2)] = float(conf["latency_{}_{}".format(loc, loc2)])
            latencies[(loc2, loc)] = latencies[(loc, loc2)]
        latencies[(loc, loc)] = 0.2

    return ips_per_loc, latencies


def get_my_ip() -> str:
    return [p for p in subprocess.run(["hostname", "-I"], check=True, capture_output=True).stdout.decode().split() if
            p.startswith("10.188.42.")][0]


def fix_network_card_performance():
    # the refit15-18 hosts have Intel I219-LM network chips
    # according to https://docs.hetzner.com/robot/dedicated-server/troubleshooting/performance-intel-i218-nic/
    # the send performance is limited since Linux 4.15 to bugfix network card hangs
    # Disable the hardware offload to get full network throughput
    print(subprocess.run(["ethtool", "-K", "eth0", "tso", "off", "gso", "off"], capture_output=True, check=True))


def configure_cpu_sched():
    print(subprocess.run(["cpupower", "frequency-set", "-g", "performance"], capture_output=True, check=True))
    with open("/sys/devices/system/cpu/intel_pstate/no_turbo", 'wb') as f:
        f.write(b"1")


def init():
    import sys
    server_fn = sys.argv[1]

    mi = get_my_ip()
    ld = load_addrs(mi, server_fn)
    setup_bridge(mi)
    setup_addrs(ld)
    ips, lat = load_locations(server_fn)
    setup_latencies(ld, ips, lat)
    # setup_tcp_buffers() has to be called for each namespace
    setup_tcp_buffers_global()
    fix_network_card_performance()
    configure_cpu_sched()


if __name__ == "__main__":
    init()
