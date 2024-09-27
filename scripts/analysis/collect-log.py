#!/usr/bin/env python3

import re
import sys
from collections import OrderedDict
from typing import Sequence, Dict, Any, List, Tuple

from common.log import load_pickle_logs
from common.plot import regroup_and_print


def load_data() -> Dict[Any, dict]:
    log_bundles = load_pickle_logs()
    results = {}

    for bundle in log_bundles:
        for log in bundle:
            if log.logtype != "client-raw-latencies":
                continue

            basekey = parse_scenario(log.logid, log.scenario)
            for e in log.entries():
                key = (basekey[0], basekey[1], e["tag"])
                # print(log.scenario, log.logid, key)
                if key in results:
                    print(key)
                    print(results.keys())
                    raise ValueError("Unexpected duplicates!")

                results[key] = e

    return results


def parse_scenario(logid: str, scenario: str) -> Sequence[str]:
    client = -1
    if logid.startswith("client-"):
        client = int(logid[7:])

    locations = None
    parts = scenario.split("-")
    matches = 0
    system_parts = []
    for p in parts:
        matches += 1
        if re.match(r"^c\d+$", p):
            locations = p
        else:
            matches -= 1

        if matches == 0:
            system_parts.append(p)

    system = "-".join(system_parts)
    return locations, system, client


def collect_results(results: Dict[Any, dict], offset: int) -> None:
    # print(sorted(results.keys()))

    fields = ["latency_90", "latency_50"]
    locations = ["c00", "c11", "c22"]
    systems = OrderedDict([("mencius-regular", "bft"), ("mencius-fast", "steward"),
                           ("metro-fast", "spider")])  # type: Dict[str, str]
    tags = OrderedDict([("a", "1"), ("r", "2"), ("a+r", "3"), ("gc", "4")])  # type: Dict[str, str]

    for field in fields:
        pattern = ""
        if field == fields[0]:
            pattern = ", postaction={pattern=crosshatch, pattern color=black}"

        groups = []

        for xlocation in locations:
            group = []
            groups.append(group)
            first = True
            for xsystem, color_prefix in systems.items():
                if not first:
                    group.append(None)
                first = False

                for xtag, color_suffix in tags.items():
                    key = (xlocation, xsystem, xtag)
                    e = results[key]
                    color = color_prefix + color_suffix
                    name = ""
                    if field == fields[0]:
                        name = " {}".format(xtag.upper())

                    group.append((color, name, str(e[field])))

        regroup_and_print(groups, pattern, offset)


def main():
    offset = 0
    if len(sys.argv) > 1:
        offset = int(sys.argv[1])

    print("Using offset {}".format(offset))
    data = load_data()
    collect_results(data, offset)


if __name__ == '__main__':
    main()
