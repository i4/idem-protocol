#!/usr/bin/env python3
import os
import sys
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor, Future
from typing import List, Tuple, Iterable, Optional

from bench.base import AnalysisBenchmark
from bench.ping import PingBenchmark
from bench.refit import REFITBenchmark
from bench.sign_bench import SignatureBenchmark
from common.errors import printerr
from common.log import LogData
from common.statistics import median
from output.pickle import OutputPickleTransformer
from parser.base import Parser
# Mass processing:
# mv */* . && find . -iname '*.DS_Store' -delete && \
# ../heterogeneous-smr/scripts/group-results.py . . rezk \
# readwrite-0,readwrite-0.5,readwrite-1,exists,increment,queue memory,sqlite data128,data1024 && \
# find . -type d -depth 1 -print0 | \
# xargs -0 -L1 -P8 -I {} \
# bash -c 'cd {} && echo {} && ../../heterogeneous-smr/scripts/parse-results.py'
from transformer.apply_base_time import ApplyBaseTimeFilter
from transformer.base import Transformer, FatalLogIssue
from transformer.cut_log import CutLogFilter
from transformer.print_warnings import PrintWarningsFilter

AUTO_RANGE_SLACK_AT_END = 5


class LogError(Exception):
    def __init__(self, msg):
        self.msg = msg

    def __str__(self):
        return str(self.msg)


benchmarks = [
    REFITBenchmark(),
    PingBenchmark(),
    SignatureBenchmark(),
]


def walk_folders(folder_name: str, relative_time_range: Optional[Tuple[float, float]],
                 use_min_log_start: bool = False):
    # split into groups
    groups = defaultdict(lambda: [])
    for root, dirs, _ in os.walk(folder_name):
        for dirname in dirs:
            for bench in benchmarks:
                scenario, client_count = bench.parse_folder_name(dirname)
                if scenario is not None:
                    break
            else:
                printerr("Skipping folder {}".format(dirname))
                continue

            groups[(bench, scenario)].append(dirname)
        break

    if not groups:
        printerr("No results found!")
        sys.exit(1)

    # process group by group
    effective_time_range = None  # type: Optional[Tuple[float, float]]
    with ProcessPoolExecutor() as executor:
        for (bench, group), dirnames in groups.items():
            used_time_range = walk_folders_of_group(folder_name, relative_time_range, use_min_log_start,
                                                    bench, group, dirnames, executor)
            if effective_time_range is None:
                effective_time_range = used_time_range
            else:
                effective_time_range = (
                    max(used_time_range[0], effective_time_range[0]),
                    min(used_time_range[1], effective_time_range[1])
                )

    print("Recommended time range: {} {}".format(effective_time_range[0], effective_time_range[1]))


def walk_folders_of_group(folder_name: str, relative_time_range: Optional[Tuple[float, float]],
                          use_min_log_start: bool, bench: AnalysisBenchmark, group: str, dirnames: List[str],
                          executor: ProcessPoolExecutor) \
        -> Tuple[float, float]:
    ftsraw = []
    for dirname in sorted(dirnames):
        scenario, client_count = bench.parse_folder_name(dirname)
        parsers = bench.setup_parsers(scenario)
        path = os.path.join(folder_name, dirname)
        ftsraw.append((parse_logs(parsers, path, scenario, client_count, executor), path, scenario))

    fts = []
    for ftlogs, path, scenario in ftsraw:
        logs = []
        for ft in ftlogs:
            log = ft.result()
            if log is not None:
                logs.append(log)
        fts.append(
            executor.submit(range_and_transform, logs, bench, path, scenario, relative_time_range, use_min_log_start))
        ftlogs.clear()
    ftsraw.clear()

    bundle = []
    effective_time_range_inner = None
    for ft in fts:
        log_bundle, used_time_range = ft.result()
        bundle.extend(log_bundle)
        if effective_time_range_inner is None:
            effective_time_range_inner = used_time_range
        else:
            effective_time_range_inner = (
                max(used_time_range[0], effective_time_range_inner[0]),
                min(used_time_range[1], effective_time_range_inner[1])
            )
    fts.clear()

    group_transformers = bench.setup_group_transformers(group)
    group_transformers += [CutLogFilter(), OutputPickleTransformer("parsed-" + group)]
    group_transformers += bench.setup_output_transformers(group)
    for transformer in group_transformers:
        bundle = transformer.transform(bundle, effective_time_range_inner[0], effective_time_range_inner[1])
    return effective_time_range_inner


def parse_logs(parsers: Iterable[Parser], path: str, scenario: str, client_count: int,
               executor: ProcessPoolExecutor) -> "List[Future[LogData]]":
    ft = []
    for _, _, files in os.walk(path):
        for fn in files:
            log_path = os.path.join(path, fn)
            if not os.path.isfile(log_path):
                continue

            ft.append(executor.submit(_parse_single_log, parsers, fn, log_path, scenario, client_count))
    return ft


def _parse_single_log(parsers: Iterable[Parser], fn: str, log_path: str, scenario: str,
                      client_count: int) -> Optional[LogData]:
    for parser in parsers:
        if not parser.can_handle(fn):
            continue
        try:
            log = parser.parse_file(log_path)
        except:
            printerr("Error while parsing {}".format(log_path))
            raise
        log.set_scenario(scenario, client_count)
        return log
    return None


def range_and_transform(logs: List[LogData], bench: AnalysisBenchmark, path: str, scenario: str,
                        relative_time_range: Optional[Tuple[float, float]] = None,
                        use_min_log_start: bool = False) -> Tuple[Iterable[LogData], Tuple[float, float]]:
    log_base_time, auto_start_time, auto_end_time = auto_range(bench, logs, use_min_log_start)
    if relative_time_range is None:
        print("Auto range {:.3} {:.3} for {}".format(auto_start_time, auto_end_time, path))
    for log in logs:
        log.set_base_time(log_base_time)
    try:
        start_time, end_time = merge_time_range(auto_start_time, auto_end_time, relative_time_range)
    except LogError:
        print("Error while processing {}".format(path))
        raise
    # print("Using range {} {}".format(start_time, end_time))

    transformers = [ApplyBaseTimeFilter()]  # type: List[Transformer]
    transformers += bench.setup_transformers(scenario, path)
    transformers += [
        PrintWarningsFilter(),
        # OutputJSONTransformer(path),
    ]
    try:
        for transformer in transformers:
            logs = transformer.transform(logs, start_time, end_time)
    except FatalLogIssue:
        print("Error while processing {}".format(path))
        raise

    return logs, (start_time, end_time)


def auto_range(bench: AnalysisBenchmark, logs: List[LogData],
               use_min_log_start: bool = False) -> Tuple[float, float, float]:
    base = float("inf")
    start = 0
    end = -base
    merge_func = max
    if use_min_log_start:
        start = float("inf")
        merge_func = min

    for log in logs:
        if not bench.is_auto_range_log(log):
            continue

        for entry in log.entries():
            base = min(base, entry["timestamp"])
            end = max(end, entry["timestamp"])

        if log.logtype == "client":
            log_start = find_start(log)
            if log_start is None:
                raise LogError("Failed to find start in log {}".format(log.source_path))
            start = merge_func(start, log_start)
        else:
            start = merge_func(start, base + 5)

    return base, start - base, end - AUTO_RANGE_SLACK_AT_END - base


def find_start(log: LogData) -> Optional[float]:
    data = [e["throughput"] for e in log.entries() if "throughput" in e]
    if not data:
        return None
    warm_throughput = median(data) * 0.9

    for e in log.entries():
        if "throughput" not in e:
            continue
        if e["throughput"] > warm_throughput:
            return e["timestamp"]

    return None


def merge_time_range(auto_start_time: float, auto_end_time: float,
                     relative_time_range: Optional[Tuple[float, float]]) -> Tuple[float, float]:
    if relative_time_range is not None:
        start_time, end_time = relative_time_range[0], relative_time_range[1]
    else:
        start_time, end_time = auto_start_time, auto_end_time
    if not (auto_start_time <= start_time < auto_end_time and auto_start_time < end_time <= auto_end_time):
        raise LogError("Requested time interval outside auto range interval. "
                       "Auto: [{};{}[, Requested: [{};{}[".format(auto_start_time, auto_end_time,
                                                                  start_time, end_time))
    if end_time < start_time + 10:
        raise LogError("Selected time interval is less than 10 seconds: "
                       "Interval: [{};{}[".format(start_time, end_time))
    return start_time, end_time


def run():
    import argparse
    parser = argparse.ArgumentParser(description='Parse, check and process results')
    parser.add_argument('--range', type=int, nargs=2, default=None,
                        help='Use range between start and end from results', metavar=('START', 'END'))
    parser.add_argument('--min-log-start', help="Use the minimal log start as range limit instead of the maximum. "
                                                "This is useful when not all clients are active at the same time",
                        action="store_true")

    args = parser.parse_args()
    relative_time_range = args.range
    use_min_log_start = args.min_log_start
    walk_folders(".", relative_time_range, use_min_log_start)


if __name__ == '__main__':
    run()
