#!/usr/bin/env python3
from typing import Tuple, Iterable, Optional, List

from bench.base import AnalysisBenchmark
from common.log import LogData
from output.log_averages import OutputLogAveragesTransformer
from output.log_throughput import OutputLogThroughputTransformer
from output.raw_averages import OutputRawAveragesTransformer
from output.raw_avg_throughput import OutputRawAvgThroughputTransformer
from output.raw_latencies import OutputRawLatenciesTransformer
from output.raw_throughput import OutputRawThroughputTransformer
from parser.base import Parser
from parser.client import ClientLogParser
from parser.iftop import IftopLogParser
from parser.latency import LatencyLogParser
from parser.network import NetworkLogParser
from parser.resources import ResourcesLogParser
from parser.server import ServerLogParser
from parser.ycsb_raw import YCSBRawLogParser
from transformer.base import Transformer, Rule
from transformer.client import ClientLogFilter
from transformer.client_log_averages import ClientLogAveragesTransformer
from transformer.client_raw_averages import ClientRawAveragesTransformer
from transformer.client_raw_avg_throughput import ClientRawAvgThroughputTransformer
from transformer.client_raw_latencies import ClientRawLatenciesTransformer
from transformer.client_raw_throughput import ClientRawThroughputTransformer
from transformer.latency import LatencyLogFilter
from transformer.resources import ResourceLogFilter
from transformer.server import ServerLogFilter
from transformer.server_channel import ServerChannelLogFilter


class REFITBenchmark(AnalysisBenchmark):
    def parse_folder_name(self, name: str) -> Tuple[Optional[str], int]:
        name_chunks = name.split("-refit-", 1)
        if len(name_chunks) < 2:
            return None, 0

        # expected structure: <scenario>-<clients>-<duration>
        # but also accept <scenario>-<clients>-<scenario2>-<duration>
        chunks = name_chunks[1].split("-")[:-1]
        client_count = 0
        client_idx = -1
        for i in reversed(range(len(chunks))):
            try:
                client_count = int(chunks[i])
                client_idx = i
                break
            except ValueError:
                pass

        if client_idx >= 0:
            del chunks[client_idx]
        scenario = "-".join(chunks)

        return scenario, client_count

    def setup_parsers(self, scenario: str) -> Iterable[Parser]:
        return [
            ClientLogParser(),
            IftopLogParser(),
            LatencyLogParser(),
            NetworkLogParser(),
            ResourcesLogParser(),
            ServerLogParser(),
            YCSBRawLogParser(),
        ]

    def is_auto_range_log(self, log: LogData) -> bool:
        if "channel-micro" in log.scenario:
            return log.logtype == "server"
        return log.logtype == "client"

    def setup_transformers(self, scenario: str, path: str) -> List[Transformer]:
        client_pattern = r"CLNT\d+-1?\d "
        client_ignores = [
            Rule(r"(BENCH\d+|CLNT\d-\d) \[EVENT\] BENCH: OK", 1),
            Rule(r"(BENCH\d+|CLNT\d-\d) \[EVENT\] BENCH: Start time: [0-9]+.[0-9]+s", 1),
            Rule(client_pattern + r"\[WARNG\] LIB(BYZ|SPD|STW)\[\d+\]: retry \{REQUEST\|\d+\|\[\d+\-\d+\]\} "
                                  r"\(replies from \[[\d,]*\]\)", 0, True),
            Rule(client_pattern + r"\[WARNG\] RPNET: Reconnecting to \d+@.+: connection failed: "
                                  r"java.net.ConnectException: Connection refused", 0, True, True),
            Rule(client_pattern + r"\[WARNG\] RPNET: Lost connection to \d+@.+ by \d+", 0, True, True),
            Rule(client_pattern + r"\[WARNG\] RPNET: Reconnecting to \d+@.+ by \d+: "
                                  r"connection (was )?closed", 0, True, True),
            Rule(client_pattern + r"\[WARNG\] RPNET: Reconnecting to \d+@.+ by \d+: "
                                  r"timeout", 0, True, True),
            Rule(r"hang \[ERROR\] HangDetector: Detected hang of " + client_pattern + r"for 1?\d\d\d\d\d\dus", 0, True),
            Rule(client_pattern + r"\[EVENT\] CONWK\d+: connected to \d+@[^:]+:\d+ by \d+ via CNCTN\[.+", 0, True),
        ]
        if "fuzzy" in scenario:
            client_ignores.append(Rule(r"\[\d+\.\d\d\ds\]\[info\]\[gc\] GC\(\d+\) Pause Young \(Normal\) "
                                       r"\(G1 Evacuation Pause\) \d+M->\d+M\(\d+M\) \d?\d?\d.\d\d\dms", float("inf")))
        if "weave-log" in scenario:
            client_ignores.append(Rule(r"BENCH\d+ \[WARNG\] LOGC-0: Got 1?[0-9] entries; base \d+", float("inf")))
        if "omada-readonly-ratio" in scenario:
            client_ignores += [
                Rule(client_pattern + r"\[WARNG\] \[REPLY\]: received different results for request \[\d+-\d+\]",
                     float("inf")),
                Rule(client_pattern + r"\[WARNG\] LIBBYZ\[\d+\]: Failed receiving reply to read-only request "
                                      r"\{REQUEST\|\d+\|\[\d+-\d+\]\}, resending as read-write", float("inf")),
            ]
        if "ycsb" in scenario:
            client_ignores += [
                Rule(r"Command line: -threads \d+ -P workloads/workload. -p measurementtype=raw "
                     r"-p measurement.raw.output_file=[^ ]+ -p measurement.raw.no_summary=true "
                     r"-db refit.application.ycsb.REFITYCSBClient "
                     r"-p dataintegrity=true -s", 1),
                Rule(r"Raw data measurement: will output to result file: .+", 100),
                Rule(r"YCSB Client 0\.17\.0", 1),
                Rule(r"Loading workload...", 1),
                Rule(r"Starting test.", 1),
                Rule(r"Thread-\d+ \[EVENT\] BENCH: OK", 1),
                Rule(r"Thread-\d+ \[EVENT\] BENCH: Start time: [0-9]+.[0-9]+s", 1),
                Rule(r"DBWrapper: report latency for each error is false and specific error codes "
                     r"to track for latency are: \[\]", 100),
                Rule(r"\d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d:\d\d\d \d+ sec: \d+ operations; \d+(\.\d+)? current ops/sec;"
                     r"( est completion in \d+ seconds?)?( \[(READ|VERIFY|UPDATE|CLEANUP) AverageLatency\(us\)=\d+(\.\d+)?\])*",
                     float("inf")),
                Rule(r"\d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d:\d\d\d 0 sec: 0 operations; est completion in 0 second",
                     float("inf")),
                Rule(r"Client \d+ done: \d+/\d+", float("inf")),
                Rule(r"\[TOTAL_GC.*\], .+, \d+(\.\d+)?", float("inf")),
                Rule(r"\[OVERALL\], (RunTime\(ms\)|Throughput\(ops/sec\)), \d+(\.\d+)?", 2),
                Rule(r"\[(READ|VERIFY|UPDATE|CLEANUP)\], Total Operations, \d+", 4),
                Rule(r"\[(READ|VERIFY|UPDATE)\], Return=OK, \d+", 3),
            ]

        replica_pattern = r"RPLC[12]?[0-9]-[0-4] "
        replica_ignores = [
            Rule(replica_pattern + r"\[EVENT\] EXCTR: Start time: [0-9]+\.[0-9]+s", 1),
            Rule(replica_pattern + r"\[EVENT\] EXCTR:  *\d+ @ \d+", float("inf")),
            Rule(replica_pattern + r"\[EVENT\] EXCTR: change checkpoint-creation "
                                   r"setting to \"(regular|lightweight)\"", 0, True),
            Rule(replica_pattern + r"\[EVENT\] EXCTR: change update-creation setting "
                                   r"to \"(disabled|enabled)\"", 0, True),
            Rule(replica_pattern + r"\[WARNG\] EXECT: TODO: implement checkpoint fetching", float("inf")),
            Rule(replica_pattern + r"\[EVENT\] ORDER: switch to REFIT(Steward|PBFT(Passive)?|Weave)Protocol"
                                   r" \(view \d\)", 0, True),
            Rule(replica_pattern + r"\[EVENT\] ORDER: \d is now the contact replica", 0, True),
            Rule(replica_pattern + r"\[WARNG\] ORDER: No free slots in the agreement window. "
                                   r"completed \d+/\d+", 0, True),
            Rule(replica_pattern + r"\[WARNG\] (EXCLI|CLINT\[\d\]): resend reply"
                                   r" \{REPLY\|\d+\|\[\d+-\d\]\|true\|\d\|\d+\|(READ|WRITE)\|\d+ bytes\}", 100, True),
            Rule(replica_pattern + r"\[EVENT\] CLINT\[(-1|\d)\]: Configuration update"
                                   r" \(send replies: (true|false) contactReplica: \d\)", 0, True),
            Rule(replica_pattern + r"\[WARNG\] RPNET: Reconnecting to \d+@[^:]+:\d+ by \d+: connection failed: "
                                   r"java.net.ConnectException: Connection refused", 0, True, True),
            Rule(replica_pattern + r"\[WARNG\] RPNET: Lost connection to (\d+@.+|null)", 0, True, True),
            Rule(replica_pattern + r"\[WARNG\] RPNET: Reconnecting to \d+@[^:]+:\d+ by \d+: connection (was )?closed",
                 0,
                 True, True),
            Rule(replica_pattern + r"\[WARNG\] CNCTN\[Socket\[addr=[^/]*/[^,]+,port=\d+,localport=\d+\]\]: "
                                   r"Broken connection", 0, False, True),
            Rule(replica_pattern + r"\[WARNG\] ORDER: dropping old order message.+", float("inf")),
            Rule(r"hang \[ERROR\] HangDetector: Detected hang of " + replica_pattern + r"for "
                                                                                       r"1?\d\d\d\d\d\dus", 0, True),
            Rule(replica_pattern + r"\[EVENT\] CH-SRC: CHAN @ \d+00 \d+\.\d+", float("inf")),
            Rule(replica_pattern + r"\[EVENT\] ORDER: ORDER @ \d+ (\d+\.\d+|NaN)", float("inf")),
            Rule(replica_pattern + r"\[WARNG\] EXCLI: no reply yet for \[\d+-\d+\]", 0, True),
            Rule(replica_pattern + r"\[EVENT\] CONWK\d+: connected to \d+@[^:]+:\d+ by \d+ via CNCTN\[.+", 0, True),
        ]
        if "probe" in scenario:
            replica_ignores += [
                Rule(replica_pattern + r"\[DEBUG\] PRBSS: (Active leaders|"
                                       r"Raw (replies|prepares \d|commits \d)) \[[0-9, \-]+\]", float("inf")),
                Rule(replica_pattern + r"\[DEBUG\] PRBSS: (Raw probe from client \d+ in \d+ with leader \d|"
                                       r"Recommend leader \d with \d+|Last \d with \d+)", float("inf")),
                Rule(replica_pattern + r"\[DEBUG\] PRBSS: (Passive leader \d percentiles \[[0-9, \-]+\]|"
                                       r"Recommend passive replicas \[\d\])", float("inf")),
            ]
        if "weave" in scenario:
            replica_ignores.append(
                Rule(replica_pattern + r"\[WARNG\] TT: Update delay between \d and \d to \d+ ms", float("inf")))
        if "spider" in scenario:
            replica_ignores.append(Rule(replica_pattern + r"\[EVENT\] OCLINT: Configuration update", 0, True))
        if "spider-split-newloc" in scenario:
            replica_ignores += [
                Rule(r"RPLC[0-3]-[0-4] \[WARNG\] CH-SRC: Setting channel group 4 to status (false|true)", 2),
                Rule(r"RPLC1[6-8]-0 \[WARNG\] CHKPT: Got cp", 2),
            ]
        if "fuzzy" in scenario:
            replica_ignores += [
                Rule(replica_pattern + r"\[DEBUG\] EXCTR: (Requesting fuzzy checkpoint start @\d+|Next offset: \d+|"
                                       r"FCP took \d+ seqs|getting changelist for fuzzy collection|getting changes"
                                       r"|clearing changes|\d+ms until \d+ objects of checkpoint data retrieved from "
                                       r"application|Notifying about finished interval up to \d+)", float("inf")),
                Rule(replica_pattern + r"\[DEBUG\] EXECT: Notified about finished collection while @ \d+",
                     float("inf")),
                Rule(replica_pattern + r"\[FCP\] FCP: (Fuzzy collection started @\d+|Finishing checkpoint @\d+"
                                       r"|Collecting full checkpoint|Collection done after \d{4}ms"
                                       r"|Serializing and broadcasting done for \d+ objects of checkpoint @ \d+ "
                                       r"after \d{2,3}ms)", float("inf")),
                Rule(r"\[\d+\.\d\d\ds\]\[info\]\[gc\] GC\(\d+\) Pause (Remark|Cleanup|Young "
                     r"\((Normal|Concurrent Start|Prepare Mixed|Mixed)\) "
                     r"\((G1 Evacuation Pause|GCLocker Initiated GC|G1 Humongous Allocation)\)) "
                     r"\d+M->\d+M\(\d+M\) 1?\d?\d?\d.\d\d\dms", float("inf")),
                Rule(r"\[\d+\.\d\d\ds\]\[info\]\[gc\] GC\(\d+\) Concurrent Cycle( \d?\d?\d.\d\d\dms)?",
                     float("inf")),
            ]
        if "omada-readonly-ratio" in scenario:
            replica_ignores.append(
                Rule(replica_pattern + r"\[WARNG\] CLINT\[\d\]: resend reply"
                                       r" \{REPLY\|\d+\|\[\d+-\d+\]\|true\|\d\|\d+\|WRITE\|\d+ bytes\}", float("inf")))

        replica_must_haves = [
            # Rule(replica_pattern + r"\[EVENT\] CLINT: Configuration update \(send replies: (true|false)\)"),
        ]

        return [
            ClientLogFilter(client_ignores),
            LatencyLogFilter(),
            ResourceLogFilter(),
            ServerLogFilter(replica_ignores, replica_must_haves),
            ServerChannelLogFilter(),
            # FIXME Add transformer to check for required logs
        ]

    def setup_group_transformers(self, scenario: str) -> List[Transformer]:
        return [
            ClientLogAveragesTransformer(),
            ClientRawLatenciesTransformer(),
            ClientRawThroughputTransformer(),
            ClientRawAvgThroughputTransformer(),
            ClientRawAveragesTransformer(),
        ]

    def setup_output_transformers(self, scenario: str) -> List[Transformer]:
        return [
            OutputLogAveragesTransformer(),
            OutputLogThroughputTransformer(),
            OutputRawLatenciesTransformer(),
            OutputRawThroughputTransformer(),
            OutputRawAvgThroughputTransformer(),
            OutputRawAveragesTransformer(),
        ]
