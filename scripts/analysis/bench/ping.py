#!/usr/bin/env python3
from typing import Tuple, Iterable, Optional, List

from bench.base import AnalysisBenchmark
from common.log import LogData
from output.ping import OutputPingTransformer
from parser.base import Parser
from parser.ping import PingLogParser
from transformer.base import Transformer


class PingBenchmark(AnalysisBenchmark):
    def parse_folder_name(self, name: str) -> Tuple[Optional[str], int]:
        name_chunks = name.split("-measure_server_pings-", 1)
        if len(name_chunks) < 2:
            return None, 0

        scenario = name_chunks[1].rsplit("-", 2)[0]
        return scenario, 0

    def setup_parsers(self, scenario: str) -> Iterable[Parser]:
        return [PingLogParser()]

    def is_auto_range_log(self, log: LogData) -> bool:
        return log.logtype == "ping"

    def setup_output_transformers(self, scenario: str) -> List[Transformer]:
        return [OutputPingTransformer()]
