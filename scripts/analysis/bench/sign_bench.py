#!/usr/bin/env python3
from typing import Tuple, Iterable, Optional, List

from bench.base import AnalysisBenchmark
from common.log import LogData
from output.signature_avg_bench import OutputSignatureAvgBenchTransformer
from output.signature_bench import OutputSignatureBenchTransformer
from parser.base import Parser
from parser.signature import SignatureBenchLogParser
from transformer.base import Transformer
from transformer.signature_average import SignatureAveragesTransformer


class SignatureBenchmark(AnalysisBenchmark):
    def parse_folder_name(self, name: str) -> Tuple[Optional[str], int]:
        name_chunks = name.split("-signature-perf-", 1)
        if len(name_chunks) < 2:
            return None, 0

        scenario = name_chunks[1].rsplit("-", 2)[0]
        return scenario, 0

    def setup_parsers(self, scenario: str) -> Iterable[Parser]:
        return [
            SignatureBenchLogParser(),
        ]

    def is_auto_range_log(self, log: LogData) -> bool:
        return log.logtype == "signature"

    def setup_transformers(self, scenario: str, path: str) -> List[Transformer]:
        return [
            SignatureAveragesTransformer(),
        ]

    def setup_output_transformers(self, scenario: str) -> List[Transformer]:
        return [
            OutputSignatureBenchTransformer(),
            OutputSignatureAvgBenchTransformer(),
        ]
