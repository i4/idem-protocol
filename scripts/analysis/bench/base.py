from typing import Tuple, Optional, Iterable, List, Protocol

from common.log import LogData
from parser.base import Parser
from transformer.base import Transformer


class AnalysisBenchmark(Protocol):
    def parse_folder_name(self, name: str) -> Tuple[Optional[str], int]: ...

    def setup_parsers(self, scenario: str) -> Iterable[Parser]: ...

    def is_auto_range_log(self, log: LogData) -> bool: ...

    def setup_transformers(self, scenario: str, path: str) -> List[Transformer]:
        return []

    def setup_group_transformers(self, scenario: str) -> List[Transformer]:
        return []

    def setup_output_transformers(self, scenario: str) -> List[Transformer]: ...
