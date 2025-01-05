from dataclasses import dataclass
from typing import List


@dataclass
class ColumnParsing:
    name: str
    suffixes: List[str]
        
    @property
    def derivative_columns_names(self) -> List[str]:
        return [self.name+suffix for suffix in self.suffixes]


@dataclass
class ParsingConfig:
    parsing_rules: List[ColumnParsing]