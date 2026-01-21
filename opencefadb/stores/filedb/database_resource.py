from dataclasses import dataclass
from typing import Any

import rdflib


@dataclass
class DatabaseResource:
    identifier: Any
    metadata: rdflib.Graph

    def serialize(self, format, **kwargs) -> str:
        return self.metadata.serialize(format=format, **kwargs)
