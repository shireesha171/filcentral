from dataclasses import dataclass


@dataclass
class TargetConfig:
    uuid: str
    location_pattern: str
