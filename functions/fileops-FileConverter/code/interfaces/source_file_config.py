from dataclasses import dataclass
from enum import Enum
from typing import List, Optional


class TargetFileTypes(Enum):
    CSV = "CSV"
    PARQUET = "Parquet"


class ParquetPartitionTypes(Enum):
    SOURCE_COLUMNS = "Source Columns"
    SOURCE_TIMESTAMP = "Source timestamp"


class ParquetPartitionColumn:
    order: int
    column: str
    selected: bool


def map_target_file_type(target_file_type: str) -> TargetFileTypes:
    if target_file_type == TargetFileTypes.CSV.value:
        return TargetFileTypes.CSV
    elif target_file_type == TargetFileTypes.PARQUET.value:
        return TargetFileTypes.PARQUET


def map_parquet_partition_type(parquet_partition_type: str) -> ParquetPartitionTypes:
    if parquet_partition_type == ParquetPartitionTypes.SOURCE_COLUMNS.value:
        return ParquetPartitionTypes.SOURCE_COLUMNS
    elif parquet_partition_type == ParquetPartitionTypes.SOURCE_TIMESTAMP.value:
        return ParquetPartitionTypes.SOURCE_TIMESTAMP


@dataclass(frozen=True)
class TargetFileConfigDetails:
    target_file_type: TargetFileTypes
    target_filename_pattern: Optional[str]
    parquet_partition_type: ParquetPartitionTypes
    parquet_partitions_schema: List[Optional[ParquetPartitionColumn]]
