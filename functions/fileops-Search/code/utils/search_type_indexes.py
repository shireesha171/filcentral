"""
    Search type indexes

    In this module, we can keep track of the search type index.
    Every query_resource will have a unique search type index.
    Each search type index defines a simple/advance search for a resource.

    Every time you implement a new search for a resource add a search type index here.
    Use the search type index in the lambda_handler function if/else conditions to differentiate different type of
    searches.
"""

from enum import Enum


class JOB_RUNS_SEARCH_TYPE_INDEXES(Enum):
    """
        Enum representing search type indexes

        ADVANCE_SEARCH_1: "Job Runs" page advance search
    """

    ADVANCE_SEARCH_1 = "1"
