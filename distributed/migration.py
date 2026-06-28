"""
Key migration planning.
"""

from __future__ import annotations

from distributed.rebalance import RebalancePlanner


class MigrationPlanner:
    """
    Computes which keys should migrate.
    """

    def __init__(self, planner: RebalancePlanner):
        self._planner = planner

    def keys_to_move(
        self,
        keys: list[str],
    ) -> list[str]:
        """
        Return all keys whose ownership changes.
        """
        return [
            key
            for key in keys
            if self._planner.needs_migration(key)
        ]