"""
Coordinates cluster rebalancing.
"""

from __future__ import annotations
import time

from distributed.migration import MigrationPlanner
from distributed.migration_executor import MigrationExecutor


class RebalanceCoordinator:
    """
    High-level orchestration for rebalancing.
    """

    def __init__(
        self,
        planner: MigrationPlanner,
    ):
        self._planner = planner

    async def rebalance(
        self,
        source: MigrationExecutor,
        destination: MigrationExecutor,
        keys: list[str],
    ) -> dict[str, int]:
        """
        Rebalance keys between two nodes.
        """
        keys_to_move = self._planner.keys_to_move(keys)

        exported = await source.export_keys(keys_to_move)

        await destination.import_keys(exported)

        return {
            "migrated": len(exported),
            "unchanged": len(keys) - len(exported),
        }
        