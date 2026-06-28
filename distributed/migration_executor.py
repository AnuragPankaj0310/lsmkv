"""
Migration execution.

Moves keys between owners.
"""

from __future__ import annotations

from storage.engine import StorageEngine


class MigrationExecutor:
    """
    Executes key migrations.
    """

    def __init__(self, engine: StorageEngine):
        self._engine = engine

    async def export_keys(
        self,
        keys: list[str],
    ) -> dict[str, bytes]:
        """
        Read keys from local storage.
        """
        exported = {}

        for key in keys:
            value = await self._engine.get(key)

            if value is not None:
                exported[key] = value

        return exported
    
    async def export_key_batches(
        self,
        keys: list[str],
        batch_size: int = 500,
    ):
        """
        Yield exported key/value batches.
        """

        for i in range(0, len(keys), batch_size):
            batch_keys = keys[i:i + batch_size]

            exported = await self.export_keys(batch_keys)

            yield batch_keys, exported

    async def import_keys(
        self,
        data: dict[str, bytes],
    ) -> None:
        """
        Import migrated keys.
        """
        for key, value in data.items():
            await self._engine.set(key, value)  

    async def delete_keys(
        self,
        keys: list[str],
    ) -> None:
        """
        Delete migrated keys from local storage.
        """
        for key in keys:
            await self._engine.delete(key)