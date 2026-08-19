import time
import threading
from typing import Dict, Any, Optional, List, Tuple
from dataclasses import dataclass, field

class WriteConflictError(Exception):
    """Raised when an Optimistic Concurrency Control (OCC) conflict occurs during commit."""
    pass

@dataclass
class VersionedValue:
    val: Any
    created_at_tx: int
    deleted_at_tx: Optional[int] = None

@dataclass
class Snapshot:
    read_tx_id: int
    store: "MVCCStore"

    def get(self, key: str) -> Optional[Any]:
        return self.store._get_at_version(key, self.read_tx_id)

class Transaction:
    def __init__(self, tx_id: int, store: "MVCCStore"):
        self.tx_id = tx_id
        self.store = store
        self.reads: Dict[str, int] = {}  # key -> read_version
        self.writes: Dict[str, Any] = {} # key -> new_val
        self.is_active = True

    def get(self, key: str) -> Optional[Any]:
        if key in self.writes:
            return self.writes[key]
        val, ver = self.store._get_with_version(key, self.tx_id)
        self.reads[key] = ver
        return val

    def set(self, key: str, value: Any):
        self.writes[key] = value

    def commit(self) -> bool:
        if not self.is_active:
            raise RuntimeError("Cannot commit an inactive transaction")
        success = self.store._commit_tx(self)
        self.is_active = False
        return success

    def rollback(self):
        self.writes.clear()
        self.is_active = False

class MVCCStore:
    def __init__(self):
        self._lock = threading.Lock()
        self._tx_counter = 0
        # key -> list of VersionedValue sorted by created_at_tx
        self._records: Dict[str, List[VersionedValue]] = {}

    def _next_tx_id(self) -> int:
        self._tx_counter += 1
        return self._tx_counter

    def begin(self) -> Transaction:
        with self._lock:
            tx_id = self._next_tx_id()
            return Transaction(tx_id, self)

    def snapshot(self) -> Snapshot:
        with self._lock:
            return Snapshot(self._tx_counter, self)

    def _get_at_version(self, key: str, tx_id: int) -> Optional[Any]:
        with self._lock:
            versions = self._records.get(key, [])
            for v in reversed(versions):
                if v.created_at_tx <= tx_id:
                    if v.deleted_at_tx is None or v.deleted_at_tx > tx_id:
                        return v.val
            return None

    def _get_with_version(self, key: str, tx_id: int) -> Tuple[Optional[Any], int]:
        with self._lock:
            versions = self._records.get(key, [])
            for v in reversed(versions):
                if v.created_at_tx <= tx_id:
                    if v.deleted_at_tx is None or v.deleted_at_tx > tx_id:
                        return v.val, v.created_at_tx
            return None, 0

    def _commit_tx(self, tx: Transaction) -> bool:
        with self._lock:
            # 1. Validate Reads (Optimistic Concurrency Control)
            for k, read_ver in tx.reads.items():
                current_val, current_ver = self._get_with_version(k, self._tx_counter)
                if current_ver > read_ver and k not in tx.writes:
                    raise WriteConflictError(
                        f"Conflict on key '{k}': read version {read_ver}, but current version is {current_ver}"
                    )

            # 2. Check Write-Write Conflicts
            for k in tx.writes:
                current_val, current_ver = self._get_with_version(k, self._tx_counter)
                if k in tx.reads and current_ver > tx.reads[k]:
                    raise WriteConflictError(
                        f"Write-Write conflict on key '{k}': modified by concurrent transaction"
                    )

            # 3. Apply Writes
            commit_tx_id = self._next_tx_id()
            for k, new_val in tx.writes.items():
                if k not in self._records:
                    self._records[k] = []
                # Mark previous active version as deleted
                for v in self._records[k]:
                    if v.deleted_at_tx is None:
                        v.deleted_at_tx = commit_tx_id
                # Append new version
                self._records[k].append(VersionedValue(new_val, commit_tx_id))

            return True
