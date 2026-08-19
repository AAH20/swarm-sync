"""
Swarm-Sync: Distributed Multi-Version Concurrency Control (MVCC) & State Synchronization
Engine for High-Concurrency Multi-Agent Autonomous Swarms (Claude 5 / GPT-5.6).
"""

from .mvcc import MVCCStore, Transaction, WriteConflictError, Snapshot
from .arbiter import ThunderingHerdArbiter, RateLimitQuotaExceeded
from .mesh import SharedMemoryMesh

__version__ = "0.1.0"
__all__ = [
    "MVCCStore",
    "Transaction",
    "WriteConflictError",
    "Snapshot",
    "ThunderingHerdArbiter",
    "RateLimitQuotaExceeded",
    "SharedMemoryMesh",
]
