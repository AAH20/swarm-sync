# Swarm-Sync

A single-writer, thread-safe Multi-Version Concurrency Control (MVCC) store
with snapshot isolation and optimistic conflict detection, plus a
token-bucket concurrency arbiter, for coordinating multiple threads writing
shared state.

[![CI](https://github.com/AAH20/swarm-sync/actions/workflows/ci.yml/badge.svg)](https://github.com/AAH20/swarm-sync/actions)
[![License](https://img.shields.io/badge/license-MIT%2FApache--2.0-blue.svg)](LICENSE-MIT)

An earlier version of this repo had a real, 100%-reproducible bug and a set
of claims the code didn't back up. Both are fixed now, and the fixes are
substantive, not cosmetic:

- **The store deadlocked on the first commit of any write, every time.**
  `_commit_tx` acquired `self._lock`, then called `_get_with_version`, which
  acquired the *same* non-reentrant `threading.Lock` again. Caught by
  actually running the test suite (it hung indefinitely) rather than
  trusting the "Zero-Race-Conditions" badge that shipped next to it. Fixed
  by splitting out `_lookup_locked`, an internal helper that assumes the
  lock is already held, so `_commit_tx` no longer re-acquires it.
- **"Lock-free" was false, architecturally, not just a wrong word.** Every
  method on `MVCCStore` — reads included — serializes through one global
  lock. There is no concurrent access here for "optimistic" concurrency
  control to be optimistic about. The version-tracking and conflict
  detection are real and correctly implemented (see below), but the store
  itself is a single-writer-at-a-time structure, not a concurrent one.
- **The retry-on-conflict path had never actually been exercised.** The
  50-agent benchmark test always reports "Conflicts Resolved: 0" — under
  CPython's GIL plus the (now-fixed) global lock, threads never interleave
  mid-transaction, so `atomic_update`'s retry logic was present but
  untested. Added `test_atomic_update_retries_and_recovers_from_a_real_conflict`,
  which deterministically forces a conflict (a second transaction commits
  mid-mutation) and asserts the retry actually recovers with the
  post-conflict value — proving that path works instead of leaving it
  unverified.
- **`pip install swarm-sync` never worked** — this package has never been
  published to PyPI. The Quickstart below installs from source instead of
  claiming a registry listing that doesn't exist.
- **`pydantic`, `anyio`, and `typing_extensions` were listed as dependencies
  and never imported anywhere.** Removed; this project has zero third-party
  runtime dependencies.
- **`swarm_sync/telemetry.py` was named in the architecture diagram and
  didn't exist.** Removed from the diagram; see Roadmap.
- **The cited CrewAI issue (#831, "fixed mixin") doesn't relate to
  connection pooling or lock contention at all** — checked its actual body,
  it's unrelated. Removed. The LangGraph citations (#8115, #8114, #7259,
  #8136, #7857) are real and on-topic; kept.
- **The "$142,000+ per incident" figure and the "Commercial Integration
  with A2Z SOC" section had no basis in this repo** — no calculator
  producing that number, no code talking to a2zsoc.com. Both removed.

## What's actually here

```
swarm-sync/
├── swarm_sync/
│   ├── mvcc.py     # MVCCStore: single-lock, versioned key-value store with snapshot isolation.
│   ├── mesh.py      # SharedMemoryMesh: atomic_update() with retry-on-conflict over MVCCStore.
│   └── arbiter.py   # ThunderingHerdArbiter: asyncio token-bucket + semaphore rate limiter.
```

## Try it

```bash
git clone https://github.com/AAH20/swarm-sync.git && cd swarm-sync
pip install -e .
python -m unittest tests.test_concurrency -v
```

## What each piece actually does

- **`MVCCStore`** gives you snapshot isolation (a transaction sees a
  consistent point-in-time view even while other transactions commit) and
  optimistic conflict detection (a transaction that commits based on a
  stale read raises `WriteConflictError` instead of silently overwriting).
  Both properties are real and tested — `test_mvcc_snapshot_isolation`
  demonstrates isolation and conflict detection deterministically, and
  `test_atomic_update_retries_and_recovers_from_a_real_conflict` proves the
  retry path recovers correctly. What it does *not* give you is concurrent
  throughput: it's one global lock, so operations serialize.
- **`SharedMemoryMesh.atomic_update`** wraps a read-mutate-write cycle with
  automatic retry on conflict, capped at 10 attempts with jittered backoff.
- **`ThunderingHerdArbiter`** is a straightforward asyncio token-bucket rate
  limiter plus a semaphore cap on concurrent in-flight operations — this
  part was accurately described from the start.

## Honest scope

- This is a single-process, in-memory, single-lock store — no persistence,
  no cross-process or cross-machine synchronization, no real concurrent
  execution of store operations (Python's GIL means the 50-thread benchmark
  test doesn't exercise true parallel access either; it demonstrates
  correctness under interleaved threading, not throughput under real
  concurrency).
- The MVCC/OCC *mechanism* — versioned records, stale-read detection — is
  real and correctly implemented. The *performance model* implied by "lock-
  free" and "high-throughput" was not; this store trades concurrency for
  correctness via a single lock, which is a legitimate design choice, just
  not the one the original README claimed.
- Not published to PyPI. Install from source (see above).

## Roadmap

- Per-key locking (striped locks, keyed and sorted to avoid lock-ordering
  deadlocks across multi-key transactions) to allow genuine concurrent
  access to different keys, rather than one global lock — this is what
  would make a "concurrent" claim honestly true rather than something to
  walk back.
- `telemetry.py`: real contention metrics and conflict-rate tracking.
- PyPI publication.

## License

MIT OR Apache-2.0
