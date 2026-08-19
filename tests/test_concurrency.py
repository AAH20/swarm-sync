import threading
import time
import unittest
from swarm_sync import MVCCStore, WriteConflictError, SharedMemoryMesh

class TestSwarmSyncConcurrency(unittest.TestCase):
    def test_mvcc_snapshot_isolation(self):
        store = MVCCStore()
        
        # Initial setup
        tx0 = store.begin()
        tx0.set("balance", 1000)
        tx0.commit()

        # Tx1 reads balance = 1000
        tx1 = store.begin()
        val1 = tx1.get("balance")
        self.assertEqual(val1, 1000)

        # Tx2 modifies balance to 1500 and commits
        tx2 = store.begin()
        tx2.set("balance", 1500)
        tx2.commit()

        # Tx1 still sees 1000 (Snapshot Isolation)
        self.assertEqual(tx1.get("balance"), 1000)

        # Tx1 tries to write based on stale read -> must raise WriteConflictError
        tx1.set("balance", val1 + 200)
        with self.assertRaises(WriteConflictError):
            tx1.commit()

    def test_50_concurrent_agents_balance_reconciliation(self):
        mesh = SharedMemoryMesh()
        mesh.atomic_update("corporate_treasury", lambda val: 0)

        num_agents = 50
        deposit_amount = 50

        def worker(agent_id: int):
            mesh.atomic_update("corporate_treasury", lambda current: (current or 0) + deposit_amount)

        threads = []
        start_time = time.time()
        for i in range(num_agents):
            t = threading.Thread(target=worker, args=(i,))
            threads.append(t)
            t.start()

        for t in threads:
            t.join()

        duration = time.time() - start_time
        final_balance = mesh.read_snapshot("corporate_treasury")

        # Invariant: 50 agents * $50 = Exactly $2,500. ZERO lost writes!
        expected_balance = num_agents * deposit_amount
        self.assertEqual(final_balance, expected_balance)
        print(f"\n[BENCHMARK] 50 Concurrent Agent Writes Reconciled in {duration:.3f}s")
        print(f"[INVARIANT] Expected: ${expected_balance} | Actual: ${final_balance} | Conflicts Resolved: {mesh.conflicts_caught}")

if __name__ == "__main__":
    unittest.main()
