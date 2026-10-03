"""并发竞争测试 + HTTP API 端到端测试。

重点场景:
  * 旧进程迟到写入 vs 恢复后新进程同时提交相同 output_id — 不双输出;
  * 多线程同时提交/写同一分片 — 状态不破坏、恰好一次落盘;
  * 失联节点补齐与提交并发 — 绝不提前 COMPLETE。
"""

import base64
import json
import threading
import unittest
import urllib.request
import urllib.error

from hpc_cr import JobManager
from hpc_cr.api import build_server
from hpc_cr.epochs import Scheduler
from hpc_cr.security import Signer


class ConcurrencyTest(unittest.TestCase):
    def setUp(self):
        self.mgr = JobManager(Scheduler(Signer("conc-secret")))
        nodes = {r: f"n{r}" for r in range(4)}
        self.mgr.create_job(name="par", world_size=4, shards_per_rank=1,
                            rank_nodes=nodes, job_id="par-1")
        pre = self.mgr.preempt("par-1")
        self.epoch_token = pre["epoch_token"]
        # 完成检查点并释放
        barrier = threading.Barrier(4)

        def worker(r):
            barrier.wait()
            self.mgr.upload_shard(
                "par-1", epoch_token=self.epoch_token, rank=r, index=0,
                node=f"n{r}", input_pos=100 + r,
                data=f"shard-{r}".encode())

        threads = [threading.Thread(target=worker, args=(r,)) for r in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.mgr.commit_checkpoint("par-1", epoch_token=self.epoch_token)
        self.mgr.confirm_release("par-1", epoch_token=self.epoch_token)
        report = self.mgr.restore("par-1", generation=1)
        self.assertTrue(report.success)
        self.lease_token = self.mgr.get("par-1").leases.current().token

    def test_late_write_vs_restored_writer_no_double_output(self):
        results: list[tuple[str, bool]] = []
        lock = threading.Lock()

        def old_writer():
            """旧进程: 持抢占前的 gen0 租约 (已吊销), 迟到。"""
            old_lease = self.mgr.get("par-1").leases.get(0)
            try:
                r = self.mgr.append_output(
                    "par-1", lease_token=old_lease.token,
                    output_id="result-0", data=b"OLD OUTPUT", rank=0)
                with lock:
                    results.append(("old", r["delivered"]))
            except Exception as exc:  # noqa: BLE001
                with lock:
                    results.append(("old_rejected", type(exc).__name__ != ""))

        def new_writer():
            """恢复后的新进程: 持 gen1 新租约。"""
            try:
                r = self.mgr.append_output(
                    "par-1", lease_token=self.lease_token,
                    output_id="result-0", data=b"NEW OUTPUT", rank=0)
                with lock:
                    results.append(("new", r["delivered"]))
            except Exception as exc:  # noqa: BLE001
                with lock:
                    results.append(("new_rejected", str(exc)))

        ts = [threading.Thread(target=old_writer),
              threading.Thread(target=new_writer)]
        for t in ts:
            t.start()
        for t in ts:
            t.join()

        kinds = {k for k, _ in results}
        self.assertIn("old_rejected", kinds)  # 迟到旧写入必被栅栏拒绝
        self.assertIn("new", kinds)
        # 输出日志中只有一条 gen1 记录, 内容是新输出
        outputs = self.mgr.list_outputs("par-1")
        self.assertEqual(len(outputs), 1)
        self.assertEqual(outputs[0]["generation"], 1)
        self.assertEqual(
            self.mgr.store.get(outputs[0]["blob"]), b"NEW OUTPUT")
        # 审计里留有授权拒绝安全事件证据, 且不含凭证原文
        events = [a["event"] for a in self.mgr.get("par-1").audit]
        self.assertIn("security_authz_denied", events)
        sec = self.mgr.security_events("par-1")
        self.assertEqual(sec[-1]["reason"], "stale_generation")
        leaked = [e for e in sec if "token" in json.dumps(
            {k: v for k, v in e.items() if k != "type"})]
        self.assertEqual(leaked, [])

    def test_concurrent_duplicate_output_idempotent_not_double(self):
        outcomes = []
        barrier = threading.Barrier(8)

        def submit(i):
            barrier.wait()
            r = self.mgr.append_output(
                "par-1", lease_token=self.lease_token,
                output_id="same-id", data=b"same bytes", rank=i % 4)
            outcomes.append(r["delivered"])

        ts = [threading.Thread(target=submit, args=(i,)) for i in range(8)]
        for t in ts:
            t.start()
        for t in ts:
            t.join()
        self.assertEqual(sum(outcomes), 1)  # 恰好一次 delivered
        self.assertEqual(len(self.mgr.list_outputs("par-1", 1)), 1)

    def test_concurrent_duplicate_shards_and_commits(self):
        # 新作业: 8 个线程并发上传同一分片 + 并发提交
        self.mgr.create_job(name="par2", world_size=2,
                            rank_nodes={0: "n0", 1: "n1"}, job_id="par-2")
        pre = self.mgr.preempt("par-2")
        token = pre["epoch_token"]
        errs = []

        def upload():
            try:
                self.mgr.upload_shard(
                    "par-2", epoch_token=token, rank=0, index=0,
                    node="n0", input_pos=42, data=b"same shard bytes")
            except Exception as exc:  # noqa: BLE001
                # 同内容重传是幂等成功, 不应进这里
                errs.append(exc)
        self.assertEqual(errs, [])

        ts = [threading.Thread(target=upload) for _ in range(8)]
        for t in ts:
            t.start()
        for t in ts:
            t.join()
        self.assertEqual(errs, [])

        # 第二片一次性补齐后并发 commit, 任何时刻都不可能错误 COMPLETE
        commit_results, commit_errors = [], []
        done = threading.Event()

        def try_commit():
            done.wait()
            try:
                commit_results.append(
                    self.mgr.commit_checkpoint(
                        "par-2", epoch_token=token)["checkpoint"]["state"])
            except Exception as exc:  # noqa: BLE001
                commit_errors.append(type(exc).__name__)

        cts = [threading.Thread(target=try_commit) for _ in range(6)]
        for t in cts:
            t.start()
        done.set()
        for t in cts:
            t.join()
        # 未补齐 r1:s0 时全部提交必须失败
        self.assertEqual(commit_results, [])
        self.assertEqual(len(commit_errors), 6)

        self.mgr.upload_shard(
            "par-2", epoch_token=token, rank=1, index=0,
            node="n1", input_pos=7, data=b"second")
        cp = self.mgr.commit_checkpoint("par-2", epoch_token=token)
        self.assertEqual(cp["checkpoint"]["state"], "COMPLETE")

    def test_lost_node_recovery_race_with_commit(self):
        self.mgr.create_job(name="par3", world_size=2,
                            rank_nodes={0: "a", 1: "b"}, job_id="par-3")
        pre = self.mgr.preempt("par-3")
        token = pre["epoch_token"]
        self.mgr.upload_shard("par-3", epoch_token=token, rank=0, index=0,
                              node="a", input_pos=1, data=b"a-shard")
        self.mgr.mark_node_lost("par-3", "b")

        outcomes = []

        def commit_loop():
            for _ in range(20):
                try:
                    r = self.mgr.commit_checkpoint("par-3", epoch_token=token)
                    outcomes.append(("committed",
                                     r["checkpoint"]["state"]))
                except Exception:  # noqa: BLE001
                    outcomes.append(("rejected", None))

        def recover():
            self.mgr.heartbeat("par-3", "b")
            self.mgr.upload_shard("par-3", epoch_token=token, rank=1,
                                  index=0, node="b", input_pos=2,
                                  data=b"b-shard")

        t1 = threading.Thread(target=commit_loop)
        t2 = threading.Thread(target=recover)
        t1.start()
        t2.start()
        t1.join()
        t2.join()
        # 恢复前的提交全部 rejected; 最终最多一次 COMPLETE 且在最后
        states = [s for k, s in outcomes if k == "committed"]
        self.assertTrue(all(s == "COMPLETE" for s in states))
        self.assertLessEqual(len(states), 1)
        if not states:
            self.assertEqual(
                self.mgr.commit_checkpoint(
                    "par-3", epoch_token=token)["checkpoint"]["state"],
                "COMPLETE")


class HttpApiTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.server = build_server(
            JobManager(Scheduler(Signer("http-secret"))),
            host="127.0.0.1", port=0)
        cls.port = cls.server.server_address[1]
        cls.thread = threading.Thread(target=cls.server.serve_forever,
                                      daemon=True)
        cls.thread.start()

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()

    def _req(self, method, path, body=None, headers=None):
        url = f"http://127.0.0.1:{self.port}{path}"
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(url, data=data, method=method,
                                     headers={"Content-Type": "application/json",
                                              **(headers or {})})
        try:
            with urllib.request.urlopen(req, timeout=5) as resp:
                return resp.status, json.loads(resp.read())
        except urllib.error.HTTPError as e:
            return e.code, json.loads(e.read())

    def test_full_flow_over_http(self):
        st, job = self._req("POST", "/api/jobs", {
            "job_id": "http-1", "name": "demo", "world_size": 2,
            "rank_nodes": {0: "h0", 1: "h1"}})
        self.assertEqual(st, 201)
        # 抢占前的 gen0 租约令牌 (模拟旧进程手中持有的旧凭证)
        old_lease_token = self.server.manager.get(
            "http-1").leases.current().token

        st, pre = self._req("POST", "/api/jobs/http-1",
                            {"action": "preempt", "grace_seconds": 60})
        self.assertEqual(st, 200)
        token = pre["epoch_token"]
        self.assertEqual(pre["checkpoint"]["received"], 0)

        st, up = self._req("POST", "/api/jobs/http-1", {
            "action": "upload_shard", "rank": 0, "index": 0,
            "node": "h0", "input_pos": 11,
            "data_b64": base64.b64encode(b"http-shard-0").decode()},
            headers={"X-Epoch-Token": token})
        self.assertEqual(st, 200)
        self.assertTrue(up["stored"])

        # 缺片提交 -> 409
        st, err = self._req("POST", "/api/jobs/http-1",
                            {"action": "commit_checkpoint"},
                            headers={"X-Epoch-Token": token})
        self.assertEqual(st, 409)
        self.assertIn("missing", err["message"])

        # 无令牌 -> 401
        st, err = self._req("POST", "/api/jobs/http-1",
                            {"action": "confirm_release"})
        self.assertEqual(st, 401)

        self._req("POST", "/api/jobs/http-1", {
            "action": "upload_shard", "rank": 1, "index": 0,
            "node": "h1", "input_pos": 22,
            "data_b64": base64.b64encode(b"http-shard-1").decode()},
            headers={"X-Epoch-Token": token})
        st, commit = self._req("POST", "/api/jobs/http-1",
                               {"action": "commit_checkpoint"},
                               headers={"X-Epoch-Token": token})
        self.assertEqual(st, 200)
        self.assertEqual(commit["checkpoint"]["state"], "COMPLETE")
        st, _ = self._req("POST", "/api/jobs/http-1",
                          {"action": "confirm_release"},
                          headers={"X-Epoch-Token": token})
        self.assertEqual(st, 200)

        # 旧租约输出被栅栏: 412
        st, restore = self._req("POST", "/api/jobs/http-1",
                                {"action": "restore", "generation": 1})
        self.assertEqual(st, 200)
        self.assertTrue(restore["success"])
        new_lease = restore["lease"]
        lease_token = self.server.manager.get("http-1").leases.current().token
        self.assertEqual(new_lease["lease_id"],
                         self.server.manager.get("http-1").leases.current().lease_id)

        # 旧进程持旧租约迟到写入 -> 412 栅栏拒绝, 不会产生双份输出
        st, fenced = self._req("POST", "/api/jobs/http-1", {
            "action": "append_output", "output_id": "result",
            "rank": 0, "data_b64": base64.b64encode(b"old process").decode()},
            headers={"X-Lease-Token": old_lease_token})
        self.assertEqual(st, 412)
        self.assertEqual(fenced["error"], "fence_violation")

        st, out = self._req("POST", "/api/jobs/http-1", {
            "action": "append_output", "output_id": "out-1",
            "rank": 0, "data_b64": base64.b64encode(b"final!").decode()},
            headers={"X-Lease-Token": lease_token})
        self.assertEqual(st, 200)
        self.assertTrue(out["delivered"])
        # 幂等重试
        st, out2 = self._req("POST", "/api/jobs/http-1", {
            "action": "append_output", "output_id": "out-1",
            "rank": 0, "data_b64": base64.b64encode(b"final!").decode()},
            headers={"X-Lease-Token": lease_token})
        self.assertFalse(out2["delivered"])

        st, prog = self._req("GET", "/api/jobs/http-1/progress.json")
        self.assertEqual(st, 200)
        self.assertEqual(len(prog["shards"]), 2)
        self.assertIn("security_authz_denied",
                      [a["event"] for a in prog["audit"]])
        # 授权失败有稳定错误码/原因, 响应不回显凭证
        self.assertEqual(fenced["reason"], "stale_generation")
        self.assertNotIn(old_lease_token, json.dumps(fenced))

        # HTML 进度页可访问
        with urllib.request.urlopen(
                f"http://127.0.0.1:{self.port}/ui/jobs/http-1") as r:
            html = r.read().decode()
        self.assertIn("检查点分片矩阵", html)

        st, rep = self._req("GET",
                            "/api/jobs/http-1/restore/report")
        self.assertEqual(st, 200)
        self.assertTrue(rep["success"])
        self.assertEqual(rep["shards_verified"], 2)


if __name__ == "__main__":
    unittest.main()
