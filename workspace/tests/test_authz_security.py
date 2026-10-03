"""授权失败统一处理 + 顺序化安全事件 + 并发不增条 + 重启可查。

覆盖需求:
  1. 撤销 / 超期 / 伪造 / 跨作业凭证提交结果均得到稳定错误
     (固定 HTTP 状态 + error + reason + 固定消息, 不回显凭证);
  2. 每次拒绝写入一条顺序化安全事件, 事件不含凭证原文;
  3. 重复并发请求不增加结果条目;
  4. 服务重启后, 页面聚合与恢复验证报告仍能查询这些事件。
"""

import base64
import json
import shutil
import tempfile
import threading
import time
import unittest
import urllib.error
import urllib.request

from hpc_cr import JobManager
from hpc_cr.api import build_server
from hpc_cr.checkpoints import COMPLETE
from hpc_cr.epochs import Scheduler
from hpc_cr.errors import FenceViolation, InvalidToken
from hpc_cr.security import Signer

SECRET = "authz-secret"


def _make_manager(data_dir=None):
    return JobManager(Scheduler(Signer(SECRET)), data_dir=data_dir)


def _prepare_released_and_restored(mgr, jid="j1", world_size=2,
                                   nodes=None, grace=300.0):
    """创建作业 -> 抢占 -> 补齐 -> 提交 -> 释放 -> 恢复, 返回当前租约令牌。"""
    nodes = nodes or {r: f"n{r}" for r in range(world_size)}
    mgr.create_job(name="j", world_size=world_size, job_id=jid,
                   rank_nodes=nodes, lease_ttl=100000)
    pre = mgr.preempt(jid, grace_seconds=grace)
    token = pre["epoch_token"]
    for r in range(world_size):
        mgr.upload_shard(jid, epoch_token=token, rank=r, index=0,
                         node=nodes[r], input_pos=100 + r,
                         data=f"shard-{r}".encode())
    mgr.commit_checkpoint(jid, epoch_token=token)
    mgr.confirm_release(jid, epoch_token=token)
    rep = mgr.restore(jid, generation=pre["epoch"]["generation"])
    assert rep.success
    return mgr.get(jid).leases.current().token, token


class AuthorizationDenialTest(unittest.TestCase):
    def setUp(self):
        self.mgr = _make_manager()
        self.lease, self.epoch = _prepare_released_and_restored(self.mgr)
        self.job = self.mgr.get("j1")

    def _deny(self, token, output_id="out", data=b"payload"):
        try:
            self.mgr.append_output(
                "j1", lease_token=token, output_id=output_id, data=data)
            return None
        except Exception as exc:  # noqa: BLE001
            return exc

    # ------------------------------------------------- 1) 稳定错误分类
    def test_missing_token_is_stable_401(self):
        exc = self._deny(None)
        self.assertIsInstance(exc, InvalidToken)
        self.assertEqual(exc.http_status, 401)
        self.assertEqual(exc.error_code, "invalid_token")
        self.assertEqual(exc.reason, "missing_token")
        body = exc.to_dict()
        self.assertEqual(body["reason"], "missing_token")
        # 固定消息, 不随请求变化
        self.assertIn("credential", body["message"])

    def test_forged_token_is_stable_401(self):
        for forged in ("garbage", "v1.abc.def", "v2.aa.bb",
                       "v1.@@@.00", 3.14, ""):
            exc = self._deny(forged)
            self.assertIsInstance(exc, InvalidToken, forged)
            self.assertEqual(exc.http_status, 401, forged)
            self.assertEqual(exc.error_code, "invalid_token", forged)
            self.assertIn(exc.reason, {"bad_signature", "malformed_token",
                                      "bad_token_version",
                                      "undecodable_payload",
                                      "missing_token"}, forged)

    def test_wrong_token_type_is_401(self):
        # epoch 令牌冒充输出租约
        exc = self._deny(self.epoch)
        self.assertIsInstance(exc, InvalidToken)
        self.assertEqual(exc.http_status, 401)
        self.assertEqual(exc.reason, "wrong_token_type")

    def test_revoked_lease_is_412_lease_revoked(self):
        # 再次抢占: 同代际 gen1 租约被吊销, output_generation 仍为 1,
        # 因此旧租约走到 REVOKED 判定
        self.mgr.preempt("j1", force_new_epoch=True, grace_seconds=300)
        exc = self._deny(self.lease)
        self.assertIsInstance(exc, FenceViolation)
        self.assertEqual(exc.http_status, 412)
        self.assertEqual(exc.error_code, "fence_violation")
        self.assertEqual(exc.reason, "lease_revoked")

    def test_expired_token_is_412_lease_expired(self):
        lease = self.job.leases.current()
        past = time.time() - 5
        tok = self.mgr.scheduler.issue_lease_token(
            job_id="j1", generation=lease.generation,
            lease_id=lease.lease_id, until_ts=past)
        exc = self._deny(tok)
        self.assertIsInstance(exc, FenceViolation)
        self.assertEqual(exc.reason, "lease_expired")
        self.assertEqual(exc.http_status, 412)

    def test_unknown_lease_id_is_401(self):
        tok = self.mgr.scheduler.issue_lease_token(
            job_id="j1", generation=self.job.output_generation,
            lease_id="0" * 32, until_ts=time.time() + 100)
        exc = self._deny(tok)
        self.assertIsInstance(exc, InvalidToken)
        self.assertEqual(exc.reason, "unknown_lease")
        self.assertEqual(exc.http_status, 401)

    def test_stale_generation_is_412(self):
        # gen0 旧租约 (抢占前), 当前生效代际为 gen1
        old = self.job.leases.get(0)
        exc = self._deny(old.token)
        self.assertIsInstance(exc, FenceViolation)
        self.assertEqual(exc.reason, "stale_generation")

    def test_cross_job_token_is_412_cross_job(self):
        # 另一个作业的合法当前租约, 用在 j1 上
        other_lease, _ = _prepare_released_and_restored(
            self.mgr, jid="j2")
        exc = self._deny(other_lease)
        self.assertIsInstance(exc, FenceViolation)
        self.assertEqual(exc.http_status, 412)
        self.assertEqual(exc.reason, "cross_job_token")

    def test_same_failure_is_deterministic(self):
        # 同一伪造凭证重复拒绝: 状态码/error/reason/消息完全一致
        forged = "v1.abc.def"
        e1 = self._deny(forged, output_id="a")
        e2 = self._deny(forged, output_id="b")
        self.assertEqual(e1.to_dict(), e2.to_dict())

    def test_error_response_and_events_contain_no_raw_credential(self):
        forged = "v1.SECRETPAYLOAD.deadbeefsignature"
        exc = self._deny(forged, output_id="sec")
        # 错误体不含令牌任何片段
        self.assertNotIn("SECRETPAYLOAD", json.dumps(exc.to_dict()))
        self.assertNotIn("deadbeefsignature", json.dumps(exc.to_dict()))
        events_json = json.dumps(self.mgr.security_events("j1"))
        self.assertNotIn("SECRETPAYLOAD", events_json)
        self.assertNotIn("deadbeefsignature", events_json)

    # ----------------------------------------------- 2) 顺序化安全事件
    def test_every_denial_writes_one_ordered_security_event(self):
        denials = [None, "garbage", "v1.abc.def", self.epoch]
        for t in denials:
            self._deny(t)
        events = self.mgr.security_events("j1")
        self.assertEqual(len(events), len(denials))
        seqs = [e["seq"] for e in events]
        self.assertEqual(seqs, sorted(seqs))
        self.assertEqual(len(set(seqs)), len(seqs))  # seq 唯一
        self.assertTrue(all(isinstance(s, int) and s > 0 for s in seqs))
        reasons = [e["reason"] for e in events]
        self.assertEqual(
            reasons,
            ["missing_token", "malformed_token", "bad_signature",
             "wrong_token_type"],
        )
        for e in events:
            self.assertEqual(e["type"], "security_authz_denied")
            self.assertEqual(e["job_id"], "j1")
            self.assertEqual(e["operation"], "append_output")
            self.assertEqual(e["credential"], "output_lease")
            self.assertIn("http_status", e)
            self.assertIn("expected_generation", e)
            # 只含可安全公开的绑定字段, 没有令牌/签名字段
            self.assertNotIn("token", e)
            self.assertNotIn("signature", e)

    def test_denial_never_creates_output_entry(self):
        for t in (None, "garbage", self.epoch,
                  self.job.leases.get(0).token):
            self._deny(t, output_id="never")
        self.assertEqual(self.mgr.list_outputs("j1"), [])

    def test_security_events_visible_in_audit_and_page(self):
        self._deny("garbage")
        audit_events = [a["event"] for a in self.job.audit]
        self.assertIn("security_authz_denied", audit_events)
        page = self.mgr.progress_page_data("j1")
        self.assertGreaterEqual(len(page["security_events"]), 1)

    # ------------------------------------------- 3) 并发不增加结果条目
    def test_concurrent_denied_requests_add_no_entries(self):
        barrier = threading.Barrier(8)

        def hit(i):
            barrier.wait()
            try:
                self.mgr.append_output(
                    "j1", lease_token="garbage",
                    output_id="dup", data=b"x")
            except (InvalidToken, FenceViolation):
                pass

        ts = [threading.Thread(target=hit, args=(i,)) for i in range(8)]
        for t in ts:
            t.start()
        for t in ts:
            t.join()
        self.assertEqual(self.mgr.list_outputs("j1"), [])
        # 每次拒绝仍各有一条安全事件 (重复攻击尝试本身可追踪)
        self.assertEqual(len(self.mgr.security_events("j1")), 8)

    def test_concurrent_valid_duplicate_delivers_once(self):
        barrier = threading.Barrier(8)
        outcomes = []

        def hit():
            barrier.wait()
            r = self.mgr.append_output(
                "j1", lease_token=self.lease, output_id="one",
                data=b"same")
            outcomes.append(r["delivered"])

        ts = [threading.Thread(target=hit) for _ in range(8)]
        for t in ts:
            t.start()
        for t in ts:
            t.join()
        self.assertEqual(sum(outcomes), 1)
        self.assertEqual(len(self.mgr.list_outputs("j1")), 1)

    def test_concurrent_mixed_valid_and_denied(self):
        # 旧租约(拒绝)与新租约(成功)并发同一 output_id: 恰好一条结果
        old = self.job.leases.get(0).token
        start = threading.Barrier(2)
        results = []

        def attacker():
            start.wait()
            try:
                self.mgr.append_output(
                    "j1", lease_token=old, output_id="race",
                    data=b"OLD")
                results.append("attacker_ok")
            except FenceViolation:
                results.append("attacker_denied")

        def writer():
            start.wait()
            r = self.mgr.append_output(
                "j1", lease_token=self.lease, output_id="race",
                data=b"NEW")
            results.append("writer" if r["delivered"] else "writer_dup")

        for _ in range(10):
            ths = [threading.Thread(target=attacker),
                   threading.Thread(target=writer)]
            per = []
            # 简化: 串行小批, 主要断言结果日志不超过一条
            for t in ths:
                t.start()
            for t in ths:
                t.join()
        outs = self.mgr.list_outputs("j1")
        race = [o for o in outs if o["output_id"] == "race"]
        self.assertEqual(len(race), 1)
        self.assertEqual(self.mgr.store.get(race[0]["blob"]), b"NEW")


class EpochCredentialDenialTest(unittest.TestCase):
    def test_forged_and_cross_job_epoch_credentials_are_logged(self):
        mgr = _make_manager()
        nodes = {r: f"n{r}" for r in range(2)}
        mgr.create_job(name="a", world_size=2, job_id="A", rank_nodes=nodes)
        mgr.create_job(name="b", world_size=2, job_id="B", rank_nodes=nodes)
        preA = mgr.preempt("A")
        preB = mgr.preempt("B")

        with self.assertRaises(InvalidToken):
            mgr.commit_checkpoint("A", epoch_token="garbage")
        with self.assertRaises(FenceViolation):
            mgr.commit_checkpoint("A", epoch_token=preB["epoch_token"])

        events = mgr.security_events("A")
        reasons = {e["reason"] for e in events}
        self.assertIn("malformed_token", reasons)
        self.assertIn("epoch_cross_job_or_epoch", reasons)
        for e in events:
            self.assertEqual(e["credential"], "epoch_token")
        # B 的凭证事件不应记在 A (除跨作业被攻击的目标 A)
        self.assertEqual(
            [e["job_id"] for e in mgr.security_events("B")], [])


class PersistenceRestartTest(unittest.TestCase):
    def setUp(self):
        self.data_dir = tempfile.mkdtemp(prefix="hpc-cr-test-")
        self._open_managers = []

    def tearDown(self):
        for m in self._open_managers:
            m.journal.close()
        shutil.rmtree(self.data_dir, ignore_errors=True)

    def _fresh(self):
        m = _make_manager(self.data_dir)
        self._open_managers.append(m)
        return m

    def test_security_events_survive_restart_sequential(self):
        mgr = self._fresh()
        lease, _ = _prepare_released_and_restored(mgr)
        job = mgr.get("j1")
        # 各类拒绝
        mgr.append_output  # noqa
        for tok, name in [
            (None, "missing"),
            ("garbage", "forged"),
            (job.leases.get(0).token, "stale"),
        ]:
            try:
                mgr.append_output("j1", lease_token=tok,
                                  output_id=name, data=b"d")
            except (InvalidToken, FenceViolation):
                pass
        before = mgr.security_events("j1")
        self.assertEqual(len(before), 3)

        # 重启
        mgr2 = self._fresh()
        after = mgr2.security_events("j1")
        self.assertEqual(len(after), 3)
        self.assertEqual([e["seq"] for e in after],
                         [e["seq"] for e in before])
        self.assertEqual(
            [e["reason"] for e in after],
            ["missing_token", "malformed_token", "stale_generation"])
        # 事件文件中没有凭证原文
        with open(__import__("os").path.join(
                self.data_dir, "journal.jsonl"), encoding="utf-8") as fp:
            raw = fp.read()
        self.assertNotIn('"token"', raw)

    def test_outputs_and_state_survive_restart(self):
        mgr = self._fresh()
        lease, _ = _prepare_released_and_restored(mgr)
        mgr.append_output("j1", lease_token=lease, output_id="final",
                          data=b"RESULT")
        mgr2 = self._fresh()
        job = mgr2.get("j1")
        self.assertEqual(job.state, "RUNNING")
        self.assertEqual(job.cpm.get(1).state, COMPLETE)
        outs = mgr2.list_outputs("j1")
        self.assertEqual(len(outs), 1)
        self.assertEqual(outs[0]["output_id"], "final")
        self.assertEqual(mgr2.store.get(outs[0]["blob"]), b"RESULT")
        # 重启后旧凭证依旧被稳定拒绝
        with self.assertRaises(FenceViolation):
            mgr2.append_output(
                "j1", lease_token=job.leases.get(0).token,
                output_id="late", data=b"x")
        events = mgr2.security_events("j1")
        self.assertEqual(events[-1]["reason"], "stale_generation")

    def test_page_and_restore_report_queryable_after_restart(self):
        mgr = self._fresh()
        lease, _ = _prepare_released_and_restored(mgr)
        try:
            mgr.append_output("j1", lease_token="forged",
                              output_id="x", data=b"y")
        except InvalidToken:
            pass
        # 页面聚合数据
        page_before = mgr.progress_page_data("j1")
        n_sec = len(page_before["security_events"])
        # 恢复验证报告 (含安全事件快照)
        rep_before = mgr.last_restore_report_dict("j1")

        mgr2 = self._fresh()
        page_after = mgr2.progress_page_data("j1")
        self.assertEqual(len(page_after["security_events"]), n_sec)
        self.assertTrue(page_after["outputs"] or
                        page_after["checkpoints"])
        rep_after = mgr2.last_restore_report_dict("j1")
        self.assertTrue(rep_after["success"])
        self.assertEqual(
            len(rep_after["security_events"]),
            len(rep_before["security_events"]))
        self.assertEqual(rep_after["lease"]["lease_id"],
                         rep_before["lease"]["lease_id"])
        # Markdown 报告同样可渲染且包含安全事件小节
        report_obj = mgr2.last_restore_report("j1")
        md = report_obj.to_markdown()
        self.assertIn("安全事件", md)

    def test_writes_after_restart_still_dedupe_and_sequence(self):
        mgr = self._fresh()
        lease, _ = _prepare_released_and_restored(mgr)
        mgr.append_output("j1", lease_token=lease, output_id="a",
                          data=b"1")
        mgr2 = self._fresh()
        new_lease = mgr2.get("j1").leases.current().token
        # 同 output_id 重启后重放 + 重复提交幂等
        r = mgr2.append_output("j1", lease_token=new_lease, output_id="a",
                               data=b"1")
        self.assertFalse(r["delivered"])
        # 新 seq 水位在历史最大值之上继续递增
        try:
            mgr2.append_output("j1", lease_token="bad", output_id="z",
                               data=b"2")
        except InvalidToken:
            pass
        mgr3 = self._fresh()
        seqs = [e["seq"] for e in mgr3.security_events("j1")]
        self.assertEqual(seqs, sorted(seqs))
        self.assertEqual(seqs, sorted(set(seqs)))
        self.assertEqual(len(mgr3.list_outputs("j1")), 1)


class HttpDenialPersistenceTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.data_dir = tempfile.mkdtemp(prefix="hpc-cr-http-")
        cls.server = build_server(
            _make_manager(cls.data_dir),
            host="127.0.0.1", port=0, data_dir=None)
        cls.port = cls.server.server_address[1]
        cls.thread = threading.Thread(target=cls.server.serve_forever,
                                      daemon=True)
        cls.thread.start()

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()
        cls.server.manager.journal.close()
        shutil.rmtree(cls.data_dir, ignore_errors=True)

    def _req(self, method, path, body=None, headers=None):
        url = f"http://127.0.0.1:{self.port}{path}"
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(
            url, data=data, method=method,
            headers={"Content-Type": "application/json", **(headers or {})})
        try:
            with urllib.request.urlopen(req, timeout=5) as resp:
                return resp.status, json.loads(resp.read())
        except urllib.error.HTTPError as e:
            return e.code, json.loads(e.read())

    def test_denial_over_http_stable_and_restart_queryable(self):
        st, _ = self._req("POST", "/api/jobs", {
            "job_id": "http-authz", "name": "d", "world_size": 1,
            "rank_nodes": {"0": "h0"}})
        self.assertEqual(st, 201)
        st, pre = self._req("POST", "/api/jobs/http-authz",
                            {"action": "preempt", "grace_seconds": 300})
        self.assertEqual(st, 200)
        tok = pre["epoch_token"]
        self._req("POST", "/api/jobs/http-authz", {
            "action": "upload_shard", "rank": 0, "index": 0,
            "node": "h0", "input_pos": 1,
            "data_b64": base64.b64encode(b"s").decode()},
            headers={"X-Epoch-Token": tok})
        self._req("POST", "/api/jobs/http-authz",
                  {"action": "commit_checkpoint"},
                  headers={"X-Epoch-Token": tok})
        self._req("POST", "/api/jobs/http-authz",
                  {"action": "confirm_release"},
                  headers={"X-Epoch-Token": tok})
        st, restore = self._req("POST", "/api/jobs/http-authz",
                                {"action": "restore", "generation": 1})
        self.assertTrue(restore["success"])

        # 无凭证
        st, missing = self._req("POST", "/api/jobs/http-authz", {
            "action": "append_output", "output_id": "o",
            "data_b64": base64.b64encode(b"d").decode()})
        self.assertEqual(st, 401)
        self.assertEqual(missing["error"], "invalid_token")
        self.assertEqual(missing["reason"], "missing_token")

        # 伪造凭证
        st, forged = self._req("POST", "/api/jobs/http-authz", {
            "action": "append_output", "output_id": "o",
            "data_b64": base64.b64encode(b"d").decode()},
            headers={"X-Lease-Token": "v1.abc.def"})
        self.assertEqual(st, 401)
        self.assertEqual(forged["reason"], "bad_signature")

        # 无新增结果
        st, outs = self._req("GET", "/api/jobs/http-authz/outputs")
        self.assertEqual(outs["outputs"], [])

        # 安全事件查询接口
        st, sec = self._req(
            "GET", "/api/jobs/http-authz/security-events")
        self.assertEqual(st, 200)
        self.assertEqual(len(sec["security_events"]), 2)

        # 重启后通过新服务器查询同样的事件
        self.server.server_close()
        self.server.manager.journal.close()
        mgr2 = _make_manager(self.data_dir)
        server2 = build_server(mgr2, host="127.0.0.1", port=0)
        port2 = server2.server_address[1]
        th = threading.Thread(target=server2.serve_forever, daemon=True)
        th.start()
        try:
            with urllib.request.urlopen(
                    f"http://127.0.0.1:{port2}/api/jobs/http-authz/"
                    "security-events") as r:
                sec2 = json.loads(r.read())
            self.assertEqual(len(sec2["security_events"]), 2)
            with urllib.request.urlopen(
                    f"http://127.0.0.1:{port2}/ui/jobs/http-authz") as r:
                page = r.read().decode()
            self.assertIn("安全事件", page)
        finally:
            server2.shutdown()
            server2.server_close()
            mgr2.journal.close()


if __name__ == "__main__":
    unittest.main()
