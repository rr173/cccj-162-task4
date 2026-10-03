"""作业 API: stdlib ThreadingHTTPServer + JSON。

凭证传递约定 (不放进 URL, 避免日志泄露):
  * 抢占纪元令牌: ``X-Epoch-Token`` (写分片 / 提交 / 释放)
  * 输出租约令牌: ``X-Lease-Token`` (提交输出)
二进制分片/输出载荷在 JSON 中以 ``data_b64`` 传递。
"""

from __future__ import annotations

import base64
import binascii
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlsplit

from .epochs import Scheduler
from .errors import CpError
from .jobs import JobManager
from .persistence import load_or_create_secret
from .security import Signer
from .ui import render_progress_page


def _b64_decode(body: dict, field: str = "data_b64") -> bytes:
    raw = body.get(field)
    if not isinstance(raw, str):
        from .errors import ValidationError

        raise ValidationError(f"{field} must be a base64 string")
    try:
        return base64.b64decode(raw, validate=True)
    except (binascii.Error, ValueError) as exc:
        from .errors import ValidationError

        raise ValidationError(f"{field} is not valid base64") from exc


def _optional_int(value):
    if value is None:
        return None
    return int(value)


def build_default_manager(
    secret: str | None = None, data_dir: str | None = None,
    destinations: dict | None = None,
) -> JobManager:
    """构造默认管理器。

    secret 缺省时: 持久化模式 (data_dir 给定) 从环境变量/密钥文件加载,
    保证重启后 HMAC 密钥一致、旧令牌仍可验证; 纯内存模式用固定演示密钥。
    destinations 是迁移目标内容存储配置: 名字 -> ContentStore 或持久化
    目录路径 (字符串时自动创建持久化 ContentStore)。属于配置项, 重启后
    需以同样配置重新提供。
    """
    if secret is None:
        secret = (load_or_create_secret(data_dir)
                  if data_dir is not None else "demo-scheduler-secret")
    from .storage import ContentStore
    stores = {
        name: (ContentStore(path) if isinstance(path, str) else path)
        for name, path in (destinations or {}).items()
    }
    return JobManager(Scheduler(Signer(secret)), data_dir=data_dir,
                      destinations=stores)


class ApiHandler(BaseHTTPRequestHandler):
    server_version = "HpcCP/1.0"
    manager: JobManager = None  # 由 factory 注入到类上

    # 静默标准访问日志, 改由审计事件流记录关键操作
    def log_message(self, fmt, *args):  # noqa: N802
        if self.server.verbose:  # type: ignore[attr-defined]
            super().log_message(fmt, *args)

    # ------------------------------------------------------------- helpers
    def _send_json(self, status: int, obj):
        data = json.dumps(obj, ensure_ascii=False, indent=2).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _read_json(self) -> dict:
        length = int(self.headers.get("Content-Length") or 0)
        if length == 0:
            return {}
        raw = self.rfile.read(length)
        try:
            body = json.loads(raw)
        except json.JSONDecodeError as exc:
            from .errors import ValidationError

            raise ValidationError(f"invalid JSON body: {exc}") from exc
        if not isinstance(body, dict):
            from .errors import ValidationError

            raise ValidationError("request body must be a JSON object")
        return body

    def _handle(self, fn, status=200):
        try:
            result = fn()
        except CpError as exc:
            self._send_json(exc.http_status, exc.to_dict())
        except Exception as exc:  # noqa: BLE001
            self._send_json(500, {"error": "internal_error", "message": str(exc)})
            raise
        else:
            self._send_json(status, result if result is not None else {"ok": True})

    @property
    def mgr(self) -> JobManager:
        return self.manager  # type: ignore[return-value]

    # --------------------------------------------------------------- verbs
    def do_GET(self):  # noqa: N802
        parts = [p for p in urlsplit(self.path).path.split("/") if p]
        try:
            if parts == ["healthz"]:
                return self._send_json(200, {"ok": True})
            if parts == [""]:
                return self._html(
                    render_index(self.mgr), self.headers.get("Accept", "")
                )
            if parts == ["api", "jobs"]:
                return self._handle(lambda: {
                    "jobs": [j.snapshot() for j in self.mgr.list_jobs()]})
            if len(parts) == 3 and parts[:2] == ["api", "jobs"]:
                return self._handle(
                    lambda jid=parts[2]: self.mgr.get(jid).snapshot())
            if len(parts) == 4 and parts[:2] == ["api", "jobs"] \
                    and parts[3] == "progress.json":
                return self._handle(
                    lambda jid=parts[2]: self.mgr.progress_page_data(jid))
            if len(parts) == 4 and parts[:2] == ["api", "jobs"] \
                    and parts[3] == "checkpoints":
                return self._handle(lambda jid=parts[2]: {
                    "checkpoints": [c.to_dict()
                                    for c in self.mgr.get(jid).cpm.list_all()]})
            if len(parts) == 4 and parts[:2] == ["api", "jobs"] \
                    and parts[3] == "outputs":
                return self._handle(
                    lambda jid=parts[2]: {"outputs": self.mgr.list_outputs(jid)})
            if len(parts) == 4 and parts[:2] == ["api", "jobs"] \
                    and parts[3] == "security-events":
                return self._handle(lambda jid=parts[2]: {
                    "security_events": self.mgr.security_events(jid)})
            if len(parts) == 5 and parts[:2] == ["api", "jobs"] \
                    and parts[3:] == ["restore", "report"]:
                return self._handle(
                    lambda jid=parts[2]:
                        self.mgr.last_restore_report_dict(jid))
            if parts == ["api", "gc", "plans"]:
                return self._handle(
                    lambda: {"plans": self.mgr.list_gc_plans()})
            if parts == ["api", "migrations"]:
                return self._handle(
                    lambda: {"migrations": self.mgr.list_migrations(),
                             "destinations": self.mgr.list_destinations()})
            if len(parts) == 3 and parts[:2] == ["api", "migrations"]:
                return self._handle(
                    lambda tid=parts[2]: self.mgr.get_migration(tid))
            if len(parts) == 4 and parts[:3] == ["api", "gc", "plans"]:
                return self._handle(
                    lambda pid=parts[3]: self.mgr.get_gc_plan(pid))
            if len(parts) == 5 and parts[:2] == ["api", "jobs"] \
                    and parts[3:] == ["gc", "pins"]:
                return self._handle(lambda jid=parts[2]:
                                    self.mgr.generation_pins(jid))
            if len(parts) == 4 and parts[:2] == ["api", "jobs"] \
                    and parts[3] == "migrations":
                return self._handle(lambda jid=parts[2]: {
                    "migrations": self.mgr.list_migrations(jid),
                    "published": self.mgr.published_generations(jid)})
            if parts[:1] == ["ui"] and len(parts) == 3 and parts[1] == "jobs":
                data = self.mgr.progress_page_data(parts[2])
                return self._html(
                    render_progress_page(data), self.headers.get("Accept", ""))
        except CpError as exc:
            return self._send_json(exc.http_status, exc.to_dict())
        self._send_json(404, {"error": "not_found", "message": self.path})

    def do_POST(self):  # noqa: N802
        parts = [p for p in urlsplit(self.path).path.split("/") if p]
        body = self._safe_body()
        if isinstance(body, CpError):
            return self._send_json(body.http_status, body.to_dict())

        def epoch_header():
            # 缺失凭证交由 JobManager 统一归类 (401 missing_token) 并写入
            # 安全事件, 与输出租约路径保持一致。
            return self.headers.get("X-Epoch-Token")

        def lease_header():
            # 缺失凭证交由 JobManager 统一归类 (401 missing_token) 并写入
            # 安全事件, 因此这里返回 None 而不是直接拒绝。
            return self.headers.get("X-Lease-Token")

        try:
            if parts == ["api", "gc", "plans"]:
                return self._handle(lambda: self.mgr.create_gc_plan(
                    policy=body.get("policy", "recoverable"),
                    min_retain_generation=_optional_int(
                        body.get("min_retain_generation")),
                    retain_generations=body.get("retain_generations"),
                    include_pins=bool(body.get("include_pins", True)),
                    plan_id=body.get("plan_id"),
                ), status=201)
            if parts == ["api", "migrations"]:
                return self._handle(lambda: self.mgr.create_migration(
                    body["job_id"],
                    generation=int(body["generation"]),
                    destination=body["destination"],
                    task_id=body.get("task_id"),
                ), status=201)
            if len(parts) == 3 and parts[:2] == ["api", "migrations"]:
                tid = parts[2]
                action = body.get("action", "run")
                if action == "run":
                    return self._handle(lambda: self.mgr.run_migration(
                        tid,
                        max_items=_optional_int(body.get("max_items"))))
                if action == "cancel":
                    return self._handle(lambda: self.mgr.cancel_migration(
                        tid, reason=body.get("reason", "operator_cancel")))
                self._send_json(404, {"error": "not_found",
                                      "message": self.path})
                return
            if len(parts) == 4 and parts[:3] == ["api", "gc", "plans"]:
                pid = parts[3]
                action = body.get("action", "execute")
                if action == "execute":
                    return self._handle(lambda: self.mgr.execute_gc_plan(pid))
                self._send_json(404, {"error": "not_found",
                                      "message": self.path})
                return
            if parts == ["api", "jobs"]:
                return self._handle(lambda: self.mgr.create_job(
                    name=body.get("name", "unnamed"),
                    world_size=int(body["world_size"]),
                    shards_per_rank=int(body.get("shards_per_rank", 1)),
                    rank_nodes={int(k): v for k, v in
                                (body.get("rank_nodes") or {}).items()} or None,
                    job_id=body.get("job_id"),
                    lease_ttl=float(body.get("lease_ttl", 300.0)),
                ).snapshot(), status=201)

            if len(parts) != 3 or parts[:2] != ["api", "jobs"]:
                self._send_json(404, {"error": "not_found",
                                      "message": self.path})
                return
            jid = parts[2]
            action = body.get("action")

            if action == "preempt":
                return self._handle(lambda: self.mgr.preempt(
                    jid,
                    reason=body.get("reason", "scheduler_reclaim"),
                    kind=body.get("kind", "all"),
                    nodes=body.get("nodes"),
                    grace_seconds=float(body.get("grace_seconds", 30.0)),
                    force_new_epoch=bool(body.get("force_new_epoch", False)),
                ))
            if action == "heartbeat":
                return self._handle(lambda: self.mgr.heartbeat(
                    jid, body["node_id"]))
            if action == "mark_node_lost":
                return self._handle(lambda: self.mgr.mark_node_lost(
                    jid, body["node_id"]))
            if action == "progress":
                return self._handle(lambda: self.mgr.report_progress(
                    jid, int(body["rank"]), int(body["input_pos"])))
            if action == "upload_shard":
                return self._handle(lambda: self.mgr.upload_shard(
                    jid,
                    epoch_token=epoch_header(),
                    rank=int(body["rank"]),
                    index=int(body.get("index", 0)),
                    node=body["node"],
                    input_pos=int(body["input_pos"]),
                    data=_b64_decode(body),
                ))
            if action == "commit_checkpoint":
                return self._handle(lambda: self.mgr.commit_checkpoint(
                    jid, epoch_token=epoch_header()))
            if action == "confirm_release":
                return self._handle(lambda: self.mgr.confirm_release(
                    jid, epoch_token=epoch_header()))
            if action == "abandon":
                return self._handle(lambda: self.mgr.abort_epoch(
                    jid, reason=body.get("reason", "grace_expired")))
            if action == "append_output":
                return self._handle(lambda: self.mgr.append_output(
                    jid,
                    lease_token=lease_header(),
                    output_id=body["output_id"],
                    data=_b64_decode(body),
                    rank=body.get("rank"),
                ))
            if action == "pin_generation":
                return self._handle(lambda: self.mgr.pin_generation(
                    jid, int(body["generation"]),
                    reason=body.get("reason", "manual"),
                    unpin=bool(body.get("unpin", False))))
            if action == "restore":
                placement = body.get("new_rank_nodes")
                return self._handle(lambda: self.mgr.restore(
                    jid,
                    generation=int(body["generation"]),
                    new_rank_nodes={int(k): v for k, v in placement.items()}
                    if placement else None,
                    lease_ttl=body.get("lease_ttl"),
                ).to_dict())
        except (KeyError, TypeError, ValueError) as exc:
            return self._send_json(400, {"error": "validation_error",
                                         "message": f"bad request field: {exc}"})
        self._send_json(404, {"error": "not_found", "message": self.path})

    def _safe_body(self):
        try:
            return self._read_json()
        except CpError as exc:
            return exc

    # ----------------------------------------------------------------- html
    def _html(self, html: str, accept: str):
        data = html.encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)


def render_index(mgr: JobManager) -> str:
    rows = "".join(
        f'<tr><td><a href="/ui/jobs/{j.job_id}">{j.job_id}</a></td>'
        f"<td>{j.name}</td><td>{j.state}</td>"
        f"<td>{j.world_size}×{j.shards_per_rank}</td>"
        f"<td>gen{j.current_generation}</td></tr>"
        for j in mgr.list_jobs()
    )
    return f"""<!doctype html><html lang="zh-CN"><head><meta charset="utf-8">
<title>HPC 抢占/检查点服务</title></head><body>
<h1>HPC 作业抢占与检查点恢复</h1>
<p><a href="/api/jobs">/api/jobs (JSON)</a></p>
<table border="1" cellpadding="6"><tr>
<th>作业</th><th>名称</th><th>状态</th><th>规模</th><th>当前代际</th></tr>
{rows or '<tr><td colspan=5>暂无作业</td></tr>'}
</table></body></html>"""


def build_server(
    manager: JobManager | None = None,
    host: str = "127.0.0.1",
    port: int = 8080,
    verbose: bool = False,
    data_dir: str | None = None,
    destinations: dict | None = None,
) -> ThreadingHTTPServer:
    manager = manager or build_default_manager(data_dir=data_dir,
                                               destinations=destinations)

    class _Handler(ApiHandler):
        pass

    _Handler.manager = manager
    server = ThreadingHTTPServer((host, port), _Handler)
    server.manager = manager  # type: ignore[attr-defined]
    server.verbose = verbose  # type: ignore[attr-defined]
    server.daemon_threads = True
    return server


def serve_forever(host: str = "127.0.0.1", port: int = 8080,
                  verbose: bool = False, data_dir: str | None = None,
                  destinations: dict | None = None) -> None:  # pragma: no cover
    server = build_server(host=host, port=port, verbose=verbose,
                          data_dir=data_dir, destinations=destinations)
    print(f"HPC checkpoint service on http://{host}:{port}  "
          f"(UI: http://{host}:{port}/ui/jobs/<job_id>)")
    if data_dir:
        print(f"persistent data directory: {data_dir}")
    for name in server.manager.list_destinations():
        print(f"migration destination registered: {name}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        server.shutdown()
