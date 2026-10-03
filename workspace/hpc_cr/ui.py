"""节点进度页面 (服务端渲染 HTML, 2 秒自动刷新)。

展示: 作业与抢占纪元概览、检查点分片矩阵、节点心跳/失联、进程位点、
输出条目、审计事件流 (含栅栏拒绝记录)。
"""

from __future__ import annotations

import html
import time


def _h(v) -> str:
    return html.escape(str(v), quote=True)


_STATE_COLORS = {
    "COMPLETE": "#0a7d2c",
    "PENDING": "#b26a00",
    "INCOMPLETE": "#b00020",
    "SUPERSEDED": "#666",
    "RUNNING": "#0a7d2c",
    "PREEMPTING": "#b26a00",
    "RELEASED": "#1a4fb3",
    "ABORTED": "#b00020",
    "UP": "#0a7d2c",
    "ALIVE": "#0a7d2c",
    "LOST": "#b00020",
    "PLANNED": "#1a4fb3",
    "RUNNING": "#b26a00",
    "COMPLETED": "#0a7d2c",
    "PARTIALLY_COMPLETED": "#b26a00",
    "STALE": "#b00020",
    "DELETED": "#0a7d2c",
    "SKIPPED": "#666",
    "FAILED": "#b00020",
    "TRANSFERRING": "#b26a00",
    "PUBLISHED": "#0a7d2c",
    "CANCELLED": "#666",
    "TRANSFERRED": "#0a7d2c",
    "REUSED": "#1a4fb3",
}


def _badge(state: str) -> str:
    color = _STATE_COLORS.get(state, "#444")
    return (f'<span style="background:{color};color:#fff;padding:2px 8px;'
            f'border-radius:10px;font-size:12px">{_h(state)}</span>')


def _ts(ts: float | None) -> str:
    if not ts:
        return "-"
    return time.strftime("%H:%M:%S", time.localtime(ts))


def render_gc_section(gc_plans: list[dict], job_id: str) -> str:
    if not gc_plans:
        return """
        <h2>空间回收计划</h2>
        <p><small>暂无回收计划 · <code>POST /api/gc/plans</code> 生成计划</small></p>
        """
    blocks = []
    for p in gc_plans[:10]:
        candidate_rows = "".join(
            f"<tr><td><code>{_h(c['blob'][:16])}</code></td>"
            f"<td>{_h(c['kind'])}</td><td>{c['size']}</td>"
            f"<td>{_badge(c['status'])}</td>"
            f"<td>{_h(c.get('skip_reason') or '')}</td>"
            f"<td><small>{_h(c.get('eligible_reason', ''))}</small></td></tr>"
            for c in p.get("candidates", [])[:50]
        ) or '<tr><td colspan="6">无候选</td></tr>'
        blocks.append(f"""
        <h3>计划 <code>{_h(p['plan_id'])}</code> {_badge(p['state'])}</h3>
        <p><small>
        水位 seq {p['watermark_seq']} · 完成 seq {p.get('completion_seq') or '-'}
        · 策略 {_h(p['policy'])} · 锁定 {_h(p.get('pins', {}))}
        · 最小保留代际 {p.get('min_retain_generation') if p.get('min_retain_generation') is not None else '-'}
        </small></p>
        <table border="1" cellpadding="6">
          <tr><th>预计释放项</th><th>预计释放字节</th><th>实际删除项</th>
          <th>实际释放字节</th><th>跳过</th><th>结果/原因</th></tr>
          <tr><td>{p['estimated_reclaim_items']}</td>
          <td>{p['estimated_reclaim_bytes']}</td>
          <td>{p['deleted_items']}</td><td>{p['deleted_bytes']}</td>
          <td>{p['skipped_items']}</td>
          <td>{_h(p.get('stale_reason') or p.get('error') or '成功')}
          {_h(p.get('skip_reasons') or '')}</td></tr>
        </table>
        <details><summary>候选/删除明细 (前 50 项)</summary>
        <table border="1" cellpadding="6"><tr><th>blob</th><th>类型</th>
        <th>大小</th><th>状态</th><th>跳过原因</th><th>入选原因</th></tr>
        {candidate_rows}</table></details>""")
    return """
    <h2>空间回收计划 (先计划、锁代际、按水位执行)</h2>
    <p><small><a href="/api/gc/plans">全部计划 JSON</a> ·
    <a href="/api/jobs/%s/gc/pins">本作业代际锁</a></small></p>
    %s
    """ % (_h(job_id), "".join(blocks))


def render_migration_section(migrations: list[dict],
                             published: list[dict]) -> str:
    """迁移任务进度: 计划总量/已传输/复用/失败/最终状态与原因。"""
    if not migrations:
        return """
        <h2>检查点迁移任务</h2>
        <p><small>暂无迁移任务 · <code>POST /api/migrations</code> 创建
        (job_id, generation, destination)</small></p>
        """
    blocks = []
    for t in migrations[:10]:
        item_rows = "".join(
            "<tr><td><code>{}</code></td><td>{}</td><td>{}</td>"
            "<td>{}</td><td><small>{}</small></td></tr>".format(
                _h(it["blob"][:16]), it["size"],
                _h(", ".join(r["kind"] + ":" + r["ref_id"]
                             for r in it["references"])),
                _badge(it["status"]),
                _h(it.get("error") or ""))
            for it in t.get("items", [])[:50]
        ) or '<tr><td colspan="5">无清单项</td></tr>'
        reason = (t.get("error") or t.get("cancel_reason")
                  or t.get("state_reason") or "")
        cleanup = t.get("cleanup") or {}
        cleanup_html = ""
        if cleanup and t["state"] in ("FAILED", "CANCELLED"):
            cleanup_html = (
                f" · 目标端清理: 删除 {cleanup.get('deleted_items', 0)} 项/"
                f"{cleanup.get('deleted_bytes', 0)} 字节, "
                f"保留 {cleanup.get('kept_items', 0)} 项"
                + (" (目标存储未注册, 已跳过)" if cleanup.get("skipped")
                   else ""))
        published_html = ""
        if t.get("published"):
            rec = t["published"]
            published_html = (
                f"<p><small>✅ 已发布可恢复代际 gen{rec['generation']} → "
                f"<code>{_h(rec['destination'])}</code> · 清单摘要 "
                f"<code>{_h(rec['manifest_digest'][:16])}</code> · "
                f"发布于 {_ts(rec['published_ts'])}</small></p>")
        blocks.append(f"""
        <h3>任务 <code>{_h(t['task_id'])}</code> {_badge(t['state'])}</h3>
        <p><small>代际 gen{t['generation']} → 目标存储
        <code>{_h(t['destination'])}</code> · 清单摘要
        <code>{_h(t['manifest_digest'][:16])}</code> · 创建于
        {_ts(t.get('created_ts'))} · 结束于 {_ts(t.get('finished_ts'))}
        {cleanup_html}</small></p>
        <table border="1" cellpadding="6">
          <tr><th>计划总量</th><th>已传输项</th><th>复用项</th>
          <th>失败项</th><th>待传输</th><th>最终状态/原因</th></tr>
          <tr><td>{t['planned_items']} 项 / {t['planned_bytes']} 字节</td>
          <td>{t['transferred_items']} 项 / {t['transferred_bytes']} 字节</td>
          <td>{t['reused_items']} 项 / {t['reused_bytes']} 字节</td>
          <td>{t['failed_items']}</td>
          <td>{t['pending_items']}</td>
          <td>{_badge(t['state'])} {_h(reason)}</td></tr>
        </table>
        {published_html}
        <details><summary>清单项明细 (前 50 项)</summary>
        <table border="1" cellpadding="6"><tr><th>blob</th><th>大小</th>
        <th>引用</th><th>状态</th><th>错误</th></tr>
        {item_rows}</table></details>""")
    pub_rows = "".join(
        f"<tr><td>gen{r['generation']}</td>"
        f"<td><code>{_h(r['destination'])}</code></td>"
        f"<td><code>{_h(r['manifest_digest'][:16])}</code></td>"
        f"<td>{r['planned_items']}</td>"
        f"<td>{r['transferred_items']}</td><td>{r['reused_items']}</td>"
        f"<td>{_ts(r['published_ts'])}</td></tr>"
        for r in published
    )
    published_table = ""
    if pub_rows:
        published_table = f"""
        <h3>已发布可恢复代际 (目标存储可用)</h3>
        <table border="1" cellpadding="6"><tr><th>代际</th><th>目标存储</th>
        <th>清单摘要</th><th>总项</th><th>传输</th><th>复用</th>
        <th>发布时间</th></tr>{pub_rows}</table>"""
    return """
    <h2>检查点迁移任务 (冻结清单 → 断点续传 → 校验后原子发布)</h2>
    <p><small><a href="/api/migrations">全部迁移任务 JSON</a></small></p>
    %s
    %s
    """ % ("".join(blocks), published_table)


def render_progress_page(data: dict) -> str:
    job = data["job"]
    ep = job.get("epoch")
    cp = job.get("current_checkpoint")
    shards = data["shards"]
    audit = data["audit"]

    # --- 分片矩阵: 每个代际一块, 行=rank 列=shard index ---
    matrices = []
    for c in data["checkpoints"]:
        g = c["generation"]
        head = "".join(
            f"<th>s{i}</th>" for i in range(c["shards_per_rank"])
        )
        rows = []
        for r in range(c["world_size"]):
            cells = []
            for i in range(c["shards_per_rank"]):
                key = f"gen{g}:r{r}:s{i}"
                s = shards.get(key)
                if s:
                    suffix = ""
                    if s.get("collected_by"):
                        suffix = f'<br><small>已回收 {_h(s["collected_by"][:12])}</small>'
                    cells.append(
                        '<td title="blob ' + _h(s["blob"][:16]) + ' @'
                        + _h(s["node"]) + '" style="background:#e6f4ea">✓ '
                        + _h(s["blob"][:8]) + f'<br><small>pos {s["input_pos"]}'
                        + suffix + "</small></td>"
                    )
                else:
                    cells.append('<td style="background:#fdecea">—</td>')
            node = next(
                (p["node"] for p in c["processes"] if p["rank"] == r), "?"
            )
            rows.append(
                f"<tr><td><b>r{r}</b><br><small>{_h(node)}</small></td>"
                + "".join(cells) + "</tr>"
            )
        matrices.append(f"""
        <h3>检查点 gen{g} (epoch {c['epoch']}) {_badge(c['state'])}</h3>
        <table border="1" cellpadding="6" style="border-collapse:collapse">
        <tr><th>rank @ node</th>{head}</tr>
        {''.join(rows)}
        </table>
        <p><small>进度 {c['received']}/{c['expected']} 分片,
        缺失: {_h(c['missing'] or '无')}
        {'· 完成于 ' + _ts(c['completed_ts']) if c['completed_ts'] else ''}
        </small></p>""")

    node_rows = "".join(
        f"<tr><td>{_h(n['node_id'])}</td><td>{_badge(n['state'])}</td>"
        f"<td>{_ts(n['last_heartbeat'])}</td>"
        f"<td>{_h(n['ranks'])}</td></tr>"
        for n in data["nodes"]
    )
    proc_rows = "".join(
        f"<tr><td>r{p['rank']}</td><td>{_h(p['node'])}</td>"
        f"<td>{_badge(p['state'])}</td>"
        f"<td>inc {p['incarnation']}</td>"
        f"<td>{p['last_input_pos']}</td>"
        f"<td>{_ts(p['last_progress_ts'])}</td></tr>"
        for p in data["processes"]
    )
    out_rows = "".join(
        f"<tr><td>gen{o['generation']}</td><td>{_h(o['output_id'])}</td>"
        f"<td>{_h(o['blob'][:12])}</td><td>{o['size']}</td>"
        f"<td>{_h(o['rank'])}</td><td>{_ts(o['ts'])}</td>"
        f"<td>{_h(o.get('collected_by', ''))}</td></tr>"
        for o in data["outputs"]
    ) or '<tr><td colspan=7>暂无输出</td></tr>'
    audit_rows = "".join(
        f"<tr><td>{_ts(a['ts'])}</td><td><code>{_h(a['event'])}</code></td>"
        f"<td><small>{_h(a['detail'])}</small></td></tr>"
        for a in audit[:60]
    )
    security = data.get("security_events") or []
    sec_rows = "".join(
        f"<tr><td>{_h(e.get('seq'))}</td><td>{_ts(e['ts'])}</td>"
        f"<td><code>{_h(e.get('credential'))}</code></td>"
        f"<td><code>{_h(e.get('operation'))}</code></td>"
        f"<td>{_h(e.get('reason'))}</td>"
        f"<td>{_h(e.get('http_status'))}</td>"
        f"<td>{_h(e.get('expected_generation'))}</td>"
        f"<td><small>{_h(e.get('output_id', ''))} "
        f"{_h(e.get('claimed', {}))}</small></td></tr>"
        for e in security[:100]
    ) or '<tr><td colspan=8>暂无安全事件</td></tr>'

    grace = job.get("epoch_grace_remaining")
    ep_html = ""
    if ep:
        ep_html = f"""
        <h2>抢占纪元 epoch {ep['epoch']} / gen{ep['generation']}</h2>
        <p>原因: {_h(ep['reason'])} · 范围: {_h(ep['kind'])}
        {_h(ep['nodes'])} · 剩余宽限: <b>{grace:.1f}s</b>
        · 截止 {_ts(ep['deadline_ts'])}</p>"""

    return f"""<!doctype html><html lang="zh-CN"><head>
<meta charset="utf-8"><meta http-equiv="refresh" content="2">
<title>节点进度 — {_h(job['job_id'])}</title>
<style>
 body{{font-family:system-ui,sans-serif;margin:24px;color:#222}}
 table{{border-collapse:collapse;margin:8px 0 20px}}
 td,th{{font-size:13px;text-align:left;vertical-align:top}}
 code{{background:#f4f4f4;padding:1px 4px;border-radius:3px}}
</style></head><body>
<p><a href="/">← 作业列表</a> · <a href="/api/jobs/{_h(job['job_id'])}/progress.json">JSON</a></p>
<h1>作业 {_h(job['job_id'])} <small>({_h(job['name'])})</small> {_badge(job['state'])}</h1>
<p>规模 {job['world_size']} 进程 × {job['shards_per_rank']} 分片 ·
当前代际 gen{job['current_generation']} ·
输出生效代际 gen{job['output_generation']} ·
已释放代际 {('gen'+str(job['released_generation'])) if job['released_generation'] is not None else '-'} ·
已恢复代际 {('gen'+str(job['restored_generation'])) if job['restored_generation'] is not None else '-'} ·
输出条目 {job['outputs_total']}</p>
{ep_html}
<h2>检查点分片矩阵</h2>
{''.join(matrices) or '<p>尚无检查点</p>'}
{render_gc_section(data.get('gc_plans') or [], job['job_id'])}
{render_migration_section(data.get('migrations') or [],
                          data.get('published_migrations') or [])}
<h2>节点</h2>
<table border="1" cellpadding="6"><tr><th>节点</th><th>状态</th>
<th>最近心跳</th><th>ranks</th></tr>{node_rows}</table>
<h2>进程与输入位点</h2>
<table border="1" cellpadding="6"><tr><th>rank</th><th>节点</th><th>状态</th>
<th>incarnation</th><th>input_pos</th><th>上报时间</th></tr>{proc_rows}</table>
<h2>已提交输出 (代际隔离, 幂等去重)</h2>
<table border="1" cellpadding="6"><tr><th>代际</th><th>output_id</th>
<th>blob</th><th>size</th><th>rank</th><th>时间</th><th>回收计划</th></tr>{out_rows}</table>
<h2>安全事件 — 授权失败 (顺序化, 不含凭证原文)</h2>
<p><small>共 {len(security)} 条, 显示最近 100 条 ·
<a href="/api/jobs/{_h(job['job_id'])}/security-events">JSON</a></small></p>
<table border="1" cellpadding="6"><tr><th>seq</th><th>时间</th>
<th>凭证类型</th><th>操作</th><th>原因</th><th>HTTP</th>
<th>期望代际</th><th>目标/声明绑定</th></tr>{sec_rows}</table>
<h2>审计事件 (最近 60 条)</h2>
<table border="1" cellpadding="6"><tr><th>时间</th><th>事件</th>
<th>详情</th></tr>{audit_rows}</table>
<p><small>页面每 2 秒自动刷新 · server_ts {_ts(data['server_ts'])}</small></p>
</body></html>"""
