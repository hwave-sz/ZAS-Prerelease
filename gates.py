#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""阶段一门禁：①物理拓扑 ③压力语义校准 ④口径证据 ⑤环境指纹（一次跑完）。

门禁② （遥测缺失/过期/恢复）由 Go 测试 `TestKVStore_MissingStaleRecover` 覆盖，不在此处重复。
"""
import hashlib
import json
import os
import subprocess
import sys
import time
import urllib.request

D = "/tmp/ZAS-Prerelease-relay-gw7"
sys.path.insert(0, D)
import phaseD as P  # noqa: E402

ENGINES = {"8001": "http://127.0.0.1:8001", "8002": "http://127.0.0.1:8002"}


def sha(path):
    try:
        return hashlib.sha256(open(path, "rb").read()).hexdigest()[:16]
    except Exception as e:
        return "ERR:%s" % e


def metric_sum(base, name):
    """把某个 counter 的所有标签实例求和。"""
    try:
        with urllib.request.urlopen(base + "/metrics", timeout=10) as r:
            txt = r.read().decode("utf-8", "replace")
    except Exception as e:
        return None, str(e)
    tot = 0.0
    seen = False
    for line in txt.splitlines():
        if line.startswith("#") or not line:
            continue
        nm = line.split("{")[0].split(" ")[0]
        if nm != name:
            continue
        try:
            tot += float(line.rsplit(" ", 1)[1])
            seen = True
        except Exception:
            continue
    return (tot if seen else None), None


def present_metrics(base):
    try:
        with urllib.request.urlopen(base + "/metrics", timeout=10) as r:
            txt = r.read().decode("utf-8", "replace")
    except Exception as e:
        return "ERR:%s" % e
    want = ["vllm:kv_cache_usage_perc", "vllm:num_requests_running", "vllm:num_requests_waiting"]
    return ",".join(w for w in want if w in txt) or "(none)"


def main():
    base_cfg = P.find_base_cfg()
    model = P.detect_model(base_cfg)
    os.environ["KVSH"] = "1"
    cfg = P.patch_yaml(base_cfg, "/tmp/phaseD/cfg_gates.yaml", "utility_v2", True, P.GW_PORT)

    print("=== 门禁⑤ 环境指纹 ===", flush=True)
    print("binary_sha16 = %s" % sha("/tmp/gw7"), flush=True)
    print("config_sha16 = %s  (%s)" % (sha(cfg), cfg), flush=True)
    print("model=%s  seed=20261127  duration=20s  concurrency=8" % model, flush=True)
    for p, u in ENGINES.items():
        print("engine %s metrics_present = %s" % (p, present_metrics(u)), flush=True)

    print("\n=== 门禁① 物理拓扑（跑前计数）===", flush=True)
    before = {}
    for p, u in ENGINES.items():
        s, err = metric_sum(u, "vllm:request_success_total")
        before[p] = s
        print("  %s request_success_total = %s %s" % (p, s, err or ""), flush=True)

    gw = P.start_gw(cfg)
    try:
        if not P.wait_port(P.GW_PORT, 30):
            print("[FATAL] 网关未起")
            return 2
        # 跑 20s / c=8，只为让两个引擎都收到请求（不是性能测试）
        recs = P.run_bench(P.GW, model, 8, 20, "/tmp/phaseD/gates.jsonl")
        summ = P.summarize(recs, 20)
        print("\n=== 门禁④ 口径证据（本次 20s 运行）===", flush=True)
        for k in ("n", "ok", "success", "ttft_p50", "ttft_p99", "tpot_p50",
                  "out_tps_p50", "e2e_p99", "out_tok_per_s", "usage_share_pct", "tokens_src"):
            print("  %-16s %s" % (k, summ.get(k)), flush=True)

        rs = ""
        try:
            with urllib.request.urlopen(P.GW + "/_route_state", timeout=10) as r:
                rs = r.read().decode("utf-8", "replace")
        except Exception as e:
            rs = "err:%s" % e
        dim = [l for l in rs.splitlines() if l.startswith("route_utility_v2")]
        print("  %s" % (dim[0] if dim else "(no route_utility_v2 line)"), flush=True)
        last = [l for l in rs.splitlines() if l.startswith("last_dim")]
        if last:
            print("  %s" % last[-1][:220], flush=True)
        rd = [l for l in rs.splitlines() if l.startswith("m1_shards")]
        if rd:
            print("  %s" % rd[0], flush=True)
    finally:
        P.stop_gw(gw)

    print("\n=== 门禁① 物理拓扑（跑后计数 + 差值）===", flush=True)
    ok_all = True
    for p, u in ENGINES.items():
        after, _ = metric_sum(u, "vllm:request_success_total")
        d = None if (after is None or before[p] is None) else after - before[p]
        print("  %s after=%s delta=%s" % (p, after, d), flush=True)
        if not d or d <= 0:
            ok_all = False
    print("  拓扑判定: %s" % ("PASS（两个引擎都真实收到请求）" if ok_all
                          else "FAIL（有引擎没有收到任何请求 ⇒ 逻辑双副本可能是物理单副本）"), flush=True)

    print("\n=== 门禁③ 压力语义（抓一次原始指标）===", flush=True)
    for p, u in ENGINES.items():
        try:
            with urllib.request.urlopen(u + "/metrics", timeout=10) as r:
                txt = r.read().decode("utf-8", "replace")
            for nm in ("vllm:num_requests_running", "vllm:num_requests_waiting"):
                vals = [l for l in txt.splitlines() if l.split("{")[0].startswith(nm)]
                print("  %s %s -> %s" % (p, nm, vals[:2]), flush=True)
        except Exception as e:
            print("  %s ERR %s" % (p, e), flush=True)
    print("  说明: num_requests_waiting 是**排队数**，不是容量分母；", flush=True)
    print("        ρ_engine 仅在 waiting>0 时启用（0.5+0.1×waiting，封顶 0.95）。", flush=True)
    print("\n=== GATES DONE ===", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
