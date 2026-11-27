#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""纯负载正反切换（W 完全相等，排除亲和干扰）。

为什么这样设计（负责人 09:24 指正后）：
  上一轮 acc.py 用固定 prompt，两候选 W 差 48 GPU-ms（4163 vs 4211，1.1%），
  实测 kv=0.01 折成 penalty 只把 4163 抬到 4205，**刚好差 6 没跨过 4211**。
  所以 A3 失败不是"信号太弱"，而是"恰好停在临界点下方 0.14pp"。

本轮把 W 做成**完全相等**：每次探针用带随机后缀的内容，
两实例都 hit=0、InputTokens 相同 ⇒ W_A ≡ W_B，此时**任何**负载差都必然改变选择。
这就把"负载维度能不能改变选择"与"亲和 vs 负载谁更强"两个问题分离了。

阶段：
  P0 无压         → 平手，按 inst 字典序（应 01）
  P1 压 8001 (8)  → 期望翻到 02
  P2 压 8001 (16) → 若 P1 未翻，看是否随 ρ 上升翻转（临界点扫描）
  P3 压 8002 (8)  → 期望翻回 01
  P4 停压         → 回平手，且必须是"新鲜的 0"而非过期
"""
import json
import os
import random
import signal
import subprocess
import sys
import time
import urllib.request

D = "/tmp/ZAS-Prerelease-relay-gw7"
sys.path.insert(0, D)
import phaseD as P  # noqa: E402

RND = random.Random(20261127)


def hog(port, conc, dur=240):
    return subprocess.Popen(
        ["python3", os.path.join(D, "hog.py"), str(port), str(conc), "384", str(dur)],
        stdout=open("/tmp/hog_acc2.log", "a"), stderr=subprocess.STDOUT,
        preexec_fn=os.setsid)


def kill(p):
    if p is None:
        return
    try:
        os.killpg(os.getpgid(p.pid), signal.SIGTERM)
    except Exception:
        pass
    try:
        p.wait(timeout=8)
    except Exception:
        pass


def probe(model, tag):
    """带随机后缀 ⇒ 两实例都不可能命中前缀 ⇒ hit=0、W 相等。"""
    q = "验收%d：请只回答 OK。" % RND.randrange(10 ** 9)
    body = json.dumps({
        "model": model,
        "messages": [{"role": "user", "content": q}],
        "max_tokens": 16, "temperature": 0, "stream": True,
        "stream_options": {"include_usage": True},
    }).encode()
    r = urllib.request.Request(P.GW + "/v1/chat/completions", data=body,
                               headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(r, timeout=90) as resp:
        for _ in resp:
            pass


def lastdim():
    with urllib.request.urlopen(P.GW + "/_route_state", timeout=10) as r:
        ls = [l for l in r.read().decode("utf-8", "replace").splitlines()
              if l.startswith("last_dim")]
    return ls[-1] if ls else "(none)"


def short(line):
    """压成一行关键信息：chosen / by / 两候选 p 与 kv。"""
    try:
        head = line.split("chosen=")[1]
        chosen = head.split()[0]
        by = head.split("by=")[1].split()[0]
        body = line[line.index("["):]
        parts = []
        for seg in body.strip("[]").split("|"):
            seg = seg.strip()
            nm = seg.split(":")[0].split("-")[-1]
            p = seg.split("p=")[1].split()[0]
            kv = seg.split("kv=")[1].split()[0]
            w = seg.split("w=")[1].split()[0]
            parts.append("%s w=%s p=%s kv=%s" % (nm, w, p, kv))
        return "chosen=%s by=%s | %s" % (chosen, by, " ; ".join(parts))
    except Exception:
        return line[:160]


def step(name, model, n=4, gap=2):
    print("\n### %s" % name, flush=True)
    for _ in range(n):
        try:
            probe(model, name)
        except Exception as e:
            print("  probe err: %s" % e, flush=True)
        print("  " + short(lastdim()), flush=True)
        time.sleep(gap)


def main():
    base = P.find_base_cfg()
    model = P.detect_model(base)
    os.environ["KVSH"] = "1"
    cfg = P.patch_yaml(base, "/tmp/phaseD/cfg_acc2.yaml", "utility_v2", True, P.GW_PORT)
    print("[cfg] %s" % cfg, flush=True)
    gw = P.start_gw(cfg)
    h = None
    try:
        if not P.wait_port(P.GW_PORT, 30):
            print("[FATAL] 网关未起")
            return 2
        step("P0 无压（W 相等 ⇒ 应平手，字典序 01）", model, n=3, gap=2)

        h = hog(8001, 8)
        time.sleep(15)
        step("P1 压 8001 x8（期望翻到 02）", model, n=4)
        kill(h)

        h = hog(8001, 16)
        time.sleep(15)
        step("P2 压 8001 x16（临界点扫描）", model, n=4)
        kill(h)

        time.sleep(4)
        h = hog(8002, 8)
        time.sleep(15)
        step("P3 压 8002 x8（期望翻回 01）", model, n=4)
        kill(h)

        time.sleep(6)
        step("P4 停压（回平手；需为新鲜 0 而非过期）", model, n=3, gap=2)
    finally:
        kill(h)
        P.stop_gw(gw)
        print("\n=== ACC2 DONE ===", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
